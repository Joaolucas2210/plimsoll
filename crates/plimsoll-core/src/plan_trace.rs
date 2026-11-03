//! Planning events for query traces (ADR 0056).
//!
//! Planning runs deep inside DataFusion (catalog lookups in schema
//! providers, connector `scan`s), where nothing request-scoped is in reach.
//! A task-local collector bridges that: the front door wraps a query's
//! planning in [`collect`], and code anywhere in that task calls
//! [`record`]. Outside a collection, `record` is a no-op.
//!
//! ponytail: events, not live spans. Planning is short and sequential per
//! query, so spans are rebuilt from (start, duration) afterwards, like the
//! phase spans (ADR 0031). Upgrade path: `tracing` spans if work below
//! planning (execution operators) needs tracing.

use std::cell::RefCell;
use std::future::Future;
use std::time::{Duration, Instant, SystemTime};

/// Something that happened while planning one query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlanEvent {
    pub name: &'static str,
    pub start: SystemTime,
    pub duration: Duration,
    pub attrs: Vec<(&'static str, String)>,
}

tokio::task_local! {
    static EVENTS: RefCell<Vec<PlanEvent>>;
}

/// Runs `fut` collecting the events [`record`]ed in its task.
pub async fn collect<F: Future>(fut: F) -> (F::Output, Vec<PlanEvent>) {
    EVENTS
        .scope(RefCell::new(Vec::new()), async {
            let out = fut.await;
            let events = EVENTS.with(|e| e.take());
            (out, events)
        })
        .await
}

/// Records an event that began at `started` and ends now.
pub fn record(name: &'static str, started: Instant, attrs: Vec<(&'static str, String)>) {
    let duration = started.elapsed();
    let start = SystemTime::now()
        .checked_sub(duration)
        .unwrap_or(SystemTime::UNIX_EPOCH);
    let _ = EVENTS.try_with(|e| {
        e.borrow_mut().push(PlanEvent {
            name,
            start,
            duration,
            attrs,
        });
    });
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn events_are_collected_per_task_and_ignored_outside() {
        record("outside", Instant::now(), vec![]);
        let ((), events) = collect(async {
            let started = Instant::now();
            tokio::task::yield_now().await;
            record("inside", started, vec![("k", "v".into())]);
        })
        .await;
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].name, "inside");
        assert_eq!(events[0].attrs, [("k", "v".to_owned())]);
        // A concurrent collection does not see another's events.
        let (a, b) = tokio::join!(
            collect(async { record("a", Instant::now(), vec![]) }),
            collect(async { record("b", Instant::now(), vec![]) }),
        );
        assert_eq!((a.1[0].name, b.1[0].name), ("a", "b"));
    }
}

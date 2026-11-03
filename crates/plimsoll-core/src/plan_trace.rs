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
//! phase spans (ADR 0031). Execution operators are traced the same way
//! (ADR 0071): [`operators`] reads each executed node's metrics, whose
//! start and end timestamps DataFusion records itself.

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

/// Plan nodes beyond this are not traced: a span per node, and plans of
/// hundreds of nodes are rare and would drown the trace.
pub const TRACED_OPERATORS: usize = 200;

/// One executed plan node, for an execution span (ADR 0071).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperatorSpan {
    /// `ExecutionPlan::name`.
    pub name: String,
    /// The node's one-line description (truncated).
    pub detail: String,
    /// Index of the parent node in the same list; `None` for the root.
    pub parent: Option<usize>,
    /// Earliest partition start and latest partition end, when recorded.
    pub start: Option<SystemTime>,
    pub end: Option<SystemTime>,
    pub output_rows: Option<usize>,
    /// CPU time inside the operator, across partitions.
    pub compute: Option<Duration>,
    pub spilled_bytes: Option<usize>,
}

/// The nodes of an executed plan, parents before children, at most
/// `limit` of them.
pub fn operators(
    plan: &std::sync::Arc<dyn datafusion::physical_plan::ExecutionPlan>,
    limit: usize,
) -> Vec<OperatorSpan> {
    use datafusion::physical_plan::metrics::MetricValue;
    let mut out = Vec::new();
    let mut todo = vec![(plan.clone(), None)];
    while let Some((node, parent)) = todo.pop() {
        if out.len() >= limit {
            break;
        }
        let metrics = node.metrics().map(|m| m.aggregate_by_name());
        let stamp = |end: bool| {
            metrics.as_ref().and_then(|m| {
                m.iter().find_map(|metric| match metric.value() {
                    MetricValue::StartTimestamp(t) if !end => t.value(),
                    MetricValue::EndTimestamp(t) if end => t.value(),
                    _ => None,
                })
            })
        };
        let mut detail = datafusion::physical_plan::displayable(node.as_ref())
            .one_line()
            .to_string();
        if detail.len() > 256 {
            let cut = (0..=256)
                .rev()
                .find(|i| detail.is_char_boundary(*i))
                .unwrap_or(0);
            detail.truncate(cut);
        }
        let index = out.len();
        out.push(OperatorSpan {
            name: node.name().to_owned(),
            detail: detail.trim_end().to_owned(),
            parent,
            start: stamp(false).map(SystemTime::from),
            end: stamp(true).map(SystemTime::from),
            output_rows: metrics.as_ref().and_then(|m| m.output_rows()),
            compute: metrics
                .as_ref()
                .and_then(|m| m.elapsed_compute())
                .map(|ns| Duration::from_nanos(ns as u64)),
            spilled_bytes: metrics.as_ref().and_then(|m| m.spilled_bytes()),
        });
        // Reversed, so the first child is visited first.
        for child in node.children().into_iter().rev() {
            todo.push((child.clone(), Some(index)));
        }
    }
    out
}

//! Query governance: registry, admission control and time limits.
//!
//! The memory ceiling (core's CeilingPool) protects the node from one big
//! query; admission protects it from *many* queries. Together they make
//! overload a per-query failure instead of a process crash.
//!
//! Flow, all inside [`Admission::execute`]:
//!
//! ```text
//! register (Queued) ─► acquire slot ─► Running ─► run future ─► Finished
//!        │                 │                          │
//!        │                 └─ queue full ─► Failed    └─ error/timeout ─► Failed
//!        ├─ caller dropped the future at any point ──────────────────────► Cancelled
//!        └─ [`QueryRegistry::cancel`] (queued or running) ────────────────► Cancelled
//! ```

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::fmt;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::sync::{Notify, oneshot};

pub type QueryId = u64;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum QueryState {
    Queued,
    Running,
    Finished,
    Failed,
    Cancelled,
}

impl QueryState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Running => "running",
            Self::Finished => "finished",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }

    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Finished | Self::Failed | Self::Cancelled)
    }
}

impl fmt::Display for QueryState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// One row of `system.runtime.queries`. Timestamps are Unix epoch millis.
/// Serialized for other coordinators' listings (ADR 0102).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct QueryInfo {
    pub id: QueryId,
    pub user: String,
    pub resource_group: String,
    pub sql: String,
    pub state: QueryState,
    pub created_at_ms: u64,
    pub started_at_ms: Option<u64>,
    pub finished_at_ms: Option<u64>,
    pub error: Option<String>,
}

#[derive(Debug, Clone)]
pub struct QueryRequest {
    pub user: String,
    pub resource_group: String,
    pub sql: String,
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or_default()
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    // A panic while holding the lock leaves counters consistent enough to keep
    // serving; poisoning must not take the node down.
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Terminal-state counters, exported as `plimsoll_queries_total{state=...}`.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct QueryTotals {
    pub finished: u64,
    pub failed: u64,
    pub cancelled: u64,
}

/// In-memory registry of recent and active queries.
///
/// ponytail: keeps at most `retain_terminal` finished/failed/cancelled entries
/// (oldest evicted, O(n) scan on eviction). Upgrade path when history matters:
/// ring buffer + export to an audit table.
pub struct QueryRegistry {
    queries: Mutex<BTreeMap<QueryId, QueryInfo>>,
    next_id: AtomicU64,
    retain_terminal: usize,
    finished: AtomicU64,
    failed: AtomicU64,
    cancelled: AtomicU64,
    /// Cancel signals of queries not yet terminal (ADR 0034).
    cancels: Mutex<HashMap<QueryId, Arc<CancelSignal>>>,
}

/// Wakes the governed query, and remembers the request: a run that fails
/// *because* of the cancel (its reader is gone) may complete before the
/// wake-up is seen, and must still be recorded as cancelled.
#[derive(Default)]
struct CancelSignal {
    notify: Notify,
    requested: std::sync::atomic::AtomicBool,
}

/// Result of [`QueryRegistry::cancel`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CancelOutcome {
    /// Signalled; the query stops at its next await point.
    Cancelling,
    /// Already finished, failed or cancelled.
    NotRunning(QueryState),
    NotFound,
}

impl QueryRegistry {
    pub fn new(retain_terminal: usize) -> Self {
        Self {
            queries: Mutex::new(BTreeMap::new()),
            next_id: AtomicU64::new(1),
            retain_terminal,
            finished: AtomicU64::new(0),
            failed: AtomicU64::new(0),
            cancelled: AtomicU64::new(0),
            cancels: Mutex::new(HashMap::new()),
        }
    }

    /// The id the first registered query gets (the default is 1).
    pub fn starting_at(self, first: QueryId) -> Self {
        self.next_id.store(first, Ordering::Relaxed);
        self
    }

    /// Asks a queued or running query to stop. Who may cancel what is the
    /// caller's decision (the server allows owners and operators).
    pub fn cancel(&self, id: QueryId) -> CancelOutcome {
        let Some(info) = self.get(id) else {
            return CancelOutcome::NotFound;
        };
        if info.state.is_terminal() {
            return CancelOutcome::NotRunning(info.state);
        }
        match lock(&self.cancels).get(&id) {
            Some(signal) => {
                signal.requested.store(true, Ordering::SeqCst);
                // Stored as a permit if the query is not awaiting it yet.
                signal.notify.notify_one();
                CancelOutcome::Cancelling
            }
            // Registered but terminal by now (raced with `finish`).
            None => {
                CancelOutcome::NotRunning(self.get(id).map_or(QueryState::Finished, |q| q.state))
            }
        }
    }

    fn cancel_signal(&self, id: QueryId) -> Arc<CancelSignal> {
        Arc::clone(lock(&self.cancels).entry(id).or_default())
    }

    pub fn register(&self, req: &QueryRequest) -> QueryId {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let info = QueryInfo {
            id,
            user: req.user.clone(),
            resource_group: req.resource_group.clone(),
            sql: req.sql.clone(),
            state: QueryState::Queued,
            created_at_ms: now_ms(),
            started_at_ms: None,
            finished_at_ms: None,
            error: None,
        };
        lock(&self.queries).insert(id, info);
        id
    }

    pub fn mark_running(&self, id: QueryId) {
        if let Some(q) = lock(&self.queries).get_mut(&id) {
            q.state = QueryState::Running;
            q.started_at_ms = Some(now_ms());
        }
    }

    /// Moves a query to a terminal state. Idempotent: the first terminal
    /// transition wins, later ones are ignored.
    pub fn finish(&self, id: QueryId, state: QueryState, error: Option<String>) {
        debug_assert!(state.is_terminal());
        let mut queries = lock(&self.queries);
        let Some(q) = queries.get_mut(&id) else {
            return;
        };
        if q.state.is_terminal() {
            return;
        }
        q.state = state;
        q.finished_at_ms = Some(now_ms());
        q.error = error;
        lock(&self.cancels).remove(&id);
        let counter = match state {
            QueryState::Finished => &self.finished,
            QueryState::Cancelled => &self.cancelled,
            _ => &self.failed,
        };
        counter.fetch_add(1, Ordering::Relaxed);

        let terminal: Vec<QueryId> = queries
            .values()
            .filter(|q| q.state.is_terminal())
            .map(|q| q.id)
            .collect();
        let excess = terminal.len().saturating_sub(self.retain_terminal);
        for old in terminal.into_iter().take(excess) {
            queries.remove(&old);
        }
    }

    pub fn get(&self, id: QueryId) -> Option<QueryInfo> {
        lock(&self.queries).get(&id).cloned()
    }

    /// Point-in-time copy, ordered by query id. Backs `system.runtime.queries`.
    pub fn snapshot(&self) -> Vec<QueryInfo> {
        lock(&self.queries).values().cloned().collect()
    }

    pub fn totals(&self) -> QueryTotals {
        QueryTotals {
            finished: self.finished.load(Ordering::Relaxed),
            failed: self.failed.load(Ordering::Relaxed),
            cancelled: self.cancelled.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    pub max_running: usize,
    pub max_queued: usize,
}

/// The resource group every deployment has (ADR 0023).
pub const DEFAULT_GROUP: &str = "default";

/// Admission limits. A query runs only when the global, its resource group's
/// and its user's running counts are all below their limits.
///
/// Groups are a closed set: [`DEFAULT_GROUP`] with `per_group` limits, plus
/// the configured `groups` (ADR 0023). Callers check [`Self::has_group`]
/// before admitting; an unknown name never creates a group.
/// ponytail: every user shares `per_user`; per-user overrides when needed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdmissionConfig {
    pub global: Limits,
    /// Limits of [`DEFAULT_GROUP`] (unless `groups` overrides it).
    pub per_group: Limits,
    pub per_user: Limits,
    /// Configured resource groups and their limits.
    pub groups: HashMap<String, Limits>,
}

impl AdmissionConfig {
    pub fn has_group(&self, group: &str) -> bool {
        group == DEFAULT_GROUP || self.groups.contains_key(group)
    }

    fn group_limits(&self, group: &str) -> Limits {
        self.groups.get(group).copied().unwrap_or(self.per_group)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Scope {
    Global,
    Group(String),
    User(String),
}

impl fmt::Display for Scope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Global => f.write_str("global"),
            Self::Group(g) => write!(f, "resource group '{g}'"),
            Self::User(u) => write!(f, "user '{u}'"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AdmissionError {
    #[error("{scope} queue is full ({limit} queued)")]
    QueueFull { scope: Scope, limit: usize },
    #[error("admission controller shut down")]
    Closed,
}

/// Outcome of a governed execution.
#[derive(Debug, thiserror::Error)]
pub enum GovernedError<E> {
    #[error("rejected: {0}")]
    Rejected(AdmissionError),
    #[error("query exceeded time limit of {}ms", .0.as_millis())]
    TimedOut(Duration),
    #[error("query cancelled by request")]
    Cancelled,
    #[error("{0}")]
    Failed(E),
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct Counts {
    running: usize,
    queued: usize,
}

struct Waiter {
    id: u64,
    user: String,
    group: String,
    tx: oneshot::Sender<AdmissionSlot>,
}

#[derive(Default)]
struct State {
    global: Counts,
    groups: HashMap<String, Counts>,
    users: HashMap<String, Counts>,
    queue: VecDeque<Waiter>,
    next_waiter: u64,
}

impl State {
    fn group(&self, g: &str) -> Counts {
        self.groups.get(g).copied().unwrap_or_default()
    }

    fn user(&self, u: &str) -> Counts {
        self.users.get(u).copied().unwrap_or_default()
    }

    fn can_run(&self, cfg: &AdmissionConfig, user: &str, group: &str) -> bool {
        self.global.running < cfg.global.max_running
            && self.group(group).running < cfg.group_limits(group).max_running
            && self.user(user).running < cfg.per_user.max_running
    }

    fn queue_check(
        &self,
        cfg: &AdmissionConfig,
        user: &str,
        group: &str,
    ) -> Result<(), AdmissionError> {
        let full = |count: usize, limit: usize, scope: Scope| {
            if count >= limit {
                Err(AdmissionError::QueueFull { scope, limit })
            } else {
                Ok(())
            }
        };
        full(self.global.queued, cfg.global.max_queued, Scope::Global)?;
        full(
            self.group(group).queued,
            cfg.group_limits(group).max_queued,
            Scope::Group(group.to_owned()),
        )?;
        full(
            self.user(user).queued,
            cfg.per_user.max_queued,
            Scope::User(user.to_owned()),
        )
    }

    /// Applies `f` to the global, group and user counters, dropping map
    /// entries that return to zero so idle users don't leak memory.
    fn update(&mut self, user: &str, group: &str, f: impl Fn(&mut Counts)) {
        f(&mut self.global);
        for (map, key) in [(&mut self.groups, group), (&mut self.users, user)] {
            let c = map.entry(key.to_owned()).or_default();
            f(c);
            if *c == Counts::default() {
                map.remove(key);
            }
        }
    }
}

struct Inner {
    cfg: AdmissionConfig,
    state: Mutex<State>,
}

/// Admission controller + registry. Cheap to clone (shared state).
#[derive(Clone)]
pub struct Admission {
    inner: Arc<Inner>,
    registry: Arc<QueryRegistry>,
}

/// A running slot. Dropping it frees the slot and promotes queued queries.
#[must_use = "dropping the slot immediately releases it"]
pub struct AdmissionSlot {
    inner: Arc<Inner>,
    user: String,
    group: String,
}

impl Drop for AdmissionSlot {
    fn drop(&mut self) {
        let promoted = {
            let mut st = lock(&self.inner.state);
            st.update(&self.user, &self.group, |c| c.running -= 1);
            promote(&self.inner, &mut st)
        };
        // Send outside the lock: a failed send drops the slot, which re-enters
        // this Drop and must be able to take the lock.
        for (tx, slot) in promoted {
            let _ = tx.send(slot);
        }
    }
}

/// Moves every queued waiter that can now run into the running set, FIFO.
fn promote(
    inner: &Arc<Inner>,
    st: &mut State,
) -> Vec<(oneshot::Sender<AdmissionSlot>, AdmissionSlot)> {
    let mut out = Vec::new();
    while let Some(pos) = st
        .queue
        .iter()
        .position(|w| st.can_run(&inner.cfg, &w.user, &w.group))
    {
        let Some(w) = st.queue.remove(pos) else { break };
        st.update(&w.user, &w.group, |c| {
            c.queued -= 1;
            c.running += 1;
        });
        let slot = AdmissionSlot {
            inner: Arc::clone(inner),
            user: w.user,
            group: w.group,
        };
        out.push((w.tx, slot));
    }
    out
}

/// Removes a still-queued waiter if the acquiring future is dropped
/// (timeout, client disconnect).
struct QueuedGuard<'a> {
    inner: &'a Inner,
    id: u64,
}

impl Drop for QueuedGuard<'_> {
    fn drop(&mut self) {
        let mut st = lock(&self.inner.state);
        if let Some(pos) = st.queue.iter().position(|w| w.id == self.id) {
            if let Some(w) = st.queue.remove(pos) {
                st.update(&w.user, &w.group, |c| c.queued -= 1);
            }
        }
        // Not found: already promoted; the slot sits in the channel and is
        // released when the receiver drops.
    }
}

/// Marks the query Cancelled if the governed future is dropped mid-flight.
struct CancelOnDrop<'a> {
    registry: &'a QueryRegistry,
    id: QueryId,
}

impl Drop for CancelOnDrop<'_> {
    fn drop(&mut self) {
        // No-op when a terminal state was already recorded.
        self.registry.finish(self.id, QueryState::Cancelled, None);
    }
}

impl Admission {
    pub fn new(cfg: AdmissionConfig, registry: Arc<QueryRegistry>) -> Self {
        Self {
            inner: Arc::new(Inner {
                cfg,
                state: Mutex::new(State::default()),
            }),
            registry,
        }
    }

    pub fn registry(&self) -> &Arc<QueryRegistry> {
        &self.registry
    }

    pub fn config(&self) -> &AdmissionConfig {
        &self.inner.cfg
    }

    /// Global `(running, queued)`, exported as gauges.
    pub fn load(&self) -> (usize, usize) {
        let st = lock(&self.inner.state);
        (st.global.running, st.global.queued)
    }

    /// Waits for a running slot, or fails fast if the relevant queue is full.
    pub async fn acquire(&self, user: &str, group: &str) -> Result<AdmissionSlot, AdmissionError> {
        let (id, rx) = {
            let mut st = lock(&self.inner.state);
            if st.can_run(&self.inner.cfg, user, group) {
                st.update(user, group, |c| c.running += 1);
                return Ok(AdmissionSlot {
                    inner: Arc::clone(&self.inner),
                    user: user.to_owned(),
                    group: group.to_owned(),
                });
            }
            st.queue_check(&self.inner.cfg, user, group)?;
            st.update(user, group, |c| c.queued += 1);
            let (tx, rx) = oneshot::channel();
            let id = st.next_waiter;
            st.next_waiter += 1;
            st.queue.push_back(Waiter {
                id,
                user: user.to_owned(),
                group: group.to_owned(),
                tx,
            });
            (id, rx)
        };
        let _guard = QueuedGuard {
            inner: &self.inner,
            id,
        };
        rx.await.map_err(|_| AdmissionError::Closed)
    }

    /// Registers, admits, runs and records a query under one deadline.
    ///
    /// `timeout` covers queue wait + execution. When it fires, `run`'s future
    /// is dropped, which is how execution is cancelled (DataFusion streams stop
    /// on drop). The slot is held for the whole run and freed on return.
    pub async fn execute<T, E, F, Fut>(
        &self,
        req: QueryRequest,
        timeout: Duration,
        run: F,
    ) -> Result<T, GovernedError<E>>
    where
        F: FnOnce(QueryId) -> Fut,
        Fut: Future<Output = Result<T, E>>,
        E: fmt::Display,
    {
        let id = self.registry.register(&req);
        let cancelled = self.registry.cancel_signal(id);
        let _cancel = CancelOnDrop {
            registry: &self.registry,
            id,
        };
        let governed = async {
            let _slot = self
                .acquire(&req.user, &req.resource_group)
                .await
                .map_err(GovernedError::Rejected)?;
            self.registry.mark_running(id);
            run(id).await.map_err(GovernedError::Failed)
        };
        // A cancel drops `governed` like the timeout does: queue wait or
        // execution stops, and the slot and memory are released.
        // Biased, cancel first; and a run that failed after a cancel was
        // requested (a disconnected stream's reader) is recorded as
        // cancelled even if it finished before the wake-up was seen.
        let result = tokio::select! {
            biased;
            () = cancelled.notify.notified() => Err(GovernedError::Cancelled),
            r = tokio::time::timeout(timeout, governed) => {
                r.unwrap_or(Err(GovernedError::TimedOut(timeout)))
            }
        };
        let result = match result {
            Err(_) if cancelled.requested.load(Ordering::SeqCst) => Err(GovernedError::Cancelled),
            r => r,
        };
        match &result {
            Ok(_) => self.registry.finish(id, QueryState::Finished, None),
            Err(GovernedError::Cancelled) => self.registry.finish(
                id,
                QueryState::Cancelled,
                Some("cancelled by request".to_owned()),
            ),
            Err(e) => self
                .registry
                .finish(id, QueryState::Failed, Some(e.to_string())),
        }
        result
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn lim(max_running: usize, max_queued: usize) -> Limits {
        Limits {
            max_running,
            max_queued,
        }
    }

    fn admission(global: Limits, per_group: Limits, per_user: Limits) -> Admission {
        let cfg = AdmissionConfig {
            global,
            per_group,
            per_user,
            groups: HashMap::new(),
        };
        Admission::new(cfg, Arc::new(QueryRegistry::new(100)))
    }

    fn req(user: &str) -> QueryRequest {
        QueryRequest {
            user: user.into(),
            resource_group: "default".into(),
            sql: "select 1".into(),
        }
    }

    #[tokio::test]
    async fn configured_groups_have_their_own_limits() {
        let cfg = AdmissionConfig {
            global: lim(10, 10),
            per_group: lim(10, 10),
            per_user: lim(10, 10),
            groups: HashMap::from([("batch".to_owned(), lim(1, 0))]),
        };
        let adm = Admission::new(cfg, Arc::new(QueryRegistry::new(100)));
        assert!(adm.config().has_group("batch") && adm.config().has_group(DEFAULT_GROUP));
        assert!(!adm.config().has_group("made-up"));
        let _batch = adm.acquire("a", "batch").await.unwrap();
        // batch is full (1 running, no queue); default is unaffected.
        let err = adm.acquire("b", "batch").await.err().unwrap();
        assert_eq!(
            err,
            AdmissionError::QueueFull {
                scope: Scope::Group("batch".into()),
                limit: 0
            }
        );
        let _interactive = adm.acquire("b", DEFAULT_GROUP).await.unwrap();
        assert_eq!(adm.load(), (2, 0));
    }

    #[tokio::test]
    async fn slot_drop_releases_and_promotes_queued() {
        let adm = admission(lim(1, 10), lim(10, 10), lim(10, 10));
        let first = adm.acquire("a", "g").await.unwrap();
        let waiter = tokio::spawn({
            let adm = adm.clone();
            async move { adm.acquire("b", "g").await.map(|_slot| ()) }
        });
        tokio::task::yield_now().await;
        assert_eq!(adm.load(), (1, 1));

        drop(first);
        waiter.await.unwrap().unwrap();
        assert_eq!(adm.load(), (0, 0));
    }

    #[tokio::test]
    async fn queue_overflow_rejects_with_typed_error() {
        let adm = admission(lim(1, 1), lim(10, 10), lim(10, 10));
        let _running = adm.acquire("a", "g").await.unwrap();
        let queued = tokio::spawn({
            let adm = adm.clone();
            async move { adm.acquire("b", "g").await.map(|_slot| ()) }
        });
        tokio::task::yield_now().await;

        let err = adm.acquire("c", "g").await.err().unwrap();
        assert_eq!(
            err,
            AdmissionError::QueueFull {
                scope: Scope::Global,
                limit: 1
            }
        );
        queued.abort();
    }

    #[tokio::test]
    async fn per_user_limit_queues_only_that_user() {
        let adm = admission(lim(10, 10), lim(10, 10), lim(1, 0));
        let _alice = adm.acquire("alice", "g").await.unwrap();
        // Alice is at her running limit and has no queue: rejected.
        assert_eq!(
            adm.acquire("alice", "g").await.err().unwrap(),
            AdmissionError::QueueFull {
                scope: Scope::User("alice".into()),
                limit: 0
            }
        );
        // Bob is unaffected.
        let _bob = adm.acquire("bob", "g").await.unwrap();
        assert_eq!(adm.load(), (2, 0));
    }

    #[tokio::test]
    async fn dropped_waiter_leaves_queue() {
        let adm = admission(lim(1, 5), lim(10, 10), lim(10, 10));
        let _running = adm.acquire("a", "g").await.unwrap();
        let res = tokio::time::timeout(Duration::from_millis(10), adm.acquire("b", "g")).await;
        assert!(res.is_err());
        assert_eq!(adm.load(), (1, 0));
    }

    #[tokio::test(start_paused = true)]
    async fn timeout_marks_query_failed_and_frees_slot() {
        let adm = admission(lim(1, 1), lim(1, 1), lim(1, 1));
        let res: Result<(), GovernedError<String>> = adm
            .execute(req("a"), Duration::from_millis(50), |_| async {
                tokio::time::sleep(Duration::from_secs(60)).await;
                Ok(())
            })
            .await;
        assert!(matches!(res, Err(GovernedError::TimedOut(_))));

        let q = &adm.registry().snapshot()[0];
        assert_eq!(q.state, QueryState::Failed);
        assert!(q.error.as_deref().unwrap().contains("time limit"));
        assert_eq!(adm.load(), (0, 0));
        assert_eq!(adm.registry().totals().failed, 1);
    }

    #[tokio::test]
    async fn execute_records_success_and_failure() {
        let adm = admission(lim(4, 4), lim(4, 4), lim(4, 4));
        let ok: Result<u32, GovernedError<String>> = adm
            .execute(req("a"), Duration::from_secs(5), |_| async { Ok(7) })
            .await;
        assert_eq!(ok.unwrap(), 7);
        let err: Result<u32, GovernedError<String>> = adm
            .execute(req("a"), Duration::from_secs(5), |_| async {
                Err("boom".to_owned())
            })
            .await;
        assert!(matches!(err, Err(GovernedError::Failed(_))));

        let snap = adm.registry().snapshot();
        assert_eq!(snap[0].state, QueryState::Finished);
        assert!(snap[0].started_at_ms.is_some() && snap[0].finished_at_ms.is_some());
        assert_eq!(snap[1].state, QueryState::Failed);
        assert_eq!(snap[1].error.as_deref(), Some("boom"));
    }

    #[tokio::test]
    async fn dropped_execution_is_cancelled() {
        let adm = admission(lim(1, 1), lim(1, 1), lim(1, 1));
        let fut = adm.execute(req("a"), Duration::from_secs(60), |_| async {
            std::future::pending::<Result<(), String>>().await
        });
        let _ = tokio::time::timeout(Duration::from_millis(10), fut).await;
        assert_eq!(adm.registry().snapshot()[0].state, QueryState::Cancelled);
        assert_eq!(adm.load(), (0, 0));
    }

    #[test]
    fn ids_start_where_the_node_says() {
        let reg = QueryRegistry::new(10).starting_at(7 << 32 | 1);
        assert_eq!(reg.register(&req("a")), 7 << 32 | 1);
        assert_eq!(reg.register(&req("b")), 7 << 32 | 2);
        assert_eq!(QueryRegistry::new(10).register(&req("a")), 1);
    }

    #[test]
    fn registry_evicts_oldest_terminal_entries() {
        let reg = QueryRegistry::new(1);
        let a = reg.register(&req("a"));
        let b = reg.register(&req("b"));
        let c = reg.register(&req("c"));
        reg.finish(a, QueryState::Finished, None);
        reg.finish(b, QueryState::Failed, Some("x".into()));
        let ids: Vec<_> = reg.snapshot().iter().map(|q| q.id).collect();
        assert_eq!(ids, vec![b, c]);
        assert_eq!(
            reg.totals(),
            QueryTotals {
                finished: 1,
                failed: 1,
                cancelled: 0
            }
        );
    }

    #[tokio::test]
    async fn cancel_stops_running_and_queued_queries_and_frees_the_slot() {
        let adm = admission(lim(1, 10), lim(10, 10), lim(10, 10));
        let adm = Arc::new(adm);
        let slow = |adm: Arc<Admission>| async move {
            adm.execute(req("a"), Duration::from_secs(60), |_| async {
                tokio::time::sleep(Duration::from_secs(60)).await;
                Ok::<_, String>(())
            })
            .await
        };
        let running = tokio::spawn(slow(adm.clone()));
        let queued = tokio::spawn(slow(adm.clone()));
        tokio::task::yield_now().await;
        while adm.load() != (1, 1) {
            tokio::task::yield_now().await;
        }
        let reg = adm.registry().clone();
        let (r, q) = (1, 2);
        assert_eq!(reg.get(r).unwrap().state, QueryState::Running);
        assert_eq!(reg.get(q).unwrap().state, QueryState::Queued);

        // Queued first: it leaves the queue without ever running.
        assert_eq!(reg.cancel(q), CancelOutcome::Cancelling);
        assert!(matches!(
            queued.await.unwrap(),
            Err(GovernedError::Cancelled)
        ));
        assert_eq!(reg.get(q).unwrap().state, QueryState::Cancelled);
        assert_eq!(reg.get(q).unwrap().started_at_ms, None);
        assert_eq!(adm.load(), (1, 0));

        assert_eq!(reg.cancel(r), CancelOutcome::Cancelling);
        assert!(matches!(
            running.await.unwrap(),
            Err(GovernedError::Cancelled)
        ));
        let info = reg.get(r).unwrap();
        assert_eq!(info.state, QueryState::Cancelled);
        assert_eq!(info.error.as_deref(), Some("cancelled by request"));
        assert_eq!(adm.load(), (0, 0), "slot released");
        assert_eq!(reg.totals().cancelled, 2);

        assert_eq!(
            reg.cancel(r),
            CancelOutcome::NotRunning(QueryState::Cancelled)
        );
        assert_eq!(reg.cancel(999), CancelOutcome::NotFound);
        // A cancel signal is only kept while a query can still be cancelled.
        assert!(lock(&reg.cancels).is_empty());
    }

    /// A run that fails *because* it was cancelled (a streamed query whose
    /// reader went away) can complete in the same poll as the cancel: it is
    /// still recorded as cancelled, not failed.
    #[tokio::test]
    async fn a_run_failing_after_a_cancel_request_counts_as_cancelled() {
        let adm = admission(lim(1, 1), lim(1, 1), lim(1, 1));
        let reg = adm.registry().clone();
        let out = adm
            .execute(req("a"), Duration::from_secs(5), |id| {
                let reg = reg.clone();
                async move {
                    assert_eq!(reg.cancel(id), CancelOutcome::Cancelling);
                    Err::<(), _>("receiver dropped".to_owned())
                }
            })
            .await;
        assert!(matches!(out, Err(GovernedError::Cancelled)), "{out:?}");
        assert_eq!(reg.get(1).unwrap().state, QueryState::Cancelled);
    }
}

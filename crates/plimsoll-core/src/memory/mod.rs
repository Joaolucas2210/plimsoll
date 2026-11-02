//! Hard memory ceiling (`CeilingPool`) — the Plimsoll line.
//!
//! One `CeilingPool` per process. Every byte of query memory (working
//! operators *and* output/exchange buffers) is reserved here before it is
//! allocated. When a reservation would cross the line, the *query* gets a
//! typed error naming the top consumers; the process keeps running.
//!
//! Budget layout (`limit` bytes total):
//!
//! ```text
//! |<- cache_reserve ->|<------ limit - cache - output ------>|<- output_reserve ->|
//! |  DataFusion caches|  working operators (spill + no-spill) |  output/exchange   |
//! |                   |<---- output/exchange may use all of limit - cache -------->|
//! ```
//!
//! The cache reserve is never handed to queries: DataFusion's caches (Parquet
//! footers, file statistics, Iceberg delete files) are sized to it, so caches
//! sit under the line by construction (ADR 0018).
//!
//! Working operators can never eat the output slice, so a query that has
//! produced results can always ship them out.
//!
//! Resource groups may hold a share of the line (ADR 0036): a consumer
//! registered in a capped group fails once the group's bytes would cross
//! its share, even with room under the ceiling, so one group's heavy queries
//! cannot take memory the others were promised.
//! See `docs/adr/0003-memory-ceiling.md`.

mod accounted;
mod df_adapter;

pub use accounted::AccountedExec;
pub use df_adapter::{GroupPool, runtime_env, session_context_with_ceiling, spilled_bytes};

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard};

/// How many consumers an OOM error names.
pub const TOP_CONSUMERS: usize = 5;

/// Fraction (in percent) of the working budget above which spillable
/// consumers are told to spill.
// ponytail: fixed 80% threshold; make it configurable when a workload needs it.
const SPILL_HINT_PERCENT: usize = 80;

/// Class of a memory consumer; decides which slice of the budget it draws from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConsumerKind {
    /// Working operator that can release memory by spilling to disk.
    Spillable,
    /// Working operator that cannot spill (hash build side, small state).
    NonSpillable,
    /// Output / exchange buffer; may use the reserved output slice.
    Output,
}

/// Snapshot of one consumer's usage.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerUsage {
    pub name: String,
    pub kind: ConsumerKind,
    pub bytes: usize,
}

impl fmt::Display for ConsumerUsage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}={}B ({:?})", self.name, self.bytes, self.kind)
    }
}

/// Errors raised by the ceiling.
#[derive(Debug, Clone, thiserror::Error)]
pub enum CeilingError {
    /// The reservation would cross the Plimsoll line.
    #[error(
        "memory ceiling: '{consumer}' ({kind:?}) requested {requested}B, {available}B available \
         (limit {limit}B, output reserve {output_reserve}B, cache reserve {cache_reserve}B); \
         top consumers: [{}]",
        join(top)
    )]
    Exhausted {
        consumer: String,
        kind: ConsumerKind,
        requested: usize,
        available: usize,
        limit: usize,
        output_reserve: usize,
        cache_reserve: usize,
        top: Vec<ConsumerUsage>,
    },
    /// The reservation would cross its resource group's share (ADR 0036).
    #[error(
        "memory ceiling: resource group '{group}' share exhausted: '{consumer}' requested \
         {requested}B, {available}B of the group's {share}B available; top consumers: [{}]",
        join(top)
    )]
    GroupExhausted {
        group: String,
        consumer: String,
        requested: usize,
        available: usize,
        share: usize,
        top: Vec<ConsumerUsage>,
    },
    /// Output + cache reserves larger than `limit`.
    #[error(
        "output reserve {output_reserve}B + cache reserve {cache_reserve}B exceed limit {limit}B"
    )]
    InvalidConfig {
        limit: usize,
        output_reserve: usize,
        cache_reserve: usize,
    },
    /// Grow/shrink on a consumer id that was never registered.
    #[error("unknown memory consumer id {0}")]
    UnknownConsumer(usize),
}

fn join(top: &[ConsumerUsage]) -> String {
    top.iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(", ")
}

#[derive(Debug)]
struct Entry {
    name: String,
    kind: ConsumerKind,
    bytes: usize,
    /// Resource group, when the consumer was registered in one.
    group: Option<Arc<str>>,
}

#[derive(Debug, Default)]
struct State {
    /// Bytes held by Spillable + NonSpillable consumers.
    work: usize,
    /// Bytes held by Output consumers.
    output: usize,
    consumers: HashMap<usize, Entry>,
    /// Bytes held per resource group.
    groups: HashMap<Arc<str>, usize>,
}

impl State {
    fn bucket(&mut self, kind: ConsumerKind) -> &mut usize {
        match kind {
            ConsumerKind::Output => &mut self.output,
            _ => &mut self.work,
        }
    }
}

/// Process-wide hard memory ceiling.
#[derive(Debug)]
pub struct CeilingPool {
    limit: usize,
    output_reserve: usize,
    cache_reserve: usize,
    /// Byte caps per resource group (ADR 0036); groups not listed share the
    /// ceiling freely.
    group_shares: HashMap<String, usize>,
    // ponytail: one Mutex for all accounting; contention is per reservation
    // call (operators reserve in chunks), not per row. Shard if it shows up in profiles.
    state: Mutex<State>,
}

impl CeilingPool {
    /// `limit` total bytes, of which `output_reserve` are only usable by
    /// [`ConsumerKind::Output`] consumers.
    pub fn new(limit: usize, output_reserve: usize) -> Result<Self, CeilingError> {
        Self {
            limit,
            output_reserve,
            cache_reserve: 0,
            group_shares: HashMap::new(),
            state: Mutex::default(),
        }
        .with_cache_reserve(0)
    }

    /// Sets aside `bytes` of `limit` for DataFusion's caches; queries can
    /// never reserve them. See [`runtime_env`].
    pub fn with_cache_reserve(mut self, bytes: usize) -> Result<Self, CeilingError> {
        if self.output_reserve.saturating_add(bytes) > self.limit {
            return Err(CeilingError::InvalidConfig {
                limit: self.limit,
                output_reserve: self.output_reserve,
                cache_reserve: bytes,
            });
        }
        self.cache_reserve = bytes;
        Ok(self)
    }

    /// Caps consumers registered in `group` at `bytes` in total.
    pub fn with_group_share(mut self, group: impl Into<String>, bytes: usize) -> Self {
        self.group_shares.insert(group.into(), bytes);
        self
    }

    pub fn group_share(&self, group: &str) -> Option<usize> {
        self.group_shares.get(group).copied()
    }

    /// `(group, share, used)` for every capped group, by name.
    pub fn group_usage(&self) -> Vec<(String, usize, usize)> {
        let s = self.lock();
        let mut v: Vec<_> = self
            .group_shares
            .iter()
            .map(|(g, share)| {
                let used = s.groups.get(g.as_str()).copied().unwrap_or(0);
                (g.clone(), *share, used)
            })
            .collect();
        v.sort();
        v
    }

    /// Bytes currently held by consumers of `group`.
    pub fn group_used(&self, group: &str) -> usize {
        self.lock().groups.get(group).copied().unwrap_or(0)
    }

    pub fn limit(&self) -> usize {
        self.limit
    }

    pub fn output_reserve(&self) -> usize {
        self.output_reserve
    }

    pub fn cache_reserve(&self) -> usize {
        self.cache_reserve
    }

    /// Total bytes currently reserved (working + output).
    pub fn used(&self) -> usize {
        let s = self.lock();
        s.work + s.output
    }

    /// Bytes currently reserved by output/exchange consumers.
    pub fn output_used(&self) -> usize {
        self.lock().output
    }

    /// True when working memory is above the spill threshold.
    pub fn spill_hint(&self) -> bool {
        let budget = self.limit - self.output_reserve - self.cache_reserve;
        self.lock().work * 100 >= budget * SPILL_HINT_PERCENT
    }

    /// Largest consumers first, zero-byte consumers omitted.
    pub fn top_consumers(&self, n: usize) -> Vec<ConsumerUsage> {
        top_of(&self.lock(), n)
    }

    /// Register a native consumer. Dropping the returned reservation frees its bytes.
    pub fn register(self: &Arc<Self>, name: impl Into<String>, kind: ConsumerKind) -> Reservation {
        self.register_in(None, name, kind)
    }

    /// [`Self::register`] counted against `group`'s share, if it has one.
    pub fn register_in(
        self: &Arc<Self>,
        group: Option<&Arc<str>>,
        name: impl Into<String>,
        kind: ConsumerKind,
    ) -> Reservation {
        let id = next_id();
        self.register_id(id, name.into(), kind, group.cloned());
        Reservation {
            pool: Arc::clone(self),
            id,
            kind,
            size: 0,
        }
    }

    /// `register` + `try_grow(bytes)` in one call.
    pub fn reserve(
        self: &Arc<Self>,
        name: impl Into<String>,
        kind: ConsumerKind,
        bytes: usize,
    ) -> Result<Reservation, CeilingError> {
        let mut r = self.register(name, kind);
        r.try_grow(bytes)?;
        Ok(r)
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        // Accounting never panics mid-update, so a poisoned lock still holds consistent state.
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn register_id(&self, id: usize, name: String, kind: ConsumerKind, group: Option<Arc<str>>) {
        // Only capped groups are tracked: the others need no bookkeeping.
        let group = group.filter(|g| self.group_shares.contains_key(g.as_ref()));
        self.lock().consumers.insert(
            id,
            Entry {
                name,
                kind,
                bytes: 0,
                group,
            },
        );
    }

    fn unregister_id(&self, id: usize) {
        let mut s = self.lock();
        if let Some(e) = s.consumers.remove(&id) {
            let b = s.bucket(e.kind);
            *b = b.saturating_sub(e.bytes);
            if let Some(g) = &e.group {
                Self::group_sub(&mut s, g, e.bytes);
            }
        }
    }

    fn group_sub(s: &mut State, group: &Arc<str>, bytes: usize) {
        if let Some(used) = s.groups.get_mut(group) {
            *used = used.saturating_sub(bytes);
            if *used == 0 {
                s.groups.remove(group);
            }
        }
    }

    fn try_grow_id(&self, id: usize, additional: usize) -> Result<(), CeilingError> {
        let mut s = self.lock();
        let entry = s
            .consumers
            .get(&id)
            .ok_or(CeilingError::UnknownConsumer(id))?;
        let kind = entry.kind;
        if let Some(group) = entry.group.clone() {
            let share = self
                .group_shares
                .get(group.as_ref())
                .copied()
                .unwrap_or(usize::MAX);
            let available = share.saturating_sub(s.groups.get(&group).copied().unwrap_or(0));
            if additional > available {
                return Err(CeilingError::GroupExhausted {
                    group: group.to_string(),
                    consumer: entry.name.clone(),
                    requested: additional,
                    available,
                    share,
                    top: top_of(&s, TOP_CONSUMERS),
                });
            }
        }
        // Working consumers see the output slice as always taken.
        let taken = match kind {
            ConsumerKind::Output => s.work + s.output,
            _ => s.work + s.output.max(self.output_reserve),
        };
        let available = (self.limit - self.cache_reserve).saturating_sub(taken);
        if additional > available {
            let name = s
                .consumers
                .get(&id)
                .map(|e| e.name.clone())
                .unwrap_or_default();
            return Err(CeilingError::Exhausted {
                consumer: name,
                kind,
                requested: additional,
                available,
                limit: self.limit,
                output_reserve: self.output_reserve,
                cache_reserve: self.cache_reserve,
                top: top_of(&s, TOP_CONSUMERS),
            });
        }
        self.add_id(&mut s, id, additional);
        Ok(())
    }

    /// Unchecked grow; only used for DataFusion's infallible `grow`.
    fn grow_id(&self, id: usize, additional: usize) {
        let mut s = self.lock();
        self.add_id(&mut s, id, additional);
    }

    fn add_id(&self, s: &mut State, id: usize, additional: usize) {
        if let Some(e) = s.consumers.get_mut(&id) {
            e.bytes += additional;
            let (kind, group) = (e.kind, e.group.clone());
            *s.bucket(kind) += additional;
            if let Some(g) = group {
                *s.groups.entry(g).or_default() += additional;
            }
        }
    }

    fn shrink_id(&self, id: usize, bytes: usize) {
        let mut s = self.lock();
        if let Some(e) = s.consumers.get_mut(&id) {
            let freed = bytes.min(e.bytes);
            e.bytes -= freed;
            let (kind, group) = (e.kind, e.group.clone());
            *s.bucket(kind) -= freed;
            if let Some(g) = group {
                Self::group_sub(&mut s, &g, freed);
            }
        }
    }
}

impl fmt::Display for CeilingPool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ceiling(used: {}B, limit: {}B, output reserve: {}B)",
            self.used(),
            self.limit,
            self.output_reserve
        )
    }
}

fn top_of(s: &State, n: usize) -> Vec<ConsumerUsage> {
    let mut v: Vec<_> = s
        .consumers
        .values()
        .filter(|e| e.bytes > 0)
        .map(|e| ConsumerUsage {
            name: e.name.clone(),
            kind: e.kind,
            bytes: e.bytes,
        })
        .collect();
    v.sort_by_key(|u| std::cmp::Reverse(u.bytes));
    v.truncate(n);
    v
}

/// Ids share DataFusion's consumer id space so native and DataFusion
/// consumers can live in the same map without collisions.
fn next_id() -> usize {
    datafusion::execution::memory_pool::MemoryConsumer::new("").id()
}

/// RAII handle on a slice of the ceiling. Drop frees all of its bytes.
#[derive(Debug)]
pub struct Reservation {
    pool: Arc<CeilingPool>,
    id: usize,
    kind: ConsumerKind,
    size: usize,
}

impl Reservation {
    pub fn size(&self) -> usize {
        self.size
    }

    pub fn kind(&self) -> ConsumerKind {
        self.kind
    }

    /// Grow by `additional` bytes or fail without changing anything.
    pub fn try_grow(&mut self, additional: usize) -> Result<(), CeilingError> {
        self.pool.try_grow_id(self.id, additional)?;
        self.size += additional;
        Ok(())
    }

    /// Release up to `bytes` (clamped to the current size).
    pub fn shrink(&mut self, bytes: usize) {
        let bytes = bytes.min(self.size);
        self.pool.shrink_id(self.id, bytes);
        self.size -= bytes;
    }

    /// Release everything, keeping the registration.
    pub fn free(&mut self) {
        self.shrink(self.size);
    }

    /// True when this consumer is spillable and the pool is under pressure.
    pub fn spill_hint(&self) -> bool {
        self.kind == ConsumerKind::Spillable && self.pool.spill_hint()
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        self.pool.unregister_id(self.id);
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use ConsumerKind::*;

    fn pool(limit: usize, reserve: usize) -> Arc<CeilingPool> {
        Arc::new(CeilingPool::new(limit, reserve).unwrap())
    }

    #[test]
    fn rejects_reserve_above_limit() {
        assert!(matches!(
            CeilingPool::new(10, 11),
            Err(CeilingError::InvalidConfig { .. })
        ));
    }

    #[test]
    fn cache_reserve_is_never_handed_to_queries() {
        assert!(matches!(
            CeilingPool::new(100, 60).unwrap().with_cache_reserve(41),
            Err(CeilingError::InvalidConfig { .. })
        ));
        let p = Arc::new(
            CeilingPool::new(100, 20)
                .unwrap()
                .with_cache_reserve(30)
                .unwrap(),
        );
        // Working memory: 100 - 30 cache - 20 output.
        let mut w = p.register("sort", Spillable);
        assert!(w.try_grow(51).is_err());
        w.try_grow(50).unwrap();
        // Output may use its slice, never the cache's.
        let mut o = p.register("out", Output);
        assert!(o.try_grow(21).is_err());
        o.try_grow(20).unwrap();
        let err = o.try_grow(1).unwrap_err().to_string();
        assert!(err.contains("cache reserve 30B"), "{err}");
    }

    #[test]
    fn never_exceeds_limit_and_oom_names_top_consumers() {
        let p = pool(1000, 0);
        let _a = p.reserve("sort", Spillable, 600).unwrap();
        let _b = p.reserve("hash_join", NonSpillable, 300).unwrap();
        let mut c = p.register("agg", NonSpillable);
        let err = c.try_grow(101).unwrap_err();
        assert_eq!(p.used(), 900);
        assert_eq!(c.size(), 0);
        let CeilingError::Exhausted {
            consumer,
            available,
            top,
            ..
        } = &err
        else {
            panic!("wrong error {err:?}");
        };
        assert_eq!(consumer, "agg");
        assert_eq!(*available, 100);
        let names: Vec<_> = top.iter().map(|u| u.name.as_str()).collect();
        assert_eq!(names, ["sort", "hash_join"]);
        let msg = err.to_string();
        assert!(
            msg.contains("sort=600B") && msg.contains("hash_join=300B"),
            "{msg}"
        );
        c.try_grow(100).unwrap();
        assert_eq!(p.used(), 1000);
    }

    #[test]
    fn top_is_capped() {
        let p = pool(1000, 0);
        let _rs: Vec<_> = (1..=8)
            .map(|i| p.reserve(format!("op{i}"), Spillable, i * 10).unwrap())
            .collect();
        let err = p.reserve("big", Spillable, 1000).unwrap_err();
        let CeilingError::Exhausted { top, .. } = err else {
            panic!()
        };
        assert_eq!(top.len(), TOP_CONSUMERS);
        assert_eq!(top[0].name, "op8");
    }

    #[test]
    fn shrink_and_drop_release() {
        let p = pool(100, 0);
        let mut r = p.reserve("a", Spillable, 80).unwrap();
        r.shrink(30);
        assert_eq!((r.size(), p.used()), (50, 50));
        r.shrink(1000); // clamped
        assert_eq!((r.size(), p.used()), (0, 0));
        r.try_grow(70).unwrap();
        drop(r);
        assert_eq!(p.used(), 0);
        assert!(p.top_consumers(5).is_empty());
    }

    #[test]
    fn output_slice_is_honoured() {
        let p = pool(1000, 200);
        // Working operators stop at limit - reserve.
        let _w = p.reserve("sort", Spillable, 800).unwrap();
        assert!(p.reserve("agg", NonSpillable, 1).is_err());
        // Output can still use its slice...
        let mut out = p.reserve("exchange", Output, 200).unwrap();
        // ...but not beyond the limit.
        assert!(out.try_grow(1).is_err());
        assert_eq!(p.used(), 1000);
        assert_eq!(p.output_used(), 200);
    }

    #[test]
    fn output_may_use_shared_slice_and_workers_then_lose_it() {
        let p = pool(1000, 200);
        let _out = p.reserve("exchange", Output, 500).unwrap();
        // Output spilled past its reserve into the shared slice: 500 left in total.
        assert!(p.reserve("sort", Spillable, 501).is_err());
        let _w = p.reserve("sort", Spillable, 500).unwrap();
        assert_eq!(p.used(), 1000);
    }

    #[test]
    fn spill_hint_only_for_spillable_under_pressure() {
        let p = pool(1000, 0);
        let mut s = p.reserve("sort", Spillable, 100).unwrap();
        let n = p.reserve("join", NonSpillable, 0).unwrap();
        assert!(!s.spill_hint());
        s.try_grow(700).unwrap();
        assert!(s.spill_hint());
        assert!(!n.spill_hint());
        s.shrink(500);
        assert!(!s.spill_hint());
    }

    #[test]
    fn concurrent_hammer_never_crosses_limit() {
        const LIMIT: usize = 10_000;
        let p = pool(LIMIT, 1_000);
        let threads: Vec<_> = (0..16)
            .map(|t| {
                let p = Arc::clone(&p);
                std::thread::spawn(move || {
                    let kind = [Spillable, NonSpillable, Output][t % 3];
                    let mut r = p.register(format!("t{t}"), kind);
                    let mut max_seen = 0;
                    for i in 0..5_000usize {
                        let n = (i * 7 + t * 13) % 900 + 1;
                        if i % 3 == 0 {
                            r.shrink(n);
                        } else {
                            let _ = r.try_grow(n);
                        }
                        max_seen = max_seen.max(p.used());
                    }
                    max_seen
                })
            })
            .collect();
        for h in threads {
            assert!(h.join().unwrap() <= LIMIT);
        }
        assert_eq!(p.used(), 0);
    }

    #[test]
    fn group_shares_cap_their_consumers_only() {
        let p = Arc::new(
            CeilingPool::new(1000, 100)
                .unwrap()
                .with_group_share("batch", 300),
        );
        let batch: Arc<str> = Arc::from("batch");
        let adhoc: Arc<str> = Arc::from("adhoc"); // no share: not capped
        let mut b1 = p.register_in(Some(&batch), "b1", Spillable);
        let mut b2 = p.register_in(Some(&batch), "b2", Output);
        b1.try_grow(200).unwrap();
        b2.try_grow(100).unwrap();
        assert_eq!(p.group_used("batch"), 300);
        assert_eq!(p.group_usage(), [("batch".to_owned(), 300, 300)]);
        // The group is full although the ceiling has room.
        let err = b1.try_grow(1).unwrap_err();
        assert!(
            matches!(&err, CeilingError::GroupExhausted { group, available: 0, share: 300, .. } if group == "batch"),
            "{err}"
        );
        assert!(err.to_string().contains("resource group 'batch'"), "{err}");
        // Other groups and ungrouped consumers still get the rest.
        let mut a = p.register_in(Some(&adhoc), "a", NonSpillable);
        a.try_grow(500).unwrap();
        let mut plain = p.register("plain", NonSpillable);
        // 700 working + the 100 output reserve held back: 200 left.
        assert!(plain.try_grow(201).is_err(), "the ceiling still binds");
        assert_eq!(p.group_used("adhoc"), 0, "uncapped groups are not tracked");
        // Shrink and drop give the share back.
        b1.shrink(150);
        assert_eq!(p.group_used("batch"), 150);
        b1.try_grow(150).unwrap();
        drop(b1);
        drop(b2);
        assert_eq!(p.group_used("batch"), 0);
        drop(a);
        assert_eq!(p.used(), 0);
    }
}

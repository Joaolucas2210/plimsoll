//! Result cache for repeated queries over immutable inputs (ADR 0038).
//!
//! A result is cached only when it is a pure function of its key:
//!
//! - every source in the physical plan is *pinned*: an [`AccountedExec`]
//!   that declared an immutable identity (an Iceberg table at a snapshot),
//!   or a constant source (`SELECT 1`);
//! - no scalar function in the logical plan is volatile or stable (`now()`,
//!   `random()`, `uuid()`);
//!
//! and the key is the SQL text plus every pinned identity. A new commit
//! changes a snapshot id, so the next query misses: no invalidation needed.
//!
//! Entries and the results being collected for them are reserved in the
//! pool under one consumer, capped at the configured size; a reservation
//! that does not fit evicts least recently used entries or skips caching,
//! never fails the query.
//!
//! ponytail: LRU by a tick scan (O(entries) per eviction); a linked LRU if
//! entry counts grow into the thousands.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::logical_expr::{Expr, LogicalPlan, Volatility};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::memory::MemoryStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::StreamExt;

use crate::memory::{AccountedExec, CeilingPool, ConsumerKind, Reservation};

/// Hit/miss counters and current size, for metrics.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct CacheStats {
    pub hits: u64,
    pub misses: u64,
    pub entries: usize,
    pub bytes: usize,
    pub max_bytes: usize,
}

struct Entry {
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
    bytes: usize,
    used: u64,
}

struct State {
    entries: HashMap<String, Entry>,
    tick: u64,
    /// Reserves stored entries *and* results being collected.
    memory: Reservation,
}

pub struct ResultCache {
    max_bytes: usize,
    state: Mutex<State>,
    hits: AtomicU64,
    misses: AtomicU64,
}

impl ResultCache {
    pub fn new(pool: &Arc<CeilingPool>, max_bytes: usize) -> Self {
        Self {
            max_bytes,
            state: Mutex::new(State {
                entries: HashMap::new(),
                tick: 0,
                memory: pool.register("result-cache", ConsumerKind::NonSpillable),
            }),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }

    pub fn stats(&self) -> CacheStats {
        let s = self.lock();
        CacheStats {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            entries: s.entries.len(),
            bytes: s.memory.size(),
            max_bytes: self.max_bytes,
        }
    }

    /// One entry may take at most a quarter of the cache, so one big result
    /// cannot flush everything else.
    fn max_entry(&self) -> usize {
        self.max_bytes / 4
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The cached result for `key`, as a stream.
    pub fn get(&self, key: &str) -> Option<SendableRecordBatchStream> {
        let mut s = self.lock();
        s.tick += 1;
        let tick = s.tick;
        let found = s.entries.get_mut(key).map(|e| {
            e.used = tick;
            (e.batches.clone(), e.schema.clone())
        });
        drop(s);
        let Some((batches, schema)) = found else {
            self.misses.fetch_add(1, Ordering::Relaxed);
            return None;
        };
        self.hits.fetch_add(1, Ordering::Relaxed);
        MemoryStream::try_new(batches, schema, None)
            .ok()
            .map(|s| Box::pin(s) as SendableRecordBatchStream)
    }

    /// Reserves `bytes` for a result being collected, evicting LRU entries
    /// as needed. False when it cannot fit: the caller stops collecting.
    fn grow(&self, bytes: usize) -> bool {
        let mut s = self.lock();
        while s.memory.size() + bytes > self.max_bytes {
            let Some(oldest) = s
                .entries
                .iter()
                .min_by_key(|(_, e)| e.used)
                .map(|(k, _)| k.clone())
            else {
                return false;
            };
            if let Some(e) = s.entries.remove(&oldest) {
                s.memory.shrink(e.bytes);
            }
        }
        s.memory.try_grow(bytes).is_ok()
    }

    fn release(&self, bytes: usize) {
        self.lock().memory.shrink(bytes);
    }

    fn insert(&self, key: String, schema: SchemaRef, batches: Vec<RecordBatch>, bytes: usize) {
        let mut s = self.lock();
        s.tick += 1;
        let used = s.tick;
        let entry = Entry {
            schema,
            batches,
            bytes,
            used,
        };
        if let Some(old) = s.entries.insert(key, entry) {
            // A concurrent identical query finished first; keep one copy.
            s.memory.shrink(old.bytes);
        }
    }

    /// Passes `stream` through, keeping a copy of its batches; when it ends
    /// successfully the copy becomes the entry for `key`. Collection stops
    /// (and its bytes are released) if the result outgrows an entry or the
    /// cache, or the query fails or is dropped.
    pub fn tee(
        self: &Arc<Self>,
        key: String,
        stream: SendableRecordBatchStream,
    ) -> SendableRecordBatchStream {
        let schema = stream.schema();
        let mut collecting = Collecting {
            cache: Arc::clone(self),
            batches: Some(Vec::new()),
            bytes: 0,
        };
        let entry_schema = schema.clone();
        let mut key = Some(key);
        let inner = stream
            .map(Some)
            .chain(futures::stream::once(async { None }));
        let out = inner.filter_map(move |item| {
            let out = match item {
                Some(Ok(batch)) => {
                    collecting.add(&batch);
                    Some(Ok(batch))
                }
                Some(Err(e)) => {
                    collecting.abandon();
                    Some(Err(e))
                }
                None => {
                    if let (Some(batches), Some(key)) = (collecting.batches.take(), key.take()) {
                        let bytes = std::mem::take(&mut collecting.bytes);
                        collecting
                            .cache
                            .insert(key, entry_schema.clone(), batches, bytes);
                    }
                    None
                }
            };
            std::future::ready(out)
        });
        Box::pin(RecordBatchStreamAdapter::new(schema, out))
    }
}

/// A result being copied into the cache; dropped early, it gives its bytes
/// back.
struct Collecting {
    cache: Arc<ResultCache>,
    /// `None` once abandoned or inserted.
    batches: Option<Vec<RecordBatch>>,
    bytes: usize,
}

impl Collecting {
    fn add(&mut self, batch: &RecordBatch) {
        if self.batches.is_none() {
            return;
        }
        let n = batch.get_array_memory_size();
        if self.bytes + n > self.cache.max_entry() || !self.cache.grow(n) {
            self.abandon();
            return;
        }
        self.bytes += n;
        if let Some(b) = &mut self.batches {
            b.push(batch.clone());
        }
    }

    fn abandon(&mut self) {
        self.batches = None;
        self.cache.release(std::mem::take(&mut self.bytes));
    }
}

impl Drop for Collecting {
    fn drop(&mut self) {
        self.abandon();
    }
}

/// The cache key for a planned query whose logical plan was
/// [`deterministic`], or `None` when a source is not pinned.
pub fn key(sql: &str, physical: &Arc<dyn ExecutionPlan>) -> Option<String> {
    let mut pinned = Vec::new();
    if !pinned_inputs(physical, &mut pinned) {
        return None;
    }
    pinned.sort();
    Some(format!("{sql}\u{0}{}", pinned.join("\u{0}")))
}

/// No volatile or stable scalar function anywhere, subqueries included,
/// and no scalar subquery. Checked before optimization (which folds
/// `now()` into a literal) and before scalar subqueries are pre-evaluated
/// into literals: their inputs (a Postgres watermark, say) would vanish
/// from the physical plan the key is built from.
pub fn deterministic(plan: &LogicalPlan) -> bool {
    let mut ok = true;
    let _ = plan.apply_with_subqueries(|node| {
        node.apply_expressions(|e| {
            e.apply(|e| {
                let pure = match e {
                    Expr::ScalarFunction(f) => {
                        f.func.signature().volatility == Volatility::Immutable
                    }
                    Expr::ScalarSubquery(_) => false,
                    _ => true,
                };
                if !pure {
                    ok = false;
                    return Ok(TreeNodeRecursion::Stop);
                }
                Ok(TreeNodeRecursion::Continue)
            })
        })?;
        Ok(if ok {
            TreeNodeRecursion::Continue
        } else {
            TreeNodeRecursion::Stop
        })
    });
    ok
}

/// Every leaf is under a pinned [`AccountedExec`] or is a constant source.
fn pinned_inputs(plan: &Arc<dyn ExecutionPlan>, out: &mut Vec<String>) -> bool {
    if let Some(id) = plan
        .as_ref()
        .downcast_ref::<AccountedExec>()
        .and_then(AccountedExec::pinned)
    {
        out.push(id.to_owned());
        return true;
    }
    let children = plan.children();
    if children.is_empty() {
        return matches!(plan.name(), "PlaceholderRowExec" | "EmptyExec");
    }
    children.into_iter().all(|c| pinned_inputs(c, out))
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::engine::Engine;
    use async_trait::async_trait;
    use datafusion::arrow::array::Int64Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::catalog::{Session, TableProvider};
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::logical_expr::TableType;

    /// A table whose rows are fixed per "snapshot", like an Iceberg scan.
    #[derive(Debug)]
    struct Snapshots(Mutex<(String, Vec<i64>)>);

    #[async_trait]
    impl TableProvider for Snapshots {
        fn schema(&self) -> SchemaRef {
            Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]))
        }
        fn table_type(&self) -> TableType {
            TableType::Base
        }
        async fn scan(
            &self,
            _: &dyn Session,
            projection: Option<&Vec<usize>>,
            _: &[Expr],
            _: Option<usize>,
        ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
            let (id, rows) = self.0.lock().unwrap().clone();
            let batch =
                RecordBatch::try_new(self.schema(), vec![Arc::new(Int64Array::from(rows))])?;
            let exec = MemorySourceConfig::try_new_exec(
                &[vec![batch]],
                self.schema(),
                projection.cloned(),
            )?;
            Ok(Arc::new(AccountedExec::new("Snap", exec).with_pinned(id)))
        }
    }

    fn engine(cache: usize) -> (Engine, Arc<Snapshots>, Arc<CeilingPool>) {
        let pool = Arc::new(CeilingPool::new(64 << 20, 1 << 20).unwrap());
        let e = Engine::new(pool.clone(), None)
            .unwrap()
            .with_result_cache(cache);
        let t = Arc::new(Snapshots(Mutex::new(("t@1".into(), vec![1, 2, 3]))));
        e.context().register_table("t", t.clone()).unwrap();
        (e, t, pool)
    }

    async fn sum(e: &Engine, sql: &str) -> i64 {
        let r = e.sql(sql).await.unwrap();
        r.batches
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .iter()
                    .flatten()
                    .collect::<Vec<_>>()
            })
            .sum()
    }

    #[tokio::test]
    async fn caches_pinned_results_until_the_snapshot_changes() {
        let (e, t, pool) = engine(1 << 20);
        let stats = || e.result_cache_stats().unwrap();
        let q = "SELECT sum(v) FROM t WHERE v > 1";
        assert_eq!(sum(&e, q).await, 5);
        assert_eq!((stats().hits, stats().misses, stats().entries), (0, 1, 1));
        assert_eq!(sum(&e, q).await, 5);
        assert_eq!(stats().hits, 1);
        // The entry is accounted in the pool while results are not held.
        assert!(stats().bytes > 0);
        assert_eq!(pool.used(), stats().bytes);

        // A new snapshot is a new key: the next query sees the new rows.
        *t.0.lock().unwrap() = ("t@2".into(), vec![10, 20]);
        assert_eq!(sum(&e, q).await, 30);
        assert_eq!((stats().hits, stats().misses, stats().entries), (1, 2, 2));
        // Different SQL, same snapshot: its own entry.
        assert_eq!(sum(&e, "SELECT sum(v) FROM t").await, 30);
        assert_eq!(stats().entries, 3);
    }

    #[tokio::test]
    async fn never_caches_what_could_change_under_the_same_key() {
        let (e, _t, _) = engine(1 << 20);
        let stats = || e.result_cache_stats().unwrap();
        for sql in [
            // Stable/volatile functions: folded or re-evaluated per query.
            "SELECT v, now() FROM t",
            "SELECT v + CAST(random() * 0 AS BIGINT) FROM t",
            // An unpinned source (could be a Postgres table or a directory).
            "SELECT value FROM range(3)",
            // A scalar subquery is pre-evaluated into a literal (ADR 0006).
            "SELECT v FROM t WHERE v > (SELECT min(value) FROM range(1))",
        ] {
            e.sql(sql).await.unwrap();
            e.sql(sql).await.unwrap();
            assert_eq!(stats().hits + stats().misses, 0, "{sql}");
        }
        assert_eq!(stats().entries, 0);
        // A constant source is pinned by construction.
        e.sql("SELECT 1").await.unwrap();
        assert_eq!(stats().entries, 1);
    }

    #[tokio::test]
    async fn oversized_results_are_not_kept() {
        // Entries may take a quarter of a 64-byte cache: nothing fits.
        let (e, _t, pool) = engine(64);
        assert_eq!(sum(&e, "SELECT v FROM t").await, 6);
        let s = e.result_cache_stats().unwrap();
        assert_eq!((s.entries, s.bytes), (0, 0));
        assert_eq!(pool.used(), 0);
    }
}

//! Engine façade: one `SessionContext` on one process-wide `CeilingPool`.
//!
//! The SQL path is: parse/plan -> pre-evaluate scalar subqueries (federation
//! watermarks) -> physical plan -> stream batches into an `Output`
//! reservation. Result buffers are accounted like any operator, so a huge
//! result fails the query instead of the process (ADR 0003).

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::MemorySchemaProvider;
use datafusion::error::{DataFusionError, Result};
use datafusion::physical_plan::execute_stream;
use datafusion::prelude::{SessionConfig, SessionContext};
use futures::StreamExt;

use crate::federation::preevaluate_scalar_subqueries;
use crate::memory::{CeilingPool, ConsumerKind, session_context_with_ceiling, spilled_bytes};

/// Medallion convention: these schemas always exist in the default catalog.
pub const MEDALLION: [&str; 3] = ["bronze", "silver", "gold"];

pub struct Engine {
    ctx: SessionContext,
    pool: Arc<CeilingPool>,
    spilled_total: AtomicU64,
}

impl Engine {
    /// `spill_dir = None` disables spilling (memory pressure fails the query).
    pub fn new(pool: Arc<CeilingPool>, spill_dir: Option<PathBuf>) -> Result<Self> {
        // DataFusion's default merge reservation (10 MiB) would starve small
        // ceilings before the first spill; keep it to a slice of the limit.
        let merge = (pool.limit() / 16).min(10 << 20);
        let config = SessionConfig::new()
            .with_information_schema(true)
            .with_sort_spill_reservation_bytes(merge);
        let ctx = session_context_with_ceiling(pool.clone(), spill_dir, config)?;
        let catalog = ctx
            .catalog("datafusion")
            .ok_or_else(|| DataFusionError::Internal("default catalog missing".into()))?;
        for schema in MEDALLION {
            catalog.register_schema(schema, Arc::new(MemorySchemaProvider::new()))?;
        }
        Ok(Self {
            ctx,
            pool,
            spilled_total: AtomicU64::new(0),
        })
    }

    /// For registering tables, catalogs and system tables.
    pub fn context(&self) -> &SessionContext {
        &self.ctx
    }

    pub fn pool(&self) -> &Arc<CeilingPool> {
        &self.pool
    }

    /// Bytes spilled to disk by all completed queries since start.
    pub fn spilled_bytes_total(&self) -> u64 {
        self.spilled_total.load(Ordering::Relaxed)
    }

    /// Runs one statement. Dropping the returned future cancels execution
    /// (the stream and its reservations are dropped with it).
    pub async fn sql(&self, sql: &str) -> Result<Vec<RecordBatch>> {
        let df = self.ctx.sql(sql).await?;
        let (state, plan) = df.into_parts();
        let plan = preevaluate_scalar_subqueries(&state, plan).await?;
        let physical = state.create_physical_plan(&plan).await?;

        let mut output = self.pool.register("query-output", ConsumerKind::Output);
        let mut stream = execute_stream(physical.clone(), self.ctx.task_ctx())?;
        let mut batches = Vec::new();
        let result = async {
            while let Some(batch) = stream.next().await {
                let batch = batch?;
                output.try_grow(batch.get_array_memory_size())?;
                batches.push(batch);
            }
            Ok::<_, DataFusionError>(())
        }
        .await;
        drop(stream);
        // Count spill even for failed queries: the disk was written either way.
        self.spilled_total
            .fetch_add(spilled_bytes(&physical) as u64, Ordering::Relaxed);
        result?;
        // ponytail: the reservation is released when batches leave the engine;
        // the server's JSON copy is unaccounted. Upgrade path: stream rows to
        // the client and shrink per batch sent.
        Ok(batches)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn engine(limit: usize) -> Engine {
        let pool = Arc::new(CeilingPool::new(limit, limit / 8).unwrap());
        Engine::new(pool, None).unwrap()
    }

    #[tokio::test]
    async fn runs_sql_and_releases_memory() {
        let e = engine(64 << 20);
        let batches = e
            .sql("SELECT count(*) AS n FROM range(1000)")
            .await
            .unwrap();
        assert_eq!(batches[0].num_rows(), 1);
        assert_eq!(e.pool().used(), 0);
    }

    #[tokio::test]
    async fn oversized_result_fails_query_not_process() {
        let e = engine(1 << 20);
        let err = e
            .sql("SELECT value, repeat('x', 100) AS pad FROM range(200000)")
            .await
            .unwrap_err();
        assert!(
            matches!(err.find_root(), DataFusionError::ResourcesExhausted(_)),
            "{err}"
        );
        assert_eq!(e.pool().used(), 0);
        // The node is still afloat.
        assert!(e.sql("SELECT 1").await.is_ok());
    }

    #[tokio::test]
    async fn medallion_schemas_exist() {
        let e = engine(64 << 20);
        e.sql("CREATE TABLE gold.t AS SELECT 1 AS x").await.unwrap();
        let b = e.sql("SELECT x FROM gold.t").await.unwrap();
        assert_eq!(b[0].num_rows(), 1);
    }
}

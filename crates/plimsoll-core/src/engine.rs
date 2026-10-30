//! Engine façade: one `SessionContext` on one process-wide `CeilingPool`.
//!
//! The SQL path is: parse/plan -> pre-evaluate scalar subqueries (federation
//! watermarks) -> physical plan -> stream batches into an `Output`
//! reservation. Result buffers are accounted like any operator, so a huge
//! result fails the query instead of the process (ADR 0003).

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::MemorySchemaProvider;
use datafusion::common::ResolvedTableReference;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::SQLOptions;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::execute_stream;
use datafusion::prelude::{SessionConfig, SessionContext};
use futures::StreamExt;

use crate::federation::preevaluate_scalar_subqueries;
use crate::memory::{
    CeilingPool, ConsumerKind, Reservation, session_context_with_ceiling, spilled_bytes,
};

/// Medallion convention: these schemas always exist in the default catalog.
pub const MEDALLION: [&str; 3] = ["bronze", "silver", "gold"];

/// A query's result and the `Output` reservation that accounts for it. Keep
/// `memory` alive for as long as the result (or anything derived from it, such
/// as a serialized response) is held; shrink/grow it as the result changes form.
pub struct QueryResult {
    pub batches: Vec<RecordBatch>,
    pub memory: Reservation,
    /// SQL text -> physical plan (parse, analyze, optimize, subquery
    /// pre-evaluation, connector planning such as Iceberg manifests/deletes).
    pub planning: Duration,
    /// Physical plan -> all batches collected.
    pub execution: Duration,
}

impl std::fmt::Debug for QueryResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueryResult")
            .field("batches", &self.batches.len())
            .field("reserved_bytes", &self.memory.size())
            .finish()
    }
}

/// Who may read which table (ADR 0025). Implemented by the front door (the
/// server's caller identity); the engine only asks.
pub trait TableAccess: Send + Sync + 'static {
    fn check(&self, table: &ResolvedTableReference) -> Result<()>;
}

/// The caller may not read `table`. Returned (wrapped in
/// `DataFusionError::External`) by [`TableAccess`] implementations so front
/// doors can classify it.
#[derive(Debug, thiserror::Error)]
#[error("access denied to table {table}")]
pub struct AccessDenied {
    pub table: String,
}

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
    pub async fn sql(&self, sql: &str) -> Result<QueryResult> {
        self.run(&self.ctx, sql, None).await
    }

    /// [`Engine::sql`] on behalf of `access`: every table the plan scans,
    /// subqueries included, must pass [`TableAccess::check`] before anything
    /// executes (ADR 0025). `access` is also put in this query's session
    /// config for providers that depend on who is asking (ADR 0022). Same
    /// catalogs, runtime and pool; only the config is per query.
    pub async fn sql_with<T: TableAccess>(&self, sql: &str, access: Arc<T>) -> Result<QueryResult> {
        let mut state = self.ctx.state();
        state.config_mut().set_extension(access.clone());
        let ctx = SessionContext::new_with_state(state);
        self.run(&ctx, sql, Some(access.as_ref())).await
    }

    async fn run(
        &self,
        ctx: &SessionContext,
        sql: &str,
        access: Option<&dyn TableAccess>,
    ) -> Result<QueryResult> {
        let started = Instant::now();
        // Read-only SQL (ADR 0024): DDL would let any caller read or write
        // server files (`CREATE EXTERNAL TABLE ... LOCATION '/etc/...'`,
        // `COPY ... TO`) and mutate the catalogs every user shares; `SET`
        // would tune engine limits. Tables are registered by the operator
        // (env, catalog), never through the query API.
        let read_only = SQLOptions::new()
            .with_allow_ddl(false)
            .with_allow_dml(false)
            .with_allow_statements(false);
        let df = ctx.sql_with_options(sql, read_only).await?;
        let (state, plan) = df.into_parts();
        // Before subquery pre-evaluation, which already executes.
        if let Some(access) = access {
            let options = state.config_options();
            let (catalog, schema) = (
                &options.catalog.default_catalog,
                &options.catalog.default_schema,
            );
            plan.apply_with_subqueries(|node| {
                if let LogicalPlan::TableScan(scan) = node {
                    access.check(&scan.table_name.clone().resolve(catalog, schema))?;
                }
                Ok(TreeNodeRecursion::Continue)
            })?;
        }
        let plan = preevaluate_scalar_subqueries(&state, plan).await?;
        let physical = state.create_physical_plan(&plan).await?;
        let planning = started.elapsed();

        let mut output = self.pool.register("query-output", ConsumerKind::Output);
        let mut stream = execute_stream(physical.clone(), state.task_ctx())?;
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
        Ok(QueryResult {
            batches,
            memory: output,
            planning,
            execution: started.elapsed() - planning,
        })
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
        let result = e
            .sql("SELECT count(*) AS n FROM range(1000)")
            .await
            .unwrap();
        assert_eq!(result.batches[0].num_rows(), 1);
        // The result is accounted for as long as the caller holds it.
        assert_eq!(e.pool().output_used(), result.memory.size());
        assert!(result.memory.size() > 0);
        drop(result);
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
        // Operators register tables on the context; the query API can't.
        e.context()
            .sql("CREATE TABLE gold.t AS SELECT 1 AS x")
            .await
            .unwrap();
        let b = e.sql("SELECT x FROM gold.t").await.unwrap().batches;
        assert_eq!(b[0].num_rows(), 1);
    }

    #[tokio::test]
    async fn query_api_is_read_only() {
        let e = engine(64 << 20);
        let dir = std::env::temp_dir().join(format!("plimsoll-ro-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let secret = dir.join("secret.csv");
        std::fs::write(&secret, "id,secret\n1,hunter2\n").unwrap();
        let out = dir.join("out.csv");
        for sql in [
            // Arbitrary file read through a table definition.
            format!(
                "CREATE EXTERNAL TABLE leak STORED AS CSV LOCATION '{}'",
                secret.display()
            ),
            // Arbitrary file write.
            format!("COPY (SELECT 1) TO '{}' STORED AS CSV", out.display()),
            // Shared catalog mutation, and session/engine tuning.
            "CREATE VIEW gold.v AS SELECT 1".to_owned(),
            "DROP SCHEMA bronze".to_owned(),
            "SET datafusion.execution.target_partitions = 1".to_owned(),
            // Also inside a subquery or CTE.
            "WITH x AS (SELECT 1) SELECT * FROM x".to_owned(),
        ] {
            let result = e.sql(&sql).await;
            if sql.starts_with("WITH") {
                assert!(result.is_ok(), "plain queries still work: {sql}");
                continue;
            }
            let err = result.unwrap_err().to_string();
            assert!(err.contains("not supported"), "{sql}: {err}");
        }
        assert!(!out.exists(), "COPY must not write");
        assert!(
            e.sql("SELECT * FROM leak").await.is_err(),
            "no table was created"
        );
        assert!(
            e.context()
                .catalog("datafusion")
                .unwrap()
                .schema("bronze")
                .is_some()
        );
    }

    /// Denies the `gold` schema.
    struct NoGold;
    impl TableAccess for NoGold {
        fn check(&self, table: &ResolvedTableReference) -> Result<()> {
            if table.schema.as_ref() == "gold" {
                return Err(DataFusionError::External(Box::new(AccessDenied {
                    table: table.to_string(),
                })));
            }
            Ok(())
        }
    }

    #[tokio::test]
    async fn table_access_is_checked_on_every_scan() {
        let e = engine(64 << 20);
        let ctx = e.context();
        ctx.sql("CREATE TABLE gold.g AS SELECT 1 AS x")
            .await
            .unwrap();
        ctx.sql("CREATE TABLE silver.s AS SELECT 2 AS x")
            .await
            .unwrap();
        let access = Arc::new(NoGold);
        let ok = e
            .sql_with("SELECT x FROM silver.s", access.clone())
            .await
            .unwrap();
        assert_eq!(ok.batches[0].num_rows(), 1);
        drop(ok);
        for sql in [
            "SELECT x FROM gold.g",
            // Hidden in a CTE, a join, and a scalar subquery (which is
            // pre-evaluated, i.e. executed, during planning).
            "WITH c AS (SELECT * FROM gold.g) SELECT * FROM c",
            "SELECT s.x FROM silver.s s JOIN gold.g g ON s.x = g.x",
            "SELECT x FROM silver.s WHERE x > (SELECT max(x) FROM gold.g)",
        ] {
            let err = e.sql_with(sql, access.clone()).await.unwrap_err();
            let denied = matches!(err.find_root(), DataFusionError::External(inner)
                if inner.downcast_ref::<AccessDenied>().is_some());
            assert!(denied, "{sql}: {err}");
            assert!(
                err.to_string()
                    .contains("access denied to table datafusion.gold.g")
            );
        }
        assert_eq!(e.pool().used(), 0);
    }
}

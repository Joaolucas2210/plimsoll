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
use datafusion::common::{ScalarValue, plan_err};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::execution::context::SQLOptions;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::{ExecutionPlan, execute_stream};
use datafusion::prelude::{DataFrame, SessionConfig, SessionContext};
use futures::StreamExt;

use crate::federation::preevaluate_scalar_subqueries;
use crate::memory::{
    CeilingPool, ConsumerKind, GroupPool, Reservation, session_context_with_ceiling, spilled_bytes,
};
use crate::result_cache::{self, CacheStats, ResultCache};

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

    /// The resource group the query runs in; its memory share, if any,
    /// caps every reservation the query makes (ADR 0036).
    fn resource_group(&self) -> Option<&str> {
        None
    }

    /// Whether the caller may read `columns` of `table` (ADR 0044). Called
    /// with the columns each scan of the *optimized* plan reads, filter
    /// columns included, so unused columns of a table never deny a query.
    fn check_columns(&self, _table: &ResolvedTableReference, _columns: &[&str]) -> Result<()> {
        Ok(())
    }
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
    result_cache: Option<Arc<ResultCache>>,
    spilled_total: Arc<AtomicU64>,
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
            result_cache: None,
            spilled_total: Arc::new(AtomicU64::new(0)),
        })
    }

    /// Caches results of queries over pinned inputs, up to `max_bytes`
    /// reserved in the pool (ADR 0038). `0` disables it.
    pub fn with_result_cache(mut self, max_bytes: usize) -> Self {
        self.result_cache =
            (max_bytes > 0).then(|| Arc::new(ResultCache::new(&self.pool, max_bytes)));
        self
    }

    pub fn result_cache_stats(&self) -> Option<CacheStats> {
        self.result_cache.as_ref().map(|c| c.stats())
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
        self.run(&self.ctx, sql, &[], None).await
    }

    /// [`Engine::sql`] on behalf of `access`: every table the plan scans,
    /// subqueries included, must pass [`TableAccess::check`] before anything
    /// executes (ADR 0025). `access` is also put in this query's session
    /// config for providers that depend on who is asking (ADR 0022). Same
    /// catalogs, runtime and pool; only the config is per query.
    ///
    /// `params` bind `$1`, `$2`, ... (ADR 0043): each value is cast to the
    /// type the planner inferred for its placeholder.
    pub async fn sql_with<T: TableAccess>(
        &self,
        sql: &str,
        params: &[ScalarValue],
        access: Arc<T>,
    ) -> Result<QueryResult> {
        let ctx = self.context_for(access.clone());
        self.run(&ctx, sql, params, Some(access.as_ref())).await
    }

    /// [`Engine::sql_with`] without collecting: the planned query as a
    /// stream, pulled at the caller's pace, so results of any size flow
    /// through (ADR 0035). Batches taken from it are the caller's to account
    /// for.
    pub async fn sql_stream_with<T: TableAccess>(
        &self,
        sql: &str,
        params: &[ScalarValue],
        access: Arc<T>,
    ) -> Result<QueryStream> {
        let ctx = self.context_for(access.clone());
        self.plan_stream(&ctx, sql, params, Some(access.as_ref()))
            .await
    }

    fn context_for<T: TableAccess>(&self, access: Arc<T>) -> SessionContext {
        let mut state = self.ctx.state();
        // A capped group gets its own view of the pool (ADR 0036).
        if let Some(group) = self.capped_group(access.as_ref()) {
            let runtime = GroupPool::new(self.pool.clone(), group).runtime(state.runtime_env());
            state = SessionStateBuilder::new_from_existing(state)
                .with_runtime_env(runtime)
                .build();
        }
        state.config_mut().set_extension(access);
        SessionContext::new_with_state(state)
    }

    /// `access`'s resource group when it has a memory share.
    pub fn capped_group(&self, access: &dyn TableAccess) -> Option<Arc<str>> {
        access
            .resource_group()
            .filter(|g| self.pool.group_share(g).is_some())
            .map(Arc::from)
    }

    async fn run(
        &self,
        ctx: &SessionContext,
        sql: &str,
        params: &[ScalarValue],
        access: Option<&dyn TableAccess>,
    ) -> Result<QueryResult> {
        let started = Instant::now();
        let mut query = self.plan_stream(ctx, sql, params, access).await?;
        let group = access.and_then(|a| self.capped_group(a));
        let mut output =
            self.pool
                .register_in(group.as_ref(), "query-output", ConsumerKind::Output);
        let mut batches = Vec::new();
        while let Some(batch) = query.stream.next().await {
            let batch = batch?;
            output.try_grow(batch.get_array_memory_size())?;
            batches.push(batch);
        }
        let planning = query.planning;
        drop(query);
        Ok(QueryResult {
            batches,
            memory: output,
            planning,
            execution: started.elapsed() - planning,
        })
    }

    async fn plan_stream(
        &self,
        ctx: &SessionContext,
        sql: &str,
        params: &[ScalarValue],
        access: Option<&dyn TableAccess>,
    ) -> Result<QueryStream> {
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
        let df = bind(ctx.sql_with_options(sql, read_only).await?, params)?;
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
        // Before optimization and subquery pre-evaluation (ADR 0038).
        let cache = self
            .result_cache
            .as_ref()
            .filter(|_| result_cache::deterministic(&plan));
        let plan = preevaluate_scalar_subqueries(&state, plan).await?;
        // Optimized once, here, so column grants see what each scan really
        // reads (projection pushdown) before anything executes (ADR 0044).
        let plan = state.optimize(&plan)?;
        if let Some(access) = access {
            let options = state.config_options();
            let (catalog, schema) = (
                &options.catalog.default_catalog,
                &options.catalog.default_schema,
            );
            plan.apply_with_subqueries(|node| {
                if let LogicalPlan::TableScan(scan) = node {
                    let mut columns: Vec<&str> = scan
                        .projected_schema
                        .fields()
                        .iter()
                        .map(|f| f.name().as_str())
                        .collect();
                    // Pushed-down filters may read columns the projection
                    // dropped: `WHERE email = ...` reveals email too.
                    for filter in &scan.filters {
                        for c in filter.column_refs() {
                            columns.push(c.name.as_str());
                        }
                    }
                    let table = scan.table_name.clone().resolve(catalog, schema);
                    access.check_columns(&table, &columns)?;
                }
                Ok(TreeNodeRecursion::Continue)
            })?;
        }
        let physical = state
            .query_planner()
            .create_physical_plan(&plan, &state)
            .await?;
        // Grants were checked above: a hit is only served to a principal
        // allowed to read every input.
        // Parameters are part of the statement: same SQL, other values,
        // other result.
        let statement = if params.is_empty() {
            sql.to_owned()
        } else {
            format!("{sql}\u{0}{params:?}")
        };
        let key = cache.and_then(|_| result_cache::key(&statement, &physical));
        let stream = match (cache, key) {
            (Some(cache), Some(key)) => match cache.get(&key) {
                Some(hit) => hit,
                None => cache.tee(key, execute_stream(physical.clone(), state.task_ctx())?),
            },
            _ => execute_stream(physical.clone(), state.task_ctx())?,
        };
        Ok(QueryStream {
            stream,
            planning: started.elapsed(),
            physical,
            spilled_total: self.spilled_total.clone(),
        })
    }
}

/// Binds `params` to the plan's `$n` placeholders (ADR 0043). Every
/// placeholder needs a value and every value a placeholder; a value is
/// cast to the type inferred for its placeholder (a JSON integer compared
/// with an `INT` column, a string compared with a `DATE`), so clients need
/// not know Arrow types. Values are never spliced into SQL text.
fn bind(df: DataFrame, params: &[ScalarValue]) -> Result<DataFrame> {
    let types = df.logical_plan().get_parameter_types()?;
    if types.is_empty() && params.is_empty() {
        return Ok(df);
    }
    if types.len() != params.len() {
        return plan_err!(
            "the query has {} parameter(s), {} value(s) were given",
            types.len(),
            params.len()
        );
    }
    let values = params
        .iter()
        .enumerate()
        .map(|(i, v)| {
            let name = format!("${}", i + 1);
            let Some(ty) = types.get(&name) else {
                return plan_err!("parameter {name} is not used (use $1..${})", params.len());
            };
            match ty {
                Some(ty) if !v.is_null() && &v.data_type() != ty => v
                    .cast_to(ty)
                    .map_err(|e| DataFusionError::Plan(format!("parameter {name}: {e}"))),
                Some(ty) if v.is_null() => ScalarValue::try_from(ty),
                _ => Ok(v.clone()),
            }
        })
        .collect::<Result<Vec<_>>>()?;
    df.with_param_values(values)
}

/// A planned query's result stream. Dropping it stops the query; spill is
/// counted then, failed queries included (the disk was written either way).
pub struct QueryStream {
    pub stream: SendableRecordBatchStream,
    /// SQL text -> physical plan.
    pub planning: Duration,
    physical: Arc<dyn ExecutionPlan>,
    spilled_total: Arc<AtomicU64>,
}

impl Drop for QueryStream {
    fn drop(&mut self) {
        self.spilled_total
            .fetch_add(spilled_bytes(&self.physical) as u64, Ordering::Relaxed);
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
            .sql_with("SELECT x FROM silver.s", &[], access.clone())
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
            let err = e.sql_with(sql, &[], access.clone()).await.unwrap_err();
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

    /// A query in a capped group fails on its group's share while the
    /// ceiling still has room; the same query outside the group runs.
    #[tokio::test]
    async fn group_share_caps_a_query_below_the_ceiling() {
        struct In(Option<&'static str>);
        impl TableAccess for In {
            fn check(&self, _: &ResolvedTableReference) -> Result<()> {
                Ok(())
            }
            fn resource_group(&self) -> Option<&str> {
                self.0
            }
        }
        let pool = Arc::new(
            CeilingPool::new(64 << 20, 8 << 20)
                .unwrap()
                .with_group_share("batch", 1 << 20),
        );
        let e = Engine::new(pool.clone(), None).unwrap();
        let sql = "SELECT a.value FROM range(300000) a JOIN range(300000) b ON a.value = b.value";
        let err = e
            .sql_with(sql, &[], Arc::new(In(Some("batch"))))
            .await
            .unwrap_err();
        // Some operators (the spill pool) replace the pool's message with
        // their own; the class is what clients see (memory_limit_exceeded),
        // and the same query outside the group runs (below).
        assert!(
            matches!(err.find_root(), DataFusionError::ResourcesExhausted(_)),
            "{err}"
        );
        // Partition tasks spawned by DataFusion drop their reservations
        // as they wind down, just after the error is returned.
        for _ in 0..200 {
            if pool.used() == 0 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        assert_eq!(pool.used(), 0);
        assert_eq!(pool.group_used("batch"), 0);
        // Ungrouped and other groups: only the ceiling applies.
        for group in [None, Some("adhoc")] {
            let ok = e.sql_with(sql, &[], Arc::new(In(group))).await.unwrap();
            assert_eq!(
                ok.batches.iter().map(|b| b.num_rows()).sum::<usize>(),
                300_000
            );
        }
        // A small query in the group fits its share.
        let ok = e
            .sql_with(
                "SELECT count(*) FROM range(1000)",
                &[],
                Arc::new(In(Some("batch"))),
            )
            .await
            .unwrap();
        drop(ok);
        assert_eq!(pool.used(), 0);
    }

    /// `$n` parameters (ADR 0043): bound as values, cast to the inferred
    /// type, counted exactly, never spliced into SQL.
    #[tokio::test]
    async fn binds_parameters_as_values() {
        struct Any;
        impl TableAccess for Any {
            fn check(&self, _: &ResolvedTableReference) -> Result<()> {
                Ok(())
            }
        }
        let e = engine(64 << 20);
        e.context()
            .sql(
                "CREATE TABLE gold.p AS SELECT * FROM (VALUES \
                 (CAST(1 AS INT), 'ana', DATE '2024-05-01'), \
                 (CAST(2 AS INT), 'bob', DATE '2024-05-02')) AS t(id, name, day)",
            )
            .await
            .unwrap();
        let count = async |sql: &str, params: &[ScalarValue]| {
            let r = e.sql_with(sql, params, Arc::new(Any)).await?;
            Ok::<_, DataFusionError>(r.batches.iter().map(|b| b.num_rows()).sum::<usize>())
        };
        // An Int64 value for an INT column, a string for a DATE.
        let q = "SELECT * FROM gold.p WHERE id = $1 AND day = $2";
        let p = [ScalarValue::Int64(Some(2)), ScalarValue::from("2024-05-02")];
        assert_eq!(count(q, &p).await.unwrap(), 1);
        // A value is a value: an injection attempt matches nothing.
        let q = "SELECT * FROM gold.p WHERE name = $1";
        assert_eq!(
            count(q, &[ScalarValue::from("x' OR '1'='1")])
                .await
                .unwrap(),
            0
        );
        assert_eq!(count(q, &[ScalarValue::from("ana")]).await.unwrap(), 1);
        // NULL compares as SQL NULL.
        assert_eq!(count(q, &[ScalarValue::Null]).await.unwrap(), 0);
        // Exact arity, both ways; and values that do not fit their type.
        for (sql, params) in [
            ("SELECT * FROM gold.p WHERE id = $1", vec![]),
            ("SELECT 1", vec![ScalarValue::Int64(Some(1))]),
            (
                "SELECT * FROM gold.p WHERE id = $1",
                vec![ScalarValue::Int64(Some(1)), ScalarValue::Int64(Some(2))],
            ),
            (
                "SELECT * FROM gold.p WHERE day = $1",
                vec![ScalarValue::from("not a date")],
            ),
        ] {
            let err = count(sql, &params).await.unwrap_err();
            assert!(
                matches!(err.find_root(), DataFusionError::Plan(_)),
                "{sql} {params:?}: {err}"
            );
        }
    }
}

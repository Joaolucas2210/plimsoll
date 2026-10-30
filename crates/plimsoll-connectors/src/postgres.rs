//! Read-only Postgres connector with a conservative pushdown policy.
//!
//! Federated: projection, integer/boolean comparisons, `IS [NOT] NULL`, LIMIT.
//! Local: text (collation), floats (NaN / -0.0 ordering), LIKE, temporal,
//! everything else. Aggregates are never federated. See ADR 0006.

mod sql;

use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Datelike, NaiveDate, NaiveDateTime, Utc};
use datafusion::arrow::array::{
    ArrayRef, BooleanArray, Date32Array, Float32Array, Float64Array, Int16Array, Int32Array,
    Int64Array, RecordBatch, RecordBatchOptions, StringArray, TimestampMicrosecondArray,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{Result, internal_err};
use datafusion::error::DataFusionError;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::{Expr, TableType};
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
};
use futures::{StreamExt, TryStreamExt};
use plimsoll_core::federation::{Pushdown, PushdownPolicy};
use plimsoll_core::memory::AccountedExec;
use tokio_postgres::types::{FromSql, Type};
use tokio_postgres::{Client, NoTls, Row};

use crate::{Connector, ConnectorError};
use sql::{Query, build_query, filter_to_sql, quote_ident};

/// The only place the connection string may come from.
pub const PG_URL_ENV: &str = "PLIMSOLL_PG_URL";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PgKind {
    Int2,
    Int4,
    Int8,
    Float4,
    Float8,
    Bool,
    Text,
    Numeric,
    Date,
    Timestamp,
    Timestamptz,
}

impl PgKind {
    fn from_pg(t: &Type) -> Option<Self> {
        let table = [
            (Type::INT2, PgKind::Int2),
            (Type::INT4, PgKind::Int4),
            (Type::INT8, PgKind::Int8),
            (Type::FLOAT4, PgKind::Float4),
            (Type::FLOAT8, PgKind::Float8),
            (Type::BOOL, PgKind::Bool),
            (Type::TEXT, PgKind::Text),
            (Type::VARCHAR, PgKind::Text),
            (Type::BPCHAR, PgKind::Text),
            (Type::NAME, PgKind::Text),
            (Type::NUMERIC, PgKind::Numeric),
            (Type::DATE, PgKind::Date),
            (Type::TIMESTAMP, PgKind::Timestamp),
            (Type::TIMESTAMPTZ, PgKind::Timestamptz),
        ];
        table.into_iter().find(|(pg, _)| pg == t).map(|(_, k)| k)
    }

    fn arrow(self) -> DataType {
        match self {
            PgKind::Int2 => DataType::Int16,
            PgKind::Int4 => DataType::Int32,
            PgKind::Int8 => DataType::Int64,
            PgKind::Float4 => DataType::Float32,
            PgKind::Float8 => DataType::Float64,
            PgKind::Bool => DataType::Boolean,
            PgKind::Text | PgKind::Numeric => DataType::Utf8,
            PgKind::Date => DataType::Date32,
            PgKind::Timestamp => DataType::Timestamp(TimeUnit::Microsecond, None),
            PgKind::Timestamptz => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct PgColumn {
    name: String,
    kind: PgKind,
}

impl PgColumn {
    pub(crate) fn new(name: impl Into<String>, kind: PgKind) -> Self {
        Self {
            name: name.into(),
            kind,
        }
    }
}

/// Connection string holder whose `Debug` never prints credentials.
#[derive(Clone)]
struct PgUrl(Arc<str>);

impl fmt::Debug for PgUrl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("PgUrl(<redacted>)")
    }
}

// ponytail: one fresh connection per scan, no TLS, no pool. Ceiling: connection
// setup cost per query and plaintext on the wire. Upgrade path: a shared pool
// (deadpool-postgres) and rustls behind a config flag.
async fn connect(url: &str) -> Result<Client, ConnectorError> {
    let (client, conn) = tokio_postgres::connect(url, NoTls).await?;
    // Connection errors also surface on the next client call; nothing to add here.
    tokio::spawn(async move {
        let _ = conn.await;
    });
    Ok(client)
}

/// Unguarded Postgres table source. Claims `Exact` for every filter it can
/// translate; the [`PushdownPolicy`] decides what is actually trusted.
#[derive(Debug)]
pub struct PostgresTable {
    url: PgUrl,
    relation: String,
    columns: Vec<PgColumn>,
    schema: SchemaRef,
}

impl PostgresTable {
    fn new(url: &str, pg_schema: &str, pg_table: &str, columns: Vec<PgColumn>) -> Self {
        let schema = Arc::new(Schema::new(
            columns
                .iter()
                .map(|c| Field::new(&c.name, c.kind.arrow(), true))
                .collect::<Vec<_>>(),
        ));
        Self {
            url: PgUrl(url.into()),
            relation: format!("{}.{}", quote_ident(pg_schema), quote_ident(pg_table)),
            columns,
            schema,
        }
    }

    /// Introspect the table once; fails fast on any unsupported column type.
    async fn connect(url: &str, pg_schema: &str, pg_table: &str) -> Result<Self, ConnectorError> {
        let mut table = Self::new(url, pg_schema, pg_table, vec![]);
        let client = connect(url).await?;
        let stmt = client
            .prepare(&format!("SELECT * FROM {} LIMIT 0", table.relation))
            .await?;
        let columns = stmt
            .columns()
            .iter()
            .map(|c| match PgKind::from_pg(c.type_()) {
                Some(kind) => Ok(PgColumn::new(c.name(), kind)),
                None => Err(ConnectorError::UnsupportedType {
                    column: c.name().to_string(),
                    pg_type: c.type_().to_string(),
                }),
            })
            .collect::<Result<Vec<_>, _>>()?;
        table = Self::new(url, pg_schema, pg_table, columns);
        Ok(table)
    }

    fn query(
        &self,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> (Query, Vec<PgColumn>) {
        let cols: Vec<&PgColumn> = match projection {
            Some(p) => p.iter().filter_map(|&i| self.columns.get(i)).collect(),
            None => self.columns.iter().collect(),
        };
        let q = build_query(&self.relation, &cols, filters, limit);
        (q, cols.into_iter().cloned().collect())
    }
}

#[async_trait]
impl TableProvider for PostgresTable {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(&self, filters: &[&Expr]) -> Result<Vec<Pushdown>> {
        Ok(filters
            .iter()
            .map(|f| match filter_to_sql(f, &mut Vec::new()) {
                Some(_) => Pushdown::Exact,
                None => Pushdown::Unsupported,
            })
            .collect())
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let schema = Arc::new(
            self.schema
                .project(projection.map_or(&(0..self.columns.len()).collect::<Vec<_>>(), |p| p))?,
        );
        let (query, columns) = self.query(projection, filters, limit);
        let props = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        let scan = Arc::new(PostgresScanExec {
            url: self.url.clone(),
            query,
            columns,
            schema,
            props,
        });
        // Each batch is reserved in the session pool as it is produced.
        Ok(Arc::new(AccountedExec::new(
            format!("PostgresScan[{}]", self.relation),
            scan,
        )))
    }
}

/// Leaf operator that runs one SQL statement. `EXPLAIN` shows the exact SQL.
#[derive(Debug)]
pub struct PostgresScanExec {
    url: PgUrl,
    query: Query,
    columns: Vec<PgColumn>,
    schema: SchemaRef,
    props: Arc<PlanProperties>,
}

impl PostgresScanExec {
    /// The SQL sent to Postgres (literals are `$n` parameters).
    pub fn sql(&self) -> &str {
        &self.query.sql
    }
}

impl DisplayAs for PostgresScanExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "PostgresScanExec: sql={}, params={}",
            self.query.sql,
            self.query.params.len()
        )
    }
}

impl ExecutionPlan for PostgresScanExec {
    fn name(&self) -> &str {
        "PostgresScanExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.props
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.is_empty() {
            Ok(self)
        } else {
            internal_err!("PostgresScanExec is a leaf")
        }
    }

    fn execute(
        &self,
        partition: usize,
        ctx: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return internal_err!("PostgresScanExec has one partition, got {partition}");
        }
        let (url, query, columns, schema) = (
            self.url.clone(),
            self.query.clone(),
            self.columns.clone(),
            self.schema.clone(),
        );
        // Rows stream from the socket in `batch_size` chunks: at most one
        // chunk of decoded rows exists outside the pool; every RecordBatch is
        // reserved by the `AccountedExec` above this node.
        let batch_size = ctx.session_config().batch_size().max(1);
        let rows = async move {
            let client = connect(&url.0).await?;
            let stream = client
                .query_raw(&query.sql, query.params.iter().map(|p| p.as_sql()))
                .await
                .map_err(ConnectorError::from)?;
            // The client owns the connection: keep it alive with the stream.
            Ok::<_, DataFusionError>(stream.map(move |row| {
                let _connection = &client;
                row.map_err(|e| DataFusionError::from(ConnectorError::from(e)))
            }))
        };
        let batches = futures::stream::once(rows)
            .try_flatten()
            .try_chunks(batch_size)
            .map(move |chunk: Result<Vec<Row>, _>| match chunk {
                Ok(rows) => Ok(rows_to_batch(&rows, &columns, schema.clone())?),
                Err(e) => Err(e.1),
            });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            batches,
        )))
    }
}

fn get<'a, T: FromSql<'a>>(rows: &'a [Row], i: usize) -> Result<Vec<Option<T>>, ConnectorError> {
    Ok(rows
        .iter()
        .map(|r| r.try_get(i))
        .collect::<Result<_, _>>()?)
}

/// Days between 0001-01-01 (CE day 1) and 1970-01-01.
const UNIX_EPOCH_DAYS_FROM_CE: i32 = 719_163;

fn rows_to_batch(
    rows: &[Row],
    columns: &[PgColumn],
    schema: SchemaRef,
) -> Result<RecordBatch, ConnectorError> {
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(columns.len());
    for (i, c) in columns.iter().enumerate() {
        let a: ArrayRef = match c.kind {
            PgKind::Int2 => Arc::new(Int16Array::from(get::<i16>(rows, i)?)),
            PgKind::Int4 => Arc::new(Int32Array::from(get::<i32>(rows, i)?)),
            PgKind::Int8 => Arc::new(Int64Array::from(get::<i64>(rows, i)?)),
            PgKind::Float4 => Arc::new(Float32Array::from(get::<f32>(rows, i)?)),
            PgKind::Float8 => Arc::new(Float64Array::from(get::<f64>(rows, i)?)),
            PgKind::Bool => Arc::new(BooleanArray::from(get::<bool>(rows, i)?)),
            PgKind::Text | PgKind::Numeric => Arc::new(StringArray::from(get::<&str>(rows, i)?)),
            PgKind::Date => Arc::new(Date32Array::from(
                get::<NaiveDate>(rows, i)?
                    .into_iter()
                    .map(|d| d.map(|d| d.num_days_from_ce() - UNIX_EPOCH_DAYS_FROM_CE))
                    .collect::<Vec<_>>(),
            )),
            PgKind::Timestamp => Arc::new(TimestampMicrosecondArray::from(
                get::<NaiveDateTime>(rows, i)?
                    .into_iter()
                    .map(|t| t.map(|t| t.and_utc().timestamp_micros()))
                    .collect::<Vec<_>>(),
            )),
            PgKind::Timestamptz => Arc::new(
                TimestampMicrosecondArray::from(
                    get::<DateTime<Utc>>(rows, i)?
                        .into_iter()
                        .map(|t| t.map(|t| t.timestamp_micros()))
                        .collect::<Vec<_>>(),
                )
                .with_timezone("UTC"),
            ),
        };
        arrays.push(a);
    }
    let opts = RecordBatchOptions::new().with_row_count(Some(rows.len()));
    Ok(RecordBatch::try_new_with_options(schema, arrays, &opts)?)
}

/// Postgres table exposed as a guarded provider.
#[derive(Debug)]
pub struct PostgresConnector {
    name: String,
    table: Arc<PostgresTable>,
    policy: PushdownPolicy,
}

impl PostgresConnector {
    /// Reads the connection string from [`PG_URL_ENV`] (fails fast if unset),
    /// introspects `pg_schema.pg_table` and fails fast on unsupported types.
    pub async fn from_env(
        name: impl Into<String>,
        pg_schema: &str,
        pg_table: &str,
    ) -> Result<Self, ConnectorError> {
        let url = std::env::var(PG_URL_ENV).map_err(|_| ConnectorError::MissingEnv(PG_URL_ENV))?;
        let table = PostgresTable::connect(&url, pg_schema, pg_table).await?;
        Ok(Self::from_table(name, table))
    }

    fn from_table(name: impl Into<String>, table: PostgresTable) -> Self {
        Self {
            name: name.into(),
            table: Arc::new(table),
            policy: Self::default_policy(),
        }
    }

    /// Integer/boolean comparisons and null checks are Exact (identical
    /// semantics); text, floats, LIKE, temporal and the rest stay local.
    /// Floats are not even Inexact: Postgres treats -0.0 = 0.0 while Arrow uses
    /// IEEE total order, so the remote could drop rows the engine would keep.
    pub fn default_policy() -> PushdownPolicy {
        PushdownPolicy {
            integer_cmp: Pushdown::Exact,
            bool_cmp: Pushdown::Exact,
            null_check: Pushdown::Exact,
            limit: true,
            ..PushdownPolicy::local_only()
        }
    }

    pub fn with_policy(mut self, policy: PushdownPolicy) -> Self {
        self.policy = policy;
        self
    }
}

impl Connector for PostgresConnector {
    fn name(&self) -> &str {
        &self.name
    }
    fn policy(&self) -> &PushdownPolicy {
        &self.policy
    }
    fn source(&self) -> Arc<dyn TableProvider> {
        self.table.clone()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use datafusion::physical_plan::displayable;
    use datafusion::prelude::{DataFrame, SessionContext, col, lit};

    /// Planning never touches the network, so no live Postgres is needed.
    fn ctx() -> SessionContext {
        let table = PostgresTable::new(
            "postgres://unused",
            "public",
            "users",
            vec![
                PgColumn::new("id", PgKind::Int4),
                PgColumn::new("name", PgKind::Text),
                PgColumn::new("score", PgKind::Float8),
            ],
        );
        let ctx = SessionContext::new();
        PostgresConnector::from_table("users", table)
            .register(&ctx)
            .unwrap();
        ctx
    }

    async fn plan(df: DataFrame) -> String {
        let p = df.create_physical_plan().await.unwrap();
        displayable(p.as_ref()).indent(false).to_string()
    }

    #[tokio::test]
    async fn pg_federates_integer_comparison() {
        let ctx = ctx();
        let df = ctx.table("users").await.unwrap();
        let df = df
            .filter(col("id").gt(lit(10i32)))
            .unwrap()
            .select_columns(&["name"])
            .unwrap();
        let shown = plan(df).await;
        assert!(
            shown.contains(
                "PostgresScanExec: sql=SELECT \"name\" FROM \"public\".\"users\" WHERE (\"id\" > $1::int8)"
            ),
            "{shown}"
        );
        assert!(!shown.contains("FilterExec"), "{shown}");
    }

    #[tokio::test]
    async fn pg_text_like_and_float_stay_local() {
        let ctx = ctx();
        let df = ctx.table("users").await.unwrap();
        let df = df
            .filter(
                col("name")
                    .like(lit("a%"))
                    .and(col("name").eq(lit("bob")))
                    .and(col("score").gt(lit(1.5)))
                    .and(col("id").gt_eq(lit(3i32))),
            )
            .unwrap();
        let shown = plan(df).await;
        let scan = shown
            .lines()
            .find(|l| l.contains("PostgresScanExec"))
            .unwrap();
        assert!(scan.contains("WHERE (\"id\" >= $1::int8)"), "{scan}");
        for local in ["LIKE", "name", "score"] {
            assert!(
                !scan.split("FROM").nth(1).unwrap().contains(local),
                "{scan}"
            );
        }
        let filter = shown.lines().find(|l| l.contains("FilterExec")).unwrap();
        assert!(filter.contains("LIKE a%"), "{filter}");
        assert!(filter.contains("score@2 > 1.5"), "{filter}");
    }

    #[tokio::test]
    async fn pg_from_env_fails_fast_without_url() {
        if std::env::var(PG_URL_ENV).is_ok() {
            return;
        }
        let err = PostgresConnector::from_env("t", "public", "t")
            .await
            .unwrap_err();
        assert!(matches!(err, ConnectorError::MissingEnv(PG_URL_ENV)));
    }

    /// Live: results stream in `batch_size` chunks, each reserved in the
    /// pool; a ceiling too small for one batch fails the query, not the node.
    #[tokio::test]
    #[ignore = "needs a live Postgres in PLIMSOLL_PG_URL"]
    async fn pg_streams_in_accounted_batches() {
        use datafusion::error::DataFusionError;
        use datafusion::prelude::SessionConfig;
        use plimsoll_core::memory::{CeilingPool, session_context_with_ceiling};

        let Ok(url) = std::env::var(PG_URL_ENV) else {
            return;
        };
        connect(&url)
            .await
            .unwrap()
            .batch_execute(
                "DROP TABLE IF EXISTS plimsoll_big;
                 CREATE TABLE plimsoll_big AS
                   SELECT g::int8 AS id, repeat('x', 100) AS pad FROM generate_series(1, 20000) g;",
            )
            .await
            .unwrap();
        let run = async |limit: usize| {
            let pool = Arc::new(CeilingPool::new(limit, 0).unwrap());
            // One partition: no exchange above the scan, so the scan's own
            // reservation is what meets the ceiling.
            let config = SessionConfig::new()
                .with_batch_size(1_000)
                .with_target_partitions(1);
            let ctx = session_context_with_ceiling(pool.clone(), None, config).unwrap();
            PostgresConnector::from_env("big", "public", "plimsoll_big")
                .await
                .unwrap()
                .register(&ctx)
                .unwrap();
            let out = ctx.table("big").await.unwrap().collect().await;
            assert_eq!(pool.used(), 0, "released (limit {limit})");
            out
        };
        let batches = run(64 << 20).await.unwrap();
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 20_000);
        assert!(
            batches.len() >= 20,
            "streamed, not one batch: {}",
            batches.len()
        );

        let err = run(32 << 10).await.unwrap_err();
        assert!(
            matches!(err.find_root(), DataFusionError::ResourcesExhausted(m) if m.contains("PostgresScan")),
            "{err}"
        );
    }

    /// Live round trip. Run with `PLIMSOLL_PG_URL=... cargo test -p plimsoll-connectors -- --ignored`.
    #[tokio::test]
    #[ignore = "needs a live Postgres in PLIMSOLL_PG_URL"]
    async fn pg_live_roundtrip() {
        let Ok(url) = std::env::var(PG_URL_ENV) else {
            return;
        };
        let client = connect(&url).await.unwrap();
        client
            .batch_execute(
                "DROP TABLE IF EXISTS plimsoll_it;
                 CREATE TABLE plimsoll_it (id int4, name text, amount numeric,
                   d date, ts timestamptz, ok bool, f float8);
                 INSERT INTO plimsoll_it VALUES
                   (1, 'a', 1.50, '2024-01-02', '2024-01-02T03:04:05Z', true, 0.5),
                   (2, NULL, NULL, NULL, NULL, NULL, NULL),
                   (3, 'c', 3.25, '1969-12-31', '1970-01-01T00:00:00Z', false, -1);",
            )
            .await
            .unwrap();
        let ctx = SessionContext::new();
        PostgresConnector::from_env("it", "public", "plimsoll_it")
            .await
            .unwrap()
            .register(&ctx)
            .unwrap();
        let batches = ctx
            .table("it")
            .await
            .unwrap()
            .filter(col("id").gt_eq(lit(2i32)))
            .unwrap()
            .collect()
            .await
            .unwrap();
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 2);

        let b = &ctx
            .table("it")
            .await
            .unwrap()
            .filter(col("id").eq(lit(3i32)))
            .unwrap()
            .select_columns(&["amount", "d", "ts"])
            .unwrap()
            .collect()
            .await
            .unwrap()[0];
        let amount = b.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        let d = b.column(1).as_any().downcast_ref::<Date32Array>().unwrap();
        let ts = b
            .column(2)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        assert_eq!((amount.value(0), d.value(0), ts.value(0)), ("3.25", -1, 0));
        client
            .batch_execute("DROP TABLE plimsoll_it")
            .await
            .unwrap();
    }
}

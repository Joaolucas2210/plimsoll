//! `CREATE TABLE` through the query API (ADR 0083).
//!
//! DataFusion would create an in-memory table; Plimsoll instead hands the
//! statement's schema, partitioning and properties to a [`TableCreator`]
//! (the server's REST catalog), under the same write grant as `INSERT`.
//! Nothing else DDL is accepted.

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::ResolvedTableReference;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::{DdlStatement, LogicalPlan};
use datafusion::prelude::SessionContext;
use datafusion::sql::parser::Statement as DFStatement;
use datafusion::sql::sqlparser::ast::{
    CreateTable, CreateTableOptions, Expr, FunctionArg, FunctionArgExpr, FunctionArguments,
    SqlOption, Statement, Value,
};

/// A table to create, as `CREATE TABLE` described it.
#[derive(Debug, Clone)]
pub struct NewTable {
    pub schema: SchemaRef,
    /// `(transform, column)` in Iceberg's spelling: `identity`,
    /// `bucket[16]`, `truncate[4]`, `year`, `month`, `day`, `hour`.
    pub partition: Vec<(String, String)>,
    /// `WITH (k = 'v')` or `TBLPROPERTIES (…)` before `PARTITION BY`, or
    /// `OPTIONS (…)` after it.
    pub properties: Vec<(String, String)>,
    pub if_not_exists: bool,
}

/// Creates tables in a catalog; installed as a session config extension
/// ([`Creator`]) by whoever serves catalog-backed schemas.
#[async_trait]
pub trait TableCreator: Send + Sync {
    async fn create_table(&self, table: &ResolvedTableReference, new: NewTable) -> Result<()>;
}

/// The session config extension carrying the [`TableCreator`].
pub struct Creator(pub Arc<dyn TableCreator>);

/// Whether `sql` is a `CREATE …` statement (by its first word).
pub(crate) fn is_create(sql: &str) -> bool {
    sql.trim_start()
        .get(..7)
        .is_some_and(|w| w.eq_ignore_ascii_case("create "))
}

const ONLY_CREATE_TABLE: &str =
    "of DDL, only CREATE TABLE is allowed; the query API is otherwise read-only";

fn refuse<T>(why: impl Into<String>) -> Result<T> {
    Err(DataFusionError::Plan(why.into()))
}

/// Plans `sql`, which must be a plain `CREATE TABLE`: the table it names
/// (resolved) and what to create. Executes nothing.
pub(crate) async fn plan(
    ctx: &SessionContext,
    sql: &str,
) -> Result<(ResolvedTableReference, NewTable)> {
    let state = ctx.state();
    let dialect = state.config_options().sql_parser.dialect;
    let DFStatement::Statement(statement) = state.sql_to_statement(sql, &dialect)? else {
        return refuse(ONLY_CREATE_TABLE);
    };
    let Statement::CreateTable(mut create) = *statement else {
        return refuse(ONLY_CREATE_TABLE);
    };
    if create.query.is_some() {
        // ponytail: CREATE TABLE, then INSERT INTO … SELECT.
        return refuse("CREATE TABLE … AS SELECT is not supported: create, then INSERT INTO");
    }
    let partition = match create.partition_by.take() {
        None => Vec::new(),
        Some(e) => match *e {
            Expr::Tuple(fields) => fields.iter().map(partition_field).collect::<Result<_>>()?,
            Expr::Nested(e) => vec![partition_field(&e)?],
            e => vec![partition_field(&e)?],
        },
    };
    let properties = properties(std::mem::replace(
        &mut create.table_options,
        CreateTableOptions::None,
    ))?;
    let CreateTable { or_replace, .. } = create;
    if or_replace {
        return refuse("CREATE OR REPLACE TABLE is not supported");
    }
    let plan = state
        .statement_to_plan(DFStatement::Statement(Box::new(Statement::CreateTable(
            create,
        ))))
        .await?;
    let LogicalPlan::Ddl(DdlStatement::CreateMemoryTable(c)) = plan else {
        return refuse(ONLY_CREATE_TABLE);
    };
    if !c.constraints.is_empty() || !c.column_defaults.is_empty() || c.temporary {
        return refuse("CREATE TABLE takes column names, types and NOT NULL only");
    }
    let options = state.config_options();
    let table = c.name.resolve(
        &options.catalog.default_catalog,
        &options.catalog.default_schema,
    );
    // Only into a schema the engine serves (catalogs may not check).
    if ctx
        .catalog(&table.catalog)
        .and_then(|c| c.schema(&table.schema))
        .is_none()
    {
        return refuse(format!("no schema {}.{}", table.catalog, table.schema));
    }
    let new = NewTable {
        schema: Arc::new(c.input.schema().as_arrow().clone()),
        partition,
        properties,
        if_not_exists: c.if_not_exists,
    };
    Ok((table, new))
}

/// `region`, `bucket(16, id)`, `truncate(4, name)`, `day(ts)` (or Spark's
/// `days(ts)`).
fn partition_field(e: &Expr) -> Result<(String, String)> {
    let column = |e: &Expr| match e {
        Expr::Identifier(i) if i.quote_style.is_some() => Ok(i.value.clone()),
        Expr::Identifier(i) => Ok(i.value.to_lowercase()),
        _ => refuse(format!("PARTITION BY expects a column, got {e}")),
    };
    let Expr::Function(f) = e else {
        return Ok(("identity".into(), column(e)?));
    };
    let FunctionArguments::List(list) = &f.args else {
        return refuse(format!("unsupported partition transform {e}"));
    };
    let args = list
        .args
        .iter()
        .map(|a| match a {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => Ok(e),
            _ => refuse(format!("unsupported partition transform {e}")),
        })
        .collect::<Result<Vec<_>>>()?;
    let name = f.name.to_string().to_lowercase();
    match (name.as_str(), args.as_slice()) {
        ("bucket" | "truncate", [Expr::Value(n), c]) => match &n.value {
            Value::Number(n, _) if n.parse::<u32>().is_ok_and(|n| n > 0) => {
                Ok((format!("{name}[{n}]"), column(c)?))
            }
            _ => refuse(format!("{name} needs a positive width, got {n}")),
        },
        ("year" | "years" | "month" | "months" | "day" | "days" | "hour" | "hours", [c]) => {
            Ok((name.trim_end_matches('s').to_owned(), column(c)?))
        }
        ("identity", [c]) => Ok(("identity".into(), column(c)?)),
        _ => refuse(format!(
            "unsupported partition transform {e}: use a column, bucket(n, c), truncate(w, c), \
             year(c), month(c), day(c) or hour(c)"
        )),
    }
}

fn properties(options: CreateTableOptions) -> Result<Vec<(String, String)>> {
    let options = match options {
        CreateTableOptions::None => return Ok(Vec::new()),
        CreateTableOptions::With(o)
        | CreateTableOptions::TableProperties(o)
        | CreateTableOptions::Options(o) => o,
        _ => return refuse("table properties go in WITH (…), TBLPROPERTIES (…) or OPTIONS (…)"),
    };
    options
        .into_iter()
        .map(|o| match o {
            SqlOption::KeyValue {
                key,
                value: Expr::Value(v),
            } => match v.value {
                Value::SingleQuotedString(s) | Value::DoubleQuotedString(s) => Ok((key.value, s)),
                Value::Number(n, _) => Ok((key.value, n)),
                Value::Boolean(b) => Ok((key.value, b.to_string())),
                v => refuse(format!("property {key}: unsupported value {v}")),
            },
            o => refuse(format!("unsupported table option {o}")),
        })
        .collect()
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn plans_columns_partitions_and_properties_and_refuses_the_rest() {
        let ctx = SessionContext::new();
        ctx.catalog("datafusion")
            .unwrap()
            .register_schema(
                "bronze",
                Arc::new(datafusion::catalog::MemorySchemaProvider::new()),
            )
            .unwrap();
        let (table, new) = plan(
            &ctx,
            "CREATE TABLE IF NOT EXISTS bronze.t (id BIGINT NOT NULL, region VARCHAR, ts TIMESTAMP) \
             PARTITION BY (region, bucket(16, id), days(ts)) OPTIONS ('write.format' = 'parquet', n = 3)",
        )
        .await
        .unwrap();
        assert_eq!(table.to_string(), "datafusion.bronze.t");
        assert!(new.if_not_exists);
        assert!(!new.schema.field(0).is_nullable() && new.schema.field(1).is_nullable());
        assert_eq!(
            new.partition,
            [
                ("identity".into(), "region".into()),
                ("bucket[16]".into(), "id".into()),
                ("day".into(), "ts".into()),
            ]
        );
        assert_eq!(
            new.properties,
            [
                ("write.format".into(), "parquet".into()),
                ("n".into(), "3".into())
            ]
        );
        let (_, single) = plan(
            &ctx,
            "CREATE TABLE t (r VARCHAR) WITH (a = 'b') PARTITION BY r",
        )
        .await
        .unwrap();
        assert_eq!(single.partition, [("identity".into(), "r".into())]);
        assert_eq!(single.properties, [("a".into(), "b".into())]);
        for sql in [
            "CREATE TABLE t AS SELECT 1 AS x",
            "CREATE OR REPLACE TABLE t (x INT)",
            "CREATE TABLE t (x INT) PARTITION BY (sqrt(x))",
            "CREATE TABLE t (x INT) PARTITION BY (bucket(0, x))",
            "CREATE VIEW v AS SELECT 1",
            "CREATE EXTERNAL TABLE t STORED AS CSV LOCATION '/etc/passwd'",
            "CREATE TABLE t (x INT PRIMARY KEY)",
            "CREATE TABLE nowhere.t (x INT)",
        ] {
            assert!(plan(&ctx, sql).await.is_err(), "{sql}");
        }
        assert!(is_create("  create table t (x int)") && !is_create("SELECT 1"));
    }
}

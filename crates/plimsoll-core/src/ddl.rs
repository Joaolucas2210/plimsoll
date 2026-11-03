//! `CREATE TABLE` and `ALTER TABLE` through the query API (ADR 0083,
//! 0084).
//!
//! DataFusion would create an in-memory table; Plimsoll instead hands the
//! statement's schema, partitioning and properties, or the changes to a
//! table's schema, to a [`TableDdl`] (the server's REST catalog), under
//! the same write grant as `INSERT`. Nothing else DDL is accepted.

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, Field, SchemaRef};
use datafusion::common::{ResolvedTableReference, TableReference};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::{DdlStatement, LogicalPlan};
use datafusion::prelude::SessionContext;
use datafusion::sql::parser::Statement as DFStatement;
use datafusion::sql::sqlparser::ast::helpers::stmt_create_table::CreateTableBuilder;
use datafusion::sql::sqlparser::ast::{
    AlterColumnOperation, AlterTable, AlterTableOperation, ColumnDef, CreateTable,
    CreateTableOptions, Expr, FunctionArg, FunctionArgExpr, FunctionArguments, Ident, ObjectName,
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

/// One change of `ALTER TABLE` (ADR 0084). Column names as the table
/// spells them.
#[derive(Debug, Clone, PartialEq)]
pub enum Alteration {
    /// Always nullable: Iceberg v2 cannot add a required column.
    AddColumn {
        field: Field,
        if_not_exists: bool,
    },
    DropColumn {
        name: String,
        if_exists: bool,
    },
    RenameColumn {
        from: String,
        to: String,
    },
    DropNotNull(String),
    /// A promotion Iceberg allows: `int` to `long`, `float` to `double`,
    /// a wider decimal.
    SetType {
        column: String,
        data_type: DataType,
    },
    SetProperties(Vec<(String, String)>),
}

impl Alteration {
    /// The existing column this change reads or rewrites, if any.
    pub fn column(&self) -> Option<&str> {
        match self {
            Self::DropColumn { name, .. } => Some(name),
            Self::RenameColumn { from, .. } => Some(from),
            Self::DropNotNull(c) | Self::SetType { column: c, .. } => Some(c),
            Self::AddColumn { .. } | Self::SetProperties(_) => None,
        }
    }
}

/// Creates and alters tables in a catalog; installed as a session config
/// extension ([`Ddl`]) by whoever serves catalog-backed schemas.
#[async_trait]
pub trait TableDdl: Send + Sync {
    async fn create_table(&self, table: &ResolvedTableReference, new: NewTable) -> Result<()>;

    /// Applies `changes` in order, as one commit. `if_exists`: a missing
    /// table is not an error.
    async fn alter_table(
        &self,
        table: &ResolvedTableReference,
        changes: Vec<Alteration>,
        if_exists: bool,
    ) -> Result<()>;
}

/// The session config extension carrying the [`TableDdl`].
pub struct Ddl(pub Arc<dyn TableDdl>);

/// A planned DDL statement.
#[derive(Debug)]
pub(crate) enum Planned {
    Create(ResolvedTableReference, NewTable),
    Alter {
        table: ResolvedTableReference,
        changes: Vec<Alteration>,
        if_exists: bool,
    },
}

/// Whether `sql` is a `CREATE …` or `ALTER …` statement (by its first
/// word).
pub(crate) fn is_ddl(sql: &str) -> bool {
    let sql = sql.trim_start();
    ["create ", "alter "].iter().any(|w| {
        sql.get(..w.len())
            .is_some_and(|s| s.eq_ignore_ascii_case(w))
    })
}

const ONLY_CREATE_TABLE: &str = "of DDL, only CREATE TABLE and ALTER TABLE are allowed; \
     the query API is otherwise read-only";

fn refuse<T>(why: impl Into<String>) -> Result<T> {
    Err(DataFusionError::Plan(why.into()))
}

/// Plans `sql`, which must be a plain `CREATE TABLE` or an `ALTER TABLE`
/// of columns and properties. Executes nothing.
pub(crate) async fn plan(ctx: &SessionContext, sql: &str) -> Result<Planned> {
    let state = ctx.state();
    let dialect = state.config_options().sql_parser.dialect;
    let DFStatement::Statement(statement) = state.sql_to_statement(sql, &dialect)? else {
        return refuse(ONLY_CREATE_TABLE);
    };
    match *statement {
        Statement::CreateTable(create) => {
            let (table, new) = plan_create(ctx, create).await?;
            Ok(Planned::Create(table, new))
        }
        Statement::AlterTable(alter) => plan_alter(ctx, alter).await,
        _ => refuse(ONLY_CREATE_TABLE),
    }
}

async fn plan_create(
    ctx: &SessionContext,
    mut create: CreateTable,
) -> Result<(ResolvedTableReference, NewTable)> {
    let state = ctx.state();
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
    let table = served(ctx, c.name)?;
    let new = NewTable {
        schema: Arc::new(c.input.schema().as_arrow().clone()),
        partition,
        properties,
        if_not_exists: c.if_not_exists,
    };
    Ok((table, new))
}

/// `name` resolved, in a schema the engine serves (catalogs may not
/// check).
fn served(ctx: &SessionContext, name: TableReference) -> Result<ResolvedTableReference> {
    let state = ctx.state();
    let options = state.config_options();
    let table = name.resolve(
        &options.catalog.default_catalog,
        &options.catalog.default_schema,
    );
    if ctx
        .catalog(&table.catalog)
        .and_then(|c| c.schema(&table.schema))
        .is_none()
    {
        return refuse(format!("no schema {}.{}", table.catalog, table.schema));
    }
    Ok(table)
}

/// `ALTER TABLE t ADD COLUMN …, DROP COLUMN …, RENAME COLUMN a TO b,
/// ALTER COLUMN c DROP NOT NULL | SET DATA TYPE …, SET TBLPROPERTIES (…)`.
async fn plan_alter(ctx: &SessionContext, alter: AlterTable) -> Result<Planned> {
    let name = |i: Ident| match i.quote_style {
        Some(_) => i.value,
        None => i.value.to_lowercase(),
    };
    let mut changes = Vec::new();
    for op in alter.operations {
        changes.push(match op {
            AlterTableOperation::AddColumn {
                if_not_exists,
                column_def,
                column_position: None,
                ..
            } => {
                let field = field(ctx, column_def).await?;
                if !field.is_nullable() {
                    return refuse("ADD COLUMN … NOT NULL: Iceberg v2 adds nullable columns only");
                }
                Alteration::AddColumn {
                    field,
                    if_not_exists,
                }
            }
            AlterTableOperation::DropColumn {
                column_names,
                if_exists,
                drop_behavior: None,
                ..
            } => {
                for c in column_names {
                    changes.push(Alteration::DropColumn {
                        name: name(c),
                        if_exists,
                    });
                }
                continue;
            }
            AlterTableOperation::RenameColumn {
                old_column_name,
                new_column_name,
            } => Alteration::RenameColumn {
                from: name(old_column_name),
                to: name(new_column_name),
            },
            AlterTableOperation::AlterColumn { column_name, op } => match op {
                AlterColumnOperation::DropNotNull => Alteration::DropNotNull(name(column_name)),
                AlterColumnOperation::SetDataType {
                    data_type,
                    using: None,
                    ..
                } => {
                    let column = name(column_name);
                    let def = ColumnDef {
                        name: Ident::new(&column),
                        data_type,
                        options: Vec::new(),
                    };
                    Alteration::SetType {
                        data_type: field(ctx, def).await?.data_type().clone(),
                        column,
                    }
                }
                op => return refuse(format!("unsupported ALTER COLUMN {op}")),
            },
            AlterTableOperation::SetTblProperties { table_properties } => {
                Alteration::SetProperties(properties(CreateTableOptions::TableProperties(
                    table_properties,
                ))?)
            }
            op => {
                return refuse(format!(
                    "unsupported ALTER TABLE operation {op} (supported: ADD COLUMN, \
                     DROP COLUMN, RENAME COLUMN, ALTER COLUMN … DROP NOT NULL | SET DATA \
                     TYPE, SET TBLPROPERTIES)"
                ));
            }
        });
    }
    if changes.is_empty() {
        return refuse("ALTER TABLE without a change");
    }
    let table = served(ctx, TableReference::from(alter.name.to_string().as_str()))?;
    Ok(Planned::Alter {
        table,
        changes,
        if_exists: alter.if_exists,
    })
}

/// A column definition as DataFusion's `CREATE TABLE` would type it.
async fn field(ctx: &SessionContext, def: ColumnDef) -> Result<Field> {
    let create = CreateTableBuilder::new(ObjectName::from(vec![Ident::new("t")]))
        .columns(vec![def])
        .build();
    let plan = ctx
        .state()
        .statement_to_plan(DFStatement::Statement(Box::new(Statement::CreateTable(
            create,
        ))))
        .await?;
    let LogicalPlan::Ddl(DdlStatement::CreateMemoryTable(c)) = plan else {
        return refuse("unsupported column definition");
    };
    if !c.constraints.is_empty() || !c.column_defaults.is_empty() {
        return refuse("a column takes a name, a type and NOT NULL only");
    }
    Ok(c.input.schema().field(0).as_ref().clone())
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

    async fn create(ctx: &SessionContext, sql: &str) -> Result<(ResolvedTableReference, NewTable)> {
        match plan(ctx, sql).await? {
            Planned::Create(table, new) => Ok((table, new)),
            p => panic!("{p:?}"),
        }
    }

    fn served() -> SessionContext {
        let ctx = SessionContext::new();
        ctx.catalog("datafusion")
            .unwrap()
            .register_schema(
                "bronze",
                Arc::new(datafusion::catalog::MemorySchemaProvider::new()),
            )
            .unwrap();
        ctx
    }

    #[tokio::test]
    async fn plans_columns_partitions_and_properties_and_refuses_the_rest() {
        let ctx = served();
        let (table, new) = create(
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
        let (_, single) = create(
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
        assert!(
            is_ddl("  create table t (x int)") && is_ddl("ALTER table t") && !is_ddl("SELECT 1")
        );
    }

    #[tokio::test]
    async fn plans_column_and_property_changes_and_refuses_the_rest() {
        let ctx = served();
        let Planned::Alter {
            table,
            changes,
            if_exists,
        } = plan(
            &ctx,
            "ALTER TABLE IF EXISTS bronze.t ADD COLUMN IF NOT EXISTS Note VARCHAR, \
             DROP COLUMN a, DROP COLUMN IF EXISTS \"B\", RENAME COLUMN c TO d, \
             ALTER COLUMN n SET DATA TYPE BIGINT, ALTER COLUMN m DROP NOT NULL, \
             SET TBLPROPERTIES ('k' = 'v')",
        )
        .await
        .unwrap()
        else {
            panic!()
        };
        assert_eq!(table.to_string(), "datafusion.bronze.t");
        assert!(if_exists);
        assert_eq!(
            changes,
            [
                Alteration::AddColumn {
                    field: Field::new("note", DataType::Utf8View, true),
                    if_not_exists: true
                },
                Alteration::DropColumn {
                    name: "a".into(),
                    if_exists: false
                },
                Alteration::DropColumn {
                    name: "B".into(),
                    if_exists: true
                },
                Alteration::RenameColumn {
                    from: "c".into(),
                    to: "d".into()
                },
                Alteration::SetType {
                    column: "n".into(),
                    data_type: DataType::Int64
                },
                Alteration::DropNotNull("m".into()),
                Alteration::SetProperties(vec![("k".into(), "v".into())]),
            ]
        );
        for sql in [
            "ALTER TABLE bronze.t ADD COLUMN x INT NOT NULL",
            "ALTER TABLE bronze.t RENAME TO u",
            "ALTER TABLE bronze.t ALTER COLUMN x SET NOT NULL",
            "ALTER TABLE bronze.t ALTER COLUMN x SET DEFAULT 1",
            "ALTER TABLE nowhere.t ADD COLUMN x INT",
            "ALTER VIEW v AS SELECT 1",
        ] {
            assert!(plan(&ctx, sql).await.is_err(), "{sql}");
        }
    }
}

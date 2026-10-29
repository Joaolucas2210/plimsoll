//! Filter -> Postgres SQL translation. Identifiers are always double-quoted;
//! literals are always bind parameters (`$n::type`), never inlined.

use datafusion::common::ScalarValue;
use datafusion::logical_expr::{BinaryExpr, Expr, Operator};
use tokio_postgres::types::ToSql;

use super::{PgColumn, PgKind};

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum PgParam {
    Int(i64),
    Float(f64),
    Bool(bool),
    Text(String),
}

impl PgParam {
    pub(crate) fn as_sql(&self) -> &(dyn ToSql + Sync) {
        match self {
            PgParam::Int(v) => v,
            PgParam::Float(v) => v,
            PgParam::Bool(v) => v,
            PgParam::Text(v) => v,
        }
    }

    /// Explicit cast so the bound Rust type always matches the parameter type
    /// Postgres infers (e.g. an i64 compared against an int4 column).
    fn cast(&self) -> &'static str {
        match self {
            PgParam::Int(_) => "int8",
            PgParam::Float(_) => "float8",
            PgParam::Bool(_) => "bool",
            PgParam::Text(_) => "text",
        }
    }

    fn from_scalar(v: &ScalarValue) -> Option<Self> {
        use ScalarValue::*;
        Some(match v {
            Int8(Some(x)) => PgParam::Int((*x).into()),
            Int16(Some(x)) => PgParam::Int((*x).into()),
            Int32(Some(x)) => PgParam::Int((*x).into()),
            Int64(Some(x)) => PgParam::Int(*x),
            UInt8(Some(x)) => PgParam::Int((*x).into()),
            UInt16(Some(x)) => PgParam::Int((*x).into()),
            UInt32(Some(x)) => PgParam::Int((*x).into()),
            UInt64(Some(x)) => PgParam::Int(i64::try_from(*x).ok()?),
            Float32(Some(x)) => PgParam::Float((*x).into()),
            Float64(Some(x)) => PgParam::Float(*x),
            Boolean(Some(x)) => PgParam::Bool(*x),
            Utf8(Some(s)) | LargeUtf8(Some(s)) | Utf8View(Some(s)) => PgParam::Text(s.clone()),
            _ => return None,
        })
    }
}

#[derive(Debug, Clone)]
pub(crate) struct Query {
    pub sql: String,
    pub params: Vec<PgParam>,
}

pub(crate) fn quote_ident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

/// Translate one filter, appending its literals to `params`. `None` means the
/// translator cannot express it; `params` may then contain garbage and must be
/// discarded by the caller.
pub(crate) fn filter_to_sql(expr: &Expr, params: &mut Vec<PgParam>) -> Option<String> {
    match expr {
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
            let op_sql = match op {
                Operator::And => "AND",
                Operator::Or => "OR",
                Operator::Eq => "=",
                Operator::NotEq => "<>",
                Operator::Lt => "<",
                Operator::LtEq => "<=",
                Operator::Gt => ">",
                Operator::GtEq => ">=",
                _ => return None,
            };
            if matches!(op, Operator::And | Operator::Or) {
                let l = filter_to_sql(left, params)?;
                let r = filter_to_sql(right, params)?;
                return Some(format!("({l} {op_sql} {r})"));
            }
            let l = operand(left, params)?;
            let r = operand(right, params)?;
            Some(format!("({l} {op_sql} {r})"))
        }
        Expr::Not(e) => Some(format!("(NOT {})", filter_to_sql(e, params)?)),
        Expr::IsNull(e) => match e.as_ref() {
            Expr::Column(c) => Some(format!("({} IS NULL)", quote_ident(&c.name))),
            _ => None,
        },
        Expr::IsNotNull(e) => match e.as_ref() {
            Expr::Column(c) => Some(format!("({} IS NOT NULL)", quote_ident(&c.name))),
            _ => None,
        },
        _ => None,
    }
}

fn operand(expr: &Expr, params: &mut Vec<PgParam>) -> Option<String> {
    match expr {
        Expr::Column(c) => Some(quote_ident(&c.name)),
        Expr::Literal(v, _) => {
            let p = PgParam::from_scalar(v)?;
            let cast = p.cast();
            params.push(p);
            Some(format!("${}::{cast}", params.len()))
        }
        _ => None,
    }
}

/// `columns` are the projected columns, in output order. Filters the
/// translator cannot express are skipped (the guard never marks those Exact).
pub(crate) fn build_query(
    relation: &str,
    columns: &[&PgColumn],
    filters: &[Expr],
    limit: Option<usize>,
) -> Query {
    let select = columns
        .iter()
        .map(|c| {
            let q = quote_ident(&c.name);
            match c.kind {
                // ponytail: NUMERIC travels as text and lands as Utf8. Upgrade
                // path: decode to Decimal128 using the column's typmod.
                PgKind::Numeric => format!("{q}::text AS {q}"),
                _ => q,
            }
        })
        .collect::<Vec<_>>()
        .join(", ");
    // `SELECT FROM t` (zero columns) is valid Postgres and yields row counts.
    let mut sql = format!("SELECT {select} FROM {relation}");

    let mut params = Vec::new();
    let mut preds = Vec::new();
    for f in filters {
        let mut local = params.clone();
        if let Some(p) = filter_to_sql(f, &mut local) {
            params = local;
            preds.push(p);
        }
    }
    if !preds.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(&preds.join(" AND "));
    }
    if let Some(n) = limit {
        sql.push_str(&format!(" LIMIT {n}"));
    }
    Query { sql, params }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::{col, lit};

    fn cols() -> Vec<PgColumn> {
        vec![
            PgColumn::new("id", PgKind::Int4),
            PgColumn::new("na\"me", PgKind::Text),
            PgColumn::new("amount", PgKind::Numeric),
        ]
    }

    #[test]
    fn sql_quotes_identifiers() {
        assert_eq!(quote_ident("a\"b"), "\"a\"\"b\"");
        let c = cols();
        let q = build_query("\"public\".\"t\"", &c.iter().collect::<Vec<_>>(), &[], None);
        assert_eq!(
            q.sql,
            "SELECT \"id\", \"na\"\"me\", \"amount\"::text AS \"amount\" FROM \"public\".\"t\""
        );
    }

    #[test]
    fn sql_federated_where_and_limit() {
        let c = cols();
        let filters = [
            col("id").gt(lit(10i32)),
            col("id").lt_eq(lit(99i64)).or(col("id").is_null()),
        ];
        let q = build_query("\"t\"", &[&c[0]], &filters, Some(5));
        assert_eq!(
            q.sql,
            "SELECT \"id\" FROM \"t\" WHERE (\"id\" > $1::int8) AND \
             ((\"id\" <= $2::int8) OR (\"id\" IS NULL)) LIMIT 5"
        );
        assert_eq!(q.params, [PgParam::Int(10), PgParam::Int(99)]);
    }

    #[test]
    fn sql_literals_are_parameters_not_text() {
        let c = cols();
        let evil = "x'; DROP TABLE users; --";
        let q = build_query("\"t\"", &[&c[0]], &[col("na\"me").eq(lit(evil))], None);
        assert!(!q.sql.contains("DROP"), "{}", q.sql);
        assert!(q.sql.contains("(\"na\"\"me\" = $1::text)"), "{}", q.sql);
        assert_eq!(q.params, [PgParam::Text(evil.to_string())]);
    }

    #[test]
    fn sql_untranslatable_filters_are_skipped_without_leaking_params() {
        let c = cols();
        let filters = [
            col("id").gt(lit(1i64)).and(col("na\"me").like(lit("a%"))),
            col("id").eq(lit(7i64)),
        ];
        let q = build_query("\"t\"", &[&c[0]], &filters, None);
        assert_eq!(q.sql, "SELECT \"id\" FROM \"t\" WHERE (\"id\" = $1::int8)");
        assert_eq!(q.params, [PgParam::Int(7)]);
    }
}

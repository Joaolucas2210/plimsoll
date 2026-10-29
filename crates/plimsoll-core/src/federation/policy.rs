//! Per-source pushdown policy: which filter classes a source may evaluate, and
//! whether its answer is trusted (`Exact`) or must be re-checked (`Inexact`).

use datafusion::arrow::datatypes::{DataType, Schema};
use datafusion::logical_expr::{BinaryExpr, Expr, Operator};

use super::Pushdown;

/// Declares, per expression class, what a source may evaluate.
///
/// * `Exact` — the source applies the filter with the same semantics as the
///   engine; the result is trusted.
/// * `Inexact` — the source may apply it (e.g. pruning) but may return extra
///   rows; it is re-applied locally. Only valid when the source can never drop
///   a row the engine would keep.
/// * `Unsupported` — never sent; evaluated locally.
///
/// Compound predicates take the weakest decision of their parts. Anything the
/// classifier does not recognise falls back to `other`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PushdownPolicy {
    /// `col <op> literal` on signed/unsigned integer columns.
    pub integer_cmp: Pushdown,
    /// Floats: NaN / -0.0 ordering differs between engines.
    pub float_cmp: Pushdown,
    pub decimal_cmp: Pushdown,
    /// Date / time / timestamp: timezone and precision semantics differ.
    pub temporal_cmp: Pushdown,
    pub bool_cmp: Pushdown,
    /// Text comparisons are collation-sensitive.
    pub text_cmp: Pushdown,
    /// `LIKE` / `ILIKE` / `SIMILAR TO`: escaping and case-folding differ.
    pub like: Pushdown,
    /// `IS [NOT] NULL` on a plain column.
    pub null_check: Pushdown,
    /// Everything else (functions, casts, IN lists, column-vs-column...).
    pub other: Pushdown,
    /// Whether `LIMIT` may be passed to the source. Only honoured when no filter
    /// needs a local re-check (otherwise the source could cut rows too early).
    pub limit: bool,
}

impl Default for PushdownPolicy {
    fn default() -> Self {
        Self::local_only()
    }
}

impl PushdownPolicy {
    /// Conservative default: nothing is federated, every filter runs locally.
    pub fn local_only() -> Self {
        Self::uniform(Pushdown::Unsupported)
    }

    /// Same decision for every class. `limit` is allowed unless `Unsupported`.
    pub fn uniform(decision: Pushdown) -> Self {
        Self {
            limit: decision != Pushdown::Unsupported,
            integer_cmp: decision.clone(),
            float_cmp: decision.clone(),
            decimal_cmp: decision.clone(),
            temporal_cmp: decision.clone(),
            bool_cmp: decision.clone(),
            text_cmp: decision.clone(),
            like: decision.clone(),
            null_check: decision.clone(),
            other: decision,
        }
    }

    /// Classify one filter against the table schema.
    pub fn classify(&self, expr: &Expr, schema: &Schema) -> Pushdown {
        match expr {
            Expr::BinaryExpr(BinaryExpr { left, op, right }) => match op {
                Operator::And | Operator::Or => {
                    weakest(self.classify(left, schema), self.classify(right, schema))
                }
                Operator::Eq
                | Operator::NotEq
                | Operator::Lt
                | Operator::LtEq
                | Operator::Gt
                | Operator::GtEq => match (left.as_ref(), right.as_ref()) {
                    (Expr::Column(c), Expr::Literal(..)) | (Expr::Literal(..), Expr::Column(c)) => {
                        match schema.field_with_name(&c.name) {
                            Ok(f) => self.cmp_for(f.data_type()),
                            Err(_) => Pushdown::Unsupported,
                        }
                    }
                    _ => self.other.clone(),
                },
                _ => self.other.clone(),
            },
            Expr::Like(_) | Expr::SimilarTo(_) => self.like.clone(),
            Expr::Not(inner) => self.classify(inner, schema),
            Expr::IsNull(inner) | Expr::IsNotNull(inner) if matches!(**inner, Expr::Column(_)) => {
                self.null_check.clone()
            }
            _ => self.other.clone(),
        }
    }

    fn cmp_for(&self, dt: &DataType) -> Pushdown {
        use DataType::*;
        match dt {
            Int8 | Int16 | Int32 | Int64 | UInt8 | UInt16 | UInt32 | UInt64 => {
                self.integer_cmp.clone()
            }
            Float16 | Float32 | Float64 => self.float_cmp.clone(),
            Decimal32(..) | Decimal64(..) | Decimal128(..) | Decimal256(..) => {
                self.decimal_cmp.clone()
            }
            Date32 | Date64 | Time32(_) | Time64(_) | Timestamp(..) => self.temporal_cmp.clone(),
            Boolean => self.bool_cmp.clone(),
            Utf8 | LargeUtf8 | Utf8View => self.text_cmp.clone(),
            _ => self.other.clone(),
        }
    }
}

fn rank(p: &Pushdown) -> u8 {
    match p {
        Pushdown::Unsupported => 0,
        Pushdown::Inexact => 1,
        Pushdown::Exact => 2,
    }
}

/// The weaker of two decisions (`Unsupported < Inexact < Exact`).
pub(crate) fn weakest(a: Pushdown, b: Pushdown) -> Pushdown {
    if rank(&a) <= rank(&b) { a } else { b }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::Field;
    use datafusion::logical_expr::{col, lit};

    fn schema() -> Schema {
        Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
            Field::new("score", DataType::Float64, true),
        ])
    }

    fn pg_like() -> PushdownPolicy {
        PushdownPolicy {
            integer_cmp: Pushdown::Exact,
            null_check: Pushdown::Exact,
            limit: true,
            ..PushdownPolicy::local_only()
        }
    }

    #[test]
    fn federation_default_is_local_only() {
        let p = PushdownPolicy::default();
        assert_eq!(
            p.classify(&col("id").gt(lit(1i64)), &schema()),
            Pushdown::Unsupported
        );
        assert!(!p.limit);
    }

    #[test]
    fn federation_policy_classifies_by_column_type() {
        let p = pg_like();
        let s = schema();
        assert_eq!(p.classify(&col("id").gt(lit(1i64)), &s), Pushdown::Exact);
        assert_eq!(p.classify(&lit(1i64).lt(col("id")), &s), Pushdown::Exact);
        assert_eq!(
            p.classify(&col("name").eq(lit("a")), &s),
            Pushdown::Unsupported
        );
        assert_eq!(
            p.classify(&col("name").like(lit("a%")), &s),
            Pushdown::Unsupported
        );
        assert_eq!(
            p.classify(&col("score").gt(lit(1.0)), &s),
            Pushdown::Unsupported
        );
        assert_eq!(p.classify(&col("id").is_null(), &s), Pushdown::Exact);
    }

    #[test]
    fn federation_compound_takes_weakest() {
        let p = pg_like();
        let s = schema();
        let mixed = col("id").gt(lit(1i64)).and(col("name").eq(lit("a")));
        assert_eq!(p.classify(&mixed, &s), Pushdown::Unsupported);
        let ok = col("id").gt(lit(1i64)).or(col("id").lt(lit(-1i64)));
        assert_eq!(p.classify(&ok, &s), Pushdown::Exact);
    }
}

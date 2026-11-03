//! Row filters the scan enforces itself (ADR 0062), so workers can apply
//! them to their fragments.
//!
//! A filter is *portable* when its SQL text, parsed back against the table
//! schema, means the same expression: unparsed, re-parsed, coerced and
//! simplified, it must equal the original (itself already coerced and
//! simplified by the optimizer). Anything else (UDFs a worker may lack,
//! expressions the unparser renders lossily) stays inexact and DataFusion
//! re-checks it above the scan, as before.

use std::sync::{Arc, LazyLock};

use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::common::{Column, DFSchema};
use datafusion::execution::SessionState;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::logical_expr::Expr;
use datafusion::logical_expr::simplify::SimplifyContext;
use datafusion::optimizer::simplify_expressions::ExprSimplifier;
use datafusion::sql::unparser::expr_to_sql;

/// Built-in functions only: what any worker of this version parses.
static PARSER: LazyLock<SessionState> =
    LazyLock::new(|| SessionStateBuilder::new().with_default_features().build());

/// `filter` with its columns unqualified, and as SQL that parses back to
/// that expression against `schema`; `None` when it does not round-trip.
pub(super) fn sql(filter: &Expr, schema: &DFSchema) -> Option<(Expr, String)> {
    let filter = unqualified(filter)?;
    if filter.is_volatile() {
        return None;
    }
    let text = expr_to_sql(&filter).ok()?.to_string();
    let parsed = PARSER.create_logical_expr(&text, schema).ok()?;
    let context = SimplifyContext::builder()
        .with_schema(Arc::new(schema.clone()))
        .build();
    let simplifier = ExprSimplifier::new(context);
    let parsed = simplifier
        .simplify(simplifier.coerce(parsed, schema).ok()?)
        .ok()?;
    (parsed == filter).then_some((filter, text))
}

fn unqualified(filter: &Expr) -> Option<Expr> {
    filter
        .clone()
        .transform(|e| match e {
            Expr::Column(c) => Ok(Transformed::yes(Expr::Column(Column::new_unqualified(
                c.name,
            )))),
            e => Ok(Transformed::no(e)),
        })
        .data()
        .ok()
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::prelude::SessionContext;

    /// Filters as the optimizer hands them to a scan: planned from SQL.
    async fn pushed(sql: &str) -> Vec<Expr> {
        let ctx = SessionContext::new();
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("small", DataType::Int32, true),
            Field::new("name", DataType::Utf8, true),
            Field::new("day", DataType::Date32, true),
            Field::new("price", DataType::Decimal128(10, 2), true),
        ]);
        ctx.register_batch(
            "t",
            datafusion::arrow::array::RecordBatch::new_empty(Arc::new(schema)),
        )
        .unwrap();
        let plan = ctx
            .sql(&format!("SELECT * FROM t WHERE {sql}"))
            .await
            .unwrap()
            .into_optimized_plan()
            .unwrap();
        let mut out = Vec::new();
        plan.apply(|p| {
            if let datafusion::logical_expr::LogicalPlan::Filter(f) = p {
                out.extend(datafusion::logical_expr::utils::split_conjunction_owned(
                    f.predicate.clone(),
                ));
            }
            Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue)
        })
        .unwrap();
        out
    }

    #[tokio::test]
    async fn common_filters_round_trip_and_volatile_ones_do_not() {
        let schema = DFSchema::try_from(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("small", DataType::Int32, true),
            Field::new("name", DataType::Utf8, true),
            Field::new("day", DataType::Date32, true),
            Field::new("price", DataType::Decimal128(10, 2), true),
        ]))
        .unwrap();
        for case in [
            "id > 3",
            "small = 2",
            "name LIKE 'a%'",
            "name IN ('a', 'b') OR id IS NULL",
            "day >= DATE '2024-01-01'",
            "price > 10.5",
            "NOT (id BETWEEN 1 AND 4)",
            "upper(name) = 'X'",
            "id % 3 = 1",
        ] {
            let filters = pushed(case).await;
            assert!(!filters.is_empty(), "{case}");
            for f in filters {
                let text = sql(&f, &schema);
                assert!(text.is_some(), "{case}: {f} should round-trip");
            }
        }
        let volatile = pushed("random() > 0.5").await;
        assert!(!volatile.is_empty());
        for f in volatile {
            assert!(sql(&f, &schema).is_none(), "volatile {f}");
        }
    }
}

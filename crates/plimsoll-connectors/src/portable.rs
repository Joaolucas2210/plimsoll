//! Expressions shipped to workers as SQL: row filters the scan enforces
//! itself (ADR 0062), and the keys and arguments of partial aggregations
//! (ADR 0063).
//!
//! A filter is *portable* when its SQL text, parsed back against the table
//! schema, means the same expression: unparsed, re-parsed, coerced and
//! simplified, it must equal the original (itself already coerced and
//! simplified by the optimizer). Anything else (UDFs a worker may lack,
//! expressions the unparser renders lossily) stays inexact and DataFusion
//! re-checks it above the scan, as before.

use std::sync::{Arc, LazyLock};

use datafusion::arrow::datatypes::Schema;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::common::{Column, DFSchema};
use datafusion::execution::SessionState;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::logical_expr::execution_props::ExecutionProps;
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::physical_planning_context::PhysicalPlanningContext;
use datafusion::logical_expr::simplify::SimplifyContext;
use datafusion::logical_expr::{Expr, binary_expr, cast, try_cast};
use datafusion::optimizer::simplify_expressions::ExprSimplifier;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::{
    BinaryExpr, CastExpr, IsNotNullExpr, IsNullExpr, Literal, NegativeExpr, NotExpr, TryCastExpr,
};
use datafusion::physical_expr::{ScalarFunctionExpr, create_physical_expr};
use datafusion::sql::unparser::expr_to_sql;

/// Built-in functions only: what any worker of this version parses.
static PARSER: LazyLock<SessionState> =
    LazyLock::new(|| SessionStateBuilder::new().with_default_features().build());

/// `filter` with its columns unqualified, and as SQL that parses back to
/// that expression against `schema`; `None` when it does not round-trip.
pub(crate) fn sql(filter: &Expr, schema: &DFSchema) -> Option<(Expr, String)> {
    let filter = unqualified(filter)?;
    if filter.is_volatile() {
        return None;
    }
    let text = expr_to_sql(&filter).ok()?.to_string();
    let parsed = parse(&text, schema)?;
    (parsed == filter).then_some((filter, text))
}

/// `expr`, evaluated over `schema`, as SQL that a worker plans back to the
/// same physical expression; `None` when it does not round-trip. `config`
/// is the session's: function nodes compare their options too.
pub(crate) fn physical_sql(
    expr: &Arc<dyn PhysicalExpr>,
    schema: &Schema,
    config: &ConfigOptions,
) -> Option<String> {
    let logical = logical(expr)?;
    if logical.is_volatile() {
        return None;
    }
    let text = expr_to_sql(&logical).ok()?.to_string();
    let schema = DFSchema::try_from(schema.clone()).ok()?;
    let back = planned(&parse(&text, &schema)?, &schema, config)?;
    (&back == expr).then_some(text)
}

/// [`physical_sql`] for an expression only evaluated, not claimed by a
/// scan (a join's filter, ADR 0098): the SQL must plan back to `expr` once
/// both are simplified alike (`a < b` comes back as `b > a`).
pub(crate) fn physical_sql_equivalent(
    expr: &Arc<dyn PhysicalExpr>,
    schema: &Schema,
    config: &ConfigOptions,
) -> Option<String> {
    let logical = logical(expr)?;
    if logical.is_volatile() {
        return None;
    }
    let text = expr_to_sql(&logical).ok()?.to_string();
    let schema = DFSchema::try_from(schema.clone()).ok()?;
    let want = planned(&simplified(logical, &schema)?, &schema, config)?;
    let back = planned(&parse(&text, &schema)?, &schema, config)?;
    (back == want).then_some(text)
}

fn planned(e: &Expr, schema: &DFSchema, config: &ConfigOptions) -> Option<Arc<dyn PhysicalExpr>> {
    let mut props = ExecutionProps::new();
    props.config_options = Some(Arc::new(config.clone()));
    create_physical_expr(e, schema, &props, &PhysicalPlanningContext::default()).ok()
}

/// `text` parsed, coerced and simplified, as the optimizer leaves filters.
pub(crate) fn parse(text: &str, schema: &DFSchema) -> Option<Expr> {
    simplified(PARSER.create_logical_expr(text, schema).ok()?, schema)
}

/// `e` coerced and simplified against `schema`.
fn simplified(parsed: Expr, schema: &DFSchema) -> Option<Expr> {
    let context = SimplifyContext::builder()
        .with_schema(Arc::new(schema.clone()))
        .build();
    let simplifier = ExprSimplifier::new(context);
    simplifier
        .simplify(simplifier.coerce(parsed, schema).ok()?)
        .ok()
}

/// The logical form of the physical expressions aggregations commonly
/// key on; anything else is `None` (not pushed).
fn logical(e: &Arc<dyn PhysicalExpr>) -> Option<Expr> {
    let e = e.as_ref();
    Some(
        if let Some(c) = e.downcast_ref::<datafusion::physical_expr::expressions::Column>() {
            Expr::Column(Column::new_unqualified(c.name()))
        } else if let Some(l) = e.downcast_ref::<Literal>() {
            Expr::Literal(l.value().clone(), None)
        } else if let Some(b) = e.downcast_ref::<BinaryExpr>() {
            binary_expr(logical(b.left())?, *b.op(), logical(b.right())?)
        } else if let Some(c) = e.downcast_ref::<CastExpr>() {
            cast(logical(c.expr())?, c.cast_type().clone())
        } else if let Some(c) = e.downcast_ref::<TryCastExpr>() {
            try_cast(logical(c.expr())?, c.cast_type().clone())
        } else if let Some(n) = e.downcast_ref::<NotExpr>() {
            Expr::Not(Box::new(logical(n.arg())?))
        } else if let Some(n) = e.downcast_ref::<NegativeExpr>() {
            Expr::Negative(Box::new(logical(n.arg())?))
        } else if let Some(n) = e.downcast_ref::<IsNullExpr>() {
            logical(n.arg())?.is_null()
        } else if let Some(n) = e.downcast_ref::<IsNotNullExpr>() {
            logical(n.arg())?.is_not_null()
        } else {
            // The last kind accepted; anything else is not pushed.
            let f = e.downcast_ref::<ScalarFunctionExpr>()?;
            let args = f.args().iter().map(logical).collect::<Option<Vec<_>>>()?;
            Expr::ScalarFunction(ScalarFunction::new_udf(Arc::new(f.fun().clone()), args))
        },
    )
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

    #[tokio::test]
    async fn aggregation_keys_and_arguments_plan_back_to_themselves() {
        use datafusion::physical_plan::aggregates::AggregateExec;
        // A non-default session, as on a coordinator: function nodes carry
        // its options, and must compare equal under them.
        let ctx = SessionContext::new_with_config(
            datafusion::prelude::SessionConfig::new().with_target_partitions(3),
        );
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("small", DataType::Int32, true),
            Field::new("Name", DataType::Utf8, true),
        ]));
        ctx.register_batch(
            "t",
            datafusion::arrow::array::RecordBatch::new_empty(schema),
        )
        .unwrap();
        let plan = ctx
            .sql(
                "SELECT lower(\"Name\"), small % 2, CAST(id AS INT), sum(id * 2), max(-id), \
                 count(id + 1), min(small + 1) FROM t \
                 GROUP BY lower(\"Name\"), small % 2, CAST(id AS INT)",
            )
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        let mut found = false;
        let config = ctx.state().config_options().clone();
        plan.apply(|p| {
            if let Some(a) = p.downcast_ref::<AggregateExec>() {
                found = true;
                let input = a.input().schema();
                for (e, _) in a.group_expr().expr() {
                    assert!(physical_sql(e, &input, &config).is_some(), "key {e}");
                }
                for f in a.aggr_expr() {
                    for e in f.expressions() {
                        assert!(physical_sql(&e, &input, &config).is_some(), "argument {e}");
                    }
                }
            }
            Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue)
        })
        .unwrap();
        assert!(found);
    }
}

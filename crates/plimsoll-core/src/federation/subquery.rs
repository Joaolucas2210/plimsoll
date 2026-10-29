//! Scalar subquery pre-evaluation (watermark pattern).
//!
//! `WHERE ts > (SELECT max(ts) FROM local)` cannot be pushed to a remote source
//! as written: the optimizer turns it into a join. If the subquery is
//! uncorrelated, we evaluate it once, up front, and substitute the literal, so
//! the remaining `ts > <literal>` is an ordinary, pushable comparison.
//!
//! Must run on the *unoptimized* logical plan (e.g. `DataFrame::logical_plan()`
//! from `ctx.sql`), before `ScalarSubqueryToJoin` rewrites it away.

use datafusion::catalog::Session;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{Result, ScalarValue, exec_err};
use datafusion::logical_expr::{Expr, LogicalPlan, Subquery};
use datafusion::physical_plan::collect;

// ponytail: evaluates every uncorrelated scalar subquery in the outer plan,
// sequentially, with no cost model or size guard. Ceiling: an expensive
// subquery is run eagerly even if the query is later cancelled. Upgrade path:
// only pre-evaluate subqueries whose comparison target is a federated scan, and
// run them concurrently under the memory ceiling.
pub async fn preevaluate_scalar_subqueries(
    state: &dyn Session,
    plan: LogicalPlan,
) -> Result<LogicalPlan> {
    let mut found: Vec<Subquery> = Vec::new();
    plan.apply(|node| {
        node.apply_expressions(|e| {
            e.apply(|e| {
                if let Expr::ScalarSubquery(sq) = e {
                    if sq.outer_ref_columns.is_empty() && !found.contains(sq) {
                        found.push(sq.clone());
                    }
                }
                Ok(TreeNodeRecursion::Continue)
            })
        })
    })?;
    if found.is_empty() {
        return Ok(plan);
    }

    let mut values = Vec::with_capacity(found.len());
    for sq in &found {
        values.push(evaluate(state, &sq.subquery).await?);
    }

    plan.transform_up(|node| {
        node.map_expressions(|e| {
            e.transform_up(|e| match &e {
                Expr::ScalarSubquery(sq) => match found.iter().position(|f| f == sq) {
                    Some(i) => Ok(Transformed::yes(Expr::Literal(values[i].clone(), None))),
                    None => Ok(Transformed::no(e)),
                },
                _ => Ok(Transformed::no(e)),
            })
        })
    })
    .map(|t| t.data)
}

/// SQL semantics: zero rows -> NULL, one row -> value, more -> error.
async fn evaluate(state: &dyn Session, plan: &LogicalPlan) -> Result<ScalarValue> {
    let physical = state.create_physical_plan(plan).await?;
    let schema = physical.schema();
    if schema.fields().len() != 1 {
        return exec_err!("scalar subquery must return exactly one column");
    }
    let batches = collect(physical, state.task_ctx()).await?;
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    match rows {
        0 => ScalarValue::try_from(schema.field(0).data_type()),
        1 => match batches.iter().find(|b| b.num_rows() == 1) {
            Some(b) => ScalarValue::try_from_array(b.column(0), 0),
            None => exec_err!("scalar subquery row vanished"),
        },
        n => exec_err!("scalar subquery returned {n} rows, expected at most 1"),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int64Array, RecordBatch};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::catalog::MemTable;
    use datafusion::functions_aggregate::expr_fn::max;
    use datafusion::logical_expr::scalar_subquery;
    use datafusion::prelude::{SessionContext, col};
    use std::sync::Arc;

    fn table(values: Vec<i64>) -> Arc<MemTable> {
        let schema = Arc::new(Schema::new(vec![Field::new("ts", DataType::Int64, false)]));
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(values))]).unwrap();
        Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap())
    }

    #[tokio::test]
    async fn federation_watermark_subquery_becomes_literal() {
        // SELECT ts FROM remote WHERE ts > (SELECT max(ts) FROM local)
        let ctx = SessionContext::new();
        ctx.register_table("local", table(vec![1, 5, 3])).unwrap();
        ctx.register_table("remote", table(vec![4, 6, 7])).unwrap();
        let watermark = ctx
            .table("local")
            .await
            .unwrap()
            .aggregate(vec![], vec![max(col("ts"))])
            .unwrap()
            .into_unoptimized_plan();
        let plan = ctx
            .table("remote")
            .await
            .unwrap()
            .filter(col("ts").gt(scalar_subquery(Arc::new(watermark))))
            .unwrap()
            .into_unoptimized_plan();
        assert!(plan.display_indent().to_string().contains("Subquery"));

        let plan = preevaluate_scalar_subqueries(&ctx.state(), plan)
            .await
            .unwrap();
        let shown = plan.display_indent().to_string();
        assert!(!shown.contains("Subquery"), "{shown}");
        assert!(shown.contains("Int64(5)"), "{shown}");

        let rows: usize = ctx
            .execute_logical_plan(plan)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap()
            .iter()
            .map(|b| b.num_rows())
            .sum();
        assert_eq!(rows, 2);
    }
}

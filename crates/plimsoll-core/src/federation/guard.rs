//! `FederationFilterGuard`: a `TableProvider` decorator that makes it impossible
//! for a pushed filter to vanish.
//!
//! Contract with DataFusion: every filter the guard accepts is reported as
//! `Exact`, because the guard itself owns correctness. Filters whose source
//! answer is not trusted (policy or source says `Inexact`) are re-applied by a
//! `FilterExec` the guard places directly on top of the source scan. The
//! invariant therefore lives in the provider, not in an optimizer rule: whatever
//! `scan` returns satisfies every filter it was given.

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{DFSchema, Result};
use datafusion::logical_expr::expr_rewriter::unnormalize_cols;
use datafusion::logical_expr::utils::conjunction;
use datafusion::logical_expr::{Expr, TableType};
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::filter::FilterExecBuilder;
use datafusion::physical_plan::projection::ProjectionExec;

use super::policy::weakest;
use super::{Pushdown, PushdownPolicy};

/// Wraps a source provider and enforces its [`PushdownPolicy`].
#[derive(Debug)]
pub struct FederationFilterGuard {
    inner: Arc<dyn TableProvider>,
    policy: PushdownPolicy,
}

impl FederationFilterGuard {
    pub fn new(inner: Arc<dyn TableProvider>, policy: PushdownPolicy) -> Self {
        Self { inner, policy }
    }

    pub fn policy(&self) -> &PushdownPolicy {
        &self.policy
    }

    /// Effective decision per filter: the weaker of what the policy allows and
    /// what the source claims. A source can never upgrade its own trust.
    fn decide(&self, filters: &[&Expr]) -> Result<Vec<Pushdown>> {
        let schema = self.inner.schema();
        let claims = self.inner.supports_filters_pushdown(filters)?;
        Ok(filters
            .iter()
            .zip(claims)
            .map(|(f, claim)| weakest(self.policy.classify(f, &schema), claim))
            .collect())
    }
}

#[async_trait]
impl TableProvider for FederationFilterGuard {
    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    fn table_type(&self) -> TableType {
        self.inner.table_type()
    }

    fn supports_filters_pushdown(&self, filters: &[&Expr]) -> Result<Vec<Pushdown>> {
        Ok(self
            .decide(filters)?
            .into_iter()
            .map(|d| match d {
                // The guard re-checks Inexact filters itself, so DataFusion may
                // drop its own Filter: from the engine's view this is Exact.
                Pushdown::Exact | Pushdown::Inexact => Pushdown::Exact,
                Pushdown::Unsupported => Pushdown::Unsupported,
            })
            .collect())
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let refs: Vec<&Expr> = filters.iter().collect();
        let decisions = self.decide(&refs)?;

        let mut pushed = Vec::new();
        let mut recheck = Vec::new();
        for (f, d) in filters.iter().zip(decisions) {
            if d != Pushdown::Unsupported {
                pushed.push(f.clone());
            }
            // Anything not trusted is re-applied, including an Unsupported filter
            // a caller handed us anyway (defensive: DataFusion should not).
            if d != Pushdown::Exact {
                recheck.push(f.clone());
            }
        }

        let Some(predicate) = conjunction(unnormalize_cols(recheck.clone())) else {
            let limit = limit.filter(|_| self.policy.limit);
            return self.inner.scan(state, projection, &pushed, limit).await;
        };

        // Filter-only columns may be absent from `projection` (we answered
        // Exact), so widen the scan, filter, then project back.
        let schema = self.inner.schema();
        let expanded = projection.map(|p| {
            let mut cols = p.clone();
            for e in &recheck {
                for c in e.column_refs() {
                    if let Ok(i) = schema.index_of(&c.name) {
                        if !cols.contains(&i) {
                            cols.push(i);
                        }
                    }
                }
            }
            cols
        });

        // Never push LIMIT under a local re-check: the source would cut rows
        // before we filter them.
        let scan = self
            .inner
            .scan(state, expanded.as_ref(), &pushed, None)
            .await?;
        let df_schema = DFSchema::try_from(scan.schema().as_ref().clone())?;
        let predicate = state.create_physical_expr(predicate, &df_schema)?;
        let filtered: Arc<dyn ExecutionPlan> = Arc::new(
            FilterExecBuilder::new(predicate, scan)
                .with_fetch(limit)
                .build()?,
        );

        match (projection, expanded) {
            (Some(p), Some(e)) if p.len() != e.len() => {
                let out = filtered.schema();
                let exprs = (0..p.len()).map(|i| {
                    let name = out.field(i).name().clone();
                    let c: Arc<dyn datafusion::physical_expr::PhysicalExpr> =
                        Arc::new(Column::new(&name, i));
                    (c, name)
                });
                Ok(Arc::new(ProjectionExec::try_new(exprs, filtered)?))
            }
            _ => Ok(filtered),
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int64Array, RecordBatch, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::catalog::MemTable;
    use datafusion::physical_plan::displayable;
    use datafusion::prelude::{DataFrame, SessionContext, col, lit};

    /// A source that *claims* Exact pushdown for everything but ignores filters.
    #[derive(Debug)]
    struct LyingSource(MemTable);

    #[async_trait]
    impl TableProvider for LyingSource {
        fn schema(&self) -> SchemaRef {
            self.0.schema()
        }
        fn table_type(&self) -> TableType {
            TableType::Base
        }
        fn supports_filters_pushdown(&self, f: &[&Expr]) -> Result<Vec<Pushdown>> {
            Ok(vec![Pushdown::Exact; f.len()])
        }
        async fn scan(
            &self,
            state: &dyn Session,
            projection: Option<&Vec<usize>>,
            _filters: &[Expr],
            limit: Option<usize>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            self.0.scan(state, projection, &[], limit).await
        }
    }

    fn lying() -> Arc<dyn TableProvider> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
                Arc::new(StringArray::from(vec!["a", "b", "c", "d"])),
            ],
        )
        .unwrap();
        Arc::new(LyingSource(
            MemTable::try_new(schema, vec![vec![batch]]).unwrap(),
        ))
    }

    /// `SELECT name FROM t WHERE id > {min} [LIMIT {limit}]`
    async fn query(ctx: &SessionContext, min: i64, limit: Option<usize>) -> DataFrame {
        let df = ctx.table("t").await.unwrap();
        let df = df.filter(col("id").gt(lit(min))).unwrap();
        let df = df.select_columns(&["name"]).unwrap();
        df.limit(0, limit).unwrap()
    }

    async fn names(df: DataFrame) -> Vec<String> {
        let batches = df.collect().await.unwrap();
        let mut out = Vec::new();
        for b in batches {
            let a = b.column(0).as_any().downcast_ref::<StringArray>().unwrap();
            out.extend(a.iter().map(|v| v.unwrap().to_string()));
        }
        out.sort();
        out
    }

    #[tokio::test]
    async fn federation_unguarded_lying_source_loses_the_filter() {
        // Baseline proving the test is meaningful: without the guard the filter
        // disappears and the result is wrong.
        let ctx = SessionContext::new();
        ctx.register_table("t", lying()).unwrap();
        assert_eq!(
            names(query(&ctx, 2, None).await).await,
            ["a", "b", "c", "d"]
        );
    }

    #[tokio::test]
    async fn federation_filter_cannot_disappear() {
        let policy = PushdownPolicy {
            integer_cmp: Pushdown::Inexact,
            ..PushdownPolicy::local_only()
        };
        let ctx = SessionContext::new();
        ctx.register_table("t", Arc::new(FederationFilterGuard::new(lying(), policy)))
            .unwrap();
        assert_eq!(names(query(&ctx, 2, None).await).await, ["c", "d"]);

        let plan = query(&ctx, 2, None)
            .await
            .create_physical_plan()
            .await
            .unwrap();
        let shown = displayable(plan.as_ref()).indent(false).to_string();
        assert!(shown.contains("FilterExec: id@1 > 2"), "{shown}");
    }

    #[tokio::test]
    async fn federation_limit_not_pushed_under_recheck() {
        let policy = PushdownPolicy {
            integer_cmp: Pushdown::Inexact,
            limit: true,
            ..PushdownPolicy::local_only()
        };
        let ctx = SessionContext::new();
        ctx.register_table("t", Arc::new(FederationFilterGuard::new(lying(), policy)))
            .unwrap();
        assert_eq!(names(query(&ctx, 2, Some(1)).await).await.len(), 1);
        assert_eq!(names(query(&ctx, 3, Some(5)).await).await, ["d"]);
    }
}

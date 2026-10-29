//! `AccountedExec`: puts a source's in-flight batches under the pool.
//!
//! DataFusion's file scans don't reserve their decode output, so a wide row
//! group could allocate past the ceiling unseen. Wrapping the scan keeps each
//! partition's latest batch reserved until the next one replaces it.

use std::fmt;
use std::sync::Arc;

use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{Result, internal_err};
use datafusion::execution::memory_pool::MemoryConsumer;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use futures::StreamExt;

#[derive(Debug)]
pub struct AccountedExec {
    label: String,
    input: Arc<dyn ExecutionPlan>,
}

impl AccountedExec {
    /// `label` names the consumer in OOM errors, e.g. `IcebergScan[gold.orders]`.
    pub fn new(label: impl Into<String>, input: Arc<dyn ExecutionPlan>) -> Self {
        Self {
            label: label.into(),
            input,
        }
    }
}

impl DisplayAs for AccountedExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "AccountedExec: {}", self.label)
    }
}

impl ExecutionPlan for AccountedExec {
    fn name(&self) -> &str {
        "AccountedExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.input.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        match (children.pop(), children.is_empty()) {
            (Some(input), true) => Ok(Arc::new(Self::new(self.label.clone(), input))),
            _ => internal_err!("AccountedExec takes exactly one child"),
        }
    }

    fn execute(
        &self,
        partition: usize,
        ctx: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let reservation =
            MemoryConsumer::new(format!("{}[{partition}]", self.label)).register(ctx.memory_pool());
        let input = self.input.execute(partition, ctx)?;
        let schema = input.schema();
        // ponytail: the previous batch is released when the next one arrives,
        // so a consumer that buffers it is briefly counted twice (conservative:
        // the query may fail early, the node never overshoots). Upgrade path:
        // hand the reservation to the consumer with the batch.
        let stream = input.map(move |batch| {
            let batch = batch?;
            reservation.try_resize(batch.get_array_memory_size())?;
            Ok(batch)
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::memory::{CeilingPool, session_context_with_ceiling};
    use datafusion::error::DataFusionError;
    use datafusion::physical_plan::collect;
    use datafusion::prelude::SessionConfig;

    async fn run(limit: usize) -> (Result<usize>, Arc<CeilingPool>) {
        let pool = Arc::new(CeilingPool::new(limit, 0).unwrap());
        let ctx = session_context_with_ceiling(pool.clone(), None, SessionConfig::new()).unwrap();
        let plan = ctx
            .sql("SELECT value, repeat('x', 1000) AS pad FROM range(8192)")
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        let wrapped = Arc::new(AccountedExec::new("TestScan", plan));
        let rows = collect(wrapped, ctx.task_ctx())
            .await
            .map(|b| b.iter().map(|b| b.num_rows()).sum());
        (rows, pool)
    }

    #[tokio::test]
    async fn in_flight_batches_count_against_the_ceiling() {
        let (rows, pool) = run(256 << 20).await;
        assert_eq!(rows.unwrap(), 8192);
        assert_eq!(pool.used(), 0, "released when the stream ends");

        let (err, pool) = run(64 << 10).await;
        let err = err.unwrap_err();
        assert!(
            matches!(err.find_root(), DataFusionError::ResourcesExhausted(m) if m.contains("TestScan")),
            "{err}"
        );
        assert_eq!(pool.used(), 0);
    }
}

//! Data files written on workers (ADR 0091).
//!
//! An `INSERT INTO t SELECT …` whose rows come straight from a remote scan
//! (filters and projections on the workers, ADR 0062/0063) does not bring
//! them to the coordinator: each fragment writes its rows as data files of
//! `t` where it runs, with `t`'s schema and partition spec, and returns
//! the files it wrote. The coordinator commits all of them as one append,
//! as if it had written them itself.

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use datafusion::arrow::array::{AsArray, RecordBatch, UInt64Array};
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::datasource::sink::DataSinkExec;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_expr::expressions::{CastExpr, Column};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
};
use futures::{StreamExt, TryStreamExt};
use plimsoll_core::engine::QueryTag;

use super::write::{IcebergSink, Written};
use crate::remote::{RemoteScanExec, remote_under};

/// The writing fragments, and the commit of what they wrote.
#[derive(Debug)]
pub(crate) struct RemoteWriteExec {
    sink: IcebergSink,
    remote: Arc<RemoteScanExec>,
    /// The write's output: the rows written.
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

/// `node`, a write of rows that come straight from workers, as a
/// [`RemoteWriteExec`]; `None` when it is anything else (rows then travel
/// to the coordinator and are written there, as before).
pub(crate) fn push_write(node: &Arc<dyn ExecutionPlan>) -> Option<Arc<dyn ExecutionPlan>> {
    let exec = node.as_ref().downcast_ref::<DataSinkExec>()?;
    let sink: &dyn Any = exec.sink();
    let sink = sink.downcast_ref::<IcebergSink>()?;
    if !sink.workers_may_write() {
        return None;
    }
    // A projection that only picks, reorders or losslessly casts columns
    // (as INSERT plans one to match the table) is done by the worker.
    // Merges and repartitions only move rows between partitions: the
    // fragments' rows are the write's rows all the same.
    let pass = |mut p: Arc<dyn ExecutionPlan>| {
        while matches!(
            p.name(),
            "CoalescePartitionsExec" | "RepartitionExec" | "CoalesceBatchesExec"
        ) {
            match p.children().first() {
                Some(c) => p = (*c).clone(),
                None => break,
            }
        }
        p
    };
    let mut input = pass(exec.input().clone());
    let mut picks: Option<Vec<usize>> = None;
    if let Some(p) = input.as_ref().downcast_ref::<ProjectionExec>() {
        let columns = p
            .expr()
            .iter()
            .map(|e| {
                let expr = match e.expr.as_ref().downcast_ref::<CastExpr>() {
                    Some(cast) => cast.expr(),
                    None => &e.expr,
                };
                expr.as_ref().downcast_ref::<Column>().map(Column::index)
            })
            .collect::<Option<Vec<_>>>()?;
        picks = Some(columns);
        input = pass(p.input().clone());
    }
    let remote = remote_under(&input)?;
    let remote = remote.as_ref().downcast_ref::<RemoteScanExec>()?;
    let names = remote.columns();
    let source = remote.schema();
    let picks = picks.unwrap_or_else(|| (0..source.fields().len()).collect());
    let target = sink.schema.fields();
    if picks.len() != target.len() {
        return None;
    }
    for (&i, t) in picks.iter().zip(target.iter()) {
        if !lossless(source.field(i).data_type(), t.data_type()) {
            return None;
        }
    }
    let columns = picks
        .iter()
        .map(|&i| names.get(i).cloned())
        .collect::<Option<Vec<_>>>()?;
    let writing = remote.writing(&sink.metadata_path, columns)?;
    let schema = node.schema();
    let properties = Arc::new(PlanProperties::new(
        EquivalenceProperties::new(schema.clone()),
        Partitioning::UnknownPartitioning(1),
        EmissionType::Final,
        Boundedness::Bounded,
    ));
    Some(Arc::new(RemoteWriteExec {
        sink: sink.clone(),
        remote: Arc::new(writing),
        schema,
        properties,
    }))
}

/// Whether the writer's cast from `from` to `to` keeps every value: the
/// same type, string or binary encodings, or a widening.
fn lossless(from: &DataType, to: &DataType) -> bool {
    use DataType::*;
    from.equals_datatype(to)
        || matches!(
            (from, to),
            (Utf8 | LargeUtf8 | Utf8View, Utf8 | LargeUtf8 | Utf8View)
                | (
                    Binary | LargeBinary | BinaryView,
                    Binary | LargeBinary | BinaryView
                )
                | (Int8 | Int16, Int32 | Int64)
                | (Int32, Int64)
                | (Float32, Float64)
        )
}

impl DisplayAs for RemoteWriteExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "RemoteWriteExec: table={} fragments={}",
            self.sink.name(),
            self.remote.fragment_count()
        )
    }
}

impl ExecutionPlan for RemoteWriteExec {
    fn name(&self) -> &str {
        "RemoteWriteExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(
            &Arc<dyn datafusion::physical_expr::PhysicalExpr>,
        ) -> Result<datafusion::common::tree_node::TreeNodeRecursion>,
    ) -> Result<datafusion::common::tree_node::TreeNodeRecursion> {
        Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        ctx: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "RemoteWriteExec has one partition, not {partition}"
            )));
        }
        let tag = ctx.session_config().get_extension::<QueryTag>();
        // Every fragment at once, each on its worker.
        let parts: Vec<_> = (0..self.remote.fragment_count())
            .map(|p| self.remote.partition_stream(p, tag.clone()))
            .collect();
        let sink = self.sink.clone();
        let schema = self.schema.clone();
        let out = schema.clone();
        let commit = async move {
            let batches =
                futures::future::try_join_all(parts.into_iter().map(|s| s.try_collect::<Vec<_>>()))
                    .await?;
            let mut files: Vec<Written> = Vec::new();
            for batch in batches.iter().flatten() {
                for json in batch.column(0).as_string::<i32>().iter().flatten() {
                    let v: serde_json::Value = serde_json::from_str(json)
                        .map_err(|e| DataFusionError::Execution(format!("written file: {e}")))?;
                    files.push(
                        sink.written_from_json(&v).map_err(|e| {
                            DataFusionError::Execution(format!("written file: {e}"))
                        })?,
                    );
                }
            }
            sink.commit_files(&files, &ctx.runtime_env()).await?;
            let rows: u64 = files.iter().map(|f| f.rows()).sum();
            Ok(RecordBatch::try_new(
                out,
                vec![Arc::new(UInt64Array::from(vec![rows]))],
            )?)
        };
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            futures::stream::once(commit).boxed(),
        )))
    }
}

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
use plimsoll_core::memory::AccountedExec;

use super::write::{IcebergSink, Written};
use crate::remote::{RemoteBroadcastExec, RemoteReduceExec, RemoteScanExec, WriteTo};

/// The writing fragments or reduces, and the commit of what they wrote.
#[derive(Debug)]
pub(crate) struct RemoteWriteExec {
    sink: IcebergSink,
    /// A `DELETE` (`false`) or `UPDATE` (`true`) whose files the source
    /// rewrote (ADR 0093); `None`: an append.
    change: Option<bool>,
    /// Partitions returning [`crate::remote::written_schema`] rows.
    source: Arc<dyn ExecutionPlan>,
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
    distribute(sink, exec.input(), node.schema()).map(|e| Arc::new(e) as Arc<dyn ExecutionPlan>)
}

/// The write of `input`'s rows into `sink`'s table, done where the rows
/// are (ADR 0091, 0092): when they come, through pass-through nodes and a
/// projection that only picks, reorders or losslessly casts columns, from
/// a remote scan's fragments (plain rows) or a shuffle's reduces
/// (aggregated or joined rows). `schema` is the write's output.
pub(super) fn distribute(
    sink: &IcebergSink,
    input: &Arc<dyn ExecutionPlan>,
    schema: SchemaRef,
) -> Option<RemoteWriteExec> {
    if !sink.workers_may_write() {
        return None;
    }
    // Merges and repartitions only move rows between partitions: the
    // remote rows are the write's rows all the same.
    let pass = |mut p: Arc<dyn ExecutionPlan>| {
        while matches!(
            p.name(),
            "CoalescePartitionsExec"
                | "RepartitionExec"
                | "CoalesceBatchesExec"
                | "CooperativeExec"
        ) {
            match p.children().first() {
                Some(c) => p = (*c).clone(),
                None => break,
            }
        }
        p
    };
    let mut input = pass(input.clone());
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
    // The scan's or shuffle's accounting wrapper.
    if input.as_ref().downcast_ref::<AccountedExec>().is_some() {
        input = pass(input.children().first().map(|c| (*c).clone())?);
    }
    let source = input.schema();
    let picks = picks.unwrap_or_else(|| (0..source.fields().len()).collect());
    let target = sink.schema.fields();
    if picks.len() != target.len() {
        return None;
    }
    for (&i, t) in picks.iter().zip(target.iter()) {
        if !lossless(source.fields().get(i)?.data_type(), t.data_type()) {
            return None;
        }
    }
    let to = |picks: Option<Vec<usize>>| WriteTo {
        metadata_location: sink.metadata_path.clone(),
        metadata: sink.staged.clone(),
        picks,
    };
    let writing: Arc<dyn ExecutionPlan> =
        if let Some(scan) = input.as_ref().downcast_ref::<RemoteScanExec>() {
            // A scan's columns travel by name: picked in the target's order.
            let names = scan.columns();
            let columns = picks
                .iter()
                .map(|&i| names.get(i).cloned())
                .collect::<Option<Vec<_>>>()?;
            Arc::new(scan.writing(to(None), columns)?)
        } else if let Some(join) = input.as_ref().downcast_ref::<RemoteBroadcastExec>() {
            // Joined against a broadcast build side (ADR 0122): each probe
            // fragment writes the rows it joined.
            Arc::new(join.writing(to(Some(picks))))
        } else {
            let reduce = input.as_ref().downcast_ref::<RemoteReduceExec>()?;
            Arc::new(reduce.writing(to(Some(picks))))
        };
    Some(RemoteWriteExec::new(sink.clone(), writing, schema, None))
}

impl RemoteWriteExec {
    pub(super) fn new(
        sink: IcebergSink,
        source: Arc<dyn ExecutionPlan>,
        schema: SchemaRef,
        change: Option<bool>,
    ) -> Self {
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        ));
        Self {
            sink,
            change,
            source,
            schema,
            properties,
        }
    }
}

/// A write's output: the rows written.
pub(super) fn count_schema() -> SchemaRef {
    Arc::new(datafusion::arrow::datatypes::Schema::new(vec![
        datafusion::arrow::datatypes::Field::new("count", DataType::UInt64, false),
    ]))
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
            "RemoteWriteExec: table={} source={} partitions={}",
            self.sink.name(),
            self.source.name(),
            self.source.properties().partitioning.partition_count()
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
        // Every fragment or reduce at once, each on its worker.
        let parts = (0..self.source.properties().partitioning.partition_count())
            .map(|p| self.source.execute(p, ctx.clone()))
            .collect::<Result<Vec<_>>>()?;
        let sink = self.sink.clone();
        let change = self.change;
        let schema = self.schema.clone();
        let out = schema.clone();
        // Held until the commit: a shuffle's reduce releases its maps when
        // dropped (ADR 0073), and the caller may drop this plan as soon as
        // it has the stream.
        let source = self.source.clone();
        let commit = async move {
            let _source = source;
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
            // An append counts the rows written; a row-level change the
            // rows deleted (an update's rewritten rows are those again).
            let rows: u64 = match change {
                None => {
                    sink.commit_files(&files, &ctx.runtime_env()).await?;
                    files.iter().map(|f| f.rows()).sum()
                }
                Some(update) => {
                    let deleted = files
                        .iter()
                        .filter(|f| f.is_delete())
                        .map(|f| f.added())
                        .sum();
                    if deleted > 0 {
                        sink.commit_change(&files, update, &ctx.runtime_env())
                            .await?;
                    }
                    deleted
                }
            };
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

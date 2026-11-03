//! `MERGE INTO` Iceberg tables (ADR 0082).
//!
//! The source (`USING …`) is collected once and hashed on the source side
//! of the `ON` equalities. Every target data file is read whole, in file
//! order (as `DELETE` does, ADR 0077), probing the hash: each target row
//! pairs with at most one source row (two is an error, as SQL requires),
//! and the first `WHEN` clause whose condition holds decides its fate.
//! Deleted and updated target rows become position deletes; updated and
//! inserted rows go through the partitioned writer; all of it commits as
//! one `overwrite` snapshot.

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray, BooleanArray, RecordBatch, UInt32Array};
use datafusion::arrow::compute::{cast, concat_batches, take};
use datafusion::arrow::datatypes::{Field, Schema, SchemaRef};
use datafusion::arrow::row::{RowConverter, SortField};
use datafusion::common::DFSchemaRef;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::memory_pool::MemoryConsumer;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan};
use futures::StreamExt;

use super::write::{IcebergSink, Target, Written};
use crate::remote::{ClauseSpec, MergeSpec, RemoteScanExec};

type Expr = Arc<dyn PhysicalExpr>;

/// What a matching clause does.
pub(super) enum Action {
    /// Every target column of the new row (its `SET` value or itself).
    Update(Vec<Expr>),
    Delete,
    /// Every target column of the inserted row (its value or NULL).
    Insert(Vec<Expr>),
}

pub(super) struct Clause {
    pub(super) predicate: Option<Expr>,
    pub(super) action: Action,
}

/// The compiled statement, every expression over the merged row (target
/// columns, then source columns).
pub(super) struct Merge {
    /// `ON` equalities: target side and source side.
    pub(super) target_keys: Vec<Expr>,
    pub(super) source_keys: Vec<Expr>,
    /// The rest of `ON`, checked per pair.
    pub(super) residual: Option<Expr>,
    pub(super) matched: Vec<Clause>,
    pub(super) not_matched: Vec<Clause>,
    pub(super) not_matched_by_source: Vec<Clause>,
    /// The merged row's schema, every field nullable (one side is NULL for
    /// unmatched rows).
    pub(super) row: SchemaRef,
    /// Target columns: the first `width` of `row`.
    pub(super) width: usize,
}

impl Merge {
    /// The merged row schema of `merge_schema`, every field nullable.
    pub(super) fn row_schema(merge_schema: &DFSchemaRef) -> SchemaRef {
        Arc::new(Schema::new(
            merge_schema
                .fields()
                .iter()
                .map(|f| Field::new(f.name(), f.data_type().clone(), true))
                .collect::<Vec<_>>(),
        ))
    }
}

pub(super) struct IcebergMergeExec {
    sink: Arc<IcebergSink>,
    targets: Arc<Vec<Target>>,
    source: Arc<dyn ExecutionPlan>,
    merge: Arc<Merge>,
    /// The target files as worker fragments running the merge (ADR 0094);
    /// `None`: all of it here.
    remote: Option<Arc<RemoteScanExec>>,
    properties: Arc<datafusion::physical_plan::PlanProperties>,
}

impl IcebergMergeExec {
    pub(super) fn new(
        sink: IcebergSink,
        targets: Vec<Target>,
        source: Arc<dyn ExecutionPlan>,
        merge: Merge,
    ) -> Self {
        use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
        let properties = Arc::new(datafusion::physical_plan::PlanProperties::new(
            datafusion::physical_expr::EquivalenceProperties::new(count_schema()),
            datafusion::physical_plan::Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        ));
        Self {
            sink: Arc::new(sink),
            targets: Arc::new(targets),
            source,
            merge: Arc::new(merge),
            remote: None,
            properties,
        }
    }

    pub(super) fn with_remote(mut self, remote: Option<Arc<RemoteScanExec>>) -> Self {
        self.remote = remote;
        self
    }
}

fn count_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "count",
        datafusion::arrow::datatypes::DataType::UInt64,
        false,
    )]))
}

impl fmt::Debug for IcebergMergeExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "IcebergMergeExec[{}]", self.sink.name())
    }
}

impl DisplayAs for IcebergMergeExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "IcebergMerge[{}] files={} keys={}",
            self.sink.name(),
            self.targets.len(),
            self.merge.target_keys.len()
        )?;
        if let Some(remote) = &self.remote {
            write!(f, " fragments={}", remote.fragment_count())?;
        }
        Ok(())
    }
}

impl ExecutionPlan for IcebergMergeExec {
    fn name(&self) -> &str {
        "IcebergMergeExec"
    }

    fn properties(&self) -> &Arc<datafusion::physical_plan::PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.source]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Expr) -> Result<datafusion::common::tree_node::TreeNodeRecursion>,
    ) -> Result<datafusion::common::tree_node::TreeNodeRecursion> {
        Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let Some(source) = children.pop() else {
            return Err(DataFusionError::Internal("MERGE needs its source".into()));
        };
        Ok(Arc::new(Self {
            sink: self.sink.clone(),
            targets: self.targets.clone(),
            source,
            merge: self.merge.clone(),
            remote: self.remote.clone(),
            properties: self.properties.clone(),
        }))
    }

    fn execute(
        &self,
        _partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let (sink, targets, source, merge, remote) = (
            self.sink.clone(),
            self.targets.clone(),
            self.source.clone(),
            self.merge.clone(),
            self.remote.clone(),
        );
        let run = async move {
            let changed = match remote {
                Some(remote) => run_remote(&sink, &remote, source, &merge, &context).await?,
                None => run(&sink, &targets, source, &merge, &context).await?,
            };
            Ok(RecordBatch::try_new(
                count_schema(),
                vec![Arc::new(datafusion::arrow::array::UInt64Array::from(vec![
                    changed,
                ]))],
            )?)
        };
        Ok(Box::pin(
            datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(
                count_schema(),
                futures::stream::once(run),
            ),
        ))
    }
}

/// A merged batch: target rows (or NULLs) beside source rows (or NULLs).
fn merged(
    row: &SchemaRef,
    width: usize,
    target: Option<&RecordBatch>,
    source: Option<&RecordBatch>,
    rows: usize,
) -> Result<RecordBatch> {
    let side = |batch: Option<&RecordBatch>, fields: &[Arc<Field>]| -> Vec<ArrayRef> {
        match batch {
            Some(b) => b.columns().to_vec(),
            None => fields
                .iter()
                .map(|f| datafusion::arrow::array::new_null_array(f.data_type(), rows))
                .collect(),
        }
    };
    let fields = row.fields();
    let mut columns = side(target, &fields[..width]);
    columns.extend(side(source, &fields[width..]));
    // Same values, the merged row's (nullable) fields.
    let columns = columns
        .into_iter()
        .zip(fields.iter())
        .map(|(c, f)| {
            if c.data_type() == f.data_type() {
                Ok(c)
            } else {
                cast(&c, f.data_type())
            }
        })
        .collect::<std::result::Result<Vec<_>, _>>()?;
    Ok(RecordBatch::try_new_with_options(
        row.clone(),
        columns,
        &datafusion::arrow::array::RecordBatchOptions::new().with_row_count(Some(rows)),
    )?)
}

fn evaluate(expr: &Expr, batch: &RecordBatch) -> Result<ArrayRef> {
    expr.evaluate(batch)?.into_array(batch.num_rows())
}

/// `true` where `expr` (or no condition) holds; NULL is false.
fn holds(expr: Option<&Expr>, batch: &RecordBatch) -> Result<Vec<bool>> {
    let Some(expr) = expr else {
        return Ok(vec![true; batch.num_rows()]);
    };
    let mask = evaluate(expr, batch)?;
    let mask = mask
        .as_boolean_opt()
        .ok_or_else(|| DataFusionError::Plan("MERGE condition is not boolean".into()))?;
    Ok((0..batch.num_rows())
        .map(|i| mask.is_valid(i) && mask.value(i))
        .collect())
}

/// Applies `clauses` (first match wins) to the rows of `batch`: positions
/// (of `rows`, parallel to the batch) to delete, and new target rows.
fn apply(
    clauses: &[Clause],
    batch: &RecordBatch,
    target: &SchemaRef,
    deleted: &mut Vec<usize>,
    written: &mut Vec<RecordBatch>,
) -> Result<u64> {
    let mut decided = vec![false; batch.num_rows()];
    let mut changed = 0;
    for clause in clauses {
        let pick: Vec<bool> = holds(clause.predicate.as_ref(), batch)?
            .into_iter()
            .zip(&decided)
            .map(|(h, d)| h && !d)
            .collect();
        let chosen: Vec<usize> = (0..pick.len()).filter(|i| pick[*i]).collect();
        if chosen.is_empty() {
            continue;
        }
        for &i in &chosen {
            decided[i] = true;
        }
        changed += chosen.len() as u64;
        match &clause.action {
            Action::Delete => deleted.extend(&chosen),
            Action::Update(row) | Action::Insert(row) => {
                if matches!(clause.action, Action::Update(_)) {
                    deleted.extend(&chosen);
                }
                let rows = datafusion::arrow::compute::filter_record_batch(
                    batch,
                    &BooleanArray::from(pick),
                )?;
                let columns = row
                    .iter()
                    .zip(target.fields())
                    .map(|(e, f)| Ok(cast(&evaluate(e, &rows)?, f.data_type())?))
                    .collect::<Result<Vec<_>>>()?;
                written.push(RecordBatch::try_new(target.clone(), columns)?);
            }
        }
    }
    Ok(changed)
}

/// Executes the merge; the number of target rows inserted, updated or
/// deleted.
async fn run(
    sink: &IcebergSink,
    targets: &[Target],
    source: Arc<dyn ExecutionPlan>,
    merge: &Merge,
    context: &Arc<TaskContext>,
) -> Result<u64> {
    let memory = MemoryConsumer::new(format!("IcebergMerge[{}]", sink.name()))
        .register(context.memory_pool());
    // The source, whole, hashed by its side of the ON equalities.
    // ponytail: in memory (reserved); a partitioned hash join if sources
    // outgrow the pool.
    let source_schema = source.schema();
    let batches = datafusion::physical_plan::collect(source, context.clone()).await?;
    let source = concat_batches(&source_schema, &batches)?;
    drop(batches);
    memory.try_grow(source.get_array_memory_size())?;
    let index = Index::new(merge, source)?;
    let mut probed = probe(sink, targets, &index, merge, context).await?;
    probed.changed += not_matched(
        merge,
        &index.source,
        &probed.matched,
        sink,
        &mut probed.new_rows,
    )?;
    if probed.changed == 0 {
        return Ok(0);
    }
    finish(sink, probed.files, probed.new_rows, context).await?;
    memory.resize(0);
    Ok(probed.changed)
}

/// The merge with its target files on workers (ADR 0094): the source,
/// collected here, goes to every fragment as Arrow IPC; each worker runs
/// `WHEN MATCHED` and `NOT MATCHED BY SOURCE` over its files and reports
/// its files and the source rows that paired; `WHEN NOT MATCHED` then
/// runs here over the source rows no worker paired, and everything
/// commits as one `overwrite`.
async fn run_remote(
    sink: &IcebergSink,
    remote: &RemoteScanExec,
    source: Arc<dyn ExecutionPlan>,
    merge: &Merge,
    context: &Arc<TaskContext>,
) -> Result<u64> {
    use futures::TryStreamExt;
    let memory = MemoryConsumer::new(format!("IcebergMerge[{}]", sink.name()))
        .register(context.memory_pool());
    let source_schema = source.schema();
    let batches = datafusion::physical_plan::collect(source, context.clone()).await?;
    let source = concat_batches(&source_schema, &batches)?;
    drop(batches);
    memory.try_grow(source.get_array_memory_size())?;
    let mut ipc = Vec::new();
    {
        let mut writer =
            datafusion::arrow::ipc::writer::StreamWriter::try_new(&mut ipc, &source_schema)?;
        writer.write(&source)?;
        writer.finish()?;
    }
    memory.try_grow(ipc.len())?;
    let ipc = bytes::Bytes::from(ipc);
    let tag = context
        .session_config()
        .get_extension::<plimsoll_core::engine::QueryTag>();
    let replies = futures::future::try_join_all((0..remote.fragment_count()).map(|p| {
        remote
            .attached_stream(p, tag.clone(), ipc.clone())
            .try_collect::<Vec<_>>()
    }))
    .await?;
    let bad = |e: String| DataFusionError::Execution(format!("merge fragment: {e}"));
    let mut files = Vec::new();
    let mut matched = vec![false; source.num_rows()];
    let mut changed = 0u64;
    for batch in replies.iter().flatten() {
        for json in batch.column(0).as_string::<i32>().iter().flatten() {
            let v: serde_json::Value =
                serde_json::from_str(json).map_err(|e| bad(e.to_string()))?;
            match v.get("matched") {
                Some(rows) => {
                    for j in rows.as_array().into_iter().flatten() {
                        let j = j.as_u64().and_then(|j| usize::try_from(j).ok());
                        match j.and_then(|j| matched.get_mut(j)) {
                            Some(m) => *m = true,
                            None => return Err(bad("source row out of range".into())),
                        }
                    }
                    changed += v["changed"].as_u64().unwrap_or(0);
                }
                None => files.push(sink.written_from_json(&v).map_err(bad)?),
            }
        }
    }
    let mut new_rows = Vec::new();
    changed += not_matched(merge, &source, &matched, sink, &mut new_rows)?;
    if changed == 0 {
        return Ok(0);
    }
    finish(sink, files, new_rows, context).await?;
    memory.resize(0);
    Ok(changed)
}

/// The merged row with every column renamed `t0…` (target) then `s0…`
/// (source): names unique on both sides, as clauses travel to workers in
/// (ADR 0094).
fn renamed(row: &SchemaRef, width: usize) -> SchemaRef {
    Arc::new(Schema::new(
        row.fields()
            .iter()
            .enumerate()
            .map(|(i, f)| {
                let name = if i < width {
                    format!("t{i}")
                } else {
                    format!("s{}", i - width)
                };
                Field::new(name, f.data_type().clone(), true)
            })
            .collect::<Vec<_>>(),
    ))
}

/// `merge`'s target-file pass as SQL for workers (ADR 0094); `None` when
/// an expression does not round-trip, or a clause the pass runs inserts.
pub(super) fn spec(
    merge: &Merge,
    config: &datafusion::common::config::ConfigOptions,
) -> Option<MergeSpec> {
    use datafusion::common::tree_node::{Transformed, TreeNode};
    use datafusion::physical_expr::expressions::Column;
    let schema = renamed(&merge.row, merge.width);
    let sql = |e: &Expr| {
        let e = e
            .clone()
            .transform(|node| {
                Ok(match node.as_ref().downcast_ref::<Column>() {
                    Some(c) => Transformed::yes(Arc::new(Column::new(
                        schema.field(c.index()).name(),
                        c.index(),
                    )) as Expr),
                    None => Transformed::no(node),
                })
            })
            .ok()?
            .data;
        crate::portable::physical_sql(&e, &schema, config)
    };
    let all = |es: &[Expr]| es.iter().map(sql).collect::<Option<Vec<_>>>();
    let clauses = |cs: &[Clause]| {
        cs.iter()
            .map(|c| {
                Some(ClauseSpec {
                    predicate: match &c.predicate {
                        Some(p) => Some(sql(p)?),
                        None => None,
                    },
                    update: match &c.action {
                        Action::Delete => None,
                        Action::Update(row) => Some(all(row)?),
                        Action::Insert(_) => return None,
                    },
                })
            })
            .collect::<Option<Vec<_>>>()
    };
    Some(MergeSpec {
        target_keys: all(&merge.target_keys)?,
        source_keys: all(&merge.source_keys)?,
        residual: match &merge.residual {
            Some(r) => Some(sql(r)?),
            None => None,
        },
        matched: clauses(&merge.matched)?,
        not_matched_by_source: clauses(&merge.not_matched_by_source)?,
    })
}

/// A worker's [`Merge`] from `spec` (ADR 0094): the merged row is the
/// table's columns then the source's, renamed as [`spec`] named them.
/// `WHEN NOT MATCHED` is the coordinator's.
pub(super) fn from_spec(
    spec: &MergeSpec,
    table: &Schema,
    source: &Schema,
    state: &dyn datafusion::catalog::Session,
) -> std::result::Result<Merge, crate::ConnectorError> {
    let width = table.fields().len();
    let row = Arc::new(Schema::new(
        table
            .fields()
            .iter()
            .chain(source.fields().iter())
            .map(|f| Field::new(f.name(), f.data_type().clone(), true))
            .collect::<Vec<_>>(),
    ));
    let schema = renamed(&row, width);
    let df = datafusion::common::DFSchema::try_from(schema.as_ref().clone())?;
    let expr = |sql: &str| -> std::result::Result<Expr, crate::ConnectorError> {
        let e = crate::portable::parse(sql, &df).ok_or_else(|| {
            crate::ConnectorError::Unsupported(format!("expression {sql:?} does not parse here"))
        })?;
        Ok(state.create_physical_expr(e, &df)?)
    };
    let all = |sqls: &[String]| {
        sqls.iter()
            .map(|s| expr(s))
            .collect::<std::result::Result<Vec<_>, _>>()
    };
    let clauses = |cs: &[ClauseSpec]| {
        cs.iter()
            .map(|c| {
                Ok(Clause {
                    predicate: c.predicate.as_deref().map(expr).transpose()?,
                    action: match &c.update {
                        None => Action::Delete,
                        Some(row) => Action::Update(all(row)?),
                    },
                })
            })
            .collect::<std::result::Result<Vec<_>, crate::ConnectorError>>()
    };
    Ok(Merge {
        target_keys: all(&spec.target_keys)?,
        source_keys: all(&spec.source_keys)?,
        residual: spec.residual.as_deref().map(expr).transpose()?,
        matched: clauses(&spec.matched)?,
        not_matched: Vec::new(),
        not_matched_by_source: clauses(&spec.not_matched_by_source)?,
        row: schema,
        width,
    })
}

/// The source, hashed by its side of the `ON` equalities.
pub(super) struct Index {
    pub(super) source: RecordBatch,
    converter: RowConverter,
    types: Vec<datafusion::arrow::datatypes::DataType>,
    index: HashMap<Vec<u8>, Vec<u32>>,
}

impl Index {
    pub(super) fn new(merge: &Merge, source: RecordBatch) -> Result<Self> {
        let rows = source.num_rows();
        let as_source = merged(&merge.row, merge.width, None, Some(&source), rows)?;
        let source_keys = merge
            .source_keys
            .iter()
            .map(|e| evaluate(e, &as_source))
            .collect::<Result<Vec<_>>>()?;
        let types: Vec<_> = source_keys.iter().map(|k| k.data_type().clone()).collect();
        let converter = RowConverter::new(types.iter().cloned().map(SortField::new).collect())?;
        let encoded = converter.convert_columns(&source_keys)?;
        let mut index: HashMap<Vec<u8>, Vec<u32>> = HashMap::new();
        for i in 0..rows {
            // A NULL key matches nothing, as in SQL.
            if source_keys.iter().any(|k| k.is_null(i)) {
                continue;
            }
            index
                .entry(encoded.row(i).as_ref().to_vec())
                .or_default()
                .push(i as u32);
        }
        Ok(Self {
            source,
            converter,
            types,
            index,
        })
    }
}

/// The target files' pass: position deletes written, new rows to write,
/// which source rows paired with a target row, and the rows changed.
pub(super) struct Probed {
    pub(super) files: Vec<Written>,
    pub(super) new_rows: Vec<RecordBatch>,
    pub(super) matched: Vec<bool>,
    pub(super) changed: u64,
}

/// `WHEN MATCHED` and `WHEN NOT MATCHED BY SOURCE` over `targets`: what
/// each target file decides on its own, so a worker can run it for its
/// share of the files (ADR 0094).
pub(super) async fn probe(
    sink: &IcebergSink,
    targets: &[Target],
    index: &Index,
    merge: &Merge,
    context: &Arc<TaskContext>,
) -> Result<Probed> {
    let memory = MemoryConsumer::new(format!("IcebergMergeRows[{}]", sink.name()))
        .register(context.memory_pool());
    let source = &index.source;
    let (converter, types) = (&index.converter, &index.types);
    let mut source_matched = vec![false; source.num_rows()];
    let target_schema = sink.schema_ref();
    let env = context.runtime_env();
    let run_id = super::write::unique_id();
    let mut files = Vec::new();
    let mut new_rows: Vec<RecordBatch> = Vec::new();
    let mut changed = 0u64;
    for target in targets {
        let kept = target.selection.as_ref().map(super::write::kept_ranges);
        let mut range = 0;
        let mut stream = target.plan.execute(0, context.clone())?;
        let mut offset = 0u64;
        let mut positions: Vec<i64> = Vec::new();
        while let Some(batch) = stream.next().await {
            let batch = batch?;
            let n = batch.num_rows();
            let as_target = merged(&merge.row, merge.width, Some(&batch), None, n)?;
            let keys = merge
                .target_keys
                .iter()
                .zip(types)
                .map(|(e, t)| Ok(cast(&evaluate(e, &as_target)?, t)?))
                .collect::<Result<Vec<_>>>()?;
            let probe = converter.convert_columns(&keys)?;
            let live: Vec<bool> = (0..n)
                .map(|i| super::write::alive(kept.as_deref(), offset + i as u64, &mut range))
                .collect();
            // Candidate pairs by key, then the rest of ON.
            let (mut t, mut s) = (Vec::new(), Vec::new());
            for (i, &alive) in live.iter().enumerate() {
                if !alive || keys.iter().any(|k| k.is_null(i)) {
                    continue;
                }
                if let Some(hits) = index.index.get(probe.row(i).as_ref()) {
                    for &j in hits {
                        t.push(i as u32);
                        s.push(j);
                    }
                }
            }
            let pairs = merged(
                &merge.row,
                merge.width,
                Some(&take_rows(&batch, &t)?),
                Some(&take_rows(source, &s)?),
                t.len(),
            )?;
            let on = holds(merge.residual.as_ref(), &pairs)?;
            let kept_pairs: Vec<usize> = (0..t.len()).filter(|k| on[*k]).collect();
            // At most one source row per target row (SQL's rule: the
            // outcome would otherwise depend on order).
            let mut paired = vec![false; n];
            for &k in &kept_pairs {
                let row = t[k] as usize;
                if paired[row] {
                    // The statement's fault, not the engine's (SQL:2003).
                    return Err(DataFusionError::Plan(format!(
                        "MERGE INTO {}: a target row matches more than one source row",
                        sink.name()
                    )));
                }
                paired[row] = true;
                source_matched[s[k] as usize] = true;
            }
            // WHEN MATCHED, over the kept pairs.
            if !kept_pairs.is_empty() && !merge.matched.is_empty() {
                let mask: Vec<bool> = (0..t.len()).map(|k| on[k]).collect();
                let matched_pairs = datafusion::arrow::compute::filter_record_batch(
                    &pairs,
                    &BooleanArray::from(mask),
                )?;
                let mut deleted = Vec::new();
                changed += apply(
                    &merge.matched,
                    &matched_pairs,
                    &target_schema,
                    &mut deleted,
                    &mut new_rows,
                )?;
                positions.extend(
                    deleted
                        .into_iter()
                        .map(|d| (offset + u64::from(t[kept_pairs[d]])) as i64),
                );
            }
            // WHEN NOT MATCHED BY SOURCE, over the live unpaired rows.
            if !merge.not_matched_by_source.is_empty() {
                let lone: Vec<u32> = (0..n)
                    .filter(|i| live[*i] && !paired[*i])
                    .map(|i| i as u32)
                    .collect();
                if !lone.is_empty() {
                    let rows = merged(
                        &merge.row,
                        merge.width,
                        Some(&take_rows(&batch, &lone)?),
                        None,
                        lone.len(),
                    )?;
                    let mut deleted = Vec::new();
                    changed += apply(
                        &merge.not_matched_by_source,
                        &rows,
                        &target_schema,
                        &mut deleted,
                        &mut new_rows,
                    )?;
                    positions.extend(
                        deleted
                            .into_iter()
                            .map(|d| (offset + u64::from(lone[d])) as i64),
                    );
                }
            }
            if let Some(last) = new_rows.last() {
                memory.try_grow(last.get_array_memory_size())?;
            }
            offset += n as u64;
        }
        if !positions.is_empty() {
            positions.sort_unstable();
            files.push(
                sink.position_deletes(target, positions, run_id, files.len(), &env)
                    .await?,
            );
        }
    }
    // The new rows are the caller's to write: ownership passes with them.
    memory.resize(0);
    Ok(Probed {
        files,
        new_rows,
        matched: source_matched,
        changed,
    })
}

/// `WHEN NOT MATCHED`: the source rows no target row paired with, over
/// every target file (on the coordinator when distributed, ADR 0094).
/// The rows inserted.
pub(super) fn not_matched(
    merge: &Merge,
    source: &RecordBatch,
    matched: &[bool],
    sink: &IcebergSink,
    new_rows: &mut Vec<RecordBatch>,
) -> Result<u64> {
    let lone: Vec<u32> = (0..source.num_rows())
        .filter(|j| !matched.get(*j).copied().unwrap_or(false))
        .map(|j| j as u32)
        .collect();
    if lone.is_empty() || merge.not_matched.is_empty() {
        return Ok(0);
    }
    let rows = merged(
        &merge.row,
        merge.width,
        None,
        Some(&take_rows(source, &lone)?),
        lone.len(),
    )?;
    let mut deleted = Vec::new();
    apply(
        &merge.not_matched,
        &rows,
        &sink.schema_ref(),
        &mut deleted,
        new_rows,
    )
}

/// Writes `new_rows` beside `files` and commits all as one `overwrite`.
pub(super) async fn finish(
    sink: &IcebergSink,
    mut files: Vec<Written>,
    new_rows: Vec<RecordBatch>,
    context: &Arc<TaskContext>,
) -> Result<()> {
    if !new_rows.is_empty() {
        let rows = datafusion::physical_plan::memory::MemoryStream::try_new(
            new_rows,
            sink.schema_ref(),
            None,
        )?;
        files.extend(sink.write_rows(Box::pin(rows), context).await?);
    }
    sink.commit_overwrite(&files, &context.runtime_env()).await
}

/// The rows of `batch` at `indices`, in that order.
fn take_rows(batch: &RecordBatch, indices: &[u32]) -> Result<RecordBatch> {
    let indices = UInt32Array::from(indices.to_vec());
    let columns = batch
        .columns()
        .iter()
        .map(|c| take(c.as_ref(), &indices, None))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    Ok(RecordBatch::try_new_with_options(
        batch.schema(),
        columns,
        &datafusion::arrow::array::RecordBatchOptions::new().with_row_count(Some(indices.len())),
    )?)
}

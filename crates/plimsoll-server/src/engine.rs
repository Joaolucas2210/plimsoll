//! The seam between the HTTP layer and the compute plane.
//!
//! [`CoreEngine`] adapts the `plimsoll-core` façade (one SessionContext on the
//! process CeilingPool). Admission, timeouts and the registry already wrap
//! every call in `app.rs`.

use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use futures::StreamExt;
use plimsoll_connectors::remote::{Bucket, Fragment, Reduce, open_bucket};
use plimsoll_connectors::{Connector, ConnectorError, IcebergConnector};
use plimsoll_control::catalog::CatalogError;
use plimsoll_core::datafusion::arrow::ipc::writer::StreamWriter;
use plimsoll_core::datafusion::arrow::json::ArrayWriter;
use plimsoll_core::datafusion::arrow::record_batch::RecordBatch;
use plimsoll_core::datafusion::catalog::streaming::StreamingTable;
use plimsoll_core::datafusion::common::ScalarValue;
use plimsoll_core::datafusion::error::DataFusionError;
use plimsoll_core::datafusion::execution::{SendableRecordBatchStream, SpillFile, SpillWriter};
use plimsoll_core::datafusion::object_store::Error as ObjectStoreError;
use plimsoll_core::datafusion::parquet::errors::ParquetError;
use plimsoll_core::datafusion::physical_plan::streaming::PartitionStream;
use plimsoll_core::engine::{AccessDenied, Engine, QueryResult, QueryTag, TableRead};
use plimsoll_core::plan_trace::{self, OperatorSpan, PlanEvent};

use crate::auth::Viewer;
use plimsoll_core::memory::{ConsumerKind, Reservation};
use serde_json::Value;

/// A result ready for the wire. `rows_json` is already serialized (a JSON
/// array of row arrays aligned with `columns`) and stays reserved in the
/// pool through `memory` until the response body has been handed off.
pub struct QueryOutput {
    pub columns: Vec<String>,
    pub rows_json: Vec<u8>,
    pub memory: Option<Reservation>,
    pub phases: Phases,
    /// What each scan read, for the audit trail (ADR 0046).
    pub reads: Vec<TableRead>,
    /// Catalog loads and connector planning, for the trace (ADR 0056).
    pub plan_events: Vec<PlanEvent>,
    /// Executed plan nodes, for execution spans (ADR 0071).
    pub operators: Vec<OperatorSpan>,
}

/// Where a successful query spent its time (the admission queue is timed by
/// the handler).
#[derive(Debug, Default, Clone, Copy)]
pub struct Phases {
    pub planning: Duration,
    pub execution: Duration,
    pub serialization: Duration,
}

impl QueryOutput {
    /// Unaccounted output from plain values (tests, fakes).
    pub fn from_rows(columns: Vec<String>, rows: &[Vec<Value>]) -> Self {
        Self {
            columns,
            rows_json: serde_json::to_vec(rows).unwrap_or_else(|_| b"[]".to_vec()),
            memory: None,
            phases: Phases::default(),
            reads: Vec::new(),
            plan_events: Vec::new(),
            operators: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum EngineError {
    /// Parse / plan / unknown table: the caller's fault.
    #[error("invalid query: {0}")]
    InvalidQuery(String),
    /// The query crossed the load line. The node is fine; this query is not.
    #[error("memory limit exceeded: {0}")]
    MemoryLimitExceeded(String),
    /// The table uses a feature Plimsoll does not read (yet).
    #[error("unsupported: {0}")]
    Unsupported(String),
    /// The table itself is broken: malformed metadata, a missing data file.
    #[error("invalid table: {0}")]
    InvalidTable(String),
    /// The caller may not read a table the query uses (ADR 0025).
    #[error("forbidden: {0}")]
    Forbidden(String),
    /// An upstream system failed or refused us. Retrying may succeed.
    #[error("{dependency} failed: {message}")]
    Dependency {
        dependency: &'static str,
        message: String,
    },
    #[error("internal error: {0}")]
    Internal(String),
}

/// A piece of a streamed result (ADR 0035): the column names once, then
/// NDJSON lines (one JSON array per row), each chunk reserved in the pool
/// until the body has taken it.
pub enum Chunk {
    Header(Vec<String>),
    Rows(Vec<u8>, Option<Reservation>),
}

/// Wire encoding of a streamed result (ADR 0035, 0037).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Format {
    /// One JSON array per row, newline-terminated.
    Ndjson,
    /// Arrow IPC stream: schema, record batches, end-of-stream marker.
    Arrow,
}

/// Rows per streamed chunk. ponytail: fixed; very wide rows make big
/// chunks (1024 × row size). Size by bytes if that ever matters.
const CHUNK_ROWS: usize = 1024;

/// What a streamed query did, once its last chunk was sent.
#[derive(Debug, Default, Clone)]
pub struct Streamed {
    pub rows: u64,
    pub phases: Phases,
    /// What each scan read, for the audit trail (ADR 0046).
    pub reads: Vec<TableRead>,
    /// Catalog loads and connector planning, for the trace (ADR 0056).
    pub plan_events: Vec<PlanEvent>,
    /// Executed plan nodes, for execution spans (ADR 0071).
    pub operators: Vec<OperatorSpan>,
}

/// The receiver is gone (client disconnected). The caller cancels the query
/// in the registry before this surfaces (ADR 0035), so it is never reported.
fn receiver_gone() -> EngineError {
    EngineError::Internal("result receiver dropped".into())
}

#[async_trait]
pub trait QueryEngine: Send + Sync {
    /// Runs `sql` on behalf of `viewer` (scopes system tables, ADR 0022).
    async fn execute(
        &self,
        sql: &str,
        params: &[ScalarValue],
        viewer: Viewer,
    ) -> Result<QueryOutput, EngineError>;

    /// A worker's scan fragment (ADR 0058), as a stream. Default: not a
    /// worker.
    /// `build` is the build side sent with a broadcast join (ADR 0066).
    async fn fragment(
        &self,
        _fragment: Fragment,
        _build: Option<Build>,
        _deadline: Instant,
    ) -> Result<Running, EngineError> {
        Err(EngineError::Unsupported("scan fragments".into()))
    }

    /// Drops this worker's maps of `shuffle` and of its retries (ADR
    /// 0073); an empty stream. Default: not a worker.
    async fn release(&self, _shuffle: &str) -> Result<Running, EngineError> {
        Err(EngineError::Unsupported("shuffle releases".into()))
    }

    /// A worker's shuffle reduce (ADR 0064), as a stream. `token` is the
    /// worker token it was called with, presented to its peers. Default:
    /// not a worker.
    async fn reduce(
        &self,
        _reduce: Reduce,
        _token: &str,
        _tag: Option<Arc<QueryTag>>,
    ) -> Result<Running, EngineError> {
        Err(EngineError::Unsupported("shuffle reduces".into()))
    }

    /// Runs `sql`, sending the result to `tx` as it is produced. `tx` is
    /// bounded: a slow reader slows the query instead of buffering it.
    /// Default: [`Self::execute`], then the whole result as one chunk.
    async fn stream(
        &self,
        sql: &str,
        params: &[ScalarValue],
        viewer: Viewer,
        format: Format,
        tx: tokio::sync::mpsc::Sender<Chunk>,
    ) -> Result<Streamed, EngineError> {
        if format != Format::Ndjson {
            return Err(EngineError::Unsupported(format!("{format:?} results")));
        }
        let out = self.execute(sql, params, viewer).await?;
        let rows: Vec<Value> = serde_json::from_slice(&out.rows_json)
            .map_err(|e| EngineError::Internal(e.to_string()))?;
        let mut lines = Vec::new();
        for row in &rows {
            serde_json::to_writer(&mut lines, row)
                .map_err(|e| EngineError::Internal(e.to_string()))?;
            lines.push(b'\n');
        }
        tx.send(Chunk::Header(out.columns))
            .await
            .map_err(|_| receiver_gone())?;
        tx.send(Chunk::Rows(lines, out.memory))
            .await
            .map_err(|_| receiver_gone())?;
        Ok(Streamed {
            rows: rows.len() as u64,
            phases: out.phases,
            reads: out.reads,
            plan_events: out.plan_events,
            operators: out.operators,
        })
    }
}

/// `batch` as one JSON array per row, in `columns` order, handed to `row`.
fn each_row(
    batch: &RecordBatch,
    columns: &[String],
    mut row: impl FnMut(Vec<Value>) -> Result<(), EngineError>,
) -> Result<(), EngineError> {
    let internal = |e: &dyn std::fmt::Display| EngineError::Internal(e.to_string());
    let mut writer = ArrayWriter::new(Vec::new());
    writer
        .write(batch)
        .and_then(|()| writer.finish())
        .map_err(|e| internal(&e))?;
    let objects: Vec<serde_json::Map<String, Value>> =
        serde_json::from_slice(&writer.into_inner()).map_err(|e| internal(&e))?;
    for mut object in objects {
        // Arrow's writer omits nulls: absent columns are `null`.
        row(columns
            .iter()
            .map(|c| object.remove(c).unwrap_or(Value::Null))
            .collect())?;
    }
    Ok(())
}

pub struct CoreEngine(pub Arc<Engine>);

/// A worker task's output, and the plan producing it when there is one,
/// for its operator spans (ADR 0071).
pub struct Running {
    pub stream: SendableRecordBatchStream,
    pub plan: Option<Arc<dyn plimsoll_core::datafusion::physical_plan::ExecutionPlan>>,
}

impl From<SendableRecordBatchStream> for Running {
    fn from(stream: SendableRecordBatchStream) -> Self {
        Self { stream, plan: None }
    }
}

/// Plans and runs `df`, keeping the plan for its metrics.
async fn run(df: plimsoll_core::datafusion::prelude::DataFrame) -> Result<Running, EngineError> {
    let task = Arc::new(df.task_ctx());
    let plan = df.create_physical_plan().await.map_err(|e| classify(&e))?;
    let stream = plimsoll_core::datafusion::physical_plan::execute_stream(plan.clone(), task)
        .map_err(|e| classify(&e))?;
    Ok(Running {
        stream,
        plan: Some(plan),
    })
}

/// A broadcast join's build side: schema and batches.
pub type Build = (
    plimsoll_core::datafusion::arrow::datatypes::SchemaRef,
    Vec<RecordBatch>,
);

/// `build` (left, qualified `b`) joined with the fragment's rows `probe`
/// (right, `p`) as the coordinator's `CollectLeft` join (ADR 0066).
fn broadcast_join(
    ctx: &plimsoll_core::datafusion::prelude::SessionContext,
    spec: &plimsoll_connectors::remote::Broadcast,
    schema: plimsoll_core::datafusion::arrow::datatypes::SchemaRef,
    batches: Vec<RecordBatch>,
    probe: plimsoll_core::datafusion::prelude::DataFrame,
) -> Result<plimsoll_core::datafusion::prelude::DataFrame, EngineError> {
    use plimsoll_core::datafusion::common::{Column, JoinType};
    use plimsoll_core::datafusion::datasource::MemTable;
    use plimsoll_core::datafusion::prelude::Expr;
    let fail = |e: DataFusionError| classify(&e);
    let build = MemTable::try_new(schema, vec![batches])
        .and_then(|t| ctx.read_table(Arc::new(t)))
        .and_then(|df| df.alias("b"))
        .map_err(fail)?;
    let on = spec.build_keys.iter().zip(&spec.probe_keys).map(|(a, b)| {
        Expr::Column(Column::new(Some("b"), a)).eq(Expr::Column(Column::new(Some("p"), b)))
    });
    let join_type: JoinType = spec.join_type.parse().map_err(fail)?;
    let df = build
        .join_on(probe.alias("p").map_err(fail)?, join_type, on)
        .map_err(fail)?;
    project(df, spec.projection.as_deref())
}

/// `df` with only the columns at `projection`, if any (a join's own
/// output projection).
fn project(
    df: plimsoll_core::datafusion::prelude::DataFrame,
    projection: Option<&[usize]>,
) -> Result<plimsoll_core::datafusion::prelude::DataFrame, EngineError> {
    let Some(p) = projection else {
        return Ok(df);
    };
    let columns = df.schema().columns();
    let kept = p
        .iter()
        .map(|&i| {
            columns
                .get(i)
                .cloned()
                .map(plimsoll_core::datafusion::prelude::Expr::Column)
        })
        .collect::<Option<Vec<_>>>()
        .ok_or_else(|| EngineError::InvalidQuery("join projection out of range".into()))?;
    df.select(kept).map_err(|e| classify(&e))
}

/// Bucket `r.partition` of every one of `maps` (numbered from `offset`
/// among the shuffle's maps, hashed by `keys`), opened concurrently, as a
/// table with one partition per map that streams as the reduce reads it
/// (ADR 0064, 0065, 0068).
///
/// A map whose worker cannot be reached (a retryable failure before the
/// bucket's schema) runs again on another of `peers`, for this bucket
/// only, under its own shuffle id (ADR 0072): nothing of it was read yet.
async fn open_side(
    r: &Reduce,
    maps: &[(String, Fragment)],
    offset: usize,
    keys: impl Fn(&Fragment) -> Result<Vec<usize>, EngineError>,
    peers: &[String],
    token: &str,
    tag: &Option<Arc<QueryTag>>,
) -> Result<Arc<StreamingTable>, EngineError> {
    let opens = maps
        .iter()
        .enumerate()
        .map(|(i, (url, f))| {
            let map = offset + i;
            let mut f = f.clone();
            let bucket = Bucket {
                shuffle: r.shuffle.clone(),
                map,
                partitions: r.partitions,
                index: r.partition,
                keys: keys(&f)?,
                only: r.only,
            };
            let retry = Bucket {
                shuffle: format!("{}~m{map}r{}", r.shuffle, r.partition),
                only: true,
                ..bucket.clone()
            };
            let others: Vec<&String> = peers.iter().filter(|p| *p != url).collect();
            let fallback = (!others.is_empty()).then(|| others[map % others.len()].clone());
            f.bucket = Some(bucket);
            let (url, tag) = (url.clone(), tag.clone());
            Ok(async move {
                match open_bucket(url, token, &f, tag.clone()).await {
                    Err(e) if plimsoll_connectors::remote::retryable(&e) => {
                        let Some(fallback) = fallback else {
                            return Err(e);
                        };
                        f.bucket = Some(retry);
                        open_bucket(fallback, token, &f, tag)
                            .await
                            .map_err(|again| {
                                DataFusionError::External(
                                    format!("{again} (map re-run; first attempt: {e})").into(),
                                )
                            })
                    }
                    other => other,
                }
            })
        })
        .collect::<Result<Vec<_>, EngineError>>()?;
    let opened = futures::future::try_join_all(opens)
        .await
        .map_err(|e| classify(&e))?;
    let Some(schema) = opened.first().map(|(s, _)| s.clone()) else {
        return Err(EngineError::InvalidQuery("a reduce side needs maps".into()));
    };
    // Every map's batches in the first map's schema (same version, same
    // types; `decode` casts the rest).
    let partitions = opened
        .into_iter()
        .map(|(_, batches)| {
            let target = schema.clone();
            let batches = batches
                .map(move |b| b.and_then(|b| plimsoll_connectors::remote::conform(&b, &target)))
                .boxed();
            Arc::new(Bucketed {
                schema: schema.clone(),
                batches: std::sync::Mutex::new(Some(batches)),
            }) as Arc<dyn PartitionStream>
        })
        .collect();
    StreamingTable::try_new(schema, partitions)
        .map(Arc::new)
        .map_err(|e| classify(&e))
}

/// A fetched bucket as a partition of a [`StreamingTable`]: its batches
/// can be read once.
struct Bucketed {
    schema: plimsoll_core::datafusion::arrow::datatypes::SchemaRef,
    batches: std::sync::Mutex<
        Option<
            futures::stream::BoxStream<
                'static,
                plimsoll_core::datafusion::error::Result<RecordBatch>,
            >,
        >,
    >,
}

impl std::fmt::Debug for Bucketed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Bucketed")
    }
}

impl PartitionStream for Bucketed {
    fn schema(&self) -> &plimsoll_core::datafusion::arrow::datatypes::SchemaRef {
        &self.schema
    }

    fn execute(
        &self,
        _ctx: Arc<plimsoll_core::datafusion::execution::TaskContext>,
    ) -> SendableRecordBatchStream {
        use plimsoll_core::datafusion::physical_plan::stream::RecordBatchStreamAdapter;
        let batches = self
            .batches
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
            .unwrap_or_else(|| {
                futures::stream::iter([Err(DataFusionError::Internal(
                    "a shuffle bucket is read once".into(),
                ))])
                .boxed()
            });
        Box::pin(RecordBatchStreamAdapter::new(self.schema.clone(), batches))
    }
}

/// Shuffle maps kept on this worker until every bucket is fetched, or
/// until they expire (ADR 0064). Keyed by (shuffle, map), so workers that
/// share a process (tests) never collide.
type MapCell = tokio::sync::OnceCell<Result<Arc<std::sync::Mutex<ShuffleMap>>, String>>;
type Maps = std::collections::HashMap<(String, usize), (Instant, Arc<MapCell>)>;
static MAPS: std::sync::LazyLock<std::sync::Mutex<Maps>> =
    std::sync::LazyLock::new(Default::default);

/// Shuffles released on this worker (ADR 0073), by base id (before any
/// `~`), with when to forget them: a straggling bucket request for one is
/// refused instead of starting its map again (ADR 0074).
static RELEASED: std::sync::LazyLock<std::sync::Mutex<std::collections::HashMap<String, Instant>>> =
    std::sync::LazyLock::new(Default::default);

/// How long a released id is remembered: longer than any query runs.
const RELEASED_FOR: Duration = Duration::from_secs(600);

/// How often expired maps and released ids are swept (ADR 0074).
const SWEEP_EVERY: Duration = Duration::from_secs(5);
static SWEEPER: std::sync::OnceLock<()> = std::sync::OnceLock::new();

/// The shuffle a (possibly retry-derived, ADR 0072) id belongs to.
fn base(shuffle: &str) -> &str {
    shuffle.split('~').next().unwrap_or(shuffle)
}

/// Drops maps past their query's deadline and forgets old releases.
fn sweep() {
    let now = Instant::now();
    MAPS.lock()
        .unwrap_or_else(|e| e.into_inner())
        .retain(|_, (expires, _)| *expires > now);
    RELEASED
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .retain(|_, until| *until > now);
}

struct ShuffleMap {
    schema: plimsoll_core::datafusion::arrow::datatypes::SchemaRef,
    /// `None` once fetched.
    buckets: Vec<Option<Kept>>,
    /// The in-memory buckets.
    memory: Arc<std::sync::Mutex<Reservation>>,
}

/// Where a shuffle bucket waits to be fetched (ADR 0067).
enum Kept {
    Memory(Vec<RecordBatch>),
    /// Arrow IPC in a spill file, deleted when dropped.
    Disk(Arc<dyn SpillFile>),
}

type SpillStream = StreamWriter<Box<dyn SpillWriter>>;

/// One output partition of a shuffle map, kept as it streams in: in
/// memory while the pool grants it, then (everything so far, and the
/// rest) in a spill file.
async fn keep(
    mut stream: SendableRecordBatchStream,
    memory: Arc<std::sync::Mutex<Reservation>>,
    disk: Arc<plimsoll_core::datafusion::execution::DiskManager>,
) -> Result<Kept, String> {
    let lock = || memory.lock().unwrap_or_else(|e| e.into_inner());
    let schema = stream.schema();
    let mut held = Vec::new();
    let mut spill: Option<(Arc<dyn SpillFile>, SpillStream)> = None;
    let fail = |e: &dyn std::fmt::Display| e.to_string();
    while let Some(batch) = stream.next().await {
        let batch = batch.map_err(|e| fail(&e))?;
        let size = batch.get_array_memory_size();
        if spill.is_none() && lock().try_grow(size).is_err() {
            let file = disk
                .create_tmp_file("shuffle bucket")
                .map_err(|e| fail(&e))?;
            let writer = file.open_writer().map_err(|e| fail(&e))?;
            let mut writer = StreamWriter::try_new(writer, &schema).map_err(|e| fail(&e))?;
            for b in held.drain(..) {
                writer.write(&b).map_err(|e| fail(&e))?;
                lock().shrink(RecordBatch::get_array_memory_size(&b));
            }
            spill = Some((file, writer));
        }
        match &mut spill {
            Some((_, writer)) => writer.write(&batch).map_err(|e| fail(&e))?,
            None => held.push(batch),
        }
    }
    Ok(match spill {
        None => Kept::Memory(held),
        Some((file, mut writer)) => {
            writer.finish().map_err(|e| fail(&e))?;
            writer
                .into_inner()
                .map_err(|e| fail(&e))?
                .finish()
                .map_err(|e| fail(&e))?;
            Kept::Disk(file)
        }
    })
}

impl CoreEngine {
    /// Runs the fragment `df` once per (shuffle, map), keeps its output
    /// hash-partitioned by the `bucket.keys` columns (group keys, ADR 0064,
    /// or join keys, ADR 0065) and hands out bucket `bucket.index`, each
    /// bucket once.
    async fn map_bucket(
        &self,
        df: plimsoll_core::datafusion::prelude::DataFrame,
        bucket: Bucket,
        deadline: Instant,
    ) -> Result<SendableRecordBatchStream, EngineError> {
        use plimsoll_core::datafusion::physical_expr::expressions::Column;
        use plimsoll_core::datafusion::physical_plan::memory::MemoryStream;
        use plimsoll_core::datafusion::physical_plan::repartition::RepartitionExec;
        use plimsoll_core::datafusion::physical_plan::stream::RecordBatchStreamAdapter;
        use plimsoll_core::datafusion::physical_plan::{ExecutionPlan, Partitioning};
        let lock = || MAPS.lock().unwrap_or_else(|e| e.into_inner());
        let key = (bucket.shuffle.clone(), bucket.map);
        // A map lives no longer than the query that asked for it: its
        // reducers cannot need it past the deadline (ADR 0074). A sweeper
        // drops the expired ones, even when no other shuffle comes.
        SWEEPER.get_or_init(|| {
            tokio::spawn(async {
                loop {
                    tokio::time::sleep(SWEEP_EVERY).await;
                    sweep();
                }
            });
        });
        let cell = lock()
            .entry(key.clone())
            .or_insert_with(|| (deadline, Arc::default()))
            .1
            .clone();
        let partitions = bucket.partitions;
        let keys = bucket.keys.clone();
        let (shuffle, map) = (bucket.shuffle.clone(), bucket.map);
        let (only, wanted_index) = (bucket.only, bucket.index);
        let map = cell
            .get_or_init(|| async move {
                let fail = |e: DataFusionError| classify(&e).to_string();
                let task = df.task_ctx();
                let plan = df.create_physical_plan().await.map_err(fail)?;
                let schema = plan.schema();
                let hash = keys
                    .iter()
                    .map(|&i| {
                        let field = schema.fields().get(i).ok_or("bucket key out of range")?;
                        Ok(Arc::new(Column::new(field.name(), i)) as _)
                    })
                    .collect::<Result<_, String>>()?;
                // Hash-partitioned after DataFusion's optimizer, so it stays
                // as asked. Its hash is seeded with a constant: every
                // worker sends a group to the same bucket.
                let plan = RepartitionExec::try_new(plan, Partitioning::Hash(hash, partitions))
                    .map_err(fail)?;
                // Every bucket is drained concurrently (the repartition
                // feeds them all), each kept in memory or spilled.
                let plan: Arc<dyn ExecutionPlan> = Arc::new(plan);
                let task = Arc::new(task);
                let memory = Arc::new(std::sync::Mutex::new(
                    self.0
                        .pool()
                        .register("shuffle-map", ConsumerKind::Spillable),
                ));
                let disk = task.runtime_env().disk_manager.clone();
                let parts = (0..partitions)
                    .map(|i| {
                        let mut stream = plan.execute(i, task.clone()).map_err(fail)?;
                        let (memory, disk) = (memory.clone(), disk.clone());
                        // A map re-run for one reducer (ADR 0072) keeps
                        // that reducer's bucket and drops the rest.
                        let wanted = !only || i == wanted_index;
                        Ok(async move {
                            if wanted {
                                return keep(stream, memory, disk).await.map(Some);
                            }
                            while let Some(b) = stream.next().await {
                                b.map_err(|e| e.to_string())?;
                            }
                            Ok(None)
                        })
                    })
                    .collect::<Result<Vec<_>, String>>()?;
                let parts = futures::future::try_join_all(parts).await?;
                // Spilled buckets count as spill, and say so (ADR 0069).
                for (index, part) in parts.iter().enumerate() {
                    if let Some(Kept::Disk(file)) = part {
                        let bytes = file.size().unwrap_or(0);
                        self.0.record_spill(bytes);
                        crate::log::emit(
                            crate::log::Level::Info,
                            "shuffle_spill",
                            serde_json::json!({
                                "shuffle": shuffle,
                                "map": map,
                                "bucket": index,
                                "bytes": bytes,
                            }),
                        );
                    }
                }
                Ok(Arc::new(std::sync::Mutex::new(ShuffleMap {
                    schema,
                    buckets: parts,
                    memory,
                })))
            })
            .await
            .clone()
            .map_err(EngineError::Internal)?;
        let (schema, batches, done) = {
            let mut m = map.lock().unwrap_or_else(|e| e.into_inner());
            if m.buckets.len() != partitions {
                return Err(EngineError::InvalidQuery(format!(
                    "shuffle {} has {} buckets, not {partitions}",
                    bucket.shuffle,
                    m.buckets.len()
                )));
            }
            let kept = m.buckets.get_mut(bucket.index).and_then(Option::take);
            if let Some(Kept::Memory(batches)) = &kept {
                let size = batches.iter().map(RecordBatch::get_array_memory_size).sum();
                m.memory
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .shrink(size);
            }
            (
                m.schema.clone(),
                kept,
                m.buckets.iter().all(Option::is_none),
            )
        };
        if done {
            lock().remove(&key);
        }
        let kept = batches.ok_or_else(|| {
            EngineError::InvalidQuery(format!(
                "bucket {} of shuffle {} map {} was already fetched",
                bucket.index, bucket.shuffle, bucket.map
            ))
        })?;
        Ok(match kept {
            Kept::Memory(batches) => {
                Box::pin(MemoryStream::try_new(batches, schema, None).map_err(|e| classify(&e))?)
            }
            // Read back as it is sent; the file lives until the stream ends.
            Kept::Disk(file) => {
                let bytes = file.read_stream().map_err(|e| classify(&e))?;
                let batches = plimsoll_connectors::remote::decode(bytes.boxed(), schema.clone())
                    .map(move |b| {
                        let _file = &file;
                        b
                    });
                Box::pin(RecordBatchStreamAdapter::new(schema, batches))
            }
        })
    }
}

#[async_trait]
impl QueryEngine for CoreEngine {
    async fn fragment(
        &self,
        f: Fragment,
        build: Option<Build>,
        deadline: Instant,
    ) -> Result<Running, EngineError> {
        let fail = |e: DataFusionError| classify(&e);
        // A straggler of a query that already ended (ADR 0074).
        if let Some(b) = &f.bucket {
            let released = RELEASED
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .contains_key(base(&b.shuffle));
            if released {
                return Err(EngineError::InvalidQuery(format!(
                    "shuffle {} was released: its query has ended",
                    b.shuffle
                )));
            }
        }
        let state = self.0.context().state();
        let connector = IcebergConnector::fragment(
            &f.metadata_location,
            f.snapshot_id,
            f.files.into_iter().collect(),
            &state,
        )
        .await
        .map_err(|e| fail(e.into()))?;
        let table = connector.source();
        // Built on the provider directly (`read_table`): no SQL statement,
        // and nothing registered in the catalog the worker's users share.
        // The worker's own ceiling and runtime apply, as for any scan.
        use plimsoll_core::datafusion::prelude::{SessionContext, ident, lit};
        let ctx = SessionContext::new_with_state(state);
        let mut df = ctx.read_table(table).map_err(fail)?;
        // Row filters (ADR 0062) first, then projection or aggregation.
        for sql in &f.filters {
            let filter = df.parse_sql_expr(sql).map_err(fail)?;
            df = df.filter(filter).map_err(fail)?;
        }
        // Partial aggregation (ADR 0061).
        if let Some(spec) = &f.aggregate {
            use plimsoll_core::datafusion::functions_aggregate::expr_fn::{count, max, min, sum};
            // Keys and arguments are SQL over the table (ADR 0063).
            let parse = |sql: &str| df.parse_sql_expr(sql).map_err(fail);
            let groups = spec
                .group_by
                .iter()
                .map(|g| parse(g))
                .collect::<Result<Vec<_>, _>>()?;
            let aggregates = spec
                .aggregates
                .iter()
                .enumerate()
                .map(|(i, (f, c))| {
                    let arg = || match c.as_deref() {
                        Some(c) => parse(c),
                        None => Err(EngineError::InvalidQuery(format!("{f} needs an argument"))),
                    };
                    let e = match f.as_str() {
                        "count" => match c {
                            None => count(lit(1)),
                            Some(_) => count(arg()?),
                        },
                        "sum" => sum(arg()?),
                        "min" => min(arg()?),
                        "max" => max(arg()?),
                        other => {
                            return Err(EngineError::InvalidQuery(format!(
                                "aggregate {other} is not partial-safe"
                            )));
                        }
                    };
                    Ok(e.alias(format!("a{i}")))
                })
                .collect::<Result<Vec<_>, EngineError>>()?;
            df = df.aggregate(groups, aggregates).map_err(fail)?;
        } else {
            df = df.select(f.columns.iter().map(ident)).map_err(fail)?;
        }
        if let Some(b) = &f.broadcast {
            let Some((schema, batches)) = build else {
                return Err(EngineError::InvalidQuery(
                    "a broadcast join needs its build side attached".into(),
                ));
            };
            df = broadcast_join(&ctx, b, schema, batches, df)?;
        }
        match f.bucket {
            Some(bucket) => self
                .map_bucket(df, bucket, deadline)
                .await
                .map(Running::from),
            None => run(df).await,
        }
    }

    async fn release(&self, shuffle: &str) -> Result<Running, EngineError> {
        use plimsoll_core::datafusion::arrow::datatypes::Schema;
        use plimsoll_core::datafusion::physical_plan::stream::RecordBatchStreamAdapter;
        let derived = format!("{shuffle}~");
        RELEASED
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(base(shuffle).to_owned(), Instant::now() + RELEASED_FOR);
        let released = {
            let mut maps = MAPS.lock().unwrap_or_else(|e| e.into_inner());
            let before = maps.len();
            // Dropping a map drops its buckets: reservations shrink and
            // spill files are deleted (ADR 0067).
            maps.retain(|(id, _), _| id != shuffle && !id.starts_with(&derived));
            before - maps.len()
        };
        if released > 0 {
            crate::log::emit(
                crate::log::Level::Info,
                "shuffle_release",
                serde_json::json!({"shuffle": shuffle, "maps": released}),
            );
        }
        let schema = Arc::new(Schema::empty());
        Ok(Running::from(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            futures::stream::empty(),
        )) as SendableRecordBatchStream))
    }

    async fn reduce(
        &self,
        r: Reduce,
        token: &str,
        tag: Option<Arc<QueryTag>>,
    ) -> Result<Running, EngineError> {
        use plimsoll_core::datafusion::common::{Column, JoinType};
        use plimsoll_core::datafusion::functions_aggregate::expr_fn::{max, min, sum};
        use plimsoll_core::datafusion::prelude::{Expr, SessionContext};
        let fail = |e: DataFusionError| classify(&e);
        // Join keys by position in each map's columns; group keys lead an
        // aggregated map's output.
        let positions = |names: &[String]| {
            let names = names.to_vec();
            move |f: &Fragment| {
                names
                    .iter()
                    .map(|k| {
                        f.columns.iter().position(|c| c == k).ok_or_else(|| {
                            EngineError::InvalidQuery(format!("join key {k} is not a map column"))
                        })
                    })
                    .collect()
            }
        };
        let groups =
            |f: &Fragment| Ok((0..f.aggregate.as_ref().map_or(0, |a| a.group_by.len())).collect());
        // Every worker holding a map: where a lost map may run again.
        let mut peers: Vec<String> = r
            .maps
            .iter()
            .chain(r.join.iter().flat_map(|j| &j.right))
            .map(|(url, _)| url.clone())
            .collect();
        peers.sort();
        peers.dedup();
        let (left, right) = match &r.join {
            None => (
                open_side(&r, &r.maps, 0, groups, &peers, token, &tag).await?,
                None,
            ),
            Some(j) => {
                let (l, r) = futures::future::try_join(
                    open_side(&r, &r.maps, 0, positions(&j.left_keys), &peers, token, &tag),
                    open_side(
                        &r,
                        &j.right,
                        r.maps.len(),
                        positions(&j.right_keys),
                        &peers,
                        token,
                        &tag,
                    ),
                )
                .await?;
                (l, Some(r))
            }
        };
        // The merge or join reads the buckets as they arrive; its own
        // operators account (and, for aggregation, spill) their state.
        let ctx = SessionContext::new_with_state(self.0.context().state());
        let read = |t: Arc<StreamingTable>| ctx.read_table(t).map_err(fail);
        let left = read(left)?;
        let df = match (&r.join, right) {
            (Some(j), Some(right)) => {
                // Qualified sides, so equal column names stay apart.
                let (l, rt) = (
                    left.alias("l").map_err(fail)?,
                    read(right)?.alias("r").map_err(fail)?,
                );
                let on = j.left_keys.iter().zip(&j.right_keys).map(|(a, b)| {
                    Expr::Column(Column::new(Some("l"), a))
                        .eq(Expr::Column(Column::new(Some("r"), b)))
                });
                let join_type: JoinType = j.join_type.parse().map_err(fail)?;
                let df = l.join_on(rt, join_type, on).map_err(fail)?;
                project(df, j.projection.as_deref())?
            }
            _ => {
                let Some(spec) = r.maps.first().and_then(|(_, f)| f.aggregate.clone()) else {
                    return Err(EngineError::InvalidQuery("a reduce needs maps".into()));
                };
                // Partial states merged: counts and sums add up, min and
                // max compose. Each merged state is the aggregate's final
                // value.
                let columns = left.schema().columns();
                let column = |i: usize| {
                    columns.get(i).cloned().map(Expr::Column).ok_or_else(|| {
                        EngineError::InvalidQuery("map output narrower than its aggregate".into())
                    })
                };
                let k = spec.group_by.len();
                let groups = (0..k).map(column).collect::<Result<Vec<_>, _>>()?;
                let merges = spec
                    .aggregates
                    .iter()
                    .enumerate()
                    .map(|(i, (f, _))| {
                        let state = column(k + i)?;
                        Ok(match f.as_str() {
                            "count" | "sum" => sum(state),
                            "min" => min(state),
                            "max" => max(state),
                            other => {
                                return Err(EngineError::InvalidQuery(format!(
                                    "aggregate {other} is not partial-safe"
                                )));
                            }
                        }
                        .alias(format!("a{i}")))
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                left.aggregate(groups, merges).map_err(fail)?
            }
        };
        run(df).await
    }

    async fn execute(
        &self,
        sql: &str,
        params: &[ScalarValue],
        viewer: Viewer,
    ) -> Result<QueryOutput, EngineError> {
        let plan_events;
        let QueryResult {
            batches,
            mut memory,
            planning,
            execution,
            reads,
            operators,
        } = {
            let (result, events) =
                plan_trace::collect(self.0.sql_with(sql, params, Arc::new(viewer))).await;
            plan_events = events;
            result.map_err(|e| classify(&e))?
        };
        let serializing = Instant::now();
        let columns: Vec<String> = batches
            .first()
            .map(|b| {
                b.schema()
                    .fields()
                    .iter()
                    .map(|f| f.name().clone())
                    .collect()
            })
            .unwrap_or_default();
        let internal = |e: &dyn std::fmt::Display| EngineError::Internal(e.to_string());
        let mut rows_json = b"[".to_vec();
        let mem_err = |e: plimsoll_core::memory::CeilingError| {
            EngineError::MemoryLimitExceeded(e.to_string())
        };
        memory.try_grow(2).map_err(mem_err)?; // `[` and `]`
        // Batch by batch: each batch leaves the pool as its JSON enters it, so
        // only one batch's transient JSON objects are ever unaccounted.
        for batch in batches {
            let before = rows_json.len();
            each_row(&batch, &columns, |row| {
                if rows_json.len() > 1 {
                    rows_json.push(b',');
                }
                serde_json::to_writer(&mut rows_json, &row).map_err(|e| internal(&e))
            })?;
            memory.shrink(batch.get_array_memory_size());
            drop(batch);
            memory.try_grow(rows_json.len() - before).map_err(mem_err)?;
        }
        rows_json.push(b']');
        Ok(QueryOutput {
            columns,
            rows_json,
            memory: Some(memory),
            phases: Phases {
                planning,
                execution,
                serialization: serializing.elapsed(),
            },
            reads,
            plan_events,
            operators,
        })
    }

    async fn stream(
        &self,
        sql: &str,
        params: &[ScalarValue],
        viewer: Viewer,
        format: Format,
        tx: tokio::sync::mpsc::Sender<Chunk>,
    ) -> Result<Streamed, EngineError> {
        let started = Instant::now();
        let viewer = Arc::new(viewer);
        let group = self.0.capped_group(viewer.as_ref());
        let (query, plan_events) =
            plan_trace::collect(self.0.sql_stream_with(sql, params, viewer)).await;
        let mut query = query.map_err(|e| classify(&e))?;
        let columns: Vec<String> = query
            .stream
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        tx.send(Chunk::Header(columns.clone()))
            .await
            .map_err(|_| receiver_gone())?;
        let internal = |e: &dyn std::fmt::Display| EngineError::Internal(e.to_string());
        // Arrow: the writer emits the schema message now; its bytes leave
        // with the first chunk.
        let mut ipc = match format {
            Format::Arrow => Some(
                StreamWriter::try_new(Vec::new(), &query.stream.schema())
                    .map_err(|e| internal(&e))?,
            ),
            Format::Ndjson => None,
        };
        let send = async |bytes: Vec<u8>| -> Result<(), EngineError> {
            // Reserved until the body takes it: the bytes in flight (the
            // channel plus the chunk being written) stay under the ceiling.
            let mut memory =
                self.0
                    .pool()
                    .register_in(group.as_ref(), "query-output", ConsumerKind::Output);
            memory
                .try_grow(bytes.len())
                .map_err(|e| EngineError::MemoryLimitExceeded(e.to_string()))?;
            tx.send(Chunk::Rows(bytes, Some(memory)))
                .await
                .map_err(|_| receiver_gone())
        };
        let (mut rows, mut serialization) = (0u64, Duration::ZERO);
        while let Some(batch) = query.stream.next().await {
            let batch = batch.map_err(|e| classify(&e))?;
            // Zero-copy slices bound each chunk, so the window in flight is
            // a few chunks whatever the batch size.
            let mut offset = 0;
            while offset < batch.num_rows() {
                let slice = batch.slice(offset, CHUNK_ROWS.min(batch.num_rows() - offset));
                offset += slice.num_rows();
                let serializing = Instant::now();
                let bytes = match &mut ipc {
                    Some(writer) => {
                        rows += slice.num_rows() as u64;
                        writer.write(&slice).map_err(|e| internal(&e))?;
                        std::mem::take(writer.get_mut())
                    }
                    None => {
                        let mut lines = Vec::new();
                        each_row(&slice, &columns, |row| {
                            rows += 1;
                            serde_json::to_writer(&mut lines, &row).map_err(|e| internal(&e))?;
                            lines.push(b'\n');
                            Ok(())
                        })?;
                        lines
                    }
                };
                serialization += serializing.elapsed();
                send(bytes).await?;
            }
        }
        // The end-of-stream marker (and, for an empty result, the schema):
        // a reader that never sees it knows the stream was cut short.
        if let Some(mut writer) = ipc {
            writer.finish().map_err(|e| internal(&e))?;
            send(std::mem::take(writer.get_mut())).await?;
        }
        let planning = query.planning;
        Ok(Streamed {
            rows,
            reads: std::mem::take(&mut query.reads),
            plan_events,
            operators: query.operators(),
            phases: Phases {
                planning,
                // Includes time spent waiting for the reader.
                execution: started.elapsed().saturating_sub(planning + serialization),
                serialization,
            },
        })
    }
}

/// Maps an engine error to its class (ADR 0013). Typed errors from
/// connectors, the catalog and object stores are found by downcasting; only
/// what nobody classified is `Internal`.
pub fn classify(e: &DataFusionError) -> EngineError {
    let msg = e.to_string();
    match e.find_root() {
        DataFusionError::ResourcesExhausted(m) => EngineError::MemoryLimitExceeded(m.clone()),
        DataFusionError::SQL(..)
        | DataFusionError::Plan(_)
        | DataFusionError::SchemaError(..)
        | DataFusionError::NotImplemented(_) => EngineError::InvalidQuery(msg),
        DataFusionError::ObjectStore(os) => object_store(os, msg),
        DataFusionError::ParquetError(p) => match p.as_ref() {
            ParquetError::External(inner) => external(inner.as_ref(), msg),
            // Undecodable Parquet referenced by the table.
            _ => EngineError::InvalidTable(msg),
        },
        DataFusionError::External(inner) => external(inner.as_ref(), msg),
        _ => EngineError::Internal(msg),
    }
}

fn external(inner: &(dyn std::error::Error + 'static), msg: String) -> EngineError {
    let dependency = |dependency| EngineError::Dependency {
        dependency,
        message: msg.clone(),
    };
    if inner.downcast_ref::<AccessDenied>().is_some() {
        return EngineError::Forbidden(msg);
    }
    if let Some(e) = inner.downcast_ref::<ConnectorError>() {
        return match e {
            ConnectorError::Unsupported(_) | ConnectorError::UnsupportedType { .. } => {
                EngineError::Unsupported(msg)
            }
            ConnectorError::InvalidMetadata(_) => EngineError::InvalidTable(msg),
            ConnectorError::PostgresUnavailable(_) => dependency("postgres"),
            ConnectorError::DataFusion(e) => classify(e),
            ConnectorError::MissingEnv(_)
            | ConnectorError::Postgres(_)
            | ConnectorError::Arrow(_) => EngineError::Internal(msg),
        };
    }
    if let Some(e) = inner.downcast_ref::<CatalogError>() {
        return match e {
            CatalogError::TableNotFound(_) | CatalogError::NamespaceNotFound(_) => {
                EngineError::InvalidQuery(msg)
            }
            CatalogError::Config(_) => EngineError::Internal(msg),
            _ => dependency("catalog"),
        };
    }
    if let Some(e) = inner.downcast_ref::<ObjectStoreError>() {
        return object_store(e, msg);
    }
    if let Some(e) = inner.downcast_ref::<DataFusionError>() {
        return classify(e);
    }
    EngineError::Internal(msg)
}

fn object_store(e: &ObjectStoreError, msg: String) -> EngineError {
    match e {
        // The table references a file that is gone: the table is broken.
        ObjectStoreError::NotFound { .. } => EngineError::InvalidTable(msg),
        _ => EngineError::Dependency {
            dependency: "object_store",
            message: msg,
        },
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use plimsoll_core::memory::CeilingPool;

    #[tokio::test]
    async fn shuffle_buckets_spill_to_disk_when_the_pool_says_no() {
        use plimsoll_core::datafusion::arrow::array::Int64Array;
        use plimsoll_core::datafusion::arrow::datatypes::{DataType, Field, Schema};
        use plimsoll_core::datafusion::execution::runtime_env::RuntimeEnvBuilder;
        use plimsoll_core::datafusion::physical_plan::memory::MemoryStream;
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = |from: i64| {
            RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(Int64Array::from_iter_values(from..from + 1000))],
            )
            .unwrap()
        };
        let batches: Vec<_> = (0..4).map(|i| batch(i * 1000)).collect();
        let disk = RuntimeEnvBuilder::new()
            .build()
            .unwrap()
            .disk_manager
            .clone();
        let kept = async |limit: usize| {
            let pool = Arc::new(CeilingPool::new(limit, 0).unwrap());
            let memory = Arc::new(std::sync::Mutex::new(
                pool.register("shuffle-map", ConsumerKind::Spillable),
            ));
            let stream = MemoryStream::try_new(batches.clone(), schema.clone(), None).unwrap();
            let kept = keep(Box::pin(stream), memory.clone(), disk.clone())
                .await
                .unwrap();
            let held = memory.lock().unwrap().size();
            (kept, held)
        };
        // Room for everything: kept in memory, all of it reserved.
        let (roomy, held) = kept(64 << 20).await;
        assert!(matches!(&roomy, Kept::Memory(b) if b.len() == 4));
        assert!(held > 0);
        // Room for a batch and a half: the bucket spills, the batch held
        // before is released, and every row reads back from the file.
        let (tight, held) = kept(batches[0].get_array_memory_size() * 3 / 2).await;
        let Kept::Disk(file) = tight else {
            panic!("expected a spilled bucket");
        };
        assert_eq!(held, 0);
        let back: Vec<RecordBatch> = plimsoll_connectors::remote::decode(
            file.read_stream().unwrap().boxed(),
            schema.clone(),
        )
        .map(|b| b.unwrap())
        .collect()
        .await;
        assert_eq!(back, batches);
        // The spill directory's size limit applies to shuffle buckets too:
        // past it, the map fails rather than fill the disk.
        let small = Arc::new(
            plimsoll_core::datafusion::execution::DiskManager::builder()
                .with_max_temp_directory_size(1024)
                .build()
                .unwrap(),
        );
        let pool = Arc::new(CeilingPool::new(1024, 0).unwrap());
        let memory = Arc::new(std::sync::Mutex::new(
            pool.register("shuffle-map", ConsumerKind::Spillable),
        ));
        let stream = MemoryStream::try_new(batches.clone(), schema.clone(), None).unwrap();
        let Err(e) = keep(Box::pin(stream), memory, small).await else {
            panic!("expected the temp directory limit");
        };
        assert!(e.contains("max_temp_directory_size"), "{e}");
    }

    #[tokio::test]
    async fn releasing_a_shuffle_drops_its_maps_and_its_retries() {
        let pool = Arc::new(CeilingPool::new(1 << 20, 1 << 17).unwrap());
        let e = CoreEngine(Arc::new(Engine::new(pool, None).unwrap()));
        let ids = ["rel-a", "rel-a~r2", "rel-a~m1r0", "rel-ab", "rel-b"];
        {
            let mut maps = MAPS.lock().unwrap();
            for id in ids {
                maps.insert(
                    (id.to_owned(), 0),
                    (Instant::now() + Duration::from_secs(60), Arc::default()),
                );
            }
        }
        e.release("rel-a").await.unwrap();
        let left: Vec<String> = MAPS
            .lock()
            .unwrap()
            .keys()
            .map(|(id, _)| id.clone())
            .filter(|id| id.starts_with("rel-"))
            .collect();
        // Its retries go with it; a shuffle that merely shares a prefix
        // does not.
        let mut left = left;
        left.sort();
        assert_eq!(left, ["rel-ab", "rel-b"]);
        // A straggling bucket request for it, or for one of its retries,
        // is refused before any table is opened (ADR 0074)...
        let fragment = |shuffle: &str| Fragment {
            metadata_location: "file:///nowhere/metadata/v1.metadata.json".into(),
            snapshot_id: None,
            files: vec![],
            columns: vec!["id".into()],
            aggregate: None,
            filters: vec![],
            bucket: Some(Bucket {
                shuffle: shuffle.into(),
                map: 0,
                partitions: 2,
                index: 0,
                keys: vec![0],
                only: false,
            }),
            broadcast: None,
        };
        let deadline = Instant::now() + Duration::from_secs(5);
        for id in ["rel-a", "rel-a~m1r0"] {
            let Err(e) = e.fragment(fragment(id), None, deadline).await else {
                panic!("{id} should be refused");
            };
            assert!(e.to_string().contains("was released"), "{e}");
        }
        // ...while another shuffle goes on to open its table.
        let Err(e) = e.fragment(fragment("rel-ab"), None, deadline).await else {
            panic!("no such table");
        };
        assert!(!e.to_string().contains("was released"), "{e}");
    }

    #[test]
    fn maps_expire_with_their_query() {
        let past = Instant::now() - Duration::from_millis(1);
        MAPS.lock()
            .unwrap()
            .insert(("exp-old".into(), 0), (past, Arc::default()));
        MAPS.lock().unwrap().insert(
            ("exp-live".into(), 0),
            (Instant::now() + Duration::from_secs(60), Arc::default()),
        );
        RELEASED.lock().unwrap().insert("exp-gone".into(), past);
        sweep();
        let maps = MAPS.lock().unwrap();
        assert!(!maps.contains_key(&("exp-old".into(), 0)));
        assert!(maps.contains_key(&("exp-live".into(), 0)));
        assert!(!RELEASED.lock().unwrap().contains_key("exp-gone"));
    }

    #[tokio::test]
    async fn sql_to_rows_and_typed_errors() {
        let pool = Arc::new(CeilingPool::new(1 << 20, 1 << 17).unwrap());
        let e = CoreEngine(Arc::new(Engine::new(pool, None).unwrap()));
        let out = e
            .execute("SELECT 1 AS a, NULL AS b", &[], Viewer::default())
            .await
            .unwrap();
        assert_eq!(out.columns, ["a", "b"]);
        let rows: Value = serde_json::from_slice(&out.rows_json).unwrap();
        assert_eq!(rows, serde_json::json!([[1, null]]));
        assert!(matches!(
            e.execute("SELEC", &[], Viewer::default()).await,
            Err(EngineError::InvalidQuery(_))
        ));
        assert!(matches!(
            e.execute(
                "SELECT value, repeat('x', 100) FROM range(200000)",
                &[],
                Viewer::default()
            )
            .await,
            Err(EngineError::MemoryLimitExceeded(_))
        ));
    }

    #[test]
    fn errors_are_classified_at_their_source() {
        let ext = |e: Box<dyn std::error::Error + Send + Sync>| DataFusionError::External(e);
        let class = |e: DataFusionError| classify(&e);
        assert!(matches!(
            class(ext(Box::new(ConnectorError::Unsupported(
                "delete files".into()
            )))),
            EngineError::Unsupported(_)
        ));
        assert!(matches!(
            class(ext(Box::new(ConnectorError::InvalidMetadata("bad".into())))),
            EngineError::InvalidTable(_)
        ));
        assert!(matches!(
            class(ext(Box::new(CatalogError::Http("refused".into())))),
            EngineError::Dependency {
                dependency: "catalog",
                ..
            }
        ));
        let os = |e| DataFusionError::ObjectStore(Box::new(e));
        assert!(matches!(
            class(os(ObjectStoreError::NotFound {
                path: "s3://wh/f.parquet".into(),
                source: "gone".into(),
            })),
            EngineError::InvalidTable(_)
        ));
        assert!(matches!(
            class(os(ObjectStoreError::PermissionDenied {
                path: "s3://wh/f.parquet".into(),
                source: "403".into(),
            })),
            EngineError::Dependency {
                dependency: "object_store",
                ..
            }
        ));
        // Wrapped in context, inside Parquet's external error, or nested in a
        // connector: still found.
        let nested = ext(Box::new(ConnectorError::DataFusion(ext(Box::new(
            CatalogError::Auth("401".into()),
        )))))
        .context("planning");
        assert!(matches!(
            class(nested),
            EngineError::Dependency {
                dependency: "catalog",
                ..
            }
        ));
        let pq = DataFusionError::ParquetError(Box::new(ParquetError::External(Box::new(
            ObjectStoreError::Generic {
                store: "S3",
                source: "timeout".into(),
            },
        ))));
        assert!(matches!(
            class(pq),
            EngineError::Dependency {
                dependency: "object_store",
                ..
            }
        ));
        assert!(matches!(
            class(DataFusionError::Internal("bug".into())),
            EngineError::Internal(_)
        ));
    }

    /// The JSON copy is accounted: a result whose Arrow form fits under the
    /// ceiling but whose JSON does not fails the query (and frees everything).
    #[tokio::test]
    async fn serialized_response_is_accounted() {
        let pool = Arc::new(CeilingPool::new(4 << 20, 2 << 20).unwrap());
        let e = CoreEngine(Arc::new(Engine::new(pool.clone(), None).unwrap()));
        let small = e
            .execute("SELECT value FROM range(1000)", &[], Viewer::default())
            .await
            .unwrap();
        let held = small.memory.as_ref().map(Reservation::size).unwrap();
        assert_eq!(
            held,
            small.rows_json.len(),
            "exactly the JSON bytes are reserved"
        );
        drop(small);
        assert_eq!(pool.used(), 0);

        // NULL columns are ~0 bytes as Arrow (NullArray) but ~22 bytes per
        // row as JSON (`[null,null,null,null]`): 200k rows only cross the
        // 4 MiB ceiling once serialized.
        let err = e
            .execute(
                "SELECT NULL AS a, NULL AS b, NULL AS c, NULL AS d FROM range(200000)",
                &[],
                Viewer::default(),
            )
            .await;
        assert!(matches!(err, Err(EngineError::MemoryLimitExceeded(_))));
        assert_eq!(pool.used(), 0);
    }

    /// A result too big for the ceiling as one document streams through it:
    /// only the chunks in flight are held (ADR 0035).
    #[tokio::test]
    async fn streamed_results_are_bounded_by_the_window_not_the_result() {
        let pool = Arc::new(CeilingPool::new(1 << 20, 1 << 17).unwrap());
        let e = CoreEngine(Arc::new(Engine::new(pool.clone(), None).unwrap()));
        let sql = "SELECT value, repeat('x', 100) FROM range(200000)";
        assert!(matches!(
            e.execute(sql, &[], Viewer::default()).await,
            Err(EngineError::MemoryLimitExceeded(_))
        ));
        let (tx, mut rx) = tokio::sync::mpsc::channel(2);
        let reader = tokio::spawn(async move {
            let (mut header, mut lines, mut peak) = (None, 0usize, 0usize);
            while let Some(chunk) = rx.recv().await {
                match chunk {
                    Chunk::Header(c) => header = Some(c),
                    Chunk::Rows(bytes, memory) => {
                        peak = peak.max(memory.as_ref().map_or(0, |m| m.size()));
                        lines += bytes.iter().filter(|b| **b == b'\n').count();
                    }
                }
            }
            (header, lines, peak)
        });
        let done = e
            .stream(sql, &[], Viewer::default(), Format::Ndjson, tx)
            .await
            .unwrap();
        let (header, lines, peak) = reader.await.unwrap();
        assert_eq!(header.unwrap().len(), 2);
        assert_eq!((done.rows, lines), (200_000, 200_000));
        assert!(peak > 0 && peak < 1 << 20, "chunks are reserved: {peak}");
        assert_eq!(pool.used(), 0);

        // Planning errors surface before any chunk.
        let (tx, mut rx) = tokio::sync::mpsc::channel(2);
        assert!(matches!(
            e.stream("SELEC", &[], Viewer::default(), Format::Ndjson, tx)
                .await,
            Err(EngineError::InvalidQuery(_))
        ));
        assert!(rx.recv().await.is_none());
    }

    /// Arrow IPC: the chunks concatenate into one valid stream (schema,
    /// batches, end-of-stream), bounded like NDJSON (ADR 0037).
    #[tokio::test]
    async fn arrow_stream_decodes_and_is_bounded() {
        use plimsoll_core::datafusion::arrow::ipc::reader::StreamReader;
        let pool = Arc::new(CeilingPool::new(1 << 20, 1 << 17).unwrap());
        let e = Arc::new(CoreEngine(Arc::new(
            Engine::new(pool.clone(), None).unwrap(),
        )));
        let collect = async |sql: &'static str| {
            let (tx, mut rx) = tokio::sync::mpsc::channel(2);
            let e = e.clone();
            let run = tokio::spawn(async move {
                e.stream(sql, &[], Viewer::default(), Format::Arrow, tx)
                    .await
            });
            let mut bytes = Vec::new();
            while let Some(chunk) = rx.recv().await {
                if let Chunk::Rows(b, _) = chunk {
                    bytes.extend(b);
                }
            }
            (run.await.unwrap(), bytes)
        };
        let (done, bytes) =
            collect("SELECT value, repeat('x', 100) AS pad FROM range(200000)").await;
        assert_eq!(done.unwrap().rows, 200_000);
        let reader = StreamReader::try_new(std::io::Cursor::new(bytes), None).unwrap();
        assert_eq!(reader.schema().field(1).name(), "pad");
        let rows: usize = reader.map(|b| b.unwrap().num_rows()).sum();
        assert_eq!(rows, 200_000);
        assert_eq!(pool.used(), 0);

        // Empty result: still a complete stream with the schema.
        let (done, bytes) = collect("SELECT value FROM range(10) WHERE value > 99").await;
        assert_eq!(done.unwrap().rows, 0);
        let reader = StreamReader::try_new(std::io::Cursor::new(bytes), None).unwrap();
        assert_eq!(reader.schema().fields().len(), 1);
        assert_eq!(reader.count(), 0);
    }

    /// Nested values (ADR 0041) come out as JSON objects and arrays, in
    /// both the document and the streamed formats.
    #[tokio::test]
    async fn nested_values_serialize_as_json() {
        let pool = Arc::new(CeilingPool::new(16 << 20, 1 << 20).unwrap());
        let e = CoreEngine(Arc::new(Engine::new(pool, None).unwrap()));
        let sql = "SELECT named_struct('a', 1, 'b', 'x') AS s, make_array(1, 2) AS l, \
                   map(['k'], [7]) AS m";
        let out = e.execute(sql, &[], Viewer::default()).await.unwrap();
        let rows: Value = serde_json::from_slice(&out.rows_json).unwrap();
        assert_eq!(
            rows,
            serde_json::json!([[{"a": 1, "b": "x"}, [1, 2], {"k": 7}]])
        );
    }
}

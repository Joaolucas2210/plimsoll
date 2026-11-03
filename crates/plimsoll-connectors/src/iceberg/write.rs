//! Appends to Iceberg tables (ADR 0075, 0076).
//!
//! `INSERT INTO t SELECT …` reaches [`IcebergSink`] through DataFusion's
//! `insert_into`. The sink routes each row to its partition (the default
//! spec's transforms, computed by the same code the pruner maps
//! predicates with), writes each partition's rows as Parquet data files
//! under the table's `data/` with per-column statistics, then one
//! manifest listing them and a manifest list carrying the parent
//! snapshot's manifests plus the new one, and commits a snapshot through
//! the table's [`Committer`] (the REST catalog's `updateTable`). A commit
//! that loses a race (another writer committed first) is rebuilt on the
//! new parent and retried; the data files and the manifest are reused.
//!
//! Scope: format v2, primitive columns; partition transforms identity,
//! bucket, truncate, year, month, day and hour over the types the pruner
//! understands. Anything else is refused at planning, before a byte is
//! written.

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use apache_avro::Schema as AvroSchema;
use apache_avro::types::Value as Avro;
use async_trait::async_trait;
use datafusion::arrow::array::{Array, ArrayRef, RecordBatch, UInt32Array};
use datafusion::arrow::datatypes::{DataType, SchemaRef, TimeUnit};
use datafusion::common::ScalarValue;
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::datasource::sink::DataSink;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::memory_pool::MemoryConsumer;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::object_store::{ObjectStoreExt, PutPayload};
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType};
use futures::StreamExt;
use plimsoll_core::storage::ScopedStore;
use serde_json::{Value as Json, json};

use super::prune::{Datum, Prim, Transform, partition_spec, partition_value};
use super::{read_env, store_url};

/// Where a table's commits go: its catalog.
#[async_trait]
pub trait Committer: Send + Sync + fmt::Debug {
    /// The table's current metadata, fresh from the catalog.
    async fn current(&self) -> std::result::Result<Json, String>;

    /// Commits `updates` if `requirements` still hold.
    async fn commit(
        &self,
        requirements: Vec<Json>,
        updates: Vec<Json>,
    ) -> std::result::Result<(), CommitError>;
}

#[derive(Debug)]
pub enum CommitError {
    /// A requirement failed: another writer committed first. Retryable.
    Conflict(String),
    Failed(String),
}

/// Commits that lose a race are rebuilt and retried this many times.
const COMMIT_ATTEMPTS: usize = 4;

/// Data files roll over at about this size (Parquet bytes buffered).
const TARGET_FILE_BYTES: usize = 128 << 20;

/// Partitions open at once; past this, every open file is finished, so a
/// high-cardinality insert writes more, smaller files rather than hold
/// unbounded writers.
const OPEN_PARTITIONS: usize = 512;

/// String bounds keep this many characters, as Iceberg's default
/// `truncate(16)` metrics mode.
const BOUND_CHARS: usize = 16;

/// How a table's rows are laid out in files: its columns' ids and types
/// and its default partition spec, as the writer needs them.
#[derive(Debug, Clone)]
pub(super) struct Layout {
    spec_id: i64,
    /// `partition-specs[].fields` of the default spec, verbatim.
    spec_fields: Json,
    /// Per table column (current schema order): field id, and its type
    /// when bounds can be recorded for it.
    columns: Vec<(i64, Option<Prim>)>,
    parts: Vec<Part>,
}

/// One partition field of the default spec.
#[derive(Debug, Clone)]
struct Part {
    name: String,
    field_id: i64,
    /// Index of the source column.
    column: usize,
    source: Prim,
    transform: Transform,
    /// The partition value's type.
    out: Prim,
}

impl Layout {
    pub(super) fn spec_id(&self) -> i64 {
        self.spec_id
    }

    /// Why `meta` cannot take an append, as an `Err`.
    pub(super) fn of(meta: &Json) -> std::result::Result<Self, String> {
        if meta["format-version"].as_i64() != Some(2) {
            return Err(format!(
                "INSERT supports Iceberg format v2 tables (this one is v{})",
                meta["format-version"]
            ));
        }
        let schema = current_schema(meta).ok_or("the current schema is missing")?;
        let fields: Vec<&Json> = schema["fields"].as_array().into_iter().flatten().collect();
        if fields.iter().any(|f| !f["type"].is_string()) {
            return Err("INSERT into tables with nested columns is not supported yet".into());
        }
        let columns: Vec<(i64, Option<Prim>)> = fields
            .iter()
            .map(|f| {
                (
                    f["id"].as_i64().unwrap_or(-1),
                    f["type"].as_str().and_then(Prim::from_iceberg),
                )
            })
            .collect();
        let spec_id = meta["default-spec-id"].as_i64().unwrap_or(0);
        let spec_fields = meta["partition-specs"]
            .as_array()
            .into_iter()
            .flatten()
            .find(|s| s["spec-id"].as_i64() == Some(spec_id))
            .map(|s| s["fields"].clone())
            .unwrap_or_else(|| json!([]));
        let parsed = partition_spec(&spec_fields);
        let mut parts = Vec::new();
        for (raw, field) in spec_fields.as_array().into_iter().flatten().zip(parsed) {
            let name = raw["name"].as_str().unwrap_or("?").to_owned();
            let unsupported = |why: &str| {
                Err(format!(
                    "INSERT into a table partitioned by {name} ({}): {why}",
                    raw["transform"].as_str().unwrap_or("?")
                ))
            };
            let Some(field) = field else {
                return unsupported("transform not supported");
            };
            let Some(column) = columns.iter().position(|(id, _)| *id == field.source_id) else {
                return unsupported("source column not in the current schema");
            };
            let Some(source) = columns[column].1 else {
                return unsupported("source type not supported");
            };
            // Decimal partition values need Avro decimal types; booleans
            // have no bucket hash.
            let out = match (field.transform, source) {
                (Transform::Identity | Transform::Truncate(_), Prim::Decimal(_)) => {
                    return unsupported("decimal partition values not supported yet");
                }
                (Transform::Identity, p) => p,
                (Transform::Truncate(_), p @ (Prim::Int | Prim::Long | Prim::String)) => p,
                (Transform::Bucket(_), Prim::Bool) => return unsupported("bucket of a boolean"),
                (Transform::Bucket(_), _) => Prim::Int,
                (
                    Transform::Year | Transform::Month | Transform::Day,
                    Prim::Date | Prim::Timestamp,
                )
                | (Transform::Hour, Prim::Timestamp) => Prim::Int,
                _ => return unsupported("transform does not apply to the source type"),
            };
            parts.push(Part {
                name,
                field_id: raw["field-id"].as_i64().unwrap_or(-1),
                column,
                source,
                transform: field.transform,
                out,
            });
        }
        Ok(Self {
            spec_id,
            spec_fields,
            columns,
            parts,
        })
    }
}

fn current_schema(meta: &Json) -> Option<&Json> {
    let id = meta["current-schema-id"].as_i64()?;
    meta["schemas"]
        .as_array()?
        .iter()
        .find(|s| s["schema-id"].as_i64() == Some(id))
}

/// The `INSERT INTO` sink of one Iceberg table.
pub(super) struct IcebergSink {
    pub(super) name: String,
    /// The table's Arrow schema with Parquet field ids: what is written.
    pub(super) schema: SchemaRef,
    /// Table base location.
    pub(super) location: String,
    pub(super) scoped: Option<ScopedStore>,
    pub(super) committer: Arc<dyn Committer>,
    /// The schema id the files are written against; a concurrent schema
    /// change fails the commit rather than mislabel them.
    pub(super) schema_id: i64,
    pub(super) layout: Layout,
    /// The snapshot rows were read from when planning: deletes and
    /// rewrites commit only if nothing but appends happened since.
    pub(super) base: Option<i64>,
}

impl IcebergSink {
    pub(super) fn name(&self) -> &str {
        &self.name
    }

    /// The table's Arrow schema rows are written in.
    pub(super) fn schema_ref(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// Commits position deletes and new data files together (`UPDATE`,
    /// `MERGE INTO`; ADR 0081, 0082).
    pub(super) async fn commit_overwrite(
        &self,
        files: &[Written],
        env: &datafusion::execution::runtime_env::RuntimeEnv,
    ) -> Result<()> {
        self.commit(files, Change::Overwrite, None, env).await
    }
}

impl fmt::Debug for IcebergSink {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IcebergSink")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

impl DisplayAs for IcebergSink {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "IcebergAppend[{}]", self.name)
    }
}

/// One column's statistics in one data file.
#[derive(Debug, Clone, Default)]
struct ColumnStats {
    values: u64,
    nulls: u64,
    lower: Option<Datum>,
    upper: Option<Datum>,
}

/// A partition tuple: one value (or null) per partition field.
type Key = Vec<Option<Datum>>;

/// A data file being written.
struct Open {
    writer: ArrowWriter<Vec<u8>>,
    rows: u64,
    stats: Vec<ColumnStats>,
}

/// A data file written and not yet committed.
pub(super) struct Written {
    path: String,
    rows: u64,
    bytes: u64,
    partition: Key,
    /// Per column: field id, type (when bounds apply) and statistics.
    stats: Vec<(i64, Option<Prim>, ColumnStats)>,
    /// A position delete file's one data file (ADR 0077).
    referenced: Option<String>,
    /// 0: data file; 1: position delete file.
    content: i32,
}

/// What a commit adds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Change {
    /// Data files (ADR 0075).
    Append,
    /// Position delete files (ADR 0077).
    Delete,
    /// Data files replacing others with the same live rows (compaction,
    /// ADR 0078).
    Replace,
    /// Position deletes for changed rows plus data files with their new
    /// values (`UPDATE`, ADR 0081).
    Overwrite,
}

/// See [`unique`]: for the sibling modules (ADR 0082).
pub(super) fn unique_id() -> u64 {
    unique()
}

/// A unique-enough id for files and snapshots: time, process and a counter.
fn unique() -> u64 {
    use std::hash::{BuildHasher, Hasher};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let mut h = std::collections::hash_map::RandomState::new().build_hasher();
    h.write_u128(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos()),
    );
    h.write_u64(COUNTER.fetch_add(1, Ordering::Relaxed));
    h.finish()
}

/// A value as the pruner compares it, by its Iceberg type.
fn datum(v: &ScalarValue) -> Option<Datum> {
    Some(match v {
        ScalarValue::Boolean(Some(b)) => Datum::Int(i64::from(*b)),
        ScalarValue::Int32(Some(x)) | ScalarValue::Date32(Some(x)) => Datum::Int(i64::from(*x)),
        ScalarValue::Int64(Some(x)) | ScalarValue::TimestampMicrosecond(Some(x), _) => {
            Datum::Int(*x)
        }
        ScalarValue::Utf8(Some(s))
        | ScalarValue::LargeUtf8(Some(s))
        | ScalarValue::Utf8View(Some(s)) => Datum::Bytes(s.as_bytes().to_vec()),
        ScalarValue::Decimal128(Some(x), _, _) => Datum::Big(*x),
        _ => return None,
    })
}

/// Lowest and highest non-null value of `array`, for a column of type
/// `prim`; `None` when every value is null or the type is not covered.
fn bounds(array: &ArrayRef, prim: Prim) -> Option<(Datum, Datum)> {
    use datafusion::arrow::array::{AsArray, Decimal128Array};
    use datafusion::arrow::compute::{max, max_boolean, max_string, min, min_boolean, min_string};
    use datafusion::arrow::datatypes::{
        Date32Type, Int32Type, Int64Type, TimestampMicrosecondType,
    };
    let int = |lo: Option<i64>, hi: Option<i64>| Some((Datum::Int(lo?), Datum::Int(hi?)));
    match (prim, array.data_type()) {
        (Prim::Bool, DataType::Boolean) => {
            let a = array.as_boolean();
            int(min_boolean(a).map(i64::from), max_boolean(a).map(i64::from))
        }
        (Prim::Int, DataType::Int32) => {
            let a = array.as_primitive::<Int32Type>();
            int(min(a).map(i64::from), max(a).map(i64::from))
        }
        (Prim::Date, DataType::Date32) => {
            let a = array.as_primitive::<Date32Type>();
            int(min(a).map(i64::from), max(a).map(i64::from))
        }
        (Prim::Long, DataType::Int64) => {
            let a = array.as_primitive::<Int64Type>();
            int(min(a), max(a))
        }
        (Prim::Timestamp, DataType::Timestamp(TimeUnit::Microsecond, _)) => {
            let a = array.as_primitive::<TimestampMicrosecondType>();
            int(min(a), max(a))
        }
        (Prim::String, DataType::Utf8) => {
            let a = array.as_string::<i32>();
            let (lo, hi) = (min_string(a)?, max_string(a)?);
            Some((Datum::Bytes(lo.into()), Datum::Bytes(hi.into())))
        }
        (Prim::String, DataType::LargeUtf8) => {
            let a = array.as_string::<i64>();
            let (lo, hi) = (min_string(a)?, max_string(a)?);
            Some((Datum::Bytes(lo.into()), Datum::Bytes(hi.into())))
        }
        (Prim::Decimal(_), DataType::Decimal128(_, _)) => {
            let a: &Decimal128Array = array.as_primitive();
            Some((Datum::Big(min(a)?), Datum::Big(max(a)?)))
        }
        _ => None,
    }
}

impl ColumnStats {
    fn update(&mut self, array: &ArrayRef, prim: Option<Prim>) {
        self.values += array.len() as u64;
        self.nulls += array.null_count() as u64;
        let Some((lo, hi)) = prim.and_then(|p| bounds(array, p)) else {
            return;
        };
        if self.lower.as_ref().is_none_or(|l| lo < *l) {
            self.lower = Some(lo);
        }
        if self.upper.as_ref().is_none_or(|u| hi > *u) {
            self.upper = Some(hi);
        }
    }
}

/// A string bound cut to [`BOUND_CHARS`]: the prefix is still a lower
/// bound; an upper bound needs its last character incremented, and is
/// dropped when that is impossible.
fn truncated(d: &Datum, upper: bool) -> Option<Datum> {
    let Datum::Bytes(b) = d else {
        return Some(d.clone());
    };
    let s = std::str::from_utf8(b).ok()?;
    if s.chars().count() <= BOUND_CHARS {
        return Some(d.clone());
    }
    let mut prefix: Vec<char> = s.chars().take(BOUND_CHARS).collect();
    if upper {
        let last = prefix.pop()?;
        prefix.push(char::from_u32(u32::from(last) + 1)?);
    }
    Some(Datum::Bytes(
        prefix.into_iter().collect::<String>().into_bytes(),
    ))
}

#[async_trait]
impl DataSink for IcebergSink {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    async fn write_all(
        &self,
        data: SendableRecordBatchStream,
        context: &Arc<TaskContext>,
    ) -> Result<u64> {
        let written = self.write_rows(data, context).await?;
        let rows = written.iter().map(|w| w.rows).sum();
        if !written.is_empty() {
            self.commit(&written, Change::Append, None, &context.runtime_env())
                .await?;
        }
        Ok(rows)
    }
}

impl IcebergSink {
    /// Writes `data` as data files, routed by partition (ADR 0076).
    pub(super) async fn write_rows(
        &self,
        mut data: SendableRecordBatchStream,
        context: &Arc<TaskContext>,
    ) -> Result<Vec<Written>> {
        let env = context.runtime_env();
        let run = unique();
        let mut written = Vec::new();
        let mut open: HashMap<Key, Open> = HashMap::new();
        // Open writers buffer row groups: their bytes are reserved, and a
        // refusal finishes every open file instead of failing the insert.
        let memory = MemoryConsumer::new(format!("IcebergAppend[{}]", self.name))
            .register(context.memory_pool());
        while let Some(batch) = data.next().await {
            // Same columns, relabelled with the table's field ids; a NULL
            // in a required column fails here.
            let batch = RecordBatch::try_new(self.schema.clone(), batch?.columns().to_vec())?;
            if batch.num_rows() == 0 {
                continue;
            }
            for (key, rows) in self.split(&batch)? {
                if !open.contains_key(&key) && open.len() >= OPEN_PARTITIONS {
                    for (key, file) in open.drain() {
                        written.push(self.finish(file, key, run, written.len(), &env).await?);
                    }
                }
                let file = match open.entry(key.clone()) {
                    std::collections::hash_map::Entry::Occupied(e) => e.into_mut(),
                    std::collections::hash_map::Entry::Vacant(e) => e.insert(Open {
                        writer: ArrowWriter::try_new(Vec::new(), self.schema.clone(), None)?,
                        rows: 0,
                        stats: vec![ColumnStats::default(); self.layout.columns.len()],
                    }),
                };
                file.writer.write(&rows)?;
                file.rows += rows.num_rows() as u64;
                for ((stats, column), (_, prim)) in file
                    .stats
                    .iter_mut()
                    .zip(rows.columns())
                    .zip(&self.layout.columns)
                {
                    stats.update(column, *prim);
                }
                if file.writer.in_progress_size() + file.writer.bytes_written() >= TARGET_FILE_BYTES
                {
                    if let Some(file) = open.remove(&key) {
                        written.push(self.finish(file, key, run, written.len(), &env).await?);
                    }
                }
            }
            let buffered = open.values().map(|f| f.writer.in_progress_size()).sum();
            if memory.try_resize(buffered).is_err() {
                for (key, file) in open.drain() {
                    written.push(self.finish(file, key, run, written.len(), &env).await?);
                }
                memory.resize(0);
            }
        }
        for (key, file) in open.drain() {
            written.push(self.finish(file, key, run, written.len(), &env).await?);
        }
        memory.resize(0);
        Ok(written)
    }

    /// Compaction (ADR 0078): writes the live rows of `data` (read from
    /// the files in `removed`) as new files, and commits them replacing
    /// those files and the position deletes that referenced them. The
    /// rows written, and the files added.
    pub(super) async fn replace(
        &self,
        data: SendableRecordBatchStream,
        removed: &std::collections::HashSet<String>,
        context: &Arc<TaskContext>,
    ) -> Result<(u64, usize)> {
        let written = self.write_rows(data, context).await?;
        self.commit(
            &written,
            Change::Replace,
            Some(removed),
            &context.runtime_env(),
        )
        .await?;
        Ok((written.iter().map(|w| w.rows).sum(), written.len()))
    }
}

impl IcebergSink {
    /// `batch`'s rows grouped by partition tuple, in order of first
    /// appearance. Unpartitioned: the whole batch under the empty tuple.
    fn split(&self, batch: &RecordBatch) -> Result<Vec<(Key, RecordBatch)>> {
        let parts = &self.layout.parts;
        if parts.is_empty() {
            return Ok(vec![(Vec::new(), batch.clone())]);
        }
        // ponytail: per-row ScalarValues for the partition columns only;
        // vectorize the transforms if inserts become CPU-bound here.
        let mut groups: Vec<(Key, Vec<u32>)> = Vec::new();
        let mut index: HashMap<Key, usize> = HashMap::new();
        for row in 0..batch.num_rows() {
            let mut key = Vec::with_capacity(parts.len());
            for part in parts {
                let value = ScalarValue::try_from_array(batch.column(part.column), row)?;
                key.push(datum(&value).and_then(|d| {
                    partition_value(part.source, &d, part.transform).map(|(v, _)| v)
                }));
            }
            let at = *index.entry(key.clone()).or_insert_with(|| {
                groups.push((key, Vec::new()));
                groups.len() - 1
            });
            groups[at].1.push(row as u32);
        }
        groups
            .into_iter()
            .map(|(key, rows)| {
                let rows = UInt32Array::from(rows);
                Ok((
                    key,
                    datafusion::arrow::compute::take_record_batch(batch, &rows)?,
                ))
            })
            .collect()
    }

    /// Finishes `file` as data file `n` of this insert and uploads it.
    async fn finish(
        &self,
        file: Open,
        partition: Key,
        run: u64,
        n: usize,
        env: &datafusion::execution::runtime_env::RuntimeEnv,
    ) -> Result<Written> {
        let bytes = file.writer.into_inner()?;
        let path = format!("{}/data/{run:016x}-{n:05}.parquet", self.location);
        let size = bytes.len() as u64;
        self.put_bytes(&path, bytes, env).await?;
        Ok(Written {
            path,
            rows: file.rows,
            bytes: size,
            partition,
            stats: self
                .layout
                .columns
                .iter()
                .zip(file.stats)
                .map(|((id, prim), s)| (*id, *prim, s))
                .collect(),
            referenced: None,
            content: 0,
        })
    }

    async fn put_bytes(
        &self,
        path: &str,
        bytes: Vec<u8>,
        env: &datafusion::execution::runtime_env::RuntimeEnv,
    ) -> Result<()> {
        let url = ListingTableUrl::parse(path)?;
        let store = env.object_store(store_url(&url, self.scoped.as_ref()))?;
        store
            .put(url.prefix(), PutPayload::from(bytes))
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(())
    }

    /// Writes the manifest once, then (per attempt) a manifest list on the
    /// current parent, and commits; a lost race is retried on the new
    /// parent.
    async fn commit(
        &self,
        files: &[Written],
        change: Change,
        removed: Option<&std::collections::HashSet<String>>,
        env: &datafusion::execution::runtime_env::RuntimeEnv,
    ) -> Result<()> {
        let failed = |m: String| DataFusionError::Execution(format!("{}: {m}", self.name));
        // Positive, as Java writers make them.
        let snapshot_id = (unique() >> 1) as i64;
        let meta = self.committer.current().await.map_err(failed)?;
        // One manifest per content: data files, then position deletes.
        let mut manifests = Vec::new();
        for content in [0, 1] {
            let group: Vec<&Written> = files.iter().filter(|f| f.content == content).collect();
            if group.is_empty() {
                continue;
            }
            let path = format!("{}/metadata/{snapshot_id:x}-m{content}.avro", self.location);
            let bytes = manifest(
                &meta,
                &self.layout,
                &group,
                snapshot_id,
                self.schema_id,
                content,
            )
            .map_err(|e| failed(format!("manifest: {e}")))?;
            let length = bytes.len() as i64;
            self.put_bytes(&path, bytes, env).await?;
            manifests.push((content, path, length, group));
        }
        let rows_of = |content: i32| -> u64 {
            files
                .iter()
                .filter(|f| f.content == content)
                .map(|f| f.rows)
                .sum()
        };
        let files_of = |content: i32| files.iter().filter(|f| f.content == content).count();
        let added_rows = rows_of(0);
        let mut meta = meta;
        for attempt in 0..COMMIT_ATTEMPTS {
            if attempt > 0 {
                meta = self.committer.current().await.map_err(failed)?;
            }
            let layout = Layout::of(&meta).map_err(failed)?;
            if meta["current-schema-id"].as_i64() != Some(self.schema_id)
                || layout.spec_id != self.layout.spec_id
            {
                return Err(failed(
                    "the table's schema or partitioning changed during the insert; nothing \
                     was committed"
                        .into(),
                ));
            }
            let parent = main_snapshot(&meta);
            // Deletes and rewrites name files of the snapshot they were
            // planned on: only appends may slip in between (their rows are
            // newer and untouched).
            if change != Change::Append && parent != self.base {
                if let Some(op) = operations_since(&meta, parent, self.base)
                    .into_iter()
                    .find(|op| op != "append")
                {
                    return Err(failed(format!(
                        "a concurrent `{op}` changed the table's data meanwhile; nothing was \
                         committed, run it again"
                    )));
                }
            }
            let sequence = meta["last-sequence-number"].as_i64().unwrap_or(0) + 1;
            let parent_list = match parent {
                None => None,
                Some(id) => {
                    let snap = meta["snapshots"]
                        .as_array()
                        .into_iter()
                        .flatten()
                        .find(|s| s["snapshot-id"].as_i64() == Some(id))
                        .ok_or_else(|| failed(format!("parent snapshot {id} not in metadata")))?;
                    let list = snap["manifest-list"]
                        .as_str()
                        .ok_or_else(|| failed("parent snapshot without manifest-list".into()))?;
                    Some(
                        read_env(list, env, self.scoped.as_ref())
                            .await
                            .map_err(DataFusionError::from)?,
                    )
                }
            };
            // A rewrite marks the replaced files (and position deletes that
            // only referenced them) DELETED in rewritten copies of the
            // manifests that list them.
            let mut rewritten = HashMap::new();
            let mut gone = Gone::default();
            if let (Some(removed), Some(list)) = (removed, parent_list.as_deref()) {
                let reader = apache_avro::Reader::new(list).map_err(|e| failed(e.to_string()))?;
                for (n, entry) in reader.enumerate() {
                    let entry = entry.map_err(|e| failed(e.to_string()))?;
                    let (Some(Avro::String(path)), Some(seq)) = (
                        field(&entry, "manifest_path").map(strip),
                        field(&entry, "sequence_number").map(strip).and_then(long),
                    ) else {
                        return Err(failed("manifest list entry without path".into()));
                    };
                    let bytes = read_env(path, env, self.scoped.as_ref())
                        .await
                        .map_err(DataFusionError::from)?;
                    let Some((copy, counts)) = rewrite_manifest(&bytes, removed, seq, snapshot_id)
                        .map_err(|e| failed(format!("rewriting {path}: {e}")))?
                    else {
                        continue;
                    };
                    let new_path = format!(
                        "{}/metadata/{snapshot_id:x}-r{attempt}-{n}.avro",
                        self.location
                    );
                    let length = copy.len() as i64;
                    self.put_bytes(&new_path, copy, env).await?;
                    gone.add(&counts);
                    rewritten.insert(path.clone(), (new_path, length, counts));
                }
                if gone.data_paths.len() != removed.len() {
                    return Err(failed(
                        "some files to rewrite are no longer in the table; nothing was committed"
                            .into(),
                    ));
                }
            }
            let new_entries: Vec<ManifestFile> = manifests
                .iter()
                .map(|(content, path, length, group)| ManifestFile {
                    path: path.clone(),
                    length: *length,
                    spec_id: self.layout.spec_id,
                    sequence,
                    snapshot_id,
                    files: group.len() as i64,
                    rows: group.iter().map(|f| f.rows as i64).sum(),
                    partitions: summaries(&self.layout, group),
                    content: *content,
                })
                .collect();
            let list = manifest_list(
                &new_entries,
                parent_list.as_deref(),
                snapshot_id,
                parent,
                sequence,
                &rewritten,
            )
            .map_err(|e| failed(format!("manifest list: {e}")))?;
            let list_path = format!(
                "{}/metadata/snap-{snapshot_id}-{attempt}-{:x}.avro",
                self.location,
                unique()
            );
            self.put_bytes(&list_path, list, env).await?;
            let size = files.iter().map(|f| f.bytes).sum::<u64>().to_string();
            let mut summary = json!({
                "operation": match change {
                    Change::Append => "append",
                    Change::Delete => "delete",
                    Change::Replace => "replace",
                    Change::Overwrite => "overwrite",
                },
                "added-files-size": size,
            });
            if files_of(0) > 0 {
                summary["added-data-files"] = json!(files_of(0).to_string());
                summary["added-records"] = json!(added_rows.to_string());
            }
            if files_of(1) > 0 {
                summary["added-delete-files"] = json!(files_of(1).to_string());
                summary["added-position-delete-files"] = json!(files_of(1).to_string());
                summary["added-position-deletes"] = json!(rows_of(1).to_string());
            }
            if change == Change::Replace {
                summary["deleted-data-files"] = json!(gone.data_paths.len().to_string());
                summary["deleted-records"] = json!(gone.data_rows.to_string());
                summary["removed-delete-files"] = json!(gone.delete_files.to_string());
                summary["removed-position-deletes"] = json!(gone.delete_rows.to_string());
            }
            summary["engine-name"] = json!("plimsoll");
            summary["changed-partition-count"] = json!(
                files
                    .iter()
                    .map(|f| &f.partition)
                    .collect::<std::collections::HashSet<_>>()
                    .len()
                    .to_string()
            );
            let mut snapshot = json!({
                "snapshot-id": snapshot_id,
                "sequence-number": sequence,
                "timestamp-ms": std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map_or(0, |d| d.as_millis() as i64),
                "manifest-list": list_path,
                "summary": summary,
                "schema-id": self.schema_id,
            });
            if let Some(parent) = parent {
                snapshot["parent-snapshot-id"] = json!(parent);
            }
            let requirements = vec![
                json!({"type": "assert-table-uuid", "uuid": meta["table-uuid"]}),
                json!({"type": "assert-ref-snapshot-id", "ref": "main", "snapshot-id": parent}),
                json!({"type": "assert-default-spec-id", "default-spec-id": self.layout.spec_id}),
            ];
            let updates = vec![
                json!({"action": "add-snapshot", "snapshot": snapshot}),
                json!({"action": "set-snapshot-ref", "ref-name": "main", "type": "branch", "snapshot-id": snapshot_id}),
            ];
            match self.committer.commit(requirements, updates).await {
                Ok(()) => return Ok(()),
                Err(CommitError::Conflict(_)) if attempt + 1 < COMMIT_ATTEMPTS => continue,
                Err(CommitError::Conflict(m) | CommitError::Failed(m)) => {
                    return Err(failed(format!("commit failed: {m}")));
                }
            }
        }
        Err(failed("commit failed".into()))
    }
}

/// A data file a `DELETE` reads (ADR 0077): every row, in file order, so
/// a running count is each row's position.
pub(super) struct Target {
    pub(super) path: String,
    pub(super) partition: Key,
    /// Rows surviving earlier position deletes; `None` = all rows.
    pub(super) selection: Option<datafusion::parquet::arrow::arrow_reader::RowSelection>,
    /// A scan of this file alone: one partition, no row selection, no
    /// pushed predicate.
    pub(super) plan: Arc<dyn datafusion::physical_plan::ExecutionPlan>,
}

/// Iceberg's reserved field ids of a position delete file's columns.
const DELETE_FILE_PATH: i64 = 2_147_483_546;
const DELETE_POS: i64 = 2_147_483_545;

/// `DELETE FROM t WHERE …` (ADR 0077): position deletes for the rows of
/// `targets` matching `predicate`, committed as one snapshot.
pub(super) struct IcebergDeleteExec {
    pub(super) sink: Arc<IcebergSink>,
    pub(super) targets: Arc<Vec<Target>>,
    pub(super) predicate: Arc<dyn datafusion::physical_expr::PhysicalExpr>,
    /// `UPDATE` (ADR 0081): each table column of a changed row, as its
    /// new value (`SET`) or itself. `None`: a `DELETE`.
    update: Option<Arc<Vec<Arc<dyn datafusion::physical_expr::PhysicalExpr>>>>,
    properties: Arc<datafusion::physical_plan::PlanProperties>,
}

impl IcebergDeleteExec {
    pub(super) fn new(
        sink: IcebergSink,
        targets: Vec<Target>,
        predicate: Arc<dyn datafusion::physical_expr::PhysicalExpr>,
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
            predicate,
            update: None,
            properties,
        }
    }

    /// An `UPDATE` instead: matching rows are rewritten as `row` computes
    /// them, besides being deleted (ADR 0081).
    pub(super) fn with_update(
        mut self,
        row: Vec<Arc<dyn datafusion::physical_expr::PhysicalExpr>>,
    ) -> Self {
        self.update = Some(Arc::new(row));
        self
    }
}

/// The schema DataFusion expects of DML: one `count` column.
fn count_schema() -> SchemaRef {
    use datafusion::arrow::datatypes::{Field, Schema};
    Arc::new(Schema::new(vec![Field::new(
        "count",
        DataType::UInt64,
        false,
    )]))
}

impl fmt::Debug for IcebergDeleteExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "IcebergDeleteExec[{}]", self.sink.name)
    }
}

impl DisplayAs for IcebergDeleteExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}[{}] files={} predicate={}",
            if self.update.is_some() {
                "IcebergUpdate"
            } else {
                "IcebergDelete"
            },
            self.sink.name,
            self.targets.len(),
            self.predicate
        )
    }
}

impl datafusion::physical_plan::ExecutionPlan for IcebergDeleteExec {
    fn name(&self) -> &str {
        "IcebergDeleteExec"
    }

    fn properties(&self) -> &Arc<datafusion::physical_plan::PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
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
        _children: Vec<Arc<dyn datafusion::physical_plan::ExecutionPlan>>,
    ) -> Result<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let (sink, targets, predicate, update) = (
            self.sink.clone(),
            self.targets.clone(),
            self.predicate.clone(),
            self.update.clone(),
        );
        let run = async move {
            let deleted = sink
                .delete(
                    &targets,
                    &predicate,
                    update.as_deref().map(Vec::as_slice),
                    &context,
                )
                .await?;
            Ok(RecordBatch::try_new(
                count_schema(),
                vec![Arc::new(datafusion::arrow::array::UInt64Array::from(vec![
                    deleted,
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

/// Positions a [`datafusion::parquet::arrow::arrow_reader::RowSelection`]
/// keeps, as sorted half-open ranges.
pub(super) fn kept_ranges(
    selection: &datafusion::parquet::arrow::arrow_reader::RowSelection,
) -> Vec<(u64, u64)> {
    let mut at = 0u64;
    let mut out = Vec::new();
    for s in selection.iter() {
        let end = at + s.row_count as u64;
        if !s.skip {
            out.push((at, end));
        }
        at = end;
    }
    out
}

/// Whether `pos` survived earlier deletes, given the kept ranges (`None`:
/// all rows). Positions must be asked in increasing order: `range` walks
/// forward with them.
pub(super) fn alive(kept: Option<&[(u64, u64)]>, pos: u64, range: &mut usize) -> bool {
    let Some(kept) = kept else { return true };
    while *range < kept.len() && kept[*range].1 <= pos {
        *range += 1;
    }
    kept.get(*range).is_some_and(|(start, _)| *start <= pos)
}

impl IcebergSink {
    /// Finds the rows of `targets` matching `predicate`, writes one
    /// position delete file per data file with matches, and commits them;
    /// the number of rows deleted. Rows an earlier delete removed are not
    /// deleted (or counted) again.
    async fn delete(
        &self,
        targets: &[Target],
        predicate: &Arc<dyn datafusion::physical_expr::PhysicalExpr>,
        update: Option<&[Arc<dyn datafusion::physical_expr::PhysicalExpr>]>,
        context: &Arc<TaskContext>,
    ) -> Result<u64> {
        use datafusion::arrow::array::{AsArray, BooleanArray};
        let env = context.runtime_env();
        let run = unique();
        let mut written = Vec::new();
        // UPDATE: the changed rows' new values, reserved until written.
        // ponytail: buffered whole; stream them into the writer if updates
        // of more rows than the pool holds matter.
        let mut changed = Vec::new();
        let memory = MemoryConsumer::new(format!("IcebergUpdate[{}]", self.name))
            .register(context.memory_pool());
        for target in targets {
            let kept = target.selection.as_ref().map(kept_ranges);
            let mut range = 0;
            let mut stream = target.plan.execute(0, context.clone())?;
            let mut offset = 0u64;
            let mut positions: Vec<i64> = Vec::new();
            while let Some(batch) = stream.next().await {
                let batch = batch?;
                let n = batch.num_rows();
                let mask = predicate.evaluate(&batch)?.into_array(n)?;
                let mask = mask.as_boolean_opt().ok_or_else(|| {
                    DataFusionError::Plan("DELETE predicate is not boolean".into())
                })?;
                let mut hit = Vec::with_capacity(n);
                for i in 0..n {
                    let pos = offset + i as u64;
                    // NULL is not true: SQL keeps the row.
                    let take = mask.is_valid(i)
                        && mask.value(i)
                        && alive(kept.as_deref(), pos, &mut range);
                    if take {
                        positions.push(pos as i64);
                    }
                    hit.push(take);
                }
                offset += n as u64;
                if let Some(row) = update {
                    let rows = datafusion::arrow::compute::filter_record_batch(
                        &batch,
                        &BooleanArray::from(hit),
                    )?;
                    if rows.num_rows() > 0 {
                        let columns = row
                            .iter()
                            .zip(self.schema.fields())
                            .map(|(expr, field)| {
                                let value = expr.evaluate(&rows)?.into_array(rows.num_rows())?;
                                // Assignments are planned in the column's type;
                                // cast anything that is not.
                                Ok(datafusion::arrow::compute::cast(&value, field.data_type())?)
                            })
                            .collect::<Result<Vec<_>>>()?;
                        let rows = RecordBatch::try_new(self.schema.clone(), columns)?;
                        memory.try_grow(rows.get_array_memory_size())?;
                        changed.push(rows);
                    }
                }
            }
            if !positions.is_empty() {
                written.push(
                    self.position_deletes(target, positions, run, written.len(), &env)
                        .await?,
                );
            }
        }
        let deleted = written.iter().map(|w| w.rows).sum();
        if written.is_empty() {
            return Ok(0);
        }
        let change = match update {
            None => Change::Delete,
            Some(_) => {
                let rows = datafusion::physical_plan::memory::MemoryStream::try_new(
                    changed,
                    self.schema.clone(),
                    None,
                )?;
                written.extend(self.write_rows(Box::pin(rows), context).await?);
                Change::Overwrite
            }
        };
        self.commit(&written, change, None, &env).await?;
        memory.resize(0);
        Ok(deleted)
    }

    /// One position delete file: `(file_path, pos)` rows, sorted, with
    /// Iceberg's reserved field ids.
    pub(super) async fn position_deletes(
        &self,
        target: &Target,
        positions: Vec<i64>,
        run: u64,
        n: usize,
        env: &datafusion::execution::runtime_env::RuntimeEnv,
    ) -> Result<Written> {
        use datafusion::arrow::array::{Int64Array, StringArray};
        use datafusion::arrow::datatypes::{Field, Schema};
        use datafusion::parquet::arrow::PARQUET_FIELD_ID_META_KEY;
        let field = |name: &str, ty: DataType, id: i64| {
            Field::new(name, ty, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_owned(),
                id.to_string(),
            )]))
        };
        let schema = Arc::new(Schema::new(vec![
            field("file_path", DataType::Utf8, DELETE_FILE_PATH),
            field("pos", DataType::Int64, DELETE_POS),
        ]));
        let rows = positions.len();
        let paths: ArrayRef = Arc::new(StringArray::from(vec![target.path.as_str(); rows]));
        let pos: ArrayRef = Arc::new(Int64Array::from(positions));
        let batch = RecordBatch::try_new(schema.clone(), vec![paths.clone(), pos.clone()])?;
        let mut writer = ArrowWriter::try_new(Vec::new(), schema, None)?;
        writer.write(&batch)?;
        let bytes = writer.into_inner()?;
        let path = format!("{}/data/{run:016x}-deletes-{n:05}.parquet", self.location);
        let size = bytes.len() as u64;
        self.put_bytes(&path, bytes, env).await?;
        let mut path_stats = ColumnStats::default();
        path_stats.update(&paths, Some(Prim::String));
        let mut pos_stats = ColumnStats::default();
        pos_stats.update(&pos, Some(Prim::Long));
        Ok(Written {
            path,
            rows: rows as u64,
            bytes: size,
            partition: target.partition.clone(),
            stats: vec![
                (DELETE_FILE_PATH, Some(Prim::String), path_stats),
                (DELETE_POS, Some(Prim::Long), pos_stats),
            ],
            referenced: Some(target.path.clone()),
            content: 1,
        })
    }
}

/// The snapshot `main` points at, if any.
fn main_snapshot(meta: &Json) -> Option<i64> {
    meta["refs"]["main"]["snapshot-id"]
        .as_i64()
        .or_else(|| meta["current-snapshot-id"].as_i64().filter(|id| *id >= 0))
}

/// The operations of the snapshots from `head` back to (not including)
/// `base`, following parents. A chain that never reaches `base` reports
/// an unknown operation, which no caller treats as safe.
fn operations_since(meta: &Json, head: Option<i64>, base: Option<i64>) -> Vec<String> {
    let snapshots: HashMap<i64, &Json> = meta["snapshots"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|s| Some((s["snapshot-id"].as_i64()?, s)))
        .collect();
    let mut ops = Vec::new();
    let mut at = head;
    while at != base {
        let Some(snap) = at.and_then(|id| snapshots.get(&id)) else {
            ops.push("unknown".to_owned());
            break;
        };
        ops.push(
            snap["summary"]["operation"]
                .as_str()
                .unwrap_or("unknown")
                .to_owned(),
        );
        at = snap["parent-snapshot-id"].as_i64();
    }
    ops
}

/// The Avro type of a partition value.
fn avro_type(prim: Prim) -> &'static str {
    match prim {
        Prim::Bool => "boolean",
        Prim::Int | Prim::Date => "int",
        Prim::Long | Prim::Timestamp => "long",
        // Not produced: decimal partitions are refused by [`Layout::of`].
        Prim::String | Prim::Decimal(_) => "string",
    }
}

fn avro_value(prim: Prim, d: &Datum) -> Avro {
    match (prim, d) {
        (Prim::Bool, Datum::Int(v)) => Avro::Boolean(*v != 0),
        (Prim::Int | Prim::Date, Datum::Int(v)) => Avro::Int(*v as i32),
        (Prim::Long | Prim::Timestamp, Datum::Int(v)) => Avro::Long(*v),
        (_, Datum::Bytes(b)) => Avro::String(String::from_utf8_lossy(b).into_owned()),
        _ => Avro::Null,
    }
}

/// The v2 `manifest_entry` schema, with the default spec's partition
/// struct and the statistics maps, carrying the field ids Iceberg readers
/// resolve by.
fn manifest_entry_schema(layout: &Layout) -> String {
    let partition: Vec<Json> = layout
        .parts
        .iter()
        .map(|p| {
            json!({"name": p.name, "type": ["null", avro_type(p.out)], "default": null, "field-id": p.field_id})
        })
        .collect();
    let map = |name: &str, id: i64, key: i64, value: i64, value_type: &str| {
        json!({"name": name, "type": ["null", {"type": "array", "logicalType": "map", "items": {
            "type": "record", "name": format!("k{key}_v{value}"), "fields": [
                {"name": "key", "type": "int", "field-id": key},
                {"name": "value", "type": value_type, "field-id": value},
            ]}}], "default": null, "field-id": id})
    };
    json!({"type": "record", "name": "manifest_entry", "fields": [
        {"name": "status", "type": "int", "field-id": 0},
        {"name": "snapshot_id", "type": ["null", "long"], "default": null, "field-id": 1},
        {"name": "sequence_number", "type": ["null", "long"], "default": null, "field-id": 3},
        {"name": "file_sequence_number", "type": ["null", "long"], "default": null, "field-id": 4},
        {"name": "data_file", "type": {"type": "record", "name": "r2", "fields": [
            {"name": "content", "type": "int", "field-id": 134},
            {"name": "file_path", "type": "string", "field-id": 100},
            {"name": "file_format", "type": "string", "field-id": 101},
            {"name": "partition", "type": {"type": "record", "name": "r102", "fields": partition}, "field-id": 102},
            {"name": "record_count", "type": "long", "field-id": 103},
            {"name": "file_size_in_bytes", "type": "long", "field-id": 104},
            map("value_counts", 109, 119, 120, "long"),
            map("null_value_counts", 110, 121, 122, "long"),
            map("lower_bounds", 125, 126, 127, "bytes"),
            map("upper_bounds", 128, 129, 130, "bytes"),
            {"name": "referenced_data_file", "type": ["null", "string"], "default": null, "field-id": 143},
        ]}, "field-id": 2},
    ]})
    .to_string()
}

/// The v2 `manifest_file` schema (one manifest list entry).
const MANIFEST_FILE: &str = r#"{"type":"record","name":"manifest_file","fields":[
 {"name":"manifest_path","type":"string","field-id":500},
 {"name":"manifest_length","type":"long","field-id":501},
 {"name":"partition_spec_id","type":"int","field-id":502},
 {"name":"content","type":"int","field-id":517},
 {"name":"sequence_number","type":"long","field-id":515},
 {"name":"min_sequence_number","type":"long","field-id":516},
 {"name":"added_snapshot_id","type":"long","field-id":503},
 {"name":"added_files_count","type":"int","field-id":504},
 {"name":"existing_files_count","type":"int","field-id":505},
 {"name":"deleted_files_count","type":"int","field-id":506},
 {"name":"added_rows_count","type":"long","field-id":512},
 {"name":"existing_rows_count","type":"long","field-id":513},
 {"name":"deleted_rows_count","type":"long","field-id":514},
 {"name":"partitions","type":["null",{"type":"array","items":{"type":"record","name":"r508","fields":[
   {"name":"contains_null","type":"boolean","field-id":509},
   {"name":"contains_nan","type":["null","boolean"],"default":null,"field-id":518},
   {"name":"lower_bound","type":["null","bytes"],"default":null,"field-id":510},
   {"name":"upper_bound","type":["null","bytes"],"default":null,"field-id":511}
 ]},"element-id":508}],"default":null,"field-id":507},
 {"name":"key_metadata","type":["null","bytes"],"default":null,"field-id":519}
]}"#;

fn avro_schema(text: &str) -> std::result::Result<AvroSchema, String> {
    AvroSchema::parse_str(text).map_err(|e| e.to_string())
}

/// An Avro container writer whose header carries `schema_json` verbatim.
///
/// `apache_avro` re-serializes a schema it writes and drops attributes it
/// does not model, such as the `"logicalType": "map"` Iceberg puts on the
/// arrays its `map<int, _>` statistics are stored as; Java readers then
/// see a list and refuse the file. So the header (magic, metadata with the
/// original schema text and the codec, sync marker) is written here, and
/// the writer only appends blocks after it. Deflate, as Java and Python
/// Iceberg writers use.
fn avro_writer<'a>(
    schema: &'a AvroSchema,
    schema_json: &str,
    metadata: &[(&str, String)],
) -> std::result::Result<apache_avro::Writer<'a, Vec<u8>>, String> {
    fn long(out: &mut Vec<u8>, v: i64) {
        // Avro longs: zig-zag, then base-128 varint.
        let mut z = ((v << 1) ^ (v >> 63)) as u64;
        loop {
            let byte = (z & 0x7f) as u8;
            z >>= 7;
            if z == 0 {
                out.push(byte);
                return;
            }
            out.push(byte | 0x80);
        }
    }
    fn bytes(out: &mut Vec<u8>, b: &[u8]) {
        long(out, b.len() as i64);
        out.extend_from_slice(b);
    }
    let mut marker = [0u8; 16];
    marker[..8].copy_from_slice(&unique().to_le_bytes());
    marker[8..].copy_from_slice(&unique().to_le_bytes());
    let mut header = b"Obj\x01".to_vec();
    let entries: Vec<(&str, &[u8])> = [
        ("avro.schema", schema_json.as_bytes()),
        ("avro.codec", b"deflate".as_slice()),
    ]
    .into_iter()
    .chain(metadata.iter().map(|(k, v)| (*k, v.as_bytes())))
    .collect();
    long(&mut header, entries.len() as i64);
    for (k, v) in entries {
        bytes(&mut header, k.as_bytes());
        bytes(&mut header, v);
    }
    long(&mut header, 0);
    header.extend_from_slice(&marker);
    apache_avro::Writer::builder()
        .schema(schema)
        .writer(header)
        .codec(apache_avro::Codec::Deflate(
            apache_avro::DeflateSettings::default(),
        ))
        .marker(marker)
        .has_header(true)
        .build()
        .map_err(|e| e.to_string())
}

fn opt(v: Option<Avro>) -> Avro {
    match v {
        None => Avro::Union(0, Box::new(Avro::Null)),
        Some(v) => Avro::Union(1, Box::new(v)),
    }
}

/// An Iceberg `map<int, _>` as Avro: an array of `{key, value}` records.
fn avro_map(entries: Vec<(i64, Avro)>) -> Avro {
    opt(Some(Avro::Array(
        entries
            .into_iter()
            .map(|(k, v)| {
                Avro::Record(vec![
                    ("key".into(), Avro::Int(k as i32)),
                    ("value".into(), v),
                ])
            })
            .collect(),
    )))
}

/// One manifest listing `files` as ADDED by `snapshot_id`; data sequence
/// numbers are inherited from the manifest list entry.
fn manifest(
    meta: &Json,
    layout: &Layout,
    files: &[&Written],
    snapshot_id: i64,
    schema_id: i64,
    content: i32,
) -> std::result::Result<Vec<u8>, String> {
    let schema_json = manifest_entry_schema(layout);
    let schema = avro_schema(&schema_json)?;
    let table_schema = meta["schemas"]
        .as_array()
        .into_iter()
        .flatten()
        .find(|s| s["schema-id"].as_i64() == Some(schema_id))
        .ok_or("current schema not found")?;
    let mut writer = avro_writer(
        &schema,
        &schema_json,
        &[
            ("schema", table_schema.to_string()),
            ("schema-id", schema_id.to_string()),
            ("partition-spec", layout.spec_fields.to_string()),
            ("partition-spec-id", layout.spec_id.to_string()),
            ("format-version", "2".to_owned()),
            (
                "content",
                if content == 0 { "data" } else { "deletes" }.to_owned(),
            ),
        ],
    )?;
    for f in files {
        let partition = layout
            .parts
            .iter()
            .zip(&f.partition)
            .map(|(p, v)| {
                (
                    p.name.clone(),
                    opt(v.as_ref().map(|d| avro_value(p.out, d))),
                )
            })
            .collect();
        let mut values = Vec::new();
        let mut nulls = Vec::new();
        let mut lower = Vec::new();
        let mut upper = Vec::new();
        for (id, prim, s) in &f.stats {
            values.push((*id, Avro::Long(s.values as i64)));
            nulls.push((*id, Avro::Long(s.nulls as i64)));
            let Some(prim) = prim else { continue };
            let bound = |d: &Option<Datum>, is_upper: bool| {
                d.as_ref()
                    .and_then(|d| truncated(d, is_upper))
                    .and_then(|d| prim.encode(&d))
            };
            if let Some(b) = bound(&s.lower, false) {
                lower.push((*id, Avro::Bytes(b)));
            }
            if let Some(b) = bound(&s.upper, true) {
                upper.push((*id, Avro::Bytes(b)));
            }
        }
        let entry = Avro::Record(vec![
            ("status".into(), Avro::Int(1)),
            ("snapshot_id".into(), opt(Some(Avro::Long(snapshot_id)))),
            ("sequence_number".into(), opt(None)),
            ("file_sequence_number".into(), opt(None)),
            (
                "data_file".into(),
                Avro::Record(vec![
                    ("content".into(), Avro::Int(content)),
                    ("file_path".into(), Avro::String(f.path.clone())),
                    ("file_format".into(), Avro::String("PARQUET".into())),
                    ("partition".into(), Avro::Record(partition)),
                    ("record_count".into(), Avro::Long(f.rows as i64)),
                    ("file_size_in_bytes".into(), Avro::Long(f.bytes as i64)),
                    ("value_counts".into(), avro_map(values)),
                    ("null_value_counts".into(), avro_map(nulls)),
                    ("lower_bounds".into(), avro_map(lower)),
                    ("upper_bounds".into(), avro_map(upper)),
                    (
                        "referenced_data_file".into(),
                        opt(f.referenced.clone().map(Avro::String)),
                    ),
                ]),
            ),
        ]);
        writer.append_value(entry).map_err(|e| e.to_string())?;
    }
    writer.into_inner().map_err(|e| e.to_string())
}

/// One partition field across a manifest: contains a null, and the
/// encoded lowest and highest values.
type FieldSummary = (bool, Option<Vec<u8>>, Option<Vec<u8>>);

/// Per partition field across `files`: whether any value is null, and
/// the encoded lowest and highest values (the manifest list's
/// `partitions`, which readers prune whole manifests by).
fn summaries(layout: &Layout, files: &[&Written]) -> Vec<FieldSummary> {
    layout
        .parts
        .iter()
        .enumerate()
        .map(|(i, p)| {
            let values = files.iter().map(|f| f.partition.get(i).cloned().flatten());
            let contains_null = values.clone().any(|v| v.is_none());
            let present: Vec<Datum> = values.flatten().collect();
            let lo = present
                .iter()
                .min_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
            let hi = present
                .iter()
                .max_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
            (
                contains_null,
                lo.and_then(|d| p.out.encode(d)),
                hi.and_then(|d| p.out.encode(d)),
            )
        })
        .collect()
}

/// The new manifest's entry in the manifest list.
struct ManifestFile {
    path: String,
    length: i64,
    spec_id: i64,
    sequence: i64,
    snapshot_id: i64,
    files: i64,
    rows: i64,
    partitions: Vec<FieldSummary>,
    /// 0: data manifest; 1: delete manifest.
    content: i32,
}

/// The new snapshot's manifest list: the new manifest first, then every
/// manifest of the parent's list, carried over field by field.
fn manifest_list(
    new: &[ManifestFile],
    parent: Option<&[u8]>,
    snapshot_id: i64,
    parent_id: Option<i64>,
    sequence: i64,
    rewritten: &HashMap<String, (String, i64, Counts)>,
) -> std::result::Result<Vec<u8>, String> {
    let schema = avro_schema(MANIFEST_FILE)?;
    let mut writer = avro_writer(
        &schema,
        MANIFEST_FILE,
        &[
            ("snapshot-id", snapshot_id.to_string()),
            (
                "parent-snapshot-id",
                parent_id.map_or("null".to_owned(), |p| p.to_string()),
            ),
            ("sequence-number", sequence.to_string()),
            ("format-version", "2".to_owned()),
        ],
    )?;
    for new in new {
        let partitions = new
            .partitions
            .iter()
            .map(|(null, lo, hi)| {
                Avro::Record(vec![
                    ("contains_null".into(), Avro::Boolean(*null)),
                    ("contains_nan".into(), opt(None)),
                    ("lower_bound".into(), opt(lo.clone().map(Avro::Bytes))),
                    ("upper_bound".into(), opt(hi.clone().map(Avro::Bytes))),
                ])
            })
            .collect();
        writer
            .append_value(Avro::Record(vec![
                ("manifest_path".into(), Avro::String(new.path.clone())),
                ("manifest_length".into(), Avro::Long(new.length)),
                ("partition_spec_id".into(), Avro::Int(new.spec_id as i32)),
                ("content".into(), Avro::Int(new.content)),
                ("sequence_number".into(), Avro::Long(new.sequence)),
                ("min_sequence_number".into(), Avro::Long(new.sequence)),
                ("added_snapshot_id".into(), Avro::Long(new.snapshot_id)),
                ("added_files_count".into(), Avro::Int(new.files as i32)),
                ("existing_files_count".into(), Avro::Int(0)),
                ("deleted_files_count".into(), Avro::Int(0)),
                ("added_rows_count".into(), Avro::Long(new.rows)),
                ("existing_rows_count".into(), Avro::Long(0)),
                ("deleted_rows_count".into(), Avro::Long(0)),
                ("partitions".into(), opt(Some(Avro::Array(partitions)))),
                ("key_metadata".into(), opt(None)),
            ]))
            .map_err(|e| e.to_string())?;
    }
    if let Some(bytes) = parent {
        let reader = apache_avro::Reader::new(bytes).map_err(|e| e.to_string())?;
        for entry in reader {
            let entry = entry.map_err(|e| e.to_string())?;
            let mut entry = conform(&entry, &schema);
            let old = match field(&entry, "manifest_path") {
                Some(Avro::String(p)) => p.clone(),
                _ => String::new(),
            };
            // A rewritten manifest: its new copy, added now, with what it
            // keeps and what it marks deleted.
            if let Some((path, length, c)) = rewritten.get(&old) {
                for (name, value) in [
                    ("manifest_path", Avro::String(path.clone())),
                    ("manifest_length", Avro::Long(*length)),
                    ("sequence_number", Avro::Long(sequence)),
                    (
                        "min_sequence_number",
                        Avro::Long(c.min_sequence.unwrap_or(sequence)),
                    ),
                    ("added_snapshot_id", Avro::Long(snapshot_id)),
                    ("added_files_count", Avro::Int(0)),
                    ("existing_files_count", Avro::Int(c.existing_files as i32)),
                    ("deleted_files_count", Avro::Int(c.deleted_files as i32)),
                    ("added_rows_count", Avro::Long(0)),
                    ("existing_rows_count", Avro::Long(c.existing_rows)),
                    ("deleted_rows_count", Avro::Long(c.deleted_rows)),
                ] {
                    set(&mut entry, name, value);
                }
            }
            writer
                .append_value(entry)
                .map_err(|e| format!("carrying over a parent manifest: {e}"))?;
        }
    }
    writer.into_inner().map_err(|e| e.to_string())
}

/// What rewriting one manifest kept and marked deleted.
#[derive(Debug, Clone, Default)]
struct Counts {
    existing_files: i64,
    existing_rows: i64,
    deleted_files: i64,
    deleted_rows: i64,
    min_sequence: Option<i64>,
    /// Data files marked deleted, by path.
    data_paths: Vec<String>,
    /// Position delete files marked deleted, and their rows.
    delete_files: i64,
    delete_rows: i64,
}

/// Totals across rewritten manifests, for the snapshot summary.
#[derive(Debug, Default)]
struct Gone {
    data_paths: std::collections::HashSet<String>,
    data_rows: i64,
    delete_files: i64,
    delete_rows: i64,
}

impl Gone {
    fn add(&mut self, c: &Counts) {
        self.data_paths.extend(c.data_paths.iter().cloned());
        self.data_rows += c.deleted_rows - c.delete_rows;
        self.delete_files += c.delete_files;
        self.delete_rows += c.delete_rows;
    }
}

fn long(v: &Avro) -> Option<i64> {
    match v {
        Avro::Long(n) => Some(*n),
        Avro::Int(n) => Some(i64::from(*n)),
        _ => None,
    }
}

/// Sets a record's field, keeping its union wrapping if any.
fn set(record: &mut Avro, name: &str, value: Avro) {
    let Avro::Record(fields) = record else { return };
    if let Some((_, v)) = fields.iter_mut().find(|(n, _)| n == name) {
        *v = match v {
            Avro::Union(i, _) => Avro::Union(*i, Box::new(value)),
            _ => value,
        };
    }
}

/// The header of an Avro container: its schema text and its metadata
/// (`avro.*` keys excluded).
fn read_header(bytes: &[u8]) -> Option<(String, Vec<(String, String)>)> {
    fn long(bytes: &[u8], at: &mut usize) -> Option<i64> {
        let (mut z, mut shift) = (0u64, 0);
        loop {
            let b = *bytes.get(*at)?;
            *at += 1;
            z |= u64::from(b & 0x7f) << shift;
            if b & 0x80 == 0 {
                return Some(((z >> 1) as i64) ^ -((z & 1) as i64));
            }
            shift += 7;
            if shift > 63 {
                return None;
            }
        }
    }
    fn text(bytes: &[u8], at: &mut usize) -> Option<String> {
        let n = usize::try_from(long(bytes, at)?).ok()?;
        let s = std::str::from_utf8(bytes.get(*at..*at + n)?)
            .ok()?
            .to_owned();
        *at += n;
        Some(s)
    }
    let mut at = 4;
    if bytes.get(..4)? != b"Obj\x01" {
        return None;
    }
    let (mut schema, mut metadata) = (None, Vec::new());
    loop {
        let mut n = long(bytes, &mut at)?;
        if n == 0 {
            break;
        }
        if n < 0 {
            // A negative count is followed by the block's byte size.
            long(bytes, &mut at)?;
            n = -n;
        }
        for _ in 0..n {
            let (k, v) = (text(bytes, &mut at)?, text(bytes, &mut at)?);
            match k.as_str() {
                "avro.schema" => schema = Some(v),
                k if k.starts_with("avro.") => {}
                _ => metadata.push((k, v)),
            }
        }
    }
    Some((schema?, metadata))
}

/// A copy of manifest `bytes` in which the data files in `removed`, and
/// the position delete files that reference one of them, are DELETED by
/// `snapshot_id`; every other live entry becomes EXISTING with its
/// sequence numbers made explicit (`inherited`: the manifest's own), and
/// entries deleted earlier are dropped. `None` when nothing matches.
fn rewrite_manifest(
    bytes: &[u8],
    removed: &std::collections::HashSet<String>,
    inherited: i64,
    snapshot_id: i64,
) -> std::result::Result<Option<(Vec<u8>, Counts)>, String> {
    let (schema_text, metadata) = read_header(bytes).ok_or("not an Avro container")?;
    let schema = avro_schema(&schema_text)?;
    let reader = apache_avro::Reader::new(bytes).map_err(|e| e.to_string())?;
    let mut entries = Vec::new();
    let mut counts = Counts::default();
    for entry in reader {
        let entry = entry.map_err(|e| e.to_string())?;
        if field(&entry, "status").map(strip).and_then(long) == Some(2) {
            continue;
        }
        let file = field(&entry, "data_file")
            .map(strip)
            .ok_or("entry without data_file")?;
        let content = field(file, "content")
            .map(strip)
            .and_then(long)
            .unwrap_or(0);
        let text = |name: &str| match field(file, name).map(strip) {
            Some(Avro::String(s)) => Some(s.clone()),
            _ => None,
        };
        let rows = field(file, "record_count")
            .map(strip)
            .and_then(long)
            .unwrap_or(0);
        let gone = match content {
            0 => text("file_path").is_some_and(|p| removed.contains(&p)),
            1 => text("referenced_data_file").is_some_and(|p| removed.contains(&p)),
            _ => false,
        };
        let seq = field(&entry, "sequence_number")
            .map(strip)
            .and_then(long)
            .unwrap_or(inherited);
        let file_seq = field(&entry, "file_sequence_number")
            .map(strip)
            .and_then(long)
            .unwrap_or(seq);
        let mut entry = entry.clone();
        if gone {
            counts.deleted_files += 1;
            counts.deleted_rows += rows;
            if content == 0 {
                counts.data_paths.extend(text("file_path"));
            } else {
                counts.delete_files += 1;
                counts.delete_rows += rows;
            }
            set(&mut entry, "status", Avro::Int(2));
            set(&mut entry, "snapshot_id", Avro::Long(snapshot_id));
        } else {
            counts.existing_files += 1;
            counts.existing_rows += rows;
            counts.min_sequence = Some(counts.min_sequence.map_or(seq, |m| m.min(seq)));
            set(&mut entry, "status", Avro::Int(0));
        }
        set(&mut entry, "sequence_number", Avro::Long(seq));
        set(&mut entry, "file_sequence_number", Avro::Long(file_seq));
        entries.push(conform(&entry, &schema));
    }
    if counts.deleted_files == 0 {
        return Ok(None);
    }
    let metadata: Vec<(&str, String)> = metadata
        .iter()
        .map(|(k, v)| (k.as_str(), v.clone()))
        .collect();
    let mut writer = avro_writer(&schema, &schema_text, &metadata)?;
    for entry in entries {
        writer.append_value(entry).map_err(|e| e.to_string())?;
    }
    Ok(Some((
        writer.into_inner().map_err(|e| e.to_string())?,
        counts,
    )))
}

/// `v` rebuilt for `schema`: unions re-chosen, records matched by field
/// name (v1-era names accepted), missing optional fields null.
fn conform(v: &Avro, schema: &AvroSchema) -> Avro {
    let v = strip(v);
    match schema {
        AvroSchema::Union(u) => match v {
            Avro::Null => Avro::Union(0, Box::new(Avro::Null)),
            v => {
                let (i, branch) = u
                    .variants()
                    .iter()
                    .enumerate()
                    .find(|(_, s)| !matches!(s, AvroSchema::Null))
                    .unwrap_or((0, &AvroSchema::Null));
                Avro::Union(i as u32, Box::new(conform(v, branch)))
            }
        },
        AvroSchema::Record(r) => Avro::Record(
            r.fields
                .iter()
                .map(|f| {
                    let value = field(v, &f.name).cloned().unwrap_or(Avro::Null);
                    (f.name.clone(), conform(&value, &f.schema))
                })
                .collect(),
        ),
        AvroSchema::Array(a) => match v {
            Avro::Array(items) => Avro::Array(items.iter().map(|i| conform(i, &a.items)).collect()),
            other => other.clone(),
        },
        AvroSchema::Int => match v {
            Avro::Long(n) => Avro::Int(*n as i32),
            other => other.clone(),
        },
        AvroSchema::Long => match v {
            Avro::Int(n) => Avro::Long(i64::from(*n)),
            other => other.clone(),
        },
        _ => v.clone(),
    }
}

fn strip(v: &Avro) -> &Avro {
    match v {
        Avro::Union(_, inner) => strip(inner),
        other => other,
    }
}

/// A record's field by name, accepting the v1 names of the count fields.
fn field<'a>(v: &'a Avro, name: &str) -> Option<&'a Avro> {
    let Avro::Record(fields) = v else {
        return None;
    };
    let alias = match name {
        "added_files_count" => "added_data_files_count",
        "existing_files_count" => "existing_data_files_count",
        "deleted_files_count" => "deleted_data_files_count",
        _ => name,
    };
    fields
        .iter()
        .find(|(n, _)| n == name || n == alias)
        .map(|(_, v)| v)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn table(version: i64, spec: Json, fields: Json) -> Json {
        json!({
            "format-version": version, "default-spec-id": 0, "current-schema-id": 0,
            "partition-specs": [{"spec-id": 0, "fields": spec}],
            "schemas": [{"schema-id": 0, "fields": fields}],
        })
    }

    fn flat() -> Json {
        json!([
            {"id": 1, "name": "id", "required": true, "type": "long"},
            {"id": 2, "name": "name", "required": false, "type": "string"},
            {"id": 3, "name": "ts", "required": false, "type": "timestamptz"},
        ])
    }

    #[test]
    fn manifests_round_trip_with_field_ids_partitions_and_bounds() {
        let spec =
            json!([{"source-id": 3, "field-id": 1000, "name": "ts_day", "transform": "day"}]);
        let meta = table(2, spec, flat());
        let layout = Layout::of(&meta).unwrap();
        let stats = vec![
            ColumnStats {
                values: 3,
                nulls: 0,
                lower: Some(Datum::Int(1)),
                upper: Some(Datum::Int(9)),
            },
            ColumnStats {
                values: 3,
                nulls: 1,
                lower: Some(Datum::Bytes("a".into())),
                upper: Some(Datum::Bytes("abcdefghijklmnopqrstuvwxyz".into())),
            },
            ColumnStats {
                values: 3,
                nulls: 3,
                lower: None,
                upper: None,
            },
        ];
        let files = [Written {
            path: "s3://b/t/data/a.parquet".into(),
            rows: 3,
            bytes: 100,
            partition: vec![Some(Datum::Int(19_723))],
            stats: layout
                .columns
                .iter()
                .zip(stats)
                .map(|((id, prim), s)| (*id, *prim, s))
                .collect(),
            referenced: None,
            content: 0,
        }];
        let refs: Vec<&Written> = files.iter().collect();
        let bytes = manifest(&meta, &layout, &refs, 7, 0, 0).unwrap();
        // The header keeps the schema as written: Java reads the stats
        // arrays as maps only with their `logicalType`.
        let head = String::from_utf8_lossy(&bytes[..bytes.len().min(4096)]).into_owned();
        assert!(head.contains(r#""logicalType":"map""#), "{head}");
        let reader = apache_avro::Reader::new(&bytes[..]).unwrap();
        // Readers resolve fields by id: the ids are in the written schema.
        let text = serde_json::to_string(reader.writer_schema()).unwrap();
        for id in [
            r#""field-id":103"#,
            r#""field-id":1000"#,
            r#""field-id":127"#,
        ] {
            assert!(text.contains(id), "{id}: {text}");
        }
        let entries: Vec<Avro> = reader.map(|e| e.unwrap()).collect();
        let data_file = field(&entries[0], "data_file").unwrap();
        assert_eq!(field(data_file, "record_count"), Some(&Avro::Long(3)));
        let partition = field(data_file, "partition").unwrap();
        assert_eq!(
            strip(field(partition, "ts_day").unwrap()),
            &Avro::Int(19_723)
        );
        // The pruner's own decoder reads the bounds back.
        let bounds = |name: &str| match strip(field(data_file, name).unwrap()) {
            Avro::Array(items) => items
                .iter()
                .map(|kv| {
                    (
                        field(kv, "key").cloned().unwrap(),
                        field(kv, "value").cloned().unwrap(),
                    )
                })
                .collect::<Vec<_>>(),
            other => panic!("{other:?}"),
        };
        let lower = bounds("lower_bounds");
        let upper = bounds("upper_bounds");
        assert_eq!(
            lower[0],
            (Avro::Int(1), Avro::Bytes(1i64.to_le_bytes().to_vec()))
        );
        assert_eq!(
            upper[0],
            (Avro::Int(1), Avro::Bytes(9i64.to_le_bytes().to_vec()))
        );
        // Strings are cut to 16 characters; the upper bound stays above.
        assert_eq!(
            upper[1],
            (Avro::Int(2), Avro::Bytes(b"abcdefghijklmnoq".to_vec()))
        );
        // An all-null column has counts but no bounds.
        assert_eq!(lower.len(), 2);
        let nulls = bounds("null_value_counts");
        assert_eq!(nulls[2], (Avro::Int(3), Avro::Long(3)));
        // The list summarizes partitions and carries the parent's
        // manifests, v1-era names included.
        let new = ManifestFile {
            content: 0,
            path: "m1".into(),
            length: 1,
            spec_id: 0,
            sequence: 2,
            snapshot_id: 7,
            files: 1,
            rows: 3,
            partitions: summaries(&layout, &refs),
        };
        assert_eq!(new.partitions[0].1, Some(19_723i32.to_le_bytes().to_vec()));
        let first = manifest_list(
            std::slice::from_ref(&new),
            None,
            6,
            None,
            1,
            &HashMap::new(),
        )
        .unwrap();
        let second = manifest_list(
            &[ManifestFile {
                path: "m2".into(),
                sequence: 3,
                partitions: vec![],
                ..new
            }],
            Some(&first),
            8,
            Some(7),
            3,
            &HashMap::new(),
        )
        .unwrap();
        let entries: Vec<Avro> = apache_avro::Reader::new(&second[..])
            .unwrap()
            .map(|e| e.unwrap())
            .collect();
        let paths: Vec<&Avro> = entries
            .iter()
            .map(|e| field(e, "manifest_path").unwrap())
            .collect();
        assert_eq!(
            paths,
            [&Avro::String("m2".into()), &Avro::String("m1".into())]
        );
        let carried = strip(field(&entries[1], "partitions").unwrap());
        assert!(matches!(carried, Avro::Array(p) if p.len() == 1));
    }

    #[test]
    fn rewritten_manifests_mark_replaced_files_and_their_deletes() {
        let meta = table(2, json!([]), flat());
        let layout = Layout::of(&meta).unwrap();
        let file = |path: &str, rows: u64, referenced: Option<&str>| Written {
            path: path.into(),
            rows,
            bytes: 10,
            partition: vec![],
            stats: vec![],
            content: i32::from(referenced.is_some()),
            referenced: referenced.map(str::to_owned),
        };
        // A data manifest with two files, sequence numbers inherited.
        let data = manifest(
            &meta,
            &layout,
            &[&file("a", 3, None), &file("b", 4, None)],
            7,
            0,
            0,
        )
        .unwrap();
        // Its header is read back as written, Iceberg metadata included.
        let (schema, metadata) = read_header(&data).unwrap();
        assert!(schema.contains(r#""logicalType":"map""#));
        assert!(metadata.contains(&("content".to_owned(), "data".to_owned())));
        let removed = std::collections::HashSet::from(["a".to_owned()]);
        let (copy, counts) = rewrite_manifest(&data, &removed, 5, 9).unwrap().unwrap();
        assert_eq!((counts.deleted_files, counts.existing_files), (1, 1));
        assert_eq!((counts.deleted_rows, counts.existing_rows), (3, 4));
        assert_eq!(counts.data_paths, ["a"]);
        assert_eq!(counts.min_sequence, Some(5));
        let entries: Vec<Avro> = apache_avro::Reader::new(&copy[..])
            .unwrap()
            .map(|e| e.unwrap())
            .collect();
        let status = |e: &Avro| field(e, "status").map(strip).and_then(long);
        let seq = |e: &Avro| field(e, "sequence_number").map(strip).and_then(long);
        // DELETED by the rewrite; EXISTING keeps its (now explicit) seq.
        assert_eq!((status(&entries[0]), seq(&entries[0])), (Some(2), Some(5)));
        assert_eq!((status(&entries[1]), seq(&entries[1])), (Some(0), Some(5)));
        assert_eq!(
            field(&entries[0], "snapshot_id").map(strip),
            Some(&Avro::Long(9))
        );
        // Rewritten again: the earlier DELETED entry is dropped.
        let removed_b = std::collections::HashSet::from(["b".to_owned()]);
        let (again, counts) = rewrite_manifest(&copy, &removed_b, 6, 10).unwrap().unwrap();
        assert_eq!((counts.deleted_files, counts.existing_files), (1, 0));
        assert_eq!(apache_avro::Reader::new(&again[..]).unwrap().count(), 1);
        // A delete manifest: the delete file referencing a replaced file
        // goes; one referencing another file stays. Nothing matches: None.
        let deletes = manifest(
            &meta,
            &layout,
            &[&file("d1", 1, Some("a")), &file("d2", 1, Some("z"))],
            7,
            0,
            1,
        )
        .unwrap();
        let (_, counts) = rewrite_manifest(&deletes, &removed, 5, 9).unwrap().unwrap();
        assert_eq!((counts.delete_files, counts.existing_files), (1, 1));
        assert!(counts.data_paths.is_empty());
        let none = std::collections::HashSet::from(["q".to_owned()]);
        assert!(rewrite_manifest(&deletes, &none, 5, 9).unwrap().is_none());
    }

    #[test]
    fn earlier_deletes_are_not_deleted_again() {
        use datafusion::parquet::arrow::arrow_reader::{RowSelection, RowSelector};
        // Rows 0-1 kept, 2-4 deleted before, 5-6 kept, 7 deleted.
        let selection = RowSelection::from(vec![
            RowSelector::select(2),
            RowSelector::skip(3),
            RowSelector::select(2),
            RowSelector::skip(1),
        ]);
        let kept = kept_ranges(&selection);
        assert_eq!(kept, [(0, 2), (5, 7)]);
        let mut range = 0;
        let survivors: Vec<u64> = (0..9)
            .filter(|p| alive(Some(&kept), *p, &mut range))
            .collect();
        assert_eq!(survivors, [0, 1, 5, 6]);
        let mut range = 0;
        assert!(alive(None, 7, &mut range));
    }

    #[test]
    fn layouts_accept_the_transforms_the_pruner_knows() {
        let ok = |spec: Json| Layout::of(&table(2, spec, flat())).map(|l| l.parts.len());
        assert_eq!(ok(json!([])), Ok(0));
        for transform in ["identity", "bucket[16]", "truncate[4]"] {
            let spec =
                json!([{"source-id": 2, "field-id": 1000, "name": "p", "transform": transform}]);
            assert_eq!(ok(spec), Ok(1), "{transform}");
        }
        for transform in ["year", "month", "day", "hour", "bucket[8]"] {
            let spec =
                json!([{"source-id": 3, "field-id": 1000, "name": "p", "transform": transform}]);
            assert_eq!(ok(spec), Ok(1), "{transform}");
        }
        // A temporal transform of a string, a void field, a missing source.
        for (source, transform) in [(2, "day"), (1, "void"), (9, "identity")] {
            let spec = json!([{"source-id": source, "field-id": 1000, "name": "p", "transform": transform}]);
            assert!(ok(spec).is_err(), "{source} {transform}");
        }
        assert!(
            Layout::of(&table(1, json!([]), flat()))
                .unwrap_err()
                .contains("v2")
        );
        assert!(Layout::of(&table(3, json!([]), flat())).is_err());
        let nested = json!([{"id": 1, "name": "s", "required": false, "type": {"type": "struct", "fields": []}}]);
        assert!(
            Layout::of(&table(2, json!([]), nested))
                .unwrap_err()
                .contains("nested")
        );
    }

    #[test]
    fn rows_split_by_partition_value() {
        use datafusion::arrow::array::{Int64Array, StringArray};
        use datafusion::arrow::datatypes::{Field, Schema};
        let spec = json!([
            {"source-id": 2, "field-id": 1000, "name": "name", "transform": "identity"},
            {"source-id": 1, "field-id": 1001, "name": "id_bucket", "transform": "bucket[2]"},
        ]);
        let layout = Layout::of(&table(2, spec, flat())).unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                true,
            ),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 1, 3])),
                Arc::new(StringArray::from(vec![
                    Some("a"),
                    Some("b"),
                    Some("a"),
                    None,
                ])),
                datafusion::arrow::array::new_null_array(schema.field(2).data_type(), 4),
            ],
        )
        .unwrap();
        #[derive(Debug)]
        struct Never;
        #[async_trait]
        impl Committer for Never {
            async fn current(&self) -> std::result::Result<Json, String> {
                Err("unused".into())
            }
            async fn commit(
                &self,
                _: Vec<Json>,
                _: Vec<Json>,
            ) -> std::result::Result<(), CommitError> {
                Err(CommitError::Failed("unused".into()))
            }
        }
        let sink = IcebergSink {
            name: "t".into(),
            schema,
            location: "file:///t".into(),
            scoped: None,
            committer: Arc::new(Never),
            schema_id: 0,
            layout,
            base: None,
        };
        let groups = sink.split(&batch).unwrap();
        // ("a", bucket(1)) twice, then ("b", ...), then (NULL, ...).
        let sizes: Vec<usize> = groups.iter().map(|(_, b)| b.num_rows()).collect();
        assert_eq!(sizes, [2, 1, 1]);
        assert_eq!(groups[0].0[0], Some(Datum::Bytes(b"a".to_vec())));
        assert_eq!(groups[2].0[0], None);
        // Iceberg's bucket of the long 1 under bucket[2] (spec vector:
        // murmur3 of 1L is 1392991556; 1392991556 % 2 == 0).
        assert_eq!(groups[0].0[1], Some(Datum::Int(0)));
    }
}

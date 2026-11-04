//! Read-only Apache Iceberg connector (format v1/v2, Parquet data files).
//!
//! Plimsoll walks the metadata itself (`metadata.json` -> manifest list ->
//! manifests) and hands the live data files to DataFusion's own Parquet scan,
//! wrapped in [`AccountedExec`]. Manifest decoding is reserved in the
//! session's pool, so the whole read path runs under the `CeilingPool`.
//! See ADR 0007.
//!
//! Correctness first: anything we can't read exactly (column defaults, encryption, renamed
//! columns, non-Parquet files, nested types) is a typed error, never a
//! silently wrong answer.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;

use apache_avro::types::Value as Avro;
use async_trait::async_trait;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::config::TableParquetOptions;
use datafusion::common::stats::Precision;
use datafusion::common::{ColumnStatistics, DFSchema, Statistics};
use datafusion::common::{JoinType, NullEquality};
use datafusion::datasource::listing::{ListingTableUrl, PartitionedFile};
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::datasource::physical_plan::parquet::ParquetRowSelection;
use datafusion::datasource::physical_plan::{FileGroup, FileScanConfigBuilder, ParquetSource};
use datafusion::datasource::source::DataSourceExec;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::memory_pool::MemoryConsumer;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::logical_expr::utils::conjunction;
use datafusion::logical_expr::{Expr, TableType};
use datafusion::object_store::ObjectStoreExt;
use datafusion::parquet::arrow::arrow_reader::RowSelection;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{PhysicalExpr, PhysicalExprRef};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::filter::FilterExecBuilder;
use datafusion::physical_plan::joins::{HashJoinExecBuilder, PartitionMode};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::union::UnionExec;
use futures::StreamExt;
use plimsoll_core::federation::{Pushdown, PushdownPolicy};
use plimsoll_core::memory::AccountedExec;
use plimsoll_core::storage::{ScopedStore, scoped_s3_store};
use serde_json::Value as Json;

use crate::{Connector, ConnectorError, portable};

mod alter;
mod create;
mod deletes;
mod field_ids;
mod files;
pub mod meta;

pub use alter::alter_updates;
pub use create::create_request;
pub mod expire;
mod merge;
mod prune;
mod remote_write;
pub(crate) use remote_write::push_write;
pub mod write;
use deletes::{DeleteFile, EqualityDelete};
use prune::{PartitionField, Prim, Pruner, Summary};

/// A point in a table's history for time travel (ADR 0028).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AsOf {
    /// A snapshot id.
    Snapshot(i64),
    /// The snapshot that was current at this instant (ms since the epoch).
    TimestampMs(i64),
}

impl AsOf {
    /// `4876546583428101137` (snapshot id) or an RFC 3339 instant
    /// (`2024-05-01T00:00:00Z`). Anything else is not a time-travel spec.
    pub fn parse(spec: &str) -> Option<Self> {
        if let Ok(id) = spec.parse::<i64>() {
            return Some(Self::Snapshot(id));
        }
        chrono::DateTime::parse_from_rfc3339(spec)
            .ok()
            .map(|t| Self::TimestampMs(t.timestamp_millis()))
    }
}

/// `metadata` with `as_of` made current: `current-snapshot-id`, and
/// `current-schema-id` when the snapshot records the schema it was written
/// with, so old snapshots read with their own columns. Timestamps resolve
/// through `snapshot-log` (the main branch's history), never through
/// `snapshots`, which may hold other branches. Errors are messages for the
/// caller (a bad id or an instant before the first snapshot).
pub fn at(metadata: &Json, as_of: AsOf) -> Result<Json, String> {
    let id = match as_of {
        AsOf::Snapshot(id) => id,
        AsOf::TimestampMs(ts) => {
            let log = metadata["snapshot-log"]
                .as_array()
                .ok_or("table has no snapshot-log to travel by time")?;
            log.iter()
                .filter_map(|e| Some((e["timestamp-ms"].as_i64()?, e["snapshot-id"].as_i64()?)))
                .filter(|(t, _)| *t <= ts)
                .max_by_key(|(t, _)| *t)
                .map(|(_, id)| id)
                .ok_or_else(|| format!("no snapshot at or before {ts} ms"))?
        }
    };
    let snapshot = metadata["snapshots"]
        .as_array()
        .and_then(|s| s.iter().find(|s| s["snapshot-id"].as_i64() == Some(id)))
        .ok_or_else(|| format!("snapshot {id} not found"))?;
    let mut pinned = metadata.clone();
    pinned["current-snapshot-id"] = Json::from(id);
    if let Some(schema) = snapshot["schema-id"].as_i64() {
        pinned["current-schema-id"] = Json::from(schema);
    }
    Ok(pinned)
}

/// The snapshot a branch or tag (`refs`) points at (ADR 0029). A branch's
/// head is its latest snapshot; a tag is fixed.
pub fn ref_snapshot(metadata: &Json, name: &str) -> Option<i64> {
    metadata["refs"][name]["snapshot-id"].as_i64()
}

#[derive(Debug)]
pub struct IcebergConnector {
    name: String,
    table: Arc<IcebergTable>,
    policy: PushdownPolicy,
}

impl IcebergConnector {
    /// `location` is a `*.metadata.json` file or a table directory, in which
    /// case the newest `metadata/*.metadata.json` is used.
    ///
    /// ponytail: the snapshot is pinned at registration; re-register to see
    /// new commits. Tables served by a REST catalog are re-resolved per query
    /// instead (server `lakehouse` module, via [`Self::from_metadata`]).
    pub async fn try_new(
        name: impl Into<String>,
        location: &str,
        state: &dyn Session,
    ) -> Result<Self, ConnectorError> {
        let metadata_path = resolve_metadata(location, state).await?;
        let raw = read(&metadata_path, state, None).await?;
        let meta: Json = serde_json::from_slice(&raw).map_err(|e| invalid(&metadata_path, e))?;
        Self::from_metadata(name, &metadata_path, &meta)
    }

    /// A worker's view of a coordinator's scan (ADR 0058): the table at
    /// `metadata_path`, pinned to `snapshot`, reading only `files`.
    /// Planning (deletes included) runs here as for any scan; only the set
    /// of data files is narrowed.
    pub async fn fragment(
        metadata_path: &str,
        snapshot: Option<i64>,
        files: HashSet<String>,
        state: &dyn Session,
    ) -> Result<Self, ConnectorError> {
        let raw = read(metadata_path, state, None).await?;
        let mut meta: Json = serde_json::from_slice(&raw).map_err(|e| invalid(metadata_path, e))?;
        if let Some(id) = snapshot {
            meta = at(&meta, AsOf::Snapshot(id)).map_err(|m| invalid(metadata_path, m))?;
        }
        let mut connector = Self::from_metadata("fragment", metadata_path, &meta)?;
        let mut table = (*connector.table).clone();
        table.only_files = Some(Arc::new(files));
        connector.table = Arc::new(table);
        Ok(connector)
    }

    /// A worker's part of a distributed `MERGE` (ADR 0094): `spec`'s
    /// `WHEN MATCHED` and `NOT MATCHED BY SOURCE` over `files` (the table
    /// at `metadata_path` and `snapshot`), `source` being the whole source.
    /// The files written, then `{"matched": [...], "changed": n}`: the
    /// source rows that paired, and the target rows changed.
    pub async fn merge_files(
        metadata_path: &str,
        snapshot: Option<i64>,
        files: HashSet<String>,
        spec: &crate::remote::MergeSpec,
        source: (datafusion::arrow::datatypes::SchemaRef, Vec<RecordBatch>),
        state: &dyn Session,
    ) -> Result<Vec<Json>, ConnectorError> {
        let connector = Self::fragment(metadata_path, snapshot, files, state).await?;
        let table = &connector.table;
        let (sink, targets, _, _) = table
            .rows_with(table.file_writer()?, state, Vec::new())
            .await?;
        let (schema, batches) = source;
        let source = datafusion::arrow::compute::concat_batches(&schema, &batches)?;
        let merge = merge::from_spec(spec, &table.schema, &schema, state)?;
        let index = merge::Index::new(&merge, source)?;
        let task = state.task_ctx();
        let probed = merge::probe(&sink, &targets, &index, &merge, &task).await?;
        let mut written = probed.files;
        written.extend(merge::write_new_rows(&sink, probed.new_rows, &task).await?);
        let mut out: Vec<Json> = written.iter().map(write::Written::to_json).collect();
        let matched: Vec<usize> = (0..probed.matched.len())
            .filter(|i| probed.matched[*i])
            .collect();
        out.push(serde_json::json!({"matched": matched, "changed": probed.changed}));
        Ok(out)
    }

    /// A worker's part of a distributed `DELETE` or `UPDATE` (ADR 0093):
    /// the rows of `files` (the table at `metadata_path` and `snapshot`)
    /// matching `predicate` are deleted by position, and with `update`
    /// rewritten with those new values (SQL over the table's columns). The
    /// files written, not committed.
    pub async fn rewrite_files(
        metadata_path: &str,
        snapshot: Option<i64>,
        files: HashSet<String>,
        change: &crate::remote::RowChange,
        state: &dyn Session,
    ) -> Result<Vec<Json>, ConnectorError> {
        let connector = Self::fragment(metadata_path, snapshot, files, state).await?;
        let table = &connector.table;
        let schema = DFSchema::try_from(table.schema.as_ref().clone())?;
        let parse = |sql: &str| {
            portable::parse(sql, &schema).ok_or_else(|| {
                ConnectorError::Unsupported(format!("expression {sql:?} does not parse here"))
            })
        };
        let filter = parse(&change.predicate)?;
        let (sink, targets, predicate, schema) = table
            .rows_with(table.file_writer()?, state, vec![filter])
            .await?;
        let update = match &change.update {
            None => None,
            Some(row) => Some(
                row.iter()
                    .map(|sql| Ok(state.create_physical_expr(parse(sql)?, &schema)?))
                    .collect::<Result<Vec<_>, ConnectorError>>()?,
            ),
        };
        let written = sink
            .rewrite(&targets, &predicate, update.as_deref(), &state.task_ctx())
            .await?;
        Ok(written.iter().map(write::Written::to_json).collect())
    }

    /// A worker's part of a distributed write (ADR 0091): `rows` written
    /// as data files of the table at `metadata_path` (its current schema
    /// and partition spec), not committed. Each file as the coordinator
    /// commits it.
    ///
    /// `staged`: a staged create's metadata, which has no file yet (ADR
    /// 0092), used instead of reading `metadata_path`.
    pub async fn write_files(
        metadata_path: &str,
        staged: Option<&Json>,
        rows: datafusion::execution::SendableRecordBatchStream,
        state: &dyn Session,
    ) -> Result<Vec<Json>, ConnectorError> {
        let meta: Json = match staged {
            Some(meta) => meta.clone(),
            None => {
                let raw = read(metadata_path, state, None).await?;
                serde_json::from_slice(&raw).map_err(|e| invalid(metadata_path, e))?
            }
        };
        let connector = Self::from_metadata("write", metadata_path, &meta)?;
        let sink = connector.table.file_writer()?;
        let written = sink.write_rows(rows, &state.task_ctx()).await?;
        Ok(written.iter().map(write::Written::to_json).collect())
    }

    /// From metadata already in hand, e.g. a REST catalog `loadTable`
    /// response. `location` is only used in error messages.
    pub fn from_metadata(
        name: impl Into<String>,
        location: &str,
        metadata: &Json,
    ) -> Result<Self, ConnectorError> {
        let name = name.into();
        let table = IcebergTable::from_metadata(&name, location, metadata)?;
        Ok(Self {
            name,
            table: Arc::new(table),
            // Every filter is evaluated by DataFusion itself (no remote
            // semantics to distrust, unlike Postgres): the policy lets the
            // table's own claim stand. It claims Exact only for identity
            // partition filters it enforces (ADR 0055); the rest are
            // Inexact and re-checked.
            policy: PushdownPolicy::uniform(Pushdown::Exact),
        })
    }

    /// Read with storage credentials handed out by the catalog (Iceberg FileIO
    /// properties from `loadTable`, e.g. `s3.access-key-id`) instead of the
    /// process env. Scoped to the table's bucket; other buckets are untouched.
    pub fn with_storage_properties(
        mut self,
        props: &BTreeMap<String, String>,
    ) -> Result<Self, ConnectorError> {
        let Some(location) = &self.table.location else {
            return Ok(self);
        };
        if let Some(scoped) = scoped_s3_store(location, props)? {
            let mut table = (*self.table).clone();
            table.scoped = Some(scoped);
            self.table = Arc::new(table);
        }
        Ok(self)
    }

    pub fn snapshot_id(&self) -> Option<i64> {
        self.table.snapshot_id
    }

    /// Rewrites small files and files carrying deletes into fresh ones,
    /// partition by partition (ADR 0078). Readers then stop paying for
    /// accumulated deletes and small appends.
    pub async fn compact(&self, state: &dyn Session) -> Result<Compaction, ConnectorError> {
        Ok(self.table.compact(state).await?)
    }

    /// Expires old snapshots through the catalog and deletes the files
    /// only they reached (ADR 0079).
    pub async fn expire_snapshots(
        &self,
        state: &dyn Session,
        opts: expire::ExpireOptions,
    ) -> Result<expire::Expiration, ConnectorError> {
        let sink = self.table.writable("expire snapshots of")?;
        self.table.register_scoped(state);
        Ok(expire::expire(
            sink.committer.as_ref(),
            opts,
            state.runtime_env(),
            self.table.scoped.as_ref(),
        )
        .await?)
    }

    /// Claims the table for `ttl_ms` of scheduled maintenance by
    /// `holder`; `false` when another coordinator holds it (ADR 0101).
    pub async fn claim_maintenance(
        &self,
        state: &dyn Session,
        holder: &str,
        ttl_ms: i64,
    ) -> Result<bool, ConnectorError> {
        let sink = self.table.writable("maintain")?;
        self.table.register_scoped(state);
        Ok(expire::claim(
            &sink.location,
            holder,
            ttl_ms,
            state.runtime_env(),
            self.table.scoped.as_ref(),
        )
        .await?)
    }

    /// Finds (and unless `dry_run`, deletes) files under the table
    /// location that nothing references (ADR 0080).
    pub async fn remove_orphans(
        &self,
        state: &dyn Session,
        opts: expire::OrphanOptions,
    ) -> Result<expire::Orphans, ConnectorError> {
        let sink = self.table.writable("remove orphan files of")?;
        self.table.register_scoped(state);
        let meta = sink
            .committer
            .current()
            .await
            .map_err(|m| ConnectorError::DataFusion(DataFusionError::Execution(m)))?;
        Ok(expire::orphans(
            &meta,
            &sink.location,
            opts,
            state.runtime_env(),
            self.table.scoped.as_ref(),
        )
        .await?)
    }

    /// Accept `INSERT INTO`, committing through `committer` (ADR 0075).
    /// `CREATE TABLE … AS SELECT` (ADR 0090): this connector is built on
    /// `staged`, a staged create's metadata; `rows` are written into it and
    /// one commit creates the table with them. The rows written.
    ///
    /// `plan` is the query: when its rows come from workers, they write
    /// them into the staged table where they are (ADR 0092).
    pub async fn create_as(
        &self,
        state: &dyn Session,
        staged: &Json,
        plan: Arc<dyn ExecutionPlan>,
    ) -> Result<u64, ConnectorError> {
        let mut sink = self.table.writable("create")?;
        self.table.register_scoped(state);
        let task = state.task_ctx();
        sink.staged = Some(staged.clone());
        if let Some(write) = remote_write::distribute(&sink, &plan, remote_write::count_schema()) {
            use datafusion::arrow::array::AsArray;
            use datafusion::arrow::datatypes::UInt64Type;
            let batches = datafusion::physical_plan::collect(Arc::new(write), task).await?;
            return Ok(batches
                .iter()
                .map(|b| b.column(0).as_primitive::<UInt64Type>().value(0))
                .sum());
        }
        let rows = datafusion::physical_plan::execute_stream(plan, task.clone())?;
        Ok(sink.create(staged.clone(), rows, &task).await?)
    }

    pub fn with_committer(mut self, committer: Arc<dyn write::Committer>) -> Self {
        let mut table = (*self.table).clone();
        table.committer = Some(committer);
        self.table = Arc::new(table);
        self
    }

    /// `$files` / `$manifests` of the pinned snapshot (ADR 0032): manifests
    /// are read at execution, one at a time, under the session's pool.
    /// `None` when `kind` is not one of them.
    pub fn metadata_table(&self, kind: &str) -> Option<Result<Arc<dyn TableProvider>>> {
        files::provider(self.table.clone(), kind)
    }
}

impl Connector for IcebergConnector {
    fn name(&self) -> &str {
        &self.name
    }
    fn policy(&self) -> &PushdownPolicy {
        &self.policy
    }
    fn source(&self) -> Arc<dyn TableProvider> {
        self.table.clone()
    }
}

/// `e` with its columns unqualified: DML expressions name the table's own
/// columns (ADR 0077, 0081).
fn unqualified(e: Expr) -> Result<Expr> {
    use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
    e.transform(|e| match e {
        Expr::Column(c) => Ok(Transformed::yes(Expr::Column(
            datafusion::common::Column::new_unqualified(c.name),
        ))),
        e => Ok(Transformed::no(e)),
    })
    .data()
}

/// What a compaction did (ADR 0078).
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Compaction {
    /// Partitions with files rewritten.
    pub partitions: usize,
    pub files_rewritten: usize,
    pub files_written: usize,
    /// Live rows carried over.
    pub rows: u64,
}

/// Data files below this are "small" and compacted with their partition's
/// other small files: 75% of the target size new files are written at,
/// as Iceberg's `min-file-size-bytes` default.
const SMALL_FILE_BYTES: u64 = 96 << 20;

#[derive(Debug, Clone)]
struct IcebergTable {
    name: String,
    schema: SchemaRef,
    snapshot_id: Option<i64>,
    /// The schema its snapshot was written with, when the metadata says.
    snapshot_schema_id: Option<i64>,
    /// `None` for a table without snapshots (empty).
    manifest_list: Option<String>,
    /// Table base location (`location` in the metadata).
    location: Option<String>,
    /// Catalog-vended credentials for the table's bucket, if any.
    scoped: Option<ScopedStore>,
    /// Prunable columns of the current schema: name -> (field id, type).
    columns: HashMap<String, (i64, Prim)>,
    /// `partition-specs` by spec id, for manifest-level pruning.
    specs: HashMap<i64, Vec<Option<PartitionField>>>,
    /// Spec ids without a non-void field (equality deletes there are global).
    unpartitioned: HashSet<i64>,
    /// Field id -> column name in the current schema (scan column
    /// resolution, equality delete keys; ADR 0040).
    names: Arc<HashMap<i64, String>>,
    /// Some column was renamed or re-added in the table's history.
    renamed: bool,
    /// Nested columns with every nested field's id (ADR 0088).
    nested_ids: Arc<HashMap<String, datafusion::arrow::datatypes::FieldRef>>,
    /// `schema.name-mapping.default`, for files without field ids (ADR 0042).
    name_mapping: Option<Arc<field_ids::NameMapping>>,
    /// Identity partition sources common to every spec (ADR 0055).
    identity: HashSet<i64>,
    /// The `metadata.json` this table was read from (ADR 0058: what a
    /// worker re-opens).
    metadata_path: String,
    /// Plan files with no live rows too (compaction, ADR 0078).
    keep_empty: bool,
    /// Where `INSERT INTO` commits (ADR 0075); `None`: not writable.
    committer: Option<Arc<dyn write::Committer>>,
    /// How appended rows are laid out, or why this table cannot take an
    /// append (ADR 0075, 0076).
    layout: std::result::Result<write::Layout, String>,
    /// `current-schema-id`, which appended files are written against.
    schema_id: i64,
    /// A worker's fragment: only these data files (ADR 0058).
    only_files: Option<Arc<HashSet<String>>>,
}

/// A data file the scan will read, from its manifest entry.
struct DataFile {
    path: String,
    /// `file_size_in_bytes`; saves a HEAD per file on object storage.
    size: Option<u64>,
    /// `record_count`; needed to apply position deletes.
    rows: Option<u64>,
    /// Data sequence number (inherited from the manifest when absent).
    seq: i64,
    /// Rows surviving position deletes; `None` = all rows.
    selection: Option<RowSelection>,
    /// Partition spec id and tuple, to match equality deletes.
    spec: i64,
    partition: Option<Avro>,
    /// Indices into the planned equality delete sets that apply to this file.
    eq: Vec<usize>,
    /// v3: the row id of the file's first row, explicit or inherited from
    /// its manifest (ADR 0096). `None`: no row lineage.
    first_row_id: Option<i64>,
}

/// Keys deleted by one equality delete file (ADR 0016).
struct EqualitySet {
    /// Key columns, as fields of the table schema.
    key: Vec<Field>,
    batches: Vec<RecordBatch>,
}

/// What planning skipped, shown in EXPLAIN
/// (`IcebergScan[t] manifests=1/4 files=2/40 deletes=1 deleted_rows=3`).
#[derive(Default)]
struct PlanStats {
    manifests: usize,
    manifests_read: usize,
    files: usize,
    files_kept: usize,
    delete_files: usize,
    deleted_rows: usize,
    equality_files: usize,
    /// Delete files served from the cache (ADR 0018).
    cache_hits: usize,
    /// Manifest lists and manifests served from the cache (ADR 0039).
    manifest_cache_hits: usize,
    /// Per top-level column (by field id), over the kept data files: what
    /// their statistics say (ADR 0113).
    columns: HashMap<i64, ColumnBounds>,
}

/// One column over the kept data files (ADR 0113): the union of their
/// bounds and the sum of their null counts, each kept only while every
/// file has it. Bounds stay bounds whatever deletes remove; counts become
/// upper bounds. Both are estimates for DataFusion, never answers.
#[derive(Debug, Clone)]
struct ColumnBounds {
    prim: Prim,
    /// Every file so far had both bounds.
    bounded: bool,
    lo: Option<prune::Datum>,
    hi: Option<prune::Datum>,
    nulls: Option<u64>,
}

impl ColumnBounds {
    fn new(prim: Prim) -> Self {
        Self {
            prim,
            bounded: true,
            lo: None,
            hi: None,
            nulls: Some(0),
        }
    }

    fn add(
        &mut self,
        id: i64,
        lower: &HashMap<i64, Vec<u8>>,
        upper: &HashMap<i64, Vec<u8>>,
        nulls: &HashMap<i64, i64>,
    ) {
        let decode = |m: &HashMap<i64, Vec<u8>>| m.get(&id).and_then(|b| self.prim.decode(b));
        match (decode(lower), decode(upper)) {
            (Some(lo), Some(hi)) if self.bounded => {
                if self.lo.as_ref().is_none_or(|l| lo < *l) {
                    self.lo = Some(lo);
                }
                if self.hi.as_ref().is_none_or(|h| hi > *h) {
                    self.hi = Some(hi);
                }
            }
            // An all-null file has no bounds either: unknown, not empty.
            _ => self.bounded = false,
        }
        self.nulls = match (
            self.nulls,
            nulls.get(&id).and_then(|n| u64::try_from(*n).ok()),
        ) {
            (Some(total), Some(n)) => Some(total + n),
            _ => None,
        };
    }

    fn statistics(&self, ty: &DataType) -> ColumnStatistics {
        let mut c = ColumnStatistics::new_unknown();
        if let (true, Some(lo), Some(hi)) = (self.bounded, &self.lo, &self.hi) {
            if let (Some(lo), Some(hi)) = (self.prim.scalar(lo, ty), self.prim.scalar(hi, ty)) {
                c.min_value = Precision::Inexact(lo);
                c.max_value = Precision::Inexact(hi);
            }
        }
        if let Some(n) = self.nulls.and_then(|n| usize::try_from(n).ok()) {
            c.null_count = Precision::Inexact(n);
        }
        c
    }
}

impl IcebergTable {
    fn from_metadata(name: &str, path: &str, meta: &Json) -> Result<Self, ConnectorError> {
        let err = |m: &str| invalid(path, m);
        let version = meta["format-version"].as_i64().unwrap_or(1);
        if !(1..=3).contains(&version) {
            return Err(unsupported(path, format!("format-version {version}")));
        }
        if meta["encryption-keys"]
            .as_array()
            .is_some_and(|k| !k.is_empty())
        {
            return Err(unsupported(path, "encrypted tables".to_owned()));
        }
        let schemas: Vec<&Json> = match meta["schemas"].as_array() {
            Some(s) => s.iter().collect(),
            None => vec![&meta["schema"]], // v1 tables may only have `schema`
        };
        let current = match meta["current-schema-id"].as_i64() {
            Some(id) => schemas
                .iter()
                .find(|s| s["schema-id"].as_i64() == Some(id))
                .copied()
                .ok_or_else(|| err("current-schema-id not found"))?,
            None => schemas.first().copied().ok_or_else(|| err("no schema"))?,
        };
        let renamed = renamed_columns(path, &schemas)?;
        // Nested children resolve by field id in the scan (ADR 0088), so
        // renamed, moved or re-added nested fields read right.
        let mut nested = Vec::new();
        for s in &schemas {
            for f in s["fields"].as_array().into_iter().flatten() {
                nested_fields(&f["type"], f["id"].as_i64().unwrap_or(-1), &mut nested);
            }
        }
        for (_, f) in &nested {
            let name = f["name"].as_str().unwrap_or_default();
            if !f["initial-default"].is_null() {
                return Err(unsupported(
                    path,
                    format!("nested field default values ({name:?})"),
                ));
            }
        }
        // v3 defaults: files written before a column was added must read its
        // `initial-default`; the Parquet scan would return NULL instead.
        if let Some(f) = current["fields"]
            .as_array()
            .into_iter()
            .flatten()
            .find(|f| !f["initial-default"].is_null())
        {
            return Err(unsupported(
                path,
                format!(
                    "column default values ({:?})",
                    f["name"].as_str().unwrap_or_default()
                ),
            ));
        }
        let schema = arrow_schema(path, current)?;
        let nested_ids: Arc<HashMap<String, datafusion::arrow::datatypes::FieldRef>> = Arc::new(
            arrow_write_schema(path, current)?
                .fields()
                .iter()
                .filter(|f| f.data_type().is_nested())
                .map(|f| (f.name().clone(), f.clone()))
                .collect(),
        );

        let snapshot_id = meta["current-snapshot-id"].as_i64().filter(|id| *id != -1);
        let snapshot_schema_id = meta["snapshots"]
            .as_array()
            .into_iter()
            .flatten()
            .find(|s| snapshot_id.is_some() && s["snapshot-id"].as_i64() == snapshot_id)
            .and_then(|s| s["schema-id"].as_i64());
        let manifest_list = match snapshot_id {
            None => None,
            Some(id) => {
                let snap = meta["snapshots"]
                    .as_array()
                    .and_then(|s| s.iter().find(|s| s["snapshot-id"].as_i64() == Some(id)))
                    .ok_or_else(|| err("current snapshot not found"))?;
                let list = snap["manifest-list"].as_str().ok_or_else(|| {
                    unsupported(path, "v1 snapshot without manifest-list".to_owned())
                })?;
                Some(list.to_owned())
            }
        };
        let mut paths = Vec::new();
        prunable(&current["fields"], "", &mut paths);
        // A top-level column literally named `s.a` would share a key with
        // struct child `s.a`: such paths are not pruned at all.
        let mut columns: HashMap<String, (i64, Prim)> = HashMap::new();
        let mut clashes = HashSet::new();
        for (path, field) in paths {
            if columns.insert(path.clone(), field).is_some() {
                clashes.insert(path);
            }
        }
        columns.retain(|path, _| !clashes.contains(path));
        let specs = meta["partition-specs"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|s| Some((s["spec-id"].as_i64()?, prune::partition_spec(&s["fields"]))))
            .collect();
        let identity = prune::identity_sources(&specs);
        let unpartitioned = meta["partition-specs"]
            .as_array()
            .into_iter()
            .flatten()
            .filter(|s| {
                s["fields"]
                    .as_array()
                    .is_none_or(|f| f.iter().all(|f| f["transform"] == "void"))
            })
            .filter_map(|s| s["spec-id"].as_i64())
            .collect();
        let names: Arc<HashMap<i64, String>> = Arc::new(
            current["fields"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|f| Some((f["id"].as_i64()?, f["name"].as_str()?.to_owned())))
                .collect(),
        );
        let name_mapping = match meta["properties"]["schema.name-mapping.default"].as_str() {
            Some(json) => Some(Arc::new(
                field_ids::parse_name_mapping(json).map_err(|m| invalid(path, m))?,
            )),
            None => None,
        };
        // Tables with renamed/re-added columns, or a name mapping (aliases),
        // carry field ids in their schema so every differing file is
        // adapted by id (ADR 0040, 0042).
        let schema = Arc::new(if renamed || name_mapping.is_some() {
            field_ids::with_field_ids(&schema, &names)
        } else {
            schema
        });
        Ok(Self {
            name: name.to_owned(),
            schema,
            snapshot_id,
            snapshot_schema_id,
            manifest_list,
            location: meta["location"].as_str().map(str::to_owned),
            scoped: None,
            columns,
            specs,
            unpartitioned,
            names,
            renamed,
            nested_ids,
            name_mapping,
            identity,
            metadata_path: path.to_owned(),
            committer: None,
            keep_empty: false,
            layout: write::Layout::of(meta),
            schema_id: meta["current-schema-id"].as_i64().unwrap_or(0),
            only_files: None,
        })
    }

    /// The share of rows `filters` keep (ADR 0113): DataFusion's interval
    /// analysis over the columns' bounds, else its default filter
    /// selectivity, as FilterExec estimates.
    fn selectivity(
        &self,
        state: &dyn Session,
        filters: &[Expr],
        columns: &[ColumnStatistics],
    ) -> f64 {
        let default =
            f64::from(state.config_options().optimizer.default_filter_selectivity) / 100.0;
        let analyzed = || -> Option<f64> {
            let predicate = datafusion::logical_expr::utils::conjunction(filters.to_vec())?;
            let schema = DFSchema::try_from(self.schema.as_ref().clone()).ok()?;
            let physical = state.create_physical_expr(predicate, &schema).ok()?;
            let context = datafusion::physical_expr::AnalysisContext::try_from_statistics(
                &self.schema,
                columns,
            )
            .ok()?;
            datafusion::physical_expr::analyze(&physical, context, &self.schema)
                .ok()?
                .selectivity
        };
        analyzed().unwrap_or(default)
    }

    /// Live data files of the pinned snapshot that may match `pruner`, with
    /// position deletes applied as row selections (ADR 0014) and the equality
    /// delete sets that apply to each (ADR 0016). Manifest bytes, the planned
    /// file list and delete state are reserved in the session pool. Pruned
    /// manifests are never read.
    async fn data_files(
        &self,
        state: &dyn Session,
        pruner: &Pruner,
    ) -> Result<(Vec<DataFile>, Vec<EqualitySet>, PlanStats)> {
        let mut stats = PlanStats::default();
        let Some(list_path) = &self.manifest_list else {
            return Ok((vec![], vec![], stats));
        };
        // Top-level columns whose bounds compare (ADR 0113).
        let tracked: Vec<(i64, Prim)> = self
            .schema
            .fields()
            .iter()
            .filter_map(|f| self.columns.get(f.name()).copied())
            .collect();
        self.register_scoped(state);
        let reservation = MemoryConsumer::new(format!("IcebergPlanning[{}]", self.name))
            .register(&state.runtime_env().memory_pool);
        // Raw Avro bytes are reserved while in use; entries are decoded one at
        // a time (never collected), so decoded state stays O(one entry).
        // Manifest lists and manifests are immutable files, so their bytes
        // are cached by path with the delete files (ADR 0039): a repeated
        // query over the same snapshot plans without object-store reads.
        // Returns the bytes and how many of them this query reserved (0 on
        // a hit: the cache accounts for them).
        let manifest_hits = AtomicUsize::new(0);
        let load = async |path: &str| -> Result<(bytes::Bytes, usize)> {
            let (bytes, held) = deletes::cached(
                state,
                &format!("manifest/{path}"),
                &reservation,
                &manifest_hits,
                async {
                    let bytes = read(path, state, self.scoped.as_ref()).await?;
                    let n = bytes.len();
                    Ok((bytes, n))
                },
            )
            .await?;
            Ok(((*bytes).clone(), held))
        };

        // Object store round trips dominate planning: manifests and delete
        // files are fetched `meta_fetch_concurrency` at a time (ADR 0020).
        // Each in-flight fetch is reserved, so the window is under the ceiling.
        let concurrency = usize::from(state.config_options().execution.meta_fetch_concurrency);
        let hits = AtomicUsize::new(0);

        // Phase 1a: the manifest list decides which manifests to read.
        let mut manifests = Vec::new();
        let (list_bytes, list_held) = load(list_path).await?;
        for manifest in avro_entries(list_path, &list_bytes)? {
            let manifest = manifest?;
            let manifest_path = str_field(&manifest, "manifest_path")
                .ok_or_else(|| invalid(list_path, "manifest_path missing"))?
                .to_owned();
            // v1 has no `content`; 0 = data, 1 = deletes.
            let is_delete = long_field(&manifest, "content").unwrap_or(0) == 1;
            let manifest_seq = long_field(&manifest, "sequence_number").unwrap_or(0);
            let spec_id = long_field(&manifest, "partition_spec_id").unwrap_or(0);
            let first_row_id = long_field(&manifest, "first_row_id");
            stats.manifests += 1;
            // Delete manifests prune like data manifests: a position delete
            // shares its data file's partition, so if no data file there can
            // match, neither can its deletes.
            if !pruner.is_empty() {
                if let Some(spec) = self.specs.get(&spec_id) {
                    if !pruner.may_match_manifest(spec, &summaries(&manifest)) {
                        continue;
                    }
                }
            }
            stats.manifests_read += 1;
            manifests.push((
                manifest_path,
                is_delete,
                manifest_seq,
                spec_id,
                first_row_id,
            ));
        }
        reservation.shrink(list_held);

        // Phase 1b: data files and delete files that survive pruning, from
        // manifests fetched concurrently and decoded in list order.
        let mut files = Vec::new();
        let mut deletes = Vec::new();
        let mut equality = Vec::new();
        let mut fetched = futures::stream::iter(manifests)
            .map(|(path, is_delete, seq, spec, first_row_id)| {
                let load = &load;
                async move {
                    let (bytes, held) = load(&path).await?;
                    Ok::<_, DataFusionError>((
                        path,
                        is_delete,
                        seq,
                        spec,
                        first_row_id,
                        bytes,
                        held,
                    ))
                }
            })
            .buffered(concurrency);
        while let Some(manifest) = fetched.next().await {
            let (
                manifest_path,
                is_delete,
                manifest_seq,
                spec_id,
                mut inherited,
                manifest_bytes,
                held,
            ) = manifest?;
            for entry in avro_entries(&manifest_path, &manifest_bytes)? {
                let entry = entry?;
                // v3 row lineage (ADR 0096): a data file without its own first
                // row id takes the next ones of its manifest's range, in entry
                // order, every entry counted.
                let first_row_id = match field(&entry, "data_file") {
                    Some(file) if !is_delete => match long_field(file, "first_row_id") {
                        Some(id) => Some(id),
                        None => {
                            let id = inherited;
                            if let Some(first) = id {
                                inherited =
                                    Some(first + long_field(file, "record_count").unwrap_or(0));
                            }
                            id
                        }
                    },
                    _ => None,
                };
                // status 2 = deleted in this snapshot.
                if long_field(&entry, "status") == Some(2) {
                    continue;
                }
                // Spec: a null entry sequence number inherits the manifest's.
                let seq = long_field(&entry, "sequence_number").unwrap_or(manifest_seq);
                let file = field(&entry, "data_file")
                    .ok_or_else(|| invalid(&manifest_path, "data_file missing"))?;
                let format = str_field(file, "file_format").unwrap_or("");
                // v3 deletion vectors live in Puffin files (ADR 0017).
                let dv = is_delete && format.eq_ignore_ascii_case("puffin");
                if !format.eq_ignore_ascii_case("parquet") && !dv {
                    return Err(
                        unsupported(&manifest_path, format!("{format:?} data files")).into(),
                    );
                }
                // Encrypted files would decode as garbage; refuse them.
                if field(file, "key_metadata").is_some_and(|k| *k != Avro::Null) {
                    return Err(
                        unsupported(&manifest_path, "encrypted data files".to_owned()).into(),
                    );
                }
                let path = str_field(file, "file_path")
                    .ok_or_else(|| invalid(&manifest_path, "file_path missing"))?;
                let size = long_field(file, "file_size_in_bytes")
                    .and_then(|s| u64::try_from(s).ok())
                    .filter(|s| *s > 0);
                match (is_delete, long_field(file, "content").unwrap_or(0)) {
                    (false, 0) => {}
                    (true, 1) => {
                        let blob = if dv {
                            let offset = long_field(file, "content_offset")
                                .and_then(|o| u64::try_from(o).ok());
                            let size = long_field(file, "content_size_in_bytes")
                                .and_then(|s| u64::try_from(s).ok());
                            let (Some(offset), Some(size)) = (offset, size) else {
                                return Err(invalid(
                                    path,
                                    "deletion vector without content offset/size",
                                )
                                .into());
                            };
                            Some((offset, size))
                        } else {
                            None
                        };
                        reservation.try_grow(path.len() + std::mem::size_of::<DeleteFile>())?;
                        deletes.push(DeleteFile {
                            path: path.to_owned(),
                            seq,
                            referenced: str_field(file, "referenced_data_file").map(str::to_owned),
                            blob,
                        });
                        continue;
                    }
                    (true, 2) => {
                        let Some(Avro::Array(ids)) = field(file, "equality_ids") else {
                            return Err(
                                invalid(path, "equality delete without equality_ids").into()
                            );
                        };
                        let ids = ids
                            .iter()
                            .map(|id| match id {
                                Avro::Int(i) => Ok(i64::from(*i)),
                                _ => Err(invalid(path, "equality_ids is not a list of int")),
                            })
                            .collect::<Result<Vec<_>, _>>()?;
                        reservation.try_grow(path.len() + std::mem::size_of::<EqualityDelete>())?;
                        equality.push(EqualityDelete {
                            path: path.to_owned(),
                            seq,
                            spec: spec_id,
                            partition: field(file, "partition").cloned(),
                            ids,
                        });
                        continue;
                    }
                    (_, content) => {
                        let kind = if is_delete { "delete" } else { "data" };
                        return Err(invalid(
                            &manifest_path,
                            format!("content {content} in a {kind} manifest"),
                        )
                        .into());
                    }
                }
                stats.files += 1;
                let (lower, upper) = (
                    bound_map(file, "lower_bounds"),
                    bound_map(file, "upper_bounds"),
                );
                let nulls = count_map(file, "null_value_counts");
                if !pruner.is_empty()
                    && !(pruner.may_match_partition(
                        self.specs.get(&spec_id).map_or(&[][..], Vec::as_slice),
                        field(file, "partition"),
                    ) && pruner.may_match_file(
                        &lower,
                        &upper,
                        &prune::Counts {
                            values: count_map(file, "value_counts"),
                            nulls: nulls.clone(),
                        },
                    ))
                {
                    continue;
                }
                stats.files_kept += 1;
                for &(id, prim) in &tracked {
                    stats
                        .columns
                        .entry(id)
                        .or_insert_with(|| ColumnBounds::new(prim))
                        .add(id, &lower, &upper, &nulls);
                }
                // The planned file list grows with the table: reserve it too.
                // ponytail: released when planning returns; the list then
                // lives in the plan (O(files) paths) until the query ends.
                reservation.try_grow(path.len() + std::mem::size_of::<DataFile>())?;
                files.push(DataFile {
                    path: path.to_owned(),
                    size,
                    rows: long_field(file, "record_count").and_then(|r| u64::try_from(r).ok()),
                    seq,
                    selection: None,
                    spec: spec_id,
                    partition: field(file, "partition").cloned(),
                    eq: vec![],
                    first_row_id,
                });
            }
            reservation.shrink(held);
        }
        drop(fetched);

        // Phase 2: position deletes -> row selections for the kept files.
        if !deletes.is_empty() && !files.is_empty() {
            let targets: deletes::Targets<'_> =
                files.iter().map(|f| (f.path.as_str(), f.seq)).collect();
            let mut positions = deletes::positions(
                state,
                self.scoped.as_ref(),
                &deletes,
                &targets,
                &reservation,
                concurrency,
                &hits,
            )
            .await?;
            stats.delete_files = deletes.len();
            for file in &mut files {
                let Some(deleted) = positions.remove(&file.path) else {
                    continue;
                };
                let rows = file.rows.ok_or_else(|| {
                    invalid(
                        &file.path,
                        "record_count missing on a file with position deletes",
                    )
                })?;
                stats.deleted_rows += deleted.len();
                file.selection = Some(deletes::selection(&deleted, rows)?);
            }
            // Fully deleted files need not be opened at all.
            // Files whose every row is deleted are skipped, except for
            // compaction, which must retire them (ADR 0078).
            if !self.keep_empty {
                files.retain(|f| f.selection.as_ref().is_none_or(|s| s.row_count() > 0));
            }
        }

        // Phase 3: equality deletes that apply to at least one kept file.
        let mut pending = Vec::new();
        for (d, delete) in equality.iter().enumerate() {
            let unpartitioned = self.unpartitioned.contains(&delete.spec);
            let targets: Vec<usize> = (0..files.len())
                .filter(|&i| {
                    let f = &files[i];
                    delete.applies(f.seq, f.spec, f.partition.as_ref(), unpartitioned)
                })
                .collect();
            if targets.is_empty() {
                continue;
            }
            let key = delete
                .ids
                .iter()
                .map(|id| {
                    let name = self.names.get(id).ok_or_else(|| {
                        unsupported(
                            &delete.path,
                            format!("equality delete on dropped field {id}"),
                        )
                    })?;
                    Ok(self.schema.field_with_name(name)?.clone())
                })
                .collect::<Result<Vec<_>>>()?;
            pending.push((d, key, targets));
        }
        let mut loaded = futures::stream::iter(pending)
            .map(|(d, key, targets): (usize, Vec<Field>, Vec<usize>)| {
                let (reservation, hits, path) = (&reservation, &hits, &equality[d].path);
                let ids = &equality[d].ids;
                let mapping = self.name_mapping.as_deref();
                async move {
                    let batches = deletes::equality_keys(
                        state,
                        self.scoped.as_ref(),
                        path,
                        &key,
                        ids,
                        mapping,
                        reservation,
                        hits,
                    )
                    .await?;
                    Ok::<_, DataFusionError>((key, targets, batches))
                }
            })
            .buffered(concurrency);
        let mut sets = Vec::new();
        while let Some(set) = loaded.next().await {
            let (key, targets, batches) = set?;
            for i in targets {
                files[i].eq.push(sets.len());
            }
            sets.push(EqualitySet { key, batches });
        }
        drop(loaded);
        stats.equality_files = sets.len();
        stats.cache_hits = hits.into_inner();
        stats.manifest_cache_hits = manifest_hits.into_inner();
        Ok((files, sets, stats))
    }
}

#[async_trait]
impl TableProvider for IcebergTable {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    /// `INSERT INTO` (ADR 0075): an append, committed through the table's
    /// catalog. Refused before anything runs when the table cannot take
    /// one.
    async fn insert_into(
        &self,
        _state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        insert_op: datafusion::logical_expr::dml::InsertOp,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        use datafusion::logical_expr::dml::InsertOp;
        if insert_op != InsertOp::Append {
            return Err(DataFusionError::Plan(format!(
                "cannot insert into {}: only appends are supported (INSERT INTO, not OVERWRITE)",
                self.name
            )));
        }
        let sink = self.writable("insert into")?;
        Ok(Arc::new(datafusion::datasource::sink::DataSinkExec::new(
            input,
            Arc::new(sink),
            None,
        )))
    }

    /// `DELETE FROM t WHERE …` (ADR 0077): the positions of the matching
    /// rows are committed as position delete files. Refused before
    /// anything runs when the table cannot take a write.
    async fn delete_from(
        &self,
        state: &dyn Session,
        filters: Vec<Expr>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let (exec, _) = self.row_change("delete from", state, filters).await?;
        Ok(self.on_workers(state, exec))
    }

    /// `MERGE INTO t USING s ON … WHEN …` (ADR 0082): every target file
    /// is probed against the source, hashed on the `ON` equalities; the
    /// first `WHEN` clause that holds decides each row.
    async fn merge_into(
        &self,
        state: &dyn Session,
        source: Arc<dyn ExecutionPlan>,
        merge_schema: datafusion::common::DFSchemaRef,
        on: Expr,
        clauses: Vec<datafusion::logical_expr::dml::MergeIntoClause>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        use datafusion::logical_expr::dml::MergeIntoAction;
        use datafusion::logical_expr::utils::split_conjunction;
        use datafusion::logical_expr::{BinaryExpr, Operator};
        let refuse = |why: String| {
            Err(DataFusionError::Plan(format!(
                "cannot merge into {}: {why}",
                self.name
            )))
        };
        let (sink, targets, _, _) = self.rows_of("merge into", state, vec![]).await?;
        let width = self.schema.fields().len();
        let row = merge::Merge::row_schema(&merge_schema);
        // The merged row, every field nullable (one side is NULL for
        // unmatched rows), with the statement's qualifiers.
        let fields = merge_schema
            .iter()
            .map(|(q, f)| (q.cloned(), Arc::new(f.as_ref().clone().with_nullable(true))))
            .collect::<Vec<_>>();
        let merged = DFSchema::new_with_metadata(fields, HashMap::new())?;
        let compile = |e: Expr| state.create_physical_expr(e, &merged);
        // Which side an expression reads: target, source.
        let side = |e: &Expr| -> (bool, bool) {
            let mut sides = (false, false);
            for c in e.column_refs() {
                match merged.index_of_column(c) {
                    Ok(i) if i < width => sides.0 = true,
                    Ok(_) => sides.1 = true,
                    Err(_) => {}
                }
            }
            sides
        };
        let (mut target_keys, mut source_keys, mut rest) = (Vec::new(), Vec::new(), Vec::new());
        for conjunct in split_conjunction(&on) {
            if let Expr::BinaryExpr(BinaryExpr {
                left,
                op: Operator::Eq,
                right,
            }) = conjunct
            {
                match (side(left), side(right)) {
                    ((true, false), (false, true)) => {
                        target_keys.push(compile(*left.clone())?);
                        source_keys.push(compile(*right.clone())?);
                        continue;
                    }
                    ((false, true), (true, false)) => {
                        target_keys.push(compile(*right.clone())?);
                        source_keys.push(compile(*left.clone())?);
                        continue;
                    }
                    _ => {}
                }
            }
            rest.push(conjunct.clone());
        }
        if target_keys.is_empty() {
            return refuse("ON needs an equality between a target and a source expression".into());
        }
        let residual = conjunction(rest).map(compile).transpose()?;
        let target_fields = self.schema.fields();
        let (mut matched, mut not_matched, mut by_source) = (Vec::new(), Vec::new(), Vec::new());
        for clause in clauses {
            use datafusion::logical_expr::dml::MergeIntoClauseKind as Kind;
            let predicate = clause.predicate.map(compile).transpose()?;
            let kind = clause.kind.canonical();
            let action = match clause.action {
                MergeIntoAction::Delete => merge::Action::Delete,
                MergeIntoAction::Update(assignments) => {
                    let mut set: HashMap<String, Expr> = assignments.into_iter().collect();
                    let row = target_fields
                        .iter()
                        .enumerate()
                        .map(|(i, f)| match set.remove(f.name()) {
                            Some(e) => compile(e),
                            None => Ok(Arc::new(Column::new(f.name(), i)) as Arc<dyn PhysicalExpr>),
                        })
                        .collect::<Result<Vec<_>>>()?;
                    if let Some(name) = set.keys().next() {
                        return refuse(format!("no column {name}"));
                    }
                    merge::Action::Update(row)
                }
                MergeIntoAction::Insert { columns, values } => {
                    let columns: Vec<String> = if columns.is_empty() {
                        target_fields.iter().map(|f| f.name().clone()).collect()
                    } else {
                        columns
                    };
                    if columns.len() != values.len() {
                        return refuse("INSERT needs one value per column".into());
                    }
                    let mut set: HashMap<String, Expr> = columns.into_iter().zip(values).collect();
                    let row = target_fields
                        .iter()
                        .map(|f| match set.remove(f.name()) {
                            Some(e) => compile(e),
                            None => Ok(datafusion::physical_expr::expressions::lit(
                                datafusion::common::ScalarValue::try_from(f.data_type())?,
                            )),
                        })
                        .collect::<Result<Vec<_>>>()?;
                    if let Some(name) = set.keys().next() {
                        return refuse(format!("no column {name}"));
                    }
                    merge::Action::Insert(row)
                }
            };
            let fits = matches!(
                (&kind, &action),
                (
                    Kind::Matched | Kind::NotMatchedBySource,
                    merge::Action::Update(_) | merge::Action::Delete
                ) | (Kind::NotMatchedByTarget, merge::Action::Insert(_))
            );
            if !fits {
                return refuse(
                    "WHEN MATCHED takes UPDATE or DELETE; WHEN NOT MATCHED, INSERT".into(),
                );
            }
            let clause = merge::Clause { predicate, action };
            match kind {
                Kind::Matched => matched.push(clause),
                Kind::NotMatchedBySource => by_source.push(clause),
                _ => not_matched.push(clause),
            }
        }
        let merge = merge::Merge {
            target_keys,
            source_keys,
            residual,
            matched,
            not_matched,
            not_matched_by_source: by_source,
            row,
            width,
        };
        // Distributed (ADR 0094) when the files may go to workers and every
        // clause travels as SQL.
        let remote = self
            .split_targets(state, &sink, &targets)
            .zip(merge::spec(&merge, state.config_options()))
            .map(|(fragments, spec)| Arc::new(fragments.merging(spec)));
        Ok(Arc::new(
            merge::IcebergMergeExec::new(sink, targets, source, merge).with_remote(remote),
        ))
    }

    /// `UPDATE t SET … WHERE …` (ADR 0081): the rows a `DELETE` with the
    /// same WHERE would remove are deleted by position and written back
    /// with their new values, in one `overwrite` snapshot.
    async fn update(
        &self,
        state: &dyn Session,
        assignments: Vec<(String, Expr)>,
        filters: Vec<Expr>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let (exec, schema) = self.row_change("update", state, filters).await?;
        let mut set: HashMap<String, Expr> = HashMap::new();
        for (column, expr) in assignments {
            if self.schema.field_with_name(&column).is_err() {
                return Err(DataFusionError::Plan(format!(
                    "cannot update {}: no column {column}",
                    self.name
                )));
            }
            set.insert(column, unqualified(expr)?);
        }
        let row = self
            .schema
            .fields()
            .iter()
            .enumerate()
            .map(|(i, field)| match set.remove(field.name()) {
                Some(expr) => state.create_physical_expr(expr, &schema),
                None => Ok(Arc::new(Column::new(field.name(), i)) as Arc<dyn PhysicalExpr>),
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(self.on_workers(state, exec.with_update(row)))
    }

    /// Filters that identity partitions decide are `Exact` (ADR 0055): the
    /// scan enforces them per file, so no `FilterExec` is needed above it.
    /// Everything else is re-checked by DataFusion.
    fn supports_filters_pushdown(&self, filters: &[&Expr]) -> Result<Vec<Pushdown>> {
        let schema = DFSchema::try_from(self.schema.as_ref().clone())?;
        Ok(filters
            .iter()
            .map(|f| {
                // Exact by partition (ADR 0055) or by row (ADR 0062).
                if prune::Exact::new(f, &self.columns, &self.identity).is_some()
                    || portable::sql(f, &schema).is_some()
                {
                    Pushdown::Exact
                } else {
                    Pushdown::Inexact
                }
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
        let pruner = Pruner::new(filters, &self.columns);
        let planning = std::time::Instant::now();
        let (mut files, sets, mut stats) = self.data_files(state, &pruner).await?;
        let stats_columns = std::mem::take(&mut stats.columns);
        // A span in the query's trace (ADR 0056): where planning time went.
        plimsoll_core::plan_trace::record(
            "iceberg.plan",
            planning,
            vec![
                ("table", self.name.clone()),
                (
                    "snapshot",
                    self.snapshot_id
                        .map_or("none".to_owned(), |id| id.to_string()),
                ),
                (
                    "manifests",
                    format!("{}/{}", stats.manifests_read, stats.manifests),
                ),
                ("files", format!("{}/{}", stats.files_kept, stats.files)),
                ("delete_files", stats.delete_files.to_string()),
                ("equality_deletes", stats.equality_files.to_string()),
                ("manifest_cache_hits", stats.manifest_cache_hits.to_string()),
            ],
        );
        // Exact filters are this scan's to enforce: partition ones (ADR
        // 0055) decided per file from its partition tuple, row ones (ADR
        // 0062) applied to the rows read, here or on the workers. All but
        // partition ones also go to Parquet for pruning; inexact ones are
        // re-checked above the scan.
        let table_schema = DFSchema::try_from(self.schema.as_ref().clone())?;
        let (mut exact, mut inexact, mut rows) = (Vec::new(), Vec::new(), Vec::new());
        for f in filters {
            match prune::Exact::new(f, &self.columns, &self.identity) {
                Some(e) => exact.push(e),
                None => {
                    rows.extend(portable::sql(f, &table_schema));
                    inexact.push(f.clone());
                }
            }
        }
        let filters = inexact.as_slice();
        if let Some(only) = &self.only_files {
            files.retain(|f| only.contains(&f.path));
        }
        if !exact.is_empty() {
            let mut kept = Vec::with_capacity(files.len());
            for file in files {
                let spec = self.specs.get(&file.spec).map_or(&[][..], Vec::as_slice);
                let mut keep = true;
                for e in &exact {
                    match e.decide(spec, file.partition.as_ref()) {
                        Some(true) => {}
                        Some(false) => keep = false,
                        // Declared exact, so undecidable is an error, not a
                        // silently unfiltered file.
                        None => {
                            return Err(DataFusionError::from(ConnectorError::InvalidMetadata(
                                format!(
                                    "{}: partition tuple of {} cannot decide an identity filter",
                                    self.name, file.path
                                ),
                            )));
                        }
                    }
                }
                if keep {
                    kept.push(file);
                }
            }
            files = kept;
        }
        // Coordinator with workers (ADR 0058): the kept files are split
        // into fragments, one per worker, each re-planned (deletes
        // included) and scanned there. Tables read with vended credentials
        // stay local: those credentials are not sent over the wire.
        // Only healthy workers get fragments (ADR 0059); none healthy
        // means the scan runs here, as on a single node.
        let workers = state
            .config()
            .get_extension::<crate::remote::Workers>()
            .filter(|_| self.scoped.is_none() && self.only_files.is_none() && !files.is_empty())
            .map(|w| {
                let urls = w.healthy_urls();
                (w, urls)
            })
            .filter(|(_, urls)| !urls.is_empty());
        let input: Arc<dyn ExecutionPlan> = match workers {
            Some((workers, urls)) => {
                let schema = match projection {
                    Some(p) => Arc::new(self.schema.project(p)?),
                    None => self.schema.clone(),
                };
                let columns = schema.fields().iter().map(|f| f.name().clone()).collect();
                let paths: Vec<(String, Option<u64>)> =
                    files.iter().map(|f| (f.path.clone(), f.size)).collect();
                Arc::new(
                    crate::remote::RemoteScanExec::new(
                        schema,
                        workers,
                        &urls,
                        &self.metadata_path,
                        self.snapshot_id,
                        paths,
                        columns,
                    )
                    .with_filters(rows.iter().map(|(_, sql)| sql.clone()).collect()),
                )
            }
            None => {
                // Row filters need their columns read, and the limit
                // applied after them, not inside the Parquet scan.
                let requested = projection;
                let widened = projection.filter(|_| !rows.is_empty()).map(|p| {
                    let mut p = p.clone();
                    for (f, _) in &rows {
                        for c in f.column_refs() {
                            if let Ok(i) = self.schema.index_of(&c.name) {
                                if !p.contains(&i) {
                                    p.push(i);
                                }
                            }
                        }
                    }
                    p
                });
                let projection = widened.as_ref().or(projection);
                let limit = limit.filter(|_| rows.is_empty());
                // Files sharing the same equality delete sets scan together; a group
                // with sets is anti-joined against their keys (ADR 0016).
                let mut groups: BTreeMap<&[usize], Vec<&DataFile>> = BTreeMap::new();
                for file in &files {
                    groups.entry(&file.eq).or_default().push(file);
                }
                if groups.is_empty() {
                    groups.insert(&[], vec![]);
                }
                let mut plans = Vec::new();
                for (eq, files) in groups {
                    if eq.is_empty() {
                        plans.push(
                            self.parquet_scan(state, &files, projection, filters, limit)
                                .await?,
                        );
                        continue;
                    }
                    // Key columns are scanned even when not selected, then dropped.
                    let scan_projection = projection.map(|p| {
                        let mut p = p.clone();
                        for &i in eq {
                            for f in &sets[i].key {
                                if let Ok(k) = self.schema.index_of(f.name()) {
                                    if !p.contains(&k) {
                                        p.push(k);
                                    }
                                }
                            }
                        }
                        p
                    });
                    // No limit below the anti-join: it would cut rows before deletes.
                    let mut plan = self
                        .parquet_scan(state, &files, scan_projection.as_ref(), filters, None)
                        .await?;
                    for &i in eq {
                        plan = anti_join(&sets[i], plan)?;
                    }
                    if let Some(p) = projection {
                        let schema = plan.schema();
                        let exprs = (0..p.len())
                            .map(|i| {
                                let f = schema.field(i);
                                (
                                    Arc::new(Column::new(f.name(), i)) as Arc<dyn PhysicalExpr>,
                                    f.name().clone(),
                                )
                            })
                            .collect::<Vec<_>>();
                        plan = Arc::new(ProjectionExec::try_new(exprs, plan)?);
                    }
                    plans.push(plan);
                }
                let union = UnionExec::try_new(plans)?;
                match conjunction(rows.iter().map(|(f, _)| f.clone())) {
                    None => union,
                    Some(predicate) => {
                        let schema = DFSchema::try_from(union.schema().as_ref().clone())?;
                        let predicate = state.create_physical_expr(predicate, &schema)?;
                        // Back to the requested columns, dropping the
                        // filter-only ones appended above.
                        Arc::new(
                            FilterExecBuilder::new(predicate, union)
                                .apply_projection(requested.map(|p| (0..p.len()).collect()))?
                                .build()?,
                        )
                    }
                }
            }
        };
        // Exact row count from the manifests (ADR 0054), when it is exact:
        // no deletes apply to the files kept, every file states its
        // record_count, and nothing (filter pushed into Parquet, limit) can
        // drop rows inside the scan. Exact partition filters (ADR 0055) were
        // decided per file above, so they do not count.
        // DataFusion then answers `count(*)` without reading data files.
        let exact_rows =
            (filters.is_empty() && limit.is_none() && files.iter().all(|f| f.eq.is_empty()))
                .then(|| {
                    files
                        .iter()
                        .map(|f| f.rows.filter(|_| f.selection.is_none()))
                        .sum::<Option<u64>>()
                })
                .flatten()
                .and_then(|n| usize::try_from(n).ok());
        // Pinned to the snapshot and the schema: `ALTER TABLE` (ADR 0084)
        // changes what `SELECT *` returns without a new snapshot.
        let scan = AccountedExec::new(
            format!(
                "IcebergScan[{}] snapshot={} manifests={}/{} files={}/{} deletes={} deleted_rows={} equality_deletes={} delete_cache_hits={} manifest_cache_hits={}",
                self.name,
                self.snapshot_id
                    .map_or("none".to_owned(), |id| id.to_string()),
                stats.manifests_read,
                stats.manifests,
                stats.files_kept,
                stats.files,
                stats.delete_files,
                stats.deleted_rows,
                stats.equality_files,
                stats.cache_hits,
                stats.manifest_cache_hits,
            ),
            input,
        )
.with_pinned(format!(
            "{}@{}#{}",
            self.name,
            self.snapshot_id.map_or("none".to_owned(), |id| id.to_string()),
            self.schema_id
        ));
        // Otherwise the manifests' record counts are an upper bound (rows
        // may be deleted or filtered): inexact, but enough for DataFusion
        // to pick the smaller join side and collect small ones (ADR 0066).
        let rows = exact_rows.map(Precision::Exact).or_else(|| {
            files
                .iter()
                .map(|f| f.rows)
                .sum::<Option<u64>>()
                .and_then(|n| usize::try_from(n).ok())
                .map(Precision::Inexact)
        });
        // Per column, what the kept files' bounds and null counts say
        // (ADR 0113), over the whole table: the filters may read columns
        // the projection drops.
        let mut table = Statistics::new_unknown(&self.schema);
        for (field, c) in self
            .schema
            .fields()
            .iter()
            .zip(&mut table.column_statistics)
        {
            if let Some(b) = self
                .columns
                .get(field.name())
                .and_then(|(id, _)| stats_columns.get(id))
            {
                *c = b.statistics(field.data_type());
            }
        }
        // The filters this scan applies itself (exact pushdown, ADR 0055,
        // 0062) leave no FilterExec to estimate them: their selectivity is
        // estimated here, as FilterExec would from the same bounds.
        let rows = match (rows, filters.is_empty()) {
            (Some(rows), false) => Some(rows.with_estimated_selectivity(self.selectivity(
                state,
                filters,
                &table.column_statistics,
            ))),
            (rows, _) => rows,
        };
        let known = table
            .column_statistics
            .iter()
            .any(|c| c != &ColumnStatistics::new_unknown());
        Ok(Arc::new(match (rows, known) {
            (None, false) => scan,
            (rows, _) => {
                let mut statistics = match projection {
                    Some(p) => table.project(Some(p)),
                    None => table,
                };
                statistics.num_rows = rows.unwrap_or(Precision::Absent);
                scan.with_statistics(statistics)
            }
        }))
    }
}

/// `probe` rows whose key is not in `set`. Iceberg compares nulls as equal.
fn anti_join(set: &EqualitySet, probe: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
    let schema = Arc::new(Schema::new(
        set.key
            .iter()
            .map(|f| f.clone().with_nullable(true))
            .collect::<Vec<_>>(),
    ));
    let build = MemorySourceConfig::try_new_exec(std::slice::from_ref(&set.batches), schema, None)?;
    let probe_schema = probe.schema();
    let on = set
        .key
        .iter()
        .enumerate()
        .map(|(i, f)| {
            let l: PhysicalExprRef = Arc::new(Column::new(f.name(), i));
            let r: PhysicalExprRef = Arc::new(Column::new_with_schema(f.name(), &probe_schema)?);
            Ok((l, r))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Arc::new(
        HashJoinExecBuilder::new(build, probe, on, JoinType::RightAnti)
            .with_partition_mode(PartitionMode::CollectLeft)
            .with_null_equality(NullEquality::NullEqualsNull)
            .build()?,
    ))
}

impl IcebergTable {
    /// One Parquet scan over `files` (position deletes as row selections).
    async fn parquet_scan(
        &self,
        state: &dyn Session,
        files: &[&DataFile],
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let mut store: Option<ObjectStoreUrl> = None;
        let scoped = self.scoped.as_ref();
        let mut groups = vec![Vec::new(); state.config().target_partitions().max(1)];
        for (i, file) in files.iter().enumerate() {
            let url = ListingTableUrl::parse(&file.path)?;
            let file_store = store_url(&url, scoped);
            match &store {
                None => store = Some(file_store.clone()),
                Some(s) if *s != file_store => {
                    // ponytail: one object store per table; mixed locations
                    // (e.g. after a bucket migration) need one scan per store.
                    return Err(DataFusionError::NotImplemented(format!(
                        "iceberg table {} spans object stores {s:?} and {:?}",
                        self.name, file_store
                    )));
                }
                Some(_) => {}
            }
            let size = match file.size {
                Some(size) => size,
                None => {
                    state
                        .runtime_env()
                        .object_store(&file_store)?
                        .head(url.prefix())
                        .await
                        .map_err(|e| DataFusionError::External(Box::new(e)))?
                        .size
                }
            };
            let mut part = PartitionedFile::new(url.prefix().to_string(), size);
            if let Some(selection) = &file.selection {
                part = part.with_extension(ParquetRowSelection::new(selection.clone()));
            }
            let n = groups.len();
            groups[i % n].push(part);
        }

        let mut source = ParquetSource::new(self.schema.clone()).with_table_parquet_options(
            TableParquetOptions {
                global: state.config_options().execution.parquet.clone(),
                ..Default::default()
            },
        );
        if let Some(expr) = conjunction(filters.to_vec()) {
            let df_schema = DFSchema::try_from(self.schema.clone())?;
            source = source.with_predicate(state.create_physical_expr(expr, &df_schema)?);
        }
        let config = FileScanConfigBuilder::new(
            store.unwrap_or_else(ObjectStoreUrl::local_filesystem),
            Arc::new(source),
        )
        .with_file_groups(
            groups
                .into_iter()
                .filter(|g| !g.is_empty())
                .map(FileGroup::new)
                .collect(),
        )
        .with_projection_indices(projection.cloned())?
        .with_limit(limit)
        // Columns are matched by Iceberg field id, not name (ADR 0040).
        .with_expr_adapter(Some(Arc::new(field_ids::FieldIds::new(
            self.names.clone(),
            self.renamed,
            self.name_mapping.clone(),
            &self.name,
            self.nested_ids.clone(),
        ))))
        .build();
        Ok(DataSourceExec::from_data_source(config))
    }
}

impl IcebergTable {
    /// The rows of a `DELETE` or `UPDATE` (ADR 0077, 0081): the WHERE
    /// prunes the data files like a scan's, and each remaining file is
    /// planned whole, in file order (no row selection, no pushed
    /// predicate), so a running count is each row's position. Also the
    /// table's schema, for expressions over a row.
    async fn row_change(
        &self,
        verb: &str,
        state: &dyn Session,
        filters: Vec<Expr>,
    ) -> Result<(write::IcebergDeleteExec, DFSchema)> {
        let (sink, targets, predicate, schema) = self.rows_of(verb, state, filters).await?;
        Ok((
            write::IcebergDeleteExec::new(sink, targets, predicate),
            schema,
        ))
    }

    /// See [`Self::row_change`]: the sink, the files to read and the
    /// WHERE as a physical predicate.
    #[allow(clippy::type_complexity)]
    async fn rows_of(
        &self,
        verb: &str,
        state: &dyn Session,
        filters: Vec<Expr>,
    ) -> Result<(
        write::IcebergSink,
        Vec<write::Target>,
        Arc<dyn PhysicalExpr>,
        DFSchema,
    )> {
        let sink = self.writable(verb)?;
        self.rows_with(sink, state, filters).await
    }

    /// [`Self::rows_of`] for `sink`: a worker's, which does not commit
    /// (ADR 0093).
    #[allow(clippy::type_complexity)]
    async fn rows_with(
        &self,
        sink: write::IcebergSink,
        state: &dyn Session,
        filters: Vec<Expr>,
    ) -> Result<(
        write::IcebergSink,
        Vec<write::Target>,
        Arc<dyn PhysicalExpr>,
        DFSchema,
    )> {
        let filters = filters
            .into_iter()
            .map(unqualified)
            .collect::<Result<Vec<_>>>()?;
        let schema = DFSchema::try_from(self.schema.as_ref().clone())?;
        let predicate = match conjunction(filters.clone()) {
            Some(e) => state.create_physical_expr(e, &schema)?,
            None => datafusion::physical_expr::expressions::lit(true),
        };
        self.register_scoped(state);
        let pruner = Pruner::new(&filters, &self.columns);
        let (mut files, _, _) = self.data_files(state, &pruner).await?;
        // A worker's share only (ADR 0093).
        if let Some(only) = &self.only_files {
            files.retain(|f| only.contains(&f.path));
        }
        let mut targets = Vec::with_capacity(files.len());
        for file in files {
            let whole = DataFile {
                path: file.path.clone(),
                size: file.size,
                rows: file.rows,
                seq: file.seq,
                selection: None,
                spec: file.spec,
                partition: None,
                eq: Vec::new(),
                first_row_id: file.first_row_id,
            };
            let plan = self.parquet_scan(state, &[&whole], None, &[], None).await?;
            targets.push(write::Target {
                path: file.path,
                size: file.size,
                first_row_id: file.first_row_id,
                seq: file.seq,
                spec: file.spec,
                partition: prune::partition_values(file.partition.as_ref()),
                selection: file.selection,
                plan,
            });
        }
        Ok((sink, targets, predicate, schema))
    }

    /// `exec` run on the workers (ADR 0093): its target files split into
    /// fragments, each rewritten where it lands, the files written
    /// committed here. As it is when there are no healthy workers, the
    /// table is read with vended credentials (they never leave the
    /// coordinator), or the predicate or a new value does not travel as
    /// SQL.
    fn on_workers(
        &self,
        state: &dyn Session,
        exec: write::IcebergDeleteExec,
    ) -> Arc<dyn ExecutionPlan> {
        let local = |exec| Arc::new(exec) as Arc<dyn ExecutionPlan>;
        let Some(fragments) = self.split_targets(state, &exec.sink, &exec.targets) else {
            return local(exec);
        };
        let config = state.config_options();
        let sql = |e: &Arc<dyn PhysicalExpr>| portable::physical_sql(e, &self.schema, config);
        let Some(predicate) = sql(&exec.predicate) else {
            return local(exec);
        };
        let update = match &exec.update {
            None => None,
            Some(row) => match row.iter().map(sql).collect::<Option<Vec<_>>>() {
                Some(row) => Some(row),
                None => return local(exec),
            },
        };
        let fragments = fragments.changing(crate::remote::RowChange {
            predicate,
            update: update.clone(),
        });
        Arc::new(remote_write::RemoteWriteExec::new(
            (*exec.sink).clone(),
            Arc::new(fragments),
            remote_write::count_schema(),
            Some(update.is_some()),
        ))
    }

    /// `targets` split across the healthy workers by bytes (ADR 0093,
    /// 0094), as fragments to be given a task; `None` when the write must
    /// stay here: no healthy workers, a table read with vended credentials
    /// (they never leave the coordinator), or a snapshot whose schema is
    /// not the current one (workers read at the snapshot, while
    /// predicates and values are written against the current schema).
    fn split_targets(
        &self,
        state: &dyn Session,
        sink: &write::IcebergSink,
        targets: &[write::Target],
    ) -> Option<crate::remote::RemoteScanExec> {
        let workers = state.config().get_extension::<crate::remote::Workers>()?;
        let urls = workers.healthy_urls();
        let same_schema = self
            .snapshot_schema_id
            .is_none_or(|id| id == self.schema_id);
        if urls.is_empty() || targets.is_empty() || !sink.workers_may_write() || !same_schema {
            return None;
        }
        let files = targets.iter().map(|t| (t.path.clone(), t.size)).collect();
        Some(crate::remote::RemoteScanExec::new(
            crate::remote::written_schema(),
            workers,
            &urls,
            &self.metadata_path,
            self.snapshot_id,
            files,
            Vec::new(),
        ))
    }

    /// See [`IcebergConnector::compact`]. A partition is rewritten when it
    /// has a file carrying deletes (position, deletion vector or
    /// equality), or two small files; its other files stay.
    async fn compact(&self, state: &dyn Session) -> Result<Compaction> {
        let sink = self.writable("compact")?;
        let v3 = sink.layout.version() >= 3;
        self.register_scoped(state);
        let mut planner = self.clone();
        planner.keep_empty = true;
        let (files, _, _) = planner.data_files(state, &Pruner::default()).await?;
        let mut groups: BTreeMap<String, (bool, Vec<String>)> = BTreeMap::new();
        for file in files {
            // v3 (ADR 0096): rows keep their ids through a rewrite, which is
            // read by position; files with equality deletes are left alone.
            if v3 && !file.eq.is_empty() {
                continue;
            }
            // A file under an older partition spec is rewritten into the
            // current one, alone if need be (ADR 0085).
            let deletes = file.selection.is_some()
                || !file.eq.is_empty()
                || file.spec != sink.layout.spec_id();
            let small = file.size.is_none_or(|s| s < SMALL_FILE_BYTES);
            if !(deletes || small) {
                continue;
            }
            let key = format!(
                "{}:{:?}",
                file.spec,
                prune::partition_values(file.partition.as_ref())
            );
            let group = groups.entry(key).or_default();
            group.0 |= deletes;
            group.1.push(file.path);
        }
        let mut report = Compaction::default();
        let mut chosen = HashSet::new();
        for (deletes, paths) in groups.into_values() {
            if deletes || paths.len() >= 2 {
                report.partitions += 1;
                chosen.extend(paths);
            }
        }
        if chosen.is_empty() {
            return Ok(report);
        }
        report.files_rewritten = chosen.len();
        // The chosen files' live rows: the regular scan, deletes applied.
        let mut only = self.clone();
        only.only_files = Some(Arc::new(chosen.clone()));
        let task = state.task_ctx();
        let rows = if v3 {
            // With each row's lineage, as it was (ADR 0096).
            let (sink, targets, _, _) = only.rows_with(sink.clone(), state, Vec::new()).await?;
            sink.with_lineage(&targets, &task).await?
        } else {
            let plan = only.scan(state, None, &[], None).await?;
            datafusion::physical_plan::execute_stream(plan, task.clone())?
        };
        let (rows, written) = sink.replace(rows, &chosen, &task).await?;
        report.rows = rows;
        report.files_written = written;
        Ok(report)
    }

    /// The sink a write commits through, or why this table takes none
    /// (ADR 0075).
    fn writable(&self, verb: &str) -> Result<write::IcebergSink> {
        let Some(committer) = &self.committer else {
            return Err(DataFusionError::Plan(format!(
                "cannot {verb} {}: only tables served by a REST catalog accept writes",
                self.name
            )));
        };
        self.sink(verb, committer.clone())
    }

    /// A sink that writes files but never commits: a worker's, whose files
    /// the coordinator commits (ADR 0091).
    fn file_writer(&self) -> Result<write::IcebergSink> {
        #[derive(Debug)]
        struct NoCommit;
        #[async_trait::async_trait]
        impl write::Committer for NoCommit {
            async fn current(&self) -> std::result::Result<Json, String> {
                Err("workers do not commit".into())
            }
            async fn commit(
                &self,
                _: Vec<Json>,
                _: Vec<Json>,
            ) -> std::result::Result<(), write::CommitError> {
                Err(write::CommitError::Failed("workers do not commit".into()))
            }
        }
        self.sink("write files for", Arc::new(NoCommit))
    }

    fn sink(&self, verb: &str, committer: Arc<dyn write::Committer>) -> Result<write::IcebergSink> {
        let refuse = |why: &str| {
            Err(DataFusionError::Plan(format!(
                "cannot {verb} {}: {why}",
                self.name
            )))
        };
        if self.name.contains('@') {
            return refuse("a snapshot or branch reference is read-only");
        }
        let layout = match &self.layout {
            Ok(layout) => layout.clone(),
            Err(why) => return refuse(why),
        };
        let Some(location) = &self.location else {
            return refuse("the table has no location");
        };
        Ok(write::IcebergSink {
            name: self.name.clone(),
            schema: layout.arrow(),
            location: location.trim_end_matches('/').to_owned(),
            scoped: self.scoped.clone(),
            committer,
            schema_id: self.schema_id,
            layout,
            base: self.snapshot_id,
            staged: None,
            metadata_path: self.metadata_path.clone(),
        })
    }

    /// Makes the vended store resolvable by DataFusion's scan. Idempotent.
    fn register_scoped(&self, state: &dyn Session) {
        if let Some(s) = &self.scoped {
            state
                .runtime_env()
                .register_object_store(s.url.as_ref(), s.store.clone());
        }
    }
}

/// The registry key for `url`: the scoped store when it covers the bucket.
fn store_url(url: &ListingTableUrl, scoped: Option<&ScopedStore>) -> ObjectStoreUrl {
    match scoped {
        Some(s) if url.object_store() == s.bucket => s.url.clone(),
        _ => url.object_store(),
    }
}

/// Table dir -> newest `metadata/*.metadata.json`; a metadata file is used as is.
async fn resolve_metadata(location: &str, state: &dyn Session) -> Result<String, ConnectorError> {
    if location.ends_with(".metadata.json") {
        return Ok(location.to_owned());
    }
    let dir = format!("{}/metadata/", location.trim_end_matches('/'));
    let url = ListingTableUrl::parse(&dir)?;
    let store = state.runtime_env().object_store(url.object_store())?;
    let mut listing = store.list(Some(url.prefix()));
    let mut newest: Option<(u64, String)> = None;
    while let Some(meta) = futures::StreamExt::next(&mut listing).await {
        let path = meta
            .map_err(|e| DataFusionError::External(Box::new(e)))?
            .location
            .to_string();
        if let Some(version) = metadata_version(&path) {
            if newest.as_ref().is_none_or(|(v, _)| *v < version) {
                newest = Some((version, path));
            }
        }
    }
    let (_, newest) = newest.ok_or_else(|| invalid(&dir, "no *.metadata.json found"))?;
    // Rebuild a full URL from the store + object path.
    Ok(format!(
        "{}{}",
        url.object_store().as_str(),
        newest.trim_start_matches('/')
    ))
}

/// `.../v12.metadata.json` (hadoop) or `.../00012-<uuid>.metadata.json` -> 12.
fn metadata_version(path: &str) -> Option<u64> {
    let file = path.rsplit('/').next()?.strip_suffix(".metadata.json")?;
    let file = file.strip_prefix('v').unwrap_or(file);
    file.split('-').next()?.parse().ok()
}

async fn read(
    path: &str,
    state: &dyn Session,
    scoped: Option<&ScopedStore>,
) -> Result<bytes::Bytes, ConnectorError> {
    read_env(path, state.runtime_env(), scoped).await
}

/// [`read`] from execution (a `TaskContext`), where no `Session` exists.
async fn read_env(
    path: &str,
    env: &datafusion::execution::runtime_env::RuntimeEnv,
    scoped: Option<&ScopedStore>,
) -> Result<bytes::Bytes, ConnectorError> {
    let url = ListingTableUrl::parse(path)?;
    let store = env.object_store(store_url(&url, scoped))?;
    let external = |e: datafusion::object_store::Error| {
        ConnectorError::DataFusion(DataFusionError::External(Box::new(e)))
    };
    store
        .get(url.prefix())
        .await
        .map_err(external)?
        .bytes()
        .await
        .map_err(external)
}

/// Whether any column was renamed, or dropped and re-added under the same
/// name, across the table's schemas. Columns are resolved by field id
/// (ADR 0040), so this only matters for files that carry no ids.
fn renamed_columns(path: &str, schemas: &[&Json]) -> Result<bool, ConnectorError> {
    let mut by_id: HashMap<i64, &str> = HashMap::new();
    let mut by_name: HashMap<&str, i64> = HashMap::new();
    for field in schemas
        .iter()
        .filter_map(|s| s["fields"].as_array())
        .flatten()
    {
        let (Some(id), Some(name)) = (field["id"].as_i64(), field["name"].as_str()) else {
            return Err(invalid(path, "schema field without id/name"));
        };
        let conflict = by_id.insert(id, name).is_some_and(|n| n != name)
            || by_name.insert(name, id).is_some_and(|i| i != id);
        if conflict {
            return Ok(true);
        }
    }
    Ok(false)
}

fn arrow_schema(path: &str, schema: &Json) -> Result<Schema, ConnectorError> {
    arrow_schema_as(path, schema, false)
}

/// [`arrow_schema`] with every field, nested ones included, carrying its
/// Iceberg field id as Parquet field-id metadata: the schema files are
/// written with (ADR 0075, nested: ADR 0087).
pub(super) fn arrow_write_schema(path: &str, schema: &Json) -> Result<Schema, ConnectorError> {
    arrow_schema_as(path, schema, true)
}

fn arrow_schema_as(path: &str, schema: &Json, ids: bool) -> Result<Schema, ConnectorError> {
    let fields = schema["fields"]
        .as_array()
        .ok_or_else(|| invalid(path, "schema without fields"))?;
    fields
        .iter()
        .map(|f| arrow_field(path, f, ids))
        .collect::<Result<Vec<_>, _>>()
        .map(Schema::new)
}

/// `field` labelled with field id `id` when `ids`.
fn with_id(field: Field, id: &Json, ids: bool) -> Field {
    match (ids, id.as_i64()) {
        (true, Some(id)) => field.with_metadata(HashMap::from([(
            datafusion::parquet::arrow::PARQUET_FIELD_ID_META_KEY.to_owned(),
            id.to_string(),
        )])),
        _ => field,
    }
}

/// A struct field (`{"id", "name", "required", "type"}`) as an Arrow field.
fn arrow_field(path: &str, f: &Json, ids: bool) -> Result<Field, ConnectorError> {
    let name = f["name"].as_str().unwrap_or_default();
    let required = f["required"].as_bool().unwrap_or(false);
    let field = Field::new(name, iceberg_type(path, &f["type"], ids)?, !required);
    Ok(with_id(field, &f["id"], ids))
}

/// An Iceberg type: a primitive name, or a `struct` / `list` / `map` object
/// (ADR 0041). Child names follow the Parquet layout Iceberg writers use
/// (`element`, `key_value` / `key` / `value`); the scan compares nested
/// types ignoring names, so they only matter for display.
fn iceberg_type(path: &str, ty: &Json, ids: bool) -> Result<DataType, ConnectorError> {
    if let Some(primitive) = ty.as_str() {
        return arrow_type(path, primitive);
    }
    match ty["type"].as_str() {
        Some("struct") => {
            let fields = ty["fields"]
                .as_array()
                .ok_or_else(|| invalid(path, "struct without fields"))?;
            Ok(DataType::Struct(
                fields
                    .iter()
                    .map(|f| arrow_field(path, f, ids))
                    .collect::<Result<Vec<_>, _>>()?
                    .into(),
            ))
        }
        Some("list") => {
            let nullable = !ty["element-required"].as_bool().unwrap_or(false);
            let element = iceberg_type(path, &ty["element"], ids)?;
            Ok(DataType::List(Arc::new(with_id(
                Field::new("element", element, nullable),
                &ty["element-id"],
                ids,
            ))))
        }
        Some("map") => {
            let key = with_id(
                Field::new("key", iceberg_type(path, &ty["key"], ids)?, false),
                &ty["key-id"],
                ids,
            );
            let value = with_id(
                Field::new(
                    "value",
                    iceberg_type(path, &ty["value"], ids)?,
                    !ty["value-required"].as_bool().unwrap_or(false),
                ),
                &ty["value-id"],
                ids,
            );
            Ok(DataType::Map(
                Arc::new(Field::new(
                    "key_value",
                    DataType::Struct(vec![key, value].into()),
                    false,
                )),
                false,
            ))
        }
        _ => Err(unsupported(path, format!("type {ty}"))),
    }
}

/// Prunable primitives by path: top-level columns (`id`) and primitives
/// reached through structs only (`s.a`, ADR 0048). Elements of lists and
/// maps are skipped: their bounds cover any element, not one value.
fn prunable(fields: &Json, prefix: &str, out: &mut Vec<(String, (i64, Prim))>) {
    for f in fields.as_array().into_iter().flatten() {
        let (Some(name), Some(id)) = (f["name"].as_str(), f["id"].as_i64()) else {
            continue;
        };
        let path = format!("{prefix}{name}");
        match f["type"].as_str() {
            Some(ty) => {
                if let Some(prim) = Prim::from_iceberg(ty) {
                    out.push((path, (id, prim)));
                }
            }
            None if f["type"]["type"] == "struct" => {
                prunable(&f["type"]["fields"], &format!("{path}."), out);
            }
            None => {}
        }
    }
}

/// Every struct field nested under a top-level column, with the id of its
/// parent (struct field, list element or map key/value id).
fn nested_fields<'a>(ty: &'a Json, parent: i64, out: &mut Vec<(i64, &'a Json)>) {
    match ty["type"].as_str() {
        Some("struct") => {
            for f in ty["fields"].as_array().into_iter().flatten() {
                out.push((parent, f));
                nested_fields(&f["type"], f["id"].as_i64().unwrap_or(-1), out);
            }
        }
        Some("list") => nested_fields(&ty["element"], ty["element-id"].as_i64().unwrap_or(-1), out),
        Some("map") => {
            nested_fields(&ty["key"], ty["key-id"].as_i64().unwrap_or(-1), out);
            nested_fields(&ty["value"], ty["value-id"].as_i64().unwrap_or(-1), out);
        }
        _ => {}
    }
}

fn arrow_type(path: &str, ty: &str) -> Result<DataType, ConnectorError> {
    let us = TimeUnit::Microsecond;
    Ok(match ty {
        "boolean" => DataType::Boolean,
        "int" => DataType::Int32,
        "long" => DataType::Int64,
        "float" => DataType::Float32,
        "double" => DataType::Float64,
        "date" => DataType::Date32,
        "time" => DataType::Time64(us),
        "timestamp" => DataType::Timestamp(us, None),
        "timestamptz" => DataType::Timestamp(us, Some("UTC".into())),
        "timestamp_ns" => DataType::Timestamp(TimeUnit::Nanosecond, None),
        "timestamptz_ns" => DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
        "string" => DataType::Utf8,
        "uuid" => DataType::FixedSizeBinary(16),
        "binary" => DataType::Binary,
        t => {
            let parsed = if let Some(n) = t.strip_prefix("fixed[").and_then(|r| r.strip_suffix(']'))
            {
                n.parse().ok().map(DataType::FixedSizeBinary)
            } else if let Some(ps) = t.strip_prefix("decimal(").and_then(|r| r.strip_suffix(')')) {
                ps.split_once(',').and_then(|(p, s)| {
                    Some(DataType::Decimal128(
                        p.trim().parse().ok()?,
                        s.trim().parse().ok()?,
                    ))
                })
            } else {
                None
            };
            parsed.ok_or_else(|| unsupported(path, format!("type {t:?}")))?
        }
    })
}

/// Bytes `range` of the object at `path` (a deletion vector blob).
async fn read_range(
    path: &str,
    range: std::ops::Range<u64>,
    state: &dyn Session,
    scoped: Option<&ScopedStore>,
) -> Result<bytes::Bytes, ConnectorError> {
    let url = ListingTableUrl::parse(path)?;
    let store = state.runtime_env().object_store(store_url(&url, scoped))?;
    store
        .get_range(url.prefix(), range)
        .await
        .map_err(|e| ConnectorError::DataFusion(DataFusionError::External(Box::new(e))))
}

/// Avro records decoded lazily, one at a time.
fn avro_entries<'a>(
    path: &str,
    bytes: &'a [u8],
) -> Result<impl Iterator<Item = Result<Avro, ConnectorError>> + 'a, ConnectorError> {
    let owned = path.to_owned();
    let reader = apache_avro::Reader::new(bytes).map_err(|e| invalid(path, e))?;
    Ok(reader.map(move |e| e.map_err(|e| invalid(&owned, e))))
}

/// Iceberg `map<int, binary>` (Avro array of `{key, value}` records), e.g.
/// `lower_bounds`. Missing or malformed -> empty (no pruning).
fn bound_map(file: &Avro, name: &str) -> HashMap<i64, Vec<u8>> {
    let Some(Avro::Array(items)) = field(file, name) else {
        return HashMap::new();
    };
    items
        .iter()
        .filter_map(|kv| Some((long_field(kv, "key")?, bytes_field(kv, "value")?.to_vec())))
        .collect()
}

/// Iceberg `map<int, long>`, e.g. `null_value_counts`. Missing or
/// malformed -> empty (no pruning).
fn count_map(file: &Avro, name: &str) -> HashMap<i64, i64> {
    let Some(Avro::Array(items)) = field(file, name) else {
        return HashMap::new();
    };
    items
        .iter()
        .filter_map(|kv| Some((long_field(kv, "key")?, long_field(kv, "value")?)))
        .collect()
}

/// Manifest-list `partitions`: per partition field, (lower, upper) bound.
fn summaries(manifest: &Avro) -> Vec<Summary> {
    let Some(Avro::Array(items)) = field(manifest, "partitions") else {
        return vec![];
    };
    items
        .iter()
        .map(|s| {
            (
                bytes_field(s, "lower_bound").map(<[u8]>::to_vec),
                bytes_field(s, "upper_bound").map(<[u8]>::to_vec),
            )
        })
        .collect()
}

fn bytes_field<'a>(record: &'a Avro, name: &str) -> Option<&'a [u8]> {
    match field(record, name)? {
        Avro::Bytes(b) | Avro::Fixed(_, b) => Some(b),
        _ => None,
    }
}

/// Record field by name, looking through `["null", T]` unions.
fn field<'a>(record: &'a Avro, name: &str) -> Option<&'a Avro> {
    let Avro::Record(fields) = record else {
        return None;
    };
    match fields.iter().find(|(n, _)| n == name).map(|(_, v)| v)? {
        Avro::Union(_, inner) => Some(inner),
        v => Some(v),
    }
}

fn str_field<'a>(record: &'a Avro, name: &str) -> Option<&'a str> {
    match field(record, name)? {
        Avro::String(s) | Avro::Enum(_, s) => Some(s),
        _ => None,
    }
}

fn long_field(record: &Avro, name: &str) -> Option<i64> {
    match field(record, name)? {
        Avro::Int(i) => Some(i64::from(*i)),
        Avro::Long(l) => Some(*l),
        _ => None,
    }
}

fn invalid(path: &str, e: impl ToString) -> ConnectorError {
    ConnectorError::InvalidMetadata(format!("{path}: {}", e.to_string()))
}

fn unsupported(path: &str, what: String) -> ConnectorError {
    ConnectorError::Unsupported(format!("iceberg {what} ({path})"))
}

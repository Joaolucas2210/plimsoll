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
use datafusion::common::{DFSchema, Statistics};
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

mod deletes;
mod field_ids;
mod files;
pub mod meta;

mod prune;
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

    /// Accept `INSERT INTO`, committing through `committer` (ADR 0075).
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
        // Nested children resolve by name inside their parent (only
        // top-level columns resolve by id, ADR 0040): refuse histories
        // where that would read the wrong child (ADR 0041).
        let mut nested = Vec::new();
        for s in &schemas {
            for f in s["fields"].as_array().into_iter().flatten() {
                nested_fields(&f["type"], f["id"].as_i64().unwrap_or(-1), &mut nested);
            }
        }
        let mut by_id: HashMap<i64, (i64, &str)> = HashMap::new();
        let mut by_name: HashMap<(i64, &str), i64> = HashMap::new();
        for (parent, f) in &nested {
            let (Some(id), Some(name)) = (f["id"].as_i64(), f["name"].as_str()) else {
                return Err(err("nested field without id/name"));
            };
            let moved = by_id
                .insert(id, (*parent, name))
                .is_some_and(|p| p != (*parent, name));
            let reused = by_name.insert((*parent, name), id).is_some_and(|i| i != id);
            if moved || reused {
                return Err(unsupported(
                    path,
                    format!("nested field {name:?} was renamed, moved or re-added"),
                ));
            }
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

        let snapshot_id = meta["current-snapshot-id"].as_i64().filter(|id| *id != -1);
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
            manifest_list,
            location: meta["location"].as_str().map(str::to_owned),
            scoped: None,
            columns,
            specs,
            unpartitioned,
            names,
            renamed,
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
            manifests.push((manifest_path, is_delete, manifest_seq, spec_id));
        }
        reservation.shrink(list_held);

        // Phase 1b: data files and delete files that survive pruning, from
        // manifests fetched concurrently and decoded in list order.
        let mut files = Vec::new();
        let mut deletes = Vec::new();
        let mut equality = Vec::new();
        let mut fetched = futures::stream::iter(manifests)
            .map(|(path, is_delete, seq, spec)| {
                let load = &load;
                async move {
                    let (bytes, held) = load(&path).await?;
                    Ok::<_, DataFusionError>((path, is_delete, seq, spec, bytes, held))
                }
            })
            .buffered(concurrency);
        while let Some(manifest) = fetched.next().await {
            let (manifest_path, is_delete, manifest_seq, spec_id, manifest_bytes, held) = manifest?;
            for entry in avro_entries(&manifest_path, &manifest_bytes)? {
                let entry = entry?;
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
                if !pruner.is_empty()
                    && !(pruner.may_match_partition(
                        self.specs.get(&spec_id).map_or(&[][..], Vec::as_slice),
                        field(file, "partition"),
                    ) && pruner.may_match_file(
                        &bound_map(file, "lower_bounds"),
                        &bound_map(file, "upper_bounds"),
                        &prune::Counts {
                            values: count_map(file, "value_counts"),
                            nulls: count_map(file, "null_value_counts"),
                        },
                    ))
                {
                    continue;
                }
                stats.files_kept += 1;
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

    /// `DELETE FROM t WHERE …` (ADR 0077): the WHERE predicate prunes the
    /// data files like a scan's; each remaining file is read in file
    /// order, and the positions of matching rows are committed as position
    /// delete files. Refused before anything runs when the table cannot
    /// take a write.
    async fn delete_from(
        &self,
        state: &dyn Session,
        filters: Vec<Expr>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
        let sink = self.writable("delete from")?;
        // Against the table's own columns, unqualified.
        let filters = filters
            .into_iter()
            .map(|f| {
                f.transform(|e| match e {
                    Expr::Column(c) => Ok(Transformed::yes(Expr::Column(
                        datafusion::common::Column::new_unqualified(c.name),
                    ))),
                    e => Ok(Transformed::no(e)),
                })
                .data()
            })
            .collect::<Result<Vec<_>>>()?;
        let predicate = match conjunction(filters.clone()) {
            Some(e) => {
                let schema = DFSchema::try_from(self.schema.as_ref().clone())?;
                state.create_physical_expr(e, &schema)?
            }
            None => datafusion::physical_expr::expressions::lit(true),
        };
        self.register_scoped(state);
        let pruner = Pruner::new(&filters, &self.columns);
        let (files, _, _) = self.data_files(state, &pruner).await?;
        let mut targets = Vec::with_capacity(files.len());
        for file in files {
            if file.spec != sink.layout.spec_id() {
                return Err(DataFusionError::Plan(format!(
                    "cannot delete from {}: files written under an older partition spec are \
                     not supported yet",
                    self.name
                )));
            }
            // Every row, in order: no selection, no pushed predicate.
            let whole = DataFile {
                path: file.path.clone(),
                size: file.size,
                rows: file.rows,
                seq: file.seq,
                selection: None,
                spec: file.spec,
                partition: None,
                eq: Vec::new(),
            };
            let plan = self.parquet_scan(state, &[&whole], None, &[], None).await?;
            targets.push(write::Target {
                path: file.path,
                partition: prune::partition_values(file.partition.as_ref()),
                selection: file.selection,
                plan,
            });
        }
        Ok(Arc::new(write::IcebergDeleteExec::new(
            sink, targets, predicate,
        )))
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
        let (mut files, sets, stats) = self.data_files(state, &pruner).await?;
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
            "{}@{}",
            self.name,
            self.snapshot_id.map_or("none".to_owned(), |id| id.to_string())
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
        Ok(Arc::new(match rows {
            Some(rows) => {
                let schema = match projection {
                    Some(p) => Arc::new(self.schema.project(p)?),
                    None => self.schema.clone(),
                };
                let mut statistics = Statistics::new_unknown(&schema);
                statistics.num_rows = rows;
                scan.with_statistics(statistics)
            }
            None => scan,
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
        ))))
        .build();
        Ok(DataSourceExec::from_data_source(config))
    }
}

impl IcebergTable {
    /// See [`IcebergConnector::compact`]. A partition is rewritten when it
    /// has a file carrying deletes (position, deletion vector or
    /// equality), or two small files; its other files stay.
    async fn compact(&self, state: &dyn Session) -> Result<Compaction> {
        let sink = self.writable("compact")?;
        self.register_scoped(state);
        let mut planner = self.clone();
        planner.keep_empty = true;
        let (files, _, _) = planner.data_files(state, &Pruner::default()).await?;
        let mut groups: BTreeMap<String, (bool, Vec<String>)> = BTreeMap::new();
        for file in files {
            let deletes = file.selection.is_some() || !file.eq.is_empty();
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
        let plan = only.scan(state, None, &[], None).await?;
        let task = state.task_ctx();
        let rows = datafusion::physical_plan::execute_stream(plan, task.clone())?;
        let (rows, written) = sink.replace(rows, &chosen, &task).await?;
        report.rows = rows;
        report.files_written = written;
        Ok(report)
    }

    /// The sink a write commits through, or why this table takes none
    /// (ADR 0075).
    fn writable(&self, verb: &str) -> Result<write::IcebergSink> {
        let refuse = |why: &str| {
            Err(DataFusionError::Plan(format!(
                "cannot {verb} {}: {why}",
                self.name
            )))
        };
        let Some(committer) = &self.committer else {
            return refuse("only tables served by a REST catalog accept writes");
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
            schema: Arc::new(field_ids::with_field_ids(&self.schema, &self.names)),
            location: location.trim_end_matches('/').to_owned(),
            scoped: self.scoped.clone(),
            committer: committer.clone(),
            schema_id: self.schema_id,
            layout,
            base: self.snapshot_id,
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
    let fields = schema["fields"]
        .as_array()
        .ok_or_else(|| invalid(path, "schema without fields"))?;
    fields
        .iter()
        .map(|f| arrow_field(path, f))
        .collect::<Result<Vec<_>, _>>()
        .map(Schema::new)
}

/// A struct field (`{"id", "name", "required", "type"}`) as an Arrow field.
fn arrow_field(path: &str, f: &Json) -> Result<Field, ConnectorError> {
    let name = f["name"].as_str().unwrap_or_default();
    let required = f["required"].as_bool().unwrap_or(false);
    Ok(Field::new(name, iceberg_type(path, &f["type"])?, !required))
}

/// An Iceberg type: a primitive name, or a `struct` / `list` / `map` object
/// (ADR 0041). Child names follow the Parquet layout Iceberg writers use
/// (`element`, `key_value` / `key` / `value`); the scan compares nested
/// types ignoring names, so they only matter for display.
fn iceberg_type(path: &str, ty: &Json) -> Result<DataType, ConnectorError> {
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
                    .map(|f| arrow_field(path, f))
                    .collect::<Result<Vec<_>, _>>()?
                    .into(),
            ))
        }
        Some("list") => {
            let nullable = !ty["element-required"].as_bool().unwrap_or(false);
            let element = iceberg_type(path, &ty["element"])?;
            Ok(DataType::List(Arc::new(Field::new(
                "element", element, nullable,
            ))))
        }
        Some("map") => {
            let key = Field::new("key", iceberg_type(path, &ty["key"])?, false);
            let value = Field::new(
                "value",
                iceberg_type(path, &ty["value"])?,
                !ty["value-required"].as_bool().unwrap_or(false),
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

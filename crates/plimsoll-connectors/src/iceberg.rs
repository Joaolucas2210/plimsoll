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
use datafusion::common::DFSchema;
use datafusion::common::config::TableParquetOptions;
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
use datafusion::physical_plan::joins::{HashJoinExecBuilder, PartitionMode};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::union::UnionExec;
use futures::StreamExt;
use plimsoll_core::federation::{Pushdown, PushdownPolicy};
use plimsoll_core::memory::AccountedExec;
use plimsoll_core::storage::{ScopedStore, scoped_s3_store};
use serde_json::Value as Json;

use crate::{Connector, ConnectorError};

mod deletes;
mod field_ids;
mod files;
pub mod meta;
mod prune;
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
            // Parquet stats pruning is a hint: every filter is re-checked.
            policy: PushdownPolicy::uniform(Pushdown::Inexact),
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
        let columns = current["fields"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|f| {
                let prim = Prim::from_iceberg(f["type"].as_str()?)?;
                Some((f["name"].as_str()?.to_owned(), (f["id"].as_i64()?, prim)))
            })
            .collect();
        let specs = meta["partition-specs"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|s| Some((s["spec-id"].as_i64()?, prune::partition_spec(&s["fields"]))))
            .collect();
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
        // Tables with renamed/re-added columns carry field ids in their schema
        // so every file is adapted by id (ADR 0040).
        let schema = Arc::new(if renamed {
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
                    && !pruner.may_match_file(
                        &bound_map(file, "lower_bounds"),
                        &bound_map(file, "upper_bounds"),
                    )
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
            files.retain(|f| f.selection.as_ref().is_none_or(|s| s.row_count() > 0));
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
                async move {
                    let batches = deletes::equality_keys(
                        state,
                        self.scoped.as_ref(),
                        path,
                        &key,
                        ids,
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

    fn supports_filters_pushdown(&self, filters: &[&Expr]) -> Result<Vec<Pushdown>> {
        Ok(vec![Pushdown::Inexact; filters.len()])
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let pruner = Pruner::new(filters, &self.columns);
        let (files, sets, stats) = self.data_files(state, &pruner).await?;
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
        Ok(Arc::new(AccountedExec::new(
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
            UnionExec::try_new(plans)?,
        )
        // A snapshot never changes: same table + snapshot, same rows. The
        // result cache keys on this (ADR 0038).
        .with_pinned(format!(
            "{}@{}",
            self.name,
            self.snapshot_id.map_or("none".to_owned(), |id| id.to_string())
        ))))
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
            &self.name,
        ))))
        .build();
        Ok(DataSourceExec::from_data_source(config))
    }
}

impl IcebergTable {
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

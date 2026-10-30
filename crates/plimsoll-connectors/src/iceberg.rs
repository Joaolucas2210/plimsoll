//! Read-only Apache Iceberg connector (format v1/v2, Parquet data files).
//!
//! Plimsoll walks the metadata itself (`metadata.json` -> manifest list ->
//! manifests) and hands the live data files to DataFusion's own Parquet scan,
//! wrapped in [`AccountedExec`]. Manifest decoding is reserved in the
//! session's pool, so the whole read path runs under the `CeilingPool`.
//! See ADR 0007.
//!
//! Correctness first: anything we can't read exactly (delete files, renamed
//! columns, non-Parquet files, nested types) is a typed error, never a
//! silently wrong answer.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use apache_avro::types::Value as Avro;
use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::DFSchema;
use datafusion::common::config::TableParquetOptions;
use datafusion::datasource::listing::{ListingTableUrl, PartitionedFile};
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
use datafusion::physical_plan::ExecutionPlan;
use plimsoll_core::federation::{Pushdown, PushdownPolicy};
use plimsoll_core::memory::AccountedExec;
use plimsoll_core::storage::{ScopedStore, scoped_s3_store};
use serde_json::Value as Json;

use crate::{Connector, ConnectorError};

mod deletes;
mod prune;
use deletes::DeleteFile;
use prune::{PartitionField, Prim, Pruner, Summary};

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
}

impl IcebergTable {
    fn from_metadata(name: &str, path: &str, meta: &Json) -> Result<Self, ConnectorError> {
        let err = |m: &str| invalid(path, m);
        let version = meta["format-version"].as_i64().unwrap_or(1);
        if !(1..=2).contains(&version) {
            return Err(unsupported(path, format!("format-version {version}")));
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
        check_name_mapping(path, &schemas)?;
        let schema = Arc::new(arrow_schema(path, current)?);

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
        Ok(Self {
            name: name.to_owned(),
            schema,
            snapshot_id,
            manifest_list,
            location: meta["location"].as_str().map(str::to_owned),
            scoped: None,
            columns,
            specs,
        })
    }

    /// Live data files of the pinned snapshot that may match `pruner`, with
    /// position deletes applied as row selections (ADR 0014). Manifest bytes,
    /// the planned file list and delete positions are reserved in the session
    /// pool. Pruned manifests are never read.
    async fn data_files(
        &self,
        state: &dyn Session,
        pruner: &Pruner,
    ) -> Result<(Vec<DataFile>, PlanStats)> {
        let mut stats = PlanStats::default();
        let Some(list_path) = &self.manifest_list else {
            return Ok((vec![], stats));
        };
        self.register_scoped(state);
        let reservation = MemoryConsumer::new(format!("IcebergPlanning[{}]", self.name))
            .register(&state.runtime_env().memory_pool);
        // Raw Avro bytes are reserved while in use; entries are decoded one at
        // a time (never collected), so decoded state stays O(one entry).
        let load = async |path: &str| -> Result<bytes::Bytes> {
            let bytes = read(path, state, self.scoped.as_ref()).await?;
            reservation.try_grow(bytes.len())?;
            Ok(bytes)
        };

        // Phase 1: data files and delete files that survive pruning.
        let mut files = Vec::new();
        let mut deletes = Vec::new();
        let list_bytes = load(list_path).await?;
        for manifest in avro_entries(list_path, &list_bytes)? {
            let manifest = manifest?;
            let manifest_path = str_field(&manifest, "manifest_path")
                .ok_or_else(|| invalid(list_path, "manifest_path missing"))?
                .to_owned();
            // v1 has no `content`; 0 = data, 1 = deletes.
            let is_delete = long_field(&manifest, "content").unwrap_or(0) == 1;
            let manifest_seq = long_field(&manifest, "sequence_number").unwrap_or(0);
            stats.manifests += 1;
            // Delete manifests prune like data manifests: a position delete
            // shares its data file's partition, so if no data file there can
            // match, neither can its deletes.
            if !pruner.is_empty() {
                let spec =
                    long_field(&manifest, "partition_spec_id").and_then(|id| self.specs.get(&id));
                if let Some(spec) = spec {
                    if !pruner.may_match_manifest(spec, &summaries(&manifest)) {
                        continue;
                    }
                }
            }
            stats.manifests_read += 1;
            let manifest_bytes = load(&manifest_path).await?;
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
                if !format.eq_ignore_ascii_case("parquet") {
                    return Err(
                        unsupported(&manifest_path, format!("{format:?} data files")).into(),
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
                        reservation.try_grow(path.len() + std::mem::size_of::<DeleteFile>())?;
                        deletes.push(DeleteFile {
                            path: path.to_owned(),
                            seq,
                            referenced: str_field(file, "referenced_data_file").map(str::to_owned),
                        });
                        continue;
                    }
                    (true, 2) => {
                        return Err(unsupported(
                            &manifest_path,
                            "equality delete files".to_owned(),
                        )
                        .into());
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
                });
            }
            reservation.shrink(manifest_bytes.len());
        }
        reservation.shrink(list_bytes.len());

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
        Ok((files, stats))
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
        let (files, stats) = self.data_files(state, &pruner).await?;
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
        .build();
        Ok(Arc::new(AccountedExec::new(
            format!(
                "IcebergScan[{}] manifests={}/{} files={}/{} deletes={} deleted_rows={}",
                self.name,
                stats.manifests_read,
                stats.manifests,
                stats.files_kept,
                stats.files,
                stats.delete_files,
                stats.deleted_rows
            ),
            DataSourceExec::from_data_source(config),
        )))
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
    let url = ListingTableUrl::parse(path)?;
    let store = state.runtime_env().object_store(store_url(&url, scoped))?;
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

/// DataFusion's Parquet scan maps file columns to the table by *name*, while
/// Iceberg tracks them by field id. Reject histories where those disagree
/// (a rename, or a dropped column re-added under the same name).
fn check_name_mapping(path: &str, schemas: &[&Json]) -> Result<(), ConnectorError> {
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
            return Err(unsupported(
                path,
                format!("column {name:?} was renamed or re-added (field-id mapping)"),
            ));
        }
    }
    Ok(())
}

fn arrow_schema(path: &str, schema: &Json) -> Result<Schema, ConnectorError> {
    let fields = schema["fields"]
        .as_array()
        .ok_or_else(|| invalid(path, "schema without fields"))?;
    fields
        .iter()
        .map(|f| {
            let name = f["name"].as_str().unwrap_or_default();
            let ty = f["type"]
                .as_str()
                .ok_or_else(|| unsupported(path, format!("nested type for column {name:?}")))?;
            let required = f["required"].as_bool().unwrap_or(false);
            Ok(Field::new(name, arrow_type(path, ty)?, !required))
        })
        .collect::<Result<Vec<_>, _>>()
        .map(Schema::new)
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

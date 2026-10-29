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

use std::collections::HashMap;
use std::sync::Arc;

use apache_avro::types::Value as Avro;
use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::DFSchema;
use datafusion::common::config::TableParquetOptions;
use datafusion::datasource::listing::{ListingTableUrl, PartitionedFile};
use datafusion::datasource::physical_plan::{FileGroup, FileScanConfigBuilder, ParquetSource};
use datafusion::datasource::source::DataSourceExec;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::memory_pool::MemoryConsumer;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::logical_expr::utils::conjunction;
use datafusion::logical_expr::{Expr, TableType};
use datafusion::object_store::ObjectStoreExt;
use datafusion::physical_plan::ExecutionPlan;
use plimsoll_core::federation::{Pushdown, PushdownPolicy};
use plimsoll_core::memory::AccountedExec;
use serde_json::Value as Json;

use crate::{Connector, ConnectorError};

#[derive(Debug)]
pub struct IcebergConnector {
    name: String,
    table: Arc<IcebergTable>,
    policy: PushdownPolicy,
}

impl IcebergConnector {
    /// `location` is a `*.metadata.json` file (what a REST catalog's
    /// `metadata-location` points at) or a table directory, in which case the
    /// newest `metadata/*.metadata.json` is used.
    ///
    /// ponytail: the snapshot is pinned at registration; re-register to see
    /// new commits. Upgrade path: re-resolve via the REST catalog per query.
    pub async fn try_new(
        name: impl Into<String>,
        location: &str,
        state: &dyn Session,
    ) -> Result<Self, ConnectorError> {
        let name = name.into();
        let metadata_path = resolve_metadata(location, state).await?;
        let raw = read(&metadata_path, state).await?;
        let meta: Json = serde_json::from_slice(&raw).map_err(|e| invalid(&metadata_path, e))?;
        let table = IcebergTable::from_metadata(&name, &metadata_path, &meta)?;
        Ok(Self {
            name,
            table: Arc::new(table),
            // Parquet stats pruning is a hint: every filter is re-checked.
            policy: PushdownPolicy::uniform(Pushdown::Inexact),
        })
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

#[derive(Debug)]
struct IcebergTable {
    name: String,
    schema: SchemaRef,
    snapshot_id: Option<i64>,
    /// `None` for a table without snapshots (empty).
    manifest_list: Option<String>,
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
        Ok(Self {
            name: name.to_owned(),
            schema,
            snapshot_id,
            manifest_list,
        })
    }

    /// Live data files of the pinned snapshot, with manifest bytes reserved
    /// in the session pool while they are decoded.
    async fn data_files(&self, state: &dyn Session) -> Result<Vec<String>> {
        let Some(list_path) = &self.manifest_list else {
            return Ok(vec![]);
        };
        let reservation = MemoryConsumer::new(format!("IcebergManifests[{}]", self.name))
            .register(&state.runtime_env().memory_pool);
        let decode = async |path: &str| -> Result<Vec<Avro>> {
            let bytes = read(path, state).await?;
            reservation.try_grow(bytes.len())?;
            let rows = apache_avro::Reader::new(&bytes[..])
                .and_then(|r| r.collect::<Result<Vec<_>, _>>())
                .map_err(|e| invalid(path, e))?;
            reservation.shrink(bytes.len());
            Ok(rows)
        };

        let mut files = Vec::new();
        for manifest in decode(list_path).await? {
            let manifest_path = str_field(&manifest, "manifest_path")
                .ok_or_else(|| invalid(list_path, "manifest_path missing"))?
                .to_owned();
            // v1 has no `content`; 0 = data, 1 = deletes.
            let is_delete = long_field(&manifest, "content").unwrap_or(0) == 1;
            for entry in decode(&manifest_path).await? {
                // status 2 = deleted in this snapshot.
                if long_field(&entry, "status") == Some(2) {
                    continue;
                }
                let file = field(&entry, "data_file")
                    .ok_or_else(|| invalid(&manifest_path, "data_file missing"))?;
                if is_delete || long_field(file, "content").unwrap_or(0) != 0 {
                    return Err(unsupported(
                        &manifest_path,
                        "row-level delete files (merge-on-read)".to_owned(),
                    )
                    .into());
                }
                let format = str_field(file, "file_format").unwrap_or("");
                if !format.eq_ignore_ascii_case("parquet") {
                    return Err(
                        unsupported(&manifest_path, format!("{format:?} data files")).into(),
                    );
                }
                let path = str_field(file, "file_path")
                    .ok_or_else(|| invalid(&manifest_path, "file_path missing"))?;
                files.push(path.to_owned());
            }
        }
        Ok(files)
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
        let files = self.data_files(state).await?;
        let mut store: Option<ObjectStoreUrl> = None;
        let mut groups = vec![Vec::new(); state.config().target_partitions().max(1)];
        for (i, path) in files.iter().enumerate() {
            let url = ListingTableUrl::parse(path)?;
            match &store {
                None => store = Some(url.object_store()),
                Some(s) if *s != url.object_store() => {
                    // ponytail: one object store per table; mixed locations
                    // (e.g. after a bucket migration) need one scan per store.
                    return Err(DataFusionError::NotImplemented(format!(
                        "iceberg table {} spans object stores {s:?} and {:?}",
                        self.name,
                        url.object_store()
                    )));
                }
                Some(_) => {}
            }
            let size = state
                .runtime_env()
                .object_store(url.object_store())?
                .head(url.prefix())
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?
                .size;
            let n = groups.len();
            groups[i % n].push(PartitionedFile::new(url.prefix().to_string(), size));
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
            format!("IcebergScan[{}]", self.name),
            DataSourceExec::from_data_source(config),
        )))
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

async fn read(path: &str, state: &dyn Session) -> Result<bytes::Bytes, ConnectorError> {
    let url = ListingTableUrl::parse(path)?;
    let store = state.runtime_env().object_store(url.object_store())?;
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
    ConnectorError::Iceberg(format!("invalid metadata at {path}: {}", e.to_string()))
}

fn unsupported(path: &str, what: String) -> ConnectorError {
    ConnectorError::Iceberg(format!("unsupported at {path}: {what}"))
}

//! Appends to Iceberg tables (ADR 0075).
//!
//! `INSERT INTO t SELECT …` reaches [`IcebergSink`] through DataFusion's
//! `insert_into`. The sink writes the rows as Parquet data files under the
//! table's `data/`, then one manifest listing them and a manifest list
//! carrying the parent snapshot's manifests plus the new one, and commits
//! a snapshot through the table's [`Committer`] (the REST catalog's
//! `updateTable`). A commit that loses a race (another writer committed
//! first) is rebuilt on the new parent and retried; the data files and
//! the manifest are reused.
//!
//! Scope of this first step: format v2, unpartitioned, primitive columns.
//! Anything else is refused at planning, before a byte is written.

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use apache_avro::Schema as AvroSchema;
use apache_avro::types::Value as Avro;
use async_trait::async_trait;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::datasource::sink::DataSink;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::object_store::{ObjectStoreExt, PutPayload};
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType};
use futures::StreamExt;
use plimsoll_core::storage::ScopedStore;
use serde_json::{Value as Json, json};

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

/// Why `meta` cannot take an append, if it cannot (checked at planning).
pub(super) fn unwritable(meta: &Json) -> Option<String> {
    if meta["format-version"].as_i64() != Some(2) {
        return Some(format!(
            "INSERT supports Iceberg format v2 tables (this one is v{})",
            meta["format-version"]
        ));
    }
    let spec = meta["default-spec-id"].as_i64().unwrap_or(0);
    let partitioned = meta["partition-specs"]
        .as_array()
        .into_iter()
        .flatten()
        .find(|s| s["spec-id"].as_i64() == Some(spec))
        .and_then(|s| s["fields"].as_array())
        .is_some_and(|f| !f.is_empty());
    if partitioned {
        return Some("INSERT into partitioned tables is not supported yet".into());
    }
    let schema = current_schema(meta)?;
    let nested = schema["fields"]
        .as_array()
        .into_iter()
        .flatten()
        .any(|f| !f["type"].is_string());
    if nested {
        return Some("INSERT into tables with nested columns is not supported yet".into());
    }
    None
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

/// A data file written and not yet committed.
struct Written {
    path: String,
    rows: u64,
    bytes: u64,
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

#[async_trait]
impl DataSink for IcebergSink {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    async fn write_all(
        &self,
        mut data: SendableRecordBatchStream,
        context: &Arc<TaskContext>,
    ) -> Result<u64> {
        let env = context.runtime_env();
        let run = unique();
        let mut written = Vec::new();
        // The file being written, and its rows so far.
        let mut writer: Option<(ArrowWriter<Vec<u8>>, u64)> = None;
        while let Some(batch) = data.next().await {
            // Same columns, relabelled with the table's field ids; a NULL
            // in a required column fails here.
            let batch = RecordBatch::try_new(self.schema.clone(), batch?.columns().to_vec())?;
            if batch.num_rows() == 0 {
                continue;
            }
            let (w, rows) = match &mut writer {
                Some(w) => w,
                None => writer.insert((
                    ArrowWriter::try_new(Vec::new(), self.schema.clone(), None)?,
                    0,
                )),
            };
            w.write(&batch)?;
            *rows += batch.num_rows() as u64;
            if w.in_progress_size() + w.bytes_written() >= TARGET_FILE_BYTES {
                if let Some((w, rows)) = writer.take() {
                    written.push(self.put(w, rows, run, written.len(), &env).await?);
                }
            }
        }
        if let Some((w, rows)) = writer.take() {
            written.push(self.put(w, rows, run, written.len(), &env).await?);
        }
        let rows = written.iter().map(|w| w.rows).sum();
        if written.is_empty() {
            return Ok(0);
        }
        self.commit(&written, &env).await?;
        Ok(rows)
    }
}

impl IcebergSink {
    /// Finishes `writer` as data file `n` of this insert and uploads it.
    async fn put(
        &self,
        writer: ArrowWriter<Vec<u8>>,
        rows: u64,
        run: u64,
        n: usize,
        env: &datafusion::execution::runtime_env::RuntimeEnv,
    ) -> Result<Written> {
        let bytes = writer.into_inner()?;
        let path = format!("{}/data/{run:016x}-{n:05}.parquet", self.location);
        let size = bytes.len() as u64;
        self.put_bytes(&path, bytes, env).await?;
        Ok(Written {
            path,
            rows,
            bytes: size,
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
        env: &datafusion::execution::runtime_env::RuntimeEnv,
    ) -> Result<()> {
        let failed = |m: String| DataFusionError::Execution(format!("{}: {m}", self.name));
        // Positive, as Java writers make them.
        let snapshot_id = (unique() >> 1) as i64;
        let meta = self.committer.current().await.map_err(failed)?;
        let spec_id = meta["default-spec-id"].as_i64().unwrap_or(0);
        let manifest_path = format!("{}/metadata/{snapshot_id:x}-m0.avro", self.location);
        let manifest = manifest(&meta, files, snapshot_id, spec_id, self.schema_id)
            .map_err(|e| failed(format!("manifest: {e}")))?;
        let manifest_length = manifest.len() as i64;
        self.put_bytes(&manifest_path, manifest, env).await?;
        let added_rows: u64 = files.iter().map(|f| f.rows).sum();
        let mut meta = meta;
        for attempt in 0..COMMIT_ATTEMPTS {
            if attempt > 0 {
                meta = self.committer.current().await.map_err(failed)?;
            }
            if let Some(why) = unwritable(&meta) {
                return Err(failed(why));
            }
            if meta["current-schema-id"].as_i64() != Some(self.schema_id) {
                return Err(failed(
                    "the table's schema changed during the insert; nothing was committed".into(),
                ));
            }
            let parent = meta["refs"]["main"]["snapshot-id"]
                .as_i64()
                .or_else(|| meta["current-snapshot-id"].as_i64().filter(|id| *id >= 0));
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
            let new_entry = ManifestFile {
                path: manifest_path.clone(),
                length: manifest_length,
                spec_id,
                sequence,
                snapshot_id,
                files: files.len() as i64,
                rows: added_rows as i64,
            };
            let list = manifest_list(
                &new_entry,
                parent_list.as_deref(),
                snapshot_id,
                parent,
                sequence,
            )
            .map_err(|e| failed(format!("manifest list: {e}")))?;
            let list_path = format!(
                "{}/metadata/snap-{snapshot_id}-{attempt}-{:x}.avro",
                self.location,
                unique()
            );
            self.put_bytes(&list_path, list, env).await?;
            let summary = json!({
                "operation": "append",
                "added-data-files": files.len().to_string(),
                "added-records": added_rows.to_string(),
                "added-files-size": files.iter().map(|f| f.bytes).sum::<u64>().to_string(),
                "engine-name": "plimsoll",
            });
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

/// The v2 `manifest_entry` schema for an unpartitioned table, with the
/// field ids Iceberg readers resolve by.
const MANIFEST_ENTRY: &str = r#"{"type":"record","name":"manifest_entry","fields":[
 {"name":"status","type":"int","field-id":0},
 {"name":"snapshot_id","type":["null","long"],"default":null,"field-id":1},
 {"name":"sequence_number","type":["null","long"],"default":null,"field-id":3},
 {"name":"file_sequence_number","type":["null","long"],"default":null,"field-id":4},
 {"name":"data_file","type":{"type":"record","name":"r2","fields":[
   {"name":"content","type":"int","field-id":134},
   {"name":"file_path","type":"string","field-id":100},
   {"name":"file_format","type":"string","field-id":101},
   {"name":"partition","type":{"type":"record","name":"r102","fields":[]},"field-id":102},
   {"name":"record_count","type":"long","field-id":103},
   {"name":"file_size_in_bytes","type":"long","field-id":104}
 ]},"field-id":2}
]}"#;

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

/// Deflate, as Java and Python Iceberg writers use: every reader has it.
fn codec() -> apache_avro::Codec {
    apache_avro::Codec::Deflate(apache_avro::DeflateSettings::default())
}

fn avro_schema(text: &str) -> std::result::Result<AvroSchema, String> {
    AvroSchema::parse_str(text).map_err(|e| e.to_string())
}

fn opt(v: Option<Avro>) -> Avro {
    match v {
        None => Avro::Union(0, Box::new(Avro::Null)),
        Some(v) => Avro::Union(1, Box::new(v)),
    }
}

/// One manifest listing `files` as ADDED by `snapshot_id`; data sequence
/// numbers are inherited from the manifest list entry.
fn manifest(
    meta: &Json,
    files: &[Written],
    snapshot_id: i64,
    spec_id: i64,
    schema_id: i64,
) -> std::result::Result<Vec<u8>, String> {
    let schema = avro_schema(MANIFEST_ENTRY)?;
    let mut writer =
        apache_avro::Writer::with_codec(&schema, Vec::new(), codec()).map_err(|e| e.to_string())?;
    let table_schema = meta["schemas"]
        .as_array()
        .into_iter()
        .flatten()
        .find(|s| s["schema-id"].as_i64() == Some(schema_id))
        .ok_or("current schema not found")?;
    let spec_fields = json!([]);
    for (key, value) in [
        ("schema", table_schema.to_string()),
        ("schema-id", schema_id.to_string()),
        ("partition-spec", spec_fields.to_string()),
        ("partition-spec-id", spec_id.to_string()),
        ("format-version", "2".to_owned()),
        ("content", "data".to_owned()),
    ] {
        writer
            .add_user_metadata(key.to_owned(), value)
            .map_err(|e| e.to_string())?;
    }
    for f in files {
        let entry = Avro::Record(vec![
            ("status".into(), Avro::Int(1)),
            ("snapshot_id".into(), opt(Some(Avro::Long(snapshot_id)))),
            ("sequence_number".into(), opt(None)),
            ("file_sequence_number".into(), opt(None)),
            (
                "data_file".into(),
                Avro::Record(vec![
                    ("content".into(), Avro::Int(0)),
                    ("file_path".into(), Avro::String(f.path.clone())),
                    ("file_format".into(), Avro::String("PARQUET".into())),
                    ("partition".into(), Avro::Record(vec![])),
                    ("record_count".into(), Avro::Long(f.rows as i64)),
                    ("file_size_in_bytes".into(), Avro::Long(f.bytes as i64)),
                ]),
            ),
        ]);
        writer.append_value(entry).map_err(|e| e.to_string())?;
    }
    writer.into_inner().map_err(|e| e.to_string())
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
}

/// The new snapshot's manifest list: the new manifest first, then every
/// manifest of the parent's list, carried over field by field.
fn manifest_list(
    new: &ManifestFile,
    parent: Option<&[u8]>,
    snapshot_id: i64,
    parent_id: Option<i64>,
    sequence: i64,
) -> std::result::Result<Vec<u8>, String> {
    let schema = avro_schema(MANIFEST_FILE)?;
    let mut writer =
        apache_avro::Writer::with_codec(&schema, Vec::new(), codec()).map_err(|e| e.to_string())?;
    for (key, value) in [
        ("snapshot-id", snapshot_id.to_string()),
        (
            "parent-snapshot-id",
            parent_id.map_or("null".to_owned(), |p| p.to_string()),
        ),
        ("sequence-number", sequence.to_string()),
        ("format-version", "2".to_owned()),
    ] {
        writer
            .add_user_metadata(key.to_owned(), value)
            .map_err(|e| e.to_string())?;
    }
    writer
        .append_value(Avro::Record(vec![
            ("manifest_path".into(), Avro::String(new.path.clone())),
            ("manifest_length".into(), Avro::Long(new.length)),
            ("partition_spec_id".into(), Avro::Int(new.spec_id as i32)),
            ("content".into(), Avro::Int(0)),
            ("sequence_number".into(), Avro::Long(new.sequence)),
            ("min_sequence_number".into(), Avro::Long(new.sequence)),
            ("added_snapshot_id".into(), Avro::Long(new.snapshot_id)),
            ("added_files_count".into(), Avro::Int(new.files as i32)),
            ("existing_files_count".into(), Avro::Int(0)),
            ("deleted_files_count".into(), Avro::Int(0)),
            ("added_rows_count".into(), Avro::Long(new.rows)),
            ("existing_rows_count".into(), Avro::Long(0)),
            ("deleted_rows_count".into(), Avro::Long(0)),
            ("partitions".into(), opt(Some(Avro::Array(vec![])))),
            ("key_metadata".into(), opt(None)),
        ]))
        .map_err(|e| e.to_string())?;
    if let Some(bytes) = parent {
        let reader = apache_avro::Reader::new(bytes).map_err(|e| e.to_string())?;
        for entry in reader {
            let entry = entry.map_err(|e| e.to_string())?;
            writer
                .append_value(conform(&entry, &schema))
                .map_err(|e| format!("carrying over a parent manifest: {e}"))?;
        }
    }
    writer.into_inner().map_err(|e| e.to_string())
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

    #[test]
    fn manifests_round_trip_with_field_ids() {
        let meta = json!({"schemas": [{"schema-id": 0, "type": "struct", "fields": []}]});
        let files = [Written {
            path: "s3://b/t/data/a.parquet".into(),
            rows: 3,
            bytes: 100,
        }];
        let bytes = manifest(&meta, &files, 7, 0, 0).unwrap();
        let reader = apache_avro::Reader::new(&bytes[..]).unwrap();
        // Readers resolve fields by id: the ids are in the written schema.
        let written = reader.writer_schema().canonical_form();
        let text = serde_json::to_string(reader.writer_schema()).unwrap();
        assert!(text.contains(r#""field-id":103"#), "{text} / {written}");
        let entries: Vec<Avro> = reader.map(|e| e.unwrap()).collect();
        assert_eq!(entries.len(), 1);
        let data_file = field(&entries[0], "data_file").unwrap();
        assert_eq!(field(data_file, "record_count"), Some(&Avro::Long(3)));
        // A list carries the parent's manifests, v1-era names included.
        let new = ManifestFile {
            path: "m1".into(),
            length: 1,
            spec_id: 0,
            sequence: 2,
            snapshot_id: 7,
            files: 1,
            rows: 3,
        };
        let first = manifest_list(&new, None, 6, None, 1).unwrap();
        let second = manifest_list(
            &ManifestFile {
                path: "m2".into(),
                sequence: 3,
                ..new
            },
            Some(&first),
            8,
            Some(7),
            3,
        )
        .unwrap();
        let paths: Vec<Avro> = apache_avro::Reader::new(&second[..])
            .unwrap()
            .map(|e| field(&e.unwrap(), "manifest_path").cloned().unwrap())
            .collect();
        assert_eq!(
            paths,
            [Avro::String("m2".into()), Avro::String("m1".into())]
        );
    }

    #[test]
    fn only_v2_unpartitioned_flat_tables_take_appends() {
        let table = |version: i64, spec: Json, field_type: Json| {
            json!({
                "format-version": version, "default-spec-id": 0, "current-schema-id": 0,
                "partition-specs": [{"spec-id": 0, "fields": spec}],
                "schemas": [{"schema-id": 0, "fields": [{"id": 1, "name": "x", "required": false, "type": field_type}]}],
            })
        };
        assert_eq!(unwritable(&table(2, json!([]), json!("long"))), None);
        assert!(
            unwritable(&table(1, json!([]), json!("long")))
                .unwrap()
                .contains("v2")
        );
        assert!(unwritable(&table(3, json!([]), json!("long"))).is_some());
        let part =
            json!([{"source-id": 1, "field-id": 1000, "name": "x", "transform": "identity"}]);
        assert!(
            unwritable(&table(2, part, json!("long")))
                .unwrap()
                .contains("partitioned")
        );
        let nested = json!({"type": "struct", "fields": []});
        assert!(
            unwritable(&table(2, json!([]), nested))
                .unwrap()
                .contains("nested")
        );
    }
}

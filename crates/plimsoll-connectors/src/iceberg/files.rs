//! `table$files` and `table$manifests` (ADR 0032): the pinned snapshot's
//! manifest list and live file entries, as tables. Unlike `$snapshots`
//! (ADR 0030) they need object-store reads, so they run at execution time
//! like a scan:
//!
//! - manifests are fetched `meta_fetch_concurrency` at a time (ADR 0020);
//! - fetched bytes are reserved in the session pool while being decoded;
//! - each manifest becomes one batch, so memory stays O(window), not
//!   O(table).
//!
//! Column names follow Spark's metadata tables.

use std::sync::Arc;

use apache_avro::types::Value as Avro;
use datafusion::arrow::array::{ArrayRef, Int32Array, Int64Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::TableProvider;
use datafusion::catalog::streaming::StreamingTable;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::memory_pool::MemoryConsumer;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::PartitionStream;
use futures::stream::{self, BoxStream};
use futures::{StreamExt, TryStreamExt};

use super::{IcebergTable, avro_entries, field, invalid, long_field, read_env, str_field};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    Files,
    Manifests,
}

#[derive(Debug)]
struct ManifestScan {
    table: Arc<IcebergTable>,
    kind: Kind,
    schema: SchemaRef,
}

pub(super) fn provider(
    table: Arc<IcebergTable>,
    kind: &str,
) -> Option<Result<Arc<dyn TableProvider>>> {
    let (kind, schema) = match kind {
        "files" => (Kind::Files, files_schema()),
        "manifests" => (Kind::Manifests, manifests_schema()),
        _ => return None,
    };
    let scan = Arc::new(ManifestScan {
        table,
        kind,
        schema: schema.clone(),
    });
    Some(StreamingTable::try_new(schema, vec![scan]).map(|t| Arc::new(t) as Arc<dyn TableProvider>))
}

fn files_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("content", DataType::Int32, false),
        Field::new("file_path", DataType::Utf8, false),
        Field::new("file_format", DataType::Utf8, true),
        Field::new("spec_id", DataType::Int32, true),
        Field::new("partition", DataType::Utf8, true),
        Field::new("record_count", DataType::Int64, true),
        Field::new("file_size_in_bytes", DataType::Int64, true),
        Field::new("sequence_number", DataType::Int64, true),
    ]))
}

fn manifests_schema() -> SchemaRef {
    let int = |n: &str| Field::new(n, DataType::Int32, true);
    Arc::new(Schema::new(vec![
        Field::new("content", DataType::Int32, false),
        Field::new("path", DataType::Utf8, false),
        Field::new("length", DataType::Int64, true),
        int("partition_spec_id"),
        Field::new("added_snapshot_id", DataType::Int64, true),
        Field::new("sequence_number", DataType::Int64, true),
        int("added_data_files_count"),
        int("existing_data_files_count"),
        int("deleted_data_files_count"),
        int("added_delete_files_count"),
        int("existing_delete_files_count"),
        int("deleted_delete_files_count"),
    ]))
}

impl PartitionStream for ManifestScan {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let (table, kind, schema) = (self.table.clone(), self.kind, self.schema.clone());
        let batches = stream::once(listing(table, kind, schema, ctx)).try_flatten();
        Box::pin(RecordBatchStreamAdapter::new(self.schema.clone(), batches))
    }
}

/// A manifest-list row needed to read the manifest behind it.
struct Manifest {
    path: String,
    seq: i64,
    spec: i64,
}

async fn listing(
    table: Arc<IcebergTable>,
    kind: Kind,
    schema: SchemaRef,
    ctx: Arc<TaskContext>,
) -> Result<BoxStream<'static, Result<RecordBatch>>> {
    let Some(list_path) = table.manifest_list.clone() else {
        return Ok(stream::empty().boxed());
    };
    let env = ctx.runtime_env();
    if let Some(s) = &table.scoped {
        env.register_object_store(s.url.as_ref(), s.store.clone());
    }
    let reservation = Arc::new(
        MemoryConsumer::new(format!("IcebergMetadata[{}]", table.name)).register(ctx.memory_pool()),
    );
    let list = read_env(&list_path, &env, table.scoped.as_ref()).await?;
    reservation.try_grow(list.len())?;
    let mut entries = Vec::new();
    for m in avro_entries(&list_path, &list)? {
        entries.push(m?);
    }
    // Decoded manifest-list rows are small (one per manifest); the raw bytes
    // are no longer needed.
    reservation.shrink(list.len());
    if kind == Kind::Manifests {
        manifests_batch(&list_path, &entries, schema).map(|b| stream::iter([Ok(b)]).boxed())
    } else {
        let manifests = entries
            .iter()
            .map(|m| {
                Ok(Manifest {
                    path: str_field(m, "manifest_path")
                        .ok_or_else(|| invalid(&list_path, "manifest_path missing"))?
                        .to_owned(),
                    seq: long_field(m, "sequence_number").unwrap_or(0),
                    spec: long_field(m, "partition_spec_id").unwrap_or(0),
                })
            })
            .collect::<Result<Vec<_>, super::ConnectorError>>()?;
        let concurrency = usize::from(
            ctx.session_config()
                .options()
                .execution
                .meta_fetch_concurrency,
        );
        Ok(stream::iter(manifests)
            .map(move |m| {
                let (table, env, reservation, schema) = (
                    table.clone(),
                    env.clone(),
                    reservation.clone(),
                    schema.clone(),
                );
                async move {
                    let bytes = read_env(&m.path, &env, table.scoped.as_ref()).await?;
                    reservation.try_grow(bytes.len())?;
                    let batch = files_batch(&m, &bytes, schema);
                    reservation.shrink(bytes.len());
                    batch
                }
            })
            .buffered(concurrency)
            .boxed())
    }
}

fn manifests_batch(list_path: &str, entries: &[Avro], schema: SchemaRef) -> Result<RecordBatch> {
    let long = |names: &[&str]| -> Vec<Option<i64>> {
        entries
            .iter()
            .map(|m| names.iter().find_map(|n| long_field(m, n)))
            .collect()
    };
    let content: Vec<i32> = entries
        .iter()
        .map(|m| long_field(m, "content").map_or(0, |c| i32::from(c == 1)))
        .collect();
    // v2 names counts by manifest content; v1 has data manifests only, with
    // `*_data_files_count`. Spark splits them into data and delete columns.
    let counts = |verb: &str, data: bool| -> ArrayRef {
        let v = long(&[
            &format!("{verb}_files_count"),
            &format!("{verb}_data_files_count"),
        ]);
        Arc::new(Int32Array::from(
            v.iter()
                .zip(&content)
                .map(|(n, c)| {
                    if (*c == 0) == data {
                        n.and_then(|n| i32::try_from(n).ok())
                    } else {
                        Some(0)
                    }
                })
                .collect::<Vec<_>>(),
        ))
    };
    let paths = entries
        .iter()
        .map(|m| {
            str_field(m, "manifest_path").ok_or_else(|| invalid(list_path, "manifest_path missing"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let int32 = |v: Vec<Option<i64>>| -> ArrayRef {
        Arc::new(Int32Array::from(
            v.into_iter()
                .map(|v| v.and_then(|v| i32::try_from(v).ok()))
                .collect::<Vec<_>>(),
        ))
    };
    Ok(RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(content.clone())),
            Arc::new(StringArray::from(paths)),
            Arc::new(Int64Array::from(long(&["manifest_length"]))),
            int32(long(&["partition_spec_id"])),
            Arc::new(Int64Array::from(long(&["added_snapshot_id"]))),
            Arc::new(Int64Array::from(long(&["sequence_number"]))),
            counts("added", true),
            counts("existing", true),
            counts("deleted", true),
            counts("added", false),
            counts("existing", false),
            counts("deleted", false),
        ],
    )?)
}

/// Live entries (status ≠ deleted) of one manifest, data and delete files.
fn files_batch(m: &Manifest, bytes: &[u8], schema: SchemaRef) -> Result<RecordBatch> {
    let (mut content, mut path, mut format, mut partition) = (vec![], vec![], vec![], vec![]);
    let (mut rows, mut size, mut seq) = (vec![], vec![], vec![]);
    for entry in avro_entries(&m.path, bytes)? {
        let entry = entry?;
        if long_field(&entry, "status") == Some(2) {
            continue;
        }
        let file =
            field(&entry, "data_file").ok_or_else(|| invalid(&m.path, "data_file missing"))?;
        content.push(long_field(file, "content").map_or(0, |c| c as i32));
        path.push(
            str_field(file, "file_path")
                .ok_or_else(|| invalid(&m.path, "file_path missing"))?
                .to_owned(),
        );
        format.push(str_field(file, "file_format").map(str::to_uppercase));
        // ponytail: the partition tuple as JSON text (`{"day": 19845}`); a
        // struct column needs the spec's types. Group by it as a string.
        partition.push(
            field(file, "partition")
                .cloned()
                .and_then(|p| serde_json::Value::try_from(p).ok())
                .map(|p| p.to_string()),
        );
        rows.push(long_field(file, "record_count"));
        size.push(long_field(file, "file_size_in_bytes"));
        seq.push(Some(long_field(&entry, "sequence_number").unwrap_or(m.seq)));
    }
    let spec = i32::try_from(m.spec).ok();
    let n = path.len();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(content)),
            Arc::new(StringArray::from(path)),
            Arc::new(StringArray::from(format)),
            Arc::new(Int32Array::from(vec![spec; n])),
            Arc::new(StringArray::from(partition)),
            Arc::new(Int64Array::from(rows)),
            Arc::new(Int64Array::from(size)),
            Arc::new(Int64Array::from(seq)),
        ],
    )
    .map_err(DataFusionError::from)
}

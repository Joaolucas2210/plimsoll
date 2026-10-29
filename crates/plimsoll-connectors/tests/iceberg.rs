//! Iceberg read path on a hand-built v2 table. The interop test at the bottom
//! reads a table written by pyiceberg (`scripts/iceberg_fixture.py`).
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::path::{Path, PathBuf};
use std::sync::Arc;

use apache_avro::types::{Record, Value};
use apache_avro::{Codec, DeflateSettings, Schema as AvroSchema, Writer};
use datafusion::arrow::array::{Int64Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::error::DataFusionError;
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::prelude::{SessionConfig, SessionContext};
use plimsoll_connectors::{Connector, IcebergConnector};
use plimsoll_core::memory::{CeilingPool, session_context_with_ceiling};
use serde_json::json;

const MANIFEST_LIST: &str = r#"{"type":"record","name":"manifest_file","fields":[
  {"name":"manifest_path","type":"string"},{"name":"content","type":"int"}]}"#;
const MANIFEST: &str = r#"{"type":"record","name":"manifest_entry","fields":[
  {"name":"status","type":"int"},
  {"name":"data_file","type":{"type":"record","name":"r2","fields":[
    {"name":"content","type":"int"},{"name":"file_path","type":"string"},
    {"name":"file_format","type":"string"},{"name":"record_count","type":"long"},
    {"name":"file_size_in_bytes","type":"long"}]}}]}"#;

fn tmp(tag: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir =
        std::env::temp_dir().join(format!("plimsoll-ice-{tag}-{}-{nanos}", std::process::id()));
    std::fs::create_dir_all(dir.join("metadata")).unwrap();
    std::fs::create_dir_all(dir.join("data")).unwrap();
    dir
}

fn parquet(dir: &Path, file: &str, ids: &[i64]) -> String {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("city", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(ids.to_vec())),
            Arc::new(StringArray::from_iter_values(
                ids.iter().map(|i| format!("c{i}")),
            )),
        ],
    )
    .unwrap();
    let path = dir.join("data").join(file);
    let mut w = ArrowWriter::try_new(std::fs::File::create(&path).unwrap(), schema, None).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
    format!("file://{}", path.display())
}

fn avro(path: &Path, schema: &str, rows: Vec<Vec<(&str, Value)>>) -> String {
    let schema = AvroSchema::parse_str(schema).unwrap();
    let mut w = Writer::with_codec(
        &schema,
        Vec::new(),
        Codec::Deflate(DeflateSettings::default()),
    )
    .unwrap();
    for row in rows {
        let mut r = Record::new(&schema).unwrap();
        for (k, v) in row {
            r.put(k, v);
        }
        w.append_value(r).unwrap();
    }
    std::fs::write(path, w.into_inner().unwrap()).unwrap();
    format!("file://{}", path.display())
}

fn data_file(content: i32, path: &str) -> Value {
    Value::Record(vec![
        ("content".into(), Value::Int(content)),
        ("file_path".into(), Value::String(path.into())),
        ("file_format".into(), Value::String("PARQUET".into())),
        ("record_count".into(), Value::Long(0)),
        ("file_size_in_bytes".into(), Value::Long(0)),
    ])
}

/// v2 table: one live file, one entry deleted in this snapshot, one orphan
/// parquet file on disk that no manifest references.
fn table(dir: &Path, with_delete_manifest: bool, renamed: bool) {
    let live = parquet(dir, "live.parquet", &[1, 2, 3]);
    let removed = parquet(dir, "removed.parquet", &[100, 200]);
    parquet(dir, "orphan.parquet", &[999]);
    let manifest = avro(
        &dir.join("metadata/m0.avro"),
        MANIFEST,
        vec![
            vec![
                ("status", Value::Int(1)),
                ("data_file", data_file(0, &live)),
            ],
            vec![
                ("status", Value::Int(2)),
                ("data_file", data_file(0, &removed)),
            ],
        ],
    );
    let mut manifests = vec![vec![
        ("manifest_path", Value::String(manifest)),
        ("content", Value::Int(0)),
    ]];
    if with_delete_manifest {
        let deletes = avro(
            &dir.join("metadata/d0.avro"),
            MANIFEST,
            vec![vec![
                ("status", Value::Int(1)),
                ("data_file", data_file(1, &live)),
            ]],
        );
        manifests.push(vec![
            ("manifest_path", Value::String(deletes)),
            ("content", Value::Int(1)),
        ]);
    }
    let list = avro(&dir.join("metadata/snap-1.avro"), MANIFEST_LIST, manifests);
    let field = |id, name: &str, ty: &str, req| json!({"id": id, "name": name, "type": ty, "required": req});
    let mut schemas = vec![json!({"schema-id": 0, "type": "struct", "fields": [
        field(1, "id", "long", true), field(2, "city", "string", false)]})];
    if renamed {
        schemas.insert(
            0,
            json!({"schema-id": 1, "type": "struct", "fields": [
            field(1, "id", "long", true), field(2, "town", "string", false)]}),
        );
    }
    let meta = json!({
        "format-version": 2, "current-schema-id": 0, "schemas": schemas,
        "current-snapshot-id": 1,
        "snapshots": [{"snapshot-id": 1, "manifest-list": list}],
    });
    // An older version that must be ignored in favour of v10.
    std::fs::write(dir.join("metadata/v9.metadata.json"), "{}").unwrap();
    std::fs::write(dir.join("metadata/v10.metadata.json"), meta.to_string()).unwrap();
}

async fn register(ctx: &SessionContext, dir: &Path) -> Result<(), DataFusionError> {
    IcebergConnector::try_new("t", &dir.display().to_string(), &ctx.state())
        .await?
        .register(ctx)
}

async fn ids(ctx: &SessionContext, sql: &str) -> Result<Vec<i64>, DataFusionError> {
    let batches = ctx.sql(sql).await?.collect().await?;
    Ok(batches
        .iter()
        .flat_map(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect())
}

#[tokio::test]
async fn reads_only_live_files_of_the_current_snapshot() {
    let dir = tmp("live");
    table(&dir, false, false);
    let ctx = SessionContext::new();
    register(&ctx, &dir).await.unwrap();
    assert_eq!(
        ids(&ctx, "SELECT id FROM t ORDER BY id").await.unwrap(),
        [1, 2, 3]
    );
    assert_eq!(
        ids(&ctx, "SELECT id FROM t WHERE id > 1 ORDER BY id")
            .await
            .unwrap(),
        [2, 3]
    );
    let count = ctx
        .sql("SELECT count(*) FROM t WHERE city = 'c3'")
        .await
        .unwrap();
    let plan = format!(
        "{}",
        datafusion::physical_plan::displayable(
            count.create_physical_plan().await.unwrap().as_ref()
        )
        .indent(true)
    );
    assert!(plan.contains("AccountedExec: IcebergScan[t]"), "{plan}");
}

#[tokio::test]
async fn delete_files_fail_instead_of_returning_deleted_rows() {
    let dir = tmp("deletes");
    table(&dir, true, false);
    let ctx = SessionContext::new();
    register(&ctx, &dir).await.unwrap();
    let err = ids(&ctx, "SELECT id FROM t").await.unwrap_err();
    assert!(err.to_string().contains("delete files"), "{err}");
}

#[tokio::test]
async fn renamed_columns_are_rejected_at_registration() {
    let dir = tmp("renamed");
    table(&dir, false, true);
    let err = register(&SessionContext::new(), &dir).await.unwrap_err();
    assert!(err.to_string().contains("renamed"), "{err}");
}

#[tokio::test]
async fn empty_table_has_no_rows() {
    let dir = tmp("empty");
    let meta = json!({"format-version": 2, "current-schema-id": 0, "current-snapshot-id": -1,
        "schemas": [{"schema-id": 0, "fields": [{"id": 1, "name": "id", "type": "long", "required": true}]}]});
    std::fs::write(
        dir.join("metadata/00000-abc.metadata.json"),
        meta.to_string(),
    )
    .unwrap();
    let ctx = SessionContext::new();
    register(&ctx, &dir).await.unwrap();
    assert!(ids(&ctx, "SELECT id FROM t").await.unwrap().is_empty());
}

#[tokio::test]
async fn read_path_runs_under_the_ceiling() {
    let dir = tmp("ceiling");
    table(&dir, false, false);
    for (limit, ok) in [(64 << 20, true), (512, false)] {
        let pool = Arc::new(CeilingPool::new(limit, 0).unwrap());
        let ctx = session_context_with_ceiling(pool.clone(), None, SessionConfig::new()).unwrap();
        register(&ctx, &dir).await.unwrap();
        let res = ids(&ctx, "SELECT id FROM t").await;
        match res {
            Ok(rows) => assert!(ok && rows.len() == 3),
            Err(e) => assert!(
                !ok && matches!(e.find_root(), DataFusionError::ResourcesExhausted(m) if m.contains("Iceberg")),
                "{e}"
            ),
        }
        assert_eq!(pool.used(), 0, "everything released (limit {limit})");
    }
}

/// `uv run scripts/iceberg_fixture.py` prints the env for this test.
#[tokio::test]
#[ignore = "needs PLIMSOLL_ICEBERG_FIXTURE from scripts/iceberg_fixture.py"]
async fn reads_a_pyiceberg_table() {
    let Ok(location) = std::env::var("PLIMSOLL_ICEBERG_FIXTURE") else {
        return;
    };
    let ctx = SessionContext::new();
    IcebergConnector::try_new("t", &location, &ctx.state())
        .await
        .unwrap()
        .register(&ctx)
        .unwrap();
    // Fixture: ids 1..=10 over two appends, then `id <= 3` overwritten away
    // (copy-on-write), so ids 4..=10 remain: 7 rows, sum 49, min(name) "n10".
    let batches = ctx
        .sql("SELECT count(*) AS n, sum(id) AS s, count(ts) AS t, min(name) AS m FROM t")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let out = datafusion::arrow::util::pretty::pretty_format_batches(&batches)
        .unwrap()
        .to_string();
    assert!(out.contains("| 7 | 49 | 7 | n10 |"), "{out}");
}

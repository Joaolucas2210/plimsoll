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
use plimsoll_connectors::{Connector, ConnectorError, IcebergConnector};
use plimsoll_core::memory::{CeilingPool, session_context_with_ceiling};
use serde_json::json;

const MANIFEST_LIST: &str = r#"{"type":"record","name":"manifest_file","fields":[
  {"name":"manifest_path","type":"string"},{"name":"content","type":"int"},
  {"name":"sequence_number","type":"long"}]}"#;
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
        ("record_count".into(), Value::Long(3)),
        ("file_size_in_bytes".into(), Value::Long(0)),
    ])
}

/// v2 table: one live file, one entry deleted in this snapshot, one orphan
/// parquet file on disk that no manifest references.
/// Row-level deletes in the fixture.
#[derive(Clone, Copy)]
enum Deletes {
    None,
    /// A position delete file (sequence number `seq`) removing `live.parquet`
    /// row 1 (id 2), plus a row for a file not in the snapshot.
    Position {
        seq: i64,
    },
    Equality,
}

/// Position delete file: Iceberg's `file_path` + `pos` columns.
fn position_deletes(dir: &Path, rows: &[(&str, i64)]) -> String {
    let schema = Arc::new(Schema::new(vec![
        Field::new("file_path", DataType::Utf8, false),
        Field::new("pos", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.0))),
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.1))),
        ],
    )
    .unwrap();
    let path = dir.join("data").join("pos-deletes.parquet");
    let mut w = ArrowWriter::try_new(std::fs::File::create(&path).unwrap(), schema, None).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
    format!("file://{}", path.display())
}

/// Data manifest has sequence number 2.
fn table(dir: &Path, deletes: Deletes, renamed: bool) {
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
        ("sequence_number", Value::Long(2)),
    ]];
    let (content, file, seq) = match deletes {
        Deletes::None => (0, String::new(), 0),
        Deletes::Position { seq } => {
            let f = position_deletes(
                dir,
                &[(&live, 1), (&removed, 0), ("file:///elsewhere.parquet", 7)],
            );
            (1, f, seq)
        }
        Deletes::Equality => (2, live.clone(), 3),
    };
    if content != 0 {
        let deletes = avro(
            &dir.join("metadata/d0.avro"),
            MANIFEST,
            vec![vec![
                ("status", Value::Int(1)),
                ("data_file", data_file(content, &file)),
            ]],
        );
        manifests.push(vec![
            ("manifest_path", Value::String(deletes)),
            ("content", Value::Int(1)),
            ("sequence_number", Value::Long(seq)),
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
    table(&dir, Deletes::None, false);
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
async fn position_deletes_are_applied_as_row_selections() {
    let dir = tmp("pos-deletes");
    table(&dir, Deletes::Position { seq: 3 }, false);
    let ctx = SessionContext::new();
    register(&ctx, &dir).await.unwrap();
    // id 2 (live.parquet row 1) is deleted; rows for other files are ignored.
    assert_eq!(
        ids(&ctx, "SELECT id FROM t ORDER BY id").await.unwrap(),
        [1, 3]
    );
    assert_eq!(
        ids(&ctx, "SELECT id FROM t WHERE id >= 2 ORDER BY id")
            .await
            .unwrap(),
        [3]
    );
    let df = ctx.sql("SELECT count(*) FROM t").await.unwrap();
    let plan = format!(
        "{}",
        datafusion::physical_plan::displayable(df.create_physical_plan().await.unwrap().as_ref())
            .indent(true)
    );
    assert!(plan.contains("deletes=1 deleted_rows=1"), "{plan}");
}

#[tokio::test]
async fn position_deletes_older_than_the_data_do_not_apply() {
    // Delete seq 1 < data seq 2: the delete predates the data file (spec v2).
    let dir = tmp("old-deletes");
    table(&dir, Deletes::Position { seq: 1 }, false);
    let ctx = SessionContext::new();
    register(&ctx, &dir).await.unwrap();
    assert_eq!(
        ids(&ctx, "SELECT id FROM t ORDER BY id").await.unwrap(),
        [1, 2, 3]
    );
}

#[tokio::test]
async fn equality_deletes_fail_instead_of_returning_deleted_rows() {
    let dir = tmp("eq-deletes");
    table(&dir, Deletes::Equality, false);
    let ctx = SessionContext::new();
    register(&ctx, &dir).await.unwrap();
    let err = ids(&ctx, "SELECT id FROM t").await.unwrap_err();
    assert!(err.to_string().contains("equality delete files"), "{err}");
    // Typed at the source so the server can answer 422, not 500 (ADR 0013).
    let root = err.find_root();
    assert!(
        matches!(root, DataFusionError::External(e)
            if matches!(e.downcast_ref::<ConnectorError>(), Some(ConnectorError::Unsupported(_)))),
        "{root:?}"
    );
}

#[tokio::test]
async fn renamed_columns_are_rejected_at_registration() {
    let dir = tmp("renamed");
    table(&dir, Deletes::None, true);
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
    table(&dir, Deletes::None, false);
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

/// `uv run scripts/iceberg_fixture.py` prints the env for this test; the
/// location may be local or `s3://` (with `AWS_ENDPOINT` etc. set).
#[tokio::test]
#[ignore = "needs PLIMSOLL_ICEBERG_FIXTURE from scripts/iceberg_fixture.py"]
async fn reads_a_pyiceberg_table() {
    let Ok(location) = std::env::var("PLIMSOLL_ICEBERG_FIXTURE") else {
        return;
    };
    // Engine runtime: object stores (file://, s3:// from AWS_* env) on demand.
    let pool = Arc::new(CeilingPool::new(256 << 20, 0).unwrap());
    let ctx = session_context_with_ceiling(pool, None, SessionConfig::new()).unwrap();
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

/// Pruning on a real partitioned table (`bronze.events` from the fixture):
/// day(ts) partition summaries skip manifests, column bounds skip files, and
/// results are unchanged.
#[tokio::test]
#[ignore = "needs PLIMSOLL_ICEBERG_EVENTS from scripts/iceberg_fixture.py"]
async fn prunes_a_partitioned_pyiceberg_table() {
    let Ok(location) = std::env::var("PLIMSOLL_ICEBERG_EVENTS") else {
        return;
    };
    let pool = Arc::new(CeilingPool::new(256 << 20, 0).unwrap());
    let ctx = session_context_with_ceiling(pool, None, SessionConfig::new()).unwrap();
    IcebergConnector::try_new("events", &location, &ctx.state())
        .await
        .unwrap()
        .register(&ctx)
        .unwrap();
    let check = async |sql: &str, expected: &str, scan: &str| {
        let df = ctx.sql(sql).await.unwrap();
        let plan = datafusion::physical_plan::displayable(
            df.clone().create_physical_plan().await.unwrap().as_ref(),
        )
        .indent(true)
        .to_string();
        assert!(plan.contains(scan), "expected `{scan}` in\n{plan}");
        let out =
            datafusion::arrow::util::pretty::pretty_format_batches(&df.collect().await.unwrap())
                .unwrap()
                .to_string();
        assert!(out.contains(expected), "{sql}\n{out}");
    };
    // Days 7..=9 survive the partition summaries; no file is wasted.
    check(
        "SELECT count(*) AS n, min(id) AS m FROM events WHERE ts >= '2024-01-08T00:00:00Z'",
        "| 30 | 70 |",
        "manifests=3/10 files=3/3",
    )
    .await;
    // `ts < midnight` keeps the strict bound: day 0 only.
    check(
        "SELECT count(*) AS n, max(id) AS m FROM events WHERE ts < '2024-01-02T00:00:00Z'",
        "| 10 | 9 |",
        "manifests=1/10 files=1/1",
    )
    .await;
    // No partition predicate: every manifest is read, id bounds pick one file.
    check(
        "SELECT count(*) AS n, sum(id) AS s FROM events WHERE id BETWEEN 42 AND 44",
        "| 3 | 129 |",
        "manifests=10/10 files=1/10",
    )
    .await;
    // OR is not provable: nothing pruned, result still right.
    check(
        "SELECT count(*) AS n FROM events WHERE id = 3 OR id = 97",
        "| 2 |",
        "manifests=10/10 files=10/10",
    )
    .await;
}

/// Merge-on-read written by Spark (the reference engine): `DELETE` and
/// `UPDATE` produce position delete files. Expected values are Spark's own
/// answers (see `scripts/spark_mor_fixture.sh`).
#[tokio::test]
#[ignore = "needs PLIMSOLL_ICEBERG_MOR from scripts/spark_mor_fixture.sh"]
async fn reads_a_spark_merge_on_read_table() {
    let Ok(location) = std::env::var("PLIMSOLL_ICEBERG_MOR") else {
        return;
    };
    let pool = Arc::new(CeilingPool::new(256 << 20, 0).unwrap());
    let ctx = session_context_with_ceiling(pool.clone(), None, SessionConfig::new()).unwrap();
    IcebergConnector::try_new("mor", &location, &ctx.state())
        .await
        .unwrap()
        .register(&ctx)
        .unwrap();
    let check = async |sql: &str, expected: &str| {
        let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        let out = datafusion::arrow::util::pretty::pretty_format_batches(&batches)
            .unwrap()
            .to_string();
        assert!(out.contains(expected), "{sql}\n{out}");
    };
    check(
        "SELECT count(*) AS n, sum(id) AS s, sum(CASE WHEN name = 'updated' THEN 1 ELSE 0 END) AS u, \
         min(id) AS lo, max(id) AS hi FROM mor",
        "| 32 | 768 | 1 | 1  | 47 |",
    )
    .await;
    // Pruning and deletes together: day 2 only.
    check(
        "SELECT count(*) AS n, sum(id) AS s FROM mor WHERE ts >= '2024-01-02T00:00:00Z'",
        "| 16 | 576 |",
    )
    .await;
    // No deleted id survives (multiples of 3), and id 1 appears once.
    check(
        "SELECT count(*) AS n FROM mor WHERE id % 3 = 0 OR (id = 1 AND name <> 'updated')",
        "| 0 |",
    )
    .await;
    assert_eq!(pool.used(), 0);
}

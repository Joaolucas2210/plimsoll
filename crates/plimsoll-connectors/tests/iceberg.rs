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
  {"name":"manifest_path","type":"string"},{"name":"content","type":"int"},
  {"name":"sequence_number","type":"long"}]}"#;
const MANIFEST: &str = r#"{"type":"record","name":"manifest_entry","fields":[
  {"name":"status","type":"int"},
  {"name":"data_file","type":{"type":"record","name":"r2","fields":[
    {"name":"content","type":"int"},{"name":"file_path","type":"string"},
    {"name":"file_format","type":"string"},{"name":"record_count","type":"long"},
    {"name":"file_size_in_bytes","type":"long"}]}}]}"#;

/// Delete manifest entry carrying `equality_ids`.
const EQ_MANIFEST: &str = r#"{"type":"record","name":"manifest_entry","fields":[
  {"name":"status","type":"int"},
  {"name":"data_file","type":{"type":"record","name":"r2","fields":[
    {"name":"content","type":"int"},{"name":"file_path","type":"string"},
    {"name":"file_format","type":"string"},{"name":"record_count","type":"long"},
    {"name":"file_size_in_bytes","type":"long"},
    {"name":"equality_ids","type":["null",{"type":"array","items":"int"}]}]}}]}"#;

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
    /// An equality delete file (sequence number 3) on `city` removing `c2`
    /// and `c5`; `late.parquet` (ids 4-6, seq 4) is newer, so `c5` stays.
    Equality,
}

/// Equality delete file keyed on `city`.
fn equality_deletes(dir: &Path, cities: &[&str]) -> String {
    let schema = Arc::new(Schema::new(vec![Field::new("city", DataType::Utf8, true)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(StringArray::from_iter_values(
            cities.iter().copied(),
        ))],
    )
    .unwrap();
    let path = dir.join("data").join("eq-deletes.parquet");
    let mut w = ArrowWriter::try_new(std::fs::File::create(&path).unwrap(), schema, None).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
    format!("file://{}", path.display())
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
        Deletes::Equality => {
            let late = parquet(dir, "late.parquet", &[4, 5, 6]);
            let m = avro(
                &dir.join("metadata/m1.avro"),
                MANIFEST,
                vec![vec![
                    ("status", Value::Int(1)),
                    ("data_file", data_file(0, &late)),
                ]],
            );
            manifests.push(vec![
                ("manifest_path", Value::String(m)),
                ("content", Value::Int(0)),
                ("sequence_number", Value::Long(4)),
            ]);
            let f = equality_deletes(dir, &["c2", "c5"]);
            let Value::Record(mut df) = data_file(2, &f) else {
                unreachable!()
            };
            df.push((
                "equality_ids".into(),
                Value::Union(1, Box::new(Value::Array(vec![Value::Int(2)]))),
            ));
            let d = avro(
                &dir.join("metadata/d0.avro"),
                EQ_MANIFEST,
                vec![vec![
                    ("status", Value::Int(1)),
                    ("data_file", Value::Record(df)),
                ]],
            );
            manifests.push(vec![
                ("manifest_path", Value::String(d)),
                ("content", Value::Int(1)),
                ("sequence_number", Value::Long(3)),
            ]);
            (0, String::new(), 0)
        }
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
async fn equality_deletes_apply_only_to_older_data() {
    let dir = tmp("eq-deletes");
    table(&dir, Deletes::Equality, false);
    let ctx = SessionContext::new();
    register(&ctx, &dir).await.unwrap();
    // c2 (seq 2 < 3) is deleted; c5 lives in a newer file (seq 4) and stays.
    // The key column is scanned for the join even when not selected.
    assert_eq!(
        ids(&ctx, "SELECT id FROM t ORDER BY id").await.unwrap(),
        [1, 3, 4, 5, 6]
    );
    assert_eq!(
        ids(&ctx, "SELECT id FROM t WHERE id < 5 ORDER BY id LIMIT 2")
            .await
            .unwrap(),
        [1, 3]
    );
    assert_eq!(ids(&ctx, "SELECT count(*) FROM t").await.unwrap(), [5]);
    let df = ctx.sql("SELECT city FROM t").await.unwrap();
    let plan = format!(
        "{}",
        datafusion::physical_plan::displayable(df.create_physical_plan().await.unwrap().as_ref())
            .indent(true)
    );
    assert!(plan.contains("equality_deletes=1"), "{plan}");
    assert!(
        plan.contains("HashJoinExec: mode=CollectLeft, join_type=RightAnti"),
        "{plan}"
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
async fn v3_features_that_change_answers_are_refused() {
    let register_meta = async |tag: &str, meta: serde_json::Value| {
        let dir = tmp(tag);
        std::fs::write(dir.join("metadata/v1.metadata.json"), meta.to_string()).unwrap();
        register(&SessionContext::new(), &dir).await
    };
    let schema = |field: serde_json::Value| json!([{"schema-id": 0, "fields": [{"id": 1, "name": "id", "type": "long", "required": true}, field]}]);
    // A column added with a default: old files must read 'x', not NULL.
    let err = register_meta(
        "v3-default",
        json!({"format-version": 3, "current-schema-id": 0,
        "current-snapshot-id": -1, "schemas": schema(json!({"id": 2, "name": "c",
        "type": "string", "required": false, "initial-default": "x"}))}),
    )
    .await
    .unwrap_err();
    assert!(err.to_string().contains("column default values"), "{err}");
    let err = register_meta(
        "v3-encrypted",
        json!({"format-version": 3, "current-schema-id": 0,
        "current-snapshot-id": -1, "encryption-keys": [{"key-id": "k"}],
        "schemas": schema(json!({"id": 2, "name": "c", "type": "string", "required": false}))}),
    )
    .await
    .unwrap_err();
    assert!(err.to_string().contains("encrypted"), "{err}");
    // Plain v3 with a nanosecond timestamp is readable.
    register_meta(
        "v3-plain",
        json!({"format-version": 3, "current-schema-id": 0,
        "current-snapshot-id": -1, "schemas": schema(json!({"id": 2, "name": "ts",
        "type": "timestamp_ns", "required": false}))}),
    )
    .await
    .unwrap();
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

/// Equality deletes written by Iceberg's Java writer (`scripts/spark_mor_fixture.sh`):
/// keyed on `city` (including a null key), scoped to partition `region='a'`,
/// followed by a newer insert with a deleted key. Spark reads ids 2, 4, 5, 6.
#[tokio::test]
#[ignore = "needs PLIMSOLL_ICEBERG_EQ from scripts/spark_mor_fixture.sh"]
async fn reads_reference_equality_deletes() {
    let Ok(location) = std::env::var("PLIMSOLL_ICEBERG_EQ") else {
        return;
    };
    let pool = Arc::new(CeilingPool::new(256 << 20, 0).unwrap());
    let ctx = session_context_with_ceiling(pool.clone(), None, SessionConfig::new()).unwrap();
    IcebergConnector::try_new("eq", &location, &ctx.state())
        .await
        .unwrap()
        .register(&ctx)
        .unwrap();
    // Null keys match null (spec), other partitions and newer rows are kept.
    assert_eq!(
        ids(&ctx, "SELECT id FROM eq ORDER BY id").await.unwrap(),
        [2, 4, 5, 6]
    );
    assert_eq!(
        ids(&ctx, "SELECT id FROM eq WHERE region = 'b' ORDER BY id")
            .await
            .unwrap(),
        [4, 5]
    );
    assert_eq!(ids(&ctx, "SELECT count(*) FROM eq").await.unwrap(), [4]);
    assert_eq!(pool.used(), 0);
}

/// Format v3 table written by Spark with merge-on-read deletes, i.e. deletion
/// vectors in Puffin files (`scripts/spark_mor_fixture.sh`): 100 000 rows,
/// `DELETE id % 7 = 0`, then `DELETE id < 10` (replaces DVs), then an `UPDATE`.
/// Spark reads 85 706 rows summing to 4 285 685 677.
#[tokio::test]
#[ignore = "needs PLIMSOLL_ICEBERG_DV from scripts/spark_mor_fixture.sh"]
async fn reads_reference_deletion_vectors() {
    let Ok(location) = std::env::var("PLIMSOLL_ICEBERG_DV") else {
        return;
    };
    let pool = Arc::new(CeilingPool::new(256 << 20, 0).unwrap());
    let ctx = session_context_with_ceiling(pool.clone(), None, SessionConfig::new()).unwrap();
    IcebergConnector::try_new("dv", &location, &ctx.state())
        .await
        .unwrap()
        .register(&ctx)
        .unwrap();
    let out = |sql: &'static str| {
        let ctx = ctx.clone();
        async move {
            let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
            datafusion::arrow::util::pretty::pretty_format_batches(&batches)
                .unwrap()
                .to_string()
        }
    };
    let totals = out("SELECT count(*) AS n, sum(id) AS s FROM dv").await;
    assert!(totals.contains("| 85706 | 4285685677 |"), "{totals}");
    let gone = out("SELECT count(*) AS n FROM dv WHERE id % 7 = 0 OR id < 10").await;
    assert!(gone.contains("| 0 |"), "{gone}");
    let updated = out("SELECT name FROM dv WHERE id = 99999").await;
    assert!(
        updated.contains("| updated |") && !updated.contains("n99999"),
        "{updated}"
    );
    assert_eq!(pool.used(), 0);
}

/// Decoded delete files are cached inside the pool's cache reserve: the
/// second query reads no delete file and returns the same rows (ADR 0018).
#[tokio::test]
async fn delete_files_are_cached_within_the_cache_reserve() {
    let explain = async |ctx: &SessionContext, sql: &str| {
        let df = ctx.sql(sql).await.unwrap();
        format!(
            "{}",
            datafusion::physical_plan::displayable(
                df.create_physical_plan().await.unwrap().as_ref()
            )
            .indent(true)
        )
    };
    for (deletes, expected) in [
        (Deletes::Position { seq: 3 }, vec![1, 3]),
        (Deletes::Equality, vec![1, 3, 4, 5, 6]),
    ] {
        let dir = tmp("cache");
        table(&dir, deletes, false);
        let pool = Arc::new(
            CeilingPool::new(64 << 20, 0)
                .unwrap()
                .with_cache_reserve(8 << 20)
                .unwrap(),
        );
        let ctx = session_context_with_ceiling(pool.clone(), None, SessionConfig::new()).unwrap();
        register(&ctx, &dir).await.unwrap();
        let sql = "SELECT id FROM t ORDER BY id";
        assert!(explain(&ctx, sql).await.contains("delete_cache_hits=0"));
        assert!(explain(&ctx, sql).await.contains("delete_cache_hits=1"));
        // Cached state is not query memory: nothing stays reserved.
        assert_eq!(ids(&ctx, sql).await.unwrap(), expected);
        assert_eq!(pool.used(), 0);
        // Without a cache reserve nothing is cached.
        let plain = session_context_with_ceiling(
            Arc::new(CeilingPool::new(64 << 20, 0).unwrap()),
            None,
            SessionConfig::new(),
        )
        .unwrap();
        register(&plain, &dir).await.unwrap();
        explain(&plain, sql).await;
        assert!(explain(&plain, sql).await.contains("delete_cache_hits=0"));
    }
}

/// A table with `n` data manifests of one file each (ids 0..n).
fn many_manifests(dir: &Path, n: i64) {
    let manifests = (0..n)
        .map(|i| {
            let data = parquet(dir, &format!("d{i}.parquet"), &[i]);
            // Real sizes, as the spec requires: no HEAD fallback per file.
            let size = std::fs::metadata(dir.join(format!("data/d{i}.parquet")))
                .unwrap()
                .len();
            let Value::Record(mut file) = data_file(0, &data) else {
                unreachable!()
            };
            file.retain(|(k, _)| k != "file_size_in_bytes");
            file.push((
                "file_size_in_bytes".into(),
                Value::Long(i64::try_from(size).unwrap()),
            ));
            let m = avro(
                &dir.join(format!("metadata/m{i}.avro")),
                MANIFEST,
                vec![vec![
                    ("status", Value::Int(1)),
                    ("data_file", Value::Record(file)),
                ]],
            );
            vec![
                ("manifest_path", Value::String(m)),
                ("content", Value::Int(0)),
                ("sequence_number", Value::Long(1)),
            ]
        })
        .collect();
    let list = avro(&dir.join("metadata/snap-1.avro"), MANIFEST_LIST, manifests);
    let meta = json!({
        "format-version": 2, "current-schema-id": 0, "current-snapshot-id": 1,
        "schemas": [{"schema-id": 0, "fields": [
            {"id": 1, "name": "id", "type": "long", "required": true},
            {"id": 2, "name": "city", "type": "string", "required": false}]}],
        "snapshots": [{"snapshot-id": 1, "manifest-list": list}],
    });
    std::fs::write(dir.join("metadata/v1.metadata.json"), meta.to_string()).unwrap();
}

/// Manifests are fetched concurrently (ADR 0020): with 50 ms per GET and 8
/// manifests, planning takes a few round trips instead of one per manifest.
#[tokio::test]
async fn planning_fetches_manifests_concurrently() {
    use datafusion::execution::object_store::ObjectStoreUrl;
    use datafusion::object_store::memory::InMemory;
    use datafusion::object_store::path::Path as StorePath;
    use datafusion::object_store::throttle::{ThrottleConfig, ThrottledStore};
    use datafusion::object_store::{ObjectStoreExt, PutPayload};
    use std::time::{Duration, Instant};

    let dir = tmp("concurrent");
    many_manifests(&dir, 8);
    // Same paths as on disk (`ThrottledStore` can't wrap file-backed reads).
    let files = InMemory::new();
    for sub in ["metadata", "data"] {
        for entry in std::fs::read_dir(dir.join(sub)).unwrap() {
            let path = entry.unwrap().path();
            let key = StorePath::from_absolute_path(&path).unwrap();
            let bytes = std::fs::read(&path).unwrap();
            files.put(&key, PutPayload::from(bytes)).await.unwrap();
        }
    }
    let files = Arc::new(files);
    let plan_time = async |concurrency: usize| {
        let config = SessionConfig::new()
            .set_usize("datafusion.execution.meta_fetch_concurrency", concurrency);
        let ctx = SessionContext::new_with_config(config);
        let slow = ThrottleConfig {
            wait_get_per_call: Duration::from_millis(50),
            ..Default::default()
        };
        ctx.runtime_env().register_object_store(
            ObjectStoreUrl::local_filesystem().as_ref(),
            Arc::new(ThrottledStore::new(files.clone(), slow)),
        );
        register(&ctx, &dir).await.unwrap();
        let df = ctx.sql("SELECT sum(id) FROM t").await.unwrap();
        let started = Instant::now();
        df.clone().create_physical_plan().await.unwrap();
        let elapsed = started.elapsed();
        assert_eq!(ids(&ctx, "SELECT sum(id) FROM t").await.unwrap(), [28]);
        elapsed
    };
    let sequential = plan_time(1).await;
    let concurrent = plan_time(32).await;
    eprintln!("planning with 50 ms/GET: sequential {sequential:?}, concurrent {concurrent:?}");
    // Sequential: list + 8 manifests = 9 round trips (~450 ms); concurrent:
    // list + one wave (~100 ms).
    assert!(sequential >= Duration::from_millis(400), "{sequential:?}");
    assert!(
        concurrent * 2 < sequential,
        "{concurrent:?} vs {sequential:?}"
    );
}

/// Time travel on the Spark MOR table (ADR 0028): the first snapshot is the
/// plain insert (48 rows, no deletes yet); the second applies `DELETE id % 3
/// = 0`; by instant, the first snapshot-log entry's time selects the first.
#[tokio::test]
#[ignore = "needs PLIMSOLL_ICEBERG_MOR from scripts/spark_mor_fixture.sh"]
async fn time_travels_a_spark_table() {
    use plimsoll_connectors::AsOf;
    use plimsoll_connectors::iceberg::at;

    let Ok(location) = std::env::var("PLIMSOLL_ICEBERG_MOR") else {
        return;
    };
    // Hadoop catalog: newest vN.metadata.json.
    let newest = std::fs::read_dir(Path::new(&location).join("metadata"))
        .unwrap()
        .filter_map(|e| {
            let name = e.unwrap().file_name().into_string().unwrap();
            let n: u64 = name
                .strip_prefix('v')?
                .strip_suffix(".metadata.json")?
                .parse()
                .ok()?;
            Some((n, name))
        })
        .max()
        .unwrap()
        .1;
    let path = format!("{location}/metadata/{newest}");
    let meta: serde_json::Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    let log = meta["snapshot-log"].as_array().unwrap();
    let (first, second) = (&log[0], &log[1]);
    let totals = async |as_of: AsOf| {
        let pool = Arc::new(CeilingPool::new(256 << 20, 0).unwrap());
        let ctx = session_context_with_ceiling(pool.clone(), None, SessionConfig::new()).unwrap();
        let pinned = at(&meta, as_of).unwrap();
        IcebergConnector::from_metadata("t", &path, &pinned)
            .unwrap()
            .register(&ctx)
            .unwrap();
        let b = ctx
            .sql("SELECT count(*) AS n, sum(id) AS s FROM t")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let col = |i| {
            b[0].column(i)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0)
        };
        assert_eq!(pool.used(), 0);
        (col(0), col(1))
    };
    let id = |e: &serde_json::Value| e["snapshot-id"].as_i64().unwrap();
    assert_eq!(totals(AsOf::Snapshot(id(first))).await, (48, 1128));
    assert_eq!(totals(AsOf::Snapshot(id(second))).await, (32, 768));
    let ts = first["timestamp-ms"].as_i64().unwrap();
    assert_eq!(totals(AsOf::TimestampMs(ts)).await, (48, 1128));
    // Refs (ADR 0029): the tag on the insert, and a branch holding one extra
    // row (id 1000) that main never saw.
    let tag = plimsoll_connectors::iceberg::ref_snapshot(&meta, "after_insert").unwrap();
    assert_eq!(totals(AsOf::Snapshot(tag)).await, (48, 1128));
    let audit = plimsoll_connectors::iceberg::ref_snapshot(&meta, "audit").unwrap();
    assert_eq!(totals(AsOf::Snapshot(audit)).await, (49, 2128));
    let main = plimsoll_connectors::iceberg::ref_snapshot(&meta, "main").unwrap();
    assert_eq!(totals(AsOf::Snapshot(main)).await, (32, 768));
    // Before the first snapshot there is nothing to read.
    assert!(at(&meta, AsOf::TimestampMs(ts - 1)).is_err());
}

#[test]
fn as_of_parses_snapshot_ids_and_instants() {
    use plimsoll_connectors::AsOf;
    assert_eq!(
        AsOf::parse("4876546583428101137"),
        Some(AsOf::Snapshot(4876546583428101137))
    );
    assert_eq!(
        AsOf::parse("2024-05-01T00:00:00Z"),
        Some(AsOf::TimestampMs(1_714_521_600_000))
    );
    assert_eq!(
        AsOf::parse("2024-05-01T02:00:00+02:00"),
        Some(AsOf::TimestampMs(1_714_521_600_000)),
        "offsets normalise to UTC"
    );
    assert_eq!(AsOf::parse("yesterday"), None);
    assert_eq!(AsOf::parse("2024-05-01"), None, "a date is not an instant");
}

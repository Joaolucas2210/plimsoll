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

/// Iceberg scans declare table@snapshot, so results over them can be
/// cached and a new snapshot is a new key (ADR 0038).
#[tokio::test]
async fn iceberg_scans_are_pinned_to_their_snapshot() {
    let dir = tmp("pinned");
    table(&dir, Deletes::None, false);
    let ctx = SessionContext::new();
    register(&ctx, &dir).await.unwrap();
    let physical = ctx
        .sql("SELECT id FROM t WHERE id > 1")
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();
    let key = plimsoll_core::result_cache::key("q", &physical).unwrap();
    assert!(key.ends_with("t@1#0"), "{key:?}");
}

/// Distinct counts from a statistics file's metadata (ADR 0114) size joins
/// on repeated keys, which DataFusion otherwise takes as unique. Only the
/// metadata's `ndv` is read: no Puffin file needs to exist.
#[tokio::test]
async fn distinct_counts_size_joins_on_repeated_keys() {
    use datafusion::common::stats::Precision;
    use datafusion::physical_plan::statistics::{StatisticsArgs, StatisticsContext};
    let dir = tmp("ndv");
    table(&dir, Deletes::None, false);
    let estimate = async || {
        let ctx = SessionContext::new();
        register(&ctx, &dir).await.unwrap();
        let plan = ctx
            .sql("SELECT a.id FROM t a JOIN t b ON a.city = b.city")
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        StatisticsContext::new()
            .compute(plan.as_ref(), &StatisticsArgs::new())
            .unwrap()
            .num_rows
    };
    // Three rows; a string key has no range to bound its distinct values.
    assert_eq!(estimate().await, Precision::Inexact(3));
    let path = dir.join("metadata/v10.metadata.json");
    let mut meta: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
    meta["statistics"] = json!([{
        "snapshot-id": 1, "statistics-path": "file:///nowhere.puffin",
        "file-size-in-bytes": 1, "file-footer-size-in-bytes": 1,
        "blob-metadata": [{"type": "apache-datasketches-theta-v1", "snapshot-id": 1,
            "sequence-number": 1, "fields": [2], "properties": {"ndv": "1"}}],
    }]);
    std::fs::write(&path, meta.to_string()).unwrap();
    // One city: every row meets every row.
    assert_eq!(estimate().await, Precision::Inexact(9));
}

/// `count(*)` from manifest record counts (ADR 0054): no data file is
/// read when no delete applies; with deletes or filters the scan stays.
#[tokio::test]
async fn count_star_is_answered_from_metadata_only_when_exact() {
    let plan_of = async |ctx: &SessionContext, sql: &str| {
        let plan = ctx
            .sql(sql)
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        datafusion::physical_plan::displayable(plan.as_ref())
            .indent(true)
            .to_string()
    };
    let dir = tmp("count-meta");
    table(&dir, Deletes::None, false);
    let ctx = SessionContext::new();
    register(&ctx, &dir).await.unwrap();
    let plan = plan_of(&ctx, "SELECT count(*) FROM t").await;
    assert!(
        !plan.contains("IcebergScan"),
        "answered from metadata:\n{plan}"
    );
    assert_eq!(ids(&ctx, "SELECT count(*) FROM t").await.unwrap(), [3]);
    // A filter (pushed into Parquet) keeps the scan.
    let plan = plan_of(&ctx, "SELECT count(*) FROM t WHERE id > 1").await;
    assert!(plan.contains("IcebergScan"), "{plan}");
    assert_eq!(
        ids(&ctx, "SELECT count(*) FROM t WHERE id > 1")
            .await
            .unwrap(),
        [2]
    );

    for (deletes, rows) in [(Deletes::Position { seq: 3 }, 2), (Deletes::Equality, 5)] {
        let dir = tmp("count-deletes");
        table(&dir, deletes, false);
        let ctx = SessionContext::new();
        register(&ctx, &dir).await.unwrap();
        let plan = plan_of(&ctx, "SELECT count(*) FROM t").await;
        assert!(
            plan.contains("IcebergScan"),
            "deletes need the scan:\n{plan}"
        );
        assert_eq!(ids(&ctx, "SELECT count(*) FROM t").await.unwrap(), [rows]);
    }
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

/// A renamed column in a table whose files carry no field ids cannot be
/// resolved by name safely: a typed error, never a wrong column (ADR 0040).
#[tokio::test]
async fn renamed_tables_without_field_ids_are_refused() {
    let dir = tmp("renamed");
    table(&dir, Deletes::None, true);
    let ctx = SessionContext::new();
    register(&ctx, &dir).await.unwrap();
    let err = ids(&ctx, "SELECT id FROM t").await.unwrap_err();
    assert!(
        err.to_string().contains("without Parquet field ids"),
        "{err}"
    );
}

/// Columns resolve by Iceberg field id (ADR 0040): a file written before a
/// rename reads under the new name, filters included; a column dropped and
/// re-added under an old name is NULL in older files.
#[tokio::test]
async fn columns_resolve_by_field_id_across_renames() {
    use std::collections::HashMap;
    let dir = tmp("field-ids");
    let with_id = |name: &str, ty: DataType, id: i64| {
        Field::new(name, ty, true).with_metadata(HashMap::from([(
            "PARQUET:field_id".to_owned(),
            id.to_string(),
        )]))
    };
    // Written under the first schema: id (1), town (2).
    let schema = Arc::new(Schema::new(vec![
        with_id("id", DataType::Int64, 1),
        with_id("town", DataType::Utf8, 2),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["lisbon", "porto", "braga"])),
        ],
    )
    .unwrap();
    let path = dir.join("data").join("old.parquet");
    let mut w = ArrowWriter::try_new(std::fs::File::create(&path).unwrap(), schema, None).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
    let file = format!("file://{}", path.display());
    let manifest = avro(
        &dir.join("metadata/m0.avro"),
        MANIFEST,
        vec![vec![
            ("status", Value::Int(1)),
            ("data_file", data_file(0, &file)),
        ]],
    );
    let list = avro(
        &dir.join("metadata/snap-1.avro"),
        MANIFEST_LIST,
        vec![vec![
            ("manifest_path", Value::String(manifest)),
            ("content", Value::Int(0)),
            ("sequence_number", Value::Long(1)),
        ]],
    );
    let field =
        |id, name: &str, ty: &str| json!({"id": id, "name": name, "type": ty, "required": false});
    // Then `town` was renamed to `city`, and a new `town` (field 3) added.
    let meta = json!({
        "format-version": 2, "current-schema-id": 1,
        "schemas": [
            {"schema-id": 0, "type": "struct", "fields": [field(1, "id", "long"), field(2, "town", "string")]},
            {"schema-id": 1, "type": "struct", "fields": [
                field(1, "id", "long"), field(2, "city", "string"), field(3, "town", "string")]}],
        "current-snapshot-id": 1,
        "snapshots": [{"snapshot-id": 1, "manifest-list": list}],
    });
    std::fs::write(dir.join("metadata/v1.metadata.json"), meta.to_string()).unwrap();
    let ctx = SessionContext::new();
    register(&ctx, &dir).await.unwrap();
    let out = ctx
        .sql("SELECT id, city, town FROM t WHERE city <> 'porto' ORDER BY id")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let text = datafusion::arrow::util::pretty::pretty_format_batches(&out)
        .unwrap()
        .to_string();
    let rows: Vec<Vec<&str>> = text
        .lines()
        .filter(|l| l.starts_with("| ") && !l.contains("city"))
        .map(|l| {
            l.split('|')
                .map(str::trim)
                .filter(|c| !c.is_empty())
                .collect()
        })
        .collect();
    assert_eq!(rows, [["1", "lisbon"], ["3", "braga"]], "{text}");
    // The re-added `town` is NULL (empty cell) in the old file.
    assert!(text.contains("| 1  | lisbon |      |"), "{text}");
}

/// Files without field ids (imported with `add_files`) resolve through the
/// table's name mapping (ADR 0042): aliases and renames both read.
#[tokio::test]
async fn files_without_ids_resolve_through_the_name_mapping() {
    let dir = tmp("name-mapping");
    // An imported file: no field ids, its own column names.
    let schema = Arc::new(Schema::new(vec![
        Field::new("ident", DataType::Int64, false),
        Field::new("town", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["lisbon", "porto"])),
        ],
    )
    .unwrap();
    let path = dir.join("data").join("imported.parquet");
    let mut w = ArrowWriter::try_new(std::fs::File::create(&path).unwrap(), schema, None).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
    let file = format!("file://{}", path.display());
    let manifest = avro(
        &dir.join("metadata/m0.avro"),
        MANIFEST,
        vec![vec![
            ("status", Value::Int(1)),
            ("data_file", data_file(0, &file)),
        ]],
    );
    let list = avro(
        &dir.join("metadata/snap-1.avro"),
        MANIFEST_LIST,
        vec![vec![
            ("manifest_path", Value::String(manifest)),
            ("content", Value::Int(0)),
            ("sequence_number", Value::Long(1)),
        ]],
    );
    let field =
        |id, name: &str, ty: &str| json!({"id": id, "name": name, "type": ty, "required": false});
    let write = |mapping: &str| {
        let meta = json!({
            "format-version": 2, "current-schema-id": 1,
            "schemas": [
                {"schema-id": 0, "type": "struct", "fields": [field(1, "id", "long"), field(2, "town", "string")]},
                {"schema-id": 1, "type": "struct", "fields": [field(1, "id", "long"), field(2, "city", "string")]}],
            "properties": {"schema.name-mapping.default": mapping},
            "current-snapshot-id": 1,
            "snapshots": [{"snapshot-id": 1, "manifest-list": list}],
        });
        std::fs::write(dir.join("metadata/v1.metadata.json"), meta.to_string()).unwrap();
    };
    write(
        r#"[{"field-id": 1, "names": ["id", "ident"]}, {"field-id": 2, "names": ["town", "city"]}]"#,
    );
    let ctx = SessionContext::new();
    register(&ctx, &dir).await.unwrap();
    let out = ctx
        .sql("SELECT id, city FROM t WHERE city = 'porto'")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let text = datafusion::arrow::util::pretty::pretty_format_batches(&out)
        .unwrap()
        .to_string();
    assert!(text.contains("| 2  | porto |"), "{text}");
    assert_eq!(
        ids(&ctx, "SELECT id FROM t ORDER BY id").await.unwrap(),
        [1, 2]
    );

    // A mapping that sends one name to two fields is invalid metadata.
    write(r#"[{"field-id": 1, "names": ["id"]}, {"field-id": 2, "names": ["id"]}]"#);
    let err = register(&SessionContext::new(), &dir).await.unwrap_err();
    assert!(err.to_string().contains("maps to two fields"), "{err}");
}

/// Nested Iceberg types map to Arrow nested types (ADR 0041); a history
/// that renamed a nested child is refused (children resolve by name).
#[tokio::test]
async fn nested_types_map_and_nested_renames_are_accepted() {
    let nested = |b: &str| {
        json!([
            {"id": 1, "name": "id", "required": true, "type": "long"},
            {"id": 2, "name": "s", "required": false, "type": {"type": "struct", "fields": [
                {"id": 3, "name": "a", "required": false, "type": "int"},
                {"id": 4, "name": b, "required": false, "type": "string"}]}},
            {"id": 5, "name": "tags", "required": false, "type":
                {"type": "list", "element-id": 6, "element-required": true, "element": "long"}},
            {"id": 7, "name": "attrs", "required": false, "type":
                {"type": "map", "key-id": 8, "key": "string", "value-id": 9,
                 "value-required": false, "value": "int"}}])
    };
    let meta = |schemas: serde_json::Value| {
        json!({"format-version": 2, "current-schema-id": 0, "current-snapshot-id": -1,
               "schemas": schemas})
    };
    let ok = IcebergConnector::from_metadata(
        "n",
        "mem://n",
        &meta(json!([{"schema-id": 0, "type": "struct", "fields": nested("b")}])),
    )
    .unwrap();
    let schema = ok.source().schema();
    assert_eq!(
        schema.field_with_name("s").unwrap().data_type(),
        &DataType::Struct(
            vec![
                Field::new("a", DataType::Int32, true),
                Field::new("b", DataType::Utf8, true),
            ]
            .into()
        )
    );
    assert!(matches!(
        schema.field_with_name("tags").unwrap().data_type(),
        DataType::List(f) if !f.is_nullable() && f.data_type() == &DataType::Int64
    ));
    assert!(matches!(
        schema.field_with_name("attrs").unwrap().data_type(),
        DataType::Map(..)
    ));

    // A renamed nested field resolves by id in the scan (ADR 0088).
    IcebergConnector::from_metadata(
        "n",
        "mem://n",
        &meta(json!([
            {"schema-id": 0, "type": "struct", "fields": nested("bb")},
            {"schema-id": 1, "type": "struct", "fields": nested("b")}])),
    )
    .unwrap();
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

/// `$files` / `$manifests` (ADR 0032): live entries only, data and deletes,
/// read under the ceiling.
#[tokio::test]
async fn files_and_manifests_metadata_tables() {
    let dir = tmp("metafiles");
    table(&dir, Deletes::Equality, false);
    for (limit, ok) in [(64 << 20, true), (256, false)] {
        let pool = Arc::new(CeilingPool::new(limit, 0).unwrap());
        let ctx = session_context_with_ceiling(pool.clone(), None, SessionConfig::new()).unwrap();
        let c = IcebergConnector::try_new("t", &dir.display().to_string(), &ctx.state())
            .await
            .unwrap();
        for kind in ["files", "manifests", "partitions"] {
            let t = c.metadata_table(kind).unwrap().unwrap();
            ctx.register_table(format!("t${kind}"), t).unwrap();
        }
        assert!(c.metadata_table("all_files").is_none());
        let files = ctx
            .sql(
                "SELECT content, regexp_replace(file_path, '.*/', ''), sequence_number, \
                 record_count FROM \"t$files\" ORDER BY 2",
            )
            .await
            .unwrap()
            .collect()
            .await;
        let files = match files {
            Ok(b) => b,
            Err(e) => {
                assert!(
                    !ok && matches!(e.find_root(), DataFusionError::ResourcesExhausted(m) if m.contains("IcebergMetadata")),
                    "{e}"
                );
                assert_eq!(pool.used(), 0, "released after failure");
                continue;
            }
        };
        assert!(ok, "the tiny pool must refuse the manifest bytes");
        let text = datafusion::arrow::util::pretty::pretty_format_batches(&files)
            .unwrap()
            .to_string();
        // The status=2 entry (removed.parquet) is not live.
        assert!(!text.contains("removed"), "{text}");
        let rows: Vec<Vec<&str>> = text
            .lines()
            .filter(|l| l.contains(".parquet"))
            .map(|l| {
                l.split('|')
                    .map(str::trim)
                    .filter(|c| !c.is_empty())
                    .collect()
            })
            .collect();
        assert_eq!(
            rows,
            [
                ["2", "eq-deletes.parquet", "3", "3"],
                ["0", "late.parquet", "4", "3"],
                ["0", "live.parquet", "2", "3"],
            ],
            "{text}"
        );

        let manifests = ids(
            &ctx,
            "SELECT CAST(content AS BIGINT) FROM \"t$manifests\" ORDER BY sequence_number",
        )
        .await
        .unwrap();
        assert_eq!(manifests, [0, 1, 0]);
        // `$partitions` (ADR 0047): one unpartitioned row, data and delete
        // totals kept apart.
        let mut p = Vec::new();
        for c in [
            "record_count",
            "file_count",
            "equality_delete_file_count",
            "position_delete_file_count",
        ] {
            p.extend(
                ids(&ctx, &format!("SELECT {c} FROM \"t$partitions\""))
                    .await
                    .unwrap(),
            );
        }
        assert_eq!(p, [6, 2, 1, 0]);
        assert_eq!(pool.used(), 0, "everything released");
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
    // OR prunes per branch (ADR 0050): only the files holding 3 and 97.
    check(
        "SELECT count(*) AS n FROM events WHERE id = 3 OR id = 97",
        "| 2 |",
        "manifests=10/10 files=2/10",
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
    // Column statistics from the manifests (ADR 0113): the data files'
    // bounds, deleted rows included, so the live ids 1..=47 lie within
    // them; and null counts. Estimates, never exact.
    {
        use datafusion::common::stats::Precision;
        let plan = ctx
            .sql("SELECT id, name FROM mor")
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        use datafusion::physical_plan::statistics::{StatisticsArgs, StatisticsContext};
        let stats = StatisticsContext::new()
            .compute(plan.as_ref(), &StatisticsArgs::new())
            .unwrap();
        let id = &stats.column_statistics[0];
        let as_i64 = |p: &Precision<datafusion::common::ScalarValue>| match p {
            Precision::Inexact(v) => v.cast_to(&DataType::Int64).ok().and_then(|v| match v {
                datafusion::common::ScalarValue::Int64(n) => n,
                _ => None,
            }),
            other => panic!("expected an inexact bound, got {other:?}"),
        };
        let (lo, hi) = (
            as_i64(&id.min_value).unwrap(),
            as_i64(&id.max_value).unwrap(),
        );
        assert!(lo <= 1 && hi >= 47, "id bounds [{lo}, {hi}]");
        assert!(
            matches!(id.null_count, Precision::Inexact(0)),
            "{:?}",
            id.null_count
        );
        assert!(
            matches!(stats.column_statistics[1].null_count, Precision::Inexact(_)),
            "{:?}",
            stats.column_statistics[1]
        );
        // Distinct counts from the table's statistics file (ADR 0114),
        // which the fixture computes (`compute_table_stats`), and the live
        // rows (position deletes subtracted): self-joins on either column
        // are estimated at the 32 rows they return.
        assert!(
            matches!(id.distinct_count, Precision::Inexact(32)),
            "{:?}",
            id.distinct_count
        );
        assert_eq!(stats.num_rows, Precision::Inexact(32));
        for key in ["id", "name"] {
            let join = ctx
                .sql(&format!(
                    "SELECT a.id FROM mor a JOIN mor b ON a.{key} = b.{key}"
                ))
                .await
                .unwrap()
                .create_physical_plan()
                .await
                .unwrap();
            let joined = StatisticsContext::new()
                .compute(join.as_ref(), &StatisticsArgs::new())
                .unwrap()
                .num_rows;
            assert_eq!(joined, Precision::Inexact(32), "{key}");
        }
        // Filters the scan applies itself are estimated from the bounds
        // (ADR 0113): ids below 5 are a small share of [1, 47+].
        let filtered = ctx
            .sql("SELECT id FROM mor WHERE id < 5")
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        let estimate = StatisticsContext::new()
            .compute(filtered.as_ref(), &StatisticsArgs::new())
            .unwrap()
            .num_rows;
        assert!(
            matches!(estimate, Precision::Inexact(n) if n <= 8),
            "{estimate:?}"
        );
    }
    // And the join planner sees it: with a 20-row threshold for collecting
    // a side, the selective side is collected (a broadcast, distributed),
    // the unselective one is not.
    {
        let config = SessionConfig::new().with_target_partitions(4);
        let joins = session_context_with_ceiling(pool.clone(), None, config).unwrap();
        {
            let state = joins.state_ref();
            let mut state = state.write();
            let optimizer = &mut state.config_mut().options_mut().optimizer;
            optimizer.hash_join_single_partition_threshold_rows = 20;
        }
        IcebergConnector::try_new("mor", &location, &joins.state())
            .await
            .unwrap()
            .register(&joins)
            .unwrap();
        let mode = async |bound: i64| {
            let plan = joins
                .sql(&format!(
                    "SELECT a.name FROM mor a JOIN mor b ON a.id = b.id WHERE b.id < {bound}"
                ))
                .await
                .unwrap()
                .create_physical_plan()
                .await
                .unwrap();
            datafusion::physical_plan::displayable(plan.as_ref())
                .indent(true)
                .to_string()
        };
        let selective = mode(5).await;
        assert!(selective.contains("mode=CollectLeft"), "{selective}");
        let broad = mode(45).await;
        assert!(broad.contains("mode=Partitioned"), "{broad}");
    }
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
    // IN / OR / NOT prune (ADR 0050) with unchanged answers. Main has
    // three data files: day 1's insert, day 2's insert, and the file the
    // UPDATE of id 1 wrote (day 1, bounds [1, 1]). DataFusion turns IN
    // into OR.
    for (sql, expected, files) in [
        (
            "SELECT count(*) AS n FROM mor WHERE id IN (1, 2)",
            "| 2 |",
            "files=2/3",
        ),
        (
            "SELECT count(*) AS n FROM mor WHERE id IN (40, 41) OR id = 47",
            "| 3 |",
            "files=1/3",
        ),
        (
            "SELECT count(*) AS n FROM mor WHERE NOT (id <= 23)",
            "| 16 |",
            "files=1/3",
        ),
        (
            "SELECT count(*) AS n FROM mor WHERE id IN (2, 40)",
            "| 2 |",
            "files=2/3",
        ),
    ] {
        check(sql, expected).await;
        let plan = ctx
            .sql(sql)
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        let text = datafusion::physical_plan::displayable(plan.as_ref())
            .indent(true)
            .to_string();
        assert!(text.contains(files), "{sql}: {text}");
    }
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
    // `region` is an identity partition (ADR 0055): its filters are exact,
    // so no FilterExec is left for them, and a count over region b (no
    // deletes there) comes from manifests.
    let plan_of = async |sql: &str| {
        let plan = ctx
            .sql(sql)
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        datafusion::physical_plan::displayable(plan.as_ref())
            .indent(true)
            .to_string()
    };
    let plan = plan_of("SELECT id FROM eq WHERE region = 'b'").await;
    assert!(!plan.contains("FilterExec"), "{plan}");
    let plan = plan_of("SELECT count(*) FROM eq WHERE region = 'b'").await;
    assert!(!plan.contains("IcebergScan"), "{plan}");
    assert_eq!(
        ids(&ctx, "SELECT count(*) FROM eq WHERE region = 'b'")
            .await
            .unwrap(),
        [2]
    );
    // Mixed: the partition part is exact, the id part is still filtered.
    let plan = plan_of("SELECT id FROM eq WHERE region = 'b' AND id > 4").await;
    assert!(plan.contains("FilterExec"), "{plan}");
    assert_eq!(
        ids(&ctx, "SELECT id FROM eq WHERE region = 'b' AND id > 4")
            .await
            .unwrap(),
        [5]
    );
    assert_eq!(
        ids(
            &ctx,
            "SELECT id FROM eq WHERE region IN ('a', 'b') ORDER BY id"
        )
        .await
        .unwrap(),
        [2, 4, 5, 6]
    );
    assert_eq!(
        ids(&ctx, "SELECT id FROM eq WHERE region IS NULL")
            .await
            .unwrap(),
        Vec::<i64>::new()
    );
    // Region a has equality deletes: its count still scans.
    let plan = plan_of("SELECT count(*) FROM eq WHERE region = 'a'").await;
    assert!(plan.contains("IcebergScan"), "{plan}");
    assert_eq!(
        ids(&ctx, "SELECT count(*) FROM eq WHERE region = 'a'")
            .await
            .unwrap(),
        [2]
    );
    assert_eq!(pool.used(), 0);
}

/// Schema evolution written by Spark (ADR 0040): an equality delete keyed
/// on `town`, then `town` renamed to `city`, `score` promoted int -> bigint
/// and a new `town` column (new field id). Spark reads
/// (1, lisbon, 10, NULL), (3, braga, 3000000000, new), (4, faro, 40, NULL).
#[tokio::test]
#[ignore = "needs PLIMSOLL_ICEBERG_EVOLVED from scripts/spark_mor_fixture.sh"]
async fn reads_an_evolved_spark_table() {
    let Ok(location) = std::env::var("PLIMSOLL_ICEBERG_EVOLVED") else {
        return;
    };
    let pool = Arc::new(CeilingPool::new(256 << 20, 0).unwrap());
    let ctx = session_context_with_ceiling(pool.clone(), None, SessionConfig::new()).unwrap();
    IcebergConnector::try_new("ev", &location, &ctx.state())
        .await
        .unwrap()
        .register(&ctx)
        .unwrap();
    let out = ctx
        .sql("SELECT id, city, score, town FROM ev ORDER BY id")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let text = datafusion::arrow::util::pretty::pretty_format_batches(&out)
        .unwrap()
        .to_string();
    let rows: Vec<String> = text
        .lines()
        .filter(|l| l.starts_with("| ") && !l.contains("city"))
        .map(|l| {
            l.split('|')
                .map(str::trim)
                .filter(|c| !c.is_empty())
                .collect::<Vec<_>>()
                .join(",")
        })
        .collect();
    // NULL prints as an empty cell, so the row has three fields.
    assert_eq!(
        rows,
        ["1,lisbon,10", "3,braga,3000000000,new", "4,faro,40"],
        "{text}"
    );
    // Filters on renamed and promoted columns are adapted too.
    assert_eq!(
        ids(&ctx, "SELECT id FROM ev WHERE city = 'faro'")
            .await
            .unwrap(),
        [4]
    );
    assert_eq!(
        ids(&ctx, "SELECT id FROM ev WHERE score > 2147483647")
            .await
            .unwrap(),
        [3]
    );
    assert_eq!(
        ids(&ctx, "SELECT id FROM ev WHERE town IS NULL ORDER BY id")
            .await
            .unwrap(),
        [1, 4]
    );
    assert_eq!(pool.used(), 0);
}

/// Nested types written by Spark (ADR 0041): struct, list and map, with a
/// struct field `s.c` added after the first insert. Spark reads
/// (1, 1, NULL, 2, 1), (2, NULL, NULL, 0, NULL), (3, 3, new, 1, 3).
#[tokio::test]
#[ignore = "needs PLIMSOLL_ICEBERG_NESTED from scripts/spark_mor_fixture.sh"]
async fn reads_nested_spark_types() {
    let Ok(location) = std::env::var("PLIMSOLL_ICEBERG_NESTED") else {
        return;
    };
    let pool = Arc::new(CeilingPool::new(256 << 20, 0).unwrap());
    let ctx = session_context_with_ceiling(pool.clone(), None, SessionConfig::new()).unwrap();
    IcebergConnector::try_new("n", &location, &ctx.state())
        .await
        .unwrap()
        .register(&ctx)
        .unwrap();
    let out = ctx
        .sql(
            "SELECT id, s['a'] AS a, s['c'] AS c, cardinality(tags) AS n, attrs['k'] AS k \
             FROM n ORDER BY id",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let text = datafusion::arrow::util::pretty::pretty_format_batches(&out)
        .unwrap()
        .to_string();
    let rows: Vec<Vec<&str>> = text
        .lines()
        .filter(|l| l.starts_with("| ") && !l.contains(" id "))
        .map(|l| l.split('|').map(str::trim).skip(1).take(5).collect())
        .collect();
    assert_eq!(
        rows,
        [
            ["1", "1", "", "2", "1"],
            ["2", "", "", "0", ""],
            ["3", "3", "new", "1", "3"],
        ],
        "{text}"
    );
    // Filters on nested fields, and the whole struct with its new field.
    assert_eq!(
        ids(&ctx, "SELECT id FROM n WHERE s['b'] = 'z'")
            .await
            .unwrap(),
        [3]
    );
    // Nested bounds prune (ADR 0048), and null counts (ADR 0049). Spark
    // wrote three files: (1, s.a=1), (2, s NULL) and (3, s.a=3). The
    // all-NULL file matches no comparison; IS NULL keeps only it.
    assert_eq!(
        ids(&ctx, "SELECT id FROM n WHERE s['a'] = 3")
            .await
            .unwrap(),
        [3]
    );
    assert_eq!(
        ids(&ctx, "SELECT id FROM n WHERE s['a'] IS NULL")
            .await
            .unwrap(),
        [2]
    );
    for (sql, files) in [
        ("SELECT id FROM n WHERE s['a'] = 3", "files=1/3"),
        ("SELECT id FROM n WHERE s['a'] > 100", "files=0/3"),
        ("SELECT id FROM n WHERE s['a'] >= 1", "files=2/3"),
        ("SELECT id FROM n WHERE s['a'] IS NULL", "files=1/3"),
        ("SELECT id FROM n WHERE s['a'] IS NOT NULL", "files=2/3"),
    ] {
        let plan = ctx
            .sql(sql)
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        let text = datafusion::physical_plan::displayable(plan.as_ref())
            .indent(true)
            .to_string();
        assert!(text.contains(files), "{sql}: {text}");
    }
    let all = ctx
        .sql("SELECT s FROM n ORDER BY id")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let text = datafusion::arrow::util::pretty::pretty_format_batches(&all)
        .unwrap()
        .to_string();
    assert!(text.contains("{a: 1, b: x, c: }"), "{text}");
    // No deletes in this table: count(*) comes from manifests (ADR 0054).
    let plan = ctx
        .sql("SELECT count(*) FROM n")
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();
    let text = datafusion::physical_plan::displayable(plan.as_ref())
        .indent(true)
        .to_string();
    assert!(!text.contains("IcebergScan"), "{text}");
    assert_eq!(ids(&ctx, "SELECT count(*) FROM n").await.unwrap(), [3]);
    assert_eq!(pool.used(), 0);
}

/// Plain Parquet imported with Spark's `add_files` (no field ids; Iceberg
/// records a name mapping), then both columns renamed and a native insert
/// (ADR 0042). Spark reads ids 0..4 as t0..t4, plus (10, native).
#[tokio::test]
#[ignore = "needs PLIMSOLL_ICEBERG_IMPORTED from scripts/spark_mor_fixture.sh"]
async fn reads_files_imported_without_field_ids() {
    let Ok(location) = std::env::var("PLIMSOLL_ICEBERG_IMPORTED") else {
        return;
    };
    let pool = Arc::new(CeilingPool::new(256 << 20, 0).unwrap());
    let ctx = session_context_with_ceiling(pool.clone(), None, SessionConfig::new()).unwrap();
    IcebergConnector::try_new("imp", &location, &ctx.state())
        .await
        .unwrap()
        .register(&ctx)
        .unwrap();
    assert_eq!(
        ids(&ctx, "SELECT id FROM imp ORDER BY id").await.unwrap(),
        [0, 1, 2, 3, 4, 10]
    );
    assert_eq!(
        ids(&ctx, "SELECT id FROM imp WHERE city = 't3'")
            .await
            .unwrap(),
        [3]
    );
    assert_eq!(
        ids(&ctx, "SELECT id FROM imp WHERE city = 'native'")
            .await
            .unwrap(),
        [10]
    );
    assert_eq!(pool.used(), 0);
}

/// Spark table partitioned by `bucket(4, id)` and `truncate(1, code)`
/// (ADR 0051), 40 rows, code = 'a'|'b'|'c' + id. Spark places every row;
/// Plimsoll must compute the same bucket, or pruning would drop the row.
#[tokio::test]
#[ignore = "needs PLIMSOLL_ICEBERG_BUCKETED from scripts/spark_mor_fixture.sh"]
async fn prunes_spark_bucket_and_truncate_partitions() {
    let Ok(location) = std::env::var("PLIMSOLL_ICEBERG_BUCKETED") else {
        return;
    };
    let ctx = SessionContext::new();
    IcebergConnector::try_new("b", &location, &ctx.state())
        .await
        .unwrap()
        .register(&ctx)
        .unwrap();
    let scanned = async |sql: &str| {
        let plan = ctx
            .sql(sql)
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        let text = datafusion::physical_plan::displayable(plan.as_ref())
            .indent(true)
            .to_string();
        let files = text
            .split("files=")
            .nth(1)
            .unwrap()
            .split(' ')
            .next()
            .unwrap()
            .to_owned();
        let (kept, total) = files.split_once('/').unwrap();
        (
            kept.parse::<usize>().unwrap(),
            total.parse::<usize>().unwrap(),
        )
    };
    // Every id, alone: found, and only its bucket's files read.
    let (_, total) = scanned("SELECT id FROM b").await;
    assert!(total >= 4, "{total} files");
    for id in 0..40i64 {
        let sql = format!("SELECT id FROM b WHERE id = {id}");
        assert_eq!(ids(&ctx, &sql).await.unwrap(), [id], "{sql}");
        let (kept, _) = scanned(&sql).await;
        assert!(kept < total, "{sql}: {kept}/{total}");
    }
    assert_eq!(
        ids(&ctx, "SELECT id FROM b WHERE id IN (7, 23) ORDER BY id")
            .await
            .unwrap(),
        [7, 23]
    );
    // truncate(1, code): only the 'b' partitions; ranges too.
    let b = ids(
        &ctx,
        "SELECT CAST(count(*) AS BIGINT) FROM b WHERE code = 'b10'",
    )
    .await
    .unwrap();
    assert_eq!(b, [1]);
    let (kept, _) = scanned("SELECT id FROM b WHERE code = 'b10'").await;
    let (b_files, _) = scanned("SELECT id FROM b WHERE code >= 'b' AND code < 'c'").await;
    assert!(
        kept <= b_files && b_files < total,
        "{kept} {b_files} {total}"
    );
    assert_eq!(
        ids(
            &ctx,
            "SELECT CAST(count(*) AS BIGINT) FROM b WHERE code >= 'b' AND code < 'c'"
        )
        .await
        .unwrap(),
        [13]
    );
    // LIKE 'b%' is that same range (ADR 0052): same files, same answer.
    let (like_files, _) = scanned("SELECT id FROM b WHERE code LIKE 'b%'").await;
    assert_eq!(like_files, b_files);
    let n = ids(
        &ctx,
        "SELECT CAST(count(*) AS BIGINT) FROM b WHERE code LIKE 'b%'",
    )
    .await;
    assert_eq!(n.unwrap(), [13]);
}

/// Spark table with `amount DECIMAL(10, 2)` = id * 1.25, partitioned by
/// `bucket(3, amount)` and `truncate(1000, amount)` (ADR 0053). Every
/// amount looked up alone must be found: a wrong decimal hash loses it.
#[tokio::test]
#[ignore = "needs PLIMSOLL_ICEBERG_MONEY from scripts/spark_mor_fixture.sh"]
async fn prunes_spark_decimal_columns() {
    let Ok(location) = std::env::var("PLIMSOLL_ICEBERG_MONEY") else {
        return;
    };
    let ctx = SessionContext::new();
    IcebergConnector::try_new("m", &location, &ctx.state())
        .await
        .unwrap()
        .register(&ctx)
        .unwrap();
    let scanned = async |sql: &str| {
        let plan = ctx
            .sql(sql)
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        let text = datafusion::physical_plan::displayable(plan.as_ref())
            .indent(true)
            .to_string();
        let files = text
            .split("files=")
            .nth(1)
            .unwrap()
            .split(' ')
            .next()
            .unwrap()
            .to_owned();
        let (kept, total) = files.split_once('/').unwrap();
        (
            kept.parse::<usize>().unwrap(),
            total.parse::<usize>().unwrap(),
        )
    };
    for id in 0..40i64 {
        let amount = format!("{}.{:02}", id * 125 / 100, id * 125 % 100);
        let sql = format!("SELECT id FROM m WHERE amount = {amount}");
        assert_eq!(ids(&ctx, &sql).await.unwrap(), [id], "{sql}");
        let (kept, total) = scanned(&sql).await;
        assert!(kept < total, "{sql}: {kept}/{total}");
    }
    let sql = "SELECT id FROM m WHERE amount > 40.00 ORDER BY id";
    assert_eq!(ids(&ctx, sql).await.unwrap(), (33..40).collect::<Vec<_>>());
    let (kept, total) = scanned(sql).await;
    assert!(kept < total, "{kept}/{total}");
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

/// Manifest lists and manifests are immutable, so their bytes are cached
/// by path (ADR 0039): planning the same snapshot again reads nothing.
#[tokio::test]
async fn replanning_a_snapshot_reads_no_manifests() {
    use datafusion::execution::object_store::ObjectStoreUrl;
    use datafusion::object_store::memory::InMemory;
    use datafusion::object_store::path::Path as StorePath;
    use datafusion::object_store::throttle::{ThrottleConfig, ThrottledStore};
    use datafusion::object_store::{ObjectStoreExt, PutPayload};
    use std::time::{Duration, Instant};

    let dir = tmp("manifest-cache");
    many_manifests(&dir, 8);
    let files = InMemory::new();
    for entry in std::fs::read_dir(dir.join("metadata")).unwrap() {
        let path = entry.unwrap().path();
        let key = StorePath::from_absolute_path(&path).unwrap();
        let bytes = std::fs::read(&path).unwrap();
        files.put(&key, PutPayload::from(bytes)).await.unwrap();
    }
    // Under the ceiling with a cache reserve, as the server runs.
    let pool = Arc::new(
        CeilingPool::new(64 << 20, 0)
            .unwrap()
            .with_cache_reserve(4 << 20)
            .unwrap(),
    );
    let ctx = session_context_with_ceiling(pool.clone(), None, SessionConfig::new()).unwrap();
    register(&ctx, &dir).await.unwrap();
    // Registration read the metadata JSON from disk; manifests now come
    // from a store where every GET costs 50 ms.
    let slow = ThrottleConfig {
        wait_get_per_call: Duration::from_millis(50),
        ..Default::default()
    };
    ctx.runtime_env().register_object_store(
        ObjectStoreUrl::local_filesystem().as_ref(),
        Arc::new(ThrottledStore::new(Arc::new(files), slow)),
    );
    let plan = async || {
        let started = Instant::now();
        let physical = ctx
            .sql("SELECT sum(id) FROM t")
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        let text = datafusion::physical_plan::displayable(physical.as_ref())
            .indent(true)
            .to_string();
        (started.elapsed(), text)
    };
    let (cold, text) = plan().await;
    assert!(text.contains("manifest_cache_hits=0"), "{text}");
    let (warm, text) = plan().await;
    // The list and all 8 manifests: no GET at all.
    assert!(text.contains("manifest_cache_hits=9"), "{text}");
    assert!(cold >= Duration::from_millis(100), "{cold:?}");
    assert!(
        warm < Duration::from_millis(50),
        "{warm:?} vs cold {cold:?}"
    );
    assert_eq!(pool.used(), 0, "cached bytes sit in the cache reserve");
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
    // Metadata tables (ADR 0030) list what Spark wrote.
    let meta_table = |kind| {
        let b = plimsoll_connectors::iceberg::meta::table(&meta, kind)
            .unwrap()
            .unwrap();
        datafusion::arrow::util::pretty::pretty_format_batches(&[b])
            .unwrap()
            .to_string()
    };
    let refs = meta_table("refs");
    for row in [
        format!("| after_insert | TAG    | {tag} "),
        format!("| audit        | BRANCH | {audit} "),
        format!("| main         | BRANCH | {main} "),
    ] {
        assert!(refs.contains(&row), "{row} in\n{refs}");
    }
    let snaps = plimsoll_connectors::iceberg::meta::table(&meta, "snapshots")
        .unwrap()
        .unwrap();
    assert_eq!(
        snaps.num_rows(),
        meta["snapshots"].as_array().unwrap().len()
    );
    assert_eq!(
        snaps.column(3).null_count(),
        0,
        "every snapshot has an operation"
    );
    assert!(meta_table("history").contains("| true "));
    // `$files` / `$manifests` (ADR 0032) of main: Spark's MOR DELETE wrote
    // position delete files next to untouched data files.
    let pool = Arc::new(CeilingPool::new(256 << 20, 0).unwrap());
    let ctx = session_context_with_ceiling(pool.clone(), None, SessionConfig::new()).unwrap();
    let c = IcebergConnector::from_metadata("t", &path, &meta).unwrap();
    for kind in ["files", "manifests", "partitions"] {
        ctx.register_table(
            format!("t${kind}"),
            c.metadata_table(kind).unwrap().unwrap(),
        )
        .unwrap();
    }
    // `$partitions` (ADR 0047) totals agree with `$files`: 49 data rows in
    // 3 files, 17 position-deleted rows, over Spark's daily partitions.
    let mut totals = Vec::new();
    for c in ["record_count", "file_count", "position_delete_record_count"] {
        let q = format!("SELECT sum({c}) FROM \"t$partitions\"");
        totals.extend(ids(&ctx, &q).await.unwrap());
    }
    assert_eq!(totals, [49, 3, 17]);
    let parts = ids(
        &ctx,
        "SELECT CAST(count(*) AS BIGINT) FROM \"t$partitions\"",
    )
    .await
    .unwrap();
    assert!(parts[0] >= 2, "Spark's daily partitions: {parts:?}");
    let per_content = ctx
        .sql(
            "SELECT content, count(*), sum(record_count) FROM \"t$files\" \
             GROUP BY content ORDER BY content",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let text = datafusion::arrow::util::pretty::pretty_format_batches(&per_content)
        .unwrap()
        .to_string();
    // Data rows minus position-deleted rows = main's 32 live rows.
    assert!(text.contains("| 0       | 3        | 49 "), "{text}");
    assert!(text.contains("| 1       | 2        | 17 "), "{text}");
    let manifests = ctx
        .sql("SELECT content, count(*) FROM \"t$manifests\" GROUP BY content ORDER BY content")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let manifests = datafusion::arrow::util::pretty::pretty_format_batches(&manifests)
        .unwrap()
        .to_string();
    assert!(manifests.contains("| 0       | 2 "), "{manifests}");
    assert!(manifests.contains("| 1       | 2 "), "{manifests}");
    assert_eq!(pool.used(), 0);
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

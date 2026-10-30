//! Iceberg metadata tables (ADR 0030): `table$snapshots`, `table$history`,
//! `table$refs`, built from the `metadata.json` already loaded for a query.
//! Column names follow Spark's metadata tables, so queries port between
//! engines.

use std::collections::HashSet;
use std::sync::Arc;

use datafusion::arrow::array::{
    ArrayRef, BooleanArray, Int32Array, Int64Array, RecordBatch, StringArray,
    TimestampMillisecondArray,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use datafusion::error::Result;
use serde_json::Value as Json;

/// The metadata table named by `kind`, or `None` when `kind` is not one.
pub fn table(metadata: &Json, kind: &str) -> Option<Result<RecordBatch>> {
    Some(match kind {
        "snapshots" => snapshots(metadata),
        "history" => history(metadata),
        "refs" => refs(metadata),
        _ => return None,
    })
}

fn rows(metadata: &Json, key: &str) -> Vec<Json> {
    metadata[key].as_array().cloned().unwrap_or_default()
}

fn ts() -> DataType {
    DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into()))
}

fn batch(fields: Vec<Field>, columns: Vec<ArrayRef>) -> Result<RecordBatch> {
    Ok(RecordBatch::try_new(
        Arc::new(Schema::new(fields)),
        columns,
    )?)
}

fn millis(values: Vec<Option<i64>>) -> ArrayRef {
    Arc::new(TimestampMillisecondArray::from(values).with_timezone("UTC"))
}

fn snapshots(metadata: &Json) -> Result<RecordBatch> {
    let s = rows(metadata, "snapshots");
    let col = |k: &str| s.iter().map(|s| s[k].as_i64()).collect::<Vec<_>>();
    let text = |f: &dyn Fn(&Json) -> Option<String>| {
        Arc::new(s.iter().map(f).collect::<StringArray>()) as ArrayRef
    };
    batch(
        vec![
            Field::new("committed_at", ts(), true),
            Field::new("snapshot_id", DataType::Int64, true),
            Field::new("parent_id", DataType::Int64, true),
            Field::new("operation", DataType::Utf8, true),
            Field::new("manifest_list", DataType::Utf8, true),
            Field::new("schema_id", DataType::Int32, true),
            Field::new("summary", DataType::Utf8, true),
        ],
        vec![
            millis(col("timestamp-ms")),
            Arc::new(Int64Array::from(col("snapshot-id"))),
            Arc::new(Int64Array::from(col("parent-snapshot-id"))),
            text(&|s| s["summary"]["operation"].as_str().map(str::to_owned)),
            text(&|s| s["manifest-list"].as_str().map(str::to_owned)),
            Arc::new(Int32Array::from(
                col("schema-id")
                    .into_iter()
                    .map(|v| v.and_then(|v| i32::try_from(v).ok()))
                    .collect::<Vec<_>>(),
            )),
            // ponytail: JSON text, not a MAP column; query it with string
            // functions. Upgrade to MapArray if users need map access.
            text(&|s| s.get("summary").map(Json::to_string)),
        ],
    )
}

fn history(metadata: &Json) -> Result<RecordBatch> {
    let log = rows(metadata, "snapshot-log");
    let snapshots = rows(metadata, "snapshots");
    let parent = |id: i64| {
        snapshots
            .iter()
            .find(|s| s["snapshot-id"].as_i64() == Some(id))
            .and_then(|s| s["parent-snapshot-id"].as_i64())
    };
    // Ancestors of the current snapshot: entries outside this chain were
    // rolled back (Spark's `is_current_ancestor`). Bounded by the snapshot
    // count, so a corrupt parent cycle cannot loop forever.
    let mut ancestors = HashSet::new();
    let mut next = metadata["current-snapshot-id"].as_i64();
    while let Some(id) = next {
        if !ancestors.insert(id) || ancestors.len() > snapshots.len() {
            break;
        }
        next = parent(id);
    }
    let ids: Vec<_> = log.iter().map(|e| e["snapshot-id"].as_i64()).collect();
    batch(
        vec![
            Field::new("made_current_at", ts(), true),
            Field::new("snapshot_id", DataType::Int64, true),
            Field::new("parent_id", DataType::Int64, true),
            Field::new("is_current_ancestor", DataType::Boolean, false),
        ],
        vec![
            millis(log.iter().map(|e| e["timestamp-ms"].as_i64()).collect()),
            Arc::new(Int64Array::from(ids.clone())),
            Arc::new(Int64Array::from(
                ids.iter().map(|id| id.and_then(parent)).collect::<Vec<_>>(),
            )),
            Arc::new(BooleanArray::from(
                ids.iter()
                    .map(|id| id.is_some_and(|id| ancestors.contains(&id)))
                    .collect::<Vec<_>>(),
            )),
        ],
    )
}

fn refs(metadata: &Json) -> Result<RecordBatch> {
    let mut refs: Vec<(&String, &Json)> = metadata["refs"]
        .as_object()
        .map(|m| m.iter().collect())
        .unwrap_or_default();
    refs.sort_by_key(|(name, _)| *name);
    let int = |k: &str| refs.iter().map(|(_, r)| r[k].as_i64()).collect::<Vec<_>>();
    batch(
        vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("type", DataType::Utf8, true),
            Field::new("snapshot_id", DataType::Int64, true),
            Field::new("max_reference_age_in_ms", DataType::Int64, true),
            Field::new("min_snapshots_to_keep", DataType::Int64, true),
            Field::new("max_snapshot_age_in_ms", DataType::Int64, true),
        ],
        vec![
            Arc::new(StringArray::from_iter_values(
                refs.iter().map(|(n, _)| n.as_str()),
            )),
            Arc::new(
                refs.iter()
                    .map(|(_, r)| r["type"].as_str().map(str::to_uppercase))
                    .collect::<StringArray>(),
            ),
            Arc::new(Int64Array::from(int("snapshot-id"))),
            Arc::new(Int64Array::from(int("max-ref-age-ms"))),
            Arc::new(Int64Array::from(int("min-snapshots-to-keep"))),
            Arc::new(Int64Array::from(int("max-snapshot-age-ms"))),
        ],
    )
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use datafusion::arrow::util::pretty::pretty_format_batches;
    use serde_json::json;

    #[test]
    fn metadata_tables_list_snapshots_history_and_refs() {
        // 1 -> 2 -> 3 on main, then rolled back to 2; 4 is a branch commit.
        let m = json!({
            "current-snapshot-id": 2,
            "snapshots": [
                {"snapshot-id": 1, "timestamp-ms": 1000, "schema-id": 0, "manifest-list": "l1",
                 "summary": {"operation": "append", "added-records": "3"}},
                {"snapshot-id": 2, "parent-snapshot-id": 1, "timestamp-ms": 2000, "manifest-list": "l2",
                 "summary": {"operation": "delete"}},
                {"snapshot-id": 3, "parent-snapshot-id": 2, "timestamp-ms": 3000, "manifest-list": "l3",
                 "summary": {"operation": "overwrite"}},
                {"snapshot-id": 4, "parent-snapshot-id": 2, "timestamp-ms": 4000, "manifest-list": "l4",
                 "summary": {"operation": "append"}}],
            "snapshot-log": [
                {"snapshot-id": 1, "timestamp-ms": 1000},
                {"snapshot-id": 2, "timestamp-ms": 2000},
                {"snapshot-id": 3, "timestamp-ms": 3000},
                {"snapshot-id": 2, "timestamp-ms": 5000}],
            "refs": {"main": {"snapshot-id": 2, "type": "branch"},
                     "audit": {"snapshot-id": 4, "type": "branch", "min-snapshots-to-keep": 2},
                     "v1": {"snapshot-id": 1, "type": "tag", "max-ref-age-ms": 86400000}}
        });
        let show = |kind| {
            pretty_format_batches(&[table(&m, kind).unwrap().unwrap()])
                .unwrap()
                .to_string()
        };

        let snaps = show("snapshots");
        assert!(
            snaps.contains("| 1970-01-01T00:00:01Z | 1           |           | append"),
            "{snaps}"
        );
        assert!(
            snaps.contains(r#"{"added-records":"3","operation":"append"}"#),
            "{snaps}"
        );
        assert_eq!(table(&m, "snapshots").unwrap().unwrap().num_rows(), 4);

        let hist = table(&m, "history").unwrap().unwrap();
        let anc = hist
            .column(3)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        // Snapshot 3 was rolled back; the rest are ancestors of current (2).
        assert_eq!(
            anc.iter().collect::<Vec<_>>(),
            [Some(true), Some(true), Some(false), Some(true)]
        );

        let refs = show("refs");
        let lines: Vec<_> = refs
            .lines()
            .filter(|l| l.starts_with("| ") && !l.contains("name"))
            .collect();
        assert!(lines[0].starts_with("| audit | BRANCH | 4 "), "{refs}");
        assert!(
            lines[2].contains("| v1    | TAG    | 1           | 86400000"),
            "{refs}"
        );

        assert!(table(&m, "files").is_none());
        // A table with no history yet: empty, not an error.
        assert_eq!(table(&json!({}), "history").unwrap().unwrap().num_rows(), 0);
    }
}

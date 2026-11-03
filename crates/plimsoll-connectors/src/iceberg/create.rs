//! A REST `CreateTableRequest` from an Arrow schema (ADR 0083).

use datafusion::arrow::datatypes::{DataType, Schema};
use serde_json::{Map, Value, json};

use crate::ConnectorError;

/// First partition field id, as Iceberg assigns them.
const FIRST_PARTITION_FIELD: usize = 1000;

/// The request creating table `name` with `schema` (field ids 1…n, `NOT
/// NULL` as `required`), partitioned by `(transform, column)` pairs and
/// with `properties`. Format version 2, the one Plimsoll writes.
pub fn create_request(
    name: &str,
    schema: &Schema,
    partition: &[(String, String)],
    properties: &[(String, String)],
) -> Result<Value, ConnectorError> {
    let unsupported = |m: String| ConnectorError::Unsupported(m);
    let fields = schema
        .fields()
        .iter()
        .enumerate()
        .map(|(i, f)| {
            let ty = iceberg_type(f.data_type()).ok_or_else(|| {
                unsupported(format!(
                    "column {}: type {} for Iceberg",
                    f.name(),
                    f.data_type()
                ))
            })?;
            Ok(json!({"id": i + 1, "name": f.name(), "required": !f.is_nullable(), "type": ty}))
        })
        .collect::<Result<Vec<_>, ConnectorError>>()?;
    let spec = partition
        .iter()
        .enumerate()
        .map(|(i, (transform, column))| {
            let source = schema
                .index_of(column)
                .map_err(|_| unsupported(format!("PARTITION BY: no column {column}")))?;
            let name = partition_name(transform, column);
            Ok(json!({
                "source-id": source + 1,
                "field-id": FIRST_PARTITION_FIELD + i,
                "name": name,
                "transform": transform,
            }))
        })
        .collect::<Result<Vec<_>, ConnectorError>>()?;
    let mut props: Map<String, Value> = properties
        .iter()
        .map(|(k, v)| (k.clone(), Value::String(v.clone())))
        .collect();
    match props.get("format-version").and_then(Value::as_str) {
        None | Some("2") => {}
        Some(v) => {
            return Err(unsupported(format!(
                "format-version {v}: Plimsoll writes version 2"
            )));
        }
    }
    props.insert("format-version".into(), "2".into());
    Ok(json!({
        "name": name,
        "schema": {"type": "struct", "schema-id": 0, "fields": fields},
        "partition-spec": {"spec-id": 0, "fields": spec},
        "properties": props,
    }))
}

/// Iceberg's own partition field names: `region`, `id_bucket`, `ts_day`.
pub(super) fn partition_name(transform: &str, column: &str) -> String {
    match transform.split('[').next().unwrap_or(transform) {
        "identity" => column.to_owned(),
        "truncate" => format!("{column}_trunc"),
        b => format!("{column}_{b}"),
    }
}

/// ponytail: primitives only; nested types when the writer takes them.
pub(super) fn iceberg_type(t: &DataType) -> Option<String> {
    Some(match t {
        DataType::Boolean => "boolean".into(),
        DataType::Int8 | DataType::Int16 | DataType::Int32 => "int".into(),
        DataType::Int64 => "long".into(),
        DataType::Float32 => "float".into(),
        DataType::Float64 => "double".into(),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => "string".into(),
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView => "binary".into(),
        DataType::FixedSizeBinary(n) => format!("fixed[{n}]"),
        DataType::Date32 => "date".into(),
        DataType::Time64(_) => "time".into(),
        // v2 timestamps are microseconds; writes cast to them.
        DataType::Timestamp(_, None) => "timestamp".into(),
        DataType::Timestamp(_, Some(_)) => "timestamptz".into(),
        DataType::Decimal128(p, s) if *p <= 38 && *s >= 0 => format!("decimal({p}, {s})"),
        _ => return None,
    })
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{Field, TimeUnit};

    #[test]
    fn maps_types_partitions_and_properties() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("region", DataType::Utf8, true),
            Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, None), true),
            Field::new("amount", DataType::Decimal128(10, 2), true),
        ]);
        let partition = [
            ("identity".into(), "region".into()),
            ("bucket[16]".into(), "id".into()),
            ("day".into(), "ts".into()),
        ];
        let r = create_request("t", &schema, &partition, &[("k".into(), "v".into())]).unwrap();
        assert_eq!(
            r["schema"]["fields"][0],
            json!({"id": 1, "name": "id", "required": true, "type": "long"})
        );
        assert_eq!(r["schema"]["fields"][2]["type"], "timestamp");
        assert_eq!(r["schema"]["fields"][3]["type"], "decimal(10, 2)");
        assert_eq!(
            r["partition-spec"]["fields"][1],
            json!({"source-id": 1, "field-id": 1001, "name": "id_bucket", "transform": "bucket[16]"})
        );
        assert_eq!(r["partition-spec"]["fields"][2]["name"], "ts_day");
        assert_eq!(r["properties"], json!({"k": "v", "format-version": "2"}));
        // Unknown column, version 3, nested type: refused.
        let bad_column = [("identity".into(), "nope".into())];
        assert!(create_request("t", &schema, &bad_column, &[]).is_err());
        let v3 = [("format-version".into(), "3".into())];
        assert!(create_request("t", &schema, &[], &v3).is_err());
        let nested = Schema::new(vec![Field::new(
            "l",
            DataType::new_list(DataType::Int32, true),
            true,
        )]);
        assert!(create_request("t", &nested, &[], &[]).is_err());
    }
}

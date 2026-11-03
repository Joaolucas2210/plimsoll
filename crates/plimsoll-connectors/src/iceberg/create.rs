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
    // Top-level columns 1…n, then nested fields after them, as Iceberg
    // assigns ids.
    let mut next = schema.fields().len() as i64;
    let fields = schema
        .fields()
        .iter()
        .enumerate()
        .map(|(i, f)| {
            let ty = field_type(f.data_type(), &mut next).ok_or_else(|| {
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

/// An Arrow type as an Iceberg type: a primitive's name, or a `struct`,
/// `list` or `map` object (ADR 0087) whose fields, element, key and value
/// take ids after `next`, which is left at the last id used.
pub(super) fn field_type(t: &DataType, next: &mut i64) -> Option<Value> {
    let mut id = || {
        *next += 1;
        *next
    };
    Some(match t {
        DataType::Struct(fields) => {
            // A struct's own fields first, then what they contain.
            let ids: Vec<i64> = fields.iter().map(|_| id()).collect();
            let fields = fields
                .iter()
                .zip(ids)
                .map(|(f, id)| {
                    Some(
                        json!({"id": id, "name": f.name(), "required": !f.is_nullable(),
                                "type": field_type(f.data_type(), next)?}),
                    )
                })
                .collect::<Option<Vec<_>>>()?;
            json!({"type": "struct", "fields": fields})
        }
        DataType::List(e) | DataType::LargeList(e) => {
            let element_id = id();
            json!({"type": "list", "element-id": element_id,
                   "element-required": !e.is_nullable(),
                   "element": field_type(e.data_type(), next)?})
        }
        DataType::Map(entries, _) => {
            let DataType::Struct(kv) = entries.data_type() else {
                return None;
            };
            let (key_id, value_id) = (id(), id());
            json!({"type": "map", "key-id": key_id, "value-id": value_id,
                   "key": field_type(kv.first()?.data_type(), next)?,
                   "value": field_type(kv.get(1)?.data_type(), next)?,
                   "value-required": !kv.get(1)?.is_nullable()})
        }
        t => Value::String(iceberg_type(t)?),
    })
}

/// A primitive's Iceberg name. nested types when the writer takes them.
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
        // Nested: top-level ids first, then a struct's fields, a list's
        // element, a map's key and value.
        let nested = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(
                "s",
                DataType::Struct(
                    vec![
                        Field::new("a", DataType::Int32, true),
                        Field::new("tags", DataType::new_list(DataType::Utf8, true), true),
                    ]
                    .into(),
                ),
                true,
            ),
            Field::new_map(
                "m",
                "entries",
                Field::new("key", DataType::Utf8, false),
                Field::new("value", DataType::Int64, true),
                false,
                true,
            ),
        ]);
        let n = create_request("n", &nested, &[], &[]).unwrap();
        assert_eq!(
            n["schema"]["fields"][1]["type"],
            json!({"type": "struct", "fields": [
                {"id": 4, "name": "a", "required": false, "type": "int"},
                {"id": 5, "name": "tags", "required": false, "type":
                    {"type": "list", "element-id": 6, "element-required": false, "element": "string"}},
            ]})
        );
        assert_eq!(
            n["schema"]["fields"][2]["type"],
            json!({"type": "map", "key-id": 7, "value-id": 8, "key": "string", "value": "long",
                   "value-required": false})
        );
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
        let unsupported = Schema::new(vec![Field::new("u", DataType::UInt64, true)]);
        assert!(create_request("t", &unsupported, &[], &[]).is_err());
    }
}

//! `ALTER TABLE` as REST `updateTable` changes (ADR 0084).

use std::collections::HashSet;

use datafusion::error::{DataFusionError, Result};
use plimsoll_core::ddl::Alteration;
use serde_json::{Map, Value, json};

use super::create::{field_type, iceberg_type, partition_name};
use crate::ConnectorError;

/// The requirements and updates applying `changes`, in order, to the
/// table `meta` describes; `None` when they change nothing (`IF EXISTS`,
/// `IF NOT EXISTS`, a type already set).
///
/// Schema changes add a schema (fresh field ids for new columns, the
/// others keep theirs, so data files written before still resolve by id,
/// ADR 0040) and make it current, requiring the current schema and last
/// column id as seen.
///
/// Partition changes (ADR 0085) make a new default spec: an existing one
/// with the same fields, or a new one. A field that existed in an older
/// spec keeps its field id; files already written keep their spec.
#[allow(clippy::type_complexity)]
pub fn alter_updates(
    meta: &Value,
    changes: &[Alteration],
) -> Result<Option<(Vec<Value>, Vec<Value>)>> {
    let invalid =
        |m: &str| DataFusionError::External(Box::new(ConnectorError::InvalidMetadata(m.into())));
    let current = meta["current-schema-id"]
        .as_i64()
        .ok_or_else(|| invalid("no current-schema-id"))?;
    let schemas = meta["schemas"]
        .as_array()
        .ok_or_else(|| invalid("no schemas"))?;
    let schema = schemas
        .iter()
        .find(|s| s["schema-id"].as_i64() == Some(current))
        .ok_or_else(|| invalid("current schema missing"))?;
    let mut fields = schema["fields"]
        .as_array()
        .ok_or_else(|| invalid("schema without fields"))?
        .clone();
    let last_seen = meta["last-column-id"]
        .as_i64()
        .ok_or_else(|| invalid("no last-column-id"))?;
    let mut last = last_seen;
    let default_spec = meta["default-spec-id"].as_i64().unwrap_or(0);
    let partitioned: HashSet<i64> = meta["partition-specs"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|s| s["spec-id"].as_i64() == Some(default_spec))
        .flat_map(|s| s["fields"].as_array().into_iter().flatten())
        .filter_map(|f| f["source-id"].as_i64())
        .collect();
    let specs = meta["partition-specs"]
        .as_array()
        .ok_or_else(|| invalid("no partition-specs"))?;
    let mut spec_fields: Vec<Value> = specs
        .iter()
        .find(|s| s["spec-id"].as_i64() == Some(default_spec))
        .and_then(|s| s["fields"].as_array())
        .cloned()
        .unwrap_or_default();
    let last_partition_seen = meta["last-partition-id"].as_i64().unwrap_or(999);
    let mut last_partition = last_partition_seen;
    let mut respecced = false;
    let identifier: HashSet<i64> = schema["identifier-field-ids"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(Value::as_i64)
        .collect();
    let plan = |m: String| Err(DataFusionError::Plan(m));
    let pos = |fields: &[Value], name: &str| fields.iter().position(|f| f["name"] == name);
    let (mut changed, mut properties) = (false, Map::new());
    for change in changes {
        match change {
            Alteration::AddColumn {
                field,
                if_not_exists,
            } => {
                if pos(&fields, field.name()).is_some() {
                    if *if_not_exists {
                        continue;
                    }
                    return plan(format!("column {} already exists", field.name()));
                }
                last += 1;
                let id = last;
                let Some(ty) = field_type(field.data_type(), &mut last) else {
                    return plan(format!(
                        "column {}: type {} for Iceberg",
                        field.name(),
                        field.data_type()
                    ));
                };
                fields.push(json!({"id": id, "name": field.name(), "required": false, "type": ty}));
            }
            Alteration::DropColumn { name, if_exists } => {
                let Some(i) = pos(&fields, name) else {
                    if *if_exists {
                        continue;
                    }
                    return plan(format!("no column {name}"));
                };
                let id = fields[i]["id"].as_i64().unwrap_or(-1);
                if partitioned.contains(&id) {
                    return plan(format!("column {name} is a source of the partition spec"));
                }
                if identifier.contains(&id) {
                    return plan(format!("column {name} is an identifier field"));
                }
                if fields.len() == 1 {
                    return plan(format!("column {name} is the table's last"));
                }
                fields.remove(i);
            }
            Alteration::RenameColumn { from, to } => {
                let Some(i) = pos(&fields, from) else {
                    return plan(format!("no column {from}"));
                };
                if pos(&fields, to).is_some() {
                    return plan(format!("column {to} already exists"));
                }
                fields[i]["name"] = json!(to);
            }
            Alteration::DropNotNull(name) => {
                let Some(i) = pos(&fields, name) else {
                    return plan(format!("no column {name}"));
                };
                if identifier.contains(&fields[i]["id"].as_i64().unwrap_or(-1)) {
                    return plan(format!("column {name} is an identifier field"));
                }
                if fields[i]["required"] != true {
                    continue;
                }
                fields[i]["required"] = json!(false);
            }
            Alteration::SetType { column, data_type } => {
                let Some(i) = pos(&fields, column) else {
                    return plan(format!("no column {column}"));
                };
                let from = fields[i]["type"].as_str().unwrap_or("nested").to_owned();
                let Some(to) = iceberg_type(data_type) else {
                    return plan(format!("column {column}: type {data_type} for Iceberg"));
                };
                if from == to {
                    continue;
                }
                if !promotes(&from, &to) {
                    return plan(format!(
                        "column {column}: {from} cannot become {to} (Iceberg promotes int to \
                         long, float to double and decimals to a wider precision)"
                    ));
                }
                fields[i]["type"] = json!(to);
            }
            Alteration::SetProperties(kv) => {
                for (k, v) in kv {
                    if k == "format-version" {
                        return plan("format-version cannot be changed".into());
                    }
                    properties.insert(k.clone(), json!(v));
                }
                continue;
            }
            Alteration::AddNestedField { parent, field } => {
                let Some(children) = struct_at(&mut fields, parent) else {
                    return plan(format!("{} is not a struct", parent.join(".")));
                };
                if children.iter().any(|f| f["name"] == field.name().as_str()) {
                    return plan(format!(
                        "field {}.{} already exists",
                        parent.join("."),
                        field.name()
                    ));
                }
                last += 1;
                let id = last;
                let Some(ty) = field_type(field.data_type(), &mut last) else {
                    return plan(format!("type {} for Iceberg", field.data_type()));
                };
                children
                    .push(json!({"id": id, "name": field.name(), "required": false, "type": ty}));
            }
            Alteration::RenameNestedField { path, to } => {
                let Some((leaf, parent)) = path.split_last() else {
                    return plan("an empty field path".into());
                };
                let Some(children) = struct_at(&mut fields, parent) else {
                    return plan(format!("{} is not a struct", parent.join(".")));
                };
                if children.iter().any(|f| f["name"] == to.as_str()) {
                    return plan(format!("field {}.{to} already exists", parent.join(".")));
                }
                let Some(f) = children.iter_mut().find(|f| f["name"] == leaf.as_str()) else {
                    return plan(format!("no field {}", path.join(".")));
                };
                f["name"] = json!(to);
            }
            Alteration::DropNestedField { path } => {
                let Some((leaf, parent)) = path.split_last() else {
                    return plan("an empty field path".into());
                };
                let Some(children) = struct_at(&mut fields, parent) else {
                    return plan(format!("{} is not a struct", parent.join(".")));
                };
                let Some(i) = children.iter().position(|f| f["name"] == leaf.as_str()) else {
                    return plan(format!("no field {}", path.join(".")));
                };
                let id = children[i]["id"].as_i64().unwrap_or(-1);
                if partitioned.contains(&id) || identifier.contains(&id) {
                    return plan(format!(
                        "field {} is a partition source or identifier field",
                        path.join(".")
                    ));
                }
                if children.len() == 1 {
                    return plan(format!("field {} is its struct's last", path.join(".")));
                }
                children.remove(i);
            }
            Alteration::AddPartitionField {
                transform,
                column,
                name,
            } => {
                let Some(source) = pos(&fields, column).and_then(|i| fields[i]["id"].as_i64())
                else {
                    return plan(format!("no column {column}"));
                };
                let same = |f: &Value| f["source-id"] == source && f["transform"] == *transform;
                if spec_fields.iter().any(same) {
                    return plan(format!(
                        "{transform}({column}) is already a partition field"
                    ));
                }
                let name = name
                    .clone()
                    .unwrap_or_else(|| partition_name(transform, column));
                if spec_fields.iter().any(|f| f["name"] == name) {
                    return plan(format!("partition field {name} already exists"));
                }
                // Reused from an older spec, as Iceberg does.
                let id = specs
                    .iter()
                    .flat_map(|s| s["fields"].as_array().into_iter().flatten())
                    .find(|f| same(f))
                    .and_then(|f| f["field-id"].as_i64())
                    .unwrap_or_else(|| {
                        last_partition += 1;
                        last_partition
                    });
                spec_fields.push(json!({
                    "source-id": source, "field-id": id, "name": name, "transform": transform,
                }));
                respecced = true;
                continue;
            }
            Alteration::DropPartitionField { transform, column } => {
                let source = pos(&fields, column).and_then(|i| fields[i]["id"].as_i64());
                let by_name = |f: &Value| transform == "identity" && f["name"] == *column;
                let by_source = |f: &Value| {
                    source.is_some_and(|s| f["source-id"] == s) && f["transform"] == *transform
                };
                let Some(i) = spec_fields
                    .iter()
                    .position(by_name)
                    .or_else(|| spec_fields.iter().position(by_source))
                else {
                    return plan(format!("no partition field {transform}({column})"));
                };
                spec_fields.remove(i);
                respecced = true;
                continue;
            }
        }
        changed = true;
    }
    let mut requirements = vec![json!({"type": "assert-table-uuid", "uuid": meta["table-uuid"]})];
    let mut updates = Vec::new();
    if changed {
        let id = schemas
            .iter()
            .filter_map(|s| s["schema-id"].as_i64())
            .max()
            .unwrap_or(current)
            + 1;
        let mut schema = schema.clone();
        schema["schema-id"] = json!(id);
        schema["fields"] = Value::Array(fields);
        requirements
            .push(json!({"type": "assert-current-schema-id", "current-schema-id": current}));
        requirements.push(
            json!({"type": "assert-last-assigned-field-id", "last-assigned-field-id": last_seen}),
        );
        updates.push(json!({"action": "add-schema", "schema": schema, "last-column-id": last}));
        updates.push(json!({"action": "set-current-schema", "schema-id": -1}));
    }
    if !properties.is_empty() {
        updates.push(json!({"action": "set-properties", "updates": properties}));
    }
    if respecced {
        let key = |fields: &[Value]| -> Vec<(Value, Value, Value)> {
            fields
                .iter()
                .map(|f| {
                    (
                        f["source-id"].clone(),
                        f["transform"].clone(),
                        f["field-id"].clone(),
                    )
                })
                .collect()
        };
        let existing = specs.iter().find(|s| {
            s["fields"]
                .as_array()
                .is_some_and(|f| key(f) == key(&spec_fields))
        });
        let id = match existing {
            Some(s) => s["spec-id"].as_i64().unwrap_or(0),
            None => {
                specs
                    .iter()
                    .filter_map(|s| s["spec-id"].as_i64())
                    .max()
                    .unwrap_or(0)
                    + 1
            }
        };
        // Only specs Plimsoll can write: a transform it does not take, or
        // one that does not apply to the column, is refused here rather
        // than at the next INSERT.
        let mut probe = meta.clone();
        if existing.is_none() {
            if let Some(all) = probe["partition-specs"].as_array_mut() {
                all.push(json!({"spec-id": id, "fields": spec_fields}));
            }
        }
        super::write::Layout::of_spec(&probe, id).map_err(DataFusionError::Plan)?;
        requirements
            .push(json!({"type": "assert-default-spec-id", "default-spec-id": default_spec}));
        if existing.is_none() {
            requirements.push(json!({
                "type": "assert-last-assigned-partition-id",
                "last-assigned-partition-id": last_partition_seen,
            }));
            updates.push(
                json!({"action": "add-spec", "spec": {"spec-id": id, "fields": spec_fields}}),
            );
            updates.push(json!({"action": "set-default-spec", "spec-id": -1}));
        } else if id != default_spec {
            updates.push(json!({"action": "set-default-spec", "spec-id": id}));
        }
    }
    Ok((!updates.is_empty()).then_some((requirements, updates)))
}

/// The fields of the struct at `path` (a top-level column, then struct
/// field names, `element` of a list, `key` / `value` of a map).
fn struct_at<'a>(fields: &'a mut [Value], path: &[String]) -> Option<&'a mut Vec<Value>> {
    let (first, rest) = path.split_first()?;
    let column = fields.iter_mut().find(|f| f["name"] == first.as_str())?;
    struct_in(&mut column["type"], rest)
}

fn struct_in<'a>(ty: &'a mut Value, path: &[String]) -> Option<&'a mut Vec<Value>> {
    let Some((step, rest)) = path.split_first() else {
        return (ty["type"] == "struct").then(|| ty["fields"].as_array_mut())?;
    };
    let next = match (ty["type"].as_str(), step.as_str()) {
        (Some("struct"), name) => {
            let f = ty["fields"]
                .as_array_mut()?
                .iter_mut()
                .find(|f| f["name"] == name)?;
            &mut f["type"]
        }
        (Some("list"), "element") => &mut ty["element"],
        (Some("map"), k @ ("key" | "value")) => &mut ty[k],
        _ => return None,
    };
    struct_in(next, rest)
}

/// Iceberg's allowed type promotions.
fn promotes(from: &str, to: &str) -> bool {
    let decimal = |t: &str| {
        let (p, s) = t
            .strip_prefix("decimal(")?
            .strip_suffix(')')?
            .split_once(',')?;
        Some((p.trim().parse::<u32>().ok()?, s.trim().parse::<u32>().ok()?))
    };
    match (from, to) {
        ("int", "long") | ("float", "double") => true,
        _ => {
            matches!((decimal(from), decimal(to)), (Some((p, s)), Some((q, t))) if s == t && q > p)
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field};

    fn meta() -> Value {
        json!({
            "table-uuid": "u", "current-schema-id": 0, "last-column-id": 4,
            "format-version": 2, "last-partition-id": 1000,
            "default-spec-id": 0,
            "partition-specs": [{"spec-id": 0, "fields": [{"source-id": 2, "field-id": 1000, "name": "region", "transform": "identity"}]}],
            "schemas": [{"type": "struct", "schema-id": 0, "fields": [
                {"id": 1, "name": "id", "required": true, "type": "int"},
                {"id": 2, "name": "region", "required": false, "type": "string"},
                {"id": 3, "name": "amount", "required": false, "type": "decimal(10, 2)"},
                {"id": 4, "name": "note", "required": false, "type": "string"},
            ]}],
        })
    }

    #[test]
    fn nested_fields_are_added_renamed_and_dropped_by_path() {
        let mut meta = meta();
        meta["schemas"][0]["fields"][3] = json!({"id": 4, "name": "s", "required": false,
            "type": {"type": "struct", "fields": [
                {"id": 5, "name": "a", "required": false, "type": "int"},
                {"id": 6, "name": "l", "required": false, "type": {"type": "list",
                    "element-id": 7, "element-required": false, "element": {"type": "struct",
                    "fields": [{"id": 8, "name": "x", "required": false, "type": "int"},
                               {"id": 9, "name": "y", "required": false, "type": "int"}]}}}]}});
        meta["last-column-id"] = json!(9);
        let path = |p: &str| p.split('.').map(str::to_owned).collect::<Vec<_>>();
        let changes = [
            Alteration::AddNestedField {
                parent: path("s"),
                field: Field::new("b", DataType::Utf8, true),
            },
            Alteration::RenameNestedField {
                path: path("s.a"),
                to: "alpha".into(),
            },
            Alteration::DropNestedField {
                path: path("s.l.element.y"),
            },
        ];
        let (_, upd) = alter_updates(&meta, &changes).unwrap().unwrap();
        assert_eq!(
            upd[0]["schema"]["fields"][3]["type"]["fields"],
            json!([
                {"id": 5, "name": "alpha", "required": false, "type": "int"},
                {"id": 6, "name": "l", "required": false, "type": {"type": "list",
                    "element-id": 7, "element-required": false, "element": {"type": "struct",
                    "fields": [{"id": 8, "name": "x", "required": false, "type": "int"}]}}},
                {"id": 10, "name": "b", "required": false, "type": "string"},
            ])
        );
        assert_eq!(upd[0]["last-column-id"], 10);
        for bad in [
            Alteration::AddNestedField {
                parent: path("s.a"),
                field: Field::new("b", DataType::Utf8, true),
            },
            Alteration::RenameNestedField {
                path: path("s.a"),
                to: "l".into(),
            },
            Alteration::DropNestedField {
                path: path("s.nope"),
            },
            Alteration::DropNestedField {
                path: path("s.l.element.x.z"),
            },
        ] {
            assert!(
                alter_updates(&meta, std::slice::from_ref(&bad)).is_err(),
                "{bad:?}"
            );
        }
    }

    #[test]
    fn partition_changes_add_or_reuse_a_default_spec() {
        let add = Alteration::AddPartitionField {
            transform: "bucket[4]".into(),
            column: "id".into(),
            name: None,
        };
        let (req, upd) = alter_updates(&meta(), std::slice::from_ref(&add))
            .unwrap()
            .unwrap();
        assert_eq!(req[1]["default-spec-id"], 0);
        assert_eq!(req[2]["last-assigned-partition-id"], 1000);
        assert_eq!(
            upd[0]["spec"],
            json!({"spec-id": 1, "fields": [
                {"source-id": 2, "field-id": 1000, "name": "region", "transform": "identity"},
                {"source-id": 1, "field-id": 1001, "name": "id_bucket", "transform": "bucket[4]"},
            ]})
        );
        assert_eq!(upd[1], json!({"action": "set-default-spec", "spec-id": -1}));
        // Dropping the identity field by its name: an unpartitioned spec.
        let drop = Alteration::DropPartitionField {
            transform: "identity".into(),
            column: "region".into(),
        };
        let (_, upd) = alter_updates(&meta(), std::slice::from_ref(&drop))
            .unwrap()
            .unwrap();
        assert_eq!(upd[0]["spec"]["fields"], json!([]));
        // Back to a spec that exists: made default again, field id kept.
        let mut evolved = meta();
        evolved["partition-specs"]
            .as_array_mut()
            .unwrap()
            .push(json!({"spec-id": 1, "fields": []}));
        evolved["default-spec-id"] = json!(1);
        let readd = Alteration::AddPartitionField {
            transform: "identity".into(),
            column: "region".into(),
            name: None,
        };
        let (_, upd) = alter_updates(&evolved, &[readd]).unwrap().unwrap();
        assert_eq!(upd, [json!({"action": "set-default-spec", "spec-id": 0})]);
        // Refused: a duplicate, an unknown field, a transform on the wrong
        // type, a decimal identity Plimsoll cannot write.
        for bad in [
            Alteration::AddPartitionField {
                transform: "identity".into(),
                column: "region".into(),
                name: None,
            },
            Alteration::DropPartitionField {
                transform: "day".into(),
                column: "region".into(),
            },
            Alteration::AddPartitionField {
                transform: "day".into(),
                column: "note".into(),
                name: None,
            },
            Alteration::AddPartitionField {
                transform: "identity".into(),
                column: "amount".into(),
                name: None,
            },
        ] {
            assert!(
                alter_updates(&meta(), std::slice::from_ref(&bad)).is_err(),
                "{bad:?}"
            );
        }
    }

    #[test]
    fn schema_changes_keep_ids_and_add_one_schema() {
        let changes = [
            Alteration::AddColumn {
                field: Field::new("tag", DataType::Utf8, true),
                if_not_exists: false,
            },
            Alteration::DropColumn {
                name: "note".into(),
                if_exists: false,
            },
            Alteration::RenameColumn {
                from: "amount".into(),
                to: "total".into(),
            },
            Alteration::SetType {
                column: "id".into(),
                data_type: DataType::Int64,
            },
            Alteration::SetType {
                column: "total".into(),
                data_type: DataType::Decimal128(12, 2),
            },
            Alteration::DropNotNull("id".into()),
            Alteration::SetProperties(vec![("k".into(), "v".into())]),
        ];
        let (req, upd) = alter_updates(&meta(), &changes).unwrap().unwrap();
        assert_eq!(req[1]["current-schema-id"], 0);
        assert_eq!(req[2]["last-assigned-field-id"], 4);
        assert_eq!(upd[0]["last-column-id"], 5);
        assert_eq!(
            upd[0]["schema"],
            json!({"type": "struct", "schema-id": 1, "fields": [
                {"id": 1, "name": "id", "required": false, "type": "long"},
                {"id": 2, "name": "region", "required": false, "type": "string"},
                {"id": 3, "name": "total", "required": false, "type": "decimal(12, 2)"},
                {"id": 5, "name": "tag", "required": false, "type": "string"},
            ]})
        );
        assert_eq!(
            upd[2],
            json!({"action": "set-properties", "updates": {"k": "v"}})
        );
        // Nothing to do: no commit.
        let idle = [
            Alteration::DropColumn {
                name: "nope".into(),
                if_exists: true,
            },
            Alteration::AddColumn {
                field: Field::new("note", DataType::Utf8, true),
                if_not_exists: true,
            },
        ];
        assert!(alter_updates(&meta(), &idle).unwrap().is_none());
        // Refused: partition source, narrowing, clash, unknown column.
        for bad in [
            Alteration::DropColumn {
                name: "region".into(),
                if_exists: false,
            },
            Alteration::SetType {
                column: "amount".into(),
                data_type: DataType::Decimal128(8, 2),
            },
            Alteration::SetType {
                column: "region".into(),
                data_type: DataType::Int64,
            },
            Alteration::RenameColumn {
                from: "note".into(),
                to: "region".into(),
            },
            Alteration::DropNotNull("nope".into()),
            Alteration::SetProperties(vec![("format-version".into(), "3".into())]),
        ] {
            assert!(
                alter_updates(&meta(), std::slice::from_ref(&bad)).is_err(),
                "{bad:?}"
            );
        }
    }
}

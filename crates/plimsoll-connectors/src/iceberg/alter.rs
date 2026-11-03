//! `ALTER TABLE` as REST `updateTable` changes (ADR 0084).

use std::collections::HashSet;

use datafusion::error::{DataFusionError, Result};
use plimsoll_core::ddl::Alteration;
use serde_json::{Map, Value, json};

use super::create::iceberg_type;
use crate::ConnectorError;

/// The requirements and updates applying `changes`, in order, to the
/// table `meta` describes; `None` when they change nothing (`IF EXISTS`,
/// `IF NOT EXISTS`, a type already set).
///
/// Schema changes add a schema (fresh field ids for new columns, the
/// others keep theirs, so data files written before still resolve by id,
/// ADR 0040) and make it current, requiring the current schema and last
/// column id as seen.
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
                let Some(ty) = iceberg_type(field.data_type()) else {
                    return plan(format!(
                        "column {}: type {} for Iceberg",
                        field.name(),
                        field.data_type()
                    ));
                };
                last += 1;
                fields
                    .push(json!({"id": last, "name": field.name(), "required": false, "type": ty}));
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
    Ok((!updates.is_empty()).then_some((requirements, updates)))
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

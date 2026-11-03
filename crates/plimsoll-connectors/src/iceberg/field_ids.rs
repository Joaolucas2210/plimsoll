//! Column resolution by Iceberg field id (ADR 0040).
//!
//! DataFusion's Parquet scan resolves table columns in a file by *name*.
//! Iceberg resolves them by *field id*: a renamed column keeps its id, and a
//! column dropped and re-added under the same name gets a new one. Data
//! files written by Iceberg writers carry the ids as Parquet `field_id`s,
//! surfaced by arrow as `PARQUET:field_id` field metadata.
//!
//! [`FieldIds`] is the scan's expression adapter: every column reference
//! is rewritten to the file column with the same id (by its *file* name
//! and index, which is how the Parquet reader resolves projections), cast
//! when the type was promoted, or to a typed NULL when the file has no
//! such field. Files without ids fall back to DataFusion's by-name adapter.
//!
//! Nested fields resolve by id too (ADR 0088): a struct, list or map column
//! whose file type differs from the table's is rebuilt by [`NestedById`],
//! matching struct children by field id (renamed, moved or re-added
//! children read right; missing ones read NULL), recursively.
//!
//! DataFusion skips the adapter when the table and file schemas are equal.
//! For tables whose history renamed or re-added a column, the table schema
//! carries the field ids as metadata (`with_field_ids`), so a file whose
//! ids or names differ never compares equal and is always adapted.

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, RecordBatch};
use datafusion::arrow::datatypes::{Field, FieldRef, Schema, SchemaRef};
use datafusion::common::ScalarValue;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::ColumnarValue;
use datafusion::parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::{CastExpr, Column, Literal};
use datafusion::physical_expr_adapter::{
    DefaultPhysicalExprAdapterFactory, PhysicalExprAdapter, PhysicalExprAdapterFactory,
};

use crate::ConnectorError;

#[derive(Debug)]
pub(super) struct FieldIds {
    /// Field id -> current column name.
    names: Arc<HashMap<i64, String>>,
    /// The table's history renamed or re-added a column, so a file without
    /// field ids cannot be read by name safely.
    renamed: bool,
    /// `schema.name-mapping.default`: file column name -> field id, for
    /// files written without ids (ADR 0042).
    mapping: Option<Arc<NameMapping>>,
    table: String,
    /// Nested top-level columns of the current schema, by name, with the
    /// field id of every nested field as metadata (ADR 0088).
    nested: Arc<HashMap<String, FieldRef>>,
}

/// Top-level entries of an Iceberg name mapping: every name (aliases
/// included) a file may use for a field.
pub(super) type NameMapping = HashMap<String, i64>;

/// Parses `schema.name-mapping.default`
/// (`[{"field-id": 1, "names": ["id", "ident"]}, ...]`). Nested `fields`
/// are ignored: children resolve by name inside their parent (ADR 0041).
pub(super) fn parse_name_mapping(json: &str) -> Result<NameMapping, String> {
    let entries: Vec<serde_json::Value> =
        serde_json::from_str(json).map_err(|e| format!("name mapping: {e}"))?;
    let mut out = HashMap::new();
    for e in entries {
        let Some(id) = e["field-id"].as_i64() else {
            continue; // entries without an id map names to nothing
        };
        for name in e["names"].as_array().into_iter().flatten() {
            let name = name.as_str().ok_or("name mapping: non-string name")?;
            if out.insert(name.to_owned(), id).is_some_and(|old| old != id) {
                return Err(format!("name mapping: {name:?} maps to two fields"));
            }
        }
    }
    Ok(out)
}

impl FieldIds {
    pub(super) fn new(
        names: Arc<HashMap<i64, String>>,
        renamed: bool,
        mapping: Option<Arc<NameMapping>>,
        table: &str,
        nested: Arc<HashMap<String, FieldRef>>,
    ) -> Self {
        Self {
            names,
            renamed,
            mapping,
            table: table.to_owned(),
            nested,
        }
    }
}

/// The Iceberg field id of an arrow field read from Parquet, if any.
pub(super) fn field_id(field: &Field) -> Option<i64> {
    field
        .metadata()
        .get(PARQUET_FIELD_ID_META_KEY)?
        .parse()
        .ok()
}

impl PhysicalExprAdapterFactory for FieldIds {
    fn create(
        &self,
        logical_file_schema: SchemaRef,
        physical_file_schema: SchemaRef,
    ) -> Result<Arc<dyn PhysicalExprAdapter>> {
        let mut ids: Vec<Option<i64>> = physical_file_schema
            .fields()
            .iter()
            .map(|f| field_id(f))
            .collect();
        // A file without ids gets them from the name mapping, if any.
        if let (true, Some(mapping)) = (ids.iter().all(Option::is_none), &self.mapping) {
            ids = physical_file_schema
                .fields()
                .iter()
                .map(|f| mapping.get(f.name()).copied())
                .collect();
        }
        if ids.iter().all(Option::is_none) {
            // Files without ids (e.g. imported with `add_files`) can only be
            // read by name, which is right unless names were reused.
            if self.renamed {
                return Err(DataFusionError::External(Box::new(
                    ConnectorError::Unsupported(format!(
                        "iceberg data file without Parquet field ids in {}, whose columns \
                         were renamed or re-added, and no name mapping resolves its columns",
                        self.table
                    )),
                )));
            }
            return DefaultPhysicalExprAdapterFactory
                .create(logical_file_schema, physical_file_schema);
        }
        let by_id = ids
            .iter()
            .enumerate()
            .filter_map(|(i, id)| Some(((*id)?, i)))
            .collect();
        let ids_by_name = self.names.iter().map(|(id, n)| (n.clone(), *id)).collect();
        Ok(Arc::new(ById {
            logical: logical_file_schema,
            physical: physical_file_schema,
            ids_by_name,
            by_id,
            nested: self.nested.clone(),
        }))
    }
}

/// `schema` with each field's Iceberg id as `PARQUET:field_id` metadata.
pub(super) fn with_field_ids(schema: &Schema, names: &HashMap<i64, String>) -> Schema {
    let ids: HashMap<&str, i64> = names.iter().map(|(id, n)| (n.as_str(), *id)).collect();
    Schema::new_with_metadata(
        schema
            .fields()
            .iter()
            .map(|f| match ids.get(f.name().as_str()) {
                Some(id) => Arc::new(f.as_ref().clone().with_metadata(HashMap::from([(
                    PARQUET_FIELD_ID_META_KEY.to_owned(),
                    id.to_string(),
                )]))),
                None => f.clone(),
            })
            .collect::<Vec<_>>(),
        schema.metadata().clone(),
    )
}

#[derive(Debug)]
struct ById {
    logical: SchemaRef,
    physical: SchemaRef,
    /// Current column name -> field id.
    ids_by_name: HashMap<String, i64>,
    /// Field id -> index in the file.
    by_id: HashMap<i64, usize>,
    nested: Arc<HashMap<String, FieldRef>>,
}

impl PhysicalExprAdapter for ById {
    fn rewrite(&self, expr: Arc<dyn PhysicalExpr>) -> Result<Arc<dyn PhysicalExpr>> {
        expr.transform(|e| {
            let Some(column) = e.as_ref().downcast_ref::<Column>() else {
                return Ok(Transformed::no(e));
            };
            let Ok(field) = self.logical.field_with_name(column.name()) else {
                return Ok(Transformed::no(e));
            };
            let index = self
                .ids_by_name
                .get(column.name())
                .and_then(|id| self.by_id.get(id));
            let Some(&i) = index else {
                // The file predates the field: NULL, typed as the table says.
                let null = ScalarValue::try_from(field.data_type())?;
                return Ok(Transformed::yes(Arc::new(Literal::new(null))));
            };
            let file_field = self.physical.field(i);
            let read: Arc<dyn PhysicalExpr> = Arc::new(Column::new(file_field.name(), i));
            Ok(Transformed::yes(
                if file_field.data_type() == field.data_type() {
                    read
                } else if let Some(ids) = self.nested.get(column.name()) {
                    // Nested: children by field id (ADR 0088).
                    Arc::new(NestedById {
                        child: read,
                        file: Arc::new(file_field.clone()),
                        target: Arc::new(field.clone()),
                        ids: ids.clone(),
                    })
                } else {
                    // Type promotion (int -> long, float -> double), a
                    // struct that gained fields since the file was written
                    // (filled with NULLs), or nested fields carrying their
                    // field ids as metadata (ADR 0087), which the table's
                    // types do not: DataFusion's struct-aware cast.
                    Arc::new(CastExpr::new_with_target_field(
                        read,
                        Arc::new(field.clone()),
                        None,
                    ))
                },
            ))
        })
        .data()
    }
}

/// A file's nested column rebuilt as the table's type (ADR 0088): struct
/// children matched by field id (by name when the file has no ids), list
/// elements and map entries converted recursively, primitives cast
/// (promotions). Children the file lacks read NULL; children the table
/// dropped are left out.
#[derive(Debug)]
pub(super) struct NestedById {
    child: Arc<dyn PhysicalExpr>,
    /// The column as the file has it.
    file: FieldRef,
    /// The column as the table has it.
    target: FieldRef,
    /// `target`'s shape with every field's id as metadata.
    ids: FieldRef,
}

impl PartialEq for NestedById {
    fn eq(&self, other: &Self) -> bool {
        self.child.eq(&other.child) && self.file == other.file && self.target == other.target
    }
}

impl Eq for NestedById {}

impl std::hash::Hash for NestedById {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.child.hash(state);
        self.target.hash(state);
    }
}

impl std::fmt::Display for NestedById {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "nested_by_id({})", self.child)
    }
}

impl PhysicalExpr for NestedById {
    fn return_field(&self, _input_schema: &Schema) -> Result<FieldRef> {
        Ok(self.target.clone())
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let array = self.child.evaluate(batch)?.into_array(batch.num_rows())?;
        Ok(ColumnarValue::Array(by_id(
            &array,
            &self.file,
            &self.target,
            &self.ids,
        )?))
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.child]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        let [child] = <[_; 1]>::try_from(children)
            .map_err(|_| DataFusionError::Internal("nested_by_id takes one child".into()))?;
        Ok(Arc::new(Self {
            child,
            file: self.file.clone(),
            target: self.target.clone(),
            ids: self.ids.clone(),
        }))
    }

    fn fmt_sql(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self}")
    }
}

/// `array` (typed as `file`) as `target`, whose shape `ids` repeats with
/// field ids.
pub(super) fn by_id(
    array: &ArrayRef,
    file: &Field,
    target: &Field,
    ids: &Field,
) -> Result<ArrayRef> {
    use datafusion::arrow::array::{
        Array, AsArray, ListArray, MapArray, StructArray, new_null_array,
    };
    use datafusion::arrow::datatypes::DataType;
    let cast = || Ok(datafusion::arrow::compute::cast(array, target.data_type())?);
    match (file.data_type(), target.data_type(), ids.data_type()) {
        (DataType::Struct(from), DataType::Struct(to), DataType::Struct(to_ids)) => {
            let input = array.as_struct();
            let file_has_ids = from.iter().any(|f| field_id(f).is_some());
            let columns = to
                .iter()
                .zip(to_ids.iter())
                .map(|(t, i)| {
                    let found = match (file_has_ids, field_id(i)) {
                        (true, Some(id)) => from.iter().position(|f| field_id(f) == Some(id)),
                        _ => from.iter().position(|f| f.name() == t.name()),
                    };
                    match found {
                        Some(k) => by_id(input.column(k), &from[k], t, i),
                        None => Ok(new_null_array(t.data_type(), input.len())),
                    }
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(Arc::new(StructArray::try_new(
                to.clone(),
                columns,
                input.nulls().cloned(),
            )?))
        }
        (DataType::List(from), DataType::List(to), DataType::List(to_ids)) => {
            let input = array.as_list::<i32>();
            let values = by_id(input.values(), from, to, to_ids)?;
            Ok(Arc::new(ListArray::try_new(
                to.clone(),
                input.offsets().clone(),
                values,
                input.nulls().cloned(),
            )?))
        }
        (DataType::Map(from, _), DataType::Map(to, sorted), DataType::Map(to_ids, _)) => {
            let input = array.as_map();
            let entries: ArrayRef = Arc::new(input.entries().clone());
            let entries = by_id(&entries, from, to, to_ids)?;
            Ok(Arc::new(MapArray::try_new(
                to.clone(),
                input.offsets().clone(),
                entries.as_struct().clone(),
                input.nulls().cloned(),
                *sorted,
            )?))
        }
        (from, to, _) if from == to => Ok(array.clone()),
        _ => cast(),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{
        Array, AsArray, Int32Array, ListArray, StringArray, StructArray,
    };
    use datafusion::arrow::buffer::OffsetBuffer;
    use datafusion::arrow::datatypes::{DataType, Int64Type};

    fn id(f: Field, id: i64) -> Field {
        f.with_metadata(HashMap::from([(
            PARQUET_FIELD_ID_META_KEY.to_owned(),
            id.to_string(),
        )]))
    }

    /// A file struct `{a: int (3), old: string (4)}` read as a table struct
    /// `{renamed: long (4)... }`: children by id, promoted, dropped and
    /// added ones handled, through a list.
    #[test]
    fn nested_children_resolve_by_id() {
        let file_struct = vec![
            id(Field::new("a", DataType::Int32, true), 3),
            id(Field::new("old", DataType::Utf8, true), 4),
        ];
        let structs = StructArray::try_new(
            file_struct.clone().into(),
            vec![
                Arc::new(Int32Array::from(vec![Some(1), None, Some(3)])),
                Arc::new(StringArray::from(vec!["x", "y", "z"])),
            ],
            None,
        )
        .unwrap();
        let file_elem = id(
            Field::new("element", DataType::Struct(file_struct.into()), true),
            2,
        );
        let list = ListArray::try_new(
            Arc::new(file_elem.clone()),
            OffsetBuffer::from_lengths([2, 0, 1]),
            Arc::new(structs),
            None,
        )
        .unwrap();
        // Table: `old` renamed `new` and moved first; `a` promoted to long;
        // `c` (id 9) added since.
        let table_struct = |ids: bool| {
            let f = |f: Field, i: i64| if ids { id(f, i) } else { f };
            vec![
                f(Field::new("new", DataType::Utf8, true), 4),
                f(Field::new("a", DataType::Int64, true), 3),
                f(Field::new("c", DataType::Int32, true), 9),
            ]
        };
        let target = Field::new(
            "l",
            DataType::new_list(DataType::Struct(table_struct(false).into()), true),
            true,
        );
        let ids = Field::new(
            "l",
            DataType::List(Arc::new(id(
                Field::new("element", DataType::Struct(table_struct(true).into()), true),
                2,
            ))),
            true,
        );
        let file = Field::new("l", DataType::List(Arc::new(file_elem)), true);
        let out = by_id(&(Arc::new(list) as ArrayRef), &file, &target, &ids).unwrap();
        assert_eq!(out.data_type(), target.data_type());
        let values = out.as_list::<i32>().values().as_struct().clone();
        assert_eq!(
            values
                .column(0)
                .as_string::<i32>()
                .iter()
                .collect::<Vec<_>>(),
            [Some("x"), Some("y"), Some("z")]
        );
        assert_eq!(
            values
                .column(1)
                .as_primitive::<Int64Type>()
                .iter()
                .collect::<Vec<_>>(),
            [Some(1), None, Some(3)]
        );
        assert_eq!(values.column(2).null_count(), 3);
    }
}

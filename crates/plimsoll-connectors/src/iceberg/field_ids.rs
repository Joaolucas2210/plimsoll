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
//! DataFusion skips the adapter when the table and file schemas are equal.
//! For tables whose history renamed or re-added a column, the table schema
//! carries the field ids as metadata (`with_field_ids`), so a file whose
//! ids or names differ never compares equal and is always adapted.

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::datatypes::{Field, Schema, SchemaRef};
use datafusion::common::ScalarValue;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::error::{DataFusionError, Result};
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
    ) -> Self {
        Self {
            names,
            renamed,
            mapping,
            table: table.to_owned(),
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

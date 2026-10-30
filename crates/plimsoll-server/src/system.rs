//! `system.runtime.queries` as a DataFusion table over the live registry.
//!
//! Rows are scoped to the query's [`Viewer`] (ADR 0022): a user sees their
//! own queries, an operator sees all. A query without a viewer fails closed.

use std::sync::Arc;

use async_trait::async_trait;
use plimsoll_control::governance::{QueryInfo, QueryRegistry};
use plimsoll_control::system::{self, ColumnType, QUERIES_COLUMNS};
use plimsoll_core::datafusion::arrow::array::{
    ArrayRef, StringArray, TimestampMillisecondArray, UInt64Array,
};
use plimsoll_core::datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use plimsoll_core::datafusion::arrow::record_batch::RecordBatch;
use plimsoll_core::datafusion::catalog::{
    CatalogProvider, MemoryCatalogProvider, MemorySchemaProvider, SchemaProvider, Session,
    TableProvider,
};
use plimsoll_core::datafusion::datasource::TableType;
use plimsoll_core::datafusion::datasource::memory::MemorySourceConfig;
use plimsoll_core::datafusion::error::{DataFusionError, Result};
use plimsoll_core::datafusion::logical_expr::Expr;
use plimsoll_core::datafusion::physical_plan::ExecutionPlan;
use plimsoll_core::datafusion::prelude::SessionContext;

use crate::auth::Viewer;

struct QueriesTable {
    registry: Arc<QueryRegistry>,
    schema: SchemaRef,
}

impl std::fmt::Debug for QueriesTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("system.runtime.queries")
    }
}

/// Registers the `system` catalog with `runtime.queries` in `ctx`.
pub fn register(ctx: &SessionContext, registry: Arc<QueryRegistry>) -> Result<()> {
    let fields: Vec<Field> = QUERIES_COLUMNS
        .iter()
        .map(|c| {
            let ty = match c.ty {
                ColumnType::UInt64 => DataType::UInt64,
                ColumnType::Utf8 => DataType::Utf8,
                ColumnType::TimestampMillis => {
                    DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into()))
                }
            };
            Field::new(c.name, ty, c.nullable)
        })
        .collect();
    let table = QueriesTable {
        registry,
        schema: Arc::new(Schema::new(fields)),
    };
    let runtime = Arc::new(MemorySchemaProvider::new());
    runtime.register_table(system::QUERIES_TABLE.into(), Arc::new(table))?;
    let catalog = Arc::new(MemoryCatalogProvider::new());
    catalog.register_schema(system::SCHEMA, runtime)?;
    ctx.register_catalog(system::CATALOG, catalog);
    Ok(())
}

#[async_trait]
impl TableProvider for QueriesTable {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let viewer = state.config().get_extension::<Viewer>().ok_or_else(|| {
            DataFusionError::Plan(
                "system.runtime.queries needs the caller's identity (no viewer in session)".into(),
            )
        })?;
        let mut rows = system::queries_snapshot(&self.registry);
        if let Some(user) = &viewer.only {
            rows.retain(|r| &r.user == user);
        }
        let ts = |f: fn(&QueryInfo) -> Option<u64>| -> ArrayRef {
            Arc::new(
                TimestampMillisecondArray::from_iter(rows.iter().map(|r| f(r).map(|v| v as i64)))
                    .with_timezone("UTC"),
            )
        };
        let text = |f: fn(&QueryInfo) -> Option<&str>| -> ArrayRef {
            Arc::new(StringArray::from_iter(rows.iter().map(f)))
        };
        // Column order must match QUERIES_COLUMNS.
        let columns: Vec<ArrayRef> = vec![
            Arc::new(UInt64Array::from_iter_values(rows.iter().map(|r| r.id))),
            text(|r| Some(r.user.as_str())),
            text(|r| Some(r.resource_group.as_str())),
            text(|r| Some(r.sql.as_str())),
            text(|r| Some(r.state.as_str())),
            ts(|r| Some(r.created_at_ms)),
            ts(|r| r.started_at_ms),
            ts(|r| r.finished_at_ms),
            text(|r| r.error.as_deref()),
        ];
        let batch = RecordBatch::try_new(self.schema.clone(), columns)?;
        Ok(MemorySourceConfig::try_new_exec(
            &[vec![batch]],
            self.schema.clone(),
            projection.cloned(),
        )?)
    }
}

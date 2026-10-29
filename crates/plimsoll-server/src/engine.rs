//! The seam between the HTTP layer and the compute plane.
//!
//! [`CoreEngine`] adapts the `plimsoll-core` façade (one SessionContext on the
//! process CeilingPool). Admission, timeouts and the registry already wrap
//! every call in `app.rs`.

use std::sync::Arc;

use async_trait::async_trait;
use plimsoll_core::datafusion::arrow::json::ArrayWriter;
use plimsoll_core::datafusion::error::DataFusionError;
use plimsoll_core::engine::Engine;
use serde::Serialize;

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct QueryOutput {
    pub columns: Vec<String>,
    /// Row-major values, one inner vec per row, aligned with `columns`.
    pub rows: Vec<Vec<serde_json::Value>>,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum EngineError {
    /// Parse / plan / unknown table: the caller's fault.
    #[error("invalid query: {0}")]
    InvalidQuery(String),
    /// The query crossed the load line. The node is fine; this query is not.
    #[error("memory limit exceeded: {0}")]
    MemoryLimitExceeded(String),
    #[error("internal error: {0}")]
    Internal(String),
}

#[async_trait]
pub trait QueryEngine: Send + Sync {
    async fn execute(&self, sql: &str) -> Result<QueryOutput, EngineError>;
}

pub struct CoreEngine(pub Arc<Engine>);

#[async_trait]
impl QueryEngine for CoreEngine {
    async fn execute(&self, sql: &str) -> Result<QueryOutput, EngineError> {
        let batches = self.0.sql(sql).await.map_err(map_err)?;
        let columns: Vec<String> = batches
            .first()
            .map(|b| {
                b.schema()
                    .fields()
                    .iter()
                    .map(|f| f.name().clone())
                    .collect()
            })
            .unwrap_or_default();
        let mut writer = ArrayWriter::new(Vec::new());
        writer
            .write_batches(&batches.iter().collect::<Vec<_>>())
            .and_then(|()| writer.finish())
            .map_err(|e| EngineError::Internal(e.to_string()))?;
        let objects: Vec<serde_json::Map<String, serde_json::Value>> =
            serde_json::from_slice(&writer.into_inner())
                .map_err(|e| EngineError::Internal(e.to_string()))?;
        // ponytail: JSON objects -> rows by column name; arrow's writer omits
        // nulls, which become `null` here. Upgrade path: Arrow IPC responses.
        let rows = objects
            .into_iter()
            .map(|mut o| {
                columns
                    .iter()
                    .map(|c: &String| o.remove(c).unwrap_or(serde_json::Value::Null))
                    .collect()
            })
            .collect();
        Ok(QueryOutput { columns, rows })
    }
}

fn map_err(e: DataFusionError) -> EngineError {
    match e.find_root() {
        DataFusionError::ResourcesExhausted(m) => EngineError::MemoryLimitExceeded(m.clone()),
        DataFusionError::SQL(..)
        | DataFusionError::Plan(_)
        | DataFusionError::SchemaError(..)
        | DataFusionError::NotImplemented(_) => EngineError::InvalidQuery(e.to_string()),
        _ => EngineError::Internal(e.to_string()),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use plimsoll_core::memory::CeilingPool;

    #[tokio::test]
    async fn sql_to_rows_and_typed_errors() {
        let pool = Arc::new(CeilingPool::new(1 << 20, 1 << 17).unwrap());
        let e = CoreEngine(Arc::new(Engine::new(pool, None).unwrap()));
        let out = e.execute("SELECT 1 AS a, NULL AS b").await.unwrap();
        assert_eq!(out.columns, ["a", "b"]);
        assert_eq!(out.rows, [[serde_json::json!(1), serde_json::Value::Null]]);
        assert!(matches!(
            e.execute("SELEC").await,
            Err(EngineError::InvalidQuery(_))
        ));
        assert!(matches!(
            e.execute("SELECT value, repeat('x', 100) FROM range(200000)")
                .await,
            Err(EngineError::MemoryLimitExceeded(_))
        ));
    }
}

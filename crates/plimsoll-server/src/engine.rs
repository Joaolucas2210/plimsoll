//! The seam between the HTTP layer and the compute plane.
//!
//! [`CoreEngine`] adapts the `plimsoll-core` façade (one SessionContext on the
//! process CeilingPool). Admission, timeouts and the registry already wrap
//! every call in `app.rs`.

use std::sync::Arc;

use async_trait::async_trait;
use plimsoll_core::datafusion::arrow::json::ArrayWriter;
use plimsoll_core::datafusion::error::DataFusionError;
use plimsoll_core::engine::{Engine, QueryResult};
use plimsoll_core::memory::Reservation;
use serde_json::Value;

/// A result ready for the wire. `rows_json` is already serialized (a JSON
/// array of row arrays aligned with `columns`) and stays reserved in the
/// pool through `memory` until the response body has been handed off.
pub struct QueryOutput {
    pub columns: Vec<String>,
    pub rows_json: Vec<u8>,
    pub memory: Option<Reservation>,
}

impl QueryOutput {
    /// Unaccounted output from plain values (tests, fakes).
    pub fn from_rows(columns: Vec<String>, rows: &[Vec<Value>]) -> Self {
        Self {
            columns,
            rows_json: serde_json::to_vec(rows).unwrap_or_else(|_| b"[]".to_vec()),
            memory: None,
        }
    }
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
        let QueryResult {
            batches,
            mut memory,
        } = self.0.sql(sql).await.map_err(map_err)?;
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
        let internal = |e: &dyn std::fmt::Display| EngineError::Internal(e.to_string());
        let mut rows_json = b"[".to_vec();
        let mem_err = |e: plimsoll_core::memory::CeilingError| {
            EngineError::MemoryLimitExceeded(e.to_string())
        };
        memory.try_grow(2).map_err(mem_err)?; // `[` and `]`
        // Batch by batch: each batch leaves the pool as its JSON enters it, so
        // only one batch's transient JSON objects are ever unaccounted.
        for batch in batches {
            let mut writer = ArrayWriter::new(Vec::new());
            writer
                .write(&batch)
                .and_then(|()| writer.finish())
                .map_err(|e| internal(&e))?;
            let objects: Vec<serde_json::Map<String, Value>> =
                serde_json::from_slice(&writer.into_inner()).map_err(|e| internal(&e))?;
            let before = rows_json.len();
            for mut object in objects {
                if rows_json.len() > 1 {
                    rows_json.push(b',');
                }
                // Arrow's writer omits nulls: absent columns are `null`.
                let row: Vec<Value> = columns
                    .iter()
                    .map(|c| object.remove(c).unwrap_or(Value::Null))
                    .collect();
                serde_json::to_writer(&mut rows_json, &row).map_err(|e| internal(&e))?;
            }
            memory.shrink(batch.get_array_memory_size());
            drop(batch);
            memory.try_grow(rows_json.len() - before).map_err(mem_err)?;
        }
        rows_json.push(b']');
        Ok(QueryOutput {
            columns,
            rows_json,
            memory: Some(memory),
        })
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
        let rows: Value = serde_json::from_slice(&out.rows_json).unwrap();
        assert_eq!(rows, serde_json::json!([[1, null]]));
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

    /// The JSON copy is accounted: a result whose Arrow form fits under the
    /// ceiling but whose JSON does not fails the query (and frees everything).
    #[tokio::test]
    async fn serialized_response_is_accounted() {
        let pool = Arc::new(CeilingPool::new(4 << 20, 2 << 20).unwrap());
        let e = CoreEngine(Arc::new(Engine::new(pool.clone(), None).unwrap()));
        let small = e.execute("SELECT value FROM range(1000)").await.unwrap();
        let held = small.memory.as_ref().map(Reservation::size).unwrap();
        assert_eq!(
            held,
            small.rows_json.len(),
            "exactly the JSON bytes are reserved"
        );
        drop(small);
        assert_eq!(pool.used(), 0);

        // NULL columns are ~0 bytes as Arrow (NullArray) but ~22 bytes per
        // row as JSON (`[null,null,null,null]`): 200k rows only cross the
        // 4 MiB ceiling once serialized.
        let err = e
            .execute("SELECT NULL AS a, NULL AS b, NULL AS c, NULL AS d FROM range(200000)")
            .await;
        assert!(matches!(err, Err(EngineError::MemoryLimitExceeded(_))));
        assert_eq!(pool.used(), 0);
    }
}

//! The seam between the HTTP layer and the compute plane.
//!
//! [`CoreEngine`] adapts the `plimsoll-core` façade (one SessionContext on the
//! process CeilingPool). Admission, timeouts and the registry already wrap
//! every call in `app.rs`.

use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use plimsoll_connectors::ConnectorError;
use plimsoll_control::catalog::CatalogError;
use plimsoll_core::datafusion::arrow::json::ArrayWriter;
use plimsoll_core::datafusion::error::DataFusionError;
use plimsoll_core::datafusion::object_store::Error as ObjectStoreError;
use plimsoll_core::datafusion::parquet::errors::ParquetError;
use plimsoll_core::engine::{AccessDenied, Engine, QueryResult};

use crate::auth::Viewer;
use plimsoll_core::memory::Reservation;
use serde_json::Value;

/// A result ready for the wire. `rows_json` is already serialized (a JSON
/// array of row arrays aligned with `columns`) and stays reserved in the
/// pool through `memory` until the response body has been handed off.
pub struct QueryOutput {
    pub columns: Vec<String>,
    pub rows_json: Vec<u8>,
    pub memory: Option<Reservation>,
    pub phases: Phases,
}

/// Where a successful query spent its time (the admission queue is timed by
/// the handler).
#[derive(Debug, Default, Clone, Copy)]
pub struct Phases {
    pub planning: Duration,
    pub execution: Duration,
    pub serialization: Duration,
}

impl QueryOutput {
    /// Unaccounted output from plain values (tests, fakes).
    pub fn from_rows(columns: Vec<String>, rows: &[Vec<Value>]) -> Self {
        Self {
            columns,
            rows_json: serde_json::to_vec(rows).unwrap_or_else(|_| b"[]".to_vec()),
            memory: None,
            phases: Phases::default(),
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
    /// The table uses a feature Plimsoll does not read (yet).
    #[error("unsupported: {0}")]
    Unsupported(String),
    /// The table itself is broken: malformed metadata, a missing data file.
    #[error("invalid table: {0}")]
    InvalidTable(String),
    /// The caller may not read a table the query uses (ADR 0025).
    #[error("forbidden: {0}")]
    Forbidden(String),
    /// An upstream system failed or refused us. Retrying may succeed.
    #[error("{dependency} failed: {message}")]
    Dependency {
        dependency: &'static str,
        message: String,
    },
    #[error("internal error: {0}")]
    Internal(String),
}

#[async_trait]
pub trait QueryEngine: Send + Sync {
    /// Runs `sql` on behalf of `viewer` (scopes system tables, ADR 0022).
    async fn execute(&self, sql: &str, viewer: Viewer) -> Result<QueryOutput, EngineError>;
}

pub struct CoreEngine(pub Arc<Engine>);

#[async_trait]
impl QueryEngine for CoreEngine {
    async fn execute(&self, sql: &str, viewer: Viewer) -> Result<QueryOutput, EngineError> {
        let QueryResult {
            batches,
            mut memory,
            planning,
            execution,
        } = self
            .0
            .sql_with(sql, Arc::new(viewer))
            .await
            .map_err(|e| classify(&e))?;
        let serializing = Instant::now();
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
            phases: Phases {
                planning,
                execution,
                serialization: serializing.elapsed(),
            },
        })
    }
}

/// Maps an engine error to its class (ADR 0013). Typed errors from
/// connectors, the catalog and object stores are found by downcasting; only
/// what nobody classified is `Internal`.
pub fn classify(e: &DataFusionError) -> EngineError {
    let msg = e.to_string();
    match e.find_root() {
        DataFusionError::ResourcesExhausted(m) => EngineError::MemoryLimitExceeded(m.clone()),
        DataFusionError::SQL(..)
        | DataFusionError::Plan(_)
        | DataFusionError::SchemaError(..)
        | DataFusionError::NotImplemented(_) => EngineError::InvalidQuery(msg),
        DataFusionError::ObjectStore(os) => object_store(os, msg),
        DataFusionError::ParquetError(p) => match p.as_ref() {
            ParquetError::External(inner) => external(inner.as_ref(), msg),
            // Undecodable Parquet referenced by the table.
            _ => EngineError::InvalidTable(msg),
        },
        DataFusionError::External(inner) => external(inner.as_ref(), msg),
        _ => EngineError::Internal(msg),
    }
}

fn external(inner: &(dyn std::error::Error + 'static), msg: String) -> EngineError {
    let dependency = |dependency| EngineError::Dependency {
        dependency,
        message: msg.clone(),
    };
    if inner.downcast_ref::<AccessDenied>().is_some() {
        return EngineError::Forbidden(msg);
    }
    if let Some(e) = inner.downcast_ref::<ConnectorError>() {
        return match e {
            ConnectorError::Unsupported(_) | ConnectorError::UnsupportedType { .. } => {
                EngineError::Unsupported(msg)
            }
            ConnectorError::InvalidMetadata(_) => EngineError::InvalidTable(msg),
            ConnectorError::PostgresUnavailable(_) => dependency("postgres"),
            ConnectorError::DataFusion(e) => classify(e),
            ConnectorError::MissingEnv(_)
            | ConnectorError::Postgres(_)
            | ConnectorError::Arrow(_) => EngineError::Internal(msg),
        };
    }
    if let Some(e) = inner.downcast_ref::<CatalogError>() {
        return match e {
            CatalogError::TableNotFound(_) | CatalogError::NamespaceNotFound(_) => {
                EngineError::InvalidQuery(msg)
            }
            CatalogError::Config(_) => EngineError::Internal(msg),
            _ => dependency("catalog"),
        };
    }
    if let Some(e) = inner.downcast_ref::<ObjectStoreError>() {
        return object_store(e, msg);
    }
    if let Some(e) = inner.downcast_ref::<DataFusionError>() {
        return classify(e);
    }
    EngineError::Internal(msg)
}

fn object_store(e: &ObjectStoreError, msg: String) -> EngineError {
    match e {
        // The table references a file that is gone: the table is broken.
        ObjectStoreError::NotFound { .. } => EngineError::InvalidTable(msg),
        _ => EngineError::Dependency {
            dependency: "object_store",
            message: msg,
        },
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
        let out = e
            .execute("SELECT 1 AS a, NULL AS b", Viewer::default())
            .await
            .unwrap();
        assert_eq!(out.columns, ["a", "b"]);
        let rows: Value = serde_json::from_slice(&out.rows_json).unwrap();
        assert_eq!(rows, serde_json::json!([[1, null]]));
        assert!(matches!(
            e.execute("SELEC", Viewer::default()).await,
            Err(EngineError::InvalidQuery(_))
        ));
        assert!(matches!(
            e.execute(
                "SELECT value, repeat('x', 100) FROM range(200000)",
                Viewer::default()
            )
            .await,
            Err(EngineError::MemoryLimitExceeded(_))
        ));
    }

    #[test]
    fn errors_are_classified_at_their_source() {
        let ext = |e: Box<dyn std::error::Error + Send + Sync>| DataFusionError::External(e);
        let class = |e: DataFusionError| classify(&e);
        assert!(matches!(
            class(ext(Box::new(ConnectorError::Unsupported(
                "delete files".into()
            )))),
            EngineError::Unsupported(_)
        ));
        assert!(matches!(
            class(ext(Box::new(ConnectorError::InvalidMetadata("bad".into())))),
            EngineError::InvalidTable(_)
        ));
        assert!(matches!(
            class(ext(Box::new(CatalogError::Http("refused".into())))),
            EngineError::Dependency {
                dependency: "catalog",
                ..
            }
        ));
        let os = |e| DataFusionError::ObjectStore(Box::new(e));
        assert!(matches!(
            class(os(ObjectStoreError::NotFound {
                path: "s3://wh/f.parquet".into(),
                source: "gone".into(),
            })),
            EngineError::InvalidTable(_)
        ));
        assert!(matches!(
            class(os(ObjectStoreError::PermissionDenied {
                path: "s3://wh/f.parquet".into(),
                source: "403".into(),
            })),
            EngineError::Dependency {
                dependency: "object_store",
                ..
            }
        ));
        // Wrapped in context, inside Parquet's external error, or nested in a
        // connector: still found.
        let nested = ext(Box::new(ConnectorError::DataFusion(ext(Box::new(
            CatalogError::Auth("401".into()),
        )))))
        .context("planning");
        assert!(matches!(
            class(nested),
            EngineError::Dependency {
                dependency: "catalog",
                ..
            }
        ));
        let pq = DataFusionError::ParquetError(Box::new(ParquetError::External(Box::new(
            ObjectStoreError::Generic {
                store: "S3",
                source: "timeout".into(),
            },
        ))));
        assert!(matches!(
            class(pq),
            EngineError::Dependency {
                dependency: "object_store",
                ..
            }
        ));
        assert!(matches!(
            class(DataFusionError::Internal("bug".into())),
            EngineError::Internal(_)
        ));
    }

    /// The JSON copy is accounted: a result whose Arrow form fits under the
    /// ceiling but whose JSON does not fails the query (and frees everything).
    #[tokio::test]
    async fn serialized_response_is_accounted() {
        let pool = Arc::new(CeilingPool::new(4 << 20, 2 << 20).unwrap());
        let e = CoreEngine(Arc::new(Engine::new(pool.clone(), None).unwrap()));
        let small = e
            .execute("SELECT value FROM range(1000)", Viewer::default())
            .await
            .unwrap();
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
            .execute(
                "SELECT NULL AS a, NULL AS b, NULL AS c, NULL AS d FROM range(200000)",
                Viewer::default(),
            )
            .await;
        assert!(matches!(err, Err(EngineError::MemoryLimitExceeded(_))));
        assert_eq!(pool.used(), 0);
    }
}

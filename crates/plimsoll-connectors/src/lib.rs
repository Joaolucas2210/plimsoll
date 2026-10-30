//! plimsoll-connectors: sources exposed to the engine as guarded DataFusion
//! `TableProvider`s. Depends on `plimsoll-core`, never the other way around.

use std::sync::Arc;

use datafusion::catalog::TableProvider;
use datafusion::error::DataFusionError;
use datafusion::prelude::SessionContext;
use plimsoll_core::federation::{FederationFilterGuard, PushdownPolicy};

pub mod iceberg;
pub mod parquet;
pub mod postgres;

pub use iceberg::IcebergConnector;
pub use parquet::ParquetConnector;
pub use postgres::{PG_URL_ENV, PostgresConnector};

/// The whole connector contract: a name, a pushdown policy, and the raw source.
/// The engine only ever sees [`Connector::provider`], i.e. the source behind a
/// [`FederationFilterGuard`] enforcing the policy.
pub trait Connector: Send + Sync {
    fn name(&self) -> &str;
    fn policy(&self) -> &PushdownPolicy;
    /// Unguarded source. Do not register this directly.
    fn source(&self) -> Arc<dyn TableProvider>;

    fn provider(&self) -> Arc<dyn TableProvider> {
        Arc::new(FederationFilterGuard::new(
            self.source(),
            self.policy().clone(),
        ))
    }

    /// Register the guarded provider under [`Connector::name`].
    fn register(&self, ctx: &SessionContext) -> Result<(), DataFusionError> {
        ctx.register_table(self.name(), self.provider()).map(|_| ())
    }
}

/// Variants are classified at the source (ADR 0013): the server maps them to
/// distinct HTTP codes without knowing driver internals.
#[derive(Debug, thiserror::Error)]
pub enum ConnectorError {
    #[error("environment variable {0} is not set")]
    MissingEnv(&'static str),
    /// A table uses something Plimsoll does not read yet (delete files,
    /// nested types, a Postgres type, ...). Not a node fault.
    #[error("column {column:?} has unsupported Postgres type {pg_type}")]
    UnsupportedType { column: String, pg_type: String },
    #[error("unsupported: {0}")]
    Unsupported(String),
    /// The table's own metadata is malformed or inconsistent.
    #[error("invalid table metadata: {0}")]
    InvalidMetadata(String),
    /// Postgres unreachable, refusing the login or out of resources
    /// (SQLSTATE classes 08, 28, 53, 57, or no server response).
    #[error("postgres unavailable: {0}")]
    PostgresUnavailable(tokio_postgres::Error),
    /// Postgres rejected the statement: most likely a Plimsoll bug.
    #[error("postgres: {0}")]
    Postgres(tokio_postgres::Error),
    #[error("arrow: {0}")]
    Arrow(#[from] datafusion::arrow::error::ArrowError),
    #[error(transparent)]
    DataFusion(#[from] DataFusionError),
}

impl From<tokio_postgres::Error> for ConnectorError {
    fn from(e: tokio_postgres::Error) -> Self {
        let unavailable = match e.as_db_error() {
            Some(db) => matches!(&db.code().code()[..2], "08" | "28" | "53" | "57"),
            // No SQLSTATE: I/O, closed connection, timeout.
            None => true,
        };
        if unavailable {
            Self::PostgresUnavailable(e)
        } else {
            Self::Postgres(e)
        }
    }
}

impl From<ConnectorError> for DataFusionError {
    fn from(e: ConnectorError) -> Self {
        match e {
            ConnectorError::DataFusion(e) => e,
            other => DataFusionError::External(Box::new(other)),
        }
    }
}

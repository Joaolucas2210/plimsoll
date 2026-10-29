//! plimsoll-connectors: sources exposed to the engine as guarded DataFusion
//! `TableProvider`s. Depends on `plimsoll-core`, never the other way around.

use std::sync::Arc;

use datafusion::catalog::TableProvider;
use datafusion::error::DataFusionError;
use datafusion::prelude::SessionContext;
use plimsoll_core::federation::{FederationFilterGuard, PushdownPolicy};

pub mod parquet;
pub mod postgres;

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

#[derive(Debug, thiserror::Error)]
pub enum ConnectorError {
    #[error("environment variable {0} is not set")]
    MissingEnv(&'static str),
    #[error("column {column:?} has unsupported Postgres type {pg_type}")]
    UnsupportedType { column: String, pg_type: String },
    #[error("postgres: {0}")]
    Postgres(#[from] tokio_postgres::Error),
    #[error("arrow: {0}")]
    Arrow(#[from] datafusion::arrow::error::ArrowError),
    #[error(transparent)]
    DataFusion(#[from] DataFusionError),
}

impl From<ConnectorError> for DataFusionError {
    fn from(e: ConnectorError) -> Self {
        match e {
            ConnectorError::DataFusion(e) => e,
            other => DataFusionError::External(Box::new(other)),
        }
    }
}

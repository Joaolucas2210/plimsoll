//! plimsoll-server: the HTTP front door. "The query fails. The node doesn't."
//!
//! Startup: parse env (fail fast) -> build governance -> bind -> ready.
//! Shutdown (SIGTERM / ctrl-c): readiness flips to false, the listener stops
//! accepting, in-flight requests finish, then the process exits.

use std::process::ExitCode;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

use plimsoll_control::catalog::CatalogConfig;
use plimsoll_control::governance::{Admission, QueryRegistry};

use plimsoll_control::rest::RestCatalog;
use plimsoll_core::engine::Engine;
use plimsoll_core::memory::CeilingPool;
use plimsoll_server::app::{self, AppState};
use plimsoll_server::config::Config;
use plimsoll_server::engine::CoreEngine;
use plimsoll_server::metrics::PoolStats;
use plimsoll_server::{lakehouse, system, tables};

/// Finished/failed/cancelled queries kept for `system.runtime.queries`.
const RETAINED_QUERIES: usize = 1_000;

#[tokio::main]
async fn main() -> ExitCode {
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("plimsoll-server: {e}");
            ExitCode::FAILURE
        }
    }
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let cfg = Config::from_env()?;
    // Validated now so a bad catalog config fails at boot, not at first query.
    let catalog = CatalogConfig::from_lookup(|k| std::env::var(k).ok())?;

    let registry = Arc::new(QueryRegistry::new(RETAINED_QUERIES));
    let admission = Admission::new(cfg.admission, registry);

    // The Plimsoll line: one pool for the whole process. 1/8 is kept for
    // result/exchange buffers so working operators can't starve the output.
    let limit = usize::try_from(cfg.memory_limit_bytes)?;
    let pool = Arc::new(CeilingPool::new(limit, limit / 8)?);
    // Unset = no spill: memory pressure fails the query (never the process).
    let spill_dir = std::env::var_os("PLIMSOLL_SPILL_DIR").map(std::path::PathBuf::from);
    if let Some(dir) = &spill_dir {
        std::fs::create_dir_all(dir)?;
    }
    let engine = Arc::new(Engine::new(pool.clone(), spill_dir)?);
    system::register(engine.context(), admission.registry().clone())?;
    // Catalog first: env tables aimed at a catalog-backed schema then fail
    // fast (those schemas are read-only) instead of being silently shadowed.
    if let Some(cfg) = catalog.clone() {
        let catalog = Arc::new(RestCatalog::connect(cfg).await?);
        let schemas = lakehouse::register(engine.context(), catalog).await?;
        eprintln!("catalog: serving namespaces {schemas:?}");
    }
    let n = tables::register_from_env(engine.context(), |k| std::env::var(k).ok()).await?;
    eprintln!("registered {n} table(s) from env");

    let stats_engine = engine.clone();
    let state = Arc::new(AppState {
        engine: Arc::new(CoreEngine(engine)),
        admission,
        pool_stats: Arc::new(move || PoolStats {
            used_bytes: pool.used() as u64,
            limit_bytes: pool.limit() as u64,
            spill_bytes_total: stats_engine.spilled_bytes_total(),
        }),
        query_timeout: cfg.query_timeout,
        ready: AtomicBool::new(false),
        started: Instant::now(),
        errors: Default::default(),
    });

    let listener = tokio::net::TcpListener::bind(cfg.bind).await?;
    // ponytail: eprintln instead of a tracing subscriber; one less dependency
    // until structured logs / OTLP land (ROADMAP).
    eprintln!(
        "plimsoll-server listening on {} (catalog: {})",
        listener.local_addr()?,
        catalog.as_ref().map_or("none", |c| c.uri.as_str())
    );
    state.ready.store(true, Ordering::Relaxed);

    axum::serve(listener, app::router(state.clone()))
        .with_graceful_shutdown(shutdown_signal(state))
        .await?;
    eprintln!("plimsoll-server stopped");
    Ok(())
}

/// Resolves on SIGTERM or ctrl-c and marks the node not ready.
///
/// ponytail: no drain delay between "not ready" and closing the listener.
/// Upgrade path: sleep `PLIMSOLL_DRAIN_DELAY_MS` here so load balancers observe
/// `/readyz` = 503 before connections are refused.
async fn shutdown_signal(state: Arc<AppState>) {
    let ctrl_c = async {
        if let Err(e) = tokio::signal::ctrl_c().await {
            eprintln!("ctrl-c handler failed: {e}");
            std::future::pending::<()>().await;
        }
    };
    #[cfg(unix)]
    let term = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut s) => {
                s.recv().await;
            }
            Err(e) => {
                eprintln!("SIGTERM handler failed: {e}");
                std::future::pending::<()>().await;
            }
        }
    };
    #[cfg(not(unix))]
    let term = std::future::pending::<()>();

    tokio::select! {
        () = ctrl_c => {},
        () = term => {},
    }
    state.ready.store(false, Ordering::Relaxed);
    eprintln!("shutdown requested: draining in-flight queries");
}

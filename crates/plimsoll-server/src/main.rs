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
use plimsoll_server::auth::Auth;
use plimsoll_server::config::Config;
use plimsoll_server::engine::CoreEngine;
use plimsoll_server::log::{self, Level};
use plimsoll_server::metrics::PoolStats;
use plimsoll_server::trace::Tracer;
use plimsoll_server::{lakehouse, system, tables};
use serde_json::json;

/// Finished/failed/cancelled queries kept for `system.runtime.queries`.
const RETAINED_QUERIES: usize = 1_000;

#[tokio::main]
async fn main() -> ExitCode {
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            log::emit(
                Level::Error,
                "startup_failed",
                json!({ "error": e.to_string() }),
            );
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
    // result/exchange buffers so working operators can't starve the output;
    // 1/16 (at most 64 MiB) is DataFusion's caches, so they sit under the
    // line too (ADR 0018). ponytail: fixed split; make it config when a
    // deployment needs a bigger footer/delete cache.
    let limit = usize::try_from(cfg.memory_limit_bytes)?;
    let mut pool =
        CeilingPool::new(limit, limit / 8)?.with_cache_reserve((limit / 16).min(64 << 20))?;
    // Resource groups with a memory share (ADR 0036): a percent of the line.
    for (group, percent) in &cfg.group_memory {
        pool = pool.with_group_share(group, limit / 100 * usize::from(*percent));
    }
    let pool = Arc::new(pool);
    // Unset = no spill: memory pressure fails the query (never the process).
    let spill_dir = std::env::var_os("PLIMSOLL_SPILL_DIR").map(std::path::PathBuf::from);
    if let Some(dir) = &spill_dir {
        std::fs::create_dir_all(dir)?;
    }
    let engine = Arc::new(Engine::new(pool.clone(), spill_dir)?);
    system::register(engine.context(), admission.registry().clone())?;
    // Catalog first: env tables aimed at a catalog-backed schema then fail
    // fast (those schemas are read-only) instead of being silently shadowed.
    if let Some(catalog_cfg) = catalog.clone() {
        let catalog = Arc::new(RestCatalog::connect(catalog_cfg).await?);
        let schemas = lakehouse::register(engine.context(), catalog.clone()).await?;
        log::emit(
            Level::Info,
            "catalog_registered",
            json!({ "namespaces": schemas }),
        );
        if !cfg.catalog_refresh.is_zero() {
            tokio::spawn(refresh_catalog(
                engine.clone(),
                catalog,
                cfg.catalog_refresh,
            ));
        }
    }
    let n = tables::register_from_env(engine.context(), |k| std::env::var(k).ok()).await?;
    log::emit(Level::Info, "env_tables_registered", json!({ "tables": n }));

    let (tracer, exporter) = match cfg.otlp.clone() {
        Some(otlp) => {
            let (t, h) = Tracer::spawn(otlp)?;
            (Some(t), Some(h))
        }
        None => (None, None),
    };

    let stats_engine = engine.clone();
    let state = Arc::new(AppState {
        engine: Arc::new(CoreEngine(engine)),
        admission,
        pool_stats: Arc::new(move || PoolStats {
            used_bytes: pool.used() as u64,
            limit_bytes: pool.limit() as u64,
            cache_reserve_bytes: pool.cache_reserve() as u64,
            spill_bytes_total: stats_engine.spilled_bytes_total(),
            groups: pool
                .group_usage()
                .into_iter()
                .map(|(g, share, used)| (g, share as u64, used as u64))
                .collect(),
        }),
        query_timeout: cfg.query_timeout,
        ready: AtomicBool::new(false),
        serving: AtomicBool::new(false),
        started: Instant::now(),
        errors: Default::default(),
        latency: Default::default(),
        auth: cfg.auth.clone(),
        tracer,
    });

    let listener = tokio::net::TcpListener::bind(cfg.bind).await?;
    log::emit(
        Level::Info,
        "listening",
        json!({
            "addr": listener.local_addr()?.to_string(),
            "catalog": catalog.as_ref().map(|c| c.uri.as_str()),
            "auth": format!("{:?}", cfg.auth),
            "otlp": cfg.otlp.as_ref().map(|o| o.endpoint.as_str()),
        }),
    );
    if cfg.auth == Auth::Disabled {
        log::emit(
            Level::Warn,
            "auth_disabled",
            json!({ "message": "PLIMSOLL_AUTH=disabled: `user` is trusted as sent (development only)" }),
        );
    } else if !cfg.auth.grants_enforced() {
        log::emit(
            Level::Warn,
            "grants_not_enforced",
            json!({ "message": "PLIMSOLL_AUTH_GRANTS unset: every principal reads every schema" }),
        );
    }
    state.serving.store(true, Ordering::Relaxed);
    state.ready.store(true, Ordering::Relaxed);

    axum::serve(listener, app::router(state.clone()))
        .with_graceful_shutdown(shutdown_signal(state.clone(), cfg.drain_delay))
        .await?;
    // Spans of the last queries: flushed, but never allowed to hold the exit.
    if let (Some(tracer), Some(exporter)) = (&state.tracer, exporter) {
        tracer.shutdown();
        if tokio::time::timeout(std::time::Duration::from_secs(5), exporter)
            .await
            .is_err()
        {
            log::emit(Level::Warn, "trace_flush_timeout", json!({}));
        }
    }
    log::emit(Level::Info, "stopped", json!({}));
    Ok(())
}

/// Keeps `SHOW TABLES` / `information_schema` in step with the catalog
/// (ADR 0033). Lookups do not need it: every query asks the catalog.
async fn refresh_catalog(
    engine: Arc<Engine>,
    catalog: Arc<RestCatalog>,
    every: std::time::Duration,
) {
    let mut tick = tokio::time::interval(every);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    tick.tick().await; // the first tick is immediate; boot just listed
    loop {
        tick.tick().await;
        match lakehouse::refresh(engine.context(), catalog.clone()).await {
            Ok(r) if r.new_namespaces.is_empty() && r.changed.is_empty() => {}
            Ok(r) => log::emit(
                Level::Info,
                "catalog_refreshed",
                json!({ "new_namespaces": r.new_namespaces, "changed": r.changed }),
            ),
            Err(e) => log::emit(
                Level::Warn,
                "catalog_refresh_failed",
                json!({ "error": e.to_string() }),
            ),
        }
    }
}

/// Resolves when the listener should close. SIGTERM (an orchestrator) drains
/// first: `/readyz` turns 503 and queries are still served for `delay`
/// (ADR 0026). ctrl-c (a person at a terminal) closes at once.
async fn shutdown_signal(state: Arc<AppState>, delay: std::time::Duration) {
    let ctrl_c = async {
        if let Err(e) = tokio::signal::ctrl_c().await {
            log::emit(
                Level::Error,
                "signal_handler_failed",
                json!({ "signal": "SIGINT", "error": e.to_string() }),
            );
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
                log::emit(
                    Level::Error,
                    "signal_handler_failed",
                    json!({ "signal": "SIGTERM", "error": e.to_string() }),
                );
                std::future::pending::<()>().await;
            }
        }
    };
    #[cfg(not(unix))]
    let term = std::future::pending::<()>();

    let delay = tokio::select! {
        () = ctrl_c => std::time::Duration::ZERO,
        () = term => delay,
    };
    log::emit(
        Level::Info,
        "draining",
        json!({ "serving_for_ms": delay.as_millis() }),
    );
    app::drain(&state, delay).await;
    log::emit(Level::Info, "closing_listener", json!({}));
}

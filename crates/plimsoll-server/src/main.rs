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
use plimsoll_core::datafusion::execution::session_state::SessionStateBuilder;
use plimsoll_core::engine::Engine;
use plimsoll_core::memory::CeilingPool;
use plimsoll_server::app::{self, AppState};
use plimsoll_server::auth::Auth;
use plimsoll_server::config::{AdmissionScope, Config};
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

    let registry = Arc::new(QueryRegistry::new(RETAINED_QUERIES).starting_at(first_query_id()));
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
    let engine = Arc::new(
        Engine::new(pool.clone(), spill_dir)?
            .with_result_cache(usize::try_from(cfg.result_cache_bytes)?),
    );
    // A private CA for HTTPS between nodes (ADR 0099), before any client.
    if let Some(ca) = &cfg.tls_ca {
        let pem =
            std::fs::read(ca).map_err(|e| format!("PLIMSOLL_TLS_CA {}: {e}", ca.display()))?;
        let n = plimsoll_connectors::remote::trust(&pem)
            .map_err(|e| format!("PLIMSOLL_TLS_CA {}: {e}", ca.display()))?;
        if n == 0 {
            return Err(format!("PLIMSOLL_TLS_CA {}: no certificate", ca.display()).into());
        }
    }
    // The other coordinators (ADR 0102): found and health-checked as
    // workers are, so replicas join and leave without a restart.
    let peers = match (cfg.coordinators.is_empty(), &cfg.worker_token) {
        (false, Some(token)) => Some(Arc::new(plimsoll_server::peers::Peers(
            plimsoll_connectors::remote::Workers::new(cfg.coordinators.clone(), token.clone())?,
        ))),
        _ => None,
    };
    // ADR 0103: the limits are the cluster's; this node's part follows the
    // live coordinators, itself counted once. ADR 0104–0107: exactly,
    // through the elected leader, with the parts as the fallback while it
    // cannot be reached.
    let coordination = peers.as_ref().and_then(|peers| {
        let gate = (cfg.admission_scope == AdmissionScope::Exact).then(|| {
            Arc::new(plimsoll_server::peers::LeaderGate::new(
                peers.clone(),
                cfg.query_timeout,
            ))
        });
        (cfg.admission_scope != AdmissionScope::Node).then(|| {
            Arc::new(plimsoll_server::peers::Coordination::new(
                peers.clone(),
                admission.clone(),
                gate,
            ))
        })
    });
    if let Some(peers) = peers.clone() {
        let coordination = coordination.clone();
        tokio::spawn(async move {
            let mut pace = plimsoll_server::peers::Pace::default();
            loop {
                peers
                    .0
                    .check_health(std::time::Duration::from_secs(2))
                    .await;
                if let Some(c) = &coordination {
                    c.round(true).await;
                }
                tokio::time::sleep(pace.next(&peers)).await;
            }
        });
    }
    system::register(
        engine.context(),
        admission.registry().clone(),
        peers.clone(),
    )?;
    // Coordinator (ADR 0058): Iceberg scans fan out to these workers.
    if let (false, Some(token)) = (cfg.workers.is_empty(), &cfg.worker_token) {
        let workers = Arc::new(
            plimsoll_connectors::remote::Workers::new(cfg.workers.clone(), token.clone())?
                .with_broadcast_limit(cfg.broadcast_max_bytes)
                .with_build_cache(cfg.broadcast_cache_bytes)
                .with_skew(cfg.skew_bytes),
        );
        {
            // count/sum/min/max partials computed on workers (ADR 0061),
            // appended after DataFusion's own physical rules.
            let mut state = SessionStateBuilder::new_from_existing(engine.context().state())
                .with_physical_optimizer_rule(Arc::new(
                    plimsoll_connectors::remote::PartialAggregatePushdown,
                ))
                .build();
            state.config_mut().set_extension(workers.clone());
            *engine.context().state_ref().write() = state;
        }
        // Fragments only go to workers whose /readyz answers (ADR 0059): a
        // draining or dead worker drops out within one interval; `dns+`
        // entries are re-resolved each round, so workers join and leave
        // without a restart (ADR 0070).
        tokio::spawn(async move {
            let mut last = (Vec::new(), Vec::new());
            loop {
                workers
                    .check_health(std::time::Duration::from_secs(2))
                    .await;
                let now = (workers.urls(), workers.healthy_urls());
                if now != last {
                    log::emit(
                        if now.1.len() < now.0.len() {
                            Level::Warn
                        } else {
                            Level::Info
                        },
                        "workers_health",
                        json!({ "healthy": now.1, "total": now.0.len(), "workers": now.0 }),
                    );
                    last = now;
                }
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            }
        });
    }
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
        // Policy-driven maintenance (ADR 0086), queued like any query.
        if !cfg.maintenance_interval.is_zero() {
            tokio::spawn(plimsoll_server::schedule::every(
                engine.context().clone(),
                catalog.clone(),
                admission.clone(),
                cfg.maintenance_interval,
                // Who holds maintenance claims (ADR 0101), for whoever
                // reads a lease object.
                format!(
                    "{}/{}",
                    std::env::var("HOSTNAME").unwrap_or_else(|_| "plimsoll".into()),
                    std::process::id()
                ),
            ));
        }
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
            result_cache: stats_engine.result_cache_stats(),
        }),
        query_timeout: cfg.query_timeout,
        ready: AtomicBool::new(false),
        serving: AtomicBool::new(false),
        started: Instant::now(),
        errors: Default::default(),
        fragments: Default::default(),
        latency: Default::default(),
        auth: cfg.auth.clone(),
        tracer,
        slow_query: cfg.slow_query,
        worker_token: cfg
            .worker_token
            .as_deref()
            .map(plimsoll_server::auth::token_digest),
        peers,
        coordination,
    });

    // ADR 0100: the `/metrics` series, pushed to the OTLP endpoint.
    let metrics_stop = Arc::new(tokio::sync::Notify::new());
    let metrics_export = match (cfg.otlp.clone(), cfg.otlp_metrics_interval) {
        (Some(otlp), Some(every)) => {
            let st = state.clone();
            Some(plimsoll_server::metrics::spawn_export(
                otlp,
                every,
                move || st.exposition(),
                metrics_stop.clone(),
            )?)
        }
        _ => None,
    };

    let listener = tokio::net::TcpListener::bind(cfg.bind).await?;
    let flight = match cfg.flight_bind {
        Some(addr) => Some(tokio::net::TcpListener::bind(addr).await?),
        None => None,
    };
    // Loaded before listening: a bad certificate fails the boot (ADR 0099).
    let tls = cfg
        .tls
        .as_ref()
        .map(plimsoll_server::tls::acceptor)
        .transpose()
        .map_err(|e| format!("TLS: {e}"))?;
    log::emit(
        Level::Info,
        "listening",
        json!({
            "addr": listener.local_addr()?.to_string(),
            "catalog": catalog.as_ref().map(|c| c.uri.as_str()),
            "auth": format!("{:?}", cfg.auth),
            "otlp": cfg.otlp.as_ref().map(|o| o.endpoint.as_str()),
            "workers": cfg.workers,
            "worker": cfg.worker_token.is_some(),
            "tls": tls.is_some(),
            "flight": flight.as_ref().and_then(|l| l.local_addr().ok()).map(|a| a.to_string()),
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

    let router = app::router(state.clone());
    // ADR 0124: Flight SQL in front of the same router, closed with it.
    let flight_stop = Arc::new(tokio::sync::Notify::new());
    let flight = flight.map(|listener| {
        let gateway = plimsoll_server::flight::Gateway::new(router.clone(), cfg.auth.clone());
        let (tls, stop) = (cfg.tls.clone(), flight_stop.clone());
        tokio::spawn(async move {
            let stopped = async move { stop.notified().await };
            if let Err(e) =
                plimsoll_server::flight::serve(gateway, listener, tls.as_ref(), stopped).await
            {
                log::emit(Level::Error, "flight_failed", json!({ "error": e }));
            }
        })
    });
    let shutdown = shutdown_signal(state.clone(), cfg.drain_delay);
    match tls {
        Some(acceptor) => {
            let listener = plimsoll_server::tls::TlsListener::new(listener, acceptor)?;
            axum::serve(listener, router)
                .with_graceful_shutdown(shutdown)
                .await?;
        }
        None => {
            axum::serve(listener, router)
                .with_graceful_shutdown(shutdown)
                .await?;
        }
    }
    if let Some(flight) = flight {
        flight_stop.notify_one();
        let _ = tokio::time::timeout(std::time::Duration::from_secs(5), flight).await;
    }
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
    // The last metrics, under the same deadline.
    if let Some(export) = metrics_export {
        metrics_stop.notify_one();
        if tokio::time::timeout(std::time::Duration::from_secs(5), export)
            .await
            .is_err()
        {
            log::emit(Level::Warn, "metrics_flush_timeout", json!({}));
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

/// Where this node's query ids start (ADR 0101): a random multiple of 2^32
/// below 2^53, so coordinators behind one load balancer hand out ids that
/// do not meet (a cancel cannot reach another's query), and ids stay exact
/// as JSON numbers.
fn first_query_id() -> u64 {
    use ring::rand::SecureRandom;
    let mut bytes = [0u8; 8];
    // Only a broken OS fails here; 1 is still a valid start.
    if ring::rand::SystemRandom::new().fill(&mut bytes).is_err() {
        return 1;
    }
    ((u64::from_le_bytes(bytes) & 0x1F_FFFF) << 32) | 1
}

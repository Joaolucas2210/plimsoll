//! Coordinator/worker scans (ADR 0058) against Spark-written tables: the
//! same answers distributed as local, deletes applied on the workers.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::{Duration, Instant};

use plimsoll_connectors::remote::Workers;
use plimsoll_connectors::{Connector, IcebergConnector};
use plimsoll_control::governance::{Admission, AdmissionConfig, Limits, QueryRegistry};
use plimsoll_core::datafusion::arrow::util::pretty::pretty_format_batches;
use plimsoll_core::engine::Engine;
use plimsoll_core::memory::CeilingPool;
use plimsoll_server::app::{AppState, router};
use plimsoll_server::auth::{Auth, token_digest};
use plimsoll_server::engine::CoreEngine;
use plimsoll_server::metrics::PoolStats;

const TOKEN: &str = "a-worker-token-for-tests";

/// A worker node: the real router and engine on a loopback port.
async fn worker() -> String {
    let pool = Arc::new(CeilingPool::new(256 << 20, 8 << 20).unwrap());
    let limits = Limits {
        max_running: 8,
        max_queued: 8,
    };
    let cfg = AdmissionConfig {
        global: limits,
        per_group: limits,
        per_user: limits,
        groups: Default::default(),
    };
    let state = Arc::new(AppState {
        engine: Arc::new(CoreEngine(Arc::new(Engine::new(pool, None).unwrap()))),
        admission: Admission::new(cfg, Arc::new(QueryRegistry::new(10))),
        pool_stats: Arc::new(PoolStats::default),
        query_timeout: Duration::from_secs(30),
        ready: AtomicBool::new(true),
        serving: AtomicBool::new(true),
        started: Instant::now(),
        errors: Default::default(),
        latency: Default::default(),
        auth: Auth::Disabled,
        tracer: None,
        slow_query: None,
        worker_token: Some(token_digest(TOKEN)),
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, router(state)).await.unwrap() });
    url
}

/// A query's result as text, and the plan's text.
async fn run(engine: &Engine, sql: &str) -> (String, String) {
    let df = engine.context().sql(sql).await.unwrap();
    let plan = df.clone().create_physical_plan().await.unwrap();
    let plan = plimsoll_core::datafusion::physical_plan::displayable(plan.as_ref())
        .indent(true)
        .to_string();
    let out = pretty_format_batches(&engine.sql(sql).await.unwrap().batches)
        .unwrap()
        .to_string();
    (out, plan)
}

#[tokio::test]
#[ignore = "needs PLIMSOLL_ICEBERG_{MOR,EQ,DV} from scripts/spark_mor_fixture.sh"]
async fn distributed_scans_match_local_ones() {
    let tables: Vec<(&str, String)> = ["MOR", "EQ", "DV"]
        .iter()
        .filter_map(|t| Some((*t, std::env::var(format!("PLIMSOLL_ICEBERG_{t}")).ok()?)))
        .collect();
    if tables.is_empty() {
        return;
    }
    let urls = vec![worker().await, worker().await];
    let engine = |distributed: bool| {
        let pool = Arc::new(CeilingPool::new(256 << 20, 8 << 20).unwrap());
        let e = Engine::new(pool, None).unwrap();
        if distributed {
            let w = Workers::new(urls.clone(), TOKEN.into()).unwrap();
            e.context()
                .state_ref()
                .write()
                .config_mut()
                .set_extension(Arc::new(w));
        }
        e
    };
    let (local, coordinator) = (engine(false), engine(true));
    for (name, location) in &tables {
        for e in [&local, &coordinator] {
            IcebergConnector::try_new(name.to_lowercase(), location, &e.context().state())
                .await
                .unwrap()
                .register(e.context())
                .unwrap();
        }
        let t = name.to_lowercase();
        for sql in [
            format!("SELECT count(*), sum(id) FROM {t}"),
            format!("SELECT id FROM {t} WHERE id % 7 = 1 ORDER BY id"),
            format!("SELECT max(id), min(id) FROM {t}"),
        ] {
            let (want, plan) = run(&local, &sql).await;
            assert!(!plan.contains("RemoteScanExec"), "{plan}");
            let (got, plan) = run(&coordinator, &sql).await;
            assert!(plan.contains("RemoteScanExec"), "{sql}: {plan}");
            assert_eq!(got, want, "{sql}");
        }
    }
    // A worker that is down fails the query; it never returns less.
    let dead = Engine::new(Arc::new(CeilingPool::new(64 << 20, 0).unwrap()), None).unwrap();
    let w = Workers::new(vec!["http://127.0.0.1:9".into()], TOKEN.into()).unwrap();
    dead.context()
        .state_ref()
        .write()
        .config_mut()
        .set_extension(Arc::new(w));
    let (name, location) = &tables[0];
    IcebergConnector::try_new(name.to_lowercase(), location, &dead.context().state())
        .await
        .unwrap()
        .register(dead.context())
        .unwrap();
    let err = dead
        .sql(&format!("SELECT sum(id) FROM {}", name.to_lowercase()))
        .await
        .unwrap_err();
    assert!(err.to_string().contains("worker"), "{err}");
    // A wrong token is refused by the worker.
    let w = Workers::new(urls.clone(), "the-wrong-token-xyz".into()).unwrap();
    dead.context()
        .state_ref()
        .write()
        .config_mut()
        .set_extension(Arc::new(w));
    let err = dead
        .sql(&format!("SELECT sum(id) FROM {}", name.to_lowercase()))
        .await
        .unwrap_err();
    assert!(err.to_string().contains("401"), "{err}");
}

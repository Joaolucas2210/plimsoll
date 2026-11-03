//! Coordinator/worker scans (ADR 0058) against Spark-written tables: the
//! same answers distributed as local, deletes applied on the workers.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;

use futures::StreamExt;
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
    worker_with(Default::default()).await.0
}

/// A worker with these resource groups; its state too, to poke at.
async fn worker_with(groups: std::collections::HashMap<String, Limits>) -> (String, Arc<AppState>) {
    let pool = Arc::new(CeilingPool::new(256 << 20, 8 << 20).unwrap());
    let limits = Limits {
        max_running: 8,
        max_queued: 8,
    };
    let cfg = AdmissionConfig {
        global: limits,
        per_group: limits,
        per_user: limits,
        groups,
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
    let served = state.clone();
    tokio::spawn(async move { axum::serve(listener, router(served)).await.unwrap() });
    (url, state)
}

/// Makes `engine` a coordinator, as `main` does (ADR 0058, 0061).
fn coordinate(engine: &Engine, workers: Workers) {
    use plimsoll_core::datafusion::execution::session_state::SessionStateBuilder;
    let mut state = SessionStateBuilder::new_from_existing(engine.context().state())
        .with_physical_optimizer_rule(Arc::new(
            plimsoll_connectors::remote::PartialAggregatePushdown,
        ))
        .build();
    state.config_mut().set_extension(Arc::new(workers));
    *engine.context().state_ref().write() = state;
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
#[ignore = "needs PLIMSOLL_ICEBERG_{MOR,DV} from scripts/spark_mor_fixture.sh"]
async fn shuffles_survive_a_lost_worker() {
    let tables: Vec<String> = ["MOR", "DV"]
        .iter()
        .filter_map(|t| std::env::var(format!("PLIMSOLL_ICEBERG_{t}")).ok())
        .collect();
    if tables.is_empty() {
        return;
    }
    // Two live workers and one that is gone. No health checks run, so the
    // dead one is planned in: it owns maps, a reduce and scan fragments.
    let urls = vec![
        worker().await,
        worker().await,
        "http://127.0.0.1:9".to_owned(),
    ];
    let pool = || Arc::new(CeilingPool::new(256 << 20, 8 << 20).unwrap());
    let local = Engine::new(pool(), None).unwrap();
    let coordinator = Engine::new(pool(), None).unwrap();
    coordinate(&coordinator, Workers::new(urls, TOKEN.into()).unwrap());
    {
        let state = coordinator.context().state_ref();
        let mut state = state.write();
        let optimizer = &mut state.config_mut().options_mut().optimizer;
        optimizer.hash_join_single_partition_threshold = 0;
        optimizer.hash_join_single_partition_threshold_rows = 0;
    }
    for (i, location) in tables.iter().enumerate() {
        let t = format!("t{i}");
        for e in [&local, &coordinator] {
            IcebergConnector::try_new(t.clone(), location, &e.context().state())
                .await
                .unwrap()
                .register(e.context())
                .unwrap();
        }
        for (sql, shuffled) in [
            (
                format!("SELECT id % 7 AS k, count(*), sum(id) FROM {t} GROUP BY k ORDER BY k"),
                "shuffled aggregate",
            ),
            (
                format!("SELECT id, count(*) FROM {t} GROUP BY id ORDER BY id"),
                "shuffled aggregate",
            ),
            (
                format!(
                    "SELECT a.id, b.id AS b FROM {t} a JOIN {t} b ON a.id = b.id ORDER BY a.id"
                ),
                "shuffled Inner join",
            ),
        ] {
            let (want, _) = run(&local, &sql).await;
            let (got, plan) = run(&coordinator, &sql).await;
            assert!(
                plan.contains(shuffled) && plan.contains("partitions=3"),
                "{sql}: {plan}"
            );
            assert_eq!(got, want, "{sql}");
        }
    }
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
            coordinate(&e, Workers::new(urls.clone(), TOKEN.into()).unwrap());
            // Fixture tables are tiny: without this every join would
            // collect its build side instead of partitioning (ADR 0065).
            let state = e.context().state_ref();
            let mut state = state.write();
            let optimizer = &mut state.config_mut().options_mut().optimizer;
            optimizer.hash_join_single_partition_threshold = 0;
            optimizer.hash_join_single_partition_threshold_rows = 0;
        }
        e
    };
    let (local, coordinator) = (engine(false), engine(true));
    // Joins of two remote scans shuffle between workers (ADR 0065).
    let joins_match = async |sql: &str| {
        let (want, _) = run(&local, sql).await;
        let (got, plan) = run(&coordinator, sql).await;
        assert!(
            plan.contains("shuffled") && plan.contains(" join"),
            "{sql}: {plan}"
        );
        assert_eq!(got, want, "{sql}");
    };
    for (name, location) in &tables {
        for e in [&local, &coordinator] {
            IcebergConnector::try_new(name.to_lowercase(), location, &e.context().state())
                .await
                .unwrap()
                .register(e.context())
                .unwrap();
        }
        let t = name.to_lowercase();
        let group = if *name == "EQ" { "region" } else { "name" };
        for (sql, pushed) in [
            (format!("SELECT count(*), sum(id) FROM {t}"), true),
            (
                format!("SELECT id FROM {t} WHERE id % 7 = 1 ORDER BY id"),
                false,
            ),
            (format!("SELECT max(id), min(id) FROM {t}"), true),
            // Partial aggregation on workers (ADR 0061), grouped and with
            // a count over a nullable column.
            (
                format!(
                    "SELECT {group}, count(*), count({group}), sum(id), min(id), max(id) \
                     FROM {t} GROUP BY {group} ORDER BY {group}"
                ),
                true,
            ),
            // Row filters travel with the fragment (ADR 0062), so the
            // filtered aggregation is pushed too.
            (
                format!("SELECT count(*), sum(id) FROM {t} WHERE id > 3"),
                true,
            ),
            (
                format!(
                    "SELECT {group}, count(*) FROM {t} \
                     WHERE {group} IS NOT NULL AND id % 2 = 0 GROUP BY {group} ORDER BY {group}"
                ),
                true,
            ),
            // DataFusion plans count(DISTINCT id) as GROUP BY id under a
            // count: that inner grouping is pushed, workers return their
            // distinct ids and the coordinator dedups.
            (format!("SELECT count(DISTINCT id) FROM {t}"), true),
            // Expression keys and arguments travel as SQL (ADR 0063).
            (
                format!("SELECT id % 3, count(*) FROM {t} GROUP BY id % 3 ORDER BY 1"),
                true,
            ),
            (
                format!(
                    "SELECT lower({group}) AS g, sum(id * 2), max(-id), count(id + 1) \
                     FROM {t} GROUP BY lower({group}) ORDER BY g"
                ),
                true,
            ),
            (
                format!("SELECT CAST(id AS INT) % 2 AS k, sum(id) FROM {t} GROUP BY k ORDER BY k"),
                true,
            ),
            // High cardinality: one group per row, merged on workers.
            (
                format!("SELECT id, count(*), max({group}) FROM {t} GROUP BY id ORDER BY id"),
                true,
            ),
            (
                format!(
                    "SELECT id % 5 AS k, sum(id) FROM {t} GROUP BY k HAVING sum(id) > 10 ORDER BY k LIMIT 3"
                ),
                true,
            ),
            // Not pushed: avg's partial state (count, sum) is not its value.
            (format!("SELECT avg(id) FROM {t}"), false),
        ] {
            let (want, plan) = run(&local, &sql).await;
            assert!(!plan.contains("RemoteScanExec"), "{plan}");
            let (got, plan) = run(&coordinator, &sql).await;
            assert!(plan.contains("Remote"), "{sql}: {plan}");
            let on_workers = plan.contains("aggregate=[") || plan.contains("shuffled aggregate");
            assert_eq!(on_workers, pushed, "{sql}: {plan}");
            assert_eq!(got, want, "{sql}");
            // Grouped aggregations over two fragments or more shuffle
            // between workers (ADR 0064), count(DISTINCT)'s inner
            // grouping too: a partial count above it is free to take any
            // partitioning (ADR 0065).
            let grouped = sql.contains("GROUP BY") || sql.contains("DISTINCT");
            let shuffles = pushed && grouped && !plan.contains("fragments=1 ");
            assert_eq!(plan.contains(" shuffled"), shuffles, "{sql}: {plan}");
            if sql.contains("WHERE") {
                // Applied by the scan, here and on the workers.
                assert!(plan.contains("filters=["), "{sql}: {plan}");
                assert!(!plan.contains("FilterExec"), "{sql}: {plan}");
            }
        }
        // A grouped aggregation feeding a join: shuffled only where the
        // join does not rely on its hash partitioning; same answers.
        let join = format!(
            "SELECT a.id, a.c, b.id AS b FROM (SELECT id, count(*) AS c FROM {t} GROUP BY id) a \
             JOIN {t} b ON a.id = b.id ORDER BY a.id, b.id"
        );
        assert_eq!(
            run(&coordinator, &join).await.0,
            run(&local, &join).await.0,
            "{join}"
        );
        // The local scan's own filtering, checked against a filter it
        // never sees (an aggregate's FILTER clause).
        let (filtered, plan) = run(
            &local,
            &format!("SELECT count(*) AS n, sum(id) AS s FROM {t} WHERE id > 3"),
        )
        .await;
        assert!(plan.contains("FilterExec"), "{plan}");
        let (reference, _) = run(
            &local,
            &format!("SELECT count(*) FILTER (WHERE id > 3) AS n, sum(id) FILTER (WHERE id > 3) AS s FROM {t}"),
        )
        .await;
        assert_eq!(filtered, reference);
        for sql in [
            format!(
                "SELECT a.id, b.{group} FROM {t} a JOIN {t} b ON a.id = b.id ORDER BY a.id, b.{group}"
            ),
            // Outer, with a filtered side (filters travel, ADR 0062).
            format!(
                "SELECT a.id, b.id AS b FROM {t} a \
                 LEFT JOIN (SELECT id FROM {t} WHERE id % 2 = 0) b ON a.id = b.id ORDER BY a.id"
            ),
            // Semi and anti joins.
            format!(
                "SELECT id FROM {t} WHERE id IN (SELECT id FROM {t} WHERE id % 3 = 0) ORDER BY id"
            ),
            // (Unordered: with ORDER BY, DataFusion may sort the probe
            // side below the join and keep its order, which a shuffle
            // could not; such joins stay as planned.)
            format!(
                "SELECT count(*), sum(id) FROM {t} a WHERE NOT EXISTS \
                 (SELECT 1 FROM {t} b WHERE b.id = a.id AND b.id > 5)"
            ),
            // Joins with a residual filter (ADR 0098), shuffled.
            format!(
                "SELECT a.id, b.id AS b FROM {t} a JOIN {t} b ON a.id = b.id \
                 AND a.id * 2 + b.id > 12 ORDER BY a.id, b"
            ),
            format!(
                "SELECT count(*), sum(a.id), count(b.id) FROM {t} a LEFT JOIN {t} b \
                 ON a.id = b.id AND a.id + b.id > 10"
            ),
        ] {
            joins_match(&sql).await;
        }
    }
    // Across tables, and two keys.
    if tables.len() > 1 {
        let (a, b) = (tables[0].0.to_lowercase(), tables[1].0.to_lowercase());
        joins_match(&format!(
            "SELECT x.id, y.id AS y FROM {a} x FULL JOIN {b} y ON x.id = y.id ORDER BY x.id, y"
        ))
        .await;
        joins_match(&format!(
            "SELECT x.name, count(*) FROM {a} x JOIN {a} y ON x.id = y.id AND x.name = y.name \
             GROUP BY x.name ORDER BY x.name"
        ))
        .await;
    }
    // Broadcast joins (ADR 0066): with DataFusion's default thresholds a
    // fixture-sized build side is collected, and sent to the workers with
    // each probe fragment.
    let collect_left = |on: bool| {
        let state = coordinator.context().state_ref();
        let mut state = state.write();
        let optimizer = &mut state.config_mut().options_mut().optimizer;
        let (bytes, rows) = if on { (1 << 20, 128 * 1024) } else { (0, 0) };
        optimizer.hash_join_single_partition_threshold = bytes;
        optimizer.hash_join_single_partition_threshold_rows = rows;
    };
    collect_left(true);
    for (name, _) in &tables {
        let t = name.to_lowercase();
        for (sql, broadcast) in [
            (
                format!(
                    "SELECT a.id, b.id AS b FROM {t} a JOIN {t} b ON a.id = b.id ORDER BY a.id"
                ),
                Some(true),
            ),
            (
                format!(
                    "SELECT count(*), sum(a.id) FROM {t} a \
                     JOIN (SELECT id FROM {t} WHERE id < 5) b ON a.id = b.id"
                ),
                Some(true),
            ),
            // DataFusion sorts the probe side below the join for the ORDER
            // BY; that order would not survive the workers.
            (
                format!(
                    "SELECT a.id FROM {t} a JOIN (SELECT id FROM {t} WHERE id < 5) b \
                     ON a.id = b.id WHERE a.id % 2 = 0 ORDER BY a.id"
                ),
                Some(false),
            ),
            // Driven by the build side (ADR 0097): workers return which
            // build rows matched, the coordinator adds the build-only rows.
            (
                format!(
                    "SELECT id FROM {t} WHERE id IN (SELECT id FROM {t} WHERE id % 3 = 0) ORDER BY id"
                ),
                Some(true),
            ),
            (
                format!(
                    "SELECT a.id, b.id AS b FROM (SELECT id FROM {t} WHERE id < 8) a \
                     LEFT JOIN (SELECT id FROM {t} WHERE id % 2 = 0) b ON a.id = b.id ORDER BY a.id"
                ),
                Some(true),
            ),
            (
                format!(
                    "SELECT a.id, b.id AS b FROM (SELECT id FROM {t} WHERE id < 8) a \
                     FULL JOIN (SELECT id FROM {t} WHERE id % 2 = 0) b ON a.id = b.id \
                     ORDER BY a.id, b"
                ),
                Some(true),
            ),
            (
                format!(
                    "SELECT a.id FROM (SELECT id FROM {t} WHERE id < 8) a \
                     WHERE EXISTS (SELECT 1 FROM {t} b WHERE b.id = a.id AND b.id % 2 = 0) ORDER BY a.id"
                ),
                Some(true),
            ),
            (
                format!(
                    "SELECT a.id FROM (SELECT id FROM {t} WHERE id < 8) a \
                     WHERE NOT EXISTS (SELECT 1 FROM {t} b WHERE b.id = a.id AND b.id % 2 = 0) \
                     ORDER BY a.id"
                ),
                Some(true),
            ),
            // With residual filters (ADR 0098).
            (
                format!(
                    "SELECT a.id, b.id AS b FROM {t} a JOIN (SELECT id FROM {t} WHERE id < 6) b \
                     ON a.id = b.id AND a.id + b.id > 4 ORDER BY a.id, b"
                ),
                None,
            ),
            (
                format!(
                    "SELECT a.id, b.id AS b FROM (SELECT id FROM {t} WHERE id < 8) a \
                     LEFT JOIN (SELECT id FROM {t} WHERE id % 2 = 0) b \
                     ON a.id = b.id AND a.id + b.id > 6 ORDER BY a.id"
                ),
                Some(true),
            ),
            (
                format!(
                    "SELECT a.id FROM (SELECT id FROM {t} WHERE id < 8) a WHERE EXISTS \
                     (SELECT 1 FROM {t} b WHERE b.id = a.id AND b.id + a.id > 6) ORDER BY a.id"
                ),
                Some(true),
            ),
            // NOT IN is null-aware: it stays as planned.
            (
                format!(
                    "SELECT id FROM {t} WHERE id NOT IN (SELECT id FROM {t} WHERE id % 3 = 0) ORDER BY id"
                ),
                Some(false),
            ),
            // Swapped by DataFusion into a left join with the smaller side
            // collected (gathered, ADR 0097), or kept as a right join over a
            // sorted probe side (local): either way, the same answer.
            (
                format!(
                    "SELECT x.id, y.id AS y FROM {t} x RIGHT JOIN (SELECT id FROM {t} WHERE id > 3) y \
                     ON x.id = y.id ORDER BY y"
                ),
                None,
            ),
        ] {
            let (want, _) = run(&local, &sql).await;
            let (got, plan) = run(&coordinator, &sql).await;
            if let Some(broadcast) = broadcast {
                assert_eq!(plan.contains("RemoteBroadcast"), broadcast, "{sql}: {plan}");
            }
            assert_eq!(got, want, "{sql}");
        }
    }
    collect_left(false);
    // Grouped by the join key: the final aggregation may rely on the
    // join's partitioning, so the join must then stay as planned.
    let (a, _) = &tables[0];
    let keyed = format!(
        "SELECT x.id, count(*) FROM {a} x JOIN {a} y ON x.id = y.id GROUP BY x.id ORDER BY x.id",
        a = a.to_lowercase()
    );
    let (got, plan) = run(&coordinator, &keyed).await;
    assert_eq!(got, run(&local, &keyed).await.0);
    assert!(!plan.contains("shuffled"), "{plan}");
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

/// Step 2 (ADR 0059): admission, health and identity on workers.
#[tokio::test]
#[ignore = "needs PLIMSOLL_ICEBERG_MOR from scripts/spark_mor_fixture.sh"]
async fn workers_govern_fragments_and_report_health() {
    use std::sync::atomic::Ordering;
    let Ok(location) = std::env::var("PLIMSOLL_ICEBERG_MOR") else {
        return;
    };
    let one = Limits {
        max_running: 1,
        max_queued: 0,
    };
    let (url, state) = worker_with([("fragments".to_owned(), one)].into()).await;
    let workers = Arc::new(Workers::new(vec![url], TOKEN.into()).unwrap());
    let coordinator = Engine::new(
        Arc::new(CeilingPool::new(256 << 20, 8 << 20).unwrap()),
        None,
    )
    .unwrap();
    coordinator
        .context()
        .state_ref()
        .write()
        .config_mut()
        .set_extension(workers.clone());
    IcebergConnector::try_new("mor", &location, &coordinator.context().state())
        .await
        .unwrap()
        .register(coordinator.context())
        .unwrap();
    let sql = "SELECT count(*), sum(id) FROM mor";
    let (want, plan) = run(&coordinator, sql).await;
    assert!(plan.contains("RemoteScanExec"), "{plan}");
    assert!(want.contains("| 32       | 768"), "{want}");

    // The fragments group is full: the worker answers 429 and the query
    // fails cleanly (retryable upstream); once free, it runs again.
    let held = state
        .admission
        .acquire("someone", "fragments")
        .await
        .unwrap();
    let err = coordinator.sql(sql).await.unwrap_err();
    assert!(err.to_string().contains("429"), "{err}");
    drop(held);
    assert_eq!(run(&coordinator, sql).await.0, want);

    // A worker that stops being ready leaves the rotation: the scan runs
    // locally with the same answer; ready again, it comes back.
    state.ready.store(false, Ordering::Relaxed);
    assert_eq!(workers.check_health(Duration::from_secs(2)).await, 0);
    let (got, plan) = run(&coordinator, sql).await;
    assert!(!plan.contains("RemoteScanExec"), "{plan}");
    assert_eq!(got, want);
    state.ready.store(true, Ordering::Relaxed);
    assert_eq!(workers.check_health(Duration::from_secs(2)).await, 1);
    assert!(run(&coordinator, sql).await.1.contains("RemoteScanExec"));
}

/// The coordinator's query identity reaches the worker as headers.
#[tokio::test]
async fn fragments_carry_the_query_identity() {
    use plimsoll_core::datafusion::arrow::array::Int64Array;
    use plimsoll_core::datafusion::arrow::datatypes::{DataType, Field, Schema};
    use plimsoll_core::datafusion::arrow::ipc::writer::StreamWriter;
    use plimsoll_core::datafusion::arrow::record_batch::RecordBatch;
    use plimsoll_core::datafusion::physical_plan::{ExecutionPlan, collect};
    use plimsoll_core::engine::QueryTag;
    use tokio::sync::mpsc;

    // A fake worker: records the headers, answers one row as Arrow IPC.
    let (tx, mut rx) = mpsc::unbounded_channel();
    let app = axum::Router::new().route(
        "/v1/fragment",
        axum::routing::post(move |headers: axum::http::HeaderMap| async move {
            let get = |k: &str| headers.get(k).map(|v| v.to_str().unwrap().to_owned());
            tx.send((
                get("x-plimsoll-query-id"),
                get("x-plimsoll-user"),
                get("traceparent"),
                get("x-plimsoll-deadline-ms"),
                get("authorization"),
            ))
            .unwrap();
            let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
            let batch =
                RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![7]))])
                    .unwrap();
            let mut w = StreamWriter::try_new(Vec::new(), &schema).unwrap();
            w.write(&batch).unwrap();
            w.finish().unwrap();
            w.into_inner().unwrap()
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    let workers = Arc::new(Workers::new(vec![url.clone()], TOKEN.into()).unwrap());
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let plan = Arc::new(plimsoll_connectors::remote::RemoteScanExec::new(
        schema,
        workers,
        &[url],
        "file:///t/metadata/v1.metadata.json",
        Some(1),
        vec![("file:///t/data/a.parquet".into(), Some(10))],
        vec!["id".into()],
    ));
    let tag = |deadline: Duration| QueryTag {
        query_id: 7,
        user: "ana".into(),
        traceparent: Some("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".into()),
        deadline: Some(Instant::now() + deadline),
    };
    let ctx = plimsoll_core::datafusion::prelude::SessionContext::new_with_config(
        plimsoll_core::datafusion::prelude::SessionConfig::new()
            .with_extension(Arc::new(tag(Duration::from_secs(30)))),
    );
    let out = collect(plan.clone() as Arc<dyn ExecutionPlan>, ctx.task_ctx())
        .await
        .unwrap();
    assert_eq!(out[0].num_rows(), 1);
    let (id, user, tp, deadline, auth) = rx.recv().await.unwrap();
    assert_eq!(id.as_deref(), Some("7"));
    assert_eq!(user.as_deref(), Some("ana"));
    assert!(
        tp.unwrap()
            .starts_with("00-4bf92f3577b34da6a3ce929d0e0e4736-")
    );
    let left: u64 = deadline.unwrap().parse().unwrap();
    assert!(left > 25_000 && left <= 30_000, "{left}");
    assert_eq!(auth.as_deref(), Some(format!("Bearer {TOKEN}").as_str()));
    // A deadline already gone: no request is sent at all.
    let late = plimsoll_core::datafusion::prelude::SessionContext::new_with_config(
        plimsoll_core::datafusion::prelude::SessionConfig::new()
            .with_extension(Arc::new(tag(Duration::ZERO))),
    );
    let err = collect(plan as Arc<dyn ExecutionPlan>, late.task_ctx())
        .await
        .unwrap_err();
    assert!(err.to_string().contains("deadline"), "{err}");
}

/// How a fake worker answers (ADR 0060 retry tests).
#[derive(Clone, Copy)]
enum Fake {
    Ok,
    Status(u16),
    /// One batch, then the connection drops (no end-of-stream marker).
    Partial,
}

/// A fake worker; returns its URL and a request counter.
async fn fake(behaviour: Fake) -> (String, Arc<std::sync::atomic::AtomicUsize>) {
    use plimsoll_core::datafusion::arrow::array::Int64Array;
    use plimsoll_core::datafusion::arrow::datatypes::{DataType, Field, Schema};
    use plimsoll_core::datafusion::arrow::ipc::writer::StreamWriter;
    use plimsoll_core::datafusion::arrow::record_batch::RecordBatch;
    use std::sync::atomic::{AtomicUsize, Ordering};
    let hits = Arc::new(AtomicUsize::new(0));
    let counter = hits.clone();
    let app = axum::Router::new().route(
        "/v1/fragment",
        axum::routing::post(move || {
            let counter = counter.clone();
            async move {
                use axum::response::IntoResponse;
                counter.fetch_add(1, Ordering::SeqCst);
                let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
                let batch =
                    RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![7]))])
                        .unwrap();
                let mut w = StreamWriter::try_new(Vec::new(), &schema).unwrap();
                w.write(&batch).unwrap();
                match behaviour {
                    Fake::Status(code) => axum::http::StatusCode::from_u16(code)
                        .unwrap()
                        .into_response(),
                    Fake::Ok => {
                        w.finish().unwrap();
                        w.into_inner().unwrap().into_response()
                    }
                    Fake::Partial => {
                        // Not `into_inner`: that would write the marker.
                        let first = std::mem::take(w.get_mut());
                        // The batch reaches the coordinator first; only then
                        // does the worker die.
                        let body =
                            futures::stream::once(async move { Ok::<_, std::io::Error>(first) })
                                .chain(futures::stream::once(async {
                                    tokio::time::sleep(Duration::from_millis(300)).await;
                                    Err(std::io::Error::other("worker died"))
                                }));
                        axum::body::Body::from_stream(body).into_response()
                    }
                }
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (url, hits)
}

/// A fragment that fails before its first batch is retried once on
/// another worker; after a batch, or for a non-retryable answer, never.
#[tokio::test]
async fn fragments_retry_once_before_the_first_batch() {
    use plimsoll_core::datafusion::arrow::datatypes::{DataType, Field, Schema};
    use plimsoll_core::datafusion::physical_plan::{ExecutionPlan, collect};
    use std::sync::atomic::Ordering;
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let run = async |first: Fake| {
        let (a, a_hits) = fake(first).await;
        let (b, b_hits) = fake(Fake::Ok).await;
        let urls = vec![a, b];
        let workers = Arc::new(Workers::new(urls.clone(), TOKEN.into()).unwrap());
        // One file: it belongs to the first worker.
        let plan = Arc::new(plimsoll_connectors::remote::RemoteScanExec::new(
            schema.clone(),
            workers,
            &urls,
            "m",
            None,
            vec![("f".into(), Some(1))],
            vec!["id".into()],
        ));
        let ctx = plimsoll_core::datafusion::prelude::SessionContext::new();
        let out = collect(plan as Arc<dyn ExecutionPlan>, ctx.task_ctx()).await;
        let rows = out.map(|b| b.iter().map(|b| b.num_rows()).sum::<usize>());
        (
            rows,
            a_hits.load(Ordering::SeqCst),
            b_hits.load(Ordering::SeqCst),
        )
    };
    for code in [503, 429] {
        let (rows, a, b) = run(Fake::Status(code)).await;
        assert_eq!(
            (rows.unwrap(), a, b),
            (1, 1, 1),
            "{code}: retried on the other worker"
        );
    }
    // Not retryable: the other worker would refuse the same way.
    let (rows, a, b) = run(Fake::Status(401)).await;
    assert!(rows.unwrap_err().to_string().contains("401"));
    assert_eq!((a, b), (1, 0));
    // A batch was already emitted: a retry could duplicate rows.
    let (rows, a, b) = run(Fake::Partial).await;
    assert!(rows.is_err(), "{rows:?} hits {a} {b}");
    assert_eq!((a, b), (1, 0));
    // Healthy first worker: no second request.
    let (rows, a, b) = run(Fake::Ok).await;
    assert_eq!((rows.unwrap(), a, b), (1, 1, 0));
}

/// Data files written on workers (ADR 0091): an `INSERT … SELECT` from a
/// remote scan writes its files on the workers, the coordinator commits
/// them in one append, and the table holds exactly the selected rows.
#[tokio::test]
#[ignore = "needs PLIMSOLL_CATALOG_URI (iceberg-rest-fixture) and its warehouse's storage"]
async fn writes_on_workers_through_the_rest_catalog() {
    use plimsoll_control::catalog::{Catalog, CatalogConfig, Namespace, TableIdent};
    use plimsoll_control::rest::RestCatalog;
    let Some(cfg) = CatalogConfig::from_lookup(|k| std::env::var(k).ok()).unwrap() else {
        return;
    };
    let catalog: Arc<dyn Catalog> = Arc::new(RestCatalog::connect(cfg).await.unwrap());
    let coordinator = Engine::new(
        Arc::new(CeilingPool::new(256 << 20, 8 << 20).unwrap()),
        None,
    )
    .unwrap();
    plimsoll_server::lakehouse::register(coordinator.context(), catalog.clone())
        .await
        .unwrap();
    coordinate(
        &coordinator,
        Workers::new(vec![worker().await, worker().await], TOKEN.into()).unwrap(),
    );
    let text = async |sql: &str| {
        pretty_format_batches(&coordinator.sql(sql).await.unwrap().batches)
            .unwrap()
            .to_string()
    };
    let id = std::process::id();
    let (src, dst) = (
        format!("bronze.dw_src_{id:x}"),
        format!("bronze.dw_dst_{id:x}"),
    );
    text(&format!(
        "CREATE TABLE {src} (id INT NOT NULL, region VARCHAR, v DOUBLE)"
    ))
    .await;
    // Four appends: four files, for two workers.
    for i in 1..=4 {
        text(&format!(
            "INSERT INTO {src} SELECT CAST(value AS INT) + {i} * 1000, \
             CASE WHEN value % 2 = 0 THEN 'eu' ELSE 'us' END, value * 1.5 \
             FROM generate_series(1, 200)"
        ))
        .await;
    }
    text(&format!(
        "CREATE TABLE {dst} (region VARCHAR, id BIGINT NOT NULL, v DOUBLE) PARTITION BY (region)"
    ))
    .await;
    // Reordered and widened columns, a filter: all done on the workers.
    let insert = format!("INSERT INTO {dst} SELECT region, id, v FROM {src} WHERE v > 100");
    let plan = text(&format!("EXPLAIN {insert}")).await;
    assert!(
        plan.contains("RemoteWriteExec") && plan.contains("source=RemoteScanExec partitions=2"),
        "{plan}"
    );
    assert!(text(&insert).await.contains("| 536   |"));
    let check = "SELECT count(*) AS n, sum(id) AS s, min(v) AS m FROM";
    let (got, want) = (
        text(&format!("{check} {dst}")).await,
        text(&format!("{check} {src} WHERE v > 100")).await,
    );
    assert_eq!(got, want);
    let name = dst.trim_start_matches("bronze.");
    let meta = catalog
        .load_table(&TableIdent::new(Namespace::new(&["bronze"]), name))
        .await
        .unwrap()
        .metadata;
    let snapshots = meta["snapshots"].as_array().unwrap();
    assert_eq!(snapshots.len(), 1, "one commit");
    assert_eq!(snapshots[0]["summary"]["operation"], "append");
    // A computed column is not a fragment's output: written here.
    let plan = text(&format!(
        "EXPLAIN INSERT INTO {dst} SELECT region, max(id), 0.0 FROM {src} GROUP BY region"
    ))
    .await;
    assert!(!plan.contains("RemoteWriteExec"), "{plan}");
    // Post-shuffle (ADR 0092): an aggregate's reducers write its rows.
    let insert =
        format!("INSERT INTO {dst} SELECT region, max(id), max(v) FROM {src} GROUP BY region");
    let plan = text(&format!("EXPLAIN {insert}")).await;
    assert!(plan.contains("source=RemoteReduceExec"), "{plan}");
    assert!(text(&insert).await.contains("| 2     |"));
    // A shuffled join's reducers too.
    {
        let state = coordinator.context().state_ref();
        let mut state = state.write();
        let optimizer = &mut state.config_mut().options_mut().optimizer;
        optimizer.hash_join_single_partition_threshold = 0;
        optimizer.hash_join_single_partition_threshold_rows = 0;
    }
    let insert = format!(
        "INSERT INTO {dst} SELECT a.region, a.id, b.v FROM {src} a JOIN {src} b ON a.id = b.id \
         WHERE a.v > 290"
    );
    let plan = text(&format!("EXPLAIN {insert}")).await;
    assert!(plan.contains("source=RemoteReduceExec"), "{plan}");
    assert!(
        text(&insert).await.contains("| 28    |"),
        "values 194..200 of each of the 4 inserts"
    );
    // CTAS (ADR 0092): a scan's and an aggregate's rows written into the
    // staged table on the workers, one create commit each.
    let (c1, c2) = (format!("{src}_c1"), format!("{src}_c2"));
    assert!(
        text(&format!(
            "CREATE TABLE {c1} AS SELECT id, v FROM {src} WHERE v > 100"
        ))
        .await
        .contains("| 536   |")
    );
    assert!(
        text(&format!(
            "CREATE TABLE {c2} AS SELECT region, count(*) AS n FROM {src} GROUP BY region"
        ))
        .await
        .contains("| 2     |")
    );
    let counts = text(&format!("SELECT region, n FROM {c2} ORDER BY region")).await;
    assert!(
        counts.contains("| eu     | 400 |") && counts.contains("| us     | 400 |"),
        "{counts}"
    );
    // Row-level writes (ADR 0093): DELETE and UPDATE rewrite their files
    // on the workers, and the counts and contents are a local run's.
    let delete = format!("DELETE FROM {c1} WHERE v > 250");
    let plan = text(&format!("EXPLAIN {delete}")).await;
    assert!(
        plan.contains("RemoteWriteExec") && plan.contains("source=RemoteScanExec"),
        "{plan}"
    );
    let expected = text(&format!("SELECT count(*) AS n FROM {c1} WHERE v > 250")).await;
    let deleted = text(&delete).await;
    let (n, d) = (expected.lines().nth(3), deleted.lines().nth(3));
    assert_eq!(
        n.map(|l| l.split('|').nth(1).map(str::trim)),
        d.map(|l| l.split('|').nth(1).map(str::trim))
    );
    assert!(
        text(&format!("SELECT count(*) AS n FROM {c1} WHERE v > 250"))
            .await
            .contains("| 0 |")
    );
    let update = format!("UPDATE {c1} SET v = -v WHERE id % 2 = 0");
    let plan = text(&format!("EXPLAIN {update}")).await;
    assert!(plan.contains("RemoteWriteExec"), "{plan}");
    let before = text(&format!("SELECT count(*) AS n FROM {c1} WHERE id % 2 = 0")).await;
    let updated = text(&update).await;
    assert_eq!(
        before
            .lines()
            .nth(3)
            .map(|l| l.split('|').nth(1).map(str::trim)),
        updated
            .lines()
            .nth(3)
            .map(|l| l.split('|').nth(1).map(str::trim))
    );
    let negative = text(&format!("SELECT count(*) AS n FROM {c1} WHERE v < 0")).await;
    assert_eq!(negative.lines().nth(3), before.lines().nth(3), "{negative}");
    // MERGE (ADR 0094): the target files on the workers, the source sent
    // along; the result is the one a single node computes. `id` and `v`
    // are on both sides, so the clauses rely on qualified names.
    let local = Engine::new(
        Arc::new(CeilingPool::new(256 << 20, 8 << 20).unwrap()),
        None,
    )
    .unwrap();
    plimsoll_server::lakehouse::register(local.context(), catalog.clone())
        .await
        .unwrap();
    let (m1, m2) = (format!("{src}_m1"), format!("{src}_m2"));
    for t in [&m1, &m2] {
        text(&format!(
            "CREATE TABLE {t} PARTITION BY (region) AS SELECT id, region, v FROM {src}"
        ))
        .await;
    }
    let merge = |t: &str| {
        format!(
            "MERGE INTO {t} t USING (SELECT id, v FROM {src} WHERE id % 7 = 0 \
             UNION ALL SELECT id + 100000 AS id, v FROM {src} WHERE id % 50 = 0) s \
             ON t.id = s.id WHEN MATCHED AND s.v > 200 THEN DELETE \
             WHEN MATCHED THEN UPDATE SET v = s.v + 1 \
             WHEN NOT MATCHED THEN INSERT (id, region, v) VALUES (s.id, 'new', s.v) \
             WHEN NOT MATCHED BY SOURCE AND t.id % 100 = 1 THEN DELETE"
        )
    };
    let plan = text(&format!("EXPLAIN {}", merge(&m1))).await;
    assert!(plan.contains("fragments=2"), "{plan}");
    let distributed = text(&merge(&m1)).await;
    let single = pretty_format_batches(&local.sql(&merge(&m2)).await.unwrap().batches)
        .unwrap()
        .to_string();
    assert_eq!(distributed, single);
    let differ = text(&format!(
        "SELECT count(*) AS n FROM {m1} a FULL JOIN {m2} b ON a.id = b.id \
         WHERE a.id IS NULL OR b.id IS NULL OR a.v <> b.v OR a.region <> b.region"
    ))
    .await;
    assert!(differ.contains("| 0 |"), "{differ}");
    let inserted = text(&format!(
        "SELECT count(*) AS n FROM {m1} WHERE region = 'new'"
    ))
    .await;
    assert!(
        inserted.contains("| 16 |"),
        "ids 50..200 step 50, of 4 inserts: {inserted}"
    );
    for t in [src, dst, c1, c2, m1, m2] {
        text(&format!("DROP TABLE {t} PURGE")).await;
    }
}

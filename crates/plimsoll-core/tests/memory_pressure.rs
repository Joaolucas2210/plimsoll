//! Smoke: real DataFusion queries under a small `CeilingPool`.
//! "The query fails. The node doesn't."
//!
//! Uses the DataFrame API: the workspace builds DataFusion without the `sql` feature.

use std::sync::Arc;

use datafusion::arrow::array::{Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::error::{DataFusionError, Result};
use datafusion::physical_plan::collect;
use datafusion::prelude::{JoinType, SessionConfig, SessionContext, col};
use plimsoll_core::memory::{CeilingPool, session_context_with_ceiling, spilled_bytes};

const MIB: usize = 1024 * 1024;

fn config() -> SessionConfig {
    SessionConfig::new()
        .with_target_partitions(1)
        .with_batch_size(8192)
        // Default (10 MiB) would be larger than the whole ceiling.
        .with_sort_spill_reservation_bytes(256 * 1024)
}

/// ~200k rows, ~10 MiB in memory.
fn register_big(ctx: &SessionContext) -> Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("s", DataType::Utf8, false),
    ]));
    let batches = (0..25)
        .map(|b| {
            let ks: Vec<i64> = (0..8192)
                .map(|i| (b * 8192 + i) * 7919 % 1_000_003)
                .collect();
            let ss: Vec<String> = ks.iter().map(|k| format!("row-{k:032}")).collect();
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int64Array::from(ks)),
                    Arc::new(StringArray::from(ss)),
                ],
            )
        })
        .collect::<std::result::Result<Vec<_>, _>>()?;
    ctx.register_table(
        "big",
        Arc::new(MemTable::try_new(schema, vec![batches.clone()])?),
    )?;
    // Same data, renamed columns, for a self-join without ambiguity.
    let renamed = Arc::new(Schema::new(vec![
        Field::new("k2", DataType::Int64, false),
        Field::new("s2", DataType::Utf8, false),
    ]));
    let batches2 = batches
        .into_iter()
        .map(|b| RecordBatch::try_new(Arc::clone(&renamed), b.columns().to_vec()))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    ctx.register_table(
        "big2",
        Arc::new(MemTable::try_new(renamed, vec![batches2])?),
    )?;
    Ok(())
}

#[tokio::test]
async fn query_fails_naming_consumers_and_node_survives() -> Result<()> {
    let pool = Arc::new(CeilingPool::new(4 * MIB, 512 * 1024)?);
    let ctx = session_context_with_ceiling(Arc::clone(&pool), None, config())?;
    register_big(&ctx)?;

    // Hash-join build side is non-spillable: the ceiling's own error reaches the caller.
    // (A sort with spilling disabled fails too, but DataFusion replaces the pool's
    // message with its own "Memory Exhausted while Sorting".)
    let err = ctx
        .table("big")
        .await?
        .join(
            ctx.table("big2").await?,
            JoinType::Inner,
            &["k"],
            &["k2"],
            None,
        )?
        .collect()
        .await
        .err()
        .ok_or_else(|| DataFusionError::Internal("sort should hit the ceiling".into()))?;
    let msg = err.to_string();
    assert!(
        matches!(err.find_root(), DataFusionError::ResourcesExhausted(_)),
        "{err:?}"
    );
    assert!(
        msg.contains("memory ceiling") && msg.contains("top consumers"),
        "{msg}"
    );
    assert!(msg.contains("HashJoinInput"), "{msg}");

    // All reservations released; the same process/pool keeps serving.
    assert_eq!(pool.used(), 0);
    let ok = ctx
        .table("big")
        .await?
        .limit(0, Some(10))?
        .collect()
        .await?;
    assert_eq!(ok.iter().map(|b| b.num_rows()).sum::<usize>(), 10);
    assert_eq!(pool.used(), 0);
    Ok(())
}

#[tokio::test]
async fn sort_spills_under_ceiling_when_disk_configured() -> Result<()> {
    let dir = std::env::temp_dir().join(format!("plimsoll-spill-{}", std::process::id()));
    std::fs::create_dir_all(&dir)?;
    let pool = Arc::new(CeilingPool::new(8 * MIB, 512 * 1024)?);
    let ctx = session_context_with_ceiling(Arc::clone(&pool), Some(dir.clone()), config())?;
    register_big(&ctx)?;

    let df = ctx
        .table("big")
        .await?
        .sort(vec![col("s").sort(false, true)])?;
    let plan = df.create_physical_plan().await?;
    let out = collect(Arc::clone(&plan), ctx.task_ctx()).await?;
    let _ = std::fs::remove_dir_all(&dir);

    assert_eq!(out.iter().map(|b| b.num_rows()).sum::<usize>(), 25 * 8192);
    assert!(spilled_bytes(&plan) > 0, "expected a spill");
    assert_eq!(pool.used(), 0);
    Ok(())
}

/// Many queries race past the ceiling at once (the concurrent variant of
/// `query_fails_naming_consumers_and_node_survives`). 32 MiB fits one
/// self-join of `big`, not eight. Every query either
/// returns the exact answer or fails alone with `ResourcesExhausted`: no
/// panic, no other error. Small queries in the middle of the storm succeed,
/// the pool drains to zero, and the node keeps serving.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_queries_fail_alone_under_the_ceiling() -> Result<()> {
    let pool = Arc::new(CeilingPool::new(48 * MIB, 512 * 1024)?);
    let ctx = session_context_with_ceiling(Arc::clone(&pool), None, config())?;
    register_big(&ctx)?;
    const ROWS: usize = 25 * 8192; // keys are unique: a self-join returns every row

    let join = |ctx: SessionContext| async move {
        let rows: Result<usize> = async {
            let out = ctx
                .table("big")
                .await?
                .join(
                    ctx.table("big2").await?,
                    JoinType::Inner,
                    &["k"],
                    &["k2"],
                    None,
                )?
                .collect()
                .await?;
            Ok(out.iter().map(|b| b.num_rows()).sum())
        }
        .await;
        rows
    };
    let small = |ctx: SessionContext| async move {
        let out = ctx
            .table("big")
            .await?
            .limit(0, Some(10))?
            .collect()
            .await?;
        Ok::<_, DataFusionError>(out.iter().map(|b| b.num_rows()).sum::<usize>())
    };

    let joins: Vec<_> = (0..8).map(|_| tokio::spawn(join(ctx.clone()))).collect();
    let smalls: Vec<_> = (0..8).map(|_| tokio::spawn(small(ctx.clone()))).collect();

    let (mut ok, mut exhausted) = (0, 0);
    for h in joins {
        match h
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?
        {
            Ok(rows) => {
                assert_eq!(rows, ROWS, "a join that finishes is exact");
                ok += 1;
            }
            Err(e) => {
                assert!(
                    matches!(e.find_root(), DataFusionError::ResourcesExhausted(_)),
                    "only the ceiling may fail a query: {e:?}"
                );
                exhausted += 1;
            }
        }
    }
    assert!(
        exhausted > 0,
        "8 joins of ~10 MiB each cannot all fit in 8 MiB"
    );
    for h in smalls {
        let rows = h
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))??;
        assert_eq!(rows, 10, "small queries are not starved by the storm");
    }
    eprintln!("joins: {ok} ok, {exhausted} exhausted");

    // Everything released; the node serves the next query, and the ceiling
    // fits one join on its own: the failures above were contention, not a
    // query that could never run.
    assert_eq!(pool.used(), 0);
    assert_eq!(small(ctx.clone()).await?, 10);
    assert_eq!(join(ctx.clone()).await?, ROWS);
    assert_eq!(pool.used(), 0);
    Ok(())
}

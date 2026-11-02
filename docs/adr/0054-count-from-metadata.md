# ADR 0054 — `count(*)` from metadata

- Status: Accepted
- Date: 2026-11-02
- Owner: JoaoLucas2210
- Related: ADR 0007 (scan), ADR 0014/0016/0017 (deletes), ADR 0038 (result cache)
- Scope: `plimsoll_core::memory::AccountedExec::{with_statistics, statistics_from_inputs}`, `IcebergTable::scan`

## Context

"Total rows" tiles, health checks and freshness probes run
`SELECT count(*) FROM t`. Plimsoll answered it by opening and scanning
every data file. Iceberg manifests already carry each data file's
`record_count`, which is exact by the spec. When nothing removes rows,
the count is a metadata sum.

DataFusion has the machinery for this. Its `AggregateStatistics` physical
optimizer rule replaces `count(*)` with a literal when its input reports
an `Exact` row count. The Iceberg scan reported no statistics at all.

## Decision drivers

1. **Exact means exact.** A count reported as `Exact` that is not exact
   is a wrong answer (ADR 0005).
2. **No new execution path.** Let DataFusion's rule do the rewrite.

## Decision

- **Statistics.** `AccountedExec::with_statistics(stats)` lets a source
  state statistics for the whole plan. They are returned from
  `statistics_from_inputs` for the plan-wide case. Otherwise the child's
  statistics pass through, since accounting never changes rows.
- **The Iceberg scan reports `num_rows = Exact(Σ record_count)` only when
  all of these hold:**
  - no equality delete set applies (ADR 0016);
  - no kept file has a position-delete or DV row selection (ADR 0014,
    0017). Fully deleted files were already dropped from the scan;
  - every kept file states its `record_count`;
  - no filter was pushed into the scan. Parquet row-group and page
    pruning would drop rows inside it;
  - no limit was pushed into the scan.

  Column statistics stay unknown.
- **`min` and `max` are not answered.** The Iceberg spec only requires
  bounds to be *bounds*: values less than or equal to every value, not
  the actual minimum. Writers may widen them, and string bounds are
  truncated by default. They prune (ADR 0010), but they never answer.

## Consequences

**Positive**
- `count(*)` over an Iceberg table without deletes reads no data files,
  only the manifests, which are already cached (ADR 0039).
- Tables with merge-on-read deletes keep the exact scan path, as before.

**Negative (accepted)**
- **Filtered counts still scan.** That includes partition filters. The
  next ROADMAP item reports identity-partition filters as exact so they
  can be decided from metadata too.
- **No `min`/`max` shortcut,** by design (driver 1).

## Validation

- `tests/iceberg.rs::count_star_is_answered_from_metadata_only_when_exact`:
  - without deletes, `count(*)`'s plan has no `IcebergScan` and the
    answer is 3;
  - with `WHERE id > 1`, the scan stays and the answer is 2;
  - with position deletes the scan stays and the answer is 2; with
    equality deletes it stays and the answer is 5.
- `tests/iceberg.rs::reads_nested_spark_types` (interop): on Spark's table
  without deletes, `count(*)` has no scan and returns 3.
- The interop MOR (32 rows) and v3 DV (85 706 rows) counts still scan and
  match Spark.

# ADR 0019 — Query latency histograms by phase

- Status: Accepted
- Date: 2026-10-30
- Owner: JoaoLucas2210
- Related: ADR 0004 (hand-rolled exposition), ADR 0013 (error counters), ADR 0018 (caches whose effect this measures), ADR 0010, ADR 0017
- Scope: `plimsoll_core::engine::QueryResult` (`planning`, `execution`), `plimsoll-server` (`engine::Phases`, `metrics::{Histogram, Latency}`, `app::sql`)

## Context

`/metrics` had gauges and counters only: pool usage, running/queued, totals
by state, errors by code. Nothing showed where a query spends time.

The recent work only pays off if latency can be seen:

- pruning (ADR 0010) and the delete cache (ADR 0018) cut *planning* time;
- failure budgets (ADR 0013) bound *dependency* time;
- admission limits trade *queue* time for memory safety.

Without phase latency, an operator cannot tell "slow because queued" from
"slow because planning reads 300 manifests" from "slow because the scan is
big".

## Decision drivers

1. Separate the phases an operator can act on:
   - queue (admission limits);
   - planning (metadata, deletes, catalog);
   - execution (data);
   - serialization (result size).
2. Histograms, not averages: tail latency (p95/p99) is what users feel.
3. Negligible overhead per query; no locks on the hot path.
4. No new dependency for a few dozen lines (ADR 0004's stance).
5. Standard Prometheus semantics, so `histogram_quantile` works unchanged.

## Considered options

**A. Hand-rolled fixed-bucket histogram (chosen).** There is an atomic
counter per bucket plus a microsecond sum. Rendering is cumulative at scrape
time, about 40 lines. It is lock-free, with two `fetch_add`s per
observation.

**B. `prometheus-client` crate.** It brings full metric types and a
registry. But it would mean rewriting the existing hand-rolled exposition or
running two styles side by side, for one histogram family. It is the upgrade
path when metric count grows (ROADMAP: OTLP).

**C. Per-query timings in `system.runtime.queries` only.** This gives
detail per query but no aggregation for alerting. It complements a
histogram and does not replace it.

## Decision

- **Phase boundaries.**
  - `queue`: from the handler's request arrival to the admission slot
    grant, measured in the `Admission::execute` closure.
  - `planning`: from SQL text to the physical plan. This includes subquery
    pre-evaluation and connector planning (Iceberg manifests, deletes,
    catalog `loadTable`).
  - `execution`: from the physical plan to all batches collected.
  - `serialization`: from batches to the JSON body.
  - `total`: from arrival to the end of the engine call. It does not cover
    streaming the body.
- **Which queries count.** `queue` and `total` count every query that got an
  id from admission, failures and timeouts included. `planning`, `execution`
  and `serialization` count successful queries only, because a failed query
  has no complete split. Rejected requests (`queue_full`, `not_ready`,
  invalid body) are counted by `plimsoll_query_errors_total` and are not
  timed.
- **Buckets (seconds).** 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5,
  1, 2.5, 5, 10, 60, 300, +Inf. They span cached point lookups up to the
  query timeout's order of magnitude.
- **Exposition.** `plimsoll_query_duration_seconds{phase="…"}` with
  `_bucket{le}`, `_sum` and `_count`.

## Consequences

**Positive**
- `histogram_quantile(0.99, sum by (le, phase)
  (rate(plimsoll_query_duration_seconds_bucket[5m])))` gives p99 per phase.
- Planning cost is visible. In a local smoke on the Spark v3 fixture (debug
  build), planning was ~22 ms per query against ~8 ms of execution. That is
  the evidence for the next ROADMAP item, parallel manifest and DV reads.

**Negative (accepted debt)**
- Failed queries contribute no per-phase timings; a slow failure shows only
  in `total`.
- No per-table or per-user labels, to keep cardinality bounded. Per-query
  detail belongs in `system.runtime.queries` (not added yet).
- Fixed buckets: changing them is a code change.
- Time to stream the response body is not measured.

**Operational**
- Alerts: p99 `queue` rising means raise limits or add nodes; p99
  `planning` rising means catalog or object store latency, or tables that
  need compaction (`deletes=`, `manifests=` in `EXPLAIN`).
- 5 phases × (15 buckets + sum + count) = 85 series. That is constant,
  with no label explosion.

## Risks and mitigations

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| Phase split drifts from reality after engine changes (e.g. lazy planning inside execution) | Medium | Medium | Boundaries defined in `Engine::sql`; documented here; revisit when the engine changes. |
| Atomics contention at very high QPS | Low | Low | Relaxed `fetch_add` per observation; shard per CPU if ever measured. |
| Microsecond sum overflow | Negligible (u64 µs ≈ 584 000 years) | — | Saturating conversion from `u128`. |

## Validation

- `metrics::tests::renders_exposition_lines`:
  - cumulative buckets at bucket edges, values above the last bucket
    landing only in `+Inf`;
  - sum and count;
  - an empty phase still rendered with count 0.
- `app::tests::latency_histograms_split_by_phase`: one success and one
  failure. `queue`/`total` count 2, `planning`/`serialization` count 1.
- Manual smoke: server with `PLIMSOLL_ICEBERG_TABLES` pointing at the Spark
  v3 fixture, 4 successful and 1 invalid queries; `/metrics` shows the counts
  above and planning > execution.

## Revisit when

- Metric count grows or OTLP export lands (move to a client library).
- Operators need per-table latency (per-query timings in
  `system.runtime.queries`, not labels).
- Body streaming time matters (large results): measure in `json_body`.

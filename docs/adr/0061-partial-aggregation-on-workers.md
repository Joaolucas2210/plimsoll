# ADR 0061 — Partial aggregation on workers

- Status: Accepted
- Date: 2026-11-02
- Owner: JoaoLucas2210
- Related: ADR 0058 (fragments), ADR 0059 (governance), ADR 0060 (balance and retry)
- Scope: `plimsoll_connectors::remote::{PartialAggregatePushdown, AggregateSpec, RemoteScanExec::with_aggregate, decode (casts)}`, `CoreEngine::fragment`, coordinator setup in `main`

## Context

With scans on workers (ADR 0058), an aggregation still moved every row to
the coordinator. `SELECT region, count(*), sum(amount) … GROUP BY region`
over a billion rows shipped a billion rows to compute a handful of
groups. The network and the coordinator became the bottleneck that
workers were meant to remove.

DataFusion already plans aggregations in two phases:

- `Partial` runs per input partition and emits accumulator **states**;
- `Final` merges those states.

For `count`, `sum`, `min` and `max`, the state of a partition *is* the
aggregate over that partition's rows. So a worker that computes the
aggregate over its fragment produces exactly the partial state the
`Final` expects.

## Decision drivers

1. **Exact answers.** Push down only aggregates whose partial state
   equals the per-partition value, and only when nothing between the
   aggregate and the scan changes the rows.
2. **No plan serialization.** The aggregate must be describable in the
   fragment's small JSON.
3. **Workers stay safe.** Fragment work must not touch the catalog the
   worker's users share.

## Decision

- **Rule.** `PartialAggregatePushdown` is a physical optimizer rule,
  appended after DataFusion's own and only on coordinators. It rewrites
  an `AggregateExec` when all of these hold:
  - its mode is `Partial`, with no grouping sets and no aggregate
    `FILTER`;
  - every group key is a plain `Column`;
  - every aggregate is `count(*)` (planned as `count(<non-null
    literal>)`) or `count`/`sum`/`min`/`max` of a `Column`, not
    `DISTINCT`, not ordered, not `IGNORE NULLS`;
  - its input reaches an `AccountedExec` over a `RemoteScanExec` through
    `RepartitionExec`/`CoalesceBatchesExec` only, on either side of the
    accounting. A `FilterExec` or `ProjectionExec` in between means no
    rewrite.

  It is replaced by `AccountedExec("RemoteAggregate")` over the same
  fragments, now carrying an `AggregateSpec { group_by, aggregates }`.
  The rewritten node's schema is the `Partial` node's schema.
- **Worker.** `CoreEngine::fragment` builds the aggregation with the
  DataFrame API on the fragment's provider
  (`read_table(…).aggregate(…)`, with `ident` for verbatim column names):
  - no SQL text;
  - **no table registered**. The first version registered a `fragment`
    table, and `SessionContext::new_with_state` shares the worker's
    catalog, so concurrent fragments collided and the table would have
    been visible to users. The test caught this.
- **Types.** The coordinator's decoder casts each returned column to the
  partial-state field type, then labels it with the coordinator's
  schema.
- **A side effect is correct and kept.** DataFusion plans
  `count(DISTINCT x)` as an inner `GROUP BY x` under a count. That inner
  partial grouping is pushed: workers return their distinct values, and
  the coordinator deduplicates them and counts.

## Consequences

**Positive**
- Aggregations over distributed scans move one row per group per worker
  instead of every row. In the manual check, a `count`/`sum` over a v3
  table of 85 706 rows moved 1 row (512 bytes) per worker instead of
  about 42 800 rows (about 420 KB), with the same answer.
- Deletes still apply on workers before aggregating, so merge-on-read
  tables aggregate correctly.

**Negative (accepted)**
- **A filter between the aggregate and the scan blocks pushdown.** That
  includes any non-partition `WHERE`. Next ROADMAP item: push filters
  and projections into fragments.
- **`avg`, `stddev` and other multi-field states are not pushed.**
- **Expressions as group keys are not pushed.**

## Risks and mitigations

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| A partial state that is not the per-partition value | Low | High | Only count/sum/min/max are accepted; others leave the plan alone. |
| A node between aggregate and scan that changes rows | Low | High | Only named pass-through nodes are crossed; anything else blocks the rewrite. |
| Worker-side types differ from partial-state types | Medium | Low | Casting to the coordinator's field types (e.g. sum widening). |

## Validation

- `tests/distributed.rs::distributed_scans_match_local_ones` (interop), on
  the Spark MOR (position deletes), EQ (equality deletes, grouped by
  `region`) and v3 DV tables. Distributed answers equal local ones for
  every query, and `EXPLAIN` shows `aggregate=[…]` exactly where
  expected:

  | Query | Pushed |
  |---|---|
  | `count(*), sum(id)` | yes |
  | `max(id), min(id)` | yes |
  | `GROUP BY name/region` with `count(*), count(col), sum, min, max` | yes |
  | `count(DISTINCT id)` | yes (as an inner grouping) |
  | `WHERE id > 3` | no |
  | `GROUP BY id % 3` | no |

- `remote::tests::fragments_round_trip_as_json`: the aggregate spec
  survives the fragment's JSON round trip, including a quoted column
  name.
- Manual check with two workers and a coordinator over HTTP, using the
  v3 DV table:
  - `count(*), sum(id)` returned 85 706 rows summing to 4 285 685 677,
    as Spark;
  - `EXPLAIN` showed `aggregate=[count(*), sum(id)]`;
  - the workers' `fragment` lines show 1 row each for that query, against
    42 848 and 42 858 rows for a non-pushed one.

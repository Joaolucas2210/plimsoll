# ADR 0112 — Joins between a remote scan and an aggregation on workers

- Status: Accepted
- Date: 2026-11-03
- Owner: JoaoLucas2210
- Related: ADR 0064 (aggregation shuffles), ADR 0065 (shuffled joins), ADR 0098 (join filters), ADR 0111 (join sides)
- Scope: `remote::join_side`, `JoinSpec::{left_picks, right_picks}`, `join_filter`'s positional sides, the worker's `merged` and reduce join

## Context

A join against a grouped aggregation of a remote scan, such as
`… JOIN (SELECT id, count(*) c FROM t GROUP BY id) a ON a.id = b.id`,
ran its pieces apart:
- the partial aggregates on the workers;
- the final aggregation on the coordinator, hash-partitioned by the
  group keys, which the join then relied on;
- the other side's rows sent to the coordinator;
- the join itself on the coordinator.

Shuffling the aggregation (ADR 0064) was not allowed, because the join
depended on its partitioning.

Both halves already hash by the same thing. An aggregation shuffle's
maps hash partial states by the group keys, and a join shuffle's maps
hash rows by the join keys. When those keys are the same columns, bucket
`j` of each side holds the same keys, and one shuffle can merge and join
in the same reducer.

## Decision drivers

1. **One shuffle**: no intermediate gather, no second exchange.
2. **Reuse the pieces**: aggregation maps (ADR 0061, 0063), the
   reducer's merge (ADR 0064), and the join reduce (ADR 0065).
3. **Never join groups that cannot meet their matches.**

## Decision

- **`remote::join_side`.** A shuffled join's side is a remote scan
  (ADR 0065, 0111), or a grouped final aggregation of one. The
  aggregation may sit under a projection that only picks or renames its
  columns. Its partial aggregation must push down (ADR 0061, 0063), with
  one state per aggregate (count, sum, min, max), as ADR 0064 requires.
  - The side's maps are the partial aggregates.
  - `picks` maps the side's columns to positions in the aggregation's
    output.
- **The keys must be exactly the group keys.** On an aggregated side,
  the join keys, through `picks`, must be every group key once.
  Otherwise the maps would hash something other than what the join
  matches on, and the join stays as planned.
- **The worker.**
  - `JoinSpec::{left_picks, right_picks}` mark an aggregated side.
  - The reducer merges that side's buckets (`merged`, the aggregation
    reduce's own code, now shared). It then keeps the picked columns,
    named `c0…`, as the join's side.
  - On that side, keys and residual filter columns are named `c{i}`
    (`join_filter` takes the positional sides).
  - Maps hash the partial states' group columns in join-key order, the
    same order the other side hashes its key columns.
  - The join, its filter and its projection are then the planned ones,
    unchanged.
- An older worker does not know the picks. It treats the aggregated
  maps as rows, looks for a column named `c0` and fails, instead of
  joining unmerged states.

## Consequences

**Positive**
- Joining a table with an aggregation of a table (per-key counts, sums,
  extrema joined back to rows) runs entirely on the workers: one
  exchange of partial states and rows, merged and joined where they
  meet.
- Either side, or both, may be aggregated, with any join type and
  residual filter that shuffled joins take.

**Negative (accepted debt)**
- **Only plain pushable aggregations**: count, sum, min and max with one
  state each. `avg` and `count(DISTINCT)` keep the join on the
  coordinator, as they keep ADR 0064's shuffle off.
- **A filter between the aggregation and the join keeps it as
  planned.** That covers `HAVING`, and also a residual filter over keys
  that DataFusion rewrites onto one side, for example
  `a.c + b.id > 3` with `a.id = b.id` (found while testing). Filtering
  merged rows on the worker would lift it.
- **Joined on part of the group keys**, the join stays on the
  coordinator.

## Validation

- `tests/distributed.rs::distributed_scans_match_local_ones` (Spark
  fixtures, CI's interop job). The plans are shuffled joins with the
  same answers as the local engine:
  - an aggregation joined with a scan;
  - a scan left-joined with an aggregation (`sum`);
  - two aggregations joined (`count` with `max` and `min`);
  - a residual filter over an aggregate and a non-key column of the
    other side.
- The same test: joined on part of the group keys (`GROUP BY id,
  name`, `ON a.id = b.id`), the plan is not a shuffled join, and the
  answers match.
- `remote::tests::fragments_round_trip_as_json`: picks round-trip.

## Revisit when

- `HAVING` under joins is common: carry the filter as SQL over the
  merged columns, as join filters are (ADR 0098).
- Joins on part of the group keys matter: hash the aggregated side's
  maps by the join keys only. That is correct for the join, but the
  merge then needs every partial state of a group in one bucket, which
  holds only if the join keys determine the group.

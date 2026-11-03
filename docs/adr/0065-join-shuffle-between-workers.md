# ADR 0065 — Join shuffle between workers

- Status: Accepted
- Date: 2026-11-02
- Owner: JoaoLucas2210
- Related: ADR 0064 (aggregation shuffle: maps, buckets, reduces), ADR 0062 (filters on fragments), ADR 0058 (fragments)
- Scope: `remote::{shuffle_join, remote_under, RemoteReduceExec, Reduce.join, JoinSpec, Bucket.keys}`, the "free" walk in `remote::shuffle`, `CoreEngine::{reduce, map_bucket}`, `fetch_side`

## Context

After ADR 0064, workers scan, filter and aggregate, but a join of two
large tables still moved **both tables** to the coordinator.
DataFusion's partitioned hash join then ran there, under the
coordinator's memory ceiling. That made joins the remaining reason a
query would not scale out.

ADR 0064 already has everything a partitioned join needs:

- maps that hash-partition a fragment's rows into buckets;
- reduces that pull one bucket from every map;
- accounting, deadlines and end-of-stream checks.

## Decision drivers

1. **Same answers.** Matching rows must meet on one worker, and the plan
   above must not rely on the join's partitioning.
2. **Describe, do not serialize.** The join must travel in the fragment
   JSON (ADR 0004), like filters and aggregates.
3. **Reuse ADR 0064.** One reduce node, one map store, one endpoint.

## Decision

- **What is shuffled.** A `HashJoinExec` is shuffled when all of these
  hold:
  - it is `Partitioned`; a `CollectLeft` broadcast is left alone;
  - it has no residual filter;
  - its keys are not null-equal, and it is not a mark join;
  - both inputs reach a plain `RemoteScanExec` (not aggregated, at least
    one fragment) through pass-through nodes only;
  - every key pair is two **columns of the same Arrow type**. Equal types
    hash equally under DataFusion's constant-seeded hash, so a row and
    its match land in the same bucket. Expression keys are planned as a
    `ProjectionExec` below the repartition and are not matched; that is
    documented debt.

  The rule covers inner, left, right and full outer joins, and left and
  right semi and anti joins. The join's own output projection is kept.
- **Plan.** The join becomes `AccountedExec("RemoteJoin")` over a
  `RemoteReduceExec`, with one partition per healthy worker.
  - Partition j posts a `Reduce` with `join: {join_type, right maps,
    left_keys, right_keys, projection}` to worker j.
  - `RemoteReduceExec` now also carries the aggregation shuffle (ADR
    0064), which no longer marks `RemoteScanExec`. The display shows
    `shuffled aggregate` or `shuffled <type> join`, the map count and the
    filters.
- **Maps.**
  - A map is now any fragment with a `bucket`, aggregated or not.
  - `Bucket.keys` names the hashed output columns by position: the group
    keys (ADR 0064) or the join keys, where the reducer resolves the key
    names to positions in the map's `columns`.
  - Left maps are numbered 0..a and right maps a..a+b within one shuffle
    id, so a self-join's two sides never share a map.
- **Reduce.** The worker fetches both sides' buckets concurrently and
  reserves them (`shuffle-reduce`). It reads each side as a table
  qualified `l` and `r`, joins with `join_on(l.k = r.k, …)` using the
  same `JoinType`, applies the projection and streams the result back.
  The coordinator's decoder casts to the join's schema, as for every
  remote result.
- **Freedom, revisited (amends ADR 0064).** A child is free (nothing
  above depends on its partitioning) in these cases:
  - below `CoalescePartitions`, `SortPreservingMerge` or **`Repartition`**,
    since all three accept any input partitioning;
  - below projection, sort, filter, coalesce-batches, limits and
    **partial aggregations**, when those are free themselves.

  Joins, unions, windows and final aggregations block it. ADR 0064
  treated `Repartition` as blocking, which was needlessly strict. With
  it unblocked, a join under `count(*)` and the inner grouping of
  `count(DISTINCT)` now shuffle too. The live test shows the case that
  must not shuffle: `GROUP BY` on the join key, where DataFusion plans a
  `SinglePartitioned` aggregate that relies on the join's hash
  partitioning. That join is correctly left alone.

## Consequences

**Positive**
- Joins of two remote scans run on the workers.
  - Each worker joins 1/W of both sides, under its own memory pool.
  - Only joined rows reach the coordinator.
  - Filters on either side are applied in the maps (ADR 0062).
- Aggregations above a shuffled join run over its output as usual.
  Aggregations of a single scan keep shuffling (ADR 0064).

**Negative (accepted debt)**
- **Both sides are buffered whole.** Maps keep their buckets in memory,
  and the reducer keeps both of its buckets before joining. There is no
  spilling, and memory is bounded by the pools (a query fails rather than
  the process). Skewed keys load one worker.
- **Broadcast joins stay on the coordinator.** DataFusion picks
  `CollectLeft` for small build sides, so those joins still pull rows.
  Shipping the small side to every worker is the obvious next step.
- **Expression keys and residual filters are not shuffled.** The SQL
  round trip of ADR 0062/0063 could carry them.
- **Joins of a remote scan with anything else stay local.** That covers
  an aggregate, a local table or a subquery.
- **No retries.** As in ADR 0064, a lost worker fails a shuffled query.

## Validation

- `remote::tests::fragments_round_trip_as_json`: reduces with a join spec
  (anti join, projection, quoted key) round-trip as JSON; an aggregation
  reduce whose maps do not aggregate is refused.
- `distributed_scans_match_local_ones` (live, Spark-written MOR, EQ and
  DV tables, two workers). The coordinator session forces partitioned
  joins, since the fixtures are tiny. For every table it runs:
  - an inner self-join;
  - a left join against a filtered side;
  - an `IN` subquery (semi join);
  - a `NOT EXISTS` (anti join).

  Across tables it runs a full outer join and a two-key join under a
  `GROUP BY`. All of these are `shuffled … join`, with answers equal to
  the single-node ones. `GROUP BY` on the join key is checked to stay
  unshuffled, with equal answers.
- The aggregation shuffle tests of ADR 0064 still pass. `count(DISTINCT)`
  is now expected to shuffle.

## Revisit when

- Small dimension tables join large facts: broadcast the small side to
  the workers instead of collecting it on the coordinator.
- Buckets outgrow worker memory or keys skew: spill buckets to disk, and
  split hot buckets.
- Expression keys matter: compute them in the maps from round-tripped
  SQL (ADR 0063).

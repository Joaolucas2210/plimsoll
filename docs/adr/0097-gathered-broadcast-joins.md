# ADR 0097 — Left, full, semi and anti broadcast joins on workers

- Status: Accepted
- Date: 2026-11-03
- Owner: JoaoLucas2210
- Related: ADR 0066 (broadcast joins), ADR 0058/0060 (fragments), ADR 0065 (shuffled joins)
- Scope: `Broadcast::matched`, `remote::{broadcast_join (gathered kinds), RemoteGatheredJoinExec, build_only}`, the worker's `broadcast_join` / `numbered`

## Context

A broadcast join (ADR 0066) sends the collected build side with every
probe fragment, and each worker joins its fragment. That works for
join types decided per probe row: inner, right, right semi and right
anti.

The others depend on the build side as a whole:

- **left** and **full** emit each unmatched build row once, with NULLs
  for the probe side;
- **left semi** emits each matched build row once;
- **left anti** emits each unmatched build row once.

Whether a build row matched is only known after *every* probe fragment
ran. A worker that emitted those rows itself would emit each of them
once per worker, or wrongly.

These are not rare. DataFusion plans `EXISTS` / `IN` as left semi,
`NOT EXISTS` as left anti, and often swaps a right join into a left
join with the smaller side collected. They ran on the coordinator, with
every probe row pulled there.

## Decision drivers

1. **The answer of a single-node join**, including NULL keys and
   duplicate matches.
2. **The probe side stays on the workers**, which is where the rows
   are.

## Decision

- **Split by what is local.**
  - **Workers** decide pairs: each build row is numbered (`__b_row`,
    in the attachment's order), the join runs as inner (as right for a
    full join, so unmatched probe rows still come out there), and every
    output row carries its build row's number.
    - For left and full joins, the join's own output columns (after its
      projection) go back, followed by the number.
    - For semi and anti joins, only the distinct numbers matched go
      back.
  - **The coordinator** marks the numbers it receives and, once every
    fragment is done, adds the build-only rows: unmatched ones for
    left, full (with NULL probe columns, through the join's projection)
    and anti joins, and matched ones for semi joins.
- **One output partition** (`RemoteGatheredJoinExec`).
  - All fragments run at once and their joined rows stream as they
    arrive. The build-only rows follow when the last fragment ends.
  - A partition per fragment would need a last partition waiting on all
    others, which deadlocks under a consumer that runs partitions in
    turn.
- **What stays local:**
  - joins with a filter;
  - null-equal keys;
  - **null-aware anti joins** (`NOT IN`, where one NULL key empties the
    result);
  - anything ADR 0066 already refused.
- **Build side** is collected and shipped as in ADR 0066, reserved in
  the pool for the join's duration.

## Consequences

**Positive**
- `EXISTS`, `NOT EXISTS`, `IN (subquery)`, left and full joins against a
  small side run with the probe side on the workers.

**Negative (accepted debt)**
- **One output partition.** Above the join, DataFusion works on a single
  stream, where an inner broadcast join keeps one partition per
  fragment.
- **For semi and anti joins, workers return each matched number once**
  (distinct), so a fragment's reply is bounded by the build side's
  size.
- `NOT IN` still runs locally.

## Validation

- `tests/distributed.rs::distributed_scans_match_local_ones` (Spark
  fixtures, in CI's interop job), with build sides collected:
  - `IN (subquery)`, `LEFT JOIN`, `FULL JOIN`, `EXISTS` and `NOT EXISTS`
    plan as `RemoteBroadcast` and return exactly the local answer, over
    tables with position deletes and deletion vectors;
  - `NOT IN` stays local;
  - a `RIGHT JOIN` gives the local answer whether DataFusion swaps it
    into a gathered left join or keeps it.
- By hand, with a coordinator and two worker processes against the
  reference REST catalog, compared with a node without workers:
  - a grouped left join, a full join, and `EXISTS` / `NOT EXISTS` over
    the same tables gave byte-identical results;
  - `NOT EXISTS` planned as `LeftAnti join fragments=2 gathered`.

## Revisit when

- Larger build sides: the shuffled joins of ADR 0065 cover equi-joins
  of two large sides. For left and full shuffles, reducers already see
  both sides of a bucket.
- Joins with a filter: ship the filter as portable SQL over the joined
  row (as `MERGE` does, ADR 0094).

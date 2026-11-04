# ADR 0115 — Non-equi joins against a small side on workers

- Status: Accepted
- Date: 2026-11-04
- Owner: JoaoLucas2210
- Related: ADR 0066 (broadcast joins), ADR 0097 (gathered joins), ADR 0098 (join filters), ADR 0099 (broadcast limit), ADR 0111 (join sides)
- Scope: `remote::broadcast_join` (now for `NestedLoopJoinExec` too), `join_filter`'s inputs

## Context

A join with no equality between its sides (`ON a.ts BETWEEN b.start AND
b.end`, `ON a.id > b.id + 40`, a correlated `NOT EXISTS` on an
inequality) is planned by DataFusion as a `NestedLoopJoinExec`. It
collects its left side and runs every right row against it under the
join's filter. Such joins always ran on the coordinator, with every row
of both sides brought there.

That is the shape of a broadcast (ADR 0066): a collected left side, and
a right side that is a remote scan. The workers' broadcast join already
builds its condition from the shipped filter (ADR 0098). With no keys,
DataFusion plans the worker's join as a nested loop too.

## Decision drivers

1. **No new machinery.** The broadcast and gathered execs, the
   broadcast limit's local fallback and the worker's broadcast join
   already do everything a nested loop needs.
2. **Same guarantees as equi-joins**: the same join types, the same
   filter rules, the same size limit.

## Decision

- **`broadcast_join` accepts a `NestedLoopJoinExec`** as well as a
  `CollectLeft` `HashJoinExec`, using the pieces common to both: sides,
  join type, filter, projection, and keys (none for a nested loop).
  - A nested loop always collects its left side, so it has no partition
    mode to check.
  - The hash join keeps its own conditions: `CollectLeft`, nulls never
    equal, and no null-aware anti join.
- **Everything after that is shared.**
  - The probe side must be a remote scan (`scan_side`, ADR 0111).
  - The filter must travel as SQL (`join_filter`, which now takes a
    filter and two schemas instead of a hash join).
  - Inner, right, right semi and anti joins run on the workers as
    planned. Left, full, left semi and anti ones gather the matches of
    numbered build rows (ADR 0097).
  - A collected side over `PLIMSOLL_BROADCAST_MAX_BYTES` runs the
    planned join on the coordinator over the rows already collected
    (ADR 0099).
- **The worker is unchanged.** Its broadcast join with no keys joins
  on the filter alone, and DataFusion plans that as a nested loop.

## Consequences

**Positive**
- Range and inequality joins against a small side run where the large
  side's rows are, as equi-joins against a small side do.
- Every join type and filter that broadcasts take, plus the broadcast
  limit's fallback, applies to nested loops.

**Negative (accepted debt)**
- **No size check of its own.** DataFusion collects a nested loop's
  left side whatever its estimate, and swaps the sides to collect the
  smaller one. A large collected side is caught only by the broadcast
  limit, after collection (ADR 0099). The same was true locally, where
  the coordinator held it all.
- **A sorted probe side** (an `ORDER BY` DataFusion satisfies below the
  join) keeps the join local, as for equi-joins.
- Each worker pays the whole nested loop for its fragments. That is
  `|build| × |fragment|` comparisons, the same total as locally, now
  spread over the workers.

## Validation

- `tests/distributed.rs::distributed_scans_match_local_ones` (Spark
  fixtures, CI's interop job), at 12 partitions and pinned to two CPUs:
  - an inner join on `a.id > b.id + 40` against a filtered side;
  - a left join on `b.id BETWEEN a.id + 1 AND a.id + 2` (gathered);
  - a `NOT EXISTS` on `b.id > a.id + 40` (an anti join, gathered);
  - each is a `RemoteBroadcast` with the local engine's rows.
- The same test's capped coordinator (a 1-byte broadcast limit): the
  inner nested loop runs on the coordinator over the collected rows,
  with the same answer.

## Revisit when

- Large range joins matter: partition both sides by range (sorted
  bucket boundaries) instead of broadcasting one.
- Small sides are collected repeatedly: cache collected sides per
  table snapshot (ROADMAP).

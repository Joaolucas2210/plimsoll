# ADR 0066 — Broadcast joins on workers

- Status: Accepted
- Date: 2026-11-03
- Owner: JoaoLucas2210
- Related: ADR 0065 (join shuffle), ADR 0064 (shuffle placement rules), ADR 0054 (manifest row counts), ADR 0060 (retry)
- Scope: `remote::{broadcast_join, RemoteBroadcastExec, Broadcast, split_body, read_ipc}`, the ordering guard in `remote::shuffle`, `IcebergTable::scan` statistics, `CoreEngine::fragment` (`broadcast_join`), `/v1/fragment` body

## Context

ADR 0065 moved partitioned joins to the workers. The most common
analytical join, however, is a large fact table joined with a small
dimension. DataFusion plans that as `CollectLeft`: it collects the small
side once and streams the large side past it. On a coordinator, that
meant every row of the fact table still travelled from the workers.

The live test showed a second problem: DataFusion never chose
`CollectLeft` for Iceberg tables. Remote scans reported no statistics,
so no side was known to be small. Every join became partitioned,
small sides included.

## Decision drivers

1. **Same answers.** Each worker sees only part of the probe side, so
   only joins whose output per probe row does not depend on other probe
   rows qualify.
2. **The build side travels once per fragment, in binary.** No base64,
   no new dependency.
3. **Order guarantees survive.** A replaced node must not be one whose
   ordering the plan relies on.

## Decision

- **Statistics.** When the exact row count of ADR 0054 does not apply
  (deletes, filters, a limit), the Iceberg scan reports the sum of the
  manifests' `record_count`s as `Inexact`. That is an upper bound, and
  enough for DataFusion's join selection: the smaller side becomes the
  build side, and sides under its thresholds are collected. Byte sizes
  are not reported, because Parquet sizes understate in-memory size.
- **Rule.** A `HashJoinExec` is replaced by
  `AccountedExec("RemoteBroadcast")` over a `RemoteBroadcastExec` when
  all of these hold:
  - it is `CollectLeft`;
  - its type is **inner, right, right semi or right anti**. In these,
    the probe side (right) drives the output. Left, full and left
    semi/anti joins would emit build rows once per worker, so they stay
    on the coordinator;
  - it has no residual filter, and its keys are not null-equal;
  - its probe side reaches a plain remote scan through pass-through
    nodes;
  - every key pair is two columns of the same type;
  - it is free in the sense of ADR 0064/0065.

  The build side is collected whole, so it is optimized again as free
  and may shuffle itself.
- **Execution.**
  - `RemoteBroadcastExec` keeps the build plan as its child. On the
    first partition polled, it runs that plan **once** (`OnceCell`,
    shared by all partitions), writes it as Arrow IPC and reserves it in
    the coordinator's pool (`broadcast-build`).
  - Each probe fragment is then posted with the body
    `JSON \n IPC`. Serialized JSON has no raw newline, so the first one
    splits the two, and a body without one is a plain task as before.
  - The fragment's new `broadcast` field (`join_type`, build and probe
    keys, projection) tells the worker to join. The worker reads the
    attachment as a table qualified `b` and runs `b JOIN p ON b.k = p.k`
    against its filtered fragment `p`, with the same join type and
    projection.
  - Fragments are idempotent, so the retry of ADR 0060 applies, sending
    the same body.
- **Ordering guard (amends ADR 0064/0065).** No shuffle or broadcast
  replaces a node that has an output ordering. Remote results arrive
  unordered.
  - With statistics present, DataFusion started pushing `ORDER BY`
    sorts below joins, onto the probe side, and then relied on the join
    keeping that order for a `SortPreservingMerge`. Replacing such a join
    would merge unsorted streams.
  - In the plans seen, the sort sat between the scan and the join, so
    those joins were not matched anyway. The guard makes this a rule
    instead of an accident.

## Consequences

**Positive**
- Fact ⋈ dimension joins run on the workers. The dimension is sent once
  per fragment, and the fact rows never leave their worker unjoined.
  Probe-side filters still apply in the scan (ADR 0062).
- The manifest row counts improve every join on Iceberg tables, local or
  remote: the build side is now the smaller one.

**Negative (accepted debt)**
- **No cap on the build side.** It is as large as DataFusion's thresholds
  allow (1 MiB or 128 Ki rows by default, from inexact counts). It is
  sent W times and held in memory while the plan runs. The coordinator's
  reservation bounds it; there is no explicit cap.
- **Left, full and left semi/anti joins with a small side still run on
  the coordinator.** Doing them on workers would need a final step that
  merges which build rows matched anywhere.
- **Upper-bound statistics.** A heavily filtered or deleted large table
  may still look large, and join there partitioned (ADR 0065).
- **The build side is computed on the coordinator**, even when it is
  itself a remote scan. Its own scan still runs on workers.

## Validation

- `remote::tests::fragments_round_trip_as_json`: fragments with a
  `broadcast` round-trip as JSON, and `split_body` separates the JSON
  from the attachment, or returns none.
- `distributed_scans_match_local_ones` (live, Spark-written MOR, EQ and
  DV tables). With DataFusion's default thresholds on the coordinator:
  - an inner self-join, and an aggregate over a join with a filtered
    small side, are `RemoteBroadcast` (as asserted) and return the
    single-node answers;
  - these are asserted *not* to broadcast, and still return the same
    answers:
    - an ORDER BY whose sort DataFusion pushes under the join (the
      ordering guard);
    - an `IN` subquery planned as a left semi join;
    - a right join planned as a left join.
- All earlier shuffle tests (ADR 0064/0065) still pass with the new
  statistics. So does the Iceberg interop suite, including exact
  `count(*)` from manifests.

## Revisit when

- Dimension tables outgrow the defaults: cap the build side explicitly,
  or cache it on workers per snapshot.
- Left or full joins against small sides become common: have workers
  report matched build rows and emit unmatched ones once.
- Better estimates are needed: column statistics from manifests
  (null counts, bounds) and filter selectivity.

# ADR 0122 — Broadcast-join rows written on workers

- Status: Accepted
- Date: 2026-11-04
- Owner: JoaoLucas2210
- Related: ADR 0066 (broadcast joins), ADR 0091/0092 (writes on workers), ADR 0099 (broadcast limit), ADR 0115 (non-equi joins)
- Scope: `RemoteBroadcastExec::{writing, write}`, `picked`, `remote_write::distribute`, the worker's picked write

## Context

An `INSERT … SELECT` (or CTAS) whose rows come straight from workers is
written where they are. That covers plain scans (ADR 0091) and a
shuffle's reduces (ADR 0092). A join against a small side, such as
enriching facts with a dimension, is planned as a broadcast (ADR 0066,
0115). Its joined rows came back to the coordinator and were written
there: the bulk of the data crossed the network twice, and one node
encoded every file.

The worker already runs a fragment that both broadcast-joins and
writes: its `write` step follows its `broadcast` step. Only the
coordinator never planned one.

## Decision drivers

1. **Same path as the other remote writes**: one commit of every file
   written, by `RemoteWriteExec`.
2. **The broadcast limit still holds.** A build side too large to send
   (ADR 0099) is joined on the coordinator, and its rows must still be
   written, in the same commit.

## Decision

- **`distribute` accepts a broadcast join** (`RemoteBroadcastExec`,
  under the same pass-through nodes and lossless picking projection as
  the other sources).
- **`RemoteBroadcastExec::writing`** returns the same join, with:
  - each probe fragment carrying the target (`WriteTo`, with the
    table's columns picked by position in the join's output);
  - the exec's own `write`.
- **On the worker**, the fragment joins its rows against the attached
  build side. It picks the target's columns in the table's order (the
  same picks a reduce applies, ADR 0092), writes them, and returns the
  files written.
- **Over the limit**, the coordinator joins locally (ADR 0099) and
  writes the joined rows itself. It uses the same `write_files` the
  workers use, with the same picks, and returns the files in the same
  form. `RemoteWriteExec` commits them like any others, in one commit.
- Gathered joins (left, full, semi, anti, ADR 0097) still write on the
  coordinator: their build-only rows are only known there.

## Consequences

**Positive**
- Enrichment-style inserts write in parallel where the facts are read.
  Only file descriptions come back.
- Nothing changes for the commit, the limit, or the join's semantics.

**Negative (accepted debt)**
- **Gathered joins are not distributed** for writes.
- **The fallback writes on one node.** That is what happened before,
  and only for build sides too large to broadcast.

## Validation

- `tests/distributed.rs::writes_on_workers_through_the_rest_catalog`
  (the REST catalog, CI's interop job; locally against the reference
  fixture), at 12 partitions and pinned to two CPUs:
  - an `INSERT … SELECT` joining the source to its own `id < 1010` rows
    plans `source=RemoteBroadcastExec` and writes 9 rows;
  - a coordinator with a 1-byte broadcast limit runs the same insert:
    joined and written on the coordinator, 9 rows;
  - the target then holds 18 more rows below 1010, 9 distinct ids each
    written twice.
- The rest of that test (scans, reduces, CTAS, row-level writes,
  `MERGE`) is unchanged and passes.

## Revisit when

- Gathered joins feed large writes: write the probe-matched rows on the
  workers, and only the build-only rows on the coordinator.

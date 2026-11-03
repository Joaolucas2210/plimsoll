# ADR 0092 — CTAS and post-shuffle writes on workers

- Status: Accepted
- Date: 2026-11-03
- Owner: JoaoLucas2210
- Related: ADR 0091 (writes on workers), ADR 0090 (CTAS), ADR 0064/0065 (shuffled aggregates and joins), ADR 0073 (shuffle release)
- Scope: `remote::WriteTo` (`Fragment::write`, `Reduce::write`), `RemoteReduceExec::writing`, `remote_write::distribute`, `RemoteWriteExec` (any source), `IcebergConnector::{create_as, write_files}`, `Engine::plan_query`, `TableDdl::create_table_as` (a plan), the worker's reduce write

## Context

ADR 0091 moved `INSERT … SELECT` over a remote scan to the workers.
Two common large writes still pulled every row to the coordinator:

- **`CREATE TABLE … AS SELECT`.** Its query ran as a stream handed to
  the catalog code, so the write could never be planned onto workers.
  The staged table also has no metadata file a worker could open.
- **Writes of aggregated or joined rows** (`INSERT INTO daily SELECT
  day, sum(x) … GROUP BY day`, joins of two large tables). These rows
  only exist after a shuffle, on the reducing workers (ADR 0064, 0065),
  and travelled to the coordinator to be written.

## Decision drivers

1. **Write where the rows are**: on the scan fragments for plain rows,
   on the reducers for shuffled ones.
2. **One commit, decided on the coordinator**, with the semantics of
   each statement: one `append` for `INSERT`, the create transaction
   for CTAS.
3. **One mechanism.** The same rule decides for `INSERT` and CTAS.

## Decision

- **What a worker writes into** (`WriteTo`, on fragments and on
  reduces):
  - the table's metadata file;
  - for a staged create, the staged metadata itself, sent inline, since
    no file exists yet;
  - optionally, the output columns to write, by position in the table's
    order (`picks`). A reduce's output columns have no names to select
    by.
- **`distribute(sink, input)`** recognizes these sources under
  pass-through nodes, accounting wrappers and a picking or
  lossless-cast projection (ADR 0091):
  - a `RemoteScanExec` of plain rows, written by its fragments;
  - a `RemoteReduceExec`, a shuffled aggregate or join, written by its
    reducers.

  `RemoteWriteExec` now runs any such source: it executes every
  partition, collects the reported files and commits them through the
  sink. With a staged sink, that commit is the `assert-create`
  transaction (ADR 0090).
- **CTAS gets a plan, not a stream.**
  - The engine plans and checks the query without running it (the same
    grants, row filters and audit, ADR 0090) and hands the physical plan
    to the catalog code.
  - `create_as` builds the staged sink and either distributes the plan
    or, when it cannot, runs it here as before.
- **Reducers write** after merging or joining. The picked columns are
  selected in the table's order, then written with the partitioned
  writer.
- **Lifetimes.** A shuffle's reduce plan releases its maps on the
  workers when dropped (ADR 0073). `RemoteWriteExec`'s stream therefore
  holds its source until the commit. Callers that drop the plan once
  they have the stream, as `collect` does, would otherwise release the
  maps before the reducers asked for them. That happened on the first
  try of a distributed CTAS.

## Consequences

**Positive**
- Large CTAS, aggregated rollups (`INSERT … GROUP BY`) and joins
  between large tables write their files on the workers. The
  coordinator only commits.
- One rule decides for both statements; anything it does not
  recognize is written on the coordinator, unchanged.

**Negative (accepted debt)**
- **Broadcast joins** (ADR 0066) are not written by their probe
  fragments yet, even though the fragments have the joined rows.
- **Computed columns** above the source (a literal, an expression)
  still keep the write on the coordinator.
- `UPDATE`, `DELETE` and `MERGE` still run on the coordinator.
- **The staged metadata travels with every fragment and reduce.** That
  is a few kilobytes per task.

## Validation

- `remote::tests::fragments_round_trip_as_json`: `write`, with inline
  metadata and picks, round-trips.
- `tests/distributed.rs::writes_on_workers_through_the_rest_catalog`
  (live in CI, on S3), with two in-process workers:
  - an `INSERT … SELECT … GROUP BY` plans as
    `RemoteWriteExec source=RemoteReduceExec` and writes 2 rows;
  - an `INSERT … SELECT … JOIN` (shuffled) writes the 28 matching rows
    from its reducers;
  - a CTAS of a filtered scan writes 536 rows, and a CTAS of an
    aggregate has the right counts per group;
  - the earlier `INSERT` cases still hold.
- By hand, with a coordinator and two worker processes against the
  reference REST catalog:
  - a partitioned CTAS of 1736 rows had one snapshot, with counts and
    sums equal to the source;
  - an aggregate CTAS matched a plain `GROUP BY`;
  - an aggregate `INSERT` was written by the reducers (`aggregate_reduce`
    tasks writing).

## Revisit when

- Broadcast joins dominate writes: let probe fragments carry `write`
  as well.
- Row-level writes need to scale: plan `DELETE` / `UPDATE` / `MERGE`
  file by file on workers, each returning its position deletes and new
  files.

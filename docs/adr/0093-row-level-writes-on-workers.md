# ADR 0093 — `DELETE` and `UPDATE` on workers

- Status: Accepted
- Date: 2026-11-03
- Owner: JoaoLucas2210
- Related: ADR 0077 (DELETE), ADR 0081 (UPDATE), ADR 0091/0092 (writes on workers), ADR 0062/0063 (portable SQL), ADR 0058/0060 (fragments, retry)
- Scope: `remote::RowChange` (`Fragment::change`), `RemoteScanExec::changing`, `IcebergTable::{on_workers, rows_with}`, `IcebergConnector::rewrite_files`, `IcebergSink::{rewrite, commit_change}`, `RemoteWriteExec` (row changes), `Written` JSON (content, referenced file), the worker's `row_change` task

## Context

A `DELETE` or `UPDATE` reads every target file whole, in file order, to
find the positions it changes (ADR 0077, 0081). That is the most
expensive part of the statement, and all of it ran on the coordinator,
even on a cluster whose workers already scan those files for queries.

Each target file is independent. Its position deletes, and an update's
rewritten rows, depend on that file alone: the predicate, the new
values, and the deletes already applied to it at the planned snapshot.

## Decision drivers

1. **Same files, same commit.** What workers write must be what the
   coordinator would have written. Position deletes go in their data
   file's spec and partition (ADR 0085), rewritten rows through the
   partitioned writer, and one `delete` or `overwrite` commit with the
   coordinator's conflict checks.
2. **Fall back rather than guess.** Anything that cannot be shipped
   exactly runs on the coordinator as before.

## Decision

- **Planning (coordinator).** After `DELETE` / `UPDATE` planning has
  chosen the target files, `on_workers` replaces the local
  `IcebergDeleteExec` when all of these hold:
  - healthy workers exist;
  - the table is not read with vended credentials, and is
    workers-writable (ADR 0091);
  - the snapshot's schema is the table's current schema, since workers
    read at the snapshot;
  - the predicate (the `WHERE`, row filters included, ADR 0077) and
    every new value round-trip as portable SQL (ADR 0062, 0063).

  The target files are split across workers by bytes (ADR 0060). Each
  fragment carries `change: {predicate, update}`, and a
  `RemoteWriteExec` in row-change mode runs them.
- **On each worker** (`row_change` task):
  - it opens the table at the snapshot with only its files, and
    re-plans each file's surviving rows (earlier deletes applied) as
    the coordinator does (`rows_with`, honoring the fragment's files);
  - it parses the predicate and new values back against the schema;
  - it runs the shared `rewrite`: position deletes per file and, for
    `UPDATE`, the rewritten rows as data files;
  - it reports every file as JSON, now including `content` and the
    referenced data file.
- **Commit (coordinator).** Deleted rows are the position deletes'
  count. If any, all files are committed as `delete`, or `overwrite`
  for `UPDATE`, through the same `commit_change` the local path uses:
  conflicts only over appends, one manifest per content and spec.
- **`MERGE INTO` stays on the coordinator.** Its clauses are compiled
  over the merged target-and-source row, whose columns can share names,
  so they do not travel as SQL over one table. Its source would also
  have to be shipped to every worker.

## Consequences

**Positive**
- Large deletes and updates scale with the workers. The coordinator
  plans, assigns files and commits.
- One rewrite path (`IcebergSink::rewrite`) serves both the local and
  the distributed statement.

**Negative (accepted debt)**
- **`MERGE` is not distributed.** It would need a broadcast of the
  source (as broadcast joins do, ADR 0066) and clauses serialized
  against the merged row.
- **UPDATE's rewritten rows are buffered per worker**, as locally (ADR
  0081). That is now each worker's pool, which is better, but still
  not streamed.
- A fragment retried after a partial write leaves orphan files (ADR
  0091).

## Validation

- `write::tests::rows_split_by_partition_value`: a position delete
  file's report keeps its content, referenced file, spec and the
  reserved column's bounds.
- `remote::tests::fragments_round_trip_as_json` covers `change`.
- `tests/distributed.rs::writes_on_workers_through_the_rest_catalog`
  (live in CI, on S3): with two workers, a `DELETE` and an `UPDATE`
  plan as `RemoteWriteExec`. Their counts equal the rows matched
  beforehand, deleted rows are gone, and updated rows carry their new
  values.
- By hand, with a coordinator and two worker processes against the
  reference REST catalog, on a table partitioned by region:
  - a `DELETE` of 136 of 2000 rows, and an `UPDATE` of 184 that moved
    them to a new partition, ran as `row_change` tasks;
  - counts, sums and per-partition totals equal the same computation
    over the source;
  - Spark 3.5 read the same partitions and totals.
  - A first run reported double counts: the worker's planning ignored
    the fragment's file list. Fixed in `rows_with`, and the test checks
    counts exactly.

## Revisit when

- `MERGE` needs to scale: broadcast the source with each fragment, and
  ship the clauses as SQL over a merged schema with qualified names.
- Updates of more rows than a worker's pool holds: stream the rewritten
  rows into the writer.

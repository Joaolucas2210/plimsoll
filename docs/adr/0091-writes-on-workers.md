# ADR 0091 — Data files written on workers

- Status: Accepted
- Date: 2026-11-03
- Owner: JoaoLucas2210
- Related: ADR 0058 (remote scans), ADR 0062/0063 (filters and projections on workers), ADR 0075/0076 (writer), ADR 0060 (fragment retry)
- Scope: `Fragment::write`, `RemoteScanExec::writing`, `iceberg::remote_write::{push_write, RemoteWriteExec}`, `IcebergConnector::write_files`, `Written::{to_json}`, `IcebergSink::{written_from_json, commit_files, workers_may_write, metadata_path}`, the worker's `write` task

## Context

Reads are distributed: the coordinator splits a scan's files into
fragments, and workers filter and project them (ADR 0058, 0062, 0063).
Writes were not. An `INSERT INTO t SELECT … FROM s` with a distributed
scan pulled every selected row from the workers to the coordinator,
which encoded and uploaded all the Parquet itself.

For a copy or a filtered load, that makes the coordinator's network
and CPU the ceiling, even though each worker already has the rows the
file needs.

## Decision drivers

1. **Same table, same answer.** The files a worker writes must be the
   ones the coordinator would have written: schema, field ids,
   partition spec, statistics.
2. **One commit.** A distributed write is still one atomic append,
   decided on the coordinator, which alone talks to the catalog.
3. **Only when it is the same computation.** If the rows are not
   exactly a fragment's output (an aggregate, a join, an expression),
   keep today's path.

## Decision

- **Planning.** The coordinator's physical rule (with partial
  aggregation and shuffles) replaces `DataSinkExec(IcebergSink)` with a
  `RemoteWriteExec` when all of these hold:
  - between the sink and a `RemoteScanExec` there are only
    pass-through nodes (merges, repartitions, batch coalescing) and at
    most one projection;
  - that projection only picks, reorders or losslessly casts columns,
    which is how `INSERT` maps a query onto the table: widening an
    integer or a float, or switching a string or binary encoding;
  - the fragments return plain rows, not aggregated, joined or
    shuffled ones;
  - the target is not read with vended credentials, which never leave
    the coordinator (ADR 0058), and is not a staged create.

  The fragments then carry `write` (the target's metadata file) and
  `columns` in the target's order.
- **On each worker** (a `write` task):
  - it scans, filters and projects its files as for a scan;
  - it opens the target at the coordinator's metadata file, so the
    schema and partition spec are the same;
  - it writes the rows with the partitioned writer, under the table
    location and with fresh file names, and does not commit;
  - it returns one row per file: JSON with path, row count, size,
    partition tuple and per-column counts and bounds. Types are not
    sent; the coordinator looks them up by field id in its layout.
- **On the coordinator.**
  - `RemoteWriteExec` runs every fragment at once.
  - It rebuilds the reported files and commits them through the sink's
    regular commit: one `append`, the same conflict checks and retries,
    and a schema or spec change in between fails the commit (ADR 0075).
  - It returns the row count.
- **Retries** (ADR 0060). A fragment that fails before answering is
  re-sent once to another worker. Files the first attempt may have
  written are never reported, so they are not committed; they are
  orphans, which orphan removal reclaims (ADR 0080).

## Consequences

**Positive**
- Copies and filtered loads between Iceberg tables scale with the
  workers. The coordinator handles one JSON line per file, not the
  rows.
- Nothing changes for readers: the files and the commit are those of a
  local write.

**Negative (accepted debt)**
- **Only `INSERT … SELECT` over a remote scan** is distributed. These
  still write on the coordinator:
  - `CREATE TABLE … AS SELECT` (staged, ADR 0090);
  - writes of aggregated, joined or shuffled rows, and projections with
    computed expressions;
  - `UPDATE`, `DELETE` and `MERGE`.
- **Each worker writes its own files per partition**, so a table gets
  up to (workers × partitions) files per write. Compaction (ADR 0078)
  merges them.
- A failed attempt leaves orphan files until orphan removal runs.

## Validation

- `write::tests::rows_split_by_partition_value`: a reported file
  (partition values, integer, string and decimal bounds) round-trips
  through JSON, with types taken from the layout; a malformed report
  is refused.
- `remote::tests::fragments_round_trip_as_json` covers `write`, and an
  unknown field is still refused (version mismatch).
- `tests/distributed.rs::writes_on_workers_through_the_rest_catalog`
  (live in CI, on S3), with two in-process workers and a 4-file
  source:
  - a filtered, reordered and widened `INSERT … SELECT` plans as
    `RemoteWriteExec fragments=2` and writes 536 rows;
  - count, sum and minimum equal the source's under the same filter;
  - the target has exactly one `append` snapshot;
  - an aggregate's insert stays on the coordinator.
- By hand, against the reference REST catalog, with a coordinator and
  two worker processes:
  - the same insert of 1736 rows was written as 4 files, each worker
    logging a `write` task;
  - Spark 3.5 and pyiceberg read the same rows and files and pruned on
    the new files' bounds;
  - an aggregate insert planned as before.

## Revisit when

- CTAS of large queries: write on workers into the staged location,
  then the create transaction (ADR 0090).
- Writes after a shuffle: let reducers write their buckets.
- Too many small files per write: have workers exchange rows by
  partition first (one writer per partition), or compact after large
  loads.

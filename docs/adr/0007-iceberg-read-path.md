# ADR 0007 — Iceberg read path under the ceiling

- Status: Accepted
- Scope: `plimsoll-connectors::iceberg`, `plimsoll-core::memory::AccountedExec`

## Context

Phase 1 needs Iceberg reads, and the project's claim is that *all* query
memory goes through the `CeilingPool` (ADR 0003). Two facts shape the choice:

- `iceberg-datafusion` 0.10 is pinned to DataFusion 53 / Arrow 58; we are on
  DataFusion 55 / Arrow 59. Its scan also uses iceberg-rust's own Arrow reader,
  which reserves nothing in DataFusion's memory pool.
- DataFusion's own Parquet scan does not reserve its decoded batches either
  (only the Parquet *writer* does).

## Decision

1. **Walk the metadata ourselves, read data with DataFusion.** `metadata.json`
   → manifest list → manifests (Avro) yields the live data files of the
   current snapshot; they go to DataFusion's `ParquetSource` with the session's
   Parquet options, projection, limit and the filters as a pruning predicate
   (reported `Inexact`, so every filter is re-checked above the scan).
2. **Account what DataFusion doesn't.** Manifest bytes are reserved in the
   session pool while decoded (`IcebergManifests[table]`), and the scan is
   wrapped in `AccountedExec` (`IcebergScan[table]`), which keeps each
   partition's in-flight batch reserved. Over-counting by one batch is
   accepted: it can only fail a query early, never let the node overshoot.
3. **Fail instead of guessing.** Typed errors for: row-level delete files
   (merge-on-read), non-Parquet data files, nested types, format v3, and
   schema histories where a field id and a column name disagree (rename or
   drop+re-add) — DataFusion maps file columns by name, Iceberg by id.
4. **Dependency:** `apache-avro` without codec features (null + deflate, the
   Java and pyiceberg defaults). No iceberg-rust, no second Arrow version.

## Alternatives

- **Downgrade to DataFusion 53 and use `iceberg-datafusion`:** less code, but
  reads bypass the ceiling and we would be pinned to iceberg-rust's cadence.
- **`iceberg` crate for metadata only:** compiles a second Arrow (58) plus
  opendal/reqwest just to list files.
- **`ListingTable` over the data directory:** wrong — it would read files that
  are no longer in the snapshot (deleted, compacted, uncommitted).

## Consequences

- Correct for append-only and copy-on-write tables; interop with pyiceberg is
  tested in CI (`scripts/iceberg_fixture.py`).
- Debt, in order of value: manifest pruning by partition summaries and column
  bounds; delete-file application; field-id-based column mapping; snapshot
  re-resolution per query via the REST catalog (today pinned at registration);
  snappy/zstd Avro codecs.

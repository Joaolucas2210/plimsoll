# ADR 0007 — Iceberg read path under the ceiling

- Status: Accepted
- Date: 2026-10-29
- Owner: JoaoLucas2210
- Related: ADR 0003, ADR 0005, ADR 0006, ADR 0008, ADR 0009, ADR 0010
- Scope: `plimsoll_connectors::iceberg`, `plimsoll_core::memory::AccountedExec`

## Context

Phase 1 needs Iceberg reads, and the product claim is that query memory goes
through the `CeilingPool` (ADR 0003). Facts at decision time:

- `iceberg-datafusion` 0.10 is pinned to DataFusion 53 / Arrow 58; we are on
  DataFusion 55 / Arrow 59 (ADR 0001). Its scan uses iceberg-rust's own Arrow
  reader, which reserves nothing in DataFusion's memory pool.
- DataFusion's Parquet scan does not reserve its decoded batches either (only
  the Parquet *writer* reserves).
- A directory listing is not a table: data files outside the current snapshot
  (deleted, compacted, uncommitted) must never be read.
- DataFusion maps Parquet columns to the table schema **by name**; Iceberg
  maps them **by field id**.

## Decision drivers

1. Correctness: read exactly the live files of one snapshot; refuse what we
   cannot read exactly.
2. All read-path memory visible to the ceiling.
3. One DataFusion/Arrow version in the tree.
4. Interoperability with the reference writers (pyiceberg, Java).
5. Code we own stays small.

## Considered options

### Option A — Walk metadata ourselves, scan with DataFusion's `ParquetSource` (chosen)

`metadata.json` → manifest list → manifests (Avro) → live data files → a
`DataSourceExec` over `ParquetSource`. Pros: drivers 1–3; scan inherits
DataFusion's Parquet pruning and options. Cons: we own spec parsing and every
Iceberg feature we add (deletes, evolution, pruning).

### Option B — Downgrade to DataFusion 53 and use `iceberg-datafusion`

Least code. Cons: reads bypass the ceiling; we would track iceberg-rust's
release cadence for every DataFusion upgrade.

### Option C — `iceberg` crate for metadata only

Compiles a second Arrow (58) plus opendal/reqwest just to list files.

### Option D — `ListingTable` over the data directory

Wrong: reads files that are not in the snapshot.

## Decision

1. **Metadata walk.** `IcebergTable::from_metadata` parses `metadata.json`
   (format v1/v2), picks `current-schema-id` and `current-snapshot-id`
   (`-1`/absent → empty table). `data_files` decodes the manifest list and
   each manifest, skips entries with `status = 2` (deleted), and returns the
   `file_path` of live Parquet data files. `IcebergConnector::try_new` accepts
   a `*.metadata.json` or a table directory (newest `metadata/*.metadata.json`,
   `vN` or `NNNNN-uuid` naming); `from_metadata` builds from a REST
   `loadTable` response without re-reading (ADR 0008).
2. **Scan.** Files are distributed round-robin over `target_partitions`
   file groups and read by
   `ParquetSource` with the session's Parquet options, projection, limit and
   the conjunction of filters as pruning predicate. The source reports
   `Inexact` and the connector policy is `uniform(Inexact)`, so the guard
   re-applies every filter above the scan (ADR 0005).
3. **Accounting.** Manifest-list and manifest bytes are reserved
   (`IcebergManifests[<table>]`) for the duration of each Avro decode; the scan
   is wrapped in `AccountedExec` (`IcebergScan[<table>]`), which keeps each
   partition's in-flight batch reserved. Over-counting one batch may fail a
   query early; it never lets the node overshoot.
4. **Refuse instead of guessing** — `ConnectorError::Iceberg("unsupported
   at …")`: `format-version` other than 1–2, nested types, unknown primitive
   types, a v1 snapshot without `manifest-list` and any schema history where a
   field id and a name disagree (rename, drop + re-add) at registration;
   row-level delete files (merge-on-read) and non-Parquet data files at scan.
   Files spanning more than one object store → `NotImplemented` at scan.
5. **Dependency:** `apache-avro 0.22` with `default-features = false` (null and
   deflate codecs, the pyiceberg/Java defaults). No iceberg-rust, no second
   Arrow.

Why over Option B: it is the only option that keeps the read path under the
ceiling without forking a dependency, at the cost of owning spec parsing.

## Consequences

**Positive.** Correct for append-only and copy-on-write tables. Reads are
attributable in OOM errors (`IcebergScan[bronze.trips][3]`). Interop with
pyiceberg-written tables is exercised in CI, locally and on S3 (ADR 0009).

**Negative (accepted debt).**
- As decided here, planning cost was O(manifests + files): every manifest
  decoded and every file `HEAD`-ed per scan. ADR 0010 refines this step
  (manifest/file pruning, sizes from `file_size_in_bytes`); the rest of this
  decision stands.
- Decoded manifest entries (`Vec<apache_avro::Value>`) and `metadata.json`
  bytes are not reserved; only raw Avro bytes during decode are.
- No delete-file application; no field-id column mapping; no snappy/zstd Avro.
- `try_new` pins the snapshot at registration (`PLIMSOLL_ICEBERG_TABLES`);
  catalog tables are re-resolved per query (ADR 0008).

**Operational.** Unsupported features are typed errors, never wrong answers.
At registration they fail boot; at scan they fail the query. Note the HTTP
mapping: `ConnectorError::Iceberg` reaches the server as
`DataFusionError::External` → **500 `internal`**, while the multi-store case
(`NotImplemented`) maps to **400 `invalid_query`**. Ceiling breaches name
`IcebergScan[...]` or `IcebergManifests[...]` (422).

## Risks and mitigations

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| Merge-on-read tables in the wild fail every query | High | Medium | Typed error names the manifest; delete-file support is the next correctness item |
| Planning latency on large tables | High | Medium | ADR 0010 (manifest and file pruning) |
| Spec edge cases misparsed (v1 vs v2 fields) | Medium | High | Interop test against pyiceberg; refuse on missing required fields |
| Column renamed in a future writer | Medium | High | Name/id mismatch check across all schemas rejects at registration |

## Validation

- `crates/plimsoll-connectors/tests/iceberg.rs`:
  `reads_only_live_files_of_the_current_snapshot` (orphan and status-2 files
  ignored; asserts `AccountedExec: IcebergScan[t]`),
  `delete_files_fail_instead_of_returning_deleted_rows`,
  `renamed_columns_are_rejected_at_registration`, `empty_table_has_no_rows`,
  `read_path_runs_under_the_ceiling` (512-byte ceiling → `ResourcesExhausted`
  naming `Iceberg`, pool back to 0).
- CI job `iceberg-interop`: `reads_a_pyiceberg_table` against
  `scripts/iceberg_fixture.py` output (two appends + copy-on-write delete;
  expects 7 rows, sum 49), first on a local warehouse, then on S3.

## Revisit when

- A target table uses merge-on-read deletes or column renames.
- Planning time dominates query latency (ADR 0010 is the first answer).
- An Iceberg crate supports our DataFusion major and reserves through the
  session `MemoryPool`: re-evaluate Option B/C.
- Format v3 tables appear in a target catalog.

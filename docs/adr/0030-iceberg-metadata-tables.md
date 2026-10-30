# ADR 0030 — Iceberg metadata tables

- Status: Accepted
- Date: 2026-10-30
- Owner: JoaoLucas2210
- Related: ADR 0028 (time travel), ADR 0029 (refs; resolves its "refs are not listable" debt), ADR 0025 (grants), ADR 0008 (per-query `loadTable`), ADR 0003 (memory ceiling)
- Scope: `plimsoll_connectors::iceberg::meta`, `plimsoll-server::lakehouse::CatalogSchema::table`

## Context

ADRs 0028 and 0029 let users read `bronze."trips@<spec>"`, but finding a
spec still required the table's `metadata.json`:

- snapshot ids, and what each one did;
- the instants when each one became current;
- which branches and tags exist.

Users usually cannot read that file: it sits in object storage behind
credentials the catalog vends to Plimsoll only. Spark, Trino and Flink all
expose this history as **metadata tables** (`db.t.snapshots`,
`"t$snapshots"`). Without them, time travel is only usable by people who can
reach the warehouse bucket.

## Decision drivers

1. Same access path and governance as data reads: grants (ADR 0025) must
   apply with no new check.
2. As fresh as a data read: a commit is visible to the next query (ADR
   0008).
3. Column names that match another engine, so audit queries port.
4. No new dependency and no extra I/O beyond the `loadTable` every query
   already does.

## Considered options

**A. `"table$<kind>"` names resolved by the catalog schema, built in memory
from the loaded metadata (chosen).** `$` follows Trino's convention, and
like `@` it needs quoting. The `TableScan` reference stays
`bronze."trips$snapshots"`, so grants see schema `bronze` (driver 1). The
rows come from the same `loadTable` response, so no object-store read is
added (driver 4).

**B. Spark's dotted form, `bronze.trips.snapshots`.** In DataFusion a
three-part name is `catalog.schema.table`, so this would parse as table
`snapshots` in schema `trips` of catalog `bronze`. Supporting it means a
catalog provider per namespace that guesses whether a part is a table.
Rejected: ambiguous and invasive.

**C. Table functions (`iceberg_snapshots('bronze.trips')`).** The reference
would be the function, outside any schema, so grants would need a separate
check (the same reason ADR 0028 rejected them). Rejected.

## Decision

- `iceberg::meta::table(metadata, kind)` returns one `RecordBatch`, or
  `None` when `kind` is not a metadata table. The server wraps it in a
  `MemTable`.

  | Table | Columns (Spark names) | Source |
  |---|---|---|
  | `$snapshots` | `committed_at`, `snapshot_id`, `parent_id`, `operation`, `manifest_list`, `schema_id`, `summary` | `snapshots` |
  | `$history` | `made_current_at`, `snapshot_id`, `parent_id`, `is_current_ancestor` | `snapshot-log` |
  | `$refs` | `name`, `type` (`BRANCH`/`TAG`), `snapshot_id`, `max_reference_age_in_ms`, `min_snapshots_to_keep`, `max_snapshot_age_in_ms` | `refs` |

  Timestamps are `Timestamp(ms, "UTC")`. `$refs` is sorted by name.
- `is_current_ancestor` walks parents from `current-snapshot-id`. A log
  entry outside that chain was rolled back. The walk is bounded by the
  snapshot count, so a corrupt parent cycle cannot hang planning.
- Resolution in `CatalogSchema::table`: when the name has `$`, and the suffix
  is a known kind and the base loads, the result is the metadata table.
  Otherwise the existing `@` and whole-name paths run unchanged, so a table
  literally named `a$b` still resolves (ADR 0029's fallback rule).
- A missing key (a new table with no snapshots) produces an empty table,
  not an error.

## Consequences

**Positive**
- Time travel is self-serve. For example:
  `SELECT snapshot_id FROM bronze."trips$history" WHERE made_current_at <= TIMESTAMP '…'`,
  then `bronze."trips@<id>"`.
- Write-audit-publish flows can see where each branch head is (`$refs`)
  before they publish.
- No new execution code. The batches are at most a few thousand rows and are
  built in planning, as small as the metadata JSON already held.

**Negative (accepted debt)**
- `summary` is a JSON string, not a `MAP` column. Query it with
  `json`-style string functions or `LIKE`. Upgrade to `MapArray` if users
  need typed map access.
- Only three tables. `$files`, `$manifests`, `$partitions` and
  `$all_data_files` need manifest reads under the memory ceiling, and are
  added when asked for.
- The metadata tables are not listed in `information_schema`, as with the
  `@` variants (ADR 0028).
- `$` and `@` do not combine (`"t@v1$refs"`). Metadata tables always show
  the current metadata, which already contains the full history.
- The batch is not reserved in the `CeilingPool`. It is bounded by the
  metadata JSON, which the catalog client already holds in full. Revisit
  only for tables with very many snapshots.

## Risks and mitigations

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| A real table name ends in `$snapshots`, `$history` or `$refs` | Very low | Low | The metadata table wins only when the base table exists; otherwise the whole name is looked up. |
| Metadata reveals file locations (`manifest_list`) | Expected | Low | Same grant as the data: a user who can read the table can read its history. Locations are not credentials. |
| Huge snapshot lists (frequent streaming commits) | Low | Low | Bounded by the metadata the catalog already returned; expire snapshots. |

## Validation

- `iceberg::meta::tests::metadata_tables_list_snapshots_history_and_refs`:
  - a 1 → 2 → 3 lineage rolled back to 2, plus a branch commit 4;
  - `is_current_ancestor` is false only for the rolled-back 3;
  - the summary is JSON, and the operation is extracted;
  - refs are sorted, with upper-case types and retention fields;
  - an unknown kind gives `None`, and empty metadata gives an empty table.
- `lakehouse::tests::metadata_tables_list_history_through_sql`: through the
  catalog schema and SQL, `$snapshots` is ordered by `committed_at`, `$refs`
  filters by name and `$history` counts rows. An unknown `$files` falls back
  to the name lookup and is not found.
- `tests/iceberg.rs::time_travels_a_spark_table` (interop) on Spark's
  table:
  - `$refs` lists `after_insert` as `TAG`, and `audit` and `main` as
    `BRANCH`, at the same ids `@<ref>` reads;
  - `$snapshots` has one row per snapshot, each with an operation;
  - `$history` marks the current lineage.

## Revisit when

- Users need file-level metadata (`$files`, `$partitions`): read manifests
  through the existing planning path under the ceiling.
- `summary` needs typed access (switch to `MAP<STRING, STRING>`).
- DataFusion supports nested catalogs well enough for Spark's dotted form.

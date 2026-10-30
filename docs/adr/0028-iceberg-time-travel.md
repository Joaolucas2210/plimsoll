# ADR 0028 — Iceberg time travel by table-name suffix

- Status: Accepted
- Date: 2026-10-30
- Owner: JoaoLucas2210
- Related: ADR 0007 (read path), ADR 0008 (per-query `loadTable`), ADR 0010/0014/0016/0017 (planning applies unchanged), ADR 0022/0025 (grants must still apply), ADR 0024 (read-only API)
- Scope: `plimsoll_connectors::iceberg::{AsOf, at}`, `plimsoll-server::lakehouse::CatalogSchema::table`, scan label

## Context

Reading a table as it was is a core lakehouse capability. It covers audits
("what did the report see on the 1st?"), reproducing a bug against the data
of that day, and comparing before and after a bad write.

Iceberg keeps every snapshot until expiry:

- `snapshots` lists them, each with `manifest-list` and `schema-id`;
- `snapshot-log` records which snapshot was current, and when, on the main
  branch.

Plimsoll always read `current-snapshot-id`.

DataFusion parses neither `FOR SYSTEM_TIME AS OF` nor `VERSION AS OF` into
anything it can plan. So the syntax is a design decision.

## Decision drivers

1. Same guarantees as current reads:
   - pruning;
   - position, equality and DV deletes;
   - the delete cache;
   - the memory ceiling.

   Ideally with zero changes to the read path.
2. Grants (ADR 0025) must apply unchanged, because they check the schema of
   each `TableScan` reference.
3. Old snapshots read with **their** schema. A column added later must not
   appear, and one dropped later must still be readable.
4. Timestamp semantics must match Iceberg's: "the snapshot that was current
   at T on main", not "the newest snapshot created before T", which could
   come from another branch.

## Considered options

**A. Table-name suffix, `schema."table@<spec>"` (chosen).** The catalog
schema provider strips the suffix, loads the table and pins the snapshot.
The `TableScan` reference stays `schema."table@…"`, so grants see the right
schema (driver 2). It looks like Iceberg's own `table@snapshot` conventions
(Trino uses `"table@123"`-style names for snapshot tables). It needs
quoting, because `@` is not an identifier character.

**B. Table function `iceberg_at('bronze.trips', 123)`.** The `TableScan`
reference would be the function name, not `bronze.trips`, so grants would
see schema `public`. It needs its own grant check and argument parsing.
Rejected for driver 2.

**C. Extend the SQL parser and planner for `FOR SYSTEM_TIME AS OF`.** This
is the nicest syntax, but it means carrying a DataFusion fork or upstream
work. Revisit when DataFusion supports it natively.

**D. Session setting (`SET iceberg.as_of = …`).** Statements are disabled
(ADR 0024), and a session-wide setting cannot pin two tables differently in
one query.

## Decision

- **Spec.** `AsOf::parse`:
  - an `i64` is a snapshot id;
  - an RFC 3339 instant with an offset is a timestamp, in ms since the
    epoch;
  - anything else is not a time-travel spec, and the whole name, `@`
    included, is looked up as a table. A bare date is not an instant.
- **Pinning.** `iceberg::at(metadata, as_of)` returns metadata with
  `current-snapshot-id`, and the snapshot's `schema-id` when it recorded
  one, set as current. Everything downstream is unchanged: schema, pruning
  columns, manifests, deletes and caches. This is driver 1 by construction.
- **Timestamps** resolve through `snapshot-log`: the latest entry with
  `timestamp-ms` ≤ T. A table without a log, or an instant before the first
  entry, is an error.
- **Errors.** An unknown snapshot or an instant before history becomes a
  `Plan` error, `bronze.trips@: snapshot 999 not found`, answered as
  `400 invalid_query`.
- **Scope.** Catalog-backed tables (the REST catalog, `lakehouse`). Env
  tables (`PLIMSOLL_ICEBERG_TABLES`) are pinned at registration and do not
  parse suffixes.
- **Observability.** The scan label gains `snapshot=<id>`, so `EXPLAIN`
  shows which snapshot a query read.

## Consequences

**Positive**
- Time travel inherits every read-path guarantee with no new execution
  code.
- Grants and principal scoping apply, because the reference's schema is
  unchanged.
- The answers match Spark's own history on the reference table.

**Negative (accepted debt)**
- Quoted identifiers: `bronze."trips@123"`. Less ergonomic than
  `FOR SYSTEM_TIME AS OF`.
- No branches or tags by name yet (`refs`). That is the next ROADMAP item.
- Expired snapshots are gone. An instant whose snapshot was expired
  resolves to "snapshot not found".
- Env-registered tables do not support it.
- `information_schema` lists only base names, not `@` variants.

**Operational**
- Audits: query with `@<instant>` and record the `snapshot=` from `EXPLAIN`
  for reproducibility; a snapshot id is stable, an instant is not once
  snapshots expire.

## Risks and mitigations

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| A real table name contains `@` followed by digits or an instant | Very low | Medium | Only a suffix that parses as a spec is treated as one; quote-free names never contain `@`. |
| Branch snapshots chosen for an instant | Low | High (wrong data) | Instants use `snapshot-log` (main lineage), not `snapshots`. |
| Old schema missing from metadata (very old v1 tables) | Low | Low | Falls back to the current schema id; reads fail loudly if columns mismatch. |

## Validation

- `tests/iceberg.rs::as_of_parses_snapshot_ids_and_instants`: ids, UTC and
  offset instants, and non-specs (words, bare dates).
- `lakehouse::tests::time_travel_by_snapshot_id_or_instant`, with the
  catalog and two snapshots that have different schemas:
  - the current schema is `b`;
  - `@1` sees `a` and not `b`;
  - instants between and after the two snapshots choose correctly;
  - an instant before history gives an error, and so does an unknown id;
  - `@yesterday` is looked up as a name.
- `tests/iceberg.rs::time_travels_a_spark_table` (interop workflow), on
  Spark's MOR table:
  - the first snapshot reads 48 rows with sum 1128, before any delete;
  - the second reads 32 rows with sum 768, after `DELETE`;
  - the first log instant selects the first snapshot;
  - an instant before it errors;
  - the pool returns to 0.

## Revisit when

- DataFusion plans `FOR SYSTEM_TIME AS OF` / `VERSION AS OF` (switch
  syntax, keep `at`).
- Branches or tags are needed (`refs`).
- Env tables need it (a schema provider for env Iceberg tables).

# ADR 0075 — `INSERT INTO` Iceberg tables (append)

- Status: Accepted
- Date: 2026-11-03
- Owner: JoaoLucas2210
- Related: ADR 0024 (read-only query API, amended), ADR 0025 (grants), ADR 0008/0033 (REST catalog), ADR 0040 (field ids), ADR 0004 (dependency stance)
- Scope: `engine::plan_stream` (SQL options, write check), `TableAccess::check_write`, `iceberg::write::{IcebergSink, Committer, unwritable, manifest, manifest_list}`, `IcebergTable::insert_into`, `Catalog::update_table` (REST), `lakehouse::CatalogCommitter`, `PLIMSOLL_AUTH_WRITE_GRANTS`, `FederationFilterGuard::insert_into`

## Context

Plimsoll read lakehouse tables but could not write them: the query API
was read-only by design (ADR 0024). Writing is the largest gap between
a query engine and an open lakehouse platform. The first useful write
is the most common one: appending the result of a query to a table (ETL
and ELT steps, materialized rollups).

An Iceberg append has four parts:

1. data files: Parquet with Iceberg field ids;
2. a manifest that lists them;
3. a manifest list: the parent snapshot's manifests plus the new one;
4. an atomic commit of a new snapshot.

The REST catalog protocol does step 4 (`updateTable` with requirements
and updates). The catalog writes the new `metadata.json`, so concurrent
writers are serialized by it.

## Decision drivers

1. **Readable by every Iceberg engine.** Spark/Java and pyiceberg must
   read what Plimsoll commits, and Plimsoll must append on top of what
   they commit.
2. **The read-only guarantees of ADR 0024 stay.** No `COPY`, DDL,
   `UPDATE`/`DELETE` or `SET` through the API. Writes are granted
   explicitly, per schema.
3. **Atomic or nothing visible.** A failed or lost commit leaves the
   table unchanged.
4. **No new dependency.** `apache_avro` and `parquet` are already in the
   tree; the commit is a REST call.

## Considered options

**A. DataFusion's `insert_into` / `DataSink`, with the Iceberg format
written by Plimsoll (chosen).** DataFusion already parses `INSERT`, maps
and casts columns to the table's schema, and plans the source query with
every read rule (grants, row filters, distribution). The table provider
only supplies a sink.

**B. `iceberg-rust`'s writer and transaction API.** It is complete, but
it is a second Iceberg implementation beside Plimsoll's reader (ADR
0007), and a large dependency with its own `arrow` and `object_store`
versions.

**C. A bespoke `/v1/insert` endpoint.** It would duplicate SQL parsing,
column mapping and governance, which A gets for free.

## Decision

- **SQL surface (core).**
  - The query API now plans DML, but accepts exactly one write:
    `INSERT INTO <table> …`, as an append. `COPY`, `UPDATE`, `DELETE` and
    `INSERT OVERWRITE` are refused before planning anything
    ("the query API is otherwise read-only").
  - DDL and statements stay disabled (ADR 0024).
  - INSERTs bypass the result cache.
- **Authorization.**
  - `TableAccess::check_write` is checked on the target, and **denies by
    default**.
  - The server grants it from `PLIMSOLL_AUTH_WRITE_GRANTS=etl=bronze|silver`
    (schemas per principal). It is validated at boot like the read grants.
    Unset means no writes. System tables are never writable.
  - Development mode (`PLIMSOLL_AUTH=disabled`) writes, as it reads,
    everything.
  - The source query keeps every read rule: read grants, column denies,
    row filters. A write grant never lets anyone copy rows they may not
    read.
- **What can be appended (step 1).** Format v2 tables served by the REST
  catalog, unpartitioned, with primitive columns. Anything else is
  refused at planning, before a byte is written, with the reason:
  - v1 or v3;
  - partitioned;
  - nested columns;
  - tables registered from a path;
  - time-travel references.
- **Sink.**
  - Rows are relabelled with the table's field ids and written as
    Parquet under `<location>/data/`, rolling over at about 128 MiB. A
    `NULL` in a required column fails the insert.
  - One manifest lists the files as `ADDED`, with sequence numbers
    inherited.
  - Each attempt writes a manifest list: the new manifest, then every
    manifest of the parent's list carried over field by field. Unions are
    re-chosen, and v1-era count names are accepted.
  - Both Avro files use the v2 schemas with `field-id` attributes and
    the required file metadata (`schema`, `partition-spec`,
    `format-version`, `content`), compressed with **deflate**. The first
    version wrote no codec and pyiceberg refused to read it.
- **Commit.** `Catalog::update_table` (REST `POST …/tables/{t}`)
  receives:
  - `assert-table-uuid` and `assert-ref-snapshot-id(main = parent)`;
  - `add-snapshot` (sequence = last + 1, summary with
    `operation: append` and `engine-name: plimsoll`) and
    `set-snapshot-ref(main)`.

  A 409 (another writer committed first) reloads the table, rebuilds the
  manifest list on the new parent and retries, up to 4 attempts. Data
  files and the manifest are reused. A concurrent schema change aborts
  instead.

## Consequences

**Positive**
- `INSERT INTO gold.daily SELECT … FROM silver.events GROUP BY …` runs
  in Plimsoll, distributed reads included, and commits atomically
  through the catalog.
- Interop holds in both directions, checked by hand against the
  reference REST catalog:
  - Plimsoll appended twice;
  - pyiceberg and Spark 3.5 (Iceberg 1.9.2) read exactly the same rows
    and snapshots;
  - Spark appended on top;
  - Plimsoll appended on Spark's commit, carrying over its Java-written
    manifest list;
  - all three readers agreed on the 17 rows.
- No new dependency.

**Negative (accepted debt)**
- **No column statistics in data files** (bounds, null and value
  counts). Other engines and Plimsoll's manifest-level pruning cannot
  skip the new files; Parquet footer statistics still apply.
- **Writing is coordinator-only.** The sink runs where the `INSERT` is
  planned. Reads may be distributed; writes are not.
- **Orphan files.** A failed commit leaves its data files, manifest and
  manifest lists behind. Removing orphans is table maintenance, which is
  not built yet.
- **Step-1 limits.** No partitioned tables, nested types, v3, path-based
  tables, `UPDATE`/`DELETE`/`MERGE` or DDL. These are the next steps in
  the roadmap.

## Validation

- `iceberg::write::tests::manifests_round_trip_with_field_ids`:
  - the manifest's writer schema carries the field ids, and the entry
    reads back;
  - a second manifest list carries the first one's manifests, newest
    first.
- `iceberg::write::tests::only_v2_unpartitioned_flat_tables_take_appends`.
- `engine::tests::inserts_need_a_write_grant_and_other_writes_stay_refused`:
  - without a write grant: denied;
  - granted: rows appended and counted;
  - granted, but the source reads a schema the principal cannot read:
    denied;
  - `COPY`, `DELETE`, `UPDATE` and `INSERT OVERWRITE`: refused.

  `query_api_is_read_only` still passes; `COPY` writes nothing.
- `auth::tests::write_grants_name_the_schemas_a_principal_may_insert_into`:
  unset means no writes; per-schema grants; system tables are never
  writable; development mode writes; malformed grants fail boot.
- `lakehouse::tests::appends_through_the_rest_catalog` (live, in CI
  against `iceberg-rest-fixture` on S3):
  - it creates its own v2 table and makes two appends, reading the rows
    back;
  - the catalog shows two snapshots, parent to child, with
    `engine-name: plimsoll` and sequence number 2;
  - a `NULL` into a required column fails;
  - a partitioned table is refused.
- The manual interop runs (Spark and pyiceberg) are described above.

## Revisit when

- Files need pruning by other engines: write `value_counts`,
  `null_value_counts` and `lower_bounds`/`upper_bounds`. Plimsoll's
  reader already decodes the bound format.
- Large inserts bottleneck on the coordinator: write data files on
  workers and commit once on the coordinator.
- Partitioned tables are needed: route rows by the spec's transforms
  (the pruner already evaluates identity, bucket and truncate).

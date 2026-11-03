# ADR 0090 — `CREATE TABLE … AS SELECT` in one commit

- Status: Accepted
- Date: 2026-11-03
- Owner: JoaoLucas2210
- Related: ADR 0083 (CREATE TABLE), ADR 0075/0076 (writer), ADR 0025/0044/0045 (governed reads)
- Scope: `NewTable::query`, `Engine::ddl` (CTAS), `TableDdl::create_table_as`, `CatalogDdl::create` (`stage-create`), `IcebergConnector::create_as`, `IcebergSink::{create, staged}`, `write::creation_updates`

## Context

ADR 0083 refused `CREATE TABLE … AS SELECT` and pointed users to two
statements: create, then insert. That is two commits.

- A query that fails after the first leaves an empty table behind.
- A reader can see the table before it has rows.
- Retrying the pair then fails on "already exists".

Iceberg's REST protocol has the tool for this, the create transaction:

1. `createTable` with `stage-create: true` returns the new table's
   metadata (uuid, location, schema, spec) without creating anything.
2. Files are written under that location.
3. One `updateTable` with the requirement `assert-create` carries the
   table's creation and its first snapshot together.

## Decision drivers

1. **All or nothing.** Either the table exists with every row, or it
   does not exist.
2. **The query is a read like any other**: read grants, column denials
   and row filters all apply.
3. **One writer path**, the partitioned writer, for CTAS as for
   `INSERT`.

## Decision

- **Syntax:** `CREATE TABLE [IF NOT EXISTS] ns.t [PARTITION BY (…)]
  [OPTIONS (…)] AS <query>`.
  - The columns, names and types, come from the query. A column list
    with `AS` is refused.
  - Partitioning and properties work as in ADR 0083.
- **Governance.**
  - `check_write` on the new name.
  - The query runs through the regular planning path, so every scan's
    read grant, column denial and row filter applies, and it shows in
    the audit's `reads`.
  - Nothing is staged until the query has been planned and allowed.
- **Create transaction.**
  - The catalog stages the table. `IF NOT EXISTS` on an existing table
    is a no-op that writes 0 rows.
  - A connector over the staged metadata writes the query's rows with
    the partitioned writer.
  - The sink's commit uses the staged metadata instead of loading the
    table. It sends `assert-create` together with Iceberg's creation
    updates (`assign-uuid`, `upgrade-format-version`, `add-schema`,
    `set-current-schema`, `add-spec`, `set-default-spec`, the sort
    order, `set-location`, `set-properties`), then `add-snapshot` and
    `set-snapshot-ref main`.
  - If someone else created the table in between, the commit fails and
    nothing is retried.
- **Result:** the row count, as for `INSERT`.

## Consequences

**Positive**
- A failed or cancelled CTAS leaves no table. Readers never see a
  half-built one.
- The created table is an ordinary v2 table: Spark and pyiceberg read
  it, and Spark appends to it.

**Negative (accepted debt)**
- **A failed CTAS leaves its data files under the staged location.** No
  table references them, so orphan removal (ADR 0080) cannot reach them
  either. An operator removes them, or the catalog reclaims its staged
  locations.
- **The query is fully streamed into one commit**, on the coordinator,
  like any write.
- `CREATE OR REPLACE TABLE … AS SELECT` (a replace transaction) is not
  offered.

## Validation

- `ddl::tests::plans_columns_partitions_and_properties_and_refuses_the_rest`:
  a CTAS with `PARTITION BY` keeps the query text and takes its schema
  from the query, and a column list with `AS` is refused.
- `engine::tests::writes_need_a_write_grant_and_other_writes_stay_refused`:
  a CTAS selecting from a table the writer may not read is denied.
- `lakehouse::tests::writes_through_the_rest_catalog` (live in CI, on
  S3):
  - a partitioned CTAS writes 5 rows, and the table has exactly one
    snapshot;
  - a CTAS whose query fails midway (division by zero) leaves no table
    in the catalog.
- By hand, against the reference REST catalog:
  - a filtered CTAS wrote 5 rows, its one snapshot an `append`;
  - an aggregate CTAS partitioned by region pruned to 1 of 2 files;
  - an empty-result CTAS created an empty table;
  - `IF NOT EXISTS` wrote 0 rows, and a duplicate was refused;
  - a CTAS failing at execution, or at planning, left no table (`404`);
  - Spark 3.5 read the three tables and their partitions and appended;
    pyiceberg agreed on spec, property and rows.

## Revisit when

- Failed CTAS files matter: delete what was written when the query or
  the commit fails (best effort), before reporting the error.
- `CREATE OR REPLACE TABLE … AS SELECT` is wanted: a replace transaction
  (`assert-table-uuid`, new schema and spec, `set-snapshot-ref` to a
  snapshot without parent).

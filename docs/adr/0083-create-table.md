# ADR 0083 — `CREATE TABLE` through the catalog

- Status: Accepted
- Date: 2026-11-03
- Owner: JoaoLucas2210
- Related: ADR 0024 (read-only query API), ADR 0075/0076 (writes, partitioned writer), ADR 0025 (grants)
- Scope: `plimsoll_core::ddl::{plan, NewTable, TableCreator, Creator}`, `Engine::create_table`, `iceberg::create_request`, `Catalog::create_table` (REST), `lakehouse::CatalogCreator`

## Context

Plimsoll writes Iceberg tables (`INSERT`, `UPDATE`, `DELETE`, `MERGE`,
ADR 0075–0082), but only into tables that already exist. Creating one
meant calling the catalog's REST API, or another engine, by hand.

The query API refuses DDL (ADR 0024), for good reasons:

- `CREATE EXTERNAL TABLE … LOCATION` reads any server file;
- DataFusion's `CREATE TABLE` makes an in-memory table that every user
  shares;
- `SessionContext::sql` executes DDL **while planning**, before any
  grant is checked.

## Decision drivers

1. **Only the one statement.** Opening `CREATE TABLE` must not reopen
   `CREATE EXTERNAL TABLE`, views, `DROP`, or in-memory tables.
2. **Checked before anything happens**, under the same write grant as
   other writes.
3. **The table lands in the catalog**, readable by Spark, pyiceberg and
   Plimsoll, with partitioning, so writes are pruned from the first
   append.

## Decision

- **Interception (core).**
  - A statement starting with `CREATE` never reaches
    `SessionContext::sql`.
  - It is parsed and planned without executing (`sql_to_statement`,
    then `statement_to_plan`). Anything other than a plain
    `CREATE TABLE` is refused, including `EXTERNAL`, views, `OR
    REPLACE`, `AS SELECT`, constraints and defaults.
  - The table must be named in a schema the engine serves.
  - `check_write` on the new name is the grant. With
    `PLIMSOLL_AUTH_WRITE_GRANTS`, a user who may write `bronze` may
    create tables in it.
  - The creation itself goes to a `TableCreator`, a session config
    extension the server installs with the REST catalog. Without one,
    the statement is refused.
- **Syntax** (the generic SQL dialect):

  ```sql
  CREATE TABLE [IF NOT EXISTS] ns.t (id BIGINT NOT NULL, region VARCHAR, ts TIMESTAMP)
    [PARTITION BY (region, bucket(16, id), day(ts))]
    [OPTIONS ('write.parquet.compression-codec' = 'zstd')]
  ```

  - **Partitioning.** Transforms are a column (identity), `bucket(n,
    c)`, `truncate(w, c)`, and `year`, `month`, `day` or `hour` of `c`.
    Spark's plurals (`days`) are accepted.
  - **Properties** go in `OPTIONS (…)` after `PARTITION BY`, or in
    `WITH (…)` / `TBLPROPERTIES (…)` before it, as the parser orders
    them.
- **The request** (connectors).
  - Arrow types map to Iceberg primitives: `int` (from 8, 16 and
    32-bit), `long`, `float`, `double`, `string`, `binary`,
    `fixed[n]`, `date`, `time`, `timestamp` and `timestamptz` (any
    unit), and `decimal(p, s)`. Nested types are refused.
  - Field ids run 1…n, and `NOT NULL` becomes `required`.
  - Partition fields get Iceberg's own names (`id_bucket`, `ts_day`)
    and ids from 1000.
  - It is `format-version` 2; asking for another version is refused.
  - The location is left to the catalog.
- **Outcomes.**
  - An existing table is an error, or nothing with `IF NOT EXISTS`.
  - The catalog's `400` (say, `day` on a `long`) is the statement's
    fault, so it is an `invalid_query`.
  - The result is empty.

## Consequences

**Positive**
- A pipeline can go from nothing to a partitioned table it then
  appends, updates and merges into, through one API and one grant.
- The rest of DDL stays refused, now by Plimsoll's own parser rather
  than DataFusion's configuration.

**Negative (accepted debt)**
- **No `CREATE TABLE … AS SELECT`**: create, then `INSERT INTO …
  SELECT` (two commits, not one).
- **No `DROP`, `ALTER` or schema/partition evolution** yet. Dropping is
  a destructive grant of its own.
- Sort orders, nested types, column comments and `write.data.path` are
  not expressible.
- A table is visible to `SHOW TABLES` after the next catalog refresh
  (ADR 0033). Queries by name see it immediately.

## Validation

- `ddl::tests::plans_columns_partitions_and_properties_and_refuses_the_rest`:
  - columns with nullability, three transforms and properties are
    planned;
  - CTAS, `OR REPLACE`, an unknown transform, `bucket(0, …)`, views,
    `EXTERNAL`, `PRIMARY KEY` and unknown schemas are refused.
- `iceberg::create::tests::maps_types_partitions_and_properties`.
- `engine::tests::writes_need_a_write_grant_and_other_writes_stay_refused`:
  - `CREATE TABLE` without a write grant is denied, and with one but no
    creator it is refused;
  - with a creator, the creator receives the resolved name and the
    partitioning.
- `engine::tests::query_api_is_read_only` still refuses `CREATE EXTERNAL
  TABLE` and `CREATE VIEW`, and no table appears.
- `lakehouse::tests::writes_through_the_rest_catalog` (live in CI, on S3):
  - the partitioned table is now created with `CREATE TABLE … PARTITION
    BY`;
  - a second create fails, and `IF NOT EXISTS` passes;
  - every write and maintenance step that follows runs on it.
- By hand, against the reference REST catalog:
  - a table partitioned by `region`, `bucket(4, id)` and `days(ts)`, with
    a decimal column and a zstd property, was created; three rows were
    appended and pruned by region (`files=1/3`);
  - Spark 3.5 read the rows, partitions and property, and appended;
  - pyiceberg showed the spec (`identity`, `bucket[4]`, `day`) and the
    schema, and agreed on the rows;
  - a duplicate, an unknown schema, `format-version` 3, `day` of a
    `long`, CTAS and `CREATE VIEW` were all refused.

## Revisit when

- Pipelines need one-commit CTAS: create staged (`stage-create`), write,
  and commit both together.
- Tables must change: `ALTER TABLE … ADD COLUMN` and partition evolution
  through `updateTable` (`add-schema`, `add-spec`), and `DROP TABLE`
  under its own grant.

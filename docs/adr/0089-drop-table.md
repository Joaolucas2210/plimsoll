# ADR 0089 — `DROP TABLE` under its own grant

- Status: Accepted
- Date: 2026-11-03
- Owner: JoaoLucas2210
- Related: ADR 0083 (CREATE TABLE), ADR 0084 (ALTER TABLE), ADR 0075 (write grants), ADR 0025 (grants)
- Scope: `ddl::Planned::Drop`, `TableAccess::check_drop`, `PLIMSOLL_AUTH_DROP_GRANTS`, `TableDdl::drop_table`, `Catalog::drop_table` (REST `DELETE …?purgeRequested=`), `lakehouse::CatalogDdl::drop_table`

## Context

Tables can be created and changed through SQL (ADR 0083–0088), but not
removed. Removing one is the most destructive thing a client can do:
every row and the whole history go with it, and with a purge the files
go too, beyond time travel or a rollback.

## Decision drivers

1. **A separate grant.** Appending to `bronze` must not imply the right
   to delete `bronze`. That mirrors how warehouses split `INSERT` from
   `DROP`.
2. **The catalog decides what a purge deletes**, as for Spark, Trino and
   pyiceberg on a REST catalog. It owns the table's metadata and files.
3. **Only tables**, one at a time, no cascades.

## Decision

- **Syntax:** `DROP TABLE [IF EXISTS] ns.t [PURGE]`.
  - It is refused for several tables, `CASCADE`, `TEMPORARY`, views,
    schemas, and a schema the engine does not serve.
  - Every other DDL is refused as before (ADR 0024, 0083).
- **Grant.**
  - `TableAccess::check_drop` denies by default.
  - `PLIMSOLL_AUTH_DROP_GRANTS=etl=bronze` lists the schemas whose tables
    each principal may drop. A write grant does not include it, and
    `system` is never droppable.
  - With authentication disabled (development only), everything is
    droppable, as everything is writable.
- **Effect.**
  - REST `dropTable`, with `purgeRequested` set only for `PURGE`.
  - Without `PURGE`, the catalog forgets the table and the files stay.
    They can be re-registered, or removed by an operator.
  - With `PURGE`, the catalog deletes them, the reference fixture
    synchronously and other catalogs as they implement it.
  - A missing table is an error, except under `IF EXISTS`.
- **After.** Every query loads the table from the catalog, so the next
  one sees it gone. `SHOW TABLES` catches up at the next refresh (ADR
  0033). Queries already running keep reading the files they planned;
  under `PURGE` they may fail.

## Consequences

**Positive**
- The DDL lifecycle (create, alter, drop) is complete through one API,
  and each step has the grant it deserves.
- Tests and pipelines clean up with SQL instead of the catalog API.

**Negative (accepted debt)**
- **Plimsoll does not purge by itself.** A catalog that ignores
  `purgeRequested` leaves the files behind, and orphan removal (ADR
  0080) cannot help once the table is gone.
- **No undrop.** Catalogs with soft deletes offer that on their side.
- **No `DROP TABLE … CASCADE` and no several tables at once.**

## Validation

- `auth::tests::write_grants_name_the_schemas_a_principal_may_insert_into`:
  - a write grant does not allow a drop;
  - a drop grant allows its schema only;
  - development mode allows any;
  - an unknown principal in the variable fails boot.
- `ddl::tests::plans_columns_partitions_and_properties_and_refuses_the_rest`:
  - `DROP TABLE IF EXISTS … PURGE` plans;
  - several tables, `CASCADE`, views, schemas and unknown schemas are
    refused.
- `engine::tests::writes_need_a_write_grant_and_other_writes_stay_refused`:
  a writer is denied, and a dropper reaches the catalog with `purge`.
- `lakehouse::tests::writes_through_the_rest_catalog` (live in CI, on
  S3): its tables are dropped with `PURGE`; a query then fails, a second
  drop fails, and `IF EXISTS` passes.
- By hand, against the reference REST catalog:
  - `DROP TABLE` left the table's 7 files, and `DROP TABLE … PURGE`
    deleted all 7;
  - with tokens, `etl` (write grant only) created a table and got `403`
    on dropping it, while `ops` (drop grant) dropped it.

## Revisit when

- Catalogs that do not purge are in use: delete the reachable files
  (ADR 0079 reachability) before dropping.
- Soft deletes or undrop are needed: rename into a trash namespace with
  a retention policy.

# ADR 0081 — `UPDATE` Iceberg tables (merge-on-read overwrite)

- Status: Accepted
- Date: 2026-11-03
- Owner: JoaoLucas2210
- Related: ADR 0077 (DELETE, position deletes, `only_own_predicates`), ADR 0075/0076 (partitioned writer), ADR 0078 (compaction), ADR 0024 (governed writes)
- Scope: `IcebergTable::{update, row_change}`, `write::{IcebergDeleteExec::with_update, IcebergSink::delete (update), Change::Overwrite, commit (one manifest per content)}`, `engine::only_own_predicates` (SET), `FederationFilterGuard::update`

## Context

`UPDATE t SET c = expr WHERE p` is the last common row-level write
missing after `INSERT` and `DELETE` (ADR 0075–0077). DataFusion 55 plans
it and hands the provider `update(state, assignments, filters)`:
`(column, expression)` pairs with qualifiers stripped, and the WHERE
conjuncts on the target.

In Iceberg v2, merge-on-read, an update is a delete plus an append: the
old versions of the rows are removed by position, and their new
versions are written as data files. Both go in **one** `overwrite`
snapshot, so readers see either all of the update or none of it.

## Decision drivers

1. **Atomic.** Deletes and new rows are committed together.
2. **The same rows a `DELETE` would choose**, by the same code, with the
   same guarantees: earlier deletes skipped, row filters respected,
   subqueries refused.
3. **Partitions follow the data.** An update that changes a partition
   column moves the row to the right partition's files.

## Decision

- **SQL surface (core).**
  - `UPDATE` joins the allowed writes, gated by `check_write` and
    `PLIMSOLL_AUTH_WRITE_GRANTS`.
  - `only_own_predicates` refuses subqueries and joins in the WHERE, as
    for `DELETE`, and now also in the `SET` values.
  - Row filters reach the provider with the WHERE (ADR 0077), so a
    row-filtered principal updates only rows it can see.
- **Finding rows.** `row_change`, factored out of `delete_from`, is
  shared by both statements:
  - the WHERE prunes the data files;
  - each remaining file is read whole, in file order;
  - positions already deleted are skipped.
- **New values.**
  - Every table column of a matching row is computed: its `SET`
    expression, or the column itself.
  - The expressions are compiled against the table schema, evaluated on
    the matching rows only (`filter_record_batch`), and cast to the
    column's type.
  - The new rows go through the partitioned writer (ADR 0076), so they
    land in their (possibly new) partition with fresh statistics.
- **Commit.**
  - It writes **one manifest per content**: data files (`content=0`)
    and position deletes (`content=1`), each with its manifest list
    entry.
  - The snapshot's operation is `overwrite`, and its summary carries
    `added-records` and `added-position-deletes`.
  - Concurrency is as for `DELETE`: it retries only if every snapshot
    committed in between is an `append`.
- **The result** is the number of rows updated.

## Consequences

**Positive**
- Any `UPDATE` (arithmetic, partition moves, whole-table) is one atomic
  commit that Spark and pyiceberg read, and that Spark can update on
  top of.
- Compaction (ADR 0078) folds the resulting deletes and small files back
  into clean files.

**Negative (accepted debt)**
- **Changed rows are buffered in memory** (reserved in the pool) before
  being written. An update of more rows than the pool holds fails rather
  than spilling; streaming them into the writer is the fix.
- **Coordinator-only**, like other writes. Touched files are read whole.
- **Copy-on-write is not offered**, and neither is v3 (deletion vectors).
- `MERGE INTO` is not done yet; it is the next step on this machinery.

## Validation

- `engine::tests::writes_need_a_write_grant_and_other_writes_stay_refused`:
  - `UPDATE` without a write grant is denied;
  - subqueries in the WHERE or the `SET` are refused;
  - granted, the provider updates (the in-memory table) and returns the
    count.
- `lakehouse::tests::writes_through_the_rest_catalog` (live in CI, on S3):
  - an `UPDATE` moves a row from `eu` to `us`;
  - filters on both partitions see it on the right side;
  - the snapshot is an `overwrite` with one position delete;
  - compaction afterwards still yields the expected rows.
- By hand, against the reference REST catalog, on a table partitioned by
  region:
  - `SET amount = amount + 1` updated 2 rows;
  - a partition move updated 1, and the row then appeared under its new
    region;
  - a whole-table update updated 4;
  - `SET x = (SELECT …)` and `WHERE id IN (SELECT …)` were refused;
  - Spark 3.5 read the same rows and showed `overwrite` snapshots with
    their record and position delete counts;
  - Spark then ran its own `UPDATE`, and pyiceberg agreed with the final
    state.

## Revisit when

- Large updates hit the pool: stream changed rows into the writer as
  each file is read.
- `MERGE INTO`: DataFusion's `WriteOp::MergeInto`, as matched rows
  (update or delete) plus unmatched source rows (insert), on the same
  delete-plus-append commit.

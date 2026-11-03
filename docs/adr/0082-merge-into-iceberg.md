# ADR 0082 — `MERGE INTO` Iceberg tables

- Status: Accepted
- Date: 2026-11-03
- Owner: JoaoLucas2210
- Related: ADR 0081 (UPDATE), ADR 0077 (DELETE, `row_change`), ADR 0076 (partitioned writer), ADR 0024/0025/0044/0045 (governed reads and writes)
- Scope: `IcebergTable::{merge_into, rows_of}`, `iceberg::merge::{Merge, Clause, Action, IcebergMergeExec}`, `engine::plan_stream` (`WriteOp::MergeInto`), `FederationFilterGuard::merge_into`

## Context

`MERGE INTO t USING s ON … WHEN [NOT] MATCHED [BY SOURCE] [AND …] THEN
…` is how pipelines upsert and sync a table from a batch or a change
feed. With `UPDATE` and `DELETE` done (ADR 0077, 0081), it is the last
common row-level write.

DataFusion 55 parses and plans it, and hands the provider
`merge_into(state, source, merge_schema, on, clauses)`:

- the **source** is a physical plan;
- the target is **not scanned in the logical plan**, so the engine's
  per-scan checks (read grants, denied columns, row filters) never see
  it;
- the `ON` and the clauses are logical expressions over the target's
  columns followed by the source's.

## Decision drivers

1. **SQL semantics.** The first clause whose condition holds decides a
   row. A target row matched by two source rows is an error.
2. **One atomic commit**, on the `UPDATE` machinery.
3. **Governance holds even though the target is read outside a scan.**

## Decision

- **Governance (core).**
  - `MERGE` joins the allowed writes, gated by `check_write` on the
    target.
  - The source is a normal query: its scans get read grants, column
    denials and row filters as usual.
  - Since `MERGE` reads every target row and column, the target also
    needs a **read** grant and every one of its columns readable.
  - A principal with a **row filter** on the target is **refused**: it
    would otherwise match, update or delete rows it cannot see.
- **The join.**
  - `ON` is split into conjuncts. Each equality between a target-only
    and a source-only expression becomes a hash key. The rest is a
    residual evaluated on the paired row.
  - Without at least one such equality, the statement is refused.
    Non-equi merges would need a nested loop over every target row.
  - The source is collected once, its memory reserved in the pool, and
    hashed on its keys (Arrow row format). NULL keys never match.
  - Each target data file is read whole, in file order, through
    `rows_of`, the same path `DELETE` and `UPDATE` use. Positions
    already deleted are skipped. Each live row is probed against the
    hash.
- **Clauses.**
  - Each clause is compiled to physical expressions over the merged row.
    In that row every field is nullable, because one side is NULL for
    unmatched rows.
  - `UPDATE` yields the whole target row: the `SET` value, or the column
    itself.
  - `INSERT` yields the whole row: the given value, or NULL for a column
    it omits.
  - The clause kinds are checked: `MATCHED` and `NOT MATCHED BY SOURCE`
    take `UPDATE` or `DELETE`, and `NOT MATCHED` takes `INSERT`.
- **The result.**
  - Deleted and updated target rows become position deletes.
  - Updated and inserted rows are cast to the table's types and written
    by the partitioned writer.
  - Both are committed as one `overwrite` snapshot, exactly as for
    `UPDATE`, with the same concurrency rule.
  - The count is the number of rows inserted, updated or deleted.

## Consequences

**Positive**
- Upserts, deletes driven by a feed, and full syncs (`NOT MATCHED BY
  SOURCE THEN DELETE`) are each one atomic commit that Spark and
  pyiceberg read, and that Spark can merge on top of.
- Rows can move partition through a merge.

**Negative (accepted debt)**
- **The source and the changed rows are held in memory** (reserved). A
  source larger than the pool fails rather than spilling.
- **Every target file is read** (no pruning from the source's key
  range), coordinator-only.
- **Equi-joins only**; row-filtered principals cannot merge at all.
- Subqueries in clause conditions or values are not supported.

## Validation

- `engine::tests::writes_need_a_write_grant_and_other_writes_stay_refused`
  denies `MERGE` in each of these cases:
  - without a write grant on the target;
  - with a source the principal may not read;
  - with a target it may write but not read;
  - under a row filter on the target.

  Granted, the `MERGE` reaches the provider.
- `lakehouse::tests::writes_through_the_rest_catalog` (live in CI, on S3):
  - an insert-only merge, then one that deletes the inserted row and
    updates a row the source does not match (`NOT MATCHED BY SOURCE`);
  - a target row matched twice is refused;
  - compaction and expiration afterwards still yield the expected rows.
- By hand, against the reference REST catalog, on a table partitioned by
  region, with a second table as the source:
  - one merge deleted, updated, moved partition and inserted, 4 rows in
    all;
  - `NOT MATCHED BY SOURCE THEN UPDATE` and conditional clauses each did
    what they should;
  - a duplicate match came back as a `400`, and so did a non-equi `ON`;
  - Spark 3.5 read the same rows, and showed `overwrite` snapshots with
    their record and position delete counts;
  - Spark ran its own `MERGE`, pyiceberg agreed, and a Plimsoll merge on
    top of Spark's files was correct.

## Revisit when

- Sources outgrow the pool: partition source and target by key hash and
  spill, as the shuffle does (ADR 0068).
- Merges touch a small key range of a large table: prune target files
  by the source keys' min/max, or a semi-join on manifests.
- Row-filtered writers need `MERGE`: apply the filter to the target side
  and refuse inserts outside it.

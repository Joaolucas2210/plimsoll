# ADR 0077 — `DELETE FROM` Iceberg tables (position deletes)

- Status: Accepted
- Date: 2026-11-03
- Owner: JoaoLucas2210
- Related: ADR 0075/0076 (appends, manifests, commit), ADR 0014 (reading position deletes), ADR 0017 (deletion vectors), ADR 0024 (read-only API), ADR 0045 (row filters)
- Scope: `IcebergTable::delete_from`, `iceberg::write::{Target, IcebergDeleteExec, IcebergSink::delete, position_deletes, Change, operations_since, alive, kept_ranges}`, `engine::only_own_predicates`, `FederationFilterGuard::delete_from`

## Context

After appends (ADR 0075/0076), the next most needed write is removing
rows, for corrections, GDPR erasure and retention. Iceberg v2 offers
two ways:

- **Copy-on-write:** rewrite every data file that has a matching row,
  without those rows.
- **Merge-on-read:** keep the data files, and add *position delete
  files* listing `(file_path, pos)` of the deleted rows.

Plimsoll's reader already applies position deletes and v3 deletion
vectors (ADR 0014, 0017), validated against Spark.

DataFusion 55 plans `DELETE FROM t WHERE …` and hands the provider
`delete_from(state, filters)`: the WHERE conjuncts on the target table.
It **drops any predicate that is not on the target**. After
decorrelation, a `WHERE id IN (SELECT …)` becomes a join whose predicate
never reaches the provider, so the provider would see no filter and
delete every row.

## Decision drivers

1. **Never delete more than asked.** The predicate the provider applies
   must be the whole predicate.
2. **Cheap deletes.** Removing a handful of rows from a large table
   should not rewrite gigabytes.
3. **Readable by every engine**, and appends must stay possible
   concurrently.
4. **The read rules apply.** A principal can only delete rows it can
   read.

## Considered options

**A. Merge-on-read: position delete files (chosen).** Each touched data
file costs one small Parquet file of positions, and nothing is
rewritten. Readers pay for applying deletes until compaction, which is
future table maintenance.

**B. Copy-on-write.** Readers pay nothing, but the delete pays for every
touched file in full, and conflicts with concurrent appends are harder
(files are removed). Better done later as compaction.

**C. v3 deletion vectors.** These are the modern format. They need
Puffin writing and are v3-only; Plimsoll writes v2 (ADR 0075).

## Decision

- **SQL surface (core).**
  - `DELETE FROM` joins `INSERT INTO` as an allowed write. It is gated by
    the same `check_write` and the same `PLIMSOLL_AUTH_WRITE_GRANTS`.
  - `only_own_predicates` refuses, before anything runs, a `DELETE`
    whose input contains anything other than a scan of the target,
    filters, projections and aliases, or whose predicate contains a
    subquery: "DELETE supports WHERE predicates on the table's own
    columns only".
  - Row filters (ADR 0045) are `Filter`s over the target scan, so
    DataFusion passes them to the provider with the WHERE. **A
    row-filtered principal deletes only rows it can see.** Read grants
    and column denies apply to the scan as for any query.
- **Finding the rows (provider).**
  - The conjuncts, unqualified, prune data files exactly like a scan
    (manifests, partition values and column bounds, ADR 0050–0055).
  - Each remaining file is planned **alone, with no row selection and no
    pushed predicate**, so it streams every row in file order and a
    running count is each row's position.
  - The predicate is evaluated per batch, and NULL counts as false, as
    in SQL.
  - Positions already removed by earlier position deletes or deletion
    vectors are skipped (`kept_ranges` and `alive`), so nothing is
    deleted or counted twice.
- **Writing.**
  - There is one position delete file per touched data file, written as
    Parquet `(file_path, pos)` sorted, with Iceberg's reserved field ids
    (2147483546 and 2147483545).
  - It carries the data file's partition tuple, `referenced_data_file`,
    and value/null counts and bounds for both columns. Java readers use
    these to match it to its data file only.
  - A delete manifest (`content=1`, `"content": "deletes"`) lists the
    files, and the manifest list entry has `content=1`. The snapshot's
    operation is `delete`, with `added-position-deletes` in its summary.
- **Concurrency.**
  - The commit asserts the parent, as appends do.
  - On a conflict it may retry only if every snapshot committed in
    between is an `append`: newer rows are untouched by older deletes.
  - Any other concurrent operation fails the `DELETE` ("run it again").
    An overwrite, a delete or a rewrite can move rows, and the planned
    positions would then point at the wrong place.
- **Not covered yet.**
  - Rows removed only by **equality deletes** may receive position
    deletes again. That is harmless, but the returned count may include
    them.
  - **Files written under an older partition spec** are refused.
  - **v3 tables** are refused, because they need deletion vectors.

## Consequences

**Positive**
- `DELETE FROM t WHERE …` removes rows atomically, at a cost
  proportional to the touched files' reads plus tiny delete files.
- Spark and pyiceberg apply Plimsoll's deletes. Spark can then delete
  (copy-on-write) on top, and all engines agree.
- The silent "delete everything" hazard of DataFusion's DML planning is
  closed for every provider, not just Iceberg.

**Negative (accepted debt)**
- **Readers apply the deletes until compaction rewrites the files.**
  Compaction is table maintenance, not built yet.
- **The delete runs on the coordinator** and reads touched files whole,
  though only the columns it needs would do.
- **Matching positions are held in memory per file** before being
  written: proportional to deleted rows, not accounted.
- No `UPDATE` or `MERGE` yet.

## Validation

- `write::tests::earlier_deletes_are_not_deleted_again`: a row selection
  with two kept ranges yields survivors 0, 1, 5 and 6.
- `engine::tests::writes_need_a_write_grant_and_other_writes_stay_refused`:
  - a `DELETE` without a write grant is denied;
  - `IN (subquery)`, `EXISTS` and a scalar subquery are refused as "own
    columns only";
  - granted and well-formed, the provider deletes, and the remaining
    rows are checked;
  - `COPY`, `UPDATE` and `INSERT OVERWRITE` stay refused.
- `lakehouse::tests::writes_through_the_rest_catalog` (live in CI, on
  S3):
  - a `DELETE` across partitions removes 2 rows, the same `DELETE` again
    removes 0, and an append after it is visible;
  - an `IS NULL` delete works on the unpartitioned table;
  - the snapshot operations read `append, append, delete, append`.
- By hand, against the reference REST catalog:
  - Plimsoll deleted from its partitioned table and from pyiceberg's
    `bronze.trips`;
  - Spark 3.5 read the same rows, listed the position delete files, and
    showed `delete` snapshots with the counts;
  - Spark then deleted a row itself, and pyiceberg agreed with the
    result.

## Revisit when

- Readers slow down on accumulated deletes: compaction (rewrite data
  files and drop their deletes) and expiring snapshots.
- v3 tables need writes: deletion vectors (Puffin, one per data file),
  which the reader already decodes.
- `UPDATE` and `MERGE`: a delete plus an append in one snapshot (an
  `overwrite`), on this same machinery.

# ADR 0096 — Row lineage in rewrites

- Status: Accepted
- Date: 2026-11-03
- Owner: JoaoLucas2210
- Related: ADR 0095 (v3 writes), ADR 0081 (UPDATE), ADR 0082/0094 (MERGE), ADR 0078 (compaction)
- Scope: `DataFile::first_row_id` (inheritance on read), `write::{Target::{first_row_id, seq}, Lineage, ROW_ID, LAST_UPDATED, lineage_columns, IcebergSink::{lineage, lineage_schema, with_lineage}, write_rows (lineage columns), rewrite (UPDATE), rewrite_manifest (first row ids written out)}`, `merge::{apply, write_new_rows}`, `IcebergTable::compact` (v3)

## Context

ADR 0095 refused `UPDATE`, merge-update and compaction on v3 tables.
Rewriting a row into a new file must keep its lineage:

- **`_row_id`.** Its identity across commits.
- **`_last_updated_sequence_number`.** When it last changed.

Without them, an updated or compacted row would look deleted and
re-inserted to anything that tracks changes by row id.

Iceberg lets writers *materialize* both as columns of the new data
file (field ids 2147483540 and 2147483539). Where a column is null, or
absent, readers inherit:

- `_row_id` = the file's `first_row_id` + the row's position;
- `_last_updated_sequence_number` = the file's data sequence number.

## Decision drivers

1. **Same semantics as Iceberg's own engines.**
   - An update keeps the row's id and takes the new commit's sequence
     number.
   - A compaction keeps both.
   - Inserts get fresh ids.
2. **Read lineage wherever it is**: inherited for files Plimsoll wrote,
   or materialized by an earlier rewrite (Spark's copy-on-write deletes
   produce such files).

## Decision

- **Knowing each row's lineage.**
  - Planning computes every data file's `first_row_id`: the file's own
    value, else inherited from its manifest's `first_row_id`, adding the
    record counts of earlier inheriting entries in entry order, every
    entry counted.
  - Rewrites carry it on their targets with the file's data sequence
    number.
  - On v3, `IcebergSink::lineage` reads the file's materialized lineage
    columns when it has them (footer, then those two columns only).
  - `Lineage::row_id` / `seq` take the written value where it is
    non-null, and inherit otherwise. A v2-era file without ids yields
    NULL, and the rewritten row then gets a fresh id.
- **Writing it.** Rewritten rows carry two extra columns after the
  table's. `write_rows` writes batches of that shape with the lineage
  schema (the reserved field ids). Plain rows are written as before, in
  separate files.
- **`UPDATE`.** Updated rows get their ids and a NULL sequence number,
  which inherits the commit's.
- **`MERGE`.** `WHEN MATCHED` and `NOT MATCHED BY SOURCE` updates get the
  target rows' ids, also with a NULL sequence number. Inserts stay
  plain. This holds both locally and on workers (ADR 0094), since both
  share `probe` / `apply` / `write_new_rows`.
- **Compaction on v3.** Each chosen file's live rows are read by
  position (deletes applied by selection) with their ids and sequence
  numbers as they were, and rewritten with both columns. Files with
  equality deletes are left out on v3: those deletes do not apply by
  position.
- **Rewritten manifests stay consistent.** When a commit rewrites a v3
  manifest (marking entries `DELETED`), every data entry that inherited
  its first row id gets it written out. Dropping or marking entries
  then never shifts the inherited ids of the rest.
- The refusals of ADR 0095 are gone.

## Consequences

**Positive**
- Every write Plimsoll offers works on v3 tables, and row ids survive
  updates, merges and compactions, by any engine that reads lineage.

**Negative (accepted debt)**
- **Compaction on v3 collects each partition's rows in memory**
  (reserved) before writing.
- **v3 files with equality deletes are not compacted.**
- **Each rewrite reads every target file's footer** to look for
  lineage columns.
- **Spark 3.5 with Iceberg 1.9.2 misreports lineage in its vectorized
  reader.** It ignores materialized `_row_id` and positions within files.
  Its row-based reader reports the values exactly. That is a reader-side
  issue; the files follow the spec.

## Validation

- `write::tests::lineage_prefers_written_columns`: written values win,
  NULL falls back to inheritance, and a file without ids has none.
- `write::tests::rewritten_v3_manifests_keep_row_ids`: removing the
  first of three inheriting files writes out 100, 103 and 107. A second
  rewrite, which drops the `DELETED` entry, keeps 107.
- `lakehouse::tests::writes_through_the_rest_catalog` (live in CI, on
  S3), on a v3 table: `UPDATE`, a merge-update and compaction run, and
  the rows are right.
- By hand, against the reference REST catalog, on a v3 table
  partitioned by region (rows 1–12 over two appends):
  - an `UPDATE` moving id 4 to another partition, a merge-update of id
    5 with an insert, and a delete ran;
  - Spark 3.5 (row-based reader) showed every row's `_row_id` equal to
    its original (id 4 kept 1, id 5 kept 7), with sequence numbers 3 and
    4 for the updated rows and 1 or 2 for the others; the insert got
    13;
  - compaction then rewrote 7 files into 2, with no deletes left, and
    Spark showed the same ids and sequence numbers.

## Revisit when

- Large v3 compactions: stream each file's rows into the writer instead
  of collecting.
- v3 tables with equality deletes need compaction: resolve them per
  file to positions first.

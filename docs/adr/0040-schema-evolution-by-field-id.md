# ADR 0040 — Schema evolution by field id

- Status: Accepted
- Date: 2026-11-02
- Owner: JoaoLucas2210
- Related: ADR 0005 (correctness first), ADR 0007 (read path; replaces its name-mapping rejection), ADR 0016 (equality deletes)
- Scope: `plimsoll_connectors::iceberg::{field_ids, renamed_columns, IcebergTable::{names, renamed}, parquet_scan}`, `deletes::equality_keys`

## Context

Iceberg identifies columns by **field id**, not by name:

- `ALTER TABLE t RENAME COLUMN town TO city` changes only metadata, so
  files written earlier still say `town`;
- dropping `town` and adding a new `town` creates a *different* column,
  with a new id; old files' `town` is not it;
- `score INT → BIGINT` (type promotion) changes the table type while old
  files keep `INT32`.

DataFusion's Parquet scan resolves columns by name. ADR 0007 made that
safe by **refusing** any table whose schema history renamed or re-added a
column. That is correct, but renames are routine schema maintenance in
every lakehouse, and each one made a table unreadable by Plimsoll until it
was rewritten.

Iceberg writers (Spark, Flink, the Java and Python libraries) store each
column's id as a Parquet `field_id`. The arrow-rs reader surfaces it as
`PARQUET:field_id` field metadata. The information needed is in every
file.

## Decision drivers

1. **Exact Iceberg semantics:**
   - a renamed column reads its old data;
   - a re-added column is NULL in files that predate it;
   - promoted types are cast.
2. **Projections and filters both.** A filter on a renamed column must
   prune and evaluate correctly.
3. **Never a silently wrong column.** A file that cannot be resolved by id
   in a table where names were reused is a typed error (ADR 0005).
4. **Everything else unchanged:** tables without renames, files without
   ids in such tables, pruning by field id (ADR 0010) and deletes.

## Considered options

**A. A `PhysicalExprAdapterFactory` on the Parquet scan that rewrites
column references by field id (chosen).** DataFusion 55 adapts each
file's projection and predicate through this hook. For a file with ids:

- each table column reference becomes a reference to the file column with
  the same id, by *file* name and index, which is how the Parquet reader
  resolves projections;
- it is wrapped in a cast when the type was promoted;
- it becomes a typed NULL literal when no file column has that id.

**B. Rename the Parquet schema per file before reading.** That means
rewriting the file schema handed to the reader, which needs a custom
`ParquetFileReaderFactory` and duplicates the metadata path. Rejected:
the adapter is the supported extension point.

**C. Keep refusing renamed tables.** It is correct, but Plimsoll would
then be unusable on any table with ordinary maintenance. Rejected.

## Decision

- **`field_ids::FieldIds`** (the scan's expression adapter). For each
  file:
  - **With field ids:** a `ById` adapter maps table column, id, file
    index. A promoted type gets `cast(…)`; a missing id gets
    `NULL::<table type>`.
  - **Without any field id:** DataFusion's default by-name adapter is
    used, unless the table's history renamed or re-added a column. Then
    the result is `Unsupported("data file without Parquet field ids …
    (name mapping)")`, because by-name resolution could read the wrong
    column. Iceberg's `schema.name-mapping.default` is the revisit item.
- **Forcing adaptation.** DataFusion skips the adapter when the table and
  file schemas are equal. For renamed tables, the table's Arrow schema
  carries each field's id as `PARQUET:field_id` metadata
  (`with_field_ids`). Any file whose names or ids differ then never
  compares equal and is always adapted. A file that does compare equal
  has the same names *and* ids, so by-name resolution is right.
- **Equality deletes** (ADR 0016) resolve their key columns in the delete
  file by field id when it carries ids. A delete written before a rename
  uses the old name.
- **`renamed_columns`** replaces ADR 0007's `check_name_mapping`. It
  returns whether the history reused or changed names, instead of
  failing.
- **Unaffected:** partition and column-bound pruning already used field
  ids (ADR 0010), and position deletes and deletion vectors are
  name-free.

## Consequences

**Positive**
- Tables with renames, promotions and re-added columns read exactly as
  Spark reads them, filters included.
- No cost for tables that never renamed: the table schema carries no
  metadata, schemas usually compare equal, and the adapter rarely runs.

**Negative (accepted)**
- **Files without field ids in renamed tables are refused** unless the
  table has a name mapping, which ADR 0042 adds.
- **Field-id metadata reaches the results of renamed tables.** Their
  query output schemas carry `PARQUET:field_id` metadata on table
  columns. JSON responses do not show it; Arrow IPC results (ADR 0037)
  carry it as field metadata, which is harmless.
- **Nested children resolve by name.** ADR 0041 adds nested types but
  refuses histories that renamed a nested child.

## Risks and mitigations

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| A writer emits wrong field ids | Very low | High | Ids are the Iceberg contract; the Spark fixture validates against the reference engine's own reading. |
| DataFusion changes the adapter contract (projection by name) | Low | High | The interop test exercises projection and filter paths on a real evolved table on every relevant change. |
| Type promotion beyond the spec (e.g. string → int) | Very low | Medium | Only spec promotions exist in metadata; an impossible cast fails the query, not silently. |

## Validation

- `tests/iceberg.rs::columns_resolve_by_field_id_across_renames`, on a
  hand-built file written as (id 1, town 2), with the table now
  (id 1, city 2, town 3):
  - `city` reads the old `town` data;
  - the new `town` is NULL;
  - `WHERE city <> 'porto'` filters correctly.
- `tests/iceberg.rs::renamed_tables_without_field_ids_are_refused`: a
  renamed table whose files lack ids fails with the typed error.
- `tests/iceberg.rs::reads_an_evolved_spark_table` (interop workflow).
  - **Setup:** Spark wrote 3 rows and an equality delete keyed on `town`
    (from Iceberg's Java writer). Then it renamed `town` to `city`,
    promoted `score` to `BIGINT`, added a new `town`, and inserted
    `(3, braga, 3000000000, new)`.
  - **Spark's answer** (asserted in the fixture) is
    `(1,lisbon,10,NULL), (3,braga,3000000000,new), (4,faro,40,NULL)`.
  - **Plimsoll's answer** is exactly that, with:
    - `city = 'faro'` giving [4];
    - `score > 2147483647` giving [3];
    - `town IS NULL` giving [1, 4];
    - the delete applied, and the pool returning to 0.
  - **Sanity check:** the test fails when pointed at a missing table, so
    it really ran.
- The full Iceberg suite, including MOR, equality deletes and v3 DVs
  against Spark's tables: 22 of 22.

## Revisit when

- Imported files without ids must be read: implement
  `schema.name-mapping.default`.
- Nested types land: resolve child fields by id.

# ADR 0087 — Writes to nested columns

- Status: Accepted
- Date: 2026-11-03
- Owner: JoaoLucas2210
- Related: ADR 0041 (nested types, read path), ADR 0040 (field ids), ADR 0075–0082 (writes), ADR 0083/0084 (CREATE / ALTER TABLE)
- Scope: `iceberg::arrow_write_schema`, `write::Layout` (nested columns, `arrow`), `IcebergSink::write_rows` (relabelling), manifest statistics, `create::field_type`, `alter_updates` (nested `ADD COLUMN`), `field_ids::ById` (strict type check)

## Context

Plimsoll reads struct, list and map columns (ADR 0041), but every write
refused a table that had one: the partitioned writer only knew how to
label top-level columns with field ids. Iceberg readers resolve
**every** field by id, nested ones included (struct fields, list
elements, map keys and values). A nested column written without ids
reads back as NULL in Spark, or not at all.

## Decision drivers

1. **Ids at every level**, exactly as the table's schema assigns them,
   so Spark, pyiceberg and Plimsoll read nested values by id.
2. **No wrong statistics.** Iceberg keys column statistics by *leaf*
   field id. A count recorded under a struct's own id is meaningless to
   other readers.
3. **Every write path at once**: `INSERT`, `UPDATE`, `DELETE`, `MERGE`
   and compaction share the writer.

## Decision

- **Write schema.**
  - `arrow_write_schema` converts the current Iceberg schema to Arrow
    with `PARQUET:field_id` on every field: struct fields by their id,
    list elements by `element-id`, map keys and values by `key-id` and
    `value-id`.
  - The sink writes files with this schema. Incoming batches have the
    table's types without ids, so nested columns are cast (relabelled)
    to it, and top-level columns are relabelled as before.
- **Layout.** Nested columns are no longer refused. They carry no
  statistics: no value or null count, and no bounds, under their own
  id. Leaf statistics for nested fields are left for later; readers
  simply do not prune on them.
- **Reading our own files back.** The id-resolving adapter compared
  file and table types with `equals_datatype`, which ignores field
  metadata. A nested column carrying ids then went unconverted and
  failed when the batch was assembled. It now casts whenever the types
  differ at all, which is DataFusion's struct-aware cast, the same one
  promotions use.
- **DDL.**
  - `CREATE TABLE` accepts `STRUCT<…>` and `ARRAY<…>` / `T[]`, and
    `ALTER TABLE … ADD COLUMN` takes nested types.
  - Ids are assigned as Iceberg does: top-level columns 1…n first, then
    each struct's fields, list elements, and map keys and values, in
    order. New columns go after `last-column-id`.
  - DataFusion's DDL has no `MAP` type. Map columns are written into
    tables created by another engine.

## Consequences

**Positive**
- Every table Plimsoll reads, it can now write, as long as its
  partition transforms are supported. That includes tables with
  structs, lists and maps created by Spark or pyiceberg.
- Spark reads Plimsoll's nested files by id, and kept doing so after it
  renamed a nested field.

**Negative (accepted debt)**
- **No nested statistics**, so filters on `s.a` do not prune Plimsoll's
  files. Spark's files still prune (ADR 0048).
- **Nested field evolution is still refused on read** (ADR 0041). After
  another engine renames or re-adds a nested field, Plimsoll refuses
  the table, for reads and writes alike, until nested fields resolve by
  id in the scan.
- Every nested column of every batch is cast once on write, and once on
  read of Plimsoll's own files. That is a copy, not a recomputation.

## Validation

- `write::tests::layouts_accept_the_transforms_the_pruner_knows`: a
  nested column has no statistics id, and its children carry their
  field ids.
- `create::tests::maps_types_partitions_and_properties`:
  - a struct holding a list, and a map, get ids 4–8 after the three
    top-level columns;
  - an unsigned type is refused.
- `lakehouse::tests::writes_through_the_rest_catalog` (live in CI, on
  S3): a `STRUCT<a INT, tags ARRAY<VARCHAR>>` table is created, has rows
  inserted, an update of a NULL struct and a `MERGE` delete, and reads
  back by field id.
- By hand, against the reference REST catalog:
  - a partitioned table with `STRUCT<a INT, tags ARRAY<VARCHAR>>` took
    `INSERT`, field access and a filter on `s['a']`, `UPDATE`, `DELETE`,
    `MERGE` and `ADD COLUMN extra STRUCT<b DOUBLE>`;
  - a pyiceberg-created table with `map<string, long>` and
    `list<struct<x int>>` took inserts, a map lookup and an update;
  - Spark 3.5 read both tables with the same values, renamed `s.a` to
    `alpha`, inserted and deleted, and read Plimsoll's files by id
    throughout;
  - pyiceberg read the same values. It showed a NULL list as `[]`, but
    the Parquet file holds NULL, and Spark reads NULL.

## Revisit when

- Filters on nested fields matter on Plimsoll-written tables: record
  leaf statistics, by leaf field id, for primitives reached through
  structs.
- Nested evolution: resolve nested fields by id in the scan, so renames
  and re-adds made by other engines read, instead of refusing.

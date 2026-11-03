# ADR 0088 — Nested field evolution

- Status: Accepted
- Date: 2026-11-03
- Owner: JoaoLucas2210
- Related: ADR 0041 (nested types, read path), ADR 0040 (field ids), ADR 0087 (nested writes), ADR 0084 (ALTER TABLE)
- Scope: `field_ids::{NestedById, by_id}`, `IcebergTable::nested_ids`, the nested refusal in `from_metadata` (removed), `ddl::{nested_change, Alteration::{AddNestedField, RenameNestedField, DropNestedField}}`, `alter_updates` (struct paths)

## Context

Top-level columns resolve by field id (ADR 0040), but nested fields
inside a struct resolved by **name** within their parent. ADR 0041
therefore refused, for reads and writes alike, any table whose history
renamed, moved or re-added a nested field: by name, it would read the
wrong child or miss the right one.

That became a real limit once Plimsoll wrote nested columns (ADR 0087).
After Spark ran `RENAME COLUMN s.a TO alpha` on a table Plimsoll had
written, Plimsoll refused the table. There was also no way for Plimsoll
itself to evolve a nested field.

## Decision drivers

1. **Iceberg semantics**: every field resolves by id. A rename keeps
   the data, and a drop followed by an add with the same name reads NULL
   from older files.
2. **Shared by every read path**: scans, `UPDATE` and `MERGE` reads,
   compaction.
3. **Spark's syntax** for nested `ALTER`, since that is what users
   already type.

## Decision

- **Read by id.**
  - When a nested column's file type differs from the table's at all,
    the id adapter wraps it in `NestedById`. A struct, list or map is
    rebuilt as the table type: struct children are matched by field id
    (by name for files without ids), list elements and map entries are
    converted recursively, and primitives are cast for promotions.
  - Children missing from the file read as NULL. Children the table no
    longer has are dropped.
  - The table side comes from the current schema with every nested
    field's id (`arrow_write_schema`, ADR 0087).
  - The refusal of renamed, moved or re-added nested fields is gone.
    Nested `initial-default` values are still refused.
- **Nested `ALTER TABLE`**, in Spark's syntax. A small parser handles
  dotted column paths, which the SQL parser rejects; top-level columns
  still take the regular path (ADR 0084).

  ```sql
  ALTER TABLE t ADD COLUMN s.b DOUBLE
  ALTER TABLE t ADD COLUMN l.element.z VARCHAR      -- in a list's struct element
  ALTER TABLE t RENAME COLUMN s.a TO alpha
  ALTER TABLE t DROP COLUMN s.tags
  ```

  - Paths walk struct field names and the `element` of a list, or the
    `key` / `value` of a map.
  - Added fields are nullable and get ids after `last-column-id`, nested
    types included. A rename keeps the id.
  - A drop is refused for partition sources, identifier fields, and a
    struct's last field. Duplicates and unknown paths are refused.
  - Governance: a renamed or dropped nested field checks
    `check_columns` on its top-level column (ADR 0084).

## Consequences

**Positive**
- Tables whose nested fields evolved in Spark or pyiceberg now read and
  write in Plimsoll. Plimsoll's own nested changes read the same way in
  Spark and pyiceberg.
- Re-added fields read NULL from older files, as in Iceberg, instead of
  picking up old values by name.

**Negative (accepted debt)**
- **Each batch of a differing file is rebuilt**, which costs a copy of
  the nested column's structure (the values are reused). Files written
  under the current schema still pay it, because the ids are field
  metadata the table type lacks (ADR 0087).
- **Nested type promotion through `ALTER COLUMN s.a SET DATA TYPE`** and
  `DROP NOT NULL` on nested fields are not offered yet.
- Map keys are matched by position, not id; the spec does not allow a
  key to be renamed anyway.
- Nested leaf statistics are still not written (ADR 0087).

## Validation

- `field_ids::tests::nested_children_resolve_by_id`: a list of structs
  whose element was renamed and moved, promoted from `int` to `long`,
  and given a new child reads values by id, with NULLs for the new
  child.
- `tests/iceberg.rs::nested_types_map_and_nested_renames_are_accepted`:
  a history with a renamed nested field loads.
- `ddl::tests::plans_nested_field_changes`:
  - a dotted path is normalized and quoting is kept;
  - a list element path parses;
  - a top-level column still takes the regular parser;
  - trailing tokens are refused.
- `alter::tests::nested_fields_are_added_renamed_and_dropped_by_path`:
  - an add in a struct, a rename keeping the id, and a drop inside a
    list element all apply;
  - a non-struct parent, a clash, an unknown path and a path through a
    primitive are refused.
- `lakehouse::tests::writes_through_the_rest_catalog` (live in CI, on
  S3): rename, drop and re-add, and add on a struct column; an older row
  reads its renamed field and NULL for the re-added and the new ones.
- By hand, against the reference REST catalog:
  - the table Spark had renamed `s.a` to `alpha` on (refused before)
    read, filtered on `s['alpha']` and took inserts;
  - Plimsoll then added `s.b`, renamed `alpha` to `a2`, dropped and
    re-added `s.tags` as a string, and added `l.element.z` on a
    pyiceberg table;
  - old rows read `a2` and NULL for `tags`, `b` and `z`, and updates
    wrote the new shape;
  - Spark 3.5 read the same rows, added `s.c` and inserted; Plimsoll and
    pyiceberg agreed on every row.

## Revisit when

- Nested promotions or `DROP NOT NULL` are needed: extend the small
  parser to `ALTER COLUMN a.b …`.
- Rebuilding batches shows in profiles: skip `NestedById` when the
  file's ids and names match the table's at every level.

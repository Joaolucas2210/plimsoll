# ADR 0084 — `ALTER TABLE`: schema evolution and properties

- Status: Accepted
- Date: 2026-11-03
- Owner: JoaoLucas2210
- Related: ADR 0083 (CREATE TABLE), ADR 0040 (schema evolution by field id), ADR 0044 (column denials), ADR 0038 (result cache), ADR 0075 (commits)
- Scope: `ddl::{Alteration, Planned, TableDdl, Ddl}`, `Engine::ddl`, `iceberg::alter_updates`, `lakehouse::CatalogDdl::alter_table`, the result cache's pinned key

## Context

Tables can now be created through SQL (ADR 0083), but they cannot
change. A pipeline that gains a column still needs another engine.

The read side has handled evolved tables since ADR 0040: columns are
resolved by field id, so a renamed column keeps its data, an added one
reads as NULL from older files, and a promoted one is cast. What is
missing is a way to commit the change.

## Decision drivers

1. **Iceberg's rules, not ours.** Only changes every Iceberg reader
   handles, committed the way Iceberg's own `UpdateSchema` does.
2. **Governance holds.** A schema change must not open a way around a
   column denial.
3. **No stale answers.** Results cached before a change must not be
   served after it.

## Decision

- **Syntax** (generic dialect), several changes per statement, applied
  in order and committed together:

  ```sql
  ALTER TABLE [IF EXISTS] ns.t
    ADD COLUMN [IF NOT EXISTS] c type,
    DROP COLUMN [IF EXISTS] c,
    RENAME COLUMN a TO b,
    ALTER COLUMN c DROP NOT NULL,
    ALTER COLUMN c SET DATA TYPE type,
    SET TBLPROPERTIES ('k' = 'v')
  ```

  Everything else is refused: `RENAME TO`, `SET NOT NULL`, defaults,
  column positions, and `USING`.
- **Rules** (`alter_updates`, applied to the current metadata):
  - **Added columns** are nullable (Iceberg v2 has no default to fill a
    required one) and get fresh ids above `last-column-id`.
  - **Dropped columns** are refused if they source the default
    partition spec, are identifier fields, or are the last column.
  - **Renames** keep the field id, and a name already in use is refused.
  - **Types** change only by Iceberg's promotions: `int` to `long`,
    `float` to `double`, and decimals to a wider precision at the same
    scale.
  - **`DROP NOT NULL`** makes a column optional; identifier fields stay
    required.
  - **Properties** may be set, except `format-version`.
  - Changes that do nothing (`IF EXISTS`, `IF NOT EXISTS`, a type
    already set) commit nothing.
- **Commit.**
  - It adds one schema with a new schema id, and the action
    `set-current-schema -1`.
  - It requires `assert-current-schema-id` and
    `assert-last-assigned-field-id` as seen, plus the table uuid.
  - On a conflict, the changes are recomputed on fresh metadata, up to
    4 attempts.
  - A catalog `400` is the statement's fault, so it is an
    `invalid_query`.
  - Data files are never rewritten.
- **Governance.**
  - The statement needs the write grant on the table.
  - Every column it renames, drops or retypes must also pass
    `check_columns`. Renaming a denied column would otherwise move it out
    from under its deny rule.
- **Result cache.** A scan's pinned identity now includes the schema id,
  not just the snapshot. `ALTER` changes `SELECT *` without a new
  snapshot, so without this a cached answer would come back with the
  old columns.

## Consequences

**Positive**
- Tables evolve in place, and Spark and pyiceberg see the same schema
  history.
- Every write path keeps working on evolved tables: `INSERT`, `UPDATE`,
  `DELETE` and `MERGE` all resolve old files by field id.
- Pruning stays safe after a promotion. An `int` file's 4-byte bounds
  on a column that is now `long` do not decode, so those files are
  simply not pruned.

**Negative (accepted debt)**
- **No partition evolution.** There is no common SQL syntax for it in
  the parser, and deletes do not yet handle data files written under an
  older spec.
- No changes to nested fields, no column reordering or comments, no
  `RENAME TO` and no `DROP TABLE`.
- Promoted `int` bounds are not read as `long`, so files written before
  a promotion are not pruned on that column.

## Validation

- `ddl::tests::plans_column_and_property_changes_and_refuses_the_rest`:
  - all six changes parse into `Alteration`s, with names normalized;
  - `ADD … NOT NULL`, `RENAME TO`, `SET NOT NULL`, `SET DEFAULT`, an
    unknown schema and `ALTER VIEW` are refused.
- `iceberg::alter::tests::schema_changes_keep_ids_and_add_one_schema`:
  - field ids are kept, a new id is 5 and the schema id is 1;
  - no-op changes give no commit;
  - dropping a partition source, narrowing a type, changing a type's
    kind, renaming onto an existing name, an unknown column and
    `format-version` are refused.
- `engine::tests::writes_need_a_write_grant_and_other_writes_stay_refused`:
  - `ALTER TABLE` without a write grant is denied;
  - renaming a denied column is denied;
  - a granted `ALTER` reaches the catalog with its changes.
- `lakehouse::tests::writes_through_the_rest_catalog` (live in CI, on S3):
  - a column is added and another renamed; a row is inserted and the
    added column promoted;
  - old rows read the added column as NULL and keep their data under
    the new name;
  - narrowing is refused.
- By hand, against the reference REST catalog, on the partitioned table
  of ADR 0083:
  - add and rename, insert, then promote `int` to `bigint` and
    `decimal(10,2)` to `(12,2)`; a filter and sums on the promoted
    columns are correct;
  - `UPDATE` and `DELETE` work on the evolved table; a drop and a
    property set go through;
  - dropping the partition column, `NOT NULL` adds, narrowing and
    `RENAME TO` are refused; `IF EXISTS` and `IF NOT EXISTS` are no-ops;
  - Spark 3.5 read the evolved table (types, partitioning), added and
    renamed columns of its own, and inserted;
  - Plimsoll then read Spark's changes and filtered on them, and
    pyiceberg showed the same schema (ids 1–4, 6, 7) and rows.

## Revisit when

- Partition evolution: make deletes, updates and merges correct for
  files under older specs (position deletes inherit the data file's
  spec), then accept `ADD` / `DROP PARTITION FIELD` in some syntax.
- Nested fields need evolving: dotted names, then struct
  add/drop/rename, as ADR 0041 anticipated.
- `DROP TABLE`: a destructive grant of its own, with `PURGE` deleting
  files.

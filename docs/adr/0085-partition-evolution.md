# ADR 0085 — Partition evolution

- Status: Accepted
- Date: 2026-11-03
- Owner: JoaoLucas2210
- Related: ADR 0084 (ALTER TABLE), ADR 0076 (partitioned writer), ADR 0077/0081/0082 (row-level writes), ADR 0078 (compaction), ADR 0055 (partition filters across specs)
- Scope: `ddl::partition_change`, `Alteration::{AddPartitionField, DropPartitionField}`, `iceberg::alter_updates` (specs), `write::{Layout::of_spec, Written::spec, Target::spec, commit}`, `IcebergTable::{rows_of, compact}`

## Context

An Iceberg table's partitioning can change without rewriting data. A new
partition spec becomes the default, new files are written under it, and
old files keep theirs. Reads already handled several specs: pruning
uses each manifest's own spec (ADR 0055).

Writes did not:

- `DELETE`, `UPDATE` and `MERGE` refused tables holding files under a
  non-default spec. A position delete must sit in its data file's spec
  and partition, or readers never apply it, yet every manifest was
  written in the default spec.
- There was no way to change a spec through SQL. ADR 0084 deferred it
  for exactly this reason.

## Decision drivers

1. **Correct deletes for every file**, whatever spec it was written
   under, as Spark and pyiceberg index them.
2. **Iceberg's semantics** for spec changes: field ids reused, existing
   specs reused, files never rewritten by the change itself.
3. **A way to converge**: old-spec files can be moved to the current spec.

## Decision

- **Syntax**, Spark's (the SQL parser has no statement for it, so a
  small parser over its tokenizer recognizes exactly these):

  ```sql
  ALTER TABLE [IF EXISTS] ns.t ADD PARTITION FIELD bucket(16, id) [AS name]
  ALTER TABLE [IF EXISTS] ns.t DROP PARTITION FIELD days(ts)
  ALTER TABLE [IF EXISTS] ns.t DROP PARTITION FIELD name
  ```

  The transforms are those of `CREATE TABLE` (ADR 0083). A bare name in
  `DROP` matches a partition field's name first, then an identity field
  on that column.
- **The new spec** (`alter_updates`):
  - **Fields.** It is the default spec's fields with the change applied.
    An added field that existed in an older spec keeps its field id;
    otherwise it gets one above `last-partition-id`.
  - **Reuse.** When an existing spec has the same fields, it simply
    becomes the default again. Otherwise the change is `add-spec` plus
    `set-default-spec -1`.
  - **Writability.** The spec must be one Plimsoll can write (the
    partitioned writer's `Layout` accepts it). A transform on the wrong
    type, or a decimal identity, is refused at `ALTER` time rather than
    at the next `INSERT`.
  - **Concurrency.** It requires `assert-default-spec-id` and
    `assert-last-assigned-partition-id` as seen, and retries on conflict
    (ADR 0084).
  - **Refusals.** A duplicate field, an unknown field and an unknown
    column are refused.
- **Deletes in their file's spec.**
  - Each `Target` carries its data file's spec id and partition tuple,
    and each written file records its spec. New data uses the default
    spec; a position delete uses its data file's.
  - A commit writes **one manifest per content and spec**, each with
    that spec's partition schema, `partition-spec` metadata and
    manifest-list summaries (`Layout::of_spec`).
  - The refusal in `rows_of` is gone. `DELETE`, `UPDATE` and `MERGE`
    work on any table; updated and merged rows are rewritten under the
    default spec.
- **Compaction converges.** A data file under a non-default spec is
  always rewritten, even alone, so one compaction moves a table onto its
  current spec. This is Spark's `rewrite_data_files` after a spec
  change.

## Consequences

**Positive**
- Tables can repartition as they grow (by day, then by hour plus a
  bucket) without a rewrite, and every write path keeps working.
- Spark and pyiceberg read the evolved tables, delete from them, and
  agree with Plimsoll.

**Negative (accepted debt)**
- **Spark's `REPLACE PARTITION FIELD`, `WRITE ORDERED BY` and v1 void
  transforms are not offered.** A replace is a drop then an add.
- **Old specs whose transforms Plimsoll cannot write** (a decimal
  identity, say) still block row-level deletes on their files. The
  delete manifest for that spec cannot be laid out, and the commit
  fails cleanly.
- After a promotion (ADR 0084), an old spec's identity partition keeps
  its original value type in new delete manifests. That is consistent
  with its data files, but not checked further.
- Compaction rewrites every old-spec file in one go; on large tables
  that should be bounded per run.

## Validation

- `ddl::tests::plans_partition_field_changes`: `ADD … AS`, `DROP` by
  transform, name normalization and `;`. Trailing tokens and unknown
  transforms are refused.
- `iceberg::alter::tests::partition_changes_add_or_reuse_a_default_spec`:
  - an added bucket field gets spec 1 and field id 1001;
  - dropping the identity field gives an unpartitioned spec;
  - re-adding a dropped field makes its old spec the default again;
  - a duplicate, an unknown field, `day` on a string and a decimal
    identity are refused.
- `lakehouse::tests::writes_through_the_rest_catalog` (live in CI, on S3):
  - after `ADD PARTITION FIELD`, a row is inserted under the new spec
    and deleted, and a row of an old-spec file is updated;
  - position deletes exist in both specs;
  - compaction leaves only files in the new spec, with the expected
    rows.
- By hand, against the reference REST catalog:
  - a table partitioned by region evolved to `days(ts)` and then
    dropped region (specs 0 → 1 → 2), with rows under both specs;
  - day pruning read 1 of 4 files;
  - `DELETE`, `UPDATE` and `MERGE` touched files of both specs, writing
    deletes into specs 0 and 2;
  - adding and dropping a bucket field went back to spec 2, and the
    duplicate and unknown-field cases were refused;
  - Spark 3.5 read the same rows and files by spec, deleted an old-spec
    row and appended;
  - Plimsoll agreed, and compaction then rewrote all 5 affected files
    into 1 under spec 2 with no delete files left;
  - pyiceberg showed spec `day(ts)`, the same rows, and the same answer
    to a day filter.

## Revisit when

- Large tables evolve: bound compaction's spec migration per run, by
  partition.
- Users want `REPLACE PARTITION FIELD` or a sort order
  (`WRITE ORDERED BY`): extend the small parser.

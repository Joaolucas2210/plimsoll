# ADR 0095 — Writes to format v3 tables

- Status: Accepted (amended by ADR 0096: UPDATE, merge-update and compaction now keep row lineage and are allowed)
- Date: 2026-11-03
- Owner: JoaoLucas2210
- Related: ADR 0017 (v3 reads, deletion vectors), ADR 0075–0083 (writes, CREATE TABLE), ADR 0091–0094 (writes on workers)
- Scope: `write::{Layout (version), manifest_entry_schema, manifest_file_schema, manifest, manifest_list, commit (row ids), IcebergSink::{deletes, deletion_vector}, rewrite_manifest (deletes_of), Written::{dv, added}}`, `IcebergTable::keeps_lineage`, `create_request` (`format-version` 3)

## Context

Plimsoll reads Iceberg v3, deletion vectors included (ADR 0017), but
every write refused v3 tables. Writing v3 is more than a version number.
Three spec rules apply:

- **Row lineage.** Every row has a `_row_id`. A snapshot declares
  `first-row-id` (the table's `next-row-id`) and `added-rows`. Each new
  data manifest gets a `first_row_id` in the manifest list, and its
  files inherit ids from it by position.
- **Deletion vectors replace position delete files.** A v3 writer must
  not add position delete files. Deletes are `deletion-vector-v1` blobs
  in Puffin files, located by `content_offset` and
  `content_size_in_bytes`.
- **At most one deletion vector per data file.** A new one must include
  the earlier deletes of that file, and replace them.

Writes that *rewrite* existing rows (`UPDATE`, a merge's update,
compaction) must also carry each row's `_row_id` into the new files.

## Decision drivers

1. **Valid v3 for other engines.** Spark and pyiceberg read, and keep
   writing, what Plimsoll writes.
2. **Never silently break lineage.** Refuse what cannot be done right
   yet.

## Decision

- **The format version follows the table.** `Layout` takes v2 or v3.
  Manifests carry v3's `first_row_id`, `content_offset` and
  `content_size_in_bytes` (field ids 142, 144, 145), the manifest list
  carries `first_row_id` (520), and both declare their version.
- **Row lineage.**
  - At commit, new data manifests take row ids from the table's
    `next-row-id`, in list order. The snapshot carries `first-row-id`
    and `added-rows`, which the catalog uses to advance `next-row-id`.
  - New data files leave their own `first_row_id` null and inherit it.
  - Retries recompute ids on fresh metadata.
- **Deletes are deletion vectors.** On v3, `IcebergSink::deletes` writes,
  for each data file with new deletes, one Puffin file holding one blob.
  - The blob is a big-endian length, the magic, a portable 64-bit
    Roaring bitmap, and a CRC-32. The footer lists the blob with
    `referenced-data-file` and `cardinality`.
  - The bitmap holds every deleted position of the file: the ones its
    earlier deletes removed (from the planned selection) plus the new
    ones.
  - The commit marks every earlier delete entry referencing that data
    file `DELETED` (`rewrite_manifest`'s `deletes_of`), whether an older
    vector or a file-scoped position delete. Partition-scoped position
    deletes, which reference no single file, stay; they are harmless,
    since the vector repeats their positions.
  - Counts report newly deleted rows (`Written::added`), not the
    vector's cardinality.
  - The same path serves `DELETE`, a merge's deletes, and their worker
    variants (ADR 0093, 0094). Reported files carry the blob's location.
- **Refused on v3, with an explicit error** (`keeps_lineage`):
  - `UPDATE`;
  - `MERGE … WHEN … THEN UPDATE`;
  - compaction.

  Each rewrites existing rows, and Plimsoll does not write `_row_id` /
  `_last_updated_sequence_number` into new files yet. Appends, `DELETE`,
  and `MERGE` with deletes and inserts are allowed.
- **`CREATE TABLE … OPTIONS ('format-version' = '3')`** creates v3
  tables. v2 stays the default.

## Consequences

**Positive**
- v3 tables, including Spark's, take Plimsoll's inserts, CTAS, deletes
  and delete/insert merges, distributed or not. Row ids stay consistent
  across engines.
- A data file never has more than one deletion vector, however many
  deletes it takes.

**Negative (accepted debt)**
- **No `UPDATE`, merge-update or compaction on v3** until rewritten rows
  carry their lineage columns.
- **One Puffin file per deletion vector.** Engines pack many vectors per
  file, which is fewer objects, but each must then be located by
  offset.
- New v3 types (`variant`, geometry, `unknown`) and default values
  (`write-default`) are still not written; tables with defaults are
  refused (ADR 0017).

## Validation

- `write::tests::layouts_accept_the_transforms_the_pruner_knows`: v3
  layouts build, and v4 is refused.
- `create::tests::maps_types_partitions_and_properties`: `format-version`
  3 is accepted, 4 refused, and 2 is the default.
- `lakehouse::tests::writes_through_the_rest_catalog` (live in CI, on
  S3), on a v3 table:
  - two deletes on one file leave one Puffin vector with 2 positions;
  - 8 rows remain;
  - `next-row-id` is 10 and the first snapshot's `first-row-id` is 0;
  - `UPDATE` is refused.
- By hand, against the reference REST catalog, on a v3 table
  partitioned by region:
  - after two appends (100 and 2 rows), two deletes (11 and 10 rows) and
    a delete-and-insert merge, `$files` showed 3 Puffin vectors totaling
    the deleted rows, and `next-row-id` was 103;
  - Spark 3.5 (Iceberg 1.9.2) read the same counts and vectors, and
    `_row_id` 102 for the merged insert;
  - Spark deleted on top, and Plimsoll then read Spark's state and
    deleted again;
  - pyiceberg read the same 78 rows;
  - `UPDATE`, merge-update and compaction were refused.

## Revisit when

- Rewrites are needed on v3: read `_row_id` (inherited or materialized)
  and `_last_updated_sequence_number` with the target rows, and write
  them as columns of the new files. Updated rows keep their id and get
  a new sequence number; compacted rows keep both.
- Object counts matter: pack a statement's vectors into one Puffin file
  per writer.

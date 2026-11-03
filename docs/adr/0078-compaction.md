# ADR 0078 — Compaction: rewrite small files and files carrying deletes

- Status: Accepted
- Date: 2026-11-03
- Owner: JoaoLucas2210
- Related: ADR 0075–0077 (appends, partitions, deletes), ADR 0014/0016/0017 (reading deletes), ADR 0023 (admission)
- Scope: `IcebergConnector::compact`, `IcebergTable::{compact, keep_empty}`, `write::{IcebergSink::replace, write_rows, Change::Replace, rewrite_manifest, read_header, Counts, Gone}`, `lakehouse::compact`, `QueryEngine::compact`, `POST /v1/maintenance/compact`

## Context

Writes accumulate debt for readers:

- every `INSERT` adds at least one file per partition it touches,
  often small (ADR 0075/0076);
- every `DELETE` adds position delete files that each read must load and
  apply (ADR 0077);
- tables written by other engines carry equality deletes and deletion
  vectors too.

Without compaction, reads get slower with every write. Iceberg's answer
is a `replace` snapshot: new data files hold exactly the live rows of
the files they replace, and the old files, along with the deletes that
applied only to them, leave the current snapshot. They stay in history,
so time travel still works.

## Decision drivers

1. **The rows do not change.** A rewrite holds the same live rows, under
   the same partitioning; readers at the new snapshot see exactly what
   they saw before.
2. **Reuse what reads and writes already do.** The scan already applies
   every kind of delete; the writer already routes by partition with
   statistics.
3. **Safe under concurrency.** A rewrite must never resurrect a row that
   a concurrent delete removed.
4. **Interop.** Spark and pyiceberg read the result, including time
   travel to before it.

## Decision

- **Surface.** `POST /v1/maintenance/compact {"table": "schema.table"}`.
  - Authenticated like `/v1/sql`.
  - It needs the read grant and the write grant on the table (the rows
    are read and rewritten), both checked before the catalog is asked
    anything.
  - It holds an admission slot in the caller's default resource group
    within the query timeout, and logs one `compaction` line.
  - It returns `{partitions, files_rewritten, files_written, rows}`.
  - DataFusion has no `CALL`/procedure statement, and table functions run
    at planning time, synchronously, so SQL was not an option.
- **Choice of files.**
  - All live data files are planned. That includes, with `keep_empty`,
    files whose every row is deleted: queries skip those, but compaction
    must retire them, or they and their deletes would stay forever.
  - Per (spec, partition), a file is a candidate when it carries deletes
    (position, deletion vector or equality) or is small (under 96 MiB:
    75% of the 128 MiB write target, as Iceberg's `min-file-size-bytes`
    default).
  - A partition is rewritten when it has a candidate with deletes, or
    two small candidates. Large clean files are left alone.
- **Rewriting.**
  - The chosen files' live rows come from the regular scan restricted to
    them, so every kind of delete is applied by the code that serves
    queries.
  - They are written by the same partitioned writer as `INSERT`, with
    statistics, under the default spec (older-spec files are rewritten
    into it).
- **Commit: a `replace` snapshot.**
  - Every manifest of the parent that lists a replaced data file, or a
    position delete file whose `referenced_data_file` is one, is
    rewritten:
    - those entries become `DELETED` by the new snapshot;
    - every other live entry becomes `EXISTING`, its sequence numbers
      written out explicitly, because inheritance would otherwise give it
      the new manifest's;
    - entries deleted earlier are dropped.
  - The copy keeps the source manifest's **exact Avro schema text and
    metadata**: Plimsoll reads the container header itself and reuses
    the header writer of ADR 0076.
  - The manifest list swaps the rewritten manifests' entries (new path,
    length, counts, `min_sequence_number`) and adds the new data
    manifest. The summary carries `deleted-data-files`,
    `removed-delete-files` and their row counts.
  - Every chosen file must be found and marked. Otherwise the commit
    aborts.
- **Concurrency.**
  - The rows were read at the snapshot the table was loaded at (`base`).
  - The commit retries over concurrent `append`s only. Anything else
    fails it: a delete, an overwrite or another rewrite could remove
    rows Plimsoll just copied.
  - The same base-snapshot rule now also guards `DELETE`, which compared
    against the snapshot seen when committing started and missed
    operations committed in between.
- **Not covered yet.**
  - Equality delete files are not removed. Their sequence number is
    below the new files', so they no longer apply to them, but they stay
    listed until a full rewrite or snapshot expiry.
  - Position deletes without `referenced_data_file` (partition-scoped)
    stay too, applying to nothing.

## Consequences

**Positive**
- After `DELETE`s and small `INSERT`s, one call restores one file per
  partition with no deletes to apply. That is checked in Plimsoll's own
  `$files` and in Spark's `files` and `delete_files` tables.
- Time travel to before the compaction still returns the old rows: the
  old files are only removed from the current snapshot.
- No new reading or writing code paths; compaction composes the scan
  and the writer.

**Negative (accepted debt)**
- **Whole-table, on demand.** There is no partition filter, no
  scheduler, and no size-based bin packing beyond "small or has
  deletes".
- **Coordinator-only**, like other writes.
- **Old files are never deleted from storage.** That is snapshot expiry
  and orphan file removal, the next maintenance steps.
- Equality deletes and partition-scoped position deletes linger, as
  above.

## Validation

- `write::tests::rewritten_manifests_mark_replaced_files_and_their_deletes`:
  - the Avro header round-trips: the schema with `logicalType: map`, and
    the Iceberg metadata;
  - in a data manifest, a replaced file becomes DELETED by the rewrite's
    snapshot, while the other becomes EXISTING with its inherited
    sequence number made explicit;
  - a second rewrite drops the earlier DELETED entry;
  - in a delete manifest, only the delete file referencing a replaced
    file goes;
  - nothing matching yields `None`.
- `engine::tests::compaction_needs_read_and_write_grants`: a principal
  with only a read grant, or with a write grant elsewhere, is denied;
  one with the grants reaches the catalog.
- `lakehouse::tests::writes_through_the_rest_catalog` (live in CI, on
  S3), after its inserts and deletes:
  - compaction rewrites more files than it writes, carries the 3 live
    rows, removes every position delete, and leaves the rows unchanged;
  - a second call rewrites nothing;
  - the last snapshot is a `replace`.
- By hand, against the reference REST catalog, on a table partitioned by
  region with three inserts and a delete: 5 files were rewritten into 2
  across 2 partitions (including fully deleted files).
  - Spark 3.5 then read the same rows, saw 3 live files and no delete
    files, and showed a `replace` snapshot (5 data files, 2 delete files
    removed).
  - Spark read the pre-compaction snapshot by time travel, and appended
    on top; pyiceberg agreed.
  - On pyiceberg's `bronze.trips`, its 2 files became 1, with the same 7
    rows.

## Revisit when

- Storage grows with history: expire snapshots, then remove files no
  snapshot references (orphan removal).
- Large tables: compact by partition filter and in bins of the target
  size, and schedule it.
- Equality deletes dominate: drop equality delete files once no live
  data file has a lower sequence number in their partition.

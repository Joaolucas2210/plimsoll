# ADR 0080 — Orphan file removal

- Status: Accepted
- Date: 2026-11-03
- Owner: JoaoLucas2210
- Related: ADR 0079 (snapshot expiration, reachability), ADR 0075–0078 (writes), ADR 0024 (governed maintenance)
- Scope: `iceberg::expire::{orphans, OrphanOptions, Orphans, reach (deleted_too)}`, `IcebergConnector::remove_orphans`, `lakehouse::remove_orphans`, `QueryEngine::remove_orphans`, `POST /v1/maintenance/remove-orphans`

## Context

Some files under a table's location are never referenced by any commit:

- data files and manifests of a write whose commit failed or lost its
  last retry (ADR 0075);
- files an interrupted snapshot expiration meant to delete (ADR 0079);
- files written by other engines that crashed mid-commit.

Nothing reads them, but storage holds them until something lists the
location and compares it against everything the table references.
Iceberg calls this `deleteOrphanFiles`.

The danger is a write **in flight**. Its files exist before its commit
references them, and deleting them would make the commit point at
missing files and corrupt the table. Every implementation therefore
only touches files older than a safety window.

## Decision drivers

1. **Never delete a referenced file**, nor one a write may still commit.
2. **Err towards keeping.** A lingering orphan costs storage; a wrong
   deletion loses data.
3. **Reviewable.** An operator can see what would go before anything
   goes.

## Decision

- **Surface.** `POST /v1/maintenance/remove-orphans
  {"table", "older_than_ms"?, "dry_run"?}`.
  - It shares the maintenance handler: authentication, read and write
    grants, an admission slot, the timeout, and one `orphan_removal` log
    line.
  - The window defaults to 3 days ago, as Iceberg's default. **Anything
    later than 1 hour ago is refused** with `400`.
  - `dry_run` lists without deleting.
  - It answers `{scanned, orphans, deleted, dry_run, sample}`, with up to
    100 paths for review.
- **Referenced** means reachable from any snapshot of the table:
  - manifest lists, manifests, and data and delete files;
  - **including entries marked `DELETED`** (conservative: they are named
    by metadata);
  - the statistics and partition-statistics files the metadata lists.

  Paths are compared as object-store paths, so `file:/x`, `file:///x`
  and `s3://b/x` spellings of one file agree.
- **Candidates** are files under the table location last modified before
  the window and not referenced. Never candidates:
  - `*.metadata.json` and `version-hint.text`, which are the catalog's
    to manage;
  - hidden files (`.crc` and the like).
- **Deletion.** As in expiration: 16 at a time, and already-missing
  files count as deleted.

## Consequences

**Positive**
- Failed writes and interrupted expirations stop leaking storage.
- A dry run gives a reviewable list first.
- Maintenance is now complete for the write path: compaction (0078),
  expiration (0079) and orphan removal (0080).

**Negative (accepted debt)**
- **The window is the only protection against in-flight writes.** A
  write that stays uncommitted for longer than the window (the minimum
  is 1 hour) would lose its files. That is the same contract as other
  engines'.
- **Every manifest of every snapshot is read**, and the whole location
  is listed, with no prefix sharding and no cache.
- Files outside the table location (data placed elsewhere by
  `write.data.path`) are never examined.
- Stale `metadata.json` files are left to the catalog.

## Validation

- `lakehouse::tests::writes_through_the_rest_catalog` (live in CI, on S3).
  After the inserts, deletes, compaction and expiration, a stray file is
  put under the table's `data/`:
  - a dry run finds exactly it and deletes nothing;
  - the run deletes it;
  - a second run finds nothing;
  - the current rows and the tagged snapshot read unchanged.
- By hand, against the reference REST catalog, with files backdated by
  2 days and history from inserts, a delete and a compaction:
  - a window of "now" is refused;
  - a dry run finds exactly the two planted orphans (a failed write's
    Parquet and a failed commit's manifest), out of 29 files;
  - the run deletes them;
  - a fresh "in-flight" file inside the window is kept;
  - every file historic snapshots reference is kept, and time travel to
    the first snapshot still reads;
  - a repeat finds nothing.

## Revisit when

- Locations grow huge: list by prefix in parallel, and compare against a
  reachability set built once per run and shared with expiration.
- Writers use `write.data.path` / `write.metadata.path` outside the
  table location: scan those too.
- Catalogs leave stale metadata files: remove `*.metadata.json` older
  than the window and absent from `metadata-log`.

# ADR 0079 — Snapshot expiration

- Status: Accepted
- Date: 2026-11-03
- Owner: JoaoLucas2210
- Related: ADR 0078 (compaction), ADR 0075–0077 (writes), ADR 0028/0029 (time travel, branches and tags)
- Scope: `iceberg::expire::{expired, expire, reach, remove, ExpireOptions, Expiration}`, `IcebergConnector::expire_snapshots`, `lakehouse::{maintained, expire_snapshots}`, `QueryEngine::expire_snapshots`, `engine::maintainer`, `POST /v1/maintenance/expire-snapshots`

## Context

Every commit adds a snapshot, and every snapshot keeps alive every file
it reaches. Compaction (ADR 0078) and deletes (ADR 0077) take files out
of the *current* snapshot, but not off storage: earlier snapshots still
reference them, so time travel keeps working. Without expiration,
storage only grows, and metadata (the snapshot list, manifest lists)
grows with every write.

## Decision drivers

1. **Never delete a file a kept snapshot reads.** Branch and tag heads
   are always kept.
2. **Metadata first, files after.** A file is deleted only once the
   catalog has committed the snapshots' removal, so a failure leaves
   orphans (harmless), never a snapshot pointing at a missing file.
3. **Iceberg's defaults and semantics**, so operators get what other
   engines' `expire_snapshots` does.

## Decision

- **Surface.** `POST /v1/maintenance/expire-snapshots
  {"table", "older_than_ms"?, "retain_last"?}`.
  - It shares the maintenance handler with compaction: authentication,
    read and write grants on the table (history is data), an admission
    slot, the query timeout, and one `snapshot_expiration` log line.
  - The defaults are Iceberg's: `older_than_ms` 5 days ago
    (`history.expire.max-snapshot-age-ms`), `retain_last` 1
    (`min-snapshots-to-keep`, never below 1).
- **What is kept** (`expired`, pure and unit-tested):
  - every branch and tag head;
  - per branch, its `retain_last` most recent ancestors, and every
    ancestor committed after the cutoff;
  - any other snapshot (one off every branch, e.g. after a rollback)
    committed after the cutoff;
  - a table without refs treats its current snapshot as `main`.

  Everything else committed before the cutoff expires.
- **Commit.** `remove-snapshots` with those ids, through the catalog,
  requiring the table uuid and `main` as seen. On a conflict, the set is
  recomputed on the fresh metadata, up to 4 times.
- **Files.** After the commit, both sides are walked: the expired
  snapshots and the kept ones.
  - From each side: manifest lists, manifests, and the data and delete
    files live in those manifests. `DELETED` entries are skipped,
    because they name files their snapshot removed.
  - What only the expired side reaches is deleted, 16 at a time.
    Already-missing files count as deleted.
  - That is exactly the old files a compaction or delete left behind,
    once no kept snapshot can time-travel to them.
- **Not deleted.** Old `metadata.json` versions belong to the catalog
  (its `write.metadata.delete-after-commit` settings). Files no commit
  ever reached (failed writes) are orphan removal, the next step.

## Consequences

**Positive**
- After compaction, one call reclaims the replaced data files, their
  delete files and the manifests and manifest lists only history used.
  Storage tracks the retained history, not all of it.
- Tags pin what they mark: a tagged snapshot and its files survive any
  expiration, and stay readable by time travel (`t@tag`).

**Negative (accepted debt)**
- **Every manifest of every snapshot is read** to compute reachability,
  with no cache of reachable sets. That is proportional to the table's
  metadata, fine on demand, heavy on huge histories.
- **Deletion is not transactional.** If deleting fails midway, the
  remaining files become orphans for orphan removal.
- **Per-ref retention properties are not honored yet**
  (`max-ref-age-ms`, per-branch `min-snapshots-to-keep`); one policy
  applies to every branch.
- Coordinator-only and on demand; no scheduler.

## Validation

- `expire::tests::heads_recent_and_last_ancestors_are_kept`. On a branch
  `1 ← 2 ← 3 ← 4`, a tag on 1, a stale stray 9 and a recent stray 10:
  - cutoff 350 with `retain_last` 1 expires `[2, 3, 9]`;
  - `retain_last` 3 expires `[9]`;
  - a cutoff before everything expires nothing;
  - a table without refs keeps its current snapshot.
- `lakehouse::tests::writes_through_the_rest_catalog` (live in CI, on S3).
  After the inserts, deletes and compaction, a tag is set on the first
  snapshot and everything is expired:
  - all snapshots but the head and the tag go, and data files and
    manifest lists are deleted;
  - the current rows are unchanged;
  - the tagged snapshot reads exactly as before;
  - a second call expires nothing.
- The maintenance grants are checked by
  `engine::tests::compaction_needs_read_and_write_grants` (the shared
  `maintainer`).
- By hand, against the reference REST catalog: three inserts, a delete
  and a compaction, then expiring all but the head.
  - 4 snapshots expired. On disk, data files went 8 → 2 and metadata
    Avro files 14 → 6.
  - Spark 3.5 read the same rows, saw only the `replace` snapshot and 2
    files in `all_files` (no dangling references), and appended on top;
    pyiceberg agreed.

## Revisit when

- Histories grow large: cache per-manifest file sets, or compute
  reachability from manifests the expired snapshots added.
- Branch-specific retention is needed: honor `max-snapshot-age-ms`,
  `min-snapshots-to-keep` and `max-ref-age-ms` per ref.
- Failed writes leave files behind: remove orphans (files under the table
  location that no snapshot or metadata reaches, older than a safety
  window).

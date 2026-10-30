# ADR 0018 — Caches under the ceiling, and a delete-file cache

- Status: Accepted
- Date: 2026-10-30
- Owner: JoaoLucas2210
- Related: ADR 0003 (refines the budget layout), ADR 0012, ADR 0014, ADR 0016, ADR 0017, ADR 0013
- Scope: `plimsoll_core::memory` (`CeilingPool::with_cache_reserve`, `runtime_env`), `plimsoll-server` (`main`, `metrics`), `plimsoll-connectors::iceberg::deletes` (`cached`)

## Context

The ROADMAP item was a cache for Iceberg delete files, which every query
re-read and re-decoded (ADRs 0014, 0016, 0017). Working out where that cache
should live surfaced two existing defects:

1. **Memory outside the ceiling.** DataFusion 55's `RuntimeEnv` creates three
   caches by default, and none of them reserves memory in the `MemoryPool`:
   - file metadata (Parquet footers), 50 MiB;
   - file statistics, 20 MiB;
   - file listings, 1 MiB.

   `runtime_env` never configured them, so up to ~71 MiB per process sat
   above the Plimsoll line. ADR 0003's claim, "every byte of query memory is
   reserved", held for queries but not for the process.
2. **Stale directory tables.** The listing cache's default TTL is infinite.
   A `PLIMSOLL_PARQUET_TABLES` directory table never saw files added after
   its first query. Reproduced by `parquet::tests::directory_tables_see_new_files`
   (4 rows instead of 8) before the fix.

A delete-file cache added outside the pool would have widened defect 1.

## Decision drivers

1. The ceiling covers everything that grows with data, caches included
   (ADR 0003, 0012).
2. Caches never compete with queries at run time. A cache must not be the
   reason a query fails.
3. Freshness over speed. A cache must never serve data newer commits made
   stale.
4. Reuse DataFusion's cache machinery instead of writing an LRU.
5. Observable: operators can see the budget and whether delete reads hit.

## Considered options

**Budget**

- **A. Fixed cache reserve carved out of the limit (chosen).** The pool's
  `cache_reserve` is never available to queries. DataFusion's caches are
  sized to it, so their worst case is inside the limit by construction. It is
  predictable: queries always have `limit − cache − output` for work.
- **B. Dynamic: caches reserve as they grow and yield to queries.** This uses
  idle memory better. But DataFusion's caches evict internally and expose no
  usage hook (`Cache` has no `memory_used`), and a full cache could only give
  memory back by being cleared. It would fail driver 2 under pressure.
- **C. Disable all caches.** This is honest but repeats footer and delete
  reads on every query against object storage.

**Delete-file cache**

- **A. Entries in DataFusion's file metadata cache (chosen).** It is already
  LRU, size-bounded and per `RuntimeEnv` (one per process). Entries implement
  `FileMetadata` (`Cached<T>`).
- **B. Own cache keyed by snapshot.** This means our own LRU and memory
  accounting for the same result.

**Keying.** Iceberg data and delete files are immutable: a path is never
rewritten, and a commit adds new files and removes old ones. So the file
identity alone is a complete key. A snapshot key would only lower the hit
rate across commits that did not touch a delete file. Keys:

| Delete kind | Cache key |
|---|---|
| Position delete file | its path |
| Deletion vector | Puffin path `#offset+size` |
| Equality delete file | path `?eq=` plus key names and types (the decoded value is cast to table types) |

## Decision

- `CeilingPool::with_cache_reserve(bytes)`: `output + cache ≤ limit`, else
  `InvalidConfig`. Queries see `limit − cache` (output slice included). OOM
  messages name the cache reserve. `CeilingPool::new` keeps a reserve of 0,
  and with 0 every cache is disabled.
- `runtime_env` sizes the caches from the reserve:
  - file metadata (Parquet footers and delete files): 3/4 of it;
  - file statistics: 1/4;
  - file listings: **0, i.e. off**.
- The server reserves `min(limit / 16, 64 MiB)` (`ponytail:` fixed split) and
  exports `plimsoll_pool_cache_reserve_bytes`.
- Delete files are decoded whole: every `(file_path, pos)` of a position
  file, the bitmap of a DV, the cast keys of an equality file. They are then
  put in the cache.
- On a miss, the decoded value is reserved in the query's planning
  reservation until the query is done with it. The cache's own budget covers
  it afterwards (`put` drops values larger than the cache).
- Positions a query keeps from a cached entry are reserved as before
  (8 B each).
- `EXPLAIN` label gains `delete_cache_hits=<n>`.

## Consequences

**Positive**
- The process ceiling now includes DataFusion's caches (were ~71 MiB
  unaccounted by default).
- Directory tables see new files on the next query.
- Repeated queries on MOR tables skip delete-file reads and decodes:
  - the second planning of the fixture shows `delete_cache_hits=1`;
  - the reference interop answers are unchanged.

**Negative (accepted debt)**
- Up to 1/16 of the limit (≤ 64 MiB) is not available to queries even
  when the caches are cold.
- No directory listing cache. Each query on a directory table lists it, one
  object store LIST per query.
- A position delete file is now decoded whole (all data files' positions)
  on a miss, not filtered while decoding. A miss on a large delete file
  reserves more during planning than before.
- No hit-rate metric: DataFusion's cache exposes no counters. Hits are only
  visible per query in `EXPLAIN`.
- Fixed split and fixed fraction, not configurable.

**Operational**
- `plimsoll_pool_cache_reserve_bytes` makes the reserve visible. Effective
  query memory is `plimsoll_pool_limit_bytes − cache − output`.
- `delete_cache_hits=0` on repeated queries of a table with deletes means
  entries are evicted (cache too small) or larger than the cache.

## Risks and mitigations

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| A cached delete file is served after it changed | Very low (immutable by spec) | High | Keys include path, blob range and key types. A writer that rewrites files in place violates the Iceberg spec. |
| DataFusion's size estimates for cached Parquet footers undercount | Medium | Low (bounded by the cache limit's accuracy) | Same estimator DataFusion uses for its own limit; reserve sized conservatively. |
| Cache entries collide with DataFusion's footer entries | Very low | Medium | Our keys are prefixed `plimsoll-deletes/`, and a type mismatch on downcast is treated as a miss. |
| A future DataFusion default adds another unaccounted cache | Medium | Medium | `runtime_env` builds the `CacheManagerConfig` explicitly; review it on DataFusion upgrades. |

## Validation

- `memory::tests::cache_reserve_is_never_handed_to_queries`: invalid
  config, working limit `limit − cache − output`, output can't use the cache
  slice, OOM message.
- `parquet::tests::directory_tables_see_new_files`: failed (4 ≠ 8) before
  the listing cache was disabled, passes after.
- `tests/iceberg.rs::delete_files_are_cached_within_the_cache_reserve`:
  - position and equality deletes;
  - `delete_cache_hits=0` then `=1`, same rows, pool back to 0;
  - no caching with a zero reserve.
- Interop (`reads_a_spark_merge_on_read_table`,
  `reads_reference_equality_deletes`, `reads_reference_deletion_vectors`)
  unchanged.
- `metrics::tests::renders_exposition_lines` includes
  `plimsoll_pool_cache_reserve_bytes`.

## Revisit when

- Deployments need a larger footer/delete cache, or none: make the reserve
  configurable.
- DataFusion's caches expose usage counters: export hit rate and bytes.
- Directory tables on object storage show LIST latency: add a listing cache
  with a short TTL inside the reserve.

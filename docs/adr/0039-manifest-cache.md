# ADR 0039 — Manifest cache

- Status: Accepted
- Date: 2026-11-02
- Owner: JoaoLucas2210
- Related: ADR 0018 (caches under the ceiling; delete-file cache), ADR 0020 (concurrent fetches), ADR 0038 (result cache; this is its "a hit still plans" item)
- Scope: `plimsoll_connectors::iceberg::IcebergTable::data_files` (`load`), `deletes::cached` (now shared), scan label

## Context

Every query over a catalog table plans from scratch (ADR 0008):

1. `loadTable`;
2. the snapshot's manifest list;
3. every manifest that survives partition pruning;
4. the delete files.

Delete files were already cached (ADR 0018). Manifests were not. On
object storage, each manifest is a GET of 10–100 ms. A dashboard
re-running a small query over an unchanged table spends most of its
latency re-reading the same immutable manifests.

After ADR 0038, a result-cache hit skips execution but still pays all of
planning. For small queries that is the majority of the time.

## Decision drivers

1. **Correct by construction.** Iceberg never rewrites a manifest list or
   a manifest in place: a commit writes new files with new paths. A cache
   keyed by path cannot serve stale data.
2. **Under the ceiling,** with no new budget (ADR 0018's cache reserve).
3. **Visible.** Operators must be able to tell whether planning hit the
   cache.

## Considered options

**A. Cache the raw bytes of manifest lists and manifests by path, in
DataFusion's file metadata cache (chosen).**
- It uses the same helper (`deletes::cached`) and the same cache as delete
  files and Parquet footers.
- It is bounded by the cache reserve's metadata share and evicts LRU.
- Decoding still runs per query: CPU, microseconds per entry. The I/O,
  which is what costs, is gone.

**B. Cache the planned file list per `table@snapshot` + filters.** It
would skip decoding too, but the key must include the pruning predicate,
and the planned list carries per-query state (row selections, equality
delete sets). That is more code for CPU savings that do not matter next to
I/O.

**C. Cache decoded manifest entries per snapshot.** Pruning would then
need every manifest of the snapshot decoded and held, including the ones a
selective query would have skipped. That costs more memory than the bytes
for pruned workloads.

## Decision

- **Cached loads.** In `data_files`, `load(path)` goes through
  `deletes::cached` with key `manifest/<path>`:
  - **on a miss,** read, reserve in the planning reservation, insert, and
    return `held = bytes`;
  - **on a hit,** return the cached `Bytes` (a cheap clone) with
    `held = 0`, since the cache accounts for it.
  - Callers shrink by `held`, not by the length.
- **Scope.** This applies to the manifest list and the manifests read by
  scans. `$files` and `$manifests` are not covered yet (see Negative).
- **Label.** The scan label gains `manifest_cache_hits=N`:
  `EXPLAIN` shows `manifest_cache_hits=9` for a list plus 8 manifests
  served from memory.

## Consequences

**Positive**
- Replanning an unchanged snapshot reads no manifests. In the test, with
  50 ms per GET, the first plan took over 100 ms and the second under
  50 ms with zero GETs.
- Combined with ADR 0038, a repeated dashboard query over an unchanged
  table costs only `loadTable`, which is the freshness check, plus
  in-memory decoding.
- No new configuration, budget or dependency.

**Negative (accepted)**
- **Manifests compete with Parquet footers and delete files** for the
  same metadata-cache budget, at 3/4 of the cache reserve. On huge
  tables, manifests may evict footers. Separate budgets are possible if
  measured contention appears.
- **`$files` and `$manifests` (ADR 0032)** read at execution through a
  `TaskContext` and do not use this cache yet.
- **Bytes are cached, not decoded entries,** so Avro decoding still runs
  per query.

## Risks and mitigations

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| A writer overwrites a manifest in place (spec violation) | Very low | High | The Iceberg spec forbids it; all reference writers produce new paths. Same assumption as the delete-file cache (ADR 0018). |
| Cache churn on very large tables | Medium | Low | Bounded LRU; worst case is today's behaviour (re-read). |

## Validation

- `tests/iceberg.rs::replanning_a_snapshot_reads_no_manifests`: a
  table with 8 manifests served by a 50 ms-per-GET store, under a
  `CeilingPool` with a cache reserve.
  - The first plan shows `manifest_cache_hits=0` and takes over 100 ms.
  - The second shows `manifest_cache_hits=9` and takes under 50 ms.
  - The pool returns to 0, because the cached bytes live in the reserve.
- The existing planning, MOR and equality-delete suites, and the interop
  tests against Spark tables, all pass unchanged.

## Revisit when

- Decoding shows up in profiles: cache decoded entries per manifest path.
- Footers and manifests contend: split the metadata cache budget.
- `$files` and `$manifests` need the same speed-up: thread the cache
  through `read_env`.

# ADR 0038 — Result cache over pinned snapshots

- Status: Accepted
- Date: 2026-11-02
- Owner: JoaoLucas2210
- Related: ADR 0003 (ceiling), ADR 0005 (correctness first), ADR 0006 (scalar subquery pre-evaluation), ADR 0018 (caches under the ceiling), ADR 0025 (grants), ADR 0028 (snapshots), ADR 0035 (streams)
- Scope: `plimsoll_core::result_cache`, `plimsoll_core::memory::AccountedExec::with_pinned`, `Engine::{with_result_cache, result_cache_stats}`, Iceberg scan, server config and metrics

## Context

Dashboards re-run the same SQL on every refresh and for every viewer.
Twenty people opening the same board means twenty identical aggregations
over a gold table that changes once an hour. Each one pays the full scan,
decode and aggregate, and holds pool memory while doing it.

A result cache is the classic answer, and the classic failure mode is
**stale answers**: serving yesterday's numbers after a commit, or a
cached `now()`. Plimsoll's first rule (ADR 0005) is correctness over
speed. A cache that is right only "usually" is not acceptable.

Iceberg gives us what most engines lack: **immutable, identified inputs**.
A snapshot id names an exact set of files and deletes that never changes.
`bronze.trips@123` always yields the same rows.

## Decision drivers

1. **Never stale, never wrong.** A result is cached only when it is a pure
   function of its key. Anything else is not cached; there is no
   "probably fine".
2. **No invalidation protocol.** Commits must be picked up without
   callbacks from writers, TTLs or manual flushes.
3. **Governance unchanged.** A hit must never show data to a principal who
   could not read it (ADR 0025).
4. **Under the ceiling.** Cached bytes are reserved in the pool, bounded,
   and a full cache never fails a query.
5. Opt-in, with zero cost when off.

## Considered options

**A. Key = SQL text + the pinned identity of every source, after planning
(chosen).**
- Plan as usual. That runs the grant check, and the per-query `loadTable`
  learns the current snapshot.
- Then key on the SQL and the `table@snapshot` of every scan.
- A commit changes a snapshot id, so the key changes. That is
  invalidation by construction (driver 2).

**B. Key on SQL with a TTL.** It is stale for up to the TTL, by design.
Rejected (driver 1).

**C. Key on the optimized logical plan.** It is robust to whitespace and
case, but plans embed folded constants (`now()`) and need a stable
serialization. SQL text plus purity checks is simpler and stricter.
Normalizing the SQL is a revisit item.

**D. Cache at the BI layer or in a proxy.** It cannot see snapshots, so
it is back to TTLs.

## Decision

- **Pinned sources.** `AccountedExec::with_pinned(id)` lets a connector
  declare that its source reads immutable data. The Iceberg scan pins
  `name@snapshot-id` (or `name@none` for an empty table). Nothing else is
  pinned:
  - Postgres, Parquet directories and memory tables;
  - system and `information_schema` tables;
  - Iceberg `$files` and `$snapshots`.
- **Cacheable query.** All three conditions must hold:
  1. The logical plan, checked **before optimization**, has no scalar
     function that is not `Immutable`. `now()`, `random()` and `uuid()`
     are stable or volatile. Optimization folds `now()` into a literal,
     which would hide it.
  2. It has no scalar subquery. Those are pre-evaluated into literals
     (ADR 0006), so their inputs (a Postgres watermark, say) would vanish
     from the physical plan.
  3. Every physical leaf is under a pinned `AccountedExec`, or is a
     constant source (`PlaceholderRowExec`, `EmptyExec`).
- **Key.** The SQL text and the sorted pinned ids.
- **Lookup point.** In `Engine::plan_stream`, after the grant check and
  physical planning. A hit replaces the execution stream with the cached
  batches. Everything downstream is unchanged: `json`, `ndjson`, `arrow`,
  output reservations, admission and logs.
- **Filling.** A miss tees the stream. Batches are copied as they pass,
  each reserved first. Collection is abandoned, and its bytes released,
  when:
  - the result outgrows ¼ of the cache, so one result cannot flush the
    rest;
  - the cache cannot make room by evicting least recently used entries;
  - the query fails or is dropped.

  A successful end inserts the entry.
- **Memory.** One `result-cache` NonSpillable consumer in the
  `CeilingPool` holds entries and in-flight copies, capped at
  `PLIMSOLL_RESULT_CACHE_BYTES`. The default is 0, which means off; the
  value is validated to at most half the ceiling. A failed reservation
  means "don't cache", never a query error.
- **Metrics.**
  - `plimsoll_result_cache_requests_total{outcome="hit"|"miss"}`, counting
    cacheable queries only;
  - `plimsoll_result_cache_bytes`;
  - `plimsoll_result_cache_entries`.

## Consequences

**Positive**
- Repeated dashboard queries skip execution: scan, decode, deletes,
  joins and aggregates. A commit is picked up by the very next query,
  with no TTL to tune.
- Hits are as governed as misses. The grant check runs before the lookup,
  and principal-scoped sources (system tables) are never cached.
- Off by default, and zero cost when off: no tee, no checks.

**Negative (accepted)**
- **A hit still plans.** The Iceberg manifest list and manifests are read
  to learn the snapshot and build the scan. On object storage that is
  the main cost of small queries. A planning cache keyed by
  `table@snapshot` is the next ROADMAP item.
- **The SQL text is the key.** `select 1` and `SELECT 1` are different
  entries.
- **A hit is shared across principals that pass the same grants.** That
  is correct, because the data is the same, but a cached result can
  outlive a grant revocation by at most the next query's planning, which
  checks grants first. So no data is ever served past a revocation.
- **Cached bytes compete with working memory,** up to the cap. That is the
  explicit trade the operator makes by setting the size.
- **LRU eviction is an O(entries) scan.** That is fine for hundreds of
  entries.

## Risks and mitigations

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| A new non-deterministic function is marked `Immutable` by DataFusion | Very low | High | The check uses DataFusion's declared volatility; tests cover `now()` and `random()`. |
| A future connector pins data that can change | Low | High | Pinning is explicit and opt-in per connector; the doc comment states the contract. |
| A large result cached repeatedly churns the cache | Medium | Low | ¼ cap per entry; LRU. |
| Two identical queries collecting at once | Expected | None | The last insert wins and the bytes of the replaced entry are released. |

## Validation

- `result_cache::tests::caches_pinned_results_until_the_snapshot_changes`:
  - the first query misses and the second hits;
  - the entry is reserved in the pool, and the pool's `used` equals the
    cache's `bytes`;
  - a new snapshot (`t@2`) misses and returns the new rows;
  - different SQL gets its own entry.
- `result_cache::tests::never_caches_what_could_change_under_the_same_key`:
  - `now()`, `random()`, an unpinned source (`range`) and a scalar
    subquery are never looked up or stored;
  - `SELECT 1` (a constant) is cached.
- `result_cache::tests::oversized_results_are_not_kept`: nothing is
  stored, and the pool returns to 0.
- `tests/iceberg.rs::iceberg_scans_are_pinned_to_their_snapshot`: a real
  Iceberg scan's key ends in `t@1`.
- `config::tests::result_cache_is_off_by_default_and_bounded_by_the_ceiling`
  and `metrics::tests::renders_exposition_lines`.
- Manual check on the binary with Spark's MOR table (position deletes):
  - the same aggregate twice gives the second as a hit, with the same
    answer (32 rows, sum 768);
  - a `now()` query is not counted;
  - `plimsoll_pool_used_bytes` equals `plimsoll_result_cache_bytes`
    (208).

## Revisit when

- Small-query latency is dominated by planning: a planning cache by
  `table@snapshot` (next).
- Hit rates suffer from SQL formatting differences: key on a normalized
  plan.
- Other sources gain immutable identities (Parquet files by ETag/version,
  a Postgres snapshot LSN): pin them.

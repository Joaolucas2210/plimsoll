# ADR 0020 — Concurrent metadata fetches during Iceberg planning

- Status: Accepted
- Date: 2026-10-30
- Owner: JoaoLucas2210
- Related: ADR 0010 (manifests were sequential), ADR 0017 (one request per DV), ADR 0014, ADR 0016, ADR 0018 (cache), ADR 0019 (planning latency), ADR 0012 (planning reservation)
- Scope: `plimsoll-connectors::iceberg` (`IcebergTable::data_files`), `iceberg/deletes.rs` (`positions`, `load`, `cached`)

## Context

Planning an Iceberg scan is a chain of object store round trips:

1. read the manifest list;
2. read every manifest that survives pruning;
3. read every position delete file, DV blob and equality delete file that
   applies.

Every step 2 and 3 fetch was awaited one after the other. Planning latency
was therefore `round trip × (1 + manifests + delete files)`. Object stores
serve parallel requests at the same latency as a single one.

ADR 0019's histograms made this visible: on the Spark v3 fixture (12 DVs),
planning was ~22 ms against ~8 ms of execution, on a local disk. On S3, at
10–50 ms per request, a table with a year of daily manifests plans in
seconds.

The ADR 0018 cache removes repeated delete reads. It does not help the first
query or manifests that changed.

## Decision drivers

1. Planning latency should be bounded by a few round trips, not by table
   history.
2. Memory stays under the ceiling, including bytes in flight.
3. Deterministic results and plans regardless of fetch completion order.
4. No new knob if DataFusion already has one.
5. No new dependency.

## Considered options

**A. Bounded, ordered concurrency with `futures::StreamExt::buffered`
(chosen).** Fetches run up to N at a time and results arrive in input order,
so decoding, merging and plan construction stay deterministic. Each fetched
buffer is reserved in the planning consumer before it is held. The in-flight
window is therefore accounted, and a ceiling breach fails the query, not the
node.

**B. `buffer_unordered`.** This gives slightly better tail behaviour. But
results arrive in completion order, so the file order in the plan and the
`EqualitySet` indices would vary per run, which makes `EXPLAIN` and debugging
non-deterministic. Rejected; the gain is marginal when all fetches cost the
same.

**C. Spawned tasks (`tokio::spawn` / `JoinSet`).** These would add parallel
decode on more cores. But they need `'static` futures, which means cloning
state, store handles and the reservation per task. Decoding is cheap next to
the round trip. Revisit if profiles show decode-bound planning.

**D. Coalesce DV blobs per Puffin file.** This needs fewer requests. It is
complementary and still open (ADR 0017's upgrade path).

## Decision

- **Concurrency.** It comes from DataFusion's
  `datafusion.execution.meta_fetch_concurrency` (default 32), the setting
  DataFusion uses for its own metadata fetches (listing, statistics). It is
  settable per session, and no new Plimsoll env var is added.
- **Manifests (phase 1).**
  - First the manifest list is decoded and the pruning decision is made for
    every manifest; this is cheap and needs no fetch.
  - Then the surviving manifests are fetched N at a time, then decoded and
    applied in list order. Each manifest's bytes are reserved on arrival and
    released after decoding.
- **Position deletes and DVs (phase 2).** `deletes::load` fetches and
  decodes one delete file or DV blob, through the ADR 0018 cache. Up to N run
  concurrently; results merge sequentially.
- **Equality deletes (phase 3).** Applicability and keys are computed
  first, then key files are loaded N at a time; sets keep manifest order.
- **Cache hits** are counted with an atomic, because concurrent loads share
  the counter.
- **Implementation note.** Streams iterate over indices rather than borrowed
  items. Borrowed stream items fail the `Send` check on the `async_trait`
  `scan` future, because of higher-ranked lifetime inference.

## Consequences

**Positive**
- Measured with a 50 ms-per-GET store and 8 manifests
  (`planning_fetches_manifests_concurrently`, 3 runs): planning took
  **~470 ms sequential vs ~110 ms concurrent** (4.3×). This is the
  expected 9 round trips against 2 (manifest list, then one wave).
- The same applies to delete files and DV blobs: N per round trip.
- Plans and results are identical to the sequential version, because order
  is preserved.

**Negative (accepted debt)**
- Up to N manifest or delete buffers are reserved at once, against one
  before. Planning on large tables can hit the ceiling earlier, as a query
  error (`IcebergPlanning[t]`) and not a node failure. Lowering
  `meta_fetch_concurrency` trades latency for memory.
- Decoding is still single-threaded per query (option C).
- Data files without `file_size_in_bytes` still `HEAD` sequentially in the
  scan builder. This is a fallback for non-compliant writers only.

**Operational**
- `plimsoll_query_duration_seconds{phase="planning"}` (ADR 0019) is the
  signal to watch.
- Against a rate-limited object store, `meta_fetch_concurrency` is the
  throttle.

## Risks and mitigations

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| Non-deterministic plans from completion order | Low | Medium | Ordered `buffered`; results applied in input order. |
| Memory spike from N concurrent manifests | Medium | Low (query error) | Every buffer reserved before held; the window is visible in the OOM top consumers as `IcebergPlanning[t]`. |
| Object store throttling (S3 503 SlowDown) under concurrency | Low at 32 | Medium | Retries and budgets (ADR 0013); concurrency is configurable per session. |
| A cache hit counted twice, or missed, under concurrency | Low | Low (observability only) | Atomic counter; test asserts `delete_cache_hits` exactly. |

## Validation

- `tests/iceberg.rs::planning_fetches_manifests_concurrently`: an
  `object_store::throttle::ThrottledStore` (50 ms per GET) over an
  in-memory copy of an 8-manifest table. Sequential (`concurrency=1`) is at
  least 400 ms, concurrent is under half of that, and the answer is correct.
- All existing fixture tests (pruning counts, position/equality deletes,
  cache hits, ceiling) pass unchanged. So do the interop tests against Spark
  and Iceberg Java (MOR, equality, v3 DVs).

## Revisit when

- Profiles show decoding, not fetching, dominating planning (spawned tasks).
- Puffin files hold many DVs per query (coalesce ranges).
- A target store rate-limits at the default concurrency (lower the default).

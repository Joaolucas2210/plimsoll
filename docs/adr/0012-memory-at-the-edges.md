# ADR 0012 — Memory at the edges: connectors, planning and the response

- Status: Accepted
- Date: 2026-10-30
- Owner: JoaoLucas2210
- Related: ADR 0003 (refines), ADR 0004, ADR 0006, ADR 0007, ADR 0010
- Scope: `plimsoll-connectors::postgres`, `plimsoll-connectors::iceberg`, `plimsoll_core::engine`, `plimsoll-server` (`engine.rs`, `app.rs`)

## Context

ADR 0003 bounds operator memory, but the ADR review (2026-10-30) found three
places where memory proportional to *data* still bypassed the `CeilingPool`.
Each one could OOM the node, which breaks the project's only promise:

| Edge | Before | Grows with |
|---|---|---|
| Postgres scan | `client.query()` buffered the whole result, then built one `RecordBatch`; nothing reserved | result rows |
| HTTP response | the engine dropped the `Output` reservation on return; the server then built Arrow JSON objects, then `Vec<Vec<Value>>`, then the response body (3 copies, none reserved) | result size × ~3–10 |
| Iceberg planning | every manifest decoded into a `Vec<Avro>`; only the raw bytes were reserved, and only during decoding | manifest entries |

## Decision drivers

1. Anything that grows with data must be reserved *before or as* it grows.
2. Unaccounted memory is acceptable only when bounded by a constant
   (one batch, one entry), never by the input.
3. No API breaks for clients; no new dependencies.
4. Each claim provable by a test that fails if the accounting regresses.

## Considered options

**A. Per-edge streaming + reservation hand-off (chosen).** Stream where the
source allows it; where data must be materialized, keep the reservation
attached to it until it is released.

**B. Global allocator accounting** (a counting `#[global_allocator]`). Catches
everything, but it cannot fail *a query* (the allocator only knows bytes, not
owners) and adds a cost on every allocation in the process. Useful as a
watchdog, not as the ceiling.

**C. Streaming HTTP responses (NDJSON / chunked rows) now.** Removes the
materialized body entirely, but changes the API contract (the status code is
sent before the result is known to succeed). Deferred; option A keeps the
contract and is a prerequisite anyway.

## Decision

- **Postgres:** `query_raw` streams rows; they are cut into `batch_size`
  chunks, each turned into a `RecordBatch`, under an
  `AccountedExec("PostgresScan[<relation>]")`. Unaccounted: at most one chunk
  of decoded rows. The emission type is now `Incremental`.
- **Engine → server hand-off:** `Engine::sql` returns
  `QueryResult { batches, memory: Reservation }`; the `Output` reservation
  lives as long as the result.
- **Serialization:** `CoreEngine` converts batch by batch. After each batch it
  `shrink`s the batch's Arrow size and `try_grow`s the JSON bytes it produced,
  so the reservation equals the bytes held (the test asserts exact equality
  at the end). A response whose JSON crosses the ceiling fails with
  `MemoryLimitExceeded` (HTTP 422) and frees everything. Unaccounted: one
  batch's transient JSON objects.
- **Response body:** sent as three chunks (head, rows, `}`) with no further
  copy; the reservation is dropped by the body stream after hyper takes the
  last chunk.
- **Iceberg planning:** raw Avro bytes stay reserved while their manifest is in
  use; entries are decoded lazily, one at a time; every kept data file
  (`path` + struct) is reserved while the file list is built.

## Consequences

**Positive**
- The three known data-proportional bypasses are closed; their failure mode is
  now a typed, per-query error.
- Fewer copies on the response path (3 → 1 materialized JSON body).
- Postgres results start flowing before the full result exists.

**Negative (accepted debt)**
- Bounded, unaccounted windows remain: one Postgres chunk of rows, one
  batch's JSON objects, one decoded manifest entry.
- The planned file list is reserved only during planning; afterwards it lives
  in the plan (O(files) paths) unaccounted.
- `metadata.json` / REST `loadTable` bodies are parsed outside the pool (no
  session at that point); bounded by metadata size, which Iceberg caps with
  `write.metadata.previous-versions-max`.
- A buffered batch can still be counted twice for an instant (ADR 0003).

**Operational**
- New consumer names in OOM messages: `PostgresScan[...]`; the response JSON
  appears under `query-output`.
- Clients may now see 422 for results that fit as Arrow but not as JSON.

## Risks and mitigations

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| A new connector or edge reintroduces an unreserved buffer | Medium | High | Rule (drivers 1–2) stated here; each edge has a test asserting failure under a small ceiling and `used() == 0` after. |
| Dropping a Postgres stream early leaks the connection | Low | Low | The client is owned by the stream; dropping the stream drops both. |
| Exact JSON accounting drifts (e.g. framing bytes) | Low | Low | Test asserts reservation == `rows_json.len()`. |

## Validation

- `postgres::tests::pg_streams_in_accounted_batches` (live, CI): 20k rows
  arrive in ≥20 batches; under a 32 KiB ceiling the query fails naming
  `PostgresScan`; `used() == 0` in both cases.
- `engine::tests::serialized_response_is_accounted` (server): reservation equals
  the JSON bytes exactly; a result of `NULL` columns (≈0 B as Arrow, ≈4.4 MB as
  JSON) fails under 4 MiB and frees the pool.
- `engine::tests::runs_sql_and_releases_memory` (core): `output_used()` equals
  the result's reservation while held, 0 after drop.
- Iceberg interop and pruning tests unchanged and green on the streamed
  manifest decoder.

## Revisit when

- Results are large enough that a materialized body is the bottleneck:
  stream rows (option C) and shrink per chunk sent.
- A connector cannot stream (then it must reserve before buffering).
- A metadata-heavy catalog shows `loadTable` parsing in memory profiles.

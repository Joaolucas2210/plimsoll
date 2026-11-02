# ADR 0035 — Streamed results (NDJSON)

- Status: Accepted
- Date: 2026-11-02
- Owner: JoaoLucas2210
- Related: ADR 0003 (ceiling), ADR 0012 (memory at the edges; response JSON accounted), ADR 0013 (error codes), ADR 0027/0031 (query log, traces), ADR 0034 (cancellation)
- Scope: `plimsoll_core::engine::{Engine::sql_stream_with, QueryStream}`, `plimsoll-server::engine::{QueryEngine::stream, Chunk, Streamed}`, `app::{stream_sql, Receiving, StreamGuard, Deferred}`, `plimsoll_control::governance` (cancel `requested` flag)

## Context

`/v1/sql` returns one JSON document. ADR 0012 made it safe: batches and the
serialized body are reserved in the pool until hyper has taken the last
byte. Safe, but bounded. A result larger than the free pool fails with
`memory_limit_exceeded`, however small each row is. With the default 1 GiB
ceiling and its 1/8 output reserve, that is around 100 MB of JSON.

Extracts, notebooks pulling a sample of a few million rows, and
`SELECT * FROM silver.x` exports from a client are normal workloads. The
answer cannot be "ask for less". The ceiling must bound the *window*,
not the *result*.

## Decision drivers

1. Memory O(chunks in flight), independent of result size, and still
   under the ceiling (ADR 0003).
2. **Back-pressure.** A slow client slows the scan. It never buffers the
   result.
3. **Unchanged guarantees for the whole stream:**
   - the admission slot is held;
   - the deadline covers it;
   - cancellation works (ADR 0034);
   - one log line per query, with the real outcome (ADR 0027).
4. Errors that happen before any row keep their HTTP status and error body
   (ADR 0013). Clients must not parse a 200 to discover a 403.
5. A client that disconnects must release the query at once, not at the
   deadline.
6. No new dependency, and the existing `json` format is untouched.

## Considered options

**A. NDJSON over a chunked 200, produced by the governed query in its own
task (chosen).**
- `stream_sql` spawns a task running `Admission::execute` around
  `QueryEngine::stream`.
- The engine sends `Chunk::Header(columns)`, then `Chunk::Rows(bytes,
  reservation)` through a **2-slot** `mpsc` channel.
- The handler awaits the first message:
  - a header becomes a streaming 200;
  - a channel closed without a header means the query failed early, and
    the handler answers with the normal error response.
- Admission is untouched. The governed future simply lives until the last
  chunk is sent (driver 3).

**B. Arrow IPC stream (`application/vnd.apache.arrow.stream`).** It is
compact and typed, and it is the right answer for analytical clients. But
it needs a client library to read, and both curl and BI connectors speak
JSON. Arrow IPC is the next format to add on the same `Chunk` path.

**C. Server-side cursors (`/v1/statement/{id}?page=n`, Trino-style).** They
survive client reconnects, but keep server state between requests and
need paging, expiry and resumable plans. That is overkill until clients
need resumption.

**D. Raise the output reserve.** It moves the cliff instead of removing it.

## Decision

- **Request.** `{"format": "ndjson"}`. The default is `json`; any other
  value is `400 invalid_request`.
- **Wire format** (`application/x-ndjson`):

  ```text
  {"columns":["value","pad"],"query_id":2}
  [0,"xxx…"]
  …
  {"done":true,"rows":2000000}      ← or {"error":{"code",…,"query_id"}}
  ```

  Keys are alphabetical, as everywhere in the API (ADR 0027).
- **Core.** `Engine::sql_stream_with` returns a `QueryStream`: the
  DataFusion stream and the planning time. Planning, grants (ADR 0025)
  and read-only checks (ADR 0024) are shared with the collecting path,
  which is now built on the same stream. Dropping the `QueryStream` counts
  spill.
- **Chunks.** Batches are cut into zero-copy slices of 1024 rows, and each
  slice is serialized to NDJSON.
  - Each chunk is reserved in the pool (`query-output`, Output kind) until
    the body hands it to hyper.
  - The window is therefore about 4 chunks: 2 in the channel, one being
    written and one being built.
- **Back-pressure.** `tx.send` awaits when the channel is full, so the
  DataFusion stream is not polled and scans stop.
- **Errors.**
  - Before the header (admission, planning, grants, invalid SQL), the
    response is the usual status and error body.
  - After it, the body ends with an `{"error": …}` line, and the error is
    counted in `plimsoll_query_errors_total`.
- **Disconnect.** When the body is dropped before its end, `Receiving`
  (the body's end of the channel) cancels the query in the registry
  *before* the receiver closes. Then `StreamGuard` reports
  `client_disconnected` for the log and metrics.
  - The governed query stops; its slot and every reserved chunk are freed.
  - Because the engine can observe the closed channel in the same instant,
    possibly on another worker thread, the registry keeps a `requested`
    flag. A run that fails after a cancel was requested is recorded as
    `Cancelled`, not `Failed`.
- **Logs and traces.** The response carries a `Deferred` extension. The
  `sql` wrapper then emits the `query` line and spans when the stream ends,
  with the final code, the phases and the total. `execution_ms` includes
  waiting for the reader.

## Consequences

**Positive**
- Measured on the binary with a 32 MiB ceiling, for 2 000 000 rows of
  about 110 bytes:
  - `json` fails with `memory_limit_exceeded`;
  - `ndjson` streams all 225 MB, and the pool returns to 0.
- Cutting the connection after 2 lines records the query as `cancelled`
  (5 of 5) and frees the slot and pool immediately.
- One query path in core: collecting is a consumer of the stream.

**Negative (accepted)**
- **The HTTP status is 200 once rows flow.** A mid-stream failure is
  reported in the last line, so clients must check for the `done` line.
  This is the standard trade-off of streaming APIs.
- **Slow readers hold an admission slot,** up to the query deadline. That
  is intended (driver 2), but a stalled client holds capacity, and the
  deadline is the bound.
- **Chunks are fixed at 1024 rows.** Very wide rows give large chunks;
  sizing by bytes is a revisit item.
- **The batch being sliced is not reserved,** the same as the `json`
  path's transient objects (ADR 0012).
- **No resumption.** A dropped connection means re-running the query.

## Risks and mitigations

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| Client treats 200 as success without the `done` line | Medium | Medium | Documented; the `done` line carries the row count to verify. |
| Stalled readers exhaust slots | Low | Medium | The deadline cancels; per-user running limits; operators can cancel (ADR 0034). |
| Disconnect recorded as failure (race) | — | Low | `requested` flag; tested in governance and on the binary. |

## Validation

- `engine::tests::streamed_results_are_bounded_by_the_window_not_the_result`:
  - under a 1 MiB ceiling, a 200 000-row query fails as `json` but streams
    every row, with a header;
  - every chunk is reserved, and its size stays below the ceiling;
  - the pool returns to 0;
  - a planning error arrives before any chunk.
- `app::tests::ndjson_streams_rows_and_keeps_early_error_statuses`:
  - the exact lines are header, row and done;
  - `oom` gives 422 and invalid SQL gives 400, with no 200;
  - an unknown format gives 400.
- `app::tests::ndjson_client_disconnect_cancels_the_query`, with the real
  engine, a 64 MiB pool and 100 M rows:
  - read the header and a chunk, then drop the body;
  - the query becomes `Cancelled` and the slot is freed;
  - the pool returns to 0 and `client_disconnected` is counted;
  - 8 of 8 repeated runs passed.
- `governance::tests::a_run_failing_after_a_cancel_request_counts_as_cancelled`.
- The binary smoke described under Consequences.

## Revisit when

- Analytical clients need typed columnar transfer: done in ADR 0037, Arrow
  IPC on the same `Chunk` path.
- Clients need resumable results: server-side cursors.
- Wide rows make 1024-row chunks too large: size chunks by bytes.

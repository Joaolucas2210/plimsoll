# ADR 0037 — Arrow IPC results

- Status: Accepted
- Date: 2026-11-02
- Owner: JoaoLucas2210
- Related: ADR 0035 (streamed results; this is its "typed columnar transfer" revisit item), ADR 0013 (error codes), ADR 0012 (memory at the edges)
- Scope: `plimsoll-server::engine::{Format, QueryEngine::stream}`, `app::stream_sql`

## Context

NDJSON (ADR 0035) made results unbounded, but it is still JSON:

- **Types are lost.** `DOUBLE`, `DECIMAL`, timestamps and int64 beyond
  2⁵³ all become JSON numbers or strings, and the client re-infers them.
- **It is CPU-bound.** Every value is formatted row by row, through Arrow's
  JSON writer into `serde_json` values.
- **It is large.** Column names are absent, but every value is text.

Plimsoll's compute plane is Arrow end to end. The analytical clients that
pull large results (pyarrow, polars, pandas through pyarrow, DuckDB, Spark)
all read Arrow IPC natively and zero-copy. Converting columnar batches to
JSON, only for the client to parse them back into columns, is wasted work
on both ends.

## Decision drivers

1. Same guarantees as NDJSON:
   - bounded memory in flight;
   - back-pressure;
   - admission, deadline and cancellation;
   - early errors with their HTTP status;
   - disconnect cancels the query.
2. **No silent truncation.** A query that fails mid-stream must never look
   like a complete, shorter result to the reader.
3. Readable with the stock Arrow reader, with no Plimsoll client library.
4. No new dependency: `arrow-ipc` is already in DataFusion's tree.

## Considered options

**A. Arrow IPC *stream* format on the NDJSON chunk path (chosen).** The
engine's `stream` takes a `Format`. For `Arrow`:

- an Arrow `StreamWriter` writes into a `Vec`;
- each 1024-row slice is written, and the buffer is taken as one chunk;
- the schema message leaves with the first chunk;
- `finish()` writes the end-of-stream marker as the last chunk.

Everything else is the ADR 0035 path, unchanged: reservations, the
channel, the governed task and the disconnect guard.

**B. Arrow IPC *file* format.** Its footer, with random access, is written
at the end, so the body cannot start until the result is complete.
Rejected: it would undo ADR 0035.

**C. Arrow Flight / Flight SQL.** This is the standard for Arrow over the
network, with gRPC, authentication handshakes and prepared statements.
It needs tonic, prost and a second listener, which is a heavy dependency
set. Revisit when a client needs Flight specifically (for example a JDBC
driver). Plain HTTP covers pyarrow, polars and DuckDB today.

## Decision

- **Request.** `{"format": "arrow"}`. The response is
  `application/vnd.apache.arrow.stream`, and the query id is in
  `x-plimsoll-query-id`. NDJSON responses carry the same header too.
- **Body.** Schema message, record batches of up to 1024 rows, then the
  end-of-stream marker. An empty result is still a complete stream: schema
  plus marker.
- **Errors.**
  - Before the first chunk (admission, planning, grants), the usual status
    and JSON error body (ADR 0013).
  - After it, the body stream yields an I/O error, so hyper aborts the
    response. The client never receives the end-of-stream marker, and Arrow
    readers raise on the truncated stream (driver 2). The code and message
    are in the query log line, the metrics and `system.runtime.queries`.
- **The fallback engine** (the trait's default `stream`, used by test
  fakes) refuses `Arrow` as `Unsupported`. Only `CoreEngine` produces
  Arrow.

## Consequences

**Positive**
- Measured on the debug binary with a 32 MiB ceiling, for 2 000 000 rows ×
  (int64, string, double, bool):
  - pyarrow reads all of it in **0.8 s**, against 29 s for the same query
    as NDJSON;
  - all types are intact, and polars aggregates the table directly;
  - the pool returns to 0.
- Notebooks and ETL tools get typed data with one line of client code:
  `pa.ipc.open_stream(response)`.
- One streaming path, two encoders. Future encoders, such as CSV, slot in
  the same way.

**Negative (accepted)**
- **No error message on the wire after the first batch.** The client only
  sees an aborted transfer. The cause is in logs and in
  `system.runtime.queries` (by `x-plimsoll-query-id`). Arrow IPC has no
  error message type, and HTTP trailers are poorly supported by clients.
- **Batches are fixed at 1024 rows,** as with NDJSON chunks. That is a bit
  small for columnar efficiency. Larger batches mean larger in-flight
  windows; sizing chunks by bytes is ADR 0035's revisit item.
- **No compression.** IPC buffer compression (LZ4/ZSTD) needs extra arrow
  features. HTTP-level compression is a proxy's job for now.

## Risks and mitigations

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| Client treats a cut stream as complete | Low | High | No end-of-stream marker on failure; stock readers raise (tested: the body ends in an error, not EOF). |
| Schema types a client cannot read (e.g. Utf8View) | Low | Medium | Batches carry DataFusion's types as planned; verified with pyarrow and polars on common types. Cast in SQL if a reader lags. |
| Larger memory per chunk than NDJSON for wide types | Low | Low | Chunks reserved in the pool like NDJSON's; the ceiling still bounds them. |

## Validation

- `engine::tests::arrow_stream_decodes_and_is_bounded`:
  - under a 1 MiB ceiling, 200 000 rows stream, and the concatenated
    chunks decode with `StreamReader` to the same row count and schema;
  - an empty result decodes as schema plus 0 batches;
  - the pool returns to 0.
- `app::tests::arrow_results_stream_and_fail_loudly`:
  - checks the content type and `x-plimsoll-query-id`, and that the body
    decodes to 5000 rows;
  - invalid SQL gives 400 `invalid_query`, not a 200;
  - a division by zero in the second batch aborts the body after bytes
    were sent;
  - the slot is freed and the pool returns to 0.
- Manual check with pyarrow and polars against the binary (numbers
  above).

## Revisit when

- A client needs Flight or Flight SQL (JDBC/ODBC through Arrow drivers).
- Transfer size matters: IPC LZ4/ZSTD compression, negotiated by request.
- Columnar efficiency needs bigger batches: byte-sized chunks.

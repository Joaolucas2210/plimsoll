# ADR 0060 — Coordinator/worker step 3: balanced, resilient fragments

- Status: Accepted
- Date: 2026-11-02
- Owner: JoaoLucas2210
- Related: ADR 0058 (fragments), ADR 0059 (governance and health), ADR 0037 (Arrow IPC results)
- Scope: `plimsoll_connectors::remote::{RemoteScanExec::{new, assignments}, with_retry, WorkerError, decode, END_OF_STREAM}`

## Context

Step 1 split files round-robin by count, and any fragment failure failed
the query (ADR 0058, 0059). Both are poor fits for real tables:

- **Skew.** File sizes vary by orders of magnitude, between compacted
  files and streaming micro-files. By count, one worker can get a 1 GiB
  file *and* its share of the rest, and the query waits for it.
- **Transient failures.** A worker answering 429 because its
  `fragments` group is full (ADR 0059), a restart, or a dropped
  connection fail the whole query, though another worker could have
  served the fragment.

While building the retry, a correctness gap turned up. Arrow's
`StreamDecoder::finish()` accepts a stream that ends **cleanly at a
message boundary without its end-of-stream marker**. A body cut there,
for example by a proxy that closes cleanly, would have been taken as
complete, with rows missing.

## Decision drivers

1. **No duplicate rows.** A retry is safe only if nothing from the failed
   attempt was emitted.
2. **No retry storms.** Retry once, and only on errors another worker can
   plausibly avoid.
3. **Completeness must be proven, not assumed.**

## Decision

- **Balance by bytes.** Files carry their manifest `file_size_in_bytes`.
  `RemoteScanExec::new` sorts them largest first (ties by path, so the
  order is deterministic) and gives each to the currently least-loaded
  worker. This is the classic LPT heuristic, within 4/3 of the optimal
  makespan. Unknown sizes count as one byte. `EXPLAIN` shows `bytes=[…]`
  per fragment.
- **One retry before the first batch.** Each partition's targets are its
  owner and the next healthy worker. `with_retry` runs the fragment on
  the owner; if the stream fails **before any batch was emitted** with a
  retryable error, it runs once more on the second target.
  - Retryable errors are connection failures, 429, 5xx, transport errors
    and truncated streams.
  - 400, 401 and a spent deadline are not retryable: another worker
    would refuse the same way.
  - After the first batch, any failure fails the query.
  - The final error names both attempts.
- **The end-of-stream marker is required.** The decoder keeps the last 8
  bytes of the body. At the end, the stream is complete only if
  `finish()` succeeds *and* those bytes are Arrow's marker
  (`FF FF FF FF 00 00 00 00`). Otherwise the error is "worker stream
  ended without its end-of-stream marker", which is retryable if nothing
  was emitted.

## Consequences

**Positive**
- Large files no longer pile up on one worker.
- A full worker (429) or a restart costs one extra request instead of the
  query.
- A truncated worker response can no longer pass as a short, valid
  result, whatever the transport does.

**Negative (accepted)**
- **One retry only, and only to the next healthy worker.** There is no
  backoff, since the deadline (ADR 0059) bounds everything.
- **A failure after the first batch still fails the query.** Resuming
  mid-fragment would need row-level offsets.
- **LPT balances bytes, not decode cost.** Wide versus narrow schemas,
  and deletes, are not modelled.

## Validation

- `remote::tests::fragments_balance_by_bytes`: with files of 100, 90, 1
  and 1 MiB plus one of unknown size, the 100 MiB file sits alone and the
  other four go to the other worker. Round-robin would have paired 100
  with two more.
- `remote::tests::a_stream_without_its_end_marker_is_an_error`:
  - a body cut cleanly after a batch decodes the batch, then ends with
    the marker error;
  - the same body plus the marker, split across chunks, is complete.
- `tests/distributed.rs::fragments_retry_once_before_the_first_batch`,
  with fake workers:

  | First worker answers | Second worker called | Outcome |
  |---|---|---|
  | 503 or 429 | yes | 1 row |
  | 401 | no | 401 error |
  | a delivered batch, then death | no | error, so a retry cannot duplicate |
  | normally | no | 1 row |

- Clients of ADR 0037 still see a failure on a mid-stream error over real
  HTTP, because the server aborts the chunked body: `curl` exits 18
  (partial transfer), and pyarrow over `urllib` raises `IncompleteRead`.

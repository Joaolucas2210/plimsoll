# ADR 0121 — Workers' spill summed on the coordinator

- Status: Accepted
- Date: 2026-11-04
- Owner: JoaoLucas2210
- Related: ADR 0118 (per-query spill), ADR 0120 (shuffle maps by query), ADR 0037 (Arrow IPC streams), ADR 0060/0072 (retries)
- Scope: `TRAILER_HEADER`, `SPILL_TRAILER`, `spill_trailer`, `split_trailer`, `QueryTag::spill`, `SpillSink::add`, `Running::credit`, the fragment handler's trailer, `settle_spill`

## Context

ADR 0118 put a query's spill in its log line and in
`system.runtime.queries`, but only the coordinator's own. A distributed
query does most of its work on workers: scans, partial aggregates,
shuffle maps and reduces. What those spilled was logged per fragment,
and had to be summed by query id in a log tool. The query's own row
said 0 even when its workers had written gigabytes to disk.

A worker knows a fragment's spill only once the fragment's plan has
run, at the end of its stream. The stream is Arrow IPC (ADR 0037),
which carries no room for metadata after its last batch, and the
client does not read HTTP trailers.

## Decision drivers

1. **Exact and in band**: the number travels with the rows it describes,
   with no extra request and no race against the query's end.
2. **Compatible both ways**: an older coordinator must never receive
   bytes it cannot decode, and an older worker must still be readable.
3. **Each byte counted once**, including a shuffle map's spill, which
   no single request owns (ADR 0120).

## Decision

- **Asked for, then appended.**
  - A coordinator (or a reducing worker) asks for the spill with the
    request header `x-plimsoll-trailer: spill`.
  - A worker so asked ends the stream, after the end-of-stream marker,
    with 16 bytes: `PLMSPILL`, then the spill as a little-endian u64.
  - Without the header, the stream is plain IPC, as before.
- **Taken off before decoding.** `split_trailer` holds the body's last
  16 bytes back until it ends.
  - A trailer is removed and added to the query's `SpillSink`, carried
    by `QueryTag::spill`, which already travels as a session extension
    to every remote exec.
  - Anything else (an older worker) goes on as IPC.
  - On a failure, the held bytes go out before the error. A batch they
    complete is emitted as before, so whether a fragment may be retried
    (ADR 0060, 0072) is unchanged.
- **What a fragment reports** is the sum of:
  - its own plan's spill;
  - a credit: a shuffle map's whole spill (bucket files and operators,
    ADR 0120), credited once, on the first bucket handed out, whichever
    reducer or size request gets it (`Running::credit`);
  - for a reduce, what its maps reported with their buckets. The reduce
    asks its peers for trailers too, into its own sink.
- **The coordinator** adds every trailer to the query's sink, which its
  own plan's spill joins when its stream drops (ADR 0118).
  `settle_spill` now logs and records the sink's total for every query
  that ran.

## Consequences

**Positive**
- A query's row in `system.runtime.queries` and its log line show its
  whole spill, wherever it happened.
- No new request, no new state on workers, nothing to clean up.

**Negative (accepted debt)**
- **A failed fragment reports nothing**: its stream breaks before the
  trailer. Its spill is still in its worker's `fragment` and
  `shuffle_map` lines.
- **The last 16 bytes of every fragment arrive one chunk late.** They
  are released with the next chunk or at the end, which is invisible
  next to batch sizes.
- **Maps run for a retried reducer** (ADR 0072) credit their spill on
  their own first bucket. That spill is real work for the query, so it
  is counted.

## Validation

- `remote::tests::spill_trailers_are_split_off_wherever_chunks_end`:
  for chunk sizes 1, 3, 16, 17 and 100:
  - a trailer is removed and its count added, and the IPC bytes come
    through intact;
  - a body without one passes whole.
- `tests/distributed.rs::fragments_carry_the_query_identity`: a fake
  worker sees `x-plimsoll-trailer: spill` and appends a trailer, which
  reaches the query's sink (4096).
- `tests/distributed.rs::fragments_retry_once_before_the_first_batch`:
  a worker that dies after its first batch is still not retried. The
  first version of `split_trailer` held those bytes back and retried;
  that was found here and fixed.
- By hand:
  1. a coordinator and two workers with 16 MiB each and spill
     directories;
  2. a two-file, 800,000-row table written through the REST catalog;
  3. an aggregation into 300,000 groups, shuffled.
- The two shuffle maps spilled 566,544 and 554,544 bytes. The
  coordinator's `query` line reported 1,121,088, their sum, through the
  maps' credits, the reduces' trailers, and the coordinator's sink.

## Revisit when

- Failed fragments' spill matters on the coordinator: report it in the
  error body.

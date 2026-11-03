# ADR 0068 — Streaming shuffle reducers

- Status: Accepted
- Date: 2026-11-03
- Owner: JoaoLucas2210
- Related: ADR 0064/0065 (reduces), ADR 0067 (map-side spill), ADR 0060 (end-of-stream marker)
- Scope: `remote::{open_bucket, with_schema, conform, decode}`, `engine::{open_side, Bucketed}`, `CoreEngine::reduce`

## Context

After ADR 0067, shuffle maps account and spill their buckets, but
reducers still did the opposite:

1. read every fetched bucket whole into memory;
2. reserve that size in the pool (failing the query when refused);
3. only then hand the batches to the merge or join as a `MemTable`.

So the reducer's peak memory was its entire share of the data plus the
operator state built from it. Its own buckets could also never spill,
even where the operator reading them (DataFusion's aggregation) can.

## Decision drivers

1. **Hold only what the operator holds.** Rows should flow from the
   network into the merge or join, and be accounted by those operators.
2. **Keep the integrity check.** A bucket cut short must fail the query,
   never shrink the answer.
3. **No new transport.** The same HTTP body as before, the same Arrow
   IPC stream.

## Decision

- **Open, do not fetch.** `open_bucket` sends the bucket request and
  returns as soon as the body's first IPC message (the schema) has
  arrived, together with a stream of the bucket's batches.
  - `with_schema` decodes only up to that message. It then replays those
    bytes in front of the rest of the body into the incremental
    `decode`, which requires the end-of-stream marker (ADR 0060).
  - All of a reduce side's buckets are opened concurrently, so every map
    starts serving at once.
- **A streaming table.**
  - Each reduce side is a `StreamingTable`, with one partition per map.
    Each partition is a `Bucketed` that hands out its stream once; a
    second read is an error.
  - The side's schema is the first map's. Every other map's batches are
    `conform`ed to it: names and metadata relabelled, types cast. This is
    the same step the coordinator's decoder already applied, now
    factored out.
- **Operators own the memory.** The merge (`GROUP BY` with
  `sum`/`min`/`max`) and the join read the table like any source.
  - Aggregation state is accounted and spills under the worker's pool
    (ADR 0003).
  - The join's build side is accounted by the hash join.
  - The `shuffle-reduce` reservation is gone, because nothing is
    buffered outside an operator any more.

## Consequences

**Positive**
- A reducer's memory is its operator's state, not its whole share of
  the shuffled data.
  - Merges of high-cardinality aggregations can spill.
  - Joins hold only their build side.
- Maps stream their buckets out as the reducer consumes them.
  Backpressure flows over HTTP, and a spilled bucket (ADR 0067) is read
  from disk at the pace it is consumed.
- A truncated or failing bucket still fails the query: it reaches the
  operator as a stream error.

**Negative (accepted debt)**
- **Join build sides do not spill.** That is DataFusion's hash join
  limitation; a refused build fails the query, as it would on a single
  node.
- **Shuffle spill files are not counted** in spill metrics or
  temp-directory limits (ADR 0067).
- **A slow map stalls its reducer.** That is inherent to pulling; there
  is no speculative re-fetch, and no retry (ADR 0064).

## Validation

- `remote::tests::bucket_streams_yield_their_schema_first`. With an IPC
  stream split into 7-byte chunks, so the schema message spans several:
  - the schema arrives first, and both batches follow intact;
  - an empty bucket yields its schema and no batches;
  - a stream cut before its end-of-stream marker ends in an error;
  - an empty body is an error.
- `distributed_scans_match_local_ones` (live, Spark-written MOR, EQ and
  DV tables, two workers): every aggregation and join shuffle returns
  the single-node answers through streaming reducers.

## Revisit when

- Shuffle disk use needs limits or visibility: register shuffle spill
  files with the temp-directory accounting and `spilled_bytes`.
- Join build sides outgrow worker pools: route such joins to more
  partitions (more reducers per worker), or wait for spilling hash joins
  upstream.

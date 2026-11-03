# ADR 0064 — Aggregation shuffle between workers

- Status: Accepted
- Date: 2026-11-02
- Owner: JoaoLucas2210
- Related: ADR 0058–0063 (fragments, governance, balance, partial aggregation, filters, expressions), ADR 0002 (distribution plan)
- Scope: `remote::{Task, Reduce, Bucket, fetch_bucket, RemoteScanExec::with_shuffle, shuffle, shuffle_final}`, `CoreEngine::{map_bucket, reduce}`, `/v1/fragment`

## Context

With partial aggregation (ADR 0061), each of W workers returns one row
per group it saw, and the coordinator merges them in its `Final`
aggregate. That is fine for `GROUP BY region`. It does not scale for
`GROUP BY user_id`:

- the coordinator receives up to W × G rows;
- it holds a hash table of all G groups, under its own memory ceiling;
- its CPU does every merge.

Adding workers makes the coordinator the bottleneck.

The textbook fix is a shuffle. Workers hash-partition their partial
states by group key, worker j merges bucket j from everyone, and each
group is finalized on exactly one worker.

## Decision drivers

1. **Same answers.** Every group must be merged in exactly one place.
   The plan above the replaced node must not depend on its partitioning.
2. **Reuse what is there.** The same endpoint, auth, deadlines, tracing,
   end-of-stream checks and memory pool. No new dependency and no plan
   serialization.
3. **No admission deadlock.** Reduces wait on buckets from their peers,
   so the wait must not consume the slots those peers need.

## Considered options

**A. Pull-based shuffle with buckets kept on map workers (chosen).**
- The coordinator sends one **reduce** per fragment owner.
- Reducer j asks every map worker i for bucket j of its fragment.
- The first bucket request for map i runs the map once:
  1. scan, filter and partial aggregate (ADR 0061–0063);
  2. hash-repartition into W buckets;
  3. keep them in memory, accounted in the pool.
- Each bucket is handed out once; when all are taken, the map is dropped.

No coordinator round trip between the phases, and no push protocol.

**B. Coordinator-orchestrated two phases** (run all maps, then all
reduces). This needs an extra wait barrier and a second set of requests.
It buys nothing over A, where the first bucket request is the barrier.

**C. Ballista or Arrow Flight shuffle.** These bring a new runtime and
gRPC (ADR 0004), and are far more general than aggregation needs today.

## Decision

- **When (coordinator, physical rule).** A `Final`/`FinalPartitioned`
  aggregate is replaced by a shuffle when all of these hold:
  - it has group keys;
  - its input, through repartition, coalesce and coalesce-batches only,
    is a partial aggregation that ADR 0061/0063 would push;
  - every aggregate has exactly one state, so the merged state is the
    final value (`count`/`sum`/`min`/`max`);
  - there are at least two fragments;
  - nothing above relies on its partitioning.

  "Nothing relies on it" is decided top-down:
  - the root is free;
  - the children of `CoalescePartitionsExec`/`SortPreservingMergeExec`
    are free;
  - projection, sort (including TopK), filter, coalesce-batches and
    limits pass freedom down;
  - everything else blocks it (joins, unions, windows, repartitions,
    aggregates).

  Under a partitioned hash join, the aggregate keeps the ADR 0061 plan;
  the live test checks this. A `Final` that had one partition is wrapped
  in `CoalescePartitionsExec`, so the partition count is unchanged.
- **Shape.** `AccountedExec("RemoteShuffle")` over a `RemoteScanExec`
  marked `shuffled`, with one partition per fragment. Partition j posts
  `{"reduce": {shuffle, partition: j, maps: [(url, fragment)…]}}` to the
  owner of fragment j. Reduces are **not retried**: a retry would find
  its buckets already taken.
- **Map (worker).**
  - It is a fragment with `bucket: {shuffle, map, partitions, index}`.
  - The first request per (shuffle, map) runs the aggregated fragment
    under a `tokio::sync::OnceCell`. Concurrent requesters wait; a
    failure is cached for all of them.
  - The hash repartition is applied after DataFusion's optimizer, as a
    `RepartitionExec`, so the bucket count is exactly W. The first try
    (`DataFrame::repartition`) came back with 12 partitions.
  - DataFusion's hash repartitioning is seeded with a constant, so every
    worker of the same version sends a group to the same bucket.
  - Buckets are reserved in the pool (`shuffle-map`) and released as
    they are taken.
- **Reduce (worker).** It fetches its bucket from every map concurrently
  (`fetch_bucket`: the whole body, the end-of-stream marker required) and
  reserves them (`shuffle-reduce`). It then merges: `sum` of counts and of
  sums, `min` of mins, `max` of maxes, grouped by the key columns. It
  streams the result back, and the coordinator's decoder casts it to the
  `Final` schema.
- **Peers.**
  - Reducers call map workers at the URLs the coordinator uses, with the
    worker token they were called with, so no new secret is needed.
  - They forward the query id, user, traceparent and remaining deadline.
  - Bucket requests skip admission, because they serve a reduce that
    already holds a slot. Admitting them could deadlock: every slot held
    by reduces waiting on their own buckets. Reduces are admitted as
    fragments are (ADR 0059).
- **Abandoned maps** (a reduce failed before taking its bucket) expire
  after 5 minutes. Expired maps are swept on the next shuffle request.

## Consequences

**Positive**
- High-cardinality `GROUP BY` scales with workers.
  - Each group crosses the network to the coordinator once, already
    final.
  - The coordinator holds no aggregation state for it.
  - Merge CPU is spread across workers.
- `HAVING`, `ORDER BY … LIMIT` and projections above the aggregate still
  run on the coordinator, over final rows.
- Everything is reused: `/v1/fragment` (now a `Task`), the IPC streams,
  the end-of-stream marker, tracing and logs (`fragment` lines show maps
  and reduces), the memory pool, deadlines.

**Negative (accepted debt)**
- **No retry.** A worker lost mid-shuffle fails the query. Scans and
  partial aggregations without a shuffle still retry once (ADR 0060).
- **Buckets are kept in memory, without spilling.** A map larger than
  the worker's pool fails the query (`shuffle-map` reservation). The
  reducer buffers its whole bucket before merging.
- **Fixed 5-minute TTL** for abandoned maps, not the query deadline. The
  memory is still accounted, so it fails later queries rather than the
  process.
- **A shuffle always runs** for grouped aggregations over two or more
  fragments, even with few groups. That costs one extra hop: worker to
  worker, a few milliseconds on a LAN. There are no cardinality
  statistics to decide better.
- **Joins are not shuffled.** Their build and probe sides would need a
  join plan shipped to workers. A grouped aggregate under a partitioned
  join stays as in ADR 0061.
- **Workers must reach each other** at the URLs the coordinator uses.

## Validation

- `remote::tests::fragments_round_trip_as_json`: fragments with buckets
  and reduces round-trip as JSON (`Task`); a reduce partition outside its
  maps is refused.
- `distributed_scans_match_local_ones` (live, Spark-written MOR, EQ and
  DV tables, two workers):
  - every grouped, pushed aggregation over two or more fragments is
    `shuffled`, with answers equal to the single-node ones;
  - covered: column, expression and nullable-count keys, `GROUP BY id`
    (one group per row), and `HAVING … ORDER BY … LIMIT`;
  - `count(DISTINCT)` and an aggregate under a partitioned hash join are
    not shuffled, and still give the same answers.
- The existing governance, identity and retry tests are unchanged and
  pass. Scan fragments are still admitted and retried.

## Revisit when

- Maps outgrow worker memory: spill buckets (Arrow IPC files on local
  disk) and stream them.
- Joins dominate distributed work: shuffle both join sides by key, with
  the join described the way filters and aggregates are (SQL over
  fragments).
- Few-group aggregations show the extra hop: decide with manifest
  statistics or a first-batch sample.

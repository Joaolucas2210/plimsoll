# ADR 0072 — Shuffles survive a lost worker

- Status: Accepted
- Date: 2026-11-03
- Owner: JoaoLucas2210
- Related: ADR 0060 (one retry before the first batch), ADR 0064/0065 (shuffles), ADR 0067/0068 (buckets), ADR 0070 (membership changes at runtime)
- Scope: `remote::{retryable, with_retry (per-attempt bodies), RemoteReduceExec::execute, Bucket.only, Reduce.only, CONNECT_TIMEOUT}`, `engine::{open_side (map fallback), map_bucket (only)}`

## Context

Scans retry once on another worker before their first batch (ADR 0060).
Shuffles did not. A shuffle's buckets are handed out once (ADR 0064),
so re-sending a reduce, or re-fetching a bucket, would find nothing to
fetch. Any worker lost mid-shuffle failed the query.

ADR 0070 made that more likely, because workers now come and go at
runtime. A shuffle planned across W workers needs all W until it ends.

The live test exposed a second gap, independent of shuffles. The worker
client had **no connect timeout**. In the test environment, a connection
to a dead address is silently dropped rather than refused, as with a
host that has vanished from the network. A lost worker therefore held
the query until its deadline (30 s) instead of failing fast enough to
retry.

## Decision drivers

1. **Same answers.** Recompute only what nothing has consumed yet, so
   nothing is duplicated.
2. **No coordination protocol.** No shuffle state on the coordinator, no
   map registry, no acknowledgements.
3. **Cost only on failure.**

## Decision

- **Connect timeout.** Worker connections must open within 3 s
  (`CONNECT_TIMEOUT`). A connect timeout is a retryable failure, so
  scans, broadcast probes, health checks and shuffles all fail over
  promptly.
- **Re-run instead of re-fetch.** A map is a pure function of its
  fragment (a table snapshot, files and filters), so any worker can
  compute it again. The new `Bucket.only` flag makes such a re-run keep
  just the bucket that is asked for and drop the others as the map
  drains.
- **Lost map (on a reducer).**
  - Opening a bucket can fail with a retryable error before the
    bucket's schema arrives: a connection failure, 429 or 5xx, judged by
    the same `retryable` as ADR 0060.
  - The reducer then asks another worker that holds maps for the same
    shuffle to run that map again, with
    `{shuffle: "<id>~m<map>r<partition>", only: true}`. The fallback is
    the next peer by map index.
  - Nothing of that bucket was read yet, so nothing is duplicated.
  - Each reducer that lost the map re-runs it for its own bucket.
- **Lost reducer (on the coordinator).**
  - A reduce that fails retryably **before its first batch** is retried
    once on the next reducer's worker, with
    `Reduce {shuffle: "<id>~r<partition>", only: true}`.
  - Under that fresh id, every map runs again, for that one bucket only,
    so the retry never asks for buckets the lost reducer had already
    taken.
  - `with_retry` now takes one body per attempt, because a retried
    reduce is a different task.
- **Unchanged.** A failure after data has flowed still fails the query:
  after the reduce's first batch, or after a bucket's schema. In both
  cases rows may already have been consumed.

## Consequences

**Positive**
- A worker lost before or during a shuffle no longer fails the query,
  in either role, as long as another worker is up. Rolling restarts and
  crashes during shuffles become transparent.
- Every distributed path fails over in seconds, not at the deadline.
  The full distributed suite also got faster (60 s to 39 s), because its
  dead-worker case stopped hanging.
- No coordinator state, and nothing new in the steady state. Retries
  cost only when something failed.

**Negative (accepted debt)**
- **Recomputation can repeat.** A lost map is re-run once per reducer
  that needed it, and a retried reduce re-runs every map for its bucket.
  That is up to W extra scans of the lost work. It is correct and
  simple, not minimal.
- **Orphaned buckets.** The original maps keep the buckets of a lost
  reducer until the TTL (5 minutes, ADR 0064). They are still accounted
  or spilled (ADR 0067), but they hold memory or disk until then.
- **One retry, as ADR 0060.** Losing a second worker in the same step
  fails the query.
- **The connect timeout is fixed** at 3 s.

## Validation

- `remote::tests::fragments_round_trip_as_json`: `bucket.only` and
  `reduce.only` round-trip.
- `shuffles_survive_a_lost_worker` (live, Spark-written MOR and DV
  tables). A coordinator plans over two live workers and one dead
  address, with no health checks, so the dead worker owns maps, a reduce
  and scan fragments. Three queries return the single-node answers and
  plan `partitions=3`:
  - a grouped aggregation (`id % 7`);
  - a one-group-per-row aggregation;
  - a partitioned self-join.

  While debugging, the fallbacks were observed directly: maps re-run as
  `~m2r0`, `~m2r1`, and inside the retried reduce as `~r2~m2r2`.
- The existing distributed tests pass unchanged.

## Revisit when

- Recomputation shows up in cost: let the first fallback's map serve
  every reducer, with a shared re-run id, or let maps keep buckets until
  the query ends.
- Orphaned buckets pressure workers: release a shuffle's maps when the
  coordinator finishes the query (a cleanup call per shuffle).
- Two failures per step must be survived: more retries, bounded by the
  deadline.

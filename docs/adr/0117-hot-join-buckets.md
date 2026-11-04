# ADR 0117 — Hot join buckets split across reducers

- Status: Accepted
- Date: 2026-11-04
- Owner: JoaoLucas2210
- Related: ADR 0065 (shuffled joins), ADR 0067 (spilled buckets), ADR 0068 (streaming reducers), ADR 0072 (retries), ADR 0111/0112 (join sides)
- Scope: `remote::{plan_splits, bucket_sizes, BucketSplit, Split}`, `Bucket::{readers, sizes}`, `Reduce::split`, `JoinSpec::hash_keys`, `Workers::{with_skew, skew_splits}`, `PLIMSOLL_SKEW_BYTES`, the worker's `open_side` and `map_bucket`

## Context

A shuffled join (ADR 0065) sends every row with a given key to one
reducer. One heavy key, such as a default customer, a null-like
placeholder or a hot product, puts a large share of a side into one
bucket. That reducer then holds the shuffle back: it builds or probes
far more than the others, and spills (ADR 0067), while the rest finish
and wait.

The ROADMAP named two remedies. **Spilling the largest bucket first**
turned out not to be the problem: the hot bucket receives most of the
batches, so it is already the one that meets the memory limit and
spills. The cost is on the **reducer**, which this ADR addresses.

## Decision drivers

1. **Correct for every join type**, or not applied to it.
2. **Decided from facts, not estimates**: map outputs are complete
   before any reducer reads them, since maps keep every bucket first
   (ADR 0067). Their sizes can be read at that point.
3. **No cost when nothing is skewed** beyond one small request per map.

## Decision

- **The size round.** Before the first reduce of a join runs, the
  coordinator asks every map of both sides for its buckets' sizes
  (`Bucket::sizes`). The map runs with the keys its reducers would
  hash: `JoinSpec::hash_keys`, moved out of the worker so that both
  compute them alike. It answers with one `bytes` row per bucket and
  hands nothing out. This happens once per shuffle, shared by its
  partitions. If it fails, nothing splits and the reduces run and retry
  as before.
- **Hot**: a bucket whose side holds over `PLIMSOLL_SKEW_BYTES` of it
  (default 64 MiB; 0 never splits) and four times that side's lower
  median bucket.
- **The side that may split.** A side may split when each of its rows
  appears at most once in the output:
  - the left side of inner, left, left semi and left anti joins;
  - the right side of inner, right, right semi and right anti joins.

  Splitting the other side would emit its unmatched rows once per part,
  so a full join never splits.
- **The split** (`plan_splits`). The hot side's maps holding the bucket
  are grouped by bytes, largest first, into at most as many parts as
  there are reducers. Each part is a reduce (`Reduce::split`) that:
  - reads only its group's maps on the split side;
  - reads the whole other side, whose bucket is handed out once per part
    (`Bucket::readers`) and freed by the last reader;
  - runs on the next reducers' workers in turn, its output streaming as
    the bucket's partition.
- An older worker ignores `readers` and hands a bucket out once. The
  second part then fails ("already fetched"), so it never gets fewer
  rows silently.

## Consequences

**Positive**
- A heavy key's work is spread over several workers, each building
  against a share of the hot side.
- Unskewed shuffles pay one size request per map and nothing else.

**Negative (accepted debt)**
- **Parts are not retried elsewhere.** The retry path (ADR 0072) re-runs
  maps for one whole bucket, so a lost part fails the query.
- **Aggregation shuffles do not split.** A hot group would need its
  partial states merged twice.
- **The other side is read once per part**: its bucket's bytes cross
  the network that many times. That is the usual price of a skew join,
  and it is paid only for hot buckets.
- **Every map must finish before any reducer starts.** That was already
  so, because maps keep whole buckets (ADR 0067). The size round now
  relies on it.

## Validation

- `remote::tests::hot_join_buckets_split_on_the_side_that_may`:
  - a hot left bucket of an inner or left join is split in two,
    balanced by bytes, largest map first;
  - nothing splits for a right join whose right side is not hot, under
    the minimum, or with one reducer;
  - a hot right bucket of a right join splits on the right;
  - a full join never splits.
- `remote::tests::fragments_round_trip_as_json`: `readers`, `sizes` and
  `split` round-trip.
- `tests/distributed.rs::distributed_scans_match_local_ones` (Spark
  fixtures, CI's interop job), at 12 partitions and pinned to two CPUs.
  A coordinator splitting anything over one byte runs inner, left,
  right and full joins on a constant key (`a.id % 1 = b.id % 1`), which
  puts every row in one bucket:
  - inner, left and right joins split;
  - the full join does not;
  - every answer matches the local engine.

## Revisit when

- Parts must survive a lost worker: re-run their maps for the part, as
  retries do for a bucket.
- Hot groups in aggregations matter: merge partial states per part,
  then once more on the coordinator.

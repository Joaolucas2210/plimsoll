# ADR 0067 — Shuffle buckets spill to disk

- Status: Accepted
- Date: 2026-11-03
- Owner: JoaoLucas2210
- Related: ADR 0064 (shuffle maps), ADR 0065/0066 (join shuffles), ADR 0003 (memory ceiling, spilling)
- Scope: `CoreEngine::map_bucket`, `keep`, `Kept`, `remote::decode` (now public)

## Context

A shuffle map (ADR 0064) keeps every bucket of its fragment until each
reducer fetches its own.

- **Under-accounted.** The first version collected all buckets with
  `collect_partitioned`, then reserved their size. Memory was therefore
  used before it was accounted.
- **Fail when full.** If the pool said no, the query failed.

So a map larger than the worker's pool could not shuffle at all, while
DataFusion's own operators on the same worker spill to the configured
spill directory instead.

## Decision drivers

1. **Account before holding.** No batch is kept unless the pool granted
   it.
2. **Degrade, do not fail.** Use the disk the operator already gave the
   engine for spilling (`spill_dir`, ADR 0003).
3. **Bounded change.** Same buckets, same handout; only where a bucket
   waits changes.

## Decision

- **Streaming map.** The map executes each output partition of its hash
  repartition and drains all of them concurrently, because the
  repartition feeds them together. Each bucket is kept by `keep`:
  1. For each batch, grow the map's reservation (`shuffle-map`,
     `Spillable`).
  2. While the pool grants it, the batch stays in memory.
  3. The first refusal spills **that bucket**. A spill file comes from
     the runtime's `DiskManager`, and the batches held so far are written
     to it as Arrow IPC and released from the reservation. The rest of
     the bucket's batches follow them to disk.
  4. Other buckets keep going in memory until their own refusal.
- **Handout.**
  - A bucket in memory is sent as before, and its size leaves the
    reservation.
  - A bucket on disk is read as it is sent, with the same incremental
    IPC decoder that reads worker streams. It requires the end-of-stream
    marker, so a truncated spill file fails the stream instead of
    returning fewer rows.
  - The file is held by the stream and deleted when the stream (and the
    last reference) is dropped. Abandoned maps (TTL, ADR 0064) drop their
    files with them.
- **No spill directory.** Spilling is disabled when `spill_dir` is
  unset, so the `DiskManager` refuses and the query fails as before,
  with the pool's message.

## Consequences

**Positive**
- Maps never hold unaccounted memory, and maps larger than the pool
  complete through the spill directory.
- No new code path for serving: a spilled bucket is one more IPC stream.

**Negative (accepted debt)**
- **Reducers still buffer their buckets in memory** (ADR 0064/0065). A
  reducer's share is 1/W of the data, and a refused reservation fails
  the query. Streaming fetched buckets straight into the merge or join
  is the next step.
- **Spill files are not counted** in DataFusion's temp-directory limit
  or in Plimsoll's spill metrics (`spilled_bytes`). They are deleted when
  their bucket is served or the map expires.
- **The first refusal spills the bucket that asked**, not the largest
  one. A skewed map may spill small buckets while a large one stays in
  memory. That is simple and correct; it is not optimal.
- **Writes and reads use the spill file's blocking I/O** inside async
  tasks, as DataFusion's own spill writers do.

## Validation

- `engine::tests::shuffle_buckets_spill_to_disk_when_the_pool_says_no`:
  - with a roomy pool, four batches stay in memory and are reserved;
  - with room for a batch and a half, the bucket spills, the
    reservation drops to zero, and decoding the file returns the same
    four batches.
- `distributed_scans_match_local_ones` (live, MOR, EQ and DV tables):
  every shuffle (aggregation and join) still returns the single-node
  answers through the streaming map.

## Revisit when

- Reducers hit their pool: feed fetched buckets to the merge or join as
  streams, and spill there too.
- Spill usage needs limits or metrics: register shuffle files with the
  temp-directory accounting and `spilled_bytes`.
- Skew shows: spill the largest bucket first.

# ADR 0116 — Broadcast build sides cached per snapshot

- Status: Accepted
- Date: 2026-11-04
- Owner: JoaoLucas2210
- Related: ADR 0066 (broadcast joins), ADR 0099 (broadcast limit), ADR 0038 (result cache), ADR 0115 (non-equi joins)
- Scope: `remote::{BuildCache, build_key}`, `Workers::{with_build_cache, build_cache_hits}`, `PLIMSOLL_BROADCAST_CACHE_BYTES`

## Context

A broadcast join (ADR 0066, 0115) collects its small side on the
coordinator, encodes it as Arrow IPC and sends it with every probe
fragment. Dashboards and repeated reports join the same dimension, at
the same snapshot, again and again, and each query collected it again:
a scan of the dimension's files through the workers, then the encoding.

A build side that reads only Iceberg tables pinned to snapshots is the
same rows for as long as those snapshots are the ones read. The result
cache (ADR 0038) already relies on that for whole queries.

## Decision drivers

1. **Correct by construction.** A new commit, a different filter or a
   different projection must never reuse old rows.
2. **Under the memory ceiling**, like every cache (ADR 0018, 0038).
3. **Off unless asked**, like the result cache.

## Decision

- **`build_key`** identifies what a build side reads.
  - There is a key only when every source is an accounted scan pinned
    to a table, snapshot and schema (`result_cache`'s check). Anything
    else gets no key: a shuffle, a federated scan, a placeholder.
  - The key lists every node of the build plan with its one-line
    display and output fields, so filters, projections, joins and
    aggregates are all in it. Accounted scans are listed by their
    pinned id, not by their label, whose planning counters (cache hits)
    change between identical queries.
- **`BuildCache`**, per coordinator (`Workers`), holds the encoded
  bytes by key.
  - It keeps the least recently used entries within
    `PLIMSOLL_BROADCAST_CACHE_BYTES` (0, the default, keeps none).
  - Each entry holds the memory reservation the broadcast made, shared
    with the query that made it. It is counted once, and released when
    the entry is evicted and no query holds it.
- **`RemoteBroadcastExec`** looks its key up before collecting.
  - A hit within the broadcast limit is sent as is.
  - A miss collects and encodes as before, then stores the bytes.
  - A build side over the limit is joined locally (ADR 0099) and never
    cached.
  - `Workers::build_cache_hits` counts the reuses.

## Consequences

**Positive**
- A repeated join against an unchanged dimension skips collecting it:
  no scan of its files and no encoding. Only the probe fragments run.
- A new commit pins a new snapshot, so the next query misses and caches
  the new rows. Nothing has to be invalidated.

**Negative (accepted debt)**
- **Gathered joins are not cached.** Left, full, semi and anti
  broadcasts (ADR 0097) keep the build rows on the coordinator to add
  unmatched ones, so they collect anyway.
- **Shuffled build sides (ADR 0064) have no key.** Their reduces are
  not pinned scans.
- **Stale entries linger** until evicted by size, after their snapshot
  is no longer read. They are bounded by the cache's size.
- **The key is the plan's text.** Two plans for the same rows that
  print differently miss each other, which wastes the cache but is
  never wrong.

## Validation

- `config::tests::tls_needs_both_files_and_broadcasts_have_a_limit`:
  the cache is off by default, sized by the variable, and invalid
  values fail.
- `tests/distributed.rs::distributed_scans_match_local_ones` (Spark
  fixtures, CI's interop job), at 12 partitions and pinned to two CPUs.
  A coordinator with a 16 MiB cache runs a broadcast join twice, then a
  differently filtered one twice:
  - hits go 0, 1, 1, 2: a different filter never hits;
  - every answer matches the local engine;
  - every plan is a `RemoteBroadcast`.

## Revisit when

- Gathered joins repeat often: cache their build batches too, which
  the coordinator needs decoded.
- Snapshots move fast: evict entries whose snapshot is no longer
  current, using the catalog refresh (ADR 0033).

# ADR 0069 — Shuffle spill and traffic visibility

- Status: Accepted
- Date: 2026-11-03
- Owner: JoaoLucas2210
- Related: ADR 0067 (map-side spill), ADR 0068 (streaming reducers), ADR 0059 (`fragment` log and span), ADR 0019 (metrics)
- Scope: `Engine::record_spill`, `CoreEngine::map_bucket`, `keep` (test), `FragmentReport` (`kind`, span attributes)

## Context

Shuffles (ADR 0064–0068) became the heaviest traffic between workers.
Operators could not see them:

- **Spill was invisible.** Shuffle spill files did not show up in
  `plimsoll_spill_bytes_total`.
- **Shuffle traffic was indistinguishable.** Every request a worker
  served was logged as an anonymous `fragment` line: bucket fetches,
  reduces, broadcast probes and plain scans alike.
- **The disk-limit status was unknown.** ADRs 0067 and 0068 recorded
  that shuffle files escaped the spill directory's size limit
  (`max_temp_directory_size`). That turned out to be wrong: see below.

## Decision drivers

1. **Answer the on-call questions from existing signals.** Is the
   shuffle spilling, how much, for which query, and is the disk
   protected? Use the log, the trace and `/metrics` already in place.
2. **No new metric families or dependencies.**

## Decision

- **Disk limit: verified, not added.** DataFusion's local spill writer,
  the one `DiskManager::create_tmp_file` returns, checks
  `max_temp_directory_size` on **every write**, and returns that space
  when the file is dropped. Shuffle buckets were therefore bounded all
  along. A test now pins this: with a 1 KiB limit and a refusing pool,
  the map fails with the limit's error instead of filling the disk. The
  "not counted" claim in ADRs 0067 and 0068 is corrected in place.
- **`spilled_bytes`.**
  - `Engine::record_spill` adds bytes spilled outside a plan's operators
    to the same counter that completed queries feed.
  - After a map completes, each spilled bucket's file size is recorded.
  - `plimsoll_spill_bytes_total` on `/metrics` now includes shuffles.
- **A `shuffle_spill` log line** is emitted per spilled bucket, with
  `shuffle`, `map`, `bucket` and `bytes`. The shuffle id ties it to the
  bucket requests that served it.
- **Kinds on the `fragment` line and span.** Every worker request is
  classified:
  - `scan`;
  - `partial_aggregate`;
  - `broadcast_probe`;
  - `shuffle_map` (bucket requests, including the one that ran the map);
  - `aggregate_reduce`;
  - `join_reduce`.

  The `fragment` log line carries the kind as `kind`, next to the
  existing `query_id`, `rows` and `bytes`. The span carries
  `plimsoll.fragment_kind` and now also `plimsoll.bytes`. Summing
  `bytes` by `kind` for a `query_id` gives a query's shuffle traffic.

## Consequences

**Positive**
- Spill dashboards and alerts now cover shuffles, with no new
  configuration.
- One log query answers how much a query shuffled, and between which
  steps. The trace shows reduces and bucket fetches as distinct spans.
- The disk-limit behaviour is tested rather than assumed.

**Negative (accepted debt)**
- **Spill is attributed at the node level, not the query level.**
  `shuffle_spill` lines carry the shuffle id, not the query id; the
  `fragment` lines for the same requests carry both.
- **No per-kind metrics.** Traffic by kind comes from logs or traces.
  Prometheus counters by kind can come if dashboards need them.

## Validation

- `engine::tests::shuffle_buckets_spill_to_disk_when_the_pool_says_no`
  now also covers the limit: with a 1 KiB temp-directory limit and a 1
  KiB pool, `keep` fails with `max_temp_directory_size`.
- `distributed_scans_match_local_ones` (live) logs every kind. One run
  produced `scan` 69, `partial_aggregate` 24, `broadcast_probe` 12,
  `shuffle_map` 206, `aggregate_reduce` 48 and `join_reduce` 28, and all
  answers matched the single node.

## Revisit when

- Dashboards need shuffle traffic without log queries: add
  `plimsoll_fragment_bytes_total{kind}`.
- Per-query spill accounting is needed: thread the query id into the map
  (it is already in the request headers) and log it on `shuffle_spill`.

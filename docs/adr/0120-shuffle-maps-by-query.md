# ADR 0120 — Shuffle maps attributed to queries

- Status: Accepted
- Date: 2026-11-04
- Owner: JoaoLucas2210
- Related: ADR 0064/0067 (shuffle maps and spilled buckets), ADR 0069 (shuffle spill visibility), ADR 0118 (per-query spill), ADR 0059 (query identity on workers)
- Scope: `QueryEngine::fragment`'s `query_id`, `map_bucket`, the `shuffle_map` and `shuffle_spill` log lines

## Context

A shuffle map runs once per (shuffle, map) on its worker, started by
whichever request reaches it first: a reducer's bucket fetch, or the
coordinator's size round (ADR 0117). Every one of those requests
already carries the query's id, so their `fragment` lines name the
query. The map's own run did not:

- `shuffle_spill` (ADR 0069) named the shuffle, not the query. It was
  the last spill line without one (ADR 0118's debt).
- Nothing logged a map run as such: how many buckets it kept, how many
  went to disk.
- Found while looking: the spill of the **map's own operators** (a
  partial aggregate over more than memory, say) was never added to the
  node's spill total. Only bucket spills were. Plans run as queries add
  theirs when their stream drops, but a map's plan is never a query
  stream.

## Decision drivers

1. Every spill line names its query.
2. Every byte written to disk counts in `plimsoll_spill_bytes_total`.

## Decision

- **`QueryEngine::fragment` takes the request's `query_id`** (from
  `x-plimsoll-query-id`) and hands it to `map_bucket`.
- **`shuffle_spill` lines carry it.**
- **One `shuffle_map` line per map run**, whichever request started it,
  with:
  - the shuffle, the map and the query;
  - the buckets kept, and how many of them went to disk;
  - `spilled_bytes`, its bucket files plus what its own operators
    spilled.
- **The map's operator spill is added to the node's total** once the
  map has drained, where bucket spills already were.

## Consequences

**Positive**
- A query's shuffle work is fully attributable from the workers' logs:
  `fragment` lines per request, plus one `shuffle_map` line per map
  with its spill.
- The node's spill total no longer misses map operators.

**Negative (accepted debt)**
- **The id is the starting request's.** Maps belong to one query (their
  shuffle id is per query), so that is always the right one, except
  for an older coordinator that sends none, where it is `null`.
- **No per-map metrics series.** Maps are per query, which would make
  for unbounded labels; logs are the place for them.

## Validation

- By hand: a coordinator with two workers ran a grouped aggregation
  over the REST catalog's `bronze.dsrc`, planned as an aggregation
  shuffle with two maps. Both workers logged a `shuffle_map` line with
  the coordinator's query id (`4927169302102018`), 2 buckets kept,
  none on disk, and 0 bytes spilled. The answer was right.
- `tests/distributed.rs`: every shuffle still matches the local engine.
  Without a query tag, the test coordinator's maps log `null`.
- Engine tests call `fragment` without a query id, as an untagged
  request does.

## Revisit when

- Map runs should show in traces: give the map run its own span, child
  of the starting request's.

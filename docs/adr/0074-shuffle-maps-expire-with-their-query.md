# ADR 0074 — Shuffle maps expire with their query

- Status: Accepted
- Date: 2026-11-03
- Owner: JoaoLucas2210
- Related: ADR 0059 (deadline forwarded to workers), ADR 0064 (map TTL), ADR 0072 (retry-derived ids), ADR 0073 (release)
- Scope: `CoreEngine::{fragment, map_bucket, release}`, `MAPS`, `RELEASED`, `sweep`, `QueryEngine::fragment` (now takes the deadline)

## Context

ADR 0073 releases a shuffle's maps when its query ends. Two gaps were
left, both falling back on the fixed 5-minute TTL of ADR 0064:

- **A coordinator that never sends the release.** It may crash or
  restart, or its release request may be lost. Its maps then lived 5
  minutes, regardless of how long their query could still run.
- **Stragglers.** A bucket request from an already cancelled reducer can
  arrive after the release, and would start the map again.

There was also an implementation detail behind the first gap. Expired
maps were only swept on the next shuffle request. A worker that served
one big shuffle and then only scans kept the expired buckets
indefinitely.

## Decision drivers

1. **A map lives no longer than the query that needs it.**
2. **A released shuffle stays released.**
3. **Bounded bookkeeping.** No per-query state beyond what is swept.

## Decision

- **Deadline, not TTL.** Every worker request already carries the
  coordinator's remaining deadline. The handler turns it into an
  absolute deadline, bounded by the worker's own timeout (ADR 0059), and
  now passes it to `fragment`.
  - A map's entry expires at the deadline of the request that created
    it. Its reducers belong to the same query, so they cannot need it
    afterwards.
  - The fixed `SHUFFLE_TTL` is gone.
- **A sweeper.** On first use, a task sweeps every 5 s. It drops expired
  maps, releasing their reservations and spill files, and forgets old
  releases. Expiry no longer depends on more shuffle traffic arriving.
- **Released ids are remembered.**
  - `release` records the shuffle's base id (the part before `~`) for 10
    minutes, longer than any query runs.
  - A bucket request whose shuffle, or any retry derived from it
    (`id~…`), was released is refused with `InvalidQuery` ("its query
    has ended") **before the table is even opened**. A shuffle that
    merely shares a prefix is unaffected.

## Consequences

**Positive**
- Worker shuffle memory and disk are bounded by the lifetime of the
  queries they serve, even when coordinators crash or releases are lost.
- Stragglers no longer recreate maps after their query has ended.
- A worker that stops receiving shuffles still frees expired maps within
  5 s.

**Negative (accepted debt)**
- **A map's lifetime is set by the first request that creates it.**
  Other requesters of the same map belong to the same query and carry
  essentially the same deadline, so this is close enough.
- **Released ids are kept 10 minutes,** a short string each, swept with
  the maps.
- **The sweeper runs every 5 s for the life of a worker that ever
  shuffled,** whether or not there is anything to sweep. It is one
  mutex pass.

## Validation

- `engine::tests::releasing_a_shuffle_drops_its_maps_and_its_retries`
  (extended). After releasing `rel-a`:
  - bucket requests for `rel-a` and `rel-a~m1r0` are refused as
    released;
  - a request for `rel-ab` proceeds to open its table, and fails there
    for want of one.
- `engine::tests::maps_expire_with_their_query`: a sweep drops a map
  past its deadline and an old release, and keeps a live map.
- The live distributed tests (shuffles, retries, releases) pass
  unchanged.

## Revisit when

- Long-running queries share maps across very different deadlines:
  extend a map's expiry to the latest requester's deadline.
- Releases must survive worker restarts: there is nothing to keep then
  either, since the maps went with the process.

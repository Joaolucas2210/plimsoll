# ADR 0073 — Release shuffles when queries end

- Status: Accepted
- Date: 2026-11-03
- Owner: JoaoLucas2210
- Related: ADR 0064 (maps, TTL), ADR 0067 (spilled buckets), ADR 0072 (retries derive shuffle ids)
- Scope: `remote::{Task::Release, RemoteReduceExec (executed, Drop)}`, `QueryEngine::release`, `CoreEngine::release`, the `/v1/fragment` handler

## Context

A shuffle map drops itself once every bucket has been fetched (ADR 0064).
Some queries never fetch every bucket:

- a query that fails or is cancelled mid-shuffle;
- a query whose reducer was lost and retried under a fresh id
  (ADR 0072): the original maps keep that reducer's bucket.

Those maps were only reclaimed by the 5-minute TTL, swept on the next
shuffle request. Meanwhile their buckets held pool memory, which fails
or spills other queries, or disk in the spill directory. The cost was
small per query, but it stacked up on workers that serve many
dashboards.

## Decision drivers

1. **Free resources when the query ends**, whatever way it ends.
2. **Best effort, never on the query's path.** A release that fails
   leaves the TTL as the backstop.
3. **No new endpoint or secret.**

## Decision

- **Trigger.** `RemoteReduceExec` (aggregation and join shuffles)
  records that it executed. When the plan is dropped (the query
  finished, failed or was cancelled), it spawns, on the current runtime,
  one `{"release": {"shuffle": id}}` per worker involved: its reducers,
  its maps and, for joins, the right side's maps.
  - Plans that never ran, such as plans built only to be displayed, send
    nothing.
  - Each call has a 5 s timeout, and its outcome is ignored.
- **Worker.** `Task::Release` goes through the existing `/v1/fragment`
  handler: same worker-token check, no admission slot, a `fragment` line
  with `kind: "release"`.
  - The worker drops every map of that shuffle id **and of ids derived
    from it** (`id~…`, the map and reduce retries of ADR 0072). A shuffle
    that merely shares a prefix is not touched.
  - Dropping a map drops its buckets. Pool reservations shrink, and
    spilled bucket files are deleted (ADR 0067).
  - When anything was dropped, the worker logs a `shuffle_release` line
    with the shuffle id and the number of maps.
- **Unchanged.** Maps still drop themselves when fully fetched, and the
  TTL stays as the backstop for a coordinator that died, or a release
  that never arrived.

## Consequences

**Positive**
- A failed, cancelled or retried query frees its workers' shuffle memory
  and disk within milliseconds of ending, instead of minutes.
- Released buckets show in the logs per shuffle, next to `shuffle_spill`
  (ADR 0069).

**Negative (accepted debt)**
- **Release can race with a straggler.** A bucket request still in
  flight from a cancelled reducer can arrive after the release and start
  the map again. That map is then reclaimed by the TTL, as before.
- **Even a successful shuffle sends one small request per worker** when
  it ends. Usually that frees nothing, because fully fetched maps already
  dropped themselves.
- **Fire-and-forget.** If the coordinator process exits right after a
  query, the spawned releases may not run; the TTL covers them.

## Validation

- `remote::tests::fragments_round_trip_as_json`: `{"release":
  {"shuffle": "s1"}}` parses as `Task::Release`; an empty id is refused.
- `engine::tests::releasing_a_shuffle_drops_its_maps_and_its_retries`:
  with maps for `rel-a`, `rel-a~r2`, `rel-a~m1r0`, `rel-ab` and `rel-b`,
  releasing `rel-a` leaves exactly `rel-ab` and `rel-b`.
- `shuffles_survive_a_lost_worker` (live), run with logs: every query
  over the dead worker ends with `shuffle_release` lines freeing 2 to 4
  orphaned maps (the lost reducer's buckets), with the same answers as
  before.

## Revisit when

- Stragglers re-create maps often enough to matter: remember released
  ids for the TTL and refuse their bucket requests.
- Coordinators restart under load: have workers expire maps by the query
  deadline they were sent (ADR 0059) instead of a fixed TTL.

# ADR 0036 — Memory shares per resource group

- Status: Accepted
- Date: 2026-11-02
- Owner: JoaoLucas2210
- Related: ADR 0003 (ceiling), ADR 0018 (cache reserve), ADR 0023 (resource groups; this extends its spec), ADR 0035 (streamed chunks)
- Scope: `plimsoll_core::memory::{CeilingPool::{with_group_share, register_in, group_used}, GroupPool, CeilingError::GroupExhausted}`, `plimsoll_core::engine::{TableAccess::resource_group, Engine::capped_group}`, `plimsoll-server::{auth::Viewer::group, config::group_memory}`

## Context

ADR 0023 gave resource groups their own running and queued limits.
`batch=2/64` means at most two batch queries run at once. It said nothing
about **memory**, which is the resource the node actually dies of.

Two batch queries with large hash joins can take the whole working budget.
Every interactive query then fails with `memory_limit_exceeded` until they
finish. The node survives, which is the invariant, but the platform is
unusable for everyone else: the noisy-neighbour problem has moved from
concurrency to memory.

Operators need to say "batch may hold at most 40% of the line", so that
interactive work always keeps its headroom.

## Decision drivers

1. **Enforced where bytes are reserved,** not estimated at admission.
   Query memory is unknown until execution.
2. **Covers every consumer of the group's queries:**
   - DataFusion operators;
   - native reservations, such as the collected result (ADR 0012) and
     streamed chunks (ADR 0035).
3. **Ungrouped and uncapped queries pay nothing,** and behave exactly as
   before.
4. **The ceiling still binds everyone.** A share is a cap below the line,
   never above it.
5. **Typed failure.** The error names the group, so the operator knows
   which knob to turn.

## Considered options

**A. Shares inside the `CeilingPool`, plus a per-query pool view (chosen).**
- Each consumer entry carries an optional group.
- `try_grow` checks the group's bytes against its share, then the ceiling,
  under the same lock as today's accounting.
- The engine runs each query of a capped group in a `SessionContext`
  whose `RuntimeEnv` has the same disk manager, caches and object stores,
  with one change: its memory pool is a `GroupPool` that registers every
  DataFusion consumer in the group.
- Native reservations use `register_in(group)`.

**B. One `CeilingPool` per group.** Simple isolation, but then the groups'
pools sum to the line. Unused batch memory could not serve interactive
queries, so memory would be stranded. A cap that lends when idle is
strictly better. Rejected.

**C. Admission-time estimates** (reserve N MiB per batch query). This needs
a memory estimate per query, which DataFusion cannot give, and it would
be wrong both ways. Rejected (driver 1).

**D. DataFusion's `FairSpillPool`.** It shares between *spillable
consumers*, not between tenants, and has no notion of groups.

## Decision

- **Configuration.** `PLIMSOLL_RESOURCE_GROUPS=interactive=4/16,batch=2/64/40%`.
  - The optional third field is a percent from 1 to 100 of
    `PLIMSOLL_MEMORY_LIMIT_BYTES`.
  - It is validated fail-fast: `0%`, `101%` and a missing `%` are errors.
  - Groups without it, `default` included, have no cap.
- **`CeilingPool`.**
  - `with_group_share(group, bytes)` sets a cap.
  - `register_in(Some(group), …)` tags a native consumer.
  - Only *capped* groups are tracked, so uncapped tags cost nothing.
  - `try_grow` fails with `GroupExhausted { group, consumer, requested,
    available, share, top }` when the group is full, even if the ceiling
    has room.
  - Shrink, unregister and drop return bytes to the group.
- **`GroupPool`** is a `MemoryPool` that delegates to the ceiling, with
  registration tagged by group. `GroupPool::runtime(&base)` builds the
  per-query `RuntimeEnv`.
- **Engine.**
  - `TableAccess::resource_group()` defaults to `None`. The server's
    `Viewer` carries the request's group.
  - `Engine::context_for` swaps the runtime only for capped groups, so
    ungrouped and uncapped queries keep the shared one (driver 3).
  - The collected output and streamed chunks reserve in the group.
- **Errors.** A group cap failure is `ResourcesExhausted`, so clients see
  `422 memory_limit_exceeded`, as for the ceiling. The message names the
  group when the failing operator passes the pool's message through. Some
  operators (DataFusion's spill pool) substitute their own text.

## Consequences

**Positive**
- Batch work cannot starve interactive queries of memory. Interactive
  queries keep at least `limit − batch share − reserves`.
- Idle shares are not stranded. A cap only limits its own group, and
  other groups can use the memory while batch is idle.
- Uncapped groups have zero overhead: same runtime, no tracking.
- Shares can be sized from data. `/metrics` exports
  `plimsoll_pool_group_share_bytes{group}` and
  `plimsoll_pool_group_used_bytes{group}` for every capped group.

**Negative (accepted)**
- **Shares are caps, not guarantees.** If the uncapped groups fill the
  line, a capped group still fails at the ceiling. A guarantee would need
  every group capped, with shares summing to at most 100%. That is
  possible with this mechanism, and is the operator's choice.
- **Shares are a percent of `limit`, not of the working budget.** A 100%
  share therefore still ends at the ceiling.
- **The error text depends on the operator.** Spill-pool failures say
  "Memory Exhausted while SpillPool" without the group. The class is
  right, but the text is less helpful.


## Risks and mitigations

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| A share set too low fails every query of the group | Medium | Medium | Fail-fast validation; the error names the group when possible; per-group used/share gauges. |
| Per-query runtime clone diverges from the shared one | Low | Medium | Only the memory pool differs; disk manager, caches and object-store registry are the same `Arc`s (vended stores registered by one query are seen by all). |
| Partition tasks release just after the error | Expected | None | Accounting converges to 0 within milliseconds (tested with a bounded wait). |

## Validation

- `memory::tests::group_shares_cap_their_consumers_only`:
  - a 300 B share fills, and `GroupExhausted` names the group while the
    ceiling has room;
  - an uncapped group and an ungrouped consumer still reserve, and the
    ceiling still binds them;
  - shrink and drop return the share;
  - the pool returns to 0.
- `engine::tests::group_share_caps_a_query_below_the_ceiling`:
  - a 300k × 300k join fails in `batch` with a 1 MiB share;
  - the same query succeeds ungrouped and in an uncapped group;
  - a small query in `batch` fits;
  - the pool and the group drain to 0.
- `config::tests::resource_groups_are_parsed_and_validated`: the
  `/40%` form and four malformed shares.
- `metrics::tests::renders_exposition_lines`: the two per-group gauges.
- Manual smoke on the binary, with a 256 MiB ceiling and
  `batch=2/4/1%`:
  - a 2M-row self-join in `batch` answers `422 memory_limit_exceeded` with
    "resource group 'batch' share exhausted: 'HashJoinInput[1]' … of the
    group's 2684354B";
  - in `default` it returns 2 000 000;
  - the pool then reads 0.

## Revisit when

- Operators need guarantees, not caps: reserved minimums per group.
- Spill-pool errors need the group in the text: wrap or map them in
  `classify`.

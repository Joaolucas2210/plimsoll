# ADR 0003 — Process-wide memory ceiling (`CeilingPool`)

- Status: Accepted
- Date: 2026-10-29
- Owner: JoaoLucas2210
- Related: ADR 0001, ADR 0007, ADR 0009, ADR 0012 (closes the edge bypasses)
- Scope: `plimsoll_core::memory` (`CeilingPool`, `AccountedExec`, `df_adapter`), `plimsoll_core::engine`, `plimsoll-server` (`main.rs`, `engine.rs`)

## Context

The invariant: *the query fails, the node doesn't.* JVM engines show what goes
wrong without a hard ceiling: query pools track operator memory, but exchange
and output buffers, deserialized pages and connector state live outside them;
when the untracked part grows, the heap overflows and every query on the
worker dies. Headroom plus a cluster-level killer wastes memory and still
fails under skew. In Rust there is no heap to blow, but the same failure ends
in the kernel OOM killer. DataFusion's stock pools (`GreedyMemoryPool`,
`FairSpillPool`, `TrackConsumersPool`) account operators but have no notion of
an output slice and treat all consumers alike.

## Decision drivers

1. Hard bound: tracked bytes never cross `limit` through a fallible call.
2. Blast radius of one query: a breach is a typed error on that query.
3. Diagnosability: the error names who holds the memory.
4. Progress: a query that produced rows can always ship them.
5. Reuse DataFusion operators and spill logic unchanged.

## Considered options

### Option A — Own `MemoryPool` with an output slice (chosen)

Replace only the pool policy; keep DataFusion operators. Pros: drivers 1–5.
Cons: one mutex on the accounting path; we own an adapter to a trait that can
change between DataFusion majors.

### Option B — DataFusion `GreedyMemoryPool` / `FairSpillPool` (+ `TrackConsumersPool`)

Zero code. Cons: no output/exchange slice (working operators can starve the
result buffer); error attribution limited; no native consumer API for our own
buffers.

### Option C — Per-query fixed budgets

Simple fairness. Cons: the sum is either over-committed (no ceiling) or
under-used (idle budget cannot be borrowed). Can be layered on top later.

### Option D — cgroup/container limit only

Enforced by the kernel. Cons: its failure mode *is* process death. Kept as
defence in depth, set above `limit`.

## Decision

One `CeilingPool` per process, installed as the DataFusion `MemoryPool` of
every session through `memory::runtime_env` / `session_context_with_ceiling`.

1. **Checked reservations.** `try_grow` that would cross the line returns
   `CeilingError::Exhausted { consumer, kind, requested, available, limit,
   output_reserve, top }`, converted to `DataFusionError::ResourcesExhausted`.
   `top` lists the **5 largest consumers** (`TOP_CONSUMERS`).
2. **Consumer classes** (`ConsumerKind`): `Spillable` / `NonSpillable` (from
   DataFusion's `MemoryConsumer::can_spill()`) and `Output` (native API only).
3. **Output slice.** `output_reserve` bytes are usable only by `Output`
   consumers; working consumers see `max(output_used, output_reserve)` as
   taken. `Output` may use the whole `limit`.
4. **Spill hint.** `Reservation::spill_hint()` is true for `Spillable` native
   consumers when working memory is ≥ 80% of `limit - output_reserve`.
5. **RAII.** Dropping a `Reservation` unregisters it and frees its bytes;
   `Reservation::free()` releases without unregistering (free on idle).
6. **Unaccounted sources are wrapped.** `AccountedExec` keeps each
   partition's in-flight batch reserved for scans DataFusion does not account
   (Parquet decode; used by Iceberg, ADR 0007).
7. **Results are accounted.** `Engine::sql` streams batches into an `Output`
   reservation (`query-output`).

Server wiring (`plimsoll-server/src/main.rs`): `limit` =
`PLIMSOLL_MEMORY_LIMIT_BYTES` (default 1 GiB, must be > 0), `output_reserve`
= `limit / 8`. `PLIMSOLL_SPILL_DIR` unset disables spilling (pressure fails
the query); `Engine::new` sets `sort_spill_reservation_bytes` to
`min(limit / 16, 10 MiB)` so the merge reservation cannot starve small
ceilings.

Why not Option B: the stock pools cannot guarantee that a query which has
produced its result can return it, nor say who held the memory; those two
properties are the product.

## Consequences

**Positive.** A breach is HTTP 422 `memory_limit_exceeded` with the top
consumers in the message; the pool returns to its baseline; the node keeps
serving.

**Negative (accepted debt).**
- One `Mutex<State>` guards accounting (`ponytail:`); operators reserve in
  chunks, so contention is per reservation. Shard if profiles show it.
- DataFusion's infallible `grow` is accounted but not refused; overshoot is
  visible in `used()` and blocks every later `try_grow`.
- DataFusion consumers are only Spillable/NonSpillable; its exchange
  (`RepartitionExec`) uses the working budget. Only Plimsoll's own buffers use
  the output slice.
- Some DataFusion paths rewrap pool errors (sort with spilling disabled),
  so the consumer list may be missing from the final message.
- The 80% spill threshold is fixed (`SPILL_HINT_PERCENT`, `ponytail:`), and
  DataFusion operators use their own spill triggers, not `spill_hint()`.
- `AccountedExec` counts a batch twice while a consumer buffers it: may fail
  early, never overshoots.
- Not yet under the pool: the server's JSON copy of results
  (`ponytail:` in `Engine::sql`), Postgres result batches (ADR 0006),
  decoded Iceberg manifest entries and `metadata.json` (ADR 0007).

**Operational.** Metrics: `plimsoll_pool_used_bytes`,
`plimsoll_pool_limit_bytes`, `plimsoll_spill_bytes_total`; `/healthz` reports
`pool.used_bytes` / `pool.limit_bytes`. Failed queries appear in
`system.runtime.queries` with `state = 'failed'` and the error text. A
sustained non-zero `used_bytes` with no running queries indicates a leak or a
`grow` overshoot.

## Risks and mitigations

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| Untracked allocation path grows past physical memory | Medium | High | Wrap sources (`AccountedExec`); container limit set above `limit` |
| `grow` overshoot pushes `used` above `limit` | Medium | Medium | Every later `try_grow` fails until it is released; visible in `plimsoll_pool_used_bytes` |
| Spill itself needs memory and trips the ceiling | Medium | Medium | Bounded merge reservation (`sort_spill_reservation_bytes`) |
| Idle consumer holds its high-water mark | Medium | Medium | `Reservation::free()`; shrink on idle, not only on drop |
| Mutex contention at high concurrency | Low | Medium | Chunked reservations; shard when profiled |

## Validation

- `plimsoll_core::memory::tests`: `never_exceeds_limit_and_oom_names_top_consumers`,
  `top_is_capped`, `output_slice_is_honoured`,
  `output_may_use_shared_slice_and_workers_then_lose_it`,
  `spill_hint_only_for_spillable_under_pressure`, `shrink_and_drop_release`,
  `concurrent_hammer_never_crosses_limit` (16 threads), `rejects_reserve_above_limit`.
- `plimsoll_core::memory::accounted::tests::in_flight_batches_count_against_the_ceiling`.
- `crates/plimsoll-core/tests/memory_pressure.rs`:
  `query_fails_naming_consumers_and_node_survives`,
  `sort_spills_under_ceiling_when_disk_configured`.
- `plimsoll_core::engine::tests::oversized_result_fails_query_not_process`;
  `plimsoll_server::app::tests::real_engine_oom_is_query_only_and_visible_in_system_table`.

## Revisit when

- Profiles show the pool mutex in the top frames under concurrency.
- DataFusion exposes per-scan reservations for Parquet decode or an
  output/exchange consumer class (remove `AccountedExec` / extend the slice).
- Multi-tenant fairness is required: add per-query or per-group caps on top.
- Results are streamed to clients (shrink the `Output` reservation per batch;
  ADR 0012 keeps the reservation through serialization until then).

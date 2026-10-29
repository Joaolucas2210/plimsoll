# ADR 0003 — Process-wide memory ceiling (`CeilingPool`)

- Status: Accepted
- Scope: `plimsoll_core::memory`

## Context

The project's invariant: *the query fails, the node doesn't.*

JVM engines (Trino/Presto) show what goes wrong without a hard ceiling. Query
memory pools track operator memory, but not everything: exchange/output
buffers, deserialized pages, planner and connector state live outside the
pools. When the untracked part grows, the heap overflows and the
`OutOfMemoryError` kills the whole worker — every query on it, not just the
offender. The usual mitigation (a large heap headroom plus a cluster-level
killer) wastes memory and still fails under skew.

In Rust there is no GC heap to blow up, but the same failure exists: an
untracked allocation path ends in the kernel OOM killer.

## Decision

One `CeilingPool` per process with a hard `limit` in bytes, installed as the
DataFusion `MemoryPool` of every session (`session_context_with_ceiling`).

1. **Every reservation is checked** (`try_grow`). Crossing the limit returns a
   typed `CeilingError::Exhausted` (mapped to
   `DataFusionError::ResourcesExhausted`) that names the requesting consumer
   and the **top 5 consumers by bytes**, so the operator who reads the error
   knows what was holding the memory.
2. **Consumer classes**: `Spillable`, `NonSpillable` (from DataFusion's
   `MemoryConsumer::can_spill()`) and `Output`.
3. **Reserved output slice**: `output_reserve` bytes are usable only by
   `Output` consumers (result/exchange buffers). Working operators see that
   slice as always taken; output may use the whole limit. A query that has
   produced rows can always ship them.
4. **Spill hint**: when working memory crosses 80% of its budget,
   `Reservation::spill_hint()` returns true for spillable consumers.
5. **RAII**: dropping a reservation releases its bytes; a failed or cancelled
   query returns everything to the pool.

## Alternatives considered

- **DataFusion `GreedyMemoryPool` / `FairSpillPool` alone.** Good operator
  accounting, but no notion of an output/exchange slice, and the error does
  not say who holds the memory (`TrackConsumersPool` helps but still treats
  every consumer equally). We keep DataFusion's operators and spill logic and
  replace only the policy.
- **Per-query pools.** Each query gets a fixed budget. Simple fairness, but
  the sum of budgets is either over-committed (no ceiling) or under-used (idle
  budget can't be borrowed). A per-query cap can be layered on top later; the
  process ceiling is the invariant.
- **cgroup / container memory limit only.** Enforces a ceiling, but its
  failure mode is exactly the one we reject: the kernel kills the process. It
  stays as defence in depth, set above `limit`.

## Defects to avoid

- **Output buffers outside the pool.** Result batches waiting for a slow
  client and exchange queues must reserve memory (`ConsumerKind::Output`).
  Untracked buffers are how "tracked" engines still die.
- **Idle exchange consumers holding reservations.** A consumer whose
  downstream is blocked or finished must shrink to what it actually buffers,
  not keep its high-water mark. RAII release on drop is necessary, not
  sufficient: free on idle, not only on drop.
- **Spilling a whole batch without chunking.** Spilling requires memory too
  (sort/merge buffers). Spill in bounded chunks and keep the merge reservation
  small (`sort_spill_reservation_bytes`), or spill itself trips the ceiling.

## Consequences

- One mutex guards the accounting; operators reserve in chunks, so contention
  is per reservation, not per row. Shard if profiles show it.
- DataFusion's infallible `grow` is accounted but cannot be refused; overshoot
  is visible in `used()` and blocks every later `try_grow`.
- DataFusion's own exchange (`RepartitionExec`) registers as a working
  consumer; only Plimsoll's own result/exchange buffers use the output slice.
- Some DataFusion paths rewrap pool errors (e.g. sort with spilling disabled
  reports "Memory Exhausted while Sorting"), so the consumer list is not
  always in the final message.

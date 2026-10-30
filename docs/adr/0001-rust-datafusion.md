# ADR 0001 — Rust + Apache DataFusion as the engine foundation

- Status: Accepted
- Date: 2026-10-29
- Owner: JoaoLucas2210
- Related: ADR 0002, ADR 0003, ADR 0004, ADR 0007
- Scope: whole workspace (`Cargo.toml`, all crates)

## Context

Plimsoll's promise is *the query fails, the node doesn't*. That is a claim
about resource ownership: every byte a query holds must be attributable, and
crossing a limit must surface as an error on that query, never as a dead
process. The engine foundation decides whether the claim is enforceable at
all or only aspirational.

The failure we are designing out is structural in GC-heap engines: operator
pools track most memory, but buffers, deserialized pages and connector state
live outside them, and an `OutOfMemoryError` takes every query on the worker
down with it. In any language, an untracked allocation path ends in a crash
(JVM) or the kernel OOM killer (native).

Constraints: one engineer, open-source dependencies only, Arrow as the
in-memory format (Iceberg/Parquet ecosystem), embeddable — the platform owns
the process, the HTTP surface and governance.

## Decision drivers

1. Enforceable memory accounting: a pluggable, fallible allocator-of-record
   for query memory.
2. Correctness and process survival: no GC pauses, no unchecked panics.
3. Embeddability: a library, not a server we wrap.
4. Ecosystem fit: Arrow, Parquet, object stores, SQL front end.
5. Build and dependency weight.

## Considered options

### Option A — Rust + Apache DataFusion (chosen)

Arrow-native, vectorized query engine shipped as a library with extension
points we need: `TableProvider`, `MemoryPool`, `ExecutionPlan`, optimizer
rules, `ObjectStoreRegistry`. Operators reserve memory through the pool and
several can spill. Pros: the pool is the hook for a process ceiling (ADR
0003); no GC; same Arrow as the Parquet reader. Cons: smaller ecosystem than
Spark/Trino, more glue owned by us; heavy compile; DataFusion's major-version
cadence forces upgrades (connectors pinned to older majors become unusable,
see ADR 0007).

### Option B — JVM engine (Trino, Spark)

Mature, broad connector ecosystem. The memory model is the problem: untracked
heap allocations can OOM the whole worker, which is exactly the failure mode
the project exists to remove. Mitigations (heap headroom, cluster killers)
waste memory and remain probabilistic.

### Option C — C++ engine (Velox, embedded DuckDB)

Precise memory control and top-tier performance. Cons: memory-unsafe core,
an FFI boundary between every piece of control code and the engine, and the
project would spend its time on bindings rather than on the invariants.

## Decision

Build on **Rust, edition 2024** (`rust-version = "1.85"`) and **DataFusion
55** (Arrow 59), declared once in `[workspace.dependencies]` with
`default-features = false` and only the features used: `parquet`, `sql`,
`nested_expressions`, `unicode_expressions`, `string_expressions`,
`datetime_expressions`.

- DataFusion is used as a library. Plimsoll owns the process, the HTTP layer
  (ADR 0004) and governance (ADR 0002).
- `plimsoll-core` re-exports `datafusion` so every crate shares one version.
- Only the compute plane (`plimsoll-core`, `plimsoll-connectors`) and the edge
  (`plimsoll-server`, through `plimsoll_core::datafusion`) touch DataFusion.
  The control plane (`plimsoll-control`) does not depend on it; this is
  enforced by `scripts/gates.sh` (`forbid plimsoll-control datafusion`).
- `clippy::unwrap_used` and `clippy::expect_used` are `deny` workspace-wide:
  a panic is a process-level failure. Tests opt out locally.

Why not the runner-up (JVM): a JVM engine can be tuned to fail less often but
cannot be made to fail *only per query*, because the heap is shared and its
exhaustion is fatal. DataFusion's `MemoryPool` gives us a single fallible
choke point we can own.

## Consequences

**Positive.** One allocator-of-record for query memory (ADR 0003). One Arrow
version across reader, engine and connectors. Panic-free code paths are
lint-enforced.

**Negative (accepted debt).**
- Accounting is cooperative: only reservations are seen. Allocations outside
  them (DataFusion's Parquet decode, connector buffers, the server's JSON copy
  of results) must be wrapped by us, per path (ADR 0003, ADR 0007).
- DataFusion's infallible `MemoryPool::grow` can overshoot (ADR 0003).
- Compile time and binary size are high; feature trimming is the mitigation.
- Upgrading DataFusion majors is ongoing work; ecosystem crates pinned to
  older majors cannot be adopted without pinning us too.

**Operational.** A single static binary (`plimsoll-server`), no runtime VM to
size. Memory-limit failures surface as HTTP 422 `memory_limit_exceeded`
(ADR 0003), not as process restarts.

## Risks and mitigations

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| A DataFusion path allocates without reserving | High | High | Wrap sources in `AccountedExec`; cgroup limit above the ceiling as defence in depth (ADR 0003) |
| Major-version churn breaks extension APIs | High | Medium | One workspace pin; thin, owned adapters (`df_adapter.rs`, connectors) |
| Ecosystem crate needs an older DataFusion | Medium | Medium | Implement the narrow piece ourselves (ADR 0007, ADR 0008) |
| Panic in a dependency kills the process | Low | High | `deny(unwrap_used, expect_used)` in our code; typed errors at boundaries |

## Validation

- `scripts/gates.sh` (CI job `gates`): `fmt`, `clippy -D warnings`
  (enforces the unwrap/expect denial), `test`, and the `arch` step
  (`forbid plimsoll-control datafusion`).
- `plimsoll_core::engine::tests::oversized_result_fails_query_not_process`.

## Revisit when

- DataFusion stops exposing a pluggable, fallible `MemoryPool`, or the API
  diverges enough that the adapter is no longer thin.
- A required capability (e.g. distributed execution) exists only outside the
  DataFusion ecosystem.
- Upgrade cost per DataFusion major exceeds roughly a week of work.

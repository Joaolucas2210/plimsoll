# ADR 0001 — Rust + Apache DataFusion as the engine foundation

- Status: Accepted
- Scope: whole workspace

## Context

Plimsoll is a lakehouse query platform whose central promise is *the query
fails, the node doesn't*. That promise is about resource ownership: every byte
a query holds must be accounted for, and exceeding a limit must surface as an
error on that query, not as a dead process.

Options considered:

| Option | Memory model | Cost of ownership |
|---|---|---|
| JVM engine (Trino, Spark) | GC heap; untracked allocations can OOM the whole worker | Mature, but the failure mode we want to remove is structural |
| C++ engine (Velox, DuckDB embedded) | Precise, but unsafe; FFI boundary from any control code | Fast, but a learning project would spend its time on bindings |
| Rust + DataFusion | No GC; explicit `MemoryPool` reservations; spill-capable operators | Smaller ecosystem than Spark; we own more glue |

## Decision

Build on **Rust (edition 2024)** and **Apache DataFusion** (Arrow columnar,
vectorized, extensible `TableProvider` / `MemoryPool` / optimizer rules).

- DataFusion is a *library*, not a server: we keep control over the process,
  the HTTP surface and the governance layer.
- Its `MemoryPool` trait is the hook for the process-wide ceiling (ADR 0003).
- Only the compute plane (`plimsoll-core`, `plimsoll-connectors`) depends on
  DataFusion. The control plane (`plimsoll-control`) and the HTTP layer
  (`plimsoll-server`) stay DataFusion-free, which keeps their build fast and
  their types engine-agnostic (enforced in `scripts/gates.sh`).

## Consequences

- Memory accounting is explicit but cooperative: operators must reserve. Bytes
  allocated outside reservations (e.g. connector buffers) are a known gap,
  addressed per connector (ADR 0003, ADR 0005).
- DataFusion is heavy to compile; feature flags are trimmed at the workspace
  level (`default-features = false`).
- `unwrap`/`expect` are denied workspace-wide: a panic is a process-level
  failure, exactly what the platform promises not to have.

## Related

- HTTP framework choice: ADR 0004.

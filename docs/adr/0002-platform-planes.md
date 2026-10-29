# ADR 0002 — Control plane vs compute plane

- Status: Accepted
- Scope: crate boundaries (`plimsoll-control`, `plimsoll-core`, `plimsoll-connectors`, `plimsoll-server`)

## Context

Lakehouse platforms separate *deciding* from *doing*: a control plane owns
metadata, identity and policy; a compute plane executes queries against data.
Mixing them leads to engines that cannot be governed without being modified,
and governance that cannot be tested without spinning up an engine.

## Decision

Two planes, with a one-way dependency rule:

- **Control plane — `plimsoll-control`.** Decides *whether and where* a query
  runs: Iceberg REST catalog client (namespaces, tables, metadata location),
  query registry, admission control (running/queued limits globally, per
  resource group and per user), time limits, and the plain-Rust description of
  system tables (`system.runtime.queries`). No DataFusion dependency.
- **Compute plane — `plimsoll-core` + `plimsoll-connectors`.** Decides *how*
  a query runs: planning, execution, memory ceiling (ADR 0003), federation
  correctness and pushdown policy (ADR 0005, ADR 0006). `plimsoll-core` never
  depends on concrete connectors; connectors depend on core.
- **Edge — `plimsoll-server`.** Composes both: HTTP request → admission
  (control) → engine (compute) → registry update (control).

**The catalog is control plane.** It tells the engine where table metadata
lives; it never reads data files. Polaris (or any Iceberg REST catalog) is
the source of truth for table identity; the engine resolves metadata
locations into scans.

Dependency direction (enforced by `scripts/gates.sh`):

```text
plimsoll-server ─► plimsoll-control          (no datafusion)
       │
       └────────► plimsoll-core ◄── plimsoll-connectors
```

`plimsoll-control` must not depend on `plimsoll-core` or `datafusion`.
`plimsoll-core` must not depend on `plimsoll-connectors`.

## Consequences

- Governance is unit-testable in milliseconds (no engine, no network).
- The engine adapts control-plane types (e.g. `system::QUERIES_COLUMNS`) to
  Arrow, so the schema is defined once in plain Rust.
- Splitting planes into separate processes later (one coordinator, N workers)
  is a deployment change, not a rewrite: the boundary is already a crate edge.

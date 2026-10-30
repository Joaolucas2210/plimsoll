# ADR 0002 — Control plane vs compute plane

- Status: Accepted
- Date: 2026-10-29
- Owner: JoaoLucas2210
- Related: ADR 0001, ADR 0003, ADR 0005, ADR 0008
- Scope: crate boundaries — `plimsoll-control`, `plimsoll-core`, `plimsoll-connectors`, `plimsoll-server`

## Context

A lakehouse platform has to decide *whether and where* a query runs
(identity, catalog, admission, limits) and *how* it runs (planning, execution,
memory, connectors). When the two are mixed, governance cannot be changed
without touching the engine, cannot be tested without starting one, and the
engine's heavy dependency tree leaks into every component. Later separation
into a coordinator and workers becomes a rewrite rather than a deployment
change.

## Decision drivers

1. Governance testable without an engine or network (milliseconds).
2. Dependency direction that cannot erode silently (machine-checked).
3. Engine-agnostic control-plane types (catalog, registry, system tables).
4. A crate edge that can later become a process edge.

## Considered options

### Option A — Two planes plus an edge, one-way dependencies (chosen)

Control plane and compute plane as separate crates; the HTTP server composes
them. Dependency rules enforced in CI. Pros: drivers 1–4. Cons: some
adaptation code (plain-Rust types mapped to Arrow), one more crate boundary.

### Option B — Single crate, modules only

Least ceremony. Cons: nothing stops governance from importing DataFusion or a
connector; the boundary erodes under deadline pressure and is invisible in
review.

### Option C — Separate processes from day one

Clean isolation. Cons: RPC, deployment and failure handling cost before
there is a second node to justify it.

## Decision

Two planes and an edge, with a one-way dependency rule:

- **Control plane — `plimsoll-control`.** Decides *whether and where*:
  Iceberg REST catalog contract and client (`catalog::Catalog`,
  `rest::RestCatalog`, `catalog::InMemoryCatalog`), query registry
  (`governance::QueryRegistry`), admission control (`governance::Admission`,
  running/queued limits global, per resource group and per user), time limits
  (the `timeout` passed to `Admission::execute`), and the plain-Rust schema of
  `system.runtime.queries` (`system::QUERIES_COLUMNS`). No DataFusion.
- **Compute plane — `plimsoll-core` + `plimsoll-connectors`.** Decides *how*:
  engine façade (`engine::Engine`), memory ceiling (ADR 0003), federation
  correctness and pushdown (ADR 0005, ADR 0006), object stores (ADR 0009).
  `plimsoll-core` never depends on concrete connectors; connectors depend on
  core.
- **Edge — `plimsoll-server`.** Composes both: HTTP request → admission
  (control) → `QueryEngine` (compute) → registry update (control). It also
  adapts catalog namespaces into DataFusion schemas (`lakehouse` module) and
  control-plane rows into Arrow (`system` module).

**The catalog is control plane.** It answers which tables exist and where
their current metadata lives; it never reads data files. The engine turns a
`loadTable` result into a scan (ADR 0008).

```text
plimsoll-server ─► plimsoll-control            (no datafusion, no core)
       │
       └────────► plimsoll-connectors ─► plimsoll-core ─► datafusion
```

Invariants checked by the `arch` step of `scripts/gates.sh`
(`cargo tree -e normal`):

- `plimsoll-core` does not depend on `plimsoll-connectors`;
- `plimsoll-control` does not depend on `datafusion`;
- `plimsoll-control` does not depend on `plimsoll-core`.

Why this beats a single crate: the boundary is enforced by a gate that fails
CI, not by convention, at the cost of a small amount of adapter code.

## Consequences

**Positive.** Governance and catalog logic are unit-tested without an engine
(`governance`, `catalog`, `rest` tests run against an in-process axum stub).
`system.runtime.queries` is defined once in plain Rust and adapted to Arrow by
the server. Splitting into coordinator + workers is a deployment change along
an existing crate edge.

**Negative (accepted debt).**
- Adapter code at the edge (`plimsoll-server::system`, `lakehouse`).
- Only *normal* dependencies are checked: `plimsoll-control` has `axum` as a
  dev-dependency for its REST stub; this is allowed.
- The edge depends on both planes, so its build is as heavy as the engine's.

**Operational.** No runtime effect: one process today. A dependency violation
fails CI with `arch violation: <crate> depends on <dep>`.

## Risks and mitigations

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| Control logic drifts into the server to avoid the boundary | Medium | Medium | Review rule: handlers are thin translation (`app.rs` header); governance lives in `plimsoll-control` |
| Transitive dependency sneaks in (e.g. via a shared util crate) | Low | Medium | Gate uses the resolved `cargo tree`, not `Cargo.toml` text |
| Plain-Rust types and Arrow schema diverge | Low | Low | Column order is the field order of `QueryInfo`; covered by `snapshot_reflects_registry_states` and server tests |

## Validation

- `scripts/gates.sh`, step `arch` (CI job `gates`).
- `plimsoll_control::governance` tests (e.g. `queue_overflow_rejects_with_typed_error`,
  `dropped_execution_is_cancelled`) and `plimsoll_control::system::tests::snapshot_reflects_registry_states`
  run with no engine.
- `plimsoll_server::app::tests::real_engine_oom_is_query_only_and_visible_in_system_table`
  exercises the full edge composition.

## Revisit when

- A second node is introduced (coordinator/worker split): the crate edge
  becomes an RPC edge and needs its own ADR.
- The control plane needs engine types (e.g. plan-level cost estimates for
  admission); decide on a plain-Rust contract rather than relaxing the gate.

# ADR 0071 — Execution spans per operator

- Status: Accepted
- Date: 2026-11-03
- Owner: JoaoLucas2210
- Related: ADR 0031 (OTLP traces, hand-built spans), ADR 0056 (planning spans), ADR 0059 (fragment spans), ADR 0064–0069 (shuffles), ADR 0004 (dependency stance)
- Scope: `plan_trace::{OperatorSpan, operators, TRACED_OPERATORS}`, `QueryResult.operators`, `QueryStream::operators`, `trace::operator_spans`, `QueryLog.operators`, `FragmentReport.plan`, `engine::{Running, run}`

## Context

A query's trace showed its phases (ADR 0031), its planning work (ADR
0056) and the worker requests made for it (ADR 0059, 0069). Execution
was still one opaque span. To answer "why was this query slow", the
operator needed to know which operator took the time, how many rows it
produced and whether it spilled, on the coordinator and on each worker.

ADR 0031 named `tracing` + `opentelemetry-otlp` as the upgrade path for
this. That stack instruments code live: a span is opened as each
operator starts. DataFusion operators do not emit `tracing` spans,
though. Getting live spans would mean wrapping every operator, which
adds a per-batch cost.

## Decision drivers

1. **Operator-level timing, rows and spill, on every node** that ran
   part of the query.
2. **No hot-path cost.** Nothing added per batch.
3. **No new dependency** (ADR 0004), unless spans need something the
   existing exporter cannot carry.

## Considered options

**A. Spans from the executed plan's metrics (chosen).**
- DataFusion already records, per operator and partition, a **start and
  end timestamp**, output rows, compute time and spilled bytes.
  `aggregate_by_name` folds them across partitions: earliest start,
  latest end.
- After execution, the plan tree becomes spans, with real instants,
  nested as the plan, exported by the existing OTLP/JSON exporter.

**B. `tracing` + `opentelemetry-otlp`, wrapping operators.** This gives
live spans and exact parent/child timing per partition. The costs:
- a large dependency tree;
- a wrapper node around every operator;
- per-poll overhead;
- the same information, plus partition-level detail nobody asked for.

Option B stays the upgrade path if per-partition or per-fetch timing is
needed.

## Decision

- **Collection (core).** `plan_trace::operators(plan, limit)` walks an
  executed plan, parents first. For each node it records:
  - `name`, and its one-line description truncated to 256 bytes;
  - its parent's index;
  - start and end, when recorded;
  - `output_rows`, compute time and spilled bytes.

  At most `TRACED_OPERATORS` (200) nodes are recorded, because huge
  plans would drown a trace. `QueryResult` carries the list, and
  `QueryStream::operators` reads it after the stream ends.
- **Coordinator spans.** Each operator becomes an INTERNAL span with its
  node's name. The root operator sits under the `execution` phase span,
  and every other one under its plan parent. Attributes:
  - `plimsoll.operator` (the description);
  - `plimsoll.output_rows`, `plimsoll.compute_ms`,
    `plimsoll.spilled_bytes`.

  Nodes without timestamps (Plimsoll's own `AccountedExec`, the remote
  nodes) span the execution phase.
- **Worker spans.** Worker tasks (`fragment`, `reduce`) now return
  `Running { stream, plan }`. The handler keeps the plan in the
  `FragmentReport`. When the report drops, after its stream, so the
  metrics are final, the operator spans go under that request's
  `fragment` span. With `traceparent` forwarding (ADR 0059, 0064), one
  trace shows:

  ```
  coordinator query → execution → operators → RemoteReduceExec
      ├─ worker A fragment (aggregate_reduce) → merge operators
      │    └─ fragment (shuffle_map) on each map worker
      └─ worker B …
  ```

## Consequences

**Positive**
- Slow-query triage reads straight off the trace: which operator, how
  many rows, compute versus wall time, and spill, on every node.
- There is no cost while queries run. The walk happens once per query,
  after it ends, and only when the request is sampled for export: spans
  are built only when a tracer exists and the context is sampled.
- No new dependency; the exporter, sampling and back-pressure are
  unchanged (ADR 0031).

**Negative (accepted debt)**
- **Shuffle maps have no operator spans.** Their plan runs inside the
  shared map cell (ADR 0064), not in a single request. Their
  `shuffle_map` request spans and `shuffle_spill` lines remain.
- **Partitions are folded into one span per operator.** A skewed
  partition shows as a long span, not as its own.
- **Nodes without timestamps take the phase's bounds.** That covers
  `AccountedExec` and the remote nodes. The worker spans beneath them
  carry the real timing.
- **OTLP metrics remain out of scope.** `/metrics` (Prometheus) is the
  metrics path (ADR 0031).

## Validation

- `plan_trace` via `engine::tests::executed_operators_carry_their_metrics`,
  with a real aggregation:
  - the root has no parent, and every other node's parent comes before
    it;
  - `AggregateExec` has output rows, a start, an end at or after it, and
    its description.
- `trace::tests::query_spans_nest_phases_back_to_back_inside_the_request`:
  - operators nest under `execution` and then under their plan parents;
  - a node without timestamps spans the execution phase;
  - a node with timestamps keeps its own;
  - the attributes are present.
- A manual smoke test ran the binary as a coordinator plus two workers,
  exporting to a local OTLP collector, on a grouped aggregation over the
  Spark MOR table:
  - the coordinator exported its phases, `iceberg.plan`, and operator
    spans down to `RemoteReduceExec` with row counts;
  - each worker exported its `aggregate_reduce` fragment under the
    coordinator's `query`, with the merge's operators beneath it, and its
    `shuffle_map` fragments under the reduce that fetched them.

## Revisit when

- Per-partition or per-fetch timing matters: wrap operators with
  `tracing` + `opentelemetry-otlp` (option B).
- Shuffle maps need operator detail: keep the map's plan in the map cell
  and emit its spans under the request that ran it.
- Metrics must be pushed: OTLP metrics.

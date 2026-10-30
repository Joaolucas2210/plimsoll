# ADR 0004 — HTTP surface with axum 0.8

- Status: Accepted
- Date: 2026-10-29
- Owner: JoaoLucas2210
- Related: ADR 0013 (error taxonomy), ADR 0001, ADR 0002, ADR 0003, ADR 0008
- Scope: `plimsoll-server` (`app.rs`, `main.rs`, `metrics.rs`)

## Context

The server needs Kubernetes-style probes (`/livez`, `/readyz`, `/healthz`),
Prometheus `/metrics`, and `POST /v1/sql` with JSON in and out; graceful
shutdown on SIGTERM; and handler tests that do not open sockets. Every error
must map to a typed HTTP response so that a memory-limit failure is never
confused with an unhealthy node. `tokio` is already the runtime (DataFusion,
`tokio-postgres`, `reqwest`).

## Decision drivers

1. Small marginal dependency cost on top of tokio/hyper.
2. In-process handler tests (no network).
3. Graceful shutdown with a readiness flip.
4. Typed, stable error contract for clients and on-call.

## Considered options

### Option A — axum 0.8, trimmed features (chosen)

`tokio` + `hyper` + `tower`. `Router` is a `tower::Service`, so tests call
`oneshot` in-process; `axum::serve(..).with_graceful_shutdown(..)` covers
drain. Cons: no HTTP/2 or TLS with the chosen features.

### Option B — raw hyper

Fewest dependencies. Cons: routing, JSON extraction, rejection handling and
shutdown become our code, with no gain.

### Option C — actix-web

Mature and fast. Cons: its own actor/runtime model next to tokio and a larger
dependency tree.

### Metrics: hand-rolled exposition vs a metrics crate

Six series rendered from values read at scrape time (`metrics::render`) vs
`prometheus` / `metrics` + exporter. Hand-rolled keeps one source of truth
(no duplicated counters) and no dependency, until histograms are needed.

## Decision

- `axum = 0.8`, `default-features = false`, features `http1`, `json`,
  `tokio`. `tower 0.5` (`util`) is a dev-dependency only, for `oneshot`.
- Routes (`app::router`): `GET /livez` (always `ok`, never checks
  dependencies), `GET /readyz` (200/503 from `AppState::ready`),
  `GET /healthz` (JSON: readiness, uptime, running/queued, pool bytes),
  `GET /metrics`, `POST /v1/sql` (`{sql, user, resource_group?}`).
- Error contract for `/v1/sql`: `{"error": {"code", "message", "retryable",
  "query_id", "dependency"?}}`. The full class table (400 / 422 / 429 / 502 /
  503 / 504 / 500) and the rule "only `internal` means Plimsoll is wrong" are
  in ADR 0013, which refines this ADR. 422 is deliberate for requests that are
  valid but cannot complete (ceiling, unsupported or broken table); a 5xx
  would signal an unhealthy node.
- Shutdown: SIGTERM or ctrl-c flips `ready` to false, the listener stops
  accepting, in-flight requests finish. SIGTERM drains first since ADR 0026.
- Metrics (`text/plain; version=0.0.4`): `plimsoll_pool_used_bytes`,
  `plimsoll_pool_limit_bytes`, `plimsoll_spill_bytes_total`,
  `plimsoll_queries_running`, `plimsoll_queries_queued`,
  `plimsoll_queries_total{state}`.
- The outbound HTTP client (catalog) is `reqwest` with rustls; see ADR 0008.

Why over raw hyper: the same transport with routing, extraction and shutdown
already solved, for a small set of extra crates.

## Consequences

**Positive.** Handler tests run in-process; the error contract is
table-tested. Metrics never disagree with their owners.

**Negative (accepted debt).**
- No HTTP/2 and no in-process TLS: expected behind an ingress/proxy.
- No drain delay between "not ready" and closing the listener (`ponytail:`
  in `shutdown_signal`); load balancers may see refused connections.
  Resolved by ADR 0026.
- `user` in the request body was trusted as sent (`ponytail:`): no authN.
  Resolved by ADR 0021 (bearer tokens; `user` = principal).
- Results are JSON rows built from Arrow's JSON writer; no Arrow IPC.
- No latency histograms; logs are `eprintln!`, no tracing subscriber.

**Operational.** On-call reads `/readyz` for routing, `/healthz` for a
one-shot status, `/metrics` for trends. A rising `plimsoll_queries_total{state="failed"}`
with steady `/livez` is the designed failure mode, not an outage.

## Risks and mitigations

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| Spoofed `user` bypasses per-user admission | High (if exposed) | Medium | Deploy behind an authenticating proxy; derive user from a principal before multi-tenant use |
| Connections refused during rollout | Medium | Low | Add `PLIMSOLL_DRAIN_DELAY_MS` (upgrade path in code) |
| Large JSON responses outside the ceiling | Medium | Medium | Stream results / Arrow IPC (ADR 0003 debt) |

## Validation

`plimsoll_server::app::tests`: `health_endpoints_follow_readiness`,
`metrics_expose_pool_and_query_totals`, `sql_returns_rows`,
`sql_maps_errors_to_typed_responses`, `sql_rejected_when_not_ready`,
`sql_queue_full_returns_429`, `sql_timeout_fails_the_query_not_the_node`,
`real_engine_oom_is_query_only_and_visible_in_system_table`;
`plimsoll_server::metrics::tests::renders_exposition_lines`. All run in CI
job `gates`.

## Revisit when

- Clients need streaming results, Arrow Flight / IPC, or HTTP/2.
- Latency histograms are required (adopt `prometheus-client` or OTLP).
- The server is exposed without an authenticating proxy.

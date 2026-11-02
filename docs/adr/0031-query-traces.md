# ADR 0031 — Query traces over OTLP/HTTP JSON

- Status: Accepted
- Date: 2026-11-01
- Owner: JoaoLucas2210
- Related: ADR 0019 (phase timings: the spans' source), ADR 0027 (structured logs; this is its "OTLP traces" revisit item), ADR 0015 (one HTTP/TLS stack), ADR 0004 (dependency stance)
- Scope: `plimsoll-server` (`trace`, `app::sql`, `config::otlp`, `log::QueryLog::trace_id`, `main`)

## Context

Plimsoll is one hop in a longer request:

1. a BI tool or service;
2. Plimsoll;
3. the catalog, object storage and Postgres.

When a dashboard is slow, the operator's tool is a trace backend (Tempo,
Jaeger, Honeycomb, Datadog), and Plimsoll was invisible in it:

- no span of its own;
- no way to join the caller's trace;
- logs (ADR 0027) with nothing to correlate them with a trace.

Plimsoll already measures the phases of every query (ADR 0019): queue wait,
planning, execution and serialization. What is missing is a trace identity
and an export.

## Decision drivers

1. **Join the caller's trace.** W3C Trace Context (`traceparent`) in and
   out, on every request, including rejections.
2. **Log ↔ trace correlation**, even when no collector is configured.
3. **Never on the hot path.**
   - Export is asynchronous and bounded.
   - A slow or down collector must not slow or fail a query (the
     invariant).
   - Shutdown flushes, but never hangs.
4. **Standard configuration.** A deployment's existing `OTEL_*`
   environment applies unchanged.
5. **Dependency budget** (ADR 0004). The OpenTelemetry Rust SDK +
   `opentelemetry-otlp` + `tracing-opentelemetry` is a large tree with
   frequent breaking releases. It should come in only when it buys
   something the server cannot do in a page of code.

## Considered options

**A. Hand-built spans from the measured phases, exported as OTLP/HTTP
JSON with the existing reqwest client (chosen).**
- **Built after the request.** The request handler (`app::sql`) already
  knows each phase's duration, so the spans are assembled once the
  request ends: one `query` span (kind SERVER) and one child (INTERNAL)
  per phase that ran.
- **Exported in batches.** A bounded queue (4096 spans) feeds one exporter
  task. It posts batches of up to 512 spans to `<endpoint>/v1/traces`.
- **No new crate.** reqwest is already in the binary for the catalog
  (ADR 0015); the server crate only adds it as a direct dependency, with
  the same version and features.

**B. `tracing` + `tracing-opentelemetry` + `opentelemetry-otlp`.**
- **For:** this is the idiomatic stack. It gives in-process spans at any
  depth (per operator, per fetch) and OTLP metrics.
- **Against:** it adds the SDK, the exporter, a tonic or hyper transport,
  and a subscriber stack for a single request-level span that we can
  build from data we already have.
- **Verdict:** this is the upgrade path, not the starting point.

**C. OTLP/gRPC.** Every mainstream collector accepts OTLP/HTTP too
(port 4318), and gRPC would bring tonic and prost. Rejected for now.

**D. Logs only (`trace_id` in the line, no export).** This covers
correlation but not the waterfall view. We keep it as the behavior when
no endpoint is configured.

## Decision

- **Trace context, always on.**
  - `TraceContext::from_traceparent` continues a valid header: version
    `00`, lowercase hex, non-zero ids, `ff` rejected, extra fields allowed
    on later versions. Otherwise it starts a new trace.
  - An invalid header is ignored, as the W3C spec requires.
  - Every `/v1/sql` response carries a `traceparent` naming this request's
    span. The `query` log line carries `trace_id`.
- **Sampling.** We are parent-based. When the caller sends a header, its
  sampled flag decides whether we export. When it does not, we sample
  every request. That makes the default cost one span per query plus one
  per phase, about five spans.
- **Export.** It is enabled by `OTEL_EXPORTER_OTLP_ENDPOINT` (an
  `http(s)://` base URL, validated at boot). `OTEL_EXPORTER_OTLP_HEADERS`
  (`k=v,…`, validated, never logged) and `OTEL_SERVICE_NAME` (default
  `plimsoll`) are also read.
- **Span content.**
  - The `query` span carries `http.request.method`, `http.route`,
    `http.response.status_code`, `plimsoll.code`, `plimsoll.query_id`,
    `enduser.id`, `plimsoll.resource_group` and `plimsoll.sql_sha256`.
  - Never SQL text (ADR 0027 driver 2).
  - Status is ERROR only for 5xx; a 4xx is the caller's fault, per the
    OTel HTTP server conventions.
- **Phase layout.** The phases are laid back to back and end when the
  request ends. The only time not covered is authentication and body
  parsing before the queue, which is sub-millisecond.
- **Back-pressure.** `record` uses `try_send`. When the queue is full,
  spans are dropped and counted. Export failures and drops are reported
  in a `trace_export_failed` warn line.
- **Shutdown.** After the listener closes, `Tracer::shutdown` closes the
  queue, the exporter posts what is left, and `main` waits at most 5 s
  (`trace_flush_timeout` otherwise).

## Consequences

**Positive**
- Plimsoll appears in end-to-end traces as a span with its phase
  waterfall. Queue wait (admission) is now visible next to the caller's
  own latency.
- `trace_id` connects a log line, a trace and, through `query_id`,
  `system.runtime.queries`.
- Clients that send no `traceparent` still get one back, to quote in a
  bug report.
- Zero new crates; the binary's dependency tree is unchanged.

**Negative (accepted debt)**
- **Request-level only.** No spans per operator, per manifest fetch or
  per catalog call; their cost is visible in aggregate under `planning`
  and `execution`.
- **No OTLP metrics.** `/metrics` (Prometheus format) remains the
  metrics path; collectors can scrape it.
- **Phase start times are reconstructed.** Durations are exact; start
  instants are offset by the sub-millisecond pre-queue work.
- **Header values are taken verbatim.** The spec allows %-encoding,
  which is not decoded.
- **No `tracestate` passthrough and no trace-id ratio sampler.**

**Operational**
- To find a slow query's trace from a log line: search the trace backend
  for the line's `trace_id`.
- A collector outage costs warn lines and dropped spans, never query
  latency.

## Risks and mitigations

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| Collector down or slow | Medium | Low | Bounded queue, non-blocking `try_send`, 10 s HTTP timeout per batch, drops counted and logged. |
| Export key leaked through logs or config errors | Low | High | Headers are excluded from `Debug` and from config error messages (tested); the boot line logs the endpoint only. |
| Untrusted `traceparent` used to pollute traces | Medium | Low | It only sets ids and the sampled flag; malformed headers are ignored; ids are validated hex. |
| Flood of 4xx requests exports many spans | Low | Low | Bounded queue; the caller can unset the sampled flag; a ratio sampler is a revisit item. |

## Validation

- `trace::tests::traceparent_is_continued_or_started`:
  - continuing a trace, with a new span id;
  - echoing the flags;
  - unsampled callers;
  - future versions with extra fields;
  - seven malformed headers that start a fresh trace.
- `trace::tests::query_spans_nest_phases_back_to_back_inside_the_request`:
  - the span names and parent links;
  - the 5xx error status, and no error status for a 4xx;
  - nanosecond timestamps, with the phases ending at the request end;
  - integer attributes encoded as strings (OTLP JSON).
- `trace::tests::exporter_posts_otlp_json_and_flushes_on_shutdown`: a real
  HTTP collector (axum on a loopback port) receives the batch.
  - It carries the configured header and `service.name`.
  - It is delivered by `shutdown`.
- `config::tests::otlp_export_uses_the_standard_otel_variables`:
  - defaults and the parsing of the variables;
  - fail-fast on an invalid endpoint or header;
  - no secret appears in `Debug` or in errors.
- `app::tests::sql_continues_the_callers_trace`:
  - the response's `traceparent` keeps the caller's trace id, with a new
    span id;
  - a request without one gets one back.

## Revisit when

- Per-operator or per-fetch spans are needed: move to `tracing` +
  `opentelemetry-otlp`, keep the span names and attributes defined here.
- Metrics must be pushed (no scraper available): OTLP metrics.
- Trace volume needs head sampling beyond parent-based (a trace-id ratio).

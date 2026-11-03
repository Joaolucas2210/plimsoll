# ADR 0100 — OTLP metrics export

- Status: Accepted
- Date: 2026-11-03
- Owner: JoaoLucas2210
- Related: ADR 0019 (latency histograms), ADR 0031 (OTLP traces), ADR 0036/0038 (groups, result cache)
- Scope: `metrics::{spawn_export, to_otlp}`, `AppState::exposition`, `trace::post`, `OTEL_METRIC_EXPORT_INTERVAL`, `OTEL_METRICS_EXPORTER`

## Context

Metrics were only pulled, as Prometheus text from `/metrics`. Deployments
that run an OpenTelemetry collector and no Prometheus scraper had traces
(ADR 0031) but no metrics, unless they added a scraping receiver per node
and kept its target list in step with the cluster.

## Decision drivers

1. **One source of series.** `/metrics` and the push must never drift:
   same names, labels and values.
2. **No OpenTelemetry SDK.** Same choice as ADR 0031: the opentelemetry
   crates are a large tree for what is a periodic POST of JSON with the
   reqwest client already in use.
3. **The standard variables.** A deployment's existing `OTEL_*`
   settings apply unchanged.

## Decision

- **Push what `/metrics` renders.** With `OTEL_EXPORTER_OTLP_ENDPOINT`
  set, a task renders the exposition text and parses it back into OTLP
  metrics. The upload is a JSON POST to `<endpoint>/v1/metrics`, with
  `OTEL_EXPORTER_OTLP_HEADERS` and `service.name` as for spans.
  - It runs every `OTEL_METRIC_EXPORT_INTERVAL` ms (the SDKs' variable,
    default 60000).
  - `OTEL_METRICS_EXPORTER=none` keeps traces and turns metrics off.
- **Mapping.**
  - `gauge` → OTLP gauge.
  - `counter` → monotonic cumulative sum, starting at process start.
  - `histogram` → explicit-bucket cumulative histogram: Prometheus'
    cumulative `le` buckets become per-bucket counts, and `+Inf` becomes
    the last count, not a bound.
  - Labels become string attributes. Integers are sent as `asInt` and
    anything else as `asDouble`.
  - Metrics with no samples yet (no resource groups, no errors) are
    left out.
- **Shutdown flushes.** After the listener drains, one last push is
  made, under the same 5 s deadline as the span flush. Counters of a
  short-lived node are not lost between ticks.
- A failed push is logged (`metrics_export_failed`) and the next tick
  sends fresh cumulative values. Nothing is buffered or retried, since
  cumulative values make a lost push harmless.

## Consequences

**Positive**
- A collector-only deployment gets every Plimsoll metric, with no
  per-node scrape configuration.
- Adding a series to `render` adds it to both outputs.

**Negative (accepted debt)**
- **Parsing our own text** is a round trip. It only has to read what
  `render` writes, which its test pins. A structured registry replaces
  both when metrics outgrow the hand-rolled exposition (upgrade path in
  `metrics.rs`).
- **JSON only, no protobuf.** Every OTLP/HTTP receiver accepts JSON;
  protobuf would need prost and the OTLP protos.
- **Not validated against a real collector in this change.** The format
  follows the OTLP JSON mapping (camelCase fields, 64-bit integers as
  strings) that the trace export uses.

## Validation

- `metrics::tests::exposition_becomes_otlp_metrics` covers:
  - gauges with labels and descriptions;
  - counters as monotonic sums with a start time;
  - per-phase histograms with per-bucket counts, bounds, count and sum;
  - empty metrics left out.
- `metrics::tests::labels_with_escapes_are_parsed` covers escaped quotes
  in label values.
- `config::tests::metrics_are_pushed_every_minute_unless_disabled`
  covers the default, an override, `none` and rejecting `0`.
- By hand, a server with a 2 s interval pointed at a local HTTP
  collector:
  - pushed `/v1/metrics` on every tick and once more on SIGTERM, after
    draining;
  - the last push carried the request's `invalid_request` error count
    and the latency histograms by phase;
  - the trace of the same request went to `/v1/traces`.

## Revisit when

- A collector needs protobuf, or delta temporality (some vendors).
- Per-worker or per-fragment metrics are added (ROADMAP): they may want
  their own instruments rather than more exposition lines.

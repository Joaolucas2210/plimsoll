# ADR 0119 — Fragments served, by kind, in /metrics

- Status: Accepted
- Date: 2026-11-04
- Owner: JoaoLucas2210
- Related: ADR 0019 (metrics), ADR 0059 (worker governance), ADR 0069/0071 (worker visibility), ADR 0100 (OTLP metrics), ADR 0118 (per-query spill)
- Scope: `FragmentCounts`, `metrics::FragmentStats`, `plimsoll_fragments_total`, `plimsoll_fragment_rows_total`, `plimsoll_fragment_bytes_total`

## Context

A worker's load was visible only in its logs: one `fragment` line per
request. `/metrics` showed its memory pool and admission, which it
shares with any coordinator role, but not what it served. A dashboard
could not tell a worker busy with scans from one held up by shuffle
reduces, or see failing fragments rise, without a log pipeline.

## Decision drivers

1. **Counted where the log line is written**, so the counts and the
   lines cannot disagree.
2. **A closed set of kinds**, so label cardinality stays bounded.
3. **Pushed with everything else**: the OTLP export (ADR 0100) parses
   `/metrics`, so new series need no code there.

## Decision

- **`FragmentCounts`** on `AppState`, per fragment kind:
  - requests that ended well (`ok`) and those that failed (`error`);
  - rows and Arrow IPC bytes sent back.

  The kinds are `scan`, `partial_aggregate`, `shuffle_map`,
  `aggregate_reduce`, `broadcast_probe`, `release`, and the other
  static names the `fragment` line already uses.
- **Counted by `FragmentReport::drop`**, the same place that writes the
  `fragment` line and its span. A request refused before it ran (a full
  `fragments` group answering 429) wrote no line and counts nothing.
  Admission's own counters cover refusals.
- **Exposed** as:
  - `plimsoll_fragments_total{kind, outcome}`;
  - `plimsoll_fragment_rows_total{kind}`;
  - `plimsoll_fragment_bytes_total{kind}`.

  All three are counters. Through ADR 0100 they become cumulative OTLP
  sums, and they are left out until the node has served a fragment.

## Consequences

**Positive**
- Per worker and kind: request rates, error rates, and rows and bytes
  per second, straight from `/metrics` or the OTLP push.
- A coordinator that never serves fragments shows none of these series.

**Negative (accepted debt)**
- **No per-kind latency.** A histogram per kind would multiply series
  by the bucket count. The `fragment` lines and spans carry durations.
- **Requests refused at admission are not here**; they show in the
  admission figures instead.

## Validation

- `metrics::tests::renders_exposition_lines`: the three series, with
  `ok` and `error` outcomes and rows and bytes per kind.
- `metrics::tests::exposition_becomes_otlp_metrics`: with no fragments
  served, the series are left out of the OTLP push.
- `tests/distributed.rs::workers_govern_fragments_and_report_health`
  (Spark fixtures): a worker counts two scans served with 64 rows. The
  scan it refused with 429 never ran and is not counted. Its `/metrics`
  text shows `plimsoll_fragments_total{kind="scan",outcome="ok"} 2`.

## Revisit when

- Kind latencies are needed in dashboards: one histogram over all
  kinds, or per kind with fewer buckets.

# ADR 0027 — Structured JSON logs

- Status: Accepted
- Date: 2026-10-30
- Owner: JoaoLucas2210
- Related: ADR 0013 (resolves its "log `code` per failed query" revisit item), ADR 0019 (phase timings), ADR 0021–0023 (identity, group), ADR 0004
- Scope: `plimsoll-server` (`log`, `app::{sql, run_sql, ApiError::respond}`, `main`, `lakehouse`)

## Context

The server logged with `eprintln!`: a dozen free-text lines at boot and
shutdown, and **nothing per query**. To answer "which queries failed for
user X with `dependency_failed` in the last hour", an operator had two
places to look, and neither worked:

- `system.runtime.queries`: in-memory, capped at 1 000 entries, lost on
  restart, and now scoped per principal (ADR 0022);
- the metrics: aggregates without identity.

Log pipelines (Loki, CloudWatch, Elastic) need one parseable record per
event.

## Decision drivers

1. One machine-parseable record per `/v1/sql` request, **including
   rejections**: 401, 403, 429 and invalid requests are exactly what
   security and capacity reviews look for.
2. Never log SQL text. Queries carry literals such as customer ids and
   emails, and logs have broader access and longer retention than the
   registry.
3. No interleaving under concurrency; logging must never fail a request.
4. No new dependency for ~100 lines (ADR 0004's stance). A `tracing` stack
   arrives with OTLP spans, not before.

## Considered options

**A. Hand-rolled JSON lines with `serde_json` (chosen).** `log::emit(level,
event, fields)` builds the object and writes it with one `write_all` on the
locked stderr. A `QueryLog` is filled as the handler learns facts, and a
thin wrapper (`sql` → `run_sql`) emits it for every outcome.
`ApiError::respond` stores `(code, message)` as a response extension, so the
wrapper learns the outcome without changing the eight return paths.

**B. `tracing` + `tracing-subscriber` (JSON).** This is the standard and
gives spans for free, but it adds two crates and a subscriber now, before
OTLP needs them. It is the upgrade path, and `log::emit` call sites map
one-to-one onto `tracing` events.

**C. Log from `ApiError::respond` and the success path separately.** That
duplicates field assembly and misses the user and group on error paths that
know them. Rejected.

## Decision

- Every line is `{"ts_ms", "level", "event", …fields}` on stderr, one
  object per line.
- The `query` event is emitted for every `/v1/sql` request.

  | Field | Meaning |
  |---|---|
  | `status` | HTTP status |
  | `code` | `ok` or the ADR 0013 code |
  | `message` | Error text, truncated to 512 chars |
  | `query_id` | Set when the request was admitted |
  | `user`, `resource_group` | Once resolved: authenticated or bound |
  | `sql_sha256` | First 16 hex chars of SHA-256, to correlate with `system.runtime.queries`; absent for unauthenticated requests |
  | `sql_bytes` | SQL length |
  | `queue_ms`, `planning_ms`, `execution_ms`, `serialization_ms`, `total_ms` | Phase timings at µs precision; null when the phase did not happen |

- **Levels.** 5xx is `error`, 4xx is `warn`, everything else is `info`.
- **Lifecycle events.**
  - `listening` (address, catalog, auth summary);
  - `auth_disabled` and `grants_not_enforced` (warn);
  - `catalog_registered` and `env_tables_registered`;
  - `namespace_skipped`;
  - `draining`, `closing_listener`, `stopped`;
  - `startup_failed` and `signal_handler_failed` (error).
- No `eprintln!` remains in production code.

## Consequences

**Positive**
- Per-user and per-code forensics survive restarts and the registry cap.
  `code="forbidden"` lines with `user` show attempted access outside grants.
- Latency outliers can be traced to a request (`total_ms`, phase split),
  complementing the ADR 0019 histograms.
- No SQL text, no tokens, no digests in logs. The auth summary lists users
  only.

**Negative (accepted debt)**
- Keys are emitted in alphabetical order (`serde_json` without
  `preserve_order`). That is harmless for collectors, but less readable by
  eye.
- No log levels filter or sampling. At very high QPS the `query` line is
  per request; sample or aggregate in the pipeline.
- No request ids or trace context; the `query_id` exists only once a
  request is admitted.
- Truncated messages can cut a long upstream error.

**Operational**
- Example query, Loki: `{app="plimsoll"} | json | code="dependency_failed"
  | line_format "{{.user}} {{.message}}"`.
- `sql_sha256` joins log lines to `system.runtime.queries` for authorized
  viewers who need the text.

## Risks and mitigations

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| SQL text leaks into a message (a planner error quoting the query) | Medium | Medium | Messages are truncated; most DataFusion errors quote identifiers, not literals. Revisit if literals appear; a scrubber can go into `QueryLog::fields`. |
| Interleaved lines under load | Low | Low | One `write_all` per line under the stderr lock. |
| Logging overhead on hot path | Low | Low | One small JSON object per request; no allocation in the success path beyond it. |

## Validation

- `log::tests::query_lines_carry_identity_outcome_and_timings_but_no_sql`:
  - fields and levels;
  - µs-precision timings, with null for phases that did not run;
  - a 16-hex fingerprint;
  - **the SQL literal is absent** from the line;
  - message truncation.
- Manual smoke on the binary: a successful query, an unauthenticated one
  (401) and an unknown table (400) each produced one `query` line with the
  right code, user and timings. The email literal in the SQL appears in no
  line, every line parses as JSON, and the lifecycle events run from
  `listening` to `stopped`.

## Revisit when

- OTLP traces land: move to `tracing` (spans for queue, planning and
  execution) and keep these event names.
- Log volume requires sampling or level filters.
- A field needs per-deployment redaction rules.

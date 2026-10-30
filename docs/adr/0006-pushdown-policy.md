# ADR 0006 — Per-source pushdown policy

- Status: Accepted
- Date: 2026-10-29
- Owner: JoaoLucas2210
- Related: ADR 0005, ADR 0007, ADR 0003
- Scope: `plimsoll_core::federation::PushdownPolicy`, `plimsoll_connectors::{parquet, postgres, iceberg}`

## Context

Whether a predicate can be evaluated remotely with *identical* semantics
depends on the source and on the column type, not on the operator alone.
`a = 'x'` is collation-sensitive; `x < 0.0` differs between Postgres
(`-0.0 = 0.0`) and Arrow (IEEE total order); `LIKE` escaping and case folding
differ; timestamps carry timezone and precision rules. A single global switch
is either unsafe or useless. ADR 0005 needs a per-source answer to "trusted,
re-checked, or local" for each filter.

## Decision drivers

1. Never drop a row the engine would keep (correctness).
2. Type-aware: same operator, different decision per column type.
3. Trust lives on Plimsoll's side, not in the driver.
4. Widening is cheap, explicit and reviewable.
5. No SQL injection surface in generated remote SQL.

## Considered options

### Option A — Typed per-class policy struct (chosen)

One field per expression class, each a `TableProviderFilterPushDown`. Pros:
drivers 1–4; a widening is a one-line diff. Cons: classes are coarse
(e.g. all text comparisons share one decision).

### Option B — Per-operator allow-list without types

Cannot express "integer `>` ok, text `>` not".

### Option C — Source self-describes capabilities

The driver does not know the engine's semantics; trust would move to the
untrusted side.

### Postgres client — `tokio-postgres` vs `sqlx` / `diesel`

`tokio-postgres` is the async wire client with no ORM, pool or macros; `sqlx`
and `diesel` bring compile-time checking and pools for a single dynamic
`SELECT`.

## Decision

`PushdownPolicy` fields: `integer_cmp`, `float_cmp`, `decimal_cmp`,
`temporal_cmp`, `bool_cmp`, `text_cmp` (classified by the column's Arrow type
for `col <op> literal` / `literal <op> col`, ops `= <> < <= > >=`), `like`
(`LIKE`, `ILIKE`, `SIMILAR TO`), `null_check` (`IS [NOT] NULL` on a plain
column), `other` (casts, functions, `IN`, column-vs-column, anything
unrecognised) and `limit: bool`. `AND`/`OR` take the weakest of their parts;
`NOT` takes its operand's decision. Unknown column → `Unsupported`.

Constructors: `local_only()` (default, everything `Unsupported`, no limit) and
`uniform(d)` (`limit` on unless `d` is `Unsupported`). Aggregates are never
federated: a `TableProvider` cannot receive them.

Shipped policies:

| Connector | Policy | Reason |
|---|---|---|
| `ParquetConnector` (`ListingTable`) | `uniform(Inexact)` | Row-group/page pruning is a hint; everything re-checked |
| `IcebergConnector` | `uniform(Inexact)` | Same; filters become a Parquet pruning predicate (ADR 0007) |
| `PostgresConnector::default_policy()` | `integer_cmp`, `bool_cmp`, `null_check` = `Exact`, `limit = true`, rest `Unsupported` | Floats not even `Inexact`: Postgres could drop rows Arrow keeps |

Postgres SQL generation (`postgres::sql`): identifiers always double-quoted
with `"` doubled (`quote_ident`); literals always bind parameters with an
explicit cast (`$n::int8|float8|bool|text`), never inlined. Untranslatable
filters are `Unsupported` from the source and skipped in `build_query` without
leaking parameters. Connection string only from `PLIMSOLL_PG_URL`
(`PG_URL_ENV`; missing → `ConnectorError::MissingEnv` at construction).
Unsupported column types → `ConnectorError::UnsupportedType` at construction.
`PostgresConnector::with_policy` overrides per table. Dependency:
`tokio-postgres 0.7` (`runtime`, `with-chrono-0_4`); `chrono` is already in
the Arrow tree.

Why over Option B: the type dimension is exactly where engines disagree; an
untyped allow-list would force `Unsupported` everywhere or be unsafe.

## Consequences

**Positive.** Pushdown is narrow and every widening is a reviewed one-liner
plus a semantics test. `EXPLAIN` prints `PostgresScanExec: sql=..., params=n`.

**Negative (accepted debt, `ponytail:` in code).**
- One fresh Postgres connection per scan, no pool, **no TLS** (`NoTls`).
- Whole remote result fetched into one `RecordBatch`, **not reserved in the
  `CeilingPool`** (ADR 0003): a large remote result can exceed the ceiling.
- `NUMERIC` travels as text and surfaces as `Utf8`.
- Supported Postgres types: int2/4/8, float4/8, bool, text/varchar/bpchar/name,
  numeric, date, timestamp, timestamptz.

**Operational.** A missing `PLIMSOLL_PG_URL` or an unsupported column fails at
boot (env-registered tables, `PLIMSOLL_PG_TABLES`). Remote errors surface as
query failures (HTTP 500 `internal`). The Postgres URL is redacted in `Debug`.

## Risks and mitigations

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| Widening a class introduces silent row loss | Medium | High | Lossy classes stay `Unsupported`; ADR 0005 superset rule; semantics test per widening |
| Large Postgres result OOMs the node (unaccounted batch) | Medium | High | Upgrade: `query_raw` streamed in fixed batches under the ceiling |
| Plaintext credentials on the wire | High (non-local PG) | High | Use only on trusted networks until rustls lands |
| Connection storm (one per scan) | Medium | Medium | Admission limits cap concurrency; upgrade: pool |

## Validation

- `plimsoll_core::federation::policy::tests`: `federation_default_is_local_only`,
  `federation_policy_classifies_by_column_type`, `federation_compound_takes_weakest`.
- `plimsoll_connectors::postgres::sql::tests`: `sql_quotes_identifiers`,
  `sql_literals_are_parameters_not_text`, `sql_federated_where_and_limit`,
  `sql_untranslatable_filters_are_skipped_without_leaking_params`.
- `plimsoll_connectors::postgres::tests`: `pg_federates_integer_comparison`,
  `pg_text_like_and_float_stay_local`, `pg_from_env_fails_fast_without_url`.
- CI job `postgres-live` (Postgres 16 service): `pg_live_roundtrip`.
- `plimsoll_connectors::parquet::tests::parquet_connector_scans_and_filters`.

## Revisit when

- A workload needs text, temporal or float pushdown to Postgres (add a
  collation/timezone-aware class with its own semantics tests).
- Remote result sizes approach `PLIMSOLL_MEMORY_LIMIT_BYTES / 8`, or any
  non-local Postgres is used (streaming + TLS + pool become mandatory).
- Aggregate or join pushdown is required (plan-level federation, new ADR).

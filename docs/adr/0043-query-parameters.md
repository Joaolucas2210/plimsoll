# ADR 0043 — Query parameters

- Status: Accepted
- Date: 2026-11-02
- Owner: JoaoLucas2210
- Related: ADR 0024 (read-only API), ADR 0027 (logs never carry SQL literals), ADR 0022 (registry visibility), ADR 0038 (result cache key)
- Scope: `plimsoll_core::engine::{bind, Engine::{sql_with, sql_stream_with}}`, `plimsoll-server::{app::{SqlRequest::params, param}, engine::QueryEngine}`

## Context

Clients building queries from user input (dashboards with filters, APIs
in front of Plimsoll, notebooks) had one option: format values into the
SQL string. That is the classic injection vector. The read-only API
(ADR 0024) bounds the damage to reading, but within the caller's grants
an injected `OR '1'='1'` still reads rows the application meant to
filter out. Formatted values also:

- **Land in the registry.** `system.runtime.queries` keeps the SQL text,
  and operators see it (ADR 0022). Values such as emails and customer ids
  become visible.
- **Defeat statement-level reuse.** Every value is a different SQL text.

DataFusion supports placeholders (`$1`, `$2`, …) and binding values to a
planned statement.

## Decision drivers

1. Values are never part of the SQL text: no injection, by construction.
2. Clients send JSON. They should not need Arrow types: `42` must work
   against an `INT` column, and `"2024-05-01"` against a `DATE`.
3. Mistakes are errors, not surprises. A missing value, an extra value,
   or a value that does not fit its placeholder's type is a 400.
4. Every format works (`json`, `ndjson`, `arrow`), and so does the result
   cache, which must not serve one parameter's answer for another's.

## Considered options

**A. DataFusion placeholders, bound after parsing with casting to the
inferred types (chosen).**
- Parse and plan the SQL. Read the planner's inferred placeholder types
  (`get_parameter_types`).
- Cast each JSON value to its placeholder's type, then
  `with_param_values`.
- Binding happens before the grant check, subquery pre-evaluation and
  optimization, so everything downstream sees ordinary literals.

**B. Client-side escaping libraries.** These are the client's problem,
and every client gets it wrong eventually. Rejected.

**C. Server-side SQL templating** (`{{value}}` substitution). That is
injection with extra steps. Rejected.

## Decision

- **Request.** `{"sql": "… WHERE id = $1 AND day = $2", "params": [42,
  "2024-05-01"]}`. JSON scalars map to SQL values:

  | JSON | SQL value |
  |---|---|
  | integer | `Int64` (`UInt64` beyond `i64`) |
  | float | `Float64` |
  | string | `Utf8` |
  | boolean | `Boolean` |
  | null | `NULL` |

  Arrays and objects answer `400 invalid_request`.
- **Binding** (`engine::bind`).
  - The number of distinct placeholders must equal the number of values.
  - A value is cast to its placeholder's inferred type when that type is
    known. A typed NULL is used for null.
  - An uncastable value (`"not a date"` for a `DATE`) is a `Plan` error,
    so `400 invalid_query`. So is an arity mismatch, or `$n` beyond the
    values.
- **Exposure.** Values are not in the SQL text, so they are absent from:
  - the registry and `system.runtime.queries`;
  - the log's `sql_sha256` (computed from the text);
  - traces.

  They exist only in the plan.
- **Result cache** (ADR 0038). The key is the SQL plus the bound values,
  so the same statement with other values is another entry.
- **APIs.** `Engine::sql_with` and `sql_stream_with` take
  `params: &[ScalarValue]`, and the server's `QueryEngine::{execute,
  stream}` pass them through.

## Consequences

**Positive**
- Applications can pass user input safely. In the test, an injection
  string matched nothing, because it is just a value.
- PII in filter values stays out of the registry and the logs.
- Dashboards can send one statement text with varying values, and the
  cache keys on both.

**Negative (accepted)**
- **Positional parameters only** (`$1`). Named parameters (`$name`) are
  supported by DataFusion but not exposed.
- **Placeholders whose type the planner cannot infer** (e.g. `SELECT $1`)
  are bound with the JSON value's own type.
- **The registry shows the statement without values,** which sometimes
  makes debugging harder. That is the point (driver 1).

## Validation

- `engine::tests::binds_parameters_as_values`:
  - an `Int64` value binds to an `INT` column and a string to a `DATE`;
  - the injection string `x' OR '1'='1` matches 0 rows;
  - a NULL compares as SQL NULL;
  - missing values, extra values, an unused value and an uncastable value
    are `Plan` errors.
- `app::tests::params_bind_values_not_sql`:
  - parameters work in `json` and `ndjson`;
  - the injection attempt is treated as data;
  - non-scalar params give 400 `invalid_request`, and an arity mismatch
    gives 400 `invalid_query`;
  - the registry never contains the value.
- `result_cache::tests::caches_pinned_results_until_the_snapshot_changes`:
  the same SQL with two parameter values returns two different results
  from two cache entries.

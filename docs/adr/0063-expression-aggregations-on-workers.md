# ADR 0063 — Expression keys and arguments in worker aggregations

- Status: Accepted
- Date: 2026-11-02
- Owner: JoaoLucas2210
- Related: ADR 0061 (partial aggregation), ADR 0062 (row filters as SQL), ADR 0004 (dependency stance)
- Scope: `portable::{physical_sql, logical}`, `PartialAggregatePushdown`, `AggregateSpec`, `CoreEngine::fragment`

## Context

ADR 0061 pushed partial aggregations to workers only when every group key
and every aggregate argument was a plain column. Dashboards often group
and sum computed values:

- `GROUP BY date_trunc('day', ts)`;
- `GROUP BY lower(region)`;
- `GROUP BY id % 16`;
- `sum(price * qty)`.

Each of these fell back to shipping every row to the coordinator.

ADR 0062 already moves filters to workers as SQL text, accepted only when
the text round-trips. The same tool applies here, with one difference.
The physical rule only sees **physical** expressions, and DataFusion has
no unparser for those. Its `fmt_sql` output is meant for people: types are
printed as `Int64` and string literals are not quoted.

## Decision drivers

1. **Same answers.** Never ship an expression the worker would plan
   differently.
2. **No new dependency** (`datafusion-proto` stays out, ADR 0004).
3. **Small surface.** Cover what aggregations actually key on, and refuse
   the rest.

## Decision

- **Physical to logical, by hand, for a closed set of nodes.** These are
  `Column`, `Literal`, `BinaryExpr`, `CastExpr`, `TryCastExpr`, `Not`,
  `Negative`, `IsNull`, `IsNotNull` and `ScalarFunctionExpr`. Anything
  else (CASE, IN lists, LIKE, subquery results) returns `None`, and the
  aggregate is not pushed. That is safe, and only slower.
- **Then the logical unparser** (`expr_to_sql`). It quotes identifiers
  and renders types and literals as real SQL.
- **Round-trip check, at the physical level.** The coordinator parses the
  text against the scan's schema with built-in functions, coerces and
  simplifies it, then plans it with `create_physical_expr`. The result
  must **equal** the original physical expression.
  - Function nodes compare their session options as well. So the
    expression is re-planned under the coordinator's session config,
    which the rule receives. Under the default config they would never
    compare equal (the live test caught this; the unit test now uses a
    non-default session).
  - Volatile expressions are refused.
- **`AggregateSpec` carries SQL.** Its keys and arguments are now SQL
  expressions, not column names. Plain columns become quoted identifiers,
  so `"re""gion"` and mixed-case names still work. The worker parses them
  with `DataFrame::parse_sql_expr` over the table, after the fragment's
  filters (ADR 0062).
- **Unchanged.** `count(*)` is still sent with no argument. Only
  `count`/`sum`/`min`/`max` are pushed, because their partial state is
  their value. `avg` is still not pushed; its state is (count, sum).

## Consequences

**Positive**
- Aggregations grouped by, or summing, expressions return partial states
  from workers instead of rows. Covered: arithmetic, casts, negation,
  null tests and scalar functions such as `lower`, `date_trunc` and
  `substr`.
- One module (`portable`) now owns every expression that crosses the
  wire. Filters and aggregations share the parser and the normalization.

**Negative (accepted debt)**
- **Config-sensitive functions use the worker's config.** Some functions
  read session options, for example the time zone. A worker configured
  differently from the coordinator could compute them differently.
  Coordinator and worker are expected to run the same configuration, the
  same assumption ADR 0062 makes for versions.
- **Hand-written mapping.** A node not in the list is not pushed. A future
  DataFusion node that the list wrongly accepted would be caught by the
  equality check, so not by luck.
- **No change to the wire format version.** `group_by` held column names
  before; it now holds SQL. An older worker reads a quoted identifier as
  SQL text and fails on it, which is safe. An unquoted plain name means
  the same in both readings. Mixed versions are still unsupported (ADR
  0062).

## Validation

- `portable::tests::aggregation_keys_and_arguments_plan_back_to_themselves`
  plans a real aggregation in a session with non-default options. It
  checks that each key and argument round-trips:
  - `lower("Name")` (a mixed-case identifier);
  - `small % 2` (an int32 column against an int64 literal);
  - `CAST(id AS INT)`;
  - `sum(id * 2)`, `max(-id)`, `count(id + 1)` and `min(small + 1)`.
- `distributed_scans_match_local_ones` (live, Spark-written MOR, EQ and
  DV tables):
  - `GROUP BY id % 3`, a `lower(<col>)` key with expression arguments,
    and a `CAST … % 2` alias key are all pushed (`aggregate=[…]`), with
    answers equal to the single-node ones;
  - `avg` is not pushed.

## Revisit when

- Common keys fall outside the node list (CASE, IN): extend the mapping.
- Expressions start failing the round-trip check: move to
  `datafusion-proto`.

# ADR 0062 — Row filters enforced by the Iceberg scan, on workers too

- Status: Accepted
- Date: 2026-11-02
- Owner: JoaoLucas2210
- Related: ADR 0055 (exact partition filters), ADR 0058 (fragments), ADR 0061 (partial aggregation), ADR 0045 (row filters)
- Scope: `iceberg::portable`, `IcebergTable::{supports_filters_pushdown, scan}`, `Fragment.filters`, `RemoteScanExec::with_filters`, `CoreEngine::fragment`

## Context

The Iceberg scan claimed `Exact` only for identity-partition filters (ADR
0055). Every other filter was `Inexact`: it went to Parquet for pruning,
and DataFusion kept a `FilterExec` above the scan. With workers, that had
two costs:

- a filtered scan shipped **every row** of the fragment to the
  coordinator, which then dropped most of them;
- `PartialAggregatePushdown` (ADR 0061) stops at a `FilterExec`, so
  `SELECT count(*) … WHERE …` was never aggregated on workers. That is the
  common dashboard query.

Moving the filter to the worker needs the filter to travel in the
fragment's JSON. DataFusion has no stable serialized form for expressions
without `datafusion-proto` (and prost), which ADR 0004 asks us to avoid.
It does have SQL: an unparser (`expr_to_sql`) and a parser.

## Decision drivers

1. **Same answers.** A filter the worker would read differently must
   never be claimed exact.
2. **No new dependency** (ADR 0004).
3. **Version skew fails loudly.** A worker that does not understand the
   filter must refuse the fragment. It must never return unfiltered rows.

## Considered options

**A. SQL text, accepted only when it round-trips (chosen).**
The filter is unparsed, then parsed back against the table schema with
built-in functions only, then coerced and simplified. It is accepted only
if the result *equals* the optimizer's expression, columns unqualified.
Anything else stays `Inexact`, as before: UDFs, volatile functions, or
anything the unparser renders lossily.

**B. `datafusion-proto`.** This is exact for every expression, but it
brings prost and a protobuf schema whose compatibility we would own.
Rejected for now. It is the upgrade path if A turns too much away.

**C. Keep the filters inexact and only send them to the worker for
pruning.** This cuts reading but not moving: the coordinator still
re-filters, and aggregation still stops at the `FilterExec`. It covers
neither cost above.

## Decision

- **Classification.** `supports_filters_pushdown` returns `Exact` for
  identity-partition filters (ADR 0055) and for portable filters
  (`portable::sql` returns `Some`). It returns `Inexact` otherwise. The
  scan classifies again with the same functions, so the two always agree.
- **Local enforcement.** The scan reads the filter columns too (the
  projection is widened). It skips the `limit` inside Parquet, because
  rows would be cut before the filter. It then applies a `FilterExec`
  over its union, and that filter projects back to the requested
  columns. Portable filters still go to Parquet for pruning.
  `count(*)` from manifests (ADR 0054) is never used when any non-partition
  filter is present.
- **Remote enforcement.**
  - `Fragment.filters` carries the SQL texts.
  - The worker builds `read_table → filter(parse_sql_expr) →
    select(columns) | aggregate(spec)` with the DataFrame API. Its own
    planner then pushes the filter into its local scan, which enforces it
    as above.
  - The non-aggregate path also moved to the DataFrame API, so there is
    one code path.
- **Skew.** `Fragment::from_json` rejects unknown fields. A coordinator
  newer than its worker gets a 400 (`worker version mismatch?`), which is
  not retryable. The query fails rather than return extra rows. Every
  future fragment field gets this protection, not just `filters`.
- **Effect on ADR 0061.** With no `FilterExec` above the scan, filtered
  aggregations are pushed to workers.

## Consequences

**Positive**
- Filtered scans move only matching rows. Filtered `count`/`sum`/`min`/
  `max` (grouped or not) return partial states from workers.
- The row filters of ADR 0045 are ordinary filters in the plan, so they
  are now enforced by the scan, on workers too, when portable. When not
  portable they stay where they were, above the scan.
- No new crate; one module (`portable`) of about 50 lines.

**Negative (accepted debt)**
- **Round-trip cost in planning.** Each filter is unparsed, parsed and
  simplified twice (classification and scan). That is microseconds per
  filter and not cached.
- **Expression projections are not pushed.** `GROUP BY id % 3` still
  ships rows: the worker would need to compute the key expression. The
  same SQL round trip would cover it. It is deferred until we know which
  expressions dashboards use.
- **Rejected filters are silent.** A non-portable filter stays inexact
  and works, only slower. There is no counter yet showing how often this
  happens.
- **Worker and coordinator must match versions.** Mismatches fail; there
  is no negotiation.

## Validation

- `iceberg::portable::tests::common_filters_round_trip_and_volatile_ones_do_not`
  plans real SQL and takes the optimizer's filters. It covers int64 and
  int32 comparisons, `LIKE`, `IN`/`OR`/`IS NULL`, date and decimal
  literals, `NOT BETWEEN`, scalar functions and arithmetic. All of these
  round-trip, and `random()` does not.
- `remote::tests::fragments_round_trip_as_json` checks two things. Filters
  survive the JSON round trip, and an unknown field is refused with the
  version hint.
- `distributed_scans_match_local_ones` (live, Spark-written MOR, EQ and
  DV tables):
  - `WHERE id > 3` and a filtered `GROUP BY` are now pushed
    (`aggregate=[…]`), carry `filters=[…]`, and have no `FilterExec`;
  - answers equal the single-node ones;
  - the local scan's own filtering is checked against an aggregate
    `FILTER (WHERE …)`, which never reaches the scan.
- The Iceberg interop suite is unchanged and passes. That includes the
  pruning tests, partition-exact filters and `count(*)` from manifests.

## Revisit when

- Rejected filters are common in practice: move to `datafusion-proto`
  (option B).
- `GROUP BY <expr>` or computed columns dominate distributed queries:
  push projections the same way.

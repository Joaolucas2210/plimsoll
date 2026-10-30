# ADR 0010 — Iceberg scan planning: manifest and file pruning

- Status: Accepted
- Date: 2026-10-30
- Owner: JoaoLucas2210
- Related: ADR 0003 (memory ceiling), ADR 0005 (federation correctness), ADR 0007 (Iceberg read path), ADR 0009 (object storage)
- Scope: `plimsoll-connectors::iceberg` (`iceberg/prune.rs`, `IcebergTable::data_files`, `scan`)

## Context

ADR 0007 planned a scan by decoding *every* manifest and handing *every* live
data file to DataFusion, then issued one `HEAD` per file to learn its size.
Parquet row-group statistics then skipped data inside files, but only after
each file was opened.

On a realistic table this is the dominant cost of a selective query:

- A day-partitioned table with a year of daily appends has ~365 manifests and
  thousands of files. `WHERE ts >= now() - 1 day` needs one of them.
- On S3 each manifest read and each `HEAD` is a round trip (10–50 ms). Planning
  latency grows linearly with table history, independent of the query.
- Manifest bytes are reserved in the `CeilingPool` while decoded (ADR 0007),
  so reading needless manifests also spends the node's memory budget.

Iceberg already records what is needed to avoid this: per-manifest partition
summaries (`partitions` in the manifest list) and per-file column bounds
(`lower_bounds` / `upper_bounds`) and sizes (`file_size_in_bytes`).

## Decision drivers

1. **Correctness.** Pruning may only skip data that provably cannot match. A
   wrong skip silently drops rows, which is the worst failure in a query engine.
2. **Planning cost proportional to the answer, not the history.** Fewer object
   store round trips, fewer pool reservations.
3. **Observability.** An operator must see what was skipped without a debugger.
4. **Small surface.** No new dependency; no second expression language.

## Considered options

**A. Leave it to DataFusion (status quo).** Parquet pruning only. Zero code.
Planning stays O(manifests + files) round trips. Rejected: this is the main
latency problem.

**B. iceberg-rust's expression evaluators.** Full Iceberg predicate
semantics (inclusive/strict projections, every transform). Requires the
`iceberg` crate, i.e. a second Arrow version (ADR 0007), and converting
DataFusion `Expr` into Iceberg expressions anyway. Rejected on dependency
weight.

**C. A narrow, provably-safe pruner over DataFusion `Expr` (chosen).**
Extract AND-ed `column <op> literal` and `BETWEEN` predicates, evaluate them
against manifest partition summaries (through monotonic transforms) and file
column bounds. Anything not understood keeps the file. ~400 lines including
tests.

**D. Statistics-only planning via DataFusion `Statistics`.** Expose bounds as
file statistics and let DataFusion prune. Works for files, not for manifests
(they must be skipped before decoding), and still reads every manifest.

## Decision

Option C:

- **Predicates.** From the scan filters (already conjunctive), collect
  `col = | < | <= | > | >= literal` (either side) and non-negated `BETWEEN`,
  recursing through `AND`. Ignored, hence never pruning: `OR`, `NOT`, `!=`,
  `IN`, casts on the column, functions, `NULL` literals.
- **Types.** `boolean`, `int`, `long`, `date`, `timestamp[tz]` (µs), `string`
  (bytewise, same order in Iceberg and Arrow). Literals convert only when the
  conversion is exact (e.g. a nanosecond literal not on a µs boundary is
  ignored). **Floats and doubles are never pruned:** Iceberg bounds exclude
  NaN while DataFusion orders NaN above every value, so `x > 5` matches NaN rows
  that bounds hide (same reasoning as the Postgres float policy, ADR 0006).
  Decimals are not pruned yet.
- **Manifest level.** Using the manifest's `partition_spec_id`, a predicate on
  a source column is mapped through monotonic transforms (`identity`, `year`,
  `month`, `day`, `hour`): `col < v` becomes `T(col) <= T(v)`, kept strict
  only when `v` is the first instant of its bucket (so `ts < '2024-01-02'`
  prunes exactly). `bucket`, `truncate` and `void` never prune. Delete
  manifests are always read, so the "delete files are unsupported" refusal of
  ADR 0007 can never be pruned away.
- **File level.** Each entry's `lower_bounds` / `upper_bounds` (truncated
  bounds are valid by spec: lower ≤ min, upper ≥ max) are compared; missing or
  malformed bounds keep the file.
- **Sizes.** `file_size_in_bytes` replaces the per-file `HEAD`; `HEAD` remains
  the fallback when the field is missing or zero.
- **Safety net.** Filters stay `Inexact` (ADR 0005/0007): DataFusion re-applies
  them above the scan. Pruning can change cost, never results.
- **Observability.** The scan node is labelled
  `IcebergScan[<table>] manifests=<read>/<total> files=<kept>/<seen>`, visible
  in `EXPLAIN` and in OOM messages naming the scan consumer.

Why C over B: B is more complete, but the predicates that matter for a
lakehouse (time ranges, equality on keys) are covered by C at a fraction of
the weight, and C's "unknown means keep" rule makes its incompleteness safe.

## Consequences

**Positive**
- Selective queries plan in time proportional to the matching partitions:
  manifests that cannot match are never fetched or reserved.
- One fewer round trip per data file on object storage.
- Pruning effectiveness is visible per query in `EXPLAIN`.

**Negative (accepted debt)**
- No pruning for `OR` / `IN`, decimals, floats, `bucket` / `truncate`
  partitions, or predicates on casted columns.
- Partition summaries are only used for the spec each manifest declares;
  evolved specs work but prune only on fields present in that spec.
- Manifests are still read sequentially (one round trip each).

**Operational**
- A query that unexpectedly reads `manifests=N/N` signals a predicate the
  pruner cannot use (cast, OR, float); `EXPLAIN` is the first diagnostic.
- No new failure modes: a malformed bound degrades to "keep".

## Risks and mitigations

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| A writer emits wrong bounds (violates spec) and rows are skipped | Low | High (silent wrong result) | Only spec-defined fields are trusted; truncated bounds handled by spec semantics; interop tested against pyiceberg. Upgrade path: a session flag to disable pruning for incident triage. |
| Literal coercion differs from column type (e.g. timezone) and pruning uses a wrong value | Low | High | Only exact `ScalarValue` conversions are accepted; anything else is ignored. Timestamps compare as UTC epoch µs. |
| Transform arithmetic off-by-one at bucket edges | Medium | High | Non-decreasing mapping with `<` → `<=` by default; strict only at proven bucket starts; unit tests at midnight/noon edges and month boundaries. |
| Pruner silently stops working after a DataFusion upgrade changes filter shapes | Medium | Medium (latency) | Interop test asserts exact `manifests=`/`files=` counts in `EXPLAIN`. |

## Validation

- Unit (`iceberg::prune::tests`): bound comparisons per operator, literal on
  either side, `BETWEEN`, `AND`, non-provable cases (`OR`, `!=`, missing
  bounds, unknown column), nanosecond exactness, `day` and `month` transforms
  including strict `<` at midnight and non-strict at noon.
- Interop (`tests/iceberg.rs::prunes_a_partitioned_pyiceberg_table`, CI job
  *iceberg interop*): a pyiceberg table partitioned by `day(ts)` with 10
  appends; asserts results **and** `manifests=3/10 files=3/3`,
  `manifests=1/10 files=1/1`, `manifests=10/10 files=1/10`, and no pruning for
  `OR`. CI fails if the fixture env is missing, so the test cannot pass by
  returning early.
- Existing Iceberg tests (live files, deletes refused, renames refused,
  ceiling) unchanged and green.

## Revisit when

- A workload filters mainly with `IN` lists or `OR` of equalities (add
  set-membership evaluation).
- Tables partition by `bucket` / `truncate` (add transform projections).
- Planning shows manifest reads as the bottleneck even after pruning
  (parallel manifest fetch, bounded by the pool).
- DataFusion gains Iceberg-style statistics pruning upstream that subsumes
  file-level bounds.

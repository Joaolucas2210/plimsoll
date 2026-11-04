# ADR 0113 — Column statistics from manifests, and the scan's own filter estimates

- Status: Accepted
- Date: 2026-11-03
- Owner: JoaoLucas2210
- Related: ADR 0010/0049/0053 (bounds for pruning), ADR 0054 (row counts), ADR 0055/0062 (exact filters), ADR 0064/0066 (shuffle or broadcast)
- Scope: `ColumnBounds`, `PlanStats::columns`, `IcebergTable::selectivity`, `Prim::scalar`, the scan's `Statistics`

## Context

DataFusion picks join modes from estimates. A side under its row
threshold is collected (a broadcast when distributed, ADR 0066), and a
larger one is partitioned (a shuffle, ADR 0065).

The Iceberg scan reported only a row count: the record counts of the
data files kept by pruning (ADR 0054). Two things were missing:
- **Column statistics.** Manifests carry bounds and null counts per
  column and file, already decoded for pruning (ADR 0010, 0049, 0053),
  but none reached DataFusion.
- **Filter estimates.** Almost every filter is applied by the scan
  itself (exact pushdown, ADR 0055, 0062), so no `FilterExec`
  estimates it. `WHERE id < 5` was estimated as every row of the files
  pruning kept (25 of 32 live rows in the fixture). A small filtered
  side could not be collected, even with a 20-row threshold.

## Decision drivers

1. **Use what the manifests already say.** No data file is read for
   statistics.
2. **Estimate as DataFusion would**, with its interval analysis and its
   default selectivity, so that a filter costs the same whether a
   `FilterExec` or the scan applies it.
3. **Estimates only**: never an answer, never exact.

## Decision

- **Per column, over the kept data files** (`ColumnBounds`, gathered
  while planning walks the manifests):
  - the union of the files' bounds, for types whose bounds compare
    (ADR 0010: bool, int, long, date, timestamp in µs, string, decimal;
    floats are left out because of NaN);
  - the sum of their null counts.

  Each is kept only while every file has it. A file without bounds (an
  all-null column, an old writer) makes that column's bounds unknown.
- **Reported as `Inexact`** column statistics (`Prim::scalar` converts
  bounds to the column's Arrow type):
  - deletes remove rows, but a bound is still a bound;
  - truncated string bounds are still bounds;
  - null counts become upper bounds.
- **The scan estimates its own filters** (`IcebergTable::selectivity`).
  It runs DataFusion's `analyze` over the filters' conjunction and the
  whole table's column statistics, since filters may read columns the
  projection drops. When analysis gives no answer (`id % 2 = 0`,
  `LIKE`), it uses the session's `default_filter_selectivity` (20%),
  as `FilterExec` would. The row count is scaled by that selectivity.
- The column statistics are then projected with the scan's projection.

## Consequences

**Positive**
- Filtered sides are estimated near their size, so small ones are
  collected (broadcast) and large ones partitioned (shuffled).
  Previously every filtered side looked like its whole table.
- `FilterExec`s above the scan (inexact pushdown), aggregations and
  other estimators get real column ranges and null counts.

**Negative (accepted debt)**
- **Join modes and sides change.** A filtered side estimated small
  becomes the build side. Under an `ORDER BY`, DataFusion may then sort
  the other side below the join, and such a join stays local (a sorted
  probe side is never distributed, ADR 0066). Answers are unchanged,
  but some ordered joins that were distributed now run on the
  coordinator. The distributed tests now check those joins unordered.
- **No distinct counts.** Manifests do not carry them, and DataFusion's
  join cardinality estimate does not use the bounds alone. Checked: the
  estimate of a self-join is the same with or without them. Puffin
  `apache-datasketches-theta-v1` blobs would provide them.
- **No byte sizes.** File sizes are compressed, and reporting them as
  in-memory size would understate build sides. Row thresholds keep
  deciding.
- **The default selectivity is a guess**, and it now applies to
  filters that previously reduced nothing. That is exactly what
  `FilterExec` already did.

## Validation

- `tests/iceberg.rs::reads_a_spark_merge_on_read_table` (Spark fixture,
  CI's interop job):
  - `id` bounds contain the live ids `1..=47` (`Inexact`), with null
    counts known for both columns;
  - `WHERE id < 5` is estimated at 8 rows at most (it was 25 before this
    change);
  - with a 20-row threshold, joining the table to its `id < 5` rows
    collects the filtered side (`CollectLeft`, a broadcast when
    distributed), while `id < 45` stays partitioned. Before this change
    both were partitioned.
- `tests/distributed.rs::distributed_scans_match_local_ones`: every
  shuffled and broadcast join still matches the local engine. The cases
  that broke on ordering were made unordered, or compared with rows in
  any order.

## Revisit when

- Join cardinality matters (joins of joins): read distinct counts from
  Puffin statistics files (`statistics` in table metadata).
- Byte thresholds matter: estimate in-memory size from the files'
  column sizes and the projection.

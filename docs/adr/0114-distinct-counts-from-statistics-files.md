# ADR 0114 — Distinct counts from statistics files, and live row counts

- Status: Accepted
- Date: 2026-11-04
- Owner: JoaoLucas2210
- Related: ADR 0113 (column statistics), ADR 0014 (position deletes), ADR 0054 (row counts), ADR 0079/0080 (statistics files kept by maintenance)
- Scope: `distinct_counts`, `IcebergTable::distinct`, the scan's `distinct_count` and live row estimate, `scripts/spark_mor_fixture.sh` (`compute_table_stats`)

## Context

DataFusion estimates an equi-join's output as `left × right / max(distinct
keys)`. ADR 0113 gave it bounds, but bounds yield a distinct count only
for numeric ranges. For a string key, or a numeric key with repeated
values, DataFusion assumes the key is unique and underestimates the
join.

Iceberg tables can carry statistics files: Puffin files of theta
sketches, one per column, written by Spark's `compute_table_stats`,
Trino's `ANALYZE` and others. The table metadata lists them under
`statistics`, and each blob's estimate is recorded there as
`properties.ndv`. The estimate is in the metadata JSON, so the Puffin
file does not need to be read.

While measuring the effect, the row estimate turned out to be the real
problem in tables with deletes. It summed record counts, deleted rows
included (49 in the fixture), against 32 distinct live ids. With NDV
alone, the self-join estimate got worse: 75 instead of 50, and the true
answer is 32.

## Decision drivers

1. **No new reads.** The table metadata is already loaded.
2. **Estimates that agree with each other.** Distinct counts are over
   live rows, so the row count must be too, wherever it is known for
   free.

## Decision

- **`distinct_counts(meta, snapshot)`**:
  - For the pinned snapshot, or its nearest ancestor with a statistics
    file, the per-field-id `ndv` of every single-field blob. Statistics
    are often computed some commits back, and an ancestor's are still a
    fair estimate.
  - A snapshot on another branch, or one earlier than every statistics
    file, gets none.
  - Malformed entries are skipped.
- **Reported per column** as `Inexact` `distinct_count`, by name in the
  current schema, for any type (floats included, unlike bounds). Time
  travel (ADR 0028) re-pins the metadata, so a past snapshot uses its
  own statistics.
- **Live row estimate.** A file with position deletes already carries
  its row selection (ADR 0014). Its live rows (`selection.row_count()`)
  replace its record count in the scan's estimate. Equality deletes
  remain unknown until scanned, so files they touch still count their
  records.
- The Spark fixture now runs `CALL local.system.compute_table_stats` on
  the merge-on-read table, so CI checks statistics as Spark writes
  them.

## Consequences

**Positive**
- Joins on repeated keys are sized from real distinct counts, wherever
  an engine has computed table statistics.
- Row estimates for tables with position deletes are exact, at no
  cost, for every estimate that uses them: join sides, broadcasts, and
  the filter estimates of ADR 0113.

**Negative (accepted debt)**
- **Stale statistics are used as is.** An ancestor's NDV is not scaled
  by what changed since. Nothing writes statistics here: computing
  sketches on maintenance passes (ADR 0086) would keep them fresh.
- **Only metadata `ndv`.** A blob without the property, or a
  multi-field blob, is ignored, and the sketches themselves are never
  read.
- **Filters do not scale distinct counts.** DataFusion keeps the
  table-wide count for a filtered scan, which overestimates distinct
  values for a narrow filter and underestimates the join.

## Validation

- `iceberg::tests::distinct_counts_come_from_the_nearest_statistics`:
  - the snapshot's own statistics are used, then a descendant's
    nearest ancestor's;
  - nothing is used for an earlier snapshot, another branch or no
    snapshot;
  - multi-field blobs and non-numeric `ndv` are skipped.
- `tests/iceberg.rs::distinct_counts_size_joins_on_repeated_keys`: a
  three-row table self-joined on its string key is estimated at 3 rows,
  then at 9 once the metadata says the key has one value.
- `tests/iceberg.rs::reads_a_spark_merge_on_read_table`, with the Spark
  fixture computing statistics:
  - `id` has `distinct_count` 32;
  - the scan estimates 32 rows (live, not the 49 written);
  - self-joins on `id` and on `name` are estimated at 32, their actual
    size.
  - Measured on the way: 50 (bounds only) or 75 (NDV with written rows)
    before the live count; 32 with both.
- The distributed tests pass with the regenerated fixtures, at 12
  partitions and pinned to two CPUs, as CI runs.

## Revisit when

- Statistics should exist without another engine: compute theta
  sketches (Apache DataSketches' format) during compaction, and commit
  them with `set-statistics`.
- Narrow filters on high-cardinality keys misjoin: scale distinct
  counts by selectivity, as some engines do.

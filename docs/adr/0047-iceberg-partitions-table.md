# ADR 0047 — Iceberg `$partitions`

- Status: Accepted
- Date: 2026-11-02
- Owner: JoaoLucas2210
- Related: ADR 0032 (`$files` / `$manifests`; this adds what it deferred), ADR 0044 (column deny), ADR 0045 (row filters)
- Scope: `plimsoll_connectors::iceberg::files::{Kind::Partitions, partitions, partitions_schema}`, `lakehouse`, `auth::Viewer::check_columns`

## Context

ADR 0032 deferred `$partitions` as "a `GROUP BY partition` over
`$files`". In practice operators ask for it by name:

- Spark, Trino and pyiceberg all expose it;
- it answers skew and small-file questions per partition in one query;
- delete debt per partition (which partitions need compaction) needs
  data and delete totals side by side. That takes a `CASE`-heavy
  aggregate over `$files` that nobody writes correctly the first time.

## Decision

- **`table$partitions`** has one row per (spec id, partition tuple),
  ordered by both, with Spark's columns:

  | Column | Meaning |
  |---|---|
  | `partition` | The tuple as JSON text, as in `$files` |
  | `spec_id` | Partition spec id |
  | `record_count`, `file_count`, `total_data_file_size_in_bytes` | Data files only |
  | `position_delete_record_count`, `position_delete_file_count` | Position delete files |
  | `equality_delete_record_count`, `equality_delete_file_count` | Equality delete files |

- **Implementation.** It folds the same manifest stream that `$files`
  produces: concurrent fetches, each reserved while decoded (ADR 0032).
  Each new partition reserves its entry before the fold, so memory is
  O(partitions) and under the ceiling. The single result batch replaces
  the map's reservation.
- **Governance.**
  - Column deny (ADR 0044) withholds `partition` here as in `$files`,
    because partition values can be a denied column's values. The counts
    stay readable.
  - Row-filtered principals are denied `$partitions` like every `$`
    table (ADR 0045).

## Consequences

**Positive**
- `SELECT partition, record_count, position_delete_record_count FROM
  "t$partitions" ORDER BY 3 DESC` finds the partitions that need
  compaction.
- It is one code path with `$files`, so the two always agree.

**Negative (accepted)**
- **It reads every manifest.** There is no partition pruning of the
  metadata scan, as with `$files`.
- **No `last_updated_at` or `last_updated_snapshot_id`.** They need
  per-entry snapshot ids, which `$files` does not decode.

## Validation

- `tests/iceberg.rs::files_and_manifests_metadata_tables`, on the
  unpartitioned table with an equality delete: one row with
  `record_count` 6, `file_count` 2, one equality delete file and no
  position delete files. The pool returns to 0 and the 256-byte ceiling
  case still fails cleanly.
- `tests/iceberg.rs::time_travels_a_spark_table` (interop), on Spark's
  table partitioned by `days(ts)`:
  - `sum(record_count)` 49, `sum(file_count)` 3 and
    `sum(position_delete_record_count)` 17, the same totals as `$files`;
  - at least two partitions.
- `auth::tests::column_deny_is_parsed_and_checked_per_principal`:
  `$partitions` withholds `partition` and allows `record_count`.

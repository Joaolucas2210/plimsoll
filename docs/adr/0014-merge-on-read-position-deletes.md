# ADR 0014 — Merge-on-read: position deletes as Parquet row selections

- Status: Accepted
- Date: 2026-10-30
- Owner: JoaoLucas2210
- Related: ADR 0007 (refines: deletes no longer refused), ADR 0010 (delete manifests now pruned), ADR 0012, ADR 0013, ADR 0011
- Scope: `plimsoll-connectors::iceberg` (`iceberg/deletes.rs`, `IcebergTable::data_files`, `scan`), `.github/workflows/interop.yml`

## Context

ADR 0007 refused any table with row-level delete files. Merge-on-read (MOR)
is the default for `DELETE`/`UPDATE`/`MERGE` in Spark, Trino and Flink on
v2 tables, so in practice a large share of production Iceberg tables were
unreadable (`unsupported_feature`, ADR 0013). Refusing was correct; it was
not useful.

Iceberg v2 has two delete kinds:

- **Position deletes**: files of `(file_path, pos)` rows. Written by Spark and
  Trino for `DELETE`/`UPDATE`/`MERGE` (MOR).
- **Equality deletes**: files of column values; any data row with equal values
  and a lower sequence number is deleted. Written mainly by Flink upserts.

Spec v2 applicability rule for a position delete: same `file_path` **and**
data file sequence number ≤ delete file sequence number (sequence numbers
inherit from the manifest when the entry's is null).

## Decision drivers

1. Correctness: a deleted row must never be returned; a live row must never be
   dropped (sequence-number rule included).
2. Deleted rows should not be decoded at all (cost proportional to survivors).
3. Delete state is data-proportional: it must be reserved (ADR 0012).
4. Validated against a real MOR writer, not only hand-built files.
5. No new dependencies; CI cost stays within ADR 0011's budget.

## Considered options

**A. Row selections on DataFusion's Parquet scan (chosen).** DataFusion 55
accepts a file-level `ParquetRowSelection` extension on `PartitionedFile` and
splits it across row groups itself. Positions per data file become
`select/skip` runs over `record_count` rows. Deleted rows are never decoded,
predicates and pruning still apply. Cost: read the relevant delete files during
planning.

**B. Anti-join on a row-position virtual column.** Add `_pos` and anti-join
against `(file_path, pos)`. Works for both kinds later, but decodes every
deleted row, needs a join per scan and a row-position column through the
Parquet reader.

**C. Keep refusing; recommend compaction.** Zero code. Pushes the problem to
every user.

**D. iceberg-rust's delete handling.** Second Arrow version (ADR 0007).

## Decision

Option A for position deletes; equality deletes stay refused (typed
`Unsupported`, HTTP 422).

- **Two-phase planning.** Phase 1 walks manifests once, collecting kept data
  files (with `record_count` and data sequence number) and live position
  delete files (with their sequence number and v2 `referenced_data_file`).
  Delete manifests are pruned by partition summaries like data manifests: a
  position delete shares its data file's partition, so a manifest that cannot
  match the query cannot hold deletes for a scanned file.
- **Phase 2** reads only delete files that may apply (skipping those whose
  `referenced_data_file` was pruned), whole, through the table's store (scoped
  credentials included, ADR 0009), with bytes reserved while decoded. Each
  `(file_path, pos)` applies only to a scanned file with data seq ≤ delete seq;
  every kept position reserves 8 bytes.
- **Selection.** Sorted, unique positions → `RowSelection` over
  `record_count`; a position ≥ `record_count` is `invalid_table`. Fully
  deleted files are dropped from the scan.
- **Observability.** `EXPLAIN` / OOM label:
  `IcebergScan[t] manifests=a/b files=c/d deletes=<files> deleted_rows=<n>`;
  the planning consumer is `IcebergPlanning[t]`.
- **Interop CI.** `scripts/spark_mor_fixture.sh` writes a day-partitioned MOR
  table with Apache Spark 3.5.5 + Iceberg 1.9.2 (`DELETE`, then `UPDATE`).
  `.github/workflows/interop.yml` runs it only when the Iceberg reader or the
  fixture changes, weekly, or on demand (Spark is ~1 GB; ADR 0011).

## Consequences

**Positive**
- MOR tables written by Spark are readable and match Spark's own answers
  (32 rows / sum 768 / 1 updated; day-2 slice 16 / 576).
- Deleted rows cost nothing at scan time; pruning and deletes compose.

**Negative (accepted debt)**
- Delete files are read whole and per query (no cache). Large delete backlogs
  cost planning time and memory until compacted.
- Positions are reserved during planning only; the resulting selections live
  in the plan (O(runs)) unaccounted.
- Equality deletes and v3 deletion vectors are refused.
- Delete files are not pruned by their `file_path` column bounds.

**Operational**
- `deleted_rows` in `EXPLAIN` is the signal for tables that need compaction.
- MOR tables with very large delete sets may now hit the ceiling during
  planning (`IcebergPlanning[t]` in the OOM message) instead of being refused.

## Risks and mitigations

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| Sequence-number inheritance mis-read returns deleted rows | Low | High | Spec rule implemented explicitly; fixture tests both "applies" (delete seq 3 > data seq 2) and "does not apply" (seq 1 < 2); Spark interop. |
| Pruning a delete manifest drops deletes for a scanned file | Low | High | Only partition-summary pruning, under the same spec as the data; Spark test combines pruning (`ts >= day 2`) with deletes. |
| `record_count` wrong in a writer's manifest | Low | Medium | Positions beyond it fail as `invalid_table` instead of misreading. |
| Spark image or Maven availability breaks interop CI | Medium | Low | Separate workflow; pinned image tag and jar version; main CI unaffected. |

## Validation

- Unit: `iceberg::deletes::tests::selection_skips_exactly_the_deleted_rows`.
- Fixture (`tests/iceberg.rs`): `position_deletes_are_applied_as_row_selections`
  (including `deletes=1 deleted_rows=1` in `EXPLAIN`),
  `position_deletes_older_than_the_data_do_not_apply`,
  `equality_deletes_fail_instead_of_returning_deleted_rows` (typed
  `ConnectorError::Unsupported`).
- Interop: `reads_a_spark_merge_on_read_table` against Spark's MOR output:
  totals, the updated row, the day-2 slice with pruning, and no surviving
  deleted id; pool back to 0.

## Revisit when

- Flink or other upsert writers are in scope (equality deletes: anti-join with
  sequence numbers, option B).
- Format v3 tables appear (deletion vectors in Puffin files).
- Planning profiles show delete-file reads dominating (cache per snapshot, or
  prune by `file_path` bounds).

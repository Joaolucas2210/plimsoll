# ADR 0032 — Iceberg file metadata tables: `$files`, `$manifests`

- Status: Accepted
- Date: 2026-11-01
- Owner: JoaoLucas2210
- Related: ADR 0030 (metadata tables from `metadata.json`; this is its "file-level metadata" revisit item), ADR 0003/0012 (memory at the edges), ADR 0020 (concurrent fetches), ADR 0025 (grants)
- Scope: `plimsoll_connectors::iceberg::{files, IcebergConnector::metadata_table, read_env}`, `plimsoll-server::lakehouse::CatalogSchema::{table, connector}`

## Context

ADR 0030 answered "which snapshots and refs exist". Operators also need to
see a table's physical shape:

- **Small files.** A streaming writer leaves thousands of 1 MB files, and
  planning and scan cost grow with them.
- **Delete debt.** Merge-on-read tables accumulate position and equality
  delete files (ADRs 0014 and 0016) until compaction. Read cost grows with
  them.
- **Partition skew.** One partition holds most rows.
- **Manifest bloat.** Too many small manifests slow planning
  (ADR 0020 measured the cost per GET).

This information is in the manifest list and the manifests, not in
`metadata.json`. Reading it means object-store I/O that scales with the
table. That is the property ADR 0003 says must never escape the ceiling.

## Decision drivers

1. Memory bounded by the fetch window, not by table size. A table with
   10⁶ files must not materialize 10⁶ rows at planning time.
2. The same read path as scans:
   - vended credentials (ADR 0009);
   - concurrent fetches (ADR 0020);
   - reservations in the session pool (ADR 0012).
3. Grants apply unchanged (`schema."t$files"`, as in ADR 0030).
4. Spark-compatible column names, and no new dependency.

## Considered options

**A. A streaming partition that reads manifests at execution (chosen).**
- `StreamingTable` over one `PartitionStream`.
- Its stream fetches the manifest list, then the manifests,
  `meta_fetch_concurrency` at a time, in list order.
- It yields one `RecordBatch` per manifest.
- Fetched bytes are reserved in the task's pool while they are decoded,
  then released.
- Rows flow to downstream operators, which account for their own state
  (an `ORDER BY` or `GROUP BY` spills or fails under the ceiling as
  usual).

**B. Build all rows at planning time into a `MemTable` (ADR 0030's
approach).** This is fine for metadata JSON, which is already in memory.
For manifests, it would hold O(files) rows outside any operator for the
query's lifetime. Rejected (driver 1).

**C. Reuse `IcebergTable::data_files` (the scan planner).** It is built
for scans:
- it prunes;
- it drops deleted rows;
- it loads delete contents.

The planner would have to grow a "list everything raw" mode. Rejected: one
small decoder per metadata table is simpler than a mode flag through the
hottest code path.

## Decision

- `IcebergConnector::metadata_table(kind)` returns `Some` for `files` and
  `manifests`.
- **`$files`** lists the live entries (status ≠ deleted) of the pinned
  snapshot, data and delete files alike.

  | Column | Meaning |
  |---|---|
  | `content` | 0 data, 1 position deletes, 2 equality deletes |
  | `file_path`, `file_format` | `file_format` upper-case: `PARQUET`, `PUFFIN` |
  | `spec_id`, `partition` | `partition` is the tuple as JSON text |
  | `record_count`, `file_size_in_bytes` | |
  | `sequence_number` | Inherited from the manifest when null |

- **`$manifests`** has one row per manifest:
  - `content` (0 data, 1 deletes), `path`, `length`;
  - `partition_spec_id`, `added_snapshot_id`, `sequence_number`;
  - Spark's six counts: `{added,existing,deleted}_{data,delete}_files_count`.
    These are split by manifest content, and read from v2 or v1 field names.
- **Resolution.** `CatalogSchema` serves `"t$files"` and `"t$manifests"`
  after the in-memory kinds of ADR 0030, with the same base-table rule and
  the same fallback to a whole-name lookup.
- **Execution-time I/O.** `read` gained a `RuntimeEnv` variant
  (`read_env`), because a `TaskContext` has no `Session`. The scoped store
  is registered on the runtime env, as the scan does.

## Consequences

**Positive**
- Common health checks become SQL:
  - small files:
    `SELECT count(*), avg(file_size_in_bytes) FROM bronze."trips$files" WHERE content = 0`;
  - delete debt:
    `SELECT content, count(*), sum(record_count) FROM bronze."trips$files" GROUP BY content`;
  - skew: `GROUP BY partition`.
- Memory is O(concurrency × manifest size). Tested with a pool too small
  for one manifest: the query fails with `IcebergMetadata` named in the
  error, and the pool returns to 0.

**Negative (accepted debt)**
- **`partition` is JSON text, not a struct.** A struct needs each spec's
  result types, and specs evolve. Grouping and filtering work as strings.
- **No `$partitions`.** It is a `GROUP BY partition` over `$files`, so it
  was skipped until it is asked for (YAGNI).
- **Some Spark columns are omitted:**
  - column-level stats (`column_sizes`, `lower_bounds`, …);
  - `key_metadata`;
  - `split_offsets`;
  - `readable_metrics`;
  - `partition_summaries` on `$manifests`.
- **No `$all_files` or `$all_manifests` across snapshots.**
- **Current snapshot only.** `"t@v1$files"` does not combine; ADR 0030
  has the same limit.
- **No pruning.** A filter on `$files` reads every manifest; filters are
  applied after decoding.

## Risks and mitigations

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| Huge tables make `$files` slow | Medium | Low | Streaming, concurrent fetches; the query timeout bounds it; admission limits concurrency. |
| Downstream `ORDER BY` over millions of rows | Medium | Low | Normal operator accounting: spill or `memory_limit_exceeded`, never the node. |
| Paths reveal bucket layout | Expected | Low | Same grant as the data (ADR 0030's rule). |

## Validation

- `tests/iceberg.rs::files_and_manifests_metadata_tables`, on the
  hand-built v2 table with equality deletes:
  - `$files` lists exactly the two live data files and the equality delete
    file, with inherited sequence numbers;
  - the entry deleted in this snapshot is absent;
  - `$manifests` content is ordered by sequence;
  - a 256-byte pool fails with `ResourcesExhausted` naming
    `IcebergMetadata`;
  - the pool returns to 0 in both cases.
- `tests/iceberg.rs::time_travels_a_spark_table` (interop), on Spark's MOR
  table:
  - `$files` shows data and position-delete files;
  - 3 data files with 49 rows and 2 position-delete files with 17 rows,
    whose difference is main's 32 live rows;
  - `$manifests` has 2 data and 2 delete manifests;
  - the pool returns to 0.
- `lakehouse::tests::metadata_tables_list_history_through_sql`: unknown
  kinds still fall back to a name lookup.

## Revisit when

- Users need column stats (`readable_metrics`) or typed partitions.
- `$partitions` or `$all_files` are asked for.
- `$files` queries on very large tables need manifest pruning by partition
  summaries (the scan's `Pruner`).

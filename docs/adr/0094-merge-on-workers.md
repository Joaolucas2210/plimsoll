# ADR 0094 — `MERGE INTO` on workers

- Status: Accepted
- Date: 2026-11-03
- Owner: JoaoLucas2210
- Related: ADR 0082 (MERGE), ADR 0093 (DELETE/UPDATE on workers), ADR 0066 (attached build sides), ADR 0062/0063 (portable SQL)
- Scope: `remote::{MergeSpec, ClauseSpec}` (`Fragment::merge`), `RemoteScanExec::{merging, attached_stream}`, `merge::{Index, probe, not_matched, finish, run_remote, spec, from_spec}`, `IcebergTable::split_targets`, `IcebergConnector::merge_files`, the worker's `merge` task

## Context

ADR 0093 moved `DELETE` and `UPDATE` to the workers and left `MERGE`
on the coordinator. Two things stood in the way:

- **The clauses are expressions over the merged row**: the target's
  columns, then the source's. Both sides often share names (`id`, `v`),
  so the expressions cannot be shipped as SQL over either table.
- **`WHEN NOT MATCHED` is global.** A source row is unmatched only if
  no target row in *any* file pairs with it, which no single worker
  knows.

`WHEN MATCHED` and `WHEN NOT MATCHED BY SOURCE`, on the other hand, are
decided per target row, so per file, like a `DELETE`.

## Decision drivers

1. **The result of a single-node merge**: the same rows changed and the
   same counts, including the "target row matches more than one source
   row" error.
2. **One commit**, as for every write.
3. **Fall back** wherever an expression does not travel exactly.

## Decision

- **Split by what is local.**
  - **Workers** run the target-file pass, `probe`: hash the source on
    its `ON` keys, then for each of their files apply `WHEN MATCHED`
    and `WHEN NOT MATCHED BY SOURCE`, write position deletes and
    updated rows, and report which source rows paired.
  - **The coordinator** ORs the paired sets. It runs `WHEN NOT MATCHED`
    over the source rows no worker paired, writes those inserts, and
    commits everything as one `overwrite`.
  - The local merge is the same three steps run in one place: `probe`,
    then `not_matched`, then `finish`.
- **Names on the wire.**
  - The merged row's columns are renamed `t0…` (target, in table order)
    and `s0…` (source, in its output order) before the expressions are
    rendered as portable SQL (ADR 0063).
  - The worker rebuilds the same renamed row from the table's schema
    and the attached source's schema, and parses the clauses back.
  - If any key, residual, condition or value does not round-trip, the
    merge runs locally.
- **The source travels as an attachment** (Arrow IPC after the
  fragment's JSON, as broadcast joins do, ADR 0066). The coordinator
  collects it once, as the local merge already did, and every fragment
  receives the same bytes. Row positions in that batch are the paired
  indices the workers report.
- **Which files go out** is the same decision as for `DELETE` /
  `UPDATE` (`split_targets`): healthy workers, a workers-writable table
  without vended credentials, and the snapshot's schema current.
- **Results.** A worker's reply is its files (JSON per row, ADR 0091)
  followed by one `{"matched": [...], "changed": n}` row. Changed
  counts add up across workers, plus the coordinator's inserts.

## Consequences

**Positive**
- The expensive part of a merge, reading every target file, scales
  with the workers. That includes upsert patterns where both sides use
  the same column names.
- Local and distributed merges share one implementation of each step,
  and a test compares their results row for row.

**Negative (accepted debt)**
- **The source is collected on the coordinator and sent to every
  worker**, so its size is bounded by memory on each node and by the
  network times the number of workers. A partitioned, shuffled merge
  (source and target hashed on the keys) is the next step.
- **Paired indices travel as a JSON list.** For a large source with
  most rows matched, a bitmap would be smaller.
- **Inserts are written on the coordinator.**

## Validation

- `remote::tests::fragments_round_trip_as_json` covers `merge` (keys,
  residual, delete and update clauses).
- `tests/distributed.rs::writes_on_workers_through_the_rest_catalog`
  (live in CI, on S3), with two workers. Two identical partitioned
  tables take the same `MERGE`, with a conditional delete, an update,
  an insert and `NOT MATCHED BY SOURCE`, and `id` and `v` on both
  sides:
  - one is merged distributed (planned with `fragments=2`), the other
    on a node without workers;
  - the counts are equal, a full join of the two tables finds no
    differing row, and the 16 inserts are present.
- By hand, with a coordinator, two worker processes and a separate
  single node against the reference REST catalog:
  - the same merge changed 344 rows on both, ran as `merge` tasks, and
    left identical per-partition counts and sums;
  - zero rows differ between the two tables.

## Revisit when

- Sources outgrow a node: shuffle the source and the target files by
  key hash, and let reducers merge their buckets (ADR 0064).
- Paired sets get large: send them as bitmaps.

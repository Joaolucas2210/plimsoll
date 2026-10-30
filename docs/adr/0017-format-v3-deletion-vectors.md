# ADR 0017 — Format v3: deletion vectors, and what v3 must not silently change

- Status: Accepted
- Date: 2026-10-30
- Owner: JoaoLucas2210
- Related: ADR 0007 (refines: v3 accepted), ADR 0014 (same row selections), ADR 0016, ADR 0013, ADR 0012
- Scope: `plimsoll-connectors::iceberg` (`from_metadata`, `data_files`, `read_range`, `arrow_type`), `iceberg/deletes.rs`, `scripts/spark_mor_fixture.sh`, `.github/workflows/interop.yml`, dependency `roaring`

## Context

ADR 0007 refused `format-version` 3. Iceberg 1.8+ writes v3 when asked, and
in v3 merge-on-read deletes are **deletion vectors (DVs)** instead of
position delete files: one Roaring bitmap of deleted row positions per data
file, stored as a `deletion-vector-v1` blob inside a Puffin file. The
manifest entry is a position delete (`content = 1`) with `file_format =
PUFFIN`, `referenced_data_file`, `content_offset` and `content_size_in_bytes`;
many DVs share one Puffin file. A later delete on the same data file writes a
new DV and removes the old entry (status 2).

Accepting v3 is not only DVs. v3 also adds features that, if ignored, return
wrong answers without an error:

- **Column defaults** (`initial-default`): files written before a column was
  added must read the default. DataFusion's Parquet scan fills missing
  columns with `NULL`.
- **Table encryption** (`encryption-keys`, per-file `key_metadata`): the
  scan would decode ciphertext.
- **New types** (`timestamp_ns`, `variant`, `geometry`, `geography`,
  `unknown`). Unknown types were already refused by `arrow_type`.
- **Row lineage** (`_row_id`, `first_row_id`): metadata columns that only
  matter when requested; ignoring them never changes a user column.

## Decision drivers

1. A v3 table either reads exactly as Spark reads it, or fails with a typed
   error naming the feature. Never a silently different answer.
2. DVs reuse ADR 0014's mechanism (no new execution path).
3. Corrupt or truncated blobs are detected, not decoded into wrong positions.
4. Dependency weight stays small and justified (this ADR).
5. Validated against the reference writer.

## Considered options

**Roaring decoding**

- **A. `roaring` crate (chosen).** Pure Rust, no `unsafe` in our code;
  adds 5 packages to `Cargo.lock` (`roaring`, `bytemuck`, `fearless_simd`
  + its proc-macro, `libm`; `byteorder` was already present). `RoaringTreemap::deserialize_from` reads exactly
  the spec's portable 64-bit layout (u64 LE bitmap count, then per bitmap a
  u32 LE key and a portable 32-bit Roaring bitmap). Used by iceberg-rust for
  the same purpose.
- **B. Own parser.** ~80 lines covering array, bitmap and run containers,
  the two cookie variants and the offset header. Saves one small crate, but
  this is a binary format from outside the trust boundary where an
  off-by-one deletes the wrong rows. Rejected: the crate is the cheaper
  correctness bet.

**Reading blobs**

- **A. Range read per DV (chosen).** `get_range(content_offset ..
  +content_size)`, only for DVs whose referenced file survived pruning.
- **B. Whole Puffin file once, slice blobs.** Fewer requests when one Puffin
  holds many relevant DVs, but reads bytes for pruned files too. Upgrade path
  if request counts dominate (coalesce ranges per file).

**v3 features**

- **A. Accept v3; refuse what changes answers (chosen).** Defaults and
  encryption are typed `Unsupported`; `timestamp_ns`/`timestamptz_ns` map to
  Arrow nanosecond timestamps (not pruned, `Prim` has no ns); row lineage is
  ignored.
- **B. Keep refusing v3 until everything is supported.** Every v3 table
  unreadable for features most tables do not use. Rejected.

## Decision

- `format-version` 1–3 accepted. Refused at registration: any current-schema
  field with a non-null `initial-default`; non-empty `encryption-keys`.
  Refused at planning: any data or delete file with `key_metadata`.
- A delete entry with `file_format = PUFFIN` is a DV: `content_offset` and
  `content_size_in_bytes` are required (`invalid_table` otherwise), as is
  `referenced_data_file`. It applies under the same rule as position deletes
  (data seq ≤ delete seq) and only to a scanned file.
- Blob decoding (`deletes::deletion_vector`): total length ≥ 12; big-endian
  length prefix equals `size − 8`; magic `D1 D3 39 64`; big-endian CRC-32
  (IEEE) over magic + bitmap; then `RoaringTreemap::deserialize_from`. Any
  failure is `invalid_table` naming the Puffin file.
- Positions join the per-file set used for position deletes and become
  `ParquetRowSelection`s (ADR 0014). Blob bytes are reserved while decoded;
  positions reserve 8 bytes each.
- Interop: `spark_mor_fixture.sh` writes `db.dv` (v3, 100 000 rows,
  `DELETE id % 7 = 0`, `DELETE id < 10`, `UPDATE id = 99999`).

## Consequences

**Positive**
- v3 MOR tables written by Spark read identically (85 706 rows, sum
  4 285 685 677; replaced DVs honoured; updated row once).
- No new execution path: DVs, v2 position deletes and pruning compose.
- Corruption is detected (CRC, magic, length) before positions are used.

**Negative (accepted debt)**
- One range request per DV; a Puffin file holding many relevant DVs costs
  many requests (concurrent since ADR 0020; coalescing is the upgrade path).
- DV positions are materialised into a sorted `Vec<u64>` before becoming a
  selection (8 B/position reserved): a file with tens of millions of deletes
  costs hundreds of MB during planning. Iterating the bitmap directly into
  selection runs is the upgrade path.
- Bitwise CRC (`ponytail:`, ~10 ms/MB); blobs are KB-sized in practice.
- v3 column defaults, encryption, `variant`, geo types are refused.
- New dependency: `roaring` 0.12 (+ `bytemuck`, `fearless_simd`, `libm`).

**Operational**
- `deletes=` in `EXPLAIN` counts DVs alongside position delete files.
- A table using defaults or encryption fails with `unsupported_feature`
  naming the feature (HTTP 422, ADR 0013).

## Risks and mitigations

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| Treemap layout differs from the spec for positions ≥ 2³² | Low | High | Unit test round-trips a position above 2³²; spec layout matches `roaring`'s treemap serialization. |
| A replaced DV is still applied | Low | High (extra deletes) | Status-2 entries skipped (existing rule); fixture deletes twice on the same files. |
| A v3 feature not listed here changes answers silently | Low | High | Unknown types already fail; new spec features are reviewed against this ADR's list; `v3_features_that_change_answers_are_refused`. |
| CRC check too slow on very large DVs | Low | Low | Measurable in planning time; switch to table-driven CRC. |

## Validation

- Unit (`iceberg::deletes::tests::deletion_vectors_decode_and_are_verified`):
  CRC-32 check value, round trip including a 64-bit position, flipped CRC,
  bad magic, truncated blob.
- Fixture (`tests/iceberg.rs::v3_features_that_change_answers_are_refused`):
  `initial-default` and `encryption-keys` refused; plain v3 with
  `timestamp_ns` registers.
- Interop (`reads_reference_deletion_vectors`, workflow *interop*): Spark's
  answer (count, sum), no deleted id survives, the updated row appears once
  with its new value, pool back to 0.

## Revisit when

- Tables use v3 column defaults (fill missing columns with literals via the
  schema adapter), or encryption (KMS integration).
- Planning profiles show DV range reads dominating (coalesce per Puffin).
- Single files carry tens of millions of deletes (stream bitmap to
  selection).

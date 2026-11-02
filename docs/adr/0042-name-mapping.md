# ADR 0042 — Iceberg name mapping

- Status: Accepted
- Date: 2026-11-02
- Owner: JoaoLucas2210
- Related: ADR 0040 (field ids; this is its "files without ids" revisit item), ADR 0041 (nested types), ADR 0016 (equality deletes)
- Scope: `plimsoll_connectors::iceberg::field_ids::{NameMapping, parse_name_mapping, FieldIds}`, `IcebergTable::name_mapping`, `deletes::equality_keys`

## Context

ADR 0040 resolves columns by Parquet field id. Not every data file has
ids:

- **Imports.** `add_files` / `migrate` / `snapshot` register existing Hive
  or plain Parquet files in an Iceberg table without rewriting them.
- **Some writers** (older or non-Iceberg tools) write files without ids.

Iceberg's answer is the **name mapping**: the table property
`schema.name-mapping.default`, a JSON list such as
`[{"field-id": 1, "names": ["id", "ident"]}, …]`, gives each field every
name a file may use for it. Writers keep it current. When a column is
renamed, Iceberg appends the new name to the mapping and keeps the old
one, so old files still resolve.

Without it, ADR 0040 refuses id-less files in renamed tables. That made
every imported-then-renamed table unreadable, which is a standard
migration path (Hive to Iceberg, then clean up the names).

## Decision drivers

1. **Spec semantics.** A file without ids is resolved through the mapping
   when the table has one, aliases included, and otherwise by name.
2. **One code path.** Ids from the mapping must flow into ADR 0040's
   adapter, so renames, promotions and missing fields behave identically
   to files with ids.
3. **No silent misreads.** An ambiguous mapping (one name for two fields)
   is invalid metadata, not a guess.

## Decision

- **Parsing.** `parse_name_mapping` reads the property into a map from
  name to field id. Every alias of an entry maps to its id; entries
  without `field-id` map to nothing, as the spec says. A name mapped to
  two different ids is `InvalidMetadata` at registration. Nested `fields`
  entries are ignored, because children resolve by name inside their
  parent (ADR 0041).
- **Adapter** (`FieldIds::create`). When a file has no field ids and the
  table has a mapping, the file's ids are its column names looked up in
  the mapping; then `ById` runs as for files with ids. Without a mapping,
  ADR 0040 applies unchanged: by name, or refused in renamed tables.
- **Always adapt.** Tables with a mapping carry field ids in their Arrow
  schema, as renamed tables already do (ADR 0040). Their id-less files
  therefore never compare equal and are always adapted. This matters for
  aliases: a file column `ident` must become `id` even in a table that
  never renamed anything.
- **Equality deletes.** Key columns resolve by Parquet id, then through
  the mapping, then by the current name.

## Consequences

**Positive**
- Tables imported with `add_files` and then renamed read exactly as Spark
  reads them, and native files written after the import mix in normally.
- Mapping aliases work. A file column named differently from the table
  column reads under the table name.

**Negative (accepted)**
- **Nested mappings are not used.** Imported files with nested columns
  resolve children by name. That is correct unless an imported nested
  child was later renamed, and such renames are refused anyway
  (ADR 0041).
- **`schema.name-mapping.default` is the only property read.** There is
  no per-snapshot mapping history; the spec defines only the default.

## Risks and mitigations

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| A stale mapping (writer did not update it after a rename) | Low | High | Old names stay in the mapping by Iceberg's own update rules; the Spark fixture verifies a real rename keeps imported files readable. |
| Ambiguous mapping | Very low | High | Refused as invalid metadata (tested). |

## Validation

- `tests/iceberg.rs::files_without_ids_resolve_through_the_name_mapping`:
  - an id-less file with columns `ident` and `town` sits in a table whose
    columns are now `id` and `city`, with mapping
    `id|ident`, `town|city`;
  - the file reads, filters included;
  - a mapping giving `id` to two fields is refused.
- `tests/iceberg.rs::reads_files_imported_without_field_ids` (interop
  workflow):
  - **Setup:** Spark wrote plain Parquet, imported it with
    `CALL system.add_files`, renamed `ident` to `id` and `town` to
    `city`, and inserted a native row.
  - **Spark's answer** (asserted in the fixture) is 0..4 as t0..t4, plus
    (10, native).
  - **Plimsoll's answer** matches, filters on the renamed column hit both
    the imported and the native file, and the pool returns to 0.

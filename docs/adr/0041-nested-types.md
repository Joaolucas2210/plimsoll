# ADR 0041 — Nested types

- Status: Accepted
- Date: 2026-11-02
- Owner: JoaoLucas2210
- Related: ADR 0005 (correctness first), ADR 0007 (read path; nested types were refused), ADR 0040 (field ids)
- Scope: `plimsoll_connectors::iceberg::{arrow_schema, arrow_field, iceberg_type, nested_fields}`, the nested-history check in `IcebergTable::from_metadata`, `field_ids::ById`

## Context

`arrow_schema` refused any column whose Iceberg type is not a primitive
(ADR 0007), so tables with nested columns were unreadable. Nested
columns are common in lakehouse data:

- event payloads (`struct`);
- tags (`list<string>`);
- attributes (`map<string,string>`);
- CDC envelopes.

DataFusion and Arrow support nested types natively: the Parquet reader
decodes them, and SQL reaches into them with `s['a']`, `cardinality(l)`
and `m['k']`. The missing pieces are the type mapping and evolution
safety.

## Decision drivers

1. Read nested columns exactly as Spark does, including **struct fields
   added later**, which must be NULL in files that predate them.
2. **No silently wrong children.** ADR 0040 resolves *top-level* columns by
   field id, while children inside a struct are still matched by name by
   DataFusion's struct cast. A history that renamed a nested child would
   read the wrong data, so it must be refused.
3. JSON, NDJSON and Arrow outputs carry nested values naturally.

## Considered options

**A. Map types recursively; reuse DataFusion's struct-aware cast for
evolution; refuse nested renames (chosen).**
- Iceberg `struct`, `list` and `map` become Arrow `Struct`,
  `List(element)` and `Map(key_value{key, value})`, with Iceberg's
  `required` flags as nullability.
- In the scan adapter (ADR 0040), when a file's column type differs from
  the table's, the column is wrapped in
  `CastExpr::new_with_target_field`. That is DataFusion's own
  struct-aware cast: struct fields the file lacks are filled with NULL,
  and nested primitives are promoted.
- Types are compared with `equals_datatype`, which ignores child names
  and metadata, so cosmetic differences such as Parquet's list child
  names do not force a cast.

**B. Resolve nested children by field id too.** This would need a
recursive rewrite of nested arrays per file (re-assembling structs and
lists). It is correct, but it is a lot of code for a rare operation
(renaming a field inside a struct). Deferred: such tables are refused
instead (driver 2).

**C. Keep refusing nested types.** That leaves a large class of tables
unreadable. Rejected.

## Decision

- **Type mapping.** `iceberg_type(json)` handles primitives (unchanged)
  and:

  | Iceberg | Arrow |
  |---|---|
  | `{"type": "struct", "fields": [...]}` | `Struct(fields)` |
  | `{"type": "list", "element": T, "element-required": b}` | `List(Field("element", T, !b))` |
  | `{"type": "map", "key": K, "value": V, "value-required": b}` | `Map(Field("key_value", Struct[key: K not null, value: V nullable !b]), unsorted)` |

  The mapping is recursive, so any nesting depth works.
- **Nested-history check, at registration.** Across all schemas, every
  nested struct field is keyed by its parent id (struct field, list
  element or map key/value id) and its name. The table is refused when:
  - an id appears under a different parent or name (renamed or moved);
  - the same parent and name have a different id (re-added).

  Nested `initial-default`s (v3) are refused like top-level ones.
- **Unchanged:**
  - nested columns are never pruning candidates (only primitives have
    bounds in `Prim`);
  - equality delete keys are primitives by spec;
  - partition transforms apply to primitives.

## Consequences

**Positive**
- Tables with struct, list and map columns read, and SQL navigates them:
  `s['a']`, `cardinality(tags)`, `attrs['k']`, filters on `s['b']`.
- A struct field added by `ALTER TABLE … ADD COLUMN s.c` reads as NULL in
  older files, as in Spark.
- Results serialize as JSON objects and arrays, and as nested Arrow
  columns in Arrow IPC.

**Negative (accepted)**
- **Renamed, moved or re-added nested children make the table
  unreadable** (a typed error at registration). Top-level renames are
  fine (ADR 0040).
- **Promotions inside nested types** rely on DataFusion's cast, without
  per-child field-id resolution.
- **No pruning on nested fields.** Bounds on nested primitives exist in
  manifests but are not used.

## Risks and mitigations

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| Struct cast matches children by name and order incorrectly | Low | High | Child renames and re-adds are refused at registration, so names are stable per id; the Spark fixture covers an added child. |
| Writers name list and map children differently | Expected | None | Types compare with `equals_datatype` (names ignored); the cast handles layout. |
| Deeply nested huge values blow memory | Low | Low | Batches are accounted like any other (`AccountedExec`). |

## Validation

- `tests/iceberg.rs::nested_types_map_and_nested_renames_are_refused`:
  - struct, list (required element) and map map to the expected Arrow
    types;
  - a history renaming `s.bb` to `s.b` is refused with "nested field "b"
    was renamed".
- `tests/iceberg.rs::reads_nested_spark_types` (interop workflow):
  - **Setup:** Spark wrote `struct<a,b>`, `array<bigint>` and
    `map<string,int>` rows, then `ADD COLUMN s.c`, then a row with `c`.
  - **Spark's answer** (asserted in the fixture) is
    `[1,1,null,2,1]; [2,null,null,0,null]; [3,3,new,1,3]`.
  - **Plimsoll's answer** is identical. `s['b'] = 'z'` gives [3], the old
    row's struct prints `{a: 1, b: x, c: }`, and the pool returns to 0.
  - **Sanity check:** the test fails when pointed at a missing table.
- `engine::tests::nested_values_serialize_as_json`: the result is
  `[[{"a":1,"b":"x"},[1,2],{"k":7}]]`.
- The full Iceberg suite against Spark tables: 24 of 24.

## Revisit when

- Tables with renamed nested children must be read: resolve children by
  field id recursively.
- Queries filter on nested fields at scale: use nested bounds for
  pruning.

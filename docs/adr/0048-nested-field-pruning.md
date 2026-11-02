# ADR 0048 — Pruning on nested struct fields

- Status: Accepted
- Date: 2026-11-02
- Owner: JoaoLucas2210
- Related: ADR 0010 (pruning by column bounds), ADR 0041 (nested types; resolves its pruning revisit item)
- Scope: `plimsoll_connectors::iceberg::{prunable, IcebergTable::columns}`, `prune::path`

## Context

Iceberg writers record lower and upper bounds for every primitive leaf,
including leaves inside structs (`s.a` has its own field id and bounds).
The pruner (ADR 0010) only knew top-level columns. So `WHERE s['a'] = 3`,
a common filter on event payloads, read every file.

## Decision

- **Prunable columns.** These are now *paths*: top-level primitives
  (`id`) and primitives reached through structs (`s.a`, `s.t.b`), each
  with its own field id and type.
  - List elements and map keys and values are excluded, because their
    bounds span all elements of all rows. `tags = …` is not a scalar
    comparison anyway.
- **Filters.** `prune::path` turns `Column` into `"col"` and
  `get_field(get_field(col, 't'), 'b')` into `"col.t.b"`. Comparisons and
  `BETWEEN` on a path prune with that field's bounds, exactly as for
  top-level columns.
- **Clash safety.** A top-level column literally named `s.a` would share
  the key of struct child `s.a`. Such a path is removed from the
  prunable set, so no pruning happens rather than pruning by the wrong
  field's bounds.

## Consequences

**Positive**
- Filters on struct fields skip files like any other filter.

**Negative (accepted)**
- **Files where the field is all NULL have no bounds.** They were kept
  until ADR 0049 pruned them by null counts.
- **No pruning through list or map elements.**

## Validation

- `prune::tests::struct_children_are_paths`: `col`, `s['a']` and
  `s['t']['b']` map to paths; other expressions do not.
- `tests/iceberg.rs::reads_nested_spark_types` (interop). Spark wrote
  three files: `(1, s.a=1)`, `(2, s NULL)` and `(3, s.a=3)`.

  | Filter | Files scanned | Why |
  |---|---|---|
  | `s['a'] = 3` | 2/3 | Prunes the `s.a = 1` file; the all-NULL file is kept |
  | `s['a'] > 100` | 1/3 | Only the all-NULL file is kept |
  | `s['a'] >= 1` | 3/3 | Every file may match |

  The answers are unchanged: `s['a'] = 3` gives [3].

# ADR 0029 — Iceberg branches and tags in time travel

- Status: Accepted
- Date: 2026-10-30
- Owner: JoaoLucas2210
- Related: ADR 0028 (time travel; this extends its spec), ADR 0025 (grants), ADR 0008 (per-query `loadTable`)
- Scope: `plimsoll_connectors::iceberg::ref_snapshot`, `plimsoll-server::lakehouse::CatalogSchema::{table, load, provider}`, `scripts/spark_mor_fixture.sh`

## Context

Iceberg v2 `refs` name snapshots:

- **tags** are fixed, e.g. `eod-2024-05-01` or `audit-q1`;
- **branches** are moving heads, e.g. `main`, or `staging` for
  write-audit-publish, where writers commit to a branch, validate it and
  then fast-forward `main`.

ADR 0028 accepted snapshot ids and instants only. Users had to look up ids,
and could not read a WAP branch at all, because branch commits are not in
`main`'s `snapshot-log`, so no instant reaches them.

## Decision drivers

1. One syntax for every "as of": `"table@<spec>"`.
2. Unambiguous resolution order. Existing specs keep their meaning.
3. Names that are not specs keep working as plain table names (ADR 0028's
   rule).
4. Grants and every read-path guarantee unchanged (ADR 0028 driver 1).

## Considered options

**A. Resolve the spec in order: snapshot id, then instant, then ref name;
otherwise a whole-name lookup (chosen).** A ref is looked up only after
the spec failed to parse as an id or an instant. So a tag named `123` is
shadowed by snapshot id 123: ids win. That is deterministic, and matches
how engines treat numeric versions.

**B. A prefix per kind (`@branch:audit`, `@tag:eod`).** It is
self-describing, but more verbose, and it breaks the convention other
engines use. The kind is already recorded in `refs`, so the prefix carries
no information.

**C. Refs first.** A ref named like a snapshot id or an instant would
silently change the meaning of existing queries. Rejected (driver 2).

## Decision

- `iceberg::ref_snapshot(metadata, name)` reads
  `refs.<name>.snapshot-id`. A branch's head is its newest snapshot; a tag is
  fixed. Both pin through `iceberg::at(…, AsOf::Snapshot(id))`, so the
  snapshot's own schema applies (ADR 0028).
- `CatalogSchema::table(name)`, when the name contains `@`:
  1. load the base table;
  2. try the spec as an id or instant, then as a ref;
  3. if none apply, load the whole name.

  So a table literally named `x@y` still resolves when `y` is not a ref of
  `x`.
- The lookup and provider construction are factored into `load` and
  `provider`, shared by both paths.

## Consequences

**Positive**
- `bronze."trips@audit"` reads a WAP branch before it is published.
  `bronze."trips@eod-2024-05-01"` reads a tag with no id lookup.
- The read path, grants and `EXPLAIN` (`snapshot=`) are unchanged.

**Negative (accepted)**
- Two `loadTable` calls for a name with `@` that is neither a spec nor a
  ref, which is rare: one for the base and one for the whole name.
- The `main` ref and the current snapshot are normally the same, but are
  not checked for agreement. Plimsoll reads `current-snapshot-id` for plain
  names, as before.
- Refs were not listable; ADR 0030 adds `"table$refs"`.

## Risks and mitigations

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| A tag named like a snapshot id or instant is shadowed | Low | Low | Ids and instants win by design; documented; use the snapshot id the tag points at. |
| A branch's head moves between planning and a later query | Expected | — | Per-query `loadTable` (ADR 0008): each query sees the head at its planning time; `EXPLAIN` shows which. |

## Validation

- `lakehouse::tests::time_travel_by_snapshot_id_or_instant`, with refs added:
  - the `v1` tag (snapshot 1) sees column `a`;
  - the `main` branch sees `b`;
  - `users@yesterday` still falls back to a name lookup and is not found.
- `tests/iceberg.rs::time_travels_a_spark_table`, on the Spark fixture,
  which now has `CREATE TAG after_insert`, `CREATE BRANCH audit` and an
  insert into `branch_audit`:
  - the tag reads 48 rows with sum 1128;
  - the `audit` branch reads 49 rows with sum 2128, including the
    branch-only row;
  - `main` reads 32 rows with sum 768;
  - main's MOR answers are unchanged.

## Revisit when

- Refs need listing (`$refs` metadata table).
- WAP publishing flows need Plimsoll to verify `main` against a branch.

# ADR 0006 — Per-source pushdown policy

- Status: Accepted
- Scope: `plimsoll-core::federation::PushdownPolicy`, `plimsoll-connectors`

## Context

Whether a predicate can be evaluated remotely with *identical* semantics
depends on the source and on the column type, not on the operator alone.
`a = 'x'` is collation-sensitive; `x < 0.0` differs between Postgres
(`-0.0 = 0.0`) and Arrow (IEEE total order); `LIKE` escaping and case folding
differ; timestamps carry timezone/precision rules. A single global switch is
either unsafe or useless.

## Decision

`PushdownPolicy` is a plain struct, one field per expression class, each a
DataFusion `TableProviderFilterPushDown`:

| Class | Example |
|---|---|
| `integer_cmp`, `float_cmp`, `decimal_cmp`, `temporal_cmp`, `bool_cmp`, `text_cmp` | `col <op> literal`, classified by the column's Arrow type |
| `like` | `LIKE`, `ILIKE`, `SIMILAR TO` |
| `null_check` | `col IS [NOT] NULL` |
| `other` | casts, functions, `IN`, column-vs-column, anything unrecognised |
| `limit: bool` | may `LIMIT` reach the source |

`AND`/`OR`/`NOT` take the weakest decision of their parts. The default is
`local_only()`: everything `Unsupported`. Aggregates are never federated — a
`TableProvider` cannot receive them, and doing so needs a separate plan-level
mechanism.

Shipped policies:

- **Parquet** (`ListingTable`): `uniform(Inexact)` — DataFusion reads the files
  itself; pruning is a hint and every filter is re-checked.
- **Postgres**: `integer_cmp`, `bool_cmp`, `null_check` = `Exact`, `limit` on;
  everything else `Unsupported`. Floats are not even `Inexact` because the
  remote could drop rows the engine would keep (ADR 0005).

Postgres SQL generation: identifiers are always double-quoted (`"` doubled);
literals are always bind parameters with an explicit cast (`$n::int8`), never
inlined. Untranslatable filters are reported `Unsupported` by the source.
Connection string comes only from `PLIMSOLL_PG_URL`; missing → typed error at
construction. Unsupported column types → typed error at construction.

Dependency: `tokio-postgres` (+ `with-chrono-0_4`) — the de-facto async
Postgres wire client, no ORM/pool/macro layer; `chrono` is already in the Arrow
dependency tree.

## Alternatives

- **Per-operator allow-list without types.** Cannot express "integer `>` ok,
  text `>` not". Rejected.
- **Ask the source to self-describe (capabilities from the driver).** The
  driver does not know the engine's semantics; trust must stay on our side.
- **`sqlx` / `diesel`.** Heavier (macros, compile-time checks, pools) for a
  single dynamic `SELECT`. Rejected.
- **Inline escaped literals.** Works but one escaping bug is SQL injection;
  parameters remove the class of bug.

## Consequences

- Pushdown is deliberately narrow; widening a class is a one-line policy change
  plus a semantics test, per source.
- `PostgresConnector::with_policy` allows tightening/loosening per table.
- Known ceilings (`ponytail:` in code): one connection per scan, no TLS, whole
  result materialised in one batch, `NUMERIC` surfaced as `Utf8`. Upgrade path:
  pool + rustls, streamed `query_raw` batches under the memory ceiling, decimal
  decoding via typmod.

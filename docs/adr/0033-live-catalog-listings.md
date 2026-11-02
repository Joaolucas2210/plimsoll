# ADR 0033 — Live catalog listings

- Status: Accepted
- Date: 2026-11-01
- Owner: JoaoLucas2210
- Related: ADR 0008 (per-query `loadTable`), ADR 0030 (metadata tables), ADR 0027 (log events)
- Scope: `plimsoll-server::lakehouse::{refresh, register, Refreshed, CatalogSchema::names}`, `config::catalog_refresh`, `main::refresh_catalog`

## Context

Table *resolution* has been live since ADR 0008: `SELECT … FROM
bronze.trips` calls `loadTable` on every query. Table *listing* was not.
`CatalogSchema::names` was filled once at boot, and namespaces were
registered once at boot. So after startup:

- a table created by Spark or dbt was queryable but missing from
  `SHOW TABLES` and `information_schema.tables`;
- a new namespace was neither listed nor queryable, because
  `platinum.facts` fails with "schema not found".

BI tools and SQL clients discover tables through `information_schema`.
To them, a table that is not listed does not exist. The workaround was a
restart, which drains queries (ADR 0026).

## Decision drivers

1. New tables and namespaces become discoverable without a restart.
2. Listing never slows queries. `SchemaProvider::table_names` is
   synchronous and is called during planning of `information_schema`
   queries.
3. A catalog outage must not empty the listings. A flapping catalog would
   otherwise make tables "disappear" from BI tools.
4. Env-registered tables (`PLIMSOLL_*_TABLES`) are never shadowed by a
   namespace created later with the same name.

## Considered options

**A. Periodic background re-list, then swap (chosen).** A task calls
`refresh` every `PLIMSOLL_CATALOG_REFRESH_MS` (default 60 s):

- it lists every namespace and its tables;
- only then does it update each schema's name list (`RwLock`) and register
  new namespaces.

Any error before the swap leaves everything as it was. Planning only reads
the last list (driver 2).

**B. List on demand in `table_names`.** It is synchronous, so we cannot
await the catalog there without blocking a runtime thread inside planning.
Rejected.

**C. Catalog change notifications.** Iceberg REST has no standard event
API, and Polaris events are deployment-specific. Polling is portable.

**D. Add names lazily when `table()` resolves one.** It does not help
discovery: a table nobody has queried is still not listed. It is not
needed on top of A.

## Decision

- `lakehouse::refresh(ctx, catalog) -> Refreshed { namespaces,
  new_namespaces, changed, skipped }`. `register` is the boot call of the
  same function, and logs `skipped` as `namespace_skipped`.
- **Updating.** For each single-level namespace:
  - an existing `CatalogSchema` (found by downcast) gets its sorted names
    swapped in;
  - otherwise a new schema is registered, but only where no schema exists
    or the existing one has no tables (the empty medallion placeholders);
  - a schema holding env tables is skipped with the reason
    `schema has env tables`.
- **Dropped namespaces.** Catalog schemas whose namespace disappeared are
  emptied. Their tables already fail to resolve, because lookups are live.
- **Background task.** `main` spawns `refresh_catalog` when a catalog is
  configured and the interval is non-zero. Missed ticks are delayed, not
  bursted.
  - When a refresh changes something, it logs `catalog_refreshed` with
    `new_namespaces` and `changed`.
  - When the catalog fails, it logs `catalog_refresh_failed` (warn) and
    retries on the next tick.
- **Configuration.** `PLIMSOLL_CATALOG_REFRESH_MS` takes a non-negative
  integer. `0` means boot only, the previous behaviour.

## Consequences

**Positive**
- Tables appear in BI tools within one interval of their creation, and new
  namespaces become queryable. No restart is needed.
- Outages are invisible to listings: the last good list is kept and a warn
  line is logged.

**Negative (accepted)**
- Up to one interval of staleness for *listings*. Queries are unaffected.
- One `listNamespaces` plus N `listTables` calls per interval. That is
  negligible for typical catalogs. Raise the interval for catalogs with
  thousands of namespaces.
- A dropped namespace keeps an empty schema until restart. DataFusion
  schemas are not deregistered, so a query against it fails with "table not
  found" instead of "schema not found".
- Multi-level namespaces are still skipped (ADR 0008's limitation).

## Risks and mitigations

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| Catalog rate limits from polling | Low | Low | 60 s default; configurable; one list per namespace. |
| A partial list from a mid-refresh error | — | — | List everything, then swap: an error aborts before any change (tested). |
| A new namespace collides with env tables | Low | Medium | Never replaced; skipped with a reason (tested). |

## Validation

- `lakehouse::tests::refresh_tracks_tables_and_namespaces_created_after_boot`:
  - after boot, `information_schema.tables` lists `gold.kpis` (env) and
    `silver.users`;
  - after creating `silver.orders`, a `platinum` namespace and a
    `gold.shadow` in the catalog, a refresh:
    - reports `new_namespaces=[platinum]` and `changed=[silver]`;
    - skips `gold` (env tables);
    - lists all four tables;
  - a no-op refresh reports nothing;
  - with the catalog down, the refresh errors and the listing is
    unchanged;
  - back up, a dropped namespace stops listing its tables.
- `config::tests::catalog_refresh_defaults_to_a_minute_and_zero_disables_it`.

## Revisit when

- Catalogs expose change events (Polaris events, REST extensions).
- Very large catalogs need incremental listing (paginated, per-namespace
  intervals).
- DataFusion supports deregistering schemas from `MemoryCatalogProvider`
  cleanly enough to drop vanished namespaces.

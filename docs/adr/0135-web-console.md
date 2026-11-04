# ADR 0135 — A web console in the server

- Status: Accepted
- Date: 2026-11-04
- Owner: JoaoLucas2210
- Related: ADR 0021 (tokens), ADR 0022 (scoped system tables), ADR 0034 (cancel), ADR 0035 (NDJSON streaming), ADR 0043 (parameters), ADR 0123 (quickstart)
- Scope: `crates/plimsoll-server/ui/*`, `ui.rs`, `compose.yaml`, README

## Context

Using Plimsoll meant `curl`, a notebook, or a desktop SQL client over
Flight SQL. People need a quick, friendly way to see what they can read
and to run queries. The requirement was also that it must not weigh on
the project. A BI server (Metabase, Superset) is a JVM or a Python stack,
with its own database. Metabase also has no Flight SQL driver, and
Plimsoll does not speak the Postgres wire protocol.

## Decision

- **Three static files** (`ui/index.html`, `app.css`, `app.js`): plain
  HTML, CSS and JavaScript, with no framework and no build step. They
  are compiled into the binary (`include_str!`) and served at `/ui`.
  `/` redirects there. Every node serves it; it holds no data.
- **It is just another API client.**
  - It signs in with a bearer token, kept in `sessionStorage` (gone
    with the tab).
  - It calls `POST /v1/sql`, so authentication, grants, column denies,
    row filters, admission, deadlines and the query log are the API's.
    The console adds no privilege and no endpoint.
- **What it does:**
  - **Catalog tree.** Catalogs → schemas → tables, from
    `information_schema.tables`, as the user sees them (grants
    applied). A click lists a table's columns, types and nullability
    (`information_schema.columns`, filtered with bound parameters, ADR
    0043). A double-click runs `SELECT * … LIMIT 100`. The tree filters
    by name.
  - **SQL editor.** Runs with Ctrl/⌘+Enter, as NDJSON (ADR 0035).
    Rows fill a grid; the status shows rows, time and query id.
  - **Bounded results.** At 1,000 rows it stops reading and closes the
    stream, which cancels the query on the server (ADR 0035): a large
    result never fills the browser.
  - **Cancel** calls `DELETE /v1/queries/{id}` (ADR 0034), then closes
    the stream.
  - Errors show the API's code and message.
- **Hardening.**
  - Every value is written as text (`textContent`), never as HTML.
  - A strict Content-Security-Policy: `default-src 'none'`, this
    origin's script and style only, `frame-ancestors 'none'`.
  - `nosniff`, `no-referrer`.
  - The page has nothing inline, and a test checks it.
- **Quickstart uses the released image.** `compose.yaml` names
  `ghcr.io/joaolucas2210/plimsoll:${PLIMSOLL_VERSION:-latest}` with
  `pull_policy: missing`. It pulls when logged in to GHCR (the package
  is private), and falls back to building from source when it cannot
  pull. That fallback was checked with an image that does not exist.

## Consequences

**Positive**
- Zero added weight: no container, service, dependency or toolchain.
  It adds about 15 KB to the binary.
- The same governance as every other client, by construction.
- Usable on any node and any browser, light or dark.

**Negative (accepted debt)**
- **No saved queries, history, charts or result export.** A desktop
  client (DBeaver over Flight SQL) or a BI tool does that.
- **The token sits in the tab's session storage.** Any script on this
  origin could read it. The policy allows only the console's own
  script, and nothing else is served on that origin.
- **The console ships with the server.** Images before the next release
  (`v0.1.0`) do not have it.

## Validation

- `ui::tests`: each asset is served with its type, the policy (no
  `unsafe-*`) and `nosniff`; `/` redirects to `/ui`; the page has no
  inline script, style or handler.
- `node --check` on `app.js`.
- The real page and script, driven in jsdom against the running binary
  over a Parquet table:
  - sign-in refuses a wrong token and accepts the right one;
  - the tree lists `bronze.trips` and its columns with types, and the
    filter hides other tables;
  - a double-click writes and runs `SELECT * FROM "bronze"."trips"
    LIMIT 100` (100 rows);
  - a cross join stops at 1,000 rows, and the server logged the query
    `client_disconnected`;
  - an unknown table shows `invalid_query`;
  - a value `<img src=x onerror=…>` stays text;
  - sign-out forgets the token.

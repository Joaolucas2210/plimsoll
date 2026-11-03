# ADR 0024 — Read-only query API

- Status: Accepted (amended by ADR 0075, 0077, 0081, 0082 and 0083: `INSERT INTO`, `DELETE FROM`, `UPDATE`, `MERGE INTO` and `CREATE TABLE` are allowed, granted per schema)
- Date: 2026-10-30
- Owner: JoaoLucas2210
- Related: ADR 0002 (operator registers tables), ADR 0004 (HTTP surface), ADR 0013 (error classes), ADR 0021/0022/0023 (identity and authorization this would have bypassed)
- Scope: `plimsoll_core::engine::Engine::run`

## Context

While designing per-namespace data governance, a review of what
`POST /v1/sql` can do found that `Engine` ran user SQL with
`SessionContext::sql`. That method accepts **every** statement DataFusion
supports. Verified against the running server (auth disabled for brevity;
any valid token would do):

| Statement | Effect |
|---|---|
| `CREATE EXTERNAL TABLE leak STORED AS CSV LOCATION '/path/secret.csv'` then `SELECT * FROM leak` | Read an arbitrary file readable by the server process (credentials, keys, `/etc/*`). Also object storage under the server's env credentials. |
| `COPY (SELECT …) TO '/path/x.csv'` | Write arbitrary files as the server user. |
| `CREATE VIEW` / `CREATE TABLE` / `DROP …` | Mutate the catalogs **every** user shares. Tables created in one query persisted for all later queries. |
| `SET datafusion.execution.…` | Tune engine settings. Per-query state since ADR 0022, but still not the caller's business. |

Severity is critical. Any principal could read and write the server's
filesystem. That bypasses every control built so far: authentication,
principal scoping and resource groups.

## Decision drivers

1. The query API is a **read** path. Plimsoll's product is querying a
   lakehouse; tables come from the operator (env, REST catalog), never from
   callers.
2. Deny by default, at the engine. Not in the HTTP layer, so every caller
   of `Engine` gets it.
3. Enforced on the whole plan, subqueries included.
4. No new dependency or parser.

## Considered options

**A. DataFusion `SQLOptions` with DDL, DML and statements disabled
(chosen).** `SessionContext::sql_with_options` runs `verify_plan` over the
full logical plan, subqueries included. It rejects `Ddl`, `Dml`, `Copy` and
`Statement` nodes with a planning error. This is a native feature, one call
site and no parsing of our own.

**B. Parse and allow-list statements in the handler.** That is a second SQL
parser to keep in sync, and it would protect only HTTP callers. Rejected.

**C. Sandbox the filesystem (chroot, read-only container, no local object
store).** Good defence in depth, and recommended for deployments. But it does
not stop catalog mutation, and correctness should not depend on deployment
hardening.

**D. Per-principal DDL rights.** There is no use case: tables are
operator-managed. Revisit if Plimsoll ever offers a write path.

## Decision

- `Engine::run` (behind both `Engine::sql` and `Engine::sql_with`) uses
  `sql_with_options` with `allow_ddl = false`, `allow_dml = false` and
  `allow_statements = false`.
- Operators still register tables through code paths that use the
  `SessionContext` directly:
  - env tables (`tables.rs`);
  - the lakehouse catalog;
  - system tables;
  - tests (`engine.context().sql(...)`).
- Rejections are planning errors such as `DDL not supported:
  CreateExternalTable` and `DML not supported: COPY`. Clients see them as
  `400 invalid_query`, non-retryable (ADR 0013).
- DataFusion's URL tables (`SELECT * FROM '/path/file.csv'`) are not
  enabled; this ADR keeps it that way. Verified: such a query fails with
  "table not found".

## Consequences

**Positive**
- Closes arbitrary file read, file write and shared-catalog mutation through
  the API.
- The guard sits in the core, so future front doors (Flight SQL, other
  protocols) inherit it.

**Negative (accepted)**
- There are no temporary views or `CREATE TABLE AS` for users. Users needing
  intermediate results use CTEs, which still work.
- `EXPLAIN` still works (it is not a statement). `PREPARE`/`EXECUTE` do not:
  they are statements.

**Operational**
- Defence in depth is still recommended: run the server as an unprivileged
  user, with a read-only root filesystem, and scope its object store
  credentials to the warehouse.

## Risks and mitigations

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| A future code path runs user SQL via `SessionContext::sql` directly | Medium | Critical | All user SQL goes through `Engine::run`; review rule; test `query_api_is_read_only`. |
| A DataFusion upgrade adds a plan node with side effects outside the guarded categories | Low | High | Re-run the test on upgrades; check `verify_plan` coverage in release notes. |
| URL tables get enabled by a config change | Low | Critical | Not enabled anywhere; this ADR forbids it; the smoke query is in Validation. |

## Validation

- `engine::tests::query_api_is_read_only`:
  - rejected: `CREATE EXTERNAL TABLE` on a secret file, `COPY … TO`,
    `CREATE VIEW`, `DROP SCHEMA`, `SET`;
  - nothing is written and no table appears;
  - medallion schemas are intact;
  - CTE queries still run.
- `engine::tests::medallion_schemas_exist` registers through the context,
  not the query API.
- Manual exploit replay on the binary, before and after the fix:
  - before: the secret file was read (`hunter2`) and `COPY` wrote a file;
  - after: `400 DDL not supported` / `DML not supported: COPY`, no file
    written, URL table not found, `SELECT 42` still 200.

## Revisit when

- Plimsoll offers a write path (`INSERT` into Iceberg): allow specific DML
  per principal, never DDL on local paths.
- A new protocol front door is added: confirm it goes through `Engine::run`.

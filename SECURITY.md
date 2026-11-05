# Security policy

**English** · [Português (Brasil)](SECURITY.pt-BR.md)

Plimsoll promises that a query fails and the node doesn't, and that governance (grants, column denies, row
filters) applies to every scan. A bug that breaks either promise is a vulnerability. Please report it privately
and give us the chance to fix it before it is public.

## Supported versions

| Version | Supported |
| --- | --- |
| `main` and the latest `0.x` release | Yes |
| Older `0.x` releases | No; upgrade first |

The project is experimental (`0.x`): the HTTP API, Flight SQL behaviour and environment variables may change
between minor versions, and the [CHANGELOG](CHANGELOG.md) says so.

## How to report

- Use GitHub's private reporting: **Security → Report a vulnerability** on this repository
  (<https://github.com/Joaolucas2210/plimsoll/security/advisories/new>).
- Do not open a public issue, pull request or discussion for a suspected vulnerability.
- Include the commit SHA or release tag, the role (single node, coordinator, worker), the relevant
  `PLIMSOLL_*` settings with secrets removed, the smallest SQL or request that reproduces it, and what you
  expected versus what happened.
- Never send real tokens, catalog credentials, storage keys or private data. Synthetic tables are enough.

## What to expect

| Step | Target |
| --- | --- |
| Acknowledgement | within 3 business days |
| First assessment (valid, severity, affected versions) | within 10 business days |
| Fix or mitigation for a confirmed high/critical issue | as fast as possible, coordinated with you |
| Public advisory | after a fixed release, crediting you unless you prefer otherwise |

These are goals of a single-maintainer project, not a contractual SLA.

## In scope

- **The node dies because of one query:** a panic, an abort, or the kernel OOM killer triggered by SQL or a
  request from an authenticated user, under the documented memory ceiling (ADR 0003).
- **Governance bypass:** reading a schema without a read grant, a denied column, or rows outside a row filter;
  writing or dropping without the matching grant (ADR 0025, 0044, 0045, 0077, 0089).
- **Authentication bypass** on `/v1/sql`, Flight SQL, `/v1/queries`, `/v1/maintenance/*`, the worker endpoint
  `/v1/fragment` (`PLIMSOLL_WORKER_TOKEN`) or the peer endpoints `/v1/peer/*`.
- **Injection:** a bound parameter (`params`, Flight SQL prepared statements) that changes the SQL, or a federated
  Postgres filter that reaches the source as anything but a parameter (ADR 0005, 0006, 0043).
- **Wrong results reported as success** because a pushed-down filter was dropped (ADR 0005).
- **Secrets or SQL text in logs, traces or metrics** (tokens, `PLIMSOLL_PG_URL`, catalog credentials, AWS keys,
  OTLP headers).
- **Data loss in table maintenance:** compaction, snapshot expiration or orphan removal deleting files that a
  live snapshot, branch or tag still references (ADR 0078–0080).

## Out of scope

These are documented limits, not vulnerabilities:

- Anything with `PLIMSOLL_AUTH=disabled`, which trusts the `user` in the body by design (development only).
- Plain HTTP between nodes or to clients when TLS (`PLIMSOLL_TLS_*`) is not configured; a node exposed to the
  internet without TLS.
- Memory paths listed as accounted-but-not-refused in [ADR 0003](docs/adr/0003-memory-ceiling.md), unless they
  kill the process under the documented configuration.
- Resource exhaustion that admission is configured to allow: large `PLIMSOLL_MAX_RUNNING`, an unbounded queue,
  or a memory limit above the host's physical memory.
- A holder of a valid token doing what its grants allow; a holder of `PLIMSOLL_WORKER_TOKEN` acting as a
  coordinator (it is a cluster-internal shared secret).
- Bugs in Apache DataFusion, Arrow or Iceberg libraries themselves: report them upstream, and tell us if they
  crash a Plimsoll node.

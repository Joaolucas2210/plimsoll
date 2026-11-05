# Contributing to Plimsoll

**English** · [Português (Brasil)](CONTRIBUTING.pt-BR.md)

Thanks for helping. Plimsoll has one rule that shapes every change — **the query fails, the node doesn't** — and
one habit: every decision is written down as an ADR, with its accepted debt.

## Before you start

- Read the [architecture](docs/architecture.md) and the [ADR index](docs/adr/README.md), which has a reading
  order by area. [ROADMAP.md](ROADMAP.md) lists what is done, the next obvious PR and what is missing.
- Open an issue first for anything bigger than a small fix. A change of behaviour, a new endpoint, a new
  environment variable or a new dependency needs an ADR (copy [docs/adr/0000-template.md](docs/adr/0000-template.md)).
- Report vulnerabilities privately, as described in [SECURITY.md](SECURITY.md). Never in a public issue.

## Development setup

Rust ≥ 1.94 (edition 2024), `curl` and `python3`. Docker only for the Compose quickstart and the live tests.

```bash
git clone https://github.com/Joaolucas2210/plimsoll.git && cd plimsoll
make help        # the entry points
make demo        # builds the server and overloads one local node on purpose
make gates       # fmt, clippy -D warnings, cargo test --workspace, plane invariants
```

## Repository layout

| Path | What it is |
| --- | --- |
| `crates/plimsoll-core/` | Compute plane: engine façade, `CeilingPool`, federation guard. Never depends on connectors |
| `crates/plimsoll-connectors/` | Iceberg, Parquet, Postgres, distribution (fragments, shuffles, broadcasts) |
| `crates/plimsoll-control/` | Control plane: admission, registry, Iceberg REST client. Never depends on DataFusion or core |
| `crates/plimsoll-server/` | The binary: HTTP, Flight SQL, auth, TLS, config, observability, the web console |
| `docs/adr/` | One decision per file; accepted ADRs are superseded, never rewritten |
| `scripts/` | `gates.sh`, `demo.sh`, Iceberg and Spark fixtures, the TPC-H benchmark |

## Workflow

1. Branch from `main`; open a pull request. `main` takes changes only through pull requests, merged by squash, so
   each change is one commit with a sentence-style summary, as in the history (`Batched Flight SQL inserts as one
   statement and one commit`).
2. The required checks are `gates + live integration` and `secret scan`. A docs-only change skips the heavy job,
   which counts as passing (ADR 0136).
3. Tests come with the change. The invariant has its own tests: a change that touches memory, admission or
   cancellation adds a case where the query fails and the node keeps serving.
4. Run `make gates` and, for user-visible behaviour, `make demo` before pushing. A gate you could not run is
   reported as skipped in the PR, never as passing.
5. Update `CHANGELOG.md` under `[Unreleased]` for anything a user can see: API, Flight SQL, environment variables,
   SQL accepted.

## Conventions

- **No `unwrap`/`expect` outside tests** (workspace lints). Errors are typed and classified at the source
  (ADR 0013); a new error code is an API change.
- **Memory goes through the pool.** A new buffer, cache or connector reserves from the `CeilingPool`; an
  allocation it cannot account for is named as debt in its ADR.
- **Secrets come from the environment only** and are never logged; logs carry a SQL fingerprint, never SQL text.
- **Shortcuts are marked** with a `ponytail:` comment that names the ceiling and the upgrade path; the owning ADR
  lists them under *Consequences → Negative*.
- **Every claim is checkable:** ADRs and docs cite type, variable, test or CI job names, not paraphrases.
- **Docs:** English first. The README has a Portuguese sibling (`README.pt-BR.md`) with the same sections; change
  both in the same PR.

## License

By contributing you agree that your contribution is licensed under the [Apache License 2.0](LICENSE). Please
follow the [Code of Conduct](CODE_OF_CONDUCT.md).

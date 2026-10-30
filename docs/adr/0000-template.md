# ADR NNNN — Title

- Status: Proposed | Accepted | Superseded by ADR XXXX
- Date: YYYY-MM-DD
- Owner: <github-handle, plain text>
- Related: ADR ..., ADR ...
- Scope: crates / modules affected

<!--
Rules for authors
- One decision per ADR. 60–150 lines. A staff engineer must be able to read it
  in three minutes and either defend or revisit the decision.
- Every concrete claim (type, env var, error, test, CI job, dependency version)
  must exist in the repo at the commit that accepts the ADR. Link names, not
  line numbers.
- Never edit an Accepted decision in place to change it. Write a new ADR and
  set this one to "Superseded by ADR XXXX". Fixing a factual error, a stale
  debt list or a broken reference is allowed and should be noted in the PR.
- Known shortcuts in code are marked `ponytail:`; list the ones this decision
  owns under Consequences → Negative.
-->

## Context

The problem, the constraints and the forces in tension. Numbers where they
exist (sizes, latencies, versions). State what breaks if we do nothing.

## Decision drivers

Ranked; the order is what resolves conflicts between options.

1. ...
2. ...

## Considered options

### Option A — name (chosen)

One paragraph. Pros / cons, including cost: dependencies, complexity,
operations.

### Option B — name

...

## Decision

What we do, precisely: types, functions, env vars, invariants, defaults.
Close with one paragraph on why this beats the runner-up.

## Consequences

**Positive.** ...

**Negative (accepted debt).** ...

**Operational.** Metrics, failure modes, what an on-call engineer sees
(HTTP status, error code, log line) and how the system fails.

## Risks and mitigations

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| ... | Low/Med/High | Low/Med/High | ... |

## Validation

Tests, CI jobs and gates that prove the decision holds. Use real names:
`crate::module::test_name`, CI job ids from `.github/workflows/ci.yml`,
steps of `scripts/gates.sh`.

## Revisit when

Concrete, observable triggers, e.g. "a deployment needs GCS", "p99 planning
latency > 200 ms", "DataFusion exposes per-scan reservations".

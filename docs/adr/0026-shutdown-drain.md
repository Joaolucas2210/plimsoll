# ADR 0026 — Shutdown drain: unadvertise, keep serving, then close

- Status: Accepted
- Date: 2026-10-30
- Owner: JoaoLucas2210
- Related: ADR 0004 (resolves its drain `ponytail:`), ADR 0013 (`not_ready`)
- Scope: `plimsoll-server` (`app::{AppState::{ready, serving}, drain}`, `config::drain_delay`, `main::shutdown_signal`)

## Context

On SIGTERM the server flipped `ready` to false and immediately closed the
listener. Two problems:

1. **No delay.** Load balancers and Kubernetes endpoints learn about
   readiness only on their next probe (Kubernetes default period: 10 s), and
   endpoint propagation adds more. In that window they keep routing new
   connections to a pod whose listener is closed. Clients see connection
   refused or resets during every rolling deploy.
2. **One flag, two meanings.** `/v1/sql` rejected queries with
   `503 not_ready` whenever `ready` was false. That flag is right at
   startup, before catalogs are loaded. During a drain it is wrong: the
   requests a load balancer still sends are exactly the ones a drain exists
   to serve.

## Decision drivers

1. Zero client-visible errors on a rolling deploy under a standard
   orchestrator.
2. Interactive use (ctrl-c) must stay instant.
3. In-flight queries finish. The query timeout still bounds them.
4. No new dependency; testable without real signals or sleeps.

## Considered options

**A. Separate readiness from serving, and delay closing on SIGTERM
(chosen).** `ready` drives `/readyz` only; `serving` gates `/v1/sql`. On
SIGTERM: set `ready` to false, keep serving for a delay, then return from
the shutdown future so axum closes the listener and drains connections.

**B. Rely on a Kubernetes `preStop` sleep hook.** It works on Kubernetes
only, and every deployment must remember it. A built-in delay is portable,
and `preStop` can still be used with `PLIMSOLL_DRAIN_DELAY_MS=0`.

**C. Keep one flag, add a delay.** During the delay, every query would be
rejected with 503. That changes the error, not the outcome. Rejected.

## Decision

- `AppState.ready` is for `/readyz` and `/healthz`. It is false at startup
  and from SIGTERM on.
- `AppState.serving` is for `/v1/sql`. It is false only at startup and
  becomes true together with `ready` once boot finishes.
- `app::drain(state, delay)` sets `ready` to false, then sleeps `delay`.
- `shutdown_signal`:
  - SIGTERM uses `PLIMSOLL_DRAIN_DELAY_MS`, default **5000**;
  - ctrl-c uses a delay of zero;
  - after `drain`, axum's graceful shutdown stops accepting and waits for
    in-flight requests.
- `PLIMSOLL_DRAIN_DELAY_MS` is a non-negative integer, and `0` disables
  the delay.

## Consequences

**Positive**
- Rolling deploys: the pod disappears from endpoints while still answering.
  No refused connections in the probe window.
- Late requests during the drain are served, not rejected.
- Startup behaviour is unchanged: queries before boot finishes get `503
  not_ready`, which is retryable.

**Negative (accepted)**
- Pod termination takes `delay` longer, plus in-flight time. Kubernetes
  `terminationGracePeriodSeconds` (default 30 s) must exceed
  `delay + query timeout`, or queries are killed by SIGKILL. With the
  defaults, 5 s + 30 s is above 30 s: raise the grace period, or lower the
  query timeout, in Kubernetes deployments.
- A second SIGTERM during the delay does not shorten it (ctrl-c does not
  interrupt a SIGTERM drain either).

**Operational**
- Kubernetes: set `terminationGracePeriodSeconds` ≥ drain delay + query
  timeout + margin (e.g. 45 s with the defaults).
- Behind a load balancer with a slower health check, raise the delay to at
  least one probe period plus propagation.

## Risks and mitigations

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| Grace period shorter than delay + timeout kills queries | Medium | Medium | Documented sizing rule; queries are retryable on the client side. |
| Delay too short for the LB's probe interval | Medium | Low | Configurable; default covers Kubernetes' typical 5–10 s with propagation. |
| Serving flag left false after a failed boot step | Low | High | Boot sets `serving` then `ready` only after every boot step succeeded; a failed step exits the process. |

## Validation

- `app::tests::drain_unadvertises_but_keeps_serving_for_the_delay`
  (tokio paused clock):
  - during the drain, `/readyz` is 503 and `/v1/sql` is 200;
  - the drain completes exactly when the delay elapses.
- `app::tests::sql_rejected_when_not_ready`: before boot (`serving` false),
  the response is `503 not_ready`.
- Manual smoke on the binary with `PLIMSOLL_DRAIN_DELAY_MS=2000`:
  - after SIGTERM, `/readyz` returned 503 and `SELECT 1` returned 200;
  - the process exited 0 after 2.0 s;
  - the logs show "not ready, serving for 2s before closing" and then
    "closing the listener".

## Revisit when

- Long-running queries make `delay + timeout` exceed acceptable termination
  times (per-group timeouts, or cancel on drain).
- Connection-level draining (HTTP keep-alive `Connection: close` during the
  delay) is needed for clients with long-lived connections.

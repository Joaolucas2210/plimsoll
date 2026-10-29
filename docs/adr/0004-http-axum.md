# ADR 0004 — HTTP surface with axum 0.8

- Status: Accepted
- Scope: `plimsoll-server`

## Context

The server needs: health probes (`/livez`, `/readyz`, `/healthz`),
Prometheus `/metrics`, and `POST /v1/sql` with JSON in/out, graceful shutdown,
and handler tests without opening sockets.

## Decision

Use **axum 0.8** with `default-features = false` and only
`http1`, `json`, `tokio`.

- Built on `tokio` + `hyper` + `tower`, which are already in the tree via
  tokio; the marginal dependency cost is small.
- `Router` is a `tower::Service`: tests call `oneshot` in-process, no network.
- `axum::serve(..).with_graceful_shutdown(..)` gives drain-on-SIGTERM for free.

Rejected:

- **Raw hyper**: more code for routing, JSON extraction and shutdown; no gain.
- **actix-web**: its own runtime model and a larger tree.
- **A metrics crate** (`prometheus`, `metrics` + exporter): the six series we
  export are rendered by ~40 lines over values read at scrape time
  (`metrics.rs`). Revisit when histograms are needed.
- **reqwest for the catalog client**: deferred; the Iceberg REST client is a
  trait with an in-memory implementation until the Iceberg TableProvider
  needs real calls. When added: `reqwest` with `rustls-tls`, no OpenSSL.

## Consequences

- No HTTP/2, no TLS termination in-process (expected behind a proxy/ingress).
- `tower` appears as a dev-dependency only (`util` feature for `oneshot`).

# ADR 0099 — In-process TLS and a broadcast build-side limit

- Status: Accepted
- Date: 2026-11-03
- Owner: JoaoLucas2210
- Related: ADR 0021 (tokens), ADR 0058/0070 (workers, discovery), ADR 0066/0097/0098 (broadcast joins)
- Scope: `tls::{TlsFiles, acceptor, TlsListener}`, `PLIMSOLL_TLS_{CERT,KEY,CA}`, `remote::{trust, Workers::with_broadcast_limit, Built, local_join}`, `PLIMSOLL_BROADCAST_MAX_BYTES`, `remote::conform` (rows without columns)

## Context

Two gaps stood between Plimsoll and running without extra
infrastructure or surprises.

- **TLS needed a proxy.** The query API served plain HTTP (README
  non-goals). Bearer tokens, SQL and results crossed the network in
  clear text unless something terminated TLS in front, and traffic
  between coordinator and workers had nothing at all.
- **Broadcast build sides had no cap.** DataFusion chooses a
  `CollectLeft` join from estimates. When an estimate is low or
  missing, a broadcast join (ADR 0066) would ship a build side of any
  size to every worker: the network times the number of workers, plus
  the reservation on each node.

## Decision drivers

1. **No new crates.** rustls, tokio-rustls and hyper are already in the
   tree through reqwest (ring provider).
2. **Certificates rotate.** cert-manager and renewed secrets replace the
   files in place; a restart for that is an outage.
3. **A cap degrades, it does not fail.** A build side over the limit
   still gets its answer.

## Decision

- **TLS on the listener.**
  - `PLIMSOLL_TLS_CERT` and `PLIMSOLL_TLS_KEY` (PEM) enable it for
    everything the node serves: SQL, maintenance, metrics, health and
    worker fragments.
  - One without the other fails config. Files that do not load, or a key
    that does not match its chain, fail the boot.
  - TLS 1.2/1.3 (rustls safe defaults, ring), ALPN `http/1.1`.
  - **Handshakes run off the accept loop.** TCP accepts in one task, and
    each handshake runs in its own with a 10 s timeout. A slow or silent
    client costs a task, never the listener. axum serves the finished
    streams through its `Listener` trait, with graceful shutdown
    unchanged.
  - **Rotation.** On each handshake the files' modification times are
    checked. If either changed, the pair is reloaded (`tls_reloaded`).
    A pair that does not load keeps the old one and logs
    `tls_reload_failed`, which covers a half-written rotation.
- **TLS between nodes.**
  - Workers are reached at `https://` URLs (seeds or `dns+https://`).
  - `PLIMSOLL_TLS_CA` adds a PEM bundle to the trusted roots of every
    worker client, including reducers fetching buckets from peers, for
    clusters with a private CA. It is loaded at boot; an empty or bad
    bundle fails it.
- **Broadcast limit.**
  - `PLIMSOLL_BROADCAST_MAX_BYTES` (default 64 MiB of Arrow IPC; `0`
    disables broadcasts) caps what a broadcast join sends.
  - The build side is collected first, as before, and then measured. If
    it is over the limit, nothing is sent: the planned join runs on the
    coordinator over the build side already in hand, with its probe side
    as planned (a plain remote scan). Inner and right joins run it as
    their first partition; gathered joins (ADR 0097) run it as their
    only one.
- **Rows without columns.** Found while testing: a broadcast join that
  keeps no column (`count(*)` over `EXISTS`) returned column-less batches
  that `conform` rebuilt without their row count, and failed. Batches
  are now rebuilt with their row count.

## Consequences

**Positive**
- Plimsoll terminates TLS itself, rotates certificates without
  restarts, and encrypts coordinator-to-worker traffic.
- A misestimated broadcast can no longer flood the network. The query
  still completes, just like a local join.

**Negative (accepted debt)**
- **No client certificates (mTLS).** Workers authenticate callers with
  the shared token (ADR 0058) over TLS.
- **A build side over the limit is collected before being refused**,
  which costs its collection on the coordinator, not on the workers.
  Planning-time refusal needs better size estimates (column statistics,
  ROADMAP).
- **The fallback is not visible in `EXPLAIN`** (decided at run time).
  Worker logs show no `broadcast_probe` tasks.

## Validation

- `tls::tests::missing_or_mismatched_files_fail` and
  `config::tests::tls_needs_both_files_and_broadcasts_have_a_limit`.
- `tests/distributed.rs::distributed_scans_match_local_ones` (Spark
  fixtures, CI's interop job):
  - a coordinator with a 1-byte limit answers inner and left broadcast
    joins exactly as the local engine;
  - a `count(*)` over `EXISTS` (a join keeping no column) matches.
- By hand, with a private CA and openssl-made certificates:
  - a TLS coordinator answered `curl --cacert`;
  - plain HTTP and an unknown CA were refused;
  - with `PLIMSOLL_TLS_CA` it sent fragments to two TLS workers at
    `https://` URLs;
  - after the certificate files were replaced, the next connection
    presented the new certificate (`tls_reloaded`).
- By hand, a broadcast `EXISTS` with the default limit ran as
  `broadcast_probe` tasks. With a 10-byte limit, the workers saw only
  plain scans. Both answered 2000.

## Revisit when

- Workers must authenticate the coordinator by certificate: mTLS with
  `PLIMSOLL_TLS_CA` as the client root.
- Size estimates are good: refuse oversized broadcasts at planning time
  and plan a shuffled join (ADR 0065) instead.

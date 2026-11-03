# ADR 0070 — Worker discovery by DNS

- Status: Accepted
- Date: 2026-11-03
- Owner: JoaoLucas2210
- Related: ADR 0058 (coordinator/worker), ADR 0059 (health checks, local fallback), ADR 0004 (dependency stance)
- Scope: `remote::Workers` (`seeds`, `members`, `discover`, `check_health`), `config` (`PLIMSOLL_WORKERS`), the health loop in `main`

## Context

`PLIMSOLL_WORKERS` was a static list of URLs read at boot. Growing or
shrinking the worker pool meant editing the coordinator's environment
and restarting it, which drops every query in flight. On Kubernetes,
where workers are a Deployment that scales, that list is never right
for long.

The coordinator already polls every worker's `/readyz` every 5 seconds
(ADR 0059), and routes only to healthy ones, falling back to local
scans when none is healthy.

## Decision drivers

1. **Workers join and leave without a coordinator restart.**
2. **Use what platforms already provide.** No registry service, no new
   protocol, no new dependency.
3. **Degrade safely.** A DNS hiccup must not empty the pool.

## Considered options

**A. DNS names re-resolved by the health loop (chosen).** An entry
`dns+http://workers.ns.svc:8080` means "every address this name
resolves to, on port 8080, is a worker".
- A Kubernetes headless service resolves to exactly the ready pods'
  IPs. Consul, Nomad and plain round-robin DNS work the same way.
- Resolution uses `tokio::net::lookup_host`, the system resolver, so no
  new crate is needed.

**B. Workers register with the coordinator** (heartbeat endpoint).
- It needs a registration API, its authentication, expiry, and the
  coordinator's address configured on every worker.
- With several coordinators, each worker registers with each, or
  coordinators share state.
- It is more moving parts for what A gets from the platform.

**C. Kubernetes API watch.** This is precise, but it brings a client
dependency and RBAC, and is Kubernetes-only.

## Decision

- **Configuration.** `PLIMSOLL_WORKERS` entries are either
  `http(s)://host:port` (static, as before) or
  `dns+http(s)://name:port`.
  - Both are validated at boot as URLs with a host.
  - An entry like `dns+w1:8080` or `dns+ftp://…` fails fast.
  - Static and DNS entries can be mixed.
- **Membership.** `Workers` keeps the configured seeds and a current
  member list, each member being (URL, seed, healthy).
  - Static seeds are members from boot, presumed healthy, as before.
  - DNS seeds have no members until the first round, which the health
    loop runs immediately at start.
- **Each round** (every 5 s), in order:
  1. Every DNS seed is resolved. Each address (deduplicated and sorted)
     becomes a worker at `scheme://addr`; IPv6 addresses are bracketed.
     - An address already known keeps its health.
     - A new one joins **unhealthy**, and is checked in the same round.
     - An address no longer returned leaves.
     - A name that **fails to resolve keeps its last known members**,
       so a resolver blip changes nothing.
     - A name that resolves to no addresses has none.
  2. Every member's `/readyz` is checked, as in ADR 0059.
- **Planning is unchanged.** It takes the healthy URLs at planning time.
  - A worker that leaves mid-query fails that query's requests to it.
  - Scans and broadcast probes retry once on another worker (ADR 0060);
    shuffles fail (ADR 0064).
- **The `workers_health` log line** is emitted whenever the member list
  or the healthy set changes. It carries `workers`, `total` and
  `healthy`.

## Consequences

**Positive**
- Scaling a worker Deployment needs no coordinator change: a new pod
  takes fragments within one interval once its `/readyz` answers. A
  terminating pod first fails `/readyz` while it drains (ADR 0059), then
  leaves DNS.
- No new dependency, endpoint or secret.

**Negative (accepted debt)**
- **TLS with discovered workers.** They are addressed by IP, so `https`
  needs worker certificates with IP SANs, or a mesh doing mTLS
  underneath. Plain `http` inside a private network is the expected
  setup; the worker token still authenticates every request.
- **The system resolver's caching applies.** Some resolvers cache past
  the 5-second round, so changes show up once the TTL expires.
- **Shuffle membership is fixed at planning.** A shuffle planned across
  W workers needs all W for its duration. A worker that leaves fails
  the query rather than re-planning it.
- **There is no SRV support.** The port comes from the entry, not from
  DNS.

## Validation

- `remote::tests::dns_seeds_are_resolved_into_workers_each_round` uses a
  ready worker on `127.0.0.1` and three seeds: static, `dns+` localhost,
  and an unresolvable `.invalid` name.
  - Before the first round, only the static seed is a member.
  - After it, the resolved address and the static seed are members.
  - Only the listening address is healthy.
  - A second round keeps it.
  - The unresolvable name never adds a member.
- `config::tests::workers_need_a_token_and_urls`: `dns+http://name:port`
  is accepted; `dns+w1:8080` and `dns+ftp://…` are refused.
- The distributed live tests (static URLs) are unchanged and pass.

## Revisit when

- Ports differ per worker: support SRV records.
- Shuffles must survive membership changes: re-plan a shuffle on a lost
  map or reducer.
- Several coordinators must agree on membership: a shared registry
  (option B) becomes worth its parts.

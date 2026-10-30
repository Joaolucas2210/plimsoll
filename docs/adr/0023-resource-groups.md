# ADR 0023 — Configurable resource groups bound to principals

- Status: Accepted
- Date: 2026-10-30
- Owner: JoaoLucas2210
- Related: ADR 0021 (authentication), ADR 0022 (authorization pattern), ADR 0013 (error contract), ADR 0004
- Scope: `plimsoll_control::governance` (`AdmissionConfig::{groups, has_group}`, `DEFAULT_GROUP`), `plimsoll-server` (`config::resource_groups`, `auth::{Auth::groups_for}`, `app::sql`)

## Context

Admission (global, per group, per user) existed, but resource groups were a
placeholder:

- every group shared one `per_group` limit, which equalled the global one;
- the client chose its group with any string;
- a `ponytail:` in `AdmissionConfig` deferred configurable groups.

So nothing stopped a batch job from taking every running slot, and an
interactive user had no reserved capacity. Groups only work when two things
hold:

1. groups have **different** limits;
2. a principal cannot pick a group it was not given.

Before ADR 0021 the second could not be enforced, because the principal
itself was client-asserted. Now it can.

## Decision drivers

1. Isolation between workloads (batch vs interactive) without separate
   deployments.
2. Group choice is an authorization decision, not a client preference.
3. Closed set: an unknown group name is an error. It must never silently
   create a group with default limits, which would bypass the configured
   ones.
4. Fail fast on misconfiguration (typos in group or user names).
5. Keep the governance crate free of auth concepts (ADR 0002). It knows
   groups and limits, and the server knows principals.

## Considered options

**A. Env-configured groups with per-group limits, plus a principal → groups
binding in auth config (chosen).** Two variables:

- `PLIMSOLL_RESOURCE_GROUPS=interactive=4/16,batch=2/64` sets running and
  queued limits per group;
- `PLIMSOLL_AUTH_GROUPS=etl=batch,ana=interactive|batch` lists the groups
  each principal may use, its default first.

This matches the existing env-only config style (driver 4).

**B. Groups selected by rules on the query (user regex, SQL shape) as in
Trino's selectors.** More flexible, but it is a rule language to design,
test and explain. Static bindings cover service accounts and teams. This is
the upgrade path when routing must depend on the query.

**C. Weighted fair sharing instead of hard per-group caps.** Better
utilisation, but it needs a scheduler. Hard caps are predictable and are
what the memory ceiling assumes (ADR 0003). Revisit with many tenants.

## Decision

- **Groups.**
  - `default` always exists. Its limits are `per_group`, which equal the
    global limits unless `PLIMSOLL_RESOURCE_GROUPS` overrides `default`.
  - Configured groups have their own `max_running` / `max_queued` and are
    still bounded by the global and per-user limits.
  - Names are `[A-Za-z0-9_-]+`. Running must be > 0; queued may be 0, which
    means reject when busy. Duplicates fail boot.
- **Bindings (auth on).**
  - `PLIMSOLL_AUTH_GROUPS` maps a user to `group|group|…`, and the first
    group is the user's default.
  - Unlisted users may use `default` only.
  - Unknown users or groups in bindings fail boot. A binding with
    `PLIMSOLL_AUTH=disabled` also fails boot.
- **Resolution in `/v1/sql`.**
  - The group is the requested `resource_group`, else the principal's
    default, else `default`.
  - An unknown group gives `400 invalid_request`.
  - A group that exists but is not bound to the principal gives
    `403 forbidden`.
  - With auth disabled, any existing group is allowed.
- **Governance API.**
  - `AdmissionConfig.groups: HashMap<String, Limits>`.
  - `has_group()` for callers.
  - A private `group_limits()` used by `can_run`/`queue_check`.
  - `Admission::config()` exposes the config to the server.
  - Rejections already name the scope: `resource group 'batch' queue is full`.

## Consequences

**Positive**
- Batch and interactive workloads stop starving each other. A group's caps
  bound its share of slots, and therefore of the memory ceiling.
- Group choice is enforced per principal, and the registry records the
  effective group (`system.runtime.queries.resource_group`).
- Breaking clients early is better than silently mis-routing them: unknown
  groups now fail where they used to be accepted.

**Negative (accepted debt)**
- Static config; changing groups needs a restart.
- Hard caps only. There are no weights and no borrowing of idle capacity
  between groups.
- No per-group memory budget. Memory is protected by the process ceiling,
  not split per group.
- Per-user limits are still one value for all users (`ponytail:` kept in
  `AdmissionConfig`).
- The boot line logs users and operators, but not groups or bindings.

**Operational**
- Typical setup:
  - `PLIMSOLL_RESOURCE_GROUPS=interactive=6/32,batch=2/64`;
  - service accounts bound to `batch`;
  - analysts bound to `interactive|batch`.
- `queue_full` errors name the group. Alert per group via
  `plimsoll_query_errors_total{code="queue_full"}`, with logs or the
  registry for the group name.

## Risks and mitigations

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| A client relying on arbitrary group names breaks | Medium (pre-1.0) | Low | Clear 400 message naming the group; README documents configuration. |
| Misconfigured binding locks a service out | Low | Medium | Boot fails on unknown users or groups; 403 message names user and group. |
| Group caps sum above global caps, which confuses capacity planning | Medium | Low | Global limits still bind; document that group caps are ceilings, not reservations. |

## Validation

- `governance::tests::configured_groups_have_their_own_limits`:
  - `batch` (1 running, no queue) rejects the second query with a
    group-scoped `QueueFull`;
  - `default` still admits;
  - unknown names are not groups.
- `config::tests::resource_groups_are_parsed_and_validated`: valid entries,
  plus malformed, zero-running, bad-name, duplicate and non-numeric entries.
- `auth::tests::principals_are_bound_to_resource_groups`:
  - lists and defaults;
  - unlisted user gets `default`;
  - disabled auth gives `None`;
  - unknown group, unknown user, malformed and duplicate entries fail boot.
- `app::tests::sql_requires_a_bearer_token_and_runs_as_its_principal`:
  - `default` 200, `batch` 403 (not bound), `nightly` 400 (unknown);
  - the registry records the effective group;
  - error metrics are counted.
- Manual smoke on the binary with `batch=1/0` and `etl=batch`:
  - a second concurrent `etl` query gets
    `429 resource group 'batch' queue is full`;
  - Ana runs on `default` and gets 403 on `batch`;
  - `etl=nightly` fails boot naming the group.

## Revisit when

- Routing must depend on query shape or source (selectors, option B).
- Idle capacity should be borrowed between groups (weighted sharing,
  option C).
- Groups need memory budgets of their own, not just slot caps.

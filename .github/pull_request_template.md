<!-- Squash-merged: the PR title becomes the commit summary on main. Use a sentence, as in the history. -->

## Why

<!-- The problem, and the issue, ROADMAP item or ADR this follows. A behaviour change needs an ADR. -->

## Invariant

<!-- Does this touch memory, admission, deadlines or cancellation? Which test shows the query fails and the
     node keeps serving? Does it touch grants, column denies or row filters? Write "not touched" otherwise. -->

## Checks

| Check | Result |
| --- | --- |
| `make gates` | <!-- pass / fail; anything you could not run is "skipped", never green --> |
| `make demo` (user-visible behaviour) | |
| CHANGELOG `[Unreleased]` / ADR | |

## Debt and follow-ups

<!-- New `ponytail:` shortcuts, and where the ADR lists them. -->

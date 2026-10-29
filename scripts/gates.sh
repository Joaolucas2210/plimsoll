#!/usr/bin/env bash
# Quality gates. Run from anywhere; exits non-zero on the first failure.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

step() { printf '\n==> %s\n' "$*"; }

step "fmt"
cargo fmt --all -- --check

step "clippy"
cargo clippy --workspace --all-targets -- -D warnings

step "test"
cargo test --workspace

# Architecture invariants (ADR 0002): dependency direction between planes.
#   - core (compute plane) never depends on concrete connectors;
#   - control plane never depends on the execution engine.
step "arch"
forbid() { # forbid <crate> <forbidden-dependency>
  if cargo tree -p "$1" -e normal --prefix none --format '{p}' | grep -q "^$2 "; then
    echo "arch violation: $1 depends on $2" >&2
    exit 1
  fi
  echo "ok: $1 does not depend on $2"
}
forbid plimsoll-core plimsoll-connectors
forbid plimsoll-control datafusion
forbid plimsoll-control plimsoll-core

printf '\nall gates passed\n'

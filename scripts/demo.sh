#!/usr/bin/env bash
# The load line, live: one node, a 64 MiB memory ceiling, one running slot, no queue, a 2 s deadline.
# Shows that overload ends one query with a typed error and the node keeps serving. No Docker, no catalog,
# no data: every query reads DataFusion's range() table function. Touches nothing outside a temp dir.
#   scripts/demo.sh                 (builds plimsoll-server in debug if needed, ~1 min the first time)
#   PLIMSOLL_BIN=path scripts/demo.sh
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."
for tool in curl python3; do command -v "$tool" >/dev/null || { echo "demo needs $tool" >&2; exit 1; }; done

BIN="${PLIMSOLL_BIN:-}"
if [ -z "$BIN" ]; then
  command -v cargo >/dev/null || { echo "demo needs cargo, or PLIMSOLL_BIN=/path/to/plimsoll-server" >&2; exit 1; }
  echo "building plimsoll-server (debug)…"
  cargo build -q -p plimsoll-server
  BIN=target/debug/plimsoll-server
fi

work="$(mktemp -d)"
pid=""
cleanup() { [ -n "$pid" ] && kill "$pid" 2>/dev/null; wait 2>/dev/null; rm -rf "$work"; }
trap cleanup EXIT

port="$(python3 -c 'import socket; s = socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1])')"
token="$(python3 -c 'import secrets; print(secrets.token_hex(32))')"
base="http://127.0.0.1:$port"
PLIMSOLL_BIND="127.0.0.1:$port" \
PLIMSOLL_AUTH_TOKENS="ana:sha256:$(printf %s "$token" | python3 -c 'import hashlib,sys; print(hashlib.sha256(sys.stdin.buffer.read()).hexdigest())')" \
PLIMSOLL_MEMORY_LIMIT_BYTES=$((64 * 1024 * 1024)) PLIMSOLL_MAX_RUNNING=1 PLIMSOLL_MAX_QUEUED=0 \
PLIMSOLL_QUERY_TIMEOUT_MS=2000 PLIMSOLL_DRAIN_DELAY_MS=0 \
  "$BIN" >"$work/server.log" 2>&1 &
pid=$!
for _ in $(seq 80); do curl -sf "$base/readyz" >/dev/null 2>&1 && break; sleep 0.25; done
curl -sf "$base/readyz" >/dev/null || { echo "server did not become ready:" >&2; tail -5 "$work/server.log" >&2; exit 1; }

# sql <out-file> <statement>: prints the HTTP status
sql() {
  curl -s -o "$1" -w '%{http_code}' "$base/v1/sql" -H 'content-type: application/json' \
    -H "authorization: Bearer $token" -d "$(python3 -c 'import json,sys; print(json.dumps({"sql": sys.argv[1]}))' "$2")"
}
field() { python3 -c 'import json,sys; d=json.load(open(sys.argv[1])); print(eval(sys.argv[2], {}, {"d": d}))' "$1" "$2"; }
health() { curl -s "$base/healthz" >"$work/h.json"; field "$work/h.json" "$1"; }
fail() { echo "  ✗ $1" >&2; tail -5 "$work/server.log" >&2; exit 1; }
row() { printf '  ✓ %-40s %-5s %s\n' "$1" "$2" "$3"; }

echo "node on $base · ceiling 64 MiB · 1 running slot, no queue · 2 s deadline"
echo

code="$(sql "$work/ok.json" "SELECT count(*) AS n FROM range(10000000)")"
[ "$code" = 200 ] || fail "small query: HTTP $code"
row "count 10M rows" 200 "rows=$(field "$work/ok.json" 'd["rows"][0][0]')"

code="$(sql "$work/oom.json" "SELECT v, count(*) FROM (SELECT value % 5000000 AS v FROM range(20000000)) GROUP BY v")"
[ "$code" = 422 ] || fail "group by 5M keys: expected 422, got $code"
[ "$(field "$work/oom.json" 'd["error"]["code"]')" = memory_limit_exceeded ] || fail "wrong error code"
row "group by 5M keys (crosses the line)" 422 "memory_limit_exceeded, this query only"

for _ in $(seq 40); do [ "$(health 'd["pool"]["used_bytes"]')" = 0 ] && break; sleep 0.05; done
[ "$(health 'd["pool"]["used_bytes"]')" = 0 ] || fail "pool did not return to 0 bytes"
code="$(curl -s -o /dev/null -w '%{http_code}' "$base/readyz")"
[ "$code" = 200 ] || fail "node not ready after the breach"
row "node after the breach" 200 "ready, pool back to 0 bytes"

sql "$work/long.json" "SELECT sum(value % 7) FROM range(10000000000)" >"$work/long.code" &
long=$!
for _ in $(seq 40); do [ "$(health 'd["queries"]["running"]')" = 1 ] && break; sleep 0.05; done
code="$(sql "$work/busy.json" "SELECT 1")"
[ "$code" = 429 ] || fail "second query while the slot is taken: expected 429, got $code"
row "second query while the slot is taken" 429 "$(field "$work/busy.json" 'd["error"]["code"]'), rejected instead of queued"

wait "$long"
code="$(cat "$work/long.code")"
[ "$code" = 504 ] || fail "10^10-row scan: expected 504, got $code"
row "10^10-row scan past its 2 s deadline" 504 "$(field "$work/long.json" 'd["error"]["code"]'), cancelled"

for _ in $(seq 40); do [ "$(health 'd["queries"]["running"]')" = 0 ] && break; sleep 0.05; done
code="$(sql "$work/after.json" "SELECT 'still serving' AS status")"
[ "$code" = 200 ] || fail "query after the timeout: HTTP $code"
row "query after all of that" 200 "slot and memory released"

curl -s "$base/metrics" >"$work/metrics"
counts="$(grep '^plimsoll_queries_total' "$work/metrics" | sed -E 's/.*state="([a-z]+)"\} ([0-9]+)/\1=\2/' | tr '\n' ' ')"
echo
echo "  /metrics: $counts· one process the whole time (pid $pid)"
echo "demo passed"

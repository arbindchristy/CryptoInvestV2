!/usr/bin/env bash
set -euo pipefail

fail() { echo "❌ $1" >&2; exit 1; }
pass() { echo "✅ $1"; }

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

SERVICE="src/cryptoinvest/service.py"
WORKER="src/cryptoinvest/worker.py"
STORE="src/cryptoinvest/store.py"
SNAP="src/cryptoinvest/snapshot.py"
COMPOSE="docker-compose.yml"

[[ -f "$SERVICE" ]] || fail "Missing $SERVICE"
[[ -f "$WORKER"  ]] || fail "Missing $WORKER"
[[ -f "$STORE"   ]] || fail "Missing $STORE"
[[ -f "$SNAP"    ]] || fail "Missing $SNAP"
[[ -f "$COMPOSE" ]] || fail "Missing $COMPOSE"

pass "Required files exist"

# 1) API must not start scheduler / worker thread
# 1) API must not start scheduler / worker thread
# Allow reporting interval_seconds in status endpoints; block actual worker/scheduler constructs.
if grep -nE "(threading\.Thread\(|ThreadingHTTPServer\(|BaseHTTPRequestHandler|_worker_loop|_stop_event|while .*stop_event|@app\.on_event\(\"startup\"\)[[:space:]]*.*\n.*\.start\()" "$SERVICE" >/dev/null; then
  fail "service.py appears to contain scheduler/worker runtime logic"
fi

# Also block obvious start() calls inside startup handlers (common regression)
if grep -nE "@app\.on_event\(\"startup\"\)" -n "$SERVICE" >/dev/null && grep -nE "\.start\(\)" "$SERVICE" >/dev/null; then
  fail "service.py calls .start() (likely starting scheduler) — API must be read-only"
fi

pass "service.py does not contain scheduler/worker patterns"

# 2) Worker must enforce last-closed-candle
if ! grep -nE "iloc\[\s*:-1\s*\]" "$WORKER" >/dev/null; then
  fail "worker.py does not appear to exclude the currently-forming candle (missing iloc[:-1])"
fi
pass "worker.py appears to enforce last-closed-candle logic"

# 3) FastAPI endpoints exist
for ep in "/health" "/signal" "/engine/status"; do
  if ! grep -nF "\"$ep\"" "$SERVICE" >/dev/null; then
    fail "service.py missing endpoint $ep"
  fi
done
pass "service.py endpoints exist (/health, /signal, /engine/status)"

# 4) Snapshot contract keys must be present in normalize
REQ_KEYS=(symbol timeframe timestamp candle_time signal stale error last_fetch_status last_success_at)
for k in "${REQ_KEYS[@]}"; do
  if ! grep -nE "\"$k\"|'$k'" "$SNAP" >/dev/null; then
    fail "snapshot.py does not reference required key: $k"
  fi
done
pass "snapshot contract keys referenced in snapshot.py"

# 5) Compose must define redis/api/worker
if ! grep -nE "^\s*redis:\s*$" "$COMPOSE" >/dev/null; then fail "docker-compose.yml missing redis service"; fi
if ! grep -nE "^\s*api:\s*$"   "$COMPOSE" >/dev/null; then fail "docker-compose.yml missing api service"; fi
if ! grep -nE "^\s*worker:\s*$" "$COMPOSE" >/dev/null; then fail "docker-compose.yml missing worker service"; fi
pass "docker-compose.yml defines redis, api, worker services"

echo
pass "Architecture verification PASSED"


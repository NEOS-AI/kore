#!/usr/bin/env bash
# Batch GK: multi-client smoke against a local Kore instance.
# Usage: scripts/client_smoke.sh [port]
# Requires: redis-cli (redis-tools) on PATH; optional python3 + redis package.
set -euo pipefail

PORT="${1:-16379}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="${KORE_BIN:-$ROOT/target/release/kore}"
DIR="${TMPDIR:-/tmp}/kore-client-smoke-$$"
mkdir -p "$DIR"

cleanup() {
  if [[ -n "${KORE_PID:-}" ]] && kill -0 "$KORE_PID" 2>/dev/null; then
    kill "$KORE_PID" 2>/dev/null || true
    wait "$KORE_PID" 2>/dev/null || true
  fi
  rm -rf "$DIR"
}
trap cleanup EXIT

if [[ ! -x "$BIN" ]]; then
  echo "Building release kore..."
  (cd "$ROOT" && cargo build --release -q)
fi

if ! command -v redis-cli >/dev/null 2>&1; then
  echo "redis-cli not found (install redis-tools)" >&2
  exit 1
fi

"$BIN" --host 127.0.0.1 --port "$PORT" --save "" --dir "$DIR" \
  --shards 64 --maxconns 64 -v 0 \
  >"$DIR/server.log" 2>&1 &
KORE_PID=$!

# Wait for listen
for i in $(seq 1 50); do
  if redis-cli -h 127.0.0.1 -p "$PORT" PING 2>/dev/null | grep -q PONG; then
    break
  fi
  sleep 0.1
done
redis-cli -h 127.0.0.1 -p "$PORT" PING | grep -q PONG

echo "== redis-cli smoke =="
redis-cli -h 127.0.0.1 -p "$PORT" SET gk:key hello | grep -q OK
redis-cli -h 127.0.0.1 -p "$PORT" GET gk:key | grep -q hello
redis-cli -h 127.0.0.1 -p "$PORT" INCR gk:n | grep -q 1
redis-cli -h 127.0.0.1 -p "$PORT" INCR gk:n | grep -q 2
redis-cli -h 127.0.0.1 -p "$PORT" DEL gk:key | grep -q 1
# Each redis-cli invocation is a new connection — use -n for multi-DB.
redis-cli -h 127.0.0.1 -p "$PORT" -n 1 SET db1:x y | grep -q OK
# key from db1 must not appear on db0
test "$(redis-cli -h 127.0.0.1 -p "$PORT" -n 0 EXISTS db1:x)" = "0"
test "$(redis-cli -h 127.0.0.1 -p "$PORT" -n 1 GET db1:x)" = "y"
redis-cli -h 127.0.0.1 -p "$PORT" HSET gk:h f1 v1 | grep -q 1
redis-cli -h 127.0.0.1 -p "$PORT" HGET gk:h f1 | grep -q v1
redis-cli -h 127.0.0.1 -p "$PORT" LPUSH gk:l a b | grep -q 2
redis-cli -h 127.0.0.1 -p "$PORT" LRANGE gk:l 0 -1 | tr '\n' ' ' | grep -q 'b a'
redis-cli -h 127.0.0.1 -p "$PORT" SADD gk:s m1 m2 | grep -q 2
redis-cli -h 127.0.0.1 -p "$PORT" SCARD gk:s | grep -q 2
redis-cli -h 127.0.0.1 -p "$PORT" ZADD gk:z 1 a 2 b | grep -q 2
redis-cli -h 127.0.0.1 -p "$PORT" ZCARD gk:z | grep -q 2
echo "redis-cli: OK"

if python3 -c 'import redis' 2>/dev/null; then
  echo "== redis-py smoke =="
  python3 - <<PY
import redis
r = redis.Redis(host="127.0.0.1", port=int("$PORT"), decode_responses=True)
assert r.ping() is True
r.set("py:k", "v")
assert r.get("py:k") == "v"
assert r.incr("py:n") == 1
r.hset("py:h", mapping={"a": "1"})
assert r.hget("py:h", "a") == "1"
print("redis-py: OK")
PY
else
  echo "redis-py: skip (python redis package not installed)"
fi

if command -v node >/dev/null 2>&1 && node -e "require('ioredis')" 2>/dev/null; then
  echo "== ioredis smoke =="
  node - <<'NODE'
const Redis = require('ioredis');
const r = new Redis({ host: '127.0.0.1', port: process.env.PORT || 16379, lazyConnect: true });
(async () => {
  await r.connect();
  if (await r.ping() !== 'PONG') throw new Error('ping');
  await r.set('io:k', 'v');
  if (await r.get('io:k') !== 'v') throw new Error('get');
  console.log('ioredis: OK');
  r.disconnect();
})().catch((e) => { console.error(e); process.exit(1); });
NODE
else
  echo "ioredis: skip (node/ioredis not installed)"
fi

echo "client_smoke: ALL PASSED"

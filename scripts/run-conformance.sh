#!/usr/bin/env bash
set -euo pipefail

# Run Durable Streams server conformance tests against a local PicoMQ node.
# Usage:
#   ./scripts/run-conformance.sh [httpPort] [adminPort]
#
# Needs: cargo (or PICO_BIN), curl, node/npm.
# Builds pico in release if PICO_BIN is unset.

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
HTTP_PORT="${1:-4437}"
ADMIN_PORT="${2:-9090}"
BASE_URL="http://127.0.0.1:${HTTP_PORT}"
ADMIN_URL="http://127.0.0.1:${ADMIN_PORT}"
DATA="$(mktemp -d /tmp/picomq-conf-XXXXXX)"
WORK="$(mktemp -d /tmp/ds-conf-XXXXXX)"

if [[ -n "${PICO_BIN:-}" ]]; then
  BIN="${PICO_BIN}"
else
  cd "${ROOT}"
  cargo build --release -p picomq-cli
  BIN="${ROOT}/target/release/pico"
fi

free_ports() {
  lsof -tiTCP:"${HTTP_PORT}" -sTCP:LISTEN | xargs kill -9 2>/dev/null || true
  lsof -tiTCP:"${ADMIN_PORT}" -sTCP:LISTEN | xargs kill -9 2>/dev/null || true
}

cleanup() {
  if [[ -f "${DATA}/server.pid" ]]; then
    kill "$(cat "${DATA}/server.pid")" 2>/dev/null || true
    wait "$(cat "${DATA}/server.pid")" 2>/dev/null || true
  fi
  free_ports
  rm -rf "${DATA}" "${WORK}"
}
trap cleanup EXIT

free_ports

mkdir -p "${DATA}/objects"
cd "${ROOT}"
"${BIN}" --protocol ds serve \
  --listen "127.0.0.1:${HTTP_PORT}" \
  --admin-listen "127.0.0.1:${ADMIN_PORT}" \
  --meta-url "sqlite:${DATA}/meta.db" \
  --storage="-2@file://${DATA}/objects" \
  --routing local \
  >"${DATA}/server.log" 2>&1 &
echo $! >"${DATA}/server.pid"

echo "Waiting for server at ${ADMIN_URL}/ready (log: ${DATA}/server.log)"
READY=0
for _ in $(seq 1 90); do
  if curl -sf -o /dev/null "${ADMIN_URL}/ready"; then
    echo "Server ready"
    READY=1
    break
  fi
  if ! kill -0 "$(cat "${DATA}/server.pid")" 2>/dev/null; then
    echo "Server process exited early:" >&2
    cat "${DATA}/server.log" >&2
    exit 1
  fi
  sleep 1
done
if [[ "${READY}" -ne 1 ]]; then
  echo "Server failed to become ready within 90s:" >&2
  cat "${DATA}/server.log" >&2
  exit 1
fi

cd "${WORK}"
npm init -y >/dev/null 2>&1
npm pkg set type=module >/dev/null
npm install --silent @durable-streams/server-conformance-tests@0.3.6 vitest@4
cat > runner.test.js <<'EOF'
import { runConformanceTests } from '@durable-streams/server-conformance-tests'
const baseUrl = process.env.CONFORMANCE_TEST_URL
if (!baseUrl) throw new Error('missing CONFORMANCE_TEST_URL')
runConformanceTests({ baseUrl })
EOF
cat > vitest.config.js <<'EOF'
import { defineConfig } from 'vitest/config'
export default defineConfig({
  test: {
    include: ['runner.test.js'],
    fileParallelism: false,
    testTimeout: 120000,
    hookTimeout: 120000,
    reporters: ['verbose'],
  },
})
EOF

echo "Running conformance against ${BASE_URL}"
CONFORMANCE_TEST_URL="${BASE_URL}" npx vitest run --reporter=verbose

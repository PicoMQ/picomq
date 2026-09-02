#!/usr/bin/env bash
set -euo pipefail


ROOT="$(cd "$(dirname "$0")/.." && pwd)"
COMPOSE_DIR="${ROOT}/harness/aio"
COMPOSE=(docker compose -f "${COMPOSE_DIR}/compose.yml")
ENDPOINT="${PICO_ENDPOINT:-http://127.0.0.1:4437}"
ADMIN="${PICO_ADMIN:-http://127.0.0.1:9090}"
KAFKA="${PICO_KAFKA:-127.0.0.1:9092}"
KEEP="${PICO_E2E_KEEP:-0}"

cleanup() {
  if [[ "${KEEP}" != "1" ]]; then
    "${COMPOSE[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

echo "==> tearing down any previous aio stack"
"${COMPOSE[@]}" down -v --remove-orphans

echo "==> building and starting Postgres + RustFS + pico"
"${COMPOSE[@]}" up --build -d --force-recreate --remove-orphans

echo "==> waiting for ${ADMIN}/ready"
READY=0
for _ in $(seq 1 180); do
  if curl -sf -o /dev/null "${ADMIN}/ready"; then
    READY=1
    break
  fi
  sleep 2
done
if [[ "${READY}" -ne 1 ]]; then
  echo "node never became ready" >&2
  "${COMPOSE[@]}" logs pico >&2 || true
  exit 1
fi
echo "node ready"

export PICO_E2E=1
export PICO_ENDPOINT="${ENDPOINT}"
export PICO_KAFKA="${KAFKA}"

echo "==> rust protocol e2e"
(
  cd "${ROOT}"
  cargo test -p picomq-runtime --test docker_e2e -- --ignored --test-threads=1 --nocapture
)

echo "==> rust client e2e"
(
  cd "${ROOT}"
  cargo test -p picomq-client --test docker_e2e -- --ignored --test-threads=1 --nocapture
)

echo "==> typescript live e2e"
(
  cd "${ROOT}/client/typescript"
  npm ci
  npm test
)

echo "==> load test (pico raw, pico producers, kafka)"
(
  cd "${ROOT}"
  pico() { cargo run -q -p picomq-cli --release -- "$@"; }
  pico --endpoint "${ENDPOINT}" --http2 bench -d 20 -b 1024 -w 256 --connections 4 --streams 4 --interval 0
  pico --endpoint "${ENDPOINT}" --http2 bench --producer -d 20 -b 1024 -n 64 -w 64 --connections 4 --streams 8 --interval 0
  pico --endpoint "${KAFKA}" --protocol kafka bench -d 20 -b 1024 -n 32 -w 32 --connections 4 --streams 4 --interval 0
)

echo "==> e2e passed"

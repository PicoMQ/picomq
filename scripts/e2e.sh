#!/usr/bin/env bash
# Runs the end to end scenarios against Postgres + RustFS + pico in docker compose.
#
#   scripts/e2e.sh                    all scenarios
#   scripts/e2e.sh single cluster     a subset
#   KEEP=1 scripts/e2e.sh single      leave the stack running afterwards
#   PICO_IMAGE=... SKIP_BUILD=1       use an already built pico image
#
# Scenarios:
#   single    one node, Pico HTTP + Kafka, auth off
#             protocol cross tests (pico <-> kafka, groups, ttl, trim, schema, producers)
#             Rust, TypeScript and Go client suites, Go restart test
#   cluster   two nodes, Pico HTTP + Kafka, auth required
#             ownership redirects, groups across nodes, Rust, TypeScript and Go cluster suites
#   ds        two nodes, Durable Streams HTTP + Kafka, auth off
#             Go Durable Streams live and cluster suites

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SCENARIOS=("$@")
if [ ${#SCENARIOS[@]} -eq 0 ]; then
  SCENARIOS=(single cluster ds)
fi

NODE1="http://127.0.0.1:4437"
NODE2="http://127.0.0.1:4438"
ADMIN1="http://127.0.0.1:9090"
ADMIN2="http://127.0.0.1:9091"
KAFKA="127.0.0.1:9092"

export PICO_IMAGE="${PICO_IMAGE:-picomq-e2e:local}"

log() { printf '\n\033[1;34m==> %s\033[0m\n' "$*"; }

compose_file() {
  case "$1" in
    single) echo "$ROOT/harness/aio/compose.yml" ;;
    cluster | ds) echo "$ROOT/harness/aio/compose.cluster.yml" ;;
    *) echo "unknown scenario: $1" >&2; exit 2 ;;
  esac
}

compose() {
  docker compose -f "$(compose_file "$1")" "${@:2}"
}

wait_ready() {
  local url="$1"
  for _ in $(seq 1 180); do
    if curl -sf -o /dev/null "$url/ready"; then
      return 0
    fi
    sleep 2
  done
  echo "$url never became ready" >&2
  return 1
}

rust_protocol_suite() {
  log "$1: rust protocol e2e"
  (cd "$ROOT" && cargo test --locked -p picomq-runtime --test docker_e2e -- --ignored --test-threads=1 --nocapture)
}

rust_client_suite() {
  log "$1: rust client e2e"
  (cd "$ROOT" && cargo test --locked -p picomq-client --test docker_e2e -- --ignored --test-threads=1 --nocapture "${@:2}")
}

typescript_suite() {
  log "$1: typescript live e2e"
  (cd "$ROOT/client/typescript" && npm ci --no-audit --no-fund && npm test)
}

go_suite() {
  log "$1: go live e2e"
  (cd "$ROOT/client/go" && go test ./... -count=1 -timeout 20m -v)
}

run_single() {
  export PICO_PROTOCOL=pico PICO_AUTH=off
  export PICO_ENDPOINT="$NODE1" PICO_KAFKA="$KAFKA"
  export PICOMQ_INTEGRATION=1 PICOMQ_ENDPOINT="$NODE1"
  export PICOMQ_RESTART_CMD="docker compose -f $(compose_file single) restart pico"
  unset PICO_ENDPOINT_2 PICOMQ_ENDPOINT_2 PICOMQ_AUTH_REQUIRED PICOMQ_DS_INTEGRATION

  wait_ready "$ADMIN1"
  rust_protocol_suite single
  rust_client_suite single --skip follow_cluster_redirects
  typescript_suite single
  go_suite single
}

run_cluster() {
  export PICO_PROTOCOL=pico PICO_AUTH=required
  export PICO_ENDPOINT="$NODE1" PICO_ENDPOINT_2="$NODE2" PICO_KAFKA="$KAFKA"
  export PICOMQ_INTEGRATION=1 PICOMQ_AUTH_REQUIRED=1 PICOMQ_ENDPOINT="$NODE1" PICOMQ_ENDPOINT_2="$NODE2"
  unset PICOMQ_RESTART_CMD PICOMQ_DS_INTEGRATION

  wait_ready "$ADMIN1"
  wait_ready "$ADMIN2"
  rust_client_suite cluster
  typescript_suite cluster
  go_suite cluster
}

run_ds() {
  export PICO_PROTOCOL=ds PICO_AUTH=off
  export PICOMQ_DS_INTEGRATION=1 PICOMQ_DS_ENDPOINT="$NODE1" PICOMQ_DS_ENDPOINT_2="$NODE2"
  unset PICO_ENDPOINT PICO_ENDPOINT_2 PICOMQ_INTEGRATION PICOMQ_AUTH_REQUIRED PICOMQ_ENDPOINT PICOMQ_ENDPOINT_2 PICOMQ_RESTART_CMD

  wait_ready "$ADMIN1"
  wait_ready "$ADMIN2"
  go_suite ds
}

run() {
  local scenario="$1"
  local status=0

  case "$scenario" in
    single) export PICO_PROTOCOL=pico PICO_AUTH=off ;;
    cluster) export PICO_PROTOCOL=pico PICO_AUTH=required ;;
    ds) export PICO_PROTOCOL=ds PICO_AUTH=off ;;
  esac

  log "$scenario: starting stack"
  compose "$scenario" down --volumes --remove-orphans >/dev/null 2>&1 || true
  compose "$scenario" up --detach --force-recreate --remove-orphans || status=$?

  if [ "$status" -eq 0 ]; then
    "run_$scenario" || status=$?
  fi

  if [ "$status" -ne 0 ]; then
    log "$scenario: service logs"
    compose "$scenario" logs --no-log-prefix --tail=200
  fi
  if [ "${KEEP:-0}" != "1" ]; then
    log "$scenario: tearing down"
    compose "$scenario" down --volumes --remove-orphans
  fi
  return "$status"
}

if [ "${SKIP_BUILD:-0}" != "1" ]; then
  log "building $PICO_IMAGE"
  docker build -t "$PICO_IMAGE" "$ROOT"
fi

log "building test binaries"
(cd "$ROOT" && cargo test --locked -p picomq-runtime -p picomq-client --no-run)

failed=()
for scenario in "${SCENARIOS[@]}"; do
  run "$scenario" || failed+=("$scenario")
done

if [ ${#failed[@]} -ne 0 ]; then
  log "failed: ${failed[*]}"
  exit 1
fi
log "all scenarios passed"

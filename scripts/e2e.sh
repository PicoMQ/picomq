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
#             Kafka partitions across nodes, admin APIs, failover of a killed node
#   chaos     two nodes under faults: paused nodes, a paused leader, a paused
#             Postgres and a clean stop lose no acknowledged write and fence
#             no healthy node
#   ds        two nodes, Durable Streams HTTP + Kafka, auth off
#             Go Durable Streams live and cluster suites
#   tls       two nodes, Pico HTTPS + Kafka SASL_SSL, auth required
#             Rust client cluster suite over HTTPS, redirects included

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SCENARIOS=("$@")
if [ ${#SCENARIOS[@]} -eq 0 ]; then
  SCENARIOS=(single cluster chaos ds tls)
fi

NODE1="http://127.0.0.1:4437"
NODE2="http://127.0.0.1:4438"
ADMIN1="http://127.0.0.1:9090"
ADMIN2="http://127.0.0.1:9091"
KAFKA="127.0.0.1:9092"
KAFKA2="127.0.0.1:9093"

export PICO_IMAGE="${PICO_IMAGE:-picomq-e2e:local}"

log() { printf '\n\033[1;34m==> %s\033[0m\n' "$*"; }

compose_files() {
  case "$1" in
    single) echo "-f $ROOT/harness/aio/compose.yml" ;;
    cluster | chaos | ds) echo "-f $ROOT/harness/aio/compose.cluster.yml" ;;
    tls) echo "-f $ROOT/harness/aio/compose.cluster.yml -f $ROOT/harness/aio/compose.tls.yml" ;;
    *) echo "unknown scenario: $1" >&2; exit 2 ;;
  esac
}

compose() {
  # shellcheck disable=SC2046
  docker compose $(compose_files "$1") "${@:2}"
}

wait_ready() {
  local url="$1"
  for _ in $(seq 1 180); do
    if curl -sf ${PICO_CA_CERTIFICATE:+--cacert "$PICO_CA_CERTIFICATE"} -o /dev/null "$url/ready"; then
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

rust_cluster_suite() {
  log "$1: rust cluster e2e"
  (cd "$ROOT" && cargo test --locked -p picomq-runtime --test cluster -- --ignored --test-threads=1 --nocapture "${@:2}")
}

rust_chaos_suite() {
  log "$1: rust chaos e2e"
  (cd "$ROOT" && cargo test --locked -p picomq-runtime --test chaos -- --ignored --test-threads=1 --nocapture)
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
  export PICO_INTEGRATION=1 PICO_ENDPOINT="$NODE1" PICO_KAFKA="$KAFKA"
  export PICO_RESTART_CMD="docker compose $(compose_files single) restart pico"
  unset PICO_ENDPOINT_2 PICO_DS_INTEGRATION

  wait_ready "$ADMIN1" &&
    rust_protocol_suite single &&
    rust_client_suite single --skip follow_cluster_redirects &&
    typescript_suite single &&
    go_suite single
}

run_cluster() {
  export PICO_PROTOCOL=pico PICO_AUTH=required
  export PICO_INTEGRATION=1 PICO_ENDPOINT="$NODE1" PICO_ENDPOINT_2="$NODE2" PICO_KAFKA="$KAFKA"
  export PICO_KAFKA_2="$KAFKA2" PICO_ADMIN_2="$ADMIN2"
  export PICO_KILL_CMD="docker compose $(compose_files cluster) kill -s KILL pico2"
  export PICO_START_CMD="docker compose $(compose_files cluster) up --detach pico2"
  unset PICO_RESTART_CMD PICO_DS_INTEGRATION

  wait_ready "$ADMIN1" &&
    wait_ready "$ADMIN2" &&
    rust_client_suite cluster &&
    typescript_suite cluster &&
    go_suite cluster &&
    rust_cluster_suite cluster
}

run_chaos() {
  export PICO_PROTOCOL=pico PICO_AUTH=required
  export PICO_ENDPOINT="$NODE1" PICO_KAFKA="$KAFKA" PICO_KAFKA_2="$KAFKA2" PICO_ADMIN_2="$ADMIN2"
  export PICO_COMPOSE="docker compose $(compose_files chaos)"

  wait_ready "$ADMIN1" &&
    wait_ready "$ADMIN2" &&
    rust_chaos_suite chaos
}

run_tls() {
  export PICO_PROTOCOL=pico PICO_AUTH=required
  export PICO_ENDPOINT="https://localhost:4437" PICO_ENDPOINT_2="https://localhost:4438"
  export PICO_CA_CERTIFICATE="$PICO_CERTS/ca.pem"
  unset PICO_RESTART_CMD PICO_DS_INTEGRATION

  export PICO_ADMIN="https://localhost:9090" PICO_ADMIN_2="https://localhost:9091"

  wait_ready "https://localhost:9090" &&
    wait_ready "https://localhost:9091" &&
    rust_client_suite tls &&
    rust_cluster_suite tls each_node_answers_for_the_other
}

mint_certificates() {
  local dir
  dir="$(mktemp -d)" || return 1
  if openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes -days 1 \
    -subj "/CN=PicoMQ e2e CA" -keyout "$dir/ca-key.pem" -out "$dir/ca.pem" &&
    openssl req -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes \
      -subj "/CN=PicoMQ e2e" -keyout "$dir/key.pem" -out "$dir/request.pem" &&
    openssl x509 -req -days 1 -in "$dir/request.pem" -CA "$dir/ca.pem" -CAkey "$dir/ca-key.pem" \
      -CAcreateserial -out "$dir/certificate.pem" \
      -extfile <(printf 'subjectAltName=DNS:localhost,DNS:pico1,DNS:pico2,IP:127.0.0.1\nextendedKeyUsage=serverAuth') &&
    chmod 644 "$dir"/*.pem; then
    echo "$dir"
  else
    rm -rf "${dir:?}"
    return 1
  fi
}

run_ds() {
  export PICO_PROTOCOL=ds PICO_AUTH=off
  export PICO_DS_INTEGRATION=1 PICO_DS_ENDPOINT="$NODE1" PICO_DS_ENDPOINT_2="$NODE2"
  unset PICO_ENDPOINT PICO_ENDPOINT_2 PICO_INTEGRATION PICO_RESTART_CMD

  wait_ready "$ADMIN1" &&
    wait_ready "$ADMIN2" &&
    go_suite ds
}

run() {
  local scenario="$1"
  local status=0

  case "$scenario" in
    single) export PICO_PROTOCOL=pico PICO_AUTH=off ;;
    cluster | chaos) export PICO_PROTOCOL=pico PICO_AUTH=required PICO_HEARTBEAT_INTERVAL_SEC=1 PICO_NODE_TIMEOUT_SEC=5 ;;
    ds) export PICO_PROTOCOL=ds PICO_AUTH=off ;;
    tls)
      export PICO_PROTOCOL=pico PICO_AUTH=required
      PICO_CERTS="$(mint_certificates)" || return 1
      export PICO_CERTS
      ;;
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
    if [ -n "${PICO_CERTS:-}" ]; then
      rm -rf "${PICO_CERTS:?}"
    fi
  fi
  unset PICO_CERTS PICO_CA_CERTIFICATE PICO_ADMIN
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

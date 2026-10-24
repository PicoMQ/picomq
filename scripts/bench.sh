#!/usr/bin/env bash
# Benchmarks a pico image against Postgres + RustFS in docker compose.
#
#   scripts/bench.sh <image> [single] [cluster]
#   DURATION=30 WARMUP=5        seconds measured and discarded per case
#   PARTITIONS=8                also bench a Kafka topic with this many partitions
#
# The client is this checkout's release `pico`, so two images benched in turn
# differ only in the server.
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
IMAGE="${1:?usage: scripts/bench.sh <image> [single] [cluster]}"
shift
TOPOLOGIES=("$@")
if [ ${#TOPOLOGIES[@]} -eq 0 ]; then
  TOPOLOGIES=(single cluster)
fi

DURATION="${DURATION:-30}"
WARMUP="${WARMUP:-5}"
BIN="${PICO_BIN:-$ROOT/target/release/pico}"

HTTP="--endpoint http://127.0.0.1:4437 --protocol pico --http2 bench -b 1024 -w 512 --connections 4 --streams 4"
HTTP_BATCHED="--endpoint http://127.0.0.1:4437 --protocol pico --http2 bench -b 1024 -w 32 -n 256 --connections 4 --streams 4"
KAFKA="--endpoint 127.0.0.1:9092 --protocol kafka bench -b 1024 -w 5 -n 256 --connections 4 --streams 4"

compose_file() {
  case "$1" in
    single) echo "$ROOT/harness/aio/compose.yml" ;;
    cluster) echo "$ROOT/harness/aio/compose.cluster.yml" ;;
    *) echo "unknown topology: $1" >&2; exit 2 ;;
  esac
}

compose() {
  PICO_IMAGE="$IMAGE" PICO_AUTH=off docker compose -f "$(compose_file "$1")" "${@:2}"
}

wait_ready() {
  for _ in $(seq 1 180); do
    curl -sf -o /dev/null "$1/ready" && return 0
    sleep 2
  done
  echo "$1 never became ready" >&2
  return 1
}

bench() {
  local topology="$1" name="$2" args="$3" json

  # shellcheck disable=SC2086
  json=$(env -u PICO_TOKEN "$BIN" $args -d "$DURATION" --warmup "$WARMUP" --output json 2>/dev/null) || {
    printf '%-22s %-8s %-18s failed\n' "$IMAGE" "$topology" "$name"
    return
  }

  echo "$json" | python3 -c '
import json, sys
image, topology, name = sys.argv[1:]
run = json.load(sys.stdin)
w, r = run["write"], run["read"]
print("%-22s %-8s %-18s %-12.2f %-11.0f %-9.2f %-9.2f %-11.2f" % (
    image, topology, name, w["mib_per_sec"], w["records_per_sec"],
    w["p50_ms"], w["p99_ms"], r["mib_per_sec"]))
' "$IMAGE" "$topology" "$name"
}

printf '%-22s %-8s %-18s %-12s %-11s %-9s %-9s %-11s\n' \
  image topology case write_MiB/s rec/s p50_ms p99_ms read_MiB/s

for topology in "${TOPOLOGIES[@]}"; do
  compose "$topology" down --volumes --remove-orphans >/dev/null 2>&1
  compose "$topology" up --detach --no-build --force-recreate >/dev/null 2>&1 || {
    echo "$topology: stack did not start" >&2
    continue
  }
  wait_ready "http://127.0.0.1:9090" || continue
  if [ "$topology" = cluster ]; then
    wait_ready "http://127.0.0.1:9091" || continue
  fi

  bench "$topology" http "$HTTP"
  bench "$topology" http-batched "$HTTP_BATCHED"
  bench "$topology" kafka "$KAFKA"
  if [ -n "${PARTITIONS:-}" ]; then
    bench "$topology" "kafka-${PARTITIONS}p" "$KAFKA --partitions $PARTITIONS"
  fi

  compose "$topology" down --volumes --remove-orphans >/dev/null 2>&1
done

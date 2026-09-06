#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

wait_for_pico() {
    local attempts=0
    while [ "$attempts" -lt 60 ]; do
        if curl -fsS http://127.0.0.1:9090/ready >/dev/null 2>&1; then
            return 0
        fi
        attempts=$((attempts + 1))
        sleep 1
    done
    echo "PicoMQ did not become ready" >&2
    return 1
}

PICO_PROTOCOL=pico docker compose up -d pico
wait_for_pico
./pico.sh
./kafka.sh

# One HTTP listener serves one dialect at a time. Restarting the same node with
# the persistent metadata and object volumes proves that Durable Streams sees
# the record written through Pico and consumed through Kafka.
PICO_PROTOCOL=ds docker compose up -d --force-recreate pico
wait_for_pico
./durable-streams.sh

echo "all protocol checks passed"

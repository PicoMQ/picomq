#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

endpoint="${PICO_ENDPOINT:-http://127.0.0.1:4437}"
stream="/replay"
records="${RECORDS:-512}"
record_bytes="${RECORD_BYTES:-65536}"

wait_for_pico() {
    local i
    for i in $(seq 1 60); do
        if curl -fsS http://127.0.0.1:9090/ready >/dev/null 2>&1; then
            return 0
        fi
        sleep 1
    done
    echo "PicoMQ did not become ready" >&2
    exit 1
}

wait_for_pico

status="$(curl -sS -o /dev/null -w '%{http_code}' -X DELETE "$endpoint$stream")"
case "$status" in 204|404) ;; *) echo "unexpected cleanup status: $status" >&2; exit 1 ;; esac

status="$(curl -sS -o /dev/null -w '%{http_code}' -X PUT \
    -H 'Content-Type: application/octet-stream' \
    "$endpoint$stream")"
test "$status" = 201

payload="$(mktemp)"
trap 'rm -f "$payload"' EXIT
head -c "$record_bytes" /dev/urandom >"$payload"

total_mib=$(( records * record_bytes / 1024 / 1024 ))
echo "appending ${records} records of ${record_bytes} bytes (${total_mib} MiB) to ${stream}"

for i in $(seq 1 "$records"); do
    curl -fsS -o /dev/null -X POST \
        -H 'Content-Type: application/octet-stream' \
        --data-binary "@$payload" \
        "$endpoint$stream"
    if (( i % 64 == 0 )); then
        echo "  ${i}/${records}"
    fi
done

echo "waiting for the WAL to be packed into committed objects"
sleep 5

echo "objects in the bucket:"
docker compose run --rm --entrypoint aws createbucket \
    --endpoint-url http://rustfs:9000 s3 ls --recursive --summarize s3://picomq 2>/dev/null \
    | tail -n 3

echo "seed done"

#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"
endpoint="${PICO_ENDPOINT:-http://127.0.0.1:4437}"
stream="/orders"

status="$(curl -sS -o /dev/null -w '%{http_code}' -X DELETE "$endpoint$stream")"
case "$status" in 204|404) ;; *) echo "unexpected cleanup status: $status" >&2; exit 1 ;; esac

status="$(curl -sS -o /dev/null -w '%{http_code}' -X PUT \
    -H 'Content-Type: text/plain' \
    -H 'Pico-Kafka-Topic: orders' \
    "$endpoint$stream")"
test "$status" = 201

headers="$(mktemp)"
trap 'rm -f "$headers"' EXIT
curl -fsS -o /dev/null -D "$headers" -X POST \
    -H 'Content-Type: text/plain' \
    -H 'Pico-Key: order-7' \
    --data-binary 'keyed-order' \
    "$endpoint$stream"

start="$(awk 'tolower($1) == "pico-start-seq:" { gsub("\\r", "", $2); print $2 }' "$headers")"
next="$(awk 'tolower($1) == "pico-next-seq:" { gsub("\\r", "", $2); print $2 }' "$headers")"
test "$start" = 0
test "$next" = 1

cli_read="$(docker compose exec -T pico pico read "$stream")"
test "$cli_read" = $'0\tkeyed-order'
docker compose exec -T pico pico close "$stream" >/dev/null

printf 'Pico HTTP append: start=%s next=%s key=order-7 body=keyed-order\n' "$start" "$next"
printf 'Pico CLI read: %s\n' "$cli_read"

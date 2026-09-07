#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"
endpoint="${PICO_ENDPOINT:-http://127.0.0.1:4437}"

sse="$(curl -fsSN "$endpoint/orders?offset=-1&live=sse")"
printf '%s\n' "$sse" | grep -Fqx 'event: data'
printf '%s\n' "$sse" | grep -Fqx 'data:keyed-order'
printf '%s\n' "$sse" | grep -Fqx 'event: control'
printf '%s\n' "$sse" | grep -Fq '"streamNextOffset":"00000000000000000001"'
printf '%s\n' "$sse" | grep -Fq '"streamClosed":true'

printf 'Durable Streams SSE: body=keyed-order next=00000000000000000001 closed=true\n'

#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

record="$(docker compose --profile tools run --rm kafka-client \
    --bootstrap-server pico:9092 \
    --topic orders \
    --from-beginning \
    --max-messages 1 \
    --formatter-property print.offset=true \
    --formatter-property print.key=true \
    --formatter-property key.separator=$'\t' \
    --formatter-property line.separator=)"
test "$record" = $'Offset:0\torder-7\tkeyed-order'

printf 'Kafka consume: %s\n' "$record"

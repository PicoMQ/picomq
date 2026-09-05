#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

REGIONS=(eu na apac)
TYPES=(placed accepted picked_up delivered)
MERCHANTS_EU=(lemongrass brasserie)
MERCHANTS_NA=(diner taco)
MERCHANTS_APAC=(ramen hawker)

wait_for_postgres() {
    local i
    for i in $(seq 1 60); do
        if docker compose exec -T postgres pg_isready -U pico -d example >/dev/null 2>&1; then
            return 0
        fi
        sleep 1
    done
    echo "postgres did not become ready" >&2
    exit 1
}

wait_for_source() {
    local i
    for i in $(seq 1 60); do
        if python3 - <<'PY' >/dev/null 2>&1
import json, urllib.request
with urllib.request.urlopen("http://127.0.0.1:8081/sources") as response:
    data = json.load(response)
if not any(item.get("key") == "orders_cdc" and item.get("status") == "running" for item in data):
    raise SystemExit(1)
PY
        then
            return 0
        fi
        sleep 1
    done
    echo "postgres source did not become running on :8081" >&2
    exit 1
}

wait_for_postgres
wait_for_source

echo "inserting order events for ~20s"

end=$((SECONDS + 20))
n=0
while (( SECONDS < end )); do
    region="${REGIONS[$((n % 3))]}"
    case "$region" in
        eu) merchant="${MERCHANTS_EU[$((n % 2))]}" ;;
        na) merchant="${MERCHANTS_NA[$((n % 2))]}" ;;
        apac) merchant="${MERCHANTS_APAC[$((n % 2))]}" ;;
    esac
    event_type="${TYPES[$(( (n / 3) % 4 ))]}"
    order_id="ord-${region}-$((n / 12))"
    docker compose exec -T postgres psql -U pico -d example -v ON_ERROR_STOP=1 -c \
        "INSERT INTO order_events (order_id, region, merchant_id, \"type\", payload)
         VALUES ('${order_id}', '${region}', '${merchant}', '${event_type}', jsonb_build_object('n', ${n}));" \
        >/dev/null
    n=$((n + 1))
    sleep 0.4
done

echo "inserted ${n} events, applying a few updates"

docker compose exec -T postgres psql -U pico -d example -v ON_ERROR_STOP=1 <<'SQL' >/dev/null
UPDATE order_events SET "type" = 'corrected'
WHERE id IN (SELECT id FROM order_events ORDER BY id LIMIT 3);
SQL

echo "seed done"

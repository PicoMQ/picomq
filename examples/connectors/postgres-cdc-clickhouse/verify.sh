#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

EXPECTED_TOPICS=$'/orders.apac\n/orders.eu\n/orders.na'

pg_counts() {
    docker compose exec -T postgres psql -U pico -d example -At -F $'\t' -c \
        "SELECT region, count(*) FROM order_events GROUP BY region ORDER BY region;"
}

ch_counts() {
    docker compose exec -T clickhouse clickhouse-client --user pico --password pico --query \
        "SELECT region, uniqExact(id) FROM order_events GROUP BY region ORDER BY region FORMAT TSV"
}

topics() {
    docker compose exec -T pico pico ls --prefix /orders. --limit 50
}

eu_stream() {
    docker compose exec -T pico pico read /orders.eu
}

wait_for_match() {
    local i pg ch listed
    for i in $(seq 1 60); do
        pg="$(pg_counts || true)"
        ch="$(ch_counts || true)"
        listed="$(topics || true)"
        if [[ -n "$pg" && "$pg" == "$ch" ]]; then
            local missing=0
            local line
            while IFS= read -r line; do
                if ! grep -q "^${line} " <<<"$listed"; then
                    missing=1
                    break
                fi
            done <<<"$EXPECTED_TOPICS"
            if (( missing == 0 )); then
                return 0
            fi
        fi
        sleep 1
    done
    echo "timed out waiting for ClickHouse and topics to catch up" >&2
    echo "postgres:" >&2
    echo "$pg" >&2
    echo "clickhouse:" >&2
    echo "$ch" >&2
    echo "topics:" >&2
    echo "$listed" >&2
    exit 1
}

assert_eu_stream() {
    python3 - <<'PY'
import json, subprocess, sys

result = subprocess.run(
    ["docker", "compose", "exec", "-T", "pico", "pico", "read", "/orders.eu"],
    check=True,
    capture_output=True,
    text=True,
)
lines = [line for line in result.stdout.splitlines() if line.strip()]
if not lines:
    sys.exit("eu stream is empty")
for line in lines:
    _, body = line.split("\t", 1)
    record = json.loads(body)
    region = (record.get("data") or {}).get("region")
    if region != "eu":
        sys.exit(f"eu stream contained region {region!r}")
print(f"eu stream records={len(lines)} region=eu")
PY
}

echo "runtime"
python3 - <<'PY'
import json, sys, urllib.request

failed = False
for path in ("/sources", "/sinks"):
    with urllib.request.urlopen(f"http://127.0.0.1:8081{path}") as response:
        data = json.load(response)
    for item in data:
        print(f"{item['key']} {item['status']}")
        if item.get("status") != "running":
            failed = True
if failed:
    sys.exit(1)
PY

wait_for_match
assert_eu_stream

echo
echo "topics"
topics
echo
echo "postgres (region, count)"
pg_counts
echo
echo "clickhouse (region, uniqExact(id))"
ch_counts
echo
echo "ok"

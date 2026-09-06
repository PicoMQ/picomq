#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

wait_for_pico() {
    local i
    for i in $(seq 1 60); do
        if docker compose exec -T pico pico ls --limit 1 >/dev/null 2>&1; then
            return 0
        fi
        sleep 1
    done
    echo "pico did not become ready" >&2
    exit 1
}

wait_for_sink() {
    local i
    for i in $(seq 1 60); do
        if python3 - <<'PY' >/dev/null 2>&1
import json, urllib.request
with urllib.request.urlopen("http://127.0.0.1:8081/sinks") as response:
    data = json.load(response)
if not any(item.get("key") == "fleets_iceberg" and item.get("status") == "running" for item in data):
    raise SystemExit(1)
PY
        then
            return 0
        fi
        sleep 1
    done
    echo "iceberg sink did not become running on :8081" >&2
    exit 1
}

append_fleet() {
    local fleet="$1"
    python3 - "$fleet" <<'PY' | docker compose exec -T pico pico append "/fleets.${fleet}" --batch 20
import json, sys

fleet = sys.argv[1]
bases = {"a": (51.5074, -0.1278), "b": (40.7128, -74.0060)}
lat0, lon0 = bases[fleet]
for n in range(8):
    vehicle = f"van-{(n % 2) + 1}"
    record = {
        "id": f"{fleet}-{n}",
        "fleet_id": fleet,
        "vehicle_id": vehicle,
        "lat": lat0 + (n * 0.001),
        "lon": lon0 + (n * 0.001),
        "ts": f"2026-09-05T20:00:{n:02d}Z",
    }
    print(json.dumps(record))
PY
}

wait_for_pico
docker compose exec -T pico pico create /fleets.a --content-type application/json
docker compose exec -T pico pico create /fleets.b --content-type application/json
wait_for_sink

echo "appending location pings for fleets a and b"
append_fleet a
append_fleet b
echo "seed done"

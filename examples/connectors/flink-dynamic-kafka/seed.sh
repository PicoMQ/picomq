#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"
mkdir -p out

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

append_fleet() {
    local fleet="$1"
    python3 - "$fleet" <<'PY' | docker compose exec -T pico pico append "/fleets.${fleet}" --batch 20
import json, sys

fleet = sys.argv[1]
bases = {"a": (51.5074, -0.1278), "b": (40.7128, -74.0060), "c": (35.6762, 139.6503)}
lat0, lon0 = bases[fleet]
for n in range(4):
    record = {
        "id": f"{fleet}-{n}",
        "fleet_id": fleet,
        "vehicle_id": f"van-{(n % 2) + 1}",
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
echo "appending fleets a and b"
append_fleet a
append_fleet b
echo "seed done"

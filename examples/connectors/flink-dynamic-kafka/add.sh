#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

docker compose exec -T pico pico create /fleets.c --content-type application/json
python3 - <<'PY' | docker compose exec -T pico pico append /fleets.c --batch 20
import json
for n in range(4):
    record = {
        "id": f"c-{n}",
        "fleet_id": "c",
        "vehicle_id": f"van-{(n % 2) + 1}",
        "lat": 35.6762 + (n * 0.001),
        "lon": 139.6503 + (n * 0.001),
        "ts": f"2026-09-05T20:10:{n:02d}Z",
    }
    print(json.dumps(record))
PY
echo "added /fleets.c"

#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

docker compose exec -T pico pico delete /fleets.a
python3 - <<'PY' | docker compose exec -T pico pico append /fleets.b --batch 20
import json
for n in range(4, 6):
    record = {
        "id": f"b-{n}",
        "fleet_id": "b",
        "vehicle_id": "van-1",
        "lat": 40.7128 + (n * 0.001),
        "lon": -74.0060 + (n * 0.001),
        "ts": f"2026-09-05T20:20:{n:02d}Z",
    }
    print(json.dumps(record))
PY
echo "deleted /fleets.a, appended two more to /fleets.b"

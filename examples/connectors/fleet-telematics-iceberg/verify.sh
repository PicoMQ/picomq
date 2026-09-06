#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

topics() {
    docker compose exec -T pico pico ls --prefix /fleets. --limit 50
}

iceberg_counts() {
    docker compose exec -T duckdb duckdb -init /opt/duckdb.sql -csv -c \
        "SELECT fleet_id, count(*) FROM lake.analytics.locations GROUP BY 1 ORDER BY 1" \
        | grep ','
}

wait_for_match() {
    python3 - <<'PY'
import json, subprocess, sys, time


def run(args):
    result = subprocess.run(args, capture_output=True, text=True)
    return result.returncode, result.stdout, result.stderr


def stream_counts():
    counts = {}
    for fleet in ("a", "b"):
        code, out, err = run(
            ["docker", "compose", "exec", "-T", "pico", "pico", "read", f"/fleets.{fleet}"]
        )
        if code != 0:
            return None
        counts[fleet] = sum(1 for line in out.splitlines() if line.strip())
    return counts


def iceberg_counts():
    code, out, err = run(
        [
            "docker",
            "compose",
            "exec",
            "-T",
            "duckdb",
            "duckdb",
            "-init",
            "/opt/duckdb.sql",
            "-csv",
            "-c",
            "SELECT fleet_id, count(*) FROM lake.analytics.locations GROUP BY 1 ORDER BY 1",
        ]
    )
    if code != 0:
        return None
    counts = {}
    for line in out.splitlines():
        line = line.strip()
        if "," not in line or line.lower().startswith("fleet_id"):
            continue
        fleet, n = line.split(",", 1)
        counts[fleet] = int(n)
    return counts


def topics_ok():
    code, out, err = run(
        ["docker", "compose", "exec", "-T", "pico", "pico", "ls", "--prefix", "/fleets.", "--limit", "50"]
    )
    if code != 0:
        return False
    return all(f"/fleets.{fleet} " in out or out.strip().endswith(f"/fleets.{fleet}") or f"/fleets.{fleet}\n" in out or f"/fleets.{fleet}" in out for fleet in ("a", "b"))


last = None
for _ in range(60):
    pico = stream_counts()
    ice = iceberg_counts()
    last = (pico, ice)
    if pico and ice and pico == ice and pico.get("a") and pico.get("b") and topics_ok():
        sys.exit(0)
    time.sleep(1)

print("timed out waiting for Iceberg and topics to catch up", file=sys.stderr)
print("pico:", last[0], file=sys.stderr)
print("iceberg:", last[1], file=sys.stderr)
sys.exit(1)
PY
}

assert_a_stream() {
    python3 - <<'PY'
import json, subprocess, sys

result = subprocess.run(
    ["docker", "compose", "exec", "-T", "pico", "pico", "read", "/fleets.a"],
    check=True,
    capture_output=True,
    text=True,
)
lines = [line for line in result.stdout.splitlines() if line.strip()]
if not lines:
    sys.exit("fleet a stream is empty")
for line in lines:
    _, body = line.split("\t", 1)
    record = json.loads(body)
    fleet_id = record.get("fleet_id")
    if fleet_id != "a":
        sys.exit(f"fleet a stream contained fleet_id {fleet_id!r}")
print(f"fleet a stream records={len(lines)} fleet_id=a")
PY
}

echo "runtime"
python3 - <<'PY'
import json, sys, urllib.request

with urllib.request.urlopen("http://127.0.0.1:8081/sinks") as response:
    data = json.load(response)
failed = False
for item in data:
    print(f"{item['key']} {item['status']}")
    if item.get("status") != "running":
        failed = True
if failed:
    sys.exit(1)
PY

wait_for_match
assert_a_stream

echo
echo "topics"
topics
echo
echo "iceberg (fleet_id, count)"
iceberg_counts
echo
echo "ok"

#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

if [ "$#" -lt 1 ]; then
    echo "usage: $0 <fleet> [<fleet>...]" >&2
    exit 1
fi

python3 - "$@" <<'PY'
import json, os, subprocess, sys, time
from pathlib import Path

fleets = sys.argv[1:]
root = Path("out")


def run(args):
    result = subprocess.run(args, capture_output=True, text=True)
    return result.returncode, result.stdout, result.stderr


def pico_count(fleet):
    code, out, err = run(["docker", "compose", "exec", "-T", "pico", "pico", "read", f"/fleets.{fleet}"])
    if code != 0:
        return None
    return sum(1 for line in out.splitlines() if line.strip())


def file_count(fleet):
    bucket = root / f"fleets.{fleet}"
    if not bucket.is_dir():
        return 0
    total = 0
    for path in bucket.rglob("*"):
        if not path.is_file() or "inprogress" in path.name:
            continue
        total += sum(1 for line in path.read_text().splitlines() if line.strip())
    return total


def pico_has(fleet):
    code, out, err = run(["docker", "compose", "exec", "-T", "pico", "pico", "ls", "--prefix", "/fleets.", "--limit", "50"])
    if code != 0:
        return False
    return f"/fleets.{fleet}" in out


last = None
for _ in range(60):
    pico = {fleet: pico_count(fleet) for fleet in fleets}
    files = {fleet: file_count(fleet) for fleet in fleets}
    last = (pico, files)
    ok = True
    for fleet in fleets:
        if not pico_has(fleet):
            ok = False
            break
        if not pico[fleet] or pico[fleet] != files[fleet]:
            ok = False
            break
    if ok:
        for fleet in fleets:
            print(f"fleets.{fleet} pico={pico[fleet]} files={files[fleet]}")
        sys.exit(0)
    time.sleep(1)

print("timed out waiting for files to match pico", file=sys.stderr)
print("pico:", last[0], file=sys.stderr)
print("files:", last[1], file=sys.stderr)
sys.exit(1)
PY

#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

endpoint="${PICO_ENDPOINT:-http://127.0.0.1:4437}"
metrics="${NESTOR_METRICS:-http://127.0.0.1:9100/metrics}"
stream="/replay"
readers="${READERS:-32}"

wait_for_pico() {
    local i
    for i in $(seq 1 60); do
        if curl -fsS http://127.0.0.1:9090/ready >/dev/null 2>&1; then
            return 0
        fi
        sleep 1
    done
    echo "PicoMQ did not become ready" >&2
    exit 1
}

nestor_counters() {
    curl -fsS "$metrics" | python3 -c '
import re, sys
want = ("nestor_origin_requests_total", "nestor_origin_bytes_total",
        "nestor_blocks_hit_total", "nestor_blocks_miss_total",
        "nestor_blocks_joined_total", "nestor_bytes_served_total")
totals = {name: 0.0 for name in want}
for line in sys.stdin:
    if line.startswith("#"):
        continue
    m = re.match(r"^(\w+)(?:\{[^}]*\})?\s+([0-9.eE+-]+)", line)
    if m and m.group(1) in totals:
        totals[m.group(1)] += float(m.group(2))
for name in want:
    print(name, int(totals[name]))
'
}

counter() {
    awk -v n="$2" '$1 == n { print $2 }' <<<"$1"
}

replay() {
    local n="$1"
    python3 - "$endpoint$stream" "$n" <<'PY'
import sys, threading, urllib.request

url, n = sys.argv[1], int(sys.argv[2])
totals = [0] * n
errors = []

def reader(i):
    seq = "0"
    try:
        while True:
            req = urllib.request.Request(f"{url}?seq={seq}&format=raw&bytes=4194304")
            with urllib.request.urlopen(req, timeout=60) as resp:
                totals[i] += len(resp.read())
                if resp.headers.get("Pico-Up-To-Date", "").lower() == "true":
                    return
                seq = resp.headers["Pico-Next-Seq"]
    except Exception as e:  # noqa: BLE001
        errors.append(f"reader {i}: {e}")

threads = [threading.Thread(target=reader, args=(i,)) for i in range(n)]
for t in threads:
    t.start()
for t in threads:
    t.join()
if errors:
    sys.exit("\n".join(errors))
print(sum(totals))
PY
}

mib() {
    python3 -c "print(f'{$1 / 1048576:.1f} MiB')"
}

wait_for_pico

echo "cold replay: ${readers} readers from seq=0"
before="$(nestor_counters)"
served="$(replay "$readers")"
after="$(nestor_counters)"

origin_req=$(( $(counter "$after" nestor_origin_requests_total) - $(counter "$before" nestor_origin_requests_total) ))
origin_bytes=$(( $(counter "$after" nestor_origin_bytes_total) - $(counter "$before" nestor_origin_bytes_total) ))
hits=$(( $(counter "$after" nestor_blocks_hit_total) - $(counter "$before" nestor_blocks_hit_total) ))
misses=$(( $(counter "$after" nestor_blocks_miss_total) - $(counter "$before" nestor_blocks_miss_total) ))
joined=$(( $(counter "$after" nestor_blocks_joined_total) - $(counter "$before" nestor_blocks_joined_total) ))

echo "  delivered to readers   $(mib "$served")"
echo "  fetched from origin    $(mib "$origin_bytes") in ${origin_req} requests"
echo "  blocks                 miss=${misses} joined=${joined} hit=${hits}"

if (( origin_bytes == 0 )); then
    echo "no origin traffic, PicoMQ is not reading through Nestor" >&2
    exit 1
fi
if (( origin_bytes * 4 > served )); then
    echo "origin fetched more than a quarter of what was served" >&2
    exit 1
fi

first_origin_bytes=$origin_bytes

echo
echo "restart PicoMQ, replay once"
docker compose restart pico >/dev/null
wait_for_pico

before="$(nestor_counters)"
served="$(replay 1)"
after="$(nestor_counters)"
origin_req=$(( $(counter "$after" nestor_origin_requests_total) - $(counter "$before" nestor_origin_requests_total) ))
origin_bytes=$(( $(counter "$after" nestor_origin_bytes_total) - $(counter "$before" nestor_origin_bytes_total) ))
hits=$(( $(counter "$after" nestor_blocks_hit_total) - $(counter "$before" nestor_blocks_hit_total) ))
misses=$(( $(counter "$after" nestor_blocks_miss_total) - $(counter "$before" nestor_blocks_miss_total) ))

echo "  delivered to reader    $(mib "$served")"
echo "  fetched from origin    $(mib "$origin_bytes") in ${origin_req} requests"
echo "  blocks                 miss=${misses} hit=${hits}"

# Remaining misses are the newest records PicoMQ served from its WAL cache
# before the restart, which Nestor never saw.
if (( hits == 0 )); then
    echo "expected cache hits after the restart" >&2
    exit 1
fi
if (( origin_bytes >= first_origin_bytes )); then
    echo "origin traffic did not drop after the restart" >&2
    exit 1
fi

echo
echo "ok"

# Replay fan-out with Nestor

[Nestor](https://nestor.picomq.com/) is a read-through block cache for S3-compatible object storage. It speaks S3, so PicoMQ uses it as the endpoint for the data bucket. Reads of committed objects are served from Nestor's RAM and disk after the first fetch. Writes pass through to the origin.

Source: [`examples/storage/nestor-fanout`](https://github.com/PicoMQ/picomq/tree/main/examples/storage/nestor-fanout).

## Architecture

<div class="pico-diagram">
<svg viewBox="0 0 873 548" width="873" role="img" aria-label="A replay misses the tail and log caches, then the 1 MiB block cache, and leaves the node as a ranged GET to Nestor. Nestor checks RAM, then disk, then fetches the block from RustFS once. The WAL is written to RustFS directly.">
  <defs>
    <marker id="nfa" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M0 0.5 L7.5 4 L0 7.5 Z" class="arrow"/>
    </marker>
  </defs>
  <g transform="translate(0 6)">
    <rect x="176" y="14" width="210" height="508" class="edge-soft"/>
<text x="186" y="30" class="sub">pico</text>
    <rect x="482" y="170" width="178" height="252" class="edge-soft"/>
<text x="492" y="186" class="sub">nestor :9000</text>
    <rect x="20" y="196" width="142" height="56" class="box"/>
<text x="91" y="220" text-anchor="middle" class="label">consumers</text>
<text x="91" y="238" text-anchor="middle" class="sub">replay from seq=0</text>
    <rect x="190" y="40" width="182" height="56" class="box"/>
<text x="281" y="64" text-anchor="middle" class="label">tail cache</text>
<text x="281" y="82" text-anchor="middle" class="sub">recent records</text>
    <rect x="190" y="118" width="182" height="56" class="box"/>
<text x="281" y="142" text-anchor="middle" class="label">log cache</text>
<text x="281" y="160" text-anchor="middle" class="sub">8 MiB, not yet uploaded</text>
    <rect x="190" y="196" width="182" height="56" class="box"/>
<text x="281" y="220" text-anchor="middle" class="label">block cache</text>
<text x="281" y="238" text-anchor="middle" class="sub">1 MiB, object pages</text>
    <rect x="190" y="452" width="182" height="56" class="box"/>
<text x="281" y="476" text-anchor="middle" class="label">wal</text>
<text x="281" y="494" text-anchor="middle" class="sub">batch 5ms, upload 2 MiB</text>
    <rect x="496" y="196" width="150" height="56" class="box-accent"/>
<text x="571" y="220" text-anchor="middle" class="label">RAM</text>
<text x="571" y="238" text-anchor="middle" class="sub">256 MiB</text>
    <rect x="496" y="274" width="150" height="56" class="box-accent"/>
<text x="571" y="298" text-anchor="middle" class="label">disk</text>
<text x="571" y="316" text-anchor="middle" class="sub">2 GiB</text>
    <rect x="496" y="352" width="150" height="56" class="box-accent"/>
<text x="571" y="376" text-anchor="middle" class="label">fetch</text>
<text x="571" y="394" text-anchor="middle" class="sub">one per block</text>
    <rect x="730" y="352" width="123" height="56" class="box"/>
<text x="792" y="376" text-anchor="middle" class="label">rustfs :9000</text>
<text x="792" y="394" text-anchor="middle" class="sub">origin</text>
    <path d="M162 224 L186 224" class="edge" marker-end="url(#nfa)"/>
    <path d="M281 196 L281 178" class="edge" marker-end="url(#nfa)"/>
    <path d="M281 118 L281 100" class="edge" marker-end="url(#nfa)"/>
    <path d="M372 224 L492 224" class="edge" marker-end="url(#nfa)"/>
    <path d="M571 252 L571 270" class="edge" marker-end="url(#nfa)"/>
    <path d="M571 330 L571 348" class="edge" marker-end="url(#nfa)"/>
    <path d="M646 380 L726 380" class="edge" marker-end="url(#nfa)"/>
    <path d="M372 480 L792 480 L792 412" class="edge-soft" marker-end="url(#nfa)"/>
    <text x="291" y="189" text-anchor="start" class="sub">miss</text>
    <text x="291" y="111" text-anchor="start" class="sub">miss</text>
    <text x="427" y="216" text-anchor="middle" class="sub">GET range</text>
    <text x="581" y="267" text-anchor="start" class="sub">miss</text>
    <text x="581" y="345" text-anchor="start" class="sub">miss</text>
    <text x="695" y="372" text-anchor="middle" class="sub">once</text>
    <text x="474" y="472" text-anchor="middle" class="sub">WAL, direct to origin</text>
  </g>
</svg>
</div>

| Path | Endpoint | Why |
| --- | --- | --- |
| `--storage` | Nestor | Committed objects are read many times. Immutable keys, one origin fetch per block. |
| `--wal` | Object store | Written once, read on recovery. Nothing to cache. |

PicoMQ's own block cache is per node and per process. Nestor is shared by every node and survives PicoMQ restarts and stream transfers.

## Configuration

PicoMQ:

```bash
--storage='-2@s3://picomq?region=us-east-1&endpoint=http://nestor:9000&pathStyle=true'
--wal='0@s3://picomq?region=us-east-1&endpoint=http://s3:9000&pathStyle=true'
```

`nestor.toml`:

```toml
[origin]
endpoint = "http://s3:9000"
credentials = { source = "static", access_key = "...", secret_key = "..." }

[auth]
mode = "static"
access_key = "..."
secret_key = "..."

[buckets]
consistency = { mode = "immutable" }   # PicoMQ never reuses an object key
readahead = 0                          # PicoMQ prefetches for itself
```

PicoMQ signs with the `[auth]` keys through the usual `AWS_*` variables.

## The example

32 consumers replay one 32 MiB stream. PicoMQ's caches are set to 8 MiB WAL and 1 MiB block so the reads leave the node.

<div class="pico-diagram">
<svg viewBox="0 0 771 176" width="771" role="img" aria-label="32 consumers replay one stream from seq 0. PicoMQ issues 38 ranged GETs to Nestor. Nestor fetches 38 blocks from RustFS once and serves 1 GiB.">
  <defs>
    <marker id="nfb" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M0 0.5 L7.5 4 L0 7.5 Z" class="arrow"/>
    </marker>
  </defs>
  <g transform="translate(0 -10)">
    <rect x="20" y="30" width="130" height="36" class="box"/>
<text x="85" y="53" text-anchor="middle" class="label">consumer 1</text>
    <rect x="20" y="80" width="130" height="36" class="box"/>
<text x="85" y="103" text-anchor="middle" class="label">consumer 2</text>
    <rect x="20" y="130" width="130" height="36" class="box"/>
<text x="85" y="153" text-anchor="middle" class="label">consumer 32</text>
    <rect x="200" y="70" width="128" height="56" class="box"/>
<text x="264" y="94.0" text-anchor="middle" class="label">pico</text>
<text x="264" y="112.0" text-anchor="middle" class="sub">/replay, 32 MiB</text>
    <rect x="424" y="70" width="108" height="56" class="box-accent"/>
<text x="479" y="94.0" text-anchor="middle" class="label">nestor</text>
<text x="479" y="112.0" text-anchor="middle" class="sub">1 GiB served</text>
    <rect x="629" y="70" width="122" height="56" class="box"/>
<text x="690" y="94.0" text-anchor="middle" class="label">rustfs</text>
<text x="690" y="112.0" text-anchor="middle" class="sub">25 MiB fetched</text>
    <path d="M150 48 L174 48" class="edge"/>
<path d="M150 98 L174 98" class="edge"/>
<path d="M150 148 L174 148" class="edge"/>
<path d="M174 48 L174 148" class="edge"/>
<path d="M174 98 L196 98" class="edge" marker-end="url(#nfb)"/>
    <path d="M328 98 L420 98" class="edge" marker-end="url(#nfb)"/>
    <path d="M533 98 L625 98" class="edge" marker-end="url(#nfb)"/>
    <text x="376" y="90" text-anchor="middle" class="sub">38 GETs</text>
    <text x="581" y="90" text-anchor="middle" class="sub">38 fetches</text>
  </g>
</svg>
</div>

```bash
cd examples/storage/nestor-fanout
docker compose up -d
./seed.sh
./verify.sh
```

`verify.sh` output from one run:

```
cold replay: 32 readers from seq=0
  delivered to readers   1024.0 MiB
  fetched from origin    25.3 MiB in 38 requests
  blocks                 miss=38 joined=0 hit=2111

restart PicoMQ, replay once
  delivered to reader    32.0 MiB
  fetched from origin    6.8 MiB in 10 requests
  blocks                 miss=10 hit=125
```

First pass: 32 readers, one origin fetch per block. Second pass: PicoMQ restarted with empty caches, Nestor did not. The 10 misses are the newest records PicoMQ was serving from its WAL cache before the restart, which Nestor had never been asked for.

| Service | Port | Purpose |
| --- | --- | --- |
| `pico` | `4437`, `9090`, `9092` | Pico HTTP, admin, Kafka |
| `nestor` | `9000`, `9100` | S3 endpoint, Prometheus metrics |
| `rustfs` | `19000` | Origin |

Metrics to watch at `:9100/metrics`: `nestor_origin_requests_total`, `nestor_blocks_hit_total`, `nestor_blocks_miss_total`.

# PicoMQ

PicoMQ is durable, real-time streams over HTTP, built on S3-compatible object storage.
[Documentation](https://picomq.com/docs/) · [Quick start](https://picomq.com/docs/quick-start) · [Playground](https://picomq.com/docs/playground)

- **`s3stream/`** the stream engine (see [s3stream/README.md](s3stream/README.md))
- **`picomq/`** the host: metadata plane, server, protocol frontends (HTTP with Pico protocol and Durable Streams, plus Kafka), client, and the `pico` CLI

## Install

```bash
cargo install --path picomq/pico-cli
```

This puts the `pico` binary in `~/.cargo/bin`. Or run it in place with `cargo run -p pico-cli -- <args>`.

## Run a node

```bash
# single node: SQLite metadata log, local object storage
pico serve \
    --meta-url sqlite:./data/meta.db \
    --storage=-2@file://./objects
```

Every flag has a `PICO_*` env equivalent. `/health` and `/ready` are on `--admin-listen` (default `127.0.0.1:9090`). Auth is off by default, non-loopback binds need `--auth required` or `--insecure-allow-remote`.

## Docker

Skips the install, everything runs in compose:

```bash
cd harness/aio
cp .env.example .env

docker compose up --build                          # Postgres + RustFS, 1 node
docker compose -f compose.cluster.yml up --build   # same stack, 2 nodes
docker compose -f compose.lite.yml up --build      # SQLite + file://, no deps
```

Pico: `http://localhost:4437` (cluster also `:4438`). Dashboard: `:9090`. 
`harness/byo` is the same against an existing Postgres and object store, configured through `.env`.

## Use it

```bash
pico create /streams/orders --content-type text/plain
seq 1 1000 | pico append /streams/orders --batch 100
pico read /streams/orders
pico tail /streams/orders -f
pico close /streams/orders && pico delete /streams/orders

pico --http2 bench -b 1024 -w 512 --connections 4 --streams 4 -d 60
```

## Test

```bash
cargo test --workspace

# Postgres-backed tests, env-gated
PICOMQ_PG_URL=postgres://user:pass@localhost:5432/picomq \
    cargo test -p pico-sql --test pg_contract --test pg_e2e
```

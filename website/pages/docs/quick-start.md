# Quick start

A node needs a metadata database and an object store. For a first run both can be local files, so nothing has to be installed besides the binary.

## Install

Build the `pico` binary from source with the Rust toolchain:

```bash
git clone https://github.com/picomq/picomq && cd picomq
cargo install --path picomq/pico-cli
```

This puts `pico` in `~/.cargo/bin`, which cargo adds to the `PATH`. To build without installing, use `cargo build --release -p picomq-cli` and run `./target/release/pico`. The [Docker](#docker) section below skips the host install entirely.

## Run a node

```bash
# single node: SQLite metadata log, local object storage
pico serve \
    --meta-url sqlite:./data/meta.db \
    --storage=-2@file://./objects
```

The server listens on `http://127.0.0.1:4437` and the admin listener on `http://127.0.0.1:9090`. Every flag has a `PICO_*` environment variable equivalent. `--protocol pico|ds|kafka` selects the client protocol, `pico` by default, and Kafka mode also takes `--kafka-listen`.

Against real infrastructure the same command points at Postgres and an S3 bucket:

```bash
pico serve \
    --node-id 2 --protocol ds \
    --listen 0.0.0.0:4437 --http-address http://node2.internal:4437 \
    --auth required --auth-bootstrap-token-file ./root-token \
    --meta-url postgres://user:pass@pg:5432/picomq \
    --storage=-2@s3://bucket?region=us-east-1
```

::: info Note
Listening beyond `127.0.0.1` requires auth or an explicit `--insecure-allow-remote` opt-out. See [Authentication](/docs/operations/auth) for generating the token.
:::

<div class="pico-or">or</div>

## Docker

The `harness/aio` compose files start everything in one command, including Postgres and RustFS as the object store:

```bash
cd harness/aio
cp .env.example .env

docker compose up --build                          # Postgres + RustFS, 1 node
docker compose -f compose.cluster.yml up --build   # same stack, 2 nodes
docker compose -f compose.lite.yml up --build      # SQLite + file://, no deps
```

Nodes serve on `http://localhost:4437` (the cluster adds `:4438`). The admin dashboard is at `http://localhost:9090`, and `:9091` for the second node. RustFS exposes its API on `:9000` and a console on `:9001`.

::: info Note
The compose nodes run with auth off for development. Setting `PICO_AUTH=required` in `.env` turns it on, see [Authentication](/docs/operations/auth).
:::

`harness/byo` has the same layout for an existing Postgres and object store. Set `PICO_META_URL`, `PICO_STORAGE`, and the `AWS_*` credentials in `.env`.

## First stream

Each tab assumes a node started with the matching `--protocol` (or `PICO_PROTOCOL` in the compose `.env`).

:::tabs key:protocol

== Pico

The `pico` binary is also the client. If the node runs in compose and `pico` is not installed on the host, prefix the commands with `docker compose exec <service>`. The single-node file names the service `pico`. The cluster file names them `pico1` and `pico2`.

```bash
# single-node compose.yml
docker compose exec pico pico ls

# cluster compose.cluster.yml
docker compose -f compose.cluster.yml exec pico1 pico ls
```

```bash
pico create /streams/orders --content-type text/plain
seq 1 1000 | pico append /streams/orders --batch 100
pico read /streams/orders
pico tail /streams/orders -f
pico ls --prefix /streams/
pico close /streams/orders && pico delete /streams/orders
```

`--endpoint` selects the server, or save one as a profile:

```bash
pico --endpoint http://node2.internal:4437 config set prod
pico --profile prod ls
```

Streams are URL paths, so any HTTP client works too:

```bash
# create
curl -X PUT -H 'Content-Type: text/plain' http://localhost:4437/streams/orders

# append one record
curl -X POST -H 'Content-Type: text/plain' -d 'order-1' \
    http://localhost:4437/streams/orders

# read from the start
curl 'http://localhost:4437/streams/orders?seq=0'
```

Appends return the assigned sequence in the `Pico-Next-Seq` header. Reads accept `seq=now` to start at the tail, `live=long-poll` to wait for the next record, and `live=sse` to keep the response open as an event stream.

== Durable Streams

The protocol is plain HTTP on the [Durable Streams](/docs/design/protocols) wire vocabulary:

```bash
# create
curl -X PUT -H 'Content-Type: text/plain' http://localhost:4437/streams/orders

# append one record
curl -X POST -H 'Content-Type: text/plain' -d 'order-1' \
    http://localhost:4437/streams/orders

# read from the start
curl http://localhost:4437/streams/orders

# follow from the tail
curl -N 'http://localhost:4437/streams/orders?offset=now&live=sse'
```

Position state travels in `Stream-*` response headers, so a reader resumes from its last offset with `?offset=`.

== Kafka

Standard Kafka clients connect to the `--kafka-listen` address. Producing auto-creates the topic:

```bash
# produce
printf 'order-1\norder-2\n' | kcat -P -b localhost:9092 -t orders

# consume from the start
kcat -C -b localhost:9092 -t orders -o beginning -e

# consume with a group, offsets are committed and survive restarts
kcat -G orders-group -b localhost:9092 -X auto.offset.reset=earliest orders
```

Topics map to streams one to one, detail in the [Kafka protocol](/docs/kafka) reference.

:::

## Check the cluster

```bash
pico admin --admin-endpoint http://localhost:9090 cluster
pico admin --admin-endpoint http://localhost:9090 nodes
```

The same information is on the dashboard at `http://localhost:9090`.

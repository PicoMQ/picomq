# Playground

Two-node Docker cluster, then stream and auth exercises against it. The cluster serves one client protocol, chosen in `.env` before startup, so pick a protocol tab once and follow it through the page. Tabs stay in sync.

## Tooling

Needs Docker, plus the client for your protocol.

:::tabs key:protocol

== Pico

The `pico` CLI:

```bash
cargo install --path picomq/pico-cli
```

macOS also ships a text editor named `pico`. Put Cargo first on `PATH` for the current shell:

```bash
export PATH="$HOME/.cargo/bin:$PATH"
hash -r
which -a pico
# first line should be .../.cargo/bin/pico
```

If `/usr/bin/pico` is still first, Cargo is missing from `PATH` or sits after `/usr/bin`. Add this to `~/.zshrc` (zsh) or `~/.bashrc` (bash), then open a new terminal:

```bash
echo 'export PATH="$HOME/.cargo/bin:$PATH"' >> ~/.zshrc
source ~/.zshrc
which pico
```

Or call the CLI by full path:

```bash
~/.cargo/bin/pico --help
```

== Durable Streams

`curl`. The protocol is plain HTTP, so nothing to install.

== Kafka

Any Kafka client works. This page uses `kcat`:

```bash
brew install kcat   # or: apt install kcat
```

:::

## Cluster

```bash
cd harness/aio
cp -n .env.example .env
```

Set the protocol in `.env`:

:::tabs key:protocol

== Pico

```bash
PICO_PROTOCOL=pico
```

Auth ships commented out. Uncomment both lines to exercise the auth sections below:

```bash
PICO_AUTH=required
PICO_AUTH_BOOTSTRAP_TOKEN=ZGV2L3Jvb3Q.BwgJCgsMDQ4PEBESExQVFhcYGRobHB0eHyAhIiMkJSY
```

== Durable Streams

```bash
PICO_PROTOCOL=ds
```

Auth ships commented out. Uncomment both lines to exercise the auth sections below:

```bash
PICO_AUTH=required
PICO_AUTH_BOOTSTRAP_TOKEN=ZGV2L3Jvb3Q.BwgJCgsMDQ4PEBESExQVFhcYGRobHB0eHyAhIiMkJSY
```

== Kafka

```bash
PICO_PROTOCOL=kafka
```

The Kafka listener carries no client auth. `PICO_AUTH` gates only the admin API in this mode, so the auth sections below do not apply. See [exposure](/docs/kafka#exposure).

:::

Then start the cluster:

```bash
docker compose -f compose.cluster.yml up --build
```

Wait until `http://localhost:9090/ready` returns ready, then export the addresses:

:::tabs key:protocol

== Pico

```bash
export ENDPOINT=http://localhost:4437   # node 2 is :4438
export ADMIN=http://localhost:9090
export PICO_TOKEN='ZGV2L3Jvb3Q.BwgJCgsMDQ4PEBESExQVFhcYGRobHB0eHyAhIiMkJSY'
```

== Durable Streams

```bash
export ENDPOINT=http://localhost:4437   # node 2 is :4438
export ADMIN=http://localhost:9090
export PICO_TOKEN='ZGV2L3Jvb3Q.BwgJCgsMDQ4PEBESExQVFhcYGRobHB0eHyAhIiMkJSY'
```

== Kafka

```bash
export BOOTSTRAP=localhost:9092   # node 2 is :9093
export ADMIN=http://localhost:9090
```

Either node works as bootstrap. Clients discover both brokers and route to the topic's owner.

:::

## Streams

Create a stream, follow it live, write to it, then read back and clean up.

:::tabs key:protocol

== Pico

```bash
pico --endpoint $ENDPOINT --http2 --token "$PICO_TOKEN" \
  create /streams/demo --content-type text/plain
```

`created=true` means new. `created=false` means the stream already existed.

Write and follow in two terminals.

Terminal 1:

```bash
pico --endpoint $ENDPOINT --http2 --token "$PICO_TOKEN" \
  tail /streams/demo -f
```

Terminal 2 (each Enter sends one record):

```bash
while IFS= read -r line; do
  printf '%s\n' "$line" | pico --endpoint $ENDPOINT --http2 --token "$PICO_TOKEN" \
    append /streams/demo --batch 1
done
```

Plain `pico append` without a pipe buffers stdin until Ctrl-D, then sends.

Catch up and clean up:

```bash
pico --endpoint $ENDPOINT --http2 --token "$PICO_TOKEN" read /streams/demo
pico --endpoint $ENDPOINT --http2 --token "$PICO_TOKEN" close /streams/demo
pico --endpoint $ENDPOINT --http2 --token "$PICO_TOKEN" delete /streams/demo
```

== Durable Streams

```bash
curl -X PUT "$ENDPOINT/streams/demo" \
  -H "Authorization: Bearer $PICO_TOKEN" \
  -H 'Content-Type: text/plain'
```

`201` means new. `200` means the stream already existed with the same config.

Write and follow in two terminals.

Terminal 1 (SSE from the tail):

```bash
curl -N "$ENDPOINT/streams/demo?offset=now&live=sse" \
  -H "Authorization: Bearer $PICO_TOKEN"
```

Terminal 2 (each Enter sends one record):

```bash
while IFS= read -r line; do
  printf '%s\n' "$line" | curl -s -X POST "$ENDPOINT/streams/demo" \
    -H "Authorization: Bearer $PICO_TOKEN" \
    -H 'Content-Type: text/plain' --data-binary @-
done
```

Catch up and clean up. A plain `GET` reads from the beginning, and the response headers carry the next offset and cursor for resuming:

```bash
curl -i "$ENDPOINT/streams/demo" -H "Authorization: Bearer $PICO_TOKEN"
curl -X DELETE "$ENDPOINT/streams/demo" -H "Authorization: Bearer $PICO_TOKEN"
```

== Kafka

Producing to a topic auto-creates it as the stream `/demo` with one partition:

```bash
printf 'hello\nworld\n' | kcat -P -b $BOOTSTRAP -t demo
```

Write and follow in two terminals.

Terminal 1:

```bash
kcat -C -b $BOOTSTRAP -t demo
```

Terminal 2 (each Enter sends one record):

```bash
kcat -P -b $BOOTSTRAP -t demo
```

Catch up from the beginning, then with a consumer group. The group commits offsets, so a second run resumes where the first stopped:

```bash
kcat -C -b $BOOTSTRAP -t demo -o beginning -e
kcat -G demo-group -b $BOOTSTRAP -X auto.offset.reset=earliest demo
```

Topic deletion goes through `DeleteTopics`, which `kcat` does not expose. Use any admin-capable client, or the admin API at `$ADMIN` to inspect and delete the stream behind the topic.

:::

## Auth gate

The auth sections need `PICO_AUTH=required` from the cluster step and apply to the HTTP protocols. In Kafka mode the token gates only the admin API.

Health stays open. Protocol calls need the bearer.

```bash
curl -s -o /dev/null -w '%{http_code}\n' $ENDPOINT/streams/demo
curl -s -o /dev/null -w '%{http_code}\n' $ADMIN/ready
```

Expect `401` without a token on the protocol listener, `200` on `/ready`.

:::tabs key:protocol

== Pico

```bash
pico --endpoint $ENDPOINT --http2 --token "$PICO_TOKEN" \
  create /streams/demo --content-type text/plain
pico --endpoint $ENDPOINT --http2 --token "$PICO_TOKEN" head /streams/demo
```

== Durable Streams

```bash
curl -X PUT "$ENDPOINT/streams/demo" \
  -H "Authorization: Bearer $PICO_TOKEN" -H 'Content-Type: text/plain'
curl -I "$ENDPOINT/streams/demo" -H "Authorization: Bearer $PICO_TOKEN"
```

== Kafka

Not applicable. The Kafka listener accepts clients without credentials, and the `curl` checks above still hold for the admin listener.

:::

## Narrow token

Issue a write-only credential under `/logs/`. The response body includes `token` once. Save it.

```bash
RESP=$(curl -s -X POST $ADMIN/admin/tokens \
  -H "Authorization: Bearer $PICO_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{
    "id": "svc/ingest",
    "scope": {
      "streams": [{ "prefix": "/logs/" }],
      "groups": { "stream": { "read": false, "write": true } },
      "audiences": ["pico"]
    }
  }')
echo "$RESP"
export INGEST=$(echo "$RESP" | python3 -c 'import sys,json; print(json.load(sys.stdin)["token"])')
```

If the id already exists, delete it and issue again. The secret cannot be fetched later.

```bash
curl -s -X DELETE "$ADMIN/admin/tokens/svc%2Fingest" \
  -H "Authorization: Bearer $PICO_TOKEN"
```

Exercise the scope:

:::tabs key:protocol

== Pico

```bash
pico --endpoint $ENDPOINT --http2 --token "$PICO_TOKEN" \
  create /logs/app --content-type text/plain

echo hi | pico --endpoint $ENDPOINT --http2 --token "$INGEST" \
  append /logs/app --batch 1

pico --endpoint $ENDPOINT --http2 --token "$INGEST" read /logs/app

echo nope | pico --endpoint $ENDPOINT --http2 --token "$INGEST" \
  append /streams/demo --batch 1

pico --endpoint $ENDPOINT --http2 --token "$PICO_TOKEN" read /logs/app
```

== Durable Streams

```bash
curl -X PUT "$ENDPOINT/logs/app" \
  -H "Authorization: Bearer $PICO_TOKEN" -H 'Content-Type: text/plain'

echo hi | curl -s -X POST "$ENDPOINT/logs/app" \
  -H "Authorization: Bearer $INGEST" -H 'Content-Type: text/plain' --data-binary @-

curl -s -o /dev/null -w '%{http_code}\n' "$ENDPOINT/logs/app" \
  -H "Authorization: Bearer $INGEST"

echo nope | curl -s -o /dev/null -w '%{http_code}\n' -X POST "$ENDPOINT/streams/demo" \
  -H "Authorization: Bearer $INGEST" -H 'Content-Type: text/plain' --data-binary @-

curl "$ENDPOINT/logs/app" -H "Authorization: Bearer $PICO_TOKEN"
```

== Kafka

Not applicable to the Kafka listener. Scoped tokens still govern the admin API, so the issue and delete calls above behave the same.

:::

Expect append under `/logs/` to succeed, read with ingest to return `403`, append outside the prefix to return `403`, and root read to succeed.

## Revoke

```bash
curl -s $ADMIN/admin/tokens -H "Authorization: Bearer $PICO_TOKEN"

curl -s -X DELETE "$ADMIN/admin/tokens/svc%2Fingest" \
  -H "Authorization: Bearer $PICO_TOKEN"
```

The revoked token stops working on every node:

:::tabs key:protocol

== Pico

```bash
echo hi | pico --endpoint $ENDPOINT --http2 --token "$INGEST" \
  append /logs/app --batch 1
```

== Durable Streams

```bash
echo hi | curl -s -o /dev/null -w '%{http_code}\n' -X POST "$ENDPOINT/logs/app" \
  -H "Authorization: Bearer $INGEST" -H 'Content-Type: text/plain' --data-binary @-
```

== Kafka

```bash
curl -s -o /dev/null -w '%{http_code}\n' $ADMIN/admin/tokens \
  -H "Authorization: Bearer $INGEST"
```

:::

Expect `401` after revoke. Detail on scopes and audiences is in [Authentication](/docs/operations/auth) and [Authorization](/docs/design/auth).

## Bench

:::tabs key:protocol

== Pico

```bash
pico --endpoint $ENDPOINT --http2 --token "$PICO_TOKEN" bench \
  -d 20 -b 1024 -w 512 -n 1 --connections 4 --streams 4 --no-read
```

`-b` is record size in bytes, `-n` records per append, `-w` in-flight appends, `--connections` and `--streams` spread the load, `--no-read` is write-only.

== Durable Streams

`pico bench` speaks the Pico protocol, so it needs a cluster started with `PICO_PROTOCOL=pico`. Any HTTP load tool works against the Durable Streams routes directly.

== Kafka

`pico bench` speaks the Pico protocol. For Kafka mode use standard Kafka load tools, for example `kafka-producer-perf-test` or `librdkafka`'s `rdkafka_performance`.

:::

## Tear down

From `harness/aio`:

```bash
docker compose -f compose.cluster.yml down
```

Add `-v` to delete the Postgres and RustFS volumes too:

```bash
docker compose -f compose.cluster.yml down -v
```

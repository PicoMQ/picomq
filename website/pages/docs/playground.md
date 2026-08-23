# Playground

Two-node Docker cluster, then stream and auth exercises against it. Needs Docker and a host `pico` CLI.

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

## Cluster

```bash
cd harness/aio
cp -n .env.example .env
```

`.env.example` ships auth commented out. In `.env`, uncomment both lines and set the bootstrap token:

```bash
# before
# PICO_AUTH=required
# PICO_AUTH_BOOTSTRAP_TOKEN=

# after
PICO_AUTH=required
PICO_AUTH_BOOTSTRAP_TOKEN=ZGV2L3Jvb3Q.BwgJCgsMDQ4PEBESExQVFhcYGRobHB0eHyAhIiMkJSY
```

Then start the cluster:

```bash
docker compose -f compose.cluster.yml up --build
```

Wait until `http://localhost:9090/ready` returns ready.

- Protocol: `http://localhost:4437` (node 1), `http://localhost:4438` (node 2)
- Admin: `http://localhost:9090` (node 1), `http://localhost:9091` (node 2)

```bash
export ENDPOINT=http://localhost:4437
export ADMIN=http://localhost:9090
export PICO_TOKEN='ZGV2L3Jvb3Q.BwgJCgsMDQ4PEBESExQVFhcYGRobHB0eHyAhIiMkJSY'
```

## Streams

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

## Auth gate

Health stays open. Protocol calls need the bearer.

```bash
curl -s -o /dev/null -w '%{http_code}\n' $ENDPOINT/streams/demo
curl -s -o /dev/null -w '%{http_code}\n' $ADMIN/ready

pico --endpoint $ENDPOINT --http2 --token "$PICO_TOKEN" \
  create /streams/demo --content-type text/plain
pico --endpoint $ENDPOINT --http2 --token "$PICO_TOKEN" head /streams/demo
```

Expect `401` without a token on the protocol listener, `200` on `/ready`.

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

Expect append under `/logs/` to succeed, read with ingest to return `403`, append outside the prefix to return `403`, and root read to succeed.

## Revoke

```bash
curl -s $ADMIN/admin/tokens -H "Authorization: Bearer $PICO_TOKEN"

curl -s -X DELETE "$ADMIN/admin/tokens/svc%2Fingest" \
  -H "Authorization: Bearer $PICO_TOKEN"

echo hi | pico --endpoint $ENDPOINT --http2 --token "$INGEST" \
  append /logs/app --batch 1
```

Expect `401` after revoke. Same on every node. Detail on scopes and audiences is in [Authentication](/docs/operations/auth) and [Authorization](/docs/design/auth).

## Bench

```bash
pico --endpoint $ENDPOINT --http2 --token "$PICO_TOKEN" bench \
  -d 20 -b 1024 -w 512 -n 1 --connections 4 --streams 4 --no-read
```

`-b` is record size in bytes, `-n` records per append, `-w` in-flight appends, `--connections` and `--streams` spread the load, `--no-read` is write-only.

## Tear down

From `harness/aio`:

```bash
docker compose -f compose.cluster.yml down
```

Add `-v` to delete the Postgres and RustFS volumes too:

```bash
docker compose -f compose.cluster.yml down -v
```

# Docker

A PicoMQ cluster is defined by two shared resources. Every node points at the same Postgres database and the same object storage bucket, and that is the entire membership mechanism. There is no join procedure, no seed list, and no quorum to size. A node that starts with the right `--meta-url` and `--storage` registers itself and is part of the cluster. This page covers running nodes yourself, from a bare binary to Docker Compose.

## Single node

One node with SQLite and a local directory is a complete deployment, suitable for development and small single-machine workloads.

```bash
pico serve --meta-url sqlite:./data/meta.db --storage=-2@file://./objects
```

Durability follows the storage. With `file://` the data is as durable as that disk. Pointing the same single node at S3 gives object store durability without running Postgres, since SQLite only limits how many nodes can share the metadata log, one.

## Cluster

Each node needs a unique `--node-id`, the shared Postgres URL, the shared bucket, and a `--http-address` that clients and other nodes can reach. Binding beyond loopback requires either auth or an explicit `--insecure-allow-remote` opt-out. Here each node gets `--auth required` and the shared bootstrap token, which is idempotent across nodes and restarts (see [Authentication](/docs/operations/auth)).

```bash
pico serve --node-id 1 --listen 0.0.0.0:4437 \
    --http-address http://node1.internal:4437 \
    --auth required --auth-bootstrap-token-file /run/secrets/pico-root \
    --meta-url postgres://user:pass@pg:5432/picomq \
    --storage=-2@s3://picomq?region=us-east-1
```

Routing shapes what sits in front of the nodes. Clients are redirected to a stream's owner with its advertised address, so clients must be able to reach every node directly and follow redirects. A load balancer works fine as the entry point for creates and first requests, but it should not be the only reachable address, since redirects bypass it by design.

## Images and compose

Images are published to GitHub Container Registry on every merge, tagged `latest`, by version, and by commit SHA. The image builds the dashboard and embeds it, so the admin listener serves the full UI with no extra setup.

The repository has two compose harnesses. `harness/aio` is self-contained, starting Postgres and RustFS alongside one or two nodes, auth off by default (`PICO_AUTH=required` in `.env` turns it on with a known dev bootstrap token). `harness/byo` has the same layout against an existing Postgres and object store, configured through `.env`, runs with auth required, and refuses to start without a bootstrap token.

```bash
cd harness/aio && cp .env.example .env
docker compose up --build                          # 1 node
docker compose -f compose.cluster.yml up --build   # 2 nodes
```

## Health and readiness

The admin listener exposes two probes. `/health` answers whenever the process is up, suitable as a liveness check. `/ready` reports `true` only when the node is serving and its registration has been applied to the metadata state, which is the signal to start routing traffic to it.

`--shutdown-drain-sec` makes shutdown orchestration-friendly: for that many seconds the node fails `/ready` while still serving, giving a load balancer time to move traffic before the listeners close.

## Restarts and upgrades

Restarting a node is safe at any moment. Acknowledged data is in object storage, and the new process registers at a higher epoch, which fences anything the old one left in flight. Streams the node owned come back in one of two ways: the restarted node reopens them on demand, or if the node stays down, their next request lands on any node, finds them closed, and revives them there.

Upgrades are rolling restarts, one node at a time, waiting for `/ready` between nodes. For a planned drain, transfer the node's streams away first so clients see a controlled handoff instead of a recovery.

```bash
pico admin set-slots 1 --slots 0        # stop new placements on node 1
pico admin transfer /streams/orders --to-node 2 --wait
```

Setting slots to `0` excludes a node from future placements without touching what it already serves, which makes the transfer list stable while draining.

## Growing the cluster

Adding a node is starting one with a new id against the same database and bucket. It registers itself, begins taking new placements weighted by its slots, and existing streams stay where they are until transferred. Removing a node is the drain above followed by stopping the process. The metadata row for a departed node remains, it simply never holds streams again.

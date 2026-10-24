# Docker

A PicoMQ cluster is defined by two shared resources. Every node points at the same Postgres database and the same object storage bucket, and that is the entire membership mechanism. There is no join procedure, no seed list, and no quorum to size. A node that starts with the right `--meta-url` and `--storage` registers itself and is part of the cluster. This page covers running nodes yourself, from a bare binary to Docker Compose.

## Single node

One node with SQLite and a local directory is a complete deployment, suitable for development and small single-machine workloads.

```bash
pico serve --meta-url sqlite:./data/meta.db --storage=-2@file://./objects
```

Durability follows the storage. With `file://` the data is as durable as that disk. Pointing the same single node at S3 gives object store durability without running Postgres, since SQLite only limits how many nodes can share the metadata log, one.

## Cluster

Each node needs a unique `--node-id`, the shared Postgres URL, the shared bucket, and a `--http-address` that clients and other nodes can reach. Binding beyond loopback requires either auth or an explicit `--insecure-allow-remote` opt-out. Here each node gets `--auth required` and the shared bootstrap token, which is idempotent across nodes and restarts (see [Authentication](/docs/operations/auth)). The Kafka listener has no auth, so exposing it takes the opt-out and its own advertised address.

```bash
pico serve --node-id 1 --listen 0.0.0.0:4437 \
    --http-address http://node1.internal:4437 \
    --kafka-listen 0.0.0.0:9092 --kafka-advertise node1.internal:9092 \
    --insecure-allow-remote \
    --auth required --auth-bootstrap-token-file /run/secrets/pico-root \
    --meta-url postgres://user:pass@pg:5432/picomq \
    --storage=-2@s3://picomq?region=us-east-1
```

Every node keeps its WAL in the bucket, so any node can take over a dead node's WAL. See [Configuration](/docs/operations/configuration#storage).

Routing shapes what sits in front of the nodes. Clients are redirected to a stream's owner with its advertised address, so clients must be able to reach every node directly and follow redirects. A load balancer works fine as the entry point for creates and first requests, but it should not be the only reachable address, since redirects bypass it by design.

## Images and compose

One image is published to GitHub Container Registry for every release.

| Image | Tags | Contents |
| --- | --- | --- |
| `ghcr.io/picomq/picomq` | `latest`, version, major.minor, commit SHA | The `pico` binary with the dashboard embedded |

The repository has two compose harnesses. `harness/aio` is self-contained, starting Postgres and RustFS alongside one or two nodes, auth off by default (`PICO_AUTH=required` in `.env` turns it on with a known dev bootstrap token). `harness/byo` has the same layout against an existing Postgres and object store, configured through `.env`, runs with auth required, and refuses to start without a bootstrap token.

```bash
cd harness/aio && cp .env.example .env
docker compose up --build                            # 1 node
docker compose -f compose.lite.yml up --build        # 1 node, SQLite and local files, no Postgres or RustFS
docker compose -f compose.cluster.yml up --build     # 2 nodes
```

## Health and readiness

The admin listener exposes two probes. `/health` answers whenever the process is up, suitable as a liveness check. `/ready` reports ready only when the node is serving, its registration has been applied to the metadata state, and it is not fenced, which is the signal to route traffic to it.

`--shutdown-drain-sec` makes shutdown orchestration-friendly: for that many seconds the node fails `/ready` while still serving, giving a load balancer time to move traffic before the node hands off. The orchestrator's stop timeout must cover the drain plus a few seconds for the handoff, or the node is killed first and its streams wait for the node timeout instead.

## Restarts and upgrades

Restarting a node is safe at any moment. Acknowledged data is durable in the WAL, and the new process registers at a higher epoch, which fences anything the old one left in flight. What happens to the node's streams depends on how it stopped. The mechanism is in [Failover](/docs/design/failover).

| The node | Its streams | Clients see |
| --- | --- | --- |
| Gets `SIGTERM` | Move to live nodes about a second after the drain | Live reads end with `503`, other requests are redirected |
| Crashes or loses its network | Move after `--node-timeout-sec`, once its WAL is recovered | Retryable errors until then |
| Is fenced while still running | Move as for a crash. The node exits with code `1` | The same |

A node that exits with code `1` should be restarted by the orchestrator, and rejoins at a new epoch.

Upgrades are rolling restarts, one node at a time, waiting for `/ready` between nodes. Moving chosen streams first with a transfer is still possible, but no longer needed for a clean handoff.

```bash
pico admin transfer /streams/orders --to-node 2 --wait
```

If the metadata database becomes unreachable, nodes keep serving the streams they hold and nobody is fenced. Creates and other metadata changes return `503` until it is back.

## Growing the cluster

Adding a node is starting one with a new id against the same database and bucket. It registers itself, begins taking new placements weighted by its slots, and existing streams stay where they are until transferred.

Removing a node is stopping it. Setting its slots to `0` first, `pico admin set-slots 1 --slots 0`, keeps new placements off it while it drains. The metadata row of a departed node remains with `fenced: true` and never holds streams again.

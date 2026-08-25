# Configuration

A node is configured entirely through `pico serve` flags. There is no server config file. Every flag has a `PICO_*` environment variable, so container deployments can configure everything through the environment, and a flag always wins over its variable.

```bash
pico serve \
    --node-id 2 \
    --listen 0.0.0.0:4437 --http-address http://node2.internal:4437 \
    --meta-url postgres://user:pass@pg:5432/picomq \
    --storage=-2@s3://bucket?region=us-east-1
```

## Identity

| Flag | Default | Purpose |
| --- | --- | --- |
| `--node-id` | `1` | Stable identity in the cluster. Must be unique per node. |
| `--node-epoch` | current time in ms | Fencing token for this process. Leave unset so every restart gets a higher one. |
| `--cluster-id` | `picomq` | Reported in the admin API, useful when running several clusters. |
| `--slots` | `1` | Placement weight registered at startup. A node with `4` slots takes four times the streams of a node with `1`. |

The node id is the one value worth care. Reusing an id for a different machine is safe only after the old one is gone, since the epoch of the new process fences the old.

## Listeners

| Flag | Default | Purpose |
| --- | --- | --- |
| `--listen` | `127.0.0.1:4437` | The stream protocol listener. |
| `--admin-listen` | `127.0.0.1:9090` | The admin API and dashboard. |
| `--no-admin` | off | Disables the admin listener entirely. |
| `--http-address` | `http://{listen}` | The public URL other nodes redirect clients to. |
| `--backlog` | `1024` | Listener accept queue depth. |
| `--shutdown-drain-sec` | `0` | How long to fail readiness before closing listeners on shutdown. |

`--http-address` matters in any multi-node deployment. It is registered in the metadata state and used verbatim in redirects, so it must be a URL clients can actually reach, not a bind address. The default only works single-node on localhost.

Binding either listener to anything but loopback requires `--auth required`. A node asked to expose an unauthenticated listener refuses to start, unless `--insecure-allow-remote` deliberately opts out for deployments that bring their own network boundary.

`--protocol` is a global flag rather than a serve flag, selecting whether this listener speaks `pico` or `ds`.

## Metadata

`--meta-url` points at the SQL database holding the metadata log. Three forms are accepted.

```bash
--meta-url sqlite::memory:                        # tests, throwaway
--meta-url sqlite:./data/meta.db                  # single node
--meta-url postgres://user:pass@host:5432/picomq  # cluster
```

SQLite is a file, so it works for exactly one node. Every multi-node cluster needs Postgres, and all nodes must point at the same database, which is what makes them one cluster.

## Storage

`--storage` is the data bucket in the form `bucket-id@uri`. The bucket id is the engine's internal identifier and any stable value works, and the URI selects the backend.

```bash
--storage=-2@file://./objects                # local filesystem
--storage=-2@s3://bucket?region=us-east-1    # S3 and compatible stores
```

S3 credentials come from the standard `AWS_*` environment variables. Compatible stores such as MinIO or RustFS take `endpoint` and `pathStyle` parameters on the URI:

```bash
--storage=-2@s3://picomq?region=us-east-1&endpoint=http://rustfs:9000&pathStyle=true
```

`--wal` optionally puts the WAL in its own bucket. When absent the WAL shares the data bucket under the next bucket id, which is the right default unless WAL and data need different storage classes or lifecycle rules.

## Auth

| Flag | Default | Purpose |
| --- | --- | --- |
| `--auth` | `off` | `required` gates every request on both listeners. `off` allows loopback binds only. |
| `--insecure-allow-remote` | off | Permit non-loopback binds with auth off. |
| `--auth-bootstrap-token` | none | Root token in wire form, seeded at startup. Idempotent across restarts. |
| `--auth-bootstrap-token-file` | none | Read the bootstrap token from a file instead, keeping it out of process listings. |

A different token under an already-stored bootstrap id fails startup rather than silently rotating a live credential. Bootstrap, token issuance, and client credentials are covered in [Authentication](/docs/operations/auth).

## Behavior

| Flag | Default | Purpose |
| --- | --- | --- |
| `--routing` | `redirect` | `redirect` sends clients to the owner with `307`. `local` serves everything locally, for single-node setups or a routing proxy in front. |
| `--long-poll-timeout-sec` | `25` | How long a waiting read parks before returning empty. |
| `--sse-max-duration-sec` | `55` | Connection cap for SSE, after which the client reconnects. |
| `--max-chunk-size` | `65536` | Response chunk size for streamed reads. |
| `--max-request-size` | `33554432` | Cap on a single request body. Oversized bodies get `413`. |

The two timeouts default below common proxy idle limits. Raise them only if every intermediary between clients and nodes is known to allow longer idle connections.

## Engine

Four flags override the storage engine defaults: `--wal-cache-size`, `--block-cache-size`, `--wal-upload-threshold`, and `--wal-upload-interval-ms`. What they trade against each other is covered in [Tuning](/docs/operations/tuning).

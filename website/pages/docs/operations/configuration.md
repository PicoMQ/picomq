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
| `--cluster-id` | `picomq` | Reported in the admin API, useful when running several clusters. |
| `--slots` | `1` | Placement weight registered at startup. A node with `4` slots takes four times the streams of a node with `1`. |

The node id is the one value worth care. Each start registers the id at a higher epoch, which fences the process before it: if that one is still running somewhere, it stops. Two machines sharing an id therefore take turns instead of serving together, so give every node its own.

## Listeners

| Flag | Default | Purpose |
| --- | --- | --- |
| `--listen` | `127.0.0.1:4437` | The HTTP stream protocol listener. |
| `--admin-listen` | `127.0.0.1:9090` | The [admin API](/docs/operations/admin), metrics, and [console](/docs/operations/console). |
| `--no-admin` | off | Disables the admin listener entirely. |
| `--http-address` | `http://{listen}`, `https://` with TLS | The public URL other nodes redirect clients to. |
| `--admin-address` | `http://{admin-listen}`, `https://` with TLS | The admin URL other nodes forward admin requests to, see [Any node answers](/docs/operations/admin#any-node-answers). |
| `--kafka-listen` | `127.0.0.1:9092` | The Kafka listener. |
| `--kafka-advertise` | `{kafka-listen}` | The Kafka address registered in metadata and returned to clients. |
| `--backlog` | `1024` | Listener accept queue depth. |
| `--shutdown-drain-sec` | `0` | How long to fail readiness on shutdown before ending live reads and handing off, see [Failover](/docs/design/failover#clean-shutdown). |
| `--shutdown-timeout-sec` | `10` | How long to wait for open connections once the listeners stop accepting. After that the node stops waiting and hands off, so a stalled client cannot hold up the handoff. |

`--protocol` is a global flag rather than a serve flag. It selects the HTTP protocol the stream listener speaks, `pico` or `ds`. Kafka is a separate listener that is always on, covered on the [Kafka protocol](/docs/kafka) page.

`--http-address` and `--kafka-advertise` matter in any multi-node deployment. Both are registered in the metadata state and handed to clients verbatim, in redirects for HTTP and in metadata responses for Kafka, so they must be addresses clients can actually reach, not bind addresses. The defaults only work single-node on localhost.

Binding an unauthenticated listener to anything but loopback fails startup unless `--insecure-allow-remote` deliberately opts out for deployments that bring their own network boundary. `--auth required` is the authenticated alternative for every listener: bearer tokens on HTTP and SASL on Kafka, see [Authentication](/docs/operations/auth).

## Liveness

| Flag | Default | Purpose |
| --- | --- | --- |
| `--heartbeat-interval-sec` | `5` | How often the node heartbeats through the metadata log. |
| `--node-timeout-sec` | `30` | How long a node may stay silent before it is fenced and its streams fail over. |

The timeout must be greater than the interval, and startup refuses anything else. The lease holder counts the timeout in its own heartbeats, so with the defaults a node is fenced after `6` of them pass without one of its own. How this works is in [Failover](/docs/design/failover), and choosing the values in [Tuning](/docs/operations/tuning#failover).

## Metadata

`--meta-url` points at the SQL database holding the metadata log. Three forms are accepted.

```bash
--meta-url sqlite::memory:                        # tests, throwaway
--meta-url sqlite:./data/meta.db                  # single node, the default
--meta-url postgres://user:pass@host:5432/picomq  # cluster, postgresql:// works too
```

SQLite is a file, so it works for exactly one node. Every multi-node cluster needs Postgres, and all nodes must point at the same database, which is what makes them one cluster.

## Storage

`--storage` is the data bucket in the form `bucket-id@uri`, `-2@file://./objects` by default. The bucket id is the engine's internal identifier and any stable value works, and the URI selects the backend.

```bash
--storage=-2@file://./objects                # local filesystem
--storage=-2@s3://bucket?region=us-east-1    # S3 and compatible stores
```

S3 credentials come from the standard `AWS_*` environment variables. Compatible stores such as MinIO or RustFS take `endpoint` and `pathStyle` parameters on the URI:

```bash
--storage=-2@s3://picomq?region=us-east-1&endpoint=http://rustfs:9000&pathStyle=true
```

`--wal` selects where the write-ahead log lives.

| `--wal` | WAL location |
| --- | --- |
| absent | The data bucket, under the next bucket id |
| `id@s3://...` | Its own bucket, for a different storage class or lifecycle rules |

When a node dies, another node takes over its WAL, so every node keeps its WAL in object storage the other nodes can reach. A `file://` WAL can only be taken over on the same host, so it suits a single node. See [Write-ahead log](/docs/design/wal#takeover).

### S3 Express One Zone for the WAL

The WAL bucket can be an [S3 Express One Zone](https://aws.amazon.com/s3/storage-classes/express-one-zone/) directory bucket while the data bucket stays on standard S3. Express PUTs complete in single-digit milliseconds, so this cuts append acknowledgment latency without changing the data bucket's economics.

A directory bucket is detected automatically from AWS's reserved name suffix (`{base}--{zone-id}--x-s3`), which switches the client to Express session authentication. The `s3Express=true` URI parameter forces it explicitly.

```bash
--storage=-2@s3://picomq-data?region=us-east-1 \
--wal=-4@s3://picomq-wal--use1-az4--x-s3?region=us-east-1&batchInterval=15
```

Three things make this setup effective:

- **Lower `batchInterval`.** The default 250 ms batch window exists to amortize standard S3 request costs. Express PUTs are faster and cheaper, so a 10 to 25 ms window turns the latency gain into client-visible acknowledgment latency.
- **Run the node in the bucket's availability zone.** The zone is encoded in the bucket name. Cross-zone access works but gives most of the latency win back.
- **Keep the Express residency short.** `--wal-upload-threshold` and `--wal-upload-interval-ms` control how quickly WAL data compacts out to the data bucket. Express is single-zone storage, so acknowledged records carry that exposure until they land in the standard bucket.

## Auth

| Flag | Default | Purpose |
| --- | --- | --- |
| `--auth` | `off` | `required` gates every request on the stream and admin listeners and every Kafka connection. `off` allows loopback binds only. |
| `--insecure-allow-remote` | off | Permit non-loopback binds with auth off, when something else guards the network. |
| `--auth-bootstrap-token` | none | Root token in wire form, seeded at startup. Idempotent across restarts. |
| `--auth-bootstrap-token-file` | none | Read the bootstrap token from a file instead, keeping it out of process listings. Not with `--auth-bootstrap-token`. |

A different token under an already-stored bootstrap id fails startup rather than silently rotating a live credential. Bootstrap, token issuance, and client credentials are covered in [Authentication](/docs/operations/auth).

## TLS

| Flag | Default | Purpose |
| --- | --- | --- |
| `--tls-certificate` | none | PEM certificate chain for every listener. Turns on TLS. Needs `--tls-key`. |
| `--tls-key` | none | PEM private key for the certificate. |
| `--tls-ca-certificate` | none | PEM file of CA certificates this node trusts, on top of the system's, when it calls another node's admin listener. Needed when node certificates come from a private CA. |
| `--tls-handshake-timeout-sec` | `10` | How long a client has to finish its TLS handshake. |

Certificate names, rotation, and client trust are covered in [TLS](/docs/operations/tls).

## Behavior

| Flag | Default | Purpose |
| --- | --- | --- |
| `--routing` | `redirect` | `redirect` sends clients to the owner with `307`. `local` serves everything locally, for single-node setups or a routing proxy in front. |
| `--long-poll-timeout-sec` | `25` | How long a waiting read parks before returning empty. |
| `--sse-max-duration-sec` | `55` | Connection cap for SSE, after which the client reconnects. |
| `--max-chunk-size` | `65536` | Response chunk size for streamed reads. |
| `--max-request-size` | `33554432` | Cap on a single request body. Oversized bodies get `413`. |
| `--max-groups` | `10000` | Consumer groups held in memory at once. Groups without members are unloaded first. |

The two timeouts default below common proxy idle limits. Raise them only if every intermediary between clients and nodes is known to allow longer idle connections.

## Engine

Four flags override the storage engine defaults: `--wal-cache-size`, `--block-cache-size`, `--wal-upload-threshold`, and `--wal-upload-interval-ms`. What they trade against each other is covered in [Tuning](/docs/operations/tuning).

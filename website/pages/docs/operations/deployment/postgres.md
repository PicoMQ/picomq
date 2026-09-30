# Postgres extension

`pico` runs inside Postgres as a background worker. The worker is the same node that `pico serve` starts: HTTP, Kafka, and admin listeners, the ownership router, the stream service, and the engine. The database holds the metadata log and the [WAL](/docs/design/wal). The only external dependency is an object store.

<div class="pico-diagram">
<svg viewBox="0 0 720 420" width="720" role="img" aria-label="Clients connect to pico's listeners on ports 4437, 9090, and 9092. The pico background worker runs inside the Postgres postmaster next to regular backends, writes metadata and the WAL to the same database over a local socket, and writes data objects to object storage. A standby replicates the database and starts its own worker only on promotion.">
  <defs>
    <marker id="arrpg" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M0 0.5 L7.5 4 L0 7.5 Z" class="arrow"/>
    </marker>
  </defs>
  <rect x="20" y="150" width="120" height="72" class="box"/>
  <text x="80" y="182" text-anchor="middle" class="label">clients</text>
  <text x="80" y="200" text-anchor="middle" class="sub">HTTP or Kafka</text>
  <rect x="200" y="20" width="320" height="380" class="edge-soft"/>
  <text x="210" y="38" class="sub">postgres primary</text>
  <rect x="220" y="50" width="280" height="56" class="box"/>
  <text x="360" y="74" text-anchor="middle" class="label">backends</text>
  <text x="360" y="92" text-anchor="middle" class="sub">SQL clients on 5432</text>
  <rect x="220" y="130" width="280" height="120" class="box-accent"/>
  <text x="360" y="154" text-anchor="middle" class="label">pico worker</text>
  <text x="360" y="174" text-anchor="middle" class="sub">listeners 4437, 9090, 9092</text>
  <text x="360" y="192" text-anchor="middle" class="sub">router, stream service</text>
  <text x="360" y="210" text-anchor="middle" class="sub">s3stream engine</text>
  <text x="360" y="234" text-anchor="middle" class="sub">tokio runtime, pico.threads</text>
  <rect x="220" y="300" width="280" height="76" class="box"/>
  <text x="360" y="326" text-anchor="middle" class="label">pico.database</text>
  <text x="360" y="344" text-anchor="middle" class="sub">metadata log</text>
  <text x="360" y="360" text-anchor="middle" class="sub">WAL tables</text>
  <rect x="560" y="150" width="140" height="72" class="box"/>
  <text x="630" y="182" text-anchor="middle" class="label">object storage</text>
  <text x="630" y="200" text-anchor="middle" class="sub">data objects</text>
  <rect x="560" y="300" width="140" height="76" class="box"/>
  <text x="630" y="326" text-anchor="middle" class="label">standby</text>
  <text x="630" y="344" text-anchor="middle" class="sub">streaming replica</text>
  <text x="630" y="360" text-anchor="middle" class="sub">worker idle</text>
  <path d="M140 186 L212 186" class="edge" marker-end="url(#arrpg)"/>
  <path d="M360 250 L360 292" class="edge" marker-end="url(#arrpg)"/>
  <path d="M500 186 L552 186" class="edge" marker-end="url(#arrpg)"/>
  <path d="M500 338 L552 338" class="edge-soft" marker-end="url(#arrpg)"/>
  <text x="372" y="276" class="sub">unix socket</text>
  <text x="526" y="330" text-anchor="middle" class="sub">wal</text>
</svg>
</div>

| | `pico serve` next to Postgres | Extension |
| --- | --- | --- |
| Processes to run | Postgres, pico | Postgres |
| WAL write | Network round trip to Postgres or object store | Local commit |
| Memory and threads | Own host or container | Added to the Postgres host, sized with `pico.threads`, `pico.wal_cache_mb`, `pico.block_cache_mb` |
| Client ports | 4437, 9090, 9092 | Same. The extension does not speak the Postgres wire protocol |
| Configuration | Flags and `PICO_*` variables | `pico.*` parameters in `postgresql.conf` |

## Install

Releases ship the extension for Postgres 16, 17, and 18 on `linux/amd64`, built against glibc 2.34 (Debian 12, Ubuntu 22.04, RHEL 9 and newer).

| Artifact | Name | Contents |
| --- | --- | --- |
| Image | `ghcr.io/picomq/picomq-pg:pg{major}` | `postgres:{major}-bookworm` with the extension installed and preloaded. Also tagged `{version}-pg{major}` and `sha-{commit}-pg{major}`. |
| Tarball | `pico-{version}-pg{major}-linux-amd64.tar.gz` | `lib/pico.so`, `extension/pico.control`, `extension/pico--{version}.sql` |

### Into an existing Postgres image

Copy the files out of the published image. The Postgres major must match in both stages.

```dockerfile
FROM ghcr.io/picomq/picomq-pg:pg17 AS pico

FROM postgres:17
COPY --from=pico /usr/lib/postgresql/17/lib/pico.so /usr/lib/postgresql/17/lib/
COPY --from=pico /usr/share/postgresql/17/extension/pico* /usr/share/postgresql/17/extension/
RUN echo "shared_preload_libraries = 'pico'" >> /usr/share/postgresql/postgresql.conf.sample
```

### The published image

Takes every `POSTGRES_*` variable the official image takes.

```bash
docker run -e POSTGRES_PASSWORD=secret \
    -e AWS_ACCESS_KEY_ID=... -e AWS_SECRET_ACCESS_KEY=... \
    -p 5432:5432 -p 4437:4437 -p 9092:9092 \
    ghcr.io/picomq/picomq-pg:pg17 \
    postgres -c "pico.storage=-2@s3://picomq?region=us-east-1"
```

### On a host

```bash
tar -xzf pico-0.1.1-pg17-linux-amd64.tar.gz
sudo install -m 755 lib/pico.so "$(pg_config --pkglibdir)/"
sudo install -m 644 extension/* "$(pg_config --sharedir)/extension/"
```

```ini
# postgresql.conf
shared_preload_libraries = 'pico'
pico.storage = '-2@s3://picomq?region=us-east-1'
```

Restart Postgres. The worker logs `pico: serving on 127.0.0.1:4437` once recovery has finished.

- `CREATE EXTENSION pico` is not required. The preload starts the worker.
- `pico.storage` is the only setting without a default. Without it the worker logs `pico: not starting` and exits. Postgres is unaffected.
- Object store credentials come from the `AWS_*` environment of the Postgres process.

## Settings

All settings have `postmaster` context. A change takes a restart. They are set in `postgresql.conf`, with `ALTER SYSTEM`, or as `-c` flags. The flag column is the `pico serve` equivalent, documented in [Configuration](/docs/operations/configuration).

| Setting | Default | Flag | Purpose |
| --- | --- | --- | --- |
| `pico.storage` | none, required | `--storage` | Object storage bucket URI for data. |
| `pico.wal` | empty | `--wal` | WAL location. Empty keeps the WAL in `pico.database`. Superuser only. |
| `pico.database` | `postgres` | `--meta-url` | Database holding the metadata log and the WAL. |
| `pico.role` | server OS user | | Role the worker connects as. |
| `pico.database_url` | empty | `--meta-url` | Full connection URL, replacing `pico.database` and `pico.role`. Superuser only. |
| `pico.listen` | `127.0.0.1:4437` | `--listen` | HTTP stream listener. |
| `pico.admin_listen` | `127.0.0.1:9090` | `--admin-listen` | Admin API and dashboard. Empty disables it. |
| `pico.kafka_listen` | `127.0.0.1:9092` | `--kafka-listen` | Kafka listener. Empty disables it. |
| `pico.advertised_url` | `http://{listen}` | `--http-address` | URL other nodes redirect clients to. |
| `pico.kafka_advertise` | `{kafka_listen}` | `--kafka-advertise` | Address returned in Kafka metadata. |
| `pico.cluster_id` | `picomq` | `--cluster-id` | Cluster identifier. |
| `pico.node_id` | `1` | `--node-id` | Node identity, unique per node. |
| `pico.auth` | `off` | `--auth` | `off` or `required`. |
| `pico.auth_bootstrap_token` | empty | `--auth-bootstrap-token` | Root token seeded at startup. Superuser only. |
| `pico.insecure_allow_remote` | `off` | `--insecure-allow-remote` | Permit non-loopback listeners with auth off. |
| `pico.schema_registry` | empty | `--schema-registry` | Bucket URI for the schema registry. |
| `pico.wal_cache_mb` | `64` | `--wal-cache-size` | Memory for records not yet packed into objects. Minimum 16. |
| `pico.wal_upload_interval_ms` | `0` | `--wal-upload-interval-ms` | Upload buffered records at least this often. `0` uploads by size only. |
| `pico.block_cache_mb` | `64` | `--block-cache-size` | Memory for cached object blocks. Minimum 16. |
| `pico.threads` | `2` | | Runtime worker threads. |
| `pico.log` | `warn` | `RUST_LOG` | Tracing filter for pico's log lines. |

- The upload threshold is not a setting. It is `2/5` of `pico.wal_cache_mb`, the ratio the engine clamps to.
- The worker connects over the first entry of `unix_socket_directories`, as the server's OS user unless `pico.role` is set, under the same `pg_hba.conf` rules as any local connection.
- A server with no filesystem socket needs `pico.database_url`.

## Network

Listener rules are the same as for a process node, see [Authentication](/docs/operations/auth).

| Bind | Requirement |
| --- | --- |
| Loopback | None |
| Any other address, HTTP listeners | `pico.auth = required` with `pico.auth_bootstrap_token`, or `pico.insecure_allow_remote = on` |
| Any other address, Kafka listener | `pico.insecure_allow_remote = on`. Kafka has no authentication |
| Clients off the Postgres host | `pico.advertised_url` and `pico.kafka_advertise` set to addresses clients can reach |

```ini
pico.listen = '0.0.0.0:4437'
pico.kafka_listen = '0.0.0.0:9092'
pico.advertised_url = 'http://db.internal:4437'
pico.kafka_advertise = 'db.internal:9092'
pico.auth = 'required'
pico.auth_bootstrap_token = 'pk_...'
```

## Lifecycle

| Event | Worker |
| --- | --- |
| Primary starts | Starts after startup recovery |
| Standby starts | Library loaded, worker not started |
| Standby promoted | Starts with the replicated metadata and WAL. See [Write-ahead log](/docs/design/wal#recovery) |
| `pico.storage` unset | Logs `pico: not starting`, exits, not restarted |
| Start fails | Logs the error, Postgres restarts it after 5 seconds |
| `SIGTERM` during start | Exits, not restarted |
| Fast or immediate shutdown | Closes listeners, engine gets 2 seconds to finish in-flight work, unacknowledged records recovered from the WAL on next start |
| Smart shutdown | Waits for the worker as for a client session. Does not complete while pico runs |

The worker is not a session and does not appear in `pg_stat_activity`. Its log lines go to the Postgres log with a `pico:` prefix, filtered by `pico.log`.

## Multiple nodes

Nodes form a cluster by sharing a metadata database and a bucket. Two Postgres servers each running the extension against their own database are two clusters. To join a second node to the first:

- `pico.database_url` pointing at the first server's database
- a distinct `pico.node_id`
- a reachable `pico.advertised_url`

Its WAL lives in that database too unless `pico.wal` points elsewhere. `pico serve` process nodes join the same way.

## Compose

`harness/aio/compose.extension.yml` runs the extension on a primary with a streaming standby and RustFS. The standby exposes port 4438, which answers only after a promotion.

```bash
cd harness/aio
docker compose -f compose.extension.yml up --build
```

## Build from source

The extension is the `picomq-pg` crate at `picomq/pico-pg`, built with [pgrx](https://github.com/pgcentralfoundation/pgrx). It is outside the main workspace because pgrx pins its own build settings.

```bash
cargo install --locked cargo-pgrx --version 0.16.1
cargo pgrx init --pg17 "$(which pg_config)"
cd picomq/pico-pg
cargo pgrx install --release --no-default-features --features pg17
```

| Command | Output |
| --- | --- |
| `cargo pgrx install` | `pico.so` and control files installed into the server `pg_config` points at |
| `cargo pgrx package` | Same tree under `target/release/pico-pg17/` |
| `docker buildx build --target package` | `lib/` and `extension/` only, see below |

```bash
docker buildx build -f picomq/pico-pg/Dockerfile --build-arg PG_MAJOR=17 \
    --target package --output type=local,dest=out .
```

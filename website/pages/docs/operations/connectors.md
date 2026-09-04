# Connectors runtime

`pico-connectors` is the process that hosts connector plugins. It is deployed next to a PicoMQ node or cluster, talks to it over the Kafka listener, and needs nothing from the node beyond a bootstrap address.

This page covers running it: the image, configuration, the state volume, the HTTP API, metrics, and the shape of a healthy deployment. What connectors do is covered in the [Connectors](/docs/connectors/) section.

<div class="pico-diagram">
<svg viewBox="0 30 720 240" width="720" role="img" aria-label="A pico-connectors container with a config directory mounted read-only, a state volume mounted read-write, plugins under /usr/local/lib, an HTTP API on 8081, and a Kafka connection to a PicoMQ node on 9092.">
  <defs>
    <marker id="arrops" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M0 0.5 L7.5 4 L0 7.5 Z" class="arrow"/>
    </marker>
  </defs>
  <rect x="200" y="50" width="320" height="210" fill="none" class="edge-soft" stroke-dasharray="4 4"/>
  <text x="360" y="70" text-anchor="middle" class="sub">pico-connectors container</text>
  <rect x="220" y="90" width="130" height="44" class="box"/>
  <text x="285" y="109" text-anchor="middle" class="label">runtime</text>
  <text x="285" y="126" text-anchor="middle" class="sub">/usr/local/bin</text>
  <rect x="370" y="90" width="130" height="44" class="box"/>
  <text x="435" y="109" text-anchor="middle" class="label">plugins</text>
  <text x="435" y="126" text-anchor="middle" class="sub">/usr/local/lib</text>
  <rect x="220" y="160" width="130" height="44" class="box-accent"/>
  <text x="285" y="179" text-anchor="middle" class="label">definitions</text>
  <text x="285" y="196" text-anchor="middle" class="sub">TOML, read-only</text>
  <rect x="370" y="160" width="130" height="44" class="box-accent"/>
  <text x="435" y="179" text-anchor="middle" class="label">state</text>
  <text x="435" y="196" text-anchor="middle" class="sub">checkpoints</text>
  <rect x="20" y="150" width="130" height="64" class="box"/>
  <text x="85" y="176" text-anchor="middle" class="label">./connectors</text>
  <text x="85" y="194" text-anchor="middle" class="sub">*.toml, read-only</text>
  <path d="M150 182 L212 182" class="edge" marker-end="url(#arrops)"/>
  <rect x="20" y="60" width="130" height="64" class="box"/>
  <text x="85" y="86" text-anchor="middle" class="label">operator</text>
  <text x="85" y="104" text-anchor="middle" class="sub">HTTP :8081</text>
  <path d="M150 92 L212 106" class="edge" marker-start="url(#arrops)" marker-end="url(#arrops)"/>
  <rect x="570" y="60" width="130" height="64" class="box"/>
  <text x="635" y="86" text-anchor="middle" class="label">PicoMQ node</text>
  <text x="635" y="104" text-anchor="middle" class="sub">Kafka :9092</text>
  <path d="M500 106 L562 92" class="edge" marker-start="url(#arrops)" marker-end="url(#arrops)"/>
  <rect x="570" y="150" width="130" height="64" class="box"/>
  <text x="635" y="176" text-anchor="middle" class="label">volume</text>
  <text x="635" y="194" text-anchor="middle" class="sub">persistent, rw</text>
  <path d="M500 182 L562 182" class="edge" marker-start="url(#arrops)" marker-end="url(#arrops)"/>
</svg>
</div>

## The image

`ghcr.io/picomq/picomq-connectors` contains the runtime and every light plugin. Heavy plugins, the ones with large lakehouse or warehouse dependencies, are released as separate `.so` artifacts attached to each GitHub release and are added to the image by copying them in.

| Path | Content |
| --- | --- |
| `/usr/local/bin/pico-connectors` | The runtime |
| `/usr/local/lib/libpicomq_connector_*.so` | Light plugins |
| `/etc/picomq-connectors/config.toml` | Runtime configuration |
| `/etc/picomq-connectors/connectors/` | Connector definitions, one TOML per connector |
| `/var/lib/picomq-connectors/state/` | Source checkpoints, declared as a volume |

The container exposes `8081` and starts `pico-connectors` with no arguments.

Adding a heavy plugin is a two-line Dockerfile.

```dockerfile
FROM ghcr.io/picomq/picomq-connectors:latest
ADD https://github.com/picomq/picomq/releases/download/v0.7.0/libpicomq_connector_iceberg_sink-linux-amd64.tar.gz /tmp/
RUN tar -xzf /tmp/libpicomq_connector_iceberg_sink-linux-amd64.tar.gz -C /usr/local/lib && rm /tmp/*.tar.gz
```

A plugin built outside this repository installs the same way. Copy the `.so` to `/usr/local/lib` and reference its library name in a definition.

## Running it

The runtime is a single process with no coordination between instances. Run one, or run several with disjoint sets of connectors.

With the repository's compose harness, the overlay stacks on any base file.

```bash
cd harness/aio
docker compose -f compose.lite.yml -f compose.connectors.yml up
```

Standalone, the minimum is a bootstrap address and a directory of definitions.

```bash
docker run --rm -p 8081:8081 \
  -e PICOMQ_CONNECTORS_KAFKA__BOOTSTRAP_SERVERS=pico:9092 \
  -v ./connectors:/etc/picomq-connectors/connectors:ro \
  -v connectors-state:/var/lib/picomq-connectors/state \
  ghcr.io/picomq/picomq-connectors
```

From source, the same thing is a `cargo run`.

```bash
PICOMQ_CONNECTORS_KAFKA__BOOTSTRAP_SERVERS=localhost:9092 \
PICOMQ_CONNECTORS_CONNECTORS__CONFIG_DIR=./harness/aio/connectors \
  cargo run -p picomq-connectors
```

### Startup

On start the runtime does the following in order, and exits on the first failure.

1. Loads `config.toml`, then applies environment overrides.
2. Builds the Kafka client configuration. Connections are made lazily, so a wrong bootstrap address surfaces as produce and fetch errors in the log rather than a refusal to start.
3. Reads every definition from the config directory or the HTTP provider.
4. Resolves each `path` to a shared library and loads it.
5. For each source, loads its checkpoint and calls the plugin's `open()`.
6. For each sink, joins its consumer groups and calls `open()`.
7. Starts the HTTP API.

A definition with `enabled = false` is loaded and shown in the API but not started.

### Plugin path resolution

A definition's `path` can be absolute, or a library name without extension. A name is given the platform extension and searched in order.

| Order | Directory |
| --- | --- |
| 1 | The directory containing the `pico-connectors` executable |
| 2 | The working directory |
| 3 | `/usr/lib`, `/usr/lib64`, `/lib`, `/lib64` |
| 4 | `/usr/local/lib`, `/usr/local/lib64` |

Inside the image the plugins are in `/usr/local/lib`. During development, running from the workspace root finds `target/debug/*.so` through the working directory.

## Configuration

The runtime reads `config.toml`, from `PICOMQ_CONNECTORS_CONFIG_PATH` or the built-in defaults, and then merges environment variables over it. Any key can be set from the environment.

| Rule | Example |
| --- | --- |
| Prefix `PICOMQ_CONNECTORS_` | |
| Sections joined with `__` | `[kafka] bootstrap_servers` becomes `PICOMQ_CONNECTORS_KAFKA__BOOTSTRAP_SERVERS` |
| Nested sections likewise | `[state.http] url` becomes `PICOMQ_CONNECTORS_STATE__HTTP__URL` |

The sections that matter most in a deployment.

### `[kafka]`

| Key | Default | Meaning |
| --- | --- | --- |
| `bootstrap_servers` | `localhost:9092` | Any PicoMQ node's Kafka listener |
| `client_id` | `picomq-connectors` | Shown in the node's connection logs |
| `security_protocol` | `plaintext` | `plaintext`, `ssl`, `sasl_plaintext`, `sasl_ssl` |
| `sasl.mechanism`, `sasl.username`, `sasl.password` | | Credentials when SASL is on. A PicoMQ token goes in `password` |
| `tls.ca_file`, `tls.cert_file`, `tls.key_file`, `tls.verify_hostname` | | TLS material when SSL is on |
| `properties` | `{}` | Any other librdkafka setting, applied to every client |

### `[state]`

| Key | Default | Meaning |
| --- | --- | --- |
| `storage` | `file` | `file` or `http` |
| `path` | `local_state` | Directory for `file` storage. One file per source, named by key |
| `http.url` | | Endpoint for `http` storage. The source key is appended |
| `http.load_method`, `http.save_method` | `get`, `put` | HTTP verbs used |
| `http.timeout` | `5s` | Per request |
| `http.retry.*` | 4 attempts, 200 ms to 2 s | Backoff for saves |

Only sources use the state store. Sinks keep their position in Kafka consumer groups on the node, so a deployment with no sources needs no volume.

### `[connectors]`

| Key | Default | Meaning |
| --- | --- | --- |
| `config_type` | `local` | `local` reads TOML files, `http` fetches definitions from a service |
| `config_dir` | | Directory of definitions for `local` |
| `http.base_url`, `http.url_templates`, `http.request_headers`, `http.response`, `http.retry` | | Where and how to fetch for `http` |

The HTTP provider is for control planes that generate definitions. It fetches a list of sinks and sources at startup and answers the API's create, update and delete calls by forwarding them.

### `[http]`

| Key | Default | Meaning |
| --- | --- | --- |
| `enabled` | `true` | Serve the API |
| `address` | `127.0.0.1:8081` | The image sets `0.0.0.0:8081` |
| `api_key` | empty | When set, every request must carry it in an `api-key` header |
| `metrics.enabled`, `metrics.endpoint` | `false`, `/metrics` | Prometheus exposition |
| `cors.*`, `tls.*` | off | Browser access and TLS for the API itself |

### `[logging]` and `[telemetry]`

`logging.format` is `text` or `json`. Level comes from `RUST_LOG`, default `info`.

`telemetry.enabled` turns on OTLP export of logs and traces to `telemetry.logs.endpoint` and `telemetry.traces.endpoint`, `grpc` or `http` transport, under `telemetry.service_name`.

## Definitions

Each connector is one TOML file. The [Sources](/docs/connectors/sources#definition) and [Sinks](/docs/connectors/sinks#definition) pages describe the fields. Operationally, two things about definitions matter.

### Overrides from the environment

Any field of a definition, including its `plugin_config`, can be overridden from the environment. This is how credentials reach a container without being written into a mounted file.

| Target | Variable |
| --- | --- |
| A top-level field of the sink with key `orders_pg` | `PICOMQ_CONNECTORS_SINK_ORDERS_PG_<FIELD>` |
| A `plugin_config` field of that sink | `PICOMQ_CONNECTORS_SINK_ORDERS_PG_PLUGIN_CONFIG_<FIELD>` |
| The same for a source | `PICOMQ_CONNECTORS_SOURCE_<KEY>_...` |

```bash
PICOMQ_CONNECTORS_SINK_ORDERS_PG_PLUGIN_CONFIG_CONNECTION_STRING=postgres://user:secret@db/app
PICOMQ_CONNECTORS_SINK_ORDERS_PG_ENABLED=false
```

Values are parsed as JSON when they look like it, so `true`, `42` and `["a","b"]` take their natural types, and anything else is a string.

### Versions

A definition carries a `version`. The API keeps every version it has seen for a key and marks one active, so a bad update can be rolled back by activating the previous version and restarting the connector. On disk, the runtime writes new versions next to the original file.

## HTTP API

The API is on `[http].address`, `8081` in the image. Every response is JSON. When `api_key` is set, send it as an `api-key` header.

| Method and path | What it does |
| --- | --- |
| `GET /` | Banner |
| `GET /stats` | Process stats: memory, CPU, uptime, per-connector counters |
| `GET /metrics` | Prometheus exposition, when enabled |
| `GET /sinks`, `GET /sources` | Every connector with `id`, `key`, `name`, `path`, `enabled`, `status`, `last_error` |
| `GET /sinks/{key}`, `GET /sources/{key}` | One connector with its `topics` blocks |
| `GET /sinks/{key}/transforms`, `GET /sources/{key}/transforms` | The transforms it is running |
| `GET /sinks/{key}/configs`, `GET /sources/{key}/configs` | Every stored version of the definition, with which is active |
| `GET .../configs/{version}` | One version |
| `GET .../configs/plugin` | The active `plugin_config`, secrets redacted |
| `GET .../configs/active`, `PUT .../configs/active` | Read or switch the active version |
| `POST .../configs` | Store a new version |
| `DELETE .../configs` | Remove a stored version |
| `POST /sinks/{key}/restart`, `POST /sources/{key}/restart` | Stop and start with the active version |

`status` is one of the following.

| Status | Meaning |
| --- | --- |
| `starting` | `open()` in progress |
| `running` | Processing |
| `stopping` | Shutting down |
| `stopped` | Not running, either `enabled = false` or after a clean stop |
| `error` | Stopped by a failure. `last_error` has the message and time |

A connector in `error` does not restart itself. Fix the cause and `POST .../restart`, or restart the process.

## Metrics

With `[http.metrics] enabled = true`, `GET /metrics` serves the following.

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `picomq_connectors_sources_total` | gauge | | Sources loaded |
| `picomq_connectors_sources_running` | gauge | | Sources in `running` |
| `picomq_connectors_sinks_total` | gauge | | Sinks loaded |
| `picomq_connectors_sinks_running` | gauge | | Sinks in `running` |
| `picomq_connector_messages_produced_total` | counter | `connector_key` | Records a source's `poll()` returned |
| `picomq_connector_messages_sent_total` | counter | `connector_key` | Records acknowledged by the broker |
| `picomq_connector_messages_consumed_total` | counter | `connector_key` | Records fetched for a sink |
| `picomq_connector_messages_processed_total` | counter | `connector_key` | Records a sink's `consume()` accepted |
| `picomq_connector_messages_filtered_total` | counter | `connector_key` | Records dropped by transforms |
| `picomq_connector_errors_total` | counter | `connector_key`, `connector_type` | Failures of any kind |
| `picomq_connector_stage_duration_seconds` | histogram | `connector_key`, `connector_type`, `stage` | Time per stage. Sinks: `decode`, `prepare`, `ffi`, `total`. Sources: `decode`, `prepare`, `broker_send`, `state_save`, `total` |

Alerts that catch most real problems.

- `sources_running < sources_total` or `sinks_running < sinks_total` for more than a minute. Something is in `error`.
- `rate(errors_total[5m]) > 0` on a connector. Retries are happening even if it has not stopped.
- `consumed_total - processed_total` growing. A sink is being handed batches it is not accepting.
- Consumer group lag on the node side for `picomq-connect-sink-*` groups. The runtime does not measure lag itself.

## State volume

Source checkpoints live under `[state].path`. Losing them is not data loss, since the source re-reads from whatever its plugin considers the beginning, but for a CDC source that can mean replaying a table.

- Mount a persistent volume at `/var/lib/picomq-connectors/state`.
- Back it up like any small stateful directory. Files are a few kilobytes each.
- Do not share one volume between two runtimes running the same source key. Each save is a rename, so they will not corrupt each other, but they will silently overwrite each other's progress.

`storage = "http"` moves checkpoints to a service you run. The runtime sends an idempotency key with each save and holds back the next batch until an ambiguous save is resolved, so a flaky store slows a source down rather than losing its place.

## Scaling and placement

| Situation | Approach |
| --- | --- |
| More throughput on one sink | Split its topics across two definitions with distinct keys. One consumer owns a whole topic, so two runtimes with the same key do not share load |
| Many connectors | Several runtimes, each with its own config directory. There is no coordination and no shared state between them |
| Isolation of a heavy plugin | Its own runtime, so a panic in it does not take down unrelated connectors |
| Sources with checkpoints | Pin to a node or use a network volume, or use HTTP state storage |
| Restart policy | `unless-stopped` or equivalent. A plugin panic aborts the process, and the process is designed to resume cleanly |

## Upgrading

The runtime and its plugins are built together and share the SDK version. Upgrade them together by pulling a new image. A plugin `.so` from a different release than the runtime fails to load with a symbol error rather than misbehaving.

Consumer group offsets and checkpoint files survive upgrades unchanged. A rolling upgrade is stop the old process, start the new one, and both sides resume where they were.

## Troubleshooting

| Symptom | Likely cause |
| --- | --- |
| `Plugin library not found. Searched paths:` at startup | `path` does not match a file on the search path. The message lists everywhere it looked |
| Exits with a Kafka metadata error | `bootstrap_servers` is wrong or the node's Kafka listener is not reachable from the container |
| Sink shows `running`, `consumed_total` is zero | Its topics do not exist yet, or the pattern does not match them. Patterns are anchored at the start |
| Sink shows `running`, records land twice | Expected after a crash. See [Delivery guarantees](/docs/connectors/delivery) |
| Source in `error` with thirty nacks in the log | Produce has been failing. Broker down, topic creation refused, or a routing rule with no `fallback` hitting records without the field |
| Source restarts from the beginning | The state volume was not mounted, or the key changed. State is filed by key |
| Process exits with a panic message naming a plugin | A plugin bug. The runtime restarts cleanly under a supervisor, and the connector resumes from its checkpoint |

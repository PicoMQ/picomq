# InfluxDB sink

Writes each record as one line protocol point in an InfluxDB measurement. The measurement can be fixed or derived from the topic. Every point is tagged with `offset`, and by default `topic` and `partition`, so a replayed batch rewrites the same points and no duplicate series appears.

The sink speaks to InfluxDB 2 through `/api/v2/write` and to InfluxDB 3 through `/api/v3/write_lp`. The line protocol is the same in both, only the endpoint, the authentication scheme and the required keys differ.

| | |
| --- | --- |
| Type | Sink |
| Library | `libpicomq_connector_influxdb_sink` |
| Ships in | The `pico-connectors` image |
| Destination | Measurement, templated per topic, inside one `bucket` (v2) or `db` (v3) |
| Creates destination | Not needed, the first write creates the measurement |
| On replay | No duplicates with the default tags |
| Payload | Any schema. Stored as a string field, `payload_json`, `payload_text` or `payload_base64` |

<div class="pico-diagram">
<svg viewBox="0 30 720 170" width="720" role="img" aria-label="Records from a topic are batched by the runtime, handed to the InfluxDB sink, resolved to a measurement name from the template, encoded as line protocol and posted to the write endpoint with retries on 429 and 5xx.">
  <defs>
    <marker id="arrinf" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M0 0.5 L7.5 4 L0 7.5 Z" class="arrow"/>
    </marker>
  </defs>
  <rect x="20" y="80" width="130" height="56" class="box-accent"/>
  <text x="85" y="104" text-anchor="middle" class="label">sensors.temp</text>
  <text x="85" y="122" text-anchor="middle" class="sub">batch of 1000</text>
  <rect x="210" y="80" width="170" height="56" class="box"/>
  <text x="295" y="104" text-anchor="middle" class="label">resolve measurement</text>
  <text x="295" y="122" text-anchor="middle" class="sub">m_{segment[-1]}</text>
  <rect x="420" y="80" width="130" height="56" class="box"/>
  <text x="485" y="104" text-anchor="middle" class="label">POST write</text>
  <text x="485" y="122" text-anchor="middle" class="sub">line protocol</text>
  <rect x="600" y="80" width="110" height="56" class="box-accent"/>
  <text x="655" y="104" text-anchor="middle" class="label">measurement</text>
  <text x="655" y="122" text-anchor="middle" class="sub">m_temp</text>
  <path d="M150 108 L202 108" class="edge" marker-end="url(#arrinf)"/>
  <path d="M380 108 L412 108" class="edge" marker-end="url(#arrinf)"/>
  <path d="M550 108 L592 108" class="edge" marker-end="url(#arrinf)"/>
  <text x="360" y="160" text-anchor="middle" class="sub">up to batch_size lines per request, 429 and 5xx retried with backoff</text>
</svg>
</div>

## Quick start

InfluxDB 2, one measurement per topic.

```toml
type = "sink"
key = "sensors_influx"
enabled = true
version = 0
name = "Sensors to InfluxDB"
path = "libpicomq_connector_influxdb_sink"

[[topics]]
pattern = "sensors\\..*"
schema = "json"
batch_length = 1000
poll_interval = "100ms"

[plugin_config]
url = "http://influxdb:8086"
org = "acme"
bucket = "sensors"
token = "my-token"
measurement = "m_{topic_segment[-1]}"
precision = "ms"
```

InfluxDB 3 replaces `org` and `bucket` with `db` and sets `version`.

```toml
[plugin_config]
version = "v3"
url = "http://influxdb:8181"
db = "sensors"
token = "my-token"
measurement = "m_{topic_segment[-1]}"
```

Keep the token out of the file with an environment override.

```bash
PICOMQ_CONNECTORS_SINK_SENSORS_INFLUX_PLUGIN_CONFIG_TOKEN=my-token
```

## How it works

On `open()` the sink checks that `precision` is one of `ns`, `us`, `ms`, `s`, that `org` and `bucket` are non-empty for v2, and that `db` is non-empty for v3. It builds an HTTP client with `timeout` and calls `/health`, retrying up to `max_open_retries` times with an exponential backoff from `retry_delay` capped at `open_retry_max_delay`. It then wraps the client in a retry layer and builds the write URL with the query parameters for the chosen version.

For each batch the runtime hands over, the sink does the following.

1. Refuses the batch with `Circuit breaker is open` when the breaker has tripped and `circuit_breaker_cool_down` has not passed. Nothing is written.
2. Splits the batch into chunks of `batch_size` points.
3. Resolves `measurement` against the topic name and encodes each record as one line, escaping the measurement and tag values as line protocol requires. A record whose payload cannot be encoded fails its chunk before anything is sent.
4. Posts the chunk as `text/plain` with `Authorization: Token <token>` for v2 or `Authorization: Bearer <token>` for v3.
5. Treats any `2xx` as success. A `429` or `5xx` that outlasts the retries is a transient failure, any other `4xx` is a permanent one.
6. Attempts every chunk, then returns the first error if any chunk failed. The runtime holds the offset and redelivers the whole batch.

The retry layer makes up to `max_retries` attempts per request. It retries network errors, `429` and every `5xx`, waits for `Retry-After` when a `429` carries one, and otherwise backs off exponentially from `retry_delay` capped at `retry_max_delay` with 20 percent jitter. Other status codes are returned at once.

The circuit breaker counts consecutive `consume()` calls that ended in a transient failure. After `circuit_breaker_threshold` of them it opens for `circuit_breaker_cool_down`, during which every batch fails immediately without a request. A permanent `4xx` does not count towards the threshold. Any batch with at least one successful chunk resets the count.

## Configuration

All keys go under `[plugin_config]`. Unknown keys are rejected, so a typo stops the sink from loading.

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `version` | string | `v2` | `v2` or `v3`. Selects the endpoint, the token scheme and which keys below are required |
| `url` | string | required | InfluxDB base URL. A trailing slash is trimmed |
| `org` | string | required for v2 | Organisation name or id. Must not be blank |
| `bucket` | string | required for v2 | Target bucket. Must not be blank |
| `db` | string | required for v3 | Target database. Must not be blank |
| `token` | string | required | API token. Redacted in the API |
| `measurement` | template | `picomq_messages` | Measurement name. Supports `{topic}` and `{topic_segment[n]}`, see [templating](/docs/connectors/routing#destination-templates-on-sinks) |
| `precision` | string | `ms` | Timestamp unit sent to InfluxDB. `ns`, `us`, `ms` or `s`. Record timestamps are converted from milliseconds |
| `payload_format` | string | `json` | `json` writes `payload_json`, `text` (alias `utf8`) writes `payload_text`, `base64` (alias `raw`) writes `payload_base64`. Any other value logs a warning and falls back to `json` |
| `include_metadata` | bool | `true` | Add topic and partition, as tags or fields depending on the next two keys |
| `include_topic_tag` | bool | `true` | Topic as the `topic` tag. When off, topic becomes the `pico_topic` field |
| `include_partition_tag` | bool | `true` | Partition as the `partition` tag. When off, partition becomes the `pico_partition` field |
| `include_key` | bool | `true` | Add the `pico_key` field when the record has a key |
| `batch_size` | int | `500` | Lines per write request. Values below 1 become 1 |
| `timeout` | duration | `30s` | HTTP timeout per request |
| `max_retries` | int | `3` | Attempts per request on network errors, `429` and `5xx`. Values below 1 become 1 |
| `retry_delay` | duration | `1s` | First backoff delay, doubled per attempt |
| `retry_max_delay` | duration | `5s` | Backoff cap for write retries |
| `max_open_retries` | int | `10` | Attempts for the health check on `open()` |
| `open_retry_max_delay` | duration | `60s` | Backoff cap for the health check |
| `circuit_breaker_threshold` | int | `5` | Consecutive failed batches before the breaker opens |
| `circuit_breaker_cool_down` | duration | `30s` | How long the breaker stays open |
| `verbose_logging` | bool | `false` | Log every batch at `info` instead of `debug` |

`payload_format = "json"` requires the payload to parse as JSON. A record that does not fails its chunk with `Payload is not valid JSON`, so use it only with `schema = "json"` on the topic or a transform that produces JSON. `text` requires valid UTF-8 in the same way.

## What lands in the measurement

With defaults, a JSON record becomes the following line.

```text
m_temp,topic=sensors.temp,partition=0,offset=4711 pico_offset=4711u,pico_key="azE=",payload_json="{\"celsius\":21.5}" 1756934104118
```

| Element | Present when | Content |
| --- | --- | --- |
| measurement | always | The resolved template, escaped |
| `topic` tag | `include_metadata` and `include_topic_tag` | Topic the record came from |
| `partition` tag | `include_metadata` and `include_partition_tag` | Always `0` on PicoMQ |
| `offset` tag | always | Record offset |
| `pico_offset` field | always | Record offset as an unsigned integer |
| `pico_topic` field | `include_metadata` and not `include_topic_tag` | Topic as a string field |
| `pico_partition` field | `include_metadata` and not `include_partition_tag` | Partition as a signed integer field |
| `pico_key` field | `include_key` and the record has a key | Record key, base64 |
| `payload_json`, `payload_text` or `payload_base64` field | always | The payload as a string, in the encoding `payload_format` selects |
| timestamp | always | Record timestamp converted to `precision`. A record with timestamp `0` gets the current wall-clock time and a `warn` log |

Headers are not stored. Use a [transform](/docs/connectors/transforms) to copy a header into the payload if it is needed.

The payload is a string field, not parsed into fields of its own. Queries that need numeric fields from the payload should apply a transform upstream or use InfluxDB's JSON functions on `payload_json`.

### Measurement names

The resolved name is not rewritten. Commas, spaces, backslashes and newlines are escaped as line protocol requires, and a tab fails the batch. A template of `m_{topic}` with a topic of `sensors-temp` produces a measurement literally named `m_sensors-temp`.

## Replay

The runtime redelivers a batch after a crash between the write and the offset commit. See [Delivery guarantees](/docs/connectors/delivery).

InfluxDB identifies a point by measurement, tag set and timestamp. A second write with the same identity replaces the first.

| Configuration | Result of a replayed batch |
| --- | --- |
| Default tags | Every point has the same `topic`, `partition`, `offset` and timestamp as before. It overwrites itself, no visible change |
| `include_topic_tag = false` | Still no duplicate from a replay, but two topics sharing a measurement collide when they have the same `offset` and timestamp. The later write wins |
| Records with timestamp `0` | Each delivery substitutes a fresh wall-clock time, so a replay writes a second point |

::: warning Topic tag and shared measurements
Keep `include_topic_tag` on whenever more than one topic writes to the same measurement. Without it the only distinguishing tag is `offset`, which restarts at zero in every topic.
:::

## Requirements

- InfluxDB 2 with an API token that can write to `bucket`, or InfluxDB 3 with a token for `db`.
- `/health` must be reachable and return `2xx`, or `open()` gives up after `max_open_retries`.
- Network access from the runtime to InfluxDB. Use an `https://` URL when the token crosses a network.

## Troubleshooting

| Symptom | Cause |
| --- | --- |
| `unknown precision "xx"` at start | `precision` is not one of `ns`, `us`, `ms`, `s` |
| `unknown InfluxDB version "v9"` at start | `version` is not `v2` or `v3` |
| `unknown field` when the definition loads | A key in `plugin_config` that this sink does not know, or a v2 key such as `bucket` in a v3 definition |
| `V2 sink config requires a non-empty 'org'` | `org`, `bucket` or `db` is blank for the selected version |
| `InfluxDB sink health check failed` at start | InfluxDB is not reachable from the container, or `url` points at the wrong port |
| `InfluxDB write failed 401 Unauthorized` | Wrong token, or a v2 token used with `version = "v3"` and the other way round |
| `InfluxDB write failed 404 Not Found` | The write endpoint does not exist on this server. The `version` does not match the InfluxDB major version |
| `Payload is not valid JSON` | `payload_format = "json"` with a non-JSON record. Change the format or fix the upstream schema |
| `Circuit breaker is open` | `circuit_breaker_threshold` consecutive batches failed. Writes resume after `circuit_breaker_cool_down` |

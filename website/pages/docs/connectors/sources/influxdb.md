# InfluxDB source

Polls an InfluxDB query and produces each returned row as a record. The query is yours, with placeholders the source fills in. The source keeps a timestamp cursor and advances it only after the broker acknowledges the batch.

InfluxDB 2.x and 3.x are both supported, selected with `version`. They differ in query language, endpoint and output shape, and the differences are called out throughout this page.

| | |
| --- | --- |
| Type | Source |
| Library | `libpicomq_connector_influxdb_source` |
| Ships in | The `pico-connectors` image |
| Modes | `v2` (Flux, `/api/v2/query`) and `v3` (SQL, `/api/v3/query_sql`) |
| Output schema | `json`, or `text` / `raw` when extracting a single column |
| State | Cursor timestamp, plus same-timestamp bookkeeping |
| On replay | Rows are re-read and re-produced with the same key |

<div class="pico-diagram">
<svg viewBox="0 30 720 170" width="720" role="img" aria-label="Each poll substitutes the cursor and limit into the configured query, runs it against InfluxDB, produces the rows, and on ack moves the cursor to the newest timestamp seen.">
  <defs>
    <marker id="arrinf" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M0 0.5 L7.5 4 L0 7.5 Z" class="arrow"/>
    </marker>
  </defs>
  <rect x="20" y="80" width="150" height="56" class="box"/>
  <text x="95" y="104" text-anchor="middle" class="label">render query</text>
  <text x="95" y="122" text-anchor="middle" class="sub">$cursor, $limit</text>
  <rect x="230" y="80" width="130" height="56" class="box-accent"/>
  <text x="295" y="104" text-anchor="middle" class="label">InfluxDB</text>
  <text x="295" y="122" text-anchor="middle" class="sub">CSV or JSONL rows</text>
  <rect x="420" y="80" width="130" height="56" class="box"/>
  <text x="485" y="104" text-anchor="middle" class="label">produce</text>
  <text x="485" y="122" text-anchor="middle" class="sub">runtime</text>
  <rect x="600" y="80" width="110" height="56" class="box-accent"/>
  <text x="655" y="104" text-anchor="middle" class="label">ack</text>
  <text x="655" y="122" text-anchor="middle" class="sub">advance cursor</text>
  <path d="M170 108 L222 108" class="edge" marker-end="url(#arrinf)"/>
  <path d="M360 108 L412 108" class="edge" marker-end="url(#arrinf)"/>
  <path d="M550 108 L592 108" class="edge" marker-end="url(#arrinf)"/>
  <text x="360" y="160" text-anchor="middle" class="sub">circuit breaker opens after repeated query failures</text>
</svg>
</div>

## Quick start

InfluxDB 2.x with Flux.

```toml
type = "source"
key = "cpu_v2"
enabled = true
version = 0
name = "CPU metrics"
path = "libpicomq_connector_influxdb_source"

[[topics]]
topic = { strategy = "field", path = "row.host", template = "cpu.{value}" }
schema = "json"
create_topics = true

[plugin_config]
version = "v2"
url = "http://influxdb:8086"
org = "myorg"
token = "..."
query = '''
from(bucket: "metrics")
  |> range(start: $cursor)
  |> filter(fn: (r) => r._measurement == "cpu")
  |> limit(n: $limit)
'''
poll_interval = "5s"
```

InfluxDB 3.x with SQL.

```toml
[plugin_config]
version = "v3"
url = "http://influxdb:8181"
db = "metrics"
token = "..."
query = "SELECT * FROM cpu WHERE time > '$cursor' ORDER BY time LIMIT $limit OFFSET $offset"
poll_interval = "5s"
```

```bash
PICOMQ_CONNECTORS_SOURCE_CPU_V2_PLUGIN_CONFIG_TOKEN=...
```

## How it works

`open()` validates the configuration and connects.

- Checks that `query` contains `$cursor`. Without it the cursor could never advance and the same rows would be produced forever, so the source refuses to start.
- On `v3`, checks that `query` contains `$offset` when `stuck_batch_cap_factor` is above zero, and warns about `ORDER BY ... DESC` or `>= $cursor`, both of which break cursor semantics.
- Validates `cursor_field`, `initial_offset` and `payload_format`.
- Connects with up to `max_open_retries` attempts, backing off to `open_retry_max_delay`.

Each `poll()` then does the following.

1. Sleeps `poll_interval`. If the circuit breaker is open, returns an empty batch instead of querying.
2. Substitutes the cursor, `batch_size` and, on `v3`, the offset into `query`.
3. Runs it, retrying transient failures up to `max_retries` times with backoff from `retry_delay` to `retry_max_delay`.
4. Parses the rows, skipping any it already produced at the current cursor timestamp.
5. Builds a record per row and stages the newest timestamp seen as the candidate cursor.

On `Ack` the candidate becomes the committed cursor. On `Nack` it is dropped and the next poll re-runs the same query. A failed query trips one count on the circuit breaker, and `circuit_breaker_threshold` failures open it for `circuit_breaker_cool_down`.

The cursor starts at `initial_offset`, or `1970-01-01T00:00:00Z` when unset, so a new source reads from the beginning of whatever the query returns.

### Rows sharing a timestamp

A timestamp cursor has a hole in it. If more rows share the newest timestamp than fit in one batch, advancing past that timestamp would skip the rest, and not advancing would return the same rows forever. The two versions handle this differently.

| Version | Strategy |
| --- | --- |
| `v2` | Remembers how many rows it has produced at the current cursor and skips that many on the next poll, inflating `$limit` to compensate. The inflation is capped at ten times `batch_size` |
| `v3` | Doubles the batch size on each poll that comes back full with a single timestamp, up to `stuck_batch_cap_factor` times `batch_size`. `$offset` skips rows already produced. Past the cap it advances and logs the rows it could not fetch |

Use a cursor field with enough resolution that this rarely triggers. Nanosecond `time` in InfluxDB usually is.

## Configuration

All keys go under `[plugin_config]`. Unknown keys are rejected, so a typo prevents the connector from loading.

### Both versions

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `version` | string | `v2` | `v2` or `v3` |
| `url` | string | required | Base URL of the server |
| `token` | string | required | API token. Redacted in the API |
| `query` | string | required | Flux (`v2`) or SQL (`v3`) with `$cursor` and `$limit`, plus `$offset` on `v3` |
| `poll_interval` | duration | `5s` | Sleep before each poll |
| `batch_size` | int | `500` | Value substituted for `$limit`. Values below 1 become 1 |
| `cursor_field` | string | `_time` on `v2`, `time` on `v3` | Column holding the RFC 3339 timestamp the cursor follows |
| `initial_offset` | string | none | Starting cursor, RFC 3339. Validated on `open()` |
| `payload_column` | string | none | Emit one column as the whole record |
| `payload_format` | string | `json` | Encoding of `payload_column`. `json`, `text` or `utf8`, `raw` or `base64` |
| `include_metadata` | bool | `true` | See Output below |
| `timeout` | duration | `10s` | Per request |
| `max_retries` | int | `3` | Attempts per query. Values below 1 become 1 |
| `retry_delay` | duration | `1s` | Initial backoff between attempts |
| `retry_max_delay` | duration | `5s` | Backoff ceiling |
| `max_open_retries` | int | `10` | Connection attempts in `open()` |
| `open_retry_max_delay` | duration | `60s` | Backoff ceiling in `open()` |
| `circuit_breaker_threshold` | int | `5` | Consecutive failures that open the breaker |
| `circuit_breaker_cool_down` | duration | `30s` | How long the breaker stays open |
| `verbose_logging` | bool | `false` | Log every poll at `info` |

### `v2` only

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `org` | string | required | Organization, sent as the `org` query parameter |

### `v3` only

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `db` | string | required | Database name |
| `stuck_batch_cap_factor` | int | `10` | Ceiling for batch doubling as a multiple of `batch_size`. `0` disables the mechanism and drops the `$offset` requirement |

### Query placeholders

| Placeholder | Replaced with |
| --- | --- |
| `$cursor` | The current cursor as an RFC 3339 string. Quote it in SQL |
| `$limit` | `batch_size`, or a larger value when catching up on a shared timestamp |
| `$offset` | `v3` only. Rows to skip at the current cursor |

## Output

The two versions return differently shaped rows, and the record reflects that.

### `v2`

Flux returns one row per field value. The record wraps it.

```json
{
  "measurement": "cpu",
  "field": "usage_user",
  "timestamp": "2026-09-03T21:15:04.118Z",
  "value": 12.5,
  "row": {
    "_measurement": "cpu",
    "_field": "usage_user",
    "_time": "2026-09-03T21:15:04.118Z",
    "_value": 12.5,
    "host": "web-1"
  }
}
```

With `include_metadata = false`, `row` keeps only `_time` and `_value`. The four top-level fields are always present.

### `v3`

SQL returns one row per point. The record is the row.

```json
{
  "time": "2026-09-03T21:15:04.118Z",
  "host": "web-1",
  "usage_user": 12.5,
  "usage_system": 3.1
}
```

With `include_metadata = false`, the cursor field is removed from the record.

### Common

| Aspect | Value |
| --- | --- |
| Key | A decimal number derived from the row's timestamp in nanoseconds plus its position in the batch. Stable across replays of the same rows |
| Timestamp | Time the row was read, not the point's own time |
| Headers | None |

Routing rules address fields inside the record, so `path = "row.host"` on `v2` and `path = "host"` on `v3`.

### Single-column payloads

`payload_column` emits one column as the entire record, for tables that already hold serialized messages.

| `payload_format` | Expects | Topic `schema` |
| --- | --- | --- |
| `json` | A string containing JSON on `v2`, any value on `v3` | `json` |
| `text`, `utf8` | A string | `text` |
| `raw`, `base64` | A base64 string | `raw` |

## State

| Stored in the runtime's state store | Stored in InfluxDB |
| --- | --- |
| Cursor timestamp, rows produced, same-timestamp bookkeeping | Nothing |

Losing the state file restarts from `initial_offset` or the epoch and re-produces everything the query can return. Sinks that key on the record identity absorb the repeat.

## Requirements

- InfluxDB 2.x with a token that has read access to the bucket, or InfluxDB 3.x with a token that can query the database.
- Network access from the runtime to the server.
- A query that returns the cursor field on every row. Rows without it are produced but cannot advance the cursor, and a batch with none of them trips the circuit breaker.

## Troubleshooting

| Symptom | Cause |
| --- | --- |
| `query must contain the '$cursor' placeholder` | Add `$cursor` to the range or `WHERE` clause |
| `V3 source query must contain the '$offset' placeholder` | Add `OFFSET $offset`, or set `stuck_batch_cap_factor = 0` |
| `cursor_field "time" is not valid for v2` | Flux exposes the timestamp as `_time`. The reverse applies on `v3` |
| `unknown field` at load | A key is misspelled or belongs to the other version |
| `circuit breaker is OPEN. Skipping poll.` | `circuit_breaker_threshold` consecutive queries failed. Check the server, the breaker closes after `circuit_breaker_cool_down` |
| The same rows every poll | The query ignores `$cursor`, or uses `>=` instead of `>` on `v3` |
| Rows missing after a burst | More rows shared one timestamp than the catch-up mechanism covers. Raise `batch_size` or `stuck_batch_cap_factor` |

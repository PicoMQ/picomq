# ClickHouse sink

Inserts each batch into a ClickHouse table over the HTTP interface, one `INSERT` per batch. The table can be fixed or derived from the topic, and the payload is written as it is, so the columns of the table are the fields of the record. No metadata columns are added.

Every insert carries an `insert_deduplication_token` derived from the topic, partition and offset range of the batch. A replayed batch with the same boundaries is a duplicate block, and ClickHouse drops it on tables that keep a deduplication window.

| | |
| --- | --- |
| Type | Sink |
| Library | `libpicomq_connector_clickhouse_sink` |
| Ships in | The `pico-connectors` image |
| Destination | Table, templated per topic |
| Creates destination | No. The table must exist |
| On replay | Block deduplicated by token when the destination table keeps a deduplication window, rows repeat otherwise |
| Payload | `json` for `json_each_row` and `row_binary`. `text` for `string` |

<div class="pico-diagram">
<svg viewBox="0 30 720 170" width="720" role="img" aria-label="Records from a topic are batched by the runtime, handed to the ClickHouse sink, resolved to a table name from the template, encoded into one request body and written with a single INSERT over HTTP that carries a deduplication token.">
  <defs>
    <marker id="arrchs" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M0 0.5 L7.5 4 L0 7.5 Z" class="arrow"/>
    </marker>
  </defs>
  <rect x="20" y="80" width="130" height="56" class="box-accent"/>
  <text x="85" y="104" text-anchor="middle" class="label">events.eu</text>
  <text x="85" y="122" text-anchor="middle" class="sub">batch of 1000</text>
  <rect x="210" y="80" width="150" height="56" class="box"/>
  <text x="285" y="104" text-anchor="middle" class="label">resolve table</text>
  <text x="285" y="122" text-anchor="middle" class="sub">events_{segment[-1]}</text>
  <rect x="420" y="80" width="130" height="56" class="box"/>
  <text x="485" y="104" text-anchor="middle" class="label">POST /?query=</text>
  <text x="485" y="122" text-anchor="middle" class="sub">INSERT FORMAT</text>
  <rect x="600" y="80" width="110" height="56" class="box-accent"/>
  <text x="655" y="104" text-anchor="middle" class="label">table</text>
  <text x="655" y="122" text-anchor="middle" class="sub">events_eu</text>
  <path d="M150 108 L202 108" class="edge" marker-end="url(#arrchs)"/>
  <path d="M360 108 L412 108" class="edge" marker-end="url(#arrchs)"/>
  <path d="M550 108 L592 108" class="edge" marker-end="url(#arrchs)"/>
  <text x="360" y="160" text-anchor="middle" class="sub">one INSERT per batch, token topic:partition:first-last, retried on 429, 408 and 5xx</text>
</svg>
</div>

## Quick start

```toml
type = "sink"
key = "events_ch"
enabled = true
version = 0
name = "Events to ClickHouse"
path = "libpicomq_connector_clickhouse_sink"

[[topics]]
pattern = "events\\..*"
schema = "json"
batch_length = 1000
poll_interval = "100ms"

[plugin_config]
url = "http://clickhouse:8123"
database = "analytics"
username = "picomq"
password = "secret"
table = "events_{topic_segment[-1]}"
insert_format = "json_each_row"
```

Keep the password out of the file with an environment override.

```bash
PICOMQ_CONNECTORS_SINK_EVENTS_CH_PLUGIN_CONFIG_PASSWORD=secret
```

## How it works

On `open()` the sink builds an HTTP client that sends `X-ClickHouse-User` and `X-ClickHouse-Key` on every request, then calls `GET /ping`. The ping is attempted up to `max_retries` times with a jittered backoff. When `insert_format = "row_binary"` and `table` has no placeholders, the sink also loads the table schema from `system.columns` at this point, so a missing table fails the start rather than the first batch.

For each batch the runtime hands over, the sink does the following.

1. Resolves `table` against the topic name.
2. Builds one request body for the whole batch in the configured `insert_format`. For `row_binary` the schema of the table is fetched on first use and cached for the life of the sink.
3. Computes the deduplication token `topic:partition:first_offset-last_offset`.
4. Sends `POST {url}/?database=<database>&date_time_input_format=best_effort` with the query `INSERT INTO <database>.<table> FORMAT <format>` and the token as query parameters, and the body as `application/octet-stream`.
5. Returns an error if the request fails after retries. The runtime holds the offset and retries the whole batch.

A response of `429`, `408` or any `5xx`, or a network error, is retried until `max_retries` attempts have been made in total. The wait before attempt `n` is a random duration between zero and `retry_delay` times `2^n`, capped at 60 seconds. Any other non-success status fails the batch at once as a permanent error, and a record whose payload type does not match `insert_format` fails the batch before any request is sent.

## Configuration

All keys go under `[plugin_config]`.

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `url` | string | required | Base URL of the HTTP interface, `http://clickhouse:8123` |
| `database` | string | `default` | Database the table lives in. Sent as the `database` query parameter |
| `username` | string | `default` | Sent as `X-ClickHouse-User` |
| `password` | string | empty | Sent as `X-ClickHouse-Key`. Redacted in the API |
| `table` | template | required | Table name. Supports `{topic}` and `{topic_segment[n]}`, see [templating](/docs/connectors/routing#destination-templates-on-sinks) |
| `insert_format` | string | `json_each_row` | `json_each_row`, `row_binary` or `string` |
| `string_format` | string | `json_each_row` | Wire format for `insert_format = "string"`. `json_each_row`, `csv` or `tsv`. Ignored otherwise |
| `timeout_seconds` | int | `30` | Per-request timeout |
| `max_retries` | int | `3` | Total attempts per request, the first one included |
| `retry_delay` | int | `1` | Base delay in seconds for the backoff. A plain integer, not a duration string |
| `verbose_logging` | bool | `false` | Log every batch at `info` instead of `debug` |

### Insert formats

| `insert_format` | Topic `schema` | ClickHouse `FORMAT` | Body |
| --- | --- | --- | --- |
| `json_each_row` | `json` | `JSONEachRow` | Each record serialised to one JSON line |
| `row_binary` | `json` | `RowBinaryWithDefaults` | Each record encoded column by column against the table schema |
| `string` | `text` | `JSONEachRow`, `CSV` or `TSV` per `string_format` | Each record's text appended as is, with a newline added when missing |

`row_binary` avoids the server-side JSON parse and is the fastest path, but it needs the table schema and is strict about it. `string` passes through whatever the producer wrote, so a CSV producer can feed a table directly without a transform.

## What lands in the table

The record is the row. With `json_each_row`, a record `{"id": 7, "tenant": "acme", "total": 42.5}` produces this request.

```sql
INSERT INTO `analytics`.`events_eu` FORMAT JSONEachRow
{"id":7,"tenant":"acme","total":42.5}
```

| Field | Present | Content |
| --- | --- | --- |
| Every field of the payload | always | Mapped to the column of the same name by ClickHouse |
| `pico_topic`, `pico_offset` and the other `pico_*` fields | never | The sink adds no metadata. Use a [transform](/docs/connectors/transforms) to add fields if they are needed |

The record key and headers are not written.

### Table names

The database and the resolved table name are wrapped in backticks with no rewriting, so a template of `events_{topic}` and a topic of `events-eu` produces a table literally named `events_events-eu`, hyphen included. Queries against it need the backticks. Use `{topic_segment[-1]}` with dotted topic names so only the clean tail is used.

### RowBinary encoding

The schema comes from `system.columns` for the database and table, ordered by position. `MATERIALIZED`, `ALIAS` and `EPHEMERAL` columns are skipped. `LowCardinality(T)` is treated as `T`.

| Situation | Result |
| --- | --- |
| Field present | Coerced to the column type. Numbers accept JSON numbers and numeric strings, dates and datetimes accept epoch numbers and `YYYY-MM-DD[THH:MM:SS[.fff]]` strings |
| Field missing, column has a `DEFAULT` | The default marker is written and ClickHouse fills the value |
| Field missing or `null`, column is `Nullable` | `NULL` |
| Field missing or `null`, column is neither | The batch fails with `Invalid record` |
| Payload is not a JSON object | The batch fails with `Invalid record` |
| Column type is `Int128`, `UInt128`, `Int256`, `UInt256`, `JSON`, `Variant` or a geo type | The schema load fails with `Unsupported type` |

Extra fields in the payload that have no column are ignored. `Decimal` up to precision 38, `DateTime64` up to precision 9, `Enum8`, `Enum16`, `UUID`, `IPv4`, `IPv6`, `Array`, `Map` and `Tuple` are supported.

::: warning Schema is cached
The schema of each table is fetched once and held until the sink closes. After an `ALTER TABLE` that adds or reorders columns, restart the sink with `POST /sinks/{key}/restart` or `row_binary` inserts fail with a column count mismatch.
:::

## Replay

The runtime redelivers a batch after a crash between the write and the offset commit. See [Delivery guarantees](/docs/connectors/delivery).

| Destination | Result of a replayed batch |
| --- | --- |
| `ReplicatedMergeTree` family | ClickHouse keeps a window of recent block tokens. The replayed block carries the same `topic:partition:first-last` token and is dropped, so no visible change |
| `MergeTree` family with `non_replicated_deduplication_window > 0` | Same as above |
| Any other engine or setting | The rows are inserted again |

The token only matches when the replayed batch has the same first and last offset as the original. That is the case for a redelivery after a crash, since the offset was never committed. Two batches that overlap without matching boundaries are not deduplicated, so a `ReplacingMergeTree` keyed on a field of the payload is the fallback when exact once matters.

## Requirements

- A ClickHouse release that serves the HTTP interface on `url`, 8123 by default, and accepts the `RowBinaryWithDefaults` format when `row_binary` is used.
- The user needs `INSERT` on the target tables. `row_binary` also needs `SELECT` on `system.columns` for those tables.
- The tables must exist before the first batch. The sink never runs `CREATE TABLE`.
- Network access from the runtime to ClickHouse. Use an `https://` URL for TLS.

## Troubleshooting

| Symptom | Cause |
| --- | --- |
| `Ping failed` at start | Wrong `url`, or ClickHouse is not reachable from the container |
| `Permanent HTTP error` with an authentication failure in the body | Wrong `username` or `password`. The ping at start does not prove the credentials |
| `Table '...' not found in database '...'` | The resolved table does not exist, or the user cannot read `system.columns`. Only raised with `row_binary` |
| `Permanent HTTP error` on every batch with `json_each_row` or `string` | The table does not exist, or a field does not fit its column. The response body from ClickHouse is in the log |
| `Invalid payload type` | The topic `schema` does not match `insert_format`. `json_each_row` and `row_binary` need `json`, `string` needs `text` |
| `Invalid record` with `row_binary` | A payload is not a JSON object, or a non-nullable column without a default is missing from it. The log names the column |
| Rows appear twice | The table has no deduplication window, or the replayed batch had different boundaries |

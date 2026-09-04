# SurrealDB sink

Writes each record as a SurrealDB record over the HTTP `/sql` endpoint. The table can be fixed or derived from the topic, and the sink can define it along with the namespace and database. Record ids are built from `topic`, `partition` and `offset` and every write is an `INSERT IGNORE`, so a replayed batch inserts nothing.

| | |
| --- | --- |
| Type | Sink |
| Library | `libpicomq_connector_surrealdb_sink` |
| Ships in | The `pico-connectors` image |
| Destination | Table, templated per topic, inside one `namespace` and `database` |
| Creates destination | Yes, with `auto_define_table` |
| On replay | No duplicates in any configuration |
| Payload | Any schema. JSON stays an object, text a string, everything else base64 |

<div class="pico-diagram">
<svg viewBox="0 30 720 170" width="720" role="img" aria-label="Records from a topic are batched by the runtime, handed to the SurrealDB sink, resolved to a table name from the template, and written with one INSERT IGNORE statement whose record ids come from topic, partition and offset.">
  <defs>
    <marker id="arrsur" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M0 0.5 L7.5 4 L0 7.5 Z" class="arrow"/>
    </marker>
  </defs>
  <rect x="20" y="80" width="130" height="56" class="box-accent"/>
  <text x="85" y="104" text-anchor="middle" class="label">orders.eu</text>
  <text x="85" y="122" text-anchor="middle" class="sub">batch of 1000</text>
  <rect x="210" y="80" width="160" height="56" class="box"/>
  <text x="290" y="104" text-anchor="middle" class="label">resolve table</text>
  <text x="290" y="122" text-anchor="middle" class="sub">orders_{segment[-1]}</text>
  <rect x="420" y="80" width="130" height="56" class="box"/>
  <text x="485" y="104" text-anchor="middle" class="label">INSERT IGNORE</text>
  <text x="485" y="122" text-anchor="middle" class="sub">id from offset</text>
  <rect x="600" y="80" width="110" height="56" class="box-accent"/>
  <text x="655" y="104" text-anchor="middle" class="label">table</text>
  <text x="655" y="122" text-anchor="middle" class="sub">orders_eu</text>
  <path d="M150 108 L202 108" class="edge" marker-end="url(#arrsur)"/>
  <path d="M370 108 L412 108" class="edge" marker-end="url(#arrsur)"/>
  <path d="M550 108 L592 108" class="edge" marker-end="url(#arrsur)"/>
  <text x="360" y="160" text-anchor="middle" class="sub">up to batch_size records per statement, retried on transient errors</text>
</svg>
</div>

## Quick start

```toml
type = "sink"
key = "orders_surreal"
enabled = true
version = 0
name = "Orders to SurrealDB"
path = "libpicomq_connector_surrealdb_sink"

[[topics]]
pattern = 'orders\..*'
schema = "json"
batch_length = 1000
poll_interval = "100ms"

[plugin_config]
endpoint = "surrealdb:8000"
namespace = "picomq"
database = "app"
table = "orders_{topic_segment[-1]}"
username = "root"
password = "root"
auto_define_table = true
define_indexes = true
```

Keep the password out of the file with an environment override.

```bash
PICOMQ_CONNECTORS_SINK_ORDERS_SURREAL_PLUGIN_CONFIG_PASSWORD=secret
```

## How it works

On `open()` the sink validates the configuration. `auth_scope` and `payload_format` must be known values, `endpoint` must have a host and no path, query or embedded credentials, and `namespace`, `database` and a static `table` must match `[A-Za-z_][A-Za-z0-9_]*`. `auto_define_table` requires `auth_scope = "root"` and `define_indexes` requires `include_metadata = true`, both are rejected otherwise.

It then builds an HTTP client with `query_timeout`, posts the credentials to `/signin` unless `auth_scope` is `none`, and calls `/health`. With `auto_define_table` on it runs `DEFINE NAMESPACE IF NOT EXISTS`, `USE NS` and `DEFINE DATABASE IF NOT EXISTS`, and for a static table `DEFINE TABLE IF NOT EXISTS <table> SCHEMALESS`. With `define_indexes` on it also runs `DEFINE INDEX IF NOT EXISTS <table>_pico_offset_idx ON TABLE <table> FIELDS pico_topic, pico_partition, pico_offset`.

For each batch the runtime hands over, the sink does the following.

1. Resolves `table` against the topic name and rewrites it into a valid identifier.
2. Defines the table if `auto_define_table` is on and this name has not been seen before. The result is cached per name.
3. Builds one record per message with `id`, the metadata fields, the key, the headers, `payload` and `payload_encoding`. A record whose payload cannot be converted is left out, counted as an error, and remembered.
4. Splits the records into chunks of `batch_size` and sends each as `INSERT IGNORE INTO <table> [ ...records as JSON... ] RETURN NONE` to `/sql` with `Surreal-NS` and `Surreal-DB` headers and basic auth.
5. Checks every statement in the response has status `OK`, otherwise the chunk fails with the statement's detail.
6. Attempts every chunk, then returns the last error if any chunk failed or any record was dropped. The runtime holds the offset and redelivers the whole batch.

Transient errors are retried inside the sink up to `max_retries` attempts in total, with an exponential backoff starting at `retry_delay`, doubling per attempt, capped at `max_retry_delay`, with 20 percent jitter. Transient means a response mentioning `transaction conflict` or `transaction can be retried`, a connection or timeout error from the HTTP client, or HTTP `408`, `429`, `500`, `502`, `503` or `504`. A connection error also triggers a reconnect, which repeats the sign in, health check and namespace setup before the next attempt. Query errors such as a syntax or permission failure fail the chunk immediately.

## Configuration

All keys go under `[plugin_config]`.

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `endpoint` | string | required | `host:port` or `http(s)://host:port`. No path, query or credentials |
| `namespace` | string | required | SurrealDB namespace. Letters, digits and `_` only, not starting with a digit |
| `database` | string | required | SurrealDB database, same rules |
| `table` | template | required | Table name. Supports `{topic}` and `{topic_segment[n]}`, see [templating](/docs/connectors/routing#destination-templates-on-sinks) |
| `username` | string | none | Required unless `auth_scope = "none"` |
| `password` | string | none | Required unless `auth_scope = "none"`. Redacted in the API |
| `auth_scope` | string | `root` | Level the credentials sign in at. `root`, `namespace`, `database` or `none` |
| `use_tls` | bool | `false` | Use `https://` when `endpoint` has no scheme. Ignored when a scheme is given |
| `auto_define_table` | bool | `false` | Define namespace, database and each table on first use. Needs `auth_scope = "root"` |
| `define_indexes` | bool | `false` | Also define an index on `pico_topic`, `pico_partition`, `pico_offset` per table. Needs `auto_define_table` and `include_metadata` |
| `payload_format` | string | `auto` | `auto` picks by decoded schema. `json`, `text` or `base64` force one. `binary` is an alias for `base64` |
| `include_metadata` | bool | `true` | Add `pico_topic`, `pico_partition`, `pico_offset`, `pico_timestamp` and `pico_schema` |
| `include_headers` | bool | `true` | Add `pico_headers` when the record has headers |
| `include_key` | bool | `true` | Add `pico_key` when the record has a key |
| `batch_size` | int | `1000` | Records per `INSERT` statement. Values below 1 become 1 |
| `query_timeout` | duration | `30s` | HTTP timeout for every request |
| `max_retries` | int | `3` | Attempts per chunk on transient errors. `0` is treated as `1` |
| `retry_delay` | duration | `100ms` | First backoff delay, doubled per attempt |
| `max_retry_delay` | duration | `5s` | Backoff cap. Raised to `retry_delay` if set lower |
| `verbose_logging` | bool | `false` | Log every batch at `info` instead of `debug` |

`payload_format = "json"` requires the payload to parse as JSON, and `text` requires valid UTF-8. A record that fails is dropped from its chunk and the batch returns an error, so the runtime redelivers it until the sink stops. Use `auto` unless the topic schema is guaranteed.

## What lands in the table

With defaults, a JSON record becomes the following.

```json
{
  "id": "t6f72646572732d6575_p0_o4711",
  "pico_topic": "orders.eu",
  "pico_partition": "0",
  "pico_offset": "4711",
  "pico_timestamp": "1756934104118",
  "pico_schema": "json",
  "pico_key": "azE=",
  "pico_headers": { "trace-id": "abc" },
  "payload": { "order_id": 42, "total": 42.5 },
  "payload_encoding": "json"
}
```

| Field | Present when | Content |
| --- | --- | --- |
| `id` | always | `t<hex of topic>_p<partition>_o<offset>`. Hex keeps the id a plain identifier for any topic name |
| `pico_topic` | `include_metadata` | Topic the record came from |
| `pico_partition` | `include_metadata` | Always `"0"` on PicoMQ, as a string |
| `pico_offset` | `include_metadata` | Record offset as a string, so 64-bit values survive |
| `pico_timestamp` | `include_metadata` | Record timestamp, epoch milliseconds as a string |
| `pico_schema` | `include_metadata` | Topic `schema` the batch was decoded with |
| `pico_key` | `include_key` and the record has a key | Record key, base64 |
| `pico_headers` | `include_headers` and the record has headers | Object of header name to value. UTF-8 values are strings, others are `{ "data": base64, "pico_header_encoding": "base64" }` |
| `payload` | always | Object for JSON, string for text, base64 string otherwise |
| `payload_encoding` | always | `json`, `text` or `base64` |

The metadata numbers are strings, so a range query on `pico_offset` has to cast, `type::int(pico_offset) > 4000`. The index created by `define_indexes` is not unique, deduplication comes from the record id.

### Table names

The resolved name is rewritten. Every character outside letters, digits and `_` becomes `_`, and a leading digit gets a `_` prefix. A topic of `orders.us-east` under `events_{topic}` produces the table `events_orders_us_east`, and a topic of `42_events` under `{topic}` produces `_42_events`.

## Replay

The runtime redelivers a batch after a crash between the write and the offset commit. See [Delivery guarantees](/docs/connectors/delivery).

| Configuration | Result of a replayed batch |
| --- | --- |
| Any | Every record id already exists, `INSERT IGNORE` skips it. No visible change |

The id does not depend on `include_metadata`, `include_key` or `include_headers`, so the guarantee holds with all of them off. An existing record is never updated by a replay.

## Requirements

- SurrealDB with the HTTP API reachable from the runtime, `/sql`, `/signin` and `/health`.
- A user matching `auth_scope`. `auto_define_table` needs the root user because it issues namespace and database DDL. Without it the namespace, database and tables must exist, or the database must allow implicit table creation.
- `use_tls = true` or an `https://` endpoint when credentials cross a network.

## Troubleshooting

| Symptom | Cause |
| --- | --- |
| `SurrealDB auth_scope must be one of root, namespace, database, or none` | A typo in `auth_scope`. Unlike other sinks this one refuses to start |
| `SurrealDB endpoint must not include embedded credentials` | `user:pass@` in `endpoint`. Move them to `username` and `password` |
| `SurrealDB namespace must contain only ASCII letters, digits, and underscores` | Hyphens or dots in `namespace` or `database` |
| `SurrealDB auto_define_table requires auth_scope=root` | Table creation with a namespace or database user. Define the tables ahead of time or use root |
| `Failed to authenticate with SurrealDB` at start | Wrong credentials, or credentials that exist at a different level than `auth_scope` |
| `Invalid JSON payload` or `Invalid UTF-8 payload` | A forced `payload_format` that does not match the records. Use `auto` |
| `SurrealDB batch insert failed after N attempts` | A query error, or a transient error that outlasted `max_retries`. The message carries the statement detail |

# MongoDB sink

Writes each record as a document in a MongoDB collection. The collection can be fixed or derived from the topic, and the sink can create it. Every document gets `_id = topic:partition:offset`, so a replayed batch hits duplicate key errors that the sink recognises and ignores, and nothing changes.

| | |
| --- | --- |
| Type | Sink |
| Library | `libpicomq_connector_mongodb_sink` |
| Ships in | The `pico-connectors` image |
| Destination | Collection, templated per topic, inside one `database` |
| Creates destination | Yes, with `auto_create_collection` |
| On replay | No duplicates in any configuration |
| Payload | Any schema. Stored as BSON binary, a BSON document or a string |

<div class="pico-diagram">
<svg viewBox="0 30 720 170" width="720" role="img" aria-label="Records from a topic are batched by the runtime, handed to the MongoDB sink, resolved to a collection name from the template, and written with an unordered insertMany whose duplicate key errors are ignored.">
  <defs>
    <marker id="arrmgo" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M0 0.5 L7.5 4 L0 7.5 Z" class="arrow"/>
    </marker>
  </defs>
  <rect x="20" y="80" width="130" height="56" class="box-accent"/>
  <text x="85" y="104" text-anchor="middle" class="label">orders.eu</text>
  <text x="85" y="122" text-anchor="middle" class="sub">batch of 1000</text>
  <rect x="210" y="80" width="160" height="56" class="box"/>
  <text x="290" y="104" text-anchor="middle" class="label">resolve collection</text>
  <text x="290" y="122" text-anchor="middle" class="sub">orders_{segment[-1]}</text>
  <rect x="420" y="80" width="130" height="56" class="box"/>
  <text x="485" y="104" text-anchor="middle" class="label">insertMany</text>
  <text x="485" y="122" text-anchor="middle" class="sub">dup key ignored</text>
  <rect x="600" y="80" width="110" height="56" class="box-accent"/>
  <text x="655" y="104" text-anchor="middle" class="label">collection</text>
  <text x="655" y="122" text-anchor="middle" class="sub">orders_eu</text>
  <path d="M150 108 L202 108" class="edge" marker-end="url(#arrmgo)"/>
  <path d="M370 108 L412 108" class="edge" marker-end="url(#arrmgo)"/>
  <path d="M550 108 L592 108" class="edge" marker-end="url(#arrmgo)"/>
  <text x="360" y="160" text-anchor="middle" class="sub">_id is topic:partition:offset, so a replayed batch inserts nothing</text>
</svg>
</div>

## Quick start

```toml
type = "sink"
key = "orders_mongo"
enabled = true
version = 0
name = "Orders to MongoDB"
path = "libpicomq_connector_mongodb_sink"

[[topics]]
pattern = 'orders\..*'
schema = "json"
batch_length = 1000
poll_interval = "100ms"

[plugin_config]
connection_uri = "mongodb://user:pass@mongo:27017"
database = "app"
collection = "orders_{topic_segment[-1]}"
auto_create_collection = true
payload_format = "json"
```

Keep the connection URI out of the file with an environment override.

```bash
PICOMQ_CONNECTORS_SINK_ORDERS_MONGO_PLUGIN_CONFIG_CONNECTION_URI=mongodb://user:secret@mongo:27017
```

## How it works

On `open()` the sink parses `connection_uri`, applies `max_pool_size` when set, builds a client and runs `{ ping: 1 }` against `database`. If `collection` has no placeholders and `auto_create_collection` is on, it lists the collections in the database and creates the collection when it is missing.

For each batch the runtime hands over, the sink does the following.

1. Resolves `collection` against the topic name.
2. Creates the collection if `auto_create_collection` is on and this name has not been seen before. The result is cached per name.
3. Splits the batch into chunks of `batch_size` documents.
4. Builds one document per record with `_id`, the metadata fields, the key and the payload. A record whose payload cannot be converted fails its whole chunk before anything is sent.
5. Writes each chunk with one unordered `insertMany`. A response whose only write errors are duplicate keys, code `11000`, counts as success and is logged at `warn`.
6. Attempts every chunk, then returns the last error if any chunk failed. The runtime holds the offset and redelivers the whole batch.

Transient errors are retried inside the sink up to `max_retries` attempts in total, with a linear backoff of `retry_delay` times the attempt number. Transient means the driver's `RetryableWriteError` label, an I/O error, a cleared connection pool, a server selection failure, or a write or command error whose code is not `11000`, `13` or `121`. Authentication and BSON conversion errors are never retried. Errors of other kinds are retried only when the message mentions `timeout`, `network`, `pool` or `server selection`.

## Configuration

All keys go under `[plugin_config]`.

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `connection_uri` | string | required | A MongoDB URI, `mongodb://` or `mongodb+srv://`. Redacted in the API |
| `database` | string | required | Database that holds every collection this sink writes |
| `collection` | template | required | Collection name. Supports `{topic}` and `{topic_segment[n]}`, see [templating](/docs/connectors/routing#destination-templates-on-sinks) |
| `auto_create_collection` | bool | `false` | Create each collection on first use when it does not exist |
| `payload_format` | string | `binary` | Type of `payload`. `binary`, `json` or `string` (`text` is accepted as an alias). Any other value logs a warning and falls back to `binary` |
| `include_metadata` | bool | `true` | Add `pico_offset`, `pico_timestamp`, `pico_topic` and `pico_partition` |
| `include_key` | bool | `true` | Add `pico_key` when the record has a key |
| `batch_size` | int | `100` | Documents per `insertMany`. Values below 1 become 1 |
| `max_pool_size` | int | driver default | Maximum connections in the driver pool |
| `max_retries` | int | `3` | Attempts per chunk on transient errors |
| `retry_delay` | duration | `1s` | Base delay between attempts, multiplied by the attempt number. An unparseable value falls back to `1s` |
| `verbose_logging` | bool | `false` | Log every batch at `info` instead of `debug` |

`payload_format = "json"` requires the payload to parse as JSON. A record that does not fails its chunk as a non-transient error, so use it only with `schema = "json"` on the topic or a transform that produces JSON. `payload_format = "string"` requires valid UTF-8 in the same way.

## What lands in the collection

With defaults and `payload_format = "json"`, a document looks like the following.

```js
{
  _id: "orders.eu:0:4711",
  pico_offset: Long(4711),
  pico_timestamp: ISODate("2026-09-03T21:15:04.118Z"),
  pico_topic: "orders.eu",
  pico_partition: 0,
  pico_key: BinData(0, "azE="),
  payload: { order_id: 42, total: 42.5 }
}
```

| Field | Present when | Content |
| --- | --- | --- |
| `_id` | always | `topic:partition:offset` as a string |
| `pico_offset` | `include_metadata` | Record offset as a 64-bit integer. Offsets above the signed range are written to `pico_offset_str` as a string instead |
| `pico_timestamp` | `include_metadata` | Record timestamp as a BSON `Date` |
| `pico_topic` | `include_metadata` | Topic the record came from |
| `pico_partition` | `include_metadata` | Always `0` on PicoMQ, 32-bit integer |
| `pico_key` | `include_key` and the record has a key | Record key as generic binary |
| `payload` | always | Binary for `binary`, a nested document for `json`, a string for `string` |

Headers are not stored. Use a [transform](/docs/connectors/transforms) to copy a header into the payload if it is needed.

### Collection names

The resolved name is used verbatim, so a template of `orders_{topic}` and a topic of `orders.eu` produces a collection named `orders_orders.eu`. MongoDB accepts hyphens and dots in collection names, so nothing is rewritten. Use `{topic_segment[-1]}` with dotted topics when a cleaner name is wanted.

When `auto_create_collection` is off, MongoDB still creates the collection implicitly on the first insert. The option exists so the collection is created explicitly and up front for static names.

## Replay

The runtime redelivers a batch after a crash between the write and the offset commit. See [Delivery guarantees](/docs/connectors/delivery).

| Configuration | Result of a replayed batch |
| --- | --- |
| Any | Every document collides on `_id`. The sink counts the `11000` errors, logs `ignored N duplicate writes in batch` and returns success |

`_id` does not depend on `include_metadata` or `include_key`, so the guarantee holds with both off. An existing document is never updated by a replay, the first write wins.

## Requirements

- A MongoDB deployment reachable from the runtime. Standalone, replica set and `mongodb+srv` URIs all work.
- A user with `insert` on the target collections, and `listCollections` plus `createCollection` on the database when `auto_create_collection` is on.
- TLS and authentication options go in the URI.

## Troubleshooting

| Symptom | Cause |
| --- | --- |
| `Failed to parse connection URI` at start | The URI is not a valid MongoDB connection string |
| `Database connectivity test failed` at start | Wrong credentials, or the server is not reachable from the container |
| `Failed to create collection` or `Failed to list collections` | The user lacks `createCollection` or `listCollections` on the database |
| `Failed to parse payload as JSON` | `payload_format = "json"` with a non-JSON record. Change the format or fix the upstream schema |
| `Failed to parse payload as UTF-8 text` | `payload_format = "string"` with binary content |
| `Batch insert failed after N attempts` | A non-transient write error, or a transient one that outlasted `max_retries`. The log has the driver error |
| `Unknown MongoDB sink payload format` at start | A typo in `payload_format`. The sink runs with `binary` |

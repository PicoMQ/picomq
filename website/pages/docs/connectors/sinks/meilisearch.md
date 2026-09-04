# Meilisearch sink

Indexes each record as a document in a Meilisearch index. The index can be fixed or derived from the topic, and the sink creates it by default. Every document carries a primary key derived from `topic`, `partition` and `offset`, so a replayed batch replaces the same documents and no duplicate appears in search results.

Documents are submitted as Meilisearch tasks. By default the sink waits for each task to finish before returning, so the offset commits only after the documents are searchable.

| | |
| --- | --- |
| Type | Sink |
| Library | `libpicomq_connector_meilisearch_sink` |
| Ships in | The `pico-connectors` image |
| Destination | Index, templated per topic |
| Creates destination | Yes, `create_index_if_not_exists` is on by default |
| On replay | No duplicates. Documents are replaced under the same primary key |
| Payload | `json`, `raw` and `text`. Other schemas are dropped |

<div class="pico-diagram">
<svg viewBox="0 30 720 170" width="720" role="img" aria-label="Records from a topic are batched by the runtime, handed to the Meilisearch sink, resolved to an index uid from the template, submitted as an add-documents task, and the sink waits for the task to succeed before returning.">
  <defs>
    <marker id="arrmei" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M0 0.5 L7.5 4 L0 7.5 Z" class="arrow"/>
    </marker>
  </defs>
  <rect x="20" y="80" width="130" height="56" class="box-accent"/>
  <text x="85" y="104" text-anchor="middle" class="label">orders.eu</text>
  <text x="85" y="122" text-anchor="middle" class="sub">batch of 1000</text>
  <rect x="210" y="80" width="160" height="56" class="box"/>
  <text x="290" y="104" text-anchor="middle" class="label">resolve index</text>
  <text x="290" y="122" text-anchor="middle" class="sub">orders_{segment[-1]}</text>
  <rect x="420" y="80" width="130" height="56" class="box"/>
  <text x="485" y="104" text-anchor="middle" class="label">add documents</text>
  <text x="485" y="122" text-anchor="middle" class="sub">wait for task</text>
  <rect x="600" y="80" width="110" height="56" class="box-accent"/>
  <text x="655" y="104" text-anchor="middle" class="label">index</text>
  <text x="655" y="122" text-anchor="middle" class="sub">orders_eu</text>
  <path d="M150 108 L202 108" class="edge" marker-end="url(#arrmei)"/>
  <path d="M370 108 L412 108" class="edge" marker-end="url(#arrmei)"/>
  <path d="M550 108 L592 108" class="edge" marker-end="url(#arrmei)"/>
  <text x="360" y="160" text-anchor="middle" class="sub">up to batch_size documents per request, task awaited before returning</text>
</svg>
</div>

## Quick start

```toml
type = "sink"
key = "orders_meili"
enabled = true
version = 0
name = "Orders to Meilisearch"
path = "libpicomq_connector_meilisearch_sink"

[[topics]]
pattern = 'orders\..*'
schema = "json"
batch_length = 1000
poll_interval = "100ms"

[plugin_config]
url = "http://meilisearch:7700"
api_key = "masterKey"
index = "orders_{topic_segment[-1]}"
primary_key = "order_id"
```

Keep the API key out of the file with an environment override.

```bash
PICOMQ_CONNECTORS_SINK_ORDERS_MEILI_PLUGIN_CONFIG_API_KEY=masterKey
```

## How it works

On `open()` the sink normalises `url`, adding `http://` when no scheme is given and dropping any path or query, and warns when an API key is about to travel over plain HTTP to a host other than loopback. It then calls `/health` until the status is `available`, retrying up to `max_open_retries` times. If `index` has no placeholders, it fetches the index, creates it with `primary_key` when missing, and warns when an existing index has a different primary key.

For each batch the runtime hands over, the sink does the following.

1. Resolves `index` against the topic name and rewrites every character outside `[A-Za-z0-9_-]` to `_`.
2. Fetches or creates the index if this uid has not been seen before. The result is cached per uid.
3. Builds one document per record. A JSON object is the document itself, any other JSON value is wrapped as `{ "value": ... }`. A `raw` payload is parsed as JSON when possible, otherwise stored as base64. A `text` payload becomes `{ "text": ... }`.
4. Sets `primary_key` on the document to the generated id when the payload does not already carry that field, then adds the `pico_*` metadata fields, overwriting any payload field with the same name.
5. Splits the documents into chunks of `batch_size` and submits each with the documents endpoint, `add_or_replace` or `add_or_update` depending on `document_action`, passing `primary_key` with the request.
6. Waits for the task when `wait_for_tasks` is on, polling every `task_poll_interval` until it succeeds, fails, or `task_timeout` passes.
7. Stops at the first chunk that fails and returns its error. Chunks already accepted stay in the index. The runtime holds the offset and redelivers the whole batch.

Each request to Meilisearch is retried up to `max_retries` times on transient errors, with an exponential backoff of `retry_delay` doubling per attempt, capped at `max_retry_delay`, with 20 percent jitter. The whole sequence of attempts for one request is bounded by `timeout`. Transient means a Meilisearch error of type `internal`, a communication failure, HTTP `429` or any `5xx`, or a request timeout. Errors of type `invalid_request` and `auth` fail the batch immediately.

::: warning Records that cannot become documents are dropped
A topic decoded as `proto`, `flatbuffer` or `avro` produces payloads the sink cannot turn into documents. Such records are logged at `warn`, counted as errors, and left out of the batch. The remaining documents are indexed and the offset commits past the dropped records. Use `schema = "json"`, `raw` or `text`, or a transform that produces JSON.
:::

## Configuration

All keys go under `[plugin_config]`.

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `url` | string | required | Meilisearch base URL. `http://` is assumed when no scheme is present. Path, query and fragment are ignored |
| `index` | template | required | Index uid. Supports `{topic}` and `{topic_segment[n]}`, see [templating](/docs/connectors/routing#destination-templates-on-sinks) |
| `api_key` | string | none | API key sent with every request. Redacted in the API |
| `primary_key` | string | `pico_id` | Document primary key field. Used when creating the index and passed with every documents request. Whitespace is trimmed and an empty value falls back to the default |
| `document_action` | string | `replace` | `replace` overwrites the whole document, `update` merges fields into an existing document |
| `create_index_if_not_exists` | bool | `true` | Create a missing index on first use. When off, a missing index fails the batch |
| `include_metadata` | bool | `true` | Add the `pico_*` fields listed below |
| `batch_size` | int | `1000` | Documents per request. Values below 1 become 1 |
| `wait_for_tasks` | bool | `true` | Wait for each indexing task to reach a terminal state before returning |
| `task_timeout` | duration | `30s` | Maximum wait for one task |
| `task_poll_interval` | duration | `100ms` | Sleep between task status polls |
| `timeout` | duration | `30s` | Budget for one request including its retries |
| `max_retries` | int | `3` | Retries per request on transient errors during batches |
| `max_open_retries` | int | `5` | Retries for the health check and index creation on `open()` |
| `retry_delay` | duration | `500ms` | Base backoff delay, doubled on each retry |
| `max_retry_delay` | duration | `5s` | Backoff cap. If smaller than `retry_delay` the two values are swapped |

::: warning wait_for_tasks = false
With `wait_for_tasks` off the sink returns as soon as Meilisearch has enqueued the task. The offset commits before the documents are indexed, and a task that later fails is not retried. Only turn it off when throughput matters more than confirmation.
:::

## What lands in the index

With defaults, a JSON record becomes the following document.

```json
{
  "pico_id": "pico_WyJvcmRlcnMtZXUiLDAsNDcxMV0",
  "order_id": 42,
  "total": 42.5,
  "pico_offset": 4711,
  "pico_topic": "orders.eu",
  "pico_partition": 0,
  "pico_timestamp": 1756934104118,
  "pico_ingested_at": 1756934104402,
  "pico_key": "azE=",
  "pico_headers": { "trace-id": "YWJj" }
}
```

| Field | Present when | Content |
| --- | --- | --- |
| `<primary_key>` | payload lacks it | Generated id, `pico_` followed by URL-safe base64 of the JSON array `[topic, partition, offset]` |
| `pico_id` | `include_metadata` and `primary_key` is not `pico_id` | The generated id, so provenance survives a custom primary key |
| `pico_offset` | `include_metadata` | Record offset |
| `pico_topic` | `include_metadata` | Topic the record came from |
| `pico_partition` | `include_metadata` | Always `0` on PicoMQ |
| `pico_timestamp` | `include_metadata` | Record timestamp, epoch milliseconds |
| `pico_ingested_at` | `include_metadata` | Time the sink built the document, epoch milliseconds |
| `pico_key` | `include_metadata` and the record has a key | Record key, base64 |
| `pico_headers` | `include_metadata` and the record has headers | Object of header name to base64 value. Every value is base64, including valid UTF-8 |
| payload fields | always | The JSON object's own fields, or `value`, `text`, or `data` with `data_type` and `data_encoding` for non-object payloads |

When the payload already carries the `primary_key` field, that value is used as the document id and the generated id is only kept in `pico_id`. Two records with the same payload key then map to one document, and the later one wins.

### Index names

The resolved name is rewritten. Every character outside letters, digits, `-` and `_` becomes `_`, so a topic of `orders.eu` under `orders_{topic}` produces the index `orders_orders_eu`. Hyphens are kept. A name that resolves to an empty string fails the batch.

## Replay

The runtime redelivers a batch after a crash between the write and the offset commit. See [Delivery guarantees](/docs/connectors/delivery).

| Configuration | Result of a replayed batch |
| --- | --- |
| `document_action = "replace"` | Each document is replaced by an identical one. Only `pico_ingested_at` changes |
| `document_action = "update"` | The same fields are merged again. Same result, only `pico_ingested_at` changes |
| `wait_for_tasks = false` | Same as above, but a task that failed before the crash is not retried |

The primary key does not depend on `include_metadata`, so no configuration produces duplicates.

## Requirements

- A Meilisearch instance whose `/health` reports `available`, reachable from the runtime.
- An API key allowed to read health, get and create indexes, add documents and read tasks. The master key works.
- HTTPS when the API key crosses a network. The sink logs a warning otherwise.

## Troubleshooting

| Symptom | Cause |
| --- | --- |
| `Meilisearch health check returned status` at start | The instance is up but not `available`, still starting or in maintenance |
| `Meilisearch index '...' does not exist and create_index_if_not_exists=false` | The resolved uid has not been created. Create it or turn the option on |
| `Meilisearch index '...' primary key '...' differs from configured primary key` | The index was created elsewhere with another key. Documents still use the sink's `primary_key`, which Meilisearch may reject |
| `Meilisearch task failed` | Meilisearch rejected the documents. A payload primary key with characters outside `[A-Za-z0-9_-]` is the common case |
| `Meilisearch task ... timed out after` | Indexing is slower than `task_timeout`. Raise it or lower `batch_size` |
| `Dropping invalid Meilisearch sink record` | The topic uses `proto`, `flatbuffer` or `avro`. Those records are skipped |
| `Meilisearch index resolved from topic '...' is empty` | The template produced only characters that sanitise away |

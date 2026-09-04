# Elasticsearch sink

Indexes each record as a document in an Elasticsearch index. The index can be fixed or derived from the topic, and the sink can create it, with a mapping of your choice. Each document carries the record's topic, partition, offset, timestamp, key and headers alongside its fields.

The document `_id` is `topic:partition:offset`, so a replayed batch overwrites the same documents and leaves no duplicate.

| | |
| --- | --- |
| Type | Sink |
| Library | `libpicomq_connector_elasticsearch_sink` |
| Ships in | The `pico-connectors` image |
| Destination | Index, templated per topic |
| Creates destination | Yes, with `create_index_if_not_exists` |
| On replay | No visible duplicate. Same `_id`, same document |
| Payload | `json` indexed as is. `raw` and `text` wrapped in an object |

<div class="pico-diagram">
<svg viewBox="0 30 720 170" width="720" role="img" aria-label="Records from a topic are batched by the runtime, handed to the Elasticsearch sink, resolved to a sanitised index name from the template, and written with one bulk request whose document ids are topic, partition and offset.">
  <defs>
    <marker id="arress" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M0 0.5 L7.5 4 L0 7.5 Z" class="arrow"/>
    </marker>
  </defs>
  <rect x="20" y="80" width="130" height="56" class="box-accent"/>
  <text x="85" y="104" text-anchor="middle" class="label">orders.eu</text>
  <text x="85" y="122" text-anchor="middle" class="sub">batch of 1000</text>
  <rect x="210" y="80" width="150" height="56" class="box"/>
  <text x="285" y="104" text-anchor="middle" class="label">resolve index</text>
  <text x="285" y="122" text-anchor="middle" class="sub">lowercase, sanitised</text>
  <rect x="420" y="80" width="130" height="56" class="box"/>
  <text x="485" y="104" text-anchor="middle" class="label">POST /_bulk</text>
  <text x="485" y="122" text-anchor="middle" class="sub">_id from offset</text>
  <rect x="600" y="80" width="110" height="56" class="box-accent"/>
  <text x="655" y="104" text-anchor="middle" class="label">index</text>
  <text x="655" y="122" text-anchor="middle" class="sub">orders.eu</text>
  <path d="M150 108 L202 108" class="edge" marker-end="url(#arress)"/>
  <path d="M360 108 L412 108" class="edge" marker-end="url(#arress)"/>
  <path d="M550 108 L592 108" class="edge" marker-end="url(#arress)"/>
  <text x="360" y="160" text-anchor="middle" class="sub">one bulk request per batch, a single rejected document fails the whole batch</text>
</svg>
</div>

## Quick start

```toml
type = "sink"
key = "orders_es"
enabled = true
version = 0
name = "Orders to Elasticsearch"
path = "libpicomq_connector_elasticsearch_sink"

[[topics]]
pattern = "orders\\..*"
schema = "json"
batch_length = 1000
poll_interval = "100ms"

[plugin_config]
url = "http://elasticsearch:9200"
username = "elastic"
password = "secret"
index = "orders-{topic_segment[-1]}"
create_index_if_not_exists = true
```

Keep the password out of the file with an environment override.

```bash
PICOMQ_CONNECTORS_SINK_ORDERS_ES_PLUGIN_CONFIG_PASSWORD=secret
```

## How it works

On `open()` the sink builds a client for the single node at `url`, with basic authentication when both `username` and `password` are set. If `index` has no placeholders and `create_index_if_not_exists` is on, it checks that the index exists and creates it otherwise. No request is made to the cluster when the index is templated.

For each batch the runtime hands over, the sink does the following.

1. Resolves `index` against the topic name and sanitises the result.
2. Checks that the index exists and creates it if allowed. Each index name is checked once and remembered for the life of the sink.
3. Converts each record to a JSON object and adds the `pico_*` fields.
4. Sends one `_bulk` request with an `index` action per document, `_id` set to `topic:partition:offset`.
5. Reads the per-item results. If any item carries an `error`, the batch fails with `Cannot store data: N of M documents failed to index into '<index>'` and the first error.

The sink has no retry of its own. A transport error or a rejected bulk request returns immediately, and the runtime retries the whole batch. Documents that were accepted in the failed attempt are simply overwritten by the retry.

::: info Silently dropped records
A record whose payload is `proto`, `flatbuffer` or `avro` is skipped with a warning and is not indexed. Decode those schemas on the topic, or convert them with a transform, before this sink.
:::

## Configuration

All keys go under `[plugin_config]`.

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `url` | string | required | Node URL, `http://elasticsearch:9200`. One node only |
| `index` | template | required | Index name. Supports `{topic}` and `{topic_segment[n]}`, see [templating](/docs/connectors/routing#destination-templates-on-sinks) |
| `username` | string | none | Basic auth user. Only used when `password` is also set |
| `password` | string | none | Basic auth password. Redacted in the API |
| `timeout_seconds` | int | `30` | Request timeout. Values below `1` are raised to `1` |
| `create_index_if_not_exists` | bool | `true` | Check for the index and create it on first use |
| `index_mapping` | table | none | Body of the create index request, `settings` and `mappings`. Only applied when the sink creates the index |
| `include_key` | bool | `true` | Add `pico_key` when the record has a key |
| `batch_size` | int | none | Accepted but not used. The bulk request always holds the whole batch the runtime delivered |

The bulk request size is governed by `batch_length` on the `[[topics]]` entry. Keep it within what the cluster accepts for a single request body.

An `index_mapping` is written in TOML under `[plugin_config.index_mapping]`.

```toml
[plugin_config.index_mapping.mappings.properties]
total = { type = "double" }
pico_timestamp = { type = "date", format = "epoch_millis" }
```

## What lands in the index

A `json` record `{"id": 7, "tenant": "acme", "total": 42.5}` from topic `orders.eu` at offset 42 produces this bulk action.

```json
{ "index": { "_index": "orders.eu", "_id": "orders.eu:0:42" } }
{
  "id": 7,
  "tenant": "acme",
  "total": 42.5,
  "pico_topic": "orders.eu",
  "pico_partition": 0,
  "pico_offset": 42,
  "pico_timestamp": 1756940104118,
  "pico_key": "acme-7",
  "pico_headers": { "trace": "abc123" }
}
```

| Field | Present when | Content |
| --- | --- | --- |
| Payload fields | `json` payload is an object | Copied as is |
| `data`, `data_type` | `raw` payload that is not valid JSON, or `text` payload | `raw` gives `data` as base64 and `data_type = "raw"`. `text` gives the string under `text` and `data_type = "text"` |
| `pico_topic` | always | Topic the record came from |
| `pico_partition` | always | Always `0` on PicoMQ |
| `pico_offset` | always | Record offset |
| `pico_timestamp` | always | Record timestamp in milliseconds |
| `pico_key` | `include_key` and the record has a key | The key as text, or base64 when it is not valid UTF-8 |
| `pico_headers` | the record has headers | An object of header name to value, each value text or base64 |

A `raw` payload that parses as JSON is treated as `json`. A `json` payload that is not an object, an array for instance, is indexed as it is with no `pico_*` fields, since there is no object to add them to.

### Index names

Elasticsearch rejects uppercase and a set of punctuation in index names, so the resolved template is rewritten rather than quoted.

| Step | Effect |
| --- | --- |
| Lowercase | `Orders` becomes `orders` |
| Replace `\`, `/`, `*`, `?`, `"`, `<`, `>`, `\|`, space, `,`, `#` and `:` | Each becomes `_` |
| Strip leading `-`, `_` and `+` | `_hidden` becomes `hidden` |
| Empty result | Becomes `index` |

Dots and hyphens are kept, so `orders.eu` stays `orders.eu`. A template of `{topic}` with topic `Orders/EU:west` yields `orders_eu_west`.

## Replay

The runtime redelivers a batch after a crash between the write and the offset commit. See [Delivery guarantees](/docs/connectors/delivery).

| Configuration | Result of a replayed batch |
| --- | --- |
| Any | Every document is indexed again under the same `_id`. The content is unchanged and no second copy appears |

The same holds for the runtime's own retries after a partial bulk failure. The documents that succeeded the first time are rewritten, the ones that failed get another chance.

## Requirements

- An Elasticsearch cluster reachable from the runtime at `url`. A single node is addressed, so point at a load balancer or coordinating node for a cluster.
- The user needs `index` and `create_index` on the target indices, and `view_index_metadata` for the existence check.
- TLS is selected by an `https://` URL. Certificates must be valid, there is no option to skip verification.

## Troubleshooting

| Symptom | Cause |
| --- | --- |
| `Invalid Elasticsearch URL` at start | `url` does not parse. It needs a scheme, `http://host:9200` |
| `Failed to check index existence` | The node is unreachable, or the credentials are rejected |
| `Failed to create index '...'` | The user lacks `create_index`, or `index_mapping` is not a valid create request body |
| `Bulk indexing failed` | The bulk request as a whole was rejected. Usually authentication, or a body larger than the cluster accepts |
| `N of M documents failed to index into '...'` | Individual documents rejected, most often a mapping conflict. The first error is in the message, all of them are in the log at `warn` |
| `Unsupported payload format` in the log and documents missing | The topic `schema` is `proto`, `flatbuffer` or `avro`. Those records are dropped |
| Sink in `error` after five attempts | A persistent rejection. Fix the mapping or the cluster and `POST /sinks/{key}/restart` |

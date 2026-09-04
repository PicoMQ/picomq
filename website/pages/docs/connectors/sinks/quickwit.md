# Quickwit sink

Ingests each batch of JSON records into a Quickwit index through the REST API. The index is described by a Quickwit index config in YAML, whose `index_id` can be fixed or derived from the topic, and the sink creates the index from that config when it is missing. Each document carries the record's topic, partition, offset and timestamp alongside its fields.

Quickwit is append-only, so a replayed batch is ingested a second time.

| | |
| --- | --- |
| Type | Sink |
| Library | `libpicomq_connector_quickwit_sink` |
| Ships in | The `pico-connectors` image |
| Destination | Index, templated per topic through `index_id` |
| Creates destination | Yes, always, from the YAML in `index` |
| On replay | Documents repeat. Deduplicate on `pico_offset` at query time |
| Payload | `json` only. Other schemas are dropped |

<div class="pico-diagram">
<svg viewBox="0 30 720 170" width="720" role="img" aria-label="Records from a topic are batched by the runtime, handed to the Quickwit sink, resolved to an index id from the template in the YAML config, serialised as newline-delimited JSON and posted to the ingest endpoint with auto commit.">
  <defs>
    <marker id="arrqws" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M0 0.5 L7.5 4 L0 7.5 Z" class="arrow"/>
    </marker>
  </defs>
  <rect x="20" y="80" width="130" height="56" class="box-accent"/>
  <text x="85" y="104" text-anchor="middle" class="label">logs.app</text>
  <text x="85" y="122" text-anchor="middle" class="sub">batch of 1000</text>
  <rect x="210" y="80" width="150" height="56" class="box"/>
  <text x="285" y="104" text-anchor="middle" class="label">resolve index_id</text>
  <text x="285" y="122" text-anchor="middle" class="sub">logs-{segment[-1]}</text>
  <rect x="420" y="80" width="130" height="56" class="box"/>
  <text x="485" y="104" text-anchor="middle" class="label">POST /ingest</text>
  <text x="485" y="122" text-anchor="middle" class="sub">commit=auto</text>
  <rect x="600" y="80" width="110" height="56" class="box-accent"/>
  <text x="655" y="104" text-anchor="middle" class="label">index</text>
  <text x="655" y="122" text-anchor="middle" class="sub">logs-app</text>
  <path d="M150 108 L202 108" class="edge" marker-end="url(#arrqws)"/>
  <path d="M360 108 L412 108" class="edge" marker-end="url(#arrqws)"/>
  <path d="M550 108 L592 108" class="edge" marker-end="url(#arrqws)"/>
  <text x="360" y="160" text-anchor="middle" class="sub">NDJSON body, one request per batch, no retries inside the plugin</text>
</svg>
</div>

## Quick start

```toml
type = "sink"
key = "logs_qw"
enabled = true
version = 0
name = "Logs to Quickwit"
path = "libpicomq_connector_quickwit_sink"

[[topics]]
pattern = "logs\\..*"
schema = "json"
batch_length = 1000
poll_interval = "100ms"

[plugin_config]
url = "http://quickwit:7280"
include_metadata = true
index = """
version: 0.8
index_id: logs-{topic_segment[-1]}
doc_mapping:
  mode: dynamic
"""
```

There is no credential in this configuration. The sink sends no authentication header, so the URL must be reachable without one.

## How it works

When the sink is constructed, `index` is parsed as YAML and its `index_id` is read as a destination template. A YAML that does not parse, or an `index_id` with an unknown placeholder, fails on `open()` with `Invalid config value: invalid index config`. On `open()`, if the `index_id` has no placeholders, the sink checks `GET /api/v1/indexes/{index_id}` and creates the index when it is missing.

For each batch the runtime hands over, the sink does the following.

1. Resolves `index_id` against the topic name.
2. Checks that the index exists and creates it otherwise. Each `index_id` is checked once and remembered for the life of the sink.
3. Adds the `pico_*` fields to every record whose payload is a JSON object, when `include_metadata` is on.
4. Serialises the records as newline-delimited JSON, one line per record.
5. Sends `POST /api/v1/{index_id}/ingest?commit=auto` with that body. A non-success status fails the batch with `Cannot store data: Status code: <status>, reason: <body>`.

To create an index the sink takes the YAML from `index`, replaces `index_id` with the resolved value, and sends it to `POST /api/v1/indexes` as `application/yaml`. Everything else in the YAML, `doc_mapping`, `search_settings`, `indexing_settings` and `retention`, is passed through unchanged.

The sink has no retry and no request timeout of its own. A transport error or a non-success response returns immediately, and the runtime retries the whole batch.

::: info Silently dropped records
A record whose payload is anything other than `json` is skipped with the warning `Unsupported payload format` and is not ingested. A batch with no JSON records at all is acknowledged without a request.
:::

## Configuration

All keys go under `[plugin_config]`.

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `url` | string | required | Base URL of the Quickwit REST API, `http://quickwit:7280`, without `/api/v1` |
| `index` | string | required | A full Quickwit index config in YAML. Its `index_id` is a template that supports `{topic}` and `{topic_segment[n]}`, see [templating](/docs/connectors/routing#destination-templates-on-sinks) |
| `include_metadata` | bool | `true` | Add `pico_topic`, `pico_partition`, `pico_offset` and `pico_timestamp` to each document |

The YAML must contain `index_id`. `version` and `doc_mapping` are required by Quickwit when the sink creates the index, and are ignored when the index already exists.

## What lands in the index

A `json` record `{"level": "info", "msg": "started"}` from topic `logs.app` at offset 42 produces this line in the ingest body.

```json
{
  "level": "info",
  "msg": "started",
  "pico_topic": "logs.app",
  "pico_partition": 0,
  "pico_offset": 42,
  "pico_timestamp": 1756940104118
}
```

| Field | Present when | Content |
| --- | --- | --- |
| Payload fields | always | Copied as is |
| `pico_topic` | `include_metadata` and the payload is an object | Topic the record came from |
| `pico_partition` | `include_metadata` and the payload is an object | Always `0` on PicoMQ |
| `pico_offset` | `include_metadata` and the payload is an object | Record offset |
| `pico_timestamp` | `include_metadata` and the payload is an object | Record timestamp in milliseconds |

The record key and headers are not written. A JSON payload that is not an object, an array for instance, is ingested as it is with no metadata.

`pico_timestamp` is a plain integer of milliseconds. To use it as the index timestamp field, map it in `doc_mapping` as a datetime that accepts unix timestamps.

### Index ids

The resolved `index_id` is used verbatim in the URL path and in the create request. Nothing is rewritten, so a topic name with characters Quickwit does not allow in an index id fails at creation. Pick a template whose output is a valid `index_id` for every topic the pattern can match, `logs-{topic_segment[-1]}` for dotted topics rather than `{topic}`.

## Replay

The runtime redelivers a batch after a crash between the write and the offset commit. See [Delivery guarantees](/docs/connectors/delivery).

| Configuration | Result of a replayed batch |
| --- | --- |
| `include_metadata = true` | The documents are ingested again. Both copies carry the same `pico_topic`, `pico_partition` and `pico_offset`, so a query can collapse them |
| `include_metadata = false` | The documents are ingested again with nothing to tell the copies apart |

Quickwit has no document id or upsert, so the duplicate stays in the index. Keep `include_metadata` on.

## Requirements

- A Quickwit server reachable from the runtime at `url`, on the REST port, 7280 by default.
- The REST API must accept unauthenticated requests from the runtime. The sink has no `username`, `password` or token setting.
- The index config in `index` must be valid for the Quickwit version in use when the sink is expected to create the index.

## Troubleshooting

| Symptom | Cause |
| --- | --- |
| `Invalid config value: invalid index config` at start | The YAML in `index` does not parse, or lacks `index_id` |
| `Invalid config value: unknown placeholder` at start | `index_id` uses a placeholder other than `{topic}` or `{topic_segment[n]}` |
| `HTTP request failed: Unexpected status code` | The existence check returned something other than `200` or `404`. Usually a wrong `url` or a proxy in between |
| `Init error: Failed to create index` | Quickwit rejected the YAML. The reason from Quickwit is in the message. Often an `index_id` with characters Quickwit does not allow, or a missing `version` |
| `Cannot store data: Status code: 400` | A document does not fit `doc_mapping`, a `strict` mode mapping with an unknown field for instance |
| `Unsupported payload format` in the log and documents missing | The topic `schema` is not `json`. Those records are dropped |
| Documents appear twice | A replay happened. Collapse on `pico_offset` in the query |

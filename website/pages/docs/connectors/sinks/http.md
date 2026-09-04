# HTTP sink

Delivers records to an HTTP endpoint. Each record can go as its own request, or a whole batch can go as one NDJSON or JSON array body. The URL can be fixed or derived from the topic, so one sink can fan a family of topics out across a family of endpoints.

The sink knows nothing about what the endpoint does with a request. A response with a success status is taken as delivered, and the endpoint is responsible for deduplicating on the metadata the sink attaches.

| | |
| --- | --- |
| Type | Sink |
| Library | `libpicomq_connector_http_sink` |
| Ships in | The `pico-connectors` image |
| Destination | URL, templated per topic |
| Creates destination | Not applicable |
| On replay | The endpoint receives the batch again. Dedupe on `pico_topic` and `pico_offset` |
| Payload | Any schema. JSON stays JSON, text becomes a string, binary is base64 |

<div class="pico-diagram">
<svg viewBox="0 30 720 170" width="720" role="img" aria-label="Records from a topic are batched by the runtime, the URL is resolved from the template, the body is built per record or per batch according to batch_mode, and the request is sent with retries on transient statuses.">
  <defs>
    <marker id="arrhttp" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M0 0.5 L7.5 4 L0 7.5 Z" class="arrow"/>
    </marker>
  </defs>
  <rect x="20" y="80" width="130" height="56" class="box-accent"/>
  <text x="85" y="104" text-anchor="middle" class="label">orders.eu</text>
  <text x="85" y="122" text-anchor="middle" class="sub">batch of 1000</text>
  <rect x="210" y="80" width="150" height="56" class="box"/>
  <text x="285" y="104" text-anchor="middle" class="label">resolve URL</text>
  <text x="285" y="122" text-anchor="middle" class="sub">.../{segment[-1]}</text>
  <rect x="420" y="80" width="130" height="56" class="box"/>
  <text x="485" y="104" text-anchor="middle" class="label">build body</text>
  <text x="485" y="122" text-anchor="middle" class="sub">batch_mode</text>
  <rect x="600" y="80" width="110" height="56" class="box-accent"/>
  <text x="655" y="104" text-anchor="middle" class="label">POST</text>
  <text x="655" y="122" text-anchor="middle" class="sub">/hooks/eu</text>
  <path d="M150 108 L202 108" class="edge" marker-end="url(#arrhttp)"/>
  <path d="M360 108 L412 108" class="edge" marker-end="url(#arrhttp)"/>
  <path d="M550 108 L592 108" class="edge" marker-end="url(#arrhttp)"/>
  <text x="360" y="160" text-anchor="middle" class="sub">429 and 5xx retried with backoff, other statuses fail the batch</text>
</svg>
</div>

## Quick start

```toml
type = "sink"
key = "orders_hook"
enabled = true
version = 0
name = "Orders webhook"
path = "libpicomq_connector_http_sink"

[[topics]]
pattern = 'orders\..*'
schema = "json"
batch_length = 500
poll_interval = "100ms"

[plugin_config]
url = "https://api.example.com/hooks/{topic_segment[-1]}"
method = "POST"
batch_mode = "ndjson"
timeout = "10s"

[plugin_config.headers]
authorization = "Bearer ..."
x-source = "picomq"
```

```bash
PICOMQ_CONNECTORS_SINK_ORDERS_HOOK_PLUGIN_CONFIG_HEADERS='{"authorization":"Bearer ..."}'
```

## How it works

On `open()` the sink validates `success_status_codes`, the URL and every custom header, then builds an HTTP client with a pool of `max_connections`. With `health_check_enabled` it sends one `health_check_method` request to the URL and refuses to start unless the status is in `success_status_codes`. Templated URLs skip the health check, since there is no single URL to probe.

For each batch the runtime hands over, the sink does the following.

1. Resolves `url` against the topic name.
2. Builds one or more request bodies according to `batch_mode`.
3. Sends each request. Transient failures, meaning `429`, `500`, `502`, `503`, `504` and connection errors, are retried inside the client up to `max_retries` times with exponential backoff from `retry_delay`, multiplied by `retry_backoff_multiplier` each time, capped at `max_retry_delay`. A `Retry-After` header is logged but the computed backoff is used.
4. Treats a final status in `success_status_codes` as delivered and anything else as a failure.
5. Returns an error if any request in the batch failed, so the runtime holds the offset and redelivers the whole batch.

| `batch_mode` | Requests per batch | Body | `Content-Type` |
| --- | --- | --- | --- |
| `individual` | One per record | A JSON object per record, with the envelope below | `application/json` |
| `ndjson` | One | One envelope per line | `application/x-ndjson` |
| `json_array` | One | A JSON array of envelopes | `application/json` |
| `raw` | One per record | The record's bytes, no envelope | `application/octet-stream` |

In `individual` and `raw` mode the sink stops sending after three consecutive request failures, reports the batch as failed, and the remaining records wait for the redelivery.

::: warning Partial delivery in per-record modes
In `individual` and `raw` mode, records delivered before a failure are delivered again when the runtime retries the batch. The endpoint sees them twice. Use `ndjson` or `json_array` for endpoints that cannot deduplicate, since a single request either lands or does not.
:::

## Configuration

All keys go under `[plugin_config]`.

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `url` | template | required | Endpoint. Supports `{topic}` and `{topic_segment[n]}`, see [templating](/docs/connectors/routing#destination-templates-on-sinks) |
| `method` | string | `POST` | `GET`, `HEAD`, `POST`, `PUT`, `PATCH` or `DELETE` |
| `batch_mode` | string | `individual` | `individual`, `ndjson`, `json_array` or `raw` |
| `headers` | table | none | Extra request headers. A `content-type` here is ignored, the sink sets its own |
| `timeout` | duration | `30s` | Per request |
| `max_payload_size_bytes` | int | `10485760` | Bodies larger than this are not sent. `0` disables the check |
| `include_metadata` | bool | `true` | Wrap each record in the metadata envelope |
| `include_key` | bool | `true` | Add `pico_key` to the envelope |
| `success_status_codes` | list | `[200, 201, 202, 204]` | Statuses treated as delivered. Must be non-empty, each in 200 to 599 |
| `max_retries` | int | `3` | Retries per request on transient failures |
| `retry_delay` | duration | `1s` | Initial backoff |
| `retry_backoff_multiplier` | int | `2` | Backoff growth factor |
| `max_retry_delay` | duration | `30s` | Backoff ceiling |
| `health_check_enabled` | bool | `false` | Probe the URL on `open()` |
| `health_check_method` | string | `HEAD` | Method for the probe |
| `max_connections` | int | `10` | Idle connections kept per host |
| `tls_danger_accept_invalid_certs` | bool | `false` | Skip certificate verification. Development only |
| `verbose_logging` | bool | `false` | Log every request at `debug` |

An unparseable duration falls back to its default with a warning rather than failing to start.

## What the endpoint receives

With `include_metadata = true`, every record in `individual`, `ndjson` and `json_array` mode is wrapped.

```json
{
  "metadata": {
    "pico_topic": "orders.eu",
    "pico_partition": 0,
    "pico_offset": 1042,
    "pico_timestamp": 1767225600000,
    "pico_key": "b3JkZXItNw==",
    "pico_headers": { "trace-id": "abc123" }
  },
  "payload": { "id": 7, "total": 42.5 }
}
```

| Field | Content |
| --- | --- |
| `pico_topic`, `pico_partition`, `pico_offset` | Record identity. Stable across replays |
| `pico_timestamp` | Epoch milliseconds |
| `pico_key` | Base64 of the record key. Absent when there is no key or `include_key` is off |
| `pico_headers` | Header values as strings when UTF-8, otherwise `{ "data": "<base64>", "pico_header_encoding": "base64" }`. Absent when there are none |
| `payload` | See below |

| Topic `schema` | `payload` becomes |
| --- | --- |
| `json` | The JSON value as is |
| `text` | A JSON string |
| `raw`, `flatbuffer`, `avro`, `proto` | `{ "data": "<base64>", "pico_payload_encoding": "base64" }` |

With `include_metadata = false`, the record is the bare `payload` value. In `raw` mode the body is the record's bytes and nothing else, regardless of `include_metadata`.

### URLs

The resolved URL is used as is. Topic names are valid in URL paths, so no rewriting is done. A template that produces an invalid URL fails the batch.

## Replay

The runtime redelivers a batch after a crash between the request succeeding and the offset commit, and after a batch that failed partway in per-record modes. See [Delivery guarantees](/docs/connectors/delivery).

| Endpoint behaviour | Result |
| --- | --- |
| Deduplicates on `pico_topic` plus `pico_offset` | No visible effect |
| Idempotent by nature, such as a PUT keyed on the payload | No visible effect |
| Appends blindly | Duplicate records |

The sink cannot make an endpoint idempotent. It can only give it the identity to do so, which is why `include_metadata` defaults to on.

## Requirements

- An endpoint reachable from the runtime that answers with a status in `success_status_codes`.
- For templated URLs, every URL the template can produce must exist, since there is no creation step.
- Valid TLS, or `tls_danger_accept_invalid_certs = true` in development.

## Troubleshooting

| Symptom | Cause |
| --- | --- |
| `success_status_codes must not be empty` at start | The list was set to `[]` |
| `Health check returned status ...` at start | The endpoint does not accept `health_check_method` on the URL. Change the method or disable the check |
| `custom 'Content-Type' header in [headers] is ignored` | Remove it. The sink sets the type from `batch_mode` |
| `request failed (status 4xx)` on every batch | The endpoint rejects the body or the auth header. Only `429` is retried among 4xx |
| `payload at offset N exceeds max size` | A single record is larger than `max_payload_size_bytes`. The batch fails until the record is removed or the limit raised |
| `aborting ... batch after 3 consecutive HTTP failures` | The endpoint is down. Records delivered before the failure will be sent again on retry |
| Sink in `error` after five attempts | A non-transient status or a record that cannot be serialized. Fix the cause and `POST /sinks/{key}/restart` |

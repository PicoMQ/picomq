# Elasticsearch source

Polls an Elasticsearch index with a search query and produces each hit's `_source` document as a JSON record. A timestamp field acts as the cursor. Every poll asks for documents newer than the latest timestamp seen so far, sorted ascending, up to `batch_size` at a time.

The source follows the stage-and-apply pattern. The cursor computed from a batch is staged when the batch is returned and only becomes the committed cursor after the broker has acknowledged the batch.

| | |
| --- | --- |
| Type | Source |
| Library | `libpicomq_connector_elasticsearch_source` |
| Ships in | The `pico-connectors` image |
| Modes | `polling` |
| Output schema | `json` |
| State | Latest timestamp and last document id seen, plus poll counters |
| On replay | Documents are re-read and re-produced. Deduplicate at the sink |

<div class="pico-diagram">
<svg viewBox="0 30 720 170" width="720" role="img" aria-label="The source runs a search with a range filter on the timestamp field greater than the last cursor and a size limit. The hits form a batch whose candidate cursor is the largest timestamp in it. The runtime produces the batch and on ack the candidate becomes the committed cursor.">
  <defs>
    <marker id="arressrc" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M0 0.5 L7.5 4 L0 7.5 Z" class="arrow"/>
    </marker>
  </defs>
  <rect x="20" y="80" width="150" height="56" class="box"/>
  <text x="95" y="104" text-anchor="middle" class="label">_search</text>
  <text x="95" y="122" text-anchor="middle" class="sub">@timestamp &gt; last</text>
  <rect x="230" y="80" width="130" height="56" class="box-accent"/>
  <text x="295" y="104" text-anchor="middle" class="label">batch</text>
  <text x="295" y="122" text-anchor="middle" class="sub">candidate: max ts</text>
  <rect x="420" y="80" width="130" height="56" class="box"/>
  <text x="485" y="104" text-anchor="middle" class="label">produce</text>
  <text x="485" y="122" text-anchor="middle" class="sub">runtime</text>
  <rect x="600" y="80" width="110" height="56" class="box"/>
  <text x="655" y="104" text-anchor="middle" class="label">ack</text>
  <text x="655" y="122" text-anchor="middle" class="sub">apply cursor</text>
  <path d="M170 108 L222 108" class="edge" marker-end="url(#arressrc)"/>
  <path d="M360 108 L412 108" class="edge" marker-end="url(#arressrc)"/>
  <path d="M550 108 L592 108" class="edge" marker-end="url(#arressrc)"/>
  <text x="360" y="160" text-anchor="middle" class="sub">nack discards the candidate and the same documents are re-read</text>
</svg>
</div>

## Quick start

```toml
type = "source"
key = "logs_es"
enabled = true
version = 0
name = "Logs from Elasticsearch"
path = "libpicomq_connector_elasticsearch_source"

[[topics]]
topic = "logs"
schema = "json"
batch_length = 500
linger_time = "5ms"

[plugin_config]
url = "http://elasticsearch:9200"
index = "logs-app"
username = "elastic"
password = "changeme"
timestamp_field = "@timestamp"
polling_interval = "10s"
batch_size = 500
```

Keep the password out of the file with an environment override.

```bash
PICOMQ_CONNECTORS_SOURCE_LOGS_ES_PLUGIN_CONFIG_PASSWORD=secret
```

## How it works

On `open()` the source builds a single-node client for `url`, with basic auth when both `username` and `password` are set, and checks that `index` exists with a `HEAD` request. A missing or forbidden index fails `open()`. If the optional file state is enabled, the source then loads its cursor from that file, replacing whatever the runtime restored.

Each `poll()` sleeps `polling_interval` and then does the following.

1. Builds the search body. The `query` from the configuration, or `match_all`, is wrapped in a `bool.must` together with a `range` on `timestamp_field` greater than the committed cursor. On the first poll, or without a cursor, the range is omitted.
2. Runs `POST /<index>/_search` with `size = batch_size` and `sort` on `timestamp_field` ascending.
3. Turns each hit's `_source` into one record. Hits without `_source` are skipped.
4. Computes the candidate cursor as the largest `timestamp_field` value in the batch that parses as RFC 3339, and the last `_id` seen.
5. Stages the candidate and returns the batch together with the serialised state.

An empty poll updates the poll counters in the committed state and returns nothing, with no state to save. A failed search increments `error_count` in the committed state and returns the error, which the runtime logs before polling again. The source has no retry loop of its own beyond that.

### Stage and apply

The candidate state lives in a pending slot from the moment `poll()` returns until the runtime reports the batch result. On ack the candidate replaces the committed state, so the next poll starts after the batch's largest timestamp. On nack the candidate is discarded, the committed state is untouched, and the next poll re-runs the same search and re-reads the same documents. A crash between produce and ack has the same effect on restart, which is the at-least-once promise.

::: warning The cursor is a strict greater-than on a timestamp
Documents that share the batch's largest timestamp but fall outside `batch_size` are never read, because the next poll asks for strictly newer ones. Keep `batch_size` well above the number of documents that can share one timestamp, and use a field with millisecond or finer precision.
:::

Without `timestamp_field` no range filter is added and no cursor is computed. Every poll returns the same first `batch_size` documents sorted by `@timestamp`. Set it.

## Configuration

All keys go under `[plugin_config]`.

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `url` | string | required | Base URL of one node, `http://host:9200` |
| `index` | string | required | Index, alias or pattern to search. Checked for existence on `open()` |
| `username` | string | none | Basic auth user. Used only when `password` is also set |
| `password` | string | none | Basic auth password. Redacted in the API |
| `query` | JSON object | `{ "match_all": {} }` | Query DSL clause. Combined with the range filter in a `bool.must` |
| `timestamp_field` | string | none | Field used for the range filter, the sort and the cursor. Without it the source has no cursor |
| `polling_interval` | duration | `10s` | Sleep before each poll. An unparseable value falls back to `10s` |
| `batch_size` | int | `100` | `size` of each search |
| `scroll_timeout` | string | none | Accepted but unused |
| `state` | table | none | Optional plugin-local state file, see below |

### Plugin-local state file

The runtime already persists the source's cursor in its state store. `state` adds a second copy in a JSON file that the plugin loads on `open()` and writes on `close()`.

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `state.enabled` | bool | required inside `state` | Turn the file on |
| `state.storage_type` | string | `file` | Only `file` is implemented. `elasticsearch` and anything else fall back to `file` with a warning |
| `state.storage_config.base_path` | string | `./connector_states` | Directory for the file |
| `state.state_id` | string | `elasticsearch_source_<id>` | File name without `.json` |
| `state.auto_save_interval` | duration | none | Accepted but unused |
| `state.tracked_fields` | list | none | Accepted but unused |

The directory has to exist. The plugin creates the parent of `base_path`, not `base_path` itself, so with the default it creates nothing and the save fails with `Failed to write state file` unless `./connector_states` is already there. When the file loads successfully it overrides the state the runtime restored, so a stale file moves the cursor backwards. Leave `state` unset unless there is a reason to have the second copy.

## Output

Each record is the hit's `_source` object, exactly as Elasticsearch returned it. There is no envelope.

```json
{
  "@timestamp": "2026-09-03T21:15:04.118Z",
  "level": "info",
  "service": "checkout",
  "message": "order 7 placed"
}
```

| Field | Content |
| --- | --- |
| Every field | The document's `_source`, unchanged |
| `_id`, `_index`, `_score` | Not included |

Routing rules address fields at the top of the document, so `path = "service"` works as is. Records carry no key, no headers and no timestamp. Add a `key` route or a transform if a sink needs them.

## State

| Field | Meaning |
| --- | --- |
| `last_poll_timestamp` | The committed cursor |
| `last_document_id` | `_id` of the last hit in the last acknowledged batch. Informational |
| `total_documents_fetched`, `poll_count`, `error_count`, `last_error` | Counters logged on `close()` |
| `processing_stats` | Average poll time, bytes, empty and successful poll counts |

Losing the runtime's state file restarts the index from the beginning with no range filter, which re-reads everything the query matches.

## Requirements

- Elasticsearch 7 or 8, reachable from the runtime at `url`.
- A user with `read` on the index, and `view_index_metadata` so the existence check passes.
- A `date` field to use as `timestamp_field`, stored in a format that reads back as RFC 3339. Custom `format` mappings that produce anything else leave the cursor stuck.

## Troubleshooting

| Symptom | Cause |
| --- | --- |
| `Invalid Elasticsearch URL` at start | `url` is not a URL with a scheme |
| `Index 'logs-app' does not exist or is not accessible` at start | Wrong `index`, or the user lacks `view_index_metadata` |
| `Failed to check index existence` at start | The node is not reachable from the container |
| `Search request failed: ...` on every poll | The `query` is not valid Query DSL, or the user lacks `read`. The body carries the Elasticsearch error |
| The same documents arrive every poll | `timestamp_field` is unset, or its values are not RFC 3339 strings, so the cursor never advances |
| Documents are missing after a burst | More documents shared one timestamp than `batch_size` allowed. Raise `batch_size` or use a finer timestamp |
| `Failed to write state file` on shutdown | `state.enabled` is on and the `base_path` directory does not exist |
| Documents arrive twice after a restart | Expected after a crash between produce and ack. See [Delivery guarantees](/docs/connectors/delivery) |

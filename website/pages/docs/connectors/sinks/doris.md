# Doris sink

Loads each batch into an Apache Doris table with Stream Load, the HTTP bulk loading API of the frontend. Records must be JSON objects, and they are sent either as a JSON array mapped to columns by name, or as CSV mapped by position. Every load carries a label derived from the topic, partition and offset range, and Doris rejects a label it has already committed, so a replayed batch changes nothing.

The table must exist. The sink validates identifiers and builds the HTTP client on `open()` but does not create or alter anything in Doris.

| | |
| --- | --- |
| Type | Sink |
| Library | `libpicomq_connector_doris_sink` |
| Ships in | Released as a separate `.so` artifact, see [Operations](/docs/operations/connectors#the-image) |
| Destination | Table, templated per topic, loaded with Stream Load |
| Creates destination | No. The database and table must exist |
| On replay | No duplicates. A committed label is reported as success |
| Payload | JSON objects only |

<div class="pico-diagram">
<svg viewBox="0 30 720 170" width="720" role="img" aria-label="Records from a topic are batched by the runtime, the table name is resolved from the template, the batch is split into chunks of batch_size, each chunk is sent as one Stream Load PUT with a deterministic label, and the rows land in the Doris table.">
  <defs>
    <marker id="arrdoris" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M0 0.5 L7.5 4 L0 7.5 Z" class="arrow"/>
    </marker>
  </defs>
  <rect x="20" y="80" width="100" height="56" class="box-accent"/>
  <text x="70" y="104" text-anchor="middle" class="label">orders.eu</text>
  <text x="70" y="122" text-anchor="middle" class="sub">batch of 1000</text>
  <rect x="156" y="80" width="120" height="56" class="box"/>
  <text x="216" y="104" text-anchor="middle" class="label">resolve table</text>
  <text x="216" y="122" text-anchor="middle" class="sub">orders_{seg[-1]}</text>
  <rect x="312" y="80" width="110" height="56" class="box"/>
  <text x="367" y="104" text-anchor="middle" class="label">chunk</text>
  <text x="367" y="122" text-anchor="middle" class="sub">1000 rows</text>
  <rect x="458" y="80" width="120" height="56" class="box"/>
  <text x="518" y="104" text-anchor="middle" class="label">Stream Load</text>
  <text x="518" y="122" text-anchor="middle" class="sub">PUT with label</text>
  <rect x="614" y="80" width="100" height="56" class="box-accent"/>
  <text x="664" y="104" text-anchor="middle" class="label">table</text>
  <text x="664" y="122" text-anchor="middle" class="sub">orders_eu</text>
  <path d="M120 108 L148 108" class="edge" marker-end="url(#arrdoris)"/>
  <path d="M276 108 L304 108" class="edge" marker-end="url(#arrdoris)"/>
  <path d="M422 108 L450 108" class="edge" marker-end="url(#arrdoris)"/>
  <path d="M578 108 L606 108" class="edge" marker-end="url(#arrdoris)"/>
  <text x="367" y="164" text-anchor="middle" class="sub">label = prefix-topic-hash-partition-first-last, a duplicate label on a FINISHED job counts as success</text>
</svg>
</div>

## Quick start

```toml
type = "sink"
key = "orders_doris"
enabled = true
version = 0
name = "Orders to Doris"
path = "libpicomq_connector_doris_sink"

[[topics]]
pattern = 'orders\..*'
schema = "json"
batch_length = 1000
poll_interval = "100ms"

[plugin_config]
fe_url = "https://doris-fe:8030"
database = "analytics"
table = "orders_{topic_segment[-1]}"
username = "loader"
password = "secret"
batch_size = 1000
```

Keep the password out of the file with an environment override.

```bash
PICOMQ_CONNECTORS_SINK_ORDERS_DORIS_PLUGIN_CONFIG_PASSWORD=secret
```

## How it works

On `open()` the sink checks that `database` matches `[A-Za-z0-9_]+`, and `table` too when it has no placeholders, that `fe_url` is an `http` or `https` URL, and that `max_filter_ratio`, `columns` and `where` are valid header values. It parses the durations, falling back to the defaults with a warning when one does not parse, and builds an HTTP client that does not follow redirects on its own. No request is sent to Doris at this point.

For each batch the runtime hands over, the sink does the following.

1. Resolves `table` against the topic name. A template result is sanitised, a literal is used as written.
2. Splits the batch into chunks of `batch_size` records.
3. Checks that every record in the chunk is a JSON object. A record with another payload type fails the whole batch at once, including chunks not yet sent.
4. Serialises the chunk as a JSON array, or as CSV when `output_format = "csv"`.
5. Builds the label `<prefix>-<topic>-<hash>-<partition>-<first offset>-<last offset>`. Prefix and topic are sanitised and cut to 16 characters, and the hash is 16 hex characters over the full prefix, table and topic so that names which sanitise alike still get distinct labels.
6. Sends `PUT /api/<database>/<table>/_stream_load` to the frontend. Doris answers with a redirect to a backend, which the sink follows itself, up to 5 hops, after checking the target.
7. Reads the JSON response and classifies its `Status`.
8. On a failed chunk, remembers the first error and continues with the remaining chunks. When every chunk has been tried, returns that first error. The runtime holds the offset and redelivers the whole batch, and the labels make the already committed chunks harmless.

Each chunk is attempted up to `max_retries` times. The delay before attempt `n` is `retry_delay` doubled `n - 1` times, capped at `max_retry_delay`, with 20 percent jitter. Only transient failures are retried.

| Outcome | Classified as |
| --- | --- |
| Network error, timeout, HTTP `408`, `429` or `5xx` | Transient, retried |
| HTTP 2xx whose body is empty or cannot be read | Transient, retried |
| `Status: Label Already Exists` with `ExistingJobStatus: RUNNING` or `CANCELLED` | Transient, retried |
| `Status: Label Already Exists` with `ExistingJobStatus: FINISHED` | Success |
| `Status: Publish Timeout` | Success. The transaction committed, visibility lags |
| `Status: Success` with `NumberFilteredRows` above zero | Success, with a warning about the filtered rows |
| `Status: Fail`, any other status, or a body that is not the Stream Load JSON | Permanent, fails the chunk |
| Any other HTTP `4xx`, or a refused redirect | Permanent, fails the chunk |

### Redirects

Stream Load is a two-step protocol. The frontend answers `307` with the address of a backend and the sink repeats the `PUT` there. The sink refuses a redirect to a non-HTTP scheme, and a redirect from `https` to `http` unless `allow_insecure_redirect = true`, because the credentials travel in the `Authorization` header. With `allowed_redirect_hosts` set, the backend host and port must be in the list.

## Configuration

All keys go under `[plugin_config]`.

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `fe_url` | string | required | Frontend HTTP address, `http://` or `https://`. Plain HTTP to a non-loopback host logs a warning |
| `database` | string | required | Database name, `[A-Za-z0-9_]+` |
| `table` | template | required | Table name. Supports `{topic}` and `{topic_segment[n]}`, see [templating](/docs/connectors/routing#destination-templates-on-sinks) |
| `username` | string | required | Doris user |
| `password` | string | required | Doris password. An empty value is accepted with a warning. Redacted in the API |
| `label_prefix` | string | `picomq` | First segment of the load label, cut to 16 characters |
| `max_filter_ratio` | float | none | Sent as the `max_filter_ratio` header. Must be between `0.0` and `1.0` |
| `columns` | string | none | Sent as the `columns` header. Column list and derived column expressions in Doris syntax. Required for `csv` |
| `where` | string | none | Sent as the `where` header. Rows failing the predicate are filtered |
| `output_format` | string | `json` | `json` or `csv` |
| `timeout` | duration | `30s` | Whole request timeout. Zero falls back to the default |
| `connect_timeout` | duration | `5s` | TCP connect timeout. Zero falls back to the default |
| `batch_size` | int | `1000` | Records per Stream Load request. Below `1` is raised to `1` |
| `max_retries` | int | `3` | Total attempts per chunk. Below `1` is raised to `1`. Above `10` logs a warning |
| `retry_delay` | duration | `200ms` | Base delay between attempts. Clamped to `max_retry_delay` when larger |
| `max_retry_delay` | duration | `5s` | Cap on the delay between attempts |
| `allow_insecure_redirect` | bool | `false` | Follow a redirect from `https` to `http` |
| `allowed_redirect_hosts` | list | none | `host` or `host:port` entries the backend redirect must match. Empty list means no restriction |

`columns` doubles as the column order for `csv`. Names are read from the left until the first entry containing `=`, so `columns = "id,total,tenant,loaded_at=now()"` yields three CSV columns and one derived column.

## What lands in the table

With `output_format = "json"` the body is a JSON array of the payloads, sent with `strip_outer_array: true`, and Doris maps object keys to columns by name.

```json
[{"id":7,"tenant":"acme","total":42.5},{"id":8,"tenant":"acme","total":9.0}]
```

With `output_format = "csv"` the body has one row per record and one field per name in `columns`, in that order. The separator is `\x01`, the line delimiter `\x02`, strings are enclosed in `"` with `\` as the escape, and the headers tell Doris the same.

| Value in the record | CSV field |
| --- | --- |
| Missing key or `null` | `\N` |
| `true`, `false`, numbers | Written as is |
| String | Enclosed in `"`, with `"` and `\` escaped |
| Object or array | Serialised to JSON and enclosed like a string |

The sink adds no metadata columns. The topic, offset and key are not written unless a [transform](/docs/connectors/transforms) copies them into the payload first.

### Table names

A literal `table` is used verbatim and must already match `[A-Za-z0-9_]+`. A templated `table` is sanitised after resolution. Every character outside that set becomes `_`, and a name that starts with a digit gets a `_` prepended. So `orders_{topic}` with topic `orders.eu` produces `orders_orders_eu`, and `{topic_segment[-1]}` with topic `2026.eu` produces `_eu`.

::: info Filtered rows are not an error
Doris drops rows that fail type conversion or the `where` predicate and reports them in `NumberFilteredRows`. Without `max_filter_ratio` the load fails when any row is filtered. With it, the load succeeds up to that ratio and the sink only logs a warning, so the offset is committed and those rows are gone.
:::

## Replay

The runtime redelivers a batch after a crash between the write and the offset commit. See [Delivery guarantees](/docs/connectors/delivery).

| Situation | Result of a replayed chunk |
| --- | --- |
| Same offset range, same table, same `label_prefix` | Doris answers `Label Already Exists` for a `FINISHED` job. No visible change |
| The original load is still `RUNNING` | Retried until it finishes or the attempts run out |
| Redelivered batch chunks differently, so the offset range differs | A new label, the overlapping rows load again |
| `label_prefix` or `batch_size` changed between the two deliveries | A new label, the rows load again |

A table with the Unique Key model collapses duplicates on the key regardless of labels. Labels are database-scoped in Doris, and the hash segment includes the table name so two sinks loading the same topic into two tables in one database do not collide.

## Requirements

- A reachable frontend, and reachable backends, since Stream Load redirects to a backend host. Inside Kubernetes or Docker the backend addresses the frontend advertises must resolve from the runtime.
- A user with `LOAD_PRIV` on the target table.
- The database and table exist with a schema the payload maps onto.
- `https://` in production. Credentials go in a Basic `Authorization` header on every request.

## Troubleshooting

| Symptom | Cause |
| --- | --- |
| `fe_url '...' must use http or https` at start | `fe_url` has another scheme or no scheme |
| `table '...' must match [A-Za-z0-9_]+` at start | A literal table name with a hyphen, dot or other character. Rename it or use a template, which is sanitised |
| `format="csv" requires a non-empty columns` at start | `output_format = "csv"` without `columns`, or `columns` starts with a derived expression |
| `refusing redirect that downgrades https -> http` | The frontend advertises backends over plain HTTP. Fix the topology or set `allow_insecure_redirect = true` |
| `redirect target '...' is not in allowed_redirect_hosts` | A backend not in the list answered. Add it, with its port if the entry has one |
| `stream load failed: ...` with a type or column message | Schema drift between the payload and the table. Check the load error log on the backend, or add `columns` mappings |
| `received non-JSON payload` | The topic `schema` is not `json`. Set it, or add a converting transform |
| `loaded N rows but FILTERED M rows` warnings | Rows fail conversion or `where` and `max_filter_ratio` lets the load succeed. Those rows are lost |
| Rows appear twice | The redelivered batch was chunked differently, or `label_prefix` changed. See [Replay](#replay) |

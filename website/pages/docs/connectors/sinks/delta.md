# Delta Lake sink

Appends each batch of JSON records to an existing Delta table. The table URI can be fixed or derived from the topic, and each batch becomes one commit, so a table's version history is a record of the batches that reached it. Values are coerced to the table's string and timestamp columns before writing, and a record that cannot be made to fit fails the batch.

The sink never creates a table. The Delta log at `table_uri` has to exist, with its schema and partition columns, before the first batch arrives.

| | |
| --- | --- |
| Type | Sink |
| Library | `libpicomq_connector_delta_sink` |
| Ships in | Released as a separate `.so` artifact, see [Operations](/docs/operations/connectors#the-image) |
| Destination | Delta table by URI, templated per topic |
| Creates destination | No |
| On replay | Appended files repeat rows |
| Payload | `json` only |

<div class="pico-diagram">
<svg viewBox="0 30 720 170" width="720" role="img" aria-label="Records from a topic are batched by the runtime, handed to the Delta sink, resolved to a table URI from the template, coerced to the table's string and timestamp columns, written as Parquet and committed as one new table version.">
  <defs>
    <marker id="arrdlt" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M0 0.5 L7.5 4 L0 7.5 Z" class="arrow"/>
    </marker>
  </defs>
  <rect x="20" y="80" width="130" height="56" class="box-accent"/>
  <text x="85" y="104" text-anchor="middle" class="label">orders.eu</text>
  <text x="85" y="122" text-anchor="middle" class="sub">batch of 1000</text>
  <rect x="180" y="80" width="150" height="56" class="box"/>
  <text x="255" y="104" text-anchor="middle" class="label">resolve table_uri</text>
  <text x="255" y="122" text-anchor="middle" class="sub">s3://lake/{topic}</text>
  <rect x="360" y="80" width="130" height="56" class="box"/>
  <text x="425" y="104" text-anchor="middle" class="label">coerce</text>
  <text x="425" y="122" text-anchor="middle" class="sub">string, timestamp</text>
  <rect x="520" y="80" width="190" height="56" class="box-accent"/>
  <text x="615" y="104" text-anchor="middle" class="label">write and commit</text>
  <text x="615" y="122" text-anchor="middle" class="sub">one version per batch</text>
  <path d="M150 108 L172 108" class="edge" marker-end="url(#arrdlt)"/>
  <path d="M330 108 L352 108" class="edge" marker-end="url(#arrdlt)"/>
  <path d="M490 108 L512 108" class="edge" marker-end="url(#arrdlt)"/>
  <text x="360" y="160" text-anchor="middle" class="sub">a failed write resets the buffer and the runtime retries the batch</text>
</svg>
</div>

## Quick start

```toml
type = "sink"
key = "orders_delta"
enabled = true
version = 0
name = "Orders to Delta Lake"
path = "libpicomq_connector_delta_sink"

[[topics]]
pattern = "orders\\..*"
schema = "json"
batch_length = 1000
poll_interval = "100ms"

[plugin_config]
table_uri = "s3://lake/orders/{topic_segment[-1]}"
storage_backend_type = "s3"
aws_s3_access_key = "minio"
aws_s3_secret_key = "minio123"
aws_s3_region = "us-east-1"
aws_s3_endpoint_url = "http://minio:9000"
aws_s3_allow_http = true
```

Keep the secret out of the file with an environment override.

```bash
PICOMQ_CONNECTORS_SINK_ORDERS_DELTA_PLUGIN_CONFIG_AWS_S3_SECRET_KEY=secret
```

A local table for development needs no backend at all.

```toml
[plugin_config]
table_uri = "file:///data/delta/orders"
```

## How it works

On `open()` the sink validates the storage settings for the chosen `storage_backend_type` and fails if a required key is missing. If `table_uri` has no placeholders it also opens the table right away, reading the current snapshot's schema to build the coercion rules and creating a JSON writer for the table. A templated `table_uri` defers this to the first batch for each topic.

For each batch the runtime hands over, the sink does the following.

1. Converts every payload to JSON. A single record with another payload type fails the whole batch with `Invalid payload type`.
2. Resolves `table_uri` against the topic name. A table seen for the first time is opened and cached for the life of the connector.
3. Takes the table's lock, so batches for the same table are written one at a time.
4. Coerces each record to the table schema. Values destined for a string column that are not strings are serialised to JSON text. Values destined for a timestamp column that are strings are parsed and replaced by microseconds since the epoch. Integers pass through. Nested structs and arrays are walked the same way.
5. Hands the records to the Delta JSON writer, which buffers them as Parquet, partitioned by the table's partition columns.
6. Flushes the buffer and commits one new table version. The version number is logged at `debug`.

Any failure in steps 5 or 6 resets the writer's buffer and returns the error. The sink has no retry loop of its own, so the runtime retries the whole batch with the offset unmoved. On `close()` the sink flushes and commits anything still buffered for every open table.

### Coercions

The Delta JSON writer is strict about types. The sink smooths over the two cases that trip up JSON producers most often.

| Column type | Incoming value | Written as |
| --- | --- | --- |
| `string` | A string | Unchanged |
| `string` | A number, bool, object or array | Its JSON text, `{"a":1}` becomes the string `{"a":1}` |
| `timestamp`, `timestamp_ntz` | RFC 3339, `2026-09-03T21:15:04Z` | Microseconds since the epoch |
| `timestamp`, `timestamp_ntz` | `2026-09-03 21:15:04` or with fractional seconds, read as UTC | Microseconds since the epoch |
| `timestamp`, `timestamp_ntz` | An integer | Unchanged, assumed to be microseconds already |
| `timestamp`, `timestamp_ntz` | Any other string | Batch fails with `cannot parse "..." as a timestamp` |
| Anything else | Anything | Unchanged. The writer rejects a mismatch |

`null` is never coerced. Fields the table does not have pass to the writer as they are.

## Configuration

All keys go under `[plugin_config]`.

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `table_uri` | template | required | Table location as a URL, `s3://bucket/path`, `az://container/path`, `gs://bucket/path` or `file:///path`. Supports `{topic}` and `{topic_segment[n]}`, see [templating](/docs/connectors/routing#destination-templates-on-sinks) |
| `storage_backend_type` | string | none | `s3`, `azure` or `gcs`. Enables the matching group below. When absent no storage options are passed and the URI has to work on its own, as `file://` paths do |
| `aws_s3_access_key` | string | none | Required with `s3`. Redacted in the API |
| `aws_s3_secret_key` | string | none | Required with `s3`. Redacted in the API |
| `aws_s3_region` | string | none | Required with `s3` |
| `aws_s3_endpoint_url` | string | none | Custom endpoint for MinIO and other S3-compatible stores |
| `aws_s3_allow_http` | bool | none | Allow a plain `http://` endpoint. Set for local MinIO |
| `azure_storage_account_name` | string | none | Required with `azure` |
| `azure_container_name` | string | none | Required with `azure` |
| `azure_storage_account_key` | string | none | One of this or `azure_storage_sas_token` with `azure`, not both. Redacted in the API |
| `azure_storage_sas_token` | string | none | See above. Redacted in the API |
| `gcs_service_account_key` | string | none | Required with `gcs`. The service account JSON as a string. Redacted in the API |

A missing key in the active group fails `open()` with `S3 backend requires 'aws_s3_region'` or the equivalent for the other backends.

## What lands in the table

Each batch is one commit on the table. The commit adds Parquet files under the table location, in partition directories when the table has partition columns.

```text
s3://lake/orders/eu/part-00000-6b2c...-c000.snappy.parquet
s3://lake/orders/eu/region=eu/part-00000-9a7f...-c000.snappy.parquet
s3://lake/orders/eu/_delta_log/00000000000000000042.json
```

| Content | Source |
| --- | --- |
| Columns | The table's schema. Each record's top-level JSON fields are matched to columns by name |
| Partition values | Taken from the record's values for the table's partition columns |
| Versions | One per batch per table, plus one on `close()` if anything was still buffered |
| Metadata | None. No `pico_topic`, `pico_offset` or similar columns are added |

Keys and headers are not stored. Add them to the payload with a [transform](/docs/connectors/transforms) and a matching column if they are needed.

### Table URIs

The resolved template is used verbatim as the table URL. It is not sanitised, so a topic name with characters that are awkward in an object key ends up in the path as is. The result has to parse as a URL, which means a scheme is mandatory. A bare `/data/delta/orders` fails with `Invalid table URI`, `file:///data/delta/orders` works.

## Replay

The runtime redelivers a batch after a crash between the commit and the offset commit. See [Delivery guarantees](/docs/connectors/delivery).

| Configuration | Result of a replayed batch |
| --- | --- |
| Any | A second commit with the same rows. Delta has no key to collide on in append mode |

Delta cannot deduplicate on the way in. Downstream, a `MERGE INTO` keyed on a record id, or a view that deduplicates by that id, hides the repeats. Give records a stable id in the payload before they reach the sink.

## Requirements

- An existing Delta table at every URI the template can produce, with the columns the records carry.
- Object store credentials with read and write on the table location, including `_delta_log/`.
- Topics with `schema = "json"`. Every record is a JSON object.
- One writer per table. The sink serialises its own batches per table but does not coordinate with other writers.

## Troubleshooting

| Symptom | Cause |
| --- | --- |
| `Invalid storage configuration: S3 backend requires 'aws_s3_access_key'` at start | `storage_backend_type = "s3"` without the full credential set. The same shape names the missing Azure or GCS key |
| `Azure backend requires exactly one of 'azure_storage_account_key' or 'azure_storage_sas_token'` | Both, or neither, Azure credential given |
| `Invalid table URI` | The resolved `table_uri` has no scheme or is not a URL |
| `Failed to load Delta table` | No `_delta_log/` at the URI, the bucket is unreachable, or the credentials lack read |
| `Invalid payload type` | The topic is not `schema = "json"` |
| `Invalid record value: field "created_at": cannot parse "..." as a timestamp` | A timestamp column received a string in a format the sink does not parse. Emit RFC 3339 |
| `Storage error: Failed to write to Delta writer` | A value does not match its column type after coercion, or a non-nullable column is missing |
| `Storage error: Failed to flush and commit` | The store rejected the commit, usually a permission problem on `_delta_log/` or a conflicting writer |

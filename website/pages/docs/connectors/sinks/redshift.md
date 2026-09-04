# Redshift sink

Loads each batch into an Amazon Redshift table through the path Redshift is built for. Records are encoded as a Parquet file, uploaded to S3, copied into a staging table with `COPY`, and inserted into the target table with a query that skips ids already present. The id is `topic:partition:offset`, so a replayed batch changes nothing.

The sink creates the target table and its `staging_<table>` twin if they do not exist, and checks the columns of both on first use. Rows carry the record's topic, offset, timestamp and key alongside the payload.

| | |
| --- | --- |
| Type | Sink |
| Library | `libpicomq_connector_redshift_sink` |
| Ships in | Released as a separate `.so` artifact, see [Operations](/docs/operations/connectors#the-image) |
| Destination | Table, templated per topic, loaded through S3 and `COPY` |
| Creates destination | Yes, always, together with a `staging_<table>` twin |
| On replay | No duplicates. Ids are `topic:partition:offset` |
| Payload | Any schema. Stored as `VARBYTE` or `VARCHAR` |

<div class="pico-diagram">
<svg viewBox="0 30 720 170" width="720" role="img" aria-label="Records from a topic are batched by the runtime, encoded as a zstd Parquet file of batch_size rows, uploaded to S3 under the configured prefix, copied into the staging table with COPY, and inserted into the target table where the id does not already exist.">
  <defs>
    <marker id="arrredshift" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M0 0.5 L7.5 4 L0 7.5 Z" class="arrow"/>
    </marker>
  </defs>
  <rect x="20" y="80" width="100" height="56" class="box-accent"/>
  <text x="70" y="104" text-anchor="middle" class="label">orders.eu</text>
  <text x="70" y="122" text-anchor="middle" class="sub">batch of 1000</text>
  <rect x="156" y="80" width="110" height="56" class="box"/>
  <text x="211" y="104" text-anchor="middle" class="label">parquet</text>
  <text x="211" y="122" text-anchor="middle" class="sub">100 rows, zstd</text>
  <rect x="302" y="80" width="110" height="56" class="box"/>
  <text x="357" y="104" text-anchor="middle" class="label">PutObject</text>
  <text x="357" y="122" text-anchor="middle" class="sub">s3_prefix/uuid</text>
  <rect x="448" y="80" width="120" height="56" class="box"/>
  <text x="508" y="104" text-anchor="middle" class="label">COPY</text>
  <text x="508" y="122" text-anchor="middle" class="sub">staging_orders</text>
  <rect x="604" y="80" width="110" height="56" class="box-accent"/>
  <text x="659" y="104" text-anchor="middle" class="label">INSERT</text>
  <text x="659" y="122" text-anchor="middle" class="sub">NOT EXISTS id</text>
  <path d="M120 108 L148 108" class="edge" marker-end="url(#arrredshift)"/>
  <path d="M266 108 L294 108" class="edge" marker-end="url(#arrredshift)"/>
  <path d="M412 108 L440 108" class="edge" marker-end="url(#arrredshift)"/>
  <path d="M568 108 L596 108" class="edge" marker-end="url(#arrredshift)"/>
  <text x="367" y="164" text-anchor="middle" class="sub">id = topic:partition:offset, rows already in the target are skipped, then the staging table is truncated</text>
</svg>
</div>

## Quick start

```toml
type = "sink"
key = "orders_rs"
enabled = true
version = 0
name = "Orders to Redshift"
path = "libpicomq_connector_redshift_sink"

[[topics]]
pattern = 'orders\..*'
schema = "json"
batch_length = 1000
poll_interval = "100ms"

[plugin_config]
connection_string = "postgres://loader:pass@cluster.abc123.eu-west-1.redshift.amazonaws.com:5439/analytics"
target_table = "orders_{topic_segment[-1]}"
aws_iam_role = "arn:aws:iam::123456789012:role/RedshiftCopy"
s3_bucket = "picomq-staging"
s3_prefix = "redshift/orders"
aws_region = "eu-west-1"
payload_format = "text"
```

Keep the connection string out of the file with an environment override. The same form works for `aws_secret_access_key` when static S3 credentials are used.

```bash
PICOMQ_CONNECTORS_SINK_ORDERS_RS_PLUGIN_CONFIG_CONNECTION_STRING=postgres://loader:secret@cluster:5439/analytics
```

## How it works

On `open()` the sink validates the configuration, connects a pool of `max_connections`, runs `SELECT 1`, and builds the S3 client with the static keys or, when both are absent, the default credential chain. If `target_table` has no placeholders it then creates `staging_<table>` and `<table>` with `CREATE TABLE IF NOT EXISTS` and compares their columns in `pg_table_def` against what the configuration expects.

For each batch the runtime hands over, the sink does the following.

1. Resolves `target_table` against the topic name. The first time a name is seen, creates both tables if missing and checks their columns. The result is cached per name.
2. Splits the batch into chunks of `batch_size` rows.
3. Builds an Arrow record batch and encodes it as one Parquet file with zstd compression.
4. Uploads the file to `s3://<s3_bucket>/<s3_prefix>/<uuid>.parquet`. The uuid is version 7, so keys sort by time. The upload is not retried and must return HTTP `200`.
5. Runs `COPY "staging_<table>" (...) FROM 's3://...' CREDENTIALS 'aws_iam_role=...' FORMAT AS PARQUET`. If this fails after retries, deletes the file and fails the batch.
6. Runs the `INSERT ... SELECT` from staging into the target, keeping one row per id from staging and only ids not already in the target.
7. Truncates the staging table, then deletes the file, or with `archive = true` copies it to `archive/messages/<name>.parquet` and deletes the original. Failures in this step are logged as warnings and do not fail the batch.
8. Returns an error on the first chunk that fails. The runtime holds the offset and redelivers the whole batch.

`COPY`, `INSERT` and `TRUNCATE` are each attempted up to `max_retries` times with a linear backoff of `retry_delay` times the attempt number. Transient means an I/O error, a pool timeout, or a database error with one of these SQLSTATE codes: `40001`, `40P01`, `57P01`, `57P02`, `57P03`, `08000`, `08003`, `08006`. Anything else fails at once. S3 calls are never retried by the sink.

::: warning One staging table per target
Every sink resolving to the same target table shares `staging_<table>` and truncates it after each chunk. Two sinks, or two runtimes, loading the same table at the same time can truncate each other's staged rows between `COPY` and `INSERT`. Give each target table exactly one writer.
:::

## Configuration

All keys go under `[plugin_config]`.

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `connection_string` | string | required | A libpq URL to the cluster or workgroup endpoint. Redacted in the API |
| `target_table` | template | required | Table name. Supports `{topic}` and `{topic_segment[n]}`, see [templating](/docs/connectors/routing#destination-templates-on-sinks) |
| `aws_iam_role` | string | required | ARN Redshift assumes for `COPY`. Must be attached to the cluster |
| `s3_bucket` | string | required | Bucket for the Parquet files |
| `s3_prefix` | string | required | Key prefix inside the bucket. May be empty |
| `aws_region` | string | required | Region of the bucket. With `s3_endpoint`, any label |
| `s3_endpoint` | string | none | Custom S3 endpoint. Turns on path-style addressing |
| `aws_access_key_id` | string | none | Static S3 credential for the runtime side. Redacted in the API. Set both keys or neither |
| `aws_secret_access_key` | string | none | Static S3 credential. Redacted in the API |
| `batch_size` | int | `100` | Rows per Parquet file and per `COPY`. Must be above `0` |
| `max_connections` | int | `5` | Pool size |
| `include_metadata` | bool | `true` | Add `pico_offset`, `pico_timestamp`, `pico_topic` and `pico_partition` |
| `include_key` | bool | `true` | Add `pico_key` |
| `payload_format` | string | `varbyte` | Column type for `payload`. `varbyte` or `text`. `json` is accepted and treated as `text` |
| `max_retries` | int | `3` | Attempts per SQL statement on transient errors |
| `retry_delay` | duration | `1s` | Base delay between attempts, multiplied by the attempt number |
| `archive` | bool | `false` | Keep each Parquet file under `archive/messages/` instead of deleting it |
| `verbose_logging` | bool | `false` | Log every batch at `info` instead of `debug` |

An unrecognised `payload_format` does not fail `open()`. It logs a warning and falls back to `varbyte`. An unparsable `retry_delay` falls back to `1s` without a warning.

## What lands in the table

With defaults, the sink creates the target and staging tables with the same columns.

```sql
CREATE TABLE IF NOT EXISTS "orders_eu" (
  id VARCHAR(512),
  pico_offset VARCHAR(20),
  pico_timestamp VARCHAR(20),
  pico_topic VARCHAR,
  pico_partition BIGINT,
  pico_key VARCHAR(MAX),
  payload VARBYTE(16777216),
  created_at VARCHAR
);
```

| Column | Present when | Content |
| --- | --- | --- |
| `id` | always | `topic:partition:offset`, the deduplication key |
| `pico_offset` | `include_metadata` | Record offset, as a decimal string |
| `pico_timestamp` | `include_metadata` | Record timestamp in milliseconds, as a decimal string |
| `pico_topic` | `include_metadata` | Topic the record came from |
| `pico_partition` | `include_metadata` | Always `0` on PicoMQ |
| `pico_key` | `include_key` | Record key, base64, `NULL` when the record had none |
| `payload` | always | The record bytes, or the record as a string with `payload_format = "text"` |
| `created_at` | always | Time the batch was encoded, RFC 3339 string. Identical for every row in a chunk |

The timestamp columns are strings, not `TIMESTAMP`. Cast in queries, for instance `TIMESTAMP 'epoch' + pico_timestamp::BIGINT / 1000 * INTERVAL '1 second'`. Headers are not stored. With `payload_format = "text"` a payload that is not valid UTF-8 fails the batch.

### Table names

The resolved name is quoted verbatim, so `orders_{topic}` with topic `orders.eu` produces a table literally named `orders_orders.eu`, and a staging table named `staging_orders_orders.eu`. Queries against them need the quotes.

The schema check looks the table up in `pg_table_def` by bare name, which only covers tables in the current `search_path`. A schema-qualified `target_table` such as `analytics.orders` fails the check with `Table '...' was not found or has no visible columns` even though the table was just created. Set the schema through the connection string instead, `?options=-c%20search_path%3Danalytics`.

An existing table is not altered. Its columns must match the types the configuration implies, by family, so an existing `TEXT` payload column satisfies `payload_format = "text"` and a `BYTEA` or `VARBINARY` one satisfies `varbyte`. A mismatch fails `open()` or the first batch for that table with `Schema mismatch detected`.

## Replay

The runtime redelivers a batch after a crash between the write and the offset commit. See [Delivery guarantees](/docs/connectors/delivery).

| Configuration | Result of a replayed batch |
| --- | --- |
| Any | The `INSERT` skips every id already in the target. No visible change |

The `id` column is always present, so turning `include_metadata` off does not open a duplicate path. A crash between `COPY` and `INSERT` leaves rows in the staging table, and the next chunk's `INSERT` picks them up after the same deduplication. A crash between the upload and `COPY` leaves an orphan Parquet file under `s3_prefix`.

## Requirements

- A Redshift cluster or serverless workgroup reachable on its endpoint from the runtime. TLS is controlled through the connection string, `?sslmode=require`.
- A database user with `CREATE` on the schema, `INSERT` on the target, and ownership of, or `TRUNCATE` on, the staging table.
- `aws_iam_role` attached to the cluster with `s3:GetObject` and `s3:ListBucket` on the bucket, so `COPY` can read the files.
- Runtime-side S3 credentials with `s3:PutObject` and `s3:DeleteObject` on the prefix. `archive = true` also needs `s3:GetObject` for the server-side copy.
- The bucket in the same region as the cluster, or `COPY` needs `REGION`, which the sink does not add.

## Troubleshooting

| Symptom | Cause |
| --- | --- |
| `Failed to connect to Redshift` at start | Wrong `connection_string`, or the endpoint is not reachable from the container |
| `Choosing to use aws_access_key_id and aws_secret_access_key then both MUST be provided` | Only one of the two static keys is set, or one is empty |
| `Table '...' was not found or has no visible columns` | `target_table` is schema-qualified, or the schema is outside `search_path`. See [Table names](#table-names) |
| `Schema mismatch detected` | An existing table was created under different `include_metadata`, `include_key` or `payload_format` settings |
| `S3 upload failed` or `S3 upload failed with status 403` | The runtime-side credentials lack `PutObject` on `s3_prefix`, or `aws_region` does not match the bucket |
| `Redshift COPY failed after 3 attempts` | `aws_iam_role` is not attached to the cluster or cannot read the bucket. The `stl_load_errors` table has the detail |
| `Json is not supported, falling back to Text` warning | `payload_format = "json"`. Use `text` and cast with `JSON_PARSE` in Redshift if needed |
| Rows missing after two writers were pointed at one table | The shared staging table was truncated between `COPY` and `INSERT`. One writer per target table |

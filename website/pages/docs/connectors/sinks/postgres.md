# Postgres sink

Writes each record as a row in a PostgreSQL table. The table can be fixed or derived from the topic, and the sink can create it. Rows carry the record's topic, offset, timestamp and key alongside the payload, and inserts are idempotent on `(topic, partition, offset)`, so a replayed batch changes nothing.

| | |
| --- | --- |
| Type | Sink |
| Library | `libpicomq_connector_postgres_sink` |
| Ships in | The `pico-connectors` image |
| Destination | Table, templated per topic |
| Creates destination | Yes, with `auto_create_table` |
| On replay | No duplicates when metadata columns are on |
| Payload | Any schema. Stored as `BYTEA`, `JSONB` or `TEXT` |

<div class="pico-diagram">
<svg viewBox="0 30 720 170" width="720" role="img" aria-label="Records from a topic are batched by the runtime, handed to the Postgres sink, resolved to a table name from the template, and written with a multi-row INSERT that ignores conflicts on topic, partition and offset.">
  <defs>
    <marker id="arrpgs" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M0 0.5 L7.5 4 L0 7.5 Z" class="arrow"/>
    </marker>
  </defs>
  <rect x="20" y="80" width="130" height="56" class="box-accent"/>
  <text x="85" y="104" text-anchor="middle" class="label">orders.eu</text>
  <text x="85" y="122" text-anchor="middle" class="sub">batch of 1000</text>
  <rect x="210" y="80" width="150" height="56" class="box"/>
  <text x="285" y="104" text-anchor="middle" class="label">resolve table</text>
  <text x="285" y="122" text-anchor="middle" class="sub">orders_{segment[-1]}</text>
  <rect x="420" y="80" width="130" height="56" class="box"/>
  <text x="485" y="104" text-anchor="middle" class="label">INSERT</text>
  <text x="485" y="122" text-anchor="middle" class="sub">ON CONFLICT skip</text>
  <rect x="600" y="80" width="110" height="56" class="box-accent"/>
  <text x="655" y="104" text-anchor="middle" class="label">table</text>
  <text x="655" y="122" text-anchor="middle" class="sub">orders_eu</text>
  <path d="M150 108 L202 108" class="edge" marker-end="url(#arrpgs)"/>
  <path d="M360 108 L412 108" class="edge" marker-end="url(#arrpgs)"/>
  <path d="M550 108 L592 108" class="edge" marker-end="url(#arrpgs)"/>
  <text x="485" y="160" text-anchor="middle" class="sub">up to batch_size rows per statement, retried on transient errors</text>
</svg>
</div>

## Quick start

```toml
type = "sink"
key = "orders_pg"
enabled = true
version = 0
name = "Orders to Postgres"
path = "libpicomq_connector_postgres_sink"

[[topics]]
pattern = 'orders\..*'
schema = "json"
batch_length = 1000
poll_interval = "100ms"

[plugin_config]
connection_string = "postgres://user:pass@db:5432/app"
target_table = "orders_{topic_segment[-1]}"
auto_create_table = true
payload_format = "json"
```

Keep the connection string out of the file with an environment override.

```bash
PICOMQ_CONNECTORS_SINK_ORDERS_PG_PLUGIN_CONFIG_CONNECTION_STRING=postgres://user:secret@db:5432/app
```

## How it works

On `open()` the sink connects a pool of `max_connections`, runs `SELECT 1`, and if `target_table` has no placeholders and `auto_create_table` is on, creates the table.

For each batch the runtime hands over, the sink does the following.

1. Resolves `target_table` against the topic name. A template with placeholders is resolved once per topic and the result cached.
2. Creates the table if `auto_create_table` is on and this table has not been seen before.
3. Splits the batch into chunks of `batch_size` rows.
4. Writes each chunk as one multi-row `INSERT ... ON CONFLICT (pico_topic, pico_partition, pico_offset) DO NOTHING`.
5. Returns an error on the first chunk that fails, after retries. The runtime holds the offset and retries the whole batch.

Transient errors are retried inside the sink up to `max_retries` times with a linear backoff of `retry_delay` times the attempt number. Transient means an I/O error, a pool timeout, or a database error with one of these SQLSTATE codes: `40001`, `40P01`, `57P01`, `57P02`, `57P03`, `08000`, `08003`, `08006`. Anything else fails the batch immediately.

## Configuration

All keys go under `[plugin_config]`.

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `connection_string` | string | required | A libpq URL, `postgres://user:pass@host:5432/db`. Redacted in the API |
| `target_table` | template | required | Table name. Supports `{topic}` and `{topic_segment[n]}`, see [templating](/docs/connectors/routing#destination-templates-on-sinks) |
| `auto_create_table` | bool | `false` | Run `CREATE TABLE IF NOT EXISTS` for each table on first use |
| `payload_format` | string | `bytea` | Column type for `payload`. `bytea`, `json` (stored as `JSONB`) or `text` |
| `include_metadata` | bool | `true` | Add `pico_topic`, `pico_partition`, `pico_offset`, `pico_timestamp` and the unique constraint on them |
| `include_key` | bool | `true` | Add `pico_key` |
| `batch_size` | int | `100` | Rows per `INSERT` statement |
| `max_connections` | int | `10` | Pool size |
| `max_retries` | int | `3` | Attempts per chunk on transient errors |
| `retry_delay` | duration | `1s` | Base delay between attempts, multiplied by the attempt number |
| `verbose_logging` | bool | `false` | Log every batch at `info` instead of `debug` |

`payload_format = "json"` requires the payload to parse as JSON. A record that does not fails the batch as a non-transient error, so use it only with `schema = "json"` on the topic or a transform that produces JSON.

## What lands in the table

With defaults, `auto_create_table` produces the following.

```sql
CREATE TABLE IF NOT EXISTS "orders_orders.eu" (
  id BIGSERIAL PRIMARY KEY,
  pico_topic TEXT NOT NULL,
  pico_partition INTEGER NOT NULL,
  pico_offset BIGINT NOT NULL,
  pico_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
  pico_key BYTEA,
  payload BYTEA,
  created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
  UNIQUE (pico_topic, pico_partition, pico_offset)
)
```

| Column | Present when | Content |
| --- | --- | --- |
| `id` | always | Surrogate key |
| `pico_topic` | `include_metadata` | Topic the record came from |
| `pico_partition` | `include_metadata` | Always `0` on PicoMQ |
| `pico_offset` | `include_metadata` | Record offset |
| `pico_timestamp` | `include_metadata` | Record timestamp |
| `pico_key` | `include_key` | Record key, `NULL` when the record had none |
| `payload` | always | The record, in the type `payload_format` selects |
| `created_at` | always | Insert time |

Headers are not stored. Use a [transform](/docs/connectors/transforms) to copy a header into the payload if it is needed.

### Table names

The resolved name is quoted verbatim, so a template of `orders_{topic}` and a topic of `orders.eu` produces a table literally named `orders_orders.eu`, dot included. Queries against it need the quotes.

```sql
SELECT count(*) FROM "orders_orders.eu";
```

If that is inconvenient, route the source into topics that are already valid identifiers, or use `{topic_segment[-1]}` with dotted topic names so only the clean tail is used.

When the table already exists, the sink does not alter it. It must have the columns the configuration expects. A table created with `include_key = false` and later run with `include_key = true` fails every insert.

## Replay

The runtime redelivers a batch after a crash between the write and the offset commit. See [Delivery guarantees](/docs/connectors/delivery).

| Configuration | Result of a replayed batch |
| --- | --- |
| `include_metadata = true` | Every row hits `ON CONFLICT DO NOTHING`. No visible change |
| `include_metadata = false` | No unique constraint, so the rows are inserted again |

Keep metadata on unless the destination has its own deduplication.

## Requirements

- PostgreSQL 12 or later. Any managed service works.
- The role needs `INSERT` on the target tables, and `CREATE` on the schema when `auto_create_table` is on.
- Network access from the runtime to the database. TLS is controlled through the connection string, `?sslmode=require`.

## Troubleshooting

| Symptom | Cause |
| --- | --- |
| `Failed to connect to PostgreSQL` at start | Wrong `connection_string`, or the database is not reachable from the container |
| `Failed to create table` at start or on first batch | The role lacks `CREATE`, or the resolved name is not a valid identifier |
| `Failed to parse payload as JSON` | `payload_format = "json"` with a non-JSON record. Change the format or fix the upstream schema |
| `column "pico_key" of relation ... does not exist` | The table was created under a different `include_key` or `include_metadata` setting |
| Sink in `error` after five attempts | A non-transient database error. The log has the SQLSTATE. Fix it and `POST /sinks/{key}/restart` |
| Rows appear twice | `include_metadata = false` and a replay happened |

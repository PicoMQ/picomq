# Postgres source

Reads rows or changes from PostgreSQL and produces them as JSON records. Two modes are available. Polling selects new rows from tables by a monotonic column. CDC reads the write-ahead log through a logical replication slot and emits every insert, update and delete.

Both modes follow the stage-and-apply pattern, so a cursor only moves, and rows are only marked or deleted, after the broker has acknowledged the batch.

| | |
| --- | --- |
| Type | Source |
| Library | `libpicomq_connector_postgres_source` |
| Ships in | The `pico-connectors` image |
| Modes | `polling`, `cdc` |
| Output schema | `json`, or `raw` / `text` when extracting a single column |
| State | Per-table tracking offsets (polling). Replication slot position (CDC) |
| On replay | Rows are re-read and re-produced. Deduplicate at the sink |

<div class="pico-diagram">
<svg viewBox="0 20 720 230" width="720" role="img" aria-label="Two paths into the source. Polling issues SELECT with a WHERE on the tracking column and a LIMIT. CDC peeks the replication slot. Both hand a batch to the runtime and stage a candidate. On ack, polling marks or deletes rows and CDC advances the slot.">
  <defs>
    <marker id="arrpgsrc" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M0 0.5 L7.5 4 L0 7.5 Z" class="arrow"/>
    </marker>
  </defs>
  <text x="20" y="48" class="label">polling</text>
  <rect x="20" y="60" width="150" height="56" class="box"/>
  <text x="95" y="84" text-anchor="middle" class="label">SELECT</text>
  <text x="95" y="102" text-anchor="middle" class="sub">id &gt; last, LIMIT n</text>
  <rect x="230" y="60" width="130" height="56" class="box-accent"/>
  <text x="295" y="84" text-anchor="middle" class="label">batch</text>
  <text x="295" y="102" text-anchor="middle" class="sub">candidate: max id</text>
  <rect x="420" y="60" width="130" height="56" class="box"/>
  <text x="485" y="84" text-anchor="middle" class="label">produce</text>
  <text x="485" y="102" text-anchor="middle" class="sub">runtime</text>
  <rect x="600" y="60" width="110" height="56" class="box"/>
  <text x="655" y="84" text-anchor="middle" class="label">ack</text>
  <text x="655" y="102" text-anchor="middle" class="sub">mark rows</text>
  <path d="M170 88 L222 88" class="edge" marker-end="url(#arrpgsrc)"/>
  <path d="M360 88 L412 88" class="edge" marker-end="url(#arrpgsrc)"/>
  <path d="M550 88 L592 88" class="edge" marker-end="url(#arrpgsrc)"/>
  <text x="20" y="168" class="label">cdc</text>
  <rect x="20" y="180" width="150" height="56" class="box"/>
  <text x="95" y="204" text-anchor="middle" class="label">peek slot</text>
  <text x="95" y="222" text-anchor="middle" class="sub">no consume</text>
  <rect x="230" y="180" width="130" height="56" class="box-accent"/>
  <text x="295" y="204" text-anchor="middle" class="label">batch</text>
  <text x="295" y="222" text-anchor="middle" class="sub">candidate: lsn</text>
  <rect x="420" y="180" width="130" height="56" class="box"/>
  <text x="485" y="204" text-anchor="middle" class="label">produce</text>
  <text x="485" y="222" text-anchor="middle" class="sub">runtime</text>
  <rect x="600" y="180" width="110" height="56" class="box"/>
  <text x="655" y="204" text-anchor="middle" class="label">ack</text>
  <text x="655" y="222" text-anchor="middle" class="sub">advance slot</text>
  <path d="M170 208 L222 208" class="edge" marker-end="url(#arrpgsrc)"/>
  <path d="M360 208 L412 208" class="edge" marker-end="url(#arrpgsrc)"/>
  <path d="M550 208 L592 208" class="edge" marker-end="url(#arrpgsrc)"/>
</svg>
</div>

## Quick start

CDC on one table, routed into a topic per tenant.

```toml
type = "source"
key = "orders_cdc"
enabled = true
version = 0
name = "Orders CDC"
path = "libpicomq_connector_postgres_source"

[[topics]]
topic = { strategy = "field", path = "data.tenant", template = "orders.{value}" }
schema = "json"
batch_length = 1000
linger_time = "5ms"
create_topics = true

[plugin_config]
connection_string = "postgres://user:pass@db:5432/app"
mode = "cdc"
tables = ["public.orders"]
replication_slot = "picomq_orders"
```

Polling on two tables, one topic.

```toml
[[topics]]
topic = "events"
schema = "json"

[plugin_config]
connection_string = "postgres://user:pass@db:5432/app"
mode = "polling"
tables = ["public.events", "public.audit"]
tracking_column = "id"
poll_interval = "5s"
batch_size = 500
```

## Polling mode

Each `poll()` sleeps `poll_interval`, then for each table in `tables` runs the following query.

```sql
SELECT * FROM "public"."events"
WHERE "id" > <last offset for this table>
ORDER BY "id" ASC
LIMIT <batch_size>
```

| Behaviour | Detail |
| --- | --- |
| Cursor | The highest `tracking_column` value seen, kept per table in the state store |
| First run | No `WHERE` clause, so the whole table from the beginning. `initial_offset` sets a starting point instead |
| Column type | Any type that sorts and compares with `>`. Integers and timestamps are typical. Strings work but sort lexically |
| `processed_column` | Adds `AND "<column>" = FALSE` to the query, and on ack sets the column to `TRUE` for the rows read |
| `delete_after_read` | On ack, deletes the rows read by `primary_key_column` |
| `custom_query` | Replaces the generated query. Placeholders `$table`, `$offset`, `$limit`, `$now`, `$now_unix` are substituted |

Polling sees inserts and, if `tracking_column` is an updated-at timestamp, updates. It does not see deletes.

### Stage and apply in polling

The per-table cursor and any pending `UPDATE` or `DELETE` are staged when the batch is returned and applied only on ack. A crash between produce and ack means the rows are read again on restart, which is the at-least-once promise.

`delete_after_read` and `processed_column` need a `primary_key_column` if the primary key is not `tracking_column`. The cleanup statement is `WHERE <pk> IN (...)` over every row in the batch.

## CDC mode

CDC uses the `test_decoding` output plugin that ships with PostgreSQL, through a logical replication slot. Each `poll()` sleeps `poll_interval` and then peeks up to `batch_size` changes with `pg_logical_slot_peek_changes`. Peeking leaves the slot where it is. The slot is advanced to the last LSN in the batch only on ack.

| Behaviour | Detail |
| --- | --- |
| Slot | `replication_slot`, created on `open()` if it does not exist. Default `picomq_slot` |
| Existing slot | Reused if its plugin is `test_decoding`. Rejected with an error otherwise |
| Filtering | Only tables in `tables` and operations in `capture_operations` are emitted. Everything else in the WAL is peeked and skipped |
| Transactions | `BEGIN` and `COMMIT` markers are dropped. Changes inside are emitted in commit order |
| Empty poll | Nothing is staged and the slot is not touched |

::: warning Slot retention
A replication slot holds WAL until it is advanced. A stopped or broken source keeps its slot, and the primary keeps every WAL segment since, until disk fills or `max_slot_wal_keep_size` cuts it off. Drop the slot when decommissioning a source: `SELECT pg_drop_replication_slot('picomq_slot')`.
:::

Each source needs its own slot. Two sources sharing a slot would each advance it past changes the other had not seen.

## Configuration

All keys go under `[plugin_config]`.

### Common

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `connection_string` | string | required | A libpq URL. Redacted in the API |
| `mode` | string | required | `polling` or `cdc` |
| `tables` | list | required | Tables to read, schema-qualified where needed. In CDC, an empty list captures every table |
| `poll_interval` | duration | `10s` | Sleep between polls |
| `batch_size` | int | `1000` | Rows per table per poll, or changes per poll |
| `max_connections` | int | `10` | Pool size |
| `include_metadata` | bool | `true` | Polling only. `false` nests the row one level deeper, at `data.data`, with the envelope otherwise unchanged. Leave it on |
| `snake_case_columns` | bool | `false` | Convert `camelCase` column names to `snake_case` in the output |
| `max_retries` | int | `3` | Attempts for each query |
| `retry_delay` | duration | `1s` | Delay between attempts |
| `verbose_logging` | bool | `false` | Log every poll at `info` |

### Polling only

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `tracking_column` | string | `id` | Column the cursor follows |
| `initial_offset` | string | none | Starting cursor for tables with no saved state |
| `primary_key_column` | string | `tracking_column` | Column used in cleanup statements |
| `processed_column` | string | none | Boolean column to filter on and set `TRUE` after ack |
| `delete_after_read` | bool | `false` | Delete rows after ack |
| `custom_query` | string | none | Full replacement query with placeholders |
| `payload_column` | string | none | Emit one column as the whole payload instead of the row |
| `payload_format` | string | `json` | Encoding of `payload_column`. `json`, `json_direct`, `bytea`, `text` |

### CDC only

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `replication_slot` | string | `picomq_slot` | Slot name, one per source |
| `capture_operations` | list | all | Subset of `INSERT`, `UPDATE`, `DELETE` |
| `cdc_backend` | string | `builtin` | Only `builtin` is available in the shipped build |

## Output

Every record is a JSON object with the same envelope in both modes.

```json
{
  "table_name": "orders",
  "operation_type": "UPDATE",
  "timestamp": "2026-09-03T21:15:04.118Z",
  "data": { "id": 7, "tenant": "acme", "total": 42.5 },
  "old_data": { "id": 7 }
}
```

| Field | Polling | CDC |
| --- | --- | --- |
| `table_name` | The table, as configured | The table, unqualified |
| `operation_type` | Always `SELECT` | `INSERT`, `UPDATE` or `DELETE` |
| `timestamp` | Time the row was read | Time the change was read, not the commit time |
| `data` | The row | New tuple for insert and update, old tuple for delete |
| `old_data` | Absent | Old key for updates with a changed key, absent otherwise |

Routing rules address fields inside the envelope, so `path = "data.tenant"` rather than `path = "tenant"`. A sink that wants the bare row can apply `unwrap_envelope` with `field = "data"`.

Records carry no key and no headers. Add a `key` route or a transform if a sink needs them.

### Single-column payloads

In polling mode, `payload_column` emits one column's value as the entire record and drops the envelope. This is for tables that already hold a serialized message, an outbox table for instance.

| `payload_format` | Column type | Topic `schema` |
| --- | --- | --- |
| `json` | `text` or `jsonb` holding JSON | `json` |
| `json_direct` | `jsonb`, passed through without re-encoding | `json` |
| `text` | any text type | `text` |
| `bytea` | `bytea` | `raw` |

## State

| Mode | Stored in the runtime's state store | Stored in Postgres |
| --- | --- | --- |
| Polling | Per-table cursor, rows processed, last poll time | `processed_column` flags or deleted rows |
| CDC | Nothing that matters for resumption | The slot position |

Losing the state file in polling mode restarts every table from `initial_offset` or the beginning. Losing it in CDC mode changes nothing, since the slot is the cursor. Dropping the slot loses every change since it was last advanced.

## Requirements

### Both modes

- Network access from the runtime to the database.
- `SELECT` on the tables.

### Polling

- `UPDATE` when `processed_column` is set, `DELETE` when `delete_after_read` is on.
- An index on `tracking_column`, or every poll is a sequential scan.

### CDC

- `wal_level = logical` in `postgresql.conf`. The source checks this on `open()` and refuses to start otherwise.
- A role with `REPLICATION`, or the `rds_replication` role on RDS and Aurora.
- `max_replication_slots` high enough for one slot per source.
- The `test_decoding` plugin, which is part of every standard PostgreSQL distribution.

## Troubleshooting

| Symptom | Cause |
| --- | --- |
| `WAL level must be 'logical' for CDC` | Set `wal_level = logical` and restart PostgreSQL |
| `Replication slot ... already exists with plugin ...` | The slot was created by something else. Use a different `replication_slot` or drop it |
| CDC produces nothing though the table changes | The table is not in `tables`, or the operation is not in `capture_operations`. Check with `SELECT * FROM pg_logical_slot_peek_changes('picomq_slot', NULL, 10)` |
| Polling re-reads the same rows every poll | `tracking_column` is not monotonic, or the state file is not persisted between restarts |
| Polling misses updates | Polling by an insert id only sees new rows. Track an `updated_at` column or use CDC |
| Disk growing on the primary | A slot is held by a stopped source. Drop it or restart the source |
| Rows arrive twice after a restart | Expected after a crash between produce and ack. See [Delivery guarantees](/docs/connectors/delivery) |

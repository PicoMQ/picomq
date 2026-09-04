# Sinks

A sink turns topics into writes against some other system. The runtime is the Kafka consumer, with everything that implies about groups, offsets and subscriptions. The plugin is a function from a batch of records to a write.

## The loop

Each `[[topics]]` block in a sink definition becomes one consumer in one consumer group. The runtime pulls records off it and buckets them by topic.

<div class="pico-diagram">
<svg viewBox="0 40 690 200" width="690" role="img" aria-label="A sink batch flows from fetch to decode and transforms to the plugin's consume call. Only on success does the offset get stored and committed. On failure the offset is untouched and the batch is retried or the sink stops.">
  <defs>
    <marker id="arrs" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M0 0.5 L7.5 4 L0 7.5 Z" class="arrow"/>
    </marker>
  </defs>
  <rect x="20" y="70" width="120" height="56" class="box"/>
  <text x="80" y="94" text-anchor="middle" class="label">fetch</text>
  <text x="80" y="112" text-anchor="middle" class="sub">bucket per topic</text>
  <rect x="190" y="70" width="120" height="56" class="box"/>
  <text x="250" y="94" text-anchor="middle" class="label">decode</text>
  <text x="250" y="112" text-anchor="middle" class="sub">transforms</text>
  <rect x="360" y="70" width="120" height="56" class="box-accent"/>
  <text x="420" y="94" text-anchor="middle" class="label">consume</text>
  <text x="420" y="112" text-anchor="middle" class="sub">plugin writes</text>
  <rect x="530" y="70" width="140" height="56" class="box"/>
  <text x="600" y="94" text-anchor="middle" class="label">commit</text>
  <text x="600" y="112" text-anchor="middle" class="sub">offset after batch</text>
  <rect x="360" y="170" width="120" height="50" class="box"/>
  <text x="420" y="191" text-anchor="middle" class="label">retry or stop</text>
  <text x="420" y="208" text-anchor="middle" class="sub">offset unchanged</text>
  <path d="M140 98 L182 98" class="edge" marker-end="url(#arrs)"/>
  <path d="M310 98 L352 98" class="edge" marker-end="url(#arrs)"/>
  <path d="M480 98 L522 98" class="edge" marker-end="url(#arrs)"/>
  <path d="M420 126 L420 162" class="edge-soft" marker-end="url(#arrs)"/>
  <text x="500" y="62" class="sub">Ok</text>
  <text x="430" y="150" class="sub">Err</text>
</svg>
</div>

A bucket is flushed when either of two things happens, whichever comes first.

- It reaches `batch_length` records.
- `poll_interval` passes with nothing new arriving.

A busy topic moves in full batches and a quiet one is not held back. Each flush is decoded according to `schema`, run through the [transforms](/docs/connectors/transforms), and handed to the plugin's `consume()` as one batch from one topic.

The batch carries everything a plugin needs to build a stable identity for each record.

| Level | Fields |
| --- | --- |
| Batch | `topic`, `partition`, `schema` |
| Record | `offset`, `timestamp`, `key`, `headers`, decoded `payload` |

Sinks that write to stores with primary keys use `topic:partition:offset` as the record id for exactly this reason.

## Commit after the write

The consumer runs with auto-commit off and offset auto-store off. Nothing advances on a timer.

- When `consume()` returns success, the runtime stores the offset just past the batch and commits it to the group.
- When `consume()` returns an error, the same batch is handed to the plugin again, up to five times, with backoff from 200 ms to 5 s. The offset does not move.
- On the fifth failure the consumer stops, commits whatever earlier batches had confirmed, and marks the sink `Error`. The failed batch is the first thing delivered on restart.
- On shutdown, clean or on an error path, the runtime flushes what it has buffered and performs one synchronous commit.

This is what the SDK's `Error` type is for. A plugin returns `Err` when the write did not happen, and the runtime treats the offset accordingly. A plugin that swallows a failed write and returns `Ok` is the one way to lose data through a sink, so the shipped sinks are careful to propagate.

## Subscriptions

A `[[topics]]` block subscribes in one of three ways.

| Setting | Behaviour |
| --- | --- |
| `topics = ["a", "b"]` | Explicit list |
| `pattern = 'orders\..*'` | Regular expression matched against the broker's topic list. Anchored with `^` if you did not. Re-evaluated every two seconds, so a topic created after the sink started is picked up within that window |
| Both | The union |

A pattern is how a sink follows a source that [fans out](/docs/connectors/routing) into topics nobody named in advance.

<div class="pico-diagram">
<svg viewBox="0 20 690 200" width="690" role="img" aria-label="A sink with a pattern subscription. At time zero three topics match. A fourth topic is created later and appears in the consumer's assignment within the two second metadata refresh.">
  <defs>
    <marker id="arrp" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M0 0.5 L7.5 4 L0 7.5 Z" class="arrow"/>
    </marker>
  </defs>
  <rect x="20" y="90" width="150" height="56" class="box"/>
  <text x="95" y="114" text-anchor="middle" class="label">sink consumer</text>
  <text x="95" y="132" text-anchor="middle" class="sub">pattern orders\..*</text>
  <rect x="270" y="50" width="120" height="32" class="box-accent"/>
  <text x="330" y="71" text-anchor="middle" class="label">orders.eu</text>
  <rect x="270" y="102" width="120" height="32" class="box-accent"/>
  <text x="330" y="123" text-anchor="middle" class="label">orders.us</text>
  <rect x="270" y="154" width="120" height="32" fill="none" class="edge-soft" stroke-dasharray="4 4"/>
  <text x="330" y="175" text-anchor="middle" class="label">orders.apac</text>
  <path d="M262 66 L170 108" class="edge" marker-end="url(#arrp)"/>
  <path d="M262 118 L170 118" class="edge" marker-end="url(#arrp)"/>
  <path d="M262 170 L170 128" class="edge-soft" marker-end="url(#arrp)"/>
  <rect x="470" y="86" width="200" height="64" class="box"/>
  <text x="570" y="108" text-anchor="middle" class="label">broker metadata</text>
  <text x="570" y="126" text-anchor="middle" class="sub">refreshed every 2s</text>
  <text x="570" y="142" text-anchor="middle" class="sub">new topics join</text>
  <path d="M470 118 L398 118" class="edge-soft" marker-end="url(#arrp)"/>
  <text x="330" y="40" text-anchor="middle" class="sub">matching at start</text>
  <text x="330" y="204" text-anchor="middle" class="sub">created later, picked up automatically</text>
</svg>
</div>

Each block gets its own consumer group, `picomq-connect-sink-<key>` unless `consumer_group` says otherwise. Since PicoMQ topics have a single partition, one consumer per group owns every topic it is subscribed to.

- Running two runtimes with the same sink definition does not spread load. It makes one of them idle.
- Scaling a sink means splitting its topics across definitions with distinct keys.
- `auto_offset_reset` is `earliest` by default, so a new sink reads a topic from the beginning. `latest` starts at the tail.

## Destinations

Most sinks write to a named place: a table, a collection, an index, a measurement, a key prefix, a URL. That name is a template resolved once per topic.

| Template | Result |
| --- | --- |
| `target_table = "events"` | Every subscribed topic into one table |
| `target_table = "events_{topic_segment[-1]}"` | One table per topic |

Sinks that can create their destination do so on the first batch for a new topic and remember it. Doris, Iceberg and Delta cannot, and expect it to exist. [Routing and templating](/docs/connectors/routing) has the placeholder syntax and sanitisation rules.

Every sink that adds metadata to what it writes uses the same field names.

| Field | Content |
| --- | --- |
| `pico_topic` | Topic the record came from |
| `pico_partition` | Always `0` on PicoMQ |
| `pico_offset` | Record offset in the topic |
| `pico_timestamp` | Record timestamp, epoch milliseconds |
| `pico_key` | Record key when present, base64 where the destination has no binary type |

Headers, where kept, arrive as strings when they are valid UTF-8 and base64 otherwise.

## Definition

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
```

| Field | Meaning |
| --- | --- |
| `topics`, `pattern` | The subscription, see above |
| `schema` | Decoding of the payload: `json`, `raw`, `text`, `proto`, `flatbuffer`, `avro` |
| `avro_schema_json`, `avro_schema_path` | The Avro schema, when `schema = "avro"` |
| `batch_length` | Flush when a topic bucket reaches this many records, default 1000 |
| `poll_interval` | Flush every bucket after this long without new records, default 100 ms |
| `consumer_group` | Override the default `picomq-connect-sink-<key>` |
| `auto_offset_reset` | `earliest` or `latest` for a group with no committed offset |
| `properties` | Any other librdkafka consumer setting |

The catalog pages list each sink's `plugin_config` in full and state its behaviour under replay.

| Light, in the image | Heavy, released as artifacts |
| --- | --- |
| [Postgres](/docs/connectors/sinks/postgres), [ClickHouse](/docs/connectors/sinks/clickhouse), [Elasticsearch](/docs/connectors/sinks/elasticsearch), [Quickwit](/docs/connectors/sinks/quickwit), [MongoDB](/docs/connectors/sinks/mongodb), [Meilisearch](/docs/connectors/sinks/meilisearch), [SurrealDB](/docs/connectors/sinks/surrealdb), [InfluxDB](/docs/connectors/sinks/influxdb), [S3](/docs/connectors/sinks/s3), [HTTP](/docs/connectors/sinks/http), [stdout](/docs/connectors/sinks/stdout) | [Doris](/docs/connectors/sinks/doris), [Iceberg](/docs/connectors/sinks/iceberg), [Delta](/docs/connectors/sinks/delta), [Redshift](/docs/connectors/sinks/redshift) |

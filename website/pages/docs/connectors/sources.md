# Sources

A source turns some other system into topics. The plugin knows how to read that system. The runtime knows how to get what it read into PicoMQ and how to remember where it got to. The plugin never talks to the broker, and the runtime never talks to the external system.

## The loop

The runtime calls the plugin's `poll()` in a loop. Each call returns a batch of records and the state the plugin would like remembered if this batch makes it through.

<div class="pico-diagram">
<svg viewBox="0 40 690 200" width="690" role="img" aria-label="A source poll returns a batch and a candidate state. The runtime produces the batch to PicoMQ, then saves the state and acks the plugin, which applies its staged work. On a failed produce the runtime nacks and the plugin discards the candidate.">
  <defs>
    <marker id="arro" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M0 0.5 L7.5 4 L0 7.5 Z" class="arrow"/>
    </marker>
  </defs>
  <rect x="20" y="70" width="120" height="56" class="box-accent"/>
  <text x="80" y="94" text-anchor="middle" class="label">poll</text>
  <text x="80" y="112" text-anchor="middle" class="sub">batch, candidate</text>
  <rect x="190" y="70" width="120" height="56" class="box"/>
  <text x="250" y="94" text-anchor="middle" class="label">produce</text>
  <text x="250" y="112" text-anchor="middle" class="sub">route, send, ack</text>
  <rect x="360" y="70" width="120" height="56" class="box"/>
  <text x="420" y="94" text-anchor="middle" class="label">save state</text>
  <text x="420" y="112" text-anchor="middle" class="sub">file or HTTP</text>
  <rect x="530" y="70" width="140" height="56" class="box-accent"/>
  <text x="600" y="94" text-anchor="middle" class="label">ack, apply</text>
  <text x="600" y="112" text-anchor="middle" class="sub">apply side effects</text>
  <rect x="190" y="170" width="120" height="50" class="box"/>
  <text x="250" y="191" text-anchor="middle" class="label">nack, discard</text>
  <text x="250" y="208" text-anchor="middle" class="sub">poll re-reads</text>
  <path d="M140 98 L182 98" class="edge" marker-end="url(#arro)"/>
  <path d="M310 98 L352 98" class="edge" marker-end="url(#arro)"/>
  <path d="M480 98 L522 98" class="edge" marker-end="url(#arro)"/>
  <path d="M250 126 L250 162" class="edge-soft" marker-end="url(#arro)"/>
  <text x="330" y="62" class="sub">all delivered</text>
  <text x="260" y="150" class="sub">any failed</text>
</svg>
</div>

For each batch the runtime does the following in order.

1. Decodes the records according to `schema` and runs the [transforms](/docs/connectors/transforms).
2. Computes a destination topic for each record under the [routing rule](/docs/connectors/routing), creating topics that do not exist yet if `create_topics` is on.
3. Produces every record through one Kafka producer and waits for a delivery report on each.
4. If all were acknowledged, writes the state to the state store and calls the plugin back with `Ack`.
5. If any failed, calls the plugin back with `Nack` and writes nothing.

Pacing belongs to the plugin. `poll()` is called again as soon as the previous batch is resolved, so a plugin with nothing new sleeps for its own interval inside the call rather than returning empty batches in a tight loop. An empty batch carries no state and is acknowledged without touching the store.

## Stage and apply

The important word above is *candidate*. A plugin that advanced its cursor inside `poll()`, before anything was produced, would lose records whenever the process died between the read and the acknowledgement.

So a well-behaved source keeps two states.

| State | Meaning | Changes when |
| --- | --- | --- |
| Committed | What the plugin has been told was delivered | `on_batch_result(Ack)` |
| Candidate | What this batch would make true if it succeeds | Every `poll()`, travels in the returned `state` |

On `Ack` the plugin promotes the candidate to committed and performs the side effect that goes with it. On `Nack` it drops the candidate and the next `poll()` reads the same data again.

<div class="pico-diagram">
<svg viewBox="0 30 690 230" width="690" role="img" aria-label="Inside the plugin, poll reads from the external system and builds a candidate state without touching committed state. Ack promotes candidate to committed and applies side effects. Nack drops the candidate.">
  <defs>
    <marker id="arrsa" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M0 0.5 L7.5 4 L0 7.5 Z" class="arrow"/>
    </marker>
  </defs>
  <rect x="20" y="50" width="650" height="190" fill="none" class="edge-soft" stroke-dasharray="4 4"/>
  <text x="345" y="70" text-anchor="middle" class="sub">source plugin</text>
  <rect x="50" y="100" width="150" height="56" class="box-accent"/>
  <text x="125" y="124" text-anchor="middle" class="label">committed state</text>
  <text x="125" y="142" text-anchor="middle" class="sub">lsn 0/1A3F</text>
  <rect x="270" y="100" width="150" height="56" class="box"/>
  <text x="345" y="124" text-anchor="middle" class="label">candidate state</text>
  <text x="345" y="142" text-anchor="middle" class="sub">lsn 0/1B90</text>
  <rect x="490" y="100" width="150" height="56" class="box"/>
  <text x="565" y="124" text-anchor="middle" class="label">external system</text>
  <text x="565" y="142" text-anchor="middle" class="sub">peek, no advance</text>
  <path d="M490 118 L428 118" class="edge" marker-end="url(#arrsa)"/>
  <text x="459" y="108" text-anchor="middle" class="sub">poll</text>
  <path d="M270 128 L208 128" class="edge" marker-end="url(#arrsa)"/>
  <text x="239" y="118" text-anchor="middle" class="sub">Ack</text>
  <path d="M125 156 L125 190 L565 190 L565 156" class="edge" marker-end="url(#arrsa)"/>
  <text x="345" y="206" text-anchor="middle" class="sub">Ack: advance slot to 0/1B90</text>
  <path d="M345 156 L345 176" class="edge-soft" marker-end="url(#arrsa)"/>
  <text x="380" y="176" class="sub">Nack: drop</text>
</svg>
</div>

The Postgres source is the reference for the pattern.

- In CDC mode it peeks the replication slot without consuming, records the last LSN as the candidate, and advances the slot only on `Ack`.
- In polling mode the candidate is the highest tracking-column value it saw, and any `processed_column` update or `delete_after_read` is held until `Ack`.

The runtime enforces ordering on its side too. Batches are acknowledged one at a time, a later batch cannot save state ahead of an earlier one that failed, and after a `Nack` an empty batch is not allowed to persist state it did not earn.

## State

What the plugin returns as state is opaque to the runtime, a byte blob it stores and hands back unchanged. Two stores are available.

| `state.storage` | Where | Write path | Suits |
| --- | --- | --- | --- |
| `file` | One file per source under `state.path`, named by `key` | Temporary file then rename, so a crash mid-write keeps the previous checkpoint | A persistent volume |
| `http` | An endpoint under `[state.http]` | `GET` loads, `PUT` saves, retried with backoff, idempotency key on every write | No volume, or a control plane that wants to see checkpoints |

On start the runtime loads the blob and passes it to the plugin's constructor. That is how a Postgres source knows its last LSN and a random source its last sequence number. A source with no stored state starts from whatever its plugin defines as the beginning.

## When things go wrong

| Event | What the runtime does |
| --- | --- |
| `poll()` returns an error | Logs it and calls `poll()` again. The plugin is expected to have retried internally where that makes sense |
| A record cannot be routed and has no `fallback` | Nacks the batch. The source re-reads it |
| Produce fails, broker down or topic creation failed | Nacks the batch, backs off, polls again |
| Thirty consecutive nacks | Stops the poll task and sets status `Error`. Visible at `/sources/{key}` and in the `picomq_connectors_sources_running` gauge |
| State save fails after a successful produce | Nacks the batch and sets `Error`. The plugin re-reads, so PicoMQ sees the batch twice |
| A batch succeeds after a run of failures | Clears `Error`, status returns to `Running` |

None of these lose data. Every path that gives up leaves the store and the plugin's committed state where the last acknowledged batch put them. What they can produce is duplicates, and [Delivery guarantees](/docs/connectors/delivery) has the precise window.

## Definition

A source definition names the plugin, one or more producers, and the plugin's own configuration.

```toml
type = "source"
key = "orders_cdc"
enabled = true
version = 0
name = "Orders CDC"
path = "libpicomq_connector_postgres_source"

[[topics]]
topic = { strategy = "field", path = "tenant", template = "orders.{value}" }
schema = "json"
batch_length = 1000
linger_time = "5ms"
create_topics = true

[plugin_config]
connection_string = "postgres://user:pass@db:5432/app"
mode = "cdc"
tables = ["public.orders"]
```

Each `[[topics]]` block is a producer with its own routing rule, batch size and linger, so one source can feed several topic families.

| Field | Meaning |
| --- | --- |
| `topic` | A literal name or a [routing rule](/docs/connectors/routing) |
| `schema` | Encoding on the wire into PicoMQ: `json`, `raw`, `text`, `proto`, `flatbuffer`, `avro` |
| `avro_schema_json`, `avro_schema_path` | The Avro schema, when `schema = "avro"` |
| `batch_length`, `linger_time` | Producer batching, passed through to librdkafka |
| `create_topics` | Create missing destinations through the admin API |
| `partitions`, `replication_factor` | Accepted only as `1`, since PicoMQ topics have one partition |
| `properties` | Any other librdkafka producer setting |

The catalog pages list each source's `plugin_config` in full: [Postgres](/docs/connectors/sources/postgres), [Elasticsearch](/docs/connectors/sources/elasticsearch), [InfluxDB](/docs/connectors/sources/influxdb) and a [random](/docs/connectors/sources/random) generator for testing.

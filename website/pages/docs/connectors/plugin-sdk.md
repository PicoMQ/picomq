# Writing a plugin

A connector is a Rust crate that implements one trait and exports it with one macro. The result is a shared library the runtime loads by path. Nothing about PicoMQ, Kafka, offsets or checkpoints appears in the plugin, since all of that lives in the runtime.

This page builds a sink and a source from scratch, then covers the parts that separate a working plugin from a correct one.

<div class="pico-diagram">
<svg viewBox="0 30 720 200" width="720" role="img" aria-label="A plugin crate implements the Sink or Source trait and invokes the export macro. Cargo builds it as a cdylib. The runtime dlopens the library and calls its C entry points, which dispatch to the trait methods.">
  <defs>
    <marker id="arrsdk" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M0 0.5 L7.5 4 L0 7.5 Z" class="arrow"/>
    </marker>
  </defs>
  <rect x="20" y="60" width="180" height="120" class="box"/>
  <text x="110" y="84" text-anchor="middle" class="label">your crate</text>
  <text x="110" y="108" text-anchor="middle" class="sub">impl Sink for MySink</text>
  <text x="110" y="128" text-anchor="middle" class="sub">sink_connector!(MySink)</text>
  <text x="110" y="148" text-anchor="middle" class="sub">crate-type = ["cdylib"]</text>
  <rect x="270" y="90" width="180" height="60" class="box-accent"/>
  <text x="360" y="114" text-anchor="middle" class="label">libmy_sink.so</text>
  <text x="360" y="134" text-anchor="middle" class="sub">open, consume, close</text>
  <rect x="520" y="60" width="180" height="120" class="box"/>
  <text x="610" y="84" text-anchor="middle" class="label">pico-connectors</text>
  <text x="610" y="108" text-anchor="middle" class="sub">dlopen by path</text>
  <text x="610" y="128" text-anchor="middle" class="sub">hands batches over C ABI</text>
  <text x="610" y="148" text-anchor="middle" class="sub">owns offsets and state</text>
  <path d="M200 120 L262 120" class="edge" marker-end="url(#arrsdk)"/>
  <path d="M450 120 L512 120" class="edge" marker-end="url(#arrsdk)"/>
  <text x="231" y="110" text-anchor="middle" class="sub">cargo build</text>
  <text x="481" y="110" text-anchor="middle" class="sub">load</text>
</svg>
</div>

## The crate

A plugin crate depends on `picomq-connector-sdk` and builds as a `cdylib`. The `lib` target also keeps `lib` so unit tests can link it normally.

```toml
[package]
name = "my-connector-sink"
version = "0.1.0"
edition = "2024"

[lib]
name = "my_connector_sink"
crate-type = ["cdylib", "lib"]

[dependencies]
async-trait = "0.1"
dashmap = "6"
picomq-connector-sdk = { path = "../../sdk" }
serde = { version = "1", features = ["derive"] }
tokio = { version = "1", features = ["sync"] }
tracing = "0.1"

[lints.rust]
unsafe_code = "allow"
```

| Item | Why |
| --- | --- |
| `cdylib` | The runtime loads a shared object, not a Rust library |
| `dashmap` | The export macro keeps a per-instance table in it |
| `unsafe_code = "allow"` | The macro emits `extern "C"` entry points |
| `[lib] name` | Becomes the file name, `libmy_connector_sink.so`, which is what a definition's `path` refers to |

## A sink

The whole contract is the `Sink` trait and the `sink_connector!` macro.

```rust
use async_trait::async_trait;
use picomq_connector_sdk::{
    ConsumedMessage, Error, MessagesMetadata, Sink, TopicMetadata, sink_connector,
};
use serde::{Deserialize, Serialize};
use tracing::info;

sink_connector!(CountingSink);

#[derive(Debug, Serialize, Deserialize)]
pub struct CountingSinkConfig {
    prefix: Option<String>,
}

pub struct CountingSink {
    id: u32,
    prefix: String,
}

impl CountingSink {
    pub fn new(id: u32, config: CountingSinkConfig) -> Self {
        Self {
            id,
            prefix: config.prefix.unwrap_or_else(|| "batch".to_owned()),
        }
    }
}

#[async_trait]
impl Sink for CountingSink {
    async fn open(&mut self) -> Result<(), Error> {
        info!("Opened counting sink {}", self.id);
        Ok(())
    }

    async fn consume(
        &self,
        topic_metadata: &TopicMetadata,
        messages_metadata: MessagesMetadata,
        messages: Vec<ConsumedMessage>,
    ) -> Result<(), Error> {
        info!(
            "{}: {} records from {} at offset {}",
            self.prefix,
            messages.len(),
            topic_metadata.topic,
            messages_metadata.current_offset
        );
        Ok(())
    }

    async fn close(&mut self) -> Result<(), Error> {
        Ok(())
    }
}
```

The lifecycle the runtime drives is small.

| Call | When | Contract |
| --- | --- | --- |
| `new(id, config)` | Once, at load. `config` is the definition's `plugin_config` deserialized into your struct | Cheap. No I/O |
| `open()` | Once, after `new` | Connect, validate, create what must exist. An error here fails the connector |
| `consume(topic, metadata, records)` | Once per batch. One topic per call | `Ok` only after the write is durable. `Err` for anything else |
| `close()` | Once, on shutdown or restart | Flush and disconnect |

`consume` receives the following.

| Argument | Fields |
| --- | --- |
| `topic_metadata` | `topic` |
| `messages_metadata` | `partition`, `current_offset` of the first record, `schema` the payload was decoded with |
| `messages` | Each with `offset`, `timestamp` in epoch milliseconds, `key`, `headers`, and a decoded `payload` |

`payload` is a `Payload` enum. Match on the variants you support and return `Error::InvalidPayloadType` for the rest.

```rust
match &message.payload {
    Payload::Json(value) => { /* simd_json::OwnedValue */ }
    Payload::Text(text) => { /* String */ }
    Payload::Raw(bytes) => { /* Vec<u8> */ }
    other => return Err(Error::InvalidPayloadType),
}
```

## A source

A source implements `Source` and exports with `source_connector!`. Its constructor takes a third argument, the state the runtime saved last time.

```rust
use async_trait::async_trait;
use picomq_connector_sdk::source::SourceBatchResult;
use picomq_connector_sdk::{
    ConnectorState, Error, ProducedMessage, ProducedMessages, Schema, Source, source_connector,
};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

source_connector!(CounterSource);

#[derive(Debug, Serialize, Deserialize)]
pub struct CounterSourceConfig {
    batch: Option<usize>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct State {
    next: u64,
}

pub struct CounterSource {
    id: u32,
    batch: usize,
    committed: Mutex<State>,
    pending: Mutex<Option<State>>,
}

impl CounterSource {
    pub fn new(id: u32, config: CounterSourceConfig, state: Option<ConnectorState>) -> Self {
        let restored = state
            .and_then(|state| state.deserialize::<State>("counter", id))
            .unwrap_or_default();
        Self {
            id,
            batch: config.batch.unwrap_or(10),
            committed: Mutex::new(restored),
            pending: Mutex::new(None),
        }
    }
}

#[async_trait]
impl Source for CounterSource {
    async fn open(&mut self) -> Result<(), Error> {
        Ok(())
    }

    async fn poll(&self) -> Result<ProducedMessages, Error> {
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        let start = self.committed.lock().await.next;
        let messages = (start..start + self.batch as u64)
            .map(|n| ProducedMessage {
                key: None,
                timestamp: None,
                headers: None,
                payload: format!("{{\"n\":{n}}}").into_bytes(),
            })
            .collect();
        let candidate = State { next: start + self.batch as u64 };
        let state = ConnectorState::serialize(&candidate, "counter", self.id);
        *self.pending.lock().await = Some(candidate);
        Ok(ProducedMessages { schema: Schema::Json, messages, state })
    }

    async fn on_batch_result(&self, result: SourceBatchResult) -> Result<(), Error> {
        let pending = self.pending.lock().await.take();
        if let (SourceBatchResult::Ack, Some(candidate)) = (result, pending) {
            *self.committed.lock().await = candidate;
        }
        Ok(())
    }

    async fn close(&mut self) -> Result<(), Error> {
        Ok(())
    }
}
```

| Call | When | Contract |
| --- | --- | --- |
| `new(id, config, state)` | Once, at load. `state` is `None` on first run | Restore the cursor. No I/O |
| `open()` | Once, after `new` | Connect, validate |
| `poll()` | In a loop, as soon as the previous batch resolves | Read, build a batch, stage a candidate state. Sleep here when idle |
| `on_batch_result(Ack)` | After every record was acknowledged and the state was saved | Promote the candidate, apply side effects |
| `on_batch_result(Nack)` | After a produce failure | Drop the candidate |
| `close()` | On shutdown or restart | Disconnect |

`ProducedMessages` carries a `schema` telling the runtime how to encode the payload bytes for PicoMQ, the `messages`, and an optional `state`. Return `state: None` for an empty batch, and the runtime saves nothing.

## Getting the source right

The example above follows the stage-and-apply pattern from the [Sources](/docs/connectors/sources) page, and the details matter.

- `poll()` never mutates `committed`. It only reads it and builds a candidate.
- The candidate goes two places: serialized into `ProducedMessages.state` for the runtime to store, and into `pending` for the plugin to apply on `Ack`.
- `on_batch_result` is the only place `committed` changes, and the only place side effects on the external system happen. Advancing a replication slot, marking rows processed, deleting consumed rows, all belong here under `Ack`.
- On `Nack`, dropping the candidate is the whole job. The next `poll()` reads from `committed` again and reproduces the same batch.

A source that advances its cursor inside `poll()` works until the first crash between produce and save, at which point it has lost a batch. The runtime cannot detect this. It is the one correctness property only the plugin can provide.

## Errors

The SDK's `Error` enum is what both traits return. The runtime does not distinguish variants, so pick the one that reads best in the log.

| Variant | Use for |
| --- | --- |
| `InvalidConfig`, `InvalidConfigValue(String)` | Rejected configuration in `new` or `open` |
| `Connection(String)` | Could not reach the external system |
| `WriteFailure(String)`, `CannotStoreData(String)` | A write that did not happen |
| `InvalidRecord`, `InvalidRecordValue(String)` | A record the plugin cannot represent |
| `InvalidPayloadType` | A `Payload` variant the plugin does not handle |
| `InitError(String)` | Anything else that fails `open` |

What the runtime does with an `Err` differs by side.

| From | Runtime response |
| --- | --- |
| Sink `consume()` | Same batch retried up to five times, then the sink stops with `Error`. Offset never moves past a failed batch |
| Source `poll()` | Logged, `poll()` called again |
| Either `open()` | Connector fails to start |

Never return `Ok` from `consume()` for a batch that was not written. A plugin can retry internally for as long as it likes, but its final answer has to be honest, because the offset commit depends on it.

## Configuration and secrets

`plugin_config` in the definition is deserialized straight into your config struct with serde, so `Option<T>` fields are optional and everything else is required. The runtime accepts TOML by default and JSON or YAML when `plugin_config_format` says so.

Operators can override any top-level field from the environment as `PICOMQ_CONNECTORS_<TYPE>_<KEY>_PLUGIN_CONFIG_<FIELD>`, which is how a connection string reaches a container without appearing in a file.

Fields holding credentials should be `secrecy::SecretString`, serialized through the SDK's `secret` module so they are redacted when the runtime's API returns the configuration.

```rust
use picomq_connector_sdk::secret::serialize_secret;
use secrecy::SecretString;

#[derive(Debug, Serialize, Deserialize)]
pub struct MySinkConfig {
    #[serde(serialize_with = "serialize_secret")]
    connection_string: SecretString,
}
```

## Templated destinations

A sink that writes to a named place should accept a `DestinationTemplate` rather than a `String`, so operators get `{topic}` and `{topic_segment[n]}` for free.

```rust
use picomq_connector_sdk::destination::DestinationTemplate;

#[derive(Debug, Serialize, Deserialize)]
pub struct MySinkConfig {
    table: DestinationTemplate,
}

let table = self.config.table.resolve(&topic_metadata.topic)?;
```

- `is_static()` tells you whether the template has placeholders, so `open()` can pre-create a fixed destination and leave dynamic ones to the first batch.
- Cache what you have created, keyed by the resolved name, so a busy topic does not pay a metadata round trip per batch.
- Sanitize the resolved name for your destination. Topic names allow `-` and `.` that most identifiers do not.

## Replay

Every plugin is handed a batch twice sooner or later. See [Delivery guarantees](/docs/connectors/delivery) for when.

A sink that writes to a keyed store should derive its record id from `topic_metadata.topic`, `messages_metadata.partition` and `message.offset`, and upsert on it. That makes a replayed batch a no-op. A sink that cannot key its writes should say so on its catalog page so operators know to deduplicate downstream.

## Logging

Use `tracing`. The export macro installs a subscriber in the plugin that forwards every event, with its level, target and message, into the runtime's own log output. `info!` and `warn!` in a plugin appear alongside the runtime's lines, in the same format, and honour the runtime's `RUST_LOG`. Include the connector id in messages where it helps, as the shipped plugins do.

## Panics

A panic in a plugin aborts the runtime process. There is no catching it across the C boundary. Treat every `unwrap()` on external I/O as a bug, return `Err` instead, and let the runtime's retry and restart machinery handle it.

## Testing

Because the crate also builds as `lib`, ordinary `#[cfg(test)]` unit tests work. The export macro compiles its `extern "C"` functions out under `cfg(test)`, so tests never touch the FFI layer.

For an end-to-end check, point a definition at your library's build output and run the runtime against a node.

```bash
cargo build -p my-connector-sink
PICOMQ_CONNECTORS_CONNECTORS__CONFIG_DIR=./my-connectors \
PICOMQ_CONNECTORS_KAFKA__BOOTSTRAP_SERVERS=localhost:9092 \
  cargo run -p picomq-connectors
```

With `path = "libmy_connector_sink"` in the definition, the runtime searches the executable's directory, the working directory, and the system library directories. During development `target/debug` is found through the working directory.

## Shipping

A plugin ships as its `.so`. Operators drop it into `/usr/local/lib` inside the `pico-connectors` image, or anywhere on the search path listed in [Operations](/docs/operations/connectors), and reference it by library name in a definition. Nothing needs rebuilding on the runtime side.

The [stdout sink](https://github.com/picomq/picomq/tree/main/connectors/sinks/stdout) and [random source](https://github.com/picomq/picomq/tree/main/connectors/sources/random) in the repository are the smallest complete examples of each side. The [Postgres source](https://github.com/picomq/picomq/tree/main/connectors/sources/postgres) is the reference for stage-and-apply against a real system.

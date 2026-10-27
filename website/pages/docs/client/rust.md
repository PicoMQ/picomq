# Rust client

`picomq-client` is the Rust SDK for the HTTP protocols. It speaks the native Pico protocol and [Durable Streams](/docs/design/protocols) behind one `StreamApi` trait, and adds a batching producer and consumer group members on Pico.

The crate depends only on `picomq-protocol`, the shared wire vocabulary, and the usual HTTP stack. It builds no part of the server.

## Install

```toml
[dependencies]
picomq-client = "0.1"
```

## Usage

```rust
use picomq_client::{connect, CreateOptions, Live, Protocol, ReadLimits};

#[tokio::main]
async fn main() -> picomq_client::Result<()> {
    let client = connect(Protocol::Pico, "http://127.0.0.1:4437")?;

    client
        .create("/orders/1042", &CreateOptions::new("application/json"))
        .await?;
    let ack = client
        .append("/orders/1042", None, &[r#"{"item":"widget"}"#.into()], "application/json")
        .await?;
    println!("appended at {}", ack.start);

    let page = client
        .read("/orders/1042", None, &client.beginning(), Live::Off, ReadLimits::server_default())
        .await?;
    for record in page.records {
        println!("{}: {:?}", record.position, record.body);
    }
    Ok(())
}
```

- **Reads.** `read` with `Live::LongPoll` waits server side until data arrives or the poll times out. Positions are protocol-specific strings, so always continue from the `next` a call returned.
- **Records.** A `Record` has an optional `key`, `headers` as a `Vec<(String, Option<Bytes>)>` kept in order, and a `body` that is `None` for a tombstone, see [Records](/docs/api#records). Durable Streams reads carry a body only.
- **Lists.** `list` and `list_groups` take a `limit` and a `cursor` and return one [page](/docs/api#lists) whose `next` continues it, `None` on the last page.

## Partitions

`head`, `append`, and `read` take a partition as their second argument. `None` addresses a topic with one partition. On a topic with several, each call names one with `Some(index)`.

## Topic settings

`CreateOptions` carries every [topic setting](/docs/api#topic-settings), and unset fields take the server's defaults. `config` reads the settings back and `update_config` changes them. In a `ConfigUpdate`, `None` leaves a field alone and `Some(None)` removes the setting.

```rust
use picomq_client::{ConfigUpdate, CreateOptions};

let options = CreateOptions {
    partitions: Some(3),
    retention_ms: Some(86_400_000),
    ..CreateOptions::new("application/json")
};
client.create("/orders", &options).await?;

let update = ConfigUpdate {
    stream: "/orders".into(),
    partitions: Some(6),
    retention_ms: Some(None),
    ..Default::default()
};
let config = client.update_config(&update).await?;
```

Settings work on both protocols.

## Producers

`Producer` batches sends and retries them safely under a producer id, epoch, and sequence, on the Pico protocol.

```rust
use std::sync::Arc;

use picomq_client::producer::{Producer, ProducerConfig};
use picomq_client::PicoClient;

let pico = Arc::new(PicoClient::new("http://127.0.0.1:4437")?);
let producer = Producer::new(pico, "/orders", "writer-1", ProducerConfig::default());
let seq = producer.send_durable("x".into()).await?;
producer.close().await?;
```

`send` returns a `Pending` whose `durable()` resolves to the assigned sequence. A terminal batch failure poisons the producer, and recovery takes a new one with a higher `epoch`. `ProducerConfig.partition` targets one partition. For one-off idempotent appends, `PicoClient::append_as` sends records under an explicit producer reference. Plain appends are never retried, because they carry no deduplication key.

## Consumer groups

`GroupMember` joins, heartbeats in the background, and follows rebalances. See [Consumer groups](/docs/design/groups) for how assignment works.

```rust
use picomq_client::{CommittedOffset, GroupConfig, GroupMember, Offsets};

let member = GroupMember::join(
    pico.clone(),
    "billing",
    vec!["/orders".into(), "/payments".into()],
    GroupConfig::default(),
)
.await?;
for topic in member.assignment().streams {
    let offsets = Offsets::from([(topic, CommittedOffset { position: 42, metadata: None })]);
    member.commit(&offsets).await?;
}
member.leave().await?;
```

`assignments()` is a watch channel of the current assignment, and `error()` reports a member that could not rejoin. The lower-level calls are on `PicoClient`: `join_group`, `heartbeat`, `group_assignment`, `leave_group`, `commit_offsets`, `fetch_offsets`, `describe_group`, `list_groups`, and `delete_group`.

## Configuration

`connect_with`, `PicoClient::with_config`, and `DsClient::with_config` take a `ClientConfig`:

| Field | Meaning |
| --- | --- |
| `token` | Bearer token sent on every request, including each redirect hop |
| `http2` | Speak cleartext HTTP/2 (h2c) for multiplexed appends. The server must support it |
| `retry` | `RetryPolicy` for read-shaped calls. Appends are never retried implicitly |

The client follows ownership redirects itself and re-attaches the credential on every hop, see the [HTTP API conventions](/docs/api#conventions).

## Errors

Every failure is a `ClientError` with a `kind`, the HTTP status, and the server's code, and `retryable()` says whether trying again can help. Durable Streams has no equivalent for listing, trimming, or groups, which fail with the code `unsupported`.

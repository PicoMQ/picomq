# Rust client

`picomq-client` is the Rust SDK for the HTTP protocols. It speaks the native Pico protocol and [Durable Streams](/docs/design/protocols) behind one `StreamApi` trait, and includes a batching producer for high-throughput appends.

The crate is standalone. It depends on `picomq-protocol`, the small crate holding the shared wire vocabulary (header constants and the Pico record codec), plus the usual HTTP stack (`reqwest`, `tokio`). Pulling in the client does not build any part of the server.

## Install

```toml
[dependencies]
picomq-client = "0.1.0"
```

`picomq-protocol` is a transitive dependency. It contains the protocol headers and codecs.

## Usage

```rust
use picomq_client::{connect, Live, Protocol, ReadLimits};

#[tokio::main]
async fn main() -> picomq_client::Result<()> {
    let client = connect(Protocol::Pico, "http://localhost:8080")?;

    client.create("/orders/1042", "application/json", None).await?;
    let ack = client
        .append("/orders/1042", &[r#"{"item":"widget"}"#.into()], "application/json")
        .await?;
    println!("appended at {}", ack.start);

    let page = client
        .read("/orders/1042", &client.beginning(), Live::Off, ReadLimits::server_default())
        .await?;
    for record in page.records {
        println!("{}: {:?}", record.position, record.body);
    }
    Ok(())
}
```

`read` with `Live::LongPoll` blocks server-side until data arrives or the poll times out, which is how a consumer tails a stream without spinning.

## Configuration

`connect_with` takes a `ClientConfig`:

| Field | Meaning |
| --- | --- |
| `token` | Bearer token sent on every request, including each redirect hop. |
| `http2` | Speak cleartext HTTP/2 (h2c) for multiplexed appends. Opt-in, the server must support it. |
| `retry` | `RetryPolicy` applied to read-shaped calls. Appends are never retried implicitly. |

The client follows ownership redirects (`307`) itself and re-attaches the credential on every hop, which standard HTTP clients refuse to do across origins. See the [HTTP API conventions](/docs/api#conventions).

## Producers

For exactly-once appends, `picomq_client::producer` provides identified producer sessions over the Pico protocol. The server tracks the producer's id, epoch, and sequence, rejects stale epochs, and recognizes re-sent requests as duplicates instead of applying them twice.

## Protocol differences

The `StreamApi` trait exposes the union of both protocols and returns an `unsupported` error where one side has no equivalent. Notably, listing streams is Pico-only, batch appends over Durable Streams are limited to one record per request, and position tokens are protocol-specific strings, so always feed back the `next` value a call returned rather than constructing one.

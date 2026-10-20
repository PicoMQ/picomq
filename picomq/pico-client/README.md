# picomq-client

Rust client for [PicoMQ](https://picomq.com) over HTTP. It speaks the Pico protocol and Durable Streams behind one `StreamApi` trait, with a batching producer and consumer group members on Pico.

```toml
[dependencies]
picomq-client = "0.1"
```

```rust
use picomq_client::{connect, Live, Protocol, ReadLimits};

#[tokio::main]
async fn main() -> picomq_client::Result<()> {
    let client = connect(Protocol::Pico, "http://127.0.0.1:4437")?;

    client.create("/orders", "application/json", None).await?;
    client
        .append("/orders", None, &[r#"{"item":"widget"}"#.into()], "application/json")
        .await?;

    let page = client
        .read("/orders", None, &client.beginning(), Live::Off, ReadLimits::server_default())
        .await?;
    for record in page.records {
        println!("{}: {:?}", record.position, record.body);
    }
    Ok(())
}
```

Partitions, producers, consumer groups, configuration, and errors are covered in the [Rust client documentation](https://picomq.com/docs/client/rust).

License: Apache-2.0

//! Both clients against a real server, through the shared trait: create,
//! append, read, tail, close, delete, and the error mapping for a missing
//! stream.

use std::net::SocketAddr;
use std::time::Duration;

use bytes::Bytes;
use pico_client::{connect, ErrorKind, Live, Protocol, ReadLimits};
use pico_http::Protocol as ServeProtocol;
use pico_runtime::{MetaBackend, PicoServer, ServerConfig};

struct Server {
    server: PicoServer,
    endpoint: String,
}

async fn start(protocol: Protocol, dir: &std::path::Path) -> Server {
    let server = pico_runtime::start(ServerConfig {
        addr: SocketAddr::from(([127, 0, 0, 1], 0)),
        admin_addr: None,
        protocol: match protocol {
            Protocol::Pico => ServeProtocol::Pico,
            Protocol::Ds => ServeProtocol::Ds,
        },
        meta_backend: MetaBackend::parse("sqlite::memory:").unwrap(),
        storage_uri: format!("1@file://{}", dir.join("objects").display()),
        wal_uri: Some(format!("2@file://{}", dir.join("wal").display())),
        // Short long-poll so a tail against an idle stream returns promptly.
        long_poll_timeout: Duration::from_secs(1),
        engine: s3stream::Config {
            wal_upload_interval_ms: 200,
            ..Default::default()
        },
        ..Default::default()
    })
    .await
    .unwrap();
    let endpoint = format!("http://{}", server.local_addr());
    Server { server, endpoint }
}

/// The whole trait surface, driven identically for both protocols. Anything
/// that has to differ (positions, listing) is asserted per protocol below.
async fn lifecycle(protocol: Protocol) {
    let dir = tempfile::tempdir().unwrap();
    let server = start(protocol, dir.path()).await;
    let client = connect(protocol, &server.endpoint).unwrap();

    assert!(client.head("/streams/nope").await.unwrap().is_none());

    assert!(client
        .create("/streams/orders", "text/plain", None)
        .await
        .unwrap());
    assert!(
        !client
            .create("/streams/orders", "text/plain", None)
            .await
            .unwrap(),
        "creating twice is idempotent"
    );

    let ack = client
        .append(
            "/streams/orders",
            &[Bytes::from_static(b"first")],
            "text/plain",
        )
        .await
        .unwrap();
    let head = client.head("/streams/orders").await.unwrap().unwrap();
    assert_eq!(head.next, ack.next, "head agrees with the append ack");
    assert!(!head.closed);

    let page = client
        .read(
            "/streams/orders",
            &client.beginning(),
            Live::Off,
            ReadLimits::server_default(),
        )
        .await
        .unwrap();
    assert_eq!(page.records.len(), 1);
    assert_eq!(page.records[0].body.as_ref(), b"first");
    assert_eq!(page.next, ack.next);

    // Tailing an idle stream: the long poll returns caught-up and empty.
    let idle = client
        .read(
            "/streams/orders",
            &page.next,
            Live::LongPoll,
            ReadLimits::server_default(),
        )
        .await
        .unwrap();
    assert!(idle.records.is_empty());
    assert!(idle.up_to_date);

    let final_position = client.close("/streams/orders").await.unwrap();
    assert_eq!(final_position, ack.next);
    let closed = client.head("/streams/orders").await.unwrap().unwrap();
    assert!(closed.closed);

    let error = client
        .append(
            "/streams/orders",
            &[Bytes::from_static(b"after close")],
            "text/plain",
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind, ErrorKind::Closed, "{error}");

    assert!(client.delete("/streams/orders").await.unwrap());
    assert!(
        !client.delete("/streams/orders").await.unwrap(),
        "deleting a gone stream reports nothing to do"
    );

    server.server.shutdown().await;
}

#[tokio::test]
async fn pico_lifecycle() {
    lifecycle(Protocol::Pico).await;
}

#[tokio::test]
async fn ds_lifecycle() {
    lifecycle(Protocol::Ds).await;
}

/// Pico only. DS reports it as unsupported rather
/// than guessing a shape the protocol does not define.
#[tokio::test]
async fn listing_is_pico_only() {
    let dir = tempfile::tempdir().unwrap();
    let server = start(Protocol::Pico, dir.path()).await;
    let client = connect(Protocol::Pico, &server.endpoint).unwrap();

    client
        .create("/streams/a", "text/plain", None)
        .await
        .unwrap();
    client
        .create("/streams/b", "text/plain", None)
        .await
        .unwrap();
    let listing = client.list("/streams/", 0).await.unwrap();
    let names: Vec<&str> = listing.streams.iter().map(|s| s.name.as_str()).collect();
    assert_eq!(names, vec!["/streams/a", "/streams/b"]);
    assert!(!listing.has_more);

    server.server.shutdown().await;

    let ds = connect(Protocol::Ds, "http://127.0.0.1:1").unwrap();
    let error = ds.list("/", 0).await.unwrap_err();
    assert_eq!(error.code, "unsupported", "{error}");
}

/// Pico records carry the server timestamp and per-record headers. DS bodies
/// travel raw. The trait keeps both, so a caller can print either.
#[tokio::test]
async fn pico_records_carry_timestamps() {
    let dir = tempfile::tempdir().unwrap();
    let server = start(Protocol::Pico, dir.path()).await;
    let client = connect(Protocol::Pico, &server.endpoint).unwrap();

    client
        .create("/streams/ts", "text/plain", None)
        .await
        .unwrap();
    client
        .append(
            "/streams/ts",
            &[Bytes::from_static(b"a"), Bytes::from_static(b"b")],
            "text/plain",
        )
        .await
        .unwrap();

    let page = client
        .read(
            "/streams/ts",
            &client.beginning(),
            Live::Off,
            ReadLimits::server_default(),
        )
        .await
        .unwrap();
    assert_eq!(page.records.len(), 2, "one request, one batch");
    assert_eq!(page.records[0].position, "0");
    assert_eq!(page.records[1].position, "1");
    assert!(page.records[0].timestamp.unwrap() > 0);

    server.server.shutdown().await;
}

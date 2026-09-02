//! Cleartext HTTP/2 against the frontend.
//!
//! The point of h2c here is concurrency: HTTP/1.1 gives one request per
//! connection at a time, so a deep append pipeline costs one socket per
//! in-flight request. These tests check that the transport is really HTTP/2
//! (not a silent fallback) and that the protocol behaves identically over it.

use std::net::SocketAddr;
use std::time::Duration;

use bytes::Bytes;
use picomq_client::{ClientConfig, Live, PicoClient, Protocol, ReadLimits, StreamApi};
use picomq_http::HttpProtocol as ServeProtocol;
use picomq_runtime::{MetaBackend, ServerConfig};

async fn start(dir: &std::path::Path) -> (picomq_runtime::PicoServer, String) {
    let server = picomq_runtime::start(ServerConfig {
        addr: SocketAddr::from(([127, 0, 0, 1], 0)),
        admin_addr: None,
        http_protocol: ServeProtocol::Pico,
        kafka: None,
        meta_backend: MetaBackend::parse("sqlite::memory:").unwrap(),
        storage_uri: format!("1@file://{}", dir.join("objects").display()),
        wal_uri: Some(format!(
            "2@file://{}?batchInterval=5",
            dir.join("wal").display()
        )),
        long_poll_timeout: Duration::from_secs(1),
        ..Default::default()
    })
    .await
    .unwrap();
    let endpoint = format!("http://{}", server.local_addr());
    (server, endpoint)
}

#[tokio::test]
async fn appends_over_h2c_negotiate_http2() {
    let dir = tempfile::tempdir().unwrap();
    let (server, endpoint) = start(dir.path()).await;
    let http = picomq_client::http_client(&ClientConfig {
        http2: true,
        ..Default::default()
    })
    .unwrap();

    // Assert on the transport directly: a client with prior knowledge would
    // error rather than downgrade, but a passing round trip below should not be
    // read as proof that HTTP/2 was used.
    let response = http.get(format!("{endpoint}/")).send().await.unwrap();
    assert_eq!(response.version(), reqwest::Version::HTTP_2);

    let client = PicoClient::with_http(&endpoint, http, Default::default());
    assert!(client
        .create("/streams/h2", "text/plain", None)
        .await
        .unwrap());
    let ack = client
        .append(
            "/streams/h2",
            &[Bytes::from_static(b"over h2c")],
            "text/plain",
        )
        .await
        .unwrap();
    let page = client
        .read(
            "/streams/h2",
            &client.beginning(),
            Live::Off,
            ReadLimits::server_default(),
        )
        .await
        .unwrap();
    assert_eq!(page.records[0].body.as_ref(), b"over h2c");
    assert_eq!(page.next, ack.next);

    server.shutdown().await;
}

/// The reason h2c exists here: many more concurrent appends than a connection
/// pool would allow, over a single connection.
#[tokio::test]
async fn one_connection_carries_many_concurrent_appends() {
    let dir = tempfile::tempdir().unwrap();
    let (server, endpoint) = start(dir.path()).await;
    let client = picomq_client::connect_with(
        Protocol::Pico,
        &endpoint,
        &ClientConfig {
            http2: true,
            ..Default::default()
        },
    )
    .unwrap();
    client
        .create("/streams/fanout", "text/plain", None)
        .await
        .unwrap();

    let client = std::sync::Arc::new(client);
    let appends = 200;
    let mut tasks = Vec::with_capacity(appends);
    for i in 0..appends {
        let client = client.clone();
        tasks.push(tokio::spawn(async move {
            client
                .append(
                    "/streams/fanout",
                    &[Bytes::from(format!("record-{i}"))],
                    "text/plain",
                )
                .await
        }));
    }
    for task in tasks {
        task.await.unwrap().expect("append");
    }

    let head = client.head("/streams/fanout").await.unwrap().unwrap();
    assert_eq!(head.next, appends.to_string(), "every append landed once");

    server.shutdown().await;
}

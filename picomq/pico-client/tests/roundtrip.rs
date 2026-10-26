//! Both clients against a real server, through the shared trait: create,
//! append, read, tail, close, delete, topic settings, schemas, and the error
//! mapping for a missing stream.

use std::net::SocketAddr;
use std::time::Duration;

use bytes::Bytes;
use picomq_client::{ConfigUpdate, CreateOptions, ErrorKind, Live, Protocol, ReadLimits, connect};
use picomq_http::HttpProtocol as ServeProtocol;
use picomq_runtime::{KafkaConfig, MetaBackend, PicoServer, ServerConfig};

struct Server {
    server: PicoServer,
    endpoint: String,
}

async fn start(protocol: Protocol, dir: &std::path::Path) -> Server {
    let server = picomq_runtime::start(ServerConfig {
        addr: SocketAddr::from(([127, 0, 0, 1], 0)),
        admin_addr: None,
        kafka: KafkaConfig::new(SocketAddr::from(([127, 0, 0, 1], 0))),
        http_protocol: match protocol {
            Protocol::Pico => ServeProtocol::Pico,
            Protocol::Ds => ServeProtocol::Ds,
        },
        meta_backend: MetaBackend::parse("sqlite::memory:").unwrap(),
        storage_uri: format!("1@file://{}", dir.join("objects").display()),
        wal_uri: Some(format!("2@file://{}", dir.join("wal").display())),
        schema_registry: Some(format!("3@file://{}", dir.join("schemas").display())),
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

    assert!(client.head("/streams/nope", None).await.unwrap().is_none());

    assert!(
        client
            .create("/streams/orders", &CreateOptions::new("text/plain"))
            .await
            .unwrap()
    );
    assert!(
        !client
            .create("/streams/orders", &CreateOptions::new("text/plain"))
            .await
            .unwrap(),
        "creating twice is idempotent"
    );

    let ack = client
        .append(
            "/streams/orders",
            None,
            &[Bytes::from_static(b"first")],
            "text/plain",
        )
        .await
        .unwrap();
    let head = client.head("/streams/orders", None).await.unwrap().unwrap();
    assert_eq!(
        head.next,
        Some(ack.next.clone()),
        "head agrees with the append ack"
    );
    assert!(!head.closed);

    let page = client
        .read(
            "/streams/orders",
            None,
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
            None,
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
    let closed = client.head("/streams/orders", None).await.unwrap().unwrap();
    assert!(closed.closed);

    let error = client
        .append(
            "/streams/orders",
            None,
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
/// Settings set on create, read back, and changed, plus the schemas they
/// bind, the same on both protocols.
async fn settings(protocol: Protocol) {
    let dir = tempfile::tempdir().unwrap();
    let server = start(protocol, dir.path()).await;
    let client = connect(protocol, &server.endpoint).unwrap();
    let name = "/streams/settings";

    let options = CreateOptions {
        partitions: Some(2),
        retention_ms: Some(60_000),
        ..CreateOptions::new("application/json")
    };
    assert!(client.create(name, &options).await.unwrap());
    let head = client.head(name, None).await.unwrap().unwrap();
    assert_eq!(head.partitions, 2);
    assert_eq!(head.retention_ms, Some(60_000));
    assert_eq!(head.retention_bytes, None);

    client
        .put_schema(
            "team/order#v2.json",
            None,
            Bytes::from_static(br#"{"type": "object"}"#),
        )
        .await
        .unwrap();
    let schema = client
        .get_schema("team/order#v2.json")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        schema.content_type.as_deref(),
        Some("application/schema+json")
    );

    let update = ConfigUpdate {
        stream: name.to_owned(),
        schema: Some(Some("team/order#v2.json".to_owned())),
        retention_ms: Some(None),
        retention_bytes: Some(Some(4096)),
        partitions: Some(3),
        ..Default::default()
    };
    let config = client.update_config(&update).await.unwrap();
    assert_eq!(config.partitions, 3);
    assert_eq!(config.schema.as_deref(), Some("team/order#v2.json"));
    assert_eq!(config.retention_ms, None);
    assert_eq!(config.retention_bytes, Some(4096));
    assert_eq!(client.config(name).await.unwrap(), Some(config));
    assert!(client.config("/streams/nope").await.unwrap().is_none());

    let dotted = "//streams/settings";
    assert!(
        client
            .create(dotted, &CreateOptions::new("text/plain"))
            .await
            .unwrap()
    );
    let grown = ConfigUpdate {
        stream: dotted.to_owned(),
        partitions: Some(2),
        ..Default::default()
    };
    assert_eq!(client.update_config(&grown).await.unwrap().partitions, 2);
    assert_eq!(client.config(name).await.unwrap().unwrap().partitions, 3);

    let escaped = "/streams/a%41 #1";
    assert!(
        client
            .create(escaped, &CreateOptions::new("text/plain"))
            .await
            .unwrap()
    );
    assert!(client.head(escaped, None).await.unwrap().is_some());
    assert!(client.head("/streams/aA #1", None).await.unwrap().is_none());
    assert_eq!(client.config(escaped).await.unwrap().unwrap().partitions, 1);

    for refused in [
        client
            .delete_schema("../streams/settings")
            .await
            .unwrap_err(),
        client
            .delete("/streams/../streams/settings")
            .await
            .unwrap_err(),
    ] {
        assert_eq!(refused.kind, ErrorKind::BadRequest);
    }
    assert!(client.head(name, None).await.unwrap().is_some());

    let shrink = ConfigUpdate {
        stream: name.to_owned(),
        partitions: Some(1),
        ..Default::default()
    };
    let refused = client.update_config(&shrink).await.unwrap_err();
    assert_eq!(refused.kind, ErrorKind::BadRequest);
    assert!(refused.message.is_some());

    assert!(client.delete_schema("team/order#v2.json").await.unwrap());
    assert!(!client.delete_schema("team/order#v2.json").await.unwrap());
    assert!(
        client
            .get_schema("team/order#v2.json")
            .await
            .unwrap()
            .is_none()
    );

    server.server.shutdown().await;
}

#[tokio::test]
async fn pico_settings() {
    settings(Protocol::Pico).await;
}

#[tokio::test]
async fn ds_settings() {
    settings(Protocol::Ds).await;
}

#[tokio::test]
async fn listing_is_pico_only() {
    let dir = tempfile::tempdir().unwrap();
    let server = start(Protocol::Pico, dir.path()).await;
    let client = connect(Protocol::Pico, &server.endpoint).unwrap();

    for name in ["/streams/a", "/streams/b", "/streams/c"] {
        client
            .create(name, &CreateOptions::new("text/plain"))
            .await
            .unwrap();
    }
    let names = |listing: &picomq_client::StreamListing| -> Vec<String> {
        listing.streams.iter().map(|s| s.name.clone()).collect()
    };

    let first = client.list("/streams/", 2, None).await.unwrap();
    assert_eq!(names(&first), ["/streams/a", "/streams/b"]);
    let rest = client
        .list("/streams/", 2, first.next.as_deref())
        .await
        .unwrap();
    assert_eq!(names(&rest), ["/streams/c"]);
    assert_eq!(rest.next, None);

    server.server.shutdown().await;

    let ds = connect(Protocol::Ds, "http://127.0.0.1:1").unwrap();
    let error = ds.list("/", 0, None).await.unwrap_err();
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
        .create("/streams/ts", &CreateOptions::new("text/plain"))
        .await
        .unwrap();
    client
        .append(
            "/streams/ts",
            None,
            &[Bytes::from_static(b"a"), Bytes::from_static(b"b")],
            "text/plain",
        )
        .await
        .unwrap();

    let page = client
        .read(
            "/streams/ts",
            None,
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

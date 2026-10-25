use std::net::SocketAddr;

use picomq_client::{ClientConfig, CreateOptions, PicoClient, StreamApi};
use picomq_http::HttpProtocol as ServeProtocol;
use picomq_listener::testing::Authority;
use picomq_runtime::{KafkaConfig, MetaBackend, PicoServer, ServerConfig};

async fn tls_server(dir: &std::path::Path) -> (PicoServer, String, Vec<u8>) {
    let authority = Authority::new().unwrap();
    let tls = authority.issue(&["localhost"]).unwrap().write(dir).unwrap();

    let server = picomq_runtime::start(ServerConfig {
        addr: SocketAddr::from(([127, 0, 0, 1], 0)),
        admin_addr: None,
        http_protocol: ServeProtocol::Pico,
        kafka: KafkaConfig::new(SocketAddr::from(([127, 0, 0, 1], 0))),
        meta_backend: MetaBackend::parse("sqlite::memory:").unwrap(),
        storage_uri: format!("1@file://{}", dir.join("objects").display()),
        wal_uri: Some(format!("2@file://{}", dir.join("wal").display())),
        tls: Some(tls),
        ..Default::default()
    })
    .await
    .unwrap();
    let endpoint = format!("https://localhost:{}", server.local_addr().port());
    (server, endpoint, authority.root_pem().as_bytes().to_vec())
}

#[tokio::test]
async fn a_private_ca_is_trusted_only_when_given() {
    let dir = tempfile::tempdir().unwrap();
    let (server, endpoint, root) = tls_server(dir.path()).await;

    let untrusting = PicoClient::with_config(&endpoint, &ClientConfig::default()).unwrap();
    assert!(
        untrusting
            .create("/orders", &CreateOptions::default())
            .await
            .is_err(),
        "a certificate from an unknown CA was accepted"
    );

    for http2 in [false, true] {
        let trusting = PicoClient::with_config(
            &endpoint,
            &ClientConfig {
                http2,
                ca_certificate: Some(root.clone()),
                ..Default::default()
            },
        )
        .unwrap();
        trusting
            .create(&format!("/orders-{http2}"), &CreateOptions::default())
            .await
            .unwrap();
    }

    assert!(
        PicoClient::with_config(
            &endpoint,
            &ClientConfig {
                ca_certificate: Some(b"not a certificate".to_vec()),
                ..Default::default()
            },
        )
        .is_err()
    );

    server.shutdown().await;
}

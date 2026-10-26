use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use picomq_listener::testing::{Authority, Issued};
use picomq_listener::{Listener, Stream, Tls, TlsConfig, TlsError};
use rustls::RootCertStore;
use rustls::crypto::aws_lc_rs;
use rustls::pki_types::{CertificateDer, ServerName};
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;
use tokio_rustls::client::TlsStream;

async fn connect(authority: &Authority, addr: SocketAddr) -> std::io::Result<TlsStream<TcpStream>> {
    let mut roots = RootCertStore::empty();
    roots.add(authority.root().clone()).unwrap();
    let mut config =
        rustls::ClientConfig::builder_with_provider(Arc::new(aws_lc_rs::default_provider()))
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_root_certificates(roots)
            .with_no_client_auth();
    config.alpn_protocols = vec![b"h2".to_vec()];

    let socket = TcpStream::connect(addr).await?;
    TlsConnector::from(Arc::new(config))
        .connect(ServerName::try_from("localhost").unwrap(), socket)
        .await
}

fn write(dir: &Path, issued: &Issued, key: &str) -> TlsConfig {
    std::fs::write(dir.join("certificate.pem"), &issued.certificate).unwrap();
    std::fs::write(dir.join("key.pem"), key).unwrap();
    TlsConfig::new(dir.join("certificate.pem"), dir.join("key.pem"))
}

fn loopback() -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], 0))
}

async fn echo(listener: &mut Listener) -> Stream {
    let (mut stream, _) = listener.accept().await.unwrap();
    let mut ping = [0u8; 4];
    stream.read_exact(&mut ping).await.unwrap();
    stream.write_all(&ping).await.unwrap();
    stream.flush().await.unwrap();
    stream
}

async fn handshake(authority: &Authority, listener: &mut Listener) -> TlsStream<TcpStream> {
    let client = connect(authority, listener.local_addr());
    let (client, _) = tokio::join!(client, listener.accept());
    client.unwrap()
}

async fn served_certificate(
    authority: &Authority,
    listener: &mut Listener,
) -> CertificateDer<'static> {
    handshake(authority, listener)
        .await
        .get_ref()
        .1
        .peer_certificates()
        .unwrap()[0]
        .clone()
}

#[tokio::test]
async fn plain_connections_pass_through() {
    let mut listener =
        Listener::bind(picomq_metrics::ListenerKind::Stream, loopback(), 16, None).unwrap();
    let mut client = TcpStream::connect(listener.local_addr()).await.unwrap();
    client.write_all(b"ping").await.unwrap();

    let stream = echo(&mut listener).await;
    let mut pong = [0u8; 4];
    client.read_exact(&mut pong).await.unwrap();

    assert_eq!(&pong, b"ping");
    assert!(!stream.is_tls());
}

#[tokio::test]
async fn tls_connections_are_decrypted() {
    let dir = TempDir::new().unwrap();
    let authority = Authority::new().unwrap();
    let issued = authority.issue(&["localhost"]).unwrap();
    let tls = Tls::load(write(dir.path(), &issued, &issued.key)).unwrap();
    let mut listener = Listener::bind(
        picomq_metrics::ListenerKind::Stream,
        loopback(),
        16,
        Some(tls),
    )
    .unwrap();

    let addr = listener.local_addr();
    let client = tokio::spawn(async move {
        let mut client = connect(&authority, addr).await.unwrap();
        client.write_all(b"ping").await.unwrap();
        let mut pong = [0u8; 4];
        client.read_exact(&mut pong).await.unwrap();
        pong
    });

    let stream = echo(&mut listener).await;
    assert_eq!(&client.await.unwrap(), b"ping");
    assert!(stream.is_tls());
}

#[tokio::test]
async fn only_listeners_that_state_protocols_offer_them() {
    let dir = TempDir::new().unwrap();
    let authority = Authority::new().unwrap();
    let issued = authority.issue(&["localhost"]).unwrap();
    let tls = Tls::load(write(dir.path(), &issued, &issued.key)).unwrap();
    let mut http = Listener::bind(
        picomq_metrics::ListenerKind::Stream,
        loopback(),
        16,
        Some(tls.alpn(&[b"h2", b"http/1.1"])),
    )
    .unwrap();
    let mut kafka = Listener::bind(
        picomq_metrics::ListenerKind::Kafka,
        loopback(),
        16,
        Some(tls),
    )
    .unwrap();

    let http = handshake(&authority, &mut http).await;
    let kafka = handshake(&authority, &mut kafka).await;

    assert_eq!(http.get_ref().1.alpn_protocol(), Some(&b"h2"[..]));
    assert_eq!(kafka.get_ref().1.alpn_protocol(), None);
}

#[tokio::test]
async fn a_stalled_handshake_times_out_without_holding_up_others() {
    let dir = TempDir::new().unwrap();
    let authority = Authority::new().unwrap();
    let issued = authority.issue(&["localhost"]).unwrap();
    let config = TlsConfig {
        handshake_timeout: Duration::from_secs(2),
        ..write(dir.path(), &issued, &issued.key)
    };
    let mut listener = Listener::bind(
        picomq_metrics::ListenerKind::Stream,
        loopback(),
        16,
        Some(Tls::load(config).unwrap()),
    )
    .unwrap();

    let mut silent = TcpStream::connect(listener.local_addr()).await.unwrap();
    let served = tokio::time::timeout(
        Duration::from_secs(1),
        served_certificate(&authority, &mut listener),
    )
    .await
    .expect("a stalled handshake held up the next client");
    assert_eq!(served, issued.der);

    let mut probe = [0u8; 1];
    let closed = tokio::time::timeout(Duration::from_secs(5), silent.read(&mut probe))
        .await
        .expect("the stalled handshake must be dropped");
    assert!(matches!(closed, Ok(0) | Err(_)));
}

#[tokio::test]
async fn a_renewed_certificate_is_served_without_a_restart() {
    let dir = TempDir::new().unwrap();
    let authority = Authority::new().unwrap();
    let first = authority.issue(&["localhost"]).unwrap();
    let config = TlsConfig {
        reload_interval: Duration::from_millis(100),
        ..write(dir.path(), &first, &first.key)
    };
    let mut listener = Listener::bind(
        picomq_metrics::ListenerKind::Stream,
        loopback(),
        16,
        Some(Tls::load(config).unwrap()),
    )
    .unwrap();
    assert_eq!(
        served_certificate(&authority, &mut listener).await,
        first.der
    );

    let renewed = authority.issue(&["localhost"]).unwrap();
    write(dir.path(), &renewed, &renewed.key);

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        if served_certificate(&authority, &mut listener).await == renewed.der {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the renewed certificate was never served"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test]
async fn unusable_files_never_replace_the_current_certificate() {
    let dir = TempDir::new().unwrap();
    let authority = Authority::new().unwrap();
    let issued = authority.issue(&["localhost"]).unwrap();
    let stranger = authority.issue(&["localhost"]).unwrap();

    let mismatched = Tls::load(write(dir.path(), &issued, &stranger.key));
    assert!(matches!(mismatched, Err(TlsError::Key(_))));
    let garbage = Tls::load(write(dir.path(), &issued, "not a key"));
    assert!(matches!(garbage, Err(TlsError::Pem { .. })));

    let tls = Tls::load(write(dir.path(), &issued, &issued.key)).unwrap();
    let mut listener = Listener::bind(
        picomq_metrics::ListenerKind::Stream,
        loopback(),
        16,
        Some(tls.clone()),
    )
    .unwrap();

    write(dir.path(), &stranger, &issued.key);
    assert!(matches!(tls.reload(), Err(TlsError::Key(_))));
    assert_eq!(
        served_certificate(&authority, &mut listener).await,
        issued.der
    );
}

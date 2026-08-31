//! Booting a real process configuration: SQLite metadata log, object storage
//! from a bucket URI, admin + protocol listeners. Start the server, drive it
//! over HTTP, restart it.

use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;

use picomq_auth::AccessToken;
use picomq_http::Protocol;
use picomq_runtime::{AuthMode, MetaBackend, RuntimeError, ServerConfig};

fn loopback() -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], 0))
}

/// A config with ephemeral ports, a SQLite log and storage under `dir`.
fn config(dir: &Path, protocol: Protocol, node_epoch: i64) -> ServerConfig {
    ServerConfig {
        node_epoch,
        addr: loopback(),
        admin_addr: Some(loopback()),
        protocol,
        meta_backend: MetaBackend::parse(&format!("sqlite:{}", dir.join("meta.db").display()))
            .unwrap(),
        storage_uri: format!("1@file://{}", dir.join("objects").display()),
        wal_uri: Some(format!("2@file://{}", dir.join("wal").display())),
        engine: s3stream::Config {
            wal_upload_interval_ms: 200,
            ..Default::default()
        },
        ..Default::default()
    }
}

#[tokio::test]
async fn pico_protocol_over_a_started_process() {
    let dir = tempfile::tempdir().unwrap();
    let server = picomq_runtime::start(config(dir.path(), Protocol::Pico, 1))
        .await
        .unwrap();
    let http = reqwest::Client::new();
    let base = format!("http://{}", server.local_addr());
    let admin = format!("http://{}", server.admin_addr().unwrap());

    let ready: serde_json::Value = http
        .get(format!("{admin}/ready"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(ready["ready"], true, "registered against the SQLite log");

    let url = format!("{base}/streams/orders");
    let created = http
        .put(&url)
        .header("Content-Type", "text/plain")
        .send()
        .await
        .unwrap();
    assert_eq!(created.status(), 201);

    let appended = http
        .post(&url)
        .header("Content-Type", "text/plain")
        .body("hello-from-pico")
        .send()
        .await
        .unwrap();
    assert_eq!(appended.status(), 200);
    assert_eq!(appended.headers()["Pico-Next-Seq"], "1");

    let read = http.get(format!("{url}?seq=0")).send().await.unwrap();
    assert_eq!(read.status(), 200);
    let records: serde_json::Value = read.json().await.unwrap();
    assert_eq!(records[0]["seq"], 0);
    assert_eq!(records[0]["body"], "hello-from-pico");

    server.shutdown().await;
}

#[tokio::test]
async fn ds_protocol_over_a_started_process() {
    let dir = tempfile::tempdir().unwrap();
    let server = picomq_runtime::start(config(dir.path(), Protocol::Ds, 1))
        .await
        .unwrap();
    let http = reqwest::Client::new();
    let url = format!("http://{}/streams/events", server.local_addr());

    let created = http
        .put(&url)
        .header("Content-Type", "text/plain")
        .send()
        .await
        .unwrap();
    assert_eq!(created.status(), 201);

    // The Durable Streams protocol acks an append with 204 + the new offset.
    let appended = http
        .post(&url)
        .header("Content-Type", "text/plain")
        .body("hello-from-ds")
        .send()
        .await
        .unwrap();
    assert_eq!(appended.status(), 204);

    let read = http.get(format!("{url}?offset=-1")).send().await.unwrap();
    assert_eq!(read.status(), 200);
    assert_eq!(read.text().await.unwrap(), "hello-from-ds");

    server.shutdown().await;
}

/// Kafka mode: the TCP listener answers ApiVersions, the HTTP data routers
/// are not mounted, and the admin surface stays up.
#[tokio::test]
async fn kafka_protocol_over_a_started_process() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let dir = tempfile::tempdir().unwrap();
    let mut config = config(dir.path(), Protocol::Kafka, 1);
    config.kafka_listen = loopback();
    let server = picomq_runtime::start(config).await.unwrap();
    let kafka_addr = server.kafka_addr().unwrap();

    // ApiVersions v0 request, framed by hand: api_key, version,
    // correlation id, null client id.
    let mut frame = Vec::new();
    frame.extend_from_slice(&18i16.to_be_bytes());
    frame.extend_from_slice(&0i16.to_be_bytes());
    frame.extend_from_slice(&77i32.to_be_bytes());
    frame.extend_from_slice(&(-1i16).to_be_bytes());
    let mut request = (frame.len() as i32).to_be_bytes().to_vec();
    request.extend_from_slice(&frame);

    let mut socket = tokio::net::TcpStream::connect(kafka_addr).await.unwrap();
    socket.write_all(&request).await.unwrap();
    let mut len = [0u8; 4];
    tokio::time::timeout(Duration::from_secs(2), socket.read_exact(&mut len))
        .await
        .unwrap()
        .unwrap();
    let mut body = vec![0u8; i32::from_be_bytes(len) as usize];
    socket.read_exact(&mut body).await.unwrap();
    assert_eq!(&body[..4], &77i32.to_be_bytes());

    let http = reqwest::Client::new();
    let admin = format!("http://{}", server.admin_addr().unwrap());
    let ready: serde_json::Value = http
        .get(format!("{admin}/ready"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(ready["ready"], true);

    let data = http
        .get(format!("http://{}/streams/orders", server.local_addr()))
        .send()
        .await
        .unwrap();
    assert_eq!(data.status(), 404, "no HTTP data router in kafka mode");

    server.shutdown().await;
}

#[tokio::test]
async fn auth_off_refuses_non_loopback_binds() {
    let dir = tempfile::tempdir().unwrap();
    let mut refused = config(dir.path(), Protocol::Pico, 1);
    refused.addr = SocketAddr::from(([0, 0, 0, 0], 0));
    assert!(matches!(
        picomq_runtime::start(refused).await,
        Err(RuntimeError::InsecureBind { .. })
    ));

    let mut refused_admin = config(dir.path(), Protocol::Pico, 1);
    refused_admin.admin_addr = Some(SocketAddr::from(([0, 0, 0, 0], 0)));
    assert!(matches!(
        picomq_runtime::start(refused_admin).await,
        Err(RuntimeError::InsecureBind { .. })
    ));
}

/// The Kafka listener has no authentication, so a non-loopback bind needs
/// the explicit opt-out even with auth required.
#[tokio::test]
async fn kafka_non_loopback_bind_refused_regardless_of_auth() {
    let dir = tempfile::tempdir().unwrap();
    let mut refused = config(dir.path(), Protocol::Kafka, 1);
    refused.auth_mode = AuthMode::Required;
    refused.kafka_listen = SocketAddr::from(([0, 0, 0, 0], 0));
    assert!(matches!(
        picomq_runtime::start(refused).await,
        Err(RuntimeError::InsecureBind { .. })
    ));
}

#[tokio::test]
async fn insecure_allow_remote_permits_non_loopback_binds() {
    let dir = tempfile::tempdir().unwrap();
    let mut allowed = config(dir.path(), Protocol::Pico, 1);
    allowed.addr = SocketAddr::from(([0, 0, 0, 0], 0));
    allowed.admin_addr = Some(SocketAddr::from(([0, 0, 0, 0], 0)));
    allowed.insecure_allow_remote = true;
    let server = picomq_runtime::start(allowed).await.unwrap();
    server.shutdown().await;
}

/// Bootstrap seeds the root token once, enforcement turns on with the mode,
/// a restart with the same token is a no-op, and a different token under the
/// same id refuses to start.
#[tokio::test]
async fn bootstrap_enforces_and_stays_idempotent() {
    let dir = tempfile::tempdir().unwrap();
    let (root, _) = AccessToken::issue("ops/root").unwrap();
    let secured = |epoch: i64, wire: String| {
        let mut config = config(dir.path(), Protocol::Pico, epoch);
        config.auth_mode = AuthMode::Required;
        config.bootstrap_token = Some(wire);
        config
    };

    let http = reqwest::Client::new();
    let first = picomq_runtime::start(secured(1, root.render()))
        .await
        .unwrap();
    let url = format!("http://{}/streams/locked", first.local_addr());
    assert_eq!(
        http.put(&url)
            .header("Content-Type", "text/plain")
            .send()
            .await
            .unwrap()
            .status(),
        401,
        "enforcement is on"
    );
    assert_eq!(
        http.put(&url)
            .header("Content-Type", "text/plain")
            .bearer_auth(root.render())
            .send()
            .await
            .unwrap()
            .status(),
        201
    );
    first.shutdown().await;

    let second = picomq_runtime::start(secured(2, root.render()))
        .await
        .unwrap();
    second.shutdown().await;

    let (imposter, _) = AccessToken::issue("ops/root").unwrap();
    assert!(matches!(
        picomq_runtime::start(secured(3, imposter.render())).await,
        Err(RuntimeError::BootstrapConflict { id }) if id == "ops/root"
    ));
}

/// The point of a SQL log plus object storage: state survives the process.
#[tokio::test]
async fn state_survives_a_restart() {
    let dir = tempfile::tempdir().unwrap();
    let http = reqwest::Client::new();

    let first = picomq_runtime::start(config(dir.path(), Protocol::Pico, 1))
        .await
        .unwrap();
    let url = format!("http://{}/streams/durable", first.local_addr());
    assert_eq!(
        http.put(&url)
            .header("Content-Type", "text/plain")
            .send()
            .await
            .unwrap()
            .status(),
        201
    );
    assert_eq!(
        http.post(&url)
            .header("Content-Type", "text/plain")
            .body("survives")
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
    // Let the periodic WAL upload land the record in object storage before the
    // process goes away.
    tokio::time::sleep(Duration::from_millis(500)).await;
    first.shutdown().await;

    // `nodeEpoch = System.currentTimeMillis()`).
    let second = picomq_runtime::start(config(dir.path(), Protocol::Pico, 2))
        .await
        .unwrap();
    let url = format!("http://{}/streams/durable", second.local_addr());
    let head = http.head(&url).send().await.unwrap();
    assert_eq!(head.status(), 200, "stream metadata survived the restart");
    assert_eq!(head.headers()["Pico-Next-Seq"], "1");

    let read = http.get(format!("{url}?seq=0")).send().await.unwrap();
    assert_eq!(read.status(), 200);
    let records: serde_json::Value = read.json().await.unwrap();
    assert_eq!(records[0]["body"], "survives");

    second.shutdown().await;
}

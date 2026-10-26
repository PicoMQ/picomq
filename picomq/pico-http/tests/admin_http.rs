//! Admin surface over a real socket: liveness, readiness, and the
//! `/admin` introspection and write endpoints.

mod common;

use std::net::SocketAddr;
use std::time::Duration;

use picomq_http::{HttpProtocol, ServeOptions, serve};
use picomq_listener::Listener;

#[tokio::test]
async fn metrics_are_served_only_by_a_node_that_collects_them() {
    let server = common::picomq_server().await;
    let client = reqwest::Client::new();

    let response = client
        .get(format!("{}/metrics", server.admin_url))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 404);

    server.shutdown().await;
}

#[tokio::test]
async fn a_node_that_does_not_forward_refuses_other_nodes() {
    let server = common::picomq_server().await;
    let client = reqwest::Client::new();

    let response = client
        .get(format!("{}/admin/nodes/99/metrics", server.admin_url))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 502);

    server.shutdown().await;
}

#[tokio::test]
async fn health_and_ready() {
    let server = common::picomq_server().await;
    let client = reqwest::Client::new();

    let health = client
        .get(format!("{}/health", server.admin_url))
        .send()
        .await
        .unwrap();
    assert_eq!(health.status(), 200);
    assert_eq!(health.text().await.unwrap(), "ok");

    let ready = client
        .get(format!("{}/ready", server.admin_url))
        .send()
        .await
        .unwrap();
    assert_eq!(ready.status(), 200);
    let body: serde_json::Value = ready.json().await.unwrap();
    assert_eq!(body["ready"], true);
    assert_eq!(body["registered"], true);
    assert_eq!(body["fenced"], false);
    assert_eq!(body["nodeId"], 1);
    assert!(
        body["appliedIndex"].as_u64().unwrap() > 0,
        "registration applied: {body}"
    );
}

#[tokio::test]
async fn a_fenced_node_is_not_ready() {
    let server = common::picomq_server().await;
    let node = server.node.metadata().proposer();
    let heartbeats = server.node.views().load().state.nodes()[&node.node_id].heartbeats;
    server
        .node
        .metadata()
        .fence_node(node.node_id, node.node_epoch, heartbeats)
        .await
        .unwrap();

    let client = reqwest::Client::new();
    let ready = client
        .get(format!("{}/ready", server.admin_url))
        .send()
        .await
        .unwrap();
    assert_eq!(ready.status(), 503);
    let body: serde_json::Value = ready.json().await.unwrap();
    assert_eq!(body["registered"], true);
    assert_eq!(body["fenced"], true);

    let nodes: serde_json::Value = client
        .get(format!("{}/admin/nodes", server.admin_url))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(nodes["items"][0]["fenced"], true);
}

#[tokio::test]
async fn ready_fails_while_draining() {
    let node = common::start_node().await;
    let loopback = SocketAddr::from(([127, 0, 0, 1], 0));
    let server = serve(
        node,
        Listener::bind(picomq_metrics::ListenerKind::Stream, loopback, 1024, None).unwrap(),
        Some(Listener::bind(picomq_metrics::ListenerKind::Admin, loopback, 1024, None).unwrap()),
        ServeOptions {
            protocol: HttpProtocol::Pico,
            shutdown_drain: Duration::from_secs(2),
            ..Default::default()
        },
    )
    .await;
    let admin_url = format!("http://{}", server.admin_addr().unwrap());

    let draining = tokio::spawn(async move { server.shutdown().await });
    tokio::time::sleep(Duration::from_millis(100)).await;

    let client = reqwest::Client::new();
    let ready = client
        .get(format!("{admin_url}/ready"))
        .send()
        .await
        .unwrap();
    assert_eq!(ready.status(), 503, "draining node is not ready");
    let body: serde_json::Value = ready.json().await.unwrap();
    assert_eq!(body["serving"], false);
    assert_eq!(body["registered"], true, "still registered while draining");

    // Liveness stays up: the process is healthy, just not accepting new work.
    let health = client
        .get(format!("{admin_url}/health"))
        .send()
        .await
        .unwrap();
    assert_eq!(health.status(), 200);

    draining.await.unwrap();
    assert!(
        client
            .get(format!("{admin_url}/ready"))
            .send()
            .await
            .is_err(),
        "listener closed after the drain window"
    );
}

#[tokio::test]
async fn a_silent_http2_client_cannot_hold_up_shutdown() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let node = common::start_node().await;
    let loopback = SocketAddr::from(([127, 0, 0, 1], 0));
    let server = serve(
        node,
        Listener::bind(picomq_metrics::ListenerKind::Stream, loopback, 1024, None).unwrap(),
        None,
        ServeOptions {
            protocol: HttpProtocol::Pico,
            shutdown_timeout: Duration::from_millis(300),
            ..Default::default()
        },
    )
    .await;

    let mut silent = tokio::net::TcpStream::connect(server.local_addr())
        .await
        .unwrap();
    silent
        .write_all(b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n\0\0\0\x04\0\0\0\0\0")
        .await
        .unwrap();
    let mut frame = [0u8; 9];
    tokio::time::timeout(Duration::from_secs(5), silent.read_exact(&mut frame))
        .await
        .expect("the server never answered the http/2 preface")
        .unwrap();
    assert_eq!(frame[3], 0x4, "the server's first frame is not SETTINGS");

    tokio::time::timeout(Duration::from_secs(5), server.shutdown())
        .await
        .expect("a silent http/2 client held up shutdown");
    drop(silent);
}

#[tokio::test]
async fn cluster_nodes_and_stream_detail() {
    let server = common::picomq_server().await;
    let client = reqwest::Client::new();

    let create = client
        .put(format!("{}/orders/live", server.base_url))
        .header("Content-Type", "text/plain")
        .send()
        .await
        .unwrap();
    assert_eq!(create.status(), 201);
    let append = client
        .post(format!("{}/orders/live", server.base_url))
        .header("Content-Type", "text/plain")
        .body("hello")
        .send()
        .await
        .unwrap();
    assert_eq!(append.status(), 200);

    let cluster: serde_json::Value = client
        .get(format!("{}/admin/cluster", server.admin_url))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(cluster["nodeId"], 1);
    assert_eq!(cluster["registered"], true);
    assert_eq!(cluster["streamCount"], 1);
    assert_eq!(cluster["pendingTransferCount"], 0);
    assert_eq!(cluster["leaseHolder"], serde_json::Value::Null);
    assert!(cluster["appliedIndex"].as_u64().unwrap() > 0);

    let nodes: serde_json::Value = client
        .get(format!("{}/admin/nodes", server.admin_url))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let list = nodes["items"].as_array().unwrap();
    assert_eq!(list.len(), 1);
    assert_eq!(list[0]["nodeId"], 1);
    assert_eq!(list[0]["local"], true);
    assert_eq!(list[0]["openingCount"], 1, "stream opened by the append");
    assert!(list[0]["slots"].as_u64().unwrap() > 0);

    let node = client
        .get(format!("{}/admin/nodes/1", server.admin_url))
        .send()
        .await
        .unwrap();
    assert_eq!(node.status(), 200);
    assert_eq!(node.json::<serde_json::Value>().await.unwrap(), list[0]);

    let unknown = client
        .get(format!("{}/admin/nodes/99", server.admin_url))
        .send()
        .await
        .unwrap();
    assert_eq!(unknown.status(), 404);

    let partition = client
        .get(format!(
            "{}/admin/topics/%2Forders%2Flive/partitions/0",
            server.admin_url
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(partition.status(), 200);
    let body: serde_json::Value = partition.json().await.unwrap();
    assert_eq!(body["index"], 0);
    assert_eq!(body["ownerNodeId"], 1);
    assert_eq!(body["state"], "opened");
    assert_eq!(body["current"], true);
    assert_eq!(body["pendingTransfer"], serde_json::Value::Null);

    for missing in ["absent/partitions/0", "%2Forders%2Flive/partitions/1"] {
        let response = client
            .get(format!("{}/admin/topics/{missing}", server.admin_url))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 404, "{missing}");
    }
}

#[tokio::test]
async fn update_node_slots() {
    let server = common::picomq_server().await;
    let client = reqwest::Client::new();

    let updated = client
        .post(format!("{}/admin/nodes/1", server.admin_url))
        .json(&serde_json::json!({ "slots": 7 }))
        .send()
        .await
        .unwrap();
    assert_eq!(updated.status(), 200);
    let body: serde_json::Value = updated.json().await.unwrap();
    assert_eq!(body["nodeId"], 1);
    assert_eq!(body["slots"], 7);
    assert!(
        body["advertisedAddress"].is_string(),
        "address preserved: {body}"
    );

    let unknown = client
        .post(format!("{}/admin/nodes/99", server.admin_url))
        .json(&serde_json::json!({ "slots": 7 }))
        .send()
        .await
        .unwrap();
    assert_eq!(unknown.status(), 404);

    let malformed = client
        .post(format!("{}/admin/nodes/1", server.admin_url))
        .json(&serde_json::json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(malformed.status(), 400);
}

#[tokio::test]
async fn transfer_validation() {
    let server = common::picomq_server().await;
    let client = reqwest::Client::new();

    let absent = client
        .post(format!("{}/admin/transfers", server.admin_url))
        .json(&serde_json::json!({ "topic": "/absent", "toNode": 2 }))
        .send()
        .await
        .unwrap();
    assert_eq!(absent.status(), 404);

    client
        .put(format!("{}/xfer/a", server.base_url))
        .header("Content-Type", "text/plain")
        .send()
        .await
        .unwrap();
    client
        .post(format!("{}/xfer/a", server.base_url))
        .header("Content-Type", "text/plain")
        .body("x")
        .send()
        .await
        .unwrap();

    // The target node is not registered, so the proposal is rejected.
    let bad_target = client
        .post(format!("{}/admin/transfers", server.admin_url))
        .json(&serde_json::json!({ "topic": "/xfer/a", "toNode": 2 }))
        .send()
        .await
        .unwrap();
    assert_eq!(bad_target.status(), 400);

    let malformed = client
        .post(format!("{}/admin/transfers", server.admin_url))
        .json(&serde_json::json!({ "topic": "/xfer/a" }))
        .send()
        .await
        .unwrap();
    assert_eq!(malformed.status(), 400);
}

#[tokio::test]
async fn console_is_served_at_root_and_every_page_path() {
    let server = common::picomq_server().await;
    let client = reqwest::Client::new();
    let get = |path: &str, encoding: &str| {
        client
            .get(format!("{}{path}", server.admin_url))
            .header("Accept-Encoding", encoding)
            .send()
    };

    let index = get("/", "identity").await.unwrap();
    assert_eq!(index.status(), 200);
    assert!(
        index.headers()["Content-Type"]
            .to_str()
            .unwrap()
            .starts_with("text/html")
    );
    let body = index.text().await.unwrap();
    assert!(body.contains("PicoMQ"), "{body}");

    let page = get("/topics/orders/partitions", "identity").await.unwrap();
    assert_eq!(page.status(), 200);
    assert_eq!(page.text().await.unwrap(), body);

    assert_eq!(
        get("/admin/absent", "identity").await.unwrap().status(),
        404
    );
    assert_eq!(
        get("/assets/absent.js", "identity").await.unwrap().status(),
        404
    );

    if let Some(script) = body.find("src=\"/assets/") {
        let start = script + "src=\"".len();
        let end = start + body[start..].find('"').unwrap();
        let path = &body[start..end];

        let plain = get(path, "identity").await.unwrap();
        assert_eq!(plain.status(), 200);
        assert_eq!(
            plain.headers()["Cache-Control"],
            "public, max-age=31536000, immutable"
        );
        assert!(plain.headers().get("Content-Encoding").is_none());
        let plain = plain.bytes().await.unwrap();

        let compressed = get(path, "br, gzip").await.unwrap();
        assert_eq!(compressed.headers()["Content-Encoding"], "gzip");
        assert_eq!(compressed.headers()["Content-Type"], "text/javascript");
        assert!(compressed.bytes().await.unwrap().len() < plain.len());

        let refused = get(path, "gzip;q=0").await.unwrap();
        assert!(refused.headers().get("Content-Encoding").is_none());
    }
}

#[tokio::test]
async fn admin_listener_can_be_disabled() {
    let node = common::start_node().await;
    let server = serve(
        node,
        Listener::bind(
            picomq_metrics::ListenerKind::Stream,
            SocketAddr::from(([127, 0, 0, 1], 0)),
            1024,
            None,
        )
        .unwrap(),
        None,
        ServeOptions {
            ..Default::default()
        },
    )
    .await;
    assert!(server.admin_addr().is_none());
    server.shutdown().await;
}

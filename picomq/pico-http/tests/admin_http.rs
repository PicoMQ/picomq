//! Admin surface over a real socket: liveness, readiness, and the
//! `/admin` introspection and write endpoints.

mod common;

use std::net::SocketAddr;
use std::time::Duration;

use picomq_http::{serve, Protocol, ServeOptions};

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
    assert_eq!(body["nodeId"], 1);
    assert!(
        body["appliedIndex"].as_u64().unwrap() > 0,
        "registration applied: {body}"
    );
}

#[tokio::test]
async fn ready_fails_while_draining() {
    let node = common::start_node().await;
    let loopback = SocketAddr::from(([127, 0, 0, 1], 0));
    let server = serve(
        node,
        ServeOptions {
            protocol: Protocol::Pico,
            addr: loopback,
            admin_addr: Some(loopback),
            shutdown_drain: Duration::from_secs(2),
            ..Default::default()
        },
    )
    .await
    .unwrap();
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
    assert_eq!(cluster["pendingTransfers"], serde_json::json!([]));
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
    let list = nodes["nodes"].as_array().unwrap();
    assert_eq!(list.len(), 1);
    assert_eq!(list[0]["nodeId"], 1);
    assert_eq!(list[0]["local"], true);
    assert_eq!(list[0]["openingCount"], 1, "stream opened by the append");
    assert!(list[0]["slots"].as_u64().unwrap() > 0);

    let stream = client
        .get(format!("{}/admin/streams/orders/live", server.admin_url))
        .send()
        .await
        .unwrap();
    assert_eq!(stream.status(), 200);
    let body: serde_json::Value = stream.json().await.unwrap();
    assert_eq!(body["name"], "/orders/live");
    assert_eq!(body["ownerNodeId"], 1);
    assert_eq!(body["ownerLocal"], true);
    assert_eq!(body["state"], "opened");
    assert_eq!(body["contentType"], "text/plain");
    assert_eq!(body["pendingTransfer"], serde_json::Value::Null);

    let missing = client
        .get(format!("{}/admin/streams/absent", server.admin_url))
        .send()
        .await
        .unwrap();
    assert_eq!(missing.status(), 404);
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
        .post(format!("{}/admin/transfer", server.admin_url))
        .json(&serde_json::json!({ "stream": "/absent", "toNode": 2 }))
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
        .post(format!("{}/admin/transfer", server.admin_url))
        .json(&serde_json::json!({ "stream": "/xfer/a", "toNode": 2 }))
        .send()
        .await
        .unwrap();
    assert!(
        bad_target.status() == 400 || bad_target.status() == 409,
        "unregistered target rejected: {}",
        bad_target.status()
    );

    let malformed = client
        .post(format!("{}/admin/transfer", server.admin_url))
        .json(&serde_json::json!({ "stream": "/xfer/a" }))
        .send()
        .await
        .unwrap();
    assert_eq!(malformed.status(), 400);
}

/// Passes with or without a built dist: `/` serves the dashboard when the
/// assets were embedded and a hint page otherwise.
#[tokio::test]
async fn dashboard_is_served_at_root() {
    let server = common::picomq_server().await;
    let client = reqwest::Client::new();

    let index = client
        .get(format!("{}/", server.admin_url))
        .send()
        .await
        .unwrap();
    assert_eq!(index.status(), 200);
    assert!(index.headers()["Content-Type"]
        .to_str()
        .unwrap()
        .starts_with("text/html"));
    let body = index.text().await.unwrap();
    assert!(body.contains("PicoMQ"), "{body}");

    if let Some(start) = body.find("assets/") {
        let end = start + body[start..].find('"').unwrap();
        let asset = client
            .get(format!("{}/{}", server.admin_url, &body[start..end]))
            .send()
            .await
            .unwrap();
        assert_eq!(asset.status(), 200);
        assert_eq!(
            asset.headers()["Cache-Control"],
            "public, max-age=31536000, immutable"
        );
    }

    let missing = client
        .get(format!("{}/assets/absent.js", server.admin_url))
        .send()
        .await
        .unwrap();
    assert_eq!(missing.status(), 404);
}

#[tokio::test]
async fn admin_listener_can_be_disabled() {
    let node = common::start_node().await;
    let server = serve(
        node,
        ServeOptions {
            addr: SocketAddr::from(([127, 0, 0, 1], 0)),
            admin_addr: None,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert!(server.admin_addr().is_none());
    server.shutdown().await;
}

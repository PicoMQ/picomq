//! Admin surface over a real socket: liveness, readiness, and the
//! `/admin` introspection and write endpoints.

mod common;

use std::net::SocketAddr;
use std::time::Duration;

use picomq_http::{HttpProtocol, ServeOptions, serve};

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
            protocol: HttpProtocol::Pico,
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
    assert!(
        index.headers()["Content-Type"]
            .to_str()
            .unwrap()
            .starts_with("text/html")
    );
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

/// The admin listener uses the native lifecycle implementation, including
/// idempotency, stream configuration, and the sequence headers.
#[tokio::test]
async fn stream_lifecycle_matches_native_protocol() {
    let server = common::picomq_server().await;
    let client = reqwest::Client::new();
    let admin = format!("{}/admin/streams/managed/orders", server.admin_url);
    let native = format!("{}/managed/orders", server.base_url);
    for expected in [201, 200] {
        let created = client
            .put(&admin)
            .header("Content-Type", "text/plain")
            .header("Pico-Kafka-Topic", "managed-orders")
            .send()
            .await
            .unwrap();
        assert_eq!(created.status(), expected);
        assert_eq!(created.headers()["Pico-Next-Seq"], "0");
        assert_eq!(created.headers()["Content-Type"], "text/plain");
        assert_eq!(created.headers()["Pico-Kafka-Topic"], "managed-orders");
        if expected == 201 {
            assert_eq!(
                created.headers()["Location"],
                "/admin/streams/managed/orders"
            );
        }
    }
    let conflict = client
        .put(&admin)
        .header("Content-Type", "application/json")
        .header("Pico-Kafka-Topic", "managed-orders")
        .send()
        .await
        .unwrap();
    assert_eq!(conflict.status(), 409);
    let body: serde_json::Value = conflict.json().await.unwrap();
    assert_eq!(body["error"], "conflict");

    let append = client
        .post(&native)
        .header("Content-Type", "text/plain")
        .body("hello from native protocol")
        .send()
        .await
        .unwrap();
    assert_eq!(append.status(), 200);
    let existing = client
        .put(&admin)
        .header("Content-Type", "text/plain")
        .header("Pico-Kafka-Topic", "managed-orders")
        .send()
        .await
        .unwrap();
    assert_eq!(existing.status(), 200);
    assert_eq!(existing.headers()["Pico-Next-Seq"], "1");
    let head = client.head(&native).send().await.unwrap();
    assert_eq!(head.status(), 200);
    assert_eq!(head.headers()["Pico-Next-Seq"], "1");
    assert_eq!(head.headers()["Pico-Kafka-Topic"], "managed-orders");

    assert_eq!(client.delete(&admin).send().await.unwrap().status(), 204);
    assert_eq!(client.head(&native).send().await.unwrap().status(), 404);
    assert_eq!(client.delete(&admin).send().await.unwrap().status(), 404);
    let recreated = client
        .put(&native)
        .header("Content-Type", "text/plain")
        .header("Pico-Kafka-Topic", "managed-orders")
        .send()
        .await
        .unwrap();
    assert_eq!(recreated.status(), 201, "delete releases the Kafka alias");
    assert_eq!(client.delete(&admin).send().await.unwrap().status(), 204);
}

#[tokio::test]
async fn stream_lifecycle_validates_native_create_options() {
    let server = common::picomq_server().await;
    let client = reqwest::Client::new();
    let url = format!("{}/admin/streams/managed/options", server.admin_url);
    let invalid = [
        client.put(&url).body("create must not append"),
        client.put(&url).header("Pico-TTL", "-1"),
        client.put(&url).header("Pico-TTL", "1.5"),
        client.put(&url).header("Pico-Expires-At", "invalid"),
        client.put(&url).header("Pico-Kafka-Topic", "has spaces"),
        client
            .put(&url)
            .header("Pico-TTL", "60")
            .header("Pico-Expires-At", "2100-01-01T00:00:00Z"),
    ];
    for request in invalid {
        let response = request.send().await.unwrap();
        assert_eq!(response.status(), 400);
        assert!(
            server
                .node
                .service()
                .head("/managed/options")
                .await
                .unwrap()
                .is_none()
        );
    }
    let created = client
        .put(&url)
        .header("Pico-TTL", "3600")
        .header("Pico-Closed", "true")
        .send()
        .await
        .unwrap();
    assert_eq!(created.status(), 201);
    assert_eq!(created.headers()["Pico-TTL"], "3600");
    assert_eq!(created.headers()["Pico-Closed"], "true");
    assert_eq!(
        client
            .delete(&url)
            .body("not empty")
            .send()
            .await
            .unwrap()
            .status(),
        400
    );
    assert!(
        server
            .node
            .service()
            .head("/managed/options")
            .await
            .unwrap()
            .is_some()
    );
    assert_eq!(client.delete(&url).send().await.unwrap().status(), 204);
}

#[tokio::test]
async fn stream_lifecycle_uses_decoded_admin_paths_and_rejects_reserved_names() {
    let server = common::picomq_server().await;
    let client = reqwest::Client::new();
    let native = format!("{}/literal%2Fname", server.base_url);
    assert_eq!(client.put(&native).send().await.unwrap().status(), 201);
    let admin = format!("{}/admin/streams/literal%252Fname", server.admin_url);
    assert_eq!(client.put(&admin).send().await.unwrap().status(), 200);
    assert!(
        server
            .node
            .service()
            .head("/literal/name")
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(client.delete(&admin).send().await.unwrap().status(), 204);
    assert_eq!(client.head(&native).send().await.unwrap().status(), 404);

    for (path, decoded) in [
        ("has%20space", "/has space"),
        ("query%3Fvalue", "/query?value"),
        ("fragment%23value", "/fragment#value"),
        ("back%5Cslash", "/back\\slash"),
        ("caf%C3%A9", "/café"),
    ] {
        for method in [reqwest::Method::PUT, reqwest::Method::DELETE] {
            assert_eq!(
                client
                    .request(method, format!("{}/admin/streams/{path}", server.admin_url))
                    .send()
                    .await
                    .unwrap()
                    .status(),
                400
            );
        }
        assert!(server.node.views().load().state.get_kv(decoded).is_none());
    }

    for prefix in ["_sys", "_schemas", "_streams", "_groups"] {
        // Internal services can legitimately create reserved streams; the
        // general administrative lifecycle must never delete these records.
        let name = format!("/{prefix}/blocked");
        let mut command = picomq_server::CreateCommand::new(&name, "text/plain");
        command.internal = true;
        server.node.service().create(command).await.unwrap();
        let registry = server.node.views().load().state.get_kv(&name).unwrap();
        for path in [format!("{prefix}/blocked"), format!("{prefix}%2Fblocked")] {
            for method in [reqwest::Method::PUT, reqwest::Method::DELETE] {
                let response = client
                    .request(method, format!("{}/admin/streams/{path}", server.admin_url))
                    .send()
                    .await
                    .unwrap();
                assert_eq!(response.status(), 400, "reserved path {path}");
                assert_eq!(
                    server.node.views().load().state.get_kv(&name).unwrap(),
                    registry
                );
            }
        }
    }
}

#[tokio::test]
async fn stream_lifecycle_cors_allows_native_options_and_exposes_metadata() {
    let server = common::picomq_server().await;
    let client = reqwest::Client::new();
    let url = format!("{}/admin/streams/cors", server.admin_url);
    let preflight = client
        .request(reqwest::Method::OPTIONS, &url)
        .header("Origin", "http://dashboard.example")
        .header("Access-Control-Request-Method", "PUT")
        .header(
            "Access-Control-Request-Headers",
            "authorization,pico-kafka-topic",
        )
        .send()
        .await
        .unwrap();
    assert_eq!(preflight.status(), 204);
    let allowed = preflight.headers()["Access-Control-Allow-Headers"]
        .to_str()
        .unwrap()
        .to_ascii_lowercase();
    for header in [
        "authorization",
        "content-type",
        "pico-kafka-topic",
        "pico-ttl",
        "pico-expires-at",
        "pico-closed",
        "pico-schema",
        "pico-schema-validate",
    ] {
        assert!(
            allowed.split(',').any(|h| h.trim() == header),
            "missing {header}: {allowed}"
        );
    }
    let created = client.put(&url).send().await.unwrap();
    assert_eq!(created.status(), 201);
    let exposed = created.headers()["Access-Control-Expose-Headers"]
        .to_str()
        .unwrap()
        .to_ascii_lowercase();
    for header in [
        "location",
        "pico-next-seq",
        "pico-kafka-topic",
        "pico-closed",
    ] {
        assert!(
            exposed.split(',').any(|h| h.trim() == header),
            "missing {header}: {exposed}"
        );
    }
}

/// A second live node must not mutate a remote registry entry or redirect an
/// admin credential to the protocol listener. The owner can still append after
/// both refused operations. An unadvertised owner is still a remote owner.
#[tokio::test]
async fn stream_lifecycle_refuses_remote_owners_and_pending_transfers() {
    use std::sync::Arc;

    use bytes::Bytes;
    use picomq_auth::{AccessToken, TokenRecord, TokenStore as _, scope_from_json};
    use picomq_metadata::{CommandSink, LocalSink, MetadataCommand};
    use picomq_server::registry::RegistryEntry;
    use picomq_server::{
        AppendCommand, CreateCommand, LogRecord, NodeConfig, OffsetToken, PicoNode,
    };
    use s3stream::{MemoryObjectStorage, ObjectStorageTrait};
    use serde_json::json;

    let (sink, views) = LocalSink::new();
    let sink: Arc<dyn CommandSink> = Arc::new(sink);
    let objects: Arc<dyn ObjectStorageTrait> = Arc::new(MemoryObjectStorage::new(2));
    let wal: Arc<dyn ObjectStorageTrait> = Arc::new(MemoryObjectStorage::new(3));
    let mut nodes = Vec::new();
    for node_id in [1, 2] {
        nodes.push(Arc::new(
            PicoNode::start(
                NodeConfig {
                    node_id,
                    node_epoch: 1,
                    http_address: String::new(),
                    engine: s3stream::Config {
                        wal_upload_interval_ms: 200,
                        wal_config: "3@mem://wal?batchInterval=5".into(),
                        ..Default::default()
                    },
                    ..Default::default()
                },
                sink.clone(),
                views.clone(),
                objects.clone(),
                wal.clone(),
                None,
            )
            .await
            .unwrap(),
        ));
    }
    let local = &nodes[0];
    let owner = &nodes[1];
    let mut tokens = Vec::new();
    for (id, prefix) in [
        ("lifecycle/allowed", "/remote/"),
        ("lifecycle/outside", "/other/"),
    ] {
        let (token, verifier) = AccessToken::issue(id).unwrap();
        local
            .tokens()
            .store()
            .put_if_absent(TokenRecord {
                id: token.id.clone(),
                verifier,
                scope: scope_from_json(&json!({
                    "audiences": ["admin"],
                    "ops": ["create", "delete"],
                    "streams": [{ "prefix": prefix }],
                }))
                .unwrap(),
                created_at_ms: 1,
                issued_by: String::new(),
            })
            .await
            .unwrap();
        tokens.push(token.render());
    }
    let loopback = SocketAddr::from(([127, 0, 0, 1], 0));
    let server = serve(
        local.clone(),
        ServeOptions {
            protocol: HttpProtocol::Pico,
            addr: loopback,
            admin_addr: Some(loopback),
            authorizer: Some(local.authorizer()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let url = format!(
        "http://{}/admin/streams/remote/orders",
        server.admin_addr().unwrap()
    );
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();
    owner
        .service()
        .create(CreateCommand::new("/remote/orders", "text/plain"))
        .await
        .unwrap();
    let append = || AppendCommand {
        name: "/remote/orders".into(),
        content_type: Some("text/plain".into()),
        records: vec![LogRecord::value(Bytes::from_static(b"still live"))],
        ..Default::default()
    };
    owner.service().append(append()).await.unwrap();
    let registry = views.load().state.get_kv("/remote/orders").unwrap();
    let stream_id = RegistryEntry::decode(&registry).unwrap().stream_id;

    for method in [reqwest::Method::PUT, reqwest::Method::DELETE] {
        for (token, expected) in [(None, 401), (Some(&tokens[1]), 403)] {
            let mut request = client.request(method.clone(), &url);
            if let Some(token) = token {
                request = request.bearer_auth(token);
            }
            let response = request.send().await.unwrap();
            assert_eq!(response.status(), expected);
            let body: serde_json::Value = response.json().await.unwrap();
            assert!(
                body.get("ownerNodeId").is_none(),
                "authorization precedes owner disclosure"
            );
        }
        let response = client
            .request(method, &url)
            .bearer_auth(&tokens[0])
            .header("Content-Type", "text/plain")
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 409);
        assert!(response.headers().get("Location").is_none());
        let body: serde_json::Value = response.json().await.unwrap();
        assert_eq!(body["code"], "owner_required");
        assert_eq!(body["ownerNodeId"], 2);
        assert_eq!(
            views.load().state.get_kv("/remote/orders").unwrap(),
            registry
        );
        assert_eq!(views.load().state.streams[&stream_id].node_id, 2);
    }
    owner.service().append(append()).await.unwrap();
    let read = owner
        .service()
        .read("/remote/orders", OffsetToken::beginning(), 1024, 0)
        .await
        .unwrap();
    assert_eq!(
        read.records.len(),
        2,
        "non-owner requests leave the live stream usable"
    );

    // Stop the owner, leaving its closed row assigned to node 2. Conservative
    // admin routing refuses the stale-owner state too instead of opening here.
    owner.close().await;
    for method in [reqwest::Method::PUT, reqwest::Method::DELETE] {
        let response = client
            .request(method, &url)
            .bearer_auth(&tokens[0])
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 409);
        let body: serde_json::Value = response.json().await.unwrap();
        assert_eq!(body["code"], "owner_required");
    }

    // Raw proposals model a crashed source that left a transfer pending. Its
    // stopped watcher cannot complete the handoff during these HTTP assertions.
    let epoch = views.load().state.streams[&stream_id].epoch + 1;
    sink.propose(MetadataCommand::OpenStream {
        node_id: 2,
        node_epoch: 1,
        stream_id,
        epoch,
    })
    .await
    .unwrap();
    sink.propose(MetadataCommand::TransferStream {
        stream_id,
        from_node: 2,
        to_node: 1,
    })
    .await
    .unwrap();
    let registry = views.load().state.get_kv("/remote/orders").unwrap();
    for method in [reqwest::Method::PUT, reqwest::Method::DELETE] {
        let response = client
            .request(method, &url)
            .bearer_auth(&tokens[0])
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 409);
        assert!(response.headers().get("Location").is_none());
        let body: serde_json::Value = response.json().await.unwrap();
        assert_eq!(body["code"], "transfer_pending");
        assert_eq!(body["fromNode"], 2);
        assert_eq!(body["toNode"], 1);
        assert_eq!(
            views.load().state.get_kv("/remote/orders").unwrap(),
            registry
        );
        assert!(
            views
                .load()
                .state
                .pending_transfers
                .contains_key(&stream_id)
        );
    }
    server.shutdown().await;
    local.close().await;
}

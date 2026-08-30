mod common;

#[tokio::test]
async fn schema_crud_on_data_plane() {
    let server = common::pico_server_with_schema(pico_schema::Registry::new(
        object_store::memory::InMemory::new(),
    ))
    .await;
    let client = reqwest::Client::new();
    let schema = r#"{
        "title": "Person",
        "type": "object",
        "properties": {
            "value": {
                "type": "object",
                "properties": { "name": { "type": "string" } },
                "required": ["name"]
            }
        }
    }"#;

    let put = client
        .put(format!("{}/_schemas/person", server.base_url))
        .header("Content-Type", "application/schema+json")
        .body(schema)
        .send()
        .await
        .unwrap();
    assert_eq!(put.status(), 204);

    let get = client
        .get(format!("{}/_schemas/person", server.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(get.status(), 200);
    assert_eq!(
        get.headers().get("content-type").unwrap(),
        "application/schema+json"
    );
    assert_eq!(get.text().await.unwrap(), schema);

    let admin_miss = client
        .get(format!("{}/admin/schemas/person", server.admin_url))
        .send()
        .await
        .unwrap();
    assert_ne!(admin_miss.status(), 200);

    let delete = client
        .delete(format!("{}/_schemas/person", server.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(delete.status(), 204);

    let missing = client
        .get(format!("{}/_schemas/person", server.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(missing.status(), 404);
}

#[tokio::test]
async fn append_validates_against_bound_schema() {
    let server = common::pico_server_with_schema(pico_schema::Registry::new(
        object_store::memory::InMemory::new(),
    ))
    .await;
    let client = reqwest::Client::new();
    let schema = r#"{
        "title": "Person",
        "type": "object",
        "properties": {
            "value": {
                "type": "object",
                "properties": { "name": { "type": "string" } },
                "required": ["name"]
            }
        }
    }"#;

    assert_eq!(
        client
            .put(format!("{}/_schemas/person", server.base_url))
            .header("Content-Type", "application/schema+json")
            .body(schema)
            .send()
            .await
            .unwrap()
            .status(),
        204
    );
    assert_eq!(
        client
            .put(format!("{}/people", server.base_url))
            .header("Content-Type", "application/json")
            .header("Pico-Schema", "person")
            .header("Pico-Schema-Validate", "true")
            .send()
            .await
            .unwrap()
            .status(),
        201
    );

    let invalid = client
        .post(format!("{}/people", server.base_url))
        .header("Content-Type", "application/json")
        .body(r#"{"name":1}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(invalid.status(), 400);

    let valid = client
        .post(format!("{}/people", server.base_url))
        .header("Content-Type", "application/json")
        .body(r#"{"name":"alice"}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(valid.status(), 200);
}

#[tokio::test]
async fn stream_config_patch_updates_schema_bind() {
    let server = common::pico_server_with_schema(pico_schema::Registry::new(
        object_store::memory::InMemory::new(),
    ))
    .await;
    let client = reqwest::Client::new();
    let schema = r#"{
        "title": "Person",
        "type": "object",
        "properties": {
            "value": {
                "type": "object",
                "properties": { "name": { "type": "string" } },
                "required": ["name"]
            }
        }
    }"#;

    assert_eq!(
        client
            .put(format!("{}/_schemas/person", server.base_url))
            .header("Content-Type", "application/schema+json")
            .body(schema)
            .send()
            .await
            .unwrap()
            .status(),
        204
    );
    assert_eq!(
        client
            .put(format!("{}/orders", server.base_url))
            .header("Content-Type", "application/json")
            .send()
            .await
            .unwrap()
            .status(),
        201
    );

    let before = client
        .get(format!("{}/_streams/orders", server.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(before.status(), 200);
    let before: serde_json::Value = before.json().await.unwrap();
    assert!(before["schema"].is_null());
    assert_eq!(before["schemaValidate"], false);

    let patch = client
        .patch(format!("{}/_streams/orders", server.base_url))
        .header("Content-Type", "application/json")
        .body(r#"{"schema":"person","schemaValidate":true}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(patch.status(), 200);
    let patched: serde_json::Value = patch.json().await.unwrap();
    assert_eq!(patched["schema"], "person");
    assert_eq!(patched["schemaValidate"], true);

    let got = client
        .get(format!("{}/_streams/orders", server.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(got.status(), 200);
    let got: serde_json::Value = got.json().await.unwrap();
    assert_eq!(got["schema"], "person");
    assert_eq!(got["schemaValidate"], true);

    let clear = client
        .patch(format!("{}/_streams/orders", server.base_url))
        .header("Content-Type", "application/json")
        .body(r#"{"schema":null}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(clear.status(), 200);
    let cleared: serde_json::Value = clear.json().await.unwrap();
    assert!(cleared["schema"].is_null());
    assert_eq!(cleared["schemaValidate"], false);

    let absent = client
        .patch(format!("{}/_streams/absent", server.base_url))
        .header("Content-Type", "application/json")
        .body(r#"{"schemaValidate":true}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(absent.status(), 404);
}

#[tokio::test]
async fn stream_config_redirects_to_remote_owner() {
    use pico_server::ownership::OwnershipService;
    use pico_server::{NodeMeta, Owner, ServiceError};
    use std::sync::Arc;

    struct RemoteOwnership;

    #[async_trait::async_trait]
    impl OwnershipService for RemoteOwnership {
        async fn owner_of(&self, _name: &str) -> Result<Owner, ServiceError> {
            Ok(Owner::remote(7, 2, "http://owner.example:4437/".to_owned()))
        }

        fn local_node(&self) -> NodeMeta {
            NodeMeta {
                node_id: 1,
                advertised_address: "http://127.0.0.1:4437".to_owned(),
            }
        }
    }

    let node = common::start_node().await;
    let router = pico_http::common::router(
        node.service(),
        Arc::new(RemoteOwnership),
        pico_http::RoutingMode::Redirect,
        None,
        32 * 1024 * 1024,
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base_url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });

    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();
    let patch = client
        .patch(format!("{base_url}/_streams/orders"))
        .header("Content-Type", "application/json")
        .body(r#"{"schemaValidate":true}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(patch.status(), 307);
    assert_eq!(
        patch.headers()["Location"],
        "http://owner.example:4437/_streams/orders"
    );

    let get = client
        .get(format!("{base_url}/_streams/orders"))
        .send()
        .await
        .unwrap();
    assert_eq!(get.status(), 307);
}

#[tokio::test]
async fn kafka_mode_exposes_schemas_only() {
    let server = common::kafka_http_with_schema(pico_schema::Registry::new(
        object_store::memory::InMemory::new(),
    ))
    .await;
    let client = reqwest::Client::new();
    let schema = r#"{"type":"object","properties":{"value":{"type":"object"}}}"#;

    let put = client
        .put(format!("{}/_schemas/orders", server.base_url))
        .header("Content-Type", "application/schema+json")
        .body(schema)
        .send()
        .await
        .unwrap();
    assert_eq!(put.status(), 204);

    let stream = client
        .put(format!("{}/orders", server.base_url))
        .header("Content-Type", "text/plain")
        .send()
        .await
        .unwrap();
    assert_eq!(stream.status(), 404);
}

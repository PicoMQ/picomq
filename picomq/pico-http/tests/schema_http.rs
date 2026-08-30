mod common;

#[tokio::test]
async fn schema_crud_on_data_plane() {
    let server = common::pico_server_with_schema("memory://".parse().unwrap()).await;
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
async fn kafka_mode_exposes_schemas_only() {
    let server = common::kafka_http_with_schema("memory://".parse().unwrap()).await;
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

mod common;

use std::net::SocketAddr;

use picomq_auth::{
    AccessToken, Audience, OperationGroups, ReadWrite, ResourceSet, Scope, TokenRecord, TokenStore,
};
use picomq_http::{HttpProtocol, RoutingMode, ServeOptions, serve};
use reqwest::Client;
use serde_json::{Value, json};

async fn create(client: &Client, base: &str, stream: &str) {
    let response = client.put(format!("{base}{stream}")).send().await.unwrap();
    assert_eq!(response.status(), 201);
}

async fn join(client: &Client, base: &str, group: &str, body: Value) -> (u16, Value) {
    let response = client
        .post(format!("{base}/_groups/{group}/members"))
        .json(&body)
        .send()
        .await
        .unwrap();
    let status = response.status().as_u16();
    (status, response.json().await.unwrap())
}

fn sorted(values: &Value) -> Vec<String> {
    let mut out: Vec<String> = values
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap().to_owned())
        .collect();
    out.sort();
    out
}

#[tokio::test]
async fn members_join_heartbeat_commit_and_leave() {
    let server = common::picomq_server().await;
    let base = server.base_url.clone();
    let client = Client::new();
    for stream in ["/jobs/a", "/jobs/b"] {
        create(&client, &base, stream).await;
    }

    let (status, first) = join(
        &client,
        &base,
        "workers",
        json!({
            "clientId": "w1",
            "subscription": ["/jobs/a", "/jobs/b"],
            "sessionTimeoutMs": 10_000,
            "rebalanceTimeoutMs": 2_000,
        }),
    )
    .await;
    assert_eq!(status, 200, "{first}");
    assert_eq!(first["generation"], 1);
    assert_eq!(sorted(&first["assignment"]), ["/jobs/a", "/jobs/b"]);
    let member1 = first["memberId"].as_str().unwrap().to_owned();
    assert_eq!(first["members"], json!([member1]));

    let rejoin = {
        let client = client.clone();
        let base = base.clone();
        let member1 = member1.clone();
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            join(
                &client,
                &base,
                "workers",
                json!({
                    "memberId": member1,
                    "clientId": "w1",
                    "subscription": ["/jobs/a", "/jobs/b"],
                    "sessionTimeoutMs": 10_000,
                }),
            )
            .await
        })
    };
    let (status, second) = join(
        &client,
        &base,
        "workers",
        json!({
            "clientId": "w2",
            "subscription": ["/jobs/a", "/jobs/b"],
            "sessionTimeoutMs": 10_000,
        }),
    )
    .await;
    let (_, rejoined) = rejoin.await.unwrap();
    assert_eq!(status, 200, "{second}");
    assert_eq!(second["generation"], 2);
    assert_eq!(rejoined["generation"], 2);
    let mut all = sorted(&second["assignment"]);
    all.extend(sorted(&rejoined["assignment"]));
    all.sort();
    assert_eq!(all, ["/jobs/a", "/jobs/b"]);
    let member2 = second["memberId"].as_str().unwrap().to_owned();

    let beat = client
        .post(format!(
            "{base}/_groups/workers/members/{member2}/heartbeat"
        ))
        .json(&json!({ "generation": 2 }))
        .send()
        .await
        .unwrap();
    assert_eq!(beat.status(), 204);
    let stale = client
        .post(format!(
            "{base}/_groups/workers/members/{member2}/heartbeat"
        ))
        .json(&json!({ "generation": 1 }))
        .send()
        .await
        .unwrap();
    assert_eq!(stale.status(), 409);
    assert_eq!(
        stale.json::<Value>().await.unwrap()["error"],
        "illegal_generation"
    );

    let current = client
        .get(format!(
            "{base}/_groups/workers/members/{member2}?generation=2"
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(current.status(), 200);
    assert_eq!(
        current.json::<Value>().await.unwrap()["assignment"],
        second["assignment"]
    );

    let owned = second["assignment"][0].as_str().unwrap();
    let commit = client
        .put(format!("{base}/_groups/workers/offsets"))
        .json(&json!({
            "memberId": member2,
            "generation": 2,
            "offsets": { owned: { "position": 7, "metadata": "batch-1" } },
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(commit.status(), 204);
    let fenced = client
        .put(format!("{base}/_groups/workers/offsets"))
        .json(&json!({
            "memberId": member2,
            "generation": 1,
            "offsets": { owned: { "position": 8 } },
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(fenced.status(), 409);

    let fetched: Value = client
        .get(format!("{base}/_groups/workers/offsets?stream={owned}"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(fetched["offsets"][owned]["position"], 7);
    assert_eq!(fetched["offsets"][owned]["metadata"], "batch-1");
    let all: Value = client
        .get(format!("{base}/_groups/workers/offsets"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(all["offsets"].as_object().unwrap().len(), 1);

    let described: Value = client
        .get(format!("{base}/_groups/workers"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(described["state"], "Stable");
    assert_eq!(described["generation"], 2);
    assert_eq!(described["members"].as_array().unwrap().len(), 2);
    let listed: Value = client
        .get(format!("{base}/_groups"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(listed["groups"][0]["group"], "workers");

    let left = client
        .delete(format!("{base}/_groups/workers/members/{member2}"))
        .send()
        .await
        .unwrap();
    assert_eq!(left.status(), 204);
    let after_leave = client
        .post(format!(
            "{base}/_groups/workers/members/{member1}/heartbeat"
        ))
        .json(&json!({ "generation": 2 }))
        .send()
        .await
        .unwrap();
    assert_eq!(after_leave.status(), 409);
    assert_eq!(
        after_leave.json::<Value>().await.unwrap()["error"],
        "rebalance_in_progress"
    );
    let gone = client
        .delete(format!("{base}/_groups/workers/members/{member2}"))
        .send()
        .await
        .unwrap();
    assert_eq!(gone.status(), 404);

    let missing = client
        .get(format!("{base}/_groups/nobody"))
        .send()
        .await
        .unwrap();
    assert_eq!(missing.status(), 404);
    let reserved = client
        .put(format!("{base}/_groups/x"))
        .send()
        .await
        .unwrap();
    assert_ne!(reserved.status(), 201);
}

#[tokio::test]
async fn bad_bodies_are_rejected() {
    let server = common::picomq_server().await;
    let base = server.base_url.clone();
    let client = Client::new();

    let (status, body) = join(&client, &base, "g", json!({ "clientId": "w" })).await;
    assert_eq!(status, 400);
    assert_eq!(body["error"], "bad_request");
    let (status, _) = join(&client, &base, "g", json!({ "subscription": [1] })).await;
    assert_eq!(status, 400);
    let (status, _) = join(
        &client,
        &base,
        "g",
        json!({ "subscription": ["/a"], "sessionTimeoutMs": 1 }),
    )
    .await;
    assert_eq!(status, 400);

    let commit = client
        .put(format!("{base}/_groups/g/offsets"))
        .json(&json!({ "offsets": { "/a": { "position": -1 } } }))
        .send()
        .await
        .unwrap();
    assert_eq!(commit.status(), 400);
    let half_fenced = client
        .put(format!("{base}/_groups/g/offsets"))
        .json(&json!({ "memberId": "m", "offsets": {} }))
        .send()
        .await
        .unwrap();
    assert_eq!(half_fenced.status(), 400);
}

fn read_scope(prefix: &str, auto_prefix: bool) -> Scope {
    Scope {
        streams: ResourceSet::prefix(prefix),
        groups: OperationGroups {
            stream: ReadWrite::read_only(),
            ..OperationGroups::default()
        },
        audiences: [Audience::Pico].into(),
        auto_prefix_streams: auto_prefix,
        ..Scope::default()
    }
}

async fn gated(scope: Scope) -> (picomq_http::RunningServer, String) {
    let node = common::start_node().await;
    let (token, verifier) = AccessToken::issue("it/consumer").unwrap();
    node.tokens()
        .store()
        .put_if_absent(TokenRecord {
            id: token.id.clone(),
            verifier,
            scope,
            created_at_ms: 1,
            issued_by: String::new(),
        })
        .await
        .unwrap();
    node.service()
        .create(picomq_server::CreateCommand::new(
            "/acct/orders",
            "text/plain",
        ))
        .await
        .unwrap();
    let server = serve(
        node.clone(),
        ServeOptions {
            protocol: HttpProtocol::Pico,
            addr: SocketAddr::from(([127, 0, 0, 1], 0)),
            admin_addr: None,
            routing_mode: RoutingMode::LocalAlways,
            authorizer: Some(node.authorizer()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    (server, token.render())
}

#[tokio::test]
async fn group_routes_require_read_on_the_streams() {
    let (server, wire) = gated(read_scope("/acct/", false)).await;
    let base = format!("http://{}", server.local_addr());
    let client = Client::new();

    let anonymous = client
        .post(format!("{base}/_groups/g/members"))
        .json(&json!({ "subscription": ["/acct/orders"] }))
        .send()
        .await
        .unwrap();
    assert_eq!(anonymous.status(), 401);

    let denied = client
        .post(format!("{base}/_groups/g/members"))
        .bearer_auth(&wire)
        .json(&json!({ "subscription": ["/acct/orders", "/other"] }))
        .send()
        .await
        .unwrap();
    assert_eq!(denied.status(), 403);

    let joined: Value = client
        .post(format!("{base}/_groups/g/members"))
        .bearer_auth(&wire)
        .json(&json!({ "subscription": ["/acct/orders"] }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(joined["assignment"], json!(["/acct/orders"]));
    let member = joined["memberId"].as_str().unwrap();

    let commit = client
        .put(format!("{base}/_groups/g/offsets"))
        .bearer_auth(&wire)
        .json(&json!({ "offsets": { "/acct/orders": { "position": 3 } } }))
        .send()
        .await
        .unwrap();
    assert_eq!(commit.status(), 204);
    let elsewhere = client
        .put(format!("{base}/_groups/g/offsets"))
        .bearer_auth(&wire)
        .json(&json!({ "offsets": { "/other": { "position": 3 } } }))
        .send()
        .await
        .unwrap();
    assert_eq!(elsewhere.status(), 403);

    let beat = client
        .post(format!("{base}/_groups/g/members/{member}/heartbeat"))
        .bearer_auth(&wire)
        .json(&json!({ "generation": 1 }))
        .send()
        .await
        .unwrap();
    assert_eq!(beat.status(), 204);

    let describe = client
        .get(format!("{base}/_groups/g"))
        .bearer_auth(&wire)
        .send()
        .await
        .unwrap();
    assert_eq!(describe.status(), 403);
}

#[tokio::test]
async fn auto_prefix_scopes_see_relative_names() {
    let (server, wire) = gated(read_scope("/acct/", true)).await;
    let base = format!("http://{}", server.local_addr());
    let client = Client::new();

    let joined: Value = client
        .post(format!("{base}/_groups/g/members"))
        .bearer_auth(&wire)
        .json(&json!({ "subscription": ["orders"] }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(joined["assignment"], json!(["orders"]));

    let commit = client
        .put(format!("{base}/_groups/g/offsets"))
        .bearer_auth(&wire)
        .json(&json!({ "offsets": { "orders": { "position": 11 } } }))
        .send()
        .await
        .unwrap();
    assert_eq!(commit.status(), 204);
    let fetched: Value = client
        .get(format!("{base}/_groups/g/offsets"))
        .bearer_auth(&wire)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        fetched["offsets"],
        json!({ "orders": { "position": 11, "metadata": null } })
    );
    assert_eq!(
        server
            .node()
            .groups()
            .fetch_offsets("g", None)
            .await
            .unwrap()["/acct/orders"]
            .position,
        11
    );
}

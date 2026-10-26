use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use picomq_auth::{
    Access, AccessToken, Audience, ReadWrite, ResourceSet, Scope, TokenRecord, TokenStore,
};
use picomq_http::forward::Forward;
use picomq_http::{HttpProtocol, RoutingMode, RunningServer, ServeOptions, serve};
use picomq_listener::testing::Authority;
use picomq_listener::{Listener, Tls};
use picomq_metadata::{CommandSink, LocalSink, MetadataCommand, ViewPublisher};
use picomq_metrics::Metrics;
use picomq_server::wal::ObjectWal;
use picomq_server::{CreateCommand, JoinInput, Log, Membership, NodeConfig, PicoNode};
use s3stream::{MemoryObjectStorage, ObjectStorageTrait};

fn loopback() -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], 0))
}

struct Shared {
    sink: Arc<dyn CommandSink>,
    views: Arc<ViewPublisher>,
    storage: Arc<dyn ObjectStorageTrait>,
}

impl Shared {
    fn new() -> Self {
        let (sink, views) = LocalSink::new();
        Self {
            sink: Arc::new(sink),
            views,
            storage: Arc::new(MemoryObjectStorage::new(2)),
        }
    }
}

struct Node {
    node: Arc<PicoNode>,
    server: RunningServer,
    scheme: &'static str,
}

impl Node {
    fn admin(&self) -> String {
        format!("{}://{}", self.scheme, self.server.admin_addr().unwrap())
    }
}

#[derive(Default)]
struct Options {
    advertise_admin: bool,
    tls: Option<Tls>,
    forward: Option<Forward>,
    auth: bool,
    wal_upload_interval_ms: Option<u64>,
}

async fn start(id: i32, shared: &Shared, options: Options) -> Node {
    let scheme = if options.tls.is_some() {
        "https"
    } else {
        "http"
    };
    let listener =
        Listener::bind(picomq_metrics::ListenerKind::Stream, loopback(), 1024, None).unwrap();
    let admin = Listener::bind(
        picomq_metrics::ListenerKind::Admin,
        loopback(),
        1024,
        options.tls.clone(),
    )
    .unwrap();

    let mut protocol_addresses = BTreeMap::new();
    if options.advertise_admin {
        let address = match options.tls {
            Some(_) => format!("https://localhost:{}", admin.local_addr().port()),
            None => format!("http://{}", admin.local_addr()),
        };
        protocol_addresses.insert(picomq_http::admin::PROTOCOL_NAME.to_owned(), address);
    }
    let config = NodeConfig {
        node_id: id,
        node_epoch: 1,
        http_address: format!("http://{}", listener.local_addr()),
        protocol_addresses,
        engine: s3stream::Config {
            wal_upload_interval_ms: options.wal_upload_interval_ms.unwrap_or(200),
            wal_config: format!("{}@mem://wal{id}?batchInterval=5", 10 + id),
            ..Default::default()
        },
        ..Default::default()
    };
    let wal_storage: Arc<dyn ObjectStorageTrait> =
        Arc::new(MemoryObjectStorage::new((10 + id) as i16));
    let wal = ObjectWal::new(&config.cluster_id, &config.engine.wal_config, wal_storage);
    let node = Arc::new(
        PicoNode::start(
            config,
            shared.sink.clone(),
            shared.views.clone(),
            shared.storage.clone(),
            Arc::new(wal),
        )
        .await
        .unwrap(),
    );

    let server = serve(
        node.clone(),
        listener,
        Some(admin),
        ServeOptions {
            protocol: HttpProtocol::Pico,
            routing_mode: RoutingMode::LocalAlways,
            authorizer: options.auth.then(|| node.authorizer()),
            forward: Some(
                options
                    .forward
                    .unwrap_or_else(|| Forward::new(None).unwrap()),
            ),
            metrics: Some(Metrics::install()),
            ..Default::default()
        },
    )
    .await;
    Node {
        node,
        server,
        scheme,
    }
}

async fn issue(node: &PicoNode, id: &str, scope: Scope) -> String {
    let (token, verifier) = AccessToken::issue(id).unwrap();
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
    token.render()
}

fn node_id_of(response: &reqwest::Response) -> Option<String> {
    response
        .headers()
        .get(picomq_http::forward::NODE_ID)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned)
}

#[tokio::test]
async fn node_metrics_are_answered_by_the_node_asked() {
    let shared = Shared::new();
    let first = start(
        1,
        &shared,
        Options {
            advertise_admin: true,
            ..Options::default()
        },
    )
    .await;
    let second = start(
        2,
        &shared,
        Options {
            advertise_admin: true,
            ..Options::default()
        },
    )
    .await;
    let client = reqwest::Client::new();

    for id in ["1", "2"] {
        let response = client
            .get(format!("{}/admin/nodes/{id}/metrics", first.admin()))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
        assert_eq!(node_id_of(&response).as_deref(), Some(id));
        assert!(response.text().await.unwrap().contains("pico_logs_owned"));
    }

    second.server.shutdown().await;
    first.server.shutdown().await;
}

#[tokio::test]
async fn forwarding_refuses_what_it_cannot_reach() {
    let shared = Shared::new();
    let first = start(
        1,
        &shared,
        Options {
            advertise_admin: true,
            ..Options::default()
        },
    )
    .await;
    let silent = start(2, &shared, Options::default()).await;
    let gone = start(
        3,
        &shared,
        Options {
            advertise_admin: true,
            ..Options::default()
        },
    )
    .await;
    gone.server.shutdown().await;
    let client = reqwest::Client::new();
    let status = |path: &str| {
        let request = client.get(format!("{}{path}", first.admin()));
        async move { request.send().await.unwrap().status() }
    };

    assert_eq!(status("/admin/nodes/99/metrics").await, 404);
    assert_eq!(status("/admin/nodes/2/metrics").await, 502);
    assert_eq!(status("/admin/nodes/3/metrics").await, 502);
    let looped = client
        .get(format!("{}/admin/nodes/3/metrics", first.admin()))
        .header(picomq_http::forward::FORWARDED_BY, "7")
        .send()
        .await
        .unwrap();
    assert_eq!(
        looped.status(),
        503,
        "a forwarded request is never forwarded again"
    );

    silent.server.shutdown().await;
    first.server.shutdown().await;
}

#[tokio::test]
async fn the_node_asked_checks_the_callers_scope() {
    let shared = Shared::new();
    let auth = || Options {
        advertise_admin: true,
        auth: true,
        ..Options::default()
    };
    let first = start(1, &shared, auth()).await;
    let second = start(2, &shared, auth()).await;
    let reader = issue(
        &first.node,
        "ops/reader",
        Scope {
            access: Access {
                admin: ReadWrite::read_only(),
                ..Access::default()
            },
            audiences: [Audience::Admin].into(),
            ..Scope::default()
        },
    )
    .await;
    let streams = issue(
        &first.node,
        "ops/streams",
        Scope {
            streams: ResourceSet::prefix(""),
            access: Access {
                stream: ReadWrite::all(),
                ..Access::default()
            },
            audiences: [Audience::Admin].into(),
            ..Scope::default()
        },
    )
    .await;
    let client = reqwest::Client::new();

    let forwarded = client
        .get(format!("{}/admin/nodes/2/metrics", first.admin()))
        .bearer_auth(&reader)
        .send()
        .await
        .unwrap();
    assert_eq!(forwarded.status(), 200);
    assert_eq!(node_id_of(&forwarded).as_deref(), Some("2"));

    let narrow = client
        .get(format!("{}/admin/nodes/2/metrics", first.admin()))
        .bearer_auth(&streams)
        .send()
        .await
        .unwrap();
    assert_eq!(narrow.status(), 403);

    let unvouched = client
        .get(format!("{}/admin/nodes/2/metrics", second.admin()))
        .header(picomq_http::forward::FORWARDED_BY, "1")
        .send()
        .await
        .unwrap();
    assert_eq!(
        unvouched.status(),
        401,
        "a forwarded hop carries no trust of its own"
    );

    second.server.shutdown().await;
    first.server.shutdown().await;
}

#[tokio::test]
async fn forwarding_over_tls_trusts_the_configured_ca() {
    let dir = tempfile::tempdir().unwrap();
    let authority = Authority::new().unwrap();
    let tls = Tls::load(
        authority
            .issue(&["localhost"])
            .unwrap()
            .write(dir.path())
            .unwrap(),
    )
    .unwrap();
    let ca = authority.root_pem().as_bytes().to_vec();

    let shared = Shared::new();
    let trusting = start(
        1,
        &shared,
        Options {
            advertise_admin: true,
            tls: Some(tls.clone()),
            forward: Some(Forward::new(Some(&ca)).unwrap()),
            ..Options::default()
        },
    )
    .await;
    let target = start(
        2,
        &shared,
        Options {
            advertise_admin: true,
            tls: Some(tls.clone()),
            ..Options::default()
        },
    )
    .await;
    let untrusting = start(
        3,
        &shared,
        Options {
            advertise_admin: true,
            tls: Some(tls),
            ..Options::default()
        },
    )
    .await;
    let client = reqwest::Client::builder()
        .add_root_certificate(reqwest::Certificate::from_der(authority.root()).unwrap())
        .build()
        .unwrap();
    let ask = |via: &Node| {
        let request = client.get(format!(
            "https://localhost:{}/admin/nodes/2/metrics",
            via.server.admin_addr().unwrap().port()
        ));
        async move { request.send().await.unwrap() }
    };

    let trusted = ask(&trusting).await;
    assert_eq!(trusted.status(), 200);
    assert_eq!(node_id_of(&trusted).as_deref(), Some("2"));
    assert_eq!(ask(&untrusting).await.status(), 502);
    assert!(Forward::new(Some(b"not a certificate")).is_err());

    untrusting.server.shutdown().await;
    target.server.shutdown().await;
    trusting.server.shutdown().await;
}

async fn transfers_completed(node: &Node, client: &reqwest::Client) -> f64 {
    let text = client
        .get(format!("{}/metrics", node.admin()))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    let series: HashMap<&str, f64> = text
        .lines()
        .filter_map(|line| {
            let (name, value) = line.rsplit_once(' ')?;
            Some((name, value.parse().ok()?))
        })
        .collect();
    series
        .get(r#"pico_transfers_total{status="completed"}"#)
        .copied()
        .unwrap_or(0.0)
}

#[tokio::test]
async fn a_completed_transfer_is_counted() {
    let shared = Shared::new();
    let first = start(
        1,
        &shared,
        Options {
            advertise_admin: true,
            ..Options::default()
        },
    )
    .await;
    let second = start(
        2,
        &shared,
        Options {
            advertise_admin: true,
            ..Options::default()
        },
    )
    .await;
    let client = reqwest::Client::new();
    first
        .node
        .topics()
        .create(CreateCommand::new("/moving", "text/plain"))
        .await
        .unwrap();
    let appended = client
        .post(format!("http://{}/moving", first.server.local_addr()))
        .header("Content-Type", "text/plain")
        .body("opened")
        .send()
        .await
        .unwrap();
    assert_eq!(appended.status(), 200);
    let before = transfers_completed(&first, &client).await;

    let requested = client
        .post(format!("{}/admin/transfers", first.admin()))
        .json(&serde_json::json!({ "topic": "/moving", "toNode": 2 }))
        .send()
        .await
        .unwrap();
    let status = requested.status();
    assert_eq!(status, 202, "{}", requested.text().await.unwrap());

    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while transfers_completed(&first, &client).await < before + 1.0 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the transfer never completed"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    second.server.shutdown().await;
    first.server.shutdown().await;
}

#[tokio::test]
async fn partition_offsets_come_live_from_the_owner() {
    let shared = Shared::new();
    let first = start(
        1,
        &shared,
        Options {
            advertise_admin: true,
            ..Options::default()
        },
    )
    .await;
    let second = start(
        2,
        &shared,
        Options {
            advertise_admin: true,
            wal_upload_interval_ms: Some(0),
            ..Options::default()
        },
    )
    .await;
    let unreachable = start(
        3,
        &shared,
        Options {
            wal_upload_interval_ms: Some(0),
            ..Options::default()
        },
    )
    .await;
    let client = reqwest::Client::new();
    for (topic, owner) in [("/live", &second), ("/dark", &unreachable)] {
        let mut command = CreateCommand::new(topic, "text/plain");
        command.owners = vec![owner.node.config().node_id];
        first.node.topics().create(command).await.unwrap();
        for record in ["a", "b", "c"] {
            let appended = client
                .post(format!("http://{}{topic}", owner.server.local_addr()))
                .header("Content-Type", "text/plain")
                .body(record)
                .send()
                .await
                .unwrap();
            assert_eq!(appended.status(), 200);
        }
    }
    let partition = |topic: &'static str| {
        let request = client.get(format!("{}/admin/topics/{topic}/partitions", first.admin()));
        async move {
            let page: serde_json::Value = request.send().await.unwrap().json().await.unwrap();
            page["items"][0].clone()
        }
    };

    let live = partition("%2Flive").await;
    assert_eq!(live["ownerNodeId"], 2);
    assert_eq!(live["nextOffset"], 3);
    assert_eq!(live["current"], true);

    let dark = partition("%2Fdark").await;
    assert_eq!(dark["ownerNodeId"], 3);
    assert_eq!(
        dark["nextOffset"], 0,
        "only the committed offsets are known"
    );
    assert_eq!(dark["current"], false);

    let fenced = first.node.views().load().state.nodes()[&2].clone();
    first
        .node
        .metadata()
        .fence_node(2, fenced.epoch, fenced.heartbeats)
        .await
        .unwrap();
    let unasked = partition("%2Flive").await;
    assert_eq!(unasked["nextOffset"], 0, "a fenced owner is not asked");
    assert_eq!(unasked["current"], false);

    unreachable.server.shutdown().await;
    second.server.shutdown().await;
    first.server.shutdown().await;
}

#[tokio::test]
async fn groups_are_described_by_their_coordinator() {
    let shared = Shared::new();
    let first = start(
        1,
        &shared,
        Options {
            advertise_admin: true,
            ..Options::default()
        },
    )
    .await;
    let second = start(
        2,
        &shared,
        Options {
            advertise_admin: true,
            ..Options::default()
        },
    )
    .await;
    first
        .node
        .topics()
        .create(CreateCommand::new("/orders", "text/plain"))
        .await
        .unwrap();
    let joined = second
        .node
        .groups()
        .join(
            None,
            JoinInput {
                group_id: "g".to_owned(),
                member_id: String::new(),
                instance_id: None,
                client_id: "worker".to_owned(),
                client_host: "10.0.0.2".to_owned(),
                membership: Membership::Subscribed(vec!["/orders".to_owned()]),
                session_timeout_ms: 30_000,
                rebalance_timeout_ms: 2_000,
                require_known_member_id: false,
            },
        )
        .await;
    let client = reqwest::Client::new();
    let get = |path: &str| {
        let request = client.get(format!("{}{path}", first.admin()));
        async move {
            let response = request.send().await.unwrap();
            let status = response.status().as_u16();
            (status, response.json::<serde_json::Value>().await.unwrap())
        }
    };

    let (_, listed) = get("/admin/groups").await;
    let group = &listed["items"][0];
    assert_eq!(group["groupId"], "g");
    assert_eq!(group["coordinatorNodeId"], 2);
    assert_eq!(group["state"], "Stable");
    assert_eq!(group["members"], 1);
    assert_eq!(group["current"], true);

    let (status, members) = get("/admin/groups/g/members").await;
    assert_eq!(status, 200);
    assert_eq!(members["items"][0]["memberId"], joined.member_id);
    assert_eq!(members["items"][0]["clientHost"], "10.0.0.2");

    second.server.shutdown().await;
    first.server.shutdown().await;
}

#[tokio::test]
async fn records_are_read_and_produced_on_their_owner() {
    let shared = Shared::new();
    let options = || Options {
        advertise_admin: true,
        ..Options::default()
    };
    let first = start(1, &shared, options()).await;
    let second = start(2, &shared, options()).await;
    let client = reqwest::Client::new();
    let mut command = CreateCommand::new("/owned", "text/plain");
    command.owners = vec![2];
    first.node.topics().create(command).await.unwrap();
    for record in ["a", "b", "c"] {
        let appended = client
            .post(format!("http://{}/owned", second.server.local_addr()))
            .header("Content-Type", "text/plain")
            .body(record)
            .send()
            .await
            .unwrap();
        assert_eq!(appended.status(), 200);
    }
    let records = format!(
        "{}/admin/topics/%2Fowned/partitions/0/records",
        first.admin()
    );

    let page: serde_json::Value = client
        .get(&records)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let values: Vec<&str> = page["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|item| item["value"]["data"].as_str().unwrap())
        .collect();
    assert_eq!(values, ["a", "b", "c"]);

    let one: serde_json::Value = client
        .get(format!("{records}/1"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(one["value"]["data"], "b");

    let produced = client
        .post(&records)
        .json(&serde_json::json!({ "value": { "data": "d", "encoding": "utf8" } }))
        .send()
        .await
        .unwrap();
    assert_eq!(produced.status(), 201);
    assert_eq!(
        produced.json::<serde_json::Value>().await.unwrap()["offset"],
        3
    );
    let last: serde_json::Value = client
        .get(format!("{records}/3"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(last["value"]["data"], "d");
    let owners: Vec<i32> = first
        .node
        .views()
        .load()
        .state
        .streams()
        .values()
        .map(|row| row.node_id)
        .collect();
    assert_eq!(owners, [2], "the first node never opened the partition");

    second.server.shutdown().await;
    first.server.shutdown().await;
}

#[tokio::test]
async fn a_trim_is_not_sent_to_a_fenced_owner() {
    let shared = Shared::new();
    let first = start(
        1,
        &shared,
        Options {
            advertise_admin: true,
            ..Options::default()
        },
    )
    .await;
    let dark = start(2, &shared, Options::default()).await;
    let mut command = CreateCommand::new("/stranded", "text/plain");
    command.owners = vec![2];
    first.node.topics().create(command).await.unwrap();
    let row = first.node.views().load().state.nodes()[&2].clone();
    first
        .node
        .metadata()
        .fence_node(2, row.epoch, row.heartbeats)
        .await
        .unwrap();

    let trimmed = reqwest::Client::new()
        .post(format!(
            "{}/admin/topics/%2Fstranded/partitions/0/trim",
            first.admin()
        ))
        .json(&serde_json::json!({ "before": 0 }))
        .send()
        .await
        .unwrap();
    let status = trimmed.status();
    assert_eq!(status, 200, "{}", trimmed.text().await.unwrap());

    dark.server.shutdown().await;
    first.server.shutdown().await;
}

async fn every(client: &reqwest::Client, url: &str) -> (Vec<serde_json::Value>, usize) {
    let mut items = Vec::new();
    let mut pages = 0;
    let mut cursor: Option<String> = None;

    loop {
        let separator = if url.contains('?') { '&' } else { '?' };
        let page_url = match &cursor {
            Some(cursor) => format!("{url}{separator}cursor={cursor}"),
            None => url.to_owned(),
        };
        let page: serde_json::Value = client
            .get(page_url)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        pages += 1;
        items.extend(page["items"].as_array().unwrap().iter().cloned());
        match page["next"].as_str() {
            Some(next) => cursor = Some(next.to_owned()),
            None => return (items, pages),
        }
    }
}

#[tokio::test]
async fn nodes_page_by_id_and_filter_by_fenced() {
    let shared = Shared::new();
    let first = start(1, &shared, Options::default()).await;
    let second = start(2, &shared, Options::default()).await;
    let third = start(3, &shared, Options::default()).await;
    let row = first.node.views().load().state.nodes()[&2].clone();
    first
        .node
        .metadata()
        .fence_node(2, row.epoch, row.heartbeats)
        .await
        .unwrap();
    let client = reqwest::Client::new();
    let nodes = format!("{}/admin/nodes", first.admin());
    let ids = |items: &[serde_json::Value]| -> Vec<i64> {
        items
            .iter()
            .map(|node| node["nodeId"].as_i64().unwrap())
            .collect()
    };

    let (all, pages) = every(&client, &format!("{nodes}?limit=1")).await;
    assert_eq!(ids(&all), [1, 2, 3]);
    assert_eq!(pages, 3);
    let (fenced, _) = every(&client, &format!("{nodes}?fenced=true")).await;
    assert_eq!(ids(&fenced), [2]);
    let (live, _) = every(&client, &format!("{nodes}?fenced=false")).await;
    assert_eq!(ids(&live), [1, 3]);

    let refused = client
        .get(format!("{nodes}?fenced=maybe"))
        .send()
        .await
        .unwrap();
    assert_eq!(refused.status(), 400);

    third.server.shutdown().await;
    second.server.shutdown().await;
    first.server.shutdown().await;
}

#[tokio::test]
async fn transfers_list_the_partitions_and_group_logs_in_flight() {
    let shared = Shared::new();
    let first = start(1, &shared, Options::default()).await;
    shared
        .sink
        .propose(MetadataCommand::RegisterNode {
            node_id: 9,
            node_epoch: 1,
            http_address: String::new(),
            slots: 1,
            protocol_addresses: BTreeMap::new(),
            wal: String::new(),
        })
        .await
        .unwrap();
    let mut command = CreateCommand::new("/moving", "text/plain");
    command.partitions = 2;
    first.node.topics().create(command).await.unwrap();
    first
        .node
        .groups()
        .commit_offsets(None, "workers", None, &[])
        .await
        .unwrap();
    let client = reqwest::Client::new();
    let unopened = client
        .post(format!("{}/admin/transfers", first.admin()))
        .json(&serde_json::json!({ "topic": "/moving", "partition": 0, "toNode": 9 }))
        .send()
        .await
        .unwrap();
    assert_eq!(unopened.status(), 409);
    for partition in 0..2 {
        first
            .node
            .topics()
            .append(
                "/moving",
                partition,
                picomq_server::AppendCommand {
                    records: vec![picomq_server::LogRecord::value("x")],
                    ..Default::default()
                },
            )
            .await
            .unwrap();
    }

    for partition in 0..2 {
        let requested = client
            .post(format!("{}/admin/transfers", first.admin()))
            .json(&serde_json::json!({ "topic": "/moving", "partition": partition, "toNode": 9 }))
            .send()
            .await
            .unwrap();
        let status = requested.status();
        assert_eq!(status, 202, "{}", requested.text().await.unwrap());
    }
    first
        .node
        .transfer(&Log::Group("workers".into()), 9)
        .await
        .unwrap();

    let (transfers, pages) = every(
        &client,
        &format!("{}/admin/transfers?limit=2", first.admin()),
    )
    .await;
    assert_eq!(pages, 2);
    let mut logs: Vec<serde_json::Value> = transfers
        .iter()
        .map(|transfer| {
            assert_eq!(transfer["fromNode"], 1);
            assert_eq!(transfer["toNode"], 9);
            transfer["log"].clone()
        })
        .collect();
    logs.sort_by_key(|log| log.to_string());
    assert_eq!(
        logs,
        [
            serde_json::json!({ "group": "workers" }),
            serde_json::json!({ "topic": "/moving", "partition": 0 }),
            serde_json::json!({ "topic": "/moving", "partition": 1 }),
        ]
    );

    let cluster: serde_json::Value = client
        .get(format!("{}/admin/cluster", first.admin()))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(cluster["pendingTransferCount"], 3);

    first.server.shutdown().await;
}

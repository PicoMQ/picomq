mod common;

use bytes::{BufMut, Bytes, BytesMut};
use common::{Console, create};
use kafka_protocol::messages::consumer_protocol_assignment::TopicPartition;
use kafka_protocol::messages::{ConsumerProtocolAssignment, TopicName};
use kafka_protocol::protocol::{Encodable, StrBytes};
use picomq_server::{
    CommittedOffset, CreateCommand, JoinInput, JoinProtocol, Membership, OffsetCommit, Partition,
    SyncInput,
};
use serde_json::{Value, json};

struct Member {
    id: String,
    generation: i64,
}

impl Console {
    async fn join(&self, group: &str, stream: &str) -> Member {
        let joined: Value = self
            .http
            .post(self.data(&format!("/_groups/{group}/members")))
            .bearer_auth(&self.root)
            .json(&json!({ "subscription": [stream] }))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        Member {
            id: joined["memberId"].as_str().unwrap().to_owned(),
            generation: joined["generation"].as_i64().unwrap(),
        }
    }

    async fn commit(&self, group: &str, member: &Member, stream: &str, position: u64) {
        let committed = self
            .http
            .put(self.data(&format!("/_groups/{group}/offsets")))
            .bearer_auth(&self.root)
            .json(&json!({
                "offsets": { stream: { "position": position } },
                "memberId": member.id,
                "generation": member.generation,
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(committed.status(), 204);
    }

    async fn leave(&self, group: &str, member: &Member) {
        let left = self
            .http
            .delete(self.data(&format!("/_groups/{group}/members/{}", member.id)))
            .bearer_auth(&self.root)
            .send()
            .await
            .unwrap();
        assert!(left.status().is_success(), "{}", left.status());
    }

    async fn send(&self, method: reqwest::Method, path: &str, body: Option<Value>) -> (u16, Value) {
        let mut request = self
            .http
            .request(method, self.admin(path))
            .bearer_auth(&self.root);
        if let Some(body) = body {
            request = request.json(&body);
        }
        let response = request.send().await.unwrap();
        let status = response.status().as_u16();
        (status, response.json().await.unwrap_or(Value::Null))
    }
}

async fn empty_group(console: &Console, group: &str) {
    console
        .node
        .groups()
        .commit_offsets(
            None,
            group,
            None,
            &[OffsetCommit {
                partition: Partition::new(picomq_server::TopicId([7; 16]), 0),
                value: CommittedOffset {
                    position: 1,
                    leader_epoch: None,
                    metadata: None,
                },
            }],
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn groups_page_by_id_with_their_state() {
    let console = Console::start().await;
    create(&console.node, "/orders", 1).await;
    for n in 0..24 {
        empty_group(&console, &format!("idle-{n:02}")).await;
    }
    console.join("busy", "/orders").await;

    let (ids, pages) = console
        .every("/admin/groups?limit=10", &console.root, "groupId")
        .await;
    assert_eq!(ids.len(), 25);
    assert_eq!(pages, 3);

    let (_, page) = console
        .get("/admin/groups?prefix=busy", &console.root)
        .await;
    let busy = &page["items"][0];
    assert_eq!(busy["groupId"], "busy");
    assert_eq!(busy["state"], "Stable");
    assert_eq!(busy["members"], 1);
    assert_eq!(busy["coordinatorNodeId"], 1);
    assert_eq!(busy["current"], true);
    let (_, idle) = console
        .get("/admin/groups?prefix=idle-00", &console.root)
        .await;
    assert_eq!(idle["items"][0]["state"], "Empty");
    assert_eq!(idle["items"][0]["members"], 0);

    console.server.shutdown().await;
}

#[tokio::test]
async fn a_group_shows_its_members_offsets_and_lag() {
    let console = Console::start().await;
    create(&console.node, "/orders", 1).await;
    console.append("/orders", 0, 5).await;
    let member = console.join("readers", "/orders").await;
    console.commit("readers", &member, "/orders", 2).await;

    let (status, detail) = console.get("/admin/groups/readers", &console.root).await;
    assert_eq!(status, 200);
    assert_eq!(detail["members"], 1);
    assert_eq!(detail["committedPartitions"], 1);

    let (_, members) = console
        .get("/admin/groups/readers/members", &console.root)
        .await;
    let listed = &members["items"][0];
    assert_eq!(listed["memberId"], member.id);
    assert_eq!(listed["clientHost"], "127.0.0.1");
    assert_eq!(listed["assignment"], json!({ "streams": ["/orders"] }));

    let (_, offsets) = console
        .get("/admin/groups/readers/offsets", &console.root)
        .await;
    let committed = &offsets["items"][0];
    assert_eq!(committed["topic"], "/orders");
    assert_eq!(committed["partition"], 0);
    assert_eq!(committed["committedOffset"], 2);
    assert_eq!(committed["nextOffset"], 5);
    assert_eq!(committed["lag"], 3);
    assert_eq!(committed["current"], true);
    assert_eq!(
        console.get("/admin/groups/absent", &console.root).await.0,
        404
    );

    console.server.shutdown().await;
}

#[tokio::test]
async fn offsets_reset_only_on_an_empty_group_to_each_target() {
    let console = Console::start().await;
    create(&console.node, "/orders", 1).await;
    console.append("/orders", 0, 5).await;
    let member = console.join("readers", "/orders").await;
    console.commit("readers", &member, "/orders", 2).await;
    let reset = |to: Value| {
        console.send(
            reqwest::Method::POST,
            "/admin/groups/readers/offsets/reset",
            Some(json!({ "topic": "/orders", "to": to })),
        )
    };
    let committed = || async {
        let (_, offsets) = console
            .get("/admin/groups/readers/offsets", &console.root)
            .await;
        offsets["items"][0]["committedOffset"].as_u64().unwrap()
    };

    assert_eq!(reset(json!("earliest")).await.0, 409);
    console.leave("readers", &member).await;

    for (to, expected) in [
        (json!("earliest"), 0),
        (json!("latest"), 5),
        (json!({ "offset": 3 }), 3),
        (json!({ "timestamp": 0 }), 0),
        (json!({ "timestamp": i64::MAX }), 5),
    ] {
        let (status, body) = reset(to.clone()).await;
        assert_eq!(status, 200, "{to}: {body}");
        assert_eq!(committed().await, expected, "{to}");
    }
    assert_eq!(reset(json!("middle")).await.0, 400);

    console.server.shutdown().await;
}

#[tokio::test]
async fn a_reset_creates_offsets_for_a_new_group() {
    let console = Console::start().await;
    create(&console.node, "/orders", 1).await;
    console.append("/orders", 0, 4).await;

    let (status, body) = console
        .send(
            reqwest::Method::POST,
            "/admin/groups/fresh/offsets/reset",
            Some(json!({ "topic": "/orders", "to": "latest" })),
        )
        .await;
    assert_eq!(status, 200, "{body}");
    let (_, offsets) = console
        .get("/admin/groups/fresh/offsets", &console.root)
        .await;
    assert_eq!(offsets["items"][0]["committedOffset"], 4);
    assert_eq!(offsets["items"][0]["lag"], 0);

    console.server.shutdown().await;
}

#[tokio::test]
async fn offsets_and_groups_are_removed_only_once_unsubscribed() {
    let console = Console::start().await;
    create(&console.node, "/orders", 1).await;
    let member = console.join("readers", "/orders").await;
    console.commit("readers", &member, "/orders", 1).await;

    let forget = || {
        console.send(
            reqwest::Method::DELETE,
            "/admin/groups/readers/offsets?topic=%2Forders",
            None,
        )
    };
    let remove = || console.send(reqwest::Method::DELETE, "/admin/groups/readers", None);
    assert_eq!(forget().await.0, 409);
    assert_eq!(remove().await.0, 409);

    console.leave("readers", &member).await;
    assert_eq!(forget().await.0, 204);
    let (_, offsets) = console
        .get("/admin/groups/readers/offsets", &console.root)
        .await;
    assert_eq!(offsets["items"], json!([]));
    assert_eq!(remove().await.0, 204);
    assert_eq!(
        console.get("/admin/groups/readers", &console.root).await.0,
        404
    );

    console.server.shutdown().await;
}

#[tokio::test]
async fn kafka_assignments_read_as_topics_and_partitions() {
    let console = Console::start().await;
    let groups = console.node.groups();
    let joined = groups
        .join(
            None,
            JoinInput {
                group_id: "kafka".to_owned(),
                member_id: String::new(),
                instance_id: None,
                client_id: "consumer-1".to_owned(),
                client_host: "10.0.0.7".to_owned(),
                membership: Membership::Client {
                    protocol_type: "consumer".to_owned(),
                    protocols: vec![JoinProtocol {
                        name: "range".to_owned(),
                        metadata: Bytes::new(),
                    }],
                },
                session_timeout_ms: 10_000,
                rebalance_timeout_ms: 2_000,
                require_known_member_id: false,
            },
        )
        .await;
    let mut assignment = BytesMut::new();
    assignment.put_i16(1);
    ConsumerProtocolAssignment::default()
        .with_assigned_partitions(vec![
            TopicPartition::default()
                .with_topic(TopicName(StrBytes::from_static_str("orders")))
                .with_partitions(vec![0, 1]),
        ])
        .encode(&mut assignment, 1)
        .unwrap();
    groups
        .sync(
            None,
            SyncInput {
                group_id: "kafka".to_owned(),
                generation: joined.generation,
                member_id: joined.member_id.clone(),
                instance_id: None,
                assignments: vec![(joined.member_id.clone(), assignment.freeze())],
            },
        )
        .await
        .unwrap();

    let (_, members) = console
        .get("/admin/groups/kafka/members", &console.root)
        .await;
    let member = &members["items"][0];
    assert_eq!(member["clientHost"], "10.0.0.7");
    assert_eq!(
        member["assignment"],
        json!({ "topics": [{ "topic": "orders", "partitions": [0, 1] }] })
    );

    console.server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn group_pages_stay_bounded_at_the_scale_target() {
    let console = Console::start().await;
    let groups = console.node.groups();
    let commit = |topic, index| OffsetCommit {
        partition: Partition::new(topic, index),
        value: CommittedOffset {
            position: 0,
            leader_epoch: None,
            metadata: None,
        },
    };

    let mut tasks = tokio::task::JoinSet::new();
    for n in 0..100_000 {
        while tasks.len() >= 1_024 {
            tasks.join_next().await.unwrap().unwrap();
        }
        let groups = groups.clone();
        let commits = [commit(picomq_server::TopicId([1; 16]), 0)];
        tasks.spawn(async move {
            groups
                .commit_offsets(None, &format!("scale-{n:06}"), None, &commits)
                .await
                .unwrap();
        });
    }
    while let Some(task) = tasks.join_next().await {
        task.unwrap();
    }
    let wide = console
        .node
        .topics()
        .create(CreateCommand {
            partitions: 10_000,
            ..CreateCommand::new("/wide", "text/plain")
        })
        .await
        .unwrap()
        .meta
        .topic;
    let commits: Vec<OffsetCommit> = (0..10_000).map(|index| commit(wide.id, index)).collect();
    groups
        .commit_offsets(None, "wide", None, &commits)
        .await
        .unwrap();

    for (path, expected) in [
        ("/admin/groups?limit=1000", 100_001),
        ("/admin/groups/wide/offsets?limit=1000", 10_000),
    ] {
        let mut cursor: Option<String> = None;
        let mut seen = 0;
        let mut slowest = std::time::Duration::ZERO;

        loop {
            let url = match &cursor {
                Some(cursor) => format!("{path}&cursor={cursor}"),
                None => path.to_owned(),
            };
            let started = std::time::Instant::now();
            let (status, page) = console.get(&url, &console.root).await;
            slowest = slowest.max(started.elapsed());
            assert_eq!(status, 200, "{page}");

            seen += page["items"].as_array().unwrap().len();
            match page["next"].as_str() {
                Some(next) => cursor = Some(next.to_owned()),
                None => break,
            }
        }

        println!("{path}: {seen} items, slowest page {slowest:?}");
        assert_eq!(seen, expected);
        assert!(slowest < std::time::Duration::from_millis(250));
    }

    console.server.shutdown().await;
}

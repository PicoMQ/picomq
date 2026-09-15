//! Rust client against a running node.
//!
//! ```bash
//! PICO_ENDPOINT=http://127.0.0.1:4437 \
//!   cargo test -p picomq-client --test docker_e2e -- --ignored --test-threads=1
//! ```

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use picomq_client::pico::ProducerRef;
use picomq_client::producer::{Producer, ProducerConfig};
use picomq_client::{
    Assignment, ClientConfig, CommittedOffset, ErrorKind, GroupConfig, GroupMember, JoinRequest,
    Live, MemberFence, Offsets, PicoClient, Protocol, ReadLimits, StreamApi,
};

fn endpoint() -> String {
    std::env::var("PICO_ENDPOINT").unwrap_or_else(|_| "http://127.0.0.1:4437".into())
}

fn unique(prefix: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("/e2e/rs-{prefix}-{nanos}")
}

fn config() -> ClientConfig {
    ClientConfig {
        http2: true,
        token: Some(
            std::env::var("PICO_TOKEN")
                .ok()
                .filter(|v| !v.is_empty())
                .unwrap_or_else(|| {
                    "ZGV2L3Jvb3Q.BwgJCgsMDQ4PEBESExQVFhcYGRobHB0eHyAhIiMkJSY".to_owned()
                }),
        ),
        ..Default::default()
    }
}

fn pico() -> PicoClient {
    pico_at(&endpoint())
}

fn pico_at(url: &str) -> PicoClient {
    let config = config();
    let http = picomq_client::http_client(&config).unwrap();
    PicoClient::with_http(url, http, config.retry)
}

async fn read_bodies(client: &PicoClient, name: &str) -> Vec<String> {
    let page = client
        .read(
            name,
            &client.beginning(),
            Live::Off,
            ReadLimits::server_default(),
        )
        .await
        .unwrap();
    page.records
        .into_iter()
        .map(|r| String::from_utf8(r.body.to_vec()).unwrap())
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn rust_client_lifecycle() {
    let client = pico();
    let name = unique("life");
    assert!(client.create(&name, "text/plain", None).await.unwrap());
    assert!(!client.create(&name, "text/plain", None).await.unwrap());

    let ack = client
        .append(
            &name,
            &[Bytes::from_static(b"one"), Bytes::from_static(b"two")],
            "text/plain",
        )
        .await
        .unwrap();
    assert_eq!(ack.start, "0");
    assert_eq!(ack.next, "2");

    let head = client.head(&name).await.unwrap().unwrap();
    assert_eq!(head.next, "2");
    assert!(!head.closed);

    assert_eq!(read_bodies(&client, &name).await, ["one", "two"]);
    assert_eq!(client.close(&name).await.unwrap(), "2");
    let error = client
        .append(&name, &[Bytes::from_static(b"late")], "text/plain")
        .await
        .unwrap_err();
    assert_eq!(error.kind, ErrorKind::Closed);
    assert!(client.delete(&name).await.unwrap());
    assert!(client.head(&name).await.unwrap().is_none());
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn rust_client_ttl_and_head() {
    let client = pico();
    let name = unique("ttl");
    client.create(&name, "text/plain", Some(2)).await.unwrap();
    let head = client.head(&name).await.unwrap().unwrap();
    assert_eq!(head.ttl_seconds, Some(2));

    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    loop {
        if client.head(&name).await.unwrap().is_none() {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "ttl stream still present"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn rust_client_gap_duplicate_and_fence() {
    let client = pico();
    let name = unique("gap");
    client.create(&name, "text/plain", None).await.unwrap();

    let first = client
        .append_as(
            &name,
            &[Bytes::from_static(b"a")],
            &ProducerRef {
                id: "w1",
                epoch: 1,
                seq: 0,
            },
        )
        .await
        .unwrap();
    assert!(first.applied);

    let again = client
        .append_as(
            &name,
            &[Bytes::from_static(b"a")],
            &ProducerRef {
                id: "w1",
                epoch: 1,
                seq: 0,
            },
        )
        .await
        .unwrap();
    assert!(again.duplicate);

    let gap = client
        .append_as(
            &name,
            &[Bytes::from_static(b"skip")],
            &ProducerRef {
                id: "w1",
                epoch: 1,
                seq: 2,
            },
        )
        .await
        .unwrap_err();
    assert_eq!(gap.kind, ErrorKind::Conflict);
    assert_eq!(gap.code, "sequence_gap");

    let fence = client
        .append_as(
            &name,
            &[Bytes::from_static(b"stale")],
            &ProducerRef {
                id: "w1",
                epoch: 0,
                seq: 1,
            },
        )
        .await
        .unwrap_err();
    assert!(fence.kind == ErrorKind::StaleEpoch || fence.code == "fenced");

    client
        .append_as(
            &name,
            &[Bytes::from_static(b"b")],
            &ProducerRef {
                id: "w1",
                epoch: 1,
                seq: 1,
            },
        )
        .await
        .unwrap();
    assert_eq!(read_bodies(&client, &name).await, ["a", "b"]);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn rust_client_session_order() {
    let client = Arc::new(pico());
    let name = unique("order");
    client.create(&name, "text/plain", None).await.unwrap();
    let producer = Producer::new(
        Arc::clone(&client),
        &name,
        "session",
        ProducerConfig {
            linger: Duration::from_millis(2),
            max_batch_records: 16,
            ..Default::default()
        },
    );
    let count = 200;
    let mut pending = Vec::with_capacity(count);
    for i in 0..count {
        pending.push(producer.send(Bytes::from(i.to_string())).await.unwrap());
    }
    for (i, p) in pending.into_iter().enumerate() {
        assert_eq!(p.durable().await.unwrap(), i as u64);
    }
    producer.close().await.unwrap();
    let bodies = read_bodies(&client, &name).await;
    let expected: Vec<String> = (0..count).map(|i| i.to_string()).collect();
    assert_eq!(bodies, expected);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn rust_client_concurrent_producers() {
    let name = unique("mp");
    pico().create(&name, "text/plain", None).await.unwrap();
    let writers = 6;
    let each = 30;
    let mut tasks = Vec::new();
    for w in 0..writers {
        let name = name.clone();
        tasks.push(tokio::spawn(async move {
            let client = pico();
            for i in 0..each {
                let id = format!("w{w}");
                client
                    .append_as(
                        &name,
                        &[Bytes::from(format!("w{w}-{i}"))],
                        &ProducerRef {
                            id: &id,
                            epoch: 0,
                            seq: i,
                        },
                    )
                    .await
                    .unwrap();
            }
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }
    let bodies = read_bodies(&pico(), &name).await;
    assert_eq!(bodies.len(), (writers * each) as usize);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn rust_and_trait_clients_agree() {
    let boxed = picomq_client::connect_with(Protocol::Pico, &endpoint(), &config()).unwrap();
    let name = unique("trait");
    boxed.create(&name, "text/plain", None).await.unwrap();
    boxed
        .append(&name, &[Bytes::from_static(b"via-trait")], "text/plain")
        .await
        .unwrap();
    let page = boxed
        .read(
            &name,
            &boxed.beginning(),
            Live::Off,
            ReadLimits::server_default(),
        )
        .await
        .unwrap();
    assert_eq!(page.records[0].body, Bytes::from_static(b"via-trait"));
}

fn names(streams: &[&str]) -> Vec<String> {
    streams.iter().map(|s| (*s).to_owned()).collect()
}

fn group_config() -> GroupConfig {
    GroupConfig {
        session_timeout: Duration::from_secs(2),
        heartbeat_interval: Some(Duration::from_millis(100)),
        ..Default::default()
    }
}

async fn next_generation(
    assignments: &mut tokio::sync::watch::Receiver<Assignment>,
    after: i32,
) -> Assignment {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            assignments.changed().await.unwrap();
            let current = assignments.borrow().clone();
            if current.generation > after {
                return current;
            }
        }
    })
    .await
    .expect("a rebalance within the session timeout")
}

fn endpoint2() -> Option<String> {
    std::env::var("PICO_ENDPOINT_2")
        .ok()
        .filter(|v| !v.is_empty())
}

async fn wait_for_group(client: &PicoClient, group: &str) {
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    while client.describe_group(group).await.is_err() {
        assert!(
            std::time::Instant::now() < deadline,
            "group ownership never reached the other node"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn rust_client_groups_join_commit_describe_leave() {
    let client = pico();
    let a = unique("ga");
    let b = unique("gb");
    for stream in [&a, &b] {
        client.create(stream, "text/plain", None).await.unwrap();
    }
    let group = unique("ops").trim_start_matches('/').to_owned();

    let joined = client
        .join_group(&JoinRequest {
            group: group.clone(),
            subscription: names(&[&a, &b]),
            member_id: None,
            instance_id: None,
            client_id: Some("rust-e2e".to_owned()),
            session_timeout_ms: Some(2_000),
            rebalance_timeout_ms: None,
        })
        .await
        .unwrap();
    assert_eq!(joined.generation, 1);
    assert_eq!(joined.assignment, names(&[&a, &b]));

    let fence = MemberFence {
        member_id: joined.member_id.clone(),
        generation: 1,
        instance_id: None,
    };
    client.heartbeat(&group, &fence).await.unwrap();
    assert_eq!(
        client
            .group_assignment(&group, &fence)
            .await
            .unwrap()
            .assignment,
        joined.assignment
    );

    let offsets = Offsets::from([(
        a.clone(),
        CommittedOffset {
            position: 3,
            metadata: Some("ck".to_owned()),
        },
    )]);
    client
        .commit_offsets(&group, Some(&fence), &offsets)
        .await
        .unwrap();
    let stale = MemberFence {
        generation: 0,
        ..fence.clone()
    };
    assert_eq!(
        client
            .commit_offsets(&group, Some(&stale), &offsets)
            .await
            .unwrap_err()
            .code,
        "illegal_generation"
    );
    assert_eq!(client.fetch_offsets(&group, &[]).await.unwrap(), offsets);
    assert!(
        client
            .fetch_offsets(&group, std::slice::from_ref(&b))
            .await
            .unwrap()
            .is_empty()
    );

    let described = client.describe_group(&group).await.unwrap();
    assert_eq!(described.state, "Stable");
    assert_eq!(described.members[0].client_id, "rust-e2e");
    assert!(
        client
            .list_groups()
            .await
            .unwrap()
            .iter()
            .any(|g| g.group == group)
    );

    client
        .leave_group(&group, &joined.member_id, None)
        .await
        .unwrap();
    assert_eq!(client.describe_group(&group).await.unwrap().state, "Empty");
    assert_eq!(
        client.heartbeat(&group, &fence).await.unwrap_err().code,
        "unknown_member"
    );
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn rust_client_groups_rebalance_and_expiry() {
    let client = Arc::new(pico());
    let streams = names(&[&unique("ma"), &unique("mb"), &unique("mc"), &unique("md")]);
    for stream in &streams {
        client.create(stream, "text/plain", None).await.unwrap();
    }
    let group = unique("shared").trim_start_matches('/').to_owned();

    let first = GroupMember::join(Arc::clone(&client), &group, streams.clone(), group_config())
        .await
        .unwrap();
    assert_eq!(first.assignment().streams, streams);
    let mut first_assignments = first.assignments();

    let (second, on_first) = tokio::join!(
        GroupMember::join(Arc::clone(&client), &group, streams.clone(), group_config()),
        next_generation(&mut first_assignments, 1),
    );
    let second = second.unwrap();
    assert_eq!(on_first.generation, 2);
    assert_eq!(second.assignment().generation, 2);
    let mut all = on_first.streams.clone();
    all.extend(second.assignment().streams.clone());
    all.sort();
    let mut expected = streams.clone();
    expected.sort();
    assert_eq!(all, expected);
    assert_eq!(on_first.streams.len(), 2);

    let mine = &second.assignment().streams[0];
    second
        .commit(&Offsets::from([(
            mine.clone(),
            CommittedOffset {
                position: 9,
                metadata: None,
            },
        )]))
        .await
        .unwrap();
    assert_eq!(
        first
            .fetch_offsets(std::slice::from_ref(mine))
            .await
            .unwrap()[mine]
            .position,
        9
    );

    second.leave().await.unwrap();
    assert_eq!(
        next_generation(&mut first_assignments, 2).await.streams,
        streams
    );
    first.leave().await.unwrap();
    assert_eq!(client.describe_group(&group).await.unwrap().state, "Empty");

    let expiry = unique("expiry").trim_start_matches('/').to_owned();
    let stream = unique("xa");
    client.create(&stream, "text/plain", None).await.unwrap();
    let member = GroupMember::join(
        Arc::clone(&client),
        &expiry,
        names(&[&stream]),
        group_config(),
    )
    .await
    .unwrap();
    let original = member.member_id();
    client.leave_group(&expiry, &original, None).await.unwrap();
    let mut assignments = member.assignments();
    assert_eq!(
        next_generation(&mut assignments, 1).await.streams,
        names(&[&stream])
    );
    assert_ne!(member.member_id(), original);
    member.leave().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn rust_client_groups_follow_cluster_redirects() {
    let Some(other) = endpoint2() else {
        panic!("set PICO_ENDPOINT_2 to the second cluster node");
    };
    let owner = pico();
    let follower = pico_at(&other);
    let stream = unique("cluster-g");
    owner.create(&stream, "text/plain", None).await.unwrap();
    let group = unique("cluster-ops").trim_start_matches('/').to_owned();

    let joined = owner
        .join_group(&JoinRequest {
            group: group.clone(),
            subscription: names(&[&stream]),
            member_id: None,
            instance_id: None,
            client_id: Some("rust-cluster".to_owned()),
            session_timeout_ms: Some(2_000),
            rebalance_timeout_ms: None,
        })
        .await
        .unwrap();
    let fence = MemberFence {
        member_id: joined.member_id.clone(),
        generation: joined.generation,
        instance_id: None,
    };
    wait_for_group(&follower, &group).await;
    follower.heartbeat(&group, &fence).await.unwrap();
    assert_eq!(
        follower
            .group_assignment(&group, &fence)
            .await
            .unwrap()
            .assignment,
        names(&[&stream])
    );
    let offsets = Offsets::from([(
        stream.clone(),
        CommittedOffset {
            position: 5,
            metadata: None,
        },
    )]);
    follower
        .commit_offsets(&group, Some(&fence), &offsets)
        .await
        .unwrap();
    assert_eq!(
        follower.fetch_offsets(&group, &[]).await.unwrap()[&stream].position,
        5
    );
    let described = follower.describe_group(&group).await.unwrap();
    assert_eq!(described.state, "Stable");
    follower
        .leave_group(&group, &joined.member_id, None)
        .await
        .unwrap();
    assert_eq!(owner.describe_group(&group).await.unwrap().state, "Empty");
}

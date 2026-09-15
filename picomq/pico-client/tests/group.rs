use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use picomq_client::{
    Assignment, CommittedOffset, GroupConfig, GroupMember, JoinRequest, MemberFence, Offsets,
    PicoClient, StreamApi,
};
use picomq_http::HttpProtocol as ServeProtocol;
use picomq_runtime::{MetaBackend, ServerConfig};

async fn start(dir: &std::path::Path) -> (picomq_runtime::PicoServer, Arc<PicoClient>) {
    let server = picomq_runtime::start(ServerConfig {
        addr: SocketAddr::from(([127, 0, 0, 1], 0)),
        admin_addr: None,
        http_protocol: ServeProtocol::Pico,
        kafka: None,
        meta_backend: MetaBackend::parse("sqlite::memory:").unwrap(),
        storage_uri: format!("1@file://{}", dir.join("objects").display()),
        wal_uri: Some(format!(
            "2@file://{}?batchInterval=5",
            dir.join("wal").display()
        )),
        long_poll_timeout: Duration::from_secs(1),
        ..Default::default()
    })
    .await
    .unwrap();
    let endpoint = format!("http://{}", server.local_addr());
    let client = Arc::new(PicoClient::new(&endpoint).unwrap());
    (server, client)
}

fn names(streams: &[&str]) -> Vec<String> {
    streams.iter().map(|s| (*s).to_owned()).collect()
}

fn config() -> GroupConfig {
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

#[tokio::test]
async fn operations_join_commit_fetch_describe_and_leave() {
    let dir = tempfile::tempdir().unwrap();
    let (server, client) = start(dir.path()).await;
    for stream in ["/g/a", "/g/b"] {
        client.create(stream, "text/plain", None).await.unwrap();
    }

    let joined = client
        .join_group(&JoinRequest {
            group: "ops".to_owned(),
            subscription: names(&["/g/a", "/g/b"]),
            member_id: None,
            instance_id: None,
            client_id: Some("test".to_owned()),
            session_timeout_ms: Some(2_000),
            rebalance_timeout_ms: None,
        })
        .await
        .unwrap();
    assert_eq!(joined.generation, 1);
    assert_eq!(joined.assignment, names(&["/g/a", "/g/b"]));
    assert_eq!(joined.members, [joined.member_id.clone()]);

    let fence = MemberFence {
        member_id: joined.member_id.clone(),
        generation: 1,
        instance_id: None,
    };
    client.heartbeat("ops", &fence).await.unwrap();
    let assignment = client.group_assignment("ops", &fence).await.unwrap();
    assert_eq!(assignment.assignment, joined.assignment);

    let offsets = Offsets::from([(
        "/g/a".to_owned(),
        CommittedOffset {
            position: 3,
            metadata: Some("ck".to_owned()),
        },
    )]);
    client
        .commit_offsets("ops", Some(&fence), &offsets)
        .await
        .unwrap();
    let stale = MemberFence {
        generation: 0,
        ..fence.clone()
    };
    let rejected = client
        .commit_offsets("ops", Some(&stale), &offsets)
        .await
        .unwrap_err();
    assert_eq!(rejected.code, "illegal_generation");
    assert_eq!(client.fetch_offsets("ops", &[]).await.unwrap(), offsets);
    assert!(
        client
            .fetch_offsets("ops", &names(&["/g/b"]))
            .await
            .unwrap()
            .is_empty()
    );

    let described = client.describe_group("ops").await.unwrap();
    assert_eq!(described.state, "Stable");
    assert_eq!(described.members.len(), 1);
    assert_eq!(described.members[0].client_id, "test");
    assert_eq!(
        described.members[0].assignment.as_deref(),
        Some(joined.assignment.as_slice())
    );
    let listed = client.list_groups().await.unwrap();
    assert!(listed.iter().any(|g| g.group == "ops"));

    client
        .leave_group("ops", &joined.member_id, None)
        .await
        .unwrap();
    assert_eq!(client.describe_group("ops").await.unwrap().state, "Empty");
    let gone = client.heartbeat("ops", &fence).await.unwrap_err();
    assert_eq!(gone.code, "unknown_member");

    server.shutdown().await;
}

#[tokio::test]
async fn members_share_streams_and_follow_rebalances() {
    let dir = tempfile::tempdir().unwrap();
    let (server, client) = start(dir.path()).await;
    let streams = names(&["/m/a", "/m/b", "/m/c", "/m/d"]);
    for stream in &streams {
        client.create(stream, "text/plain", None).await.unwrap();
    }

    let first = GroupMember::join(Arc::clone(&client), "shared", streams.clone(), config())
        .await
        .unwrap();
    assert_eq!(first.assignment().generation, 1);
    assert_eq!(first.assignment().streams, streams);
    let mut first_assignments = first.assignments();

    let (second, on_first) = tokio::join!(
        GroupMember::join(Arc::clone(&client), "shared", streams.clone(), config()),
        next_generation(&mut first_assignments, 1),
    );
    let second = second.unwrap();
    assert_eq!(on_first.generation, 2);
    assert_eq!(second.assignment().generation, 2);
    let mut all = on_first.streams.clone();
    all.extend(second.assignment().streams.clone());
    all.sort();
    assert_eq!(all, streams, "every stream lands on exactly one member");
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
        first.fetch_offsets(&[mine.clone()]).await.unwrap()[mine].position,
        9
    );

    second.leave().await.unwrap();
    let after_leave = next_generation(&mut first_assignments, 2).await;
    assert_eq!(after_leave.streams, streams);
    assert!(first.error().is_none());

    first.leave().await.unwrap();
    assert_eq!(
        client.describe_group("shared").await.unwrap().state,
        "Empty"
    );
    server.shutdown().await;
}

#[tokio::test]
async fn expired_member_rejoins_with_a_fresh_id() {
    let dir = tempfile::tempdir().unwrap();
    let (server, client) = start(dir.path()).await;
    client.create("/x/a", "text/plain", None).await.unwrap();

    let member = GroupMember::join(Arc::clone(&client), "expiry", names(&["/x/a"]), config())
        .await
        .unwrap();
    let original = member.member_id();
    client.leave_group("expiry", &original, None).await.unwrap();
    let mut assignments = member.assignments();
    let rejoined = next_generation(&mut assignments, 1).await;
    assert_eq!(rejoined.streams, names(&["/x/a"]));
    assert_ne!(member.member_id(), original);
    assert!(member.error().is_none());

    member.leave().await.unwrap();
    server.shutdown().await;
}

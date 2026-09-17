use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use picomq_metadata::{CommandSink, LocalSink, ViewPublisher};
use picomq_server::{
    CommittedOffset, GroupCoordinator, GroupError, GroupState, JoinInput, JoinOutcome,
    JoinProtocol, Joined, MemberFence, MemberRole, Membership, NodeConfig, OffsetCommit, PicoNode,
    SyncInput,
};
use s3stream::{MemoryObjectStorage, ObjectStorageTrait};

async fn start_node(
    node_id: i32,
    sink: Arc<dyn CommandSink>,
    views: Arc<ViewPublisher>,
) -> PicoNode {
    let object_storage: Arc<dyn ObjectStorageTrait> =
        Arc::new(MemoryObjectStorage::new((node_id * 2) as i16));
    let wal_storage: Arc<dyn ObjectStorageTrait> =
        Arc::new(MemoryObjectStorage::new((node_id * 2 + 1) as i16));
    PicoNode::start(
        NodeConfig {
            node_id,
            node_epoch: 1,
            http_address: format!("http://127.0.0.1:{}", 4000 + node_id),
            ..Default::default()
        },
        sink,
        views,
        object_storage,
        wal_storage,
        None,
    )
    .await
    .unwrap()
}

fn commit(stream: &str, position: u64) -> OffsetCommit {
    OffsetCommit {
        stream: stream.to_owned(),
        value: CommittedOffset {
            position,
            metadata: Some("checkpoint".to_owned()),
        },
    }
}

fn join(group_id: &str, member_id: &str, membership: Membership) -> JoinInput {
    JoinInput {
        group_id: group_id.to_owned(),
        member_id: member_id.to_owned(),
        instance_id: None,
        client_id: "test".to_owned(),
        membership,
        session_timeout_ms: 10_000,
        rebalance_timeout_ms: 2_000,
        require_known_member_id: false,
    }
}

fn subscribed(streams: &[&str]) -> Membership {
    Membership::Subscribed(streams.iter().map(|s| s.to_string()).collect())
}

fn client(protocol_type: &str, protocol: &str, metadata: &str) -> Membership {
    Membership::Client {
        protocol_type: protocol_type.to_owned(),
        protocols: vec![JoinProtocol {
            name: protocol.to_owned(),
            metadata: Bytes::copy_from_slice(metadata.as_bytes()),
        }],
    }
}

fn fence<'a>(generation: i32, member_id: &'a str) -> MemberFence<'a> {
    MemberFence {
        generation,
        member_id,
        instance_id: None,
    }
}

fn assignment_of(outcome: &JoinOutcome) -> Vec<String> {
    match outcome.result.as_ref().unwrap() {
        Joined::Subscribed { assignment, .. } => assignment.clone(),
        Joined::Client { .. } => panic!("expected a subscribed join"),
    }
}

async fn joined_together(
    groups: &Arc<GroupCoordinator>,
    first: JoinInput,
    second: JoinInput,
) -> (JoinOutcome, JoinOutcome) {
    tokio::join!(
        async {
            tokio::time::sleep(Duration::from_millis(50)).await;
            groups.join(first).await
        },
        groups.join(second),
    )
}

#[tokio::test]
async fn find_coordinator_is_the_owner_of_the_group_stream() {
    let (sink, views) = LocalSink::new();
    let sink: Arc<dyn CommandSink> = Arc::new(sink);
    let node1 = start_node(1, sink.clone(), views.clone()).await;
    let node2 = start_node(2, sink.clone(), views.clone()).await;

    node1
        .groups()
        .commit_offsets("orders", None, &[commit("/events", 7)])
        .await
        .unwrap();

    assert_eq!(node1.groups().find_coordinator("orders").await, Ok(1));
    assert_eq!(node2.groups().find_coordinator("orders").await, Ok(1));
    assert_eq!(
        node2.groups().find_coordinator("").await,
        Err(GroupError::InvalidRequest)
    );

    node1.close().await;
    node2.close().await;
}

#[tokio::test]
async fn offsets_replay_into_a_fresh_coordinator() {
    let (sink, views) = LocalSink::new();
    let node = start_node(1, Arc::new(sink), views).await;

    node.groups()
        .commit_offsets("orders", None, &[commit("/events", 42)])
        .await
        .unwrap();

    let restarted = GroupCoordinator::new(
        node.config().node_id,
        node.service(),
        node.ownership(),
        node.views(),
    );
    let fetched = restarted
        .fetch_offsets("orders", Some(&["/events".to_owned(), "/other".to_owned()]))
        .await
        .unwrap();
    assert_eq!(fetched.len(), 1);
    assert_eq!(fetched["/events"].position, 42);
    assert_eq!(fetched["/events"].metadata.as_deref(), Some("checkpoint"));
    assert!(
        restarted
            .fetch_offsets("unknown", None)
            .await
            .unwrap()
            .is_empty()
    );

    node.close().await;
}

#[tokio::test]
async fn subscribed_members_receive_a_server_side_assignment() {
    let (sink, views) = LocalSink::new();
    let node = start_node(1, Arc::new(sink), views).await;
    let groups = node.groups();
    let all = ["/a", "/b", "/c", "/d"];

    let first = groups.join(join("g", "", subscribed(&all))).await;
    assert_eq!(first.generation, 1);
    assert_eq!(assignment_of(&first), all);

    let (rejoin, second) = joined_together(
        &groups,
        join("g", &first.member_id, subscribed(&all)),
        join("g", "", subscribed(&all)),
    )
    .await;
    assert_eq!(rejoin.generation, 2);
    assert_eq!(second.generation, 2);
    let mut union = assignment_of(&rejoin);
    union.extend(assignment_of(&second));
    union.sort();
    assert_eq!(union, all);
    assert_eq!(assignment_of(&rejoin).len(), 2);

    assert_eq!(
        groups
            .assignment("g", fence(2, &second.member_id))
            .await
            .unwrap(),
        assignment_of(&second)
    );
    assert_eq!(
        groups.assignment("g", fence(1, &second.member_id)).await,
        Err(GroupError::IllegalGeneration)
    );
    assert_eq!(
        groups.heartbeat("g", fence(2, "nobody")).await,
        Err(GroupError::UnknownMember)
    );
    assert_eq!(
        groups
            .sync(SyncInput {
                group_id: "g".to_owned(),
                generation: 2,
                member_id: second.member_id.clone(),
                instance_id: None,
                assignments: Vec::new(),
            })
            .await
            .err(),
        Some(GroupError::InvalidRequest)
    );

    let described = groups.describe("g").await.unwrap();
    assert_eq!(described.state, GroupState::Stable);
    assert_eq!(described.generation, 2);
    assert_eq!(described.protocol_type, "");
    assert!(described.members.iter().all(|m| matches!(
        &m.role,
        MemberRole::Subscribed { subscription, .. } if subscription == &all
    )));

    let stream = &assignment_of(&second)[0];
    assert_eq!(
        groups
            .commit_offsets("g", Some(fence(2, &second.member_id)), &[commit(stream, 5)])
            .await,
        Ok(())
    );
    assert_eq!(
        groups
            .commit_offsets("g", Some(fence(1, &second.member_id)), &[commit(stream, 6)])
            .await,
        Err(GroupError::IllegalGeneration)
    );

    assert_eq!(
        groups.leave("g", &[(second.member_id.clone(), None)]).await,
        [Ok(())]
    );
    assert_eq!(
        groups.heartbeat("g", fence(2, &first.member_id)).await,
        Err(GroupError::RebalanceInProgress)
    );
    let third = groups
        .join(join("g", &first.member_id, subscribed(&all)))
        .await;
    assert_eq!(third.generation, 3);
    assert_eq!(assignment_of(&third), all);

    node.close().await;
}

#[tokio::test]
async fn subscriptions_only_receive_their_own_streams() {
    let (sink, views) = LocalSink::new();
    let node = start_node(1, Arc::new(sink), views).await;
    let groups = node.groups();

    let first = groups.join(join("g", "", subscribed(&["/x"]))).await;
    let (rejoin, second) = joined_together(
        &groups,
        join("g", &first.member_id, subscribed(&["/x"])),
        join("g", "", subscribed(&["/x", "/y", "/z"])),
    )
    .await;
    assert_eq!(assignment_of(&rejoin), ["/x"]);
    assert_eq!(assignment_of(&second), ["/y", "/z"]);

    node.close().await;
}

#[tokio::test]
async fn client_members_elect_a_leader_and_distribute_opaque_assignments() {
    let (sink, views) = LocalSink::new();
    let node = start_node(1, Arc::new(sink), views).await;
    let groups = node.groups();

    let first = groups
        .join(join("g", "", client("connect", "sessioned", "worker-a")))
        .await;
    let Ok(Joined::Client {
        protocol_type,
        protocol_name,
        leader,
        members,
    }) = &first.result
    else {
        panic!("{first:?}");
    };
    assert_eq!(protocol_type, "connect");
    assert_eq!(protocol_name, "sessioned");
    assert_eq!(leader, &first.member_id);
    assert_eq!(members.len(), 1);
    assert_eq!(members[0].metadata, Bytes::from_static(b"worker-a"));

    let (rejoin, second) = joined_together(
        &groups,
        join(
            "g",
            &first.member_id,
            client("connect", "sessioned", "worker-a"),
        ),
        join("g", "", client("connect", "sessioned", "worker-b")),
    )
    .await;
    let Ok(Joined::Client {
        leader, members, ..
    }) = &rejoin.result
    else {
        panic!("{rejoin:?}");
    };
    assert_eq!(leader, &first.member_id);
    assert_eq!(members.len(), 2);
    let Ok(Joined::Client {
        members: follower_view,
        ..
    }) = &second.result
    else {
        panic!("{second:?}");
    };
    assert!(follower_view.is_empty());

    let described = groups.describe("g").await.unwrap();
    assert_eq!(described.state, GroupState::CompletingRebalance);
    assert_eq!(described.protocol_type, "connect");

    let (leader_sync, follower_sync) = tokio::join!(
        async {
            tokio::time::sleep(Duration::from_millis(50)).await;
            groups
                .sync(SyncInput {
                    group_id: "g".to_owned(),
                    generation: 2,
                    member_id: first.member_id.clone(),
                    instance_id: None,
                    assignments: vec![
                        (first.member_id.clone(), Bytes::from_static(b"tasks-a")),
                        (second.member_id.clone(), Bytes::from_static(b"tasks-b")),
                    ],
                })
                .await
        },
        groups.sync(SyncInput {
            group_id: "g".to_owned(),
            generation: 2,
            member_id: second.member_id.clone(),
            instance_id: None,
            assignments: Vec::new(),
        }),
    );
    assert_eq!(
        leader_sync.unwrap().assignment,
        Bytes::from_static(b"tasks-a")
    );
    let follower_sync = follower_sync.unwrap();
    assert_eq!(follower_sync.protocol_type, "connect");
    assert_eq!(follower_sync.assignment, Bytes::from_static(b"tasks-b"));

    assert_eq!(
        groups.assignment("g", fence(2, &second.member_id)).await,
        Err(GroupError::InvalidRequest)
    );
    assert_eq!(
        groups.heartbeat("g", fence(2, &second.member_id)).await,
        Ok(())
    );
    let described = groups.describe("g").await.unwrap();
    assert_eq!(described.state, GroupState::Stable);
    assert!(described.members.iter().any(|m| matches!(
        &m.role,
        MemberRole::Client { assignment, .. } if assignment == &Bytes::from_static(b"tasks-b")
    )));

    node.close().await;
}

#[tokio::test]
async fn a_group_keeps_one_membership_mode_until_empty() {
    let (sink, views) = LocalSink::new();
    let node = start_node(1, Arc::new(sink), views).await;
    let groups = node.groups();

    let first = groups.join(join("g", "", subscribed(&["/x"]))).await;
    assert!(first.result.is_ok());
    let clash = groups
        .join(join("g", "", client("consumer", "range", "")))
        .await;
    assert_eq!(clash.result.err(), Some(GroupError::InconsistentProtocol));
    let other_type = groups
        .join(join("h", "", client("consumer", "range", "")))
        .await;
    assert!(other_type.result.is_ok());
    let clash = groups
        .join(join("h", "", client("connect", "sessioned", "")))
        .await;
    assert_eq!(clash.result.err(), Some(GroupError::InconsistentProtocol));

    groups.leave("g", &[(first.member_id.clone(), None)]).await;
    let switched = groups
        .join(join("g", "", client("consumer", "range", "")))
        .await;
    assert!(switched.result.is_ok());

    node.close().await;
}

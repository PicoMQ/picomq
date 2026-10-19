use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use picomq_metadata::{CommandSink, LocalSink, ViewPublisher};
use picomq_server::{
    CommittedOffset, CreateCommand, GroupCoordinator, GroupError, GroupState, JoinInput,
    JoinOutcome, JoinProtocol, Joined, MemberFence, MemberRole, Membership, NodeConfig,
    OffsetCommit, Partition, PicoNode, SyncInput, TopicId,
};
use s3stream::{MemoryObjectStorage, ObjectStorageTrait};
use tokio::task::JoinHandle;

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

fn partition(byte: u8) -> Partition {
    Partition::new(TopicId([byte; 16]), 0)
}

fn commit(partition: Partition, position: u64) -> OffsetCommit {
    OffsetCommit {
        partition,
        value: CommittedOffset {
            position,
            leader_epoch: None,
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
        .commit_offsets("orders", None, &[commit(partition(1), 7)])
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
        .commit_offsets("orders", None, &[commit(partition(1), 42)])
        .await
        .unwrap();

    let restarted = GroupCoordinator::new(
        node.config().node_id,
        node.service(),
        node.topics(),
        node.ownership(),
    );
    let fetched = restarted
        .fetch_offsets("orders", Some(&[partition(1), partition(2)]))
        .await
        .unwrap();
    assert_eq!(fetched.len(), 1);
    assert_eq!(fetched[&partition(1)].position, 42);
    assert_eq!(
        fetched[&partition(1)].metadata.as_deref(),
        Some("checkpoint")
    );
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

    assert!(!assignment_of(&second).is_empty());
    assert_eq!(
        groups
            .commit_offsets(
                "g",
                Some(fence(2, &second.member_id)),
                &[commit(partition(3), 5)]
            )
            .await,
        Ok(())
    );
    assert_eq!(
        groups
            .commit_offsets(
                "g",
                Some(fence(1, &second.member_id)),
                &[commit(partition(3), 6)]
            )
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

#[tokio::test]
async fn snapshots_drop_offsets_of_deleted_topics() {
    let (sink, views) = LocalSink::new();
    let node = start_node(1, Arc::new(sink), views).await;
    let topics = node.topics();

    let kept = topics
        .create(CreateCommand::new("/kept", "text/plain"))
        .await
        .unwrap()
        .meta
        .topic;
    let deleted = topics
        .create(CreateCommand::new("/deleted", "text/plain"))
        .await
        .unwrap()
        .meta
        .topic;
    let kept = Partition::new(kept.id, 0);
    let deleted_partition = Partition::new(deleted.id, 0);

    node.groups()
        .commit_offsets("orders", None, &[commit(deleted_partition, 3)])
        .await
        .unwrap();
    assert!(topics.delete("/deleted").await.unwrap());

    for position in 0..64 {
        node.groups()
            .commit_offsets("orders", None, &[commit(kept, position)])
            .await
            .unwrap();
    }

    let fetched = node.groups().fetch_offsets("orders", None).await.unwrap();
    assert_eq!(fetched.len(), 1);
    assert_eq!(fetched[&kept].position, 63);

    node.close().await;
}

#[tokio::test]
async fn offset_deletes_survive_replay_and_snapshots() {
    let (sink, views) = LocalSink::new();
    let node = start_node(1, Arc::new(sink), views).await;
    let groups = node.groups();

    groups
        .commit_offsets(
            "orders",
            None,
            &[commit(partition(1), 1), commit(partition(2), 2)],
        )
        .await
        .unwrap();
    groups
        .delete_offsets("orders", &[partition(1), partition(3)])
        .await
        .unwrap();

    let replayed = || {
        GroupCoordinator::new(
            node.config().node_id,
            node.service(),
            node.topics(),
            node.ownership(),
        )
    };
    let fetched = replayed().fetch_offsets("orders", None).await.unwrap();
    assert_eq!(fetched.keys().copied().collect::<Vec<_>>(), [partition(2)]);

    for position in 0..64 {
        groups
            .commit_offsets("orders", None, &[commit(partition(2), position)])
            .await
            .unwrap();
    }

    let fetched = replayed().fetch_offsets("orders", None).await.unwrap();
    assert_eq!(fetched.keys().copied().collect::<Vec<_>>(), [partition(2)]);
    assert_eq!(fetched[&partition(2)].position, 63);

    node.close().await;
}

#[tokio::test]
async fn groups_delete_only_when_empty() {
    let (sink, views) = LocalSink::new();
    let node = start_node(1, Arc::new(sink), views).await;
    let groups = node.groups();

    assert_eq!(groups.delete("g").await, Err(GroupError::GroupNotFound));

    let member = groups.join(join("g", "", subscribed(&["/a"]))).await;
    groups
        .commit_offsets("g", None, &[commit(partition(1), 5)])
        .await
        .unwrap();
    assert_eq!(groups.delete("g").await, Err(GroupError::NonEmpty));

    groups.leave("g", &[(member.member_id.clone(), None)]).await;
    groups.delete("g").await.unwrap();

    assert_eq!(groups.delete("g").await, Err(GroupError::GroupNotFound));
    assert!(groups.fetch_offsets("g", None).await.unwrap().is_empty());

    node.close().await;
}

async fn stable_member(groups: &Arc<GroupCoordinator>, group: &str) -> String {
    let first = groups
        .join(join(group, "", client("consumer", "range", "a")))
        .await;
    groups
        .sync(SyncInput {
            group_id: group.to_owned(),
            generation: first.generation,
            member_id: first.member_id.clone(),
            instance_id: None,
            assignments: vec![(first.member_id.clone(), Bytes::from_static(b"all"))],
        })
        .await
        .unwrap();
    first.member_id
}

fn spawn_join(groups: &Arc<GroupCoordinator>, input: JoinInput) -> JoinHandle<JoinOutcome> {
    let groups = Arc::clone(groups);
    tokio::spawn(async move { groups.join(input).await })
}

#[tokio::test]
async fn a_leave_during_a_rebalance_completes_the_pending_joins() {
    let (sink, views) = LocalSink::new();
    let node = start_node(1, Arc::new(sink), views).await;
    let groups = node.groups();
    let first = stable_member(&groups, "g").await;

    let pending = spawn_join(&groups, join("g", "", client("consumer", "range", "b")));
    tokio::time::sleep(Duration::from_millis(100)).await;
    groups.leave("g", &[(first, None)]).await;

    let second = tokio::time::timeout(Duration::from_secs(5), pending)
        .await
        .expect("the pending join completed")
        .unwrap();
    let Ok(Joined::Client {
        leader, members, ..
    }) = &second.result
    else {
        panic!("{second:?}");
    };
    assert_eq!(leader, &second.member_id);
    assert_eq!(members.len(), 1);

    node.close().await;
}

#[tokio::test]
async fn a_newer_join_supersedes_the_pending_one() {
    let (sink, views) = LocalSink::new();
    let node = start_node(1, Arc::new(sink), views).await;
    let groups = node.groups();
    let first = stable_member(&groups, "g").await;

    let named = groups
        .join(JoinInput {
            require_known_member_id: true,
            ..join("g", "", client("consumer", "range", "b"))
        })
        .await;
    assert_eq!(
        named.result.as_ref().err(),
        Some(&GroupError::MemberIdRequired)
    );
    let second = named.member_id;

    let earlier = spawn_join(
        &groups,
        join("g", &second, client("consumer", "range", "b")),
    );
    tokio::time::sleep(Duration::from_millis(100)).await;
    let later = spawn_join(
        &groups,
        join("g", &second, client("consumer", "range", "b")),
    );

    let earlier = earlier.await.unwrap();
    assert_eq!(earlier.result.err(), Some(GroupError::Superseded));

    groups
        .join(join("g", &first, client("consumer", "range", "a")))
        .await
        .result
        .unwrap();
    assert!(later.await.unwrap().result.is_ok());

    node.close().await;
}

#[tokio::test]
async fn a_group_on_a_fenced_node_waits_for_its_log_to_be_recovered() {
    use picomq_metadata::MetadataCommand;

    let (sink, views) = LocalSink::new();
    let sink: Arc<dyn CommandSink> = Arc::new(sink);
    let node1 = start_node(1, sink.clone(), views.clone()).await;
    let node2 = start_node(2, sink.clone(), views).await;

    let created = node2
        .groups()
        .join(join("g", "", client("consumer", "range", "a")))
        .await;
    assert!(created.result.is_ok());

    sink.propose(MetadataCommand::FenceNode {
        node_id: 2,
        node_epoch: 1,
    })
    .await
    .unwrap();

    let rejoined = node1
        .groups()
        .join(JoinInput {
            require_known_member_id: true,
            ..join("g", "", client("consumer", "range", "b"))
        })
        .await;
    assert_eq!(
        rejoined.result.err(),
        Some(GroupError::CoordinatorNotAvailable)
    );

    node1.close().await;
    node2.close().await;
}

#[tokio::test]
async fn an_expired_member_departs_and_the_rebalance_completes_without_it() {
    let (sink, views) = LocalSink::new();
    let node = start_node(1, Arc::new(sink), views).await;
    let groups = node.groups();

    let first = groups
        .join(JoinInput {
            session_timeout_ms: 1_000,
            ..join("g", "", client("consumer", "range", "a"))
        })
        .await;
    groups
        .sync(SyncInput {
            group_id: "g".to_owned(),
            generation: first.generation,
            member_id: first.member_id.clone(),
            instance_id: None,
            assignments: Vec::new(),
        })
        .await
        .unwrap();

    let started = std::time::Instant::now();
    let second = groups
        .join(JoinInput {
            rebalance_timeout_ms: 10_000,
            ..join("g", "", client("consumer", "range", "b"))
        })
        .await;
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "{:?}",
        started.elapsed()
    );
    let Ok(Joined::Client {
        leader, members, ..
    }) = &second.result
    else {
        panic!("{second:?}");
    };
    assert_eq!(leader, &second.member_id);
    assert_eq!(members.len(), 1);

    node.close().await;
}

//! Coordinator properties that do not depend on any wire protocol: which
//! node coordinates a group, and that committed offsets survive a
//! coordinator restart by replaying the group's stream.

use std::sync::Arc;

use picomq_metadata::{CommandSink, LocalSink, ViewPublisher};
use picomq_server::{
    CommittedOffset, GroupCoordinator, GroupError, NodeConfig, OffsetCommit, PicoNode,
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

fn commit(topic: &str, offset: i64) -> OffsetCommit {
    OffsetCommit {
        topic: topic.to_owned(),
        partition: 0,
        value: CommittedOffset {
            offset,
            leader_epoch: 3,
            metadata: Some("checkpoint".to_owned()),
        },
    }
}

#[tokio::test]
async fn find_coordinator_is_the_owner_of_the_group_stream() {
    let (sink, views) = LocalSink::new();
    let sink: Arc<dyn CommandSink> = Arc::new(sink);
    let node1 = start_node(1, sink.clone(), views.clone()).await;
    let node2 = start_node(2, sink.clone(), views.clone()).await;

    node1
        .groups()
        .commit_offsets("orders", -1, "", None, &[commit("events", 7)])
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
        .commit_offsets("orders", -1, "", None, &[commit("events", 42)])
        .await
        .unwrap();

    let restarted = GroupCoordinator::new(
        node.config().node_id,
        node.service(),
        node.ownership(),
        node.views(),
    );
    let fetched = restarted
        .fetch_offsets("orders", Some(&[("events".to_owned(), vec![0])]))
        .await
        .unwrap();
    let (partition, value) = &fetched["events"][0];
    assert_eq!(*partition, 0);
    assert_eq!(value.offset, 42);
    assert_eq!(value.leader_epoch, 3);
    assert_eq!(value.metadata.as_deref(), Some("checkpoint"));

    node.close().await;
}

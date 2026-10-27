//! Classic-group rebalances where another member leaves, joins again, or
//! never comes back while a join is pending, spoken in raw requests so
//! every response is visible.

use std::time::Duration;

use bytes::{BufMut, Bytes, BytesMut};
use kafka_protocol::messages::join_group_request::JoinGroupRequestProtocol;
use kafka_protocol::messages::leave_group_request::MemberIdentity;
use kafka_protocol::messages::offset_commit_request::{
    OffsetCommitRequestPartition, OffsetCommitRequestTopic,
};
use kafka_protocol::messages::sync_group_request::SyncGroupRequestAssignment;
use kafka_protocol::messages::{
    ApiKey, ConsumerProtocolSubscription, GroupId, JoinGroupRequest, JoinGroupResponse,
    LeaveGroupRequest, LeaveGroupResponse, ListGroupsRequest, ListGroupsResponse,
    OffsetCommitRequest, OffsetCommitResponse, SyncGroupRequest, SyncGroupResponse, TopicName,
};
use kafka_protocol::protocol::{Encodable, StrBytes};

use crate::kafka::raw::{Reply, decode, encode, exchange};

const JOIN_VERSION: i16 = 5;
const SYNC_VERSION: i16 = 3;
const LEAVE_VERSION: i16 = 3;
const COMMIT_VERSION: i16 = 7;
const LIST_VERSION: i16 = 4;
const HEADER_VERSION: i16 = 1;
const MEMBER_ID_REQUIRED: i16 = 79;
const PENDING: Duration = Duration::from_millis(500);

pub async fn leave(address: &str, group: &str) -> Vec<String> {
    let (first, second) = match two_members(address, group).await {
        Ok(members) => members,
        Err(failure) => return vec![failure],
    };
    let pending = spawn_join(address, group, &second);

    tokio::time::sleep(PENDING).await;
    let left = leave_group(address, group, &first).await;

    vec![
        format!("leave: {left}"),
        format!(
            "pending join: {}",
            joined(pending.await.expect("join task"), &second)
        ),
    ]
}

pub async fn rejoin(address: &str, group: &str) -> Vec<String> {
    let (first, second) = match two_members(address, group).await {
        Ok(members) => members,
        Err(failure) => return vec![failure],
    };
    let earlier = spawn_join(address, group, &second);

    tokio::time::sleep(PENDING).await;
    let later = spawn_join(address, group, &second);

    tokio::time::sleep(PENDING).await;
    let rejoined = join(address, group, &first).await;

    vec![
        format!(
            "earlier join: {}",
            joined(earlier.await.expect("join task"), &second)
        ),
        format!(
            "later join: {}",
            joined(later.await.expect("join task"), &second)
        ),
        format!("first member: {}", joined(rejoined, &first)),
    ]
}

pub async fn missing(address: &str, group: &str) -> Vec<String> {
    let (_, second) = match two_members(address, group).await {
        Ok(members) => members,
        Err(failure) => return vec![failure],
    };

    vec![format!(
        "join: {}",
        joined(join(address, group, &second).await, &second)
    )]
}

pub async fn simple_commit(address: &str, group: &str) -> Vec<String> {
    let (member, generation) = match stable_member(address, group).await {
        Ok(stable) => stable,
        Err(failure) => return vec![failure],
    };

    let unfenced = commit(address, group, "", -1).await;
    let fenced = commit(address, group, &member, generation).await;
    let left = leave_group(address, group, &member).await;
    let empty = commit(address, group, "", -1).await;

    vec![
        format!("unfenced commit while stable: {unfenced}"),
        format!("member commit: {fenced}"),
        format!("leave: {left}"),
        format!("unfenced commit once empty: {empty}"),
    ]
}

pub async fn list_empty(address: &str, group: &str) -> Vec<String> {
    let committed = commit(address, group, "", -1).await;
    let reply = exchange(
        address,
        ApiKey::ListGroups,
        LIST_VERSION,
        HEADER_VERSION,
        encode(&ListGroupsRequest::default(), LIST_VERSION),
    )
    .await;

    let Reply::Frame(mut frame) = reply else {
        return vec![format!("commit: {committed}"), silent(reply)];
    };
    let listed = match decode::<ListGroupsResponse>(&mut frame, ApiKey::ListGroups, LIST_VERSION) {
        Ok(response) => response
            .groups
            .iter()
            .find(|listed| listed.group_id.as_str() == group)
            .map_or("absent".to_owned(), |listed| {
                format!("listed, state {}", listed.group_state.as_str())
            }),
        Err(_) => "unreadable".to_owned(),
    };

    vec![format!("commit: {committed}"), format!("group: {listed}")]
}

/// A stable first member that holds generation 1, and the member id the
/// coordinator handed a second member.
async fn two_members(address: &str, group: &str) -> Result<(String, String), String> {
    let (first, _) = stable_member(address, group).await?;
    let second = member_id(address, group)
        .await
        .map_err(|step| format!("second member id: {step}"))?;

    Ok((first, second))
}

async fn stable_member(address: &str, group: &str) -> Result<(String, i32), String> {
    let member = member_id(address, group)
        .await
        .map_err(|step| format!("first member id: {step}"))?;
    let generation = match join(address, group, &member).await {
        Reply::Frame(mut frame) => {
            match decode::<JoinGroupResponse>(&mut frame, ApiKey::JoinGroup, JOIN_VERSION) {
                Ok(response) if response.error_code == 0 => response.generation_id,
                reply => return Err(format!("first join: {}", step(reply))),
            }
        }
        reply => return Err(format!("first join: {}", silent(reply))),
    };
    sync(address, group, &member, generation)
        .await
        .map_err(|step| format!("first sync: {step}"))?;

    Ok((member, generation))
}

async fn commit(address: &str, group: &str, member: &str, generation: i32) -> String {
    let request = OffsetCommitRequest::default()
        .with_group_id(group_id(group))
        .with_generation_id_or_member_epoch(generation)
        .with_member_id(StrBytes::from(member.to_owned()))
        .with_topics(vec![
            OffsetCommitRequestTopic::default()
                .with_name(TopicName(StrBytes::from(group.to_owned())))
                .with_partitions(vec![
                    OffsetCommitRequestPartition::default()
                        .with_partition_index(0)
                        .with_committed_offset(1),
                ]),
        ]);
    let reply = exchange(
        address,
        ApiKey::OffsetCommit,
        COMMIT_VERSION,
        HEADER_VERSION,
        encode(&request, COMMIT_VERSION),
    )
    .await;

    let Reply::Frame(mut frame) = reply else {
        return silent(reply);
    };
    match decode::<OffsetCommitResponse>(&mut frame, ApiKey::OffsetCommit, COMMIT_VERSION) {
        Ok(response) => response
            .topics
            .iter()
            .flat_map(|topic| &topic.partitions)
            .map(|partition| format!("error {}", partition.error_code))
            .collect::<Vec<_>>()
            .join(", "),
        Err(_) => "unreadable".to_owned(),
    }
}

async fn member_id(address: &str, group: &str) -> Result<String, String> {
    match join(address, group, "").await {
        Reply::Frame(mut frame) => {
            match decode::<JoinGroupResponse>(&mut frame, ApiKey::JoinGroup, JOIN_VERSION) {
                Ok(response) if response.error_code == MEMBER_ID_REQUIRED => {
                    Ok(response.member_id.to_string())
                }
                reply => Err(step(reply)),
            }
        }
        reply => Err(silent(reply)),
    }
}

async fn sync(address: &str, group: &str, member: &str, generation: i32) -> Result<(), String> {
    let request = SyncGroupRequest::default()
        .with_group_id(group_id(group))
        .with_generation_id(generation)
        .with_member_id(StrBytes::from(member.to_owned()))
        .with_assignments(vec![
            SyncGroupRequestAssignment::default()
                .with_member_id(StrBytes::from(member.to_owned()))
                .with_assignment(Bytes::from_static(b"assignment")),
        ]);
    let reply = exchange(
        address,
        ApiKey::SyncGroup,
        SYNC_VERSION,
        HEADER_VERSION,
        encode(&request, SYNC_VERSION),
    )
    .await;

    match reply {
        Reply::Frame(mut frame) => {
            match decode::<SyncGroupResponse>(&mut frame, ApiKey::SyncGroup, SYNC_VERSION) {
                Ok(response) if response.error_code == 0 => Ok(()),
                Ok(response) => Err(format!("error {}", response.error_code)),
                Err(_) => Err("unreadable".to_owned()),
            }
        }
        reply => Err(silent(reply)),
    }
}

fn step(reply: anyhow::Result<JoinGroupResponse>) -> String {
    match reply {
        Ok(response) => format!("error {}", response.error_code),
        Err(_) => "unreadable".to_owned(),
    }
}

fn silent(reply: Reply) -> String {
    match reply {
        Reply::Frame(_) => "response".to_owned(),
        Reply::Closed => "closed".to_owned(),
        Reply::Timeout => "no reply".to_owned(),
    }
}

fn spawn_join(address: &str, group: &str, member: &str) -> tokio::task::JoinHandle<Reply> {
    let (address, group, member) = (address.to_owned(), group.to_owned(), member.to_owned());
    tokio::spawn(async move { join(&address, &group, &member).await })
}

async fn join(address: &str, group: &str, member: &str) -> Reply {
    let request = JoinGroupRequest::default()
        .with_group_id(group_id(group))
        .with_session_timeout_ms(10_000)
        .with_rebalance_timeout_ms(3_000)
        .with_member_id(StrBytes::from(member.to_owned()))
        .with_protocol_type(StrBytes::from_static_str("consumer"))
        .with_protocols(vec![
            JoinGroupRequestProtocol::default()
                .with_name(StrBytes::from_static_str("range"))
                .with_metadata(subscription(group)),
        ]);

    exchange(
        address,
        ApiKey::JoinGroup,
        JOIN_VERSION,
        HEADER_VERSION,
        encode(&request, JOIN_VERSION),
    )
    .await
}

async fn leave_group(address: &str, group: &str, member: &str) -> String {
    let request = LeaveGroupRequest::default()
        .with_group_id(group_id(group))
        .with_members(vec![
            MemberIdentity::default().with_member_id(StrBytes::from(member.to_owned())),
        ]);
    let reply = exchange(
        address,
        ApiKey::LeaveGroup,
        LEAVE_VERSION,
        HEADER_VERSION,
        encode(&request, LEAVE_VERSION),
    )
    .await;

    let Reply::Frame(mut frame) = reply else {
        return silent(reply);
    };

    match decode::<LeaveGroupResponse>(&mut frame, ApiKey::LeaveGroup, LEAVE_VERSION) {
        Ok(response) => format!("error {}", response.error_code),
        Err(_) => "unreadable".to_owned(),
    }
}

fn joined(reply: Reply, member: &str) -> String {
    let Reply::Frame(mut frame) = reply else {
        return silent(reply);
    };

    match decode::<JoinGroupResponse>(&mut frame, ApiKey::JoinGroup, JOIN_VERSION) {
        Ok(response) if response.error_code == 0 => format!(
            "error 0, {}, {} member(s)",
            if response.leader.as_str() == member {
                "leader"
            } else {
                "follower"
            },
            response.members.len()
        ),
        reply => step(reply),
    }
}

/// The topic is named after the group, as the scenario's topic is.
fn subscription(group: &str) -> Bytes {
    let mut metadata = BytesMut::new();
    metadata.put_i16(0);
    ConsumerProtocolSubscription::default()
        .with_topics(vec![StrBytes::from(group.to_owned())])
        .encode(&mut metadata, 0)
        .expect("subscription encodes");
    metadata.freeze()
}

fn group_id(group: &str) -> GroupId {
    GroupId(StrBytes::from(group.to_owned()))
}

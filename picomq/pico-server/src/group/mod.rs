//! Classic consumer-group coordination backed by one internal stream per group.

mod offsets;
mod state;

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Instant;

use crate::{
    AppendCommand, CreateCommand, ErrorKind, LogRecord, MetadataOwnershipService, OffsetToken,
    OwnershipService, S3StreamService,
};
use bytes::Bytes;
use tokio::sync::{Mutex, oneshot};

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum GroupError {
    #[error("group coordinator is not available")]
    CoordinatorNotAvailable,
    #[error("this node is not the group coordinator")]
    NotCoordinator,
    #[error("illegal group generation")]
    IllegalGeneration,
    #[error("group members use inconsistent protocols")]
    InconsistentProtocol,
    #[error("unknown group member")]
    UnknownMember,
    #[error("group rebalance is in progress")]
    RebalanceInProgress,
    #[error("invalid group request")]
    InvalidRequest,
    #[error("group state could not be persisted")]
    Storage,
    #[error("group was not found")]
    GroupNotFound,
    #[error("the member ID assigned by the coordinator is required")]
    MemberIdRequired,
    #[error("group capacity was exceeded")]
    CapacityExceeded,
    #[error("static group member was fenced")]
    FencedInstance,
}

pub use offsets::{CommittedOffset, OffsetCommit};

use offsets::{OffsetTable, decode_into, empty_offset_fetch, encode_commits, encode_snapshot};
use state::{
    Group, GroupPhase, MAX_GROUPS, MAX_MEMBERS_PER_GROUP, Rebalance, complete_rebalance,
    group_stream_name, member_from_input, new_member_id, prune_empty_groups, remove_member,
    send_join_completions, validate_group_id, validate_join,
};

const GROUP_CONTENT_TYPE: &str = "application/vnd.picomq.kafka-group-state";
const OFFSET_SNAPSHOT_INTERVAL: u64 = 64;

#[derive(Debug, Clone)]
pub struct CoordinatorEndpoint {
    pub node_id: i32,
    pub address: String,
}

#[derive(Debug, Clone)]
pub struct JoinProtocol {
    pub name: String,
    pub metadata: Bytes,
}

#[derive(Debug, Clone)]
pub struct JoinInput {
    pub group_id: String,
    pub member_id: String,
    pub group_instance_id: Option<String>,
    pub protocol_type: String,
    pub protocols: Vec<JoinProtocol>,
    pub session_timeout_ms: i32,
    pub rebalance_timeout_ms: i32,
    pub client_id: String,
    pub require_known_member_id: bool,
}

#[derive(Debug, Clone)]
pub struct JoinMember {
    pub member_id: String,
    pub group_instance_id: Option<String>,
    pub metadata: Bytes,
}

#[derive(Debug, Clone)]
pub struct JoinOutcome {
    pub error: Option<GroupError>,
    pub generation_id: i32,
    pub protocol_type: Option<String>,
    pub protocol_name: Option<String>,
    pub leader: String,
    pub member_id: String,
    pub members: Vec<JoinMember>,
}

impl JoinOutcome {
    pub(crate) fn error(error: GroupError, member_id: String) -> Self {
        Self {
            error: Some(error),
            generation_id: -1,
            protocol_type: None,
            protocol_name: None,
            leader: String::new(),
            member_id,
            members: Vec::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct SyncInput {
    pub group_id: String,
    pub generation_id: i32,
    pub member_id: String,
    pub group_instance_id: Option<String>,
    pub assignments: Vec<(String, Bytes)>,
}

#[derive(Debug, Clone)]
pub struct SyncOutcome {
    pub error: Option<GroupError>,
    pub protocol_type: Option<String>,
    pub protocol_name: Option<String>,
    pub assignment: Bytes,
}

impl SyncOutcome {
    fn error(error: GroupError) -> Self {
        Self {
            error: Some(error),
            protocol_type: None,
            protocol_name: None,
            assignment: Bytes::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct MemberDescription {
    pub member_id: String,
    pub group_instance_id: Option<String>,
    pub client_id: String,
    pub metadata: Bytes,
    pub assignment: Bytes,
}

#[derive(Debug, Clone)]
pub struct GroupDescription {
    pub error: Option<GroupError>,
    pub group_id: String,
    pub state: String,
    pub protocol_type: String,
    pub protocol_name: String,
    pub members: Vec<MemberDescription>,
}

#[derive(Debug, Clone)]
pub struct ListedGroup {
    pub group_id: String,
    pub protocol_type: String,
    pub state: String,
}

pub struct GroupCoordinator {
    node_id: i32,
    protocol_name: &'static str,
    service: Arc<S3StreamService>,
    ownership: Arc<MetadataOwnershipService>,
    views: Arc<picomq_metadata::ViewPublisher>,
    groups: StdMutex<HashMap<String, Arc<Mutex<Group>>>>,
}

impl GroupCoordinator {
    pub fn new(
        node_id: i32,
        service: Arc<S3StreamService>,
        ownership: Arc<MetadataOwnershipService>,
        views: Arc<picomq_metadata::ViewPublisher>,
        protocol_name: &'static str,
    ) -> Arc<Self> {
        Arc::new(Self {
            node_id,
            protocol_name,
            service,
            ownership,
            views,
            groups: StdMutex::new(HashMap::new()),
        })
    }

    pub async fn find_coordinator(
        &self,
        group_id: &str,
    ) -> Result<CoordinatorEndpoint, GroupError> {
        validate_group_id(group_id)?;
        let stream = group_stream_name(group_id);
        let owner = self
            .ownership
            .owner_of(&stream)
            .await
            .map_err(|_| GroupError::CoordinatorNotAvailable)?;
        let node_id = if owner.local {
            self.node_id
        } else {
            owner
                .owner_node_id
                .ok_or(GroupError::CoordinatorNotAvailable)?
        };
        let view = self.views.load();
        let address = view
            .state
            .get_node_protocol_address(node_id, self.protocol_name)
            .filter(|address| !address.is_empty())
            .ok_or(GroupError::CoordinatorNotAvailable)?
            .to_owned();
        Ok(CoordinatorEndpoint { node_id, address })
    }

    pub async fn join(self: &Arc<Self>, input: JoinInput) -> JoinOutcome {
        if let Err(code) = validate_join(&input) {
            return JoinOutcome::error(code, input.member_id);
        }
        let group = match self.local_group(&input.group_id, true).await {
            Ok(group) => group,
            Err(code) => return JoinOutcome::error(code, input.member_id),
        };

        let (receiver, completion, schedule) = {
            let mut state = group.lock().await;
            state.expire_members(Instant::now());

            let mut member_id = input.member_id.clone();
            if member_id.is_empty() {
                if let Some(instance_id) = input.group_instance_id.as_deref()
                    && let Some((existing, _)) = state
                        .members
                        .iter()
                        .find(|(_, member)| member.instance_id.as_deref() == Some(instance_id))
                {
                    member_id = existing.clone();
                }
                if member_id.is_empty() {
                    if state.members.len() >= MAX_MEMBERS_PER_GROUP {
                        return JoinOutcome::error(GroupError::CapacityExceeded, String::new());
                    }
                    member_id = new_member_id(&input.client_id);
                    state
                        .members
                        .insert(member_id.clone(), member_from_input(&input, Instant::now()));
                    if input.require_known_member_id {
                        return JoinOutcome::error(GroupError::MemberIdRequired, member_id);
                    }
                }
            } else if !state.members.contains_key(&member_id) {
                return JoinOutcome::error(GroupError::UnknownMember, member_id);
            }

            if let Some(instance_id) = input.group_instance_id.as_deref() {
                let stale: Vec<String> = state
                    .members
                    .iter()
                    .filter(|(id, member)| {
                        *id != &member_id && member.instance_id.as_deref() == Some(instance_id)
                    })
                    .map(|(id, _)| id.clone())
                    .collect();
                for id in stale {
                    remove_member(&mut state, &id);
                }
            }

            let Some(member) = state.members.get_mut(&member_id) else {
                return JoinOutcome::error(GroupError::UnknownMember, member_id);
            };
            if member.instance_id != input.group_instance_id {
                return JoinOutcome::error(GroupError::FencedInstance, member_id);
            }
            *member = member_from_input(&input, Instant::now());

            if !state.protocol_type.is_empty() && state.protocol_type != input.protocol_type {
                return JoinOutcome::error(GroupError::InconsistentProtocol, member_id);
            }
            state.protocol_type = input.protocol_type.clone();

            let mut schedule = None;
            if state.rebalance.is_none() {
                state.phase = GroupPhase::PreparingRebalance;
                let id = state.next_rebalance_id;
                state.next_rebalance_id = state.next_rebalance_id.wrapping_add(1).max(1);
                let timeout = state
                    .members
                    .values()
                    .map(|member| member.rebalance_timeout)
                    .max()
                    .unwrap_or(std::time::Duration::from_secs(1));
                let deadline = Instant::now() + timeout;
                state.rebalance = Some(Rebalance {
                    id,
                    expected: state.members.keys().cloned().collect(),
                    joined: BTreeSet::new(),
                    waiters: BTreeMap::new(),
                });
                schedule = Some((id, deadline));
            }

            let (sender, receiver) = oneshot::channel();
            let rebalance = state.rebalance.as_mut().expect("rebalance initialized");
            rebalance.expected.insert(member_id.clone());
            rebalance.joined.insert(member_id.clone());
            rebalance.waiters.insert(member_id, sender);
            let ready = rebalance.joined == rebalance.expected;
            let completion = ready.then(|| complete_rebalance(&mut state, false));
            (receiver, completion, schedule)
        };

        send_join_completions(completion);
        if let Some((id, deadline)) = schedule {
            let coordinator = Arc::clone(self);
            let group_id = input.group_id.clone();
            tokio::spawn(async move {
                coordinator.watch_rebalance(&group_id, id, deadline).await;
            });
        }
        receiver.await.unwrap_or_else(|_| {
            JoinOutcome::error(GroupError::RebalanceInProgress, input.member_id)
        })
    }

    /// Drives a pending rebalance to completion without waiting for the full
    /// rebalance timeout when the only missing members are dead. Kafka removes
    /// a member once its session expires and completes the rebalance as soon as
    /// every surviving member has joined; the timer here wakes at the earliest
    /// such expiry so a crashed consumer's replacement is not blocked for
    /// `max.poll.interval.ms`.
    async fn watch_rebalance(&self, group_id: &str, rebalance_id: u64, deadline: Instant) {
        let group = {
            let groups = self.groups.lock().expect("group map lock");
            groups.get(group_id).cloned()
        };
        let Some(group) = group else {
            return;
        };
        loop {
            let (completion, next_wake) = {
                let mut state = group.lock().await;
                if state.rebalance.as_ref().map(|r| r.id) != Some(rebalance_id) {
                    return;
                }
                let now = Instant::now();
                state.expire_members(now);
                let Some(rebalance) = state.rebalance.as_ref() else {
                    return;
                };
                if rebalance.joined == rebalance.expected {
                    (Some(complete_rebalance(&mut state, false)), None)
                } else if now >= deadline {
                    (Some(complete_rebalance(&mut state, true)), None)
                } else {
                    let next_expiry = rebalance
                        .expected
                        .difference(&rebalance.joined)
                        .filter_map(|id| state.members.get(id))
                        .map(|member| member.last_heartbeat + member.session_timeout)
                        .min()
                        .unwrap_or(deadline);
                    (None, Some(next_expiry.min(deadline)))
                }
            };
            if let Some(completion) = completion {
                send_join_completions(Some(completion));
                return;
            }
            if let Some(wake) = next_wake {
                tokio::time::sleep_until(tokio::time::Instant::from_std(wake)).await;
            }
        }
    }

    pub async fn sync(&self, input: SyncInput) -> SyncOutcome {
        let group = match self.local_group(&input.group_id, false).await {
            Ok(group) => group,
            Err(code) => return SyncOutcome::error(code),
        };
        let (receiver, immediate, timeout) = {
            let mut state = group.lock().await;
            state.expire_members(Instant::now());
            let Some(member) = state.members.get(&input.member_id) else {
                return SyncOutcome::error(GroupError::UnknownMember);
            };
            if member.instance_id != input.group_instance_id {
                return SyncOutcome::error(GroupError::FencedInstance);
            }
            if state.generation != input.generation_id {
                return SyncOutcome::error(GroupError::IllegalGeneration);
            }
            if state.phase == GroupPhase::PreparingRebalance {
                return SyncOutcome::error(GroupError::RebalanceInProgress);
            }
            if state.phase == GroupPhase::Stable {
                return SyncOutcome {
                    error: None,
                    protocol_type: Some(state.protocol_type.clone()),
                    protocol_name: Some(state.protocol_name.clone()),
                    assignment: member.assignment.clone(),
                };
            }

            // The leader's sync distributes assignments even when the list
            // is empty. Parking the leader would stall the whole group.
            if input.member_id == state.leader {
                let assignments: BTreeMap<String, Bytes> = input.assignments.into_iter().collect();
                if assignments.keys().any(|id| !state.members.contains_key(id)) {
                    return SyncOutcome::error(GroupError::UnknownMember);
                }
                for (id, member) in &mut state.members {
                    member.assignment = assignments.get(id).cloned().unwrap_or_default();
                    member.last_heartbeat = Instant::now();
                }
                state.phase = GroupPhase::Stable;
                let protocol_type = Some(state.protocol_type.clone());
                let protocol_name = Some(state.protocol_name.clone());
                let own_assignment = state
                    .members
                    .get(&input.member_id)
                    .map(|member| member.assignment.clone())
                    .unwrap_or_default();
                let waiters = std::mem::take(&mut state.sync_waiters);
                for (id, sender) in waiters {
                    let assignment = state
                        .members
                        .get(&id)
                        .map(|member| member.assignment.clone())
                        .unwrap_or_default();
                    let _ = sender.send(SyncOutcome {
                        error: None,
                        protocol_type: protocol_type.clone(),
                        protocol_name: protocol_name.clone(),
                        assignment,
                    });
                }
                (
                    None,
                    Some(SyncOutcome {
                        error: None,
                        protocol_type,
                        protocol_name,
                        assignment: own_assignment,
                    }),
                    std::time::Duration::ZERO,
                )
            } else {
                let timeout = member.rebalance_timeout;
                let (sender, receiver) = oneshot::channel();
                state.sync_waiters.insert(input.member_id, sender);
                (Some(receiver), None, timeout)
            }
        };
        if let Some(outcome) = immediate {
            return outcome;
        }
        match tokio::time::timeout(timeout, receiver.expect("sync receiver")).await {
            Ok(Ok(outcome)) => outcome,
            _ => SyncOutcome::error(GroupError::RebalanceInProgress),
        }
    }

    pub async fn heartbeat(
        &self,
        group_id: &str,
        generation_id: i32,
        member_id: &str,
        instance_id: Option<&str>,
    ) -> Result<(), GroupError> {
        let group = match self.local_group(group_id, false).await {
            Ok(group) => group,
            Err(error) => return Err(error),
        };
        let mut state = group.lock().await;
        state.expire_members(Instant::now());
        let Some(member) = state.members.get(member_id) else {
            return Err(GroupError::UnknownMember);
        };
        if member.instance_id.as_deref() != instance_id {
            return Err(GroupError::FencedInstance);
        }
        if state.generation != generation_id {
            return Err(GroupError::IllegalGeneration);
        }
        if let Some(member) = state.members.get_mut(member_id) {
            member.last_heartbeat = Instant::now();
        }
        if state.phase != GroupPhase::Stable {
            return Err(GroupError::RebalanceInProgress);
        }
        Ok(())
    }

    pub async fn leave(
        &self,
        group_id: &str,
        members: &[(String, Option<String>)],
    ) -> Vec<Result<(), GroupError>> {
        let group = match self.local_group(group_id, false).await {
            Ok(group) => group,
            Err(error) => return vec![Err(error); members.len()],
        };
        let mut state = group.lock().await;
        state.expire_members(Instant::now());
        let mut results = Vec::with_capacity(members.len());
        let mut removed_any = false;
        for (member_id, instance_id) in members {
            let result = match state.members.get(member_id) {
                None => Err(GroupError::UnknownMember),
                Some(member) if member.instance_id != *instance_id => {
                    Err(GroupError::FencedInstance)
                }
                Some(_) => {
                    remove_member(&mut state, member_id);
                    removed_any = true;
                    Ok(())
                }
            };
            results.push(result);
        }
        if removed_any {
            if let Some(rebalance) = state.rebalance.take() {
                for (_, sender) in rebalance.waiters {
                    let _ = sender.send(JoinOutcome::error(
                        GroupError::RebalanceInProgress,
                        String::new(),
                    ));
                }
            }
            if state.members.is_empty() {
                state.phase = GroupPhase::Empty;
                state.leader.clear();
                state.protocol_name.clear();
            } else {
                state.phase = GroupPhase::PreparingRebalance;
            }
        }
        results
    }

    pub async fn commit_offsets(
        &self,
        group_id: &str,
        generation_id: i32,
        member_id: &str,
        instance_id: Option<&str>,
        commits: &[OffsetCommit],
    ) -> Result<(), GroupError> {
        let group = match self.local_group(group_id, true).await {
            Ok(group) => group,
            Err(error) => return Err(error),
        };
        let stream = group_stream_name(group_id);
        let mut state = group.lock().await;
        state.expire_members(Instant::now());
        if generation_id >= 0 {
            if state.generation != generation_id {
                return Err(GroupError::IllegalGeneration);
            }
            let Some(member) = state.members.get(member_id) else {
                return Err(GroupError::UnknownMember);
            };
            if member.instance_id.as_deref() != instance_id {
                return Err(GroupError::FencedInstance);
            }
            if state.phase != GroupPhase::Stable {
                return Err(GroupError::RebalanceInProgress);
            }
        }
        if commits.is_empty() {
            return Ok(());
        }
        let new_keys = commits
            .iter()
            .filter(|commit| {
                !state
                    .offsets
                    .contains_key(&(commit.topic.clone(), commit.partition))
            })
            .count();
        if state.offsets.len() + new_keys > offsets::MAX_OFFSETS_PER_GROUP {
            return Err(GroupError::CapacityExceeded);
        }
        if let Err(error) = self
            .service
            .append(AppendCommand {
                name: stream.clone(),
                records: vec![LogRecord::value(encode_commits(commits))],
                content_type: Some(GROUP_CONTENT_TYPE.to_owned()),
                ..Default::default()
            })
            .await
        {
            return Err(match error.kind {
                ErrorKind::NotFound | ErrorKind::Fenced => GroupError::NotCoordinator,
                ErrorKind::Durability => GroupError::Storage,
                _ => GroupError::InvalidRequest,
            });
        }
        for commit in commits {
            state.offsets.insert(
                (commit.topic.clone(), commit.partition),
                commit.value.clone(),
            );
        }
        state.appends_since_snapshot += 1;
        if state.appends_since_snapshot >= OFFSET_SNAPSHOT_INTERVAL {
            self.snapshot_and_trim(&stream, &mut state).await;
        }
        Ok(())
    }

    async fn snapshot_and_trim(&self, stream: &str, state: &mut Group) {
        let Ok(appended) = self
            .service
            .append(AppendCommand {
                name: stream.to_owned(),
                records: vec![LogRecord::value(encode_snapshot(&state.offsets))],
                content_type: Some(GROUP_CONTENT_TYPE.to_owned()),
                ..Default::default()
            })
            .await
        else {
            // Best effort: the delta already committed and the next commit
            // retries the snapshot.
            return;
        };
        let newest = appended.next_offset.record_offset().saturating_sub(1);
        if self.service.trim(stream, newest).await.is_ok() {
            state.appends_since_snapshot = 0;
        }
    }

    pub async fn fetch_offsets(
        &self,
        group_id: &str,
        requested: Option<&[(String, Vec<i32>)]>,
    ) -> Result<BTreeMap<String, Vec<(i32, CommittedOffset)>>, GroupError> {
        let group = match self.local_group(group_id, false).await {
            Ok(group) => group,
            Err(GroupError::GroupNotFound) => {
                return Ok(empty_offset_fetch(requested));
            }
            Err(error) => return Err(error),
        };
        let state = group.lock().await;
        let mut result: BTreeMap<String, Vec<(i32, CommittedOffset)>> = BTreeMap::new();
        match requested {
            Some(topics) => {
                for (topic, partitions) in topics {
                    let values = partitions
                        .iter()
                        .map(|partition| {
                            let value = state
                                .offsets
                                .get(&(topic.clone(), *partition))
                                .cloned()
                                .unwrap_or_else(CommittedOffset::none);
                            (*partition, value)
                        })
                        .collect();
                    result.insert(topic.clone(), values);
                }
            }
            None => {
                for ((topic, partition), value) in &state.offsets {
                    result
                        .entry(topic.clone())
                        .or_default()
                        .push((*partition, value.clone()));
                }
            }
        }
        Ok(result)
    }

    pub async fn describe(&self, group_id: &str) -> GroupDescription {
        let group = match self.local_group(group_id, false).await {
            Ok(group) => group,
            Err(GroupError::GroupNotFound) => {
                return GroupDescription {
                    error: None,
                    group_id: group_id.to_owned(),
                    state: "Dead".to_owned(),
                    protocol_type: String::new(),
                    protocol_name: String::new(),
                    members: Vec::new(),
                };
            }
            Err(error) => {
                return GroupDescription {
                    error: Some(error),
                    group_id: group_id.to_owned(),
                    state: String::new(),
                    protocol_type: String::new(),
                    protocol_name: String::new(),
                    members: Vec::new(),
                };
            }
        };
        let mut state = group.lock().await;
        state.expire_members(Instant::now());
        let protocol_name = state.protocol_name.clone();
        let members = state
            .members
            .iter()
            .map(|(id, member)| MemberDescription {
                member_id: id.clone(),
                group_instance_id: member.instance_id.clone(),
                client_id: member.client_id.clone(),
                metadata: member
                    .protocols
                    .iter()
                    .find(|protocol| protocol.name == protocol_name)
                    .map(|protocol| protocol.metadata.clone())
                    .unwrap_or_default(),
                assignment: member.assignment.clone(),
            })
            .collect();
        GroupDescription {
            error: None,
            group_id: group_id.to_owned(),
            state: state.phase.as_str().to_owned(),
            protocol_type: state.protocol_type.clone(),
            protocol_name,
            members,
        }
    }

    pub async fn list(&self) -> Vec<ListedGroup> {
        let groups: Vec<(String, Arc<Mutex<Group>>)> = {
            self.groups
                .lock()
                .expect("group map lock")
                .iter()
                .map(|(id, group)| (id.clone(), Arc::clone(group)))
                .collect()
        };
        let mut listed = Vec::new();
        for (group_id, group) in groups {
            let mut state = group.lock().await;
            state.expire_members(Instant::now());
            if !state.members.is_empty() {
                listed.push(ListedGroup {
                    group_id,
                    protocol_type: state.protocol_type.clone(),
                    state: state.phase.as_str().to_owned(),
                });
            }
        }
        listed
    }

    async fn local_group(
        &self,
        group_id: &str,
        create: bool,
    ) -> Result<Arc<Mutex<Group>>, GroupError> {
        validate_group_id(group_id)?;
        let stream = group_stream_name(group_id);
        if create {
            self.ensure_stream(&stream).await?;
        } else if self
            .service
            .lookup_stream_id(&stream)
            .await
            .map_err(|_| GroupError::NotCoordinator)?
            .is_none()
        {
            return Err(GroupError::GroupNotFound);
        }
        let owner = self
            .ownership
            .owner_of(&stream)
            .await
            .map_err(|_| GroupError::NotCoordinator)?;
        if !owner.local {
            return Err(GroupError::NotCoordinator);
        }
        let stream_id = self
            .service
            .lookup_stream_id(&stream)
            .await
            .map_err(|_| GroupError::NotCoordinator)?
            .ok_or(GroupError::NotCoordinator)?;
        let epoch = self
            .views
            .load()
            .state
            .streams
            .get(&stream_id)
            .map(|row| row.epoch)
            .unwrap_or(-1);

        let group = {
            let mut groups = self.groups.lock().expect("group map lock");
            if let Some(group) = groups.get(group_id) {
                Arc::clone(group)
            } else {
                prune_empty_groups(&mut groups);
                if groups.len() >= MAX_GROUPS {
                    return Err(GroupError::CapacityExceeded);
                }
                let group = Arc::new(Mutex::new(Group::loaded(i64::MIN, OffsetTable::new(), 0)));
                groups.insert(group_id.to_owned(), Arc::clone(&group));
                group
            }
        };
        let mut state = group.lock().await;
        if state.loaded_epoch != epoch {
            let (offsets, replayed) = self.replay_offsets(&stream).await?;
            *state = Group::loaded(epoch, offsets, replayed);
        }
        drop(state);
        Ok(group)
    }

    async fn ensure_stream(&self, stream: &str) -> Result<(), GroupError> {
        self.service
            .create(CreateCommand {
                internal: true,
                ..CreateCommand::new(stream, GROUP_CONTENT_TYPE)
            })
            .await
            .map(|_| ())
            .map_err(|_| GroupError::CoordinatorNotAvailable)
    }

    async fn replay_offsets(&self, stream: &str) -> Result<(OffsetTable, u64), GroupError> {
        let watermarks = self
            .service
            .watermarks(stream)
            .await
            .map_err(|_| GroupError::NotCoordinator)?;
        let mut cursor = watermarks.log_start_offset;
        let mut offsets = OffsetTable::new();
        let mut replayed = 0u64;
        while cursor < watermarks.high_watermark {
            let read = self
                .service
                .read(
                    stream,
                    OffsetToken::of_record_offset(cursor),
                    8 * 1024 * 1024,
                    1024,
                )
                .await
                .map_err(|_| GroupError::NotCoordinator)?;
            if read.records.is_empty() {
                break;
            }
            for record in read.records {
                decode_into(&record.record.value, &mut offsets)
                    .map_err(|_| GroupError::NotCoordinator)?;
                replayed += 1;
            }
            cursor = read.next_offset.record_offset();
        }
        Ok((offsets, replayed))
    }
}

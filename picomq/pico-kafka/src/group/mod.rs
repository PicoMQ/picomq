//! Classic consumer-group coordination backed by one internal stream per
//! group. Only committed offsets are durable, membership is ephemeral and
//! rebuilt from client rejoins after a coordinator move.

mod offsets;
mod state;

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Instant;

use bytes::Bytes;
use pico_server::{
    AppendCommand, CreateCommand, ErrorKind, MetadataOwnershipService, OffsetToken,
    OwnershipService, S3StreamService,
};
use tokio::sync::{oneshot, Mutex};

use crate::handlers::common::{
    COORDINATOR_NOT_AVAILABLE, FENCED_INSTANCE_ID, GROUP_ID_NOT_FOUND, GROUP_MAX_SIZE_REACHED,
    ILLEGAL_GENERATION, INCONSISTENT_GROUP_PROTOCOL, INVALID_REQUEST, KAFKA_STORAGE_ERROR,
    MEMBER_ID_REQUIRED, NOT_COORDINATOR, REBALANCE_IN_PROGRESS, UNKNOWN_MEMBER_ID,
};

pub use offsets::{CommittedOffset, OffsetCommit};

use offsets::{decode_into, empty_offset_fetch, encode_commits, encode_snapshot, OffsetTable};
use state::{
    complete_rebalance, group_stream_name, member_from_input, new_member_id, prune_empty_groups,
    remove_member, send_join_completions, validate_group_id, validate_join, Group, GroupPhase,
    Rebalance, MAX_GROUPS, MAX_MEMBERS_PER_GROUP,
};

const GROUP_CONTENT_TYPE: &str = "application/vnd.picomq.kafka-group-state";
/// Delta appends between full-snapshot-and-trim cycles. Bounds both replay
/// length on takeover and metadata-plane trim traffic on the commit path.
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
    pub error_code: i16,
    pub generation_id: i32,
    pub protocol_type: Option<String>,
    pub protocol_name: Option<String>,
    pub leader: String,
    pub member_id: String,
    pub members: Vec<JoinMember>,
}

impl JoinOutcome {
    pub(super) fn error(error_code: i16, member_id: String) -> Self {
        Self {
            error_code,
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
    pub error_code: i16,
    pub protocol_type: Option<String>,
    pub protocol_name: Option<String>,
    pub assignment: Bytes,
}

impl SyncOutcome {
    fn error(error_code: i16) -> Self {
        Self {
            error_code,
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
    pub error_code: i16,
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
    service: Arc<S3StreamService>,
    ownership: Arc<MetadataOwnershipService>,
    views: Arc<pico_metadata::ViewPublisher>,
    groups: StdMutex<HashMap<String, Arc<Mutex<Group>>>>,
}

impl GroupCoordinator {
    pub fn new(
        node_id: i32,
        service: Arc<S3StreamService>,
        ownership: Arc<MetadataOwnershipService>,
        views: Arc<pico_metadata::ViewPublisher>,
    ) -> Arc<Self> {
        Arc::new(Self {
            node_id,
            service,
            ownership,
            views,
            groups: StdMutex::new(HashMap::new()),
        })
    }

    /// Route the group to its coordinator without creating anything durable:
    /// FindCoordinator is a read that clients issue for arbitrary group ids,
    /// so it must not mint streams. The stream is created on the first
    /// JoinGroup or OffsetCommit, which land on the node answered here.
    pub async fn find_coordinator(&self, group_id: &str) -> Result<CoordinatorEndpoint, i16> {
        validate_group_id(group_id)?;
        let stream = group_stream_name(group_id);
        let owner = self
            .ownership
            .owner_of(&stream)
            .await
            .map_err(|_| COORDINATOR_NOT_AVAILABLE)?;
        let node_id = if owner.local {
            self.node_id
        } else {
            owner.owner_node_id.ok_or(COORDINATOR_NOT_AVAILABLE)?
        };
        let view = self.views.load();
        let address = view
            .state
            .get_node_protocol_address(node_id, crate::PROTOCOL_NAME)
            .filter(|address| !address.is_empty())
            .ok_or(COORDINATOR_NOT_AVAILABLE)?
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
                if let Some(instance_id) = input.group_instance_id.as_deref() {
                    if let Some((existing, _)) = state
                        .members
                        .iter()
                        .find(|(_, member)| member.instance_id.as_deref() == Some(instance_id))
                    {
                        member_id = existing.clone();
                    }
                }
                if member_id.is_empty() {
                    if state.members.len() >= MAX_MEMBERS_PER_GROUP {
                        return JoinOutcome::error(GROUP_MAX_SIZE_REACHED, String::new());
                    }
                    member_id = new_member_id(&input.client_id);
                    state
                        .members
                        .insert(member_id.clone(), member_from_input(&input, Instant::now()));
                    if input.require_known_member_id {
                        return JoinOutcome::error(MEMBER_ID_REQUIRED, member_id);
                    }
                }
            } else if !state.members.contains_key(&member_id) {
                return JoinOutcome::error(UNKNOWN_MEMBER_ID, member_id);
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
                return JoinOutcome::error(UNKNOWN_MEMBER_ID, member_id);
            };
            if member.instance_id != input.group_instance_id {
                return JoinOutcome::error(FENCED_INSTANCE_ID, member_id);
            }
            *member = member_from_input(&input, Instant::now());

            if !state.protocol_type.is_empty() && state.protocol_type != input.protocol_type {
                return JoinOutcome::error(INCONSISTENT_GROUP_PROTOCOL, member_id);
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
                tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)).await;
                coordinator.finish_rebalance(&group_id, id).await;
            });
        }
        receiver
            .await
            .unwrap_or_else(|_| JoinOutcome::error(REBALANCE_IN_PROGRESS, input.member_id))
    }

    async fn finish_rebalance(&self, group_id: &str, rebalance_id: u64) {
        let group = {
            let groups = self.groups.lock().expect("group map lock");
            groups.get(group_id).cloned()
        };
        let Some(group) = group else {
            return;
        };
        let completion = {
            let mut state = group.lock().await;
            if state.rebalance.as_ref().map(|r| r.id) != Some(rebalance_id) {
                return;
            }
            complete_rebalance(&mut state, true)
        };
        send_join_completions(Some(completion));
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
                return SyncOutcome::error(UNKNOWN_MEMBER_ID);
            };
            if member.instance_id != input.group_instance_id {
                return SyncOutcome::error(FENCED_INSTANCE_ID);
            }
            if state.generation != input.generation_id {
                return SyncOutcome::error(ILLEGAL_GENERATION);
            }
            if state.phase == GroupPhase::PreparingRebalance {
                return SyncOutcome::error(REBALANCE_IN_PROGRESS);
            }
            if state.phase == GroupPhase::Stable {
                return SyncOutcome {
                    error_code: 0,
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
                    return SyncOutcome::error(UNKNOWN_MEMBER_ID);
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
                        error_code: 0,
                        protocol_type: protocol_type.clone(),
                        protocol_name: protocol_name.clone(),
                        assignment,
                    });
                }
                (
                    None,
                    Some(SyncOutcome {
                        error_code: 0,
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
            _ => SyncOutcome::error(REBALANCE_IN_PROGRESS),
        }
    }

    pub async fn heartbeat(
        &self,
        group_id: &str,
        generation_id: i32,
        member_id: &str,
        instance_id: Option<&str>,
    ) -> i16 {
        let group = match self.local_group(group_id, false).await {
            Ok(group) => group,
            Err(code) => return code,
        };
        let mut state = group.lock().await;
        state.expire_members(Instant::now());
        let Some(member) = state.members.get(member_id) else {
            return UNKNOWN_MEMBER_ID;
        };
        if member.instance_id.as_deref() != instance_id {
            return FENCED_INSTANCE_ID;
        }
        if state.generation != generation_id {
            return ILLEGAL_GENERATION;
        }
        if let Some(member) = state.members.get_mut(member_id) {
            member.last_heartbeat = Instant::now();
        }
        if state.phase != GroupPhase::Stable {
            return REBALANCE_IN_PROGRESS;
        }
        0
    }

    pub async fn leave(&self, group_id: &str, members: &[(String, Option<String>)]) -> Vec<i16> {
        let group = match self.local_group(group_id, false).await {
            Ok(group) => group,
            Err(code) => return vec![code; members.len()],
        };
        let mut state = group.lock().await;
        state.expire_members(Instant::now());
        let mut results = Vec::with_capacity(members.len());
        let mut removed_any = false;
        for (member_id, instance_id) in members {
            let code = match state.members.get(member_id) {
                None => UNKNOWN_MEMBER_ID,
                Some(member) if member.instance_id != *instance_id => FENCED_INSTANCE_ID,
                Some(_) => {
                    remove_member(&mut state, member_id);
                    removed_any = true;
                    0
                }
            };
            results.push(code);
        }
        if removed_any {
            if let Some(rebalance) = state.rebalance.take() {
                for (_, sender) in rebalance.waiters {
                    let _ = sender.send(JoinOutcome::error(REBALANCE_IN_PROGRESS, String::new()));
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

    /// Append only the changed offsets as one delta record. Every
    /// [`OFFSET_SNAPSHOT_INTERVAL`] appends, snapshot and trim so takeover
    /// replay stays bounded.
    pub async fn commit_offsets(
        &self,
        group_id: &str,
        generation_id: i32,
        member_id: &str,
        instance_id: Option<&str>,
        commits: &[OffsetCommit],
    ) -> i16 {
        let group = match self.local_group(group_id, true).await {
            Ok(group) => group,
            Err(code) => return code,
        };
        let stream = group_stream_name(group_id);
        let mut state = group.lock().await;
        state.expire_members(Instant::now());
        if generation_id >= 0 {
            if state.generation != generation_id {
                return ILLEGAL_GENERATION;
            }
            let Some(member) = state.members.get(member_id) else {
                return UNKNOWN_MEMBER_ID;
            };
            if member.instance_id.as_deref() != instance_id {
                return FENCED_INSTANCE_ID;
            }
            if state.phase != GroupPhase::Stable {
                return REBALANCE_IN_PROGRESS;
            }
        }
        if commits.is_empty() {
            return 0;
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
            return GROUP_MAX_SIZE_REACHED;
        }
        if let Err(error) = self
            .service
            .append(AppendCommand {
                name: stream.clone(),
                payloads: vec![encode_commits(commits)],
                content_type: Some(GROUP_CONTENT_TYPE.to_owned()),
                atomic: true,
                ..Default::default()
            })
            .await
        {
            return match error.kind {
                ErrorKind::NotFound | ErrorKind::Fenced => NOT_COORDINATOR,
                ErrorKind::Durability => KAFKA_STORAGE_ERROR,
                _ => INVALID_REQUEST,
            };
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
        0
    }

    async fn snapshot_and_trim(&self, stream: &str, state: &mut Group) {
        let Ok(appended) = self
            .service
            .append(AppendCommand {
                name: stream.to_owned(),
                payloads: vec![encode_snapshot(&state.offsets)],
                content_type: Some(GROUP_CONTENT_TYPE.to_owned()),
                atomic: true,
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
    ) -> Result<BTreeMap<String, Vec<(i32, CommittedOffset)>>, i16> {
        let group = match self.local_group(group_id, false).await {
            Ok(group) => group,
            Err(GROUP_ID_NOT_FOUND) => {
                return Ok(empty_offset_fetch(requested));
            }
            Err(code) => return Err(code),
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
            Err(GROUP_ID_NOT_FOUND) => {
                return GroupDescription {
                    error_code: 0,
                    group_id: group_id.to_owned(),
                    state: "Dead".to_owned(),
                    protocol_type: String::new(),
                    protocol_name: String::new(),
                    members: Vec::new(),
                };
            }
            Err(code) => {
                return GroupDescription {
                    error_code: code,
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
            error_code: 0,
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

    /// Resolve the local in-memory group, verifying this node owns the group
    /// stream and (re)playing durable offsets when the stream epoch changed.
    async fn local_group(&self, group_id: &str, create: bool) -> Result<Arc<Mutex<Group>>, i16> {
        validate_group_id(group_id)?;
        let stream = group_stream_name(group_id);
        if create {
            self.ensure_stream(&stream).await?;
        } else if self
            .service
            .lookup_stream_id(&stream)
            .await
            .map_err(|_| NOT_COORDINATOR)?
            .is_none()
        {
            return Err(GROUP_ID_NOT_FOUND);
        }
        let owner = self
            .ownership
            .owner_of(&stream)
            .await
            .map_err(|_| NOT_COORDINATOR)?;
        if !owner.local {
            return Err(NOT_COORDINATOR);
        }
        let stream_id = self
            .service
            .lookup_stream_id(&stream)
            .await
            .map_err(|_| NOT_COORDINATOR)?
            .ok_or(NOT_COORDINATOR)?;
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
                    return Err(GROUP_MAX_SIZE_REACHED);
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

    async fn ensure_stream(&self, stream: &str) -> Result<(), i16> {
        self.service
            .create(CreateCommand {
                name: stream.to_owned(),
                content_type: GROUP_CONTENT_TYPE.to_owned(),
                ttl_seconds: None,
                expires_at_ms: None,
                closed: false,
                initial_payload: Bytes::new(),
                external_id: None,
                internal: true,
                schema_name: None,
                schema_validate: false,
            })
            .await
            .map(|_| ())
            .map_err(|_| COORDINATOR_NOT_AVAILABLE)
    }

    /// Fold every record from log start to the high watermark: snapshots and
    /// deltas share one layout, so later entries simply overwrite earlier
    /// ones. The log is at most one snapshot plus [`OFFSET_SNAPSHOT_INTERVAL`]
    /// deltas long. Returns the table and the record count so the caller can
    /// resume the snapshot cadence.
    async fn replay_offsets(&self, stream: &str) -> Result<(OffsetTable, u64), i16> {
        let watermarks = self
            .service
            .watermarks(stream)
            .await
            .map_err(|_| NOT_COORDINATOR)?;
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
                .map_err(|_| NOT_COORDINATOR)?;
            if read.records.is_empty() {
                break;
            }
            for record in read.records {
                decode_into(&record.payload, &mut offsets).map_err(|_| NOT_COORDINATOR)?;
                replayed += 1;
            }
            cursor = read.next_offset.record_offset();
        }
        Ok((offsets, replayed))
    }
}

mod assign;
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

pub type StreamName = Arc<str>;

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
pub use state::GroupState;

use offsets::{OffsetTable, decode_into, encode_commits, encode_snapshot};
use state::{
    Group, MAX_MEMBERS_PER_GROUP, MAX_STREAMS_PER_GROUP, MAX_SUBSCRIPTION_ENTRIES_PER_GROUP, Mode,
    Names, Rebalance, Role, compatible, complete_rebalance, evict_idle_groups, group_stream_name,
    member_from_input, mode_of, new_member_id, remove_member, send_join_completions,
    validate_group_id, validate_join, validate_stream_name,
};

const GROUP_CONTENT_TYPE: &str = "application/vnd.picomq.group-state";
const OFFSET_SNAPSHOT_INTERVAL: u64 = 64;

#[derive(Debug, Clone)]
pub struct JoinProtocol {
    pub name: String,
    pub metadata: Bytes,
}

#[derive(Debug, Clone)]
pub enum Membership {
    Subscribed(Vec<String>),
    Client {
        protocol_type: String,
        protocols: Vec<JoinProtocol>,
    },
}

#[derive(Debug, Clone)]
pub struct JoinInput {
    pub group_id: String,
    pub member_id: String,
    pub instance_id: Option<String>,
    pub client_id: String,
    pub membership: Membership,
    pub session_timeout_ms: i32,
    pub rebalance_timeout_ms: i32,
    pub require_known_member_id: bool,
}

#[derive(Debug, Clone)]
pub struct JoinMember {
    pub member_id: String,
    pub instance_id: Option<String>,
    pub metadata: Bytes,
}

#[derive(Debug, Clone)]
pub enum Joined {
    Subscribed {
        assignment: Vec<String>,
        members: Vec<String>,
    },
    Client {
        protocol_type: String,
        protocol_name: String,
        leader: String,
        members: Vec<JoinMember>,
    },
}

#[derive(Debug, Clone)]
pub struct JoinOutcome {
    pub generation: i32,
    pub member_id: String,
    pub result: Result<Joined, GroupError>,
}

impl JoinOutcome {
    pub(crate) fn error(error: GroupError, member_id: String) -> Self {
        Self {
            generation: -1,
            member_id,
            result: Err(error),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct MemberFence<'a> {
    pub generation: i32,
    pub member_id: &'a str,
    pub instance_id: Option<&'a str>,
}

#[derive(Debug, Clone)]
pub struct SyncInput {
    pub group_id: String,
    pub generation: i32,
    pub member_id: String,
    pub instance_id: Option<String>,
    pub assignments: Vec<(String, Bytes)>,
}

#[derive(Debug, Clone)]
pub struct SyncOutcome {
    pub protocol_type: String,
    pub protocol_name: String,
    pub assignment: Bytes,
}

#[derive(Debug, Clone)]
pub enum MemberRole {
    Subscribed {
        subscription: Vec<String>,
        assignment: Vec<String>,
    },
    Client {
        metadata: Bytes,
        assignment: Bytes,
    },
}

#[derive(Debug, Clone)]
pub struct MemberDescription {
    pub member_id: String,
    pub instance_id: Option<String>,
    pub client_id: String,
    pub role: MemberRole,
}

#[derive(Debug, Clone)]
pub struct GroupDescription {
    pub group_id: String,
    pub state: GroupState,
    pub generation: i32,
    pub protocol_type: String,
    pub protocol_name: String,
    pub members: Vec<MemberDescription>,
}

#[derive(Debug, Clone)]
pub struct ListedGroup {
    pub group_id: String,
    pub state: GroupState,
    pub protocol_type: String,
}

pub struct GroupCoordinator {
    node_id: i32,
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
    ) -> Arc<Self> {
        Arc::new(Self {
            node_id,
            service,
            ownership,
            views,
            groups: StdMutex::new(HashMap::new()),
        })
    }

    pub fn stream_name(group_id: &str) -> String {
        group_stream_name(group_id)
    }

    pub async fn find_coordinator(&self, group_id: &str) -> Result<i32, GroupError> {
        validate_group_id(group_id)?;
        let stream = group_stream_name(group_id);
        let owner = self
            .ownership
            .owner_of(&stream)
            .await
            .map_err(|_| GroupError::CoordinatorNotAvailable)?;
        if owner.local {
            return Ok(self.node_id);
        }
        owner
            .owner_node_id
            .ok_or(GroupError::CoordinatorNotAvailable)
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
            let now = Instant::now();
            state.expire_members(now);
            state.last_activity = now;

            let mut member_id = input.member_id.clone();
            if member_id.is_empty()
                && let Some(instance_id) = input.instance_id.as_deref()
                && let Some((existing, _)) = state
                    .members
                    .iter()
                    .find(|(_, member)| member.instance_id.as_deref() == Some(instance_id))
            {
                member_id = existing.clone();
            }
            let is_new = member_id.is_empty();
            if is_new {
                if state.members.len() >= MAX_MEMBERS_PER_GROUP {
                    return JoinOutcome::error(GroupError::CapacityExceeded, String::new());
                }
                member_id = new_member_id(&input.client_id);
            } else if !state.members.contains_key(&member_id) {
                return JoinOutcome::error(GroupError::UnknownMember, member_id);
            }

            if let Some(mode) = &state.mode
                && !compatible(mode, &input.membership)
            {
                return JoinOutcome::error(GroupError::InconsistentProtocol, member_id);
            }
            if let Membership::Subscribed(subscription) = &input.membership {
                let previous = state
                    .members
                    .get(&member_id)
                    .and_then(|member| member.subscription())
                    .map_or(0, BTreeSet::len);
                let new_names = subscription
                    .iter()
                    .filter(|name| !state.names.contains(name))
                    .collect::<BTreeSet<_>>()
                    .len();
                if state.names.len() + new_names > MAX_STREAMS_PER_GROUP
                    || state.subscription_entries() - previous + subscription.len()
                        > MAX_SUBSCRIPTION_ENTRIES_PER_GROUP
                {
                    return JoinOutcome::error(GroupError::CapacityExceeded, member_id);
                }
            }

            if is_new {
                let member = member_from_input(&input, &mut state.names, now);
                state.members.insert(member_id.clone(), member);
                if input.require_known_member_id {
                    return JoinOutcome::error(GroupError::MemberIdRequired, member_id);
                }
            }

            if let Some(instance_id) = input.instance_id.as_deref() {
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

            if state.members[&member_id].instance_id != input.instance_id {
                return JoinOutcome::error(GroupError::FencedInstance, member_id);
            }
            if !is_new {
                let member = member_from_input(&input, &mut state.names, now);
                state.members.insert(member_id.clone(), member);
            }
            if state.mode.is_none() {
                state.mode = Some(mode_of(&input.membership));
            }

            let mut schedule = None;
            if state.rebalance.is_none() {
                state.phase = GroupState::PreparingRebalance;
                let id = state.next_rebalance_id;
                state.next_rebalance_id = state.next_rebalance_id.wrapping_add(1).max(1);
                let timeout = state
                    .members
                    .values()
                    .map(|member| member.rebalance_timeout)
                    .max()
                    .unwrap_or(std::time::Duration::from_secs(1));
                state.rebalance = Some(Rebalance {
                    id,
                    expected: state.members.keys().cloned().collect(),
                    joined: BTreeSet::new(),
                    waiters: BTreeMap::new(),
                });
                schedule = Some((id, now + timeout));
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

    /// Completes a rebalance as soon as every surviving member has joined,
    /// waking at the earliest session expiry so a crashed consumer's
    /// replacement is not blocked for the full rebalance timeout.
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

    pub async fn sync(&self, input: SyncInput) -> Result<SyncOutcome, GroupError> {
        let group = self.local_group(&input.group_id, false).await?;
        let (receiver, timeout) = {
            let mut state = group.lock().await;
            state.expire_members(Instant::now());
            check_fence(
                &state,
                MemberFence {
                    generation: input.generation,
                    member_id: &input.member_id,
                    instance_id: input.instance_id.as_deref(),
                },
            )?;
            let Some(Mode::Client { leader, .. }) = &state.mode else {
                return Err(GroupError::InvalidRequest);
            };
            if state.phase == GroupState::PreparingRebalance {
                return Err(GroupError::RebalanceInProgress);
            }
            if state.phase == GroupState::Stable {
                return Ok(sync_outcome(&state, &input.member_id));
            }

            // The leader's sync distributes assignments even when the list
            // is empty. Parking the leader would stall the whole group.
            if input.member_id == *leader {
                let assignments: BTreeMap<String, Bytes> = input.assignments.into_iter().collect();
                if assignments.keys().any(|id| !state.members.contains_key(id)) {
                    return Err(GroupError::UnknownMember);
                }
                let now = Instant::now();
                for (id, member) in &mut state.members {
                    member.last_heartbeat = now;
                    if let Role::Client { assignment, .. } = &mut member.role {
                        *assignment = assignments.get(id).cloned().unwrap_or_default();
                    }
                }
                state.phase = GroupState::Stable;
                let waiters = std::mem::take(&mut state.sync_waiters);
                for (id, sender) in waiters {
                    let _ = sender.send(sync_outcome(&state, &id));
                }
                return Ok(sync_outcome(&state, &input.member_id));
            }
            let timeout = state.members[&input.member_id].rebalance_timeout;
            let (sender, receiver) = oneshot::channel();
            state.sync_waiters.insert(input.member_id, sender);
            (receiver, timeout)
        };
        match tokio::time::timeout(timeout, receiver).await {
            Ok(Ok(outcome)) => Ok(outcome),
            _ => Err(GroupError::RebalanceInProgress),
        }
    }

    pub async fn assignment(
        &self,
        group_id: &str,
        fence: MemberFence<'_>,
    ) -> Result<Vec<String>, GroupError> {
        let group = self.local_group(group_id, false).await?;
        let mut state = group.lock().await;
        state.expire_members(Instant::now());
        check_fence(&state, fence)?;
        if state.phase != GroupState::Stable {
            return Err(GroupError::RebalanceInProgress);
        }
        match &state.members[fence.member_id].role {
            Role::Subscribed { assignment, .. } => {
                Ok(assignment.iter().map(|s| s.to_string()).collect())
            }
            Role::Client { .. } => Err(GroupError::InvalidRequest),
        }
    }

    pub async fn subscription(
        &self,
        group_id: &str,
        member_id: &str,
    ) -> Result<Vec<String>, GroupError> {
        let group = self.local_group(group_id, false).await?;
        let mut state = group.lock().await;
        state.expire_members(Instant::now());
        let member = state
            .members
            .get(member_id)
            .ok_or(GroupError::UnknownMember)?;
        match &member.role {
            Role::Subscribed { subscription, .. } => {
                Ok(subscription.iter().map(|s| s.to_string()).collect())
            }
            Role::Client { .. } => Err(GroupError::InvalidRequest),
        }
    }

    pub async fn heartbeat(
        &self,
        group_id: &str,
        fence: MemberFence<'_>,
    ) -> Result<(), GroupError> {
        let group = self.local_group(group_id, false).await?;
        let mut state = group.lock().await;
        let now = Instant::now();
        state.expire_members(now);
        check_fence(&state, fence)?;
        state
            .members
            .get_mut(fence.member_id)
            .expect("fenced member exists")
            .last_heartbeat = now;
        if state.phase != GroupState::Stable {
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
        let now = Instant::now();
        state.expire_members(now);
        state.last_activity = now;
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
                state.reset_empty();
            } else {
                state.phase = GroupState::PreparingRebalance;
                state.names.sweep();
            }
        }
        results
    }

    pub async fn commit_offsets(
        &self,
        group_id: &str,
        fence: Option<MemberFence<'_>>,
        commits: &[OffsetCommit],
    ) -> Result<(), GroupError> {
        commits
            .iter()
            .try_for_each(|commit| validate_stream_name(&commit.stream))?;
        let group = self.local_group(group_id, true).await?;
        let stream = group_stream_name(group_id);
        let mut state = group.lock().await;
        let now = Instant::now();
        state.expire_members(now);
        state.last_activity = now;
        if let Some(fence) = fence {
            check_fence(&state, fence)?;
            if state.phase != GroupState::Stable {
                return Err(GroupError::RebalanceInProgress);
            }
        }
        if commits.is_empty() {
            return Ok(());
        }
        let new_names = commits
            .iter()
            .filter(|commit| !state.names.contains(&commit.stream))
            .map(|commit| commit.stream.as_str())
            .collect::<BTreeSet<_>>()
            .len();
        if state.names.len() + new_names > MAX_STREAMS_PER_GROUP {
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
            let name = state.names.intern(&commit.stream);
            state.offsets.insert(name, commit.value.clone());
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
        streams: Option<&[String]>,
    ) -> Result<BTreeMap<String, CommittedOffset>, GroupError> {
        let group = match self.local_group(group_id, false).await {
            Ok(group) => group,
            Err(GroupError::GroupNotFound) => return Ok(BTreeMap::new()),
            Err(error) => return Err(error),
        };
        let mut state = group.lock().await;
        state.last_activity = Instant::now();
        Ok(match streams {
            Some(streams) => streams
                .iter()
                .filter_map(|name| {
                    state
                        .offsets
                        .get(name.as_str())
                        .map(|value| (name.clone(), value.clone()))
                })
                .collect(),
            None => state
                .offsets
                .iter()
                .map(|(name, value)| (name.to_string(), value.clone()))
                .collect(),
        })
    }

    pub async fn describe(&self, group_id: &str) -> Result<GroupDescription, GroupError> {
        let group = self.local_group(group_id, false).await?;
        let mut state = group.lock().await;
        state.expire_members(Instant::now());
        let protocol_name = state.protocol_name().to_owned();
        let members = state
            .members
            .iter()
            .map(|(id, member)| MemberDescription {
                member_id: id.clone(),
                instance_id: member.instance_id.clone(),
                client_id: member.client_id.clone(),
                role: match &member.role {
                    Role::Subscribed {
                        subscription,
                        assignment,
                    } => MemberRole::Subscribed {
                        subscription: subscription.iter().map(|s| s.to_string()).collect(),
                        assignment: assignment.iter().map(|s| s.to_string()).collect(),
                    },
                    Role::Client {
                        protocols,
                        assignment,
                    } => MemberRole::Client {
                        metadata: protocols
                            .iter()
                            .find(|protocol| protocol.name == protocol_name)
                            .map(|protocol| protocol.metadata.clone())
                            .unwrap_or_default(),
                        assignment: assignment.clone(),
                    },
                },
            })
            .collect();
        Ok(GroupDescription {
            group_id: group_id.to_owned(),
            state: state.phase,
            generation: state.generation,
            protocol_type: state.protocol_type().to_owned(),
            protocol_name,
            members,
        })
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
                    state: state.phase,
                    protocol_type: state.protocol_type().to_owned(),
                });
            }
        }
        listed.sort_by(|a, b| a.group_id.cmp(&b.group_id));
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
                evict_idle_groups(&mut groups, Instant::now());
                if groups.len() >= state::MAX_GROUPS {
                    return Err(GroupError::CapacityExceeded);
                }
                let group = Arc::new(Mutex::new(Group::loaded(
                    i64::MIN,
                    Names::default(),
                    OffsetTable::new(),
                    0,
                )));
                groups.insert(group_id.to_owned(), Arc::clone(&group));
                group
            }
        };
        let mut state = group.lock().await;
        if state.loaded_epoch != epoch {
            let (names, offsets, replayed) = self.replay_offsets(&stream).await?;
            *state = Group::loaded(epoch, names, offsets, replayed);
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

    async fn replay_offsets(&self, stream: &str) -> Result<(Names, OffsetTable, u64), GroupError> {
        let watermarks = self
            .service
            .watermarks(stream)
            .await
            .map_err(|_| GroupError::NotCoordinator)?;
        let mut cursor = watermarks.log_start_offset;
        let mut names = Names::default();
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
                decode_into(&record.record.value, &mut offsets, &mut names)
                    .map_err(|_| GroupError::NotCoordinator)?;
                replayed += 1;
            }
            cursor = read.next_offset.record_offset();
        }
        names.sweep();
        Ok((names, offsets, replayed))
    }
}

fn check_fence(state: &Group, fence: MemberFence<'_>) -> Result<(), GroupError> {
    let Some(member) = state.members.get(fence.member_id) else {
        return Err(GroupError::UnknownMember);
    };
    if member.instance_id.as_deref() != fence.instance_id {
        return Err(GroupError::FencedInstance);
    }
    if state.generation != fence.generation {
        return Err(GroupError::IllegalGeneration);
    }
    Ok(())
}

fn sync_outcome(state: &Group, member_id: &str) -> SyncOutcome {
    SyncOutcome {
        protocol_type: state.protocol_type().to_owned(),
        protocol_name: state.protocol_name().to_owned(),
        assignment: match state.members.get(member_id).map(|m| &m.role) {
            Some(Role::Client { assignment, .. }) => assignment.clone(),
            _ => Bytes::new(),
        },
    }
}

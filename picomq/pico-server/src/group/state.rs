use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use tokio::sync::{Mutex, oneshot};

use super::assign::assign;
use super::offsets::OffsetTable;
use super::{
    GroupError, JoinInput, JoinMember, JoinOutcome, JoinProtocol, Joined, Membership, StreamName,
    SyncOutcome,
};

pub(super) const MAX_GROUPS: usize = 1_000;
pub(super) const MAX_MEMBERS_PER_GROUP: usize = 10_000;
pub(super) const MAX_STREAMS_PER_GROUP: usize = 10_000;
pub(super) const MAX_SUBSCRIPTION_ENTRIES_PER_GROUP: usize = 1_000_000;
pub(super) const MAX_GROUP_ID_BYTES: usize = 255;
pub(super) const MAX_MEMBER_ID_BYTES: usize = 512;
pub(super) const MAX_STREAM_NAME_BYTES: usize = 1024;
pub(super) const MAX_PROTOCOLS_PER_MEMBER: usize = 32;
pub(super) const MAX_METADATA_BYTES_PER_MEMBER: usize = 1024 * 1024;
pub(super) const MIN_SESSION_TIMEOUT_MS: i32 = 1_000;
pub(super) const MAX_SESSION_TIMEOUT_MS: i32 = 300_000;
pub(super) const IDLE_GROUP_TTL: Duration = Duration::from_secs(300);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GroupState {
    Empty,
    PreparingRebalance,
    CompletingRebalance,
    Stable,
}

impl GroupState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Empty => "Empty",
            Self::PreparingRebalance => "PreparingRebalance",
            Self::CompletingRebalance => "CompletingRebalance",
            Self::Stable => "Stable",
        }
    }
}

#[derive(Debug, Default)]
pub(super) struct Names(BTreeSet<StreamName>);

impl Names {
    pub(super) fn intern(&mut self, name: &str) -> StreamName {
        if let Some(existing) = self.0.get(name) {
            return Arc::clone(existing);
        }
        let interned: StreamName = Arc::from(name);
        self.0.insert(Arc::clone(&interned));
        interned
    }

    pub(super) fn contains(&self, name: &str) -> bool {
        self.0.contains(name)
    }

    pub(super) fn len(&self) -> usize {
        self.0.len()
    }

    pub(super) fn sweep(&mut self) {
        self.0.retain(|name| Arc::strong_count(name) > 1);
    }
}

#[derive(Debug)]
pub(super) enum Role {
    Subscribed {
        subscription: BTreeSet<StreamName>,
        assignment: Vec<StreamName>,
    },
    Client {
        protocols: Vec<JoinProtocol>,
        assignment: Bytes,
    },
}

#[derive(Debug)]
pub(super) struct Member {
    pub(super) instance_id: Option<String>,
    pub(super) client_id: String,
    pub(super) role: Role,
    pub(super) session_timeout: Duration,
    pub(super) rebalance_timeout: Duration,
    pub(super) last_heartbeat: Instant,
}

impl Member {
    pub(super) fn subscription(&self) -> Option<&BTreeSet<StreamName>> {
        match &self.role {
            Role::Subscribed { subscription, .. } => Some(subscription),
            Role::Client { .. } => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum Mode {
    Subscribed,
    Client {
        protocol_type: String,
        protocol_name: String,
        leader: String,
    },
}

pub(super) struct Rebalance {
    pub(super) id: u64,
    pub(super) expected: BTreeSet<String>,
    pub(super) joined: BTreeSet<String>,
    pub(super) waiters: BTreeMap<String, oneshot::Sender<JoinOutcome>>,
}

pub(super) struct Group {
    pub(super) loaded_epoch: i64,
    pub(super) phase: GroupState,
    pub(super) generation: i32,
    pub(super) mode: Option<Mode>,
    pub(super) names: Names,
    pub(super) members: BTreeMap<String, Member>,
    pub(super) offsets: OffsetTable,
    pub(super) appends_since_snapshot: u64,
    pub(super) next_rebalance_id: u64,
    pub(super) rebalance: Option<Rebalance>,
    pub(super) sync_waiters: BTreeMap<String, oneshot::Sender<SyncOutcome>>,
    pub(super) last_activity: Instant,
}

impl Group {
    pub(super) fn loaded(
        epoch: i64,
        names: Names,
        offsets: OffsetTable,
        appends_since_snapshot: u64,
    ) -> Self {
        Self {
            loaded_epoch: epoch,
            phase: GroupState::Empty,
            generation: 0,
            mode: None,
            names,
            members: BTreeMap::new(),
            offsets,
            appends_since_snapshot,
            next_rebalance_id: 1,
            rebalance: None,
            sync_waiters: BTreeMap::new(),
            last_activity: Instant::now(),
        }
    }

    pub(super) fn expire_members(&mut self, now: Instant) {
        let expired: Vec<String> = self
            .members
            .iter()
            .filter(|(_, member)| {
                now.saturating_duration_since(member.last_heartbeat) >= member.session_timeout
            })
            .map(|(id, _)| id.clone())
            .collect();
        if expired.is_empty() {
            return;
        }
        for id in expired {
            remove_member(self, &id);
        }
        if self.members.is_empty() {
            self.reset_empty();
            self.rebalance = None;
            self.sync_waiters.clear();
        } else if self.phase == GroupState::Stable {
            self.phase = GroupState::PreparingRebalance;
        }
    }

    pub(super) fn reset_empty(&mut self) {
        self.phase = GroupState::Empty;
        self.mode = None;
        self.names.sweep();
    }

    pub(super) fn subscription_entries(&self) -> usize {
        self.members
            .values()
            .filter_map(Member::subscription)
            .map(BTreeSet::len)
            .sum()
    }

    pub(super) fn idle_since(&self, now: Instant) -> Option<Duration> {
        (self.members.is_empty() && self.rebalance.is_none())
            .then(|| now.saturating_duration_since(self.last_activity))
    }

    pub(super) fn protocol_name(&self) -> &str {
        match &self.mode {
            Some(Mode::Client { protocol_name, .. }) => protocol_name,
            _ => "",
        }
    }

    pub(super) fn protocol_type(&self) -> &str {
        match &self.mode {
            Some(Mode::Client { protocol_type, .. }) => protocol_type,
            _ => "",
        }
    }
}

pub(super) fn remove_member(state: &mut Group, member_id: &str) {
    state.members.remove(member_id);
    state.sync_waiters.remove(member_id);
    if let Some(rebalance) = &mut state.rebalance {
        rebalance.expected.remove(member_id);
        rebalance.joined.remove(member_id);
        rebalance.waiters.remove(member_id);
    }
}

pub(super) fn evict_idle_groups(groups: &mut HashMap<String, Arc<Mutex<Group>>>, now: Instant) {
    groups.retain(|_, group| match group.try_lock() {
        Ok(mut state) => {
            state.expire_members(now);
            state
                .idle_since(now)
                .is_none_or(|idle| idle < IDLE_GROUP_TTL)
        }
        Err(_) => true,
    });
    if groups.len() < MAX_GROUPS {
        return;
    }
    let idlest = groups
        .iter()
        .filter_map(|(id, group)| {
            let state = group.try_lock().ok()?;
            state.idle_since(now).map(|idle| (idle, id.clone()))
        })
        .max()
        .map(|(_, id)| id);
    if let Some(id) = idlest {
        groups.remove(&id);
    }
}

pub(super) fn complete_rebalance(
    state: &mut Group,
    timed_out: bool,
) -> Vec<(oneshot::Sender<JoinOutcome>, JoinOutcome)> {
    let Some(mut rebalance) = state.rebalance.take() else {
        return Vec::new();
    };
    if timed_out {
        let missing: Vec<String> = rebalance
            .expected
            .difference(&rebalance.joined)
            .cloned()
            .collect();
        for id in missing {
            state.members.remove(&id);
            rebalance.waiters.remove(&id);
        }
    }
    if state.members.is_empty() || rebalance.joined.is_empty() {
        state.reset_empty();
        return rebalance
            .waiters
            .into_values()
            .map(|sender| {
                (
                    sender,
                    JoinOutcome::error(GroupError::RebalanceInProgress, String::new()),
                )
            })
            .collect();
    }

    let now = Instant::now();
    state.generation = state.generation.wrapping_add(1).max(1);
    state.sync_waiters.clear();
    match state.mode.clone() {
        Some(Mode::Subscribed) => complete_subscribed(state, rebalance, now),
        Some(Mode::Client {
            protocol_type,
            leader,
            ..
        }) => complete_client(state, rebalance, protocol_type, leader, now),
        None => unreachable!("a group with members has a mode"),
    }
}

fn complete_subscribed(
    state: &mut Group,
    rebalance: Rebalance,
    now: Instant,
) -> Vec<(oneshot::Sender<JoinOutcome>, JoinOutcome)> {
    state.phase = GroupState::Stable;
    let mut assignments = assign(&state.members);
    for (id, member) in &mut state.members {
        member.last_heartbeat = now;
        if let Role::Subscribed { assignment, .. } = &mut member.role {
            *assignment = assignments.remove(id).unwrap_or_default();
        }
    }
    state.names.sweep();
    let member_ids: Vec<String> = state.members.keys().cloned().collect();
    rebalance
        .waiters
        .into_iter()
        .map(|(member_id, sender)| {
            let assignment = match state.members.get(&member_id).map(|m| &m.role) {
                Some(Role::Subscribed { assignment, .. }) => {
                    assignment.iter().map(|s| s.to_string()).collect()
                }
                _ => Vec::new(),
            };
            (
                sender,
                JoinOutcome {
                    generation: state.generation,
                    member_id,
                    result: Ok(Joined::Subscribed {
                        assignment,
                        members: member_ids.clone(),
                    }),
                },
            )
        })
        .collect()
}

fn complete_client(
    state: &mut Group,
    rebalance: Rebalance,
    protocol_type: String,
    previous_leader: String,
    now: Instant,
) -> Vec<(oneshot::Sender<JoinOutcome>, JoinOutcome)> {
    let leader = if rebalance.joined.contains(&previous_leader) {
        previous_leader
    } else {
        rebalance.joined.iter().next().cloned().unwrap_or_default()
    };
    let Some(protocol_name) = select_protocol(state, &leader, &rebalance.joined) else {
        state.members.clear();
        state.reset_empty();
        return rebalance
            .waiters
            .into_iter()
            .map(|(member_id, sender)| {
                (
                    sender,
                    JoinOutcome::error(GroupError::InconsistentProtocol, member_id),
                )
            })
            .collect();
    };

    state.mode = Some(Mode::Client {
        protocol_type: protocol_type.clone(),
        protocol_name: protocol_name.clone(),
        leader: leader.clone(),
    });
    state.phase = GroupState::CompletingRebalance;
    for member in state.members.values_mut() {
        member.last_heartbeat = now;
        if let Role::Client { assignment, .. } = &mut member.role {
            *assignment = Bytes::new();
        }
    }

    let all_members: Vec<JoinMember> = state
        .members
        .iter()
        .filter_map(|(id, member)| match &member.role {
            Role::Client { protocols, .. } => protocols
                .iter()
                .find(|protocol| protocol.name == protocol_name)
                .map(|protocol| JoinMember {
                    member_id: id.clone(),
                    instance_id: member.instance_id.clone(),
                    metadata: protocol.metadata.clone(),
                }),
            Role::Subscribed { .. } => None,
        })
        .collect();
    rebalance
        .waiters
        .into_iter()
        .map(|(member_id, sender)| {
            let members = if member_id == leader {
                all_members.clone()
            } else {
                Vec::new()
            };
            (
                sender,
                JoinOutcome {
                    generation: state.generation,
                    member_id,
                    result: Ok(Joined::Client {
                        protocol_type: protocol_type.clone(),
                        protocol_name: protocol_name.clone(),
                        leader: leader.clone(),
                        members,
                    }),
                },
            )
        })
        .collect()
}

fn select_protocol(state: &Group, leader: &str, members: &BTreeSet<String>) -> Option<String> {
    let Role::Client { protocols, .. } = &state.members.get(leader)?.role else {
        return None;
    };
    protocols.iter().find_map(|candidate| {
        members
            .iter()
            .all(|id| {
                state
                    .members
                    .get(id)
                    .is_some_and(|member| match &member.role {
                        Role::Client { protocols, .. } => {
                            protocols.iter().any(|p| p.name == candidate.name)
                        }
                        Role::Subscribed { .. } => false,
                    })
            })
            .then(|| candidate.name.clone())
    })
}

pub(super) fn send_join_completions(
    completions: Option<Vec<(oneshot::Sender<JoinOutcome>, JoinOutcome)>>,
) {
    if let Some(completions) = completions {
        for (sender, outcome) in completions {
            let _ = sender.send(outcome);
        }
    }
}

pub(super) fn member_from_input(input: &JoinInput, names: &mut Names, now: Instant) -> Member {
    Member {
        instance_id: input.instance_id.clone(),
        client_id: input.client_id.clone(),
        role: match &input.membership {
            Membership::Subscribed(subscription) => Role::Subscribed {
                subscription: subscription.iter().map(|s| names.intern(s)).collect(),
                assignment: Vec::new(),
            },
            Membership::Client { protocols, .. } => Role::Client {
                protocols: protocols.clone(),
                assignment: Bytes::new(),
            },
        },
        session_timeout: Duration::from_millis(input.session_timeout_ms as u64),
        rebalance_timeout: rebalance_timeout_of(input),
        last_heartbeat: now,
    }
}

pub(super) fn mode_of(membership: &Membership) -> Mode {
    match membership {
        Membership::Subscribed(_) => Mode::Subscribed,
        Membership::Client { protocol_type, .. } => Mode::Client {
            protocol_type: protocol_type.clone(),
            protocol_name: String::new(),
            leader: String::new(),
        },
    }
}

pub(super) fn compatible(mode: &Mode, membership: &Membership) -> bool {
    match (mode, membership) {
        (Mode::Subscribed, Membership::Subscribed(_)) => true,
        (
            Mode::Client { protocol_type, .. },
            Membership::Client {
                protocol_type: t, ..
            },
        ) => protocol_type == t,
        _ => false,
    }
}

fn rebalance_timeout_of(input: &JoinInput) -> Duration {
    let ms = if input.rebalance_timeout_ms <= 0 {
        input.session_timeout_ms
    } else {
        input.rebalance_timeout_ms
    };
    Duration::from_millis(ms.max(1) as u64)
}

pub(super) fn validate_group_id(group_id: &str) -> Result<(), GroupError> {
    if group_id.is_empty() || group_id.len() > MAX_GROUP_ID_BYTES {
        Err(GroupError::InvalidRequest)
    } else {
        Ok(())
    }
}

pub(super) fn validate_stream_name(name: &str) -> Result<(), GroupError> {
    if name.is_empty() || name.len() > MAX_STREAM_NAME_BYTES {
        Err(GroupError::InvalidRequest)
    } else {
        Ok(())
    }
}

pub(super) fn validate_join(input: &JoinInput) -> Result<(), GroupError> {
    validate_group_id(&input.group_id)?;
    if input.member_id.len() > MAX_MEMBER_ID_BYTES
        || !(MIN_SESSION_TIMEOUT_MS..=MAX_SESSION_TIMEOUT_MS).contains(&input.session_timeout_ms)
    {
        return Err(GroupError::InvalidRequest);
    }
    match &input.membership {
        Membership::Subscribed(subscription) => {
            if subscription.len() > MAX_STREAMS_PER_GROUP {
                return Err(GroupError::InvalidRequest);
            }
            subscription
                .iter()
                .try_for_each(|name| validate_stream_name(name))
        }
        Membership::Client {
            protocol_type,
            protocols,
        } => {
            let metadata_bytes: usize = protocols.iter().map(|p| p.metadata.len()).sum();
            if protocol_type.is_empty()
                || protocols.is_empty()
                || protocols.len() > MAX_PROTOCOLS_PER_MEMBER
                || protocols.iter().any(|protocol| protocol.name.is_empty())
                || metadata_bytes > MAX_METADATA_BYTES_PER_MEMBER
            {
                return Err(GroupError::InvalidRequest);
            }
            Ok(())
        }
    }
}

pub(super) fn new_member_id(client_id: &str) -> String {
    format!("{client_id}-{}", uuid::Uuid::new_v4())
}

pub(super) fn group_stream_name(group_id: &str) -> String {
    let mut encoded = String::with_capacity(group_id.len() * 2);
    for byte in group_id.as_bytes() {
        use std::fmt::Write;
        let _ = write!(encoded, "{byte:02x}");
    }
    format!("/_sys/groups/{encoded}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stream_name_is_collision_free() {
        assert_ne!(group_stream_name("a/b"), group_stream_name("a%2fb"));
        assert!(group_stream_name("g").starts_with("/_sys/groups/"));
    }

    #[test]
    fn names_are_shared_and_swept() {
        let mut names = Names::default();
        let a = names.intern("a");
        let again = names.intern("a");
        assert!(Arc::ptr_eq(&a, &again));
        let _b = names.intern("b");
        drop(again);
        drop(a);
        names.sweep();
        assert_eq!(names.len(), 1);
        assert!(names.contains("b"));
    }
}

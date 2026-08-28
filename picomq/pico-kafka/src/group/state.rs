//! In-memory group state machine: membership, generations, rebalances.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use tokio::sync::{oneshot, Mutex};

use crate::handlers::common::{
    INCONSISTENT_GROUP_PROTOCOL, INVALID_REQUEST, REBALANCE_IN_PROGRESS,
};

use super::offsets::OffsetTable;
use super::{JoinInput, JoinMember, JoinOutcome, JoinProtocol, SyncOutcome};

pub(super) const MAX_GROUPS: usize = 10_000;
pub(super) const MAX_MEMBERS_PER_GROUP: usize = 10_000;
pub(super) const MAX_GROUP_ID_BYTES: usize = 255;
pub(super) const MAX_MEMBER_ID_BYTES: usize = 512;
pub(super) const MAX_PROTOCOLS_PER_MEMBER: usize = 32;
pub(super) const MIN_SESSION_TIMEOUT_MS: i32 = 1_000;
pub(super) const MAX_SESSION_TIMEOUT_MS: i32 = 300_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum GroupPhase {
    Empty,
    PreparingRebalance,
    CompletingRebalance,
    Stable,
}

impl GroupPhase {
    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::Empty => "Empty",
            Self::PreparingRebalance => "PreparingRebalance",
            Self::CompletingRebalance => "CompletingRebalance",
            Self::Stable => "Stable",
        }
    }
}

#[derive(Debug)]
pub(super) struct Member {
    pub(super) instance_id: Option<String>,
    pub(super) client_id: String,
    pub(super) protocols: Vec<JoinProtocol>,
    pub(super) assignment: Bytes,
    pub(super) session_timeout: Duration,
    pub(super) rebalance_timeout: Duration,
    pub(super) last_heartbeat: Instant,
}

pub(super) struct Rebalance {
    pub(super) id: u64,
    pub(super) expected: BTreeSet<String>,
    pub(super) joined: BTreeSet<String>,
    pub(super) waiters: BTreeMap<String, oneshot::Sender<JoinOutcome>>,
}

pub(super) struct Group {
    /// Stream epoch the durable offsets were replayed at. A change means the
    /// stream moved owners and the state must be rebuilt.
    pub(super) loaded_epoch: i64,
    pub(super) phase: GroupPhase,
    pub(super) generation: i32,
    pub(super) protocol_type: String,
    pub(super) protocol_name: String,
    pub(super) leader: String,
    pub(super) members: BTreeMap<String, Member>,
    pub(super) offsets: OffsetTable,
    /// Records appended to the group stream since the last snapshot+trim.
    pub(super) appends_since_snapshot: u64,
    pub(super) next_rebalance_id: u64,
    pub(super) rebalance: Option<Rebalance>,
    pub(super) sync_waiters: BTreeMap<String, oneshot::Sender<SyncOutcome>>,
}

impl Group {
    pub(super) fn loaded(epoch: i64, offsets: OffsetTable, appends_since_snapshot: u64) -> Self {
        Self {
            loaded_epoch: epoch,
            phase: GroupPhase::Empty,
            generation: 0,
            protocol_type: String::new(),
            protocol_name: String::new(),
            leader: String::new(),
            members: BTreeMap::new(),
            offsets,
            appends_since_snapshot,
            next_rebalance_id: 1,
            rebalance: None,
            sync_waiters: BTreeMap::new(),
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
            self.phase = GroupPhase::Empty;
            self.leader.clear();
            self.protocol_name.clear();
            self.rebalance = None;
            self.sync_waiters.clear();
        } else if self.phase == GroupPhase::Stable {
            self.phase = GroupPhase::PreparingRebalance;
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

/// Drop empty groups and lazily expire members so idle groups do not pin
/// memory.
pub(super) fn prune_empty_groups(groups: &mut HashMap<String, Arc<Mutex<Group>>>) {
    let now = Instant::now();
    groups.retain(|_, group| match group.try_lock() {
        Ok(mut state) => {
            state.expire_members(now);
            !state.members.is_empty() || state.rebalance.is_some()
        }
        Err(_) => true,
    });
}

/// Returns the responses owed to parked JoinGroup requests, which the caller
/// sends outside the lock.
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
        state.phase = GroupPhase::Empty;
        return rebalance
            .waiters
            .into_values()
            .map(|sender| {
                (
                    sender,
                    JoinOutcome::error(REBALANCE_IN_PROGRESS, String::new()),
                )
            })
            .collect();
    }

    let leader = if rebalance.joined.contains(&state.leader) {
        state.leader.clone()
    } else {
        rebalance.joined.iter().next().cloned().unwrap_or_default()
    };
    let Some(protocol_name) = select_protocol(state, &leader, &rebalance.joined) else {
        state.phase = GroupPhase::Empty;
        state.members.clear();
        return rebalance
            .waiters
            .into_iter()
            .map(|(member_id, sender)| {
                (
                    sender,
                    JoinOutcome::error(INCONSISTENT_GROUP_PROTOCOL, member_id),
                )
            })
            .collect();
    };

    state.generation = state.generation.wrapping_add(1).max(1);
    state.leader = leader.clone();
    state.protocol_name = protocol_name.clone();
    state.phase = GroupPhase::CompletingRebalance;
    state.sync_waiters.clear();
    let now = Instant::now();
    for member in state.members.values_mut() {
        member.assignment = Bytes::new();
        member.last_heartbeat = now;
    }

    let all_members: Vec<JoinMember> = state
        .members
        .iter()
        .filter_map(|(id, member)| {
            member
                .protocols
                .iter()
                .find(|protocol| protocol.name == protocol_name)
                .map(|protocol| JoinMember {
                    member_id: id.clone(),
                    group_instance_id: member.instance_id.clone(),
                    metadata: protocol.metadata.clone(),
                })
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
                    error_code: 0,
                    generation_id: state.generation,
                    protocol_type: Some(state.protocol_type.clone()),
                    protocol_name: Some(protocol_name.clone()),
                    leader: leader.clone(),
                    member_id,
                    members,
                },
            )
        })
        .collect()
}

/// First protocol in the leader's preference list supported by every joined
/// member, mirroring Kafka's selection rule.
fn select_protocol(state: &Group, leader: &str, members: &BTreeSet<String>) -> Option<String> {
    state
        .members
        .get(leader)?
        .protocols
        .iter()
        .find_map(|candidate| {
            members
                .iter()
                .all(|id| {
                    state.members.get(id).is_some_and(|member| {
                        member.protocols.iter().any(|p| p.name == candidate.name)
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

pub(super) fn member_from_input(input: &JoinInput, now: Instant) -> Member {
    Member {
        instance_id: input.group_instance_id.clone(),
        client_id: input.client_id.clone(),
        protocols: input.protocols.clone(),
        assignment: Bytes::new(),
        session_timeout: Duration::from_millis(input.session_timeout_ms as u64),
        rebalance_timeout: rebalance_timeout_of(input),
        last_heartbeat: now,
    }
}

/// Kafka treats a non-positive rebalance timeout as "use the session timeout".
fn rebalance_timeout_of(input: &JoinInput) -> Duration {
    let ms = if input.rebalance_timeout_ms <= 0 {
        input.session_timeout_ms
    } else {
        input.rebalance_timeout_ms
    };
    Duration::from_millis(ms.max(1) as u64)
}

pub(super) fn validate_group_id(group_id: &str) -> Result<(), i16> {
    if group_id.is_empty() || group_id.len() > MAX_GROUP_ID_BYTES {
        Err(INVALID_REQUEST)
    } else {
        Ok(())
    }
}

pub(super) fn validate_join(input: &JoinInput) -> Result<(), i16> {
    validate_group_id(&input.group_id)?;
    if input.member_id.len() > MAX_MEMBER_ID_BYTES
        || input.protocol_type.is_empty()
        || input.protocols.is_empty()
        || input.protocols.len() > MAX_PROTOCOLS_PER_MEMBER
        || input
            .protocols
            .iter()
            .any(|protocol| protocol.name.is_empty())
        || !(MIN_SESSION_TIMEOUT_MS..=MAX_SESSION_TIMEOUT_MS).contains(&input.session_timeout_ms)
    {
        return Err(INVALID_REQUEST);
    }
    Ok(())
}

pub(super) fn new_member_id(client_id: &str) -> String {
    format!("{client_id}-{}", uuid::Uuid::new_v4())
}

/// Hex-encode the client-chosen group id so arbitrary bytes cannot collide
/// with or escape the `/_sys/groups/` namespace.
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
}

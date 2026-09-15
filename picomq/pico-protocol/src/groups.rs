use std::collections::BTreeMap;

use bytes::Bytes;
use http::Method;
use serde_json::{Map, Value, json};

use crate::error::CodecError;
use crate::pico::{CT_JSON, GROUPS_PATH_PREFIX};
use crate::wire::{WireRequest, urlencode};

pub const Q_GENERATION: &str = "generation";
pub const Q_INSTANCE_ID: &str = "instanceId";
pub const Q_STREAM: &str = "stream";
pub const DEFAULT_SESSION_TIMEOUT_MS: u32 = 30_000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemberFence {
    pub member_id: String,
    pub generation: i32,
    pub instance_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CommittedOffset {
    pub position: u64,
    pub metadata: Option<String>,
}

pub type Offsets = BTreeMap<String, CommittedOffset>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinRequest {
    pub group: String,
    pub subscription: Vec<String>,
    pub member_id: Option<String>,
    pub instance_id: Option<String>,
    pub client_id: Option<String>,
    pub session_timeout_ms: Option<u32>,
    pub rebalance_timeout_ms: Option<u32>,
}

impl JoinRequest {
    pub fn encode(&self) -> WireRequest {
        let mut body = Map::new();
        body.insert("subscription".into(), json!(self.subscription));
        put_str(&mut body, "memberId", self.member_id.as_deref());
        put_str(&mut body, "instanceId", self.instance_id.as_deref());
        put_str(&mut body, "clientId", self.client_id.as_deref());
        put_u32(&mut body, "sessionTimeoutMs", self.session_timeout_ms);
        put_u32(&mut body, "rebalanceTimeoutMs", self.rebalance_timeout_ms);
        json_request(
            Method::POST,
            format!("{}/members", group_path(&self.group)),
            body,
            &[200],
        )
    }

    pub fn decode(group: String, body: &[u8]) -> Result<Self, CodecError> {
        let node = object(body)?;
        Ok(Self {
            group,
            subscription: strings(&node, "subscription")?
                .ok_or_else(|| CodecError::new("subscription is required"))?,
            member_id: string(&node, "memberId")?,
            instance_id: string(&node, "instanceId")?,
            client_id: string(&node, "clientId")?,
            session_timeout_ms: u32_of(&node, "sessionTimeoutMs")?,
            rebalance_timeout_ms: u32_of(&node, "rebalanceTimeoutMs")?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinResponse {
    pub member_id: String,
    pub generation: i32,
    pub assignment: Vec<String>,
    pub members: Vec<String>,
}

impl JoinResponse {
    pub fn encode(&self) -> Bytes {
        json_body(json!({
            "memberId": self.member_id,
            "generation": self.generation,
            "assignment": self.assignment,
            "members": self.members,
        }))
    }

    pub fn decode(body: &[u8]) -> Result<Self, CodecError> {
        let node = object(body)?;
        Ok(Self {
            member_id: required_string(&node, "memberId")?,
            generation: required_i32(&node, "generation")?,
            assignment: strings(&node, "assignment")?.unwrap_or_default(),
            members: strings(&node, "members")?.unwrap_or_default(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssignmentRequest {
    pub group: String,
    pub fence: MemberFence,
}

impl AssignmentRequest {
    pub fn encode(&self) -> WireRequest {
        let mut path = format!(
            "{}?{Q_GENERATION}={}",
            member_path(&self.group, &self.fence.member_id),
            self.fence.generation
        );
        if let Some(instance_id) = &self.fence.instance_id {
            path.push_str(&format!("&{Q_INSTANCE_ID}={}", urlencode(instance_id)));
        }
        WireRequest::new(Method::GET, path, &[200])
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssignmentResponse {
    pub generation: i32,
    pub assignment: Vec<String>,
}

impl AssignmentResponse {
    pub fn encode(&self) -> Bytes {
        json_body(json!({ "generation": self.generation, "assignment": self.assignment }))
    }

    pub fn decode(body: &[u8]) -> Result<Self, CodecError> {
        let node = object(body)?;
        Ok(Self {
            generation: required_i32(&node, "generation")?,
            assignment: strings(&node, "assignment")?.unwrap_or_default(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeartbeatRequest {
    pub group: String,
    pub fence: MemberFence,
}

impl HeartbeatRequest {
    pub fn encode(&self) -> WireRequest {
        let mut body = Map::new();
        body.insert("generation".into(), json!(self.fence.generation));
        put_str(&mut body, "instanceId", self.fence.instance_id.as_deref());
        json_request(
            Method::POST,
            format!(
                "{}/heartbeat",
                member_path(&self.group, &self.fence.member_id)
            ),
            body,
            &[204],
        )
    }

    pub fn decode(group: String, member_id: String, body: &[u8]) -> Result<Self, CodecError> {
        let node = object(body)?;
        Ok(Self {
            group,
            fence: MemberFence {
                member_id,
                generation: required_i32(&node, "generation")?,
                instance_id: string(&node, "instanceId")?,
            },
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaveRequest {
    pub group: String,
    pub member_id: String,
    pub instance_id: Option<String>,
}

impl LeaveRequest {
    pub fn encode(&self) -> WireRequest {
        let mut path = member_path(&self.group, &self.member_id);
        if let Some(instance_id) = &self.instance_id {
            path.push_str(&format!("?{Q_INSTANCE_ID}={}", urlencode(instance_id)));
        }
        WireRequest::new(Method::DELETE, path, &[204])
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitRequest {
    pub group: String,
    pub fence: Option<MemberFence>,
    pub offsets: Offsets,
}

impl CommitRequest {
    pub fn encode(&self) -> WireRequest {
        let mut body = Map::new();
        body.insert("offsets".into(), offsets_json(&self.offsets));
        if let Some(fence) = &self.fence {
            body.insert("memberId".into(), json!(fence.member_id));
            body.insert("generation".into(), json!(fence.generation));
            put_str(&mut body, "instanceId", fence.instance_id.as_deref());
        }
        json_request(
            Method::PUT,
            format!("{}/offsets", group_path(&self.group)),
            body,
            &[204],
        )
    }

    pub fn decode(group: String, body: &[u8]) -> Result<Self, CodecError> {
        let node = object(body)?;
        let offsets = offsets_of(&node)?;
        let fence = match (string(&node, "memberId")?, i32_of(&node, "generation")?) {
            (Some(member_id), Some(generation)) => Some(MemberFence {
                member_id,
                generation,
                instance_id: string(&node, "instanceId")?,
            }),
            (None, None) => None,
            _ => return Err(CodecError::new("memberId and generation go together")),
        };
        Ok(Self {
            group,
            fence,
            offsets,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchOffsetsRequest {
    pub group: String,
    pub streams: Vec<String>,
}

impl FetchOffsetsRequest {
    pub fn encode(&self) -> WireRequest {
        let mut path = format!("{}/offsets", group_path(&self.group));
        for (i, stream) in self.streams.iter().enumerate() {
            path.push(if i == 0 { '?' } else { '&' });
            path.push_str(&format!("{Q_STREAM}={}", urlencode(stream)));
        }
        WireRequest::new(Method::GET, path, &[200])
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct OffsetsResponse {
    pub offsets: Offsets,
}

impl OffsetsResponse {
    pub fn encode(&self) -> Bytes {
        json_body(json!({ "offsets": offsets_json(&self.offsets) }))
    }

    pub fn decode(body: &[u8]) -> Result<Self, CodecError> {
        let node = object(body)?;
        Ok(Self {
            offsets: offsets_of(&node)?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeRequest {
    pub group: String,
}

impl DescribeRequest {
    pub fn encode(&self) -> WireRequest {
        WireRequest::new(Method::GET, group_path(&self.group), &[200])
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemberDescription {
    pub member_id: String,
    pub instance_id: Option<String>,
    pub client_id: String,
    /// Absent for members that carry an opaque protocol payload (Kafka
    /// clients doing their own assignment).
    pub subscription: Option<Vec<String>>,
    pub assignment: Option<Vec<String>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupDescription {
    pub group: String,
    pub state: String,
    pub generation: i32,
    pub protocol_type: Option<String>,
    pub members: Vec<MemberDescription>,
}

impl GroupDescription {
    pub fn encode(&self) -> Bytes {
        let members: Vec<Value> = self
            .members
            .iter()
            .map(|member| {
                let mut node = Map::new();
                node.insert("memberId".into(), json!(member.member_id));
                node.insert("instanceId".into(), json!(member.instance_id));
                node.insert("clientId".into(), json!(member.client_id));
                if let Some(subscription) = &member.subscription {
                    node.insert("subscription".into(), json!(subscription));
                }
                if let Some(assignment) = &member.assignment {
                    node.insert("assignment".into(), json!(assignment));
                }
                Value::Object(node)
            })
            .collect();
        let mut node = Map::new();
        node.insert("group".into(), json!(self.group));
        node.insert("state".into(), json!(self.state));
        node.insert("generation".into(), json!(self.generation));
        node.insert("members".into(), Value::Array(members));
        put_str(&mut node, "protocolType", self.protocol_type.as_deref());
        json_body(Value::Object(node))
    }

    pub fn decode(body: &[u8]) -> Result<Self, CodecError> {
        let node = object(body)?;
        let members = match node.get("members") {
            Some(Value::Array(members)) => members
                .iter()
                .map(|member| {
                    let member = member
                        .as_object()
                        .ok_or_else(|| CodecError::new("members must be objects"))?;
                    Ok(MemberDescription {
                        member_id: required_string(member, "memberId")?,
                        instance_id: string(member, "instanceId")?,
                        client_id: string(member, "clientId")?.unwrap_or_default(),
                        subscription: strings(member, "subscription")?,
                        assignment: strings(member, "assignment")?,
                    })
                })
                .collect::<Result<_, _>>()?,
            _ => return Err(CodecError::new("members must be an array")),
        };
        Ok(Self {
            group: required_string(&node, "group")?,
            state: required_string(&node, "state")?,
            generation: required_i32(&node, "generation")?,
            protocol_type: string(&node, "protocolType")?,
            members,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ListGroupsRequest;

impl ListGroupsRequest {
    pub fn encode(&self) -> WireRequest {
        WireRequest::new(Method::GET, GROUPS_PATH_PREFIX.to_owned(), &[200])
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupSummary {
    pub group: String,
    pub state: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct GroupListing {
    pub groups: Vec<GroupSummary>,
}

impl GroupListing {
    pub fn encode(&self) -> Bytes {
        let groups: Vec<Value> = self
            .groups
            .iter()
            .map(|summary| json!({ "group": summary.group, "state": summary.state }))
            .collect();
        json_body(json!({ "groups": groups }))
    }

    pub fn decode(body: &[u8]) -> Result<Self, CodecError> {
        let node = object(body)?;
        let groups = match node.get("groups") {
            Some(Value::Array(groups)) => groups
                .iter()
                .map(|summary| {
                    let summary = summary
                        .as_object()
                        .ok_or_else(|| CodecError::new("groups must be objects"))?;
                    Ok(GroupSummary {
                        group: required_string(summary, "group")?,
                        state: required_string(summary, "state")?,
                    })
                })
                .collect::<Result<_, _>>()?,
            _ => return Err(CodecError::new("groups must be an array")),
        };
        Ok(Self { groups })
    }
}

fn group_path(group: &str) -> String {
    format!("{GROUPS_PATH_PREFIX}/{}", urlencode(group))
}

fn member_path(group: &str, member_id: &str) -> String {
    format!("{}/members/{}", group_path(group), urlencode(member_id))
}

fn json_request(
    method: Method,
    path: String,
    body: Map<String, Value>,
    ok: &'static [u16],
) -> WireRequest {
    WireRequest::new(method, path, ok)
        .header("Content-Type", CT_JSON)
        .body(json_body(Value::Object(body)))
}

fn json_body(value: Value) -> Bytes {
    Bytes::from(serde_json::to_vec(&value).expect("json encode"))
}

fn offsets_json(offsets: &Offsets) -> Value {
    Value::Object(
        offsets
            .iter()
            .map(|(stream, offset)| {
                let mut node = Map::new();
                node.insert("position".into(), json!(offset.position));
                node.insert("metadata".into(), json!(offset.metadata));
                (stream.clone(), Value::Object(node))
            })
            .collect(),
    )
}

fn offsets_of(node: &Map<String, Value>) -> Result<Offsets, CodecError> {
    let Some(Value::Object(offsets)) = node.get("offsets") else {
        return Err(CodecError::new("offsets must be an object keyed by stream"));
    };
    offsets
        .iter()
        .map(|(stream, value)| {
            let entry = value
                .as_object()
                .ok_or_else(|| CodecError::new("each offset must be an object"))?;
            let position = entry
                .get("position")
                .and_then(Value::as_u64)
                .ok_or_else(|| CodecError::new("position must be a non-negative integer"))?;
            Ok((
                stream.clone(),
                CommittedOffset {
                    position,
                    metadata: string(entry, "metadata")?,
                },
            ))
        })
        .collect()
}

fn put_str(node: &mut Map<String, Value>, key: &str, value: Option<&str>) {
    if let Some(value) = value {
        node.insert(key.into(), json!(value));
    }
}

fn put_u32(node: &mut Map<String, Value>, key: &str, value: Option<u32>) {
    if let Some(value) = value {
        node.insert(key.into(), json!(value));
    }
}

fn object(body: &[u8]) -> Result<Map<String, Value>, CodecError> {
    match serde_json::from_slice::<Value>(body) {
        Ok(Value::Object(node)) => Ok(node),
        _ => Err(CodecError::new("expected a JSON object")),
    }
}

fn string(node: &Map<String, Value>, key: &str) -> Result<Option<String>, CodecError> {
    match node.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) => Ok(Some(value.clone())),
        Some(_) => Err(CodecError::new(format!("{key} must be a string"))),
    }
}

fn required_string(node: &Map<String, Value>, key: &str) -> Result<String, CodecError> {
    string(node, key)?.ok_or_else(|| CodecError::new(format!("{key} is required")))
}

fn i32_of(node: &Map<String, Value>, key: &str) -> Result<Option<i32>, CodecError> {
    match node.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Number(value)) => value
            .as_i64()
            .and_then(|value| i32::try_from(value).ok())
            .map(Some)
            .ok_or_else(|| CodecError::new(format!("{key} must be a 32-bit integer"))),
        Some(_) => Err(CodecError::new(format!("{key} must be an integer"))),
    }
}

fn required_i32(node: &Map<String, Value>, key: &str) -> Result<i32, CodecError> {
    i32_of(node, key)?.ok_or_else(|| CodecError::new(format!("{key} is required")))
}

fn u32_of(node: &Map<String, Value>, key: &str) -> Result<Option<u32>, CodecError> {
    match node.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Number(value)) => value
            .as_u64()
            .and_then(|value| u32::try_from(value).ok())
            .map(Some)
            .ok_or_else(|| CodecError::new(format!("{key} must be a non-negative 32-bit integer"))),
        Some(_) => Err(CodecError::new(format!("{key} must be an integer"))),
    }
}

fn strings(node: &Map<String, Value>, key: &str) -> Result<Option<Vec<String>>, CodecError> {
    match node.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Array(values)) => values
            .iter()
            .map(|value| {
                value
                    .as_str()
                    .map(str::to_owned)
                    .ok_or_else(|| CodecError::new(format!("{key} must be an array of strings")))
            })
            .collect::<Result<Vec<_>, _>>()
            .map(Some),
        Some(_) => Err(CodecError::new(format!(
            "{key} must be an array of strings"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fence() -> MemberFence {
        MemberFence {
            member_id: "m/1".to_owned(),
            generation: 3,
            instance_id: Some("i-1".to_owned()),
        }
    }

    #[test]
    fn requests_round_trip() {
        let join = JoinRequest {
            group: "orders/eu".to_owned(),
            subscription: vec!["/a".to_owned(), "/b".to_owned()],
            member_id: None,
            instance_id: Some("i-1".to_owned()),
            client_id: None,
            session_timeout_ms: Some(6000),
            rebalance_timeout_ms: None,
        };
        let wire = join.encode();
        assert_eq!(wire.method, Method::POST);
        assert_eq!(wire.path_and_query, "/_groups/orders%2Feu/members");
        assert_eq!(
            JoinRequest::decode("orders/eu".to_owned(), &wire.body).unwrap(),
            join
        );

        let heartbeat = HeartbeatRequest {
            group: "g".to_owned(),
            fence: fence(),
        };
        let wire = heartbeat.encode();
        assert_eq!(wire.path_and_query, "/_groups/g/members/m%2F1/heartbeat");
        assert_eq!(
            HeartbeatRequest::decode("g".to_owned(), "m/1".to_owned(), &wire.body).unwrap(),
            heartbeat
        );

        let commit = CommitRequest {
            group: "g".to_owned(),
            fence: Some(fence()),
            offsets: Offsets::from([(
                "/a".to_owned(),
                CommittedOffset {
                    position: 7,
                    metadata: Some("ck".to_owned()),
                },
            )]),
        };
        let wire = commit.encode();
        assert_eq!(wire.method, Method::PUT);
        assert_eq!(
            CommitRequest::decode("g".to_owned(), &wire.body).unwrap(),
            commit
        );

        let wire = FetchOffsetsRequest {
            group: "g".to_owned(),
            streams: vec!["/a".to_owned(), "/b c".to_owned()],
        }
        .encode();
        assert_eq!(
            wire.path_and_query,
            "/_groups/g/offsets?stream=%2Fa&stream=%2Fb%20c"
        );
        let wire = AssignmentRequest {
            group: "g".to_owned(),
            fence: fence(),
        }
        .encode();
        assert_eq!(
            wire.path_and_query,
            "/_groups/g/members/m%2F1?generation=3&instanceId=i-1"
        );
    }

    #[test]
    fn responses_round_trip() {
        let join = JoinResponse {
            member_id: "m".to_owned(),
            generation: 2,
            assignment: vec!["/a".to_owned()],
            members: vec!["m".to_owned(), "n".to_owned()],
        };
        assert_eq!(JoinResponse::decode(&join.encode()).unwrap(), join);

        let offsets = OffsetsResponse {
            offsets: Offsets::from([(
                "/a".to_owned(),
                CommittedOffset {
                    position: 5,
                    metadata: None,
                },
            )]),
        };
        assert_eq!(OffsetsResponse::decode(&offsets.encode()).unwrap(), offsets);

        let described = GroupDescription {
            group: "g".to_owned(),
            state: "Stable".to_owned(),
            generation: 1,
            protocol_type: Some("consumer".to_owned()),
            members: vec![
                MemberDescription {
                    member_id: "m".to_owned(),
                    instance_id: None,
                    client_id: "pico".to_owned(),
                    subscription: Some(vec!["/a".to_owned()]),
                    assignment: Some(vec!["/a".to_owned()]),
                },
                MemberDescription {
                    member_id: "k".to_owned(),
                    instance_id: None,
                    client_id: "rdkafka".to_owned(),
                    subscription: None,
                    assignment: None,
                },
            ],
        };
        assert_eq!(
            GroupDescription::decode(&described.encode()).unwrap(),
            described
        );

        let listing = GroupListing {
            groups: vec![GroupSummary {
                group: "g".to_owned(),
                state: "Empty".to_owned(),
            }],
        };
        assert_eq!(GroupListing::decode(&listing.encode()).unwrap(), listing);
    }

    #[test]
    fn decoding_rejects_bad_shapes() {
        assert!(JoinRequest::decode("g".to_owned(), br#"{"clientId":"w"}"#).is_err());
        assert!(JoinRequest::decode("g".to_owned(), br#"{"subscription":[1]}"#).is_err());
        assert!(JoinRequest::decode("g".to_owned(), b"[]").is_err());
        assert!(
            CommitRequest::decode("g".to_owned(), br#"{"offsets":{"/a":{"position":-1}}}"#)
                .is_err()
        );
        assert!(
            CommitRequest::decode("g".to_owned(), br#"{"memberId":"m","offsets":{}}"#).is_err()
        );
        assert!(JoinResponse::decode(br#"{"generation":1}"#).is_err());
    }
}

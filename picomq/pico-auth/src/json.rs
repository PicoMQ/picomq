//! JSON form of [`Scope`], shared by the admin API, CLI, and dashboard.
//!
//! Decoding is strict: unknown keys, operations, audiences, and matcher kinds
//! are [`AuthError::Malformed`], never ignored.

use serde_json::{Value, json};

use crate::AuthError;
use crate::scope::{
    Audience, Operation, OperationGroups, ReadWrite, ResourceMatcher, ResourceSet, Scope,
};

pub fn operation_name(op: Operation) -> &'static str {
    match op {
        Operation::Read => "read",
        Operation::Head => "head",
        Operation::List => "list",
        Operation::Create => "create",
        Operation::Append => "append",
        Operation::Trim => "trim",
        Operation::Close => "close",
        Operation::Delete => "delete",
        Operation::IssueToken => "issue_token",
        Operation::RevokeToken => "revoke_token",
        Operation::ListTokens => "list_tokens",
        Operation::ClusterRead => "cluster_read",
        Operation::NodeRead => "node_read",
        Operation::StreamInspect => "stream_inspect",
        Operation::TransferStream => "transfer_stream",
        Operation::UpdateNodeSlots => "update_node_slots",
    }
}

pub fn operation_from_name(name: &str) -> Option<Operation> {
    Operation::ALL
        .into_iter()
        .find(|op| operation_name(*op) == name)
}

pub fn audience_name(audience: Audience) -> &'static str {
    match audience {
        Audience::Pico => "pico",
        Audience::DurableStreams => "durable_streams",
        Audience::Admin => "admin",
    }
}

pub fn audience_from_name(name: &str) -> Option<Audience> {
    match name {
        "pico" => Some(Audience::Pico),
        "durable_streams" => Some(Audience::DurableStreams),
        "admin" => Some(Audience::Admin),
        _ => None,
    }
}

pub fn scope_to_json(scope: &Scope) -> Value {
    json!({
        "streams": matchers_to_json(&scope.streams),
        "tokens": matchers_to_json(&scope.tokens),
        "groups": {
            "stream": rw_to_json(scope.groups.stream),
            "tokens": rw_to_json(scope.groups.tokens),
            "admin": rw_to_json(scope.groups.admin),
        },
        "ops": scope.ops.iter().map(|op| operation_name(*op)).collect::<Vec<_>>(),
        "audiences": scope.audiences.iter().map(|a| audience_name(*a)).collect::<Vec<_>>(),
        "autoPrefixStreams": scope.auto_prefix_streams,
        "expiresAtMs": scope.expires_at_ms,
    })
}

/// Missing keys take the deny-all defaults of [`Scope::default`].
pub fn scope_from_json(value: &Value) -> Result<Scope, AuthError> {
    let object = value.as_object().ok_or(AuthError::Malformed)?;
    let mut scope = Scope::default();
    for (key, value) in object {
        match key.as_str() {
            "streams" => scope.streams = matchers_from_json(value)?,
            "tokens" => scope.tokens = matchers_from_json(value)?,
            "groups" => scope.groups = groups_from_json(value)?,
            "ops" => {
                for name in strings_from_json(value)? {
                    let op = operation_from_name(&name).ok_or(AuthError::Malformed)?;
                    scope.ops.insert(op);
                }
            }
            "audiences" => {
                for name in strings_from_json(value)? {
                    let audience = audience_from_name(&name).ok_or(AuthError::Malformed)?;
                    scope.audiences.insert(audience);
                }
            }
            "autoPrefixStreams" => {
                scope.auto_prefix_streams = value.as_bool().ok_or(AuthError::Malformed)?;
            }
            "expiresAtMs" => {
                scope.expires_at_ms = match value {
                    Value::Null => None,
                    other => Some(other.as_i64().ok_or(AuthError::Malformed)?),
                };
            }
            _ => return Err(AuthError::Malformed),
        }
    }
    Ok(scope)
}

fn matchers_to_json(set: &ResourceSet) -> Vec<Value> {
    set.matchers()
        .iter()
        .map(|matcher| match matcher {
            ResourceMatcher::Exact(name) => json!({ "exact": name }),
            ResourceMatcher::Prefix(prefix) => json!({ "prefix": prefix }),
        })
        .collect()
}

fn matchers_from_json(value: &Value) -> Result<ResourceSet, AuthError> {
    let entries = value.as_array().ok_or(AuthError::Malformed)?;
    let mut matchers = Vec::with_capacity(entries.len());
    for entry in entries {
        let object = entry.as_object().ok_or(AuthError::Malformed)?;
        let (kind, name) = match object.iter().collect::<Vec<_>>().as_slice() {
            [(kind, name)] => (kind.as_str(), name.as_str().ok_or(AuthError::Malformed)?),
            _ => return Err(AuthError::Malformed),
        };
        matchers.push(match kind {
            "exact" => ResourceMatcher::Exact(name.to_owned()),
            "prefix" => ResourceMatcher::Prefix(name.to_owned()),
            _ => return Err(AuthError::Malformed),
        });
    }
    Ok(ResourceSet::any_of(matchers))
}

fn rw_to_json(rw: ReadWrite) -> Value {
    json!({ "read": rw.read, "write": rw.write })
}

fn rw_from_json(value: &Value) -> Result<ReadWrite, AuthError> {
    let object = value.as_object().ok_or(AuthError::Malformed)?;
    let mut rw = ReadWrite::none();
    for (key, value) in object {
        let flag = value.as_bool().ok_or(AuthError::Malformed)?;
        match key.as_str() {
            "read" => rw.read = flag,
            "write" => rw.write = flag,
            _ => return Err(AuthError::Malformed),
        }
    }
    Ok(rw)
}

fn groups_from_json(value: &Value) -> Result<OperationGroups, AuthError> {
    let object = value.as_object().ok_or(AuthError::Malformed)?;
    let mut groups = OperationGroups::default();
    for (key, value) in object {
        let rw = rw_from_json(value)?;
        match key.as_str() {
            "stream" => groups.stream = rw,
            "tokens" => groups.tokens = rw,
            "admin" => groups.admin = rw,
            _ => return Err(AuthError::Malformed),
        }
    }
    Ok(groups)
}

fn strings_from_json(value: &Value) -> Result<Vec<String>, AuthError> {
    value
        .as_array()
        .ok_or(AuthError::Malformed)?
        .iter()
        .map(|entry| {
            entry
                .as_str()
                .map(str::to_owned)
                .ok_or(AuthError::Malformed)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_root_and_default() {
        for scope in [Scope::root(), Scope::default()] {
            let decoded = scope_from_json(&scope_to_json(&scope)).unwrap();
            assert_eq!(decoded, scope);
        }
    }

    #[test]
    fn round_trips_full_scope() {
        let scope = Scope {
            streams: ResourceSet::any_of([
                ResourceMatcher::Prefix("/acct/".into()),
                ResourceMatcher::Exact("/one".into()),
            ]),
            tokens: ResourceSet::exact("svc/a"),
            groups: OperationGroups {
                stream: ReadWrite::read_only(),
                ..OperationGroups::default()
            },
            ops: [Operation::Trim, Operation::IssueToken].into(),
            audiences: [Audience::Pico, Audience::Admin].into(),
            auto_prefix_streams: true,
            expires_at_ms: Some(42),
        };
        let decoded = scope_from_json(&scope_to_json(&scope)).unwrap();
        assert_eq!(decoded, scope);
    }

    #[test]
    fn missing_keys_default_to_deny() {
        let scope = scope_from_json(&json!({})).unwrap();
        assert_eq!(scope, Scope::default());
    }

    #[test]
    fn unknown_keys_ops_and_matchers_rejected() {
        for bad in [
            json!({ "surprise": true }),
            json!({ "ops": ["fly"] }),
            json!({ "audiences": ["kafka"] }),
            json!({ "streams": [{ "regex": ".*" }] }),
            json!({ "streams": [{ "exact": "/a", "prefix": "/b" }] }),
            json!({ "groups": { "stream": { "read": 1 } } }),
            json!({ "expiresAtMs": "soon" }),
        ] {
            assert!(
                matches!(scope_from_json(&bad), Err(AuthError::Malformed)),
                "accepted: {bad}"
            );
        }
    }
}

//! Scope narrowing for token issuance.
//!
//! A child scope is accepted only when it is contained by the issuer. Requests
//! that would widen access are rejected. Nothing is silently clamped.
//!
//! Operation groups on the child require the same group flags on the parent.
//! Fine ops on the child may be satisfied by a parent group or an explicit op.
//! Fine ops on the parent never authorize a child group.

use crate::AuthError;
use crate::scope::{Operation, OperationGroups, ReadWrite, Scope};

impl Scope {
    /// True when `child` grants no access beyond `self`.
    pub fn contains(&self, child: &Scope) -> bool {
        self.streams.contains_set(&child.streams)
            && self.tokens.contains_set(&child.tokens)
            && contains_groups(&self.groups, &child.groups)
            && child.ops.iter().all(|op| self.allows_operation(*op))
            && child.audiences.iter().all(|a| self.allows_audience(*a))
            && contains_expiry(self.expires_at_ms, child.expires_at_ms)
            && contains_auto_prefix(self, child)
    }
}

fn contains_groups(parent: &OperationGroups, child: &OperationGroups) -> bool {
    contains_rw(parent.stream, child.stream)
        && contains_rw(parent.tokens, child.tokens)
        && contains_rw(parent.admin, child.admin)
}

fn contains_rw(parent: ReadWrite, child: ReadWrite) -> bool {
    (!child.read || parent.read) && (!child.write || parent.write)
}

fn contains_expiry(parent: Option<i64>, child: Option<i64>) -> bool {
    match (parent, child) {
        (None, _) => true,
        (Some(_), None) => false,
        (Some(p), Some(c)) => c <= p,
    }
}

fn contains_auto_prefix(_parent: &Scope, child: &Scope) -> bool {
    // Stream containment is checked separately. Auto-prefix only needs a sole
    // prefix matcher on the child.
    !child.auto_prefix_streams || child.streams.sole_prefix().is_some()
}

/// Validate an issue request from `issuer` for a new token with `new_id` and
/// `requested` scope.
pub fn check_issue(issuer: &Scope, new_id: &str, requested: &Scope) -> Result<(), AuthError> {
    if !issuer.allows_operation(Operation::IssueToken) {
        return Err(AuthError::Denied);
    }
    if !issuer.allows_token_id(new_id) {
        return Err(AuthError::NarrowingRejected);
    }
    if requested.auto_prefix_streams && requested.streams.sole_prefix().is_none() {
        return Err(AuthError::Malformed);
    }
    // A token with no ops or no audiences can never pass a request. Reject
    // instead of storing a dead credential.
    if requested.effective_ops().is_empty() {
        return Err(AuthError::Malformed);
    }
    if requested.audiences.is_empty() {
        return Err(AuthError::Malformed);
    }
    if !issuer.contains(requested) {
        return Err(AuthError::NarrowingRejected);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scope::{Audience, ReadWrite, ResourceSet};

    fn issuer_root() -> Scope {
        Scope {
            streams: ResourceSet::prefix(""),
            tokens: ResourceSet::prefix(""),
            groups: OperationGroups {
                stream: ReadWrite::all(),
                tokens: ReadWrite::all(),
                admin: ReadWrite::all(),
            },
            audiences: [Audience::Pico, Audience::DurableStreams, Audience::Admin].into(),
            ..Scope::default()
        }
    }

    fn reader_child(streams: ResourceSet) -> Scope {
        Scope {
            streams,
            ops: [Operation::Read].into(),
            audiences: [Audience::Pico].into(),
            ..Scope::default()
        }
    }

    #[test]
    fn narrower_resources_and_ops_ok() {
        let issuer = issuer_root();
        let child = reader_child(ResourceSet::prefix("/t/"));
        assert!(issuer.contains(&child));
        check_issue(&issuer, "t/reader", &child).unwrap();
    }

    #[test]
    fn wider_stream_prefix_rejected() {
        let mut issuer = issuer_root();
        issuer.streams = ResourceSet::prefix("/t/");
        let child = reader_child(ResourceSet::prefix(""));
        assert!(!issuer.contains(&child));
        assert!(matches!(
            check_issue(&issuer, "x", &child),
            Err(AuthError::NarrowingRejected)
        ));
    }

    #[test]
    fn parent_fine_ops_do_not_authorize_child_group() {
        let issuer = Scope {
            streams: ResourceSet::prefix(""),
            tokens: ResourceSet::prefix(""),
            ops: [
                Operation::Read,
                Operation::Head,
                Operation::List,
                Operation::IssueToken,
            ]
            .into(),
            audiences: [Audience::Pico].into(),
            ..Scope::default()
        };

        let child = Scope {
            streams: ResourceSet::prefix(""),
            groups: OperationGroups {
                stream: ReadWrite::read_only(),
                ..OperationGroups::default()
            },
            audiences: [Audience::Pico].into(),
            ..Scope::default()
        };

        assert!(!issuer.contains(&child));
    }

    #[test]
    fn parent_group_authorizes_child_fine_ops() {
        let issuer = Scope {
            streams: ResourceSet::prefix(""),
            tokens: ResourceSet::prefix(""),
            groups: OperationGroups {
                stream: ReadWrite::read_only(),
                tokens: ReadWrite::all(),
                admin: ReadWrite::none(),
            },
            audiences: [Audience::Pico].into(),
            ..Scope::default()
        };

        let child = reader_child(ResourceSet::prefix(""));

        assert!(issuer.contains(&child));
    }

    #[test]
    fn child_must_not_outlive_issuer() {
        let mut issuer = issuer_root();
        issuer.expires_at_ms = Some(1_000);
        let mut child = reader_child(ResourceSet::prefix("/t/"));
        child.expires_at_ms = None;
        assert!(!issuer.contains(&child));
        child.expires_at_ms = Some(2_000);
        assert!(!issuer.contains(&child));
        child.expires_at_ms = Some(500);
        assert!(issuer.contains(&child));
    }

    #[test]
    fn new_id_must_match_issuer_token_matcher() {
        let mut issuer = issuer_root();
        issuer.tokens = ResourceSet::prefix("svc/");
        let child = reader_child(ResourceSet::prefix("/t/"));
        assert!(matches!(
            check_issue(&issuer, "other", &child),
            Err(AuthError::NarrowingRejected)
        ));
        check_issue(&issuer, "svc/a", &child).unwrap();
    }

    #[test]
    fn issue_requires_issue_token_op() {
        let mut issuer = issuer_root();
        issuer.groups.tokens = ReadWrite::read_only();
        let child = reader_child(ResourceSet::prefix("/t/"));
        assert!(matches!(
            check_issue(&issuer, "x", &child),
            Err(AuthError::Denied)
        ));
    }

    #[test]
    fn issue_rejects_dead_scopes() {
        let issuer = issuer_root();
        let mut no_ops = reader_child(ResourceSet::prefix("/t/"));
        no_ops.ops.clear();
        assert!(matches!(
            check_issue(&issuer, "x", &no_ops),
            Err(AuthError::Malformed)
        ));
        let mut no_audience = reader_child(ResourceSet::prefix("/t/"));
        no_audience.audiences.clear();
        assert!(matches!(
            check_issue(&issuer, "x", &no_audience),
            Err(AuthError::Malformed)
        ));
    }
}

//! Scope narrowing for token issuance.
//!
//! A child scope is accepted only when it is contained by the issuer. Requests
//! that would widen access are rejected. Nothing is silently clamped.
//!
//! Access flags on the child require the same flags on the parent. Fine ops
//! on the child may be satisfied by a parent flag or an explicit op. Fine ops
//! on the parent never authorize a child flag.

use crate::AuthError;
use crate::scope::{Access, Operation, ReadWrite, Scope};

impl Scope {
    pub fn contains(&self, child: &Scope) -> bool {
        self.contains_streams(&child.streams)
            && self.tokens.contains_set(&child.tokens)
            && self.groups.contains_set(&child.groups)
            && contains_access(&self.access, &child.access)
            && child.ops.iter().all(|op| self.allows_operation(*op))
            && child.audiences.iter().all(|a| self.allows_audience(*a))
            && contains_expiry(self.expires_at_ms, child.expires_at_ms)
            && (!child.auto_prefix || prefixed(child))
    }
}

fn contains_access(parent: &Access, child: &Access) -> bool {
    contains_rw(parent.stream, child.stream)
        && contains_rw(parent.tokens, child.tokens)
        && contains_rw(parent.group, child.group)
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

fn prefixed(scope: &Scope) -> bool {
    scope.streams.sole_prefix().is_some() && scope.groups.sole_prefix().is_some()
}

pub fn check_issue(issuer: &Scope, new_id: &str, requested: &Scope) -> Result<(), AuthError> {
    if !issuer.allows_operation(Operation::IssueToken) {
        return Err(AuthError::Denied);
    }
    if !issuer.tokens.matches(new_id) {
        return Err(AuthError::NarrowingRejected);
    }
    if requested.auto_prefix && !prefixed(requested) {
        return Err(AuthError::Malformed);
    }
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
            groups: ResourceSet::prefix(""),
            access: Access::all(),
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
    fn stream_narrowing_compares_canonical_names() {
        let mut issuer = issuer_root();
        issuer.streams = ResourceSet::prefix("/t/");

        for allowed in [ResourceSet::exact("/t.x"), ResourceSet::prefix("t.x/")] {
            assert!(issuer.contains(&reader_child(allowed)));
        }
        assert!(!issuer.contains(&reader_child(ResourceSet::exact("/u.x"))));
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
    fn parent_fine_ops_do_not_authorize_child_access() {
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
            access: Access {
                stream: ReadWrite::read_only(),
                ..Access::default()
            },
            audiences: [Audience::Pico].into(),
            ..Scope::default()
        };

        assert!(!issuer.contains(&child));
    }

    #[test]
    fn parent_access_authorizes_child_fine_ops() {
        let issuer = Scope {
            streams: ResourceSet::prefix(""),
            tokens: ResourceSet::prefix(""),
            access: Access {
                stream: ReadWrite::read_only(),
                tokens: ReadWrite::all(),
                ..Access::default()
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
        issuer.access.tokens = ReadWrite::read_only();
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

    #[test]
    fn wider_groups_rejected() {
        let mut issuer = issuer_root();
        issuer.groups = ResourceSet::prefix("billing");
        let consumer = |groups| Scope {
            groups,
            ops: [Operation::Consume].into(),
            audiences: [Audience::Pico].into(),
            ..Scope::default()
        };

        check_issue(&issuer, "x", &consumer(ResourceSet::exact("billing-eu"))).unwrap();
        assert!(matches!(
            check_issue(&issuer, "x", &consumer(ResourceSet::prefix(""))),
            Err(AuthError::NarrowingRejected)
        ));
    }

    #[test]
    fn auto_prefix_needs_a_sole_prefix_for_streams_and_groups() {
        let issuer = issuer_root();
        let mut tenant = reader_child(ResourceSet::prefix("/t/"));
        tenant.auto_prefix = true;
        assert!(matches!(
            check_issue(&issuer, "x", &tenant),
            Err(AuthError::Malformed)
        ));

        tenant.groups = ResourceSet::prefix("t/");
        check_issue(&issuer, "x", &tenant).unwrap();
    }
}

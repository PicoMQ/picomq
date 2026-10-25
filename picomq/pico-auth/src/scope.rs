//! Authorization scope: resources, operations, audience, and path prefixing.

use std::collections::BTreeSet;

use picomq_common::name::{canonical, canonical_eq, canonical_starts_with, strip_canonical_prefix};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Operation {
    Read,
    Head,
    List,
    Create,
    Append,
    Trim,
    Close,
    Delete,
    IssueToken,
    RevokeToken,
    ListTokens,
    Consume,
    DescribeGroup,
    DeleteGroup,
    ClusterRead,
    NodeRead,
    StreamInspect,
    TransferStream,
    UpdateNodeSlots,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Resource {
    Stream,
    Token,
    Group,
}

impl Operation {
    pub const ALL: [Operation; 19] = [
        Operation::Read,
        Operation::Head,
        Operation::List,
        Operation::Create,
        Operation::Append,
        Operation::Trim,
        Operation::Close,
        Operation::Delete,
        Operation::IssueToken,
        Operation::RevokeToken,
        Operation::ListTokens,
        Operation::Consume,
        Operation::DescribeGroup,
        Operation::DeleteGroup,
        Operation::ClusterRead,
        Operation::NodeRead,
        Operation::StreamInspect,
        Operation::TransferStream,
        Operation::UpdateNodeSlots,
    ];

    pub fn resource(self) -> Option<Resource> {
        match self {
            Self::Read
            | Self::Head
            | Self::List
            | Self::Create
            | Self::Append
            | Self::Trim
            | Self::Close
            | Self::Delete => Some(Resource::Stream),
            Self::IssueToken | Self::RevokeToken | Self::ListTokens => Some(Resource::Token),
            Self::Consume | Self::DescribeGroup | Self::DeleteGroup => Some(Resource::Group),
            Self::ClusterRead
            | Self::NodeRead
            | Self::StreamInspect
            | Self::TransferStream
            | Self::UpdateNodeSlots => None,
        }
    }

    pub fn writes(self) -> bool {
        matches!(
            self,
            Self::Create
                | Self::Append
                | Self::Trim
                | Self::Close
                | Self::Delete
                | Self::IssueToken
                | Self::RevokeToken
                | Self::DeleteGroup
                | Self::TransferStream
                | Self::UpdateNodeSlots
        )
    }

    pub fn describes(self) -> bool {
        matches!(self, Self::Head | Self::DescribeGroup)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ReadWrite {
    pub read: bool,
    pub write: bool,
}

impl ReadWrite {
    pub const fn none() -> Self {
        Self {
            read: false,
            write: false,
        }
    }

    pub const fn read_only() -> Self {
        Self {
            read: true,
            write: false,
        }
    }

    pub const fn all() -> Self {
        Self {
            read: true,
            write: true,
        }
    }

    pub fn allows(self, write: bool) -> bool {
        if write { self.write } else { self.read }
    }

    pub fn any(self) -> bool {
        self.read || self.write
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Access {
    pub stream: ReadWrite,
    pub tokens: ReadWrite,
    pub group: ReadWrite,
    pub admin: ReadWrite,
}

impl Access {
    pub const fn all() -> Self {
        Self {
            stream: ReadWrite::all(),
            tokens: ReadWrite::all(),
            group: ReadWrite::all(),
            admin: ReadWrite::all(),
        }
    }

    pub fn of(&self, resource: Option<Resource>) -> ReadWrite {
        match resource {
            Some(Resource::Stream) => self.stream,
            Some(Resource::Token) => self.tokens,
            Some(Resource::Group) => self.group,
            None => self.admin,
        }
    }
}

/// Which listener or protocol a token may call.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Audience {
    Pico,
    DurableStreams,
    Admin,
    Kafka,
}

/// Match a single resource name (stream path or token id).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResourceMatcher {
    /// Exact string equality.
    Exact(String),
    /// Name starts with this prefix. Empty prefix matches every name.
    Prefix(String),
}

impl ResourceMatcher {
    pub fn matches(&self, name: &str) -> bool {
        match self {
            Self::Exact(exact) => name == exact,
            Self::Prefix(prefix) => name.starts_with(prefix),
        }
    }

    /// True when every name matched by `child` is also matched by `self`.
    pub fn contains_matcher(&self, child: &ResourceMatcher) -> bool {
        match (self, child) {
            (Self::Exact(a), Self::Exact(b)) => a == b,
            (Self::Exact(_), Self::Prefix(_)) => false,
            (Self::Prefix(p), Self::Exact(e)) => e.starts_with(p),
            (Self::Prefix(p), Self::Prefix(c)) => c.starts_with(p),
        }
    }
}

/// Set of resource matchers. Empty set matches nothing.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ResourceSet {
    matchers: Vec<ResourceMatcher>,
}

impl ResourceSet {
    pub fn empty() -> Self {
        Self {
            matchers: Vec::new(),
        }
    }

    pub fn exact(name: impl Into<String>) -> Self {
        Self {
            matchers: vec![ResourceMatcher::Exact(name.into())],
        }
    }

    pub fn prefix(prefix: impl Into<String>) -> Self {
        Self {
            matchers: vec![ResourceMatcher::Prefix(prefix.into())],
        }
    }

    pub fn any_of(matchers: impl IntoIterator<Item = ResourceMatcher>) -> Self {
        Self {
            matchers: matchers.into_iter().collect(),
        }
    }

    pub fn matchers(&self) -> &[ResourceMatcher] {
        &self.matchers
    }

    pub fn matches(&self, name: &str) -> bool {
        self.matchers.iter().any(|m| m.matches(name))
    }

    /// True when every name allowed by `child` is allowed by `self`.
    pub fn contains_set(&self, child: &ResourceSet) -> bool {
        if child.matchers.is_empty() {
            return true;
        }
        if self.matchers.is_empty() {
            return false;
        }
        child
            .matchers
            .iter()
            .all(|c| self.matchers.iter().any(|p| p.contains_matcher(c)))
    }

    /// Sole prefix matcher, if this set is exactly one [`ResourceMatcher::Prefix`].
    pub fn sole_prefix(&self) -> Option<&str> {
        match self.matchers.as_slice() {
            [ResourceMatcher::Prefix(p)] => Some(p.as_str()),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Scope {
    pub streams: ResourceSet,
    pub tokens: ResourceSet,
    pub groups: ResourceSet,
    pub access: Access,
    pub ops: BTreeSet<Operation>,
    pub audiences: BTreeSet<Audience>,
    pub auto_prefix: bool,
    pub expires_at_ms: Option<i64>,
}

impl Default for Scope {
    fn default() -> Self {
        Self {
            streams: ResourceSet::empty(),
            tokens: ResourceSet::empty(),
            groups: ResourceSet::empty(),
            access: Access::default(),
            ops: BTreeSet::new(),
            audiences: BTreeSet::new(),
            auto_prefix: false,
            expires_at_ms: None,
        }
    }
}

impl Scope {
    pub fn root() -> Self {
        Self {
            streams: ResourceSet::prefix(""),
            tokens: ResourceSet::prefix(""),
            groups: ResourceSet::prefix(""),
            access: Access::all(),
            audiences: [
                Audience::Pico,
                Audience::DurableStreams,
                Audience::Admin,
                Audience::Kafka,
            ]
            .into(),
            ..Self::default()
        }
    }

    pub fn allows_audience(&self, audience: Audience) -> bool {
        self.audiences.contains(&audience)
    }

    pub fn is_expired(&self, now_ms: i64) -> bool {
        self.expires_at_ms.is_some_and(|exp| now_ms >= exp)
    }

    pub fn allows_operation(&self, op: Operation) -> bool {
        if self.ops.contains(&op) || self.access.of(op.resource()).allows(op.writes()) {
            return true;
        }

        op.describes() && self.any_right_on(op.resource())
    }

    fn any_right_on(&self, resource: Option<Resource>) -> bool {
        self.access.of(resource).any() || self.ops.iter().any(|op| op.resource() == resource)
    }

    pub fn allows(&self, op: Operation, name: Option<&str>) -> bool {
        if !self.allows_operation(op) {
            return false;
        }

        match op.resource() {
            Some(Resource::Stream) => name.is_some_and(|name| self.allows_stream(name)),
            Some(Resource::Token) => name.is_some_and(|name| self.tokens.matches(name)),
            Some(Resource::Group) => name.is_some_and(|name| self.groups.matches(name)),
            None => true,
        }
    }

    pub fn effective_ops(&self) -> BTreeSet<Operation> {
        Operation::ALL
            .into_iter()
            .filter(|op| self.allows_operation(*op))
            .collect()
    }

    pub fn allows_stream(&self, name: &str) -> bool {
        self.streams.matchers().iter().any(|matcher| match matcher {
            ResourceMatcher::Exact(exact) => canonical_eq(exact, name),
            ResourceMatcher::Prefix(prefix) => canonical_starts_with(name, prefix),
        })
    }

    pub fn contains_streams(&self, child: &ResourceSet) -> bool {
        let canonical_set = |set: &ResourceSet| {
            ResourceSet::any_of(set.matchers().iter().map(|matcher| match matcher {
                ResourceMatcher::Exact(exact) => ResourceMatcher::Exact(canonical(exact)),
                ResourceMatcher::Prefix(prefix) => ResourceMatcher::Prefix(canonical(prefix)),
            }))
        };

        canonical_set(&self.streams).contains_set(&canonical_set(child))
    }

    fn set(&self, resource: Resource) -> &ResourceSet {
        match resource {
            Resource::Stream => &self.streams,
            Resource::Token => &self.tokens,
            Resource::Group => &self.groups,
        }
    }

    pub fn resolve(
        &self,
        resource: Resource,
        client_name: &str,
    ) -> Result<String, crate::AuthError> {
        if !self.auto_prefix {
            return Ok(client_name.to_owned());
        }
        let Some(prefix) = self.set(resource).sole_prefix() else {
            return Err(crate::AuthError::Denied);
        };

        let name = client_name.trim_start_matches('/');
        if prefix.ends_with('/') {
            Ok(format!("{prefix}{name}"))
        } else if name.is_empty() {
            Ok(prefix.to_owned())
        } else {
            Ok(format!("{prefix}/{name}"))
        }
    }

    pub fn strip<'a>(&self, resource: Resource, stored_name: &'a str) -> &'a str {
        if !self.auto_prefix {
            return stored_name;
        }
        let Some(prefix) = self.set(resource).sole_prefix() else {
            return stored_name;
        };

        let stripped = match resource {
            Resource::Stream => strip_canonical_prefix(stored_name, prefix),
            Resource::Token | Resource::Group => stored_name.strip_prefix(prefix),
        };
        stripped.unwrap_or(stored_name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resource_prefix_and_exact() {
        assert!(ResourceMatcher::Prefix("/a/".into()).matches("/a/b"));
        assert!(!ResourceMatcher::Prefix("/a/".into()).matches("/b"));
        assert!(ResourceMatcher::Prefix(String::new()).matches("anything"));
        assert!(ResourceMatcher::Exact("/x".into()).matches("/x"));
        assert!(!ResourceMatcher::Exact("/x".into()).matches("/x/y"));
    }

    #[test]
    fn resource_set_contains() {
        let parent = ResourceSet::prefix("/t/");
        let child = ResourceSet::exact("/t/orders");
        assert!(parent.contains_set(&child));
        assert!(!child.contains_set(&parent));
        assert!(parent.contains_set(&ResourceSet::empty()));
        assert!(!ResourceSet::empty().contains_set(&child));
    }

    #[test]
    fn access_expands_to_ops() {
        let mut scope = Scope::default();
        scope.access.stream = ReadWrite::read_only();
        assert!(scope.allows_operation(Operation::Read));
        assert!(scope.allows_operation(Operation::List));
        assert!(!scope.allows_operation(Operation::Append));

        scope.ops.insert(Operation::Append);
        assert!(scope.allows_operation(Operation::Append));
    }

    #[test]
    fn any_stream_right_grants_head() {
        let mut scope = Scope::default();
        scope.access.stream = ReadWrite {
            read: false,
            write: true,
        };
        assert!(scope.allows_operation(Operation::Head));
        assert!(!scope.allows_operation(Operation::Read));
        assert!(!scope.allows_operation(Operation::List));

        let append_only = Scope {
            ops: [Operation::Append].into(),
            ..Scope::default()
        };
        assert!(append_only.allows_operation(Operation::Head));
        assert!(!append_only.allows_operation(Operation::Read));

        let tokens_only = Scope {
            ops: [Operation::ListTokens].into(),
            ..Scope::default()
        };
        assert!(!tokens_only.allows_operation(Operation::Head));
    }

    #[test]
    fn group_operations_need_a_matching_group() {
        let scope = Scope {
            groups: ResourceSet::prefix("billing"),
            access: Access {
                group: ReadWrite::read_only(),
                ..Access::default()
            },
            ..Scope::default()
        };

        assert!(scope.allows(Operation::Consume, Some("billing-eu")));
        assert!(scope.allows(Operation::DescribeGroup, Some("billing-eu")));
        assert!(!scope.allows(Operation::Consume, Some("analytics")));
        assert!(!scope.allows(Operation::Consume, None));
        assert!(!scope.allows(Operation::DeleteGroup, Some("billing-eu")));
    }

    #[test]
    fn any_group_right_grants_describe() {
        let deleter = Scope {
            ops: [Operation::DeleteGroup].into(),
            ..Scope::default()
        };
        assert!(deleter.allows_operation(Operation::DescribeGroup));
        assert!(!deleter.allows_operation(Operation::Consume));

        let writer = Scope {
            access: Access {
                group: ReadWrite {
                    read: false,
                    write: true,
                },
                ..Access::default()
            },
            ..Scope::default()
        };
        assert!(writer.allows_operation(Operation::DescribeGroup));

        let reader = Scope {
            access: Access {
                stream: ReadWrite::read_only(),
                ..Access::default()
            },
            ..Scope::default()
        };
        assert!(!reader.allows_operation(Operation::DescribeGroup));
    }

    #[test]
    fn admin_operations_take_no_resource() {
        let scope = Scope {
            access: Access {
                admin: ReadWrite::read_only(),
                ..Access::default()
            },
            ..Scope::default()
        };

        assert!(scope.allows(Operation::ClusterRead, None));
        assert!(!scope.allows(Operation::TransferStream, None));
    }

    #[test]
    fn auto_prefix_resolve_and_strip() {
        let scope = Scope {
            streams: ResourceSet::prefix("/acct/"),
            groups: ResourceSet::prefix("acct/"),
            auto_prefix: true,
            ..Scope::default()
        };

        assert_eq!(
            scope.resolve(Resource::Stream, "orders").unwrap(),
            "/acct/orders"
        );
        assert_eq!(scope.strip(Resource::Stream, "/acct/orders"), "orders");
        assert_eq!(
            scope.resolve(Resource::Group, "workers").unwrap(),
            "acct/workers"
        );
        assert_eq!(scope.strip(Resource::Group, "acct/workers"), "workers");
    }

    #[test]
    fn auto_prefixed_names_are_always_relative() {
        let scope = Scope {
            streams: ResourceSet::prefix("/acct/"),
            groups: ResourceSet::prefix("acct/"),
            auto_prefix: true,
            ..Scope::default()
        };

        assert_eq!(
            scope.resolve(Resource::Stream, "/acct/orders").unwrap(),
            "/acct/acct/orders"
        );
        assert_eq!(
            scope.resolve(Resource::Group, "acct/workers").unwrap(),
            "acct/acct/workers"
        );
    }

    #[test]
    fn stream_scopes_match_either_spelling() {
        let scope = Scope {
            streams: ResourceSet::prefix("/acct/"),
            auto_prefix: true,
            ..Scope::default()
        };
        assert!(scope.allows_stream("/acct/orders"));
        assert!(scope.allows_stream("/acct.orders"));
        assert!(!scope.allows_stream("/other/orders"));
        assert_eq!(scope.strip(Resource::Stream, "/acct.orders"), "orders");

        let exact = Scope {
            streams: ResourceSet::exact("/a/b"),
            ..Scope::default()
        };
        assert!(exact.allows_stream("/a.b"));
    }

    #[test]
    fn group_names_match_exactly() {
        let scope = Scope {
            groups: ResourceSet::prefix("acct/"),
            access: Access {
                group: ReadWrite::read_only(),
                ..Access::default()
            },
            ..Scope::default()
        };

        assert!(scope.allows(Operation::Consume, Some("acct/workers")));
        assert!(!scope.allows(Operation::Consume, Some("acct.workers")));
    }

    #[test]
    fn auto_prefix_requires_sole_prefix() {
        let scope = Scope {
            streams: ResourceSet::any_of([
                ResourceMatcher::Prefix("/a/".into()),
                ResourceMatcher::Prefix("/b/".into()),
            ]),
            auto_prefix: true,
            ..Scope::default()
        };
        assert!(matches!(
            scope.resolve(Resource::Stream, "x"),
            Err(crate::AuthError::Denied)
        ));
        assert!(matches!(
            scope.resolve(Resource::Group, "x"),
            Err(crate::AuthError::Denied)
        ));
    }

    #[test]
    fn default_scope_denies_all() {
        let scope = Scope::default();
        assert!(!scope.allows_operation(Operation::Read));
        assert!(!scope.allows_operation(Operation::DescribeGroup));
        assert!(!scope.allows_stream("/x"));
        assert!(!scope.allows_audience(Audience::Pico));
        assert!(!scope.is_expired(0));
    }
}

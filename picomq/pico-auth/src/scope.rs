//! Authorization scope: resources, operations, audience, and path prefixing.

use std::collections::BTreeSet;

/// Fine-grained operations. Hosts map HTTP methods onto these.
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
    ClusterRead,
    NodeRead,
    StreamInspect,
    TransferStream,
    UpdateNodeSlots,
}

impl Operation {
    /// Every operation. Keep in declaration order when adding variants.
    pub const ALL: [Operation; 16] = [
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
        Operation::ClusterRead,
        Operation::NodeRead,
        Operation::StreamInspect,
        Operation::TransferStream,
        Operation::UpdateNodeSlots,
    ];

    pub fn is_stream_read(self) -> bool {
        matches!(self, Self::Read | Self::Head | Self::List)
    }

    pub fn is_stream_write(self) -> bool {
        matches!(
            self,
            Self::Create | Self::Append | Self::Trim | Self::Close | Self::Delete
        )
    }

    pub fn is_tokens_read(self) -> bool {
        matches!(self, Self::ListTokens)
    }

    pub fn is_tokens_write(self) -> bool {
        matches!(self, Self::IssueToken | Self::RevokeToken)
    }

    pub fn is_admin_read(self) -> bool {
        matches!(
            self,
            Self::ClusterRead | Self::NodeRead | Self::StreamInspect
        )
    }

    pub fn is_admin_write(self) -> bool {
        matches!(self, Self::TransferStream | Self::UpdateNodeSlots)
    }
}

/// Read/write flags for one operation group.
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
}

/// Coarse permissions. Defaults are all false (deny).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct OperationGroups {
    pub stream: ReadWrite,
    pub tokens: ReadWrite,
    pub admin: ReadWrite,
}

/// Which listener or protocol a token may call.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Audience {
    Pico,
    DurableStreams,
    Admin,
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

/// Full token scope. Defaults deny all resources, ops, and audiences.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Scope {
    pub streams: ResourceSet,
    pub tokens: ResourceSet,
    pub groups: OperationGroups,
    pub ops: BTreeSet<Operation>,
    pub audiences: BTreeSet<Audience>,
    /// When true, `streams` must be a single prefix. Client stream names are
    /// relative to that prefix.
    pub auto_prefix_streams: bool,
    /// Absolute expiry in unix milliseconds. `None` means no expiry.
    pub expires_at_ms: Option<i64>,
}

impl Default for Scope {
    fn default() -> Self {
        Self {
            streams: ResourceSet::empty(),
            tokens: ResourceSet::empty(),
            groups: OperationGroups::default(),
            ops: BTreeSet::new(),
            audiences: BTreeSet::new(),
            auto_prefix_streams: false,
            expires_at_ms: None,
        }
    }
}

impl Scope {
    /// Every stream, token, operation, and audience. No expiry.
    pub fn root() -> Self {
        Self {
            streams: ResourceSet::prefix(""),
            tokens: ResourceSet::prefix(""),
            groups: OperationGroups {
                stream: ReadWrite::all(),
                tokens: ReadWrite::all(),
                admin: ReadWrite::all(),
            },
            audiences: [Audience::Pico, Audience::DurableStreams, Audience::Admin].into(),
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
        if self.ops.contains(&op) {
            return true;
        }
        let g = &self.groups;
        match op {
            o if o.is_stream_read() => g.stream.read,
            o if o.is_stream_write() => g.stream.write,
            o if o.is_tokens_read() => g.tokens.read,
            o if o.is_tokens_write() => g.tokens.write,
            o if o.is_admin_read() => g.admin.read,
            o if o.is_admin_write() => g.admin.write,
            _ => false,
        }
    }

    /// Effective operation set (groups expanded union explicit ops).
    pub fn effective_ops(&self) -> BTreeSet<Operation> {
        Operation::ALL
            .into_iter()
            .filter(|op| self.allows_operation(*op))
            .collect()
    }

    pub fn allows_stream(&self, name: &str) -> bool {
        self.streams.matches(name)
    }

    pub fn allows_token_id(&self, id: &str) -> bool {
        self.tokens.matches(id)
    }

    /// Map a client-facing stream name to the stored name.
    ///
    /// With auto-prefix, the scope must be a single stream prefix. Relative
    /// names are joined to it. Absolute names that already start with the
    /// prefix are accepted as-is.
    pub fn resolve_stream_name(&self, client_name: &str) -> Result<String, crate::AuthError> {
        if !self.auto_prefix_streams {
            return Ok(client_name.to_owned());
        }
        let Some(prefix) = self.streams.sole_prefix() else {
            return Err(crate::AuthError::Denied);
        };
        if client_name.starts_with(prefix) {
            return Ok(client_name.to_owned());
        }
        let name = client_name.trim_start_matches('/');
        if prefix.ends_with('/') {
            Ok(format!("{prefix}{name}"))
        } else if name.is_empty() {
            Ok(prefix.to_owned())
        } else {
            Ok(format!("{prefix}/{name}"))
        }
    }

    /// Strip the auto-prefix for responses that echo stream names.
    pub fn strip_stream_name<'a>(&self, stored_name: &'a str) -> &'a str {
        if !self.auto_prefix_streams {
            return stored_name;
        }
        let Some(prefix) = self.streams.sole_prefix() else {
            return stored_name;
        };
        stored_name.strip_prefix(prefix).unwrap_or(stored_name)
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
    fn groups_expand_to_ops() {
        let mut scope = Scope::default();
        scope.groups.stream = ReadWrite::read_only();
        assert!(scope.allows_operation(Operation::Read));
        assert!(scope.allows_operation(Operation::List));
        assert!(!scope.allows_operation(Operation::Append));
        scope.ops.insert(Operation::Append);
        assert!(scope.allows_operation(Operation::Append));
    }

    #[test]
    fn auto_prefix_resolve_and_strip() {
        let scope = Scope {
            streams: ResourceSet::prefix("/acct/"),
            auto_prefix_streams: true,
            ..Scope::default()
        };
        assert_eq!(scope.resolve_stream_name("orders").unwrap(), "/acct/orders");
        assert_eq!(
            scope.resolve_stream_name("/acct/orders").unwrap(),
            "/acct/orders"
        );
        assert_eq!(scope.strip_stream_name("/acct/orders"), "orders");
    }

    #[test]
    fn auto_prefix_requires_sole_prefix() {
        let scope = Scope {
            streams: ResourceSet::any_of([
                ResourceMatcher::Prefix("/a/".into()),
                ResourceMatcher::Prefix("/b/".into()),
            ]),
            auto_prefix_streams: true,
            ..Scope::default()
        };
        assert!(matches!(
            scope.resolve_stream_name("x"),
            Err(crate::AuthError::Denied)
        ));
    }

    #[test]
    fn default_scope_denies_all() {
        let scope = Scope::default();
        assert!(!scope.allows_operation(Operation::Read));
        assert!(!scope.allows_stream("/x"));
        assert!(!scope.allows_audience(Audience::Pico));
        assert!(!scope.is_expired(0));
    }
}

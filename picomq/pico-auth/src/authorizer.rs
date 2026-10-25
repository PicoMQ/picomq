//! Request authentication and authorization over a [`TokenStore`].
//!
//! The hot path loads a record, verifies the secret, checks audience and
//! expiry, then tests the operation and resource against the scope. A small
//! per-id cache reuses an `Arc` when the freshly loaded record is equal to the
//! cached one, so scope data is not cloned on every hit. Equality is the
//! invalidation signal: a revoke or replace yields a different or missing
//! record and the cache entry is dropped.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::AuthError;
use crate::record::TokenRecord;
use crate::scope::{Audience, Operation};
use crate::store::TokenStore;
use crate::token::AccessToken;

/// Authenticated identity: the stored record after secret verification.
pub type AuthPrincipal = Arc<TokenRecord>;

/// Reserved id whose scope applies to requests with no credential.
pub const ANONYMOUS_TOKEN_ID: &str = "anonymous";

/// Authorizer over a pluggable [`TokenStore`].
pub struct Authorizer {
    store: Arc<dyn TokenStore>,
    cache: Mutex<HashMap<String, AuthPrincipal>>,
}

impl std::fmt::Debug for Authorizer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Authorizer").finish_non_exhaustive()
    }
}

impl Authorizer {
    pub fn new(store: Arc<dyn TokenStore>) -> Self {
        Self {
            store,
            cache: Mutex::new(HashMap::new()),
        }
    }

    /// Parse a bearer credential, load the record, verify the secret, and
    /// check audience plus expiry.
    ///
    /// `credential` may be the raw wire token or an `Authorization` value with
    /// a `Bearer ` prefix. Unknown ids and bad secrets both return
    /// [`AuthError::Unauthenticated`].
    pub async fn authenticate(
        &self,
        credential: &str,
        audience: Audience,
        now_ms: i64,
    ) -> Result<AuthPrincipal, AuthError> {
        let wire = strip_bearer(credential);
        let access = AccessToken::parse(wire)?;

        let fresh = match self.store.get(&access.id).await? {
            Some(record) => record,
            None => {
                self.drop_cached(&access.id);
                return Err(AuthError::Unauthenticated);
            }
        };

        if !fresh.verifier.matches_secret(&access.secret) {
            return Err(AuthError::Unauthenticated);
        }
        if fresh.scope.is_expired(now_ms) {
            return Err(AuthError::Expired);
        }
        if !fresh.scope.allows_audience(audience) {
            return Err(AuthError::WrongAudience);
        }
        if fresh.id != access.id {
            // Defensive: stored id must match the credential id.
            return Err(AuthError::Unauthenticated);
        }

        Ok(self.intern(fresh))
    }

    pub async fn refresh(
        &self,
        principal: &AuthPrincipal,
        now_ms: i64,
    ) -> Result<AuthPrincipal, AuthError> {
        let fresh = match self.store.get(&principal.id).await? {
            Some(record) => record,
            None => {
                self.drop_cached(&principal.id);
                return Err(AuthError::Unauthenticated);
            }
        };

        if fresh.verifier != principal.verifier {
            return Err(AuthError::Unauthenticated);
        }
        if fresh.scope.is_expired(now_ms) {
            return Err(AuthError::Expired);
        }

        Ok(self.intern(fresh))
    }

    /// The principal for a request with no credential: the stored
    /// [`ANONYMOUS_TOKEN_ID`] record. Every miss is
    /// [`AuthError::Unauthenticated`], the remedy is always a credential.
    pub async fn authenticate_anonymous(
        &self,
        audience: Audience,
        now_ms: i64,
    ) -> Result<AuthPrincipal, AuthError> {
        let fresh = match self.store.get(ANONYMOUS_TOKEN_ID).await? {
            Some(record) => record,
            None => {
                self.drop_cached(ANONYMOUS_TOKEN_ID);
                return Err(AuthError::Unauthenticated);
            }
        };
        if fresh.scope.is_expired(now_ms) || !fresh.scope.allows_audience(audience) {
            return Err(AuthError::Unauthenticated);
        }
        Ok(self.intern(fresh))
    }

    pub fn authorize(
        &self,
        principal: &TokenRecord,
        op: Operation,
        resource: Option<&str>,
    ) -> Result<(), AuthError> {
        if principal.scope.allows(op, resource) {
            Ok(())
        } else {
            Err(AuthError::Denied)
        }
    }

    /// Drop a cached entry. Hosts may call this after a local revoke.
    pub fn invalidate(&self, id: &str) {
        self.drop_cached(id);
    }

    fn intern(&self, fresh: TokenRecord) -> AuthPrincipal {
        let mut cache = self.cache.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(cached) = cache.get(&fresh.id)
            && cached.as_ref() == &fresh
        {
            return Arc::clone(cached);
        }
        let arc = Arc::new(fresh);
        cache.insert(arc.id.clone(), Arc::clone(&arc));
        arc
    }

    fn drop_cached(&self, id: &str) {
        let mut cache = self.cache.lock().unwrap_or_else(|e| e.into_inner());
        cache.remove(id);
    }
}

fn strip_bearer(raw: &str) -> &str {
    // The auth scheme name is case-insensitive per RFC 7235.
    let trimmed = raw.trim();
    match trimmed.split_once(' ') {
        Some((scheme, rest)) if scheme.eq_ignore_ascii_case("bearer") => rest.trim(),
        _ => trimmed,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scope::{Access, ReadWrite, Resource, ResourceSet, Scope};
    use crate::store::MemoryTokenStore;
    use crate::token::{AccessToken, Verifier};

    fn principal_record(id: &str, verifier: Verifier, mut scope: Scope) -> TokenRecord {
        if scope.audiences.is_empty() {
            scope.audiences.insert(Audience::Pico);
        }
        TokenRecord {
            id: id.into(),
            verifier,
            scope,
            created_at_ms: 1,
            issued_by: String::new(),
        }
    }

    async fn seeded() -> (Authorizer, String, AccessToken) {
        let store = Arc::new(MemoryTokenStore::new());
        let (token, verifier) = AccessToken::issue("svc/reader").unwrap();
        let scope = Scope {
            streams: ResourceSet::prefix("/acct/"),
            tokens: ResourceSet::prefix("svc/"),
            groups: ResourceSet::prefix("acct/"),
            access: Access {
                stream: ReadWrite::read_only(),
                group: ReadWrite::read_only(),
                ..Access::default()
            },
            audiences: [Audience::Pico].into(),
            auto_prefix: true,
            ..Scope::default()
        };
        store
            .put_if_absent(principal_record(&token.id, verifier, scope))
            .await
            .unwrap();
        let auth = Authorizer::new(store);
        let wire = token.render();
        (auth, wire, token)
    }

    #[tokio::test]
    async fn authenticate_accepts_valid_bearer() {
        let (auth, wire, token) = seeded().await;
        let principal = auth
            .authenticate(&format!("Bearer {wire}"), Audience::Pico, 0)
            .await
            .unwrap();
        assert_eq!(principal.id, token.id);
    }

    #[tokio::test]
    async fn bad_secret_is_unauthenticated() {
        let (auth, _, token) = seeded().await;
        let (other, _) = AccessToken::issue(&token.id).unwrap();
        assert!(matches!(
            auth.authenticate(&other.render(), Audience::Pico, 0).await,
            Err(AuthError::Unauthenticated)
        ));
    }

    #[tokio::test]
    async fn wrong_audience_and_expiry() {
        let store = Arc::new(MemoryTokenStore::new());
        let (token, verifier) = AccessToken::issue("a").unwrap();
        let mut scope = Scope::default();
        scope.audiences.insert(Audience::Pico);
        scope.expires_at_ms = Some(100);
        scope.access.stream = ReadWrite::read_only();
        scope.streams = ResourceSet::prefix("");
        store
            .put_if_absent(principal_record(&token.id, verifier, scope))
            .await
            .unwrap();
        let auth = Authorizer::new(store);
        let wire = token.render();

        assert!(matches!(
            auth.authenticate(&wire, Audience::Admin, 0).await,
            Err(AuthError::WrongAudience)
        ));
        assert!(matches!(
            auth.authenticate(&wire, Audience::Pico, 100).await,
            Err(AuthError::Expired)
        ));
        auth.authenticate(&wire, Audience::Pico, 99).await.unwrap();
    }

    #[tokio::test]
    async fn authorize_stream_and_token_resources() {
        let (auth, wire, _) = seeded().await;
        let principal = auth.authenticate(&wire, Audience::Pico, 0).await.unwrap();

        let stored = principal.scope.resolve(Resource::Stream, "orders").unwrap();
        assert_eq!(stored, "/acct/orders");
        auth.authorize(&principal, Operation::Read, Some(&stored))
            .unwrap();
        assert!(matches!(
            auth.authorize(&principal, Operation::Read, Some("/other/x")),
            Err(AuthError::Denied)
        ));
        assert!(matches!(
            auth.authorize(&principal, Operation::Append, Some(&stored)),
            Err(AuthError::Denied)
        ));
        assert!(matches!(
            auth.authorize(&principal, Operation::IssueToken, Some("svc/x")),
            Err(AuthError::Denied)
        ));
        assert_eq!(
            principal.scope.strip(Resource::Stream, "/acct/orders"),
            "orders"
        );
    }

    #[tokio::test]
    async fn authorize_group_resources() {
        let (auth, wire, _) = seeded().await;
        let principal = auth.authenticate(&wire, Audience::Pico, 0).await.unwrap();

        let group = principal.scope.resolve(Resource::Group, "workers").unwrap();
        assert_eq!(group, "acct/workers");
        auth.authorize(&principal, Operation::Consume, Some(&group))
            .unwrap();
        assert!(matches!(
            auth.authorize(&principal, Operation::Consume, Some("other/workers")),
            Err(AuthError::Denied)
        ));
        assert!(matches!(
            auth.authorize(&principal, Operation::DeleteGroup, Some(&group)),
            Err(AuthError::Denied)
        ));
    }

    #[tokio::test]
    async fn cache_reuses_arc_when_unchanged() {
        let (auth, wire, _) = seeded().await;
        let a = auth.authenticate(&wire, Audience::Pico, 0).await.unwrap();
        let b = auth.authenticate(&wire, Audience::Pico, 0).await.unwrap();
        assert!(Arc::ptr_eq(&a, &b));
    }

    #[tokio::test]
    async fn cache_drops_after_delete() {
        let store = Arc::new(MemoryTokenStore::new());
        let (token, verifier) = AccessToken::issue("z").unwrap();
        let mut scope = Scope::default();
        scope.audiences.insert(Audience::Pico);
        scope.streams = ResourceSet::prefix("");
        scope.access.stream = ReadWrite::read_only();
        let record = principal_record(&token.id, verifier, scope);
        let expected = record.verifier;
        store.put_if_absent(record).await.unwrap();
        let auth = Authorizer::new(Arc::clone(&store) as Arc<dyn TokenStore>);
        let wire = token.render();
        auth.authenticate(&wire, Audience::Pico, 0).await.unwrap();
        assert!(store.delete_if(&token.id, &expected).await.unwrap());
        assert!(matches!(
            auth.authenticate(&wire, Audience::Pico, 0).await,
            Err(AuthError::Unauthenticated)
        ));
    }

    #[tokio::test]
    async fn anonymous_grant_lookup() {
        let store = Arc::new(MemoryTokenStore::new());
        let auth = Authorizer::new(Arc::clone(&store) as Arc<dyn TokenStore>);
        assert!(matches!(
            auth.authenticate_anonymous(Audience::Pico, 0).await,
            Err(AuthError::Unauthenticated)
        ));

        let (_, verifier) = AccessToken::issue(ANONYMOUS_TOKEN_ID).unwrap();
        let mut scope = Scope::default();
        scope.audiences.insert(Audience::Pico);
        scope.streams = ResourceSet::prefix("/public/");
        scope.access.stream = ReadWrite::read_only();
        scope.expires_at_ms = Some(100);
        let record = principal_record(ANONYMOUS_TOKEN_ID, verifier, scope);
        let expected = record.verifier;
        store.put_if_absent(record).await.unwrap();

        let principal = auth
            .authenticate_anonymous(Audience::Pico, 0)
            .await
            .unwrap();
        assert_eq!(principal.id, ANONYMOUS_TOKEN_ID);
        // Expiry and audience misses both read as "present a credential".
        assert!(matches!(
            auth.authenticate_anonymous(Audience::Pico, 100).await,
            Err(AuthError::Unauthenticated)
        ));
        assert!(matches!(
            auth.authenticate_anonymous(Audience::Admin, 0).await,
            Err(AuthError::Unauthenticated)
        ));

        assert!(
            store
                .delete_if(ANONYMOUS_TOKEN_ID, &expected)
                .await
                .unwrap()
        );
        assert!(matches!(
            auth.authenticate_anonymous(Audience::Pico, 0).await,
            Err(AuthError::Unauthenticated)
        ));
    }

    #[tokio::test]
    async fn malformed_wire_rejected() {
        let auth = Authorizer::new(Arc::new(MemoryTokenStore::new()));
        assert!(matches!(
            auth.authenticate("not-a-token", Audience::Pico, 0).await,
            Err(AuthError::Malformed)
        ));
    }
}

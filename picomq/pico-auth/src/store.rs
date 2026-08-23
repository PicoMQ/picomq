//! Pluggable token record store.
//!
//! Hosts back this with the metadata KV. The in-memory impl is for unit tests
//! and local authorizer exercises. Conditional put and delete keep issue and
//! revoke races from clobbering a different generation of the same id.

use std::collections::BTreeMap;
use std::sync::Mutex;

use async_trait::async_trait;

use crate::record::TokenRecord;
use crate::token::{Verifier, ID_MAX_LEN, ID_MIN_LEN};
use crate::AuthError;

/// Persistence for [`TokenRecord`] values.
///
/// Keys are token ids. Values never include the secret.
#[async_trait]
pub trait TokenStore: Send + Sync {
    /// Load one record by id.
    async fn get(&self, id: &str) -> Result<Option<TokenRecord>, AuthError>;

    /// Insert only when the id is absent. Returns `true` if this call wrote.
    async fn put_if_absent(&self, record: TokenRecord) -> Result<bool, AuthError>;

    /// Delete only when the stored verifier matches `expected`.
    ///
    /// Returns `true` when a row was removed. Missing ids and verifier
    /// mismatches both return `false` so revoke and expiry never remove a
    /// replacement credential that reused the id after a recreate.
    async fn delete_if(&self, id: &str, expected: &Verifier) -> Result<bool, AuthError>;

    /// Records whose ids start with `prefix`, ordered by id.
    async fn list_prefix(&self, prefix: &str) -> Result<Vec<TokenRecord>, AuthError>;

    /// Number of live token records. Informational, never an enforced cap.
    async fn count(&self) -> Result<usize, AuthError>;
}

/// In-memory [`TokenStore`] for tests.
#[derive(Debug, Default)]
pub struct MemoryTokenStore {
    inner: Mutex<BTreeMap<String, TokenRecord>>,
}

impl MemoryTokenStore {
    pub fn new() -> Self {
        Self::default()
    }

    fn lock(&self) -> Result<std::sync::MutexGuard<'_, BTreeMap<String, TokenRecord>>, AuthError> {
        self.inner
            .lock()
            .map_err(|e| AuthError::Store(e.to_string()))
    }
}

fn validate_id(id: &str) -> Result<(), AuthError> {
    if !(ID_MIN_LEN..=ID_MAX_LEN).contains(&id.len()) {
        return Err(AuthError::Malformed);
    }
    Ok(())
}

#[async_trait]
impl TokenStore for MemoryTokenStore {
    async fn get(&self, id: &str) -> Result<Option<TokenRecord>, AuthError> {
        let map = self.lock()?;
        Ok(map.get(id).cloned())
    }

    async fn put_if_absent(&self, record: TokenRecord) -> Result<bool, AuthError> {
        validate_id(&record.id)?;
        let mut map = self.lock()?;
        if map.contains_key(&record.id) {
            return Ok(false);
        }
        map.insert(record.id.clone(), record);
        Ok(true)
    }

    async fn delete_if(&self, id: &str, expected: &Verifier) -> Result<bool, AuthError> {
        let mut map = self.lock()?;
        match map.get(id) {
            Some(record) if &record.verifier == expected => {
                map.remove(id);
                Ok(true)
            }
            _ => Ok(false),
        }
    }

    async fn list_prefix(&self, prefix: &str) -> Result<Vec<TokenRecord>, AuthError> {
        let map = self.lock()?;
        Ok(map
            .iter()
            .filter(|(id, _)| id.starts_with(prefix))
            .map(|(_, record)| record.clone())
            .collect())
    }

    async fn count(&self) -> Result<usize, AuthError> {
        let map = self.lock()?;
        Ok(map.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scope::Scope;
    use crate::token::AccessToken;

    fn record(id: &str) -> TokenRecord {
        let (_, verifier) = AccessToken::issue(id).unwrap();
        TokenRecord {
            id: id.into(),
            verifier,
            scope: Scope::default(),
            created_at_ms: 1,
            issued_by: String::new(),
        }
    }

    #[tokio::test]
    async fn put_get_and_duplicate_rejected() {
        let store = MemoryTokenStore::new();
        let first = record("a/one");
        assert!(store.put_if_absent(first.clone()).await.unwrap());
        assert_eq!(store.get("a/one").await.unwrap().as_ref(), Some(&first));
        assert!(!store.put_if_absent(record("a/one")).await.unwrap());
        assert_eq!(store.count().await.unwrap(), 1);
    }

    #[tokio::test]
    async fn delete_if_requires_matching_verifier() {
        let store = MemoryTokenStore::new();
        let rec = record("svc");
        let verifier = rec.verifier;
        store.put_if_absent(rec).await.unwrap();

        let other = record("other").verifier;
        assert!(!store.delete_if("svc", &other).await.unwrap());
        assert!(store.get("svc").await.unwrap().is_some());

        assert!(store.delete_if("svc", &verifier).await.unwrap());
        assert!(store.get("svc").await.unwrap().is_none());
        assert!(!store.delete_if("svc", &verifier).await.unwrap());
    }

    #[tokio::test]
    async fn list_prefix_is_ordered() {
        let store = MemoryTokenStore::new();
        for id in ["t/b", "t/a", "u/x", "t/c"] {
            store.put_if_absent(record(id)).await.unwrap();
        }
        let listed = store.list_prefix("t/").await.unwrap();
        let ids: Vec<_> = listed.iter().map(|r| r.id.as_str()).collect();
        assert_eq!(ids, ["t/a", "t/b", "t/c"]);
        assert_eq!(store.count().await.unwrap(), 4);
    }

    #[tokio::test]
    async fn empty_id_rejected_on_put() {
        let store = MemoryTokenStore::new();
        let mut rec = record("ok");
        rec.id.clear();
        assert!(matches!(
            store.put_if_absent(rec).await,
            Err(AuthError::Malformed)
        ));
    }
}

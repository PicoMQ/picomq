//! KV-backed token store over the metadata plane.
//!
//! Records live at `auth/token/{id}` with no leading slash, so they cannot
//! collide with stream registry keys (which are always URI paths).

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use pico_auth::{AuthError, Authorizer, TokenRecord, TokenStore, Verifier, ID_MAX_LEN, ID_MIN_LEN};
use s3stream::{KVClient, KeyValue};

/// Metadata KV prefix for token records. Must not start with `/`.
pub const TOKEN_KEY_PREFIX: &str = "auth/token/";

/// [`TokenStore`] over an [`KVClient`].
pub struct KvTokenStore {
    kv: Arc<dyn KVClient>,
}

impl KvTokenStore {
    pub fn new(kv: Arc<dyn KVClient>) -> Self {
        Self { kv }
    }

    fn key(id: &str) -> String {
        format!("{TOKEN_KEY_PREFIX}{id}")
    }

    fn map_err(err: impl std::fmt::Display) -> AuthError {
        AuthError::Store(err.to_string())
    }
}

fn validate_id(id: &str) -> Result<(), AuthError> {
    if !(ID_MIN_LEN..=ID_MAX_LEN).contains(&id.len()) {
        return Err(AuthError::Malformed);
    }
    Ok(())
}

#[async_trait]
impl TokenStore for KvTokenStore {
    async fn get(&self, id: &str) -> Result<Option<TokenRecord>, AuthError> {
        let Some(bytes) = self
            .kv
            .get_kv(&Self::key(id))
            .await
            .map_err(Self::map_err)?
        else {
            return Ok(None);
        };
        Ok(Some(TokenRecord::decode(&bytes)?))
    }

    async fn put_if_absent(&self, record: TokenRecord) -> Result<bool, AuthError> {
        validate_id(&record.id)?;
        let key = Self::key(&record.id);
        if self.kv.get_kv(&key).await.map_err(Self::map_err)?.is_some() {
            return Ok(false);
        }
        let encoded = record.encode();
        let stored = self
            .kv
            .put_kv_if_absent(KeyValue {
                key,
                value: encoded.clone(),
            })
            .await
            .map_err(Self::map_err)?;
        Ok(stored == encoded)
    }

    async fn delete_if(&self, id: &str, expected: &Verifier) -> Result<bool, AuthError> {
        let key = Self::key(id);
        let Some(bytes) = self.kv.get_kv(&key).await.map_err(Self::map_err)? else {
            return Ok(false);
        };
        let record = TokenRecord::decode(&bytes)?;
        if &record.verifier != expected {
            return Ok(false);
        }
        let removed = self
            .kv
            .del_kv_if(&key, &bytes)
            .await
            .map_err(Self::map_err)?;
        Ok(removed.is_some())
    }

    async fn list_prefix(&self, prefix: &str) -> Result<Vec<TokenRecord>, AuthError> {
        let kv_prefix = format!("{TOKEN_KEY_PREFIX}{prefix}");
        let entries = self.kv.list_kv(&kv_prefix).await.map_err(Self::map_err)?;
        let mut out = Vec::with_capacity(entries.len());
        for entry in entries {
            out.push(TokenRecord::decode(&entry.value)?);
        }
        Ok(out)
    }

    async fn count(&self) -> Result<usize, AuthError> {
        let entries = self
            .kv
            .list_kv(TOKEN_KEY_PREFIX)
            .await
            .map_err(Self::map_err)?;
        Ok(entries.len())
    }
}

/// Token control plane: owns the store and the authorizer built over it.
pub struct TokenService {
    store: Arc<KvTokenStore>,
    authorizer: Arc<Authorizer>,
}

impl TokenService {
    pub fn new(kv: Arc<dyn KVClient>) -> Self {
        let store = Arc::new(KvTokenStore::new(kv));
        let authorizer = Arc::new(Authorizer::new(store.clone()));
        Self { store, authorizer }
    }

    pub fn authorizer(&self) -> Arc<Authorizer> {
        self.authorizer.clone()
    }

    pub fn store(&self) -> Arc<KvTokenStore> {
        self.store.clone()
    }

    /// One expiry pass. Returns the number removed; `delete_if` skips records
    /// rotated since the list.
    pub async fn remove_expired(&self, now_ms: i64) -> Result<usize, AuthError> {
        let mut removed = 0;
        for record in self.store.list_prefix("").await? {
            if record.scope.is_expired(now_ms)
                && self.store.delete_if(&record.id, &record.verifier).await?
            {
                removed += 1;
            }
        }
        Ok(removed)
    }

    /// Background expiry loop, gated on the leadership watch. Exits when the
    /// sender is dropped.
    pub fn spawn_expiry_loop(
        self: &Arc<Self>,
        mut leadership: tokio::sync::watch::Receiver<bool>,
        tick: Duration,
    ) -> tokio::task::JoinHandle<()> {
        let service = self.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(tick).await;
                if leadership.has_changed().is_err() {
                    return;
                }
                if !*leadership.borrow_and_update() {
                    continue;
                }
                if let Err(error) = service.remove_expired(pico_common::now_ms()).await {
                    tracing::debug!(%error, "token expiry failed");
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pico_auth::{AccessToken, Scope};
    use s3stream::MemoryKvClient;

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
    async fn put_get_duplicate_and_delete_if() {
        let store = KvTokenStore::new(MemoryKvClient::new());
        let first = record("a/one");
        assert!(store.put_if_absent(first.clone()).await.unwrap());
        assert_eq!(store.get("a/one").await.unwrap().as_ref(), Some(&first));
        assert!(!store.put_if_absent(record("a/one")).await.unwrap());
        assert_eq!(store.count().await.unwrap(), 1);

        let other = record("other").verifier;
        assert!(!store.delete_if("a/one", &other).await.unwrap());
        assert!(store.get("a/one").await.unwrap().is_some());

        let verifier = first.verifier;
        assert!(store.delete_if("a/one", &verifier).await.unwrap());
        assert!(store.get("a/one").await.unwrap().is_none());
        assert!(!store.delete_if("a/one", &verifier).await.unwrap());
    }

    #[tokio::test]
    async fn list_prefix_ordered() {
        let store = KvTokenStore::new(MemoryKvClient::new());
        for id in ["t/b", "t/a", "u/x"] {
            assert!(store.put_if_absent(record(id)).await.unwrap());
        }
        let listed = store.list_prefix("t/").await.unwrap();
        let ids: Vec<_> = listed.iter().map(|r| r.id.as_str()).collect();
        assert_eq!(ids, ["t/a", "t/b"]);
        assert_eq!(store.count().await.unwrap(), 3);
    }

    #[test]
    fn auth_prefix_unreachable_from_stream_names() {
        // Stream registry keys are URI paths from `uri.path()`, so they always
        // begin with `/`. Auth keys must not.
        assert!(!TOKEN_KEY_PREFIX.starts_with('/'));
        assert!(TOKEN_KEY_PREFIX.starts_with("auth/"));
        let stream_name = "/auth/token/collide";
        assert!(stream_name.starts_with('/'));
        assert_ne!(stream_name, format!("{TOKEN_KEY_PREFIX}collide"));
    }

    fn expiring_record(id: &str, expires_at_ms: i64) -> TokenRecord {
        let mut rec = record(id);
        rec.scope.expires_at_ms = Some(expires_at_ms);
        rec
    }

    #[tokio::test]
    async fn remove_expired_only_removes_expired() {
        let kv = MemoryKvClient::new();
        let store = KvTokenStore::new(kv.clone());
        let service = TokenService::new(kv);
        store
            .put_if_absent(expiring_record("dead", 10))
            .await
            .unwrap();
        store
            .put_if_absent(expiring_record("live", 1_000))
            .await
            .unwrap();
        store.put_if_absent(record("forever")).await.unwrap();

        assert_eq!(service.remove_expired(100).await.unwrap(), 1);
        assert!(store.get("dead").await.unwrap().is_none());
        assert!(store.get("live").await.unwrap().is_some());
        assert!(store.get("forever").await.unwrap().is_some());
        assert_eq!(service.remove_expired(100).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn expiry_loop_gated_on_leadership() {
        let kv = MemoryKvClient::new();
        let store = KvTokenStore::new(kv.clone());
        let service = Arc::new(TokenService::new(kv));
        store
            .put_if_absent(expiring_record("dead", 1))
            .await
            .unwrap();

        let (tx, rx) = tokio::sync::watch::channel(false);
        let handle = service.spawn_expiry_loop(rx, Duration::from_millis(2));

        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(store.get("dead").await.unwrap().is_some());

        tx.send(true).unwrap();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while store.get("dead").await.unwrap().is_some() {
            assert!(
                tokio::time::Instant::now() < deadline,
                "expiry loop never ran"
            );
            tokio::time::sleep(Duration::from_millis(2)).await;
        }

        drop(tx);
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn empty_id_rejected_on_put() {
        let store = KvTokenStore::new(MemoryKvClient::new());
        let mut rec = record("ok");
        rec.id.clear();
        assert!(matches!(
            store.put_if_absent(rec).await,
            Err(AuthError::Malformed)
        ));
    }
}

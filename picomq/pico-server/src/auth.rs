//! KV-backed token store over the metadata plane.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use picomq_auth::{
    AuthError, Authorizer, ID_MAX_LEN, ID_MIN_LEN, TokenRecord, TokenStore, Verifier,
};
use picomq_metadata::MetadataKvClient;

/// Metadata KV prefix for token records. Must not start with `/`.
pub const TOKEN_KEY_PREFIX: &str = "auth/token/";

const EXPIRY_PAGE: usize = 1_000;

/// [`TokenStore`] over the metadata KV.
pub struct KvTokenStore {
    kv: MetadataKvClient,
}

impl KvTokenStore {
    pub fn new(kv: MetadataKvClient) -> Self {
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
        let Some(bytes) = self.kv.get_kv(&Self::key(id)) else {
            return Ok(None);
        };
        Ok(Some(TokenRecord::decode(&bytes)?))
    }

    async fn put_if_absent(&self, record: TokenRecord) -> Result<bool, AuthError> {
        validate_id(&record.id)?;
        let key = Self::key(&record.id);
        if self.kv.get_kv(&key).is_some() {
            return Ok(false);
        }
        let encoded = record.encode();
        let stored = self
            .kv
            .put_kv_if_absent(&key, encoded.clone())
            .await
            .map_err(Self::map_err)?;
        Ok(stored == encoded)
    }

    async fn delete_if(&self, id: &str, expected: &Verifier) -> Result<bool, AuthError> {
        let key = Self::key(id);
        let Some(bytes) = self.kv.get_kv(&key) else {
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

    async fn list_page(
        &self,
        prefix: &str,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<TokenRecord>, AuthError> {
        let after = after.map(Self::key);
        self.kv
            .list_kv_page(&Self::key(prefix), after.as_deref(), limit)
            .into_iter()
            .map(|(_, value)| TokenRecord::decode(&value))
            .collect()
    }

    async fn count(&self) -> Result<usize, AuthError> {
        Ok(self.kv.list_kv(TOKEN_KEY_PREFIX).len())
    }
}

/// Token control plane: owns the store and the authorizer built over it.
pub struct TokenService {
    store: Arc<KvTokenStore>,
    authorizer: Arc<Authorizer>,
}

impl TokenService {
    pub fn new(kv: MetadataKvClient) -> Self {
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

    /// One expiry pass returning the number removed. `delete_if` skips records
    /// rotated since the list.
    pub async fn remove_expired(&self, now_ms: i64) -> Result<usize, AuthError> {
        let mut removed = 0;
        let mut after = None;

        loop {
            let records = self
                .store
                .list_page("", after.as_deref(), EXPIRY_PAGE)
                .await?;
            for record in &records {
                if record.scope.is_expired(now_ms)
                    && self.store.delete_if(&record.id, &record.verifier).await?
                {
                    removed += 1;
                }
            }

            if records.len() < EXPIRY_PAGE {
                return Ok(removed);
            }
            after = records.last().map(|record| record.id.clone());
        }
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
            while let Some(leading) = crate::leadership::tick(&mut leadership, tick).await {
                if leading && let Err(error) = service.remove_expired(picomq_common::now_ms()).await
                {
                    tracing::debug!(%error, "token expiry failed");
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use picomq_auth::{AccessToken, Scope};
    use picomq_metadata::{LocalSink, MetadataNodeHandle, Registration};

    async fn kv() -> MetadataKvClient {
        let (sink, views) = LocalSink::new();
        let registration = Registration {
            node_id: 1,
            node_epoch: 1,
            slots: 1,
            ..Default::default()
        };
        MetadataNodeHandle::register(Arc::new(sink), views, registration)
            .await
            .unwrap()
            .kv_client()
    }

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
        let store = KvTokenStore::new(kv().await);
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
    async fn list_page_is_ordered_bounded_and_resumable() {
        let store = KvTokenStore::new(kv().await);
        for id in ["t/b", "t/a", "u/x", "t/c"] {
            assert!(store.put_if_absent(record(id)).await.unwrap());
        }
        let ids = |records: Vec<TokenRecord>| records.into_iter().map(|r| r.id).collect::<Vec<_>>();

        assert_eq!(
            ids(store.list_page("t/", None, 2).await.unwrap()),
            ["t/a", "t/b"]
        );
        assert_eq!(
            ids(store.list_page("t/", Some("t/b"), 2).await.unwrap()),
            ["t/c"]
        );
        assert_eq!(
            ids(store.list_page("", Some("t/c"), 5).await.unwrap()),
            ["u/x"]
        );
        assert_eq!(store.count().await.unwrap(), 4);
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
        let kv = kv().await;
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
    async fn remove_expired_walks_every_page() {
        let kv = kv().await;
        let store = KvTokenStore::new(kv.clone());
        let service = TokenService::new(kv);
        let total = EXPIRY_PAGE * 2 + 500;
        for n in 0..total {
            let id = format!("svc/{n:05}");
            let expires_at_ms = if n % 2 == 0 { 10 } else { 1_000 };
            store
                .put_if_absent(expiring_record(&id, expires_at_ms))
                .await
                .unwrap();
        }

        assert_eq!(service.remove_expired(100).await.unwrap(), total / 2);
        assert_eq!(store.count().await.unwrap(), total / 2);
        assert_eq!(service.remove_expired(100).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn expiry_loop_gated_on_leadership() {
        let kv = kv().await;
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
        let store = KvTokenStore::new(kv().await);
        let mut rec = record("ok");
        rec.id.clear();
        assert!(matches!(
            store.put_if_absent(rec).await,
            Err(AuthError::Malformed)
        ));
    }
}

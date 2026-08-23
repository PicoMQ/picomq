//! Light KV facility exposed alongside streams (hosts use it for small metadata,
//! e.g. Kafka uses it for partition -> streamId mappings).
//!
//! The implementation lives in the host's metadata plane (like
//! `StreamManager`), not in the engine. The trait is part of the embeddable
//! API surface so `Client` is complete.

use async_trait::async_trait;
use bytes::Bytes;

use crate::api::StreamError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyValue {
    pub key: String,
    pub value: Bytes,
}

#[async_trait]
pub trait KVClient: Send + Sync {
    /// Put if absent. Returns the value stored under the key after the call
    /// (the existing value if the key was already present).
    async fn put_kv_if_absent(&self, kv: KeyValue) -> Result<Bytes, StreamError>;

    /// Put, overwriting. Returns the value after the call.
    async fn put_kv(&self, kv: KeyValue) -> Result<Bytes, StreamError>;

    async fn get_kv(&self, key: &str) -> Result<Option<Bytes>, StreamError>;

    /// Delete. Returns the deleted value, None if the key did not exist.
    async fn del_kv(&self, key: &str) -> Result<Option<Bytes>, StreamError>;

    /// Delete if value matches. Returns the deleted value, None if missing or mismatched.
    async fn del_kv_if(&self, key: &str, expected: &Bytes) -> Result<Option<Bytes>, StreamError>;

    /// List key-values whose keys start with `prefix`, ordered by key. Empty
    /// list if no key matches.
    async fn list_kv(&self, prefix: &str) -> Result<Vec<KeyValue>, StreamError>;
}

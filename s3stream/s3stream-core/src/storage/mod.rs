//! The storage pipeline: WAL-backed append, cached reads, delta-WAL upload, recovery.
//!
//! Specification: `specification/upload-protocol.md`.

pub mod confirm;
pub mod recovery;
pub mod s3_storage;
pub mod upload;

use async_trait::async_trait;

use s3stream_codec::StreamRecordBatch;

use crate::api::StreamError;
use crate::api::results::CacheAccessType;
use crate::context::{AppendContext, FetchContext};

/// Result of a storage-level read: encoded batches + cache provenance.
pub struct ReadDataBlock {
    pub records: Vec<StreamRecordBatch>,
    pub cache_access: CacheAccessType,
}

/// The engine's storage abstraction (S3Stream sits on top of this).
#[async_trait]
pub trait Storage: Send + Sync {
    /// Start: run WAL recovery to completion, then accept traffic.
    async fn startup(&self) -> Result<(), StreamError>;

    async fn shutdown(&self);

    /// Append one encoded record. Resolves when durable. Provided as
    /// submit + await. Implementations supply [`Storage::submit`].
    async fn append(
        &self,
        context: AppendContext,
        record: StreamRecordBatch,
    ) -> Result<(), StreamError> {
        self.submit(context, record).await
    }

    /// Submit one encoded record synchronously. The record is admitted (WAL
    /// enqueue / backoff queue) before this returns, so call order is
    /// persistence order. Returns a future that resolves at durability.
    ///
    /// Async fns run lazily, so the synchronous half must be a real method
    /// for pipelined callers ([`crate::api::Stream::submit_append`]).
    /// The future is `'static` (owns its resources): it can be awaited,
    /// stored, or moved to a completer task independent of `&self`.
    fn submit(
        &self,
        context: AppendContext,
        record: StreamRecordBatch,
    ) -> futures::future::BoxFuture<'static, Result<(), StreamError>>;

    /// Read `[start_offset, end_offset)` of a stream, capped near `max_bytes`.
    async fn read(
        &self,
        context: FetchContext,
        stream_id: u64,
        start_offset: u64,
        end_offset: u64,
        max_bytes: usize,
    ) -> Result<ReadDataBlock, StreamError>;

    /// Force-upload everything buffered for `stream_id` (stream close path). Resolves
    /// when committed.
    async fn force_upload(&self, stream_id: u64) -> Result<(), StreamError>;
}

//! The public stream API: what hosts embed and call.

pub mod error;
pub mod kv;
pub mod link;
pub mod options;
pub mod record;
pub mod results;

use std::sync::Arc;

use async_trait::async_trait;

use crate::context::{AppendContext, FetchContext};
use crate::failover::{FailoverRequest, FailoverResponse};

pub use error::StreamError;
pub use kv::{KVClient, KeyValue};
pub use link::LinkRecordDecoder;
pub use options::{CreateStreamOptions, OpenStreamOptions};
pub use record::RecordBatch;
pub use results::{AppendResult, FetchResult, PendingAppend, RecordBatchWithContext};

/// Top-level client handle a host embeds: streams + KV + failover trigger.
#[async_trait]
pub trait Client: Send + Sync {
    async fn start(&self) -> Result<(), StreamError>;

    async fn shutdown(&self);

    fn stream_client(&self) -> std::sync::Arc<dyn StreamClient>;

    fn kv_client(&self) -> std::sync::Arc<dyn KVClient>;

    /// Recover another (dead) node's WAL into objects.
    async fn failover(&self, request: FailoverRequest) -> Result<FailoverResponse, StreamError>;
}

/// A record stream. Fetched buffers are released by `Drop` on
/// `FetchResult`. Callers track their own last append future.
#[async_trait]
pub trait Stream: Send + Sync {
    fn stream_id(&self) -> u64;

    fn stream_epoch(&self) -> u64;

    fn start_offset(&self) -> u64;

    /// Highest durably-acked offset (readable watermark).
    fn confirm_offset(&self) -> u64;

    /// Set confirm offset. Only supported in SNAPSHOT_READ mode. Outside
    /// snapshot-read this returns [`StreamError::Unexpected`].
    fn confirm_offset_set(&self, offset: u64) -> Result<(), StreamError> {
        let _ = offset;
        Err(StreamError::Unexpected(
            "Only snapshot-read mode support set confirmOffset".into(),
        ))
    }

    /// Next offset an append would receive.
    fn next_offset(&self) -> u64;

    /// Append a record batch. Resolves when durable (WAL-confirmed).
    ///
    /// Fails with
    /// `StreamError::Fenced` when the stream epoch has been superseded.
    async fn append(
        &self,
        context: AppendContext,
        batch: RecordBatch,
    ) -> Result<AppendResult, StreamError>;

    /// Submit synchronously (offset/WAL enqueue before return) so submit order
    /// is offset order. Returns a [`PendingAppend`] for durability.
    ///
    /// Cancel-safe: confirm bookkeeping is detached, so dropping the pending
    /// handle cannot stall the stream.
    fn submit_append(
        self: Arc<Self>,
        context: AppendContext,
        batch: RecordBatch,
    ) -> Result<PendingAppend, StreamError>;

    /// Fetch `[start_offset, end_offset)` capped near `max_bytes_hint`.
    ///
    /// Boundary semantics: a
    /// batch straddling either boundary is returned whole. Result size may exceed the
    /// hint by up to one batch.
    async fn fetch(
        &self,
        context: FetchContext,
        start_offset: u64,
        end_offset: u64,
        max_bytes_hint: usize,
    ) -> Result<FetchResult, StreamError>;

    /// Advance the stream start offset. Data below it becomes collectible.
    async fn trim(&self, new_start_offset: u64) -> Result<(), StreamError>;

    /// Flush + close. Further appends fail.
    async fn close(&self) -> Result<(), StreamError>;

    /// Close and delete the stream.
    async fn destroy(&self) -> Result<(), StreamError>;
}

/// Stream lifecycle entry point.
#[async_trait]
pub trait StreamClient: Send + Sync {
    async fn create_and_open_stream(
        &self,
        options: CreateStreamOptions,
    ) -> Result<std::sync::Arc<dyn Stream>, StreamError>;

    async fn open_stream(
        &self,
        stream_id: u64,
        options: OpenStreamOptions,
    ) -> Result<std::sync::Arc<dyn Stream>, StreamError>;

    /// Retrieve an already-open stream.
    fn get_stream(&self, stream_id: u64) -> Option<std::sync::Arc<dyn Stream>>;

    /// Run one compaction pass right now. A no-op when the stream is not
    /// open in this process.
    async fn compact_stream(
        &self,
        stream_id: u64,
        level: crate::compact::CompactionLevel,
    ) -> Result<(), StreamError> {
        let _ = (stream_id, level);
        Ok(())
    }

    async fn shutdown(&self);
}

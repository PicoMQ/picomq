//! In-memory WAL: the test/emulator backend. It honors the full
//! `WriteAheadLog` contract so engine-level tests exercise real
//! recovery/trim semantics without object storage.

use std::collections::BTreeMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};

use async_trait::async_trait;
use futures::stream;

use s3stream_codec::StreamRecordBatch;

use crate::{
    AppendListener, AppendResult, PendingAppend, RecordOffset, RecoverResult, RecoverStream,
    WalError, WalMetadata, WriteAheadLog,
};

/// Framing overhead accounted per record so offsets resemble the object WAL's.
const FRAME_OVERHEAD: u64 = s3stream_codec::WAL_RECORD_HEADER_SIZE as u64;

struct State {
    records: BTreeMap<u64, StreamRecordBatch>,
    next_offset: u64,
    /// Inclusive trim watermark (u64::MAX-safe: starts at 0 meaning nothing trimmed).
    trim_offset: u64,
}

/// An in-memory `WriteAheadLog`. Appends are durable-in-memory and confirm
/// immediately, in order.
pub struct MemoryWriteAheadLog {
    metadata: WalMetadata,
    uri: String,
    started: AtomicBool,
    state: Mutex<State>,
    /// Confirm hook. Appends confirm inline, so it runs under the state lock to keep
    /// callbacks in offset order (the `AppendListener` contract).
    append_listener: Mutex<Option<AppendListener>>,
}

impl MemoryWriteAheadLog {
    pub fn new(node_id: u32, epoch: u64) -> Self {
        Self {
            metadata: WalMetadata { node_id, epoch },
            uri: format!("0@memory://?nodeId={node_id}&epoch={epoch}"),
            started: AtomicBool::new(false),
            state: Mutex::new(State {
                records: BTreeMap::new(),
                next_offset: 0,
                trim_offset: 0,
            }),
            append_listener: Mutex::new(None),
        }
    }

    fn ensure_started(&self) -> Result<(), WalError> {
        if self.started.load(Ordering::Acquire) {
            Ok(())
        } else {
            Err(WalError::NotInitialized)
        }
    }
}

#[async_trait]
impl WriteAheadLog for MemoryWriteAheadLog {
    async fn start(&self) -> Result<(), WalError> {
        self.started.store(true, Ordering::Release);
        Ok(())
    }

    async fn shutdown_gracefully(&self) {
        self.started.store(false, Ordering::Release);
    }

    fn metadata(&self) -> WalMetadata {
        self.metadata
    }

    fn uri(&self) -> &str {
        &self.uri
    }

    fn submit(&self, record: StreamRecordBatch) -> Result<PendingAppend, WalError> {
        self.ensure_started()?;
        let mut state = self.state.lock().expect("wal state poisoned");
        let size = (record.encoded().len() as u64 + FRAME_OVERHEAD) as u32;
        let offset = state.next_offset;
        state.next_offset += size as u64;
        let next_offset = state.next_offset;
        state.records.insert(offset, record.clone());
        let record_offset = RecordOffset {
            epoch: self.metadata.epoch,
            offset,
            size,
        };
        let result = AppendResult {
            record_offset,
            next_offset: RecordOffset {
                epoch: self.metadata.epoch,
                offset: next_offset,
                size: 0,
            },
        };
        if let Some(listener) = self
            .append_listener
            .lock()
            .expect("listener poisoned")
            .as_ref()
        {
            listener(&record, result.record_offset, result.next_offset);
        }
        // Durable the moment it is placed: there is nothing to flush.
        Ok(PendingAppend {
            durable: Box::pin(std::future::ready(Ok(result))),
        })
    }

    fn set_append_listener(&self, listener: AppendListener) {
        *self.append_listener.lock().expect("listener poisoned") = Some(listener);
    }

    async fn get(&self, offset: RecordOffset) -> Result<StreamRecordBatch, WalError> {
        self.ensure_started()?;
        let state = self.state.lock().expect("wal state poisoned");
        state
            .records
            .get(&offset.offset)
            .cloned()
            .ok_or(WalError::NotInitialized)
    }

    async fn get_range(
        &self,
        start: RecordOffset,
        end: RecordOffset,
    ) -> Result<Vec<StreamRecordBatch>, WalError> {
        self.ensure_started()?;
        let state = self.state.lock().expect("wal state poisoned");
        Ok(state
            .records
            .range(start.offset..end.offset)
            .map(|(_, r)| r.clone())
            .collect())
    }

    fn confirm_offset(&self) -> RecordOffset {
        let state = self.state.lock().expect("wal state poisoned");
        RecordOffset {
            epoch: self.metadata.epoch,
            offset: state.next_offset,
            size: 0,
        }
    }

    fn recover(&self) -> RecoverStream {
        let state = self.state.lock().expect("wal state poisoned");
        let epoch = self.metadata.epoch;
        let items: Vec<Result<RecoverResult, WalError>> = state
            .records
            .iter()
            .map(|(offset, record)| {
                let size = (record.encoded().len() as u64 + FRAME_OVERHEAD) as u32;
                Ok(RecoverResult {
                    record: record.clone(),
                    record_offset: RecordOffset {
                        epoch,
                        offset: *offset,
                        size,
                    },
                })
            })
            .collect();
        Box::pin(stream::iter(items))
    }

    async fn reset(&self) -> Result<(), WalError> {
        let mut state = self.state.lock().expect("wal state poisoned");
        state.trim_offset = state.next_offset;
        state.records.clear();
        Ok(())
    }

    async fn trim(&self, offset: RecordOffset) -> Result<(), WalError> {
        let mut state = self.state.lock().expect("wal state poisoned");
        state.trim_offset = state.trim_offset.max(offset.end_offset());
        let trim = state.trim_offset;
        state
            .records
            .retain(|record_offset, _| *record_offset >= trim);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use futures::StreamExt;

    fn record(len: usize) -> StreamRecordBatch {
        StreamRecordBatch::from_encoded_unchecked(Bytes::from(vec![0u8; len]))
    }

    #[tokio::test]
    async fn append_assigns_sequential_offsets() {
        let wal = MemoryWriteAheadLog::new(1, 1);
        wal.start().await.unwrap();
        let a = wal.append(record(10)).await.unwrap();
        let b = wal.append(record(10)).await.unwrap();
        assert_eq!(a.record_offset.offset, 0);
        assert_eq!(a.next_offset.offset, b.record_offset.offset);
        assert_eq!(wal.confirm_offset().offset, b.next_offset.offset);
    }

    #[tokio::test]
    async fn recover_yields_untrimmed_suffix_in_order() {
        let wal = MemoryWriteAheadLog::new(1, 1);
        wal.start().await.unwrap();
        let a = wal.append(record(8)).await.unwrap();
        let _b = wal.append(record(8)).await.unwrap();
        let _c = wal.append(record(8)).await.unwrap();

        wal.trim(a.record_offset).await.unwrap();

        let recovered: Vec<_> = wal.recover().collect().await;
        assert_eq!(recovered.len(), 2);
        let offsets: Vec<u64> = recovered
            .into_iter()
            .map(|r| r.unwrap().record_offset.offset)
            .collect();
        assert!(offsets.windows(2).all(|w| w[0] < w[1]));
        assert!(offsets.iter().all(|o| *o > a.record_offset.offset));
    }

    #[tokio::test]
    async fn reset_empties_the_log() {
        let wal = MemoryWriteAheadLog::new(1, 1);
        wal.start().await.unwrap();
        wal.append(record(8)).await.unwrap();
        wal.reset().await.unwrap();
        assert_eq!(wal.recover().collect::<Vec<_>>().await.len(), 0);
        // Offsets keep advancing after reset (address space is never reused).
        let d = wal.append(record(8)).await.unwrap();
        assert!(d.record_offset.offset > 0);
    }

    #[tokio::test]
    async fn append_before_start_fails() {
        let wal = MemoryWriteAheadLog::new(1, 1);
        assert!(wal.append(record(8)).await.is_err());
    }
}

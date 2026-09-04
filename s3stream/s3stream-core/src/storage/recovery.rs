//! WAL recovery record filtering and block grouping.
//!
//! Specification: `specification/upload-protocol.md` (recovery section).

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;

use futures::StreamExt;

use s3stream_wal::{RecoverResult, RecoverStream};

use crate::api::{LinkRecordDecoder, StreamError};
use crate::cache::log_cache::{DEFAULT_MAX_BLOCK_STREAM_COUNT, LogCacheBlock};

/// Recover records from the WAL stream, grouping them into `LogCacheBlock`s.
///
/// - records of streams not in `opening_stream_end_offsets` (safely closed).
/// - records below a stream's committed end offset (already committed).
/// - discontinuous records (gap between expected and actual offset), with a warning.
///
/// Records are processed in blocks: when a block fills (size threshold or offset
/// overflow) it is handed to `block_handler` (which uploads it) and a new block starts.
/// `opening_stream_end_offsets` is advanced as records are accepted, so it ends as the
/// post-recovery end-offset map.
pub async fn recover<F, Fut>(
    mut it: RecoverStream,
    opening_stream_end_offsets: &mut HashMap<u64, u64>,
    max_cache_size: u64,
    link_decoder: Option<Arc<dyn LinkRecordDecoder>>,
    mut block_handler: F,
) -> Result<(), StreamError>
where
    F: FnMut(Arc<LogCacheBlock>) -> Fut,
    Fut: Future<Output = Result<(), StreamError>>,
{
    let mut first = true;
    let mut pending: Option<RecoverResult> = None;
    let mut next: Option<RecoverResult> = None;
    let mut eof = false;
    loop {
        if pending.is_none() && next.is_none() {
            if eof {
                break;
            }
            next = match it.next().await {
                Some(result) => Some(result?),
                None => break,
            };
        }
        let cache_block = Arc::new(LogCacheBlock::new(
            max_cache_size,
            DEFAULT_MAX_BLOCK_STREAM_COUNT,
        ));
        let mut block_last_offset = None;
        if let Some(carried) = pending.take() {
            opening_stream_end_offsets
                .insert(carried.record.stream_id(), carried.record.last_offset());
            block_last_offset = Some(carried.record_offset);
            cache_block.put(carried.record);
        }
        while !cache_block.is_full() {
            let result = match next.take() {
                Some(result) => result,
                None => {
                    if eof {
                        break;
                    }
                    match it.next().await {
                        Some(result) => result?,
                        None => {
                            eof = true;
                            break;
                        }
                    }
                }
            };
            if first {
                tracing::info!(offset = ?result.record_offset, "recover start offset");
                first = false;
            }
            let record_offset = result.record_offset;
            match process_record(result, opening_stream_end_offsets, &cache_block) {
                None => block_last_offset = Some(record_offset),
                Some(not_added) => {
                    // Block full/overflow: carry the record into the next block.
                    pending = Some(not_added);
                    break;
                }
            }
        }
        if let Some(offset) = block_last_offset {
            cache_block.set_last_record_offset(offset);
        }
        decode_link_records(&cache_block, link_decoder.as_deref()).await?;
        block_handler(cache_block).await?;
    }
    Ok(())
}

/// Returns `None` if the record was added or dropped, `Some(result)` when the block
/// was full and the record was NOT added.
fn process_record(
    result: RecoverResult,
    opening_stream_end_offsets: &mut HashMap<u64, u64>,
    cache_block: &LogCacheBlock,
) -> Option<RecoverResult> {
    let record = &result.record;
    let stream_id = record.stream_id();
    let Some(&expected_next_offset) = opening_stream_end_offsets.get(&stream_id) else {
        // Stream is already safely closed: skip.
        return None;
    };
    if expected_next_offset > record.base_offset() {
        // Already committed: skip.
        return None;
    }
    if expected_next_offset < record.base_offset() {
        tracing::warn!(
            stream_id,
            expected = expected_next_offset,
            actual = record.base_offset(),
            "[BUG] dropping discontinuous WAL record"
        );
        return None;
    }
    let last_offset = record.last_offset();
    if !cache_block.put(result.record.clone()) {
        return Some(result);
    }
    opening_stream_end_offsets.insert(stream_id, last_offset);
    None
}

async fn decode_link_records(
    cache_block: &LogCacheBlock,
    decoder: Option<&dyn LinkRecordDecoder>,
) -> Result<(), StreamError> {
    let records = cache_block.records();
    for stream_records in records.values() {
        for record in stream_records {
            if record.count() >= 0 {
                continue;
            }
            let Some(decoder) = decoder else {
                return Err(StreamError::Unexpected(
                    "recovered a link record but no link record decoder is configured".into(),
                ));
            };
            let decoded = decoder.decode(record.clone()).await?;
            cache_block.replace_record(record.stream_id(), record.base_offset(), decoded);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use s3stream_codec::StreamRecordBatch;
    use s3stream_wal::RecordOffset;

    fn result(stream_id: u64, base_offset: u64, count: i32, wal_offset: u64) -> RecoverResult {
        RecoverResult {
            record: StreamRecordBatch::new(
                stream_id,
                0,
                base_offset,
                count,
                Bytes::from(vec![0u8; 16]),
            ),
            record_offset: RecordOffset {
                epoch: 1,
                offset: wal_offset,
                size: 40,
            },
        }
    }

    fn recover_stream(items: Vec<RecoverResult>) -> RecoverStream {
        Box::pin(futures::stream::iter(items.into_iter().map(Ok)))
    }

    /// The javadoc example: stream end offset 3, WAL has 1..=6 => keep 3..=6.
    /// Records of unknown streams and gapped records are dropped.
    #[tokio::test]
    async fn filters_committed_closed_and_gapped_records() {
        let items = vec![
            result(1, 1, 1, 0),
            result(1, 2, 1, 100),
            result(1, 3, 1, 200),
            result(1, 4, 1, 300),
            result(9, 0, 1, 400), // stream 9 not opening: dropped
            result(1, 6, 1, 500), // gap (expected 5): dropped
            result(1, 5, 1, 600), // continues the tail
        ];
        let mut end_offsets = HashMap::from([(1u64, 3u64)]);
        let mut blocks = Vec::new();
        recover(
            recover_stream(items),
            &mut end_offsets,
            1 << 29,
            None,
            |block| {
                blocks.push(block);
                async { Ok(()) }
            },
        )
        .await
        .unwrap();

        assert_eq!(blocks.len(), 1);
        let records = blocks[0].records();
        let offsets: Vec<u64> = records[&1].iter().map(|r| r.base_offset()).collect();
        assert_eq!(offsets, vec![3, 4, 5]);
        assert!(!records.contains_key(&9));
        assert_eq!(end_offsets[&1], 6);
        // Block carries the WAL offset of its last accepted record for trimming.
        assert_eq!(blocks[0].last_record_offset().unwrap().offset, 600);
    }

    /// Blocks respect max_cache_size: overflowing records carry into the next block.
    #[tokio::test]
    async fn batches_bounded_by_cache_size() {
        let items: Vec<RecoverResult> = (0..10).map(|i| result(1, i, 1, i * 100)).collect();
        let mut end_offsets = HashMap::from([(1u64, 0u64)]);
        let mut block_sizes = Vec::new();
        // Each record occupies well over 1 byte, so every block holds one record
        // (isFull triggers after the first put).
        recover(recover_stream(items), &mut end_offsets, 1, None, |block| {
            block_sizes.push(block.records()[&1].len());
            async { Ok(()) }
        })
        .await
        .unwrap();
        assert_eq!(block_sizes.len(), 10);
        assert!(block_sizes.iter().all(|&n| n == 1));
        assert_eq!(end_offsets[&1], 10);
    }
}

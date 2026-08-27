//! Kafka record-batch payload inspection: header fields plus the byte
//! positions the service needs for the base-offset rewrite on append.

use bytes::Bytes;
use kafka_protocol::records::{BatchDecodeInfo, RecordBatchDecoder};
use thiserror::Error;

/// Bytes before a batch's `batchLength` field: the 8-byte base offset.
const LENGTH_FIELD_OFFSET: usize = 8;
/// Bytes covered by base offset + batchLength, not counted by `batchLength`.
const BATCH_HEADER_PREFIX: usize = 12;

#[derive(Debug, Error)]
pub enum BatchParseError {
    #[error("empty batch")]
    Empty,
    #[error("truncated batch")]
    Truncated,
    #[error("protocol: {0}")]
    Protocol(String),
}

/// One record batch inside a produce payload.
#[derive(Debug, Clone)]
pub struct PayloadBatch {
    /// Byte position of the batch (its base-offset field) in the payload.
    pub payload_offset: usize,
    pub info: BatchDecodeInfo,
}

/// Decode every v2 batch in the payload, in order.
pub fn decode_batches(records: &Bytes) -> Result<Vec<PayloadBatch>, BatchParseError> {
    if records.is_empty() {
        return Err(BatchParseError::Empty);
    }
    let mut buf = records.clone();
    let infos = RecordBatchDecoder::decode_batch_info(&mut buf)
        .map_err(|error| BatchParseError::Protocol(error.to_string()))?;
    if infos.is_empty() {
        return Err(BatchParseError::Empty);
    }

    // Walk the wire layout for byte positions: each batch occupies
    // 12 header-prefix bytes plus `batchLength`.
    let bytes = records.as_ref();
    let mut offsets = Vec::with_capacity(infos.len());
    let mut pos = 0usize;
    while offsets.len() < infos.len() {
        if pos + BATCH_HEADER_PREFIX > bytes.len() {
            return Err(BatchParseError::Truncated);
        }
        let length_at = pos + LENGTH_FIELD_OFFSET;
        let batch_length =
            i32::from_be_bytes(bytes[length_at..length_at + 4].try_into().expect("4 bytes"));
        if batch_length < 0 {
            return Err(BatchParseError::Truncated);
        }
        let total = BATCH_HEADER_PREFIX + batch_length as usize;
        if pos + total > bytes.len() {
            return Err(BatchParseError::Truncated);
        }
        offsets.push(pos);
        pos += total;
    }

    Ok(offsets
        .into_iter()
        .zip(infos)
        .map(|(payload_offset, info)| PayloadBatch {
            payload_offset,
            info,
        })
        .collect())
}

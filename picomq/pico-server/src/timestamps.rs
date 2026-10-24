use crate::error::ServiceError;
use crate::log::Log;
use crate::record::{self, StreamRecord};
use crate::service::S3StreamService;
use crate::types::StreamBatch;

const READ_BYTES: usize = 4 * 1024 * 1024;

impl S3StreamService {
    pub async fn record_at_or_after(
        &self,
        log: &Log,
        timestamp_ms: i64,
    ) -> Result<Option<StreamRecord>, ServiceError> {
        let watermarks = self.watermarks(log).await?;
        let mut cursor = watermarks.log_start_offset;

        while cursor < watermarks.high_watermark {
            let read = self.read_batches(log, cursor, READ_BYTES).await?;
            if read.batches.is_empty() {
                break;
            }

            for batch in &read.batches {
                if max_timestamp(batch)? < timestamp_ms {
                    continue;
                }

                let found = live_records(batch, watermarks.log_start_offset)?
                    .into_iter()
                    .find(|record| record.record.timestamp_ms >= timestamp_ms);
                if found.is_some() {
                    return Ok(found);
                }
            }

            cursor = read.next_offset;
        }

        Ok(None)
    }

    pub async fn record_with_max_timestamp(
        &self,
        log: &Log,
    ) -> Result<Option<StreamRecord>, ServiceError> {
        let watermarks = self.watermarks(log).await?;
        let mut cursor = watermarks.log_start_offset;
        let mut newest: Option<StreamRecord> = None;

        while cursor < watermarks.high_watermark {
            let read = self.read_batches(log, cursor, READ_BYTES).await?;
            if read.batches.is_empty() {
                break;
            }

            for batch in &read.batches {
                let ceiling = max_timestamp(batch)?;
                if newest
                    .as_ref()
                    .is_some_and(|newest| ceiling <= newest.record.timestamp_ms)
                {
                    continue;
                }

                for record in live_records(batch, watermarks.log_start_offset)? {
                    if newest.as_ref().is_none_or(|newest| {
                        record.record.timestamp_ms > newest.record.timestamp_ms
                    }) {
                        newest = Some(record);
                    }
                }
            }

            cursor = read.next_offset;
        }

        Ok(newest)
    }
}

/// The newest timestamp in every Kafka batch the stored payload holds.
fn max_timestamp(batch: &StreamBatch) -> Result<i64, ServiceError> {
    Ok(record::batch_headers(&batch.payload)?
        .iter()
        .map(|header| header.max_timestamp_ms)
        .max()
        .unwrap_or(i64::MIN))
}

/// A stored batch can start below the log start after a trim, so records
/// before it are dropped.
fn live_records(batch: &StreamBatch, log_start: u64) -> Result<Vec<StreamRecord>, ServiceError> {
    Ok(record::decode_batches(&batch.payload)?
        .into_iter()
        .filter(|record| record.offset.record_offset() >= log_start)
        .collect())
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;
    use crate::record::{LogRecord, encode_batch};

    #[test]
    fn the_newest_timestamp_covers_every_batch_in_a_payload() {
        let old = encode_batch(
            0,
            -1,
            100,
            None,
            &[LogRecord::value(Bytes::from_static(b"old"))],
        );
        let new = encode_batch(
            1,
            -1,
            500,
            None,
            &[LogRecord::value(Bytes::from_static(b"new"))],
        );
        let batch = StreamBatch {
            base_offset: 0,
            last_offset: 2,
            count: 2,
            payload: Bytes::from([&old[..], &new[..]].concat()),
        };

        assert_eq!(max_timestamp(&batch).unwrap(), 500);
    }
}

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
                if record::batch_header(&batch.payload)?.max_timestamp_ms >= timestamp_ms {
                    return first_record(batch, |timestamp| timestamp >= timestamp_ms);
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
        let mut newest: Option<(i64, StreamBatch)> = None;

        while cursor < watermarks.high_watermark {
            let read = self.read_batches(log, cursor, READ_BYTES).await?;
            if read.batches.is_empty() {
                break;
            }

            for batch in read.batches {
                let timestamp = record::batch_header(&batch.payload)?.max_timestamp_ms;
                if newest.as_ref().is_none_or(|(max, _)| timestamp > *max) {
                    newest = Some((timestamp, batch));
                }
            }

            cursor = read.next_offset;
        }

        match newest {
            Some((max, batch)) => first_record(&batch, |timestamp| timestamp == max),
            None => Ok(None),
        }
    }
}

fn first_record(
    batch: &StreamBatch,
    matches: impl Fn(i64) -> bool,
) -> Result<Option<StreamRecord>, ServiceError> {
    Ok(record::decode_batches(&batch.payload)?
        .into_iter()
        .find(|record| matches(record.record.timestamp_ms)))
}

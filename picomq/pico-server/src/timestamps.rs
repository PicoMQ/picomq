use std::future::Future;

use picomq_metadata::ObjectSpan;

use crate::error::ServiceError;
use crate::log::Log;
use crate::record::{self, StreamRecord};
use crate::service::S3StreamService;
use crate::types::StreamBatch;

const READ_BYTES: usize = 4 * 1024 * 1024;
const PROBE_BYTES: usize = 1;

impl S3StreamService {
    pub async fn record_at_or_after(
        &self,
        log: &Log,
        timestamp_ms: i64,
    ) -> Result<Option<StreamRecord>, ServiceError> {
        let watermarks = self.watermarks(log).await?;
        let floor =
            written_from(&self.object_spans(log)?, timestamp_ms).max(watermarks.log_start_offset);
        let mut cursor = first_batch_reaching(
            floor,
            watermarks.high_watermark,
            timestamp_ms,
            |offset| async move {
                let probe = self.read_batches(log, offset, PROBE_BYTES).await?;
                Ok(probe.batches.into_iter().next())
            },
        )
        .await?;

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
        let start = watermarks.log_start_offset;
        let spans = self.object_spans(log)?;
        let committed = spans
            .last()
            .map_or(start, |span| span.end_offset)
            .max(start);

        let mut best = self
            .best_between(log, committed, watermarks.high_watermark, start, None)
            .await?;
        for span in spans.iter().rev() {
            if best
                .as_ref()
                .is_some_and(|best| span.last_write_ms < best.record.timestamp_ms)
            {
                continue;
            }

            let from = span.start_offset.max(start);
            best = self
                .best_between(log, from, span.end_offset, start, best)
                .await?;
        }

        Ok(best)
    }

    async fn best_between(
        &self,
        log: &Log,
        from: u64,
        to: u64,
        log_start: u64,
        mut best: Option<StreamRecord>,
    ) -> Result<Option<StreamRecord>, ServiceError> {
        let mut cursor = from;

        while cursor < to {
            let read = self.read_batches(log, cursor, READ_BYTES).await?;
            if read.batches.is_empty() {
                break;
            }

            for batch in &read.batches {
                let ceiling = max_timestamp(batch)?;
                if best
                    .as_ref()
                    .is_some_and(|best| ceiling < best.record.timestamp_ms)
                {
                    continue;
                }

                for record in live_records(batch, log_start)? {
                    let offset = record.offset.record_offset();
                    if offset < from || offset >= to {
                        continue;
                    }
                    if best.as_ref().is_none_or(|best| replaces(&record, best)) {
                        best = Some(record);
                    }
                }
            }

            cursor = read.next_offset;
        }

        Ok(best)
    }
}

fn replaces(record: &StreamRecord, best: &StreamRecord) -> bool {
    let (timestamp, offset) = (record.record.timestamp_ms, record.offset.record_offset());
    let (best_timestamp, best_offset) = (best.record.timestamp_ms, best.offset.record_offset());
    timestamp > best_timestamp || (timestamp == best_timestamp && offset < best_offset)
}

fn written_from(spans: &[ObjectSpan], timestamp_ms: i64) -> u64 {
    match spans.iter().find(|span| span.last_write_ms >= timestamp_ms) {
        Some(span) => span.start_offset,
        None => spans.last().map_or(0, |span| span.end_offset),
    }
}

async fn first_batch_reaching<F, R>(
    floor: u64,
    end: u64,
    timestamp_ms: i64,
    mut probe: F,
) -> Result<u64, ServiceError>
where
    F: FnMut(u64) -> R,
    R: Future<Output = Result<Option<StreamBatch>, ServiceError>>,
{
    let mut low = floor;
    let mut high = end;

    while low < high {
        let middle = low + (high - low) / 2;
        let Some(batch) = probe(middle).await? else {
            break;
        };

        let reaches = max_timestamp(&batch)? >= timestamp_ms;
        if !reaches && batch.last_offset > low {
            low = batch.last_offset;
        } else if reaches && batch.base_offset < high {
            high = batch.base_offset;
        } else {
            break;
        }
    }

    Ok(low.min(high).max(floor))
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

    fn batch_at(offset: u64, width: u64) -> StreamBatch {
        let base_offset = offset - offset % width;
        let records = vec![LogRecord::value(Bytes::from_static(b"r")); width as usize];
        StreamBatch {
            base_offset,
            last_offset: base_offset + width,
            count: width as u32,
            payload: encode_batch(base_offset, -1, base_offset as i64 * 10, None, &records),
        }
    }

    async fn seek(floor: u64, end: u64, timestamp_ms: i64, width: u64) -> (u64, usize) {
        let mut probes = 0;
        let found = first_batch_reaching(floor, end, timestamp_ms, |offset| {
            probes += 1;
            let batch = batch_at(offset, width);
            async move { Ok(Some(batch)) }
        })
        .await
        .unwrap();
        (found, probes)
    }

    fn span(start_offset: u64, end_offset: u64, last_write_ms: i64) -> ObjectSpan {
        ObjectSpan {
            start_offset,
            end_offset,
            bytes: 1,
            last_write_ms,
        }
    }

    #[tokio::test]
    async fn a_seek_halves_its_range_on_every_probe() {
        let (found, probes) = seek(0, 1_000_000, 7_000_005, 1).await;

        assert_eq!(found, 700_001);
        assert!(probes <= 21, "{probes} probes");
    }

    #[tokio::test]
    async fn a_seek_lands_on_the_first_batch_that_reaches_the_timestamp() {
        assert_eq!(seek(0, 30, 95, 3).await.0, 12);
        assert_eq!(seek(0, 30, 90, 3).await.0, 9);
        assert_eq!(seek(0, 30, 0, 3).await.0, 0);
        assert_eq!(seek(0, 30, 1_000, 3).await.0, 30);
        assert_eq!(seek(12, 30, 0, 3).await.0, 12);
    }

    #[test]
    fn spans_written_before_the_timestamp_are_skipped() {
        let spans = [span(0, 10, 100), span(10, 20, 200), span(20, 30, 300)];

        assert_eq!(written_from(&spans, 100), 0);
        assert_eq!(written_from(&spans, 150), 10);
        assert_eq!(written_from(&spans, 300), 20);
        assert_eq!(written_from(&spans, 301), 30);
        assert_eq!(written_from(&[], 301), 0);
    }
}

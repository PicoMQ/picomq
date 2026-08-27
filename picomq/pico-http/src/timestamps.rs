//! Server-assigned monotonic per-stream timestamps for the Pico protocol.
//!
//! `next` returns `max(now_ms, last + 1)`. The last timestamp is cached per
//! stream and lazily read back from the tail record's envelope after a
//! restart (any decode failure means "no history": 0).

use pico_common::now_ms;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;

use pico_protocol::envelope::decode_envelope_timestamp;
use pico_server::{OffsetToken, S3StreamService, ServiceError};

pub struct StreamTimestamps {
    service: Arc<S3StreamService>,
    last_by_name: Mutex<HashMap<String, i64>>,
}

impl StreamTimestamps {
    pub fn new(service: Arc<S3StreamService>) -> Self {
        Self {
            service,
            last_by_name: Mutex::new(HashMap::new()),
        }
    }

    pub async fn next(&self, name: &str) -> Result<i64, ServiceError> {
        let last = self
            .last_by_name
            .lock()
            .expect("timestamps poisoned")
            .get(name)
            .copied();
        let last = match last {
            Some(last) => last,
            None => self.read_back(name).await?,
        };
        Ok(now_ms().max(last + 1))
    }

    pub fn record(&self, name: &str, timestamp: i64) {
        let mut map = self.last_by_name.lock().expect("timestamps poisoned");
        let entry = map.entry(name.to_owned()).or_insert(timestamp);
        *entry = (*entry).max(timestamp);
    }

    pub fn invalidate(&self, name: &str) {
        self.last_by_name
            .lock()
            .expect("timestamps poisoned")
            .remove(name);
    }

    async fn read_back(&self, name: &str) -> Result<i64, ServiceError> {
        let Some(meta) = self.service.head(name).await? else {
            return Ok(0);
        };
        if meta.next_offset.record_offset() <= meta.start_offset.record_offset() {
            return Ok(0);
        }
        let tail = meta.next_offset.record_offset() - 1;
        let read = self
            .service
            .read(name, OffsetToken::of_record_offset(tail), 0, 1)
            .await?;
        Ok(read
            .records
            .last()
            .and_then(|record| decode_envelope_timestamp(&record.payload).ok())
            .unwrap_or(0))
    }
}

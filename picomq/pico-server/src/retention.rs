//! How much of a partition a topic keeps: data written within `ms`, and at
//! most `bytes` of it. Either limit may be unset. Retention removes whole
//! committed objects, oldest first, and never touches records still only in
//! the WAL.

use picomq_metadata::ObjectSpan;

use crate::error::{ErrorKind, ServiceError};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Retention {
    pub ms: Option<u64>,
    pub bytes: Option<u64>,
}

impl Retention {
    pub fn is_unlimited(&self) -> bool {
        self.ms.is_none() && self.bytes.is_none()
    }

    pub fn validate(&self) -> Result<(), ServiceError> {
        if self.ms == Some(0) || self.bytes == Some(0) {
            return Err(ServiceError::with_message(
                ErrorKind::BadRequest,
                None,
                false,
                "a retention limit must be positive",
            ));
        }

        Ok(())
    }

    /// The start offset that drops every expired object, given a partition's
    /// committed objects in offset order. `None` when nothing has expired.
    pub fn start_offset(&self, spans: &[ObjectSpan], now_ms: i64) -> Option<u64> {
        let mut kept = spans;

        if let Some(ms) = self.ms {
            let cutoff = now_ms.saturating_sub(i64::try_from(ms).unwrap_or(i64::MAX));
            let expired = kept
                .iter()
                .take_while(|span| span.last_write_ms < cutoff)
                .count();
            kept = &kept[expired..];
        }

        if let Some(limit) = self.bytes {
            let mut total: u64 = kept.iter().map(|span| span.bytes).sum();
            while let Some((oldest, rest)) = kept.split_first() {
                if total.saturating_sub(oldest.bytes) < limit {
                    break;
                }
                total -= oldest.bytes;
                kept = rest;
            }
        }

        let dropped = spans.len() - kept.len();
        dropped.checked_sub(1).map(|last| spans[last].end_offset)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn span(start_offset: u64, end_offset: u64, bytes: u64, last_write_ms: i64) -> ObjectSpan {
        ObjectSpan {
            start_offset,
            end_offset,
            bytes,
            last_write_ms,
        }
    }

    fn spans() -> Vec<ObjectSpan> {
        vec![
            span(0, 10, 100, 1_000),
            span(10, 20, 100, 2_000),
            span(20, 30, 100, 3_000),
        ]
    }

    #[test]
    fn time_drops_objects_written_before_the_cutoff() {
        let retention = Retention {
            ms: Some(1_500),
            bytes: None,
        };

        assert_eq!(retention.start_offset(&spans(), 3_600), Some(20));
        assert_eq!(retention.start_offset(&spans(), 2_600), Some(10));
        assert_eq!(retention.start_offset(&spans(), 2_000), None);
    }

    #[test]
    fn time_stops_at_the_first_object_that_has_not_expired() {
        let mut spans = spans();
        spans[1].last_write_ms = 9_000;
        let retention = Retention {
            ms: Some(1_000),
            bytes: None,
        };

        assert_eq!(retention.start_offset(&spans, 5_000), Some(10));
    }

    #[test]
    fn bytes_keep_the_newest_data_within_the_limit() {
        let retention = |bytes| Retention {
            ms: None,
            bytes: Some(bytes),
        };

        assert_eq!(retention(200).start_offset(&spans(), 0), Some(10));
        assert_eq!(retention(150).start_offset(&spans(), 0), Some(10));
        assert_eq!(retention(100).start_offset(&spans(), 0), Some(20));
        assert_eq!(retention(300).start_offset(&spans(), 0), None);
    }

    #[test]
    fn both_limits_drop_whatever_either_one_expires() {
        let retention = Retention {
            ms: Some(1_500),
            bytes: Some(100),
        };

        assert_eq!(retention.start_offset(&spans(), 2_600), Some(20));
    }

    #[test]
    fn no_limits_and_no_objects_keep_everything() {
        assert_eq!(Retention::default().start_offset(&spans(), i64::MAX), None);
        let retention = Retention {
            ms: Some(1),
            bytes: Some(1),
        };
        assert_eq!(retention.start_offset(&[], i64::MAX), None);
    }
}

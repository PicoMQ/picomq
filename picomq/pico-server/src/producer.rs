//! Numeric idempotent-producer state over verbatim batch appends. Semantics
//! and defaults match Kafka's broker so its clients dedup exactly.

use bytes::Buf;

use crate::error::ServiceError;
use crate::registry::RegistryEntry;
use crate::types::NumericProducer;

/// One accepted batch: its sequence range and where it landed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProducerSpan {
    pub first_seq: i32,
    pub last_seq: i32,
    pub base_offset: u64,
}

/// Dedup window, matching Kafka's `max.in.flight.requests.per.connection`.
pub const PRODUCER_SPAN_WINDOW: usize = 5;

/// Idle expiry, matching Kafka's `producer.id.expiration.ms` default.
pub const PRODUCER_EXPIRY_MS: i64 = 24 * 60 * 60 * 1000;

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct NumericProducerEntry {
    pub epoch: i16,
    pub spans: Vec<ProducerSpan>,
    pub last_touched_ms: i64,
}

impl NumericProducerEntry {
    pub fn record(&mut self, epoch: i16, span: ProducerSpan, now_ms: i64) {
        if epoch != self.epoch {
            self.epoch = epoch;
            self.spans.clear();
        }
        self.spans.push(span);
        if self.spans.len() > PRODUCER_SPAN_WINDOW {
            self.spans.remove(0);
        }
        self.last_touched_ms = now_ms;
    }
}

pub fn expire_producers(entry: &mut RegistryEntry, now_ms: i64) {
    entry
        .numeric_producers
        .retain(|_, state| now_ms - state.last_touched_ms <= PRODUCER_EXPIRY_MS);
}

/// `(producer_id, epoch, base_sequence, record_count)` from a stored batch
/// header, `None` when the payload carries no producer identity.
pub fn producer_identity(payload: &[u8]) -> Option<(i64, i16, i32, i32)> {
    const MAGIC_AT: usize = 16;
    const PRODUCER_ID_AT: usize = 43;
    const HEADER_LEN: usize = 61;
    if payload.len() < HEADER_LEN || payload[MAGIC_AT] != 2 {
        return None;
    }
    let mut buf = &payload[PRODUCER_ID_AT..];
    let producer_id = buf.get_i64();
    let epoch = buf.get_i16();
    let base_sequence = buf.get_i32();
    let record_count = buf.get_i32();
    (producer_id >= 0 && base_sequence >= 0 && record_count > 0).then_some((
        producer_id,
        epoch,
        base_sequence,
        record_count,
    ))
}

/// Sequences occupy `[0, i32::MAX]` and wrap to 0.
pub fn next_seq(seq: i32, increment: u32) -> i32 {
    let increment = increment as i64;
    let wrapped = (seq as i64 + increment) % (i32::MAX as i64 + 1);
    wrapped as i32
}

pub(crate) enum Admission {
    Accepted(Option<(NumericProducer, i32)>),
    Duplicate { base_offset: u64 },
}

pub(crate) fn admit(
    entry: &mut RegistryEntry,
    producer: Option<NumericProducer>,
    record_count: u32,
    now_ms: i64,
) -> Result<Admission, ServiceError> {
    let Some(producer) = producer else {
        return Ok(Admission::Accepted(None));
    };
    let last_seq = next_seq(producer.first_seq, record_count.saturating_sub(1));
    match validate(
        entry,
        producer.id,
        producer.epoch,
        producer.first_seq,
        last_seq,
    ) {
        Decision::Accepted => Ok(Admission::Accepted(Some((producer, last_seq)))),
        Decision::Duplicate { base_offset } => {
            if let Some(state) = entry.numeric_producers.get_mut(&producer.id) {
                state.last_touched_ms = now_ms;
            }
            Ok(Admission::Duplicate { base_offset })
        }
        Decision::Fenced { current_epoch } => Err(ServiceError::fenced(current_epoch as u64)),
        Decision::OutOfOrder { expected, received } => {
            Err(ServiceError::sequence_gap(expected as u64, received as u64))
        }
    }
}

pub(crate) fn record(
    entry: &mut RegistryEntry,
    accepted: Option<(NumericProducer, i32)>,
    base_offset: u64,
    now_ms: i64,
) {
    let Some((producer, last_seq)) = accepted else {
        return;
    };
    entry
        .numeric_producers
        .entry(producer.id)
        .or_default()
        .record(
            producer.epoch,
            ProducerSpan {
                first_seq: producer.first_seq,
                last_seq,
                base_offset,
            },
            now_ms,
        );
}

pub(crate) fn fold_stored_batch(
    entry: &mut RegistryEntry,
    payload: &[u8],
    base_offset: u64,
    now_ms: i64,
) {
    let Some((producer_id, epoch, base_sequence, record_count)) = producer_identity(payload) else {
        return;
    };
    entry
        .numeric_producers
        .entry(producer_id)
        .or_default()
        .record(
            epoch,
            ProducerSpan {
                first_seq: base_sequence,
                last_seq: next_seq(base_sequence, (record_count as u32).saturating_sub(1)),
                base_offset,
            },
            now_ms,
        );
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Decision {
    Accepted,
    Duplicate { base_offset: u64 },
    OutOfOrder { expected: i32, received: i32 },
    Fenced { current_epoch: i16 },
}

/// Decision table per Kafka's ProducerStateManager.
fn validate(
    entry: &RegistryEntry,
    producer_id: i64,
    epoch: i16,
    first_seq: i32,
    last_seq: i32,
) -> Decision {
    let Some(state) = entry.numeric_producers.get(&producer_id) else {
        if first_seq != 0 {
            return Decision::OutOfOrder {
                expected: 0,
                received: first_seq,
            };
        }
        return Decision::Accepted;
    };
    if epoch < state.epoch {
        return Decision::Fenced {
            current_epoch: state.epoch,
        };
    }
    if epoch > state.epoch {
        if first_seq != 0 {
            return Decision::OutOfOrder {
                expected: 0,
                received: first_seq,
            };
        }
        return Decision::Accepted;
    }
    if let Some(span) = state
        .spans
        .iter()
        .find(|s| s.first_seq == first_seq && s.last_seq == last_seq)
    {
        return Decision::Duplicate {
            base_offset: span.base_offset,
        };
    }
    let expected = state
        .spans
        .last()
        .map(|s| next_seq(s.last_seq, 1))
        .unwrap_or(0);
    if first_seq == expected {
        return Decision::Accepted;
    }
    Decision::OutOfOrder {
        expected,
        received: first_seq,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn entry() -> RegistryEntry {
        RegistryEntry {
            stream_id: 7,
            content_type: "application/octet-stream".into(),
            ttl_seconds: None,
            expires_at_ms: None,
            closed: false,
            deadline_ms: 0,
            last_seq: None,
            producers: BTreeMap::new(),
            closed_by: None,
            external_id: [0; 16],
            numeric_producers: BTreeMap::new(),
            producer_state_offset: 0,
        }
    }

    #[test]
    fn decision_table() {
        let mut e = entry();

        assert_eq!(validate(&e, 1, 0, 0, 2), Decision::Accepted);
        assert_eq!(
            validate(&e, 1, 0, 3, 5),
            Decision::OutOfOrder {
                expected: 0,
                received: 3
            }
        );

        let mut state = NumericProducerEntry::default();
        state.record(
            2,
            ProducerSpan {
                first_seq: 0,
                last_seq: 4,
                base_offset: 10,
            },
            1,
        );
        state.record(
            2,
            ProducerSpan {
                first_seq: 5,
                last_seq: 9,
                base_offset: 15,
            },
            1,
        );
        e.numeric_producers.insert(1, state);

        assert_eq!(
            validate(&e, 1, 1, 0, 0),
            Decision::Fenced { current_epoch: 2 }
        );
        assert_eq!(validate(&e, 1, 3, 0, 0), Decision::Accepted);
        assert_eq!(
            validate(&e, 1, 3, 1, 1),
            Decision::OutOfOrder {
                expected: 0,
                received: 1
            }
        );
        assert_eq!(
            validate(&e, 1, 2, 0, 4),
            Decision::Duplicate { base_offset: 10 }
        );
        assert_eq!(
            validate(&e, 1, 2, 5, 9),
            Decision::Duplicate { base_offset: 15 }
        );
        assert_eq!(validate(&e, 1, 2, 10, 12), Decision::Accepted);
        assert_eq!(
            validate(&e, 1, 2, 12, 14),
            Decision::OutOfOrder {
                expected: 10,
                received: 12
            }
        );
        // Partial overlap of a remembered span is not a duplicate.
        assert_eq!(
            validate(&e, 1, 2, 5, 7),
            Decision::OutOfOrder {
                expected: 10,
                received: 5
            }
        );
    }

    #[test]
    fn span_window_and_epoch_reset() {
        let mut state = NumericProducerEntry::default();
        for i in 0..7_i32 {
            state.record(
                1,
                ProducerSpan {
                    first_seq: i,
                    last_seq: i,
                    base_offset: i as u64,
                },
                1,
            );
        }
        assert_eq!(state.spans.len(), PRODUCER_SPAN_WINDOW);
        assert_eq!(state.spans.first().unwrap().first_seq, 2);
        assert_eq!(state.spans.last().unwrap().first_seq, 6);

        // A new epoch clears the window.
        state.record(
            2,
            ProducerSpan {
                first_seq: 0,
                last_seq: 3,
                base_offset: 100,
            },
            1,
        );
        assert_eq!(state.epoch, 2);
        assert_eq!(state.spans.len(), 1);
    }

    #[test]
    fn expiry_drops_only_idle_producers() {
        let mut e = entry();
        let mut fresh = NumericProducerEntry::default();
        fresh.record(
            0,
            ProducerSpan {
                first_seq: 0,
                last_seq: 0,
                base_offset: 0,
            },
            1_000,
        );
        let mut stale = NumericProducerEntry::default();
        stale.record(
            0,
            ProducerSpan {
                first_seq: 0,
                last_seq: 0,
                base_offset: 0,
            },
            0,
        );
        e.numeric_producers.insert(1, fresh);
        e.numeric_producers.insert(2, stale);

        expire_producers(&mut e, PRODUCER_EXPIRY_MS + 500);
        assert!(e.numeric_producers.contains_key(&1));
        assert!(!e.numeric_producers.contains_key(&2));
    }

    #[test]
    fn seq_wraps_at_i32_max() {
        assert_eq!(next_seq(0, 5), 5);
        assert_eq!(next_seq(i32::MAX, 1), 0);
        assert_eq!(next_seq(i32::MAX - 1, 3), 1);
    }
}

use std::collections::BTreeMap;
use std::fmt::Write;

use bytes::{BufMut, Bytes, BytesMut};

use crate::codec::{
    expect_end, expect_version, get_i16, get_i32, get_i64, get_opt_str, get_str, get_u8,
    put_opt_str, put_str,
};
use crate::error::ServiceError;
use crate::partition::Partition;
use crate::producer::{NumericProducerEntry, PRODUCER_SPAN_WINDOW, ProducerSpan};
use crate::types::Producer;

const VERSION: u8 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Log {
    Partition(Partition),
    Group(String),
}

impl Log {
    pub fn key(&self) -> String {
        match self {
            Log::Partition(partition) => {
                format!(
                    "log/partition/{}/{}",
                    partition.topic.hex(),
                    partition.index
                )
            }
            Log::Group(group_id) => {
                let mut key = String::from("log/group/");
                for byte in group_id.as_bytes() {
                    write!(key, "{byte:02x}").expect("write to String");
                }
                key
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProducerState {
    pub epoch: u64,
    pub last_seq: u64,
    pub last_touched_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogEntry {
    pub stream_id: u64,
    pub last_seq: Option<String>,
    pub producers: BTreeMap<String, ProducerState>,
    pub numeric_producers: BTreeMap<i64, NumericProducerEntry>,
    pub producer_state_offset: u64,
}

impl LogEntry {
    pub fn new(stream_id: u64) -> Self {
        Self {
            stream_id,
            last_seq: None,
            producers: BTreeMap::new(),
            numeric_producers: BTreeMap::new(),
            producer_state_offset: 0,
        }
    }

    pub fn with_last_seq(mut self, seq: String) -> Self {
        self.last_seq = Some(seq);
        self
    }

    pub fn with_producer(mut self, id: String, epoch: u64, seq: u64, now_ms: i64) -> Self {
        self.producers.insert(
            id,
            ProducerState {
                epoch,
                last_seq: seq,
                last_touched_ms: now_ms,
            },
        );
        self
    }

    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::new();
        buf.put_u8(VERSION);
        buf.put_u64(self.stream_id);
        put_opt_str(&mut buf, self.last_seq.as_deref());

        buf.put_i32(self.producers.len() as i32);
        for (id, state) in &self.producers {
            put_str(&mut buf, id);
            buf.put_u64(state.epoch);
            buf.put_u64(state.last_seq);
            buf.put_i64(state.last_touched_ms);
        }

        buf.put_i32(self.numeric_producers.len() as i32);
        for (id, entry) in &self.numeric_producers {
            buf.put_i64(*id);
            buf.put_i16(entry.epoch);
            buf.put_i64(entry.last_touched_ms);
            buf.put_u8(entry.spans.len() as u8);
            for span in &entry.spans {
                buf.put_i32(span.first_seq);
                buf.put_i32(span.last_seq);
                buf.put_u64(span.base_offset);
            }
        }

        buf.put_u64(self.producer_state_offset);
        buf.freeze()
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, ServiceError> {
        let mut buf = bytes;
        expect_version(&mut buf, VERSION, "log entry")?;

        let stream_id = get_i64(&mut buf)? as u64;
        let last_seq = get_opt_str(&mut buf)?;

        let mut producers = BTreeMap::new();
        for _ in 0..get_i32(&mut buf)? {
            let id = get_str(&mut buf)?;
            let state = ProducerState {
                epoch: get_i64(&mut buf)? as u64,
                last_seq: get_i64(&mut buf)? as u64,
                last_touched_ms: get_i64(&mut buf)?,
            };
            producers.insert(id, state);
        }

        let mut numeric_producers = BTreeMap::new();
        for _ in 0..get_i32(&mut buf)? {
            let id = get_i64(&mut buf)?;
            let epoch = get_i16(&mut buf)?;
            let last_touched_ms = get_i64(&mut buf)?;
            let span_count = get_u8(&mut buf)? as usize;

            let mut spans = Vec::with_capacity(span_count.min(PRODUCER_SPAN_WINDOW));
            for _ in 0..span_count {
                spans.push(ProducerSpan {
                    first_seq: get_i32(&mut buf)?,
                    last_seq: get_i32(&mut buf)?,
                    base_offset: get_i64(&mut buf)? as u64,
                });
            }

            numeric_producers.insert(
                id,
                NumericProducerEntry {
                    epoch,
                    spans,
                    last_touched_ms,
                },
            );
        }

        let producer_state_offset = get_i64(&mut buf)? as u64;
        expect_end(buf, "log entry")?;

        Ok(Self {
            stream_id,
            last_seq,
            producers,
            numeric_producers,
            producer_state_offset,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProducerDecision {
    Accepted { epoch: u64, seq: u64 },
    Duplicate { last_seq: u64 },
    StaleEpoch { current_epoch: u64 },
    InvalidEpochSeq,
    SequenceGap { expected: u64, received: u64 },
}

pub fn validate_producer(entry: &LogEntry, producer: &Producer) -> ProducerDecision {
    let Some(state) = entry.producers.get(&producer.producer_id) else {
        if producer.seq != 0 {
            return ProducerDecision::InvalidEpochSeq;
        }
        return ProducerDecision::Accepted {
            epoch: producer.epoch,
            seq: producer.seq,
        };
    };

    if producer.epoch < state.epoch {
        return ProducerDecision::StaleEpoch {
            current_epoch: state.epoch,
        };
    }
    if producer.epoch > state.epoch {
        if producer.seq != 0 {
            return ProducerDecision::InvalidEpochSeq;
        }
        return ProducerDecision::Accepted {
            epoch: producer.epoch,
            seq: producer.seq,
        };
    }

    if producer.seq <= state.last_seq {
        return ProducerDecision::Duplicate {
            last_seq: state.last_seq,
        };
    }
    if producer.seq == state.last_seq + 1 {
        return ProducerDecision::Accepted {
            epoch: producer.epoch,
            seq: producer.seq,
        };
    }

    ProducerDecision::SequenceGap {
        expected: state.last_seq + 1,
        received: producer.seq,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::topic::TopicId;

    fn entry() -> LogEntry {
        LogEntry {
            stream_id: 7,
            last_seq: Some("seq-9".into()),
            producers: BTreeMap::from([
                (
                    "p1".into(),
                    ProducerState {
                        epoch: 2,
                        last_seq: 41,
                        last_touched_ms: 500,
                    },
                ),
                (
                    "p2".into(),
                    ProducerState {
                        epoch: 0,
                        last_seq: 0,
                        last_touched_ms: 600,
                    },
                ),
            ]),
            numeric_producers: BTreeMap::from([(
                9000_i64,
                NumericProducerEntry {
                    epoch: 3,
                    spans: vec![
                        ProducerSpan {
                            first_seq: 0,
                            last_seq: 4,
                            base_offset: 100,
                        },
                        ProducerSpan {
                            first_seq: 5,
                            last_seq: 5,
                            base_offset: 105,
                        },
                    ],
                    last_touched_ms: 777,
                },
            )]),
            producer_state_offset: 106,
        }
    }

    #[test]
    fn keys_are_distinct_per_log() {
        let topic = TopicId(*b"0123456789abcdef");
        assert_eq!(
            Log::Partition(Partition::new(topic, 2)).key(),
            "log/partition/30313233343536373839616263646566/2"
        );
        assert_eq!(Log::Group("g".into()).key(), "log/group/67");
        assert_ne!(
            Log::Group("a/b".into()).key(),
            Log::Group("a%2fb".into()).key()
        );
    }

    #[test]
    fn roundtrips() {
        for value in [entry(), LogEntry::new(3)] {
            assert_eq!(LogEntry::decode(&value.encode()).unwrap(), value);
        }
    }

    #[test]
    fn rejects_truncation_and_unknown_versions() {
        let encoded = entry().encode();
        for len in 0..encoded.len() {
            assert!(
                LogEntry::decode(&encoded[..len]).is_err(),
                "truncated at {len}"
            );
        }

        let mut bytes = encoded.to_vec();
        bytes[0] = 9;
        assert!(LogEntry::decode(&bytes).is_err());
    }

    #[test]
    fn producer_decision_table() {
        let p = |epoch, seq| Producer {
            producer_id: "p".into(),
            epoch,
            seq,
        };
        let mut e = LogEntry::new(1);

        assert_eq!(
            validate_producer(&e, &p(3, 0)),
            ProducerDecision::Accepted { epoch: 3, seq: 0 }
        );
        assert_eq!(
            validate_producer(&e, &p(3, 1)),
            ProducerDecision::InvalidEpochSeq
        );

        e.producers.insert(
            "p".into(),
            ProducerState {
                epoch: 3,
                last_seq: 5,
                last_touched_ms: 0,
            },
        );
        assert_eq!(
            validate_producer(&e, &p(2, 0)),
            ProducerDecision::StaleEpoch { current_epoch: 3 }
        );
        assert_eq!(
            validate_producer(&e, &p(4, 0)),
            ProducerDecision::Accepted { epoch: 4, seq: 0 }
        );
        assert_eq!(
            validate_producer(&e, &p(4, 1)),
            ProducerDecision::InvalidEpochSeq
        );
        assert_eq!(
            validate_producer(&e, &p(3, 5)),
            ProducerDecision::Duplicate { last_seq: 5 }
        );
        assert_eq!(
            validate_producer(&e, &p(3, 6)),
            ProducerDecision::Accepted { epoch: 3, seq: 6 }
        );
        assert_eq!(
            validate_producer(&e, &p(3, 8)),
            ProducerDecision::SequenceGap {
                expected: 6,
                received: 8
            }
        );
    }
}

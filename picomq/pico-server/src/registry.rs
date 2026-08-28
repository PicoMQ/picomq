//! Named-stream registry entry: the value stored in the metadata KV under
//! the stream's path name.

use std::collections::BTreeMap;

use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::error::{ErrorKind, ServiceError};
use crate::producer::{NumericProducerEntry, ProducerSpan, PRODUCER_SPAN_WINDOW};
use crate::types::Producer;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProducerState {
    pub epoch: u64,
    pub last_seq: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClosedBy {
    pub producer_id: String,
    pub epoch: u64,
    pub seq: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegistryEntry {
    pub stream_id: u64,
    pub content_type: String,
    pub ttl_seconds: Option<u64>,
    pub expires_at_ms: Option<i64>,
    pub closed: bool,
    pub deadline_ms: i64,
    pub last_seq: Option<String>,
    pub producers: BTreeMap<String, ProducerState>,
    pub closed_by: Option<ClosedBy>,
    /// Caller-assigned external identity (e.g. the Kafka topic UUID), set at
    /// create time. All zeros when the creating frontend has no such notion.
    pub external_id: [u8; 16],
    pub numeric_producers: BTreeMap<i64, NumericProducerEntry>,
    /// Offset up to which `numeric_producers` is known complete.
    pub producer_state_offset: u64,
}

impl RegistryEntry {
    pub fn close(mut self, by: Option<ClosedBy>) -> Self {
        self.closed = true;
        if by.is_some() {
            self.closed_by = by;
        }
        self
    }

    pub fn with_last_seq(mut self, seq: String) -> Self {
        self.last_seq = Some(seq);
        self
    }

    pub fn with_deadline(mut self, deadline_ms: i64) -> Self {
        self.deadline_ms = deadline_ms;
        self
    }

    pub fn with_producer(mut self, id: String, epoch: u64, seq: u64) -> Self {
        self.producers.insert(
            id,
            ProducerState {
                epoch,
                last_seq: seq,
            },
        );
        self
    }

    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::new();
        buf.put_u8(1);
        buf.put_i64(self.stream_id as i64);
        put_str(&mut buf, &self.content_type);
        buf.put_u8(self.ttl_seconds.is_some() as u8);
        buf.put_i64(self.ttl_seconds.unwrap_or(0) as i64);
        buf.put_u8(self.expires_at_ms.is_some() as u8);
        buf.put_i64(self.expires_at_ms.unwrap_or(0));
        buf.put_u8(self.closed as u8);
        buf.put_i64(self.deadline_ms);
        put_str(&mut buf, self.last_seq.as_deref().unwrap_or(""));
        buf.put_u8(self.closed_by.is_some() as u8);
        if let Some(closed_by) = &self.closed_by {
            put_str(&mut buf, &closed_by.producer_id);
            buf.put_i64(closed_by.epoch as i64);
            buf.put_i64(closed_by.seq as i64);
        }
        buf.put_i32(self.producers.len() as i32);
        for (id, state) in &self.producers {
            put_str(&mut buf, id);
            buf.put_i64(state.epoch as i64);
            buf.put_i64(state.last_seq as i64);
        }
        buf.put_slice(&self.external_id);
        buf.put_i32(self.numeric_producers.len() as i32);
        for (id, entry) in &self.numeric_producers {
            buf.put_i64(*id);
            buf.put_i16(entry.epoch);
            buf.put_i64(entry.last_touched_ms);
            buf.put_u8(entry.spans.len() as u8);
            for span in &entry.spans {
                buf.put_i32(span.first_seq);
                buf.put_i32(span.last_seq);
                buf.put_i64(span.base_offset as i64);
            }
        }
        buf.put_i64(self.producer_state_offset as i64);
        buf.freeze()
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, ServiceError> {
        let corrupt = |m: String| ServiceError::with_message(ErrorKind::BadRequest, None, false, m);
        let mut buf = bytes;
        let version = get_u8(&mut buf)?;
        if version != 1 {
            return Err(corrupt(format!("unknown registry entry version {version}")));
        }
        let stream_id = get_i64(&mut buf)? as u64;
        let content_type = get_str(&mut buf)?;
        let ttl_flag = get_u8(&mut buf)? == 1;
        let ttl_raw = get_i64(&mut buf)?;
        let ttl_seconds = ttl_flag.then_some(ttl_raw as u64);
        let expires_flag = get_u8(&mut buf)? == 1;
        let expires_raw = get_i64(&mut buf)?;
        let expires_at_ms = expires_flag.then_some(expires_raw);
        let closed = get_u8(&mut buf)? == 1;
        let deadline_ms = get_i64(&mut buf)?;
        let last_seq_raw = get_str(&mut buf)?;
        let last_seq = (!last_seq_raw.is_empty()).then_some(last_seq_raw);
        let closed_by = if get_u8(&mut buf)? == 1 {
            let producer_id = get_str(&mut buf)?;
            let epoch = get_i64(&mut buf)? as u64;
            let seq = get_i64(&mut buf)? as u64;
            Some(ClosedBy {
                producer_id,
                epoch,
                seq,
            })
        } else {
            None
        };
        let count = get_i32(&mut buf)?;
        let mut producers = BTreeMap::new();
        for _ in 0..count {
            let id = get_str(&mut buf)?;
            let epoch = get_i64(&mut buf)? as u64;
            let last_seq = get_i64(&mut buf)? as u64;
            producers.insert(id, ProducerState { epoch, last_seq });
        }
        let mut external_id = [0u8; 16];
        ensure(buf, 16)?;
        external_id.copy_from_slice(&buf[..16]);
        buf.advance(16);
        let numeric_count = get_i32(&mut buf)?;
        let mut numeric_producers = BTreeMap::new();
        for _ in 0..numeric_count {
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
        Ok(Self {
            stream_id,
            content_type,
            ttl_seconds,
            expires_at_ms,
            closed,
            deadline_ms,
            last_seq,
            producers,
            closed_by,
            external_id,
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

pub fn validate_producer(entry: &RegistryEntry, producer: &Producer) -> ProducerDecision {
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

fn put_str(buf: &mut BytesMut, s: &str) {
    buf.put_i32(s.len() as i32);
    buf.put_slice(s.as_bytes());
}

fn get_u8(buf: &mut &[u8]) -> Result<u8, ServiceError> {
    ensure(buf, 1)?;
    Ok(buf.get_u8())
}

fn get_i16(buf: &mut &[u8]) -> Result<i16, ServiceError> {
    ensure(buf, 2)?;
    Ok(buf.get_i16())
}

fn get_i32(buf: &mut &[u8]) -> Result<i32, ServiceError> {
    ensure(buf, 4)?;
    Ok(buf.get_i32())
}

fn get_i64(buf: &mut &[u8]) -> Result<i64, ServiceError> {
    ensure(buf, 8)?;
    Ok(buf.get_i64())
}

fn get_str(buf: &mut &[u8]) -> Result<String, ServiceError> {
    let len = get_i32(buf)?;
    if len < 0 {
        return Err(corrupt("negative length"));
    }
    ensure(buf, len as usize)?;
    let s =
        String::from_utf8(buf[..len as usize].to_vec()).map_err(|_| corrupt("invalid UTF-8"))?;
    buf.advance(len as usize);
    Ok(s)
}

fn ensure(buf: &[u8], n: usize) -> Result<(), ServiceError> {
    if buf.remaining() < n {
        return Err(corrupt("registry entry truncated"));
    }
    Ok(())
}

fn corrupt(m: &str) -> ServiceError {
    ServiceError::with_message(ErrorKind::BadRequest, None, false, m)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry() -> RegistryEntry {
        RegistryEntry {
            stream_id: 7,
            content_type: "application/json".into(),
            ttl_seconds: Some(60),
            expires_at_ms: None,
            closed: false,
            deadline_ms: 12345,
            last_seq: Some("seq-9".into()),
            producers: BTreeMap::from([
                (
                    "p1".into(),
                    ProducerState {
                        epoch: 2,
                        last_seq: 41,
                    },
                ),
                (
                    "p2".into(),
                    ProducerState {
                        epoch: 0,
                        last_seq: 0,
                    },
                ),
            ]),
            closed_by: Some(ClosedBy {
                producer_id: "p1".into(),
                epoch: 2,
                seq: 41,
            }),
            external_id: *b"0123456789abcdef",
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
    fn roundtrips() {
        for e in [
            entry(),
            RegistryEntry {
                ttl_seconds: None,
                expires_at_ms: Some(999),
                last_seq: None,
                closed_by: None,
                producers: BTreeMap::new(),
                closed: true,
                external_id: [0; 16],
                numeric_producers: BTreeMap::new(),
                ..entry()
            },
        ] {
            let encoded = e.encode();
            assert_eq!(RegistryEntry::decode(&encoded).unwrap(), e);
        }
    }

    #[test]
    fn rejects_unknown_version_and_truncation() {
        let mut bytes = entry().encode().to_vec();
        bytes[0] = 9;
        assert!(RegistryEntry::decode(&bytes).is_err());

        let encoded = entry().encode();
        for len in 0..encoded.len() {
            assert!(
                RegistryEntry::decode(&encoded[..len]).is_err(),
                "truncated at {len}"
            );
        }
    }

    #[test]
    fn producer_decision_table() {
        let p = |epoch, seq| Producer {
            producer_id: "p".into(),
            epoch,
            seq,
        };
        let mut e = entry();
        e.producers.clear();

        // Unknown producer: first seq must be 0.
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
            },
        );
        // Stale epoch.
        assert_eq!(
            validate_producer(&e, &p(2, 0)),
            ProducerDecision::StaleEpoch { current_epoch: 3 }
        );
        // New epoch restarts at 0.
        assert_eq!(
            validate_producer(&e, &p(4, 0)),
            ProducerDecision::Accepted { epoch: 4, seq: 0 }
        );
        assert_eq!(
            validate_producer(&e, &p(4, 1)),
            ProducerDecision::InvalidEpochSeq
        );
        // Same epoch: duplicate / next / gap.
        assert_eq!(
            validate_producer(&e, &p(3, 5)),
            ProducerDecision::Duplicate { last_seq: 5 }
        );
        assert_eq!(
            validate_producer(&e, &p(3, 4)),
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

    #[test]
    fn close_keeps_existing_closed_by() {
        let e = entry();
        let closed = e.clone().close(None);
        assert!(closed.closed);
        assert_eq!(closed.closed_by, e.closed_by);

        let by = ClosedBy {
            producer_id: "px".into(),
            epoch: 9,
            seq: 1,
        };
        assert_eq!(e.close(Some(by.clone())).closed_by, Some(by));
    }
}

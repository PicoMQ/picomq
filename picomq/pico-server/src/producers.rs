use std::collections::BTreeMap;

use bytes::{BufMut, Bytes, BytesMut};
use s3stream::{FetchContext, Stream};

use crate::codec::{
    expect_end, expect_version, get_i16, get_i32, get_i64, get_opt_u64, get_str, get_u8, get_u32,
    get_u64, put_opt_u64, put_str,
};
use crate::error::{ErrorKind, ServiceError};
use crate::record;
use crate::service::live_start_offset;
use crate::types::{NumericProducer, Producer};

const CHECKPOINT_VERSION: u8 = 1;
const SESSION_VERSION: u8 = 1;
const STREAM_SEQ_VERSION: u8 = 1;
const REPLAY_BYTES: usize = 8 * 1024 * 1024;
const SEQ_SPACE: u64 = 1 << 31;

/// Checkpoint cadence in offsets. Replay from the last checkpoint covers the
/// rest, so a lost checkpoint only costs replay time.
pub const CHECKPOINT_INTERVAL: u64 = 4096;

/// Dedup window, matching Kafka's `max.in.flight.requests.per.connection`.
pub const SPAN_WINDOW: usize = 5;

/// Idle expiry, matching Kafka's `producer.id.expiration.ms` default.
pub const EXPIRY_MS: i64 = 24 * 60 * 60 * 1000;

pub const MAX_TRACKED: usize = 1024;

pub const ID_BLOCK: u32 = 256;

pub fn checkpoint_key(stream_id: u64) -> String {
    format!("producers/{stream_id}")
}

pub fn stream_seq_key(stream_id: u64) -> String {
    format!("stream_seq/{stream_id}")
}

pub fn session_prefix(stream_id: u64) -> String {
    format!("producer/{stream_id}/")
}

pub fn session_key(stream_id: u64, producer_id: &str) -> String {
    format!("{}{}", session_prefix(stream_id), hex::encode(producer_id))
}

#[derive(Debug, Default)]
pub struct IdLease {
    next: u64,
    remaining: u32,
}

impl IdLease {
    pub fn take(&mut self) -> Option<i64> {
        if self.remaining == 0 {
            return None;
        }
        let id = self.next as i64;
        self.next += 1;
        self.remaining -= 1;
        Some(id)
    }

    pub fn refill(&mut self, first: u64) {
        self.next = first;
        self.remaining = ID_BLOCK;
    }
}

/// An HTTP producer's current epoch and the numeric id its batches carry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Session {
    pub epoch: u64,
    pub id: i64,
}

pub fn encode_session(session: Session) -> Bytes {
    let mut buf = BytesMut::with_capacity(17);
    buf.put_u8(SESSION_VERSION);
    buf.put_u64(session.epoch);
    buf.put_i64(session.id);
    buf.freeze()
}

pub fn decode_session(value: &[u8]) -> Result<Session, ServiceError> {
    let mut buf = value;
    expect_version(&mut buf, SESSION_VERSION, "producer session")?;
    let session = Session {
        epoch: get_u64(&mut buf)?,
        id: get_i64(&mut buf)?,
    };
    expect_end(buf, "producer session")?;

    Ok(session)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Span {
    pub first_seq: i32,
    pub last_seq: i32,
    pub base_offset: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Kafka {
    pub epoch: i16,
    pub spans: Vec<Span>,
    pub last_touched_ms: i64,
}

impl Kafka {
    fn record(&mut self, epoch: i16, span: Span, now_ms: i64) {
        if epoch != self.epoch {
            self.epoch = epoch;
            self.spans.clear();
        }
        self.spans.push(span);
        if self.spans.len() > SPAN_WINDOW {
            self.spans.remove(0);
        }
        self.last_touched_ms = now_ms;
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Http {
    pub last_seq: Option<u64>,
    pub last_touched_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Tracked {
    Kafka(Kafka),
    Http(Http),
}

impl Tracked {
    fn last_touched_ms(&self) -> i64 {
        match self {
            Tracked::Kafka(kafka) => kafka.last_touched_ms,
            Tracked::Http(http) => http.last_touched_ms,
        }
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

pub(crate) enum Admission {
    Accepted(Option<(NumericProducer, i32)>),
    Duplicate { base_offset: u64 },
}

/// Values accepted in order, each with the offset its append started at.
/// A value is durable once its append is, so a crash or a failed write can
/// only ever lose a suffix of the pending values.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Ledger<T> {
    durable: Option<T>,
    pending: Vec<(T, u64)>,
}

impl<T: Clone + Ord> Ledger<T> {
    fn last(&self) -> Option<&T> {
        self.pending
            .last()
            .map(|(value, _)| value)
            .or(self.durable.as_ref())
    }

    fn accept(&mut self, value: T, landed: Option<u64>) {
        match landed {
            Some(base_offset) => self.pending.push((value, base_offset)),
            None => self.durable = Some(value),
        }
    }

    fn confirm(&mut self, durable_end: u64) {
        let durable = self
            .pending
            .iter()
            .take_while(|(_, base_offset)| *base_offset < durable_end)
            .count();
        if let Some((value, _)) = self.pending.drain(..durable).last() {
            self.durable = Some(value);
        }
    }

    fn settle(&mut self, durable_end: u64) {
        self.pending
            .retain(|(_, base_offset)| *base_offset < durable_end);
        self.confirm(durable_end);
    }

    fn raise(&mut self, floor: T) {
        if self.durable.as_ref() < Some(&floor) {
            self.durable = Some(floor);
        }
        let floor = self.durable.clone();
        self.pending
            .retain(|(value, _)| Some(value) > floor.as_ref());
    }
}

/// The DS `Stream-Seq` of one stream, with the row bytes this owner last
/// wrote or loaded for its compare and set.
#[derive(Debug, Clone, PartialEq, Eq)]
struct StreamSeq {
    ledger: Ledger<String>,
    written: Option<Bytes>,
}

/// The idempotent producers of one stream, as its owner at one epoch knows
/// them. Kafka producers and HTTP sessions alike are rebuilt from the batches
/// in the log past `covered`. Only the stream sequence lives nowhere but its
/// row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Producers {
    epoch: u64,
    covered: u64,
    tracked: BTreeMap<i64, Tracked>,
    sessions: BTreeMap<String, Session>,
    stream_seq: Option<StreamSeq>,
}

impl Producers {
    pub fn load(
        stream: &dyn Stream,
        checkpoint: Option<Bytes>,
        sessions: Vec<(String, Bytes)>,
        stream_seq: Option<Bytes>,
    ) -> Result<Self, ServiceError> {
        let epoch = stream.stream_epoch();
        let durable_end = stream.confirm_offset();
        let prefix = session_prefix(stream.stream_id());
        let mut loaded = Self {
            epoch,
            covered: live_start_offset(stream),
            tracked: BTreeMap::new(),
            sessions: BTreeMap::new(),
            stream_seq: None,
        };

        for (key, value) in sessions {
            let producer_id = hex::decode(&key[prefix.len()..])
                .ok()
                .and_then(|id| String::from_utf8(id).ok())
                .ok_or_else(|| corrupt("producer session key"))?;
            loaded.sessions.insert(producer_id, decode_session(&value)?);
        }
        if let Some(bytes) = checkpoint {
            let (covered, tracked) = decode_checkpoint(&bytes)?;
            if covered <= durable_end {
                loaded.covered = covered.max(loaded.covered);
                loaded.tracked = tracked;
            }
        }
        if let Some(bytes) = stream_seq {
            let (row_epoch, mut ledger) = decode_stream_seq(&bytes)?;
            if row_epoch > epoch {
                return Err(moved(stream.stream_id(), row_epoch));
            }
            ledger.settle(durable_end);
            loaded.stream_seq = Some(StreamSeq {
                ledger,
                written: Some(bytes),
            });
        }

        Ok(loaded)
    }

    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    pub fn covered(&self) -> u64 {
        self.covered
    }

    pub fn session(&self, producer_id: &str) -> Option<Session> {
        self.sessions.get(producer_id).copied()
    }

    pub fn decide(&self, producer: &Producer) -> ProducerDecision {
        let Some(session) = self.session(&producer.producer_id) else {
            return new_epoch(producer);
        };
        if producer.epoch < session.epoch {
            return ProducerDecision::StaleEpoch {
                current_epoch: session.epoch,
            };
        }
        if producer.epoch > session.epoch {
            return new_epoch(producer);
        }

        let last_seq = match self.tracked.get(&session.id) {
            Some(Tracked::Http(http)) => http.last_seq,
            _ => None,
        };
        if let Some(last_seq) = last_seq
            && producer.seq <= last_seq
        {
            return ProducerDecision::Duplicate { last_seq };
        }
        let expected = last_seq.map_or(0, |last| last + 1);
        if producer.seq == expected {
            return ProducerDecision::Accepted {
                epoch: producer.epoch,
                seq: producer.seq,
            };
        }

        ProducerDecision::SequenceGap {
            expected,
            received: producer.seq,
        }
    }

    /// A producer's new epoch. The previous epoch's id is forgotten with it.
    pub fn establish(&mut self, producer_id: String, session: Session, now_ms: i64) {
        if let Some(previous) = self.sessions.insert(producer_id, session) {
            self.tracked.remove(&previous.id);
        }
        self.tracked.insert(
            session.id,
            Tracked::Http(Http {
                last_seq: None,
                last_touched_ms: now_ms,
            }),
        );
    }

    pub fn record_http(&mut self, session: Session, seq: u64, now_ms: i64) {
        self.tracked.insert(
            session.id,
            Tracked::Http(Http {
                last_seq: Some(seq),
                last_touched_ms: now_ms,
            }),
        );
    }

    /// Whether state is behind the log: at load, for any known producer or
    /// session, and at append time for a Kafka producer's first batch.
    pub fn replay_needed(&self, producer: Option<NumericProducer>, durable_end: u64) -> bool {
        (producer.is_some() || !self.tracked.is_empty() || !self.sessions.is_empty())
            && self.covered < durable_end
    }

    pub async fn replay(&mut self, stream: &dyn Stream, now_ms: i64) -> Result<(), ServiceError> {
        let durable_end = stream.confirm_offset();
        let mut cursor = self.covered.max(live_start_offset(stream));

        while cursor < durable_end {
            let fetch = stream
                .fetch(FetchContext::default(), cursor, durable_end, REPLAY_BYTES)
                .await?;
            if fetch.records.is_empty() {
                break;
            }
            for batch in fetch.records {
                self.fold(&batch.payload, now_ms);
                cursor = cursor.max(batch.last_offset);
            }
        }

        self.covered = durable_end;
        Ok(())
    }

    fn fold(&mut self, payload: &[u8], now_ms: i64) {
        let Ok(headers) = record::batch_headers(payload) else {
            return;
        };
        for header in headers {
            let Some(producer) = header.producer else {
                continue;
            };
            match self.tracked.get_mut(&producer.id) {
                Some(Tracked::Http(http)) => {
                    http.last_seq = Some(match http.last_seq {
                        Some(last) => last + 1,
                        None => producer.first_seq as u64,
                    });
                    http.last_touched_ms = now_ms;
                }
                _ if self
                    .sessions
                    .values()
                    .any(|session| session.id == producer.id) =>
                {
                    self.tracked.insert(
                        producer.id,
                        Tracked::Http(Http {
                            last_seq: Some(producer.first_seq as u64),
                            last_touched_ms: now_ms,
                        }),
                    );
                }
                _ => {
                    let last_seq =
                        next_seq(producer.first_seq, header.record_count.saturating_sub(1));
                    self.record_numeric(Some((producer, last_seq)), header.base_offset, now_ms);
                }
            }
        }
    }

    pub(crate) fn admit_numeric(
        &mut self,
        producer: Option<NumericProducer>,
        record_count: u32,
        now_ms: i64,
    ) -> Result<Admission, ServiceError> {
        let Some(producer) = producer else {
            return Ok(Admission::Accepted(None));
        };
        let last_seq = next_seq(producer.first_seq, record_count.saturating_sub(1));
        let known = match self.tracked.get(&producer.id) {
            Some(Tracked::Kafka(kafka)) => Some(kafka),
            _ => None,
        };

        match validate_numeric(known, producer.epoch, producer.first_seq, last_seq) {
            Decision::Accepted => Ok(Admission::Accepted(Some((producer, last_seq)))),
            Decision::Duplicate { base_offset } => {
                if let Some(Tracked::Kafka(kafka)) = self.tracked.get_mut(&producer.id) {
                    kafka.last_touched_ms = now_ms;
                }
                Ok(Admission::Duplicate { base_offset })
            }
            Decision::Fenced { current_epoch } => Err(ServiceError::fenced(current_epoch as u64)),
            Decision::OutOfOrder { expected, received } => {
                Err(ServiceError::sequence_gap(expected as u64, received as u64))
            }
        }
    }

    pub(crate) fn record_numeric(
        &mut self,
        accepted: Option<(NumericProducer, i32)>,
        base_offset: u64,
        now_ms: i64,
    ) {
        let Some((producer, last_seq)) = accepted else {
            return;
        };
        let span = Span {
            first_seq: producer.first_seq,
            last_seq,
            base_offset,
        };
        if let Tracked::Kafka(kafka) = self
            .tracked
            .entry(producer.id)
            .or_insert_with(|| Tracked::Kafka(Kafka::default()))
        {
            kafka.record(producer.epoch, span, now_ms);
        }
    }

    /// A batch landed at `base_offset`. Replayed state stays complete only
    /// while every batch is seen in order.
    pub fn note_appended(&mut self, base_offset: u64, next_offset: u64) {
        if self.covered == base_offset {
            self.covered = next_offset;
        }
    }

    pub fn checkpoint_due(&self, covered_before: u64) -> bool {
        !self.tracked.is_empty()
            && covered_before / CHECKPOINT_INTERVAL != self.covered / CHECKPOINT_INTERVAL
    }

    /// Drops producers idle past [`EXPIRY_MS`], then the stalest beyond
    /// [`MAX_TRACKED`]. Returns the HTTP producers whose sessions went with
    /// them.
    pub fn expire(&mut self, now_ms: i64) -> Vec<String> {
        self.tracked
            .retain(|_, tracked| now_ms - tracked.last_touched_ms() <= EXPIRY_MS);
        while self.tracked.len() > MAX_TRACKED {
            let stalest = self
                .tracked
                .iter()
                .min_by_key(|(_, tracked)| tracked.last_touched_ms())
                .map(|(id, _)| *id)
                .expect("map is non-empty");
            self.tracked.remove(&stalest);
        }

        let expired: Vec<String> = self
            .sessions
            .iter()
            .filter(|(_, session)| !self.tracked.contains_key(&session.id))
            .map(|(producer_id, _)| producer_id.clone())
            .collect();
        for producer_id in &expired {
            self.sessions.remove(producer_id);
        }
        expired
    }

    pub fn stream_seq_regressed(&self, seq: &str) -> bool {
        self.stream_seq
            .as_ref()
            .and_then(|stream_seq| stream_seq.ledger.last())
            .is_some_and(|last| seq <= last.as_str())
    }

    pub fn accept_stream_seq(&mut self, seq: String, landed: Option<u64>) -> Bytes {
        let stream_seq = self.stream_seq.get_or_insert_with(|| StreamSeq {
            ledger: Ledger {
                durable: None,
                pending: Vec::new(),
            },
            written: None,
        });
        stream_seq.ledger.accept(seq, landed);
        encode_stream_seq(self.epoch, &stream_seq.ledger)
    }

    pub fn stream_seq_written(&self) -> Option<Bytes> {
        self.stream_seq
            .as_ref()
            .and_then(|stream_seq| stream_seq.written.clone())
    }

    pub fn mark_stream_seq_written(&mut self, bytes: Bytes) {
        if let Some(stream_seq) = &mut self.stream_seq {
            stream_seq.written = Some(bytes);
        }
    }

    /// Folds in a stream sequence row an earlier owner wrote after this one
    /// loaded, and returns the row to write instead.
    pub fn merge_stream_seq(
        &mut self,
        stream_id: u64,
        stored: Bytes,
        durable_end: u64,
    ) -> Result<Bytes, ServiceError> {
        let (row_epoch, mut theirs) = decode_stream_seq(&stored)?;
        if row_epoch >= self.epoch {
            return Err(moved(stream_id, row_epoch));
        }
        theirs.settle(durable_end);

        let Some(stream_seq) = &mut self.stream_seq else {
            return Err(corrupt("stream sequence"));
        };
        if let Some(floor) = theirs.last() {
            stream_seq.ledger.raise(floor.clone());
        }
        stream_seq.written = Some(stored);

        Ok(encode_stream_seq(self.epoch, &stream_seq.ledger))
    }

    pub fn confirm_stream_seq(&mut self, durable_end: u64) {
        if let Some(stream_seq) = &mut self.stream_seq {
            stream_seq.ledger.confirm(durable_end);
        }
    }

    pub fn checkpoint(&self) -> Bytes {
        let mut buf = BytesMut::new();
        buf.put_u8(CHECKPOINT_VERSION);
        buf.put_u64(self.covered);
        buf.put_u32(self.tracked.len() as u32);
        for (id, tracked) in &self.tracked {
            buf.put_i64(*id);
            match tracked {
                Tracked::Kafka(kafka) => {
                    buf.put_u8(0);
                    buf.put_i16(kafka.epoch);
                    buf.put_i64(kafka.last_touched_ms);
                    buf.put_u8(kafka.spans.len() as u8);
                    for span in &kafka.spans {
                        buf.put_i32(span.first_seq);
                        buf.put_i32(span.last_seq);
                        buf.put_u64(span.base_offset);
                    }
                }
                Tracked::Http(http) => {
                    buf.put_u8(1);
                    buf.put_i64(http.last_touched_ms);
                    put_opt_u64(&mut buf, http.last_seq);
                }
            }
        }
        buf.freeze()
    }
}

fn new_epoch(producer: &Producer) -> ProducerDecision {
    if producer.seq != 0 {
        return ProducerDecision::InvalidEpochSeq;
    }
    ProducerDecision::Accepted {
        epoch: producer.epoch,
        seq: producer.seq,
    }
}

/// The sequence an HTTP session's batch carries in the 31-bit header field.
pub fn header_seq(seq: u64) -> i32 {
    (seq % SEQ_SPACE) as i32
}

fn decode_checkpoint(bytes: &[u8]) -> Result<(u64, BTreeMap<i64, Tracked>), ServiceError> {
    let mut buf = bytes;
    expect_version(&mut buf, CHECKPOINT_VERSION, "producers")?;
    let covered = get_u64(&mut buf)?;

    let mut tracked = BTreeMap::new();
    for _ in 0..get_u32(&mut buf)? {
        let id = get_i64(&mut buf)?;
        let entry = match get_u8(&mut buf)? {
            0 => {
                let epoch = get_i16(&mut buf)?;
                let last_touched_ms = get_i64(&mut buf)?;
                let span_count = get_u8(&mut buf)? as usize;
                let mut spans = Vec::with_capacity(span_count.min(SPAN_WINDOW));
                for _ in 0..span_count {
                    spans.push(Span {
                        first_seq: get_i32(&mut buf)?,
                        last_seq: get_i32(&mut buf)?,
                        base_offset: get_u64(&mut buf)?,
                    });
                }
                Tracked::Kafka(Kafka {
                    epoch,
                    spans,
                    last_touched_ms,
                })
            }
            1 => {
                let last_touched_ms = get_i64(&mut buf)?;
                Tracked::Http(Http {
                    last_seq: get_opt_u64(&mut buf)?,
                    last_touched_ms,
                })
            }
            _ => return Err(corrupt("producers")),
        };
        tracked.insert(id, entry);
    }
    expect_end(buf, "producers")?;

    Ok((covered, tracked))
}

fn encode_stream_seq(epoch: u64, ledger: &Ledger<String>) -> Bytes {
    let mut buf = BytesMut::new();
    buf.put_u8(STREAM_SEQ_VERSION);
    buf.put_u64(epoch);
    match &ledger.durable {
        Some(value) => {
            buf.put_u8(1);
            put_str(&mut buf, value);
        }
        None => buf.put_u8(0),
    }
    buf.put_u32(ledger.pending.len() as u32);
    for (value, base_offset) in &ledger.pending {
        put_str(&mut buf, value);
        buf.put_u64(*base_offset);
    }
    buf.freeze()
}

fn decode_stream_seq(bytes: &[u8]) -> Result<(u64, Ledger<String>), ServiceError> {
    let mut buf = bytes;
    expect_version(&mut buf, STREAM_SEQ_VERSION, "stream sequence")?;
    let epoch = get_u64(&mut buf)?;
    let durable = match get_u8(&mut buf)? {
        0 => None,
        _ => Some(get_str(&mut buf)?),
    };
    let mut pending = Vec::new();
    for _ in 0..get_u32(&mut buf)? {
        let value = get_str(&mut buf)?;
        pending.push((value, get_u64(&mut buf)?));
    }
    expect_end(buf, "stream sequence")?;

    Ok((epoch, Ledger { durable, pending }))
}

fn corrupt(what: &str) -> ServiceError {
    ServiceError::with_message(
        ErrorKind::Internal,
        None,
        false,
        format!("corrupt {what} record"),
    )
}

fn moved(stream_id: u64, epoch: u64) -> ServiceError {
    ServiceError::with_message(
        ErrorKind::Transferring,
        None,
        false,
        format!("stream {stream_id} is owned at epoch {epoch}"),
    )
}

/// Kafka sequences occupy `[0, i32::MAX]` and wrap to 0.
pub fn next_seq(seq: i32, increment: u32) -> i32 {
    let increment = increment as i64;
    let wrapped = (seq as i64 + increment) % (i32::MAX as i64 + 1);
    wrapped as i32
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Decision {
    Accepted,
    Duplicate { base_offset: u64 },
    OutOfOrder { expected: i32, received: i32 },
    Fenced { current_epoch: i16 },
}

/// Decision table per Kafka's ProducerStateManager.
fn validate_numeric(known: Option<&Kafka>, epoch: i16, first_seq: i32, last_seq: i32) -> Decision {
    let Some(state) = known else {
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
    use crate::record::{LogRecord, encode_batch};

    fn producers() -> Producers {
        Producers {
            epoch: 3,
            covered: 0,
            tracked: BTreeMap::new(),
            sessions: BTreeMap::new(),
            stream_seq: None,
        }
    }

    fn named(epoch: u64, seq: u64) -> Producer {
        Producer {
            producer_id: "p".into(),
            epoch,
            seq,
        }
    }

    fn kafka(epoch: i16, spans: &[(i32, i32, u64)]) -> Kafka {
        let mut state = Kafka::default();
        for &(first_seq, last_seq, base_offset) in spans {
            state.record(
                epoch,
                Span {
                    first_seq,
                    last_seq,
                    base_offset,
                },
                0,
            );
        }
        state
    }

    fn batch(base_offset: u64, producer: NumericProducer, values: &[&str]) -> Bytes {
        let records: Vec<LogRecord> = values
            .iter()
            .map(|value| LogRecord::value(value.to_string()))
            .collect();
        encode_batch(base_offset, 1, 1, Some(producer), &records)
    }

    #[test]
    fn numeric_decision_table() {
        assert_eq!(validate_numeric(None, 0, 0, 2), Decision::Accepted);
        assert_eq!(
            validate_numeric(None, 0, 3, 5),
            Decision::OutOfOrder {
                expected: 0,
                received: 3
            }
        );

        let state = kafka(2, &[(0, 4, 10), (5, 9, 15)]);
        let known = Some(&state);
        assert_eq!(
            validate_numeric(known, 1, 0, 0),
            Decision::Fenced { current_epoch: 2 }
        );
        assert_eq!(validate_numeric(known, 3, 0, 0), Decision::Accepted);
        assert_eq!(
            validate_numeric(known, 3, 1, 1),
            Decision::OutOfOrder {
                expected: 0,
                received: 1
            }
        );
        assert_eq!(
            validate_numeric(known, 2, 0, 4),
            Decision::Duplicate { base_offset: 10 }
        );
        assert_eq!(
            validate_numeric(known, 2, 5, 9),
            Decision::Duplicate { base_offset: 15 }
        );
        assert_eq!(validate_numeric(known, 2, 10, 12), Decision::Accepted);
        assert_eq!(
            validate_numeric(known, 2, 12, 14),
            Decision::OutOfOrder {
                expected: 10,
                received: 12
            }
        );
        assert_eq!(
            validate_numeric(known, 2, 5, 7),
            Decision::OutOfOrder {
                expected: 10,
                received: 5
            }
        );
    }

    #[test]
    fn span_window_and_epoch_reset() {
        let spans: Vec<(i32, i32, u64)> = (0..7).map(|i| (i, i, i as u64)).collect();
        let mut state = kafka(1, &spans);
        assert_eq!(state.spans.len(), SPAN_WINDOW);
        assert_eq!(state.spans.first().unwrap().first_seq, 2);

        state.record(
            2,
            Span {
                first_seq: 0,
                last_seq: 3,
                base_offset: 100,
            },
            0,
        );
        assert_eq!(state.epoch, 2);
        assert_eq!(state.spans.len(), 1);
    }

    #[test]
    fn http_decision_table() {
        let mut p = producers();
        let session = Session { epoch: 3, id: 9 };

        assert_eq!(
            p.decide(&named(3, 0)),
            ProducerDecision::Accepted { epoch: 3, seq: 0 }
        );
        assert_eq!(p.decide(&named(3, 1)), ProducerDecision::InvalidEpochSeq);

        p.establish("p".into(), session, 0);
        assert_eq!(
            p.decide(&named(3, 0)),
            ProducerDecision::Accepted { epoch: 3, seq: 0 }
        );
        assert_eq!(
            p.decide(&named(3, 2)),
            ProducerDecision::SequenceGap {
                expected: 0,
                received: 2
            }
        );
        p.record_http(session, 5, 0);
        assert_eq!(
            p.decide(&named(2, 0)),
            ProducerDecision::StaleEpoch { current_epoch: 3 }
        );
        assert_eq!(
            p.decide(&named(4, 0)),
            ProducerDecision::Accepted { epoch: 4, seq: 0 }
        );
        assert_eq!(p.decide(&named(4, 1)), ProducerDecision::InvalidEpochSeq);
        assert_eq!(
            p.decide(&named(3, 5)),
            ProducerDecision::Duplicate { last_seq: 5 }
        );
        assert_eq!(
            p.decide(&named(3, 6)),
            ProducerDecision::Accepted { epoch: 3, seq: 6 }
        );
        assert_eq!(
            p.decide(&named(3, 8)),
            ProducerDecision::SequenceGap {
                expected: 6,
                received: 8
            }
        );
    }

    #[test]
    fn a_new_epoch_forgets_the_old_sessions_id() {
        let mut p = producers();
        p.establish("p".into(), Session { epoch: 1, id: 9 }, 0);
        p.record_http(Session { epoch: 1, id: 9 }, 4, 0);
        p.establish("p".into(), Session { epoch: 2, id: 10 }, 0);

        assert!(!p.tracked.contains_key(&9));
        assert_eq!(
            p.decide(&named(2, 0)),
            ProducerDecision::Accepted { epoch: 2, seq: 0 }
        );
    }

    #[test]
    fn replay_counts_http_batches_once_each_across_the_header_wrap() {
        let mut p = producers();
        let session = Session { epoch: 1, id: 9 };
        p.establish("p".into(), session, 0);
        p.record_http(session, SEQ_SPACE - 1, 0);
        let wrapped = NumericProducer {
            id: 9,
            epoch: 0,
            first_seq: header_seq(SEQ_SPACE),
        };
        let kafka = NumericProducer {
            id: 42,
            epoch: 0,
            first_seq: 0,
        };

        p.fold(&batch(0, wrapped, &["a", "b"]), 0);
        p.fold(&batch(2, kafka, &["c", "d"]), 0);

        assert_eq!(
            p.decide(&named(1, SEQ_SPACE)),
            ProducerDecision::Duplicate {
                last_seq: SEQ_SPACE
            }
        );
        assert_eq!(
            p.decide(&named(1, SEQ_SPACE + 1)),
            ProducerDecision::Accepted {
                epoch: 1,
                seq: SEQ_SPACE + 1
            }
        );
        assert_eq!(p.tracked[&42], Tracked::Kafka(self::kafka(0, &[(0, 1, 2)])));
    }

    #[test]
    fn a_session_first_seen_in_replay_starts_from_the_header() {
        let mut p = producers();
        p.sessions.insert("p".into(), Session { epoch: 1, id: 9 });
        let producer = NumericProducer {
            id: 9,
            epoch: 0,
            first_seq: 3,
        };

        p.fold(&batch(7, producer, &["a"]), 0);

        assert_eq!(
            p.decide(&named(1, 3)),
            ProducerDecision::Duplicate { last_seq: 3 }
        );
    }

    #[test]
    fn checkpoints_round_trip() {
        let mut p = producers();
        let session = Session { epoch: 3, id: 9 };
        p.establish("p".into(), session, 7);
        p.record_http(session, 2, 8);
        p.record_numeric(
            Some((
                NumericProducer {
                    id: 42,
                    epoch: 1,
                    first_seq: 0,
                },
                4,
            )),
            1,
            7,
        );
        p.covered = 5;

        let (covered, tracked) = decode_checkpoint(&p.checkpoint()).unwrap();
        assert_eq!(covered, 5);
        assert_eq!(tracked, p.tracked);
    }

    #[test]
    fn records_reject_truncation() {
        let mut p = producers();
        p.establish("p".into(), Session { epoch: 3, id: 9 }, 7);
        let checkpoint = p.checkpoint();
        for len in 0..checkpoint.len() {
            assert!(decode_checkpoint(&checkpoint[..len]).is_err(), "{len}");
        }

        let session = encode_session(Session { epoch: 3, id: 9 });
        for len in 0..session.len() {
            assert!(decode_session(&session[..len]).is_err(), "{len}");
        }

        let row = p.accept_stream_seq("s".into(), Some(0));
        for len in 0..row.len() {
            assert!(decode_stream_seq(&row[..len]).is_err(), "{len}");
        }
    }

    #[test]
    fn stream_seq_settles_to_what_became_durable() {
        let mut p = producers();
        p.accept_stream_seq("a".into(), Some(0));
        p.accept_stream_seq("b".into(), Some(5));
        let row = p.accept_stream_seq("c".into(), Some(9));

        let (_, mut ledger) = decode_stream_seq(&row).unwrap();
        ledger.settle(9);
        assert_eq!(ledger.last(), Some(&"b".to_string()));
        assert_eq!(ledger.durable, Some("b".to_string()));

        let (_, mut nothing) = decode_stream_seq(&row).unwrap();
        nothing.settle(0);
        assert_eq!(nothing.last(), None);
    }

    #[test]
    fn merging_a_stream_seq_row_from_an_older_epoch_keeps_the_newest() {
        let mut p = producers();
        p.accept_stream_seq("m".into(), Some(20));
        let mut older = producers();
        older.epoch = 2;
        older.accept_stream_seq("z".into(), Some(10));
        let theirs = older.accept_stream_seq("zz".into(), Some(30));

        p.merge_stream_seq(1, theirs, 21).unwrap();
        assert!(p.stream_seq_regressed("y"));
        assert!(p.stream_seq_regressed("z"));
        assert!(!p.stream_seq_regressed("zz"));

        let mut newer = producers();
        newer.epoch = 4;
        let row = newer.accept_stream_seq("a".into(), None);
        assert_eq!(
            p.merge_stream_seq(1, row, 21).unwrap_err().kind,
            ErrorKind::Transferring
        );
    }

    #[test]
    fn covered_only_advances_in_order() {
        let mut p = producers();
        p.note_appended(0, 4);
        p.note_appended(8, 12);
        assert_eq!(p.covered(), 4);
        p.note_appended(4, 8);
        assert_eq!(p.covered(), 8);
    }

    #[test]
    fn expiry_drops_sessions_with_their_state() {
        let mut p = producers();
        let over = MAX_TRACKED + 10;
        for i in 0..over {
            let session = Session {
                epoch: 0,
                id: i as i64,
            };
            p.establish(format!("p{i:05}"), session, i as i64);
        }

        let expired = p.expire(over as i64);
        assert_eq!(p.tracked.len(), MAX_TRACKED);
        assert_eq!(expired.len(), 10);
        assert!(expired.contains(&"p00009".to_string()));
        assert!(p.sessions.contains_key("p00010"));

        let expired = p.expire(over as i64 + EXPIRY_MS + 1);
        assert_eq!(expired.len(), MAX_TRACKED);
        assert!(p.tracked.is_empty());
        assert!(p.sessions.is_empty());
    }

    #[test]
    fn ids_come_from_the_lease() {
        let mut lease = IdLease::default();
        assert_eq!(lease.take(), None);
        lease.refill(1000);
        assert_eq!(lease.take(), Some(1000));
        assert_eq!(lease.take(), Some(1001));
    }

    #[test]
    fn seq_wraps_at_i32_max() {
        assert_eq!(next_seq(0, 5), 5);
        assert_eq!(next_seq(i32::MAX, 1), 0);
        assert_eq!(next_seq(i32::MAX - 1, 3), 1);
        assert_eq!(header_seq(SEQ_SPACE + 7), 7);
    }
}

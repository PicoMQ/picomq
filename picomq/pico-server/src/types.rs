//! Service model types.

use bytes::Bytes;

use crate::error::{ErrorKind, ServiceError};
use crate::record::LogRecord;
pub use crate::record::StreamRecord;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct OffsetToken {
    record_offset: u64,
}

impl OffsetToken {
    const WIDTH: usize = 20;

    pub fn beginning() -> Self {
        Self::of_record_offset(0)
    }

    pub fn of_record_offset(record_offset: u64) -> Self {
        Self { record_offset }
    }

    pub fn parse(raw: Option<&str>) -> Result<Self, ServiceError> {
        let raw = match raw {
            None | Some("-1") => return Ok(Self::beginning()),
            Some(r) => r,
        };
        if raw.is_empty() {
            return Err(ServiceError::with_message(
                ErrorKind::BadRequest,
                None,
                false,
                "empty offset",
            ));
        }
        match raw.parse::<i64>() {
            Ok(offset) if offset < 0 => Ok(Self::beginning()),
            Ok(offset) => Ok(Self::of_record_offset(offset as u64)),
            Err(_) => Err(ServiceError::with_message(
                ErrorKind::BadRequest,
                None,
                false,
                format!("invalid offset token: {raw}"),
            )),
        }
    }

    pub fn value(&self) -> String {
        format!("{:0width$}", self.record_offset, width = Self::WIDTH)
    }

    pub fn record_offset(&self) -> u64 {
        self.record_offset
    }
}

impl std::fmt::Display for OffsetToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.value())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Producer {
    pub producer_id: String,
    pub epoch: u64,
    pub seq: u64,
}

impl Producer {
    pub fn new(producer_id: impl Into<String>, epoch: u64, seq: u64) -> Result<Self, ServiceError> {
        let producer_id = producer_id.into();
        if producer_id.is_empty() {
            return Err(ServiceError::with_message(
                ErrorKind::BadRequest,
                None,
                false,
                "producerId must not be empty",
            ));
        }
        Ok(Self {
            producer_id,
            epoch,
            seq,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NumericProducer {
    pub id: i64,
    pub epoch: i16,
    pub first_seq: i32,
}

#[derive(Debug, Clone)]
pub struct AppendBatchCommand {
    pub name: String,
    pub payload: Bytes,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AppendBatchResult {
    pub base_offset: u64,
    pub duplicate: bool,
    pub log_start_offset: u64,
}

pub struct SubmittedBatchAppend {
    pub(crate) name: String,
    pub(crate) stream_id: u64,
    pub(crate) base_offset: u64,
    pub(crate) log_start_offset: u64,
    pub(crate) notify_offset: u64,
    pub(crate) duplicate: bool,
    pub(crate) pending: Option<s3stream::PendingAppend>,
    pub(crate) batches: Vec<StreamBatch>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamBatch {
    pub base_offset: u64,
    pub last_offset: u64,
    pub count: u32,
    pub payload: Bytes,
}

#[derive(Debug, Clone)]
pub struct BatchReadResult {
    pub batches: Vec<StreamBatch>,
    pub next_offset: u64,
    pub high_watermark: u64,
    pub log_start_offset: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StreamWatermarks {
    pub log_start_offset: u64,
    pub high_watermark: u64,
}

#[derive(Debug, Clone)]
pub struct CreateCommand {
    pub name: String,
    pub content_type: String,
    pub ttl_seconds: Option<u64>,
    pub expires_at_ms: Option<i64>,
    pub closed: bool,
    pub initial_records: Vec<LogRecord>,
    pub external_id: Option<[u8; 16]>,
    pub internal: bool,
    pub schema_name: Option<String>,
    pub schema_validate: bool,
    pub kafka_topic: Option<String>,
}

impl CreateCommand {
    pub fn new(name: impl Into<String>, content_type: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            content_type: content_type.into(),
            ttl_seconds: None,
            expires_at_ms: None,
            closed: false,
            initial_records: Vec::new(),
            external_id: None,
            internal: false,
            schema_name: None,
            schema_validate: false,
            kafka_topic: None,
        }
    }

    pub fn with_external_id(
        name: impl Into<String>,
        content_type: impl Into<String>,
        external_id: [u8; 16],
    ) -> Self {
        Self {
            external_id: Some(external_id),
            ..Self::new(name, content_type)
        }
    }

    pub fn with_kafka_topic(mut self, topic: impl Into<String>) -> Self {
        self.kafka_topic = Some(topic.into());
        self
    }

    pub fn with_schema_name(mut self, schema_name: impl Into<String>) -> Self {
        self.schema_name = Some(schema_name.into());
        self
    }

    pub fn with_schema_validate(mut self, validate: bool) -> Self {
        self.schema_validate = validate;
        self
    }

    pub fn validate(&self) -> Result<(), ServiceError> {
        if self.ttl_seconds.is_some() && self.expires_at_ms.is_some() {
            return Err(ServiceError::with_message(
                ErrorKind::BadRequest,
                None,
                false,
                "ttlSeconds and expiresAt are mutually exclusive",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamConfig {
    pub name: String,
    pub schema_name: Option<String>,
    pub schema_validate: bool,
    pub kafka_topic: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct UpdateStreamCommand {
    pub name: String,
    pub schema_name: Option<Option<String>>,
    pub schema_validate: Option<bool>,
    pub kafka_topic: Option<Option<String>>,
}

impl UpdateStreamCommand {
    pub fn validate(&self) -> Result<(), ServiceError> {
        if self.schema_name.is_none()
            && self.schema_validate.is_none()
            && self.kafka_topic.is_none()
        {
            return Err(ServiceError::with_message(
                ErrorKind::BadRequest,
                None,
                false,
                "no stream config fields to update",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct CreateResult {
    pub created: bool,
    pub meta: StreamMeta,
}

#[derive(Debug, Clone, Default)]
pub struct AppendCommand {
    pub name: String,
    pub records: Vec<LogRecord>,
    pub content_type: Option<String>,
    pub stream_seq: Option<String>,
    pub match_seq: Option<u64>,
    pub producer: Option<Producer>,
    pub close_after: bool,
}

impl AppendCommand {
    pub fn normalized(mut self) -> Self {
        if self.stream_seq.as_deref() == Some("") {
            self.stream_seq = None;
        }
        self
    }

    pub fn payload_len(&self) -> usize {
        self.records.iter().map(LogRecord::size_hint).sum()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendResult {
    pub next_offset: OffsetToken,
    pub applied: bool,
    pub timestamp_ms: Option<i64>,
    pub closed: bool,
    pub producer_epoch: Option<u64>,
    pub producer_seq: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CloseResult {
    pub next_offset: OffsetToken,
}

#[derive(Debug, Clone)]
pub struct ReadResult {
    pub records: Vec<StreamRecord>,
    pub content_type: String,
    pub next_offset: OffsetToken,
    pub up_to_date: bool,
    pub closed: bool,
}

impl ReadResult {
    pub fn concatenated_values(&self) -> Bytes {
        let mut out = Vec::with_capacity(self.records.iter().map(|r| r.record.value.len()).sum());
        for record in &self.records {
            out.extend_from_slice(&record.record.value);
        }
        Bytes::from(out)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamMeta {
    pub name: String,
    pub stream_id: u64,
    pub content_type: String,
    pub ttl_seconds: Option<u64>,
    pub expires_at_ms: Option<i64>,
    pub start_offset: OffsetToken,
    pub next_offset: OffsetToken,
    pub submitted_offset: OffsetToken,
    pub closed: bool,
    pub external_id: [u8; 16],
    pub schema_name: Option<String>,
    pub kafka_topic: Option<String>,
}

#[derive(Debug, Clone)]
pub struct StreamList {
    pub streams: Vec<StreamMeta>,
    pub has_more: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeMeta {
    pub node_id: i32,
    pub advertised_address: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Owner {
    pub stream_id: Option<u64>,
    pub local: bool,
    pub owner_node_id: Option<i32>,
    pub owner_advertised_address: Option<String>,
}

impl Owner {
    pub fn local(stream_id: Option<u64>) -> Self {
        Self {
            stream_id,
            local: true,
            owner_node_id: None,
            owner_advertised_address: None,
        }
    }

    pub fn remote(stream_id: u64, owner_node_id: i32, owner_advertised_address: String) -> Self {
        Self {
            stream_id: Some(stream_id),
            local: false,
            owner_node_id: Some(owner_node_id),
            owner_advertised_address: Some(owner_advertised_address),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn offset_token_parse_and_format() {
        assert_eq!(OffsetToken::parse(None).unwrap(), OffsetToken::beginning());
        assert_eq!(
            OffsetToken::parse(Some("-1")).unwrap(),
            OffsetToken::beginning()
        );
        assert_eq!(
            OffsetToken::parse(Some("-7")).unwrap(),
            OffsetToken::beginning()
        );
        assert_eq!(OffsetToken::parse(Some("42")).unwrap().record_offset(), 42);
        assert!(OffsetToken::parse(Some("")).is_err());
        assert!(OffsetToken::parse(Some("nope")).is_err());
        assert_eq!(
            OffsetToken::of_record_offset(7).value(),
            "00000000000000000007"
        );
        assert_eq!(OffsetToken::of_record_offset(7).value().len(), 20);
        assert!(
            OffsetToken::of_record_offset(9).value() < OffsetToken::of_record_offset(10).value()
        );
    }

    #[test]
    fn create_command_validation() {
        let cmd = CreateCommand {
            ttl_seconds: Some(5),
            expires_at_ms: Some(10),
            ..CreateCommand::new("/a", "text/plain")
        };
        assert!(cmd.validate().is_err());
    }

    #[test]
    fn producer_rejects_empty_id() {
        assert!(Producer::new("", 0, 0).is_err());
        assert!(Producer::new("p", 0, 0).is_ok());
    }
}

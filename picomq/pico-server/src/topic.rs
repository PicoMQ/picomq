use std::fmt;

use bytes::{BufMut, Bytes, BytesMut};
use picomq_common::now_ms;

use crate::codec::{
    expect_end, expect_version, get_bytes, get_i64, get_opt_i64, get_opt_str, get_str, get_u8,
    get_u32, put_opt_i64, put_opt_str, put_str,
};
use crate::error::{ErrorKind, ServiceError};

const VERSION: u8 = 1;
const MAX_KAFKA_NAME_LEN: usize = 249;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicName {
    spelling: String,
    canonical: String,
}

impl TopicName {
    pub fn parse(spelling: &str) -> Result<Self, ServiceError> {
        let spelling = if spelling.starts_with('/') {
            spelling.to_owned()
        } else {
            format!("/{spelling}")
        };

        let canonical = picomq_common::name::canonical(&spelling);
        if canonical.is_empty() {
            return Err(ServiceError::with_message(
                ErrorKind::BadRequest,
                None,
                false,
                "topic name must not be empty",
            ));
        }

        Ok(Self {
            spelling,
            canonical,
        })
    }

    pub fn spelling(&self) -> &str {
        &self.spelling
    }

    pub fn canonical(&self) -> &str {
        &self.canonical
    }

    pub fn kafka(&self) -> Option<&str> {
        is_kafka_name(&self.canonical).then_some(&self.canonical)
    }
}

impl fmt::Display for TopicName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.spelling)
    }
}

pub fn is_kafka_name(name: &str) -> bool {
    !name.is_empty()
        && name != "."
        && name != ".."
        && name.len() <= MAX_KAFKA_NAME_LEN
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TopicId(pub [u8; 16]);

impl TopicId {
    pub fn random() -> Self {
        Self(*uuid::Uuid::new_v4().as_bytes())
    }

    pub fn hex(&self) -> String {
        use std::fmt::Write;

        let mut out = String::with_capacity(32);
        for byte in self.0 {
            write!(out, "{byte:02x}").expect("write to String");
        }
        out
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClosedBy {
    pub producer_id: String,
    pub epoch: u64,
    pub seq: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Topic {
    pub id: TopicId,
    pub name: TopicName,
    pub partitions: u32,
    pub content_type: String,
    pub ttl_seconds: Option<u64>,
    pub expires_at_ms: Option<i64>,
    pub deadline_ms: i64,
    pub closed: bool,
    pub closed_by: Option<ClosedBy>,
    pub schema_name: Option<String>,
    pub schema_validate: bool,
}

impl Topic {
    pub fn expired(&self, now_ms: i64) -> bool {
        self.deadline_ms > 0 && now_ms > self.deadline_ms
    }

    pub fn touched(mut self) -> Self {
        let Some(ttl) = self.ttl_seconds else {
            return self;
        };
        if self.expires_at_ms.is_some() {
            return self;
        }

        let next = now_ms() + ttl as i64 * 1000;
        let coarsen = (ttl as i64 * 100).max(1000);
        if self.deadline_ms > 0 && next - self.deadline_ms < coarsen {
            return self;
        }

        self.deadline_ms = next;
        self
    }

    pub fn validation_schema(&self) -> Option<&str> {
        self.schema_validate
            .then_some(self.schema_name.as_deref())
            .flatten()
    }

    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::new();
        buf.put_u8(VERSION);
        buf.put_slice(&self.id.0);
        put_str(&mut buf, self.name.spelling());
        buf.put_u32(self.partitions);
        put_str(&mut buf, &self.content_type);
        put_opt_i64(&mut buf, self.ttl_seconds.map(|ttl| ttl as i64));
        put_opt_i64(&mut buf, self.expires_at_ms);
        buf.put_i64(self.deadline_ms);
        buf.put_u8(self.closed as u8);
        buf.put_u8(self.closed_by.is_some() as u8);
        if let Some(closed_by) = &self.closed_by {
            put_str(&mut buf, &closed_by.producer_id);
            buf.put_u64(closed_by.epoch);
            buf.put_u64(closed_by.seq);
        }
        put_opt_str(&mut buf, self.schema_name.as_deref());
        buf.put_u8(self.schema_validate as u8);
        buf.freeze()
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, ServiceError> {
        let mut buf = bytes;
        expect_version(&mut buf, VERSION, "topic")?;

        let id = TopicId(get_bytes(&mut buf)?);
        let name = TopicName::parse(&get_str(&mut buf)?)?;
        let partitions = get_u32(&mut buf)?;
        let content_type = get_str(&mut buf)?;
        let ttl_seconds = get_opt_i64(&mut buf)?.map(|ttl| ttl as u64);
        let expires_at_ms = get_opt_i64(&mut buf)?;
        let deadline_ms = get_i64(&mut buf)?;
        let closed = get_u8(&mut buf)? == 1;
        let closed_by = match get_u8(&mut buf)? {
            0 => None,
            _ => Some(ClosedBy {
                producer_id: get_str(&mut buf)?,
                epoch: get_i64(&mut buf)? as u64,
                seq: get_i64(&mut buf)? as u64,
            }),
        };
        let schema_name = get_opt_str(&mut buf)?;
        let schema_validate = get_u8(&mut buf)? == 1;
        expect_end(buf, "topic")?;

        Ok(Self {
            id,
            name,
            partitions,
            content_type,
            ttl_seconds,
            expires_at_ms,
            deadline_ms,
            closed,
            closed_by,
            schema_name,
            schema_validate,
        })
    }
}

pub fn deadline_of(ttl_seconds: Option<u64>, expires_at_ms: Option<i64>) -> i64 {
    if let Some(expires) = expires_at_ms {
        return expires;
    }
    if let Some(ttl) = ttl_seconds {
        return now_ms() + ttl as i64 * 1000;
    }
    0
}

#[cfg(test)]
mod tests {
    use super::*;

    fn topic() -> Topic {
        Topic {
            id: TopicId(*b"0123456789abcdef"),
            name: TopicName::parse("/acct/orders").unwrap(),
            partitions: 1,
            content_type: "application/json".into(),
            ttl_seconds: Some(60),
            expires_at_ms: None,
            deadline_ms: 12345,
            closed: true,
            closed_by: Some(ClosedBy {
                producer_id: "p1".into(),
                epoch: 2,
                seq: 41,
            }),
            schema_name: Some("orders".into()),
            schema_validate: true,
        }
    }

    #[test]
    fn names_share_a_canonical_form() {
        let slashed = TopicName::parse("/a/b").unwrap();
        let dotted = TopicName::parse("/a.b").unwrap();
        let bare = TopicName::parse("a.b").unwrap();

        assert_eq!(slashed.canonical(), "a.b");
        assert_eq!(dotted.canonical(), "a.b");
        assert_eq!(bare.canonical(), "a.b");
        assert_eq!(slashed.spelling(), "/a/b");
        assert_eq!(bare.spelling(), "/a.b");
    }

    #[test]
    fn kafka_names_follow_kafka_rules() {
        assert_eq!(
            TopicName::parse("/v1/stream/x-1").unwrap().kafka(),
            Some("v1.stream.x-1")
        );
        assert_eq!(TopicName::parse("/tenant:1/orders").unwrap().kafka(), None);
        assert_eq!(TopicName::parse("/a b").unwrap().kafka(), None);
        assert!(
            TopicName::parse(&"/x".repeat(130))
                .unwrap()
                .kafka()
                .is_none()
        );
    }

    #[test]
    fn empty_names_are_rejected() {
        assert!(TopicName::parse("/").is_err());
        assert!(TopicName::parse("").is_err());
    }

    #[test]
    fn roundtrips() {
        for value in [
            topic(),
            Topic {
                ttl_seconds: None,
                expires_at_ms: Some(999),
                closed: false,
                closed_by: None,
                schema_name: None,
                schema_validate: false,
                ..topic()
            },
        ] {
            assert_eq!(Topic::decode(&value.encode()).unwrap(), value);
        }
    }

    #[test]
    fn rejects_truncation() {
        let encoded = topic().encode();
        for len in 0..encoded.len() {
            assert!(
                Topic::decode(&encoded[..len]).is_err(),
                "truncated at {len}"
            );
        }
    }

    #[test]
    fn touched_coarsens_and_respects_absolute_expiry() {
        let sliding = Topic {
            ttl_seconds: Some(60),
            expires_at_ms: None,
            deadline_ms: 0,
            ..topic()
        };
        let touched = sliding.touched();
        assert!(touched.deadline_ms > 0);
        assert_eq!(touched.clone().touched().deadline_ms, touched.deadline_ms);

        let fixed = Topic {
            ttl_seconds: None,
            expires_at_ms: Some(123),
            deadline_ms: 123,
            ..topic()
        };
        assert_eq!(fixed.clone().touched(), fixed);
    }
}

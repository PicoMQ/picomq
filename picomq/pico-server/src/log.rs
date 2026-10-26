use bytes::{BufMut, Bytes, BytesMut};
use picomq_metadata::MetadataState;

use crate::codec::{expect_end, expect_version, get_u64};
use crate::error::ServiceError;
use crate::partition::Partition;

const VERSION: u8 = 1;
const GROUP_PREFIX: &str = "log/group/";

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
            Log::Group(group_id) => format!("{GROUP_PREFIX}{}", hex::encode(group_id)),
        }
    }

    pub fn groups(
        state: &MetadataState,
        prefix: &str,
        after: Option<&str>,
        limit: usize,
    ) -> Vec<String> {
        let prefix = format!("{GROUP_PREFIX}{}", hex::encode(prefix));
        let after = after.map(|after| format!("{GROUP_PREFIX}{}", hex::encode(after)));

        state
            .list_kv_page(&prefix, after.as_deref(), limit)
            .into_iter()
            .map(|(key, _)| {
                let id = hex::decode(&key[GROUP_PREFIX.len()..]).unwrap_or_default();
                String::from_utf8_lossy(&id).into_owned()
            })
            .collect()
    }
}

pub fn encode_stream_id(stream_id: u64) -> Bytes {
    let mut buf = BytesMut::with_capacity(9);
    buf.put_u8(VERSION);
    buf.put_u64(stream_id);
    buf.freeze()
}

pub fn decode_stream_id(value: &[u8]) -> Result<u64, ServiceError> {
    let mut buf = value;
    expect_version(&mut buf, VERSION, "log")?;
    let stream_id = get_u64(&mut buf)?;
    expect_end(buf, "log")?;

    Ok(stream_id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::topic::TopicId;

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
    fn stream_ids_round_trip_and_reject_truncation() {
        let encoded = encode_stream_id(7);
        assert_eq!(decode_stream_id(&encoded).unwrap(), 7);

        for len in 0..encoded.len() {
            assert!(decode_stream_id(&encoded[..len]).is_err(), "{len}");
        }
        assert!(decode_stream_id(&[&encoded[..], &[0][..]].concat()).is_err());
    }
}

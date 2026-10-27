use bytes::{BufMut, Bytes, BytesMut};
use picomq_metadata::MetadataState;

use crate::codec::{expect_end, expect_version, get_u64};
use crate::error::ServiceError;
use crate::partition::Partition;
use crate::topic::TopicId;

const VERSION: u8 = 1;
const PARTITION_PREFIX: &str = "log/partition/";
const GROUP_PREFIX: &str = "log/group/";
const STREAM_PREFIX: &str = "idx/sid/";

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
                    "{PARTITION_PREFIX}{}/{}",
                    partition.topic.hex(),
                    partition.index
                )
            }
            Log::Group(group_id) => format!("{GROUP_PREFIX}{}", hex::encode(group_id)),
        }
    }

    pub fn parse(key: &str) -> Option<Self> {
        if let Some(partition) = key.strip_prefix(PARTITION_PREFIX) {
            let (topic, index) = partition.split_once('/')?;
            let partition = Partition::new(TopicId::from_hex(topic)?, index.parse().ok()?);
            return Some(Log::Partition(partition));
        }

        let group = hex::decode(key.strip_prefix(GROUP_PREFIX)?).ok()?;
        Some(Log::Group(String::from_utf8(group).ok()?))
    }

    pub fn of_stream(state: &MetadataState, stream_id: u64) -> Option<Self> {
        let key = state.get_kv(&stream_id_key(stream_id))?;
        Log::parse(std::str::from_utf8(&key).ok()?)
    }

    pub fn stream_id(&self, state: &MetadataState) -> Result<Option<u64>, ServiceError> {
        state
            .get_kv(&self.key())
            .map(|value| decode_stream_id(&value))
            .transpose()
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

pub(crate) fn stream_id_key(stream_id: u64) -> String {
    format!("{STREAM_PREFIX}{stream_id}")
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
    fn keys_parse_back_to_their_log() {
        let partition = Log::Partition(Partition::new(TopicId(*b"0123456789abcdef"), 7));
        let group = Log::Group("a/b c".into());

        assert_eq!(Log::parse(&partition.key()), Some(partition));
        assert_eq!(Log::parse(&group.key()), Some(group));
        for foreign in [
            "topic/x",
            "log/partition/zz/1",
            "log/partition/00/x",
            "log/group/zz",
        ] {
            assert_eq!(Log::parse(foreign), None, "{foreign}");
        }
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

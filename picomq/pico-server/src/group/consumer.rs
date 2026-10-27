use bytes::{Buf, Bytes};
use kafka_protocol::messages::{ConsumerProtocolAssignment, ConsumerProtocolSubscription};
use kafka_protocol::protocol::Decodable;

pub const PROTOCOL_TYPE: &str = "consumer";

const NEWEST_SUBSCRIPTION: i16 = 3;
const NEWEST_ASSIGNMENT: i16 = 3;

pub fn subscription(metadata: &Bytes) -> Option<Vec<String>> {
    let mut buf = metadata.clone();
    let version = buf.try_get_i16().ok()?;
    let subscription =
        ConsumerProtocolSubscription::decode(&mut buf, version.min(NEWEST_SUBSCRIPTION)).ok()?;

    Some(
        subscription
            .topics
            .iter()
            .map(|topic| topic.to_string())
            .collect(),
    )
}

pub fn assignment(assignment: &Bytes) -> Option<Vec<(String, Vec<i32>)>> {
    if assignment.is_empty() {
        return Some(Vec::new());
    }

    let mut buf = assignment.clone();
    let version = buf.try_get_i16().ok()?;
    let assignment =
        ConsumerProtocolAssignment::decode(&mut buf, version.min(NEWEST_ASSIGNMENT)).ok()?;

    Some(
        assignment
            .assigned_partitions
            .iter()
            .map(|topic| (topic.topic.to_string(), topic.partitions.clone()))
            .collect(),
    )
}

#[cfg(test)]
mod tests {
    use bytes::{BufMut, BytesMut};
    use kafka_protocol::messages::TopicName;
    use kafka_protocol::messages::consumer_protocol_assignment::TopicPartition;
    use kafka_protocol::protocol::{Encodable, StrBytes};

    use super::*;

    #[test]
    fn an_assignment_reads_as_topics_and_partitions() {
        let mut encoded = BytesMut::new();
        encoded.put_i16(1);
        ConsumerProtocolAssignment::default()
            .with_assigned_partitions(vec![
                TopicPartition::default()
                    .with_topic(TopicName(StrBytes::from_static_str("orders")))
                    .with_partitions(vec![0, 2]),
            ])
            .encode(&mut encoded, 1)
            .unwrap();

        assert_eq!(
            assignment(&encoded.freeze()),
            Some(vec![("orders".to_owned(), vec![0, 2])])
        );
        assert_eq!(assignment(&Bytes::new()), Some(Vec::new()));
        assert_eq!(assignment(&Bytes::from_static(b"opaque")), None);
    }

    #[test]
    fn a_subscription_reads_as_topics() {
        let mut encoded = BytesMut::new();
        encoded.put_i16(0);
        ConsumerProtocolSubscription::default()
            .with_topics(vec![StrBytes::from_static_str("orders")])
            .encode(&mut encoded, 0)
            .unwrap();

        assert_eq!(
            subscription(&encoded.freeze()),
            Some(vec!["orders".to_owned()])
        );
        assert_eq!(subscription(&Bytes::from_static(b"x")), None);
    }
}

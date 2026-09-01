//! Kafka topic name rules and stream↔topic alias helpers.

use crate::error::{ErrorKind, ServiceError};

pub const MAX_TOPIC_LEN: usize = 249;

pub fn is_valid_topic(topic: &str) -> bool {
    !topic.is_empty()
        && topic != "."
        && topic != ".."
        && topic.len() <= MAX_TOPIC_LEN
        && topic
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
}

pub fn validate_topic(topic: &str) -> Result<(), ServiceError> {
    if is_valid_topic(topic) {
        return Ok(());
    }
    Err(ServiceError::with_message(
        ErrorKind::BadRequest,
        None,
        false,
        format!("invalid Kafka topic name: {topic:?}"),
    ))
}

pub fn derive_topic(name: &str) -> Option<String> {
    if crate::service::is_reserved_name(name) {
        return None;
    }
    let candidate = name.strip_prefix('/')?.replace('/', ".");
    is_valid_topic(&candidate).then_some(candidate)
}

pub fn stream_name_for_topic(topic: &str) -> String {
    format!("/{topic}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn topic_rules_match_kafka() {
        assert!(is_valid_topic("events.v1_2-3"));
        assert!(is_valid_topic("_consumer_offsets"));
        assert!(!is_valid_topic(""));
        assert!(!is_valid_topic("."));
        assert!(!is_valid_topic(".."));
        assert!(!is_valid_topic("a/b"));
        assert!(!is_valid_topic("a b"));
        assert!(!is_valid_topic("héllo"));
        assert!(is_valid_topic(&"x".repeat(MAX_TOPIC_LEN)));
        assert!(!is_valid_topic(&"x".repeat(MAX_TOPIC_LEN + 1)));
    }

    #[test]
    fn derivation_maps_slashes_to_dots() {
        assert_eq!(derive_topic("/orders/eu/1").as_deref(), Some("orders.eu.1"));
        assert_eq!(derive_topic("/orders.eu").as_deref(), Some("orders.eu"));
        assert_eq!(derive_topic("/events").as_deref(), Some("events"));
        assert_eq!(derive_topic("/a b"), None);
        assert_eq!(derive_topic("/"), None);
        assert_eq!(derive_topic("/_sys/groups/g"), None);
        assert_eq!(derive_topic("/_streams"), None);
        assert_eq!(
            derive_topic(&stream_name_for_topic("orders.eu")).as_deref(),
            Some("orders.eu")
        );
    }
}

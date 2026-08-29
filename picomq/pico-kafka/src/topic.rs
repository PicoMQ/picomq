//! Kafka topic names. A topic maps to the stream `/{topic}` and names cannot
//! contain `/`, so a topic can never collide with the reserved `/_sys/` tree.
//! The one exception is the internal topic `__catalog`, an alias for the
//! catalog changelog stream.

use pico_server::CATALOG_STREAM;

pub const SYS_PREFIX: &str = "/_sys/";
pub const CATALOG_TOPIC: &str = "__catalog";

const KAFKA_TOPIC: &str = "application/vnd.kafka.batch";

pub fn kafka_content_type() -> &'static str {
    KAFKA_TOPIC
}

pub fn is_sys_name(name: &str) -> bool {
    name == "/_sys" || name.starts_with(SYS_PREFIX)
}

pub fn validate_topic_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 249
        && !name.contains('/')
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'))
}

pub fn is_catalog_name(name: &str) -> bool {
    name == CATALOG_TOPIC
}

pub fn stream_name(topic: &str) -> String {
    if is_catalog_name(topic) {
        return CATALOG_STREAM.into();
    }
    format!("/{topic}")
}

/// The topic a stream backs, or `None` when the stream is not a valid topic
/// (nested paths, invalid characters, the reserved subtree).
pub fn topic_from_stream(stream: &str) -> Option<&str> {
    if stream == CATALOG_STREAM {
        return Some(CATALOG_TOPIC);
    }
    if is_sys_name(stream) {
        return None;
    }
    let topic = stream.strip_prefix('/')?;
    // The alias owns the topic name, a plain stream cannot shadow it.
    if is_catalog_name(topic) {
        return None;
    }
    validate_topic_name(topic).then_some(topic)
}

pub fn is_internal_topic(topic: &str) -> bool {
    topic.starts_with('_')
}

/// Whether a stream's payloads are stored as Kafka record batches.
pub fn stores_batches(stream: &str) -> bool {
    stream != CATALOG_STREAM
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn topic_stream_round_trip() {
        assert_eq!(stream_name("events"), "/events");
        assert_eq!(topic_from_stream("/events"), Some("events"));
        assert_eq!(topic_from_stream("/a/b"), None);
        assert_eq!(topic_from_stream("events"), None);
        assert_eq!(topic_from_stream("/_sys/groups/g"), None);
        assert_eq!(topic_from_stream("/_sys"), None);
        // `_sys` alone is not the reserved subtree but still maps cleanly.
        assert_eq!(topic_from_stream("/_sysish"), Some("_sysish"));
    }

    #[test]
    fn catalog_alias_round_trip() {
        assert_eq!(stream_name(CATALOG_TOPIC), CATALOG_STREAM);
        assert_eq!(topic_from_stream(CATALOG_STREAM), Some(CATALOG_TOPIC));
        assert!(is_internal_topic(CATALOG_TOPIC));
        assert_eq!(topic_from_stream("/__catalog"), None);
    }

    #[test]
    fn topic_name_rules() {
        assert!(validate_topic_name("events.v1_2-3"));
        assert!(!validate_topic_name(""));
        assert!(!validate_topic_name("a/b"));
        assert!(!validate_topic_name(&"x".repeat(250)));
    }
}

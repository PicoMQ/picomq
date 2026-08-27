//! Kafka topic names. A topic maps to the stream `/{topic}` and names cannot
//! contain `/`, so a topic can never collide with the reserved `/_sys/` tree.

pub const SYS_PREFIX: &str = "/_sys/";

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

pub fn stream_name(topic: &str) -> String {
    format!("/{topic}")
}

/// The topic a stream backs, or `None` when the stream is not a valid topic
/// (nested paths, invalid characters, the reserved subtree).
pub fn topic_from_stream(stream: &str) -> Option<&str> {
    if is_sys_name(stream) {
        return None;
    }
    let topic = stream.strip_prefix('/')?;
    validate_topic_name(topic).then_some(topic)
}

pub fn is_internal_topic(topic: &str) -> bool {
    topic.starts_with('_')
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
    fn topic_name_rules() {
        assert!(validate_topic_name("events.v1_2-3"));
        assert!(!validate_topic_name(""));
        assert!(!validate_topic_name("a/b"));
        assert!(!validate_topic_name(&"x".repeat(250)));
    }
}

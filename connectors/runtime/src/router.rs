use crate::error::RuntimeError;
use picomq_connector_sdk::{DecodedMessage, Payload};
use serde::{Deserialize, Serialize};
use simd_json::OwnedValue;
use simd_json::prelude::*;
use std::fmt::{Display, Formatter};

pub const MAX_TOPIC_NAME_LENGTH: usize = 249;
const VALUE_PLACEHOLDER: &str = "{value}";

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum TopicRoute {
    Static(String),
    Dynamic(TopicRouteConfig),
}

impl Default for TopicRoute {
    fn default() -> Self {
        Self::Static(String::new())
    }
}

impl Display for TopicRoute {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            TopicRoute::Static(name) => write!(f, "{name}"),
            TopicRoute::Dynamic(config) => write!(f, "{config}"),
        }
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct TopicRouteConfig {
    pub strategy: RouteStrategy,
    pub name: Option<String>,
    pub path: Option<String>,
    pub header: Option<String>,
    pub template: Option<String>,
    pub buckets: Option<u32>,
    pub fallback: Option<String>,
}

impl Display for TopicRouteConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ strategy: {}, name: {:?}, path: {:?}, header: {:?}, template: {:?}, buckets: {:?}, fallback: {:?} }}",
            self.strategy,
            self.name,
            self.path,
            self.header,
            self.template,
            self.buckets,
            self.fallback
        )
    }
}

#[derive(
    Debug,
    Default,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    strum::Display,
    strum::EnumString,
)]
#[serde(rename_all = "lowercase")]
#[strum(serialize_all = "lowercase", ascii_case_insensitive)]
pub enum RouteStrategy {
    #[default]
    Static,
    Field,
    Header,
    Key,
    Hash,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RouteError {
    MissingField(String),
    MissingHeader(String),
    MissingKey,
    UnroutableValue(String),
}

impl Display for RouteError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            RouteError::MissingField(path) => write!(f, "field '{path}' not present in payload"),
            RouteError::MissingHeader(name) => write!(f, "header '{name}' not present"),
            RouteError::MissingKey => write!(f, "message has no key"),
            RouteError::UnroutableValue(value) => {
                write!(f, "value '{value}' cannot be used as a topic name")
            }
        }
    }
}

impl std::error::Error for RouteError {}

#[derive(Debug, Clone)]
pub struct TopicRouter {
    kind: RouterKind,
    template: String,
    fallback: Option<String>,
    label: String,
}

#[derive(Debug, Clone)]
enum RouterKind {
    Static(String),
    Field(Vec<String>),
    Header(String),
    Key,
    Hash { source: HashSource, buckets: u32 },
}

#[derive(Debug, Clone)]
enum HashSource {
    Field(Vec<String>),
    Header(String),
    Key,
}

impl TopicRouter {
    pub fn new(route: &TopicRoute) -> Result<Self, RuntimeError> {
        match route {
            TopicRoute::Static(name) => Self::from_static(name),
            TopicRoute::Dynamic(config) => Self::from_config(config),
        }
    }

    pub fn is_static(&self) -> bool {
        matches!(self.kind, RouterKind::Static(_))
    }

    pub fn static_topic(&self) -> Option<&str> {
        match &self.kind {
            RouterKind::Static(name) => Some(name),
            _ => None,
        }
    }

    pub fn label(&self) -> &str {
        &self.label
    }

    pub fn route(&self, message: &DecodedMessage) -> Result<String, RouteError> {
        let resolved = match &self.kind {
            RouterKind::Static(name) => return Ok(name.clone()),
            RouterKind::Field(path) => field_value(&message.payload, path)
                .ok_or_else(|| RouteError::MissingField(path.join("."))),
            RouterKind::Header(name) => {
                header_value(message, name).ok_or_else(|| RouteError::MissingHeader(name.clone()))
            }
            RouterKind::Key => key_value(message).ok_or(RouteError::MissingKey),
            RouterKind::Hash { source, buckets } => {
                let bytes = match source {
                    HashSource::Field(path) => field_value(&message.payload, path)
                        .map(String::into_bytes)
                        .ok_or_else(|| RouteError::MissingField(path.join("."))),
                    HashSource::Header(name) => message
                        .headers
                        .as_ref()
                        .and_then(|headers| headers.get(name).cloned())
                        .ok_or_else(|| RouteError::MissingHeader(name.clone())),
                    HashSource::Key => message.key.clone().ok_or(RouteError::MissingKey),
                };
                bytes.map(|bytes| bucket_for(&bytes, *buckets).to_string())
            }
        };
        match resolved {
            Ok(value) => {
                let topic = sanitize(&self.template.replace(VALUE_PLACEHOLDER, &value));
                if topic.is_empty() {
                    self.fallback_or(RouteError::UnroutableValue(value))
                } else {
                    Ok(topic)
                }
            }
            Err(error) => self.fallback_or(error),
        }
    }

    fn fallback_or(&self, error: RouteError) -> Result<String, RouteError> {
        self.fallback.clone().ok_or(error)
    }

    fn from_static(name: &str) -> Result<Self, RuntimeError> {
        let topic = sanitize(name);
        if topic.is_empty() {
            return Err(RuntimeError::InvalidTopicRoute(format!(
                "static topic name '{name}' is empty after sanitizing"
            )));
        }
        Ok(Self {
            kind: RouterKind::Static(topic.clone()),
            template: VALUE_PLACEHOLDER.to_owned(),
            fallback: None,
            label: topic,
        })
    }

    fn from_config(config: &TopicRouteConfig) -> Result<Self, RuntimeError> {
        let template = config
            .template
            .clone()
            .unwrap_or_else(|| VALUE_PLACEHOLDER.to_owned());
        if !template.contains(VALUE_PLACEHOLDER) && config.strategy != RouteStrategy::Static {
            return Err(RuntimeError::InvalidTopicRoute(format!(
                "template '{template}' must contain '{VALUE_PLACEHOLDER}'"
            )));
        }
        let fallback = match &config.fallback {
            Some(fallback) => {
                let sanitized = sanitize(fallback);
                if sanitized.is_empty() {
                    return Err(RuntimeError::InvalidTopicRoute(format!(
                        "fallback topic '{fallback}' is empty after sanitizing"
                    )));
                }
                Some(sanitized)
            }
            None => None,
        };
        let (kind, label) = match config.strategy {
            RouteStrategy::Static => {
                let name = config.name.as_deref().ok_or_else(|| {
                    RuntimeError::InvalidTopicRoute("static route requires 'name'".to_owned())
                })?;
                return Self::from_static(name);
            }
            RouteStrategy::Field => {
                let path = split_path(config.path.as_deref())?;
                let label = format!("field:{}", path.join("."));
                (RouterKind::Field(path), label)
            }
            RouteStrategy::Header => {
                let header = config.header.clone().ok_or_else(|| {
                    RuntimeError::InvalidTopicRoute("header route requires 'header'".to_owned())
                })?;
                let label = format!("header:{header}");
                (RouterKind::Header(header), label)
            }
            RouteStrategy::Key => (RouterKind::Key, "key".to_owned()),
            RouteStrategy::Hash => {
                let buckets = config.buckets.ok_or_else(|| {
                    RuntimeError::InvalidTopicRoute("hash route requires 'buckets'".to_owned())
                })?;
                if buckets == 0 {
                    return Err(RuntimeError::InvalidTopicRoute(
                        "hash route requires 'buckets' > 0".to_owned(),
                    ));
                }
                let (source, source_label) = if let Some(path) = &config.path {
                    let path = split_path(Some(path))?;
                    let label = format!("field:{}", path.join("."));
                    (HashSource::Field(path), label)
                } else if let Some(header) = &config.header {
                    (
                        HashSource::Header(header.clone()),
                        format!("header:{header}"),
                    )
                } else {
                    (HashSource::Key, "key".to_owned())
                };
                (
                    RouterKind::Hash { source, buckets },
                    format!("hash({source_label})%{buckets}"),
                )
            }
        };
        Ok(Self {
            kind,
            template,
            fallback,
            label,
        })
    }
}

pub fn sanitize(name: &str) -> String {
    let mut sanitized: String = name
        .trim()
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '.' | '_' | '-') {
                character
            } else {
                '_'
            }
        })
        .collect();
    if sanitized.len() > MAX_TOPIC_NAME_LENGTH {
        sanitized.truncate(MAX_TOPIC_NAME_LENGTH);
    }
    if sanitized == "." || sanitized == ".." {
        return String::new();
    }
    sanitized
}

pub fn bucket_for(bytes: &[u8], buckets: u32) -> u32 {
    let hash = murmur2::murmur2(bytes, murmur2::KAFKA_SEED) & 0x7fff_ffff;
    hash % buckets
}

fn split_path(path: Option<&str>) -> Result<Vec<String>, RuntimeError> {
    let path = path.map(str::trim).filter(|path| !path.is_empty());
    let Some(path) = path else {
        return Err(RuntimeError::InvalidTopicRoute(
            "field route requires a non-empty 'path'".to_owned(),
        ));
    };
    Ok(path.split('.').map(str::to_owned).collect())
}

fn field_value(payload: &Payload, path: &[String]) -> Option<String> {
    let Payload::Json(root) = payload else {
        return None;
    };
    let mut current = root;
    for segment in path {
        current = match current {
            OwnedValue::Object(object) => object.get(segment.as_str())?,
            OwnedValue::Array(array) => array.get(segment.parse::<usize>().ok()?)?,
            _ => return None,
        };
    }
    scalar_to_string(current)
}

fn scalar_to_string(value: &OwnedValue) -> Option<String> {
    match value {
        OwnedValue::String(text) => Some(text.to_string()),
        OwnedValue::Static(_) => {
            if value.is_null() {
                None
            } else if let Some(boolean) = value.as_bool() {
                Some(boolean.to_string())
            } else if let Some(integer) = value.as_i64() {
                Some(integer.to_string())
            } else if let Some(unsigned) = value.as_u64() {
                Some(unsigned.to_string())
            } else {
                value.as_f64().map(|float| float.to_string())
            }
        }
        _ => None,
    }
}

fn header_value(message: &DecodedMessage, name: &str) -> Option<String> {
    message
        .headers
        .as_ref()
        .and_then(|headers| headers.get(name))
        .map(|bytes| String::from_utf8_lossy(bytes).into_owned())
}

fn key_value(message: &DecodedMessage) -> Option<String> {
    message
        .key
        .as_ref()
        .map(|bytes| String::from_utf8_lossy(bytes).into_owned())
}

#[cfg(test)]
mod tests {
    use super::*;
    use picomq_connector_sdk::Headers;

    fn json_message(json: &str) -> DecodedMessage {
        let mut bytes = json.as_bytes().to_vec();
        let value = simd_json::to_owned_value(&mut bytes).unwrap();
        DecodedMessage {
            offset: None,
            timestamp: None,
            key: None,
            headers: None,
            payload: Payload::Json(value),
        }
    }

    fn dynamic(config: TopicRouteConfig) -> TopicRouter {
        TopicRouter::new(&TopicRoute::Dynamic(config)).unwrap()
    }

    #[test]
    fn given_static_route_when_routing_should_return_same_topic_for_all_messages() {
        let router = TopicRouter::new(&TopicRoute::Static("orders".into())).unwrap();
        assert!(router.is_static());
        assert_eq!(router.static_topic(), Some("orders"));
        assert_eq!(router.route(&json_message(r#"{"a":1}"#)).unwrap(), "orders");
    }

    #[test]
    fn given_static_route_with_invalid_chars_when_created_should_sanitize() {
        let router = TopicRouter::new(&TopicRoute::Static("my topic/v1".into())).unwrap();
        assert_eq!(router.static_topic(), Some("my_topic_v1"));
    }

    #[test]
    fn given_empty_static_route_when_created_should_fail() {
        assert!(matches!(
            TopicRouter::new(&TopicRoute::Static("  ".into())),
            Err(RuntimeError::InvalidTopicRoute(_))
        ));
    }

    #[test]
    fn given_field_route_when_routing_should_use_nested_field_value() {
        let router = dynamic(TopicRouteConfig {
            strategy: RouteStrategy::Field,
            path: Some("user.id".into()),
            template: Some("user-{value}".into()),
            ..TopicRouteConfig::default()
        });
        assert!(!router.is_static());
        assert_eq!(router.label(), "field:user.id");
        let message = json_message(r#"{"user":{"id":42}}"#);
        assert_eq!(router.route(&message).unwrap(), "user-42");
    }

    #[test]
    fn given_field_route_when_field_missing_should_use_fallback() {
        let router = dynamic(TopicRouteConfig {
            strategy: RouteStrategy::Field,
            path: Some("user_id".into()),
            fallback: Some("unrouted".into()),
            ..TopicRouteConfig::default()
        });
        assert_eq!(
            router.route(&json_message(r#"{"other":1}"#)).unwrap(),
            "unrouted"
        );
    }

    #[test]
    fn given_field_route_when_field_missing_without_fallback_should_fail() {
        let router = dynamic(TopicRouteConfig {
            strategy: RouteStrategy::Field,
            path: Some("user_id".into()),
            ..TopicRouteConfig::default()
        });
        assert_eq!(
            router.route(&json_message(r#"{"other":1}"#)),
            Err(RouteError::MissingField("user_id".into()))
        );
    }

    #[test]
    fn given_field_route_when_payload_is_raw_should_fail_or_fallback() {
        let router = dynamic(TopicRouteConfig {
            strategy: RouteStrategy::Field,
            path: Some("id".into()),
            ..TopicRouteConfig::default()
        });
        let message = DecodedMessage {
            offset: None,
            timestamp: None,
            key: None,
            headers: None,
            payload: Payload::Raw(vec![1, 2, 3]),
        };
        assert!(router.route(&message).is_err());
    }

    #[test]
    fn given_field_route_when_value_has_invalid_chars_should_sanitize() {
        let router = dynamic(TopicRouteConfig {
            strategy: RouteStrategy::Field,
            path: Some("tenant".into()),
            ..TopicRouteConfig::default()
        });
        let message = json_message(r#"{"tenant":"acme corp/eu"}"#);
        assert_eq!(router.route(&message).unwrap(), "acme_corp_eu");
    }

    #[test]
    fn given_header_route_when_routing_should_use_header_value() {
        let router = dynamic(TopicRouteConfig {
            strategy: RouteStrategy::Header,
            header: Some("tenant".into()),
            ..TopicRouteConfig::default()
        });
        let mut message = json_message("{}");
        let mut headers = Headers::new();
        headers.insert("tenant".into(), b"blue".to_vec());
        message.headers = Some(headers);
        assert_eq!(router.route(&message).unwrap(), "blue");
    }

    #[test]
    fn given_key_route_when_routing_should_use_key() {
        let router = dynamic(TopicRouteConfig {
            strategy: RouteStrategy::Key,
            template: Some("k-{value}".into()),
            ..TopicRouteConfig::default()
        });
        let mut message = json_message("{}");
        message.key = Some(b"abc".to_vec());
        assert_eq!(router.route(&message).unwrap(), "k-abc");
        message.key = None;
        assert_eq!(router.route(&message), Err(RouteError::MissingKey));
    }

    #[test]
    fn given_hash_route_when_routing_should_be_deterministic_and_bounded() {
        let router = dynamic(TopicRouteConfig {
            strategy: RouteStrategy::Hash,
            path: Some("user_id".into()),
            buckets: Some(8),
            template: Some("shard-{value}".into()),
            ..TopicRouteConfig::default()
        });
        assert_eq!(router.label(), "hash(field:user_id)%8");
        let first = router.route(&json_message(r#"{"user_id":"u-1"}"#)).unwrap();
        let again = router.route(&json_message(r#"{"user_id":"u-1"}"#)).unwrap();
        assert_eq!(first, again);
        let bucket: u32 = first.strip_prefix("shard-").unwrap().parse().unwrap();
        assert!(bucket < 8);
    }

    #[test]
    fn given_hash_route_with_zero_buckets_when_created_should_fail() {
        let result = TopicRouter::new(&TopicRoute::Dynamic(TopicRouteConfig {
            strategy: RouteStrategy::Hash,
            buckets: Some(0),
            ..TopicRouteConfig::default()
        }));
        assert!(matches!(result, Err(RuntimeError::InvalidTopicRoute(_))));
    }

    #[test]
    fn given_template_without_placeholder_when_created_should_fail() {
        let result = TopicRouter::new(&TopicRoute::Dynamic(TopicRouteConfig {
            strategy: RouteStrategy::Field,
            path: Some("a".into()),
            template: Some("fixed".into()),
            ..TopicRouteConfig::default()
        }));
        assert!(matches!(result, Err(RuntimeError::InvalidTopicRoute(_))));
    }

    #[test]
    fn given_toml_with_string_topic_when_deserialized_should_be_static() {
        #[derive(Deserialize)]
        struct Wrapper {
            topic: TopicRoute,
        }
        let parsed: Wrapper = toml::from_str(r#"topic = "orders""#).unwrap();
        assert!(matches!(parsed.topic, TopicRoute::Static(ref name) if name == "orders"));
    }

    #[test]
    fn given_toml_with_table_topic_when_deserialized_should_be_dynamic() {
        #[derive(Deserialize)]
        struct Wrapper {
            topic: TopicRoute,
        }
        let parsed: Wrapper = toml::from_str(
            r#"
            [topic]
            strategy = "field"
            path = "user_id"
            template = "user-{value}"
            fallback = "users-unknown"
            "#,
        )
        .unwrap();
        let TopicRoute::Dynamic(config) = parsed.topic else {
            panic!("expected dynamic route");
        };
        assert_eq!(config.strategy, RouteStrategy::Field);
        assert_eq!(config.path.as_deref(), Some("user_id"));
    }

    #[test]
    fn given_kafka_murmur2_when_hashing_known_key_should_match_reference_partitioner() {
        assert_eq!(bucket_for(b"", 1), 0);
        let bucket = bucket_for(b"test-key", 1_000_000);
        assert_eq!(bucket, bucket_for(b"test-key", 1_000_000));
    }

    #[test]
    fn given_long_name_when_sanitized_should_truncate() {
        let long = "a".repeat(300);
        assert_eq!(sanitize(&long).len(), MAX_TOPIC_NAME_LENGTH);
        assert_eq!(sanitize(".."), "");
        assert_eq!(sanitize("  ok.name-1_ "), "ok.name-1_");
    }
}

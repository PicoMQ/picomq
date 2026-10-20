use bytes::Bytes;
use http::Method;
use serde_json::{Map, Value, json};

use crate::error::CodecError;
use crate::json::{json_body, json_request, object, string};
use crate::wire::{WireRequest, control_path};

pub const STREAMS_PATH_PREFIX: &str = "/_streams";

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TopicConfig {
    pub schema: Option<String>,
    pub schema_validate: bool,
    pub kafka_topic: Option<String>,
    pub partitions: u32,
    pub retention_ms: Option<u64>,
    pub retention_bytes: Option<u64>,
}

impl TopicConfig {
    pub fn encode(&self) -> Bytes {
        json_body(json!({
            "schema": self.schema,
            "schemaValidate": self.schema_validate,
            "kafkaTopic": self.kafka_topic,
            "partitions": self.partitions,
            "retentionMs": self.retention_ms,
            "retentionBytes": self.retention_bytes,
        }))
    }

    pub fn decode(body: &[u8]) -> Result<Self, CodecError> {
        let node = object(body)?;
        Ok(Self {
            schema: string(&node, "schema")?,
            schema_validate: node
                .get("schemaValidate")
                .and_then(Value::as_bool)
                .unwrap_or(false),
            kafka_topic: string(&node, "kafkaTopic")?,
            partitions: partitions(&node)?.unwrap_or(1),
            retention_ms: limit(&node, "retentionMs")?.flatten(),
            retention_bytes: limit(&node, "retentionBytes")?.flatten(),
        })
    }
}

#[derive(Debug, Clone)]
pub struct ConfigRequest<'a> {
    pub stream: &'a str,
}

impl ConfigRequest<'_> {
    pub fn encode(&self) -> WireRequest {
        WireRequest::new(
            Method::GET,
            control_path(STREAMS_PATH_PREFIX, self.stream),
            &[200, 404],
        )
    }
}

/// A change to a topic's settings. An absent field is left alone, and
/// `Some(None)` removes a setting.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ConfigUpdate {
    pub stream: String,
    pub schema: Option<Option<String>>,
    pub schema_validate: Option<bool>,
    pub retention_ms: Option<Option<u64>>,
    pub retention_bytes: Option<Option<u64>>,
    pub partitions: Option<u32>,
}

impl ConfigUpdate {
    pub fn encode(&self) -> WireRequest {
        let mut body = Map::new();
        if let Some(schema) = &self.schema {
            body.insert("schema".into(), json!(schema));
        }
        if let Some(validate) = self.schema_validate {
            body.insert("schemaValidate".into(), json!(validate));
        }
        if let Some(ms) = self.retention_ms {
            body.insert("retentionMs".into(), json!(ms));
        }
        if let Some(bytes) = self.retention_bytes {
            body.insert("retentionBytes".into(), json!(bytes));
        }
        if let Some(partitions) = self.partitions {
            body.insert("partitions".into(), json!(partitions));
        }
        json_request(
            Method::PATCH,
            control_path(STREAMS_PATH_PREFIX, &self.stream),
            body,
            &[200],
        )
    }

    pub fn decode(stream: String, body: &[u8]) -> Result<Self, CodecError> {
        let node = object(body)?;
        if node.contains_key("kafkaTopic") {
            return Err(CodecError::new(
                "kafkaTopic is derived from the stream name and cannot be set",
            ));
        }

        let schema = match node.get("schema") {
            None => None,
            Some(Value::Null) => Some(None),
            Some(Value::String(name)) if !name.is_empty() => Some(Some(name.clone())),
            Some(_) => {
                return Err(CodecError::new("schema must be a non-empty string or null"));
            }
        };
        let schema_validate = match node.get("schemaValidate") {
            None => None,
            Some(Value::Bool(validate)) => Some(*validate),
            Some(_) => return Err(CodecError::new("schemaValidate must be a boolean")),
        };

        Ok(Self {
            stream,
            schema,
            schema_validate,
            retention_ms: limit(&node, "retentionMs")?,
            retention_bytes: limit(&node, "retentionBytes")?,
            partitions: partitions(&node)?,
        })
    }
}

fn limit(node: &Map<String, Value>, key: &str) -> Result<Option<Option<u64>>, CodecError> {
    match node.get(key) {
        None => Ok(None),
        Some(Value::Null) => Ok(Some(None)),
        Some(value) => value
            .as_u64()
            .map(|limit| Some(Some(limit)))
            .ok_or_else(|| CodecError::new(format!("{key} must be a positive integer or null"))),
    }
}

fn partitions(node: &Map<String, Value>) -> Result<Option<u32>, CodecError> {
    match node.get("partitions") {
        None => Ok(None),
        Some(value) => value
            .as_u64()
            .and_then(|count| u32::try_from(count).ok())
            .map(Some)
            .ok_or_else(|| CodecError::new("partitions must be a positive integer")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_round_trips() {
        let config = TopicConfig {
            schema: Some("orders.json".to_owned()),
            schema_validate: true,
            kafka_topic: Some("orders".to_owned()),
            partitions: 3,
            retention_ms: Some(60_000),
            retention_bytes: None,
        };

        assert_eq!(TopicConfig::decode(&config.encode()).unwrap(), config);
    }

    #[test]
    fn update_round_trips_removals_and_omissions() {
        let update = ConfigUpdate {
            stream: "/orders".to_owned(),
            schema: Some(None),
            retention_ms: Some(Some(1_000)),
            retention_bytes: Some(None),
            partitions: Some(4),
            ..Default::default()
        };

        let wire = update.encode();
        assert_eq!(wire.path_and_query, "/_streams/orders");
        assert_eq!(
            ConfigUpdate::decode("/orders".to_owned(), &wire.body).unwrap(),
            update
        );
    }

    #[test]
    fn config_defaults_missing_fields() {
        assert_eq!(
            TopicConfig::decode(b"{}").unwrap(),
            TopicConfig {
                partitions: 1,
                ..Default::default()
            }
        );
    }

    #[test]
    fn update_rejects_bad_fields() {
        for body in [
            r#"{"kafkaTopic": "x"}"#,
            r#"{"schema": ""}"#,
            r#"{"schemaValidate": "yes"}"#,
            r#"{"retentionMs": -1}"#,
            r#"{"partitions": 1.5}"#,
            "[]",
        ] {
            assert!(
                ConfigUpdate::decode("/orders".to_owned(), body.as_bytes()).is_err(),
                "{body}"
            );
        }
    }
}

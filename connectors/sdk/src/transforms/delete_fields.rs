use super::{Transform, TransformType};
use crate::{DecodedMessage, Error, Payload, TopicMetadata};
use serde::{Deserialize, Serialize};
use simd_json::OwnedValue;
use std::collections::HashSet;

#[derive(Debug, Serialize, Deserialize)]
pub struct DeleteFieldsConfig {
    #[serde(default)]
    pub fields: Vec<String>,
}

pub struct DeleteFields {
    pub fields: HashSet<String>,
}

impl DeleteFields {
    pub fn new(cfg: DeleteFieldsConfig) -> Self {
        Self {
            fields: cfg.fields.into_iter().collect(),
        }
    }

    pub fn should_remove(&self, k: &str, _v: &OwnedValue) -> bool {
        self.fields.contains(k)
    }
}

impl Transform for DeleteFields {
    fn r#type(&self) -> TransformType {
        TransformType::DeleteFields
    }

    fn transform(
        &self,
        metadata: &TopicMetadata,
        message: DecodedMessage,
    ) -> Result<Option<DecodedMessage>, Error> {
        if self.fields.is_empty() {
            return Ok(Some(message));
        }

        match &message.payload {
            Payload::Json(_) => self.transform_json(metadata, message),
            _ => Ok(Some(message)),
        }
    }
}

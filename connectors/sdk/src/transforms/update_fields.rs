use super::{FieldValue, Transform, TransformType};
use crate::{DecodedMessage, Error, Payload, TopicMetadata};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct Field {
    pub key: String,
    pub value: FieldValue,
    #[serde(default)]
    pub condition: Option<UpdateCondition>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct UpdateFieldsConfig {
    #[serde(default)]
    pub fields: Vec<Field>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UpdateCondition {
    Always,
    KeyExists,
    KeyNotExists,
}

pub struct UpdateFields {
    pub fields: Vec<Field>,
}

impl UpdateFields {
    pub fn new(cfg: UpdateFieldsConfig) -> Self {
        Self { fields: cfg.fields }
    }
}

impl Transform for UpdateFields {
    fn r#type(&self) -> TransformType {
        TransformType::UpdateFields
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

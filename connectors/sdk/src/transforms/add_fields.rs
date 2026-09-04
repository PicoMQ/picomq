use super::{FieldValue, Transform, TransformType};
use crate::{DecodedMessage, Error, Payload, TopicMetadata};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct Field {
    pub key: String,
    pub value: FieldValue,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AddFieldsConfig {
    #[serde(default)]
    pub fields: Vec<Field>,
}

pub struct AddFields {
    pub fields: Vec<Field>,
}

impl AddFields {
    pub fn new(cfg: AddFieldsConfig) -> Self {
        Self { fields: cfg.fields }
    }
}

impl Transform for AddFields {
    fn r#type(&self) -> TransformType {
        TransformType::AddFields
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

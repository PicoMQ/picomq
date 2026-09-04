use super::{Transform, TransformType};
use crate::{DecodedMessage, Error, Payload, TopicMetadata};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct UnwrapEnvelopeConfig {
    pub field: String,
}

#[derive(Debug)]
pub struct UnwrapEnvelope {
    pub field: String,
}

impl UnwrapEnvelope {
    pub fn new(cfg: UnwrapEnvelopeConfig) -> Result<Self, Error> {
        if cfg.field.is_empty() {
            return Err(Error::InvalidConfigValue(
                "unwrap_envelope: 'field' must not be empty".into(),
            ));
        }
        Ok(Self { field: cfg.field })
    }
}

impl Transform for UnwrapEnvelope {
    fn r#type(&self) -> TransformType {
        TransformType::UnwrapEnvelope
    }

    fn transform(
        &self,
        metadata: &TopicMetadata,
        message: DecodedMessage,
    ) -> Result<Option<DecodedMessage>, Error> {
        match &message.payload {
            Payload::Json(_) => self.transform_json(metadata, message),
            _ => Ok(Some(message)),
        }
    }
}

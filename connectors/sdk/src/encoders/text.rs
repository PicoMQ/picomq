use crate::{Error, Payload, Schema, StreamEncoder};

pub struct TextStreamEncoder;

impl StreamEncoder for TextStreamEncoder {
    fn schema(&self) -> Schema {
        Schema::Text
    }

    fn encode(&self, payload: Payload) -> Result<Vec<u8>, Error> {
        match payload {
            Payload::Text(value) => Ok(value.into_bytes()),
            Payload::Json(value) => {
                Ok(simd_json::to_vec(&value).map_err(|_| Error::InvalidJsonPayload)?)
            }
            _ => Err(Error::InvalidPayloadType),
        }
    }
}

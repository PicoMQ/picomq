use crate::{Error, Payload, Schema, StreamEncoder};

pub struct JsonStreamEncoder;

impl StreamEncoder for JsonStreamEncoder {
    fn schema(&self) -> Schema {
        Schema::Json
    }

    fn encode(&self, payload: Payload) -> Result<Vec<u8>, Error> {
        match payload {
            Payload::Text(value) => {
                Ok(simd_json::to_vec(&value).map_err(|_| Error::InvalidJsonPayload)?)
            }
            Payload::Json(value) => {
                Ok(simd_json::to_vec(&value).map_err(|_| Error::InvalidJsonPayload)?)
            }
            _ => Err(Error::InvalidPayloadType),
        }
    }
}

use crate::{Error, Payload, Schema, StreamEncoder};

pub struct RawStreamEncoder;

impl StreamEncoder for RawStreamEncoder {
    fn schema(&self) -> Schema {
        Schema::Raw
    }

    fn encode(&self, payload: Payload) -> Result<Vec<u8>, Error> {
        match payload {
            Payload::Raw(value) => Ok(value),
            _ => Err(Error::InvalidPayloadType),
        }
    }
}

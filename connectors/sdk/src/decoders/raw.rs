use crate::{Error, Payload, Schema, StreamDecoder};

pub struct RawStreamDecoder;

impl StreamDecoder for RawStreamDecoder {
    fn schema(&self) -> Schema {
        Schema::Raw
    }

    fn decode(&self, payload: Vec<u8>) -> Result<Payload, Error> {
        Ok(Payload::Raw(payload))
    }
}

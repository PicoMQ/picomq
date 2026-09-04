use crate::{Error, Payload, Schema, StreamDecoder};
use tracing::error;

pub struct TextStreamDecoder;

impl StreamDecoder for TextStreamDecoder {
    fn schema(&self) -> Schema {
        Schema::Text
    }

    fn decode(&self, payload: Vec<u8>) -> Result<Payload, Error> {
        Ok(Payload::Text(String::from_utf8(payload).map_err(
            |error| {
                error!("Failed to decode text payload: {error}");
                Error::CannotDecode(self.schema())
            },
        )?))
    }
}

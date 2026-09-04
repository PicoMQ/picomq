use crate::{Error, Payload, Schema, StreamDecoder};
use tracing::error;

pub struct JsonStreamDecoder;

impl StreamDecoder for JsonStreamDecoder {
    fn schema(&self) -> Schema {
        Schema::Json
    }

    fn decode(&self, mut payload: Vec<u8>) -> Result<Payload, Error> {
        Ok(Payload::Json(
            simd_json::to_owned_value(&mut payload).map_err(|error| {
                error!("Failed to decode JSON payload: {error}");
                Error::CannotDecode(self.schema())
            })?,
        ))
    }
}

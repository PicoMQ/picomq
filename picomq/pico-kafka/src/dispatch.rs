use bytes::Bytes;

use kafka_protocol::protocol::decode_request_header_from_buffer;

use crate::broker::BrokerContext;
use crate::handlers::{HandlerError, HandlerOutcome};

#[derive(Debug, Clone)]
pub struct RequestContext {
    pub api_key: i16,
    pub api_version: i16,
    pub correlation_id: i32,
    pub client_id: Option<String>,
}

pub async fn dispatch(ctx: &BrokerContext, frame: &[u8]) -> Result<HandlerOutcome, HandlerError> {
    let mut body = Bytes::copy_from_slice(frame);
    let header = decode_request_header_from_buffer(&mut body)
        .map_err(|error| HandlerError::Protocol(error.to_string()))?;
    let req = RequestContext {
        api_key: header.request_api_key,
        api_version: header.request_api_version,
        correlation_id: header.correlation_id,
        client_id: header.client_id.map(|id| id.to_string()),
    };
    crate::handlers::dispatch(ctx, &req, &body).await
}

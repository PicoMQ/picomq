use kafka_protocol::messages::{InitProducerIdRequest, InitProducerIdResponse, ProducerId};
use kafka_protocol::protocol::Decodable;

use crate::broker::BrokerContext;
use crate::dispatch::RequestContext;
use crate::handlers::common::{encode_response, INVALID_REQUEST, NO_ERROR};
use crate::handlers::{HandlerError, HandlerOutcome};

pub async fn handle(
    ctx: &BrokerContext,
    req: &RequestContext,
    body: &[u8],
) -> Result<HandlerOutcome, HandlerError> {
    let mut body = bytes::Bytes::copy_from_slice(body);
    let request = InitProducerIdRequest::decode(&mut body, req.api_version)
        .map_err(|error| HandlerError::Protocol(error.to_string()))?;

    if request.transactional_id.is_some() {
        let response = InitProducerIdResponse::default().with_error_code(INVALID_REQUEST);
        return Ok(HandlerOutcome::Response(encode_response(
            req.correlation_id,
            req.api_version,
            &response,
        )));
    }

    let producer_id = ctx
        .allocate_producer_id()
        .await
        .map_err(|error| HandlerError::Protocol(error.to_string()))?;
    let response = InitProducerIdResponse::default()
        .with_error_code(NO_ERROR)
        .with_producer_id(ProducerId(producer_id))
        .with_producer_epoch(0);
    Ok(HandlerOutcome::Response(encode_response(
        req.correlation_id,
        req.api_version,
        &response,
    )))
}

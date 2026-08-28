use kafka_protocol::messages::api_versions_response::ApiVersion;
use kafka_protocol::messages::{ApiVersionsRequest, ApiVersionsResponse};
use kafka_protocol::protocol::Decodable;

use crate::broker::BrokerContext;
use crate::dispatch::RequestContext;
use crate::handlers::common::{encode_response, UNSUPPORTED_VERSION};
use crate::handlers::{HandlerError, HandlerOutcome};
use crate::versions;

pub async fn handle(
    _ctx: &BrokerContext,
    req: &RequestContext,
    body: &[u8],
) -> Result<HandlerOutcome, HandlerError> {
    if !versions::is_supported(req.api_key, req.api_version) {
        let response = ApiVersionsResponse::default().with_error_code(UNSUPPORTED_VERSION);
        return Ok(HandlerOutcome::Response(encode_response(
            req.correlation_id,
            0,
            &response,
        )));
    }

    let mut body = bytes::Bytes::copy_from_slice(body);
    ApiVersionsRequest::decode(&mut body, req.api_version)
        .map_err(|error| HandlerError::Protocol(error.to_string()))?;

    let api_keys = versions::supported_apis()
        .iter()
        .map(|api| {
            ApiVersion::default()
                .with_api_key(api.api_key)
                .with_min_version(api.min_version)
                .with_max_version(api.max_version)
        })
        .collect();
    let response = ApiVersionsResponse::default().with_api_keys(api_keys);
    Ok(HandlerOutcome::Response(encode_response(
        req.correlation_id,
        req.api_version,
        &response,
    )))
}

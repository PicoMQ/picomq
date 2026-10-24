use bytes::Bytes;
use picomq_auth::{AuthPrincipal, Authorizer, Operation};

use kafka_protocol::protocol::decode_request_header_from_buffer;

use crate::broker::BrokerContext;
use crate::handlers::{HandlerError, HandlerOutcome};

#[derive(Debug, Clone)]
pub struct RequestContext {
    pub api_key: i16,
    pub api_version: i16,
    pub correlation_id: i32,
    pub client_id: Option<String>,
    pub principal: Option<AuthPrincipal>,
}

impl RequestContext {
    pub fn allowed(
        &self,
        authorizer: Option<&Authorizer>,
        op: Operation,
        resource: Option<&str>,
    ) -> bool {
        match authorizer {
            None => true,
            Some(authorizer) => self
                .principal
                .as_ref()
                .is_some_and(|principal| authorizer.authorize(principal, op, resource).is_ok()),
        }
    }

    pub fn allows_operation(&self, authorizer: Option<&Authorizer>, op: Operation) -> bool {
        match authorizer {
            None => true,
            Some(_) => self
                .principal
                .as_ref()
                .is_some_and(|principal| principal.scope.allows_operation(op)),
        }
    }
}

pub fn request_context(
    frame: &[u8],
    principal: Option<AuthPrincipal>,
) -> Result<(RequestContext, Bytes), HandlerError> {
    let mut body = Bytes::copy_from_slice(frame);
    let header = decode_request_header_from_buffer(&mut body)
        .map_err(|error| HandlerError::Protocol(error.to_string()))?;

    let req = RequestContext {
        api_key: header.request_api_key,
        api_version: header.request_api_version,
        correlation_id: header.correlation_id,
        client_id: header.client_id.map(|id| id.to_string()),
        principal,
    };
    Ok((req, body))
}

pub async fn dispatch(
    ctx: &BrokerContext,
    principal: Option<AuthPrincipal>,
    frame: &[u8],
) -> Result<HandlerOutcome, HandlerError> {
    let (req, body) = request_context(frame, principal)?;
    crate::handlers::dispatch(ctx, &req, &body).await
}

use std::time::Instant;

use bytes::Bytes;
use picomq_auth::{AuthPrincipal, Authorizer, Operation, Scope};

use kafka_protocol::messages::ApiKey;
use kafka_protocol::protocol::decode_request_header_from_buffer;

use crate::broker::BrokerContext;
use crate::handlers::{HandlerError, HandlerOutcome};
use crate::versions::supported_apis;

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
            Some(_) => self
                .principal
                .as_ref()
                .is_some_and(|principal| principal.scope.allows(op, resource)),
        }
    }

    pub fn scope(&self) -> Option<&Scope> {
        self.principal.as_ref().map(|principal| &principal.scope)
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
    let api_key = req.api_key;
    let started = Instant::now();

    let outcome = crate::handlers::dispatch(ctx, &req, &body).await;
    observed(outcome, api_key, started)
}

pub fn name_operations() {
    for api in supported_apis() {
        if let Ok(key) = ApiKey::try_from(api.api_key) {
            picomq_metrics::name_kafka_operation(api.api_key, format!("{key:?}"));
        }
    }
}

fn observed(
    outcome: Result<HandlerOutcome, HandlerError>,
    api_key: i16,
    started: Instant,
) -> Result<HandlerOutcome, HandlerError> {
    match outcome {
        Ok(HandlerOutcome::Deferred(deferred)) => {
            Ok(HandlerOutcome::Deferred(Box::pin(async move {
                observed(deferred.await, api_key, started)
            })))
        }
        outcome => {
            picomq_metrics::kafka(api_key, outcome.is_ok(), started.elapsed());
            outcome
        }
    }
}

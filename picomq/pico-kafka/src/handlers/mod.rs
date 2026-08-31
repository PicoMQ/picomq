pub(crate) mod common;
mod fetch;
mod group;
mod metadata;
mod offsets;
mod produce;
mod producer;
mod topics;
mod versions;

use bytes::Bytes;
use kafka_protocol::messages::ApiKey;
use picomq_server::ServiceError;

use crate::broker::BrokerContext;
use crate::dispatch::RequestContext;

pub use common::ResponseFrame;

#[derive(Debug, thiserror::Error)]
pub enum HandlerError {
    #[error("protocol: {0}")]
    Protocol(String),
    #[error("unimplemented api key {0}")]
    Unimplemented(i16),
    #[error(transparent)]
    Service(#[from] ServiceError),
    #[error(transparent)]
    Batch(#[from] crate::batch::BatchParseError),
}

pub type DeferredOutcome = std::pin::Pin<
    Box<dyn std::future::Future<Output = Result<HandlerOutcome, HandlerError>> + Send>,
>;

pub enum HandlerOutcome {
    Response(ResponseFrame),
    NoResponse,
    Deferred(DeferredOutcome),
}

impl std::fmt::Debug for HandlerOutcome {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Response(frame) => f.debug_tuple("Response").field(frame).finish(),
            Self::NoResponse => f.write_str("NoResponse"),
            Self::Deferred(_) => f.write_str("Deferred"),
        }
    }
}

pub async fn dispatch(
    ctx: &BrokerContext,
    req: &RequestContext,
    body: &[u8],
) -> Result<HandlerOutcome, HandlerError> {
    if req.api_key != ApiKey::ApiVersions as i16
        && !crate::versions::is_supported(req.api_key, req.api_version)
    {
        return Err(HandlerError::Protocol(format!(
            "unsupported api key {} version {}",
            req.api_key, req.api_version
        )));
    }

    match req.api_key {
        key if key == ApiKey::ApiVersions as i16 => versions::handle(ctx, req, body).await,
        key if key == ApiKey::Metadata as i16 => metadata::handle(ctx, req, body).await,
        key if key == ApiKey::Produce as i16 => produce::handle(ctx, req, body).await,
        key if key == ApiKey::Fetch as i16 => fetch::handle(ctx, req, body).await,
        key if key == ApiKey::ListOffsets as i16 => offsets::handle(ctx, req, body).await,
        key if key == ApiKey::InitProducerId as i16 => producer::handle(ctx, req, body).await,
        key if key == ApiKey::CreateTopics as i16 => topics::create(ctx, req, body).await,
        key if key == ApiKey::DeleteTopics as i16 => topics::delete(ctx, req, body).await,
        key if matches!(
            ApiKey::try_from(key),
            Ok(ApiKey::FindCoordinator
                | ApiKey::JoinGroup
                | ApiKey::SyncGroup
                | ApiKey::Heartbeat
                | ApiKey::LeaveGroup
                | ApiKey::DescribeGroups
                | ApiKey::ListGroups
                | ApiKey::OffsetCommit
                | ApiKey::OffsetFetch)
        ) =>
        {
            group::handle(ctx, req, body).await
        }
        other => Err(HandlerError::Unimplemented(other)),
    }
}

impl HandlerOutcome {
    pub fn into_frame(self) -> Option<Bytes> {
        match self {
            Self::Response(frame) => Some(frame.0),
            Self::NoResponse | Self::Deferred(_) => None,
        }
    }
}

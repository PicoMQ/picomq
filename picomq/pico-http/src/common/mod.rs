mod schemas;

use std::sync::Arc;

use axum::Router;
use pico_auth::Authorizer;
use pico_server::S3StreamService;

pub use schemas::SCHEMA_PATH_PREFIX;

#[derive(Clone)]
pub struct CommonState {
    pub service: Arc<S3StreamService>,
    pub authorizer: Option<Arc<Authorizer>>,
}

pub fn router(
    service: Arc<S3StreamService>,
    authorizer: Option<Arc<Authorizer>>,
    max_request_size: usize,
) -> Router {
    let state = CommonState {
        service,
        authorizer,
    };
    schemas::router()
        .layer(axum::extract::DefaultBodyLimit::max(max_request_size))
        .with_state(state)
}

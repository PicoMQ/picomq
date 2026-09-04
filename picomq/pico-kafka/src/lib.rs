//! Kafka wire-protocol frontend for PicoMQ.

mod broker;
mod dispatch;
mod error;
mod frame;
mod group;
mod handlers;
mod listener;
mod versions;

pub const PROTOCOL_NAME: &str = "kafka";

pub use broker::BrokerContext;
pub use dispatch::{RequestContext, dispatch};
pub use error::KafkaError;
pub use frame::{read_frame, write_frame};
pub use handlers::{HandlerError, HandlerOutcome, ResponseFrame};
pub use listener::{KafkaListener, ListenerConfig};
pub use versions::{SupportedApi, lookup_versions, supported_apis};

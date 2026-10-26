//! HTTP frontends: Pico protocol and Durable Streams, plus admin and bind/shutdown.

pub mod admin;
mod auth;
pub mod common;
pub mod ds;
pub mod forward;
pub mod groups;
mod http;
mod observe;
mod offsets;
mod paging;
mod partition;
pub mod pico;
pub mod route;
pub mod serve;
mod topics;

pub use admin::AdminState;
pub use auth::{Permit, gate};
pub use ds::DsFrontend;
pub use pico::PicoFrontend;
pub use route::RoutingMode;
pub use serve::{ALPN, HttpProtocol, RunningServer, ServeOptions, serve};

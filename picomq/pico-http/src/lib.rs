//! HTTP frontends: Pico protocol and Durable Streams, plus admin and bind/shutdown.

pub mod admin;
mod auth;
pub mod common;
pub mod ds;
mod http;
pub mod pico;
pub mod route;
pub mod serve;

pub use admin::AdminState;
pub use auth::{Permit, gate};
pub use ds::DsFrontend;
pub use pico::PicoFrontend;
pub use route::RoutingMode;
pub use serve::{HttpProtocol, RunningServer, ServeOptions, serve};

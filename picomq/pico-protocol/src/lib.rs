//! Vocabulary of the native HTTP protocols, shared by `pico-http` and
//! `pico-client`. Kafka's wire spec is external and lives with `pico-kafka`.
//!
//! - [`pico`]: Pico protocol header and content type constants.
//! - [`ds`]: Durable Streams protocol header constants.
//! - [`envelope`]: the native record model and at-rest codecs.
//! - [`error`]: the codec error type.

pub mod ds;
pub mod envelope;
pub mod error;
pub mod pico;

pub use error::CodecError;

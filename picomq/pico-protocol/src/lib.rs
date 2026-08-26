//! The PicoMQ wire protocol vocabulary, shared by the server frontends and
//! the client SDK.
//!
//! - [`pico`]: Pico protocol header and content type constants.
//! - [`ds`]: Durable Streams protocol header constants.
//! - [`envelope`]: record envelopes, sequenced records, and the wire codecs.
//! - [`error`]: the codec error type.

pub mod ds;
pub mod envelope;
pub mod error;
pub mod pico;

pub use error::CodecError;

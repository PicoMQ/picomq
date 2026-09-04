//! Record codec and WAL record framing for s3stream.
//!
//! `specification/record-format.md` and `specification/wal-protocol.md` (record framing section).
//!
//! This crate is deliberately free of async and object-storage concerns: it defines the
//! two innermost wire formats (the stream record batch and the WAL record frame) plus
//! the CRC used by the WAL.

pub mod codec;
pub mod crc;
pub mod error;
pub mod record;
pub mod wal_record;

pub use codec::*;
pub use crc::wal_crc32;
pub use error::CodecError;
pub use record::StreamRecordBatch;
pub use wal_record::{
    RECORD_DATA_MAGIC, WAL_RECORD_HEADER_SIZE, WalRecordHeader, decode_record, frame_record,
};

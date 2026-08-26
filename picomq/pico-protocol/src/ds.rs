//! Durable Streams protocol header constants. The `Stream-*` and
//! `Producer-*` names are fixed by the open protocol spec and are NOT
//! rebranded.

pub const H_STREAM_NEXT_OFFSET: &str = "Stream-Next-Offset";
pub const H_STREAM_UP_TO_DATE: &str = "Stream-Up-To-Date";
pub const H_STREAM_TTL: &str = "Stream-TTL";
pub const H_STREAM_EXPIRES_AT: &str = "Stream-Expires-At";
pub const H_STREAM_CLOSED: &str = "Stream-Closed";
pub const H_STREAM_CURSOR: &str = "Stream-Cursor";
pub const H_STREAM_SSE_DATA_ENCODING: &str = "Stream-SSE-Data-Encoding";
pub const H_STREAM_SEQ: &str = "Stream-Seq";
pub const H_PRODUCER_ID: &str = "Producer-Id";
pub const H_PRODUCER_EPOCH: &str = "Producer-Epoch";
pub const H_PRODUCER_SEQ: &str = "Producer-Seq";
pub const H_PRODUCER_EXPECTED_SEQ: &str = "Producer-Expected-Seq";
pub const H_PRODUCER_RECEIVED_SEQ: &str = "Producer-Received-Seq";

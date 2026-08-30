//! Pico protocol header and content type constants. All custom headers use
//! the `Pico-*` prefix.

pub const H_START_SEQ: &str = "Pico-Start-Seq";
pub const H_NEXT_SEQ: &str = "Pico-Next-Seq";
pub const H_TIMESTAMP: &str = "Pico-Timestamp";
pub const H_MATCH_SEQ: &str = "Pico-Match-Seq";
pub const H_TRIM_SEQ: &str = "Pico-Trim-Seq";
pub const H_TTL: &str = "Pico-TTL";
pub const H_EXPIRES_AT: &str = "Pico-Expires-At";
pub const H_CLOSED: &str = "Pico-Closed";
pub const H_SCHEMA: &str = "Pico-Schema";
pub const H_SCHEMA_VALIDATE: &str = "Pico-Schema-Validate";
pub const H_UP_TO_DATE: &str = "Pico-Up-To-Date";
pub const H_CURSOR: &str = "Pico-Cursor";
pub const H_PRODUCER_ID: &str = "Pico-Producer-Id";
pub const H_PRODUCER_EPOCH: &str = "Pico-Producer-Epoch";
pub const H_PRODUCER_SEQ: &str = "Pico-Producer-Seq";
pub const H_EXPECTED_SEQ: &str = "Pico-Expected-Seq";
pub const H_RECEIVED_SEQ: &str = "Pico-Received-Seq";
pub const CT_BATCH_JSON: &str = "application/vnd.picomq.batch+json";
pub const CT_BATCH_BINARY: &str = "application/vnd.picomq.batch";
pub const CT_JSON: &str = "application/json";
pub const CT_EVENT_STREAM: &str = "text/event-stream";
/// The engine-side wrapper MIME. The user's content type is its `ct` param.
pub const CT_CORE: &str = "application/x-picomq";
pub const CT_CORE_PARAM: &str = "ct";
pub const DEFAULT_CT: &str = "application/octet-stream";

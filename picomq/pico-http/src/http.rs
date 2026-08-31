//! Protocol-neutral HTTP plumbing shared by the Pico and Durable Streams
//! handlers.
//!
//! MIME helpers live once in [`picomq_server::framing`] and are imported by
//! both frontends.

use std::time::Duration;

use axum::body::Body;
use axum::http::{HeaderMap, StatusCode};
use axum::response::Response;
use base64::Engine as _;

use picomq_server::{ErrorKind, OffsetToken, ServiceError};

pub(crate) fn bad_request(message: impl Into<String>) -> ServiceError {
    ServiceError::with_message(ErrorKind::BadRequest, None, false, message)
}

pub(crate) fn codec_error(e: picomq_protocol::CodecError) -> ServiceError {
    bad_request(e.message)
}

pub(crate) fn base_response(status: u16) -> Response {
    let mut response = Response::new(Body::empty());
    *response.status_mut() = StatusCode::from_u16(status).expect("valid status");
    set_header(&mut response, "X-Content-Type-Options", "nosniff");
    set_header(&mut response, "Cross-Origin-Resource-Policy", "same-origin");
    response
}

pub(crate) fn set_header(response: &mut Response, name: &str, value: &str) {
    let name = axum::http::HeaderName::from_bytes(name.as_bytes()).expect("valid header name");
    let value = axum::http::HeaderValue::from_str(value)
        .unwrap_or_else(|_| axum::http::HeaderValue::from_static("invalid"));
    response.headers_mut().insert(name, value);
}

pub(crate) fn header_str<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    headers.get(name).and_then(|v| v.to_str().ok())
}

pub(crate) fn truthy(headers: &HeaderMap, name: &str) -> bool {
    header_str(headers, name).is_some_and(|v| v.eq_ignore_ascii_case("true"))
}

pub(crate) fn query_param(uri: &axum::http::Uri, name: &str) -> Option<String> {
    let query = uri.query()?;
    for pair in query.split('&') {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        if key == name {
            return Some(percent_decode(value));
        }
    }
    None
}

fn percent_decode(raw: &str) -> String {
    let mut out = Vec::with_capacity(raw.len());
    let bytes = raw.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'%' if i + 3 <= bytes.len() => match u8::from_str_radix(&raw[i + 1..i + 3], 16) {
                Ok(byte) => {
                    out.push(byte);
                    i += 3;
                }
                Err(_) => {
                    out.push(b'%');
                    i += 1;
                }
            },
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            byte => {
                out.push(byte);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

pub(crate) fn parse_strict_u64(
    raw: Option<&str>,
    message: &str,
) -> Result<Option<u64>, ServiceError> {
    let Some(raw) = raw.filter(|v| !v.is_empty()) else {
        return Ok(None);
    };
    let strict = raw.bytes().all(|b| b.is_ascii_digit()) && (raw == "0" || !raw.starts_with('0'));
    if !strict {
        return Err(bad_request(message.to_owned()));
    }
    raw.parse::<u64>()
        .map(Some)
        .map_err(|_| bad_request(message.to_owned()))
}

pub(crate) fn parse_strict_u64_header(
    headers: &HeaderMap,
    name: &str,
    message: &str,
) -> Result<Option<u64>, ServiceError> {
    parse_strict_u64(header_str(headers, name), message)
}

/// RFC3339 to epoch ms.
pub(crate) fn parse_instant_header(
    headers: &HeaderMap,
    name: &str,
    message: &str,
) -> Result<Option<i64>, ServiceError> {
    let Some(raw) = header_str(headers, name).filter(|v| !v.is_empty()) else {
        return Ok(None);
    };
    let time = humantime::parse_rfc3339(raw).map_err(|_| bad_request(message.to_owned()))?;
    let ms = time
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| bad_request(message.to_owned()))?
        .as_millis() as i64;
    Ok(Some(ms))
}

pub(crate) fn format_instant(epoch_ms: i64) -> String {
    let time = std::time::UNIX_EPOCH + Duration::from_millis(epoch_ms.max(0) as u64);
    if epoch_ms % 1000 == 0 {
        humantime::format_rfc3339_seconds(time).to_string()
    } else {
        humantime::format_rfc3339_millis(time).to_string()
    }
}

/// 20s interval counter bumped past the client's value with
/// jitter, keeping repeated polls cache-busting.
pub(crate) fn cursor(raw: Option<&str>) -> u64 {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock before unix epoch");
    let mut interval = (now.as_millis() / 20_000) as u64;
    if let Some(client) = raw.and_then(|raw| raw.parse::<u64>().ok()) {
        if interval <= client {
            interval = client + 1 + (now.subsec_nanos() as u64) % 60;
        }
    }
    interval
}

/// Etag format: `"base64(scope):start:end[:c]"`.
pub(crate) fn etag(
    scope: &str,
    start: &OffsetToken,
    end: &OffsetToken,
    closed_at_tail: bool,
) -> String {
    let encoded = base64::engine::general_purpose::STANDARD.encode(scope.as_bytes());
    let suffix = if closed_at_tail { ":c" } else { "" };
    format!(
        "\"{}:{}:{}{}\"",
        encoded,
        start.value(),
        end.value(),
        suffix
    )
}

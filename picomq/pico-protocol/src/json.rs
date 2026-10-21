use bytes::Bytes;
use http::Method;
use serde_json::{Map, Value, json};

use crate::error::CodecError;
use crate::pico::CT_JSON;
use crate::wire::WireRequest;

pub(crate) fn json_request(
    method: Method,
    path: String,
    body: Map<String, Value>,
    ok: &'static [u16],
) -> WireRequest {
    WireRequest::new(method, path, ok)
        .header("Content-Type", CT_JSON)
        .body(json_body(Value::Object(body)))
}

pub(crate) fn json_body(value: Value) -> Bytes {
    Bytes::from(serde_json::to_vec(&value).expect("json encode"))
}

pub(crate) fn put_str(node: &mut Map<String, Value>, key: &str, value: Option<&str>) {
    if let Some(value) = value {
        node.insert(key.into(), json!(value));
    }
}

pub(crate) fn put_u32(node: &mut Map<String, Value>, key: &str, value: Option<u32>) {
    if let Some(value) = value {
        node.insert(key.into(), json!(value));
    }
}

pub(crate) fn object(body: &[u8]) -> Result<Map<String, Value>, CodecError> {
    match serde_json::from_slice::<Value>(body) {
        Ok(Value::Object(node)) => Ok(node),
        _ => Err(CodecError::new("expected a JSON object")),
    }
}

pub(crate) fn string(node: &Map<String, Value>, key: &str) -> Result<Option<String>, CodecError> {
    match node.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) => Ok(Some(value.clone())),
        Some(_) => Err(CodecError::new(format!("{key} must be a string"))),
    }
}

pub(crate) fn required_string(node: &Map<String, Value>, key: &str) -> Result<String, CodecError> {
    string(node, key)?.ok_or_else(|| CodecError::new(format!("{key} is required")))
}

pub(crate) fn i32_of(node: &Map<String, Value>, key: &str) -> Result<Option<i32>, CodecError> {
    match node.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Number(value)) => value
            .as_i64()
            .and_then(|value| i32::try_from(value).ok())
            .map(Some)
            .ok_or_else(|| CodecError::new(format!("{key} must be a 32-bit integer"))),
        Some(_) => Err(CodecError::new(format!("{key} must be an integer"))),
    }
}

pub(crate) fn required_i32(node: &Map<String, Value>, key: &str) -> Result<i32, CodecError> {
    i32_of(node, key)?.ok_or_else(|| CodecError::new(format!("{key} is required")))
}

pub(crate) fn u32_of(node: &Map<String, Value>, key: &str) -> Result<Option<u32>, CodecError> {
    match node.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Number(value)) => value
            .as_u64()
            .and_then(|value| u32::try_from(value).ok())
            .map(Some)
            .ok_or_else(|| CodecError::new(format!("{key} must be a non-negative 32-bit integer"))),
        Some(_) => Err(CodecError::new(format!("{key} must be an integer"))),
    }
}

pub(crate) fn strings(
    node: &Map<String, Value>,
    key: &str,
) -> Result<Option<Vec<String>>, CodecError> {
    match node.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Array(values)) => values
            .iter()
            .map(|value| {
                value
                    .as_str()
                    .map(str::to_owned)
                    .ok_or_else(|| CodecError::new(format!("{key} must be an array of strings")))
            })
            .collect::<Result<Vec<_>, _>>()
            .map(Some),
        Some(_) => Err(CodecError::new(format!(
            "{key} must be an array of strings"
        ))),
    }
}

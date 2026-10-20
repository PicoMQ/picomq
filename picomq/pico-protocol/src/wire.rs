use bytes::Bytes;
use http::{HeaderMap, Method};

use crate::error::CodecError;

#[derive(Debug, Clone)]
pub struct WireRequest {
    pub method: Method,
    pub path_and_query: String,
    pub headers: Vec<(&'static str, String)>,
    pub body: Bytes,
    pub ok: &'static [u16],
}

impl WireRequest {
    pub(crate) fn new(method: Method, path_and_query: String, ok: &'static [u16]) -> Self {
        Self {
            method,
            path_and_query,
            headers: Vec::new(),
            body: Bytes::new(),
            ok,
        }
    }

    pub(crate) fn header(mut self, name: &'static str, value: impl Into<String>) -> Self {
        self.headers.push((name, value.into()));
        self
    }

    pub(crate) fn header_opt(mut self, name: &'static str, value: Option<impl ToString>) -> Self {
        if let Some(value) = value {
            self.headers.push((name, value.to_string()));
        }
        self
    }

    pub(crate) fn flag(mut self, name: &'static str, on: bool) -> Self {
        if on {
            self.headers.push((name, "true".to_owned()));
        }
        self
    }

    pub(crate) fn body(mut self, body: Bytes) -> Self {
        self.body = body;
        self
    }
}

pub fn bearer(token: &str) -> String {
    format!("Bearer {token}")
}

#[derive(Debug, Clone, Copy)]
pub struct Producer<'a> {
    pub id: &'a str,
    pub epoch: u64,
    pub seq: u64,
}

pub fn stream_path(name: &str) -> String {
    let name = name.strip_prefix('/').unwrap_or(name);
    let mut path = String::with_capacity(name.len() + 1);
    for segment in name.split('/') {
        path.push('/');
        path.push_str(&urlencode(segment));
    }
    path
}

pub fn check_path(path_and_query: &str) -> Result<(), CodecError> {
    let path = path_and_query.split('?').next().unwrap_or_default();
    if path
        .split('/')
        .any(|segment| segment == "." || segment == "..")
    {
        return Err(CodecError::new(format!(
            "{path} has a . or .. segment, which a URL cannot carry"
        )));
    }

    Ok(())
}

pub const Q_PARTITION: &str = "partition";

pub(crate) fn partition_path(name: &str, partition: Option<u32>) -> String {
    let mut path = stream_path(name);
    push_partition(&mut path, partition);
    path
}

pub(crate) fn push_partition(target: &mut String, partition: Option<u32>) {
    if let Some(partition) = partition {
        let separator = if target.contains('?') { '&' } else { '?' };
        target.push_str(&format!("{separator}{Q_PARTITION}={partition}"));
    }
}

pub(crate) fn urlencode(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                out.push(byte as char)
            }
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

pub(crate) fn header_str<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    headers.get(name).and_then(|value| value.to_str().ok())
}

pub(crate) fn header_string(headers: &HeaderMap, name: &str) -> Option<String> {
    header_str(headers, name).map(str::to_owned)
}

pub(crate) fn header_u64(headers: &HeaderMap, name: &str) -> Option<u64> {
    header_str(headers, name).and_then(|value| value.parse().ok())
}

pub(crate) fn header_i64(headers: &HeaderMap, name: &str) -> Option<i64> {
    header_str(headers, name).and_then(|value| value.parse().ok())
}

pub(crate) fn truthy(headers: &HeaderMap, name: &str) -> bool {
    header_str(headers, name).is_some_and(|value| value.eq_ignore_ascii_case("true"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn paths_with_dot_segments_are_refused() {
        for path in ["/..", "/../orders", "/a/./b?x=1", "/_schemas/a/.."] {
            assert!(check_path(path).is_err(), "{path}");
        }
        for path in ["/a..b", "/.orders", "//orders", "/a?seq=..", "/"] {
            assert!(check_path(path).is_ok(), "{path}");
        }
    }
}

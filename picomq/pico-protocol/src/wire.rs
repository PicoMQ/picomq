use bytes::Bytes;
use http::{HeaderMap, Method};

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

pub(crate) fn stream_path(name: &str) -> String {
    if name.starts_with('/') {
        name.to_owned()
    } else {
        format!("/{name}")
    }
}

pub(crate) fn urlencode(value: &str) -> String {
    value
        .chars()
        .map(|c| match c {
            '&' => "%26".to_owned(),
            '=' => "%3D".to_owned(),
            '?' => "%3F".to_owned(),
            '#' => "%23".to_owned(),
            ' ' => "%20".to_owned(),
            '+' => "%2B".to_owned(),
            other => other.to_string(),
        })
        .collect()
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

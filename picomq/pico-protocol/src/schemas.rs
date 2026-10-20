use bytes::Bytes;
use http::{HeaderMap, Method};

use crate::wire::{WireRequest, header_string, stream_path};

pub const SCHEMAS_PATH_PREFIX: &str = "/_schemas";

#[derive(Debug, Clone)]
pub struct PutSchemaRequest<'a> {
    pub name: &'a str,
    pub content_type: Option<&'a str>,
    pub body: Bytes,
}

impl PutSchemaRequest<'_> {
    pub fn encode(&self) -> WireRequest {
        WireRequest::new(Method::PUT, schema_path(self.name), &[204])
            .header_opt("content-type", self.content_type)
            .body(self.body.clone())
    }
}

#[derive(Debug, Clone)]
pub struct GetSchemaRequest<'a> {
    pub name: &'a str,
}

impl GetSchemaRequest<'_> {
    pub fn encode(&self) -> WireRequest {
        WireRequest::new(Method::GET, schema_path(self.name), &[200, 404])
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Schema {
    pub content_type: Option<String>,
    pub body: Bytes,
}

impl Schema {
    pub fn decode(status: u16, headers: &HeaderMap, body: Bytes) -> Option<Self> {
        (status != 404).then(|| Self {
            content_type: header_string(headers, "content-type"),
            body,
        })
    }
}

#[derive(Debug, Clone)]
pub struct DeleteSchemaRequest<'a> {
    pub name: &'a str,
}

impl DeleteSchemaRequest<'_> {
    pub fn encode(&self) -> WireRequest {
        WireRequest::new(Method::DELETE, schema_path(self.name), &[204, 404])
    }
}

fn schema_path(name: &str) -> String {
    format!("{SCHEMAS_PATH_PREFIX}{}", stream_path(name))
}

// Modified from Apache Iggy for PicoMQ.
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use async_trait::async_trait;
use base64::{self, Engine};
use decoders::{
    avro::AvroStreamDecoder, flatbuffer::FlatBufferStreamDecoder, json::JsonStreamDecoder,
    proto::ProtoStreamDecoder, raw::RawStreamDecoder, text::TextStreamDecoder,
};
use encoders::{
    avro::AvroStreamEncoder, flatbuffer::FlatBufferStreamEncoder, json::JsonStreamEncoder,
    proto::ProtoStreamEncoder, raw::RawStreamEncoder, text::TextStreamEncoder,
};
use prost::Message;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::{Arc, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};
use strum_macros::{Display, IntoStaticStr};
use thiserror::Error;
use tokio::runtime::Runtime;

#[cfg(feature = "api")]
pub mod api;
pub mod convert;
pub mod decoders;
pub mod destination;
pub mod encoders;
pub mod log;
pub mod retry;
pub mod secret;
pub mod sink;
pub mod source;
pub mod transforms;

pub use convert::owned_value_to_serde_json;
pub use log::LogCallback;
pub use transforms::Transform;

#[doc(hidden)]
pub mod connector_macro_support {
    pub use dashmap::DashMap;
}

static RUNTIME: OnceLock<Runtime> = OnceLock::new();

pub fn get_runtime() -> &'static Runtime {
    RUNTIME.get_or_init(|| Runtime::new().expect("Failed to create Tokio runtime"))
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ConnectorState(pub Vec<u8>);

impl ConnectorState {
    pub fn deserialize<T: serde::de::DeserializeOwned>(
        self,
        connector_name: &str,
        connector_id: u32,
    ) -> Option<T> {
        rmp_serde::from_slice(&self.0)
            .inspect_err(|error| {
                tracing::warn!(
                    "Failed to deserialize state for {connector_name} connector with ID: {connector_id}. {error}"
                );
            })
            .ok()
    }

    pub fn serialize<T: serde::Serialize>(
        state: &T,
        connector_name: &str,
        connector_id: u32,
    ) -> Option<Self> {
        rmp_serde::to_vec(state)
            .inspect_err(|error| {
                tracing::error!(
                    "Failed to serialize state for {connector_name} connector with ID: {connector_id}. {error}"
                );
            })
            .ok()
            .map(ConnectorState)
    }
}

#[async_trait]
pub trait Source: Send + Sync {
    async fn open(&mut self) -> Result<(), Error>;

    async fn poll(&self) -> Result<ProducedMessages, Error>;

    async fn on_batch_result(&self, _result: source::SourceBatchResult) -> Result<(), Error> {
        Ok(())
    }

    async fn close(&mut self) -> Result<(), Error>;
}

#[async_trait]
pub trait Sink: Send + Sync {
    async fn open(&mut self) -> Result<(), Error>;

    async fn consume(
        &self,
        topic_metadata: &TopicMetadata,
        messages_metadata: MessagesMetadata,
        messages: Vec<ConsumedMessage>,
    ) -> Result<(), Error>;

    async fn close(&mut self) -> Result<(), Error>;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Payload {
    Json(simd_json::OwnedValue),
    Raw(Vec<u8>),
    Text(String),
    Proto(String),
    FlatBuffer(Vec<u8>),
    Avro(Vec<u8>),
}

impl Payload {
    pub fn try_into_vec(self) -> Result<Vec<u8>, Error> {
        match self {
            Payload::Json(value) => {
                Ok(simd_json::to_vec(&value).map_err(|_| Error::InvalidJsonPayload)?)
            }
            Payload::Raw(value) => Ok(value),
            Payload::Text(text) => Ok(text.into_bytes()),
            Payload::Proto(text) => Ok(text.into_bytes()),
            Payload::FlatBuffer(value) => Ok(value),
            Payload::Avro(value) => Ok(value),
        }
    }

    pub fn try_to_bytes(&self) -> Result<Vec<u8>, Error> {
        match self {
            Payload::Json(value) => simd_json::to_vec(value).map_err(|_| Error::InvalidJsonPayload),
            Payload::Raw(value) => Ok(value.clone()),
            Payload::Text(text) => Ok(text.as_bytes().to_vec()),
            Payload::Proto(text) => Ok(text.as_bytes().to_vec()),
            Payload::FlatBuffer(value) => Ok(value.clone()),
            Payload::Avro(value) => Ok(value.clone()),
        }
    }
}

impl std::fmt::Display for Payload {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Payload::Json(value) => write!(
                f,
                "Json({})",
                simd_json::to_string_pretty(value).unwrap_or_default()
            ),
            Payload::Raw(value) => write!(f, "Raw({value:#?})"),
            Payload::Text(text) => write!(f, "Text({text})"),
            Payload::Proto(text) => write!(f, "Proto({text})"),
            Payload::FlatBuffer(value) => write!(f, "FlatBuffer({} bytes)", value.len()),
            Payload::Avro(value) => write!(f, "Avro({} bytes)", value.len()),
        }
    }
}

#[repr(C)]
#[derive(
    Debug, Default, Copy, Clone, Eq, Hash, PartialEq, Serialize, Deserialize, Display, IntoStaticStr,
)]
#[serde(rename_all = "snake_case")]
pub enum Schema {
    #[default]
    #[strum(to_string = "json")]
    Json,
    #[strum(to_string = "raw")]
    Raw,
    #[strum(to_string = "text")]
    Text,
    #[strum(to_string = "proto")]
    Proto,
    #[strum(to_string = "flatbuffer")]
    FlatBuffer,
    #[strum(to_string = "avro")]
    Avro,
}

impl Schema {
    pub fn try_into_payload(self, mut value: Vec<u8>) -> Result<Payload, Error> {
        match self {
            Schema::Json => Ok(Payload::Json(
                simd_json::to_owned_value(&mut value).map_err(|_| Error::InvalidJsonPayload)?,
            )),
            Schema::Raw => Ok(Payload::Raw(value)),
            Schema::Text => Ok(Payload::Text(
                String::from_utf8(value).map_err(|_| Error::InvalidTextPayload)?,
            )),
            Schema::Proto => match prost_types::Any::decode(value.as_slice()) {
                Ok(any) => {
                    let json_value = simd_json::json!({
                        "type_url": any.type_url,
                        "value": base64::engine::general_purpose::STANDARD.encode(&any.value),
                    });
                    Ok(Payload::Json(json_value))
                }
                Err(_) => Ok(Payload::Raw(value)),
            },
            Schema::FlatBuffer => Ok(Payload::FlatBuffer(value)),
            Schema::Avro => Ok(Payload::Avro(value)),
        }
    }

    pub fn decoder(self) -> Arc<dyn StreamDecoder> {
        match self {
            Schema::Json => Arc::new(JsonStreamDecoder),
            Schema::Raw => Arc::new(RawStreamDecoder),
            Schema::Text => Arc::new(TextStreamDecoder),
            Schema::Proto => Arc::new(ProtoStreamDecoder::default()),
            Schema::FlatBuffer => Arc::new(FlatBufferStreamDecoder::default()),
            Schema::Avro => Arc::new(AvroStreamDecoder::default()),
        }
    }

    pub fn encoder(self) -> Arc<dyn StreamEncoder> {
        match self {
            Schema::Json => Arc::new(JsonStreamEncoder),
            Schema::Raw => Arc::new(RawStreamEncoder),
            Schema::Text => Arc::new(TextStreamEncoder),
            Schema::Proto => Arc::new(ProtoStreamEncoder::default()),
            Schema::FlatBuffer => Arc::new(FlatBufferStreamEncoder::default()),
            Schema::Avro => Arc::new(AvroStreamEncoder::default()),
        }
    }
}

pub type Headers = BTreeMap<String, Vec<u8>>;

pub fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as u64)
        .unwrap_or_default()
}

#[repr(C)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicMetadata {
    pub topic: String,
}

#[repr(C)]
#[derive(Debug, Serialize, Deserialize)]
pub struct MessagesMetadata {
    pub partition: i32,
    pub current_offset: u64,
    pub schema: Schema,
}

#[repr(C)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReceivedMessage {
    pub offset: u64,
    pub timestamp: u64,
    pub key: Option<Vec<u8>>,
    pub headers: Option<Headers>,
    pub payload: Vec<u8>,
}

#[repr(C)]
#[derive(Debug, Serialize, Deserialize)]
pub struct ProducedMessages {
    pub schema: Schema,
    pub messages: Vec<ProducedMessage>,
    pub state: Option<ConnectorState>,
}

#[repr(C)]
#[derive(Debug, Serialize, Deserialize)]
pub struct ProducedMessage {
    pub key: Option<Vec<u8>>,
    pub timestamp: Option<u64>,
    pub headers: Option<Headers>,
    pub payload: Vec<u8>,
}

#[repr(C)]
#[derive(Debug, Serialize, Deserialize)]
pub struct DecodedMessage {
    pub offset: Option<u64>,
    pub timestamp: Option<u64>,
    pub key: Option<Vec<u8>>,
    pub headers: Option<Headers>,
    pub payload: Payload,
}

#[repr(C)]
#[derive(Debug, Serialize, Deserialize)]
pub struct RawMessages {
    pub schema: Schema,
    pub messages: Vec<RawMessage>,
}

#[repr(C)]
#[derive(Debug, Serialize, Deserialize)]
pub struct RawMessage {
    pub offset: u64,
    pub timestamp: u64,
    pub key: Option<Vec<u8>>,
    pub headers: Vec<u8>,
    pub payload: Vec<u8>,
}

#[repr(C)]
#[derive(Debug, Serialize, Deserialize)]
pub struct ConsumedMessage {
    pub offset: u64,
    pub timestamp: u64,
    pub key: Option<Vec<u8>>,
    pub headers: Option<Headers>,
    pub payload: Payload,
}

pub trait StreamDecoder: Send + Sync {
    fn schema(&self) -> Schema;
    fn decode(&self, payload: Vec<u8>) -> Result<Payload, Error>;
}

pub trait StreamEncoder: Send + Sync {
    fn schema(&self) -> Schema;
    fn encode(&self, payload: Payload) -> Result<Vec<u8>, Error>;
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Error)]
pub enum Error {
    #[error("Invalid config")]
    InvalidConfig,
    #[error("Invalid config value: {0}")]
    InvalidConfigValue(String),
    #[error("Invalid record")]
    InvalidRecord,
    #[error("Invalid record value: {0}")]
    InvalidRecordValue(String),
    #[error("Invalid transformer")]
    InvalidTransformer,
    #[error("HTTP request failed: {0}")]
    HttpRequestFailed(String),
    #[error("Init error: {0}")]
    InitError(String),
    #[error("Invalid payload type")]
    InvalidPayloadType,
    #[error("Invalid JSON payload.")]
    InvalidJsonPayload,
    #[error("Invalid text payload.")]
    InvalidTextPayload,
    #[error("Cannot decode schema {0}")]
    CannotDecode(Schema),
    #[error("Storage error: {0}")]
    Storage(String),
    #[error("Serialization error: {0}")]
    Serialization(String),
    #[error("Invalid protobuf payload.")]
    InvalidProtobufPayload,
    #[error("Cannot open state file")]
    CannotOpenStateFile,
    #[error("Cannot read state file")]
    CannotReadStateFile,
    #[error("Cannot write state file")]
    CannotWriteStateFile,
    #[error("Invalid state")]
    InvalidState,
    #[error("Connection error: {0}")]
    Connection(String),
    #[error("Cannot store data: {0}")]
    CannotStoreData(String),
    #[error("Permanent HTTP error: {0}")]
    PermanentHttpError(String),
    #[error("Schema mismatch: {0}")]
    SchemaMismatch(String),
    #[error("Write failure: {0}")]
    WriteFailure(String),
    #[error("Transaction apply error: {0}")]
    TransactionApplyError(String),
    #[error("Catalog commit error: {0}")]
    CatalogCommitError(String),
    #[error("Transient state error: {0}")]
    TransientState(String),
    #[error("Permanent state error: {0}")]
    PermanentState(String),
    #[error("State provider latched after a permanent state error")]
    StateLatched,
}

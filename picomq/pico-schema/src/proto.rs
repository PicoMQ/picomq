// Copyright 2026 PicoMQ contributors
// Copyright ⓒ 2024-2025 Peter Morgan
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Protocol Buffer message schema

use std::{io::Write, sync::LazyLock};

use crate::{record::Batch, Error, Result, Validator};
use bytes::{BufMut, Bytes, BytesMut};
use protobuf::{
    descriptor, reflect::FileDescriptor, reflect::MessageDescriptor, well_known_types,
    CodedInputStream, MessageDyn,
};
use protobuf_json_mapping::parse_dyn_from_str;
use serde_json::Value;
use tempfile::{tempdir, NamedTempFile};
use tracing::{debug, error};

#[derive(Copy, Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum MessageKind {
    Key,
    Value,
}

impl AsRef<str> for MessageKind {
    fn as_ref(&self) -> &str {
        match self {
            MessageKind::Key => "Key",
            MessageKind::Value => "Value",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Schema {
    file_descriptors: Vec<FileDescriptor>,
}

impl Schema {
    fn message_by_package_relative_name(
        &self,
        message_kind: MessageKind,
    ) -> Option<MessageDescriptor> {
        self.file_descriptors
            .iter()
            .find_map(|fd| fd.message_by_package_relative_name(message_kind.as_ref()))
    }

    fn value_to_message(
        &self,
        message_kind: MessageKind,
        json: &Value,
    ) -> Result<Box<dyn MessageDyn>> {
        self.file_descriptors
            .iter()
            .find_map(|fd| fd.message_by_package_relative_name(message_kind.as_ref()))
            .ok_or(Error::Message(format!(
                "message {message_kind:?} not found"
            )))
            .and_then(|message_descriptor| {
                serde_json::to_string(json)
                    .map_err(Error::from)
                    .and_then(|json| {
                        parse_dyn_from_str(&message_descriptor, json.as_str()).map_err(Into::into)
                    })
            })
    }

    pub fn encode_from_value(&self, message_kind: MessageKind, json: &Value) -> Result<Bytes> {
        self.value_to_message(message_kind, json)
            .and_then(message_to_bytes)
    }
}

fn message_to_bytes(message: Box<dyn MessageDyn>) -> Result<Bytes> {
    let mut w = BytesMut::new().writer();
    message
        .write_to_writer_dyn(&mut w)
        .and(Ok(Bytes::from(w.into_inner())))
        .map_err(Into::into)
}

fn decode(
    message_descriptor: Option<MessageDescriptor>,
    encoded: Option<Bytes>,
) -> Result<Option<Box<dyn MessageDyn>>> {
    debug!(?message_descriptor, ?encoded);

    message_descriptor.map_or(Ok(None), |message_descriptor| {
        encoded.map_or(Err(crate::Error::InvalidRecord), |encoded| {
            let mut message = message_descriptor.new_instance();

            message
                .merge_from_dyn(&mut CodedInputStream::from_tokio_bytes(&encoded))
                .inspect_err(|err| error!(?err))
                .map_err(|_err| crate::Error::InvalidRecord)
                .and(Ok(Some(message)))
                .inspect(|message| debug!(?message))
        })
    })
}

fn validate(message_descriptor: Option<MessageDescriptor>, encoded: Option<Bytes>) -> Result<()> {
    decode(message_descriptor, encoded).and(Ok(()))
}

impl Validator for Schema {
    fn validate(&self, batch: &Batch) -> Result<()> {
        debug!(?batch);

        for record in &batch.records {
            debug!(?record);

            validate(
                self.message_by_package_relative_name(MessageKind::Key),
                record.key.clone(),
            )
            .and(validate(
                self.message_by_package_relative_name(MessageKind::Value),
                record.value.clone(),
            ))
            .inspect_err(|err| error!(?err))?
        }

        Ok(())
    }
}

impl TryFrom<Bytes> for Schema {
    type Error = Error;

    fn try_from(proto: Bytes) -> Result<Self, Self::Error> {
        make_fd(proto)
            .inspect(|protos| {
                debug!(
                    protos = ?protos
                        .iter()
                        .flat_map(|proto| {
                            proto
                                .messages()
                                .map(|message| message.name_to_package().to_owned())
                        })
                        .collect::<Vec<_>>()
                );
            })
            .map(|file_descriptors| Self { file_descriptors })
    }
}

static WELL_KNOWN_TYPES: LazyLock<Vec<FileDescriptor>> = LazyLock::new(|| {
    vec![
        descriptor::file_descriptor().to_owned(),
        well_known_types::duration::file_descriptor().to_owned(),
        well_known_types::empty::file_descriptor().to_owned(),
        well_known_types::source_context::file_descriptor().to_owned(),
        well_known_types::timestamp::file_descriptor().to_owned(),
        well_known_types::wrappers::file_descriptor().to_owned(),
    ]
});

fn make_fd(proto: Bytes) -> Result<Vec<FileDescriptor>> {
    tempdir().map_err(Into::into).and_then(|temp_dir| {
        NamedTempFile::new_in(&temp_dir)
            .inspect(|temp_dir| debug!(?temp_dir))
            .map_err(Into::into)
            .and_then(|mut temp_file| {
                temp_file.write_all(&proto).map_err(Into::into).and(
                    protobuf_parse::Parser::new()
                        .pure()
                        .input(&temp_file)
                        .include(&temp_dir)
                        .parse_and_typecheck()
                        .inspect_err(|err| debug!(?err))
                        .map_err(Into::into)
                        .and_then(|parsed| {
                            parsed
                                .file_descriptors
                                .into_iter()
                                .map(|file_descriptor_proto| {
                                    FileDescriptor::new_dynamic(
                                        file_descriptor_proto,
                                        &WELL_KNOWN_TYPES[..],
                                    )
                                    .inspect_err(|err| debug!(?err))
                                    .map_err(Into::into)
                                })
                                .collect::<Result<Vec<_>>>()
                        }),
                )
            })
    })
}

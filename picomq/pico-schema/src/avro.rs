// Copyright 2026 PicoMQ contributors
// Copyright ⓒ 2024-2026 Peter Morgan
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

use apache_avro::{schema::Schema as AvroSchema, types::Value, Reader};
use bytes::Bytes;

use crate::record::Batch;
use serde_json::Value as JsonValue;
use tracing::{debug, error, info};

use crate::{Error, Result, Validator};

#[derive(Copy, Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) enum MessageKind {
    Key,
    Value,
}

impl AsRef<str> for MessageKind {
    fn as_ref(&self) -> &str {
        match self {
            MessageKind::Key => "key",
            MessageKind::Value => "value",
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct Schema {
    pub(crate) key: Option<AvroSchema>,
    pub(crate) value: Option<AvroSchema>,
}

impl Schema {
    pub fn key(&self) -> Option<&AvroSchema> {
        self.key.as_ref()
    }

    pub fn value(&self) -> Option<&AvroSchema> {
        self.value.as_ref()
    }
}

impl TryFrom<Bytes> for Schema {
    type Error = Error;

    fn try_from(encoded: Bytes) -> Result<Self, Self::Error> {
        serde_json::from_slice::<JsonValue>(&encoded[..])
            .map_err(Into::into)
            .map(Self::from)
    }
}

impl From<JsonValue> for Schema {
    fn from(schema: JsonValue) -> Self {
        debug!(%schema);

        const FIELDS: &str = "fields";

        schema
            .get(FIELDS)
            .inspect(|fields| debug!(?fields))
            .and_then(|fields| fields.as_array())
            .inspect(|fields| debug!(?fields))
            .map_or(
                Self {
                    key: None,
                    value: None,
                },
                |fields| {
                    if AvroSchema::parse(&schema)
                        .inspect_err(|err| error!(?err, ?schema))
                        .is_ok()
                    {
                        Self {
                            key: fields
                                .iter()
                                .find(|field| {
                                    field
                                        .get("name")
                                        .is_some_and(|name| name == MessageKind::Key.as_ref())
                                })
                                .inspect(|value| debug!(?value))
                                .and_then(|schema| {
                                    AvroSchema::parse(schema)
                                        .inspect_err(|err| error!(?err, ?schema))
                                        .ok()
                                }),

                            value: fields
                                .iter()
                                .find(|field| {
                                    field
                                        .get("name")
                                        .is_some_and(|name| name == MessageKind::Value.as_ref())
                                })
                                .inspect(|value| debug!(?value))
                                .and_then(|schema| {
                                    AvroSchema::parse(schema)
                                        .inspect_err(|err| error!(?err, ?schema))
                                        .ok()
                                }),
                        }
                    } else {
                        Self {
                            key: None,
                            value: None,
                        }
                    }
                },
            )
    }
}

fn decode(validator: Option<&AvroSchema>, encoded: Option<Bytes>) -> Result<Option<Value>> {
    debug!(?validator, ?encoded);
    validator.map_or(Ok(None), |schema| {
        encoded.map_or(Err(crate::Error::InvalidRecord), |encoded| {
            Reader::with_schema(schema, &encoded[..])
                .and_then(|reader| reader.into_iter().next().transpose())
                .inspect(|value| debug!(?value))
                .inspect_err(|err| debug!(?err))
                .map_err(|_| crate::Error::InvalidRecord)
                .and_then(|value| value.ok_or(crate::Error::InvalidRecord))
                .map(Some)
        })
    })
}

fn validate(validator: Option<&AvroSchema>, encoded: Option<Bytes>) -> Result<()> {
    decode(validator, encoded).and(Ok(()))
}

impl Validator for Schema {
    fn validate(&self, batch: &Batch) -> Result<()> {
        debug!(?batch);

        for record in &batch.records {
            debug!(?record);

            validate(self.key.as_ref(), record.key.clone())
                .and(validate(self.value.as_ref(), record.value.clone()))
                .inspect_err(|err| info!(?err, ?batch))?
        }

        Ok(())
    }
}

#[doc(hidden)]
pub fn r<'a>(
    schema: &AvroSchema,
    fields: impl IntoIterator<Item = (&'a str, Value)>,
) -> apache_avro::types::Record<'_> {
    apache_avro::types::Record::new(schema)
        .map(|mut record| {
            for (name, value) in fields {
                record.put(name, value);
            }
            record
        })
        .unwrap()
}

#[doc(hidden)]
pub fn schema_write(schema: &AvroSchema, value: Value) -> Result<Bytes> {
    debug!(?schema, ?value);
    let mut writer = apache_avro::Writer::new(schema, vec![]);
    _ = writer.append(value)?;
    writer.into_inner().map(Bytes::from).map_err(Into::into)
}

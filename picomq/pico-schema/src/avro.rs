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

//! AVRO schema

use std::collections::HashMap;

use apache_avro::{schema::Schema as AvroSchema, types::Value, Reader};
use bytes::Bytes;

use crate::record::Batch;
use serde_json::{Map, Value as JsonValue};
use tracing::{debug, error, info};

use crate::{Error, Result, Validator};

#[cfg(feature = "arrow")]
use apache_avro::schema::RecordSchema;

#[cfg(feature = "arrow")]
mod arrow;

#[derive(Copy, Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) enum MessageKind {
    Key,
    Meta,
    Value,
}

impl AsRef<str> for MessageKind {
    fn as_ref(&self) -> &str {
        match self {
            MessageKind::Key => "key",
            MessageKind::Meta => "meta",
            MessageKind::Value => "value",
        }
    }
}

/// AVRO Schema
#[derive(Clone, Debug, Default)]
pub struct Schema {
    #[cfg(feature = "arrow")]
    complete: Option<RecordSchema>,
    pub(crate) key: Option<AvroSchema>,
    pub(crate) value: Option<AvroSchema>,
    pub(crate) meta: Option<AvroSchema>,

    #[cfg(feature = "arrow")]
    ids: HashMap<String, i32>,
}

impl Schema {
    pub fn key(&self) -> Option<&AvroSchema> {
        self.key.as_ref()
    }

    pub fn value(&self) -> Option<&AvroSchema> {
        self.value.as_ref()
    }

    pub fn meta(&self) -> Option<&AvroSchema> {
        self.meta.as_ref()
    }
}

impl TryFrom<Bytes> for Schema {
    type Error = Error;

    fn try_from(encoded: Bytes) -> Result<Self, Self::Error> {
        const FIELDS: &str = "fields";

        let meta =
            serde_json::from_slice::<JsonValue>(&Bytes::from_static(include_bytes!("meta.avsc")))
                .inspect(|meta| debug!(%meta))
                .map(|mut meta| meta[FIELDS].take())
                .inspect(|meta| debug!(%meta))?;

        serde_json::from_slice::<JsonValue>(&encoded[..])
            .map(|mut schema| {
                _ = schema
                    .get_mut(FIELDS)
                    .and_then(|fields| fields.as_object_mut())
                    .and_then(|object| object.insert(MessageKind::Meta.as_ref().to_owned(), meta));
                schema
            })
            .map_err(Into::into)
            .map(Self::from)
    }
}

impl From<JsonValue> for Schema {
    fn from(mut schema: JsonValue) -> Self {
        debug!(%schema);

        const FIELDS: &str = "fields";

        let meta =
            serde_json::from_slice::<JsonValue>(&Bytes::from_static(include_bytes!("meta.avsc")))
                .inspect(|meta| debug!(%meta))
                .ok();

        let schema = {
            if let Some(meta) = meta {
                if let Some(fields) = schema.get_mut(FIELDS) {
                    if let Some(array) = fields.as_array_mut() {
                        array.push(JsonValue::Object(Map::from_iter([
                            ("name".into(), MessageKind::Meta.as_ref().into()),
                            ("type".into(), meta),
                        ])));
                    }
                }
            }

            debug!(%schema);

            schema
        };

        schema
            .get(FIELDS)
            .inspect(|fields| debug!(?fields))
            .and_then(|fields| fields.as_array())
            .inspect(|fields| debug!(?fields))
            .map_or(
                Self {
                    #[cfg(feature = "arrow")]
                    complete: None,
                    key: None,
                    value: None,
                    meta: None,
                    #[cfg(feature = "arrow")]
                    ids: HashMap::new(),
                },
                |fields| {
                    if let Ok(schema) =
                        AvroSchema::parse(&schema).inspect_err(|err| error!(?err, ?schema))
                    {
                        Self {
                            #[cfg(feature = "arrow")]
                            ids: field_ids(&schema),

                            #[cfg(feature = "arrow")]
                            complete: if let AvroSchema::Record(record) = schema {
                                Some(record)
                            } else {
                                None
                            },

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

                            meta: fields
                                .iter()
                                .find(|field| {
                                    field
                                        .get("name")
                                        .is_some_and(|name| name == MessageKind::Meta.as_ref())
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
                            #[cfg(feature = "arrow")]
                            complete: None,
                            key: None,
                            value: None,
                            meta: None,
                            #[cfg(feature = "arrow")]
                            ids: HashMap::new(),
                        }
                    }
                },
            )
    }
}

#[cfg(feature = "arrow")]
fn field_ids(schema: &AvroSchema) -> HashMap<String, i32> {
    use crate::ARROW_LIST_FIELD_NAME;

    fn field_ids_with_path(
        path: &[&str],
        schema: &AvroSchema,
        id: &mut i32,
    ) -> HashMap<String, i32> {
        debug!(?path, ?schema, id);

        let mut ids = HashMap::new();

        match schema {
            AvroSchema::Array(inner) => {
                let mut path = Vec::from(path);
                path.push(ARROW_LIST_FIELD_NAME);
                _ = ids.insert(path.join("."), *id);
                *id += 1;

                ids.extend(field_ids_with_path(&path[..], &inner.items, id));
            }

            AvroSchema::Map(inner) => {
                let mut path = Vec::from(path);
                path.push("entries");
                _ = ids.insert(path.join("."), *id);
                *id += 1;

                {
                    let mut path = path.clone();
                    path.push("keys");
                    _ = ids.insert(path.join("."), *id);
                    *id += 1;
                }

                {
                    let mut path = path.clone();
                    path.push("values");
                    _ = ids.insert(path.join("."), *id);
                    *id += 1;

                    ids.extend(field_ids_with_path(&path[..], &inner.types, id))
                }
            }

            AvroSchema::Record(inner) => {
                for field in inner.fields.iter() {
                    let mut path = Vec::from(path);
                    path.push(field.name.as_str());

                    _ = ids.insert(path.join("."), *id);
                    *id += 1;
                }

                for field in inner.fields.iter() {
                    let mut path = Vec::from(path);
                    path.push(field.name.as_str());
                    ids.extend(field_ids_with_path(&path[..], &field.schema, id))
                }
            }

            _ => (),
        }

        ids
    }

    field_ids_with_path(&[], schema, &mut 1)
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

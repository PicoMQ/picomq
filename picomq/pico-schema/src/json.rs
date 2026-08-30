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

//! JSON schema

use std::collections::BTreeMap;

use crate::{Error, Result, Validator, ARROW_LIST_FIELD_NAME};

use bytes::Bytes;

use serde_json::Value;

use crate::record::Batch;
use tracing::{debug, instrument, warn};

#[cfg(feature = "arrow")]
mod arrow;

#[derive(Debug, Default)]
pub struct Schema {
    key: Option<jsonschema::Validator>,
    value: Option<jsonschema::Validator>,

    #[allow(dead_code)]
    ids: BTreeMap<String, i32>,
}

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

fn validate(validator: Option<&jsonschema::Validator>, encoded: Option<Bytes>) -> Result<()> {
    debug!(validator = ?validator, ?encoded);

    validator
        .map_or(Ok(()), |validator| {
            encoded.map_or(Err(crate::Error::InvalidRecord), |encoded| {
                serde_json::from_reader(&encoded[..])
                    .map_err(|err| {
                        warn!(?err, ?encoded);
                        crate::Error::InvalidRecord
                    })
                    .inspect(|instance| debug!(?instance))
                    .and_then(|instance| {
                        validator
                            .validate(&instance)
                            .inspect_err(|err| warn!(?err, ?validator, %instance))
                            .map_err(|_err| crate::Error::InvalidRecord)
                    })
            })
        })
        .inspect(|r| debug!(?r))
        .inspect_err(|err| warn!(?err))
}

impl TryFrom<Bytes> for Schema {
    type Error = Error;

    fn try_from(encoded: Bytes) -> Result<Self, Self::Error> {
        debug!(encoded = &encoded[..]);
        const PROPERTIES: &str = "properties";

        let mut schema = serde_json::from_slice::<Value>(&encoded[..])?;

        let key = schema
            .get(PROPERTIES)
            .and_then(|properties| properties.get(MessageKind::Key.as_ref()))
            .inspect(|key| debug!(?key))
            .and_then(|key| jsonschema::validator_for(key).ok());

        let value = schema
            .get(PROPERTIES)
            .and_then(|properties| properties.get(MessageKind::Value.as_ref()))
            .inspect(|value| debug!(?value))
            .and_then(|value| jsonschema::validator_for(value).ok());

        let meta =
            serde_json::from_slice::<Value>(&Bytes::from_static(include_bytes!("meta.json")))
                .inspect(|meta| debug!(%meta))?;

        _ = schema
            .get_mut(PROPERTIES)
            .and_then(|properties| properties.as_object_mut())
            .inspect(|properties| debug!(?properties))
            .and_then(|object| object.insert(MessageKind::Meta.as_ref().to_owned(), meta));

        let ids = field_ids(&schema);
        debug!(?ids);

        Ok(Self { key, value, ids })
    }
}

impl Validator for Schema {
    #[instrument(skip(self, batch), ret)]
    fn validate(&self, batch: &Batch) -> Result<()> {
        for record in &batch.records {
            debug!(?record);

            validate(self.key.as_ref(), record.key.clone())
                .and(validate(self.value.as_ref(), record.value.clone()))?
        }

        Ok(())
    }
}

#[instrument(skip(schema), ret)]
fn field_ids(schema: &Value) -> BTreeMap<String, i32> {
    fn field_ids_with_path(path: &[&str], schema: &Value, id: &mut i32) -> BTreeMap<String, i32> {
        debug!(?path, %schema, id);

        let mut ids = BTreeMap::new();

        match schema.get("type").and_then(|r#type| r#type.as_str()) {
            Some("object") => {
                if let Some(properties) = schema
                    .get("properties")
                    .and_then(|properties| properties.as_object())
                {
                    for (k, v) in properties {
                        let mut path = Vec::from(path);
                        path.push(k);

                        _ = ids.insert(path.join("."), *id);
                        *id += 1;

                        ids.extend(field_ids_with_path(&path[..], v, id))
                    }
                }
            }

            Some("array") => {
                let mut path = Vec::from(path);
                path.push(ARROW_LIST_FIELD_NAME);
                _ = ids.insert(path.join("."), *id);
                *id += 1;

                if let Some(items) = schema.get("items") {
                    debug!(?items);

                    ids.extend(field_ids_with_path(&path[..], items, id))
                }
            }

            None | Some(_) => (),
        }

        ids
    }

    let mut ids = BTreeMap::new();
    let mut id = 1;
    let kinds = [MessageKind::Meta, MessageKind::Key, MessageKind::Value];

    for kind in kinds {
        if schema
            .get("properties")
            .and_then(|schema| schema.get(kind.as_ref()))
            .inspect(|schema| debug!(?kind, ?schema))
            .is_some()
        {
            _ = ids.insert(kind.as_ref().into(), id);
            id += 1;
        }
    }

    for kind in kinds {
        if let Some(schema) = schema
            .get("properties")
            .and_then(|schema| schema.get(kind.as_ref()))
            .inspect(|schema| debug!(?kind, ?schema))
        {
            ids.extend(field_ids_with_path(&[kind.as_ref()], schema, &mut id));
        }
    }

    ids
}

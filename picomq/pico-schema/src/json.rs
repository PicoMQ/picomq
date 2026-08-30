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

use crate::record::Batch;
use crate::{Error, Result, Validator};
use bytes::Bytes;
use serde_json::Value;
use tracing::{debug, instrument, warn};

#[derive(Debug, Default)]
pub struct Schema {
    key: Option<jsonschema::Validator>,
    value: Option<jsonschema::Validator>,
}

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

        let schema = serde_json::from_slice::<Value>(&encoded[..])?;

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

        Ok(Self { key, value })
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

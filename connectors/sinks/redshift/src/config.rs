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

use std::str::FromStr;

use arrow::datatypes::DataType;
use picomq_connector_sdk::Error;
use picomq_connector_sdk::destination::DestinationTemplate;
use secrecy::{ExposeSecret, SecretString};

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RedshiftSinkConfig {
    #[serde(serialize_with = "picomq_connector_sdk::secret::serialize_secret")]
    pub connection_string: SecretString,
    pub target_table: DestinationTemplate,
    pub batch_size: Option<u32>,
    pub max_connections: Option<u32>,
    pub include_metadata: Option<bool>,
    pub include_key: Option<bool>,
    pub payload_format: Option<String>,
    pub verbose_logging: Option<bool>,
    pub max_retries: Option<u32>,
    pub retry_delay: Option<String>,
    #[serde(serialize_with = "picomq_connector_sdk::secret::serialize_optional_secret")]
    pub aws_access_key_id: Option<SecretString>,
    #[serde(serialize_with = "picomq_connector_sdk::secret::serialize_optional_secret")]
    pub aws_secret_access_key: Option<SecretString>,
    pub aws_iam_role: String,
    pub s3_bucket: String,
    pub s3_prefix: String,
    pub s3_endpoint: Option<String>,
    pub aws_region: String,
    pub archive: Option<bool>,
}

impl RedshiftSinkConfig {
    pub fn validate(&self) -> Result<(), Error> {
        let mut errors = String::new();

        if self.batch_size.is_some_and(|v| v == 0) {
            errors.push_str("batch_size cannot be 0\n");
        }

        if self.connection_string.expose_secret().is_empty() {
            errors.push_str("connection_string is empty\n");
        }

        if self.target_table.to_string().is_empty() {
            errors.push_str(", target_table is empty\n");
        }

        if self.s3_bucket.is_empty() {
            errors.push_str(", s3_bucket is empty\n");
        }

        if self.aws_region.is_empty() {
            errors.push_str(", aws_region is empty\n");
        }

        if self.aws_iam_role.is_empty() {
            errors.push_str(", aws_iam_role is empty\n");
        }

        match (&self.aws_access_key_id, &self.aws_secret_access_key) {
            (Some(access), Some(secret)) => {
                let has_access_key = !access.expose_secret().is_empty();

                let has_secret_key = !secret.expose_secret().is_empty();

                if !(has_access_key && has_secret_key) {
                    errors.push_str(", Choosing to use aws_access_key_id and aws_secret_access_key then both MUST be provided\n");
                }
            }
            (None, None) => {}
            _ => {
                errors.push_str(
                    ", Choosing to use aws_access_key_id and aws_secret_access_key then both MUST be provided\n",
                );
            }
        }

        if !errors.is_empty() {
            Err(Error::InvalidConfigValue(errors))
        } else {
            Ok(())
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PayloadFormat {
    Text,
    #[default]
    Varbyte,
}

impl FromStr for PayloadFormat {
    type Err = Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.to_lowercase().as_str() {
            "text" => Ok(PayloadFormat::Text),
            "json" => {
                tracing::warn!("Json is not supported, falling back to Text");
                Ok(PayloadFormat::Text)
            }
            "varbyte" => Ok(PayloadFormat::Varbyte),
            other => Err(Error::InvalidConfigValue(format!(
                "Unrecognized payload_format '{other}', expected text or varbyte"
            ))),
        }
    }
}

impl PayloadFormat {
    pub fn from_config(value: Option<&str>) -> Self {
        match value {
            None => PayloadFormat::Varbyte,
            Some(value) => value.parse().unwrap_or_else(|error| {
                tracing::warn!("{error}, falling back to VARBYTE");
                PayloadFormat::Varbyte
            }),
        }
    }

    pub fn sql_type(&self) -> &'static str {
        match self {
            PayloadFormat::Varbyte => "VARBYTE(16777216)",
            PayloadFormat::Text => "VARCHAR(MAX)",
        }
    }

    pub fn arrow_type(&self) -> DataType {
        match self {
            PayloadFormat::Varbyte => DataType::Binary,
            PayloadFormat::Text => DataType::Utf8,
        }
    }
}

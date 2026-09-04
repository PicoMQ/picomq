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

use serde::{Deserialize, Serialize};
use strum::{AsRefStr, Display as StrumDisplay, EnumString};

#[derive(
    Debug,
    Serialize,
    Deserialize,
    PartialEq,
    Clone,
    Copy,
    AsRefStr,
    StrumDisplay,
    EnumString,
    Default,
)]
#[serde(rename_all = "lowercase")]
#[strum(serialize_all = "lowercase")]
#[repr(u8)]
pub enum ConnectorStatus {
    Starting,
    Running,
    Stopping,
    #[default]
    Stopped,
    Error,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectorError {
    pub message: String,
    pub timestamp: u64,
}

impl ConnectorError {
    pub fn new(message: &str) -> Self {
        Self {
            message: message.to_string(),
            timestamp: crate::now_millis(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ConnectorRuntimeStats {
    pub connectors_runtime_version: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub connectors_runtime_version_semver: Option<u32>,
    pub process_id: u32,
    pub cpu_usage: f32,
    pub total_cpu_usage: f32,
    pub memory_usage: u64,
    pub total_memory: u64,
    pub available_memory: u64,
    pub run_time: u64,
    pub start_time: u64,
    pub sources_total: u32,
    pub sources_running: u32,
    pub sinks_total: u32,
    pub sinks_running: u32,
    pub connectors: Vec<ConnectorStats>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ConnectorStats {
    pub key: String,
    pub name: String,
    pub connector_type: String,
    pub version: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version_semver: Option<u32>,
    pub status: ConnectorStatus,
    pub enabled: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub messages_produced: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub messages_sent: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub messages_consumed: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub messages_processed: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub messages_filtered: Option<u64>,
    #[serde(default)]
    pub errors: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SinkInfoResponse {
    pub id: u32,
    pub key: String,
    pub name: String,
    pub path: String,
    pub enabled: bool,
    pub status: ConnectorStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error: Option<ConnectorError>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub plugin_config_format: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SourceInfoResponse {
    pub id: u32,
    pub key: String,
    pub name: String,
    pub path: String,
    pub enabled: bool,
    pub status: ConnectorStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error: Option<ConnectorError>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub plugin_config_format: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct HealthResponse {
    pub status: String,
}

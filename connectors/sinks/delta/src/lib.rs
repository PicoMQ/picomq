use std::sync::Arc;

use deltalake::DeltaTable;
use deltalake::writer::JsonWriter;
use picomq_connector_sdk::destination::DestinationTemplate;
use picomq_connector_sdk::sink_connector;
use secrecy::SecretString;
use serde::Deserialize;
use tokio::sync::Mutex;

use crate::coercions::CoercionTree;

mod coercions;
mod sink;
mod storage;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum StorageBackendType {
    S3,
    Azure,
    Gcs,
}

sink_connector!(DeltaSink);

#[derive(Debug)]
pub struct DeltaSink {
    id: u32,
    config: DeltaSinkConfig,
    tables: dashmap::DashMap<String, Arc<Mutex<SinkState>>>,
}

#[derive(Debug)]
struct SinkState {
    table: DeltaTable,
    writer: JsonWriter,
    coercion_tree: CoercionTree,
}

#[derive(Debug, Deserialize)]
pub struct DeltaSinkConfig {
    pub table_uri: DestinationTemplate,
    #[serde(default)]
    pub storage_backend_type: Option<StorageBackendType>,

    #[serde(default)]
    pub aws_s3_access_key: Option<SecretString>,
    #[serde(default)]
    pub aws_s3_secret_key: Option<SecretString>,
    #[serde(default)]
    pub aws_s3_region: Option<String>,
    #[serde(default)]
    pub aws_s3_endpoint_url: Option<String>,
    #[serde(default)]
    pub aws_s3_allow_http: Option<bool>,

    #[serde(default)]
    pub azure_storage_account_name: Option<String>,
    #[serde(default)]
    pub azure_storage_account_key: Option<SecretString>,
    #[serde(default)]
    pub azure_storage_sas_token: Option<SecretString>,
    #[serde(default)]
    pub azure_container_name: Option<String>,

    #[serde(default)]
    pub gcs_service_account_key: Option<SecretString>,
}

impl DeltaSink {
    pub fn new(id: u32, config: DeltaSinkConfig) -> Self {
        DeltaSink {
            id,
            config,
            tables: dashmap::DashMap::new(),
        }
    }
}

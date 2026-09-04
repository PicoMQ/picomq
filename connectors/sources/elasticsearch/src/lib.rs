use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use elasticsearch::{
    Elasticsearch, SearchParts,
    auth::Credentials,
    http::{
        Url,
        transport::{SingleNodeConnectionPool, TransportBuilder},
    },
    indices::IndicesExistsParts,
};
use picomq_connector_sdk::{
    ConnectorState, Error, ProducedMessage, ProducedMessages, Schema, Source,
    source::SourceBatchResult, source_connector,
};
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::{sync::Mutex, time::sleep};
use tracing::{info, warn};

mod state_manager;
use crate::state_manager::{FileStateStorage, SourceState, StateStorage};
pub use state_manager::{StateInfo, StateManager, StateStats};

source_connector!(ElasticsearchSource);

const CONNECTOR_NAME: &str = "Elasticsearch source";
const DEFAULT_POLLING_INTERVAL: &str = "10s";
const DEFAULT_BATCH_SIZE: usize = 100;
const DEFAULT_TIMESTAMP_FIELD: &str = "@timestamp";
const DEFAULT_STATE_BASE_PATH: &str = "./connector_states";

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct State {
    last_poll_timestamp: Option<DateTime<Utc>>,
    total_documents_fetched: usize,
    poll_count: usize,
    last_document_id: Option<String>,
    last_scroll_id: Option<String>,
    last_offset: Option<u64>,
    error_count: usize,
    last_error: Option<String>,
    processing_stats: ProcessingStats,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct ProcessingStats {
    total_bytes_processed: u64,
    avg_batch_processing_time_ms: f64,
    last_successful_poll: Option<DateTime<Utc>>,
    empty_polls_count: usize,
    successful_polls_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateConfig {
    pub enabled: bool,
    pub storage_type: Option<String>,
    pub storage_config: Option<Value>,
    pub state_id: Option<String>,
    pub auto_save_interval: Option<String>,
    pub tracked_fields: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ElasticsearchSourceConfig {
    pub url: String,
    pub index: String,
    pub username: Option<String>,
    #[serde(serialize_with = "picomq_connector_sdk::secret::serialize_optional_secret")]
    pub password: Option<SecretString>,
    pub query: Option<Value>,
    pub polling_interval: Option<String>,
    pub batch_size: Option<usize>,
    pub timestamp_field: Option<String>,
    pub scroll_timeout: Option<String>,
    pub state: Option<StateConfig>,
}

#[derive(Debug)]
pub struct ElasticsearchSource {
    id: u32,
    config: ElasticsearchSourceConfig,
    client: Option<Elasticsearch>,
    polling_interval: Duration,
    state: Mutex<State>,
    pending: Mutex<Option<State>>,
}

struct SearchBatch {
    messages: Vec<ProducedMessage>,
    latest_timestamp: Option<DateTime<Utc>>,
    last_document_id: Option<String>,
    bytes: u64,
}

impl ElasticsearchSource {
    pub fn new(id: u32, config: ElasticsearchSourceConfig, state: Option<ConnectorState>) -> Self {
        let polling_interval = config
            .polling_interval
            .as_deref()
            .unwrap_or(DEFAULT_POLLING_INTERVAL)
            .parse::<humantime::Duration>()
            .unwrap_or_else(|_| {
                humantime::Duration::from_str(DEFAULT_POLLING_INTERVAL)
                    .expect("default polling interval must be valid")
            })
            .into();

        let restored_state = state
            .and_then(|state| state.deserialize::<State>(CONNECTOR_NAME, id))
            .inspect(|state| {
                info!(
                    "Restored state for {CONNECTOR_NAME} connector with ID: {id}. \
                     Documents fetched: {}, poll count: {}",
                    state.total_documents_fetched, state.poll_count
                );
            });

        ElasticsearchSource {
            id,
            config,
            client: None,
            polling_interval,
            state: Mutex::new(restored_state.unwrap_or_default()),
            pending: Mutex::new(None),
        }
    }

    fn serialize_state(&self, state: &State) -> Option<ConnectorState> {
        ConnectorState::serialize(state, CONNECTOR_NAME, self.id)
    }

    fn state_persistence_enabled(&self) -> bool {
        self.config
            .state
            .as_ref()
            .is_some_and(|state| state.enabled)
    }

    fn create_state_storage(&self) -> Option<Arc<dyn StateStorage>> {
        let state_config = self.config.state.as_ref()?;
        if !state_config.enabled {
            return None;
        }

        match state_config.storage_type.as_deref() {
            Some("file") | None => {
                let base_path = state_config
                    .storage_config
                    .as_ref()
                    .and_then(|config| config.get("base_path"))
                    .and_then(|path| path.as_str())
                    .unwrap_or(DEFAULT_STATE_BASE_PATH);

                Some(Arc::new(FileStateStorage::new(base_path)))
            }
            Some("elasticsearch") => {
                warn!(
                    "Elasticsearch state storage not yet implemented, falling back to file storage"
                );
                Some(Arc::new(FileStateStorage::new(DEFAULT_STATE_BASE_PATH)))
            }
            Some(storage_type) => {
                warn!(
                    "Unknown state storage type: {}, falling back to file storage",
                    storage_type
                );
                Some(Arc::new(FileStateStorage::new(DEFAULT_STATE_BASE_PATH)))
            }
        }
    }

    fn get_state_id(&self) -> String {
        self.config
            .state
            .as_ref()
            .and_then(|state| state.state_id.clone())
            .unwrap_or_else(|| format!("elasticsearch_source_{}", self.id))
    }

    async fn internal_state_to_source_state(&self) -> Result<SourceState, Error> {
        let state = self.state.lock().await;

        let data = json!({
            "last_poll_timestamp": state.last_poll_timestamp,
            "total_documents_fetched": state.total_documents_fetched,
            "poll_count": state.poll_count,
            "last_document_id": state.last_document_id,
            "last_scroll_id": state.last_scroll_id,
            "last_offset": state.last_offset,
            "error_count": state.error_count,
            "last_error": state.last_error,
            "processing_stats": state.processing_stats,
        });

        Ok(SourceState {
            id: self.get_state_id(),
            last_updated: Utc::now(),
            version: 1,
            data,
            metadata: Some(json!({
                "connector_type": "elasticsearch_source",
                "connector_id": self.id,
                "index": self.config.index,
                "url": self.config.url,
            })),
        })
    }

    async fn source_state_to_internal_state(
        &mut self,
        source_state: SourceState,
    ) -> Result<(), Error> {
        let mut state = self.state.lock().await;

        if let Some(data) = source_state.data.as_object() {
            if let Some(timestamp) = data.get("last_poll_timestamp")
                && let Some(timestamp_str) = timestamp.as_str()
                && let Ok(timestamp) = DateTime::parse_from_rfc3339(timestamp_str)
            {
                state.last_poll_timestamp = Some(timestamp.with_timezone(&Utc));
            }

            if let Some(count) = data.get("total_documents_fetched")
                && let Some(count_val) = count.as_u64()
            {
                state.total_documents_fetched = count_val as usize;
            }

            if let Some(count) = data.get("poll_count")
                && let Some(count_val) = count.as_u64()
            {
                state.poll_count = count_val as usize;
            }

            if let Some(document_id) = data.get("last_document_id") {
                state.last_document_id = document_id.as_str().map(|id| id.to_string());
            }

            if let Some(scroll_id) = data.get("last_scroll_id") {
                state.last_scroll_id = scroll_id.as_str().map(|id| id.to_string());
            }

            if let Some(offset) = data.get("last_offset") {
                state.last_offset = offset.as_u64();
            }

            if let Some(error_count) = data.get("error_count")
                && let Some(count_val) = error_count.as_u64()
            {
                state.error_count = count_val as usize;
            }

            if let Some(last_error) = data.get("last_error") {
                state.last_error = last_error.as_str().map(|error| error.to_string());
            }

            if let Some(stats) = data.get("processing_stats")
                && let Ok(processing_stats) = serde_json::from_value(stats.clone())
            {
                state.processing_stats = processing_stats;
            }
        }

        Ok(())
    }

    async fn create_client(&self) -> Result<Elasticsearch, Error> {
        let url = Url::parse(&self.config.url)
            .map_err(|error| Error::Storage(format!("Invalid Elasticsearch URL: {error}")))?;

        let connection_pool = SingleNodeConnectionPool::new(url);
        let mut transport_builder = TransportBuilder::new(connection_pool);

        if let (Some(username), Some(password)) = (&self.config.username, &self.config.password) {
            let credentials =
                Credentials::Basic(username.clone(), password.expose_secret().to_string());
            transport_builder = transport_builder.auth(credentials);
        }

        let transport = transport_builder
            .build()
            .map_err(|error| Error::Storage(format!("Failed to build transport: {error}")))?;

        Ok(Elasticsearch::new(transport))
    }

    fn build_search_body(&self, last_poll_timestamp: Option<DateTime<Utc>>) -> Value {
        let batch_size = self.config.batch_size.unwrap_or(DEFAULT_BATCH_SIZE);
        let mut query = self
            .config
            .query
            .clone()
            .unwrap_or_else(|| json!({ "match_all": {} }));

        if let Some(timestamp_field) = &self.config.timestamp_field
            && let Some(last_timestamp) = last_poll_timestamp
        {
            query = json!({
                "bool": {
                    "must": [
                        query,
                        {
                            "range": {
                                timestamp_field: {
                                    "gt": last_timestamp.to_rfc3339()
                                }
                            }
                        }
                    ]
                }
            });
        }

        json!({
            "query": query,
            "size": batch_size,
            "sort": [
                {
                    self.config.timestamp_field.as_deref().unwrap_or(DEFAULT_TIMESTAMP_FIELD): {
                        "order": "asc"
                    }
                }
            ]
        })
    }

    async fn search_documents(
        &self,
        client: &Elasticsearch,
        last_poll_timestamp: Option<DateTime<Utc>>,
    ) -> Result<SearchBatch, Error> {
        let search_body = self.build_search_body(last_poll_timestamp);

        let response = client
            .search(SearchParts::Index(&[&self.config.index]))
            .body(search_body)
            .send()
            .await
            .map_err(|error| Error::Storage(format!("Failed to execute search: {error}")))?;

        if !response.status_code().is_success() {
            let error_text = response
                .text()
                .await
                .unwrap_or_else(|_| "Unknown error".to_string());
            return Err(Error::Storage(format!(
                "Search request failed: {error_text}"
            )));
        }

        let response_body: Value = response
            .json()
            .await
            .map_err(|error| Error::Storage(format!("Failed to parse search response: {error}")))?;

        let mut batch = SearchBatch {
            messages: Vec::new(),
            latest_timestamp: None,
            last_document_id: None,
            bytes: 0,
        };

        let Some(hits) = response_body
            .get("hits")
            .and_then(|hits| hits.get("hits"))
            .and_then(|hits| hits.as_array())
        else {
            return Ok(batch);
        };

        batch.messages.reserve(hits.len());
        for hit in hits {
            let Some(source) = hit.get("_source") else {
                continue;
            };

            if let Some(timestamp_field) = &self.config.timestamp_field
                && let Some(timestamp_str) = source.get(timestamp_field).and_then(|v| v.as_str())
                && let Ok(timestamp) = DateTime::parse_from_rfc3339(timestamp_str)
            {
                let timestamp_utc = timestamp.with_timezone(&Utc);
                if batch
                    .latest_timestamp
                    .is_none_or(|latest| timestamp_utc > latest)
                {
                    batch.latest_timestamp = Some(timestamp_utc);
                }
            }

            if let Some(document_id) = hit.get("_id").and_then(|id| id.as_str()) {
                batch.last_document_id = Some(document_id.to_string());
            }

            let payload = serde_json::to_vec(source).map_err(|error| {
                Error::Serialization(format!("Failed to serialize document: {error}"))
            })?;
            batch.bytes += payload.len() as u64;
            batch.messages.push(ProducedMessage {
                key: None,
                timestamp: None,
                headers: None,
                payload,
            });
        }

        Ok(batch)
    }
}

fn record_successful_poll(state: &mut State, processing_time_ms: f64, empty: bool) {
    state.poll_count += 1;
    state.processing_stats.successful_polls_count += 1;
    state.processing_stats.last_successful_poll = Some(Utc::now());

    let total_polls =
        state.processing_stats.successful_polls_count + state.processing_stats.empty_polls_count;
    state.processing_stats.avg_batch_processing_time_ms =
        (state.processing_stats.avg_batch_processing_time_ms * (total_polls - 1) as f64
            + processing_time_ms)
            / total_polls as f64;

    if empty {
        state.processing_stats.empty_polls_count += 1;
    }
}

#[async_trait]
impl Source for ElasticsearchSource {
    async fn open(&mut self) -> Result<(), Error> {
        info!(
            "Opening Elasticsearch source connector with ID: {} for URL: {}, index: {}",
            self.id, self.config.url, self.config.index
        );

        let client = self.create_client().await?;

        let response = client
            .indices()
            .exists(IndicesExistsParts::Index(&[&self.config.index]))
            .send()
            .await
            .map_err(|error| Error::Storage(format!("Failed to check index existence: {error}")))?;

        if !response.status_code().is_success() {
            return Err(Error::Storage(format!(
                "Index '{}' does not exist or is not accessible",
                self.config.index
            )));
        }

        self.client = Some(client);

        if self.state_persistence_enabled()
            && let Err(error) = self.load_state().await
        {
            warn!(
                "Failed to load state for Elasticsearch source connector with ID: {}: {}",
                self.id, error
            );
        }

        info!(
            "Successfully opened Elasticsearch source connector with ID: {}",
            self.id
        );
        Ok(())
    }

    async fn poll(&self) -> Result<ProducedMessages, Error> {
        let start_time = Instant::now();

        sleep(self.polling_interval).await;

        let client = self
            .client
            .as_ref()
            .ok_or_else(|| Error::Storage("Elasticsearch client not initialized".to_string()))?;

        let snapshot = self.state.lock().await.clone();
        let batch = match self
            .search_documents(client, snapshot.last_poll_timestamp)
            .await
        {
            Ok(batch) => batch,
            Err(error) => {
                let mut state = self.state.lock().await;
                state.error_count += 1;
                state.last_error = Some(error.to_string());
                return Err(error);
            }
        };

        let processing_time_ms = start_time.elapsed().as_millis() as f64;
        if batch.messages.is_empty() {
            let mut state = self.state.lock().await;
            record_successful_poll(&mut state, processing_time_ms, true);
            return Ok(ProducedMessages {
                schema: Schema::Json,
                messages: Vec::new(),
                state: None,
            });
        }

        let mut candidate = snapshot;
        record_successful_poll(&mut candidate, processing_time_ms, false);
        candidate.total_documents_fetched += batch.messages.len();
        candidate.processing_stats.total_bytes_processed += batch.bytes;
        if let Some(timestamp) = batch.latest_timestamp {
            candidate.last_poll_timestamp = Some(timestamp);
        }
        if let Some(document_id) = batch.last_document_id {
            candidate.last_document_id = Some(document_id);
        }

        let persisted_state = self.serialize_state(&candidate).ok_or_else(|| {
            Error::Serialization("Failed to serialize Elasticsearch source state".to_string())
        })?;
        *self.pending.lock().await = Some(candidate);

        Ok(ProducedMessages {
            schema: Schema::Json,
            messages: batch.messages,
            state: Some(persisted_state),
        })
    }

    async fn on_batch_result(&self, result: SourceBatchResult) -> Result<(), Error> {
        let candidate = self.pending.lock().await.take();
        if result == SourceBatchResult::Ack
            && let Some(candidate) = candidate
        {
            *self.state.lock().await = candidate;
        }
        Ok(())
    }

    async fn close(&mut self) -> Result<(), Error> {
        let state = self.state.lock().await;
        info!(
            "Elasticsearch source connector with ID: {} is closing. Stats: {} total documents fetched, {} polls executed, {} errors",
            self.id, state.total_documents_fetched, state.poll_count, state.error_count
        );
        drop(state);

        if self.state_persistence_enabled()
            && let Err(error) = self.save_state().await
        {
            warn!(
                "Failed to save final state for Elasticsearch source connector with ID: {}: {}",
                self.id, error
            );
        }

        self.client = None;
        info!(
            "Elasticsearch source connector with ID: {} is closed.",
            self.id
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    const INDEX: &str = "logs";
    const FIRST_TIMESTAMP: &str = "2024-01-01T00:00:00Z";
    const SECOND_TIMESTAMP: &str = "2024-01-01T00:00:05Z";

    fn test_config(url: &str) -> ElasticsearchSourceConfig {
        ElasticsearchSourceConfig {
            url: url.to_string(),
            index: INDEX.to_string(),
            username: None,
            password: None,
            query: None,
            polling_interval: Some("1ms".to_string()),
            batch_size: Some(10),
            timestamp_field: Some("@timestamp".to_string()),
            scroll_timeout: None,
            state: None,
        }
    }

    async fn connected_source(server: &MockServer) -> ElasticsearchSource {
        let mut source = ElasticsearchSource::new(1, test_config(&server.uri()), None);
        let client = source
            .create_client()
            .await
            .expect("client should be created");
        source.client = Some(client);
        source
    }

    fn hits_response() -> Value {
        json!({
            "hits": {
                "hits": [
                    {
                        "_id": "doc-1",
                        "_source": { "@timestamp": FIRST_TIMESTAMP, "message": "first" }
                    },
                    {
                        "_id": "doc-2",
                        "_source": { "@timestamp": SECOND_TIMESTAMP, "message": "second" }
                    }
                ]
            }
        })
    }

    fn empty_response() -> Value {
        json!({ "hits": { "hits": [] } })
    }

    fn search_mock(body: Value) -> Mock {
        Mock::given(method("POST"))
            .and(path(format!("/{INDEX}/_search")))
            .respond_with(ResponseTemplate::new(200).set_body_json(body))
    }

    fn expected_timestamp() -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(SECOND_TIMESTAMP)
            .expect("timestamp should parse")
            .with_timezone(&Utc)
    }

    #[test]
    fn given_persisted_state_should_restore_cursor() {
        let state = State {
            last_poll_timestamp: Some(expected_timestamp()),
            total_documents_fetched: 42,
            poll_count: 7,
            last_document_id: Some("doc-42".to_string()),
            ..State::default()
        };
        let serialized = rmp_serde::to_vec(&state).expect("state should serialize");
        let source = ElasticsearchSource::new(
            1,
            test_config("http://localhost:9200"),
            Some(ConnectorState(serialized)),
        );

        let runtime = tokio::runtime::Runtime::new().expect("failed to create test runtime");
        runtime.block_on(async {
            let restored = source.state.lock().await;
            assert_eq!(restored.last_poll_timestamp, Some(expected_timestamp()));
            assert_eq!(restored.total_documents_fetched, 42);
            assert_eq!(restored.poll_count, 7);
            assert_eq!(restored.last_document_id.as_deref(), Some("doc-42"));
        });
    }

    #[test]
    fn given_invalid_state_should_start_fresh() {
        let source = ElasticsearchSource::new(
            1,
            test_config("http://localhost:9200"),
            Some(ConnectorState(b"not valid msgpack".to_vec())),
        );

        let runtime = tokio::runtime::Runtime::new().expect("failed to create test runtime");
        runtime.block_on(async {
            let state = source.state.lock().await;
            assert!(state.last_poll_timestamp.is_none());
            assert_eq!(state.total_documents_fetched, 0);
        });
    }

    #[test]
    fn given_last_poll_timestamp_when_building_query_should_add_range_filter() {
        let source = ElasticsearchSource::new(1, test_config("http://localhost:9200"), None);
        let body = source.build_search_body(Some(expected_timestamp()));
        let range = &body["query"]["bool"]["must"][1]["range"]["@timestamp"]["gt"];
        assert_eq!(range, &json!(expected_timestamp().to_rfc3339()));
        assert_eq!(body["size"], json!(10));

        let body = source.build_search_body(None);
        assert_eq!(body["query"], json!({ "match_all": {} }));
    }

    #[test]
    fn given_nack_when_batch_is_staged_should_discard_cursor_and_reread_same_data() {
        let runtime = tokio::runtime::Runtime::new().expect("failed to create test runtime");
        runtime.block_on(async {
            let server = MockServer::start().await;
            search_mock(hits_response()).mount(&server).await;
            let source = connected_source(&server).await;

            let produced = source.poll().await.expect("poll should succeed");
            assert_eq!(produced.messages.len(), 2);
            assert!(produced.state.is_some());
            assert!(source.pending.lock().await.is_some());

            source
                .on_batch_result(SourceBatchResult::Nack)
                .await
                .expect("NACK should be applied");

            {
                let state = source.state.lock().await;
                assert!(state.last_poll_timestamp.is_none());
                assert_eq!(state.total_documents_fetched, 0);
                assert!(state.last_document_id.is_none());
            }
            assert!(source.pending.lock().await.is_none());

            let redelivered = source.poll().await.expect("poll should succeed");
            assert_eq!(redelivered.messages.len(), 2);
            assert_eq!(
                redelivered.messages[0].payload,
                produced.messages[0].payload
            );
            assert_eq!(
                redelivered.messages[1].payload,
                produced.messages[1].payload
            );
        });
    }

    #[test]
    fn given_ack_when_batch_is_staged_should_apply_cursor() {
        let runtime = tokio::runtime::Runtime::new().expect("failed to create test runtime");
        runtime.block_on(async {
            let server = MockServer::start().await;
            search_mock(hits_response()).mount(&server).await;
            let source = connected_source(&server).await;

            let produced = source.poll().await.expect("poll should succeed");
            assert_eq!(produced.messages.len(), 2);

            source
                .on_batch_result(SourceBatchResult::Ack)
                .await
                .expect("ACK should be applied");

            let state = source.state.lock().await;
            assert_eq!(state.last_poll_timestamp, Some(expected_timestamp()));
            assert_eq!(state.total_documents_fetched, 2);
            assert_eq!(state.poll_count, 1);
            assert_eq!(state.last_document_id.as_deref(), Some("doc-2"));
            assert!(source.pending.lock().await.is_none());

            let persisted: State = rmp_serde::from_slice(&produced.state.expect("state").0)
                .expect("persisted state should deserialize");
            assert_eq!(persisted.last_poll_timestamp, Some(expected_timestamp()));
            assert_eq!(persisted.total_documents_fetched, 2);
        });
    }

    #[test]
    fn given_empty_poll_after_nack_should_carry_no_advanced_state() {
        let runtime = tokio::runtime::Runtime::new().expect("failed to create test runtime");
        runtime.block_on(async {
            let server = MockServer::start().await;
            search_mock(hits_response())
                .up_to_n_times(1)
                .mount(&server)
                .await;
            search_mock(empty_response()).mount(&server).await;
            let source = connected_source(&server).await;

            let produced = source.poll().await.expect("poll should succeed");
            assert_eq!(produced.messages.len(), 2);
            source
                .on_batch_result(SourceBatchResult::Nack)
                .await
                .expect("NACK should be applied");

            let empty = source.poll().await.expect("poll should succeed");
            assert!(empty.messages.is_empty());
            assert!(empty.state.is_none());
            assert!(source.pending.lock().await.is_none());

            let state = source.state.lock().await;
            assert!(state.last_poll_timestamp.is_none());
            assert_eq!(state.total_documents_fetched, 0);
            assert!(state.last_document_id.is_none());
        });
    }
}

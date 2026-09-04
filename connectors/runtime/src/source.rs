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

use dashmap::DashMap;
use dlopen2::wrapper::Container;
use flume::{Receiver, Sender};
use futures::future::join_all;
use picomq_connector_sdk::api::ConnectorStatus;
use picomq_connector_sdk::encoders::avro::{AvroEncoderConfig, AvroStreamEncoder};
use picomq_connector_sdk::{
    ConnectorState, DecodedMessage, Error as SdkError, Headers, ProducedMessages, Schema,
    StreamEncoder, TopicMetadata,
    source::{BatchResultCallback, HandleCallback, POLL_TASK_ENDED_BATCH_ID, SourceBatchResult},
    transforms::Transform,
};
use prometheus_client::metrics::counter::Counter;
use rdkafka::error::KafkaError;
use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use std::{
    collections::{HashMap, HashSet},
    future::Future,
    sync::{Arc, LazyLock, atomic::Ordering},
    time::{Duration, Instant},
};
use tokio::task::JoinHandle;
use tracing::{debug, error, info, trace, warn};

use crate::benchmark;
use crate::configs::connectors::{SourceConfig, TopicProducerConfig};
use crate::context::RuntimeContext;
use crate::kafka::KafkaClients;
use crate::log::LOG_CALLBACK;
use crate::metrics::SourceLabels;
use crate::router::TopicRouter;
use crate::{
    FailedPlugin, PLUGIN_ID, RuntimeError, SourceApi, SourceConnector, SourceConnectorPlugin,
    SourceConnectorProducer, SourceConnectorWrapper, resolve_plugin_path,
    state::{StateStorage, StateStorageFactory},
    transform,
};

const MAX_FAILED_TAIL_RETRIES: u32 = 3;
const DEFAULT_BATCH_LENGTH: u32 = 1000;
const DEFAULT_LINGER_TIME: Duration = Duration::from_millis(5);

pub(crate) struct SourceSenderEntry {
    pub(crate) sender: Sender<ProducedBatch>,
    pub(crate) error_counter: Counter,
}

#[derive(Debug)]
pub(crate) struct ProducedBatch {
    id: u64,
    messages: ProducedMessages,
}

pub(crate) static SOURCE_SENDERS: LazyLock<DashMap<u32, SourceSenderEntry>> =
    LazyLock::new(DashMap::new);

#[cfg(debug_assertions)]
mod fault_injection {
    use std::sync::atomic::{AtomicU64, Ordering};
    use tracing::warn;

    pub const CRASH_AFTER_SEND_ENV: &str = "PICOMQ_CONNECTORS_FAULT_CRASH_AFTER_SEND";
    static SENT_BATCHES: AtomicU64 = AtomicU64::new(0);

    pub fn crash_after_send_if_requested() {
        let Some(target) = std::env::var(CRASH_AFTER_SEND_ENV)
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
        else {
            return;
        };
        let sent = SENT_BATCHES.fetch_add(1, Ordering::SeqCst) + 1;
        if sent == target {
            warn!("Fault injection: aborting after {sent} acked batches before checkpoint save");
            std::process::abort();
        }
    }
}

#[cfg(not(debug_assertions))]
mod fault_injection {
    pub fn crash_after_send_if_requested() {}
}

pub(crate) fn cleanup_sender(plugin_id: u32) {
    SOURCE_SENDERS.remove(&plugin_id);
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct OutgoingMessage {
    pub topic: String,
    pub key: Option<Vec<u8>>,
    pub timestamp: Option<u64>,
    pub headers: Option<Headers>,
    pub payload: Vec<u8>,
}

pub(crate) struct SendFailure {
    pub error: KafkaError,
    pub failed: Vec<OutgoingMessage>,
    pub committed: usize,
}

pub async fn init(
    source_configs: HashMap<String, SourceConfig>,
    kafka: &Arc<KafkaClients>,
    state_factory: &Arc<dyn StateStorageFactory>,
) -> Result<(HashMap<String, SourceConnector>, Vec<FailedPlugin>), RuntimeError> {
    let mut source_connectors: HashMap<String, SourceConnector> = HashMap::new();
    let mut failed_plugins: Vec<FailedPlugin> = Vec::new();

    for (key, config) in source_configs {
        let name = config.name.clone();
        if !config.enabled {
            warn!("Source: {name} is disabled ({key})");
            continue;
        }

        let plugin_id = PLUGIN_ID.fetch_add(1, Ordering::SeqCst);

        let path = match resolve_plugin_path(&config.path) {
            Ok(path) => path,
            Err(error) => {
                let message = format!("Failed to resolve plugin path: {error}");
                error!("Source: {name} ({key}) - {message}");
                failed_plugins.push(FailedPlugin::new(
                    plugin_id,
                    &key,
                    &name,
                    &config.path,
                    config.plugin_config_format,
                    config.enabled,
                    message,
                ));
                continue;
            }
        };

        info!(
            "Initializing source container with name: {name} ({key}), config version: {}, plugin: {path}",
            &config.version
        );

        let state_storage = state_factory.storage_for(&key)?;
        let state = match state_storage.load().await {
            Ok(state) => state,
            Err(
                load_error @ (SdkError::TransientState(_)
                | SdkError::PermanentState(_)
                | SdkError::StateLatched),
            ) => {
                error!("Source: {name} ({key}) - failed to load state: {load_error}");
                return Err(RuntimeError::StateLoadFailed {
                    connector_key: key,
                    source: load_error,
                });
            }
            Err(error) => {
                let message = format!("Failed to load source state: {error}");
                error!("Source: {name} ({key}) - {message}");
                failed_plugins.push(FailedPlugin::new(
                    plugin_id,
                    &key,
                    &name,
                    &config.path,
                    config.plugin_config_format,
                    config.enabled,
                    message,
                ));
                continue;
            }
        };

        if !source_connectors.contains_key(&path) {
            let container = match unsafe { Container::<SourceApi>::load(&path) } {
                Ok(container) => container,
                Err(error) => {
                    let message = format!("Failed to load source container from {path}: {error}");
                    error!("Source: {name} ({key}) - {message}");
                    failed_plugins.push(FailedPlugin::new(
                        plugin_id,
                        &key,
                        &name,
                        &config.path,
                        config.plugin_config_format,
                        config.enabled,
                        message,
                    ));
                    continue;
                }
            };
            info!("Source container for plugin: {path} loaded successfully.");
            source_connectors.insert(
                path.clone(),
                SourceConnector {
                    container,
                    plugins: Vec::new(),
                },
            );
        } else {
            info!("Source container for plugin: {path} is already loaded.");
        }

        let connector = source_connectors
            .get_mut(&path)
            .expect("source container was just ensured for this path");
        let version = get_plugin_version(&connector.container);
        let init_error = init_source(
            &connector.container,
            &config.plugin_config.clone().unwrap_or_default(),
            plugin_id,
            state,
        )
        .err()
        .map(|error| error.to_string());

        connector.plugins.push(SourceConnectorPlugin {
            id: plugin_id,
            key: key.clone(),
            name: name.clone(),
            path: path.clone(),
            version,
            config_format: config.plugin_config_format,
            producer: None,
            transforms: vec![],
            state_storage,
            error: init_error.clone(),
            verbose: config.verbose,
            benchmark: config.benchmark,
        });

        if let Some(error) = init_error {
            error!("Source container with name: {name} ({key}) failed to initialize: {error}");
            continue;
        }

        match setup_source_producer(&key, &config, kafka).await {
            Ok((producer, transforms)) => {
                let connector = source_connectors
                    .get_mut(&path)
                    .expect("source connector was inserted above");
                let plugin = connector
                    .plugins
                    .iter_mut()
                    .find(|plugin| plugin.id == plugin_id)
                    .expect("source plugin was pushed above");
                plugin.producer = Some(producer);
                plugin.transforms = transforms;
                info!(
                    "Source container with name: {name} ({key}) initialized successfully with ID: {plugin_id}."
                );
            }
            Err(error) => {
                let message = format!("Failed to set up source producer: {error}");
                error!("Source: {name} ({key}) - {message}");
                let connector = source_connectors
                    .get_mut(&path)
                    .expect("source connector was inserted above");
                let close_result = (connector.container.pico_source_close)(plugin_id);
                if close_result != 0 {
                    warn!(
                        "pico_source_close returned {close_result} while cleaning up failed source connector with ID: {plugin_id} ({key})"
                    );
                }
                if let Some(plugin) = connector
                    .plugins
                    .iter_mut()
                    .find(|plugin| plugin.id == plugin_id)
                {
                    plugin.error = Some(message);
                }
            }
        }
    }

    Ok((source_connectors, failed_plugins))
}

fn get_plugin_version(container: &Container<SourceApi>) -> String {
    unsafe {
        let version_ptr = (container.pico_source_version)();
        std::ffi::CStr::from_ptr(version_ptr)
            .to_string_lossy()
            .into_owned()
    }
}

pub(crate) fn init_source(
    container: &Container<SourceApi>,
    plugin_config: &serde_json::Value,
    id: u32,
    state: Option<ConnectorState>,
) -> Result<(), RuntimeError> {
    trace!("Initializing source plugin with config: {plugin_config:?} (ID: {id})");
    let plugin_config =
        serde_json::to_string(plugin_config).expect("Invalid source plugin config.");
    let state_ptr = state.as_ref().map_or(std::ptr::null(), |s| s.0.as_ptr());
    let state_len = state.as_ref().map_or(0, |s| s.0.len());
    let result = (container.pico_source_open)(
        id,
        plugin_config.as_ptr(),
        plugin_config.len(),
        state_ptr,
        state_len,
        LOG_CALLBACK,
    );
    if result != 0 {
        let error = format!("Plugin initialization failed (ID: {id})");
        error!("{error}");
        Err(RuntimeError::InvalidConfiguration(error))
    } else {
        Ok(())
    }
}

pub(crate) async fn setup_source_producer(
    key: &str,
    config: &SourceConfig,
    kafka: &Arc<KafkaClients>,
) -> Result<(SourceConnectorProducer, Vec<Arc<dyn Transform>>), RuntimeError> {
    let transforms = if let Some(transforms_config) = &config.transforms {
        let loaded = transform::load(transforms_config).map_err(|error| {
            RuntimeError::InvalidConfiguration(format!("Failed to load transforms: {error}"))
        })?;
        for transform in &loaded {
            info!(
                "Loaded transform: {:?} for source: {key}",
                transform.r#type()
            );
        }
        loaded
    } else {
        vec![]
    };

    let Some(topic_config) = config.topics.first() else {
        return Err(RuntimeError::InvalidConfiguration(format!(
            "Source '{key}' has no topics configured"
        )));
    };
    if config.topics.len() > 1 {
        warn!(
            "Source '{key}' configures {} topic entries; only the first is used. Use a dynamic route to fan out.",
            config.topics.len()
        );
    }

    let linger_time = match topic_config.linger_time.as_deref() {
        Some(value) => humantime::parse_duration(value).map_err(|error| {
            RuntimeError::InvalidConfiguration(format!("Invalid linger time '{value}': {error}"))
        })?,
        None => DEFAULT_LINGER_TIME,
    };
    let batch_length = topic_config
        .batch_length
        .unwrap_or(DEFAULT_BATCH_LENGTH)
        .max(1);
    let router = TopicRouter::new(&topic_config.topic)?;
    let producer = kafka.producer(key, topic_config, linger_time, batch_length)?;
    if let Some(topic) = router.static_topic() {
        kafka.ensure_topic(topic, topic_config).await?;
    }
    let encoder: Arc<dyn StreamEncoder> = match topic_config.schema {
        Schema::Avro => {
            let avro_config = AvroEncoderConfig {
                schema_json: topic_config.avro_schema_json.clone(),
                schema_path: topic_config.avro_schema_path.clone(),
                ..AvroEncoderConfig::default()
            };
            Arc::new(AvroStreamEncoder::try_new(avro_config).map_err(|error| {
                RuntimeError::InvalidConfiguration(format!(
                    "Failed to create Avro encoder for source '{key}': {error}"
                ))
            })?)
        }
        other => other.encoder(),
    };
    info!(
        "Source '{key}' routes to topic: {} (schema: {})",
        router.label(),
        topic_config.schema
    );

    Ok((
        SourceConnectorProducer {
            producer,
            encoder,
            router,
            topic_config: topic_config.clone(),
            kafka: kafka.clone(),
        },
        transforms,
    ))
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn source_forwarding_loop(
    plugin_id: u32,
    plugin_key: String,
    verbose: bool,
    benchmark: bool,
    producer: SourceConnectorProducer,
    transforms: Vec<Arc<dyn Transform>>,
    state_storage: StateStorage,
    receiver: Receiver<ProducedBatch>,
    batch_result_callback: BatchResultCallback,
    context: Arc<RuntimeContext>,
    labels: Arc<SourceLabels>,
) {
    info!("Source connector with ID: {plugin_id} started.");
    if benchmark {
        info!(
            "Benchmark mode enabled for source connector with ID: {plugin_id}, key: {plugin_key}. \
             Per-batch events on target 'picomq_connectors::benchmark'."
        );
    }
    context
        .sources
        .update_status(
            &plugin_key,
            ConnectorStatus::Running,
            Some(&context.metrics),
        )
        .await;

    let SourceConnectorProducer {
        producer,
        encoder,
        router,
        topic_config,
        kafka,
    } = producer;
    let mut number = 1u64;
    let mut last_batch_nacked = false;
    let topic_metadata = TopicMetadata {
        topic: router.label().to_owned(),
    };

    while let Ok(produced_batch) = receiver.recv_async().await {
        let total_start = Instant::now();
        let batch_id = produced_batch.id;
        let produced_messages = produced_batch.messages;
        let count = produced_messages.messages.len();
        context
            .metrics
            .inc_messages_produced_with_labels(&labels.counter, count as u64);
        if verbose {
            info!("Source connector with ID: {plugin_id} received {count} messages");
        } else {
            debug!("Source connector with ID: {plugin_id} received {count} messages");
        }
        let schema = produced_messages.schema;
        let mut messages: Vec<DecodedMessage> = Vec::with_capacity(count);
        let mut decode_errors = 0u64;
        let decode_start = Instant::now();
        for message in produced_messages.messages {
            let Ok(payload) = schema.try_into_payload(message.payload) else {
                error!(
                    "Failed to decode message payload with schema: {schema} for source connector with ID: {plugin_id}",
                );
                decode_errors += 1;
                continue;
            };

            debug!(
                "Source connector with ID: {plugin_id}] received message: {number} | schema: {schema} | payload: {payload}"
            );
            messages.push(DecodedMessage {
                offset: None,
                timestamp: message.timestamp,
                key: message.key,
                headers: message.headers,
                payload,
            });
            number += 1;
        }
        context
            .metrics
            .inc_errors_by_with_labels(&labels.counter, decode_errors);
        let decode_elapsed = decode_start.elapsed();
        context
            .metrics
            .observe_stage_with_labels(&labels.stage_decode, decode_elapsed);

        let prepare_start = Instant::now();
        let processed = process_messages(
            plugin_id,
            &encoder,
            &router,
            &topic_metadata,
            messages,
            &transforms,
            &context.metrics,
            &labels,
        );
        let prepare_elapsed = prepare_start.elapsed();
        context
            .metrics
            .observe_stage_with_labels(&labels.stage_prepare, prepare_elapsed);
        let prepared_count = processed.messages.len();
        let processing_errors = decode_errors + processed.error_count;
        let pending_state_error = state_storage.resolve_pending().await.err();
        let state_latched = state_storage.is_latched();
        let state_unavailable = pending_state_error.is_some() || state_latched;

        let broker_send_start = Instant::now();
        let send_result = if state_unavailable || processing_errors > 0 {
            Err(None)
        } else {
            match ensure_routed_topics(&kafka, &topic_config, &router, &processed.messages).await {
                Ok(()) => send_with_failed_tail_retries(processed.messages, plugin_id, |batch| {
                    send_batch(&producer, batch)
                })
                .await
                .map_err(|failure| Some(RuntimeError::Kafka(failure.error))),
                Err(error) => Err(Some(error)),
            }
        };
        let sent_count = if send_result.is_ok() {
            prepared_count
        } else {
            0
        };
        if send_result.is_ok() && sent_count > 0 {
            fault_injection::crash_after_send_if_requested();
        }
        let broker_send_elapsed = broker_send_start.elapsed();
        context
            .metrics
            .observe_stage_with_labels(&labels.stage_broker_send, broker_send_elapsed);

        let mut state_save_us: Option<u64> = None;
        let mut batch_result = SourceBatchResult::Nack;
        if let Err(error) = send_result {
            let error_msg = if let Some(state_error) = pending_state_error.as_ref() {
                format!(
                    "Rejected source batch {batch_id} while resolving a pending checkpoint for source connector with ID: {plugin_id}. {state_error}"
                )
            } else if state_latched {
                format!(
                    "Rejected source batch {batch_id} because state storage is latched for source connector with ID: {plugin_id}"
                )
            } else if processing_errors > 0 {
                format!(
                    "Rejected source batch {batch_id} after {processing_errors} decode or processing errors for source connector with ID: {plugin_id}"
                )
            } else {
                format!(
                    "Failed to send {prepared_count} messages to topic: {} by source connector with ID: {plugin_id}. {}",
                    router.label(),
                    error
                        .map(|error| error.to_string())
                        .unwrap_or_else(|| "unknown error".to_owned()),
                )
            };
            error!("{error_msg}");
            context.metrics.inc_errors_with_labels(&labels.counter);
            let preserve_original_error =
                matches!(pending_state_error.as_ref(), Some(SdkError::StateLatched))
                    || (pending_state_error.is_none() && state_latched);
            if !preserve_original_error {
                context
                    .sources
                    .set_error(&plugin_key, &error_msg, &context.metrics)
                    .await;
            }
        } else {
            context
                .metrics
                .inc_messages_sent_with_labels(&labels.counter, sent_count as u64);

            if verbose {
                info!(
                    "Sent {sent_count} of {count} messages to topic: {} by source connector with ID: {plugin_id}",
                    router.label()
                );
            } else {
                debug!(
                    "Sent {sent_count} of {count} messages to topic: {} by source connector with ID: {plugin_id}",
                    router.label()
                );
            }

            let mut state_saved = true;
            if count == 0 && last_batch_nacked && produced_messages.state.is_some() {
                warn!(
                    "Skipping state save for empty batch {batch_id} that followed a rejected batch for source connector with ID: {plugin_id}"
                );
            } else if let Some(state) = produced_messages.state {
                let state_save_start = Instant::now();
                match state_storage.save(state).await {
                    Ok(()) => {
                        debug!("State saved for source connector with ID: {plugin_id}");
                        let state_save_elapsed = state_save_start.elapsed();
                        context.metrics.observe_stage_with_labels(
                            &labels.stage_state_save,
                            state_save_elapsed,
                        );
                        state_save_us = Some(benchmark::as_micros(state_save_elapsed));
                    }
                    Err(error) => {
                        state_saved = false;
                        let error_msg = format!(
                            "Failed to save state for source connector with ID: {plugin_id}. {error}"
                        );
                        error!("{error_msg}");
                        context.metrics.inc_errors_with_labels(&labels.counter);
                        context
                            .sources
                            .set_error(&plugin_key, &error_msg, &context.metrics)
                            .await;
                    }
                }
            } else {
                debug!("No state provided for source connector with ID: {plugin_id}");
            }

            if state_saved {
                batch_result = SourceBatchResult::Ack;
                if count > 0 {
                    last_batch_nacked = false;
                    context
                        .sources
                        .clear_error(&plugin_key, &context.metrics)
                        .await;
                }
            }
        }
        if batch_result == SourceBatchResult::Nack {
            last_batch_nacked = true;
        }

        let result_code = tokio::task::spawn_blocking(move || {
            batch_result_callback(plugin_id, batch_id, batch_result as u8)
        })
        .await
        .unwrap_or(-1);
        if result_code != 0 {
            if context.sources.is_stopping_or_stopped(&plugin_key).await {
                trace!(
                    "Source connector with ID: {plugin_id} stopped before {batch_result:?} could be delivered for batch ID: {batch_id}"
                );
            } else {
                let error_msg = format!(
                    "Failed to deliver {batch_result:?} for source connector with ID: {plugin_id}, batch ID: {batch_id}. Plugin returned: {result_code}"
                );
                error!("{error_msg}");
                context.metrics.inc_errors_with_labels(&labels.counter);
                context
                    .sources
                    .set_error(&plugin_key, &error_msg, &context.metrics)
                    .await;
            }
        }

        let total_elapsed = total_start.elapsed();
        context
            .metrics
            .observe_stage_with_labels(&labels.stage_total, total_elapsed);

        if benchmark {
            benchmark::emit_source_event(
                &plugin_key,
                router.label(),
                count,
                sent_count,
                benchmark::as_micros(decode_elapsed),
                benchmark::as_micros(prepare_elapsed),
                benchmark::as_micros(broker_send_elapsed),
                state_save_us,
                benchmark::as_micros(total_elapsed),
            );
        }
    }

    if context.sources.is_stopping_or_stopped(&plugin_key).await {
        info!("Source connector with ID: {plugin_id} stopped.");
        context
            .sources
            .update_status(
                &plugin_key,
                ConnectorStatus::Stopped,
                Some(&context.metrics),
            )
            .await;
    } else {
        let error_msg = format!(
            "Source connector with ID: {plugin_id} stopped polling and will not produce more batches until restarted"
        );
        error!("{error_msg}");
        context.metrics.inc_errors_with_labels(&labels.counter);
        context
            .sources
            .set_error(&plugin_key, &error_msg, &context.metrics)
            .await;
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn spawn_source_handler(
    plugin_id: u32,
    plugin_key: &str,
    verbose: bool,
    benchmark: bool,
    producer: SourceConnectorProducer,
    transforms: Vec<Arc<dyn Transform>>,
    state_storage: StateStorage,
    handle_callback: HandleCallback,
    batch_result_callback: BatchResultCallback,
    context: Arc<RuntimeContext>,
) -> Vec<JoinHandle<()>> {
    let (sender, receiver) = flume::unbounded();
    let plugin_key = plugin_key.to_string();
    let labels = Arc::new(SourceLabels::new(&plugin_key));
    SOURCE_SENDERS.insert(
        plugin_id,
        SourceSenderEntry {
            sender,
            error_counter: context.metrics.error_counter(&labels.counter),
        },
    );

    let blocking_handle = tokio::task::spawn_blocking(move || {
        handle_callback(plugin_id, handle_produced_messages);
    });
    let handler_task = tokio::spawn(async move {
        source_forwarding_loop(
            plugin_id,
            plugin_key,
            verbose,
            benchmark,
            producer,
            transforms,
            state_storage,
            receiver,
            batch_result_callback,
            context,
            labels,
        )
        .await;
    });

    vec![blocking_handle, handler_task]
}

pub fn handle(
    sources: Vec<SourceConnectorWrapper>,
    context: Arc<RuntimeContext>,
) -> Vec<(String, Vec<JoinHandle<()>>)> {
    let mut handles = Vec::new();
    for source in sources {
        for plugin in source.plugins {
            let plugin_id = plugin.id;
            let plugin_key = plugin.key.clone();

            if let Some(error) = &plugin.error {
                error!(
                    "Failed to initialize source connector with ID: {plugin_id}: {error}. Skipping...",
                );
                continue;
            }
            info!("Starting handler for source connector with ID: {plugin_id}...");

            let Some(producer) = plugin.producer else {
                error!("Producer not initialized for source connector with ID: {plugin_id}");
                continue;
            };

            let handler_tasks = spawn_source_handler(
                plugin_id,
                &plugin_key,
                plugin.verbose,
                plugin.benchmark,
                producer,
                plugin.transforms,
                plugin.state_storage,
                source.handle_callback,
                source.batch_result_callback,
                context.clone(),
            );

            handles.push((plugin_key, handler_tasks));
        }
    }
    handles
}

struct ProcessedMessages {
    messages: Vec<OutgoingMessage>,
    error_count: u64,
}

#[allow(clippy::too_many_arguments)]
fn process_messages(
    id: u32,
    encoder: &Arc<dyn StreamEncoder>,
    router: &TopicRouter,
    topic_metadata: &TopicMetadata,
    messages: Vec<DecodedMessage>,
    transforms: &[Arc<dyn Transform>],
    metrics: &Arc<crate::metrics::Metrics>,
    labels: &SourceLabels,
) -> ProcessedMessages {
    let mut outgoing = Vec::with_capacity(messages.len());
    let mut error_count = 0u64;
    let mut filtered_count = 0u64;
    for message in messages {
        let mut current_message = Some(message);
        let mut transform_failed = false;
        for transform in transforms.iter() {
            let Some(message) = current_message.take() else {
                break;
            };

            match transform.transform(topic_metadata, message) {
                Ok(next) => current_message = next,
                Err(error) => {
                    error!(
                        "Transform '{:?}' failed for source connector with ID: {id}, topic: {}: {error}",
                        transform.r#type(),
                        topic_metadata.topic
                    );
                    error_count += 1;
                    transform_failed = true;
                    break;
                }
            }
        }
        if transform_failed {
            continue;
        }

        let Some(message) = current_message else {
            filtered_count += 1;
            continue;
        };

        let topic = match router.route(&message) {
            Ok(topic) => topic,
            Err(error) => {
                error!(
                    "Failed to route message for source connector with ID: {id}, route: {}: {error}",
                    router.label()
                );
                error_count += 1;
                continue;
            }
        };

        let Ok(payload) = encoder.encode(message.payload) else {
            error!(
                "Failed to encode message payload for source connector with ID: {id}, topic: {topic}"
            );
            error_count += 1;
            continue;
        };

        outgoing.push(OutgoingMessage {
            topic,
            key: message.key,
            timestamp: message.timestamp,
            headers: message.headers,
            payload,
        });
    }
    metrics.inc_errors_by_with_labels(&labels.counter, error_count);
    if filtered_count > 0 {
        metrics.inc_messages_filtered_with_labels(&labels.counter, filtered_count);
    }
    ProcessedMessages {
        messages: outgoing,
        error_count,
    }
}

async fn ensure_routed_topics(
    kafka: &KafkaClients,
    topic_config: &TopicProducerConfig,
    router: &TopicRouter,
    messages: &[OutgoingMessage],
) -> Result<(), RuntimeError> {
    if !topic_config.create_topics || router.is_static() {
        return Ok(());
    }
    let mut seen: HashSet<&str> = HashSet::new();
    for message in messages {
        if seen.insert(&message.topic) {
            kafka.ensure_topic(&message.topic, topic_config).await?;
        }
    }
    Ok(())
}

async fn send_batch(
    producer: &FutureProducer,
    messages: Vec<OutgoingMessage>,
) -> Result<(), SendFailure> {
    let deliveries = messages.iter().map(|message| {
        let mut record: FutureRecord<'_, [u8], [u8]> = FutureRecord::to(&message.topic);
        record = record.payload(message.payload.as_slice());
        if let Some(key) = &message.key {
            record = record.key(key.as_slice());
        }
        if let Some(timestamp) = message.timestamp {
            record = record.timestamp(i64::try_from(timestamp).unwrap_or(0));
        }
        if let Some(headers) = &message.headers {
            let mut owned = OwnedHeaders::new_with_capacity(headers.len());
            for (name, value) in headers {
                owned = owned.insert(Header {
                    key: name,
                    value: Some(value.as_slice()),
                });
            }
            record = record.headers(owned);
        }
        producer.send(record, Timeout::Never)
    });
    let results = join_all(deliveries).await;
    let mut failed = Vec::new();
    let mut first_error: Option<KafkaError> = None;
    let mut committed = 0usize;
    for (message, result) in messages.into_iter().zip(results) {
        match result {
            Ok(_) => committed += 1,
            Err((error, _)) => {
                if first_error.is_none() {
                    first_error = Some(error);
                }
                failed.push(message);
            }
        }
    }
    match first_error {
        None => Ok(()),
        Some(error) => Err(SendFailure {
            error,
            failed,
            committed,
        }),
    }
}

async fn send_with_failed_tail_retries<F, Fut>(
    mut messages: Vec<OutgoingMessage>,
    plugin_id: u32,
    mut send: F,
) -> Result<(), SendFailure>
where
    F: FnMut(Vec<OutgoingMessage>) -> Fut,
    Fut: Future<Output = Result<(), SendFailure>>,
{
    let mut retry = 0;
    loop {
        match send(messages).await {
            Ok(()) => return Ok(()),
            Err(failure) => {
                warn!(
                    "Source connector with ID: {plugin_id} send failed after {} messages committed; {} messages remain",
                    failure.committed,
                    failure.failed.len()
                );
                if retry >= MAX_FAILED_TAIL_RETRIES || failure.failed.is_empty() {
                    return Err(failure);
                }
                messages = failure.failed;
                retry += 1;
            }
        }
    }
}

pub(crate) extern "C" fn handle_produced_messages(
    plugin_id: u32,
    batch_id: u64,
    messages_ptr: *const u8,
    messages_len: usize,
) -> i32 {
    if batch_id == POLL_TASK_ENDED_BATCH_ID && messages_ptr.is_null() {
        if SOURCE_SENDERS.remove(&plugin_id).is_some() {
            error!(
                "Source connector with ID: {plugin_id} stopped polling; closing its forwarding channel"
            );
        }
        return 0;
    }
    unsafe {
        let Some(entry) = SOURCE_SENDERS.get(&plugin_id) else {
            tracing::trace!(
                plugin_id,
                "dropping produced batch: sender already cleaned up"
            );
            return -1;
        };
        let messages = std::slice::from_raw_parts(messages_ptr, messages_len);
        match postcard::from_bytes::<ProducedMessages>(messages) {
            Ok(messages) => {
                if let Err(send_error) = entry.sender.send(ProducedBatch {
                    id: batch_id,
                    messages,
                }) {
                    error!(
                        "Failed to send messages for source connector with ID: {plugin_id}. Channel closed: {send_error}"
                    );
                    entry.error_counter.inc();
                    return -1;
                }
                0
            }
            Err(err) => {
                error!(
                    "Failed to deserialize produced messages for source connector with ID: {plugin_id}. {err}"
                );
                entry.error_counter.inc();
                -1
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;
    use std::future::ready;
    use std::sync::atomic::{AtomicU32, Ordering};

    static TEST_PLUGIN_ID: AtomicU32 = AtomicU32::new(u32::MAX / 2);

    fn next_plugin_id() -> u32 {
        TEST_PLUGIN_ID.fetch_add(1, Ordering::Relaxed)
    }

    fn test_message(marker: u8) -> OutgoingMessage {
        OutgoingMessage {
            topic: "test-topic".to_owned(),
            key: None,
            timestamp: None,
            headers: None,
            payload: vec![marker],
        }
    }

    fn failed_send(messages: Vec<OutgoingMessage>, committed: usize) -> SendFailure {
        SendFailure {
            error: KafkaError::Canceled,
            failed: messages,
            committed,
        }
    }

    #[test]
    fn given_serialized_batch_when_callback_runs_should_forward_batch_id() {
        let plugin_id = next_plugin_id();
        let batch_id = 73;
        let (sender, receiver) = flume::unbounded();
        SOURCE_SENDERS.insert(
            plugin_id,
            SourceSenderEntry {
                sender,
                error_counter: Counter::default(),
            },
        );
        let messages = ProducedMessages {
            schema: Schema::Raw,
            messages: Vec::new(),
            state: Some(ConnectorState(vec![1, 2, 3])),
        };
        let serialized = postcard::to_allocvec(&messages).expect("failed to serialize batch");

        assert_eq!(
            handle_produced_messages(plugin_id, batch_id, serialized.as_ptr(), serialized.len()),
            0
        );
        let forwarded = receiver.recv().expect("batch was not forwarded");
        assert_eq!(forwarded.id, batch_id);
        assert_eq!(
            forwarded
                .messages
                .state
                .expect("state should be preserved")
                .0,
            vec![1, 2, 3]
        );

        cleanup_sender(plugin_id);
    }

    #[test]
    fn given_invalid_payload_when_callback_runs_should_reject_batch() {
        let plugin_id = next_plugin_id();
        let (sender, _receiver) = flume::unbounded();
        let error_counter = Counter::default();
        SOURCE_SENDERS.insert(
            plugin_id,
            SourceSenderEntry {
                sender,
                error_counter: error_counter.clone(),
            },
        );
        let invalid_payload = [0xff];

        assert_eq!(
            handle_produced_messages(
                plugin_id,
                1,
                invalid_payload.as_ptr(),
                invalid_payload.len(),
            ),
            -1
        );
        assert_eq!(error_counter.get(), 1);

        cleanup_sender(plugin_id);
    }

    #[test]
    fn given_missing_sender_when_callback_runs_should_reject_batch() {
        let plugin_id = next_plugin_id();
        let serialized = postcard::to_allocvec(&ProducedMessages {
            schema: Schema::Raw,
            messages: Vec::new(),
            state: None,
        })
        .expect("failed to serialize batch");

        assert_eq!(
            handle_produced_messages(plugin_id, 1, serialized.as_ptr(), serialized.len()),
            -1
        );
    }

    #[test]
    fn given_partially_committed_send_should_retry_only_failed_tail() {
        let runtime = tokio::runtime::Runtime::new().expect("failed to create test runtime");
        runtime.block_on(async {
            let mut attempts = Vec::new();
            let mut responses = VecDeque::from([
                Err(failed_send(vec![test_message(2), test_message(3)], 1)),
                Ok(()),
            ]);

            let result = send_with_failed_tail_retries(
                vec![test_message(1), test_message(2), test_message(3)],
                31,
                |messages| {
                    attempts.push(
                        messages
                            .iter()
                            .map(|message| message.payload[0])
                            .collect::<Vec<_>>(),
                    );
                    ready(responses.pop_front().expect("send response should exist"))
                },
            )
            .await;

            assert!(result.is_ok());
            assert_eq!(attempts, vec![vec![1, 2, 3], vec![2, 3]]);
        });
    }

    #[test]
    fn given_repeated_failed_tail_when_retries_are_exhausted_should_return_error() {
        let runtime = tokio::runtime::Runtime::new().expect("failed to create test runtime");
        runtime.block_on(async {
            let mut attempts = 0;
            let mut responses = VecDeque::from(
                (0..=MAX_FAILED_TAIL_RETRIES)
                    .map(|_| Err::<(), _>(failed_send(vec![test_message(1)], 0)))
                    .collect::<Vec<_>>(),
            );

            let result = send_with_failed_tail_retries(vec![test_message(1)], 37, |_| {
                attempts += 1;
                ready(responses.pop_front().expect("send response should exist"))
            })
            .await;

            assert!(result.is_err());
            assert_eq!(attempts, MAX_FAILED_TAIL_RETRIES + 1);
        });
    }

    #[test]
    fn given_failure_with_empty_tail_when_retrying_should_return_immediately() {
        let runtime = tokio::runtime::Runtime::new().expect("failed to create test runtime");
        runtime.block_on(async {
            let mut attempts = 0;
            let result = send_with_failed_tail_retries(vec![test_message(1)], 41, |_| {
                attempts += 1;
                ready(Err(failed_send(Vec::new(), 0)))
            })
            .await;

            assert!(result.is_err());
            assert_eq!(attempts, 1);
        });
    }
}

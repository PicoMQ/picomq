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

use crate::benchmark;
use crate::configs::connectors::SinkConfig;
use crate::context::RuntimeContext;
use crate::kafka::KafkaClients;
use crate::log::LOG_CALLBACK;
use crate::metrics::{Metrics, SinkLabels};
use crate::{
    FailedPlugin, PLUGIN_ID, RuntimeError, SinkApi, SinkConnector, SinkConnectorConsumer,
    SinkConnectorPlugin, SinkConnectorWrapper, resolve_plugin_path, transform,
};
use dlopen2::wrapper::Container;
use picomq_connector_sdk::decoders::avro::{AvroConfig, AvroStreamDecoder};
use picomq_connector_sdk::{
    DecodedMessage, Headers, MessagesMetadata, RawMessage, RawMessages, ReceivedMessage, Schema,
    StreamDecoder, TopicMetadata, now_millis, retry::exponential_backoff, sink::ConsumeCallback,
    transforms::Transform,
};
use rdkafka::Message;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::message::Headers as KafkaHeaders;
use std::{
    collections::HashMap,
    sync::{Arc, atomic::Ordering},
    time::{Duration, Instant},
};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};

const DEFAULT_BATCH_LENGTH: u32 = 1000;
const DEFAULT_POLL_INTERVAL: Duration = Duration::from_millis(100);
const SINK_CONSUME_RETRYABLE_STATUS: i32 = 1;
const MAX_CONSUME_ATTEMPTS: u32 = 5;
const CONSUME_RETRY_BASE_DELAY: Duration = Duration::from_millis(200);
const CONSUME_RETRY_MAX_DELAY: Duration = Duration::from_secs(5);

#[cfg(debug_assertions)]
mod fault_injection {
    use std::sync::atomic::{AtomicU64, Ordering};
    use tracing::warn;

    pub const FAIL_CONSUME_ENV: &str = "PICOMQ_CONNECTORS_FAULT_SINK_CONSUME_FAIL";
    static ATTEMPTS: AtomicU64 = AtomicU64::new(0);

    pub fn fail_consume_if_requested() -> bool {
        let Some((skip, failures)) = std::env::var(FAIL_CONSUME_ENV)
            .ok()
            .and_then(|value| parse(&value))
        else {
            return false;
        };
        let attempt = ATTEMPTS.fetch_add(1, Ordering::SeqCst);
        if attempt >= skip && attempt < skip + failures {
            warn!(
                "Fault injection: failing sink consume attempt {} of {failures} after {skip} successful",
                attempt - skip + 1
            );
            return true;
        }
        false
    }

    fn parse(value: &str) -> Option<(u64, u64)> {
        match value.split_once(':') {
            Some((skip, failures)) => Some((skip.parse().ok()?, failures.parse().ok()?)),
            None => Some((0, value.parse().ok()?)),
        }
    }
}

#[cfg(not(debug_assertions))]
mod fault_injection {
    pub fn fail_consume_if_requested() -> bool {
        false
    }
}

pub async fn init(
    sink_configs: HashMap<String, SinkConfig>,
    kafka: &KafkaClients,
) -> Result<(HashMap<String, SinkConnector>, Vec<FailedPlugin>), RuntimeError> {
    let mut sink_connectors: HashMap<String, SinkConnector> = HashMap::new();
    let mut failed_plugins: Vec<FailedPlugin> = Vec::new();

    for (key, config) in sink_configs {
        let name = config.name.clone();
        if !config.enabled {
            warn!("Sink: {name} is disabled ({key})");
            continue;
        }

        let plugin_id = PLUGIN_ID.fetch_add(1, Ordering::SeqCst);

        let path = match resolve_plugin_path(&config.path) {
            Ok(path) => path,
            Err(error) => {
                let message = format!("Failed to resolve plugin path: {error}");
                error!("Sink: {name} ({key}) - {message}");
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
            "Initializing sink container with name: {name} ({key}), config version: {}, plugin: {path}",
            &config.version
        );

        if !sink_connectors.contains_key(&path) {
            let container = match unsafe { Container::<SinkApi>::load(&path) } {
                Ok(container) => container,
                Err(error) => {
                    let message = format!("Failed to load sink container from {path}: {error}");
                    error!("Sink: {name} ({key}) - {message}");
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
            info!("Sink container for plugin: {path} loaded successfully.");
            sink_connectors.insert(
                path.clone(),
                SinkConnector {
                    container,
                    plugins: Vec::new(),
                },
            );
        } else {
            info!("Sink container for plugin: {path} is already loaded.");
        }

        let connector = sink_connectors
            .get_mut(&path)
            .expect("sink container was just ensured for this path");
        let version = get_plugin_version(&connector.container);
        let init_error = init_sink(
            &connector.container,
            &config.plugin_config.clone().unwrap_or_default(),
            plugin_id,
        )
        .err()
        .map(|error| error.to_string());

        connector.plugins.push(SinkConnectorPlugin {
            id: plugin_id,
            key: key.clone(),
            name: name.clone(),
            path: path.clone(),
            version,
            config_format: config.plugin_config_format,
            consumers: vec![],
            error: init_error.clone(),
            verbose: config.verbose,
            benchmark: config.benchmark,
        });

        if let Some(error) = init_error {
            error!("Failed to initialize sink container with name: {name} ({key}). {error}");
            continue;
        }

        match setup_sink_consumers(&key, &config, kafka).await {
            Ok(consumers) => {
                let connector = sink_connectors
                    .get_mut(&path)
                    .expect("sink connector was inserted above");
                let plugin = connector
                    .plugins
                    .iter_mut()
                    .find(|plugin| plugin.id == plugin_id)
                    .expect("sink plugin was pushed above");
                plugin.consumers = consumers;
                info!(
                    "Sink container with name: {name} ({key}) initialized successfully with ID: {plugin_id}."
                );
            }
            Err(error) => {
                let message = format!("Failed to set up sink consumers: {error}");
                error!("Sink: {name} ({key}) - {message}");
                let connector = sink_connectors
                    .get_mut(&path)
                    .expect("sink connector was inserted above");
                let close_result = (connector.container.pico_sink_close)(plugin_id);
                if close_result != 0 {
                    warn!(
                        "pico_sink_close returned {close_result} while cleaning up failed sink connector with ID: {plugin_id} ({key})"
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

    Ok((sink_connectors, failed_plugins))
}

pub fn consume(
    sinks: Vec<SinkConnectorWrapper>,
    context: Arc<RuntimeContext>,
) -> Vec<(String, watch::Sender<()>, Vec<JoinHandle<()>>)> {
    let mut handles = Vec::new();
    for sink in sinks {
        for plugin in sink.plugins {
            if let Some(error) = &plugin.error {
                error!(
                    "Failed to initialize sink connector with ID: {}: {error}. Skipping...",
                    plugin.id,
                );
                continue;
            }
            info!("Starting consume for sink with ID: {}...", plugin.id);
            let (shutdown_tx, task_handles) = spawn_consume_tasks(
                plugin.id,
                &plugin.key,
                plugin.consumers,
                sink.callback,
                plugin.verbose,
                plugin.benchmark,
                &context.metrics,
                context.clone(),
            );
            handles.push((plugin.key, shutdown_tx, task_handles));
        }
    }
    handles
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn spawn_consume_tasks(
    plugin_id: u32,
    plugin_key: &str,
    consumers: Vec<SinkConnectorConsumer>,
    callback: ConsumeCallback,
    verbose: bool,
    benchmark: bool,
    metrics: &Arc<Metrics>,
    context: Arc<RuntimeContext>,
) -> (watch::Sender<()>, Vec<JoinHandle<()>>) {
    if benchmark {
        info!(
            "Benchmark mode enabled for sink connector with ID: {plugin_id}, key: {plugin_key}. \
             Per-batch events on target 'picomq_connectors::benchmark'."
        );
    }
    let (shutdown_tx, shutdown_rx) = watch::channel(());
    let mut task_handles = Vec::new();
    let labels = Arc::new(SinkLabels::new(plugin_key));
    for consumer in consumers {
        let plugin_key = plugin_key.to_string();
        let metrics = metrics.clone();
        let shutdown_rx = shutdown_rx.clone();
        let context = context.clone();
        let labels = labels.clone();
        let handle = tokio::spawn(async move {
            if let Err(error) = consume_messages(
                plugin_id,
                consumer,
                callback,
                verbose,
                benchmark,
                &plugin_key,
                &metrics,
                &labels,
                shutdown_rx,
            )
            .await
            {
                error!(
                    "Failed to consume messages for sink connector with ID: {plugin_id}: {error}"
                );
                metrics.inc_errors_with_labels(&labels.counter);
                context
                    .sinks
                    .set_error(&plugin_key, &error.to_string(), &metrics)
                    .await;
            }
        });
        task_handles.push(handle);
    }
    (shutdown_tx, task_handles)
}

struct PendingMessage {
    offset: i64,
    timestamp: u64,
    key: Option<Vec<u8>>,
    headers: Option<Headers>,
    payload: Vec<u8>,
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn consume_messages(
    plugin_id: u32,
    sink_consumer: SinkConnectorConsumer,
    consume: ConsumeCallback,
    verbose: bool,
    benchmark: bool,
    plugin_key: &str,
    metrics: &Arc<Metrics>,
    labels: &SinkLabels,
    mut shutdown_rx: watch::Receiver<()>,
) -> Result<(), RuntimeError> {
    info!("Started consuming messages for sink connector with ID: {plugin_id}");
    let SinkConnectorConsumer {
        consumer,
        decoder,
        batch_size,
        poll_interval,
        transforms,
    } = sink_consumer;
    let batch_size = batch_size as usize;
    let mut batches: HashMap<(String, i32), Vec<PendingMessage>> = HashMap::new();

    loop {
        let received = tokio::select! {
            _ = shutdown_rx.changed() => {
                info!("Sink connector with ID: {plugin_id} received shutdown signal");
                break;
            }
            received = tokio::time::timeout(poll_interval, consumer.recv()) => received,
        };

        let mut ready: Vec<(String, i32)> = Vec::new();
        match received {
            Ok(Ok(message)) => {
                let topic = message.topic().to_owned();
                let partition = message.partition();
                let pending = PendingMessage {
                    offset: message.offset(),
                    timestamp: message
                        .timestamp()
                        .to_millis()
                        .and_then(|millis| u64::try_from(millis).ok())
                        .unwrap_or_else(now_millis),
                    key: message.key().map(<[u8]>::to_vec),
                    headers: message.headers().map(|headers| {
                        headers
                            .iter()
                            .map(|header| {
                                (
                                    header.key.to_owned(),
                                    header.value.map(<[u8]>::to_vec).unwrap_or_default(),
                                )
                            })
                            .collect::<Headers>()
                    }),
                    payload: message.payload().map(<[u8]>::to_vec).unwrap_or_default(),
                };
                let batch = batches.entry((topic.clone(), partition)).or_default();
                batch.push(pending);
                if batch.len() >= batch_size {
                    ready.push((topic, partition));
                }
            }
            Ok(Err(error)) => {
                error!(
                    "Failed to receive message for sink connector with ID: {plugin_id}: {error}"
                );
                metrics.inc_errors_with_labels(&labels.counter);
                continue;
            }
            Err(_) => {
                ready.extend(batches.keys().cloned());
            }
        }

        for key in ready {
            let Some(messages) = batches.remove(&key) else {
                continue;
            };
            if messages.is_empty() {
                continue;
            }
            let (topic, partition) = key;
            let flushed = flush_batch(
                plugin_id,
                plugin_key,
                &consumer,
                &decoder,
                &transforms,
                &consume,
                verbose,
                benchmark,
                metrics,
                labels,
                topic,
                partition,
                messages,
            )
            .await;
            if let Err(error) = flushed {
                commit_stored_offsets(plugin_id, &consumer);
                return Err(error);
            }
        }
    }

    for ((topic, partition), messages) in batches.drain() {
        if messages.is_empty() {
            continue;
        }
        let flushed = flush_batch(
            plugin_id,
            plugin_key,
            &consumer,
            &decoder,
            &transforms,
            &consume,
            verbose,
            benchmark,
            metrics,
            labels,
            topic,
            partition,
            messages,
        )
        .await;
        if let Err(error) = flushed {
            commit_stored_offsets(plugin_id, &consumer);
            return Err(error);
        }
    }
    commit_stored_offsets(plugin_id, &consumer);
    info!("Stopped consuming messages for sink connector with ID: {plugin_id}");
    Ok(())
}

fn commit_stored_offsets(plugin_id: u32, consumer: &StreamConsumer) {
    match consumer.commit_consumer_state(CommitMode::Sync) {
        Ok(()) => {}
        Err(rdkafka::error::KafkaError::ConsumerCommit(
            rdkafka::types::RDKafkaErrorCode::NoOffset,
        )) => {}
        Err(error) => warn!(
            "Failed to commit stored offsets for sink connector with ID: {plugin_id}: {error}"
        ),
    }
}

#[allow(clippy::too_many_arguments)]
async fn flush_batch(
    plugin_id: u32,
    plugin_key: &str,
    consumer: &StreamConsumer,
    decoder: &Arc<dyn StreamDecoder>,
    transforms: &[Arc<dyn Transform>],
    consume: &ConsumeCallback,
    verbose: bool,
    benchmark: bool,
    metrics: &Arc<Metrics>,
    labels: &SinkLabels,
    topic: String,
    partition: i32,
    messages: Vec<PendingMessage>,
) -> Result<(), RuntimeError> {
    let messages_count = messages.len();
    let last_offset = messages.last().map(|message| message.offset).unwrap_or(0);
    let current_offset = u64::try_from(last_offset).unwrap_or(0);
    metrics.inc_messages_consumed_with_labels(&labels.counter, messages_count as u64);
    let topic_metadata = TopicMetadata { topic };
    let messages_metadata = MessagesMetadata {
        partition,
        current_offset,
        schema: decoder.schema(),
    };
    if verbose {
        info!(
            "Processing {messages_count} messages from topic: {} for sink connector with ID: {plugin_id}",
            topic_metadata.topic
        );
    } else {
        debug!(
            "Processing {messages_count} messages from topic: {} for sink connector with ID: {plugin_id}",
            topic_metadata.topic
        );
    }
    let start = Instant::now();
    let result = process_messages(
        plugin_id,
        messages_metadata,
        &topic_metadata,
        messages,
        consume,
        transforms,
        decoder,
        metrics,
        labels,
    )
    .await;
    let elapsed = start.elapsed();
    metrics.observe_stage_with_labels(&labels.stage_total, elapsed);

    let (processed_count, decode_us, prepare_us, ffi_us) = match &result {
        Ok(timing) => {
            let prepare_elapsed = elapsed
                .saturating_sub(timing.ffi_elapsed)
                .saturating_sub(timing.decode_elapsed);
            metrics.observe_stage_with_labels(&labels.stage_decode, timing.decode_elapsed);
            metrics.observe_stage_with_labels(&labels.stage_prepare, prepare_elapsed);
            metrics.observe_stage_with_labels(&labels.stage_ffi, timing.ffi_elapsed);
            (
                timing.processed_count,
                benchmark::as_micros(timing.decode_elapsed),
                benchmark::as_micros(prepare_elapsed),
                benchmark::as_micros(timing.ffi_elapsed),
            )
        }
        Err(_) => (0, 0, 0, 0),
    };

    if benchmark {
        benchmark::emit_sink_event(
            plugin_key,
            &topic_metadata.topic,
            partition,
            current_offset,
            messages_count,
            processed_count,
            decode_us,
            prepare_us,
            ffi_us,
            benchmark::as_micros(elapsed),
        );
    }

    if let Err(error) = result {
        error!(
            "Failed to process {messages_count} messages for sink connector with ID: {plugin_id}. {error}",
        );
        return Err(error);
    }

    if let Err(error) = consumer.store_offset(&topic_metadata.topic, partition, last_offset) {
        warn!(
            "Failed to store offset {last_offset} for topic: {}, partition: {partition}, sink connector with ID: {plugin_id}: {error}",
            topic_metadata.topic
        );
    } else if let Err(error) = consumer.commit_consumer_state(CommitMode::Async) {
        warn!(
            "Failed to commit offset {last_offset} for topic: {}, partition: {partition}, sink connector with ID: {plugin_id}: {error}",
            topic_metadata.topic
        );
    }

    metrics.inc_messages_processed_with_labels(&labels.counter, processed_count as u64);
    if verbose {
        info!(
            "Consumed {messages_count} messages in {:#?} for sink connector with ID: {plugin_id}",
            elapsed
        );
    } else {
        debug!(
            "Consumed {messages_count} messages in {:#?} for sink connector with ID: {plugin_id}",
            elapsed
        );
    }
    Ok(())
}

fn get_plugin_version(container: &Container<SinkApi>) -> String {
    unsafe {
        let version_ptr = (container.pico_sink_version)();
        std::ffi::CStr::from_ptr(version_ptr)
            .to_string_lossy()
            .into_owned()
    }
}

pub(crate) fn init_sink(
    container: &Container<SinkApi>,
    plugin_config: &serde_json::Value,
    id: u32,
) -> Result<(), RuntimeError> {
    let plugin_config = serde_json::to_string(plugin_config).expect("Invalid sink plugin config.");
    let result = (container.pico_sink_open)(
        id,
        plugin_config.as_ptr(),
        plugin_config.len(),
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

pub(crate) async fn setup_sink_consumers(
    key: &str,
    config: &SinkConfig,
    kafka: &KafkaClients,
) -> Result<Vec<SinkConnectorConsumer>, RuntimeError> {
    let transforms = if let Some(transforms_config) = &config.transforms {
        let loaded = transform::load(transforms_config).map_err(|error| {
            RuntimeError::InvalidConfiguration(format!("Failed to load transforms: {error}"))
        })?;
        for transform in &loaded {
            info!("Loaded transform: {:?} for sink: {key}", transform.r#type());
        }
        loaded
    } else {
        vec![]
    };

    if config.topics.is_empty() {
        return Err(RuntimeError::InvalidConfiguration(format!(
            "Sink '{key}' has no topics configured"
        )));
    }

    let mut consumers = Vec::with_capacity(config.topics.len());
    for topic_config in config.topics.iter() {
        let poll_interval = match topic_config.poll_interval.as_deref() {
            Some(value) => humantime::parse_duration(value).map_err(|error| {
                RuntimeError::InvalidConfiguration(format!(
                    "Invalid poll interval '{value}': {error}"
                ))
            })?,
            None => DEFAULT_POLL_INTERVAL,
        };
        let batch_length = topic_config
            .batch_length
            .unwrap_or(DEFAULT_BATCH_LENGTH)
            .max(1);
        let consumer = kafka.consumer(key, topic_config)?;
        let decoder: Arc<dyn StreamDecoder> = match topic_config.schema {
            Schema::Avro => {
                let avro_config = AvroConfig {
                    schema_json: topic_config.avro_schema_json.clone(),
                    schema_path: topic_config.avro_schema_path.clone(),
                    ..AvroConfig::default()
                };
                Arc::new(AvroStreamDecoder::try_new(avro_config).map_err(|error| {
                    RuntimeError::InvalidConfiguration(format!(
                        "Failed to create Avro decoder for sink '{key}': {error}"
                    ))
                })?)
            }
            other => other.decoder(),
        };
        consumers.push(SinkConnectorConsumer {
            consumer,
            decoder,
            batch_size: batch_length,
            poll_interval,
            transforms: transforms.clone(),
        });
    }
    Ok(consumers)
}

#[allow(clippy::too_many_arguments)]
async fn process_messages(
    plugin_id: u32,
    messages_metadata: MessagesMetadata,
    topic_metadata: &TopicMetadata,
    messages: Vec<PendingMessage>,
    consume: &ConsumeCallback,
    transforms: &[Arc<dyn Transform>],
    decoder: &Arc<dyn StreamDecoder>,
    metrics: &Arc<Metrics>,
    labels: &SinkLabels,
) -> Result<SinkBatchTiming, RuntimeError> {
    let received = messages.into_iter().map(|message| ReceivedMessage {
        offset: u64::try_from(message.offset).unwrap_or(0),
        timestamp: message.timestamp,
        key: message.key,
        headers: message.headers,
        payload: message.payload,
    });

    let count = received.len();
    let mut error_count = 0u64;
    let mut filtered_count = 0u64;

    let decode_start = Instant::now();
    let mut decoded = Vec::with_capacity(count);
    for message in received {
        let Ok(payload) = decoder.decode(message.payload) else {
            error!(
                "Failed to decode message payload (offset: {}) for sink connector with ID: {plugin_id}",
                message.offset
            );
            error_count += 1;
            continue;
        };
        decoded.push(DecodedMessage {
            offset: Some(message.offset),
            timestamp: Some(message.timestamp),
            key: message.key,
            headers: message.headers,
            payload,
        });
    }
    let decode_elapsed = decode_start.elapsed();

    let mut messages = Vec::with_capacity(decoded.len());
    for message in decoded {
        let mut current_message = Some(message);
        for transform in transforms.iter() {
            let Some(message) = current_message.take() else {
                break;
            };
            match transform.transform(topic_metadata, message) {
                Ok(next) => current_message = next,
                Err(error) => {
                    error!(
                        "Transform '{:?}' failed for sink connector with ID: {plugin_id}, topic: {}: {error}",
                        transform.r#type(),
                        topic_metadata.topic
                    );
                    error_count += 1;
                    current_message = None;
                    break;
                }
            }
        }

        let Some(message) = current_message else {
            filtered_count += 1;
            continue;
        };

        let Some(offset) = message.offset else {
            error!(
                "Offset should be present. Failed to process message for sink connector with ID: {plugin_id}"
            );
            error_count += 1;
            continue;
        };

        let Some(timestamp) = message.timestamp else {
            error!(
                "Timestamp should be present. Failed to process message with offset: {offset} for sink connector with ID: {plugin_id}"
            );
            error_count += 1;
            continue;
        };

        let Ok(payload) = message.payload.try_into_vec() else {
            error!(
                "Failed to get message payload for message with offset: {offset} for sink connector with ID: {plugin_id}"
            );
            error_count += 1;
            continue;
        };

        let headers = match message.headers {
            Some(headers) => match postcard::to_allocvec(&headers) {
                Ok(bytes) => bytes,
                Err(error) => {
                    error!(
                        "Failed to serialize headers for message with offset: {offset} for sink connector with ID: {plugin_id}. {error}"
                    );
                    error_count += 1;
                    continue;
                }
            },
            None => vec![],
        };

        messages.push(RawMessage {
            offset,
            timestamp,
            key: message.key,
            headers,
            payload,
        });
    }

    metrics.inc_errors_by_with_labels(&labels.counter, error_count);
    if filtered_count > 0 {
        metrics.inc_messages_filtered_with_labels(&labels.counter, filtered_count);
    }

    let processed_count = messages.len();

    let topic_meta = postcard::to_allocvec(topic_metadata).map_err(|error| {
        error!(
            "Failed to serialize topic metadata for sink connector with ID: {plugin_id}. {error}"
        );
        RuntimeError::FailedToSerializeTopicMetadata
    })?;

    let messages_meta = postcard::to_allocvec(&messages_metadata).map_err(|error| {
        error!(
            "Failed to serialize messages metadata for sink connector with ID: {plugin_id}. {error}"
        );
        RuntimeError::FailedToSerializeMessagesMetadata
    })?;

    let messages = postcard::to_allocvec(&RawMessages {
        schema: decoder.schema(),
        messages,
    })
    .map_err(|error| {
        error!("Failed to serialize messages for sink connector with ID: {plugin_id}. {error}");
        RuntimeError::FailedToSerializeRawMessages
    })?;

    let ffi_start = Instant::now();
    let mut attempt = 0u32;
    loop {
        let status = if fault_injection::fail_consume_if_requested() {
            SINK_CONSUME_RETRYABLE_STATUS
        } else {
            (consume)(
                plugin_id,
                topic_meta.as_ptr(),
                topic_meta.len(),
                messages_meta.as_ptr(),
                messages_meta.len(),
                messages.as_ptr(),
                messages.len(),
            )
        };
        if status == 0 {
            break;
        }
        metrics.inc_errors_with_labels(&labels.counter);
        if status != SINK_CONSUME_RETRYABLE_STATUS || attempt >= MAX_CONSUME_ATTEMPTS {
            error!(
                "Sink connector with ID: {plugin_id} failed to consume {processed_count} messages from topic: {} (offset: {}) after {} attempt(s), status: {status}",
                topic_metadata.topic,
                messages_metadata.current_offset,
                attempt + 1
            );
            return Err(RuntimeError::SinkConsumeFailed { plugin_id, status });
        }
        let delay = exponential_backoff(CONSUME_RETRY_BASE_DELAY, attempt, CONSUME_RETRY_MAX_DELAY);
        warn!(
            "Sink connector with ID: {plugin_id} failed to consume {processed_count} messages from topic: {} (offset: {}), status: {status}. Retrying in {delay:?} (attempt {}/{MAX_CONSUME_ATTEMPTS})",
            topic_metadata.topic,
            messages_metadata.current_offset,
            attempt + 1
        );
        tokio::time::sleep(delay).await;
        attempt += 1;
    }
    let ffi_elapsed = ffi_start.elapsed();

    Ok(SinkBatchTiming {
        processed_count,
        decode_elapsed,
        ffi_elapsed,
    })
}

struct SinkBatchTiming {
    processed_count: usize,
    decode_elapsed: Duration,
    ffi_elapsed: Duration,
}

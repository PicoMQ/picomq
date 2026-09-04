use crate::configs::connectors::{TopicConsumerConfig, TopicProducerConfig};
use crate::configs::runtime::KafkaConfig;
use crate::error::RuntimeError;
use rdkafka::ClientConfig;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::producer::FutureProducer;
use std::collections::HashSet;
use std::time::Duration;
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

pub const DEFAULT_CONSUMER_GROUP_PREFIX: &str = "picomq-connect-sink-";
pub const DEFAULT_PARTITIONS: i32 = 1;
pub const DEFAULT_REPLICATION_FACTOR: i32 = 1;
const ADMIN_TIMEOUT: Duration = Duration::from_secs(10);
const PATTERN_METADATA_REFRESH: Duration = Duration::from_secs(2);
const DEFAULT_MESSAGE_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_SESSION_TIMEOUT: Duration = Duration::from_secs(10);

pub struct KafkaClients {
    base: ClientConfig,
    admin: AdminClient<DefaultClientContext>,
    ensured_topics: Mutex<HashSet<String>>,
}

impl KafkaClients {
    pub fn new(config: KafkaConfig) -> Result<Self, RuntimeError> {
        let base = config.client_config()?;
        let admin: AdminClient<DefaultClientContext> = base.create()?;
        info!(
            "Kafka client configured for bootstrap servers: {}",
            config.bootstrap_servers
        );
        Ok(Self {
            base,
            admin,
            ensured_topics: Mutex::new(HashSet::new()),
        })
    }

    pub fn consumer(
        &self,
        connector_key: &str,
        topic_config: &TopicConsumerConfig,
    ) -> Result<StreamConsumer, RuntimeError> {
        if topic_config.topics.is_empty() && topic_config.pattern.is_none() {
            return Err(RuntimeError::InvalidConfiguration(format!(
                "Sink '{connector_key}' must configure at least one topic or a pattern"
            )));
        }
        let default_group = format!("{DEFAULT_CONSUMER_GROUP_PREFIX}{connector_key}");
        let group = topic_config
            .consumer_group
            .as_deref()
            .filter(|group| !group.is_empty())
            .unwrap_or(&default_group);
        let mut client_config = self.base.clone();
        client_config
            .set("group.id", group)
            .set("enable.auto.commit", "false")
            .set("enable.auto.offset.store", "false")
            .set("enable.partition.eof", "false")
            .set(
                "session.timeout.ms",
                DEFAULT_SESSION_TIMEOUT.as_millis().to_string(),
            )
            .set(
                "auto.offset.reset",
                topic_config
                    .auto_offset_reset
                    .as_deref()
                    .unwrap_or("earliest"),
            );
        if topic_config.pattern.is_some() {
            client_config.set(
                "topic.metadata.refresh.interval.ms",
                PATTERN_METADATA_REFRESH.as_millis().to_string(),
            );
        }
        for (key, value) in &topic_config.properties {
            client_config.set(key, value);
        }
        let consumer: StreamConsumer = client_config.create()?;
        let mut subscriptions: Vec<String> = topic_config.topics.clone();
        if let Some(pattern) = &topic_config.pattern {
            let pattern = if pattern.starts_with('^') {
                pattern.clone()
            } else {
                format!("^{pattern}")
            };
            subscriptions.push(pattern);
        }
        let subscription_refs: Vec<&str> = subscriptions.iter().map(String::as_str).collect();
        consumer.subscribe(&subscription_refs)?;
        info!(
            "Sink '{connector_key}' subscribed to {:?} with consumer group '{group}'",
            subscriptions
        );
        Ok(consumer)
    }

    pub fn producer(
        &self,
        connector_key: &str,
        topic_config: &TopicProducerConfig,
        linger: Duration,
        batch_length: u32,
    ) -> Result<FutureProducer, RuntimeError> {
        validate_topology(connector_key, topic_config)?;
        let mut client_config = self.base.clone();
        client_config
            .set("linger.ms", linger.as_millis().to_string())
            .set("batch.num.messages", batch_length.to_string())
            .set("enable.idempotence", "false")
            .set("acks", "all")
            .set(
                "message.timeout.ms",
                DEFAULT_MESSAGE_TIMEOUT.as_millis().to_string(),
            );
        for (key, value) in &topic_config.properties {
            client_config.set(key, value);
        }
        let producer: FutureProducer = client_config.create()?;
        debug!(
            "Source '{connector_key}' producer created with linger: {:?}, batch_length: {batch_length}",
            linger
        );
        Ok(producer)
    }

    pub async fn ensure_topic(
        &self,
        topic: &str,
        topic_config: &TopicProducerConfig,
    ) -> Result<(), RuntimeError> {
        if !topic_config.create_topics {
            return Ok(());
        }
        {
            let ensured = self.ensured_topics.lock().await;
            if ensured.contains(topic) {
                return Ok(());
            }
        }
        let partitions = topic_config.partitions.unwrap_or(DEFAULT_PARTITIONS);
        let replication = topic_config
            .replication_factor
            .unwrap_or(DEFAULT_REPLICATION_FACTOR);
        let new_topic = NewTopic::new(topic, partitions, TopicReplication::Fixed(replication));
        let options = AdminOptions::new().request_timeout(Some(ADMIN_TIMEOUT));
        let results = self.admin.create_topics([&new_topic], &options).await?;
        for result in results {
            match result {
                Ok(name) => info!("Created topic '{name}' with {partitions} partition(s)"),
                Err((name, RDKafkaErrorCode::TopicAlreadyExists)) => {
                    debug!("Topic '{name}' already exists");
                }
                Err((name, code)) => {
                    warn!("Failed to create topic '{name}': {code}");
                    return Err(RuntimeError::Kafka(KafkaError::AdminOp(code)));
                }
            }
        }
        self.ensured_topics.lock().await.insert(topic.to_owned());
        Ok(())
    }
}

fn validate_topology(
    connector_key: &str,
    topic_config: &TopicProducerConfig,
) -> Result<(), RuntimeError> {
    if let Some(partitions) = topic_config.partitions
        && partitions != DEFAULT_PARTITIONS
    {
        return Err(RuntimeError::InvalidConfiguration(format!(
            "Source '{connector_key}' requests {partitions} partitions for topic '{}', PicoMQ topics have exactly {DEFAULT_PARTITIONS}",
            topic_config.topic
        )));
    }
    if let Some(replication_factor) = topic_config.replication_factor
        && replication_factor != DEFAULT_REPLICATION_FACTOR
    {
        return Err(RuntimeError::InvalidConfiguration(format!(
            "Source '{connector_key}' requests replication factor {replication_factor} for topic '{}', PicoMQ topics have exactly {DEFAULT_REPLICATION_FACTOR}",
            topic_config.topic
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_default_topology_should_validate() {
        let config = TopicProducerConfig::default();
        assert!(validate_topology("orders", &config).is_ok());
    }

    #[test]
    fn given_multiple_partitions_should_reject() {
        let config = TopicProducerConfig {
            partitions: Some(3),
            ..Default::default()
        };
        assert!(validate_topology("orders", &config).is_err());
    }

    #[test]
    fn given_replication_factor_above_one_should_reject() {
        let config = TopicProducerConfig {
            replication_factor: Some(3),
            ..Default::default()
        };
        assert!(validate_topology("orders", &config).is_err());
    }
}

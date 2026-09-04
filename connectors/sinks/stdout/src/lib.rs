use async_trait::async_trait;
use picomq_connector_sdk::{
    ConsumedMessage, Error, MessagesMetadata, Sink, TopicMetadata, sink_connector,
};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use tracing::info;

sink_connector!(StdoutSink);

#[derive(Debug)]
struct State {
    invocations_count: usize,
}

#[derive(Debug)]
pub struct StdoutSink {
    id: u32,
    print_payload: bool,
    state: Mutex<State>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct StdoutSinkConfig {
    print_payload: Option<bool>,
}

impl StdoutSink {
    pub fn new(id: u32, config: StdoutSinkConfig) -> Self {
        StdoutSink {
            id,
            print_payload: config.print_payload.unwrap_or(false),
            state: Mutex::new(State {
                invocations_count: 0,
            }),
        }
    }
}

#[async_trait]
impl Sink for StdoutSink {
    async fn open(&mut self) -> Result<(), Error> {
        info!(
            "Opened stdout sink connector with ID: {}, print payload: {}",
            self.id, self.print_payload
        );
        Ok(())
    }

    async fn consume(
        &self,
        topic_metadata: &TopicMetadata,
        messages_metadata: MessagesMetadata,
        messages: Vec<ConsumedMessage>,
    ) -> Result<(), Error> {
        let mut state = self.state.lock().await;
        state.invocations_count += 1;
        let invocation = state.invocations_count;
        drop(state);

        info!(
            "Stdout sink with ID: {} received: {} messages, schema: {}, topic: {}, partition: {}, offset: {}, invocation: {}",
            self.id,
            messages.len(),
            messages_metadata.schema,
            topic_metadata.topic,
            messages_metadata.partition,
            messages_metadata.current_offset,
            invocation
        );
        if self.print_payload {
            for message in messages {
                info!(
                    "Message offset: {}, key: {:?}, payload: {:#?}",
                    message.offset,
                    message
                        .key
                        .as_deref()
                        .map(|key| String::from_utf8_lossy(key).into_owned()),
                    message.payload
                );
            }
        }
        Ok(())
    }

    async fn close(&mut self) -> Result<(), Error> {
        info!("Stdout sink connector with ID: {} is closed.", self.id);
        Ok(())
    }
}

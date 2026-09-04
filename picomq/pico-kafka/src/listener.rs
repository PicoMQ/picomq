//! TCP listener and per-connection pipeline. Produce requests process

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;

use bytes::Bytes;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};
use tracing::{debug, warn};

use crate::KafkaError;
use crate::broker::BrokerContext;
use crate::dispatch::dispatch;
use crate::frame::{read_frame, write_frame};
use crate::handlers::{HandlerError, HandlerOutcome};

#[derive(Debug, Clone)]
pub struct ListenerConfig {
    pub addr: SocketAddr,
    pub max_request_bytes: usize,
    pub max_in_flight: usize,
}

impl Default for ListenerConfig {
    fn default() -> Self {
        Self {
            addr: SocketAddr::from(([127, 0, 0, 1], 9092)),
            max_request_bytes: 4 * 1024 * 1024,
            max_in_flight: 16,
        }
    }
}

enum Reply {
    Frame(Bytes),
    Skip,
    Close,
}

struct Slot {
    reply: Reply,
    _permit: OwnedSemaphorePermit,
}

const PRODUCE_API_KEY: i16 = 0;

fn frame_api_key(body: &[u8]) -> Option<i16> {
    Some(i16::from_be_bytes([*body.first()?, *body.get(1)?]))
}

async fn to_reply(mut result: Result<HandlerOutcome, HandlerError>) -> Reply {
    loop {
        return match result {
            Ok(HandlerOutcome::Deferred(deferred)) => {
                result = deferred.await;
                continue;
            }
            Ok(HandlerOutcome::Response(frame)) => Reply::Frame(frame.0),
            Ok(HandlerOutcome::NoResponse) => Reply::Skip,
            Err(HandlerError::Unimplemented(api_key)) => {
                warn!(api_key, "closing connection: unimplemented kafka api");
                Reply::Close
            }
            Err(HandlerError::Protocol(message)) => {
                warn!(%message, "closing connection: invalid kafka request");
                Reply::Close
            }
            Err(HandlerError::Service(error)) => {
                warn!(?error, "closing connection: kafka handler service error");
                Reply::Close
            }
        };
    }
}

pub struct KafkaListener {
    config: ListenerConfig,
    broker: Arc<BrokerContext>,
}

impl KafkaListener {
    pub fn new(config: ListenerConfig, broker: Arc<BrokerContext>) -> Self {
        Self { config, broker }
    }

    pub fn config(&self) -> &ListenerConfig {
        &self.config
    }

    pub async fn bind(&self) -> Result<TcpListener, KafkaError> {
        Ok(TcpListener::bind(self.config.addr).await?)
    }

    pub async fn serve(self, listener: TcpListener) -> Result<(), KafkaError> {
        loop {
            let (socket, peer) = listener.accept().await?;
            debug!(%peer, "kafka client connected");
            let config = self.config.clone();
            let broker = self.broker.clone();
            tokio::spawn(async move {
                if let Err(error) = serve_connection(socket, broker, config).await {
                    debug!(%peer, %error, "kafka client disconnected");
                }
            });
        }
    }
}

async fn serve_connection(
    socket: TcpStream,
    broker: Arc<BrokerContext>,
    config: ListenerConfig,
) -> Result<(), KafkaError> {
    let (read_half, write_half) = socket.into_split();
    let (response_tx, response_rx) = mpsc::channel(config.max_in_flight.max(1));
    let in_flight = Arc::new(Semaphore::new(config.max_in_flight.max(1)));

    let mut writer = tokio::spawn(run_ordered_writer(write_half, response_rx));

    tokio::select! {
        result = run_reader(read_half, response_tx, broker, config, in_flight) => {
            // EOF or read error: let in-flight responses drain, then stop.
            let _ = writer.await;
            result
        }
        _ = &mut writer => {
            // Writer closed the connection (write error or Close marker).
            Ok(())
        }
    }
}

async fn run_reader(
    mut read_half: tokio::net::tcp::OwnedReadHalf,
    response_tx: mpsc::Sender<(u64, Slot)>,
    broker: Arc<BrokerContext>,
    config: ListenerConfig,
    in_flight: Arc<Semaphore>,
) -> Result<(), KafkaError> {
    let (produce_tx, produce_rx) = mpsc::channel(config.max_in_flight.max(1));
    tokio::spawn(run_produce_worker(
        produce_rx,
        response_tx.clone(),
        broker.clone(),
    ));

    let mut sequence = 0u64;
    loop {
        let permit = in_flight
            .clone()
            .acquire_owned()
            .await
            .expect("connection semaphore never closes");
        let frame = read_frame(&mut read_half, config.max_request_bytes).await?;
        let body = frame.freeze();
        let seq = sequence;
        sequence += 1;
        if frame_api_key(&body) == Some(PRODUCE_API_KEY) {
            if produce_tx.send((seq, body, permit)).await.is_err() {
                return Ok(());
            }
            continue;
        }
        let tx = response_tx.clone();
        let broker = broker.clone();
        tokio::spawn(async move {
            let reply = to_reply(dispatch(&broker, &body).await).await;
            let _ = tx
                .send((
                    seq,
                    Slot {
                        reply,
                        _permit: permit,
                    },
                ))
                .await;
        });
    }
}

async fn run_produce_worker(
    mut produce_rx: mpsc::Receiver<(u64, Bytes, OwnedSemaphorePermit)>,
    response_tx: mpsc::Sender<(u64, Slot)>,
    broker: Arc<BrokerContext>,
) {
    while let Some((seq, body, permit)) = produce_rx.recv().await {
        match dispatch(&broker, &body).await {
            Ok(HandlerOutcome::Deferred(deferred)) => {
                let tx = response_tx.clone();
                tokio::spawn(async move {
                    let reply = to_reply(deferred.await).await;
                    let _ = tx
                        .send((
                            seq,
                            Slot {
                                reply,
                                _permit: permit,
                            },
                        ))
                        .await;
                });
            }
            outcome => {
                let reply = to_reply(outcome).await;
                if response_tx
                    .send((
                        seq,
                        Slot {
                            reply,
                            _permit: permit,
                        },
                    ))
                    .await
                    .is_err()
                {
                    return;
                }
            }
        }
    }
}

async fn run_ordered_writer(
    mut write_half: tokio::net::tcp::OwnedWriteHalf,
    mut response_rx: mpsc::Receiver<(u64, Slot)>,
) {
    let mut next_sequence = 0u64;
    let mut pending = BTreeMap::new();
    while let Some((sequence, slot)) = response_rx.recv().await {
        pending.insert(sequence, slot);
        while let Some(slot) = pending.remove(&next_sequence) {
            match slot.reply {
                Reply::Frame(frame) => {
                    if write_frame(&mut write_half, &frame).await.is_err() {
                        return;
                    }
                }
                Reply::Skip => {}
                Reply::Close => return,
            }
            next_sequence += 1;
        }
    }
}

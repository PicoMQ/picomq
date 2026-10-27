//! TCP listener and per-connection pipeline. Produce requests process

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use kafka_protocol::messages::ApiKey;
use picomq_auth::AuthPrincipal;
use picomq_listener::{Listener, Reader, Stream, Writer};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};
use tracing::{debug, warn};

use crate::KafkaError;
use crate::broker::BrokerContext;
use crate::dispatch::{dispatch, request_context};
use crate::frame::{read_frame, write_frame};
use crate::handlers::{HandlerError, HandlerOutcome};
use crate::sasl::{self, Answer, Session};

#[derive(Debug, Clone)]
pub struct ListenerConfig {
    pub max_request_bytes: usize,
    pub max_in_flight: usize,
    pub max_frame_before_auth: usize,
    pub idle_before_auth: Duration,
}

impl Default for ListenerConfig {
    fn default() -> Self {
        Self {
            max_request_bytes: 4 * 1024 * 1024,
            max_in_flight: 16,
            max_frame_before_auth: 512 * 1024,
            idle_before_auth: Duration::from_secs(600),
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

    pub async fn serve(self, mut listener: Listener) {
        while let Some((socket, peer)) = listener.accept().await {
            debug!(%peer, "kafka client connected");
            let config = self.config.clone();
            let broker = self.broker.clone();
            tokio::spawn(async move {
                let client_host: Arc<str> = Arc::from(peer.ip().to_string());
                if let Err(error) = serve_connection(socket, client_host, broker, config).await {
                    debug!(%peer, %error, "kafka client disconnected");
                }
            });
        }
    }
}

async fn serve_connection(
    socket: Stream,
    client_host: Arc<str>,
    broker: Arc<BrokerContext>,
    config: ListenerConfig,
) -> Result<(), KafkaError> {
    let (read_half, write_half) = socket.into_split();
    let (response_tx, response_rx) = mpsc::channel(config.max_in_flight.max(1));
    let in_flight = Arc::new(Semaphore::new(config.max_in_flight.max(1)));

    let mut writer = tokio::spawn(run_ordered_writer(write_half, response_rx));

    tokio::select! {
        result = run_reader(read_half, response_tx, client_host, broker, config, in_flight) => {
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
    mut read_half: Reader,
    response_tx: mpsc::Sender<(u64, Slot)>,
    client_host: Arc<str>,
    broker: Arc<BrokerContext>,
    config: ListenerConfig,
    in_flight: Arc<Semaphore>,
) -> Result<(), KafkaError> {
    let (produce_tx, produce_rx) = mpsc::channel(config.max_in_flight.max(1));
    tokio::spawn(run_produce_worker(
        produce_rx,
        response_tx.clone(),
        client_host.clone(),
        broker.clone(),
    ));

    let mut session = broker.authorizer.clone().map(Session::new);
    let mut sequence = 0u64;
    loop {
        let permit = in_flight
            .clone()
            .acquire_owned()
            .await
            .expect("connection semaphore never closes");
        let frame = match &session {
            Some(session) if session.principal().is_none() => {
                let limit = config.max_frame_before_auth.min(config.max_request_bytes);
                let read = read_frame(&mut read_half, limit);
                match tokio::time::timeout(config.idle_before_auth, read).await {
                    Ok(frame) => frame?,
                    Err(_) => return Ok(()),
                }
            }
            _ => read_frame(&mut read_half, config.max_request_bytes).await?,
        };
        let body = frame.freeze();
        let seq = sequence;
        sequence += 1;

        let api_key = frame_api_key(&body);
        if let Some(session) = &mut session {
            let answer = if session.awaits_raw_token() {
                Some(session.authenticate_raw(&body).await)
            } else if api_key.is_some_and(sasl::is_sasl) {
                let Ok((req, rest)) = request_context(&body, None, &client_host) else {
                    send(&response_tx, seq, permit, Reply::Close).await;
                    return Ok(());
                };
                let answered = if req.api_key == ApiKey::SaslHandshake as i16 {
                    session.handshake(&req, &rest)
                } else {
                    session.authenticate(&req, &rest).await
                };
                match answered {
                    Ok(answer) => Some(answer),
                    Err(_) => {
                        send(&response_tx, seq, permit, Reply::Close).await;
                        return Ok(());
                    }
                }
            } else if session.principal().is_none() && api_key != Some(ApiKey::ApiVersions as i16) {
                send(&response_tx, seq, permit, Reply::Close).await;
                return Ok(());
            } else {
                None
            };
            if let Some(Answer { frame, close }) = answer {
                let reply = frame.map_or(Reply::Skip, Reply::Frame);
                send(&response_tx, seq, permit, reply).await;
                if close {
                    return Ok(());
                }
                continue;
            }
        } else if api_key.is_some_and(sasl::is_sasl) {
            let reply = match request_context(&body, None, &client_host) {
                Ok((req, _)) => Reply::Frame(sasl::illegal_state(&req)),
                Err(_) => Reply::Close,
            };
            send(&response_tx, seq, permit, reply).await;
            continue;
        }

        let principal = match &mut session {
            Some(session) if session.principal().is_some() => match session.refresh().await {
                Ok(principal) => Some(principal),
                Err(error) => {
                    debug!(%error, "closing kafka connection, credential no longer valid");
                    send(&response_tx, seq, permit, Reply::Close).await;
                    return Ok(());
                }
            },
            _ => None,
        };
        if api_key == Some(PRODUCE_API_KEY) {
            if produce_tx
                .send((seq, body, principal, permit))
                .await
                .is_err()
            {
                return Ok(());
            }
            continue;
        }
        let tx = response_tx.clone();
        let broker = broker.clone();
        let client_host = client_host.clone();
        tokio::spawn(async move {
            let reply = to_reply(dispatch(&broker, principal, &client_host, &body).await).await;
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

async fn send(
    response_tx: &mpsc::Sender<(u64, Slot)>,
    seq: u64,
    permit: OwnedSemaphorePermit,
    reply: Reply,
) {
    let slot = Slot {
        reply,
        _permit: permit,
    };
    let _ = response_tx.send((seq, slot)).await;
}

async fn run_produce_worker(
    mut produce_rx: mpsc::Receiver<(u64, Bytes, Option<AuthPrincipal>, OwnedSemaphorePermit)>,
    response_tx: mpsc::Sender<(u64, Slot)>,
    client_host: Arc<str>,
    broker: Arc<BrokerContext>,
) {
    while let Some((seq, body, principal, permit)) = produce_rx.recv().await {
        match dispatch(&broker, principal, &client_host, &body).await {
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

async fn run_ordered_writer(mut write_half: Writer, mut response_rx: mpsc::Receiver<(u64, Slot)>) {
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

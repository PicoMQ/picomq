use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use bytes::Bytes;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot};

use crate::error::{ClientError, ErrorKind, Result};
use crate::pico::{PicoClient, ProducerRef};
use crate::retry::RetryPolicy;

#[derive(Debug, Clone)]
pub struct ProducerConfig {
    pub epoch: u64,
    pub linger: Duration,
    pub max_batch_records: usize,
    pub max_batch_bytes: usize,
    // >1 usually hurts throughput: the server rejects out-of-order sequences,
    // so pipelined batches spend their time being retried.
    pub max_inflight: usize,
    pub max_buffered_bytes: usize,
    pub retry: RetryPolicy,
}

impl Default for ProducerConfig {
    fn default() -> Self {
        Self {
            epoch: 0,
            linger: Duration::from_millis(5),
            max_batch_records: 500,
            max_batch_bytes: 1024 * 1024,
            max_inflight: 1,
            max_buffered_bytes: 32 * 1024 * 1024,
            retry: RetryPolicy {
                max_attempts: 12,
                initial_backoff: Duration::from_millis(1),
                max_backoff: Duration::from_millis(100),
                multiplier: 2.0,
            },
        }
    }
}

#[derive(Debug)]
pub struct Pending {
    rx: oneshot::Receiver<Result<u64>>,
}

impl Pending {
    pub async fn durable(self) -> Result<u64> {
        self.rx
            .await
            .unwrap_or_else(|_| Err(stopped("producer stopped before the record was durable")))
    }
}

struct Item {
    body: Bytes,
    ack: oneshot::Sender<Result<u64>>,
    permit: OwnedSemaphorePermit,
}

pub struct Producer {
    tx: mpsc::Sender<Item>,
    budget: Arc<Semaphore>,
    poisoned: Arc<AtomicBool>,
    max_buffered_bytes: usize,
}

impl Producer {
    pub fn new(client: Arc<PicoClient>, name: &str, id: &str, config: ProducerConfig) -> Self {
        let budget = Arc::new(Semaphore::new(config.max_buffered_bytes));
        let poisoned = Arc::new(AtomicBool::new(false));
        let (tx, rx) = mpsc::channel(config.max_inflight.max(1));
        tokio::spawn(run(
            client,
            name.to_owned(),
            id.to_owned(),
            config.clone(),
            rx,
            Arc::clone(&poisoned),
        ));
        Self {
            tx,
            budget,
            poisoned,
            max_buffered_bytes: config.max_buffered_bytes,
        }
    }

    pub async fn send(&self, record: Bytes) -> Result<Pending> {
        if record.len() > self.max_buffered_bytes {
            return Err(
                ClientError::new(0, ErrorKind::BadRequest, "record_too_large").with_message(Some(
                    format!(
                        "record of {} bytes exceeds the session's {} byte budget",
                        record.len(),
                        self.max_buffered_bytes
                    ),
                )),
            );
        }
        self.check_poisoned()?;
        let permit = Arc::clone(&self.budget)
            .acquire_many_owned(record.len().max(1) as u32)
            .await
            .map_err(|_| stopped("producer is closed"))?;
        let (ack, rx) = oneshot::channel();
        self.tx
            .send(Item {
                body: record,
                ack,
                permit,
            })
            .await
            .map_err(|_| stopped("producer is closed"))?;
        Ok(Pending { rx })
    }

    pub async fn send_durable(&self, record: Bytes) -> Result<u64> {
        self.send(record).await?.durable().await
    }

    pub async fn flush(&self) -> Result<()> {
        let _all = Arc::clone(&self.budget)
            .acquire_many_owned(self.max_buffered_bytes as u32)
            .await
            .map_err(|_| stopped("producer is closed"))?;
        self.check_poisoned()
    }

    pub async fn close(self) -> Result<()> {
        let result = self.flush().await;
        drop(self.tx);
        result
    }

    fn check_poisoned(&self) -> Result<()> {
        if self.poisoned.load(Ordering::Acquire) {
            return Err(
                ClientError::new(0, ErrorKind::Conflict, "producer_poisoned").with_message(Some(
                    "producer session failed and cannot continue its sequence; open a new \
                     session (a higher epoch restarts at sequence 0)"
                        .to_owned(),
                )),
            );
        }
        Ok(())
    }
}

async fn run(
    client: Arc<PicoClient>,
    name: String,
    id: String,
    config: ProducerConfig,
    mut rx: mpsc::Receiver<Item>,
    poisoned: Arc<AtomicBool>,
) {
    let inflight = Arc::new(Semaphore::new(config.max_inflight.max(1)));
    let mut seq = 0u64;
    while let Some(first) = rx.recv().await {
        let batch = collect(&mut rx, first, &config).await;
        let this_seq = seq;
        seq += 1;
        let Ok(permit) = Arc::clone(&inflight).acquire_owned().await else {
            return;
        };
        tokio::spawn(send_batch(
            Arc::clone(&client),
            name.clone(),
            id.clone(),
            config.clone(),
            batch,
            this_seq,
            Arc::clone(&poisoned),
            permit,
        ));
    }
}

async fn collect(rx: &mut mpsc::Receiver<Item>, first: Item, config: &ProducerConfig) -> Vec<Item> {
    let mut bytes = first.body.len();
    let mut batch = vec![first];
    if config.linger.is_zero() {
        while batch.len() < config.max_batch_records && bytes < config.max_batch_bytes {
            match rx.try_recv() {
                Ok(item) => {
                    bytes += item.body.len();
                    batch.push(item);
                }
                Err(_) => break,
            }
        }
        return batch;
    }
    let deadline = tokio::time::Instant::now() + config.linger;
    while batch.len() < config.max_batch_records && bytes < config.max_batch_bytes {
        match tokio::time::timeout_at(deadline, rx.recv()).await {
            Ok(Some(item)) => {
                bytes += item.body.len();
                batch.push(item);
            }
            Ok(None) | Err(_) => break,
        }
    }
    batch
}

#[allow(clippy::too_many_arguments)]
async fn send_batch(
    client: Arc<PicoClient>,
    name: String,
    id: String,
    config: ProducerConfig,
    batch: Vec<Item>,
    seq: u64,
    poisoned: Arc<AtomicBool>,
    _permit: OwnedSemaphorePermit,
) {
    let records: Vec<Bytes> = batch.iter().map(|item| item.body.clone()).collect();
    let producer = ProducerRef {
        id: &id,
        epoch: config.epoch,
        seq,
    };
    let result = append_with_retries(&client, &name, &records, &producer, &config).await;

    match result {
        Ok(start) => {
            for (i, item) in batch.into_iter().enumerate() {
                let _ = item.ack.send(Ok(start + i as u64));
                drop(item.permit);
            }
        }
        Err(e) => {
            // A failed sequence is a permanent hole; later batches can never
            // land, so the whole session fails instead of hanging.
            poisoned.store(true, Ordering::Release);
            for item in batch {
                let _ = item.ack.send(Err(e.clone()));
                drop(item.permit);
            }
        }
    }
}

async fn append_with_retries(
    client: &PicoClient,
    name: &str,
    records: &[Bytes],
    producer: &ProducerRef<'_>,
    config: &ProducerConfig,
) -> Result<u64> {
    let mut attempt = 0;
    loop {
        match client.append_as(name, records, producer).await {
            Ok(ack) => {
                if ack.duplicate {
                    let next: u64 = ack.ack.next.parse().unwrap_or_default();
                    return Ok(next.saturating_sub(records.len() as u64));
                }
                return ack
                    .ack
                    .start
                    .parse()
                    .map_err(|_| stopped("server returned a non-numeric start"));
            }
            Err(e) => {
                let out_of_order = e.code == picomq_protocol::pico::E_SEQUENCE_GAP;
                match config.retry.delay(attempt) {
                    Some(delay) if out_of_order || e.retryable() => tokio::time::sleep(delay).await,
                    _ => return Err(e),
                }
                attempt += 1;
            }
        }
    }
}

fn stopped(message: &str) -> ClientError {
    ClientError::new(0, ErrorKind::Other, "producer_stopped").with_message(Some(message.to_owned()))
}

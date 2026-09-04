use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use hdrhistogram::Histogram;
use hdrhistogram::sync::{Recorder, SyncHistogram};
use picomq_client::producer::{Pending, Producer, ProducerConfig};
use picomq_client::{ClientError, DsClient, Live, PicoClient, Protocol, ReadLimits, StreamApi};

use super::{BenchArgs, Counters, Schedule, emit, report_progress};
use crate::io::note;
use crate::stream::Target;

const READ_BYTES: u64 = 4 * 1024 * 1024;
const READ_IDLE_BACKOFF: Duration = Duration::from_millis(10);

enum Client {
    Pico(PicoClient),
    Ds(DsClient),
}

impl Client {
    fn open(protocol: Protocol, endpoint: &Target) -> Result<Self, ClientError> {
        let http = picomq_client::http_client(&endpoint.client_config())?;
        let url = &endpoint.endpoint;
        Ok(match protocol {
            Protocol::Pico => Self::Pico(PicoClient::with_http(url, http, Default::default())),
            Protocol::Ds => Self::Ds(DsClient::with_http(url, http, Default::default())),
        })
    }

    fn api(&self) -> &dyn StreamApi {
        match self {
            Self::Pico(client) => client,
            Self::Ds(client) => client,
        }
    }
}

struct Payload {
    records: Vec<Bytes>,
    content_type: &'static str,
    record_count: u64,
    record_bytes: u64,
}

impl Payload {
    fn build(protocol: Protocol, record_size: usize, batch: usize) -> Self {
        let record_size = record_size.max(1);
        let batch = batch.max(1);
        match protocol {
            Protocol::Pico => Self {
                records: vec![Bytes::from(vec![0u8; record_size]); batch],
                content_type: "application/octet-stream",
                record_count: batch as u64,
                record_bytes: record_size as u64,
            },
            Protocol::Ds if batch > 1 => Self {
                records: vec![json_batch(record_size, batch)],
                content_type: "application/json",
                record_count: batch as u64,
                record_bytes: record_size as u64,
            },
            Protocol::Ds => Self {
                records: vec![Bytes::from(vec![b'x'; record_size])],
                content_type: "application/octet-stream",
                record_count: 1,
                record_bytes: record_size as u64,
            },
        }
    }
}

fn json_batch(record_size: usize, batch: usize) -> Bytes {
    let message = "x".repeat(record_size.saturating_sub(2).max(1));
    let mut out = String::with_capacity(batch * (record_size + 1) + 2);
    out.push('[');
    for i in 0..batch {
        if i > 0 {
            out.push(',');
        }
        out.push('"');
        out.push_str(&message);
        out.push('"');
    }
    out.push(']');
    Bytes::from(out.into_bytes())
}

pub(super) async fn run(endpoint: &Target, args: BenchArgs) -> Result<i32, ClientError> {
    let protocol = endpoint.protocol.client_protocol()?;
    let payload = Arc::new(Payload::build(protocol, args.record_size, args.batch));
    let streams = args.streams.max(1);
    let workers = args.in_flight.max(1);
    let clients: Vec<Arc<Client>> = (0..args.connections.max(1))
        .map(|_| Client::open(protocol, endpoint).map(Arc::new))
        .collect::<Result<_, _>>()?;

    let run_id = format!(
        "{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis())
            .unwrap_or_default()
    );
    let names: Vec<String> = (0..streams)
        .map(|i| format!("/bench/{run_id}-{i}"))
        .collect();
    for name in &names {
        clients[0]
            .api()
            .create(name, payload.content_type, None)
            .await?;
    }
    note(format!(
        "created {} stream{} for {} bench",
        names.len(),
        if names.len() == 1 { "" } else { "s" },
        protocol.as_str()
    ));

    let write = Arc::new(Counters::default());
    let read = Arc::new(Counters::default());
    let stop = Arc::new(AtomicBool::new(false));
    let sent_bytes = Arc::new(AtomicU64::new(0));
    let failure: Arc<std::sync::Mutex<Option<ClientError>>> = Arc::new(Default::default());
    let mut latency: SyncHistogram<u64> = Histogram::<u64>::new_with_bounds(1, 60_000_000, 3)
        .expect("histogram bounds")
        .into();

    let readers: Vec<_> = if args.no_read {
        Vec::new()
    } else {
        names
            .iter()
            .enumerate()
            .map(|(i, name)| {
                tokio::spawn(reader(
                    clients[i % clients.len()].clone(),
                    name.clone(),
                    read.clone(),
                    stop.clone(),
                ))
            })
            .collect()
    };

    let started = Instant::now();
    let deadline = started + Duration::from_secs(args.duration.max(1) + args.warmup);
    let target_bytes_per_sec = if args.target_mibps > 0.0 {
        args.target_mibps * 1024.0 * 1024.0
    } else {
        0.0
    };
    let schedule = Schedule {
        started,
        deadline,
        target_bytes_per_sec,
    };
    let writers: Vec<_> = if args.producer {
        names
            .iter()
            .enumerate()
            .map(|(i, name)| {
                tokio::spawn(producer_writer(
                    clients[i % clients.len()].clone(),
                    name.clone(),
                    payload.clone(),
                    latency.recorder(),
                    write.clone(),
                    failure.clone(),
                    schedule,
                    args.batch.max(1),
                    workers / names.len().max(1),
                ))
            })
            .collect()
    } else {
        (0..workers)
            .map(|i| {
                tokio::spawn(writer(
                    clients[i % clients.len()].clone(),
                    names[i % names.len()].clone(),
                    payload.clone(),
                    latency.recorder(),
                    write.clone(),
                    sent_bytes.clone(),
                    failure.clone(),
                    schedule,
                ))
            })
            .collect()
    };

    if args.warmup > 0 {
        tokio::time::sleep(Duration::from_secs(args.warmup)).await;
        write.reset();
        read.reset();
        latency.refresh_timeout(Duration::from_millis(200));
        latency.reset();
        note(format!("warmup of {}s discarded", args.warmup));
    }

    let measured_from = Instant::now();
    report_progress(&mut latency, &write, &read, measured_from, deadline, &args).await;

    for writer in writers {
        let _ = writer.await;
    }
    stop.store(true, Ordering::Relaxed);
    for reader in readers {
        let _ = reader.await;
    }

    let elapsed = measured_from.elapsed();
    latency.refresh_timeout(Duration::from_millis(500));
    let failed = failure.lock().unwrap().take();
    if let Some(error) = &failed {
        note(format!("append failed: {error}"));
    }
    emit(&args, protocol.as_str(), &latency, &write, &read, elapsed);

    if !args.keep {
        for name in &names {
            clients[0].api().delete(name).await?;
        }
    }

    Ok(if failed.is_some() { 1 } else { 0 })
}

#[allow(clippy::too_many_arguments)]
async fn writer(
    client: Arc<Client>,
    name: String,
    payload: Arc<Payload>,
    mut latency: Recorder<u64>,
    write: Arc<Counters>,
    sent_bytes: Arc<AtomicU64>,
    failure: Arc<std::sync::Mutex<Option<ClientError>>>,
    schedule: Schedule,
) {
    let request_bytes: u64 = payload.records.iter().map(|r| r.len() as u64).sum();

    while Instant::now() < schedule.deadline {
        if failure.lock().unwrap().is_some() {
            return;
        }

        let sent = sent_bytes.fetch_add(request_bytes, Ordering::Relaxed) + request_bytes;
        if schedule.target_bytes_per_sec > 0.0 {
            let expected = Duration::from_secs_f64(sent as f64 / schedule.target_bytes_per_sec);
            let elapsed = schedule.started.elapsed();
            if expected > elapsed {
                tokio::time::sleep(expected - elapsed).await;
            }
        }

        let start = Instant::now();
        let result = match client.as_ref() {
            Client::Pico(client) => {
                client
                    .append(&name, &payload.records, payload.content_type)
                    .await
            }
            Client::Ds(client) => {
                client
                    .append(&name, &payload.records, payload.content_type)
                    .await
            }
        };
        match result {
            Ok(_) => {
                let micros = start.elapsed().as_micros().max(1) as u64;
                let _ = latency.record(micros);
                write.add(
                    payload.record_bytes * payload.record_count,
                    payload.record_count,
                );
            }
            Err(error) => {
                let mut slot = failure.lock().unwrap();
                if slot.is_none() {
                    *slot = Some(error);
                }
                return;
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn producer_writer(
    client: Arc<Client>,
    name: String,
    payload: Arc<Payload>,
    mut latency: Recorder<u64>,
    write: Arc<Counters>,
    failure: Arc<std::sync::Mutex<Option<ClientError>>>,
    schedule: Schedule,
    batch: usize,
    in_flight: usize,
) {
    let Client::Pico(pico) = client.as_ref() else {
        let mut slot = failure.lock().unwrap();
        if slot.is_none() {
            *slot = Some(ClientError::unsupported(
                "--producer needs producer sequences, which only the Pico protocol has",
            ));
        }
        return;
    };
    let producer = Producer::new(
        Arc::new(pico.clone()),
        &name,
        &format!("bench-{name}"),
        ProducerConfig {
            max_batch_records: batch,
            ..Default::default()
        },
    );

    let (done_tx, mut done_rx) = tokio::sync::mpsc::channel(in_flight.max(1) * batch.max(1));
    let record_bytes = payload.record_bytes;
    let collector = tokio::spawn(async move {
        while let Some((sent, pending)) = done_rx.recv().await {
            match Pending::durable(pending).await {
                Ok(_) => {
                    let micros = Instant::now().duration_since(sent).as_micros().max(1) as u64;
                    let _ = latency.record(micros);
                    write.add(record_bytes, 1);
                }
                Err(error) => return Some(error),
            }
        }
        None
    });

    let body = payload.records[0].clone();
    while Instant::now() < schedule.deadline {
        if failure.lock().unwrap().is_some() {
            break;
        }
        let sent = Instant::now();
        match producer.send(body.clone()).await {
            Ok(pending) => {
                if done_tx.send((sent, pending)).await.is_err() {
                    break;
                }
            }
            Err(error) => {
                let mut slot = failure.lock().unwrap();
                if slot.is_none() {
                    *slot = Some(error);
                }
                break;
            }
        }
    }
    drop(done_tx);
    if let Ok(Some(error)) = collector.await {
        let mut slot = failure.lock().unwrap();
        if slot.is_none() {
            *slot = Some(error);
        }
    }
    let _ = producer.close().await;
}

async fn reader(client: Arc<Client>, name: String, read: Arc<Counters>, stop: Arc<AtomicBool>) {
    let api = client.api();
    let mut next = api.beginning();

    while !stop.load(Ordering::Relaxed) {
        match api
            .read(&name, &next, Live::Off, ReadLimits::bytes(READ_BYTES))
            .await
        {
            Ok(page) => {
                let bytes: u64 = page.records.iter().map(|r| r.body.len() as u64).sum();
                read.add(bytes, page.records.len() as u64);
                next = page.next;
                if page.records.is_empty() {
                    tokio::time::sleep(READ_IDLE_BACKOFF).await;
                }
            }
            Err(error) => {
                if !stop.load(Ordering::Relaxed) {
                    note(format!("read failed: {error}"));
                }
                return;
            }
        }
    }
}

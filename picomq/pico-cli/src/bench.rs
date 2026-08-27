//! `pico bench`. Write and read a temporary stream, and report what it did.
//!
//! `cli/ds/DsBench` and `cli/BenchStats`, kept
//! comparable on purpose: the same flags and defaults (1 KiB records, 15
//! seconds, 32 concurrent appends, batch of 1), bytes counted **on ack** so the
//! figure includes the durability wait, a reader running alongside the writer,
//! and the same summary line.
//!
//! Additions: per-interval lines, `--streams` and `--connections` so the load
//! generator can outrun a single connection, a discarded `--warmup`, and
//! `--output json`.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use clap::Args;
use hdrhistogram::sync::{Recorder, SyncHistogram};
use hdrhistogram::Histogram;
use pico_client::producer::{Pending, Producer, ProducerConfig};
use pico_client::{ClientError, DsClient, Live, PicoClient, Protocol, ReadLimits, StreamApi};
use serde_json::json;

use crate::io::note;
use crate::stream::Target;

/// Byte budget per read request.
const READ_BYTES: u64 = 4 * 1024 * 1024;
/// How long the reader backs off when it is caught up.
const READ_IDLE_BACKOFF: Duration = Duration::from_millis(10);

#[derive(Debug, Args)]
pub struct BenchArgs {
    #[arg(short = 'b', long, default_value_t = 1024)]
    record_size: usize,

    #[arg(short = 'd', long, default_value_t = 15)]
    duration: u64,

    #[arg(short = 'w', long, default_value_t = 32)]
    in_flight: usize,

    /// Records per append: a binary batch on Pico, a JSON array on Durable
    #[arg(short = 'n', long, default_value_t = 1)]
    batch: usize,

    #[arg(short = 't', long, default_value_t = 0.0)]
    target_mibps: f64,

    #[arg(long, default_value_t = 1)]
    streams: usize,

    /// Connection pools to spread appends over, so one pool is not the limit.
    #[arg(long, default_value_t = 1)]
    connections: usize,

    /// Seconds to run before measurement starts.
    #[arg(long, default_value_t = 0)]
    warmup: u64,

    /// Seconds between progress lines. 0 prints only the summary.
    #[arg(long, default_value_t = 1)]
    interval: u64,

    /// Write only, with no reader alongside.
    #[arg(long)]
    no_read: bool,

    /// Offer the load through a producer session (Pico protocol only): one
    /// record at a time, with the session batching and pipelining. `--batch`
    /// becomes the session's maximum batch size.
    #[arg(long)]
    producer: bool,

    /// Leave the benchmark streams behind for inspection.
    #[arg(long)]
    keep: bool,

    /// Emit a JSON object on stdout instead of a human summary.
    #[arg(long, default_value = "text")]
    output: OutputFormat,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum OutputFormat {
    Text,
    Json,
}

#[derive(Default)]
struct Counters {
    bytes: AtomicU64,
    records: AtomicU64,
}

impl Counters {
    fn add(&self, bytes: u64, records: u64) {
        self.bytes.fetch_add(bytes, Ordering::Relaxed);
        self.records.fetch_add(records, Ordering::Relaxed);
    }

    fn snapshot(&self) -> (u64, u64) {
        (
            self.bytes.load(Ordering::Relaxed),
            self.records.load(Ordering::Relaxed),
        )
    }

    fn reset(&self) {
        self.bytes.store(0, Ordering::Relaxed);
        self.records.store(0, Ordering::Relaxed);
    }
}

/// One client per protocol, so the writer can hold a concrete type and the
/// reader can use the shared trait.
enum Client {
    Pico(PicoClient),
    Ds(DsClient),
}

impl Client {
    fn open(protocol: Protocol, endpoint: &Target) -> Result<Self, ClientError> {
        let http = pico_client::http_client(&endpoint.client_config())?;
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

/// The bytes one append carries, and how many records that counts as. DS
/// appends carry a JSON array, because the protocol has no batch framing.
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

pub async fn run(endpoint: &Target, args: BenchArgs) -> Result<i32, ClientError> {
    let protocol = endpoint.protocol.client_protocol()?;
    let payload = Arc::new(Payload::build(protocol, args.record_size, args.batch));
    let streams = args.streams.max(1);
    let workers = args.in_flight.max(1);
    let clients: Vec<Arc<Client>> = (0..args.connections.max(1))
        .map(|_| Client::open(protocol, endpoint).map(Arc::new))
        .collect::<Result<_, _>>()?;

    // Process id + start time keep concurrent runs apart without a uuid dependency.
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
        // One session per stream: the session does the batching and pipelining
        // that the plain writers do by hand, so extra writer tasks per stream
        // would only measure contention on the same session.
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

    // Warmup is discarded rather than avoided: the writers keep running, and
    // the counters and histogram start over once it is done.
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
    emit(&args, protocol, &latency, &write, &read, elapsed);

    if !args.keep {
        for name in &names {
            clients[0].api().delete(name).await?;
        }
    }

    Ok(if failed.is_some() { 1 } else { 0 })
}

/// When a writer must stop, and how fast it may go.
///
/// Pacing is on bytes handed to the client rather than bytes acked, so a slow
/// server shows up as latency, not as a lower offered load.
#[derive(Clone, Copy)]
struct Schedule {
    started: Instant,
    deadline: Instant,
    target_bytes_per_sec: f64,
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

/// The same load offered through a [`Producer`] session, which batches and
/// pipelines on the caller's behalf.
///
/// This is what an SDK user writes: one record at a time, no task pool, no
/// in-flight accounting. Latency here is per record from `send` to durable, so
/// it includes the linger wait that the session trades for throughput.
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

    // Records are handed over as fast as the session accepts them. A bounded
    // queue of un-awaited handles is what keeps batches in flight without
    // letting completions pile up unmeasured.
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

/// Print a line per interval until the writers are done, so a stall is visible
/// while it happens instead of being averaged into the summary.
async fn report_progress(
    latency: &mut SyncHistogram<u64>,
    write: &Counters,
    read: &Counters,
    measured_from: Instant,
    deadline: Instant,
    args: &BenchArgs,
) {
    if args.interval == 0 || args.output == OutputFormat::Json {
        tokio::time::sleep_until(deadline.into()).await;
        return;
    }

    let step = Duration::from_secs(args.interval);
    let mut last = (0u64, 0u64, 0u64, 0u64);
    let mut at = Instant::now() + step;

    while at < deadline {
        tokio::time::sleep_until(at.into()).await;
        let (write_bytes, write_records) = write.snapshot();
        let (read_bytes, read_records) = read.snapshot();
        latency.refresh_timeout(Duration::from_millis(100));

        note(format!(
            "[{:>5.1}s] write {} | read {} | p50 {} p99 {}",
            measured_from.elapsed().as_secs_f64(),
            rate(write_bytes - last.0, write_records - last.1, step),
            rate(read_bytes - last.2, read_records - last.3, step),
            millis(latency.value_at_quantile(0.5)),
            millis(latency.value_at_quantile(0.99)),
        ));
        last = (write_bytes, write_records, read_bytes, read_records);
        at += step;
    }
    tokio::time::sleep_until(deadline.into()).await;
}

fn rate(bytes: u64, records: u64, over: Duration) -> String {
    let secs = over.as_secs_f64().max(0.001);
    format!(
        "{:>7.2} MiB/s {:>8.0} rec/s",
        bytes as f64 / (1024.0 * 1024.0) / secs,
        records as f64 / secs
    )
}

fn millis(micros: u64) -> String {
    format!("{:.2}ms", micros as f64 / 1000.0)
}

/// Output wording and units are kept stable so runs compare line to line.
fn summary(label: &str, bytes: u64, records: u64, elapsed: Duration) -> String {
    let secs = elapsed.as_secs_f64().max(0.001);
    format!(
        "{label}: {:.2} MiB/s, {:.0} records/s ({bytes} bytes, {records} records in {secs:.2}s)",
        bytes as f64 / (1024.0 * 1024.0) / secs,
        records as f64 / secs,
    )
}

fn emit(
    args: &BenchArgs,
    protocol: Protocol,
    latency: &SyncHistogram<u64>,
    write: &Counters,
    read: &Counters,
    elapsed: Duration,
) {
    let (write_bytes, write_records) = write.snapshot();
    let (read_bytes, read_records) = read.snapshot();

    if args.output == OutputFormat::Text {
        note(summary("Write", write_bytes, write_records, elapsed));
        note(format!(
            "Append latency: p50 {} p90 {} p99 {} p99.9 {} max {}",
            millis(latency.value_at_quantile(0.5)),
            millis(latency.value_at_quantile(0.9)),
            millis(latency.value_at_quantile(0.99)),
            millis(latency.value_at_quantile(0.999)),
            millis(latency.max()),
        ));
        note(summary("Read", read_bytes, read_records, elapsed));
        return;
    }

    let secs = elapsed.as_secs_f64().max(0.001);
    let body = json!({
        "protocol": protocol.as_str(),
        "record_size": args.record_size,
        "batch": args.batch,
        "in_flight": args.in_flight,
        "streams": args.streams,
        "connections": args.connections,
        "elapsed_sec": secs,
        "write": {
            "mib_per_sec": write_bytes as f64 / (1024.0 * 1024.0) / secs,
            "records_per_sec": write_records as f64 / secs,
            "bytes": write_bytes,
            "records": write_records,
            "p50_ms": latency.value_at_quantile(0.5) as f64 / 1000.0,
            "p90_ms": latency.value_at_quantile(0.9) as f64 / 1000.0,
            "p99_ms": latency.value_at_quantile(0.99) as f64 / 1000.0,
            "p999_ms": latency.value_at_quantile(0.999) as f64 / 1000.0,
            "max_ms": latency.max() as f64 / 1000.0,
        },
        "read": {
            "mib_per_sec": read_bytes as f64 / (1024.0 * 1024.0) / secs,
            "records_per_sec": read_records as f64 / secs,
            "bytes": read_bytes,
            "records": read_records,
        },
    });
    println!("{}", serde_json::to_string_pretty(&body).expect("json"));
}

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

mod http;
mod kafka;

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use clap::Args;
use hdrhistogram::sync::SyncHistogram;
use picomq_client::ClientError;
use serde_json::json;

use crate::io::note;
use crate::stream::{ProtocolArg, Target};

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
pub(crate) struct Counters {
    bytes: AtomicU64,
    records: AtomicU64,
}

impl Counters {
    pub(crate) fn add(&self, bytes: u64, records: u64) {
        self.bytes.fetch_add(bytes, Ordering::Relaxed);
        self.records.fetch_add(records, Ordering::Relaxed);
    }

    pub(crate) fn snapshot(&self) -> (u64, u64) {
        (
            self.bytes.load(Ordering::Relaxed),
            self.records.load(Ordering::Relaxed),
        )
    }

    pub(crate) fn reset(&self) {
        self.bytes.store(0, Ordering::Relaxed);
        self.records.store(0, Ordering::Relaxed);
    }
}

#[derive(Clone, Copy)]
pub(crate) struct Schedule {
    pub(crate) started: Instant,
    pub(crate) deadline: Instant,
    pub(crate) target_bytes_per_sec: f64,
}

pub async fn run(endpoint: &Target, args: BenchArgs) -> Result<i32, ClientError> {
    match endpoint.protocol {
        ProtocolArg::Kafka => kafka::run(endpoint, args).await,
        ProtocolArg::Pico | ProtocolArg::Ds => http::run(endpoint, args).await,
    }
}

pub(crate) async fn report_progress(
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

fn summary(label: &str, bytes: u64, records: u64, elapsed: Duration) -> String {
    let secs = elapsed.as_secs_f64().max(0.001);
    format!(
        "{label}: {:.2} MiB/s, {:.0} records/s ({bytes} bytes, {records} records in {secs:.2}s)",
        bytes as f64 / (1024.0 * 1024.0) / secs,
        records as f64 / secs,
    )
}

pub(crate) fn emit(
    args: &BenchArgs,
    protocol: &str,
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
        "protocol": protocol,
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

use std::sync::OnceLock;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::time::Duration;

use picomq_auth::Audience;

use crate::histogram::Histogram;

pub(crate) const KAFKA_SLOTS: usize = 128;
const KAFKA_OTHER: usize = KAFKA_SLOTS - 1;

pub(crate) static INSTRUMENTS: OnceLock<Instruments> = OnceLock::new();

pub(crate) const AUDIENCES: [Audience; 4] = [
    Audience::Pico,
    Audience::DurableStreams,
    Audience::Admin,
    Audience::Kafka,
];

pub(crate) fn audience_slot(audience: Audience) -> usize {
    match audience {
        Audience::Pico => 0,
        Audience::DurableStreams => 1,
        Audience::Admin => 2,
        Audience::Kafka => 3,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Method {
    Get,
    Head,
    Put,
    Post,
    Patch,
    Delete,
    Options,
    Other,
}

impl Method {
    pub(crate) const ALL: [Method; 8] = [
        Method::Get,
        Method::Head,
        Method::Put,
        Method::Post,
        Method::Patch,
        Method::Delete,
        Method::Options,
        Method::Other,
    ];

    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Get => "GET",
            Self::Head => "HEAD",
            Self::Put => "PUT",
            Self::Post => "POST",
            Self::Patch => "PATCH",
            Self::Delete => "DELETE",
            Self::Options => "OPTIONS",
            Self::Other => "OTHER",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reason {
    Unauthenticated,
    Malformed,
    Expired,
    WrongAudience,
    Denied,
    NarrowingRejected,
    Credential,
    AutoPrefix,
}

impl Reason {
    pub(crate) const ALL: [Reason; 8] = [
        Reason::Unauthenticated,
        Reason::Malformed,
        Reason::Expired,
        Reason::WrongAudience,
        Reason::Denied,
        Reason::NarrowingRejected,
        Reason::Credential,
        Reason::AutoPrefix,
    ];

    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Unauthenticated => "unauthenticated",
            Self::Malformed => "malformed",
            Self::Expired => "expired",
            Self::WrongAudience => "wrong_audience",
            Self::Denied => "denied",
            Self::NarrowingRejected => "narrowing_rejected",
            Self::Credential => "credential",
            Self::AutoPrefix => "auto_prefix",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ListenerKind {
    Stream,
    Admin,
    Kafka,
}

impl ListenerKind {
    pub(crate) const ALL: [ListenerKind; 3] = [
        ListenerKind::Stream,
        ListenerKind::Admin,
        ListenerKind::Kafka,
    ];

    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Stream => "stream",
            Self::Admin => "admin",
            Self::Kafka => "kafka",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransferOutcome {
    Completed,
    Failed,
}

impl TransferOutcome {
    pub(crate) const ALL: [TransferOutcome; 2] =
        [TransferOutcome::Completed, TransferOutcome::Failed];

    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::Failed => "failed",
        }
    }
}

pub(crate) const STATUS_CLASSES: [&str; 5] = ["1xx", "2xx", "3xx", "4xx", "5xx"];
pub(crate) const OUTCOMES: [&str; 2] = ["ok", "error"];

pub(crate) struct Instruments {
    pub(crate) http: [[[AtomicU64; 5]; 8]; 4],
    pub(crate) http_latency: [[Histogram; 8]; 4],
    pub(crate) kafka: [[AtomicU64; 2]; KAFKA_SLOTS],
    pub(crate) kafka_latency: [Histogram; KAFKA_SLOTS],
    pub(crate) kafka_names: [OnceLock<String>; KAFKA_SLOTS],
    pub(crate) records_in: AtomicU64,
    pub(crate) bytes_in: AtomicU64,
    pub(crate) records_out: AtomicU64,
    pub(crate) bytes_out: AtomicU64,
    pub(crate) connections: [AtomicI64; 3],
    pub(crate) live_readers: AtomicI64,
    pub(crate) groups_coordinated: AtomicI64,
    pub(crate) rebalances: AtomicU64,
    pub(crate) transfers: [AtomicU64; 2],
    pub(crate) auth_failures: [[AtomicU64; 8]; 4],
}

impl Instruments {
    pub(crate) fn new() -> Self {
        Self {
            http: Default::default(),
            http_latency: Default::default(),
            kafka: std::array::from_fn(|_| Default::default()),
            kafka_latency: std::array::from_fn(|_| Default::default()),
            kafka_names: std::array::from_fn(|_| OnceLock::new()),
            records_in: AtomicU64::new(0),
            bytes_in: AtomicU64::new(0),
            records_out: AtomicU64::new(0),
            bytes_out: AtomicU64::new(0),
            connections: Default::default(),
            live_readers: AtomicI64::new(0),
            groups_coordinated: AtomicI64::new(0),
            rebalances: AtomicU64::new(0),
            transfers: Default::default(),
            auth_failures: Default::default(),
        }
    }
}

fn installed() -> Option<&'static Instruments> {
    INSTRUMENTS.get()
}

pub(crate) fn kafka_slot(api_key: i16) -> usize {
    usize::try_from(api_key)
        .ok()
        .filter(|slot| *slot < KAFKA_OTHER)
        .unwrap_or(KAFKA_OTHER)
}

pub fn http(audience: Audience, method: Method, status: u16, elapsed: Duration) {
    if let Some(instruments) = installed() {
        let class = usize::from(status / 100).clamp(1, 5) - 1;
        instruments.http[audience_slot(audience)][method as usize][class]
            .fetch_add(1, Ordering::Relaxed);
        instruments.http_latency[audience_slot(audience)][method as usize].observe(elapsed);
    }
}

pub fn kafka(api_key: i16, ok: bool, elapsed: Duration) {
    if let Some(instruments) = installed() {
        let slot = kafka_slot(api_key);
        instruments.kafka[slot][usize::from(!ok)].fetch_add(1, Ordering::Relaxed);
        instruments.kafka_latency[slot].observe(elapsed);
    }
}

pub fn name_kafka_operation(api_key: i16, name: impl Into<String>) {
    if let Some(instruments) = installed() {
        let _ = instruments.kafka_names[kafka_slot(api_key)].set(name.into());
    }
}

pub fn written(records: usize, bytes: usize) {
    if let Some(instruments) = installed() {
        instruments
            .records_in
            .fetch_add(records as u64, Ordering::Relaxed);
        instruments
            .bytes_in
            .fetch_add(bytes as u64, Ordering::Relaxed);
    }
}

pub fn read(records: usize, bytes: usize) {
    if let Some(instruments) = installed() {
        instruments
            .records_out
            .fetch_add(records as u64, Ordering::Relaxed);
        instruments
            .bytes_out
            .fetch_add(bytes as u64, Ordering::Relaxed);
    }
}

pub fn rebalance() {
    if let Some(instruments) = installed() {
        instruments.rebalances.fetch_add(1, Ordering::Relaxed);
    }
}

pub fn transfer(outcome: TransferOutcome) {
    if let Some(instruments) = installed() {
        instruments.transfers[outcome as usize].fetch_add(1, Ordering::Relaxed);
    }
}

pub fn auth_failure(audience: Audience, reason: Reason) {
    if let Some(instruments) = installed() {
        instruments.auth_failures[audience_slot(audience)][reason as usize]
            .fetch_add(1, Ordering::Relaxed);
    }
}

pub fn connection(listener: ListenerKind) -> GaugeGuard {
    GaugeGuard::new(installed().map(|instruments| &instruments.connections[listener as usize]))
}

pub fn live_reader() -> GaugeGuard {
    GaugeGuard::new(installed().map(|instruments| &instruments.live_readers))
}

pub fn coordinated_group() -> GaugeGuard {
    GaugeGuard::new(installed().map(|instruments| &instruments.groups_coordinated))
}

#[derive(Debug)]
pub struct GaugeGuard {
    gauge: Option<&'static AtomicI64>,
}

impl GaugeGuard {
    fn new(gauge: Option<&'static AtomicI64>) -> Self {
        if let Some(gauge) = gauge {
            gauge.fetch_add(1, Ordering::Relaxed);
        }
        Self { gauge }
    }
}

impl Drop for GaugeGuard {
    fn drop(&mut self) {
        if let Some(gauge) = self.gauge {
            gauge.fetch_sub(1, Ordering::Relaxed);
        }
    }
}

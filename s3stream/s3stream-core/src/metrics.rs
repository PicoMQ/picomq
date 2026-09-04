//! Engine metrics facade.
//!
//! The metric *names*, *labels*, and *units* dashboards depend on are kept
//! stable. Emission goes through the [`metrics`] crate facade. The host
//! installs a recorder (Prometheus exporter, OTel bridge, ...). With no
//! recorder installed, emission is a no-op.

/// Histogram of every engine operation's latency in nanoseconds, dimensioned by
/// `operation_type`/`operation_name` (+ `stage` for staged operations).
pub const OPERATION_LATENCY: &str = "kafka_stream_operation_latency";
pub const NETWORK_INBOUND_USAGE: &str = "kafka_stream_network_inbound_usage";
pub const NETWORK_OUTBOUND_USAGE: &str = "kafka_stream_network_outbound_usage";
pub const NETWORK_INBOUND_LIMITER_QUEUE_TIME: &str =
    "kafka_stream_network_inbound_limiter_queue_time";
pub const NETWORK_OUTBOUND_LIMITER_QUEUE_TIME: &str =
    "kafka_stream_network_outbound_limiter_queue_time";
pub const NETWORK_INBOUND_LIMITER_QUEUE_SIZE: &str =
    "kafka_stream_network_inbound_limiter_queue_size";
pub const NETWORK_OUTBOUND_LIMITER_QUEUE_SIZE: &str =
    "kafka_stream_network_outbound_limiter_queue_size";

pub const LABEL_OPERATION_TYPE: &str = "operation_type";
pub const LABEL_OPERATION_NAME: &str = "operation_name";
pub const LABEL_STAGE: &str = "stage";
pub const LABEL_STATUS: &str = "status";
pub const LABEL_TYPE: &str = "type";

pub const LABEL_STATUS_SUCCESS: &str = "success";
pub const LABEL_STATUS_FAILED: &str = "failed";
pub const LABEL_STATUS_HIT: &str = "hit";
pub const LABEL_STATUS_MISS: &str = "miss";

/// Operation identity for `kafka_stream_operation_latency`. The
/// `operation_type` and `name` label values are kept stable for dashboards.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum S3Operation {
    CreateStream,
    OpenStream,
    AppendStream,
    FetchStream,
    TrimStream,
    CloseStream,
    AppendStorage,
    AppendStorageWal,
    AppendStorageAppendCallback,
    AppendStorageWalFull,
    AppendStorageLogCache,
    AppendStorageLogCacheFull,
    UploadStorageWal,
    ForceUploadStorageWalAwait,
    ForceUploadStorageWal,
    ReadStorage,
    ReadStorageLogCache,
    ReadStorageBlockCache,
    GetObject,
    PutObject,
    ListObjects,
    DeleteObjects,
    CreateMultiPartUpload,
    UploadPart,
    UploadPartCopy,
    CompleteMultiPartUpload,
    PrepareObject,
    CommitStreamSetObject,
    CompactedObject,
    CommitStreamObject,
    GetObjects,
    GetServerObjects,
    GetStreamObjects,
    AllocBuffer,
}

impl S3Operation {
    pub fn operation_type(self) -> &'static str {
        use S3Operation::*;
        match self {
            CreateStream | OpenStream | AppendStream | FetchStream | TrimStream | CloseStream => {
                "S3Stream"
            }
            AppendStorage
            | AppendStorageWal
            | AppendStorageAppendCallback
            | AppendStorageWalFull
            | AppendStorageLogCache
            | AppendStorageLogCacheFull
            | UploadStorageWal
            | ForceUploadStorageWalAwait
            | ForceUploadStorageWal
            | ReadStorage
            | ReadStorageLogCache
            | ReadStorageBlockCache
            | AllocBuffer => "S3Storage",
            GetObject
            | PutObject
            | ListObjects
            | DeleteObjects
            | CreateMultiPartUpload
            | UploadPart
            | UploadPartCopy
            | CompleteMultiPartUpload => "S3Request",
            PrepareObject
            | CommitStreamSetObject
            | CompactedObject
            | CommitStreamObject
            | GetObjects
            | GetServerObjects
            | GetStreamObjects => "S3Object",
        }
    }

    pub fn name(self) -> &'static str {
        use S3Operation::*;
        match self {
            CreateStream => "create",
            OpenStream => "open",
            AppendStream => "append",
            FetchStream => "fetch",
            TrimStream => "trim",
            CloseStream => "close",
            AppendStorage => "append",
            AppendStorageWal => "append_wal",
            AppendStorageAppendCallback => "append_callback",
            AppendStorageWalFull => "append_wal_full",
            AppendStorageLogCache => "append_log_cache",
            AppendStorageLogCacheFull => "append_log_cache_full",
            UploadStorageWal => "upload_wal",
            ForceUploadStorageWalAwait => "force_upload_wal_await",
            ForceUploadStorageWal => "force_upload_wal",
            ReadStorage => "read",
            ReadStorageLogCache => "read_log_cache",
            ReadStorageBlockCache => "read_block_cache",
            GetObject => "get_object",
            PutObject => "put_object",
            ListObjects => "list_objects",
            DeleteObjects => "delete_objects",
            CreateMultiPartUpload => "create_multi_part_upload",
            UploadPart => "upload_part",
            UploadPartCopy => "upload_part_copy",
            CompleteMultiPartUpload => "complete_multi_part_upload",
            PrepareObject => "prepare",
            CommitStreamSetObject => "commit_stream_set_object",
            CompactedObject => "compacted_object",
            CommitStreamObject => "commit_stream_object",
            GetObjects => "get_objects",
            GetServerObjects => "get_server_objects",
            GetStreamObjects => "get_stream_objects",
            AllocBuffer => "alloc_buffer",
        }
    }
}

/// Stage of a staged operation (extra `stage` label on the latency histogram).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum S3Stage {
    AppendWalBefore,
    AppendWalBlockPolled,
    AppendWalAwait,
    AppendWalWrite,
    AppendWalAfter,
    AppendWalComplete,
    ForceUploadWalAwait,
    ForceUploadWalComplete,
    UploadWalPrepare,
    UploadWalUpload,
    UploadWalCommit,
    UploadWalComplete,
}

impl S3Stage {
    pub fn operation(self) -> S3Operation {
        use S3Stage::*;
        match self {
            AppendWalBefore | AppendWalBlockPolled | AppendWalAwait | AppendWalWrite
            | AppendWalAfter | AppendWalComplete => S3Operation::AppendStorageWal,
            ForceUploadWalAwait | ForceUploadWalComplete => S3Operation::ForceUploadStorageWal,
            UploadWalPrepare | UploadWalUpload | UploadWalCommit | UploadWalComplete => {
                S3Operation::UploadStorageWal
            }
        }
    }

    pub fn name(self) -> &'static str {
        use S3Stage::*;
        match self {
            AppendWalBefore => "before",
            AppendWalBlockPolled => "block_polled",
            AppendWalAwait | ForceUploadWalAwait => "await",
            AppendWalWrite => "write",
            AppendWalAfter => "after",
            AppendWalComplete | ForceUploadWalComplete | UploadWalComplete => "complete",
            UploadWalPrepare => "prepare",
            UploadWalUpload => "upload",
            UploadWalCommit => "commit",
        }
    }
}

/// Network direction for the metered bandwidth limiter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    Inbound,
    Outbound,
}

/// Guarded emission: skip negative samples and swallow recorder panics.
/// Metrics must never fail an append or read.
fn record_guarded(value: i64, emit: impl FnOnce(f64)) {
    if value < 0 {
        return;
    }
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| emit(value as f64)));
    if result.is_err() {
        tracing::warn!("metrics recorder panicked; sample dropped");
    }
}

/// Record `kafka_stream_operation_latency{operation_type, operation_name}`.
/// There is no level gate. Host recorders filter.
pub fn record_operation_latency(operation: S3Operation, elapsed_nanos: i64) {
    record_guarded(elapsed_nanos, |value| {
        metrics::histogram!(
            OPERATION_LATENCY,
            LABEL_OPERATION_TYPE => operation.operation_type(),
            LABEL_OPERATION_NAME => operation.name(),
        )
        .record(value);
    });
}

/// Record `kafka_stream_operation_latency{operation_type, operation_name, stage}`.
pub fn record_stage_latency(stage: S3Stage, elapsed_nanos: i64) {
    record_guarded(elapsed_nanos, |value| {
        metrics::histogram!(
            OPERATION_LATENCY,
            LABEL_OPERATION_TYPE => stage.operation().operation_type(),
            LABEL_OPERATION_NAME => stage.operation().name(),
            LABEL_STAGE => stage.name(),
        )
        .record(value);
    });
}

/// Count network usage bytes per direction and throttle strategy.
///
/// →
/// `kafka_stream_network_{in,out}bound_usage{type=<strategy>}`.
pub fn record_network_usage(
    direction: Direction,
    strategy: s3stream_object::storage::ThrottleStrategy,
    bytes: u64,
) {
    record_guarded(bytes as i64, |value| {
        let name = match direction {
            Direction::Inbound => NETWORK_INBOUND_USAGE,
            Direction::Outbound => NETWORK_OUTBOUND_USAGE,
        };
        metrics::counter!(name, LABEL_TYPE => strategy.name()).increment(value as u64);
    });
}

/// Record time a request spent queued in the bandwidth limiter.
pub fn record_network_limiter_queue_time(
    direction: Direction,
    strategy: s3stream_object::storage::ThrottleStrategy,
    elapsed_nanos: i64,
) {
    record_guarded(elapsed_nanos, |value| {
        let name = match direction {
            Direction::Inbound => NETWORK_INBOUND_LIMITER_QUEUE_TIME,
            Direction::Outbound => NETWORK_OUTBOUND_LIMITER_QUEUE_TIME,
        };
        metrics::histogram!(name, LABEL_TYPE => strategy.name()).record(value);
    });
}

/// Publish the current limiter queue depth, set after each acquire (a push
/// gauge rather than an observable one).
pub fn set_network_limiter_queue_size(direction: Direction, size: usize) {
    record_guarded(size as i64, |value| {
        let name = match direction {
            Direction::Inbound => NETWORK_INBOUND_LIMITER_QUEUE_SIZE,
            Direction::Outbound => NETWORK_OUTBOUND_LIMITER_QUEUE_SIZE,
        };
        metrics::gauge!(name).set(value);
    });
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use metrics::{Counter, CounterFn, Gauge, GaugeFn, Histogram, HistogramFn, Key, Recorder};

    use super::*;

    /// One captured emission: metric name, sorted labels, value.
    type Event = (String, Vec<(String, String)>, f64);

    #[derive(Clone, Default)]
    struct TestRecorder {
        events: Arc<Mutex<Vec<Event>>>,
    }

    struct Instrument {
        key: Key,
        events: Arc<Mutex<Vec<Event>>>,
    }

    impl Instrument {
        fn push(&self, value: f64) {
            let labels: Vec<(String, String)> = self
                .key
                .labels()
                .map(|l| (l.key().to_string(), l.value().to_string()))
                .collect();
            self.events
                .lock()
                .unwrap()
                .push((self.key.name().to_string(), labels, value));
        }
    }

    impl CounterFn for Instrument {
        fn increment(&self, value: u64) {
            self.push(value as f64);
        }
        fn absolute(&self, value: u64) {
            self.push(value as f64);
        }
    }

    impl GaugeFn for Instrument {
        fn increment(&self, value: f64) {
            self.push(value);
        }
        fn decrement(&self, value: f64) {
            self.push(-value);
        }
        fn set(&self, value: f64) {
            self.push(value);
        }
    }

    impl HistogramFn for Instrument {
        fn record(&self, value: f64) {
            self.push(value);
        }
    }

    impl Recorder for TestRecorder {
        fn describe_counter(
            &self,
            _: metrics::KeyName,
            _: Option<metrics::Unit>,
            _: metrics::SharedString,
        ) {
        }
        fn describe_gauge(
            &self,
            _: metrics::KeyName,
            _: Option<metrics::Unit>,
            _: metrics::SharedString,
        ) {
        }
        fn describe_histogram(
            &self,
            _: metrics::KeyName,
            _: Option<metrics::Unit>,
            _: metrics::SharedString,
        ) {
        }
        fn register_counter(&self, key: &Key, _: &metrics::Metadata<'_>) -> Counter {
            Counter::from_arc(Arc::new(Instrument {
                key: key.clone(),
                events: Arc::clone(&self.events),
            }))
        }
        fn register_gauge(&self, key: &Key, _: &metrics::Metadata<'_>) -> Gauge {
            Gauge::from_arc(Arc::new(Instrument {
                key: key.clone(),
                events: Arc::clone(&self.events),
            }))
        }
        fn register_histogram(&self, key: &Key, _: &metrics::Metadata<'_>) -> Histogram {
            Histogram::from_arc(Arc::new(Instrument {
                key: key.clone(),
                events: Arc::clone(&self.events),
            }))
        }
    }

    #[test]
    fn operation_latency_names_and_labels() {
        let recorder = TestRecorder::default();
        let events = Arc::clone(&recorder.events);
        metrics::with_local_recorder(&recorder, || {
            record_operation_latency(S3Operation::AppendStream, 42);
            record_stage_latency(S3Stage::UploadWalCommit, 7);
        });
        let events = events.lock().unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].0, "kafka_stream_operation_latency");
        assert!(
            events[0]
                .1
                .contains(&("operation_type".into(), "S3Stream".into()))
        );
        assert!(
            events[0]
                .1
                .contains(&("operation_name".into(), "append".into()))
        );
        assert_eq!(events[0].2, 42.0);
        assert!(
            events[1]
                .1
                .contains(&("operation_type".into(), "S3Storage".into()))
        );
        assert!(
            events[1]
                .1
                .contains(&("operation_name".into(), "upload_wal".into()))
        );
        assert!(events[1].1.contains(&("stage".into(), "commit".into())));
    }

    #[test]
    fn negative_samples_skipped() {
        let recorder = TestRecorder::default();
        let events = Arc::clone(&recorder.events);
        metrics::with_local_recorder(&recorder, || {
            record_operation_latency(S3Operation::AppendStream, -1);
            record_operation_latency(S3Operation::AppendStream, 1);
        });
        let events = events.lock().unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].2, 1.0);
    }

    #[test]
    fn recorder_panic_does_not_bubble() {
        struct PanickingRecorder;
        struct PanickingHistogram;
        impl HistogramFn for PanickingHistogram {
            fn record(&self, _: f64) {
                panic!("broken recorder");
            }
        }
        impl Recorder for PanickingRecorder {
            fn describe_counter(
                &self,
                _: metrics::KeyName,
                _: Option<metrics::Unit>,
                _: metrics::SharedString,
            ) {
            }
            fn describe_gauge(
                &self,
                _: metrics::KeyName,
                _: Option<metrics::Unit>,
                _: metrics::SharedString,
            ) {
            }
            fn describe_histogram(
                &self,
                _: metrics::KeyName,
                _: Option<metrics::Unit>,
                _: metrics::SharedString,
            ) {
            }
            fn register_counter(&self, _: &Key, _: &metrics::Metadata<'_>) -> Counter {
                Counter::noop()
            }
            fn register_gauge(&self, _: &Key, _: &metrics::Metadata<'_>) -> Gauge {
                Gauge::noop()
            }
            fn register_histogram(&self, _: &Key, _: &metrics::Metadata<'_>) -> Histogram {
                Histogram::from_arc(Arc::new(PanickingHistogram))
            }
        }
        metrics::with_local_recorder(&PanickingRecorder, || {
            // Must not panic.
            record_operation_latency(S3Operation::AppendStream, 5);
        });
    }
}

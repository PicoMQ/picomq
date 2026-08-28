//! S3StreamClient: opens/creates streams, tracks open streams, runs stream-object
//! compaction schedules.
//!
//! ScheduledExecutor tasks, MINOR_V1 every 10 min / MAJOR_V1 every 60 min) is collapsed
//! into one background ticker that sweeps all open streams. The per-level cadence and
//! the cooldown-after-open guard are preserved.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;

use crate::api::{CreateStreamOptions, OpenStreamOptions, Stream, StreamClient, StreamError};
use crate::compact::{CompactionLevel, StreamObjectCompactor, StreamView};
use crate::manager::{ObjectManager, StreamManager};
use crate::storage::Storage;
use crate::stream::S3Stream;
use s3stream_object::ObjectStorage;

const COMPACTION_COOLDOWN_AFTER_OPEN: Duration = Duration::from_secs(60);
const MINOR_V1_COMPACTION_INTERVAL: Duration = Duration::from_secs(10 * 60);
const MAJOR_V1_COMPACTION_INTERVAL: Duration = Duration::from_secs(60 * 60);

/// Stream-object compaction knobs (subset of `Config` the client needs).
#[derive(Debug, Clone)]
pub struct StreamClientConfig {
    /// Max size of a merged stream object.
    /// (default 1 GiB, clamped by
    /// the compactor to the 5 GiB object limit).
    pub max_stream_object_size: u64,
    pub compaction_enabled: bool,
}

impl Default for StreamClientConfig {
    fn default() -> Self {
        Self {
            max_stream_object_size: 1 << 30,
            compaction_enabled: true,
        }
    }
}

struct OpenedStream {
    stream: Arc<S3Stream>,
    opened_at: tokio::time::Instant,
}

pub struct S3StreamClient {
    storage: Arc<dyn Storage>,
    stream_manager: Arc<dyn StreamManager>,
    compactor: Arc<StreamObjectCompactor>,
    opened: Arc<Mutex<HashMap<u64, OpenedStream>>>,
    shutdown: Arc<AtomicBool>,
    scheduler: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl S3StreamClient {
    pub fn new(
        storage: Arc<dyn Storage>,
        stream_manager: Arc<dyn StreamManager>,
        object_manager: Arc<dyn ObjectManager>,
        object_storage: Arc<dyn ObjectStorage>,
        config: StreamClientConfig,
    ) -> Arc<Self> {
        let compactor = Arc::new(StreamObjectCompactor::new(
            object_manager,
            object_storage,
            config.max_stream_object_size,
        ));
        let client = Arc::new(Self {
            storage,
            stream_manager,
            compactor,
            opened: Arc::new(Mutex::new(HashMap::new())),
            shutdown: Arc::new(AtomicBool::new(false)),
            scheduler: Mutex::new(None),
        });
        if config.compaction_enabled {
            let handle = tokio::spawn(compaction_scheduler(
                Arc::clone(&client.opened),
                Arc::clone(&client.compactor),
                Arc::clone(&client.shutdown),
            ));
            *client.scheduler.lock().expect("scheduler poisoned") = Some(handle);
        }
        client
    }

    fn check_state(&self) -> Result<(), StreamError> {
        if self.shutdown.load(Ordering::Acquire) {
            return Err(StreamError::Unexpected(
                "S3StreamClient is already closed".into(),
            ));
        }
        Ok(())
    }

    pub async fn force_close(&self) {
        self.shutdown.store(true, Ordering::Release);
        let streams: Vec<Arc<S3Stream>> = {
            let opened = self.opened.lock().expect("opened poisoned");
            opened.values().map(|s| Arc::clone(&s.stream)).collect()
        };
        for stream in streams {
            if let Err(e) = stream.close_with(true).await {
                tracing::error!(
                    stream_id = stream.stream_id(),
                    error = %e,
                    "force close stream failed"
                );
            }
            self.opened
                .lock()
                .expect("opened poisoned")
                .remove(&stream.stream_id());
        }
    }

    async fn open_stream0(
        &self,
        stream_id: u64,
        options: OpenStreamOptions,
    ) -> Result<Arc<dyn Stream>, StreamError> {
        self.check_state()?;
        let snapshot_read = options.snapshot_read;
        let (epoch, start_offset, next_offset) = if snapshot_read {
            (
                options.epoch,
                crate::stream::SNAPSHOT_FAKE_OFFSET,
                crate::stream::SNAPSHOT_FAKE_OFFSET,
            )
        } else {
            let metadata = self
                .stream_manager
                .open_stream(stream_id, options.epoch, options.tags)
                .await?;
            (metadata.epoch, metadata.start_offset, metadata.end_offset)
        };
        let stream = Arc::new(S3Stream::new(
            stream_id,
            epoch,
            start_offset,
            next_offset,
            Arc::clone(&self.storage),
            Arc::clone(&self.stream_manager),
            snapshot_read,
        ));
        stream.attach_metadata_listener();
        if !snapshot_read {
            self.opened.lock().expect("opened poisoned").insert(
                stream_id,
                OpenedStream {
                    stream: Arc::clone(&stream),
                    opened_at: tokio::time::Instant::now(),
                },
            );
        }
        Ok(stream)
    }

    /// Run one compaction pass for a stream right now (hosts trigger e.g. on trim).
    pub async fn compact_stream_object(
        &self,
        stream_id: u64,
        level: CompactionLevel,
    ) -> Result<(), StreamError> {
        let stream = {
            let opened = self.opened.lock().expect("opened poisoned");
            opened.get(&stream_id).map(|s| Arc::clone(&s.stream))
        };
        let Some(stream) = stream else { return Ok(()) };
        self.compactor
            .compact(
                StreamView {
                    stream_id,
                    stream_epoch: stream.stream_epoch(),
                    start_offset: stream.start_offset(),
                    confirm_offset: stream.confirm_offset(),
                },
                level,
            )
            .await
    }
}

/// The sweep ticker: every MINOR interval compact all open streams at MinorV1, every
/// MAJOR interval at MajorV1. Streams opened less than the cooldown ago are skipped
/// objects are still being uploaded/committed).
async fn compaction_scheduler(
    opened: Arc<Mutex<HashMap<u64, OpenedStream>>>,
    compactor: Arc<StreamObjectCompactor>,
    shutdown: Arc<AtomicBool>,
) {
    let mut interval = tokio::time::interval(MINOR_V1_COMPACTION_INTERVAL);
    interval.tick().await; // skip the immediate tick
    let mut ticks: u64 = 0;
    let major_every =
        (MAJOR_V1_COMPACTION_INTERVAL.as_secs() / MINOR_V1_COMPACTION_INTERVAL.as_secs()).max(1);
    loop {
        interval.tick().await;
        if shutdown.load(Ordering::Acquire) {
            return;
        }
        ticks += 1;
        let level = if ticks % major_every == 0 {
            CompactionLevel::MajorV1
        } else {
            CompactionLevel::MinorV1
        };
        let now = tokio::time::Instant::now();
        let views: Vec<StreamView> = {
            let opened = opened.lock().expect("opened poisoned");
            opened
                .values()
                .filter(|s| now.duration_since(s.opened_at) >= COMPACTION_COOLDOWN_AFTER_OPEN)
                .filter(|s| !s.stream.is_closed())
                .map(|s| StreamView {
                    stream_id: s.stream.stream_id(),
                    stream_epoch: s.stream.stream_epoch(),
                    start_offset: s.stream.start_offset(),
                    confirm_offset: s.stream.confirm_offset(),
                })
                .collect()
        };
        for view in views {
            let stream_id = view.stream_id;
            if let Err(e) = compactor.compact(view, level).await {
                tracing::error!(stream_id, ?level, error = %e, "stream object compaction failed");
            }
        }
    }
}

#[async_trait]
impl StreamClient for S3StreamClient {
    async fn create_and_open_stream(
        &self,
        options: CreateStreamOptions,
    ) -> Result<Arc<dyn Stream>, StreamError> {
        self.check_state()?;
        let stream_id = self
            .stream_manager
            .create_stream(options.tags.clone())
            .await?;
        self.open_stream0(
            stream_id,
            OpenStreamOptions {
                epoch: options.epoch,
                tags: options.tags,
                ..Default::default()
            },
        )
        .await
    }

    async fn open_stream(
        &self,
        stream_id: u64,
        options: OpenStreamOptions,
    ) -> Result<Arc<dyn Stream>, StreamError> {
        self.open_stream0(stream_id, options).await
    }

    fn get_stream(&self, stream_id: u64) -> Option<Arc<dyn Stream>> {
        let opened = self.opened.lock().expect("opened poisoned");
        opened
            .get(&stream_id)
            .map(|s| Arc::clone(&s.stream) as Arc<dyn Stream>)
    }

    async fn compact_stream(
        &self,
        stream_id: u64,
        level: CompactionLevel,
    ) -> Result<(), StreamError> {
        self.compact_stream_object(stream_id, level).await
    }

    async fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Release);
        if let Some(handle) = self.scheduler.lock().expect("scheduler poisoned").take() {
            handle.abort();
        }
        let streams: Vec<Arc<S3Stream>> = {
            let mut opened = self.opened.lock().expect("opened poisoned");
            opened.drain().map(|(_, s)| s.stream).collect()
        };
        for stream in streams {
            if let Err(e) = stream.close().await {
                tracing::error!(stream_id = stream.stream_id(), error = %e, "close on shutdown failed");
            }
        }
    }
}

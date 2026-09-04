//! Retry and inflight-limit decorator for object storage.
//!
//! Providers throttle under load (S3 returns 429/503 SlowDown), and the inner
//! `object_store` client gives up after a bounded number of attempts. Durable
//! paths (WAL, commit) treat a surfaced storage error as fatal, so throttling
//! must never surface. This layer retries every transient failure with capped
//! exponential backoff and converts a slow store into backpressure through
//! inflight permits.
//!
//! Only definitive errors abort (not found, invalid path, auth). Everything
//! else retries until it succeeds or the caller's deadline expires.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use tokio::sync::Semaphore;
use tokio::time::Instant;

use crate::error::ObjectError;
use crate::storage::{
    MultipartWriter, ObjectInfo, ObjectPath, ObjectStorage, ReadOptions, WriteOptions, WriteResult,
};

/// How the retry loop treats an error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RetryClass {
    /// Definitive outcome. Retrying cannot change it.
    Abort,
    /// Transient or throttling failure. Retry until it stops.
    Retry,
}

fn retry_class(e: &ObjectError) -> RetryClass {
    match e {
        ObjectError::NotFound { .. }
        | ObjectError::InvalidFormat { .. }
        | ObjectError::OrderingViolation { .. }
        | ObjectError::Codec(_)
        | ObjectError::Timeout { .. } => RetryClass::Abort,
        ObjectError::Backend(be) => match be {
            object_store::Error::NotFound { .. }
            | object_store::Error::NotModified { .. }
            | object_store::Error::InvalidPath { .. }
            | object_store::Error::AlreadyExists { .. }
            | object_store::Error::Precondition { .. }
            | object_store::Error::NotSupported { .. }
            | object_store::Error::NotImplemented { .. }
            | object_store::Error::PermissionDenied { .. }
            | object_store::Error::Unauthenticated { .. }
            | object_store::Error::UnknownConfigurationKey { .. } => RetryClass::Abort,
            _ => RetryClass::Retry,
        },
        ObjectError::Io(_) => RetryClass::Retry,
    }
}

/// Tuning for [`RetryingObjectStorage`].
#[derive(Debug, Clone)]
pub struct RetryConfig {
    /// Max concurrent write operations (PUT, multipart part, complete).
    pub max_inflight_writes: usize,
    /// Max concurrent read operations.
    pub max_inflight_reads: usize,
    /// First retry delay. Doubles per retry.
    pub base_delay: Duration,
    /// Backoff cap.
    pub max_delay: Duration,
    /// Random extra delay added to every retry, `0..=jitter`.
    pub jitter: Duration,
}

impl Default for RetryConfig {
    /// Concurrency 25 per core clamped to [50, 1000],
    /// delay `jitter(0..1s) + min(1s << retries, 60s)`.
    fn default() -> Self {
        let cores = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(2);
        let concurrency = (cores * 25).clamp(50, 1000);
        Self {
            max_inflight_writes: concurrency,
            max_inflight_reads: concurrency,
            base_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(60),
            jitter: Duration::from_secs(1),
        }
    }
}

fn backoff_delay(config: &RetryConfig, retries: u32) -> Duration {
    let exp = config
        .base_delay
        .saturating_mul(1u32 << retries.min(16))
        .min(config.max_delay);
    let jitter_ms = config.jitter.as_millis() as u64;
    exp + Duration::from_millis(fastrand::u64(0..=jitter_ms))
}

async fn retry_until_definitive<T, Fut>(
    config: &RetryConfig,
    op: &'static str,
    key: &str,
    deadline: Option<Instant>,
    attempt: impl Fn() -> Fut,
) -> Result<T, ObjectError>
where
    Fut: Future<Output = Result<T, ObjectError>>,
{
    let mut retries: u32 = 0;
    loop {
        let error = match attempt().await {
            Ok(value) => return Ok(value),
            Err(e) if retry_class(&e) == RetryClass::Abort => return Err(e),
            Err(e) => e,
        };
        let delay = backoff_delay(config, retries);
        if let Some(deadline) = deadline
            && Instant::now() + delay >= deadline
        {
            return Err(ObjectError::Timeout {
                key: key.to_string(),
                last: error.to_string(),
            });
        }
        tracing::warn!(
            op,
            key,
            retries,
            delay_ms = delay.as_millis() as u64,
            error = %error,
            "object storage attempt failed, will retry"
        );
        retries = retries.saturating_add(1);
        tokio::time::sleep(delay).await;
    }
}

/// Decorator that owns retries and inflight limits for any [`ObjectStorage`].
///
/// Wrap the outermost layer at construction, so every retry attempt passes
/// through inner decorators (e.g. bandwidth throttling debits per attempt).
pub struct RetryingObjectStorage {
    inner: Arc<dyn ObjectStorage>,
    config: RetryConfig,
    write_permits: Arc<Semaphore>,
    read_permits: Arc<Semaphore>,
}

impl RetryingObjectStorage {
    pub fn new(inner: Arc<dyn ObjectStorage>) -> Self {
        Self::with_config(inner, RetryConfig::default())
    }

    pub fn with_config(inner: Arc<dyn ObjectStorage>, config: RetryConfig) -> Self {
        let write_permits = Arc::new(Semaphore::new(config.max_inflight_writes));
        let read_permits = Arc::new(Semaphore::new(config.max_inflight_reads));
        Self {
            inner,
            config,
            write_permits,
            read_permits,
        }
    }
}

#[async_trait]
impl ObjectStorage for RetryingObjectStorage {
    /// No retry. The startup probe must surface a misconfigured store fast.
    async fn readiness_check(&self) -> Result<(), ObjectError> {
        self.inner.readiness_check().await
    }

    async fn range_read(
        &self,
        options: &ReadOptions,
        key: &str,
        start: u64,
        end: Option<u64>,
    ) -> Result<Bytes, ObjectError> {
        let _permit = self.read_permits.acquire().await.expect("semaphore closed");
        retry_until_definitive(&self.config, "range_read", key, None, || {
            self.inner.range_read(options, key, start, end)
        })
        .await
    }

    async fn write(
        &self,
        options: &WriteOptions,
        key: &str,
        data: Bytes,
    ) -> Result<WriteResult, ObjectError> {
        let _permit = self
            .write_permits
            .acquire()
            .await
            .expect("semaphore closed");
        let deadline = options.timeout.map(|t| Instant::now() + t);
        retry_until_definitive(&self.config, "write", key, deadline, || {
            self.inner.write(options, key, data.clone())
        })
        .await
    }

    async fn writer(
        &self,
        options: &WriteOptions,
        key: &str,
    ) -> Result<Box<dyn MultipartWriter>, ObjectError> {
        let inner = {
            let _permit = self
                .write_permits
                .acquire()
                .await
                .expect("semaphore closed");
            retry_until_definitive(&self.config, "create_multipart", key, None, || {
                self.inner.writer(options, key)
            })
            .await?
        };
        Ok(Box::new(RetryingMultipartWriter {
            inner,
            config: self.config.clone(),
            permits: Arc::clone(&self.write_permits),
            key: key.to_string(),
        }))
    }

    async fn list(&self, prefix: &str) -> Result<Vec<ObjectInfo>, ObjectError> {
        let _permit = self.read_permits.acquire().await.expect("semaphore closed");
        retry_until_definitive(&self.config, "list", prefix, None, || {
            self.inner.list(prefix)
        })
        .await
    }

    async fn delete(&self, paths: &[ObjectPath]) -> Result<(), ObjectError> {
        let _permit = self
            .write_permits
            .acquire()
            .await
            .expect("semaphore closed");
        retry_until_definitive(&self.config, "delete", "batch", None, || {
            self.inner.delete(paths)
        })
        .await
    }

    fn bucket_id(&self) -> i16 {
        self.inner.bucket_id()
    }
}

struct RetryingMultipartWriter {
    inner: Box<dyn MultipartWriter>,
    config: RetryConfig,
    permits: Arc<Semaphore>,
    key: String,
}

#[async_trait]
impl MultipartWriter for RetryingMultipartWriter {
    /// No retry. Part numbers are assigned per call inside `object_store`, so
    /// re-submitting a failed part would upload under a fresh number and leave
    /// a hole. A failed part fails the object and the caller rewrites it. The
    /// inner client still does its own bounded per-request retries.
    async fn write(&mut self, part: Bytes) -> Result<(), ObjectError> {
        let _permit = self.permits.acquire().await.expect("semaphore closed");
        self.inner.write(part).await
    }

    /// Retries. CompleteMultipartUpload is idempotent on AWS, and the parts
    /// are already durable, so giving up here would waste the whole upload.
    async fn finish(&mut self) -> Result<WriteResult, ObjectError> {
        let _permit = self.permits.acquire().await.expect("semaphore closed");
        let mut retries: u32 = 0;
        loop {
            let error = match self.inner.finish().await {
                Ok(value) => return Ok(value),
                Err(e) if retry_class(&e) == RetryClass::Abort => return Err(e),
                Err(e) => e,
            };
            let delay = backoff_delay(&self.config, retries);
            tracing::warn!(
                key = %self.key,
                retries,
                delay_ms = delay.as_millis() as u64,
                error = %error,
                "multipart complete failed, will retry"
            );
            retries = retries.saturating_add(1);
            tokio::time::sleep(delay).await;
        }
    }

    async fn abort(&mut self) -> Result<(), ObjectError> {
        self.inner.abort().await
    }

    fn bucket_id(&self) -> i16 {
        self.inner.bucket_id()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use crate::memory::MemoryObjectStorage;

    fn transient() -> ObjectError {
        ObjectError::Io(std::io::Error::new(
            std::io::ErrorKind::ConnectionReset,
            "injected transient failure",
        ))
    }

    /// Wraps memory storage and fails the first `write_failures` writes and
    /// the first `finish_failures` multipart completes with transient errors.
    /// Tracks attempt counts and peak write concurrency.
    struct FlakyStorage {
        inner: MemoryObjectStorage,
        write_failures: AtomicUsize,
        finish_failures: Arc<AtomicUsize>,
        write_attempts: AtomicUsize,
        read_attempts: AtomicUsize,
        inflight: AtomicUsize,
        peak_inflight: AtomicUsize,
    }

    impl FlakyStorage {
        fn new(write_failures: usize, finish_failures: usize) -> Self {
            Self {
                inner: MemoryObjectStorage::new(0),
                write_failures: AtomicUsize::new(write_failures),
                finish_failures: Arc::new(AtomicUsize::new(finish_failures)),
                write_attempts: AtomicUsize::new(0),
                read_attempts: AtomicUsize::new(0),
                inflight: AtomicUsize::new(0),
                peak_inflight: AtomicUsize::new(0),
            }
        }

        fn take_failure(counter: &AtomicUsize) -> bool {
            counter
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                .is_ok()
        }
    }

    #[async_trait]
    impl ObjectStorage for FlakyStorage {
        async fn readiness_check(&self) -> Result<(), ObjectError> {
            self.inner.readiness_check().await
        }

        async fn range_read(
            &self,
            options: &ReadOptions,
            key: &str,
            start: u64,
            end: Option<u64>,
        ) -> Result<Bytes, ObjectError> {
            self.read_attempts.fetch_add(1, Ordering::SeqCst);
            self.inner.range_read(options, key, start, end).await
        }

        async fn write(
            &self,
            options: &WriteOptions,
            key: &str,
            data: Bytes,
        ) -> Result<WriteResult, ObjectError> {
            self.write_attempts.fetch_add(1, Ordering::SeqCst);
            let inflight = self.inflight.fetch_add(1, Ordering::SeqCst) + 1;
            self.peak_inflight.fetch_max(inflight, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(10)).await;
            self.inflight.fetch_sub(1, Ordering::SeqCst);
            if Self::take_failure(&self.write_failures) {
                return Err(transient());
            }
            self.inner.write(options, key, data).await
        }

        async fn writer(
            &self,
            options: &WriteOptions,
            key: &str,
        ) -> Result<Box<dyn MultipartWriter>, ObjectError> {
            let inner = self.inner.writer(options, key).await?;
            Ok(Box::new(FlakyMultipartWriter {
                inner,
                finish_failures: Arc::clone(&self.finish_failures),
            }))
        }

        async fn list(&self, prefix: &str) -> Result<Vec<ObjectInfo>, ObjectError> {
            self.inner.list(prefix).await
        }

        async fn delete(&self, paths: &[ObjectPath]) -> Result<(), ObjectError> {
            self.inner.delete(paths).await
        }

        fn bucket_id(&self) -> i16 {
            self.inner.bucket_id()
        }
    }

    struct FlakyMultipartWriter {
        inner: Box<dyn MultipartWriter>,
        finish_failures: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl MultipartWriter for FlakyMultipartWriter {
        async fn write(&mut self, part: Bytes) -> Result<(), ObjectError> {
            self.inner.write(part).await
        }

        async fn finish(&mut self) -> Result<WriteResult, ObjectError> {
            if FlakyStorage::take_failure(&self.finish_failures) {
                return Err(transient());
            }
            self.inner.finish().await
        }

        async fn abort(&mut self) -> Result<(), ObjectError> {
            self.inner.abort().await
        }

        fn bucket_id(&self) -> i16 {
            self.inner.bucket_id()
        }
    }

    fn retrying(flaky: Arc<FlakyStorage>, config: RetryConfig) -> RetryingObjectStorage {
        RetryingObjectStorage::with_config(flaky, config)
    }

    #[tokio::test(start_paused = true)]
    async fn write_retries_transient_failures_until_success() {
        let flaky = Arc::new(FlakyStorage::new(3, 0));
        let storage = retrying(Arc::clone(&flaky), RetryConfig::default());
        storage
            .write(&WriteOptions::default(), "k", Bytes::from_static(b"v"))
            .await
            .unwrap();
        assert_eq!(flaky.write_attempts.load(Ordering::SeqCst), 4);
        let read = storage.read(&ReadOptions::default(), "k").await.unwrap();
        assert_eq!(read.as_ref(), b"v");
    }

    #[tokio::test(start_paused = true)]
    async fn definitive_errors_abort_without_retry() {
        let flaky = Arc::new(FlakyStorage::new(0, 0));
        let storage = retrying(Arc::clone(&flaky), RetryConfig::default());
        let err = storage
            .read(&ReadOptions::default(), "missing")
            .await
            .unwrap_err();
        assert!(matches!(err, ObjectError::NotFound { .. }));
        assert_eq!(flaky.read_attempts.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn write_deadline_bounds_retries() {
        // More failures than the deadline allows attempts for.
        let flaky = Arc::new(FlakyStorage::new(usize::MAX, 0));
        let storage = retrying(Arc::clone(&flaky), RetryConfig::default());
        let options = WriteOptions {
            timeout: Some(Duration::from_secs(5)),
            ..Default::default()
        };
        let err = storage
            .write(&options, "k", Bytes::from_static(b"v"))
            .await
            .unwrap_err();
        assert!(matches!(err, ObjectError::Timeout { .. }), "got: {err}");
        let attempts = flaky.write_attempts.load(Ordering::SeqCst);
        assert!((1..=5).contains(&attempts), "attempts: {attempts}");
    }

    #[tokio::test(start_paused = true)]
    async fn inflight_writes_are_capped() {
        let flaky = Arc::new(FlakyStorage::new(0, 0));
        let config = RetryConfig {
            max_inflight_writes: 2,
            ..Default::default()
        };
        let storage = Arc::new(retrying(Arc::clone(&flaky), config));
        let writes: Vec<_> = (0..8)
            .map(|i| {
                let storage = Arc::clone(&storage);
                tokio::spawn(async move {
                    storage
                        .write(
                            &WriteOptions::default(),
                            &format!("k{i}"),
                            Bytes::from_static(b"v"),
                        )
                        .await
                })
            })
            .collect();
        for write in writes {
            write.await.unwrap().unwrap();
        }
        assert!(flaky.peak_inflight.load(Ordering::SeqCst) <= 2);
        assert_eq!(flaky.write_attempts.load(Ordering::SeqCst), 8);
    }

    #[tokio::test(start_paused = true)]
    async fn multipart_finish_retries_transient_failures() {
        let flaky = Arc::new(FlakyStorage::new(0, 2));
        let storage = retrying(Arc::clone(&flaky), RetryConfig::default());
        let mut writer = storage
            .writer(&WriteOptions::default(), "mp")
            .await
            .unwrap();
        writer.write(Bytes::from_static(b"part1-")).await.unwrap();
        writer.write(Bytes::from_static(b"part2")).await.unwrap();
        writer.finish().await.unwrap();
        let read = storage.read(&ReadOptions::default(), "mp").await.unwrap();
        assert_eq!(read.as_ref(), b"part1-part2");
    }
}

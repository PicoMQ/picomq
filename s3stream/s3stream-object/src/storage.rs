//! The object storage abstraction used by everything above this crate.
//!
//! The trait is thin. The production impl (`ObjectStoreAdapter`) delegates
//! to the `object_store` crate. Unbounded retries, throttle handling, and
//! inflight limits live in [`crate::retry::RetryingObjectStorage`], which
//! wraps the adapter at construction. The `ObjectPath`/`ObjectInfo` shapes
//! are exact: the WAL recovery protocol depends on them.

use async_trait::async_trait;
use bytes::Bytes;
use object_store::ObjectStoreExt;

use crate::error::ObjectError;

/// Sentinel: range-read to end of object.
///
/// `range_read`'s `end` parameter instead of a magic value.
pub const RANGE_READ_TO_END: Option<u64> = None;

/// Priority class for network throttling.
///
/// `BYPASS(0) / COMPACTION(1) / TAIL(2) / CATCH_UP(3) / ICEBERG_WRITE(4)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum ThrottleStrategy {
    /// Never throttled (force-debits the bucket): WAL writes, delta WAL uploads.
    #[default]
    Bypass,
    /// Background compaction traffic.
    Compaction,
    /// Tail reads.
    Tail,
    /// Catch-up (historical) reads.
    CatchUp,
    /// Table (Iceberg) export writes.
    IcebergWrite,
}

impl ThrottleStrategy {
    /// Queue priority: lower is served first.
    pub fn priority(self) -> u32 {
        match self {
            ThrottleStrategy::Bypass => 0,
            ThrottleStrategy::Compaction => 1,
            ThrottleStrategy::Tail => 2,
            ThrottleStrategy::CatchUp => 3,
            ThrottleStrategy::IcebergWrite => 4,
        }
    }

    /// Metric label name.
    pub fn name(self) -> &'static str {
        match self {
            ThrottleStrategy::Bypass => "bypass",
            ThrottleStrategy::Compaction => "compaction",
            ThrottleStrategy::Tail => "tail",
            ThrottleStrategy::CatchUp => "catchup",
            ThrottleStrategy::IcebergWrite => "iceberg_write",
        }
    }
}

/// A (bucket, key) pair addressing one object.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ObjectPath {
    pub bucket_id: i16,
    pub key: String,
}

/// Listing entry: path + mtime + size.
///
/// WAL recovery parses epochs/offsets out of `key`
/// and uses `size` for v0 end-offset math. Both must be faithful.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectInfo {
    pub path: ObjectPath,
    /// Last-modified, milliseconds since epoch.
    pub timestamp_ms: i64,
    pub size: u64,
}

/// Options for write operations.
///
/// Retry bookkeeping (`retryCount`, `requestTime`)
/// owns retries and `Bytes` owns allocation.
#[derive(Debug, Clone, Default)]
pub struct WriteOptions {
    pub throttle: ThrottleStrategy,
    /// Target bucket.`None` = the storage's default bucket.
    pub bucket_id: Option<i16>,
    /// Overall operation deadline.
    pub timeout: Option<std::time::Duration>,
    pub enable_fast_retry: bool,
}

/// Options for read operations.
#[derive(Debug, Clone, Default)]
pub struct ReadOptions {
    pub throttle: ThrottleStrategy,
    /// Source bucket.`None` = the storage's default bucket.
    pub bucket_id: Option<i16>,
}

/// Result of a completed write: which bucket the object landed in.
#[derive(Debug, Clone, Copy)]
pub struct WriteResult {
    pub bucket_id: i16,
}

/// A streaming multipart writer for one object. `ObjectWriter` feeds parts
/// through this.
#[async_trait]
pub trait MultipartWriter: Send + Sync {
    /// Queue `part` for upload. Parts are uploaded pipelined, in order.
    async fn write(&mut self, part: Bytes) -> Result<(), ObjectError>;

    /// Complete the upload. Resolves when the object is durable. Must be called at
    /// most once. Write/finish after finish is a caller bug.
    async fn finish(&mut self) -> Result<WriteResult, ObjectError>;

    /// Abort the upload and release backend resources.
    async fn abort(&mut self) -> Result<(), ObjectError>;

    /// The bucket this writer targets.
    fn bucket_id(&self) -> i16;
}

/// Object storage: the only door to S3-compatible backends.
///
/// `s3.operator.ObjectStorage`. Semantics that MUST hold for any impl:
/// - `range_read` fails with `ObjectError::NotFound` for missing objects.
/// - `write` is all-or-nothing. A completed future means the object is durable.
/// - `delete` of a nonexistent key succeeds (idempotent). Impls handle provider batch
///   limits internally (AWS caps DeleteObjects at 1000 keys).
/// - `list` returns all objects under `prefix` (impls handle pagination).
#[async_trait]
pub trait ObjectStorage: Send + Sync {
    /// Liveness/permission probe used at startup.
    async fn readiness_check(&self) -> Result<(), ObjectError>;

    /// Read `[start, end)` of the object.`end = None` reads to the end.
    async fn range_read(
        &self,
        options: &ReadOptions,
        key: &str,
        start: u64,
        end: Option<u64>,
    ) -> Result<Bytes, ObjectError>;

    /// Read the whole object.
    async fn read(&self, options: &ReadOptions, key: &str) -> Result<Bytes, ObjectError> {
        self.range_read(options, key, 0, RANGE_READ_TO_END).await
    }

    /// Single-shot PUT of a complete object.
    async fn write(
        &self,
        options: &WriteOptions,
        key: &str,
        data: Bytes,
    ) -> Result<WriteResult, ObjectError>;

    /// Open a streaming multipart writer for large objects.
    async fn writer(
        &self,
        options: &WriteOptions,
        key: &str,
    ) -> Result<Box<dyn MultipartWriter>, ObjectError>;

    /// List all objects under `prefix`.
    async fn list(&self, prefix: &str) -> Result<Vec<ObjectInfo>, ObjectError>;

    /// Delete a batch of objects (idempotent, impls split provider batch limits).
    async fn delete(&self, paths: &[ObjectPath]) -> Result<(), ObjectError>;

    /// Default bucket id of this storage instance.
    fn bucket_id(&self) -> i16;
}

/// A parsed s3stream bucket URI: `{id}@{scheme}://{path}?k=v&k2=v2`.
#[derive(Debug, Clone)]
pub struct IdUri {
    pub id: i16,
    pub protocol: String,
    pub path: String,
    pub extension: Vec<(String, String)>,
}

impl IdUri {
    pub fn parse(raw: &str) -> Result<Self, ObjectError> {
        let invalid = |reason: String| ObjectError::InvalidFormat { reason };
        let (id_part, rest) = raw
            .split_once('@')
            .ok_or_else(|| invalid(format!("IdURI missing '@': {raw}")))?;
        let id: i16 = id_part
            .parse()
            .map_err(|_| invalid(format!("IdURI id not a short: {id_part}")))?;
        let (protocol, rest) = rest
            .split_once("://")
            .ok_or_else(|| invalid(format!("IdURI missing '://': {raw}")))?;
        let (path, query) = match rest.split_once('?') {
            Some((p, q)) => (p, Some(q)),
            None => (rest, None),
        };
        let mut extension = Vec::new();
        if let Some(query) = query {
            for pair in query.split('&').filter(|s| !s.is_empty()) {
                let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
                extension.push((k.to_string(), v.to_string()));
            }
        }
        Ok(Self {
            id,
            protocol: protocol.to_string(),
            path: path.to_string(),
            extension,
        })
    }

    /// First value of a query parameter.
    pub fn extension_str(&self, key: &str) -> Option<&str> {
        self.extension
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.as_str())
    }

    pub fn extension_bool(&self, key: &str, default: bool) -> bool {
        self.extension_str(key)
            .map(|v| v.eq_ignore_ascii_case("true"))
            .unwrap_or(default)
    }
}

fn backend_error(key: &str, e: object_store::Error) -> ObjectError {
    match e {
        object_store::Error::NotFound { .. } => ObjectError::NotFound {
            key: key.to_string(),
        },
        other => ObjectError::Backend(other),
    }
}

/// Production adapter over the `object_store` crate.
/// One bounded retry pass per request is `object_store`'s job. Unbounded
/// retries and inflight limits belong to `RetryingObjectStorage` above.
pub struct ObjectStoreAdapter {
    inner: std::sync::Arc<dyn object_store::ObjectStore>,
    bucket_id: i16,
}

impl ObjectStoreAdapter {
    /// Build from an s3stream bucket URI (`IdURI` format: `{id}@{scheme}://{bucket}?k=v`).
    ///
    /// Supported protocols:
    /// - `s3`: AWS S3 or compatible. Honors `region`, `endpoint`, `pathStyle`,
    ///   `s3Express` query builder chain).
    /// - `file`: local filesystem rooted at the path (dev/test).
    /// - `mem`: in-memory backend (tests).
    pub fn from_bucket_uri(uri: &str) -> Result<Self, ObjectError> {
        let uri = IdUri::parse(uri)?;
        let inner: std::sync::Arc<dyn object_store::ObjectStore> = match uri.protocol.as_str() {
            "s3" => std::sync::Arc::new(s3_builder(&uri).build().map_err(ObjectError::Backend)?),
            "file" => std::sync::Arc::new(
                object_store::local::LocalFileSystem::new_with_prefix(&uri.path)
                    .map_err(ObjectError::Backend)?,
            ),
            "mem" => std::sync::Arc::new(object_store::memory::InMemory::new()),
            other => {
                return Err(ObjectError::InvalidFormat {
                    reason: format!("unsupported bucket protocol: {other}"),
                });
            }
        };
        Ok(Self {
            inner,
            bucket_id: uri.id,
        })
    }

    pub fn new(inner: std::sync::Arc<dyn object_store::ObjectStore>, bucket_id: i16) -> Self {
        Self { inner, bucket_id }
    }

    pub fn object_store(&self) -> std::sync::Arc<dyn object_store::ObjectStore> {
        self.inner.clone()
    }
}

/// AWS directory bucket names carry this reserved suffix
/// (`{base}--{zone-id}--x-s3`). Standard buckets cannot use it, so it is a
/// safe signal to switch to S3 Express session auth, matching AWS SDK
/// behavior. `s3Express=true` forces it explicitly.
const S3_EXPRESS_BUCKET_SUFFIX: &str = "--x-s3";

fn s3_builder(uri: &IdUri) -> object_store::aws::AmazonS3Builder {
    let mut builder = object_store::aws::AmazonS3Builder::from_env().with_bucket_name(&uri.path);
    if let Some(region) = uri.extension_str("region") {
        builder = builder.with_region(region);
    }
    if let Some(endpoint) = uri.extension_str("endpoint") {
        builder = builder.with_endpoint(endpoint);
        if endpoint.starts_with("http://") {
            builder = builder.with_allow_http(true);
        }
    }
    if uri.extension_bool("pathStyle", false) {
        builder = builder.with_virtual_hosted_style_request(false);
    }
    if uri.extension_bool("s3Express", false) || uri.path.ends_with(S3_EXPRESS_BUCKET_SUFFIX) {
        builder = builder.with_s3_express(true);
    }
    builder
}

/// Multipart writer over `object_store::MultipartUpload`.
///
/// Parts are uploaded as they arrive (the `ObjectWriter` above already enforces the
/// backend's 5 MiB minimum part size via `MIN_PART_SIZE` clamping).
struct AdapterMultipartWriter {
    // Mutex only to make the writer `Sync` (object_store's `dyn MultipartUpload` is
    // Send but not Sync). All access is through `&mut self`, so it never contends.
    upload: tokio::sync::Mutex<Box<dyn object_store::MultipartUpload>>,
    bucket_id: i16,
    key: String,
}

#[async_trait]
impl MultipartWriter for AdapterMultipartWriter {
    async fn write(&mut self, part: Bytes) -> Result<(), ObjectError> {
        self.upload
            .get_mut()
            .put_part(object_store::PutPayload::from_bytes(part))
            .await
            .map_err(|e| backend_error(&self.key, e))
    }

    async fn finish(&mut self) -> Result<WriteResult, ObjectError> {
        self.upload
            .get_mut()
            .complete()
            .await
            .map_err(|e| backend_error(&self.key, e))?;
        Ok(WriteResult {
            bucket_id: self.bucket_id,
        })
    }

    async fn abort(&mut self) -> Result<(), ObjectError> {
        self.upload
            .get_mut()
            .abort()
            .await
            .map_err(|e| backend_error(&self.key, e))
    }

    fn bucket_id(&self) -> i16 {
        self.bucket_id
    }
}

#[async_trait]
impl ObjectStorage for ObjectStoreAdapter {
    async fn readiness_check(&self) -> Result<(), ObjectError> {
        let key = format!("__pico/readiness_check/{}", std::process::id());
        let path = object_store::path::Path::from(key.as_str());
        self.inner
            .put(&path, object_store::PutPayload::from_static(b"ok"))
            .await
            .map_err(|e| backend_error(&key, e))?;
        self.inner
            .delete(&path)
            .await
            .map_err(|e| backend_error(&key, e))?;
        Ok(())
    }

    async fn range_read(
        &self,
        _options: &ReadOptions,
        key: &str,
        start: u64,
        end: Option<u64>,
    ) -> Result<Bytes, ObjectError> {
        let path = object_store::path::Path::from(key);
        match end {
            Some(end) => self
                .inner
                .get_range(&path, start..end)
                .await
                .map_err(|e| backend_error(key, e)),
            None => {
                let options = object_store::GetOptions {
                    range: (start > 0).then_some(object_store::GetRange::Offset(start)),
                    ..Default::default()
                };
                let result = self
                    .inner
                    .get_opts(&path, options)
                    .await
                    .map_err(|e| backend_error(key, e))?;
                result.bytes().await.map_err(|e| backend_error(key, e))
            }
        }
    }

    async fn write(
        &self,
        _options: &WriteOptions,
        key: &str,
        data: Bytes,
    ) -> Result<WriteResult, ObjectError> {
        let path = object_store::path::Path::from(key);
        self.inner
            .put(&path, object_store::PutPayload::from_bytes(data))
            .await
            .map_err(|e| backend_error(key, e))?;
        Ok(WriteResult {
            bucket_id: self.bucket_id,
        })
    }

    async fn writer(
        &self,
        _options: &WriteOptions,
        key: &str,
    ) -> Result<Box<dyn MultipartWriter>, ObjectError> {
        let path = object_store::path::Path::from(key);
        let upload = self
            .inner
            .put_multipart(&path)
            .await
            .map_err(|e| backend_error(key, e))?;
        Ok(Box::new(AdapterMultipartWriter {
            upload: tokio::sync::Mutex::new(upload),
            bucket_id: self.bucket_id,
            key: key.to_string(),
        }))
    }

    async fn list(&self, prefix: &str) -> Result<Vec<ObjectInfo>, ObjectError> {
        use futures::TryStreamExt;
        let path = object_store::path::Path::from(prefix);
        let metas: Vec<object_store::ObjectMeta> = self
            .inner
            .list(Some(&path))
            .try_collect()
            .await
            .map_err(|e| backend_error(prefix, e))?;
        Ok(metas
            .into_iter()
            .map(|meta| ObjectInfo {
                path: ObjectPath {
                    bucket_id: self.bucket_id,
                    key: meta.location.to_string(),
                },
                timestamp_ms: meta.last_modified.timestamp_millis(),
                size: meta.size,
            })
            .collect())
    }

    async fn delete(&self, paths: &[ObjectPath]) -> Result<(), ObjectError> {
        for object_path in paths {
            let path = object_store::path::Path::from(object_path.key.as_str());
            match self.inner.delete(&path).await {
                Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
                Err(e) => return Err(backend_error(&object_path.key, e)),
            }
        }
        Ok(())
    }

    fn bucket_id(&self) -> i16 {
        self.bucket_id
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory::MemoryObjectStorage;

    /// Contract every ObjectStorage impl must satisfy:
    /// - range_read of missing key => NotFound
    /// - write then read => identical bytes. Range_read honors [start, end)
    /// - list returns exactly the written keys under prefix
    /// - delete is idempotent
    /// - multipart writer concatenates parts in order
    async fn contract(storage: &dyn ObjectStorage) {
        // Missing key.
        let err = storage
            .read(&ReadOptions::default(), "contract/missing")
            .await
            .unwrap_err();
        assert!(matches!(err, ObjectError::NotFound { .. }));

        // Write/read round trip + range semantics.
        let data = Bytes::from((0u16..=255).map(|b| b as u8).collect::<Vec<_>>());
        storage
            .write(&WriteOptions::default(), "contract/a/1", data.clone())
            .await
            .unwrap();
        let read = storage
            .read(&ReadOptions::default(), "contract/a/1")
            .await
            .unwrap();
        assert_eq!(read, data);
        let range = storage
            .range_read(&ReadOptions::default(), "contract/a/1", 10, Some(20))
            .await
            .unwrap();
        assert_eq!(range.as_ref(), &data[10..20]);
        let tail = storage
            .range_read(&ReadOptions::default(), "contract/a/1", 250, None)
            .await
            .unwrap();
        assert_eq!(tail.as_ref(), &data[250..]);

        // Listing under a prefix.
        storage
            .write(&WriteOptions::default(), "contract/a/2", data.clone())
            .await
            .unwrap();
        storage
            .write(&WriteOptions::default(), "contract/b/1", data.clone())
            .await
            .unwrap();
        let mut listed: Vec<String> = storage
            .list("contract/a")
            .await
            .unwrap()
            .into_iter()
            .map(|o| o.path.key)
            .collect();
        listed.sort();
        assert_eq!(
            listed,
            vec!["contract/a/1".to_string(), "contract/a/2".to_string()]
        );
        let sizes: Vec<u64> = storage
            .list("contract/a")
            .await
            .unwrap()
            .into_iter()
            .map(|o| o.size)
            .collect();
        assert_eq!(sizes, vec![data.len() as u64; 2]);

        // Multipart writer.
        let mut writer = storage
            .writer(&WriteOptions::default(), "contract/mp")
            .await
            .unwrap();
        writer.write(Bytes::from_static(b"part1-")).await.unwrap();
        writer.write(Bytes::from_static(b"part2")).await.unwrap();
        writer.finish().await.unwrap();
        let read = storage
            .read(&ReadOptions::default(), "contract/mp")
            .await
            .unwrap();
        assert_eq!(read.as_ref(), b"part1-part2");

        // Idempotent delete.
        let paths = vec![
            ObjectPath {
                bucket_id: storage.bucket_id(),
                key: "contract/a/1".into(),
            },
            ObjectPath {
                bucket_id: storage.bucket_id(),
                key: "contract/nonexistent".into(),
            },
        ];
        storage.delete(&paths).await.unwrap();
        storage.delete(&paths).await.unwrap();
        let err = storage
            .read(&ReadOptions::default(), "contract/a/1")
            .await
            .unwrap_err();
        assert!(matches!(err, ObjectError::NotFound { .. }));
    }

    #[tokio::test]
    async fn adapter_contract() {
        let adapter = ObjectStoreAdapter::from_bucket_uri("3@mem://test").unwrap();
        assert_eq!(adapter.bucket_id(), 3);
        adapter.readiness_check().await.unwrap();
        contract(&adapter).await;
    }

    #[tokio::test]
    async fn memory_storage_contract() {
        let storage = MemoryObjectStorage::new(0);
        contract(&storage).await;
    }

    #[test]
    fn s3_express_enabled_by_param() {
        let uri = IdUri::parse("0@s3://my-bucket?region=us-east-1&s3Express=true").unwrap();
        let builder = s3_builder(&uri);
        assert_eq!(
            builder.get_config_value(&object_store::aws::AmazonS3ConfigKey::S3Express),
            Some("true".into())
        );
        assert_eq!(
            builder.get_config_value(&object_store::aws::AmazonS3ConfigKey::Region),
            Some("us-east-1".into())
        );
    }

    #[test]
    fn s3_express_enabled_by_directory_bucket_name() {
        let uri = IdUri::parse("0@s3://my-wal--use1-az4--x-s3?region=us-east-1").unwrap();
        let builder = s3_builder(&uri);
        assert_eq!(
            builder.get_config_value(&object_store::aws::AmazonS3ConfigKey::S3Express),
            Some("true".into())
        );
    }

    #[test]
    fn s3_express_off_for_standard_bucket() {
        let uri = IdUri::parse(
            "0@s3://my-bucket?region=us-east-1&endpoint=http://127.0.0.1:9000&pathStyle=true",
        )
        .unwrap();
        let builder = s3_builder(&uri);
        let express = builder
            .get_config_value(&object_store::aws::AmazonS3ConfigKey::S3Express)
            .unwrap_or_else(|| "false".into());
        assert_eq!(express, "false");
        assert_eq!(
            builder.get_config_value(&object_store::aws::AmazonS3ConfigKey::Endpoint),
            Some("http://127.0.0.1:9000".into())
        );
    }

    #[test]
    fn id_uri_parses_java_format() {
        let uri = IdUri::parse(
            "0@s3://my-bucket?region=us-east-1&endpoint=http://127.0.0.1:9000&pathStyle=true",
        )
        .unwrap();
        assert_eq!(uri.id, 0);
        assert_eq!(uri.protocol, "s3");
        assert_eq!(uri.path, "my-bucket");
        assert_eq!(uri.extension_str("region"), Some("us-east-1"));
        assert_eq!(uri.extension_str("endpoint"), Some("http://127.0.0.1:9000"));
        assert!(uri.extension_bool("pathStyle", false));

        assert!(IdUri::parse("no-id-here").is_err());
        assert!(IdUri::parse("x@s3://b").is_err());
    }
}

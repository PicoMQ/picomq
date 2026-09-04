//! Object format and object-storage abstraction for s3stream.
//!
//! Contains the on-object byte format (data blocks, index block, footer, composite
//! objects. See `specification/object-format.md`), object key/metadata types, and the
//! `ObjectStorage` trait that everything above uses to talk to S3-compatible storage.
//!
//! Layering rule: crates above (`s3stream-wal`, `s3stream-core`) depend on the
//! `ObjectStorage` trait here and never on `object_store` directly, so test backends
//! (in-memory, fault-injecting, deterministic-simulation) are swappable everywhere.

pub mod composite;
pub mod error;
pub mod index;
pub mod memory;
pub mod metadata;
pub mod reader;
pub mod retry;
pub mod storage;
pub mod writer;

pub use error::ObjectError;
pub use index::{BLOCK_INDEX_SIZE, DataBlockIndex, FindIndexResult, IndexBlock};
pub use memory::MemoryObjectStorage;
pub use metadata::{
    NOOP_OBJECT_ID, NOOP_OFFSET, ObjectAttributes, S3ObjectMetadata, S3ObjectType,
    StreamOffsetRange, gen_index_key, gen_index_key_in, gen_object_key,
};
pub use reader::{ObjectReader, decode_data_block};
pub use retry::{RetryConfig, RetryingObjectStorage};
pub use storage::{
    IdUri, MultipartWriter, ObjectInfo, ObjectPath, ObjectStorage, ObjectStoreAdapter, ReadOptions,
    ThrottleStrategy, WriteOptions, WriteResult,
};
pub use writer::{FOOTER_MAGIC, FOOTER_SIZE, ObjectStreamRange, ObjectWriter};

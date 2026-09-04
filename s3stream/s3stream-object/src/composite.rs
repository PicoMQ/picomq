//! Composite objects: one logical object linking the data blocks of component objects.
//!
//! `CompositeObjectReader`.
//!
//! Used by compaction to merge many small stream objects into one logical object
//! without rewriting data: the composite holds an objects block (which component owns
//! which block ordinals), an indexes block (the components' `DataBlockIndex` entries,
//! positions still relative to their component object), and a footer.

use std::sync::Arc;

use bytes::{BufMut, Bytes, BytesMut};
use tokio::sync::OnceCell;

use crate::error::ObjectError;
use crate::index::{BLOCK_INDEX_SIZE, DataBlockIndex, FindIndexResult, IndexBlock};
use crate::metadata::{S3ObjectMetadata, gen_object_key};
use crate::storage::{ObjectPath, ObjectStorage, ReadOptions, WriteOptions};
use crate::writer::FOOTER_SIZE;

pub const OBJECTS_BLOCK_MAGIC: u8 = 0x52;
/// Layout: magic(1) + count(4).
pub const OBJECT_BLOCK_HEADER_SIZE: usize = 5;
/// Layout: object_id(8) + block_start_index(4) + bucket_id(2).
pub const OBJECT_UNIT_SIZE: usize = 14;
pub const COMPOSITE_FOOTER_MAGIC: u64 = 0x88E2_41B7_85F4_CFF8;

/// One linked component: which object owns block ordinals starting at `block_start_index`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectIndex {
    pub object_id: u64,
    pub block_start_index: u32,
    pub block_end_index: u32,
    pub bucket_id: i16,
}

/// Writer for a composite object.
///
/// `CompositeObjectWriter`. Components must be added in stream-offset order and
/// must be offset-continuous within a single stream (`continuousCheck`).
pub struct CompositeObjectWriter {
    storage_key: String,
    options: WriteOptions,
    components: Vec<(S3ObjectMetadata, Vec<DataBlockIndex>)>,
    next_block_start_index: u32,
    retained_size: u64,
    /// Continuity cursor: (stream_id, next_expected_offset, first_offset).
    cursor: Option<(u64, u64, u64)>,
}

impl CompositeObjectWriter {
    /// Storage key is `gen_object_key(0, object_id)`.
    pub fn new(object_id: u64, options: WriteOptions) -> Self {
        Self {
            storage_key: gen_object_key(0, object_id),
            options,
            components: Vec::new(),
            next_block_start_index: 0,
            retained_size: 0,
            cursor: None,
        }
    }

    /// Link a component object's blocks into the composite.
    pub fn add_component(
        &mut self,
        component: &S3ObjectMetadata,
        block_indexes: Vec<DataBlockIndex>,
    ) -> Result<(), ObjectError> {
        for index in &block_indexes {
            match self.cursor {
                None => {
                    self.cursor = Some((index.stream_id, index.end_offset(), index.start_offset));
                }
                Some((stream_id, expect_offset, first)) => {
                    if stream_id != index.stream_id || expect_offset != index.start_offset {
                        return Err(ObjectError::OrderingViolation {
                            reason: format!(
                                "invalid index {index:?}, expect streamId={stream_id}, offset={expect_offset}"
                            ),
                        });
                    }
                    self.cursor = Some((stream_id, index.end_offset(), first));
                }
            }
        }
        self.retained_size += block_indexes.iter().map(|i| i.size as u64).sum::<u64>();
        self.components.push((component.clone(), block_indexes));
        self.next_block_start_index += self.components.last().unwrap().1.len() as u32;
        Ok(())
    }

    /// Write objects block + indexes block + footer as one object.
    ///
    /// Returns the physical composite size.
    pub async fn close(&mut self, storage: &dyn ObjectStorage) -> Result<u64, ObjectError> {
        let objects_block_size =
            OBJECT_BLOCK_HEADER_SIZE + OBJECT_UNIT_SIZE * self.components.len();
        let indexes_count: usize = self.components.iter().map(|(_, i)| i.len()).sum();
        let indexes_block_size = BLOCK_INDEX_SIZE * indexes_count;
        let mut buf =
            BytesMut::with_capacity(objects_block_size + indexes_block_size + FOOTER_SIZE);

        // Objects block.
        buf.put_u8(OBJECTS_BLOCK_MAGIC);
        buf.put_u32(self.components.len() as u32);
        let mut block_start_index = 0u32;
        for (metadata, indexes) in &self.components {
            buf.put_u64(metadata.object_id);
            buf.put_u32(block_start_index);
            buf.put_i16(metadata.attributes.bucket_id());
            block_start_index += indexes.len() as u32;
        }

        // Indexes block (positions stay relative to the component objects).
        for (_, indexes) in &self.components {
            for index in indexes {
                index.encode(&mut buf);
            }
        }

        // Footer.
        buf.put_u64(objects_block_size as u64);
        buf.put_u32(indexes_block_size as u32);
        buf.put_bytes(0, 40 - 8 - 4);
        buf.put_u64(COMPOSITE_FOOTER_MAGIC);

        let total = buf.len() as u64;
        storage
            .write(&self.options, &self.storage_key, buf.freeze())
            .await?;
        Ok(total)
    }

    /// Retained size: sum of linked component block sizes, NOT the physical size of the
    /// composite object itself.
    pub fn size(&self) -> u64 {
        self.retained_size
    }

    /// The single continuous stream range covered so far, if any component was added.
    ///
    /// (always 0 or 1 range because of
    /// the continuity check).
    pub fn stream_range(&self) -> Option<(u64, u64, u64)> {
        self.cursor
            .map(|(stream_id, next, first)| (stream_id, first, next))
    }
}

/// Parsed composite object info.
#[derive(Debug, Clone)]
pub struct CompositeObjectInfo {
    pub objects: Vec<ObjectIndex>,
    pub index_block: IndexBlock,
}

impl CompositeObjectInfo {
    /// Resolve the component owning block ordinal `block_id`.
    pub fn component_of(&self, block_id: u32) -> Result<&ObjectIndex, ObjectError> {
        self.objects
            .iter()
            .find(|o| o.block_start_index <= block_id && block_id < o.block_end_index)
            .ok_or_else(|| ObjectError::InvalidFormat {
                reason: format!("no component owns block ordinal {block_id}"),
            })
    }
}

/// Reader for a composite object: resolves block reads to the owning component object.
pub struct CompositeObjectReader {
    metadata: S3ObjectMetadata,
    storage: Arc<dyn ObjectStorage>,
    read_options: ReadOptions,
    info: OnceCell<CompositeObjectInfo>,
}

impl CompositeObjectReader {
    pub fn new(metadata: S3ObjectMetadata, storage: Arc<dyn ObjectStorage>) -> Self {
        let read_options = ReadOptions {
            bucket_id: Some(metadata.attributes.bucket_id()),
            ..Default::default()
        };
        Self {
            metadata,
            storage,
            read_options,
            info: OnceCell::new(),
        }
    }

    /// Fetch and parse the whole composite object in one ranged GET (they are
    /// small: index-only).
    pub async fn info(&self) -> Result<&CompositeObjectInfo, ObjectError> {
        self.info
            .get_or_try_init(|| async {
                let buf = self
                    .storage
                    .range_read(&self.read_options, &self.metadata.key(), 0, None)
                    .await?;
                parse_composite(&buf)
            })
            .await
    }

    pub async fn find(
        &self,
        stream_id: u64,
        start_offset: u64,
        end_offset: u64,
        max_bytes: usize,
    ) -> Result<FindIndexResult, ObjectError> {
        let info = self.info().await?;
        Ok(info
            .index_block
            .find(stream_id, start_offset, end_offset, max_bytes))
    }

    /// Range-GET one linked block's bytes from its owning component object.
    pub async fn read_block(&self, index: &DataBlockIndex) -> Result<Bytes, ObjectError> {
        let info = self.info().await?;
        let component = info.component_of(index.block_id as u32)?;
        let options = ReadOptions {
            bucket_id: Some(component.bucket_id),
            ..Default::default()
        };
        self.storage
            .range_read(
                &options,
                &gen_object_key(0, component.object_id),
                index.start_position,
                Some(index.end_position()),
            )
            .await
    }

    pub fn metadata(&self) -> &S3ObjectMetadata {
        &self.metadata
    }
}

/// Parse composite object bytes into objects + index blocks.
fn parse_composite(buf: &Bytes) -> Result<CompositeObjectInfo, ObjectError> {
    if buf.len() < FOOTER_SIZE + OBJECT_BLOCK_HEADER_SIZE {
        return Err(ObjectError::InvalidFormat {
            reason: format!("composite object too small: {} bytes", buf.len()),
        });
    }
    let n = buf.len();
    let magic = u64::from_be_bytes(buf[n - 8..n].try_into().unwrap());
    if magic != COMPOSITE_FOOTER_MAGIC {
        return Err(ObjectError::InvalidFormat {
            reason: format!("composite footer magic mismatch: {magic:#x}"),
        });
    }
    let index_position =
        u64::from_be_bytes(buf[n - FOOTER_SIZE..n - 40].try_into().unwrap()) as usize;
    let index_size = u32::from_be_bytes(buf[n - 40..n - 36].try_into().unwrap()) as usize;
    if index_position + index_size + FOOTER_SIZE != n {
        return Err(ObjectError::InvalidFormat {
            reason: format!("composite geometry: {index_position}+{index_size}+footer != {n}"),
        });
    }
    let index_block = IndexBlock::parse(&buf.slice(index_position..index_position + index_size))?;
    let total_blocks = index_block.entries().len() as u32;

    if buf[0] != OBJECTS_BLOCK_MAGIC {
        return Err(ObjectError::InvalidFormat {
            reason: format!("objects block magic mismatch: {:#x}", buf[0]),
        });
    }
    let count = u32::from_be_bytes(buf[1..5].try_into().unwrap()) as usize;
    if OBJECT_BLOCK_HEADER_SIZE + count * OBJECT_UNIT_SIZE != index_position {
        return Err(ObjectError::InvalidFormat {
            reason: format!("objects block size mismatch: {count} components"),
        });
    }
    let mut objects = Vec::with_capacity(count);
    for i in 0..count {
        let base = OBJECT_BLOCK_HEADER_SIZE + i * OBJECT_UNIT_SIZE;
        let object_id = u64::from_be_bytes(buf[base..base + 8].try_into().unwrap());
        let block_start_index = u32::from_be_bytes(buf[base + 8..base + 12].try_into().unwrap());
        let bucket_id = i16::from_be_bytes(buf[base + 12..base + 14].try_into().unwrap());
        let block_end_index = if i < count - 1 {
            let next_base = base + OBJECT_UNIT_SIZE;
            u32::from_be_bytes(buf[next_base + 8..next_base + 12].try_into().unwrap())
        } else {
            total_blocks
        };
        objects.push(ObjectIndex {
            object_id,
            block_start_index,
            block_end_index,
            bucket_id,
        });
    }
    Ok(CompositeObjectInfo {
        objects,
        index_block,
    })
}

/// Delete a composite object: delete all linked components first, then the composite
/// itself.
///
/// (the DEEP_DELETE attribute decides whether callers
/// invoke this or a plain delete. See `s3.objects` deletion paths).
pub async fn delete_composite(
    metadata: &S3ObjectMetadata,
    storage: Arc<dyn ObjectStorage>,
) -> Result<(), ObjectError> {
    let reader = CompositeObjectReader::new(metadata.clone(), storage.clone());
    let info = reader.info().await?;
    let component_paths: Vec<ObjectPath> = info
        .objects
        .iter()
        .map(|o| ObjectPath {
            bucket_id: o.bucket_id,
            key: gen_object_key(0, o.object_id),
        })
        .collect();
    storage.delete(&component_paths).await?;
    storage
        .delete(&[ObjectPath {
            bucket_id: metadata.attributes.bucket_id(),
            key: metadata.key(),
        }])
        .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory::MemoryObjectStorage;
    use crate::metadata::{ObjectAttributes, S3ObjectType};
    use crate::reader::decode_data_block;
    use crate::writer::ObjectWriter;
    use s3stream_codec::StreamRecordBatch;

    fn record(stream_id: u64, base_offset: u64, payload: &[u8]) -> StreamRecordBatch {
        StreamRecordBatch::new(stream_id, 1, base_offset, 1, payload.to_vec().into())
    }

    fn object_metadata(object_id: u64, object_size: u64, composite: bool) -> S3ObjectMetadata {
        S3ObjectMetadata {
            object_id,
            object_type: S3ObjectType::Stream,
            offset_ranges: vec![],
            object_size,
            attributes: ObjectAttributes::new(0, composite, composite),
            committed_timestamp_ms: 0,
            data_timestamp_ms: 0,
        }
    }

    /// Composite round trip: write two component objects, link them, read through the
    /// composite reader, verify records and component resolution.
    #[tokio::test]
    async fn composite_round_trip() {
        let storage = Arc::new(MemoryObjectStorage::new(0));

        // Component 1: stream 5, offsets [0, 10). Component 2: stream 5, offsets [10, 20).
        let mut component_indexes = Vec::new();
        let mut all_records = Vec::new();
        for (object_id, base) in [(100u64, 0u64), (101, 10)] {
            let records: Vec<StreamRecordBatch> = (0..10)
                .map(|i| record(5, base + i, format!("c{object_id}-{i}").as_bytes()))
                .collect();
            let mut writer = ObjectWriter::open(
                object_id,
                storage.as_ref(),
                64,
                16 << 20,
                WriteOptions::default(),
            )
            .await
            .unwrap();
            writer.write(5, &records).await.unwrap();
            let size = writer.close().await.unwrap();
            component_indexes.push((
                object_metadata(object_id, size, false),
                writer.block_indexes(),
            ));
            all_records.extend(records);
        }

        // Link into composite object 200.
        let mut composite = CompositeObjectWriter::new(200, WriteOptions::default());
        for (metadata, indexes) in &component_indexes {
            composite.add_component(metadata, indexes.clone()).unwrap();
        }
        assert!(composite.size() > 0);
        let composite_size = composite.close(storage.as_ref()).await.unwrap();

        // Non-continuous component must be rejected.
        let mut bad = CompositeObjectWriter::new(201, WriteOptions::default());
        bad.add_component(&component_indexes[0].0, component_indexes[0].1.clone())
            .unwrap();
        let err = bad
            .add_component(&component_indexes[0].0, component_indexes[0].1.clone())
            .unwrap_err();
        assert!(matches!(err, ObjectError::OrderingViolation { .. }));

        // Read back through the composite reader.
        let reader =
            CompositeObjectReader::new(object_metadata(200, composite_size, true), storage.clone());
        let found = reader.find(5, 0, 20, usize::MAX).await.unwrap();
        assert!(found.fulfilled);
        let mut read_back = Vec::new();
        for index in &found.blocks {
            let block = reader.read_block(index).await.unwrap();
            read_back.extend(decode_data_block(&block).unwrap());
        }
        assert_eq!(read_back, all_records);

        // Component resolution: first ordinal in component 100, last in 101.
        let info = reader.info().await.unwrap();
        assert_eq!(info.component_of(0).unwrap().object_id, 100);
        let last = info.index_block.entries().len() as u32 - 1;
        assert_eq!(info.component_of(last).unwrap().object_id, 101);

        // Deep delete removes components and the composite itself.
        let composite_metadata = object_metadata(200, composite_size, true);
        delete_composite(&composite_metadata, storage.clone())
            .await
            .unwrap();
        for object_id in [100u64, 101, 200] {
            let err = storage
                .read(&ReadOptions::default(), &gen_object_key(0, object_id))
                .await
                .unwrap_err();
            assert!(
                matches!(err, ObjectError::NotFound { .. }),
                "object {object_id} not deleted"
            );
        }
    }
}

//! WAL object key scheme and listing/parsing.
//!
//! `WALObject`.
//! Specification: `specification/wal-protocol.md` (key scheme section).

use md5::{Digest, Md5};

use s3stream_object::ObjectInfo;

use super::header::{TRIM_OFFSET_NONE, calculate_end_offset_v0};

/// WAL data files align to 64 MiB boundaries.
pub const DATA_FILE_ALIGN_SIZE: u64 = 64 * 1024 * 1024;

/// Delimiter between start and end offset in v1 keys.
pub const OBJECT_PATH_OFFSET_DELIMITER: char = '-';

/// Namespace prefix used in all node paths.
pub const DEFAULT_NAMESPACE: &str = "_kafka_";

/// Sentinel stream id / epoch of the fake record appended by `trim` to persist the
///
/// `).
pub const TRIM_RECORD_SENTINEL: u64 = u64::MAX;

/// One WAL object parsed from a listing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalObject {
    pub bucket_id: i16,
    pub key: String,
    pub epoch: u64,
    pub start_offset: u64,
    pub end_offset: u64,
    pub size: u64,
}

/// Node prefix all of a node's WAL objects live under:
/// `md5hex(nodeId).uppercase() + "/" + namespace + clusterId + "/" + nodeId["_"+type] + "/"`.
///
/// `ObjectUtils#nodePrefix`. Must be byte-identical to Java (mixed clusters,
/// failover readers).
pub fn node_prefix(cluster_id: &str, node_id: u32, wal_type: Option<&str>) -> String {
    let digest = Md5::digest(node_id.to_string().as_bytes());
    let md5_hex = hex::encode_upper(digest);
    let type_suffix = match wal_type {
        Some(t) if !t.trim().is_empty() => format!("_{t}"),
        _ => String::new(),
    };
    format!("{md5_hex}/{DEFAULT_NAMESPACE}{cluster_id}/{node_id}{type_suffix}/")
}

/// Key for a v1 WAL object: `{nodePrefix}{epoch}/wal/{startOffset}-{endOffset}`.
pub fn gen_object_path_v1(
    node_prefix: &str,
    epoch: u64,
    start_offset: u64,
    end_offset: u64,
) -> String {
    format!("{node_prefix}{epoch}/wal/{start_offset}{OBJECT_PATH_OFFSET_DELIMITER}{end_offset}")
}

pub fn gen_object_path_v1_aligned(node_prefix: &str, epoch: u64, start_offset: u64) -> String {
    gen_object_path_v1(
        node_prefix,
        epoch,
        start_offset,
        start_offset + DATA_FILE_ALIGN_SIZE,
    )
}

/// Parse a listing into WAL objects, tolerating foreign keys (skip + warn),
/// sorted by `(epoch, start_offset)`. Supports both v0 (`{start}`) and v1
/// (`{start}-{end}`) key forms. V0 end offset is derived from the object
/// size.
pub fn parse_wal_objects(objects: Vec<ObjectInfo>) -> Vec<WalObject> {
    let mut wal_objects = Vec::with_capacity(objects.len());
    for object in objects {
        let key = object.path.key;
        let parts: Vec<&str> = key.split('/').collect();
        if parts.len() < 3 {
            tracing::warn!(key, "invalid WAL object");
            continue;
        }
        let epoch: Option<u64> = parts[parts.len() - 3].parse().ok();
        let raw_offset = parts[parts.len() - 1];
        let parsed = epoch.and_then(|epoch| {
            if let Some((start, end)) = raw_offset.split_once(OBJECT_PATH_OFFSET_DELIMITER) {
                let start_offset: u64 = start.parse().ok()?;
                let end_offset: u64 = end.parse().ok()?;
                Some(WalObject {
                    bucket_id: object.path.bucket_id,
                    key: key.clone(),
                    epoch,
                    start_offset,
                    end_offset,
                    size: object.size,
                })
            } else {
                let start_offset: u64 = raw_offset.parse().ok()?;
                Some(WalObject {
                    bucket_id: object.path.bucket_id,
                    key: key.clone(),
                    epoch,
                    start_offset,
                    end_offset: calculate_end_offset_v0(start_offset, object.size),
                    size: object.size,
                })
            }
        });
        match parsed {
            Some(wal_object) => wal_objects.push(wal_object),
            None => tracing::warn!(key, "invalid WAL object"),
        }
    }
    wal_objects.sort_by(|a, b| {
        a.epoch
            .cmp(&b.epoch)
            .then(a.start_offset.cmp(&b.start_offset))
    });
    wal_objects
}

/// Drop older-epoch objects that overlap newer-epoch ranges (dirty writes from a fenced
/// zombie writer). Returns the dropped objects.
///
/// Quirk kept for compatibility: only the FIRST object in the
/// (epoch, start_offset)-sorted list is ever considered for removal, and it
/// may appear multiple times in the returned list (deletion is idempotent).
pub fn skip_overlap_objects(objects: &mut Vec<WalObject>) -> Vec<WalObject> {
    let mut overlap_objects: Vec<WalObject> = Vec::new();
    {
        let mut last_object: Option<&WalObject> = None;
        for object in objects.iter() {
            let Some(last) = last_object else {
                last_object = Some(object);
                continue;
            };
            if last.epoch != object.epoch && last.end_offset > object.start_offset {
                overlap_objects.push(last.clone());
            }
        }
    }
    for overlap in &overlap_objects {
        if let Some(pos) = objects.iter().position(|o| o == overlap) {
            objects.remove(pos);
        }
    }
    overlap_objects
}

/// Round `offset` down / up to the 64 MiB alignment boundary.
pub fn floor_align_offset(offset: u64) -> u64 {
    offset / DATA_FILE_ALIGN_SIZE * DATA_FILE_ALIGN_SIZE
}
pub fn ceil_align_offset(offset: u64) -> u64 {
    offset.div_ceil(DATA_FILE_ALIGN_SIZE) * DATA_FILE_ALIGN_SIZE
}

/// Latest trim offset semantics helper: `TRIM_OFFSET_NONE` (-1) means never trimmed.
pub use super::header::TRIM_OFFSET_NONE as NO_TRIM_OFFSET;
const _: () = assert!(TRIM_OFFSET_NONE == -1);

#[cfg(test)]
mod tests {
    use super::*;
    use s3stream_object::{ObjectInfo, ObjectPath};

    fn info(key: &str, size: u64) -> ObjectInfo {
        ObjectInfo {
            path: ObjectPath {
                bucket_id: 0,
                key: key.to_string(),
            },
            timestamp_ms: 0,
            size,
        }
    }

    #[test]
    fn align_math_matches_java() {
        assert_eq!(floor_align_offset(0), 0);
        assert_eq!(floor_align_offset(DATA_FILE_ALIGN_SIZE - 1), 0);
        assert_eq!(
            floor_align_offset(DATA_FILE_ALIGN_SIZE),
            DATA_FILE_ALIGN_SIZE
        );
        assert_eq!(ceil_align_offset(0), 0);
        assert_eq!(ceil_align_offset(1), DATA_FILE_ALIGN_SIZE);
        assert_eq!(
            ceil_align_offset(DATA_FILE_ALIGN_SIZE),
            DATA_FILE_ALIGN_SIZE
        );
    }

    #[test]
    fn node_prefix_matches_java() {
        assert_eq!(
            node_prefix("cluster", 1, None),
            "C4CA4238A0B923820DCC509A6F75849B/_kafka_cluster/1/"
        );
        assert_eq!(
            node_prefix("cluster", 1, Some("snapshot")),
            "C4CA4238A0B923820DCC509A6F75849B/_kafka_cluster/1_snapshot/"
        );
        assert_eq!(
            node_prefix("cluster", 1, Some(" ")),
            node_prefix("cluster", 1, None)
        );
    }

    /// Golden prefixes/paths from `conformance/fixtures/keys/wal_prefixes.json`.
    #[test]
    fn golden_prefixes_match_java() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../conformance/fixtures/keys/wal_prefixes.json");
        let manifest = std::fs::read_to_string(path).expect("run conformance/generator first");
        let cases: serde_json::Value = serde_json::from_str(&manifest).unwrap();
        let cases = cases.as_array().unwrap();
        assert!(!cases.is_empty());
        for case in cases {
            let cluster_id = case["cluster_id"].as_str().unwrap();
            let node_id = case["node_id"].as_u64().unwrap() as u32;
            let prefix = node_prefix(cluster_id, node_id, None);
            assert_eq!(prefix, case["prefix"].as_str().unwrap());
            assert_eq!(
                gen_object_path_v1(&prefix, 3, 8192, 12288),
                case["path_v1"].as_str().unwrap()
            );
        }
    }

    #[test]
    fn parse_supports_v0_and_v1_keys() {
        let prefix = node_prefix("c", 2, None);
        let objects = vec![
            info(
                &format!("{prefix}5/wal/128-{}", 128 + DATA_FILE_ALIGN_SIZE),
                4096,
            ),
            info(&format!("{prefix}5/wal/0"), 1000),
            info("garbage-key", 1),
            info(&format!("{prefix}4/wal/64-128"), 64),
        ];
        let parsed = parse_wal_objects(objects);
        assert_eq!(parsed.len(), 3);
        // Sorted by (epoch, start_offset).
        assert_eq!(parsed[0].epoch, 4);
        assert_eq!((parsed[1].epoch, parsed[1].start_offset), (5, 0));
        assert_eq!((parsed[2].epoch, parsed[2].start_offset), (5, 128));
        assert_eq!(parsed[1].end_offset, 960);
        assert_eq!(parsed[2].end_offset, 128 + DATA_FILE_ALIGN_SIZE);
    }

    /// Overlap skipping: an old-epoch first object overlapping a new-epoch
    /// range is dropped.
    #[test]
    fn stale_epoch_overlap_dropped() {
        let make = |epoch: u64, start: u64, end: u64| WalObject {
            bucket_id: 0,
            key: format!("k/{epoch}/wal/{start}-{end}"),
            epoch,
            start_offset: start,
            end_offset: end,
            size: end - start,
        };
        let mut objects = vec![make(1, 100, 300), make(2, 200, 400)];
        let dropped = skip_overlap_objects(&mut objects);
        assert_eq!(dropped, vec![make(1, 100, 300)]);
        assert_eq!(objects, vec![make(2, 200, 400)]);

        // No overlap: nothing dropped.
        let mut objects = vec![make(1, 100, 200), make(2, 200, 400)];
        assert!(skip_overlap_objects(&mut objects).is_empty());
        assert_eq!(objects.len(), 2);
    }
}

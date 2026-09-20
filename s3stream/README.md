# s3stream

A stream engine on object storage. It is a library, not a server: it owns the WAL, object layout, caching, and compaction.
The host provides stream and object metadata through the `StreamManager` / `ObjectManager` traits.

Wire formats live in `specification/` and are pinned by golden fixtures in `conformance/`.

```
s3stream (facade: Config, builder)
  └── s3stream-core   (S3Storage pipeline, cache, compaction)
        ├── s3stream-wal     (object WAL, memory WAL)
        └── s3stream-object  (object format, ObjectStorage over object_store)
              └── s3stream-codec  (record codec, WAL framing, CRC)
```

```
cargo test --workspace
```

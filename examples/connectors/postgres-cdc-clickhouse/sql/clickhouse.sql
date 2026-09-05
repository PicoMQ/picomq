CREATE TABLE order_events (
    id UInt64,
    order_id String,
    region String,
    merchant_id String,
    `type` String,
    payload String,
    created_at DateTime64(6, 'UTC')
)
ENGINE = MergeTree
ORDER BY (region, created_at)
SETTINGS non_replicated_deduplication_window = 1000;

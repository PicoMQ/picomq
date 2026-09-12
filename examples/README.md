# Examples

`examples/<category>/<example>/`. Category is the product surface. Each example is a runnable story.

| Path | What you run |
| --- | --- |
| [`agents/ai-sdk`](agents/ai-sdk) | Vercel AI SDK: chat, per-run trail, multi-agent |
| [`connectors/postgres-cdc-clickhouse`](connectors/postgres-cdc-clickhouse) | Postgres CDC, one stream per region, ClickHouse |
| [`connectors/fleet-telematics-iceberg`](connectors/fleet-telematics-iceberg) | Fleet locations, one stream per fleet, Iceberg |
| [`connectors/flink-dynamic-kafka`](connectors/flink-dynamic-kafka) | Prefix list, Dynamic Kafka, one file bucket per fleet |
| [`connectors/flink-agents-s3-tables`](connectors/flink-agents-s3-tables) | ai-sdk streams, Managed Flink on the AWS harness, two S3 Tables |
| [`protocols/multi-protocol`](protocols/multi-protocol) | One keyed record through Pico HTTP, the CLI, Kafka, and Durable Streams SSE |
| [`storage/nestor-fanout`](storage/nestor-fanout) | Nestor as the S3 endpoint, 32 replays of one stream, one origin fetch per block |

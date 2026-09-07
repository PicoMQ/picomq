# One stream, every protocol

Write one keyed record through Pico HTTP, read it with the `pico` CLI, consume it through Kafka, and read it again as a Durable Streams SSE event.

The example checks the shared record contract instead of only printing output:

- Pico assigns stream offset `0` and preserves `Pico-Key: order-7`.
- Kafka reads offset `0`, key `order-7`, and body `keyed-order` from topic `orders`.
- Durable Streams reads the same body and reports next offset `00000000000000000001`.

Pico and Durable Streams are alternate HTTP dialects on one listener. `verify.sh` first runs the node in Pico mode, then recreates the same node in Durable Streams mode while retaining its SQLite metadata and object volumes. Kafka stays enabled in both modes.

## Run

Docker, curl, and Bash are required. Compose builds PicoMQ from the current checkout; the pinned Apache Kafka console consumer runs from its container image.

```bash
cd examples/unified-protocol
./verify.sh
```

Each protocol check is also a separate script:

```bash
./pico.sh
./kafka.sh
./durable-streams.sh
```

After `verify.sh`, the node remains running in Durable Streams mode for inspection. Remove the example data when finished:

```bash
docker compose down -v --remove-orphans
```

# PicoMQ connectors

Runtime, SDK and plugins for moving data between PicoMQ and external systems over the Kafka protocol. Documentation: [picomq.com/docs/connectors](https://picomq.com/docs/connectors).

```text
connectors/
├── runtime/   pico-connectors binary: loads plugins, consumes/produces, checkpoints
├── sdk/       picomq-connector-sdk: Sink/Source traits, decoders, transforms, FFI macros
├── sinks/     one cdylib crate per sink
└── sources/   one cdylib crate per source
```

## Build and run

```bash
cargo build --release -p picomq-connectors -p picomq-connector-stdout-sink -p picomq-connector-random-source
PICOMQ_CONNECTORS_CONFIG_PATH=connectors/runtime/config.toml \
PICOMQ_CONNECTORS_CONNECTORS__CONFIG_DIR=harness/aio/connectors \
    target/release/pico-connectors
```

Plugin `path` values in `harness/aio/connectors/*.toml` are bare names. The runtime finds them next to the binary in `target/release`.

Heavy sinks (`doris`, `iceberg`, `delta`, `redshift`) are workspace members but not default members, so `cargo build` skips them. Build with `-p picomq-connector-iceberg-sink` and so on.

## Test

```bash
cargo test -p picomq-connector-sdk
cargo test -p picomq-connectors            # includes tests/e2e.rs against an in-process pico
cargo test -p picomq-connector-postgres-sink
```

The e2e suite builds the stdout sink and random source, spawns `pico serve`, and runs the runtime as a child process so it can be killed mid-batch to check redelivery.

## Image

`Dockerfile.connectors` at the repository root builds the runtime and the light plugins into `ghcr.io/picomq/picomq-connectors`. Heavy plugins are attached to each GitHub release as `libpicomq_connector_<name>-linux-amd64.tar.gz`.

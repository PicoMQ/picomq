# Contribute

Thank you for your interest in contributing to PicoMQ. The project lives at [github.com/picomq/picomq](https://github.com/picomq/picomq). Bug reports, design discussion, and pull requests all go there.

::: tip
If you are new to the project, new to Rust, or just unsure whether a change belongs here, open a PR or an issue anyway. Review is part of how we learn, and there is always more to learn.
:::

## Repository layout

The workspace is split into two areas with a hard boundary between them:

- [`s3stream`](https://github.com/PicoMQ/s3stream) is the stream engine: WAL, object layout, caching, compaction. It is a self-contained library (crates, wire specification, conformance fixtures).
- **`picomq/`** is the host: metadata plane, server, HTTP frontends (Pico protocol and Durable Streams), client, and the `pico` CLI. Host crates depend only on the `s3stream` facade crate, never on engine internals. The wire vocabulary (header constants, Pico record codec) lives in `picomq-protocol`, a small crate shared by the frontends and `picomq-client`, which keeps the client publishable as a standalone SDK with no server dependencies.

Keeping the engine boundary intact is a review criterion. If a change in `picomq/*` needs something from inside the engine, the right move is to widen the facade.

The docs are at `website/` (`VitePress`). And the deployment harnesses live in `harness/` (`aio` for the all-in-one compose stacks, `byo` for an existing Postgres and object store, `terraform` and more).

## Build and test

Rust 1.98 or newer (`rust-toolchain.toml` pins the exact version). The default test suite has no external dependencies. SQLite and `file://` object storage stand in for Postgres and S3:

```bash
cargo build --workspace
cargo test --workspace
```

Postgres-backed tests need a database URL and are skipped or ignored without one:

```bash
PICOMQ_PG_URL=postgres://user:pass@localhost:5432/picomq \
    cargo test -p picomq-sql --test pg_contract --test pg_e2e

PICO_TEST_PG_URL=postgres://user:pass@localhost:5432/picomq \
    cargo test -p picomq-runtime --test postgres -- --ignored
```

### End to end

`scripts/e2e.sh` builds the images and runs scenarios against Postgres, RustFS, and pico in Docker Compose. It needs Docker, Go, and Node. With no arguments it runs all of them.

| Scenario | Runs |
| --- | --- |
| `single` | One node, Pico and Kafka: protocol cross tests, Kafka clients through a node restart, and the Rust, TypeScript, and Go client suites |
| `cluster` | Two nodes with auth: redirects, groups and Kafka partitions across nodes, admin APIs, failover of a killed node, HTTP and Kafka producers deduplicated across a transfer and through a killed owner, a Kafka group riding through the kill, every Kafka client authenticating over SASL and a narrow token enforced over Kafka |
| `chaos` | Two nodes under faults: paused nodes, a paused lease holder, a paused Postgres, and a clean stop. No acknowledged write may be lost and no healthy node fenced |
| `ds` | Two nodes on Durable Streams: the Go live and cluster suites |
| `tls` | Two nodes with TLS and auth: the Rust client cluster suite over HTTPS, redirects between nodes included |

```bash
scripts/e2e.sh                     # every scenario
scripts/e2e.sh single cluster      # a subset
KEEP=1 scripts/e2e.sh single       # leave the stack running
SKIP_BUILD=1 PICO_IMAGE=... scripts/e2e.sh   # use an image already built
```

The `e2e` workflow runs the same scenarios on demand from GitHub Actions.

### Conformance

`picomq-conformance` checks the protocols against outside references. The `ds` suite runs the official Durable Streams client tests. The `kafka` suite runs the same scenarios with the Kafka Java tools, kcat, and raw frames against PicoMQ and against Apache Kafka in Docker, and compares the results. Its SASL scenarios run against a second PicoMQ node with auth required and a SASL listener on the reference broker guarded by its standard authorizer, with matching root, read-only, and write-only identities on both. Its `SASL_SSL` scenarios run against a third node with TLS and a TLS listener on the reference broker, both serving a certificate minted for the run. It needs Docker, and runs locally or from the `conformance` workflow on demand.

```bash
cargo run -p picomq-conformance -- ds
cargo run -p picomq-conformance -- kafka
```

For an end-to-end environment, the compose stacks in `harness/aio` bring up a node with Postgres and RustFS (or SQLite and local files with `compose.lite.yml`). See [Quick start](/docs/quick-start).

A few things the toolchain enforces:

- `unsafe_code` is denied workspace-wide.
- Wire formats in `s3stream`'s `specification/` are pinned by golden fixtures in its `conformance/`. A change to a format needs a spec update and new fixtures in the same PR. Never regenerate fixtures to make a failing test pass.

## Docs

The site is VitePress. From `website/`:

```bash
npm install
npm run dev
```

Pages are markdown under `website/pages/docs/`, and the sidebar is defined in `website/.vitepress/config.mts`. Docs follow the same review bar as code.

## Pull requests

Small, focused PRs against `main`. A good PR description says *why* the change exists, not just what it touches. If it changes a wire format, a protocol behavior, or an operational default, call that out explicitly. Run `cargo fmt --all` and `cargo clippy --workspace --all-targets -- -D warnings` before pushing, as CI does.

AI-generated (or largely generated) pull requests are welcome, provided that you:

- Call out in the PR description that AI was used, and which tool or model.
- Understand the change and can explain it in review.
- Keep PR discussion human. Descriptions, comments, and review replies.
- Have reviewed the diff yourself before opening the PR.

For anything larger than a bug fix (a new [protocol facade](/docs/extending), a WAL backend, a metadata backend), [open an issue](https://github.com/picomq/picomq/issues) first so the design can be discussed before the code shows up.

By contributing, you agree your work is licensed under Apache 2.0.

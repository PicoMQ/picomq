# Contribute

Thank you for your interest in contributing to PicoMQ. The project lives at [github.com/picomq/picomq](https://github.com/picomq/picomq). Bug reports, design discussion, and pull requests all go there.

::: tip
If you are new to the project, new to Rust, or just unsure whether a change belongs here, open a PR or an issue anyway. Review is part of how we learn, and there is always more to learn.
:::

## Repository layout

The workspace is split into two areas with a hard boundary between them:

- **`s3stream/`** is the stream engine: WAL, object layout, caching, compaction. It is a self-contained library (crates, wire specification, conformance fixtures).
- **`picomq/`** is the host: metadata plane, server, HTTP frontends (Pico protocol and Durable Streams), client, and the `pico` CLI. Host crates depend only on the `s3stream` facade crate, never on engine internals. The wire vocabulary (header constants, Pico record codec) lives in `picomq-protocol`, a small crate shared by the frontends and `picomq-client`, which keeps the client publishable as a standalone SDK with no server dependencies.

Keeping that boundary intact is a review criterion. If a change in `picomq/*` needs something from inside the engine, the right move is to widen the facade.

The docs are at `website/` (`VitePress`). And the deployment harnesses live in `harness/` (`aio` for the all-in-one compose stacks, `byo` for an existing Postgres and object store, `terraform` and more).

## Build and test

Rust 1.80+ (`rust-toolchain.toml` pins the exact version). The default test suite has no external dependencies. SQLite and `file://` object storage stand in for Postgres and S3:

```bash
cargo build --workspace
cargo test --workspace
```

Postgres-backed tests are env-gated and skipped unless a URL is provided:

```bash
PICOMQ_PG_URL=postgres://user:pass@localhost:5432/picomq \
    cargo test -p picomq-sql --test pg_contract --test pg_e2e
```

For an end-to-end environment, the compose stacks in `harness/aio` bring up a node with Postgres and RustFS (or SQLite and local files with `compose.lite.yml`). See [Quick start](/docs/quick-start).

A few things the toolchain enforces:

- `unsafe_code` is denied workspace-wide.
- Wire formats in `s3stream/specification/` are pinned by golden fixtures in `s3stream/conformance/`. A change to a format needs a spec update and new fixtures in the same PR. Never regenerate fixtures to make a failing test pass.
- The Durable Streams frontend is additionally exercised through the official `durable-streams` client in e2e tests, as an independent conformance check.

## Docs

The site is VitePress. From `website/`:

```bash
npm install
npm run dev
```

Pages are markdown under `website/pages/docs/`, and the sidebar is defined in `website/.vitepress/config.mts`. Docs follow the same review bar as code.

## Pull requests

Small, focused PRs against `main`. A good PR description says *why* the change exists, not just what it touches. If it changes a wire format, a protocol behavior, or an operational default, call that out explicitly. Run `cargo fmt` and `cargo clippy --workspace` before pushing.

AI-generated (or largely generated) pull requests are welcome, provided that you:

- Call out in the PR description that AI was used, and which tool or model.
- Understand the change and can explain it in review.
- Keep PR discussion human. Descriptions, comments, and review replies.
- Have reviewed the diff yourself before opening the PR.

For anything larger than a bug fix (a new [protocol facade](/docs/extending), a WAL backend, a metadata backend), [open an issue](https://github.com/picomq/picomq/issues) first so the design can be discussed before the code shows up.

By contributing, you agree your work is licensed under Apache 2.0.

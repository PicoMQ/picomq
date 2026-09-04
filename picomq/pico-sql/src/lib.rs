//! SQL-backed metadata log (SQLite / Postgres) and lease election.
//!
//! Group-commit sink, tailer, snapshots, and a TTL lease for leader-gated maintenance.

pub mod lease;
pub mod sink;
pub mod store;

pub use lease::{LeaseConfig, LeaseKeeper};
pub use sink::{SqlSink, SqlSinkConfig, SqlSinkError};
pub use store::{DEFAULT_LEASE_TTL_MS, MetaStore, PgStore, SqliteStore, StoreError};

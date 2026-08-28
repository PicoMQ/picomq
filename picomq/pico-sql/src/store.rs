//! `MetaStore`: the SQL persistence surface. Three tables, opaque blobs:
//! ordered log entries, a durable marker, and snapshots.
//!
//! A deliberate own trait rather than `sqlx::Any`: the two dialects differ
//! in types and placeholders, and the trait is the seam for a future dialect.

use async_trait::async_trait;
use sqlx::postgres::PgPoolOptions;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous};
use sqlx::{PgPool, Row, SqlitePool};

/// Default maintenance-lease TTL. 10 s tolerates pauses without flapping.
pub const DEFAULT_LEASE_TTL_MS: i64 = 10_000;

/// Store failures. `Corrupt` covers impossible shapes (negative indexes,
/// missing seeded rows). The DB is trusted for durability, not for schema
/// invariants we can check cheaply.
#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("sql: {0}")]
    Sql(#[from] sqlx::Error),
    #[error("corrupt store: {0}")]
    Corrupt(String),
}

/// The SQL persistence surface for the metadata log.
///
/// All indexes are `u64` and start at 1 (0 = "nothing"). Stores persist them
/// as `BIGINT` and reject values above `i64::MAX`.
#[async_trait]
pub trait MetaStore: Send + Sync {
    /// Append `payload` at exactly `idx`. `Ok(true)` if this call won the
    /// slot.`Ok(false)` if the index was already taken (caller re-tails and
    /// retries at a later index). Durable before returning.
    async fn append(&self, idx: u64, payload: &[u8]) -> Result<bool, StoreError>;

    /// Highest index the store knows: `max(log tail, snapshot applied_idx)`.
    /// 0 when empty. This is where the next append goes (+1). Including
    /// right after a truncation emptied the log table.
    async fn last_idx(&self) -> Result<u64, StoreError>;

    /// Log entries with `idx > after`, ascending, at most `limit`.
    async fn fetch_after(&self, after: u64, limit: u32) -> Result<Vec<(u64, Vec<u8>)>, StoreError>;

    /// The current snapshot `(applied_idx, payload)`, if any.
    async fn load_snapshot(&self) -> Result<Option<(u64, Vec<u8>)>, StoreError>;

    /// The current snapshot's applied index without its payload. Lets a
    /// tailer that sees an empty log tail tell "nothing new" apart from
    /// "the tail was folded into a snapshot and truncated away".
    async fn snapshot_idx(&self) -> Result<Option<u64>, StoreError>;

    /// Overwrite the snapshot row. But never regress: if the stored snapshot
    /// already covers `applied_idx` or beyond, this is a no-op (two nodes may
    /// snapshot concurrently. The freshest one must win regardless of arrival
    /// order). Callers snapshot first, then [`Self::truncate_log`]. A crash
    /// in between leaves harmless extra log rows below the snapshot (skipped
    /// on restore, removed next cycle).
    async fn store_snapshot(&self, applied_idx: u64, payload: &[u8]) -> Result<(), StoreError>;

    /// Delete log rows with `idx <= up_to`.
    async fn truncate_log(&self, up_to: u64) -> Result<(), StoreError>;

    /// Lease CAS. `prev_epoch = Some(e)`: renew. Succeeds only while this
    /// holder still owns epoch `e`. `prev_epoch = None`: acquire. Succeeds
    /// only if the lease is expired (per `now_ms`) or already ours, and bumps
    /// the epoch (fencing token). Returns the owned epoch on success.
    async fn acquire_lease(
        &self,
        holder: &str,
        prev_epoch: Option<u64>,
        now_ms: i64,
        ttl_ms: i64,
    ) -> Result<Option<u64>, StoreError>;

    /// Fenced release: expires the lease only if `(holder, epoch)` still owns
    /// it (a stale holder cannot release a successor's lease).
    async fn release_lease(&self, holder: &str, epoch: u64) -> Result<(), StoreError>;
}

fn to_i64(value: u64, what: &str) -> Result<i64, StoreError> {
    i64::try_from(value).map_err(|_| StoreError::Corrupt(format!("{what} {value} exceeds i64")))
}

fn to_u64(value: i64, what: &str) -> Result<u64, StoreError> {
    u64::try_from(value).map_err(|_| StoreError::Corrupt(format!("{what} {value} is negative")))
}

/// `Ok(false)` for unique-key conflicts, error otherwise (append helper).
fn insert_outcome(result: Result<(), sqlx::Error>) -> Result<bool, StoreError> {
    match result {
        Ok(()) => Ok(true),
        Err(sqlx::Error::Database(e)) if e.is_unique_violation() => Ok(false),
        Err(e) => Err(e.into()),
    }
}

/// SQLite-backed store. WAL journal + `synchronous=FULL`: an acked append
pub struct SqliteStore {
    pool: SqlitePool,
}

impl SqliteStore {
    /// In-memory store (tests / throwaway). Single connection. Every pooled
    /// connection to `:memory:` would otherwise get its own database.
    pub async fn memory() -> Result<Self, StoreError> {
        let options = SqliteConnectOptions::new().in_memory(true);
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await?;
        let store = Self { pool };
        store.migrate().await?;
        Ok(store)
    }

    /// File-backed store at `path` (created if missing).
    pub async fn open(path: &std::path::Path) -> Result<Self, StoreError> {
        let options = SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(SqliteSynchronous::Full)
            .busy_timeout(std::time::Duration::from_secs(5));
        let pool = SqlitePoolOptions::new()
            .max_connections(4)
            .connect_with(options)
            .await?;
        let store = Self { pool };
        store.migrate().await?;
        Ok(store)
    }

    async fn migrate(&self) -> Result<(), StoreError> {
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS meta_log (\
                 idx INTEGER PRIMARY KEY, payload BLOB NOT NULL)",
        )
        .execute(&self.pool)
        .await?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS meta_snapshot (\
                 id INTEGER PRIMARY KEY, applied_idx INTEGER NOT NULL, payload BLOB NOT NULL)",
        )
        .execute(&self.pool)
        .await?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS meta_lease (\
                 id INTEGER PRIMARY KEY, holder TEXT NOT NULL, \
                 epoch INTEGER NOT NULL, expires_at_ms INTEGER NOT NULL)",
        )
        .execute(&self.pool)
        .await?;
        // Seed the single lease row so acquisition is always one CAS UPDATE
        // (no INSERT race to handle).
        sqlx::query(
            "INSERT OR IGNORE INTO meta_lease (id, holder, epoch, expires_at_ms) \
             VALUES (0, '', 0, 0)",
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}

#[async_trait]
impl MetaStore for SqliteStore {
    async fn append(&self, idx: u64, payload: &[u8]) -> Result<bool, StoreError> {
        let idx = to_i64(idx, "log idx")?;
        let result = sqlx::query("INSERT INTO meta_log (idx, payload) VALUES (?, ?)")
            .bind(idx)
            .bind(payload)
            .execute(&self.pool)
            .await
            .map(|_| ());
        insert_outcome(result)
    }

    async fn last_idx(&self) -> Result<u64, StoreError> {
        let row = sqlx::query(
            "SELECT COALESCE((SELECT MAX(idx) FROM meta_log), 0) AS log_idx, \
                    COALESCE((SELECT applied_idx FROM meta_snapshot WHERE id = 0), 0) AS snap_idx",
        )
        .fetch_one(&self.pool)
        .await?;
        let log_idx: i64 = row.get("log_idx");
        let snap_idx: i64 = row.get("snap_idx");
        to_u64(log_idx.max(snap_idx), "last idx")
    }

    async fn fetch_after(&self, after: u64, limit: u32) -> Result<Vec<(u64, Vec<u8>)>, StoreError> {
        let after = to_i64(after, "after idx")?;
        let rows =
            sqlx::query("SELECT idx, payload FROM meta_log WHERE idx > ? ORDER BY idx ASC LIMIT ?")
                .bind(after)
                .bind(i64::from(limit))
                .fetch_all(&self.pool)
                .await?;
        rows.into_iter()
            .map(|row| Ok((to_u64(row.get("idx"), "log idx")?, row.get("payload"))))
            .collect()
    }

    async fn load_snapshot(&self) -> Result<Option<(u64, Vec<u8>)>, StoreError> {
        let row = sqlx::query("SELECT applied_idx, payload FROM meta_snapshot WHERE id = 0")
            .fetch_optional(&self.pool)
            .await?;
        row.map(|row| {
            Ok((
                to_u64(row.get("applied_idx"), "snapshot idx")?,
                row.get("payload"),
            ))
        })
        .transpose()
    }

    async fn snapshot_idx(&self) -> Result<Option<u64>, StoreError> {
        let row = sqlx::query("SELECT applied_idx FROM meta_snapshot WHERE id = 0")
            .fetch_optional(&self.pool)
            .await?;
        row.map(|row| to_u64(row.get("applied_idx"), "snapshot idx"))
            .transpose()
    }

    async fn store_snapshot(&self, applied_idx: u64, payload: &[u8]) -> Result<(), StoreError> {
        let applied_idx = to_i64(applied_idx, "snapshot idx")?;
        sqlx::query(
            "INSERT INTO meta_snapshot (id, applied_idx, payload) VALUES (0, ?, ?) \
             ON CONFLICT (id) DO UPDATE SET \
                 applied_idx = excluded.applied_idx, payload = excluded.payload \
             WHERE excluded.applied_idx > meta_snapshot.applied_idx",
        )
        .bind(applied_idx)
        .bind(payload)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn truncate_log(&self, up_to: u64) -> Result<(), StoreError> {
        let up_to = to_i64(up_to, "truncate idx")?;
        sqlx::query("DELETE FROM meta_log WHERE idx <= ?")
            .bind(up_to)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn acquire_lease(
        &self,
        holder: &str,
        prev_epoch: Option<u64>,
        now_ms: i64,
        ttl_ms: i64,
    ) -> Result<Option<u64>, StoreError> {
        match prev_epoch {
            Some(epoch) => {
                let epoch_i = to_i64(epoch, "lease epoch")?;
                let done = sqlx::query(
                    "UPDATE meta_lease SET expires_at_ms = ? \
                     WHERE id = 0 AND holder = ? AND epoch = ? AND expires_at_ms >= ?",
                )
                .bind(now_ms + ttl_ms)
                .bind(holder)
                .bind(epoch_i)
                .bind(now_ms)
                .execute(&self.pool)
                .await?;
                Ok((done.rows_affected() == 1).then_some(epoch))
            }
            None => {
                let row = sqlx::query(
                    "UPDATE meta_lease SET holder = ?, epoch = epoch + 1, expires_at_ms = ? \
                     WHERE id = 0 AND (expires_at_ms < ? OR holder = ?) \
                     RETURNING epoch",
                )
                .bind(holder)
                .bind(now_ms + ttl_ms)
                .bind(now_ms)
                .bind(holder)
                .fetch_optional(&self.pool)
                .await?;
                row.map(|row| to_u64(row.get("epoch"), "lease epoch"))
                    .transpose()
            }
        }
    }

    async fn release_lease(&self, holder: &str, epoch: u64) -> Result<(), StoreError> {
        let epoch = to_i64(epoch, "lease epoch")?;
        sqlx::query(
            "UPDATE meta_lease SET expires_at_ms = 0 \
             WHERE id = 0 AND holder = ? AND epoch = ?",
        )
        .bind(holder)
        .bind(epoch)
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}

/// Postgres-backed store. Identical contract.`BYTEA` payloads, `$n`
/// placeholders, default `synchronous_commit=on` durability.
pub struct PgStore {
    pool: PgPool,
}

impl PgStore {
    /// Connect and migrate. `url` is a standard `postgres://` URL.
    pub async fn connect(url: &str) -> Result<Self, StoreError> {
        let pool = PgPoolOptions::new().max_connections(8).connect(url).await?;
        let store = Self { pool };
        store.migrate().await?;
        Ok(store)
    }

    async fn migrate(&self) -> Result<(), StoreError> {
        let mut tx = self.pool.begin().await?;
        // Concurrent `CREATE TABLE IF NOT EXISTS` races in Postgres (duplicate
        // pg_type). One advisory lock serializes first boot across nodes.
        sqlx::query("SELECT pg_advisory_xact_lock(x'7069636f'::int8)")
            .execute(&mut *tx)
            .await?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS meta_log (\
                 idx BIGINT PRIMARY KEY, payload BYTEA NOT NULL)",
        )
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS meta_snapshot (\
                 id BIGINT PRIMARY KEY, applied_idx BIGINT NOT NULL, payload BYTEA NOT NULL)",
        )
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS meta_lease (\
                 id BIGINT PRIMARY KEY, holder TEXT NOT NULL, \
                 epoch BIGINT NOT NULL, expires_at_ms BIGINT NOT NULL)",
        )
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            "INSERT INTO meta_lease (id, holder, epoch, expires_at_ms) \
             VALUES (0, '', 0, 0) ON CONFLICT (id) DO NOTHING",
        )
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(())
    }
}

#[async_trait]
impl MetaStore for PgStore {
    async fn append(&self, idx: u64, payload: &[u8]) -> Result<bool, StoreError> {
        let idx = to_i64(idx, "log idx")?;
        let result = sqlx::query("INSERT INTO meta_log (idx, payload) VALUES ($1, $2)")
            .bind(idx)
            .bind(payload)
            .execute(&self.pool)
            .await
            .map(|_| ());
        insert_outcome(result)
    }

    async fn last_idx(&self) -> Result<u64, StoreError> {
        let row = sqlx::query(
            "SELECT COALESCE((SELECT MAX(idx) FROM meta_log), 0) AS log_idx, \
                    COALESCE((SELECT applied_idx FROM meta_snapshot WHERE id = 0), 0) AS snap_idx",
        )
        .fetch_one(&self.pool)
        .await?;
        let log_idx: i64 = row.get("log_idx");
        let snap_idx: i64 = row.get("snap_idx");
        to_u64(log_idx.max(snap_idx), "last idx")
    }

    async fn fetch_after(&self, after: u64, limit: u32) -> Result<Vec<(u64, Vec<u8>)>, StoreError> {
        let after = to_i64(after, "after idx")?;
        let rows = sqlx::query(
            "SELECT idx, payload FROM meta_log WHERE idx > $1 ORDER BY idx ASC LIMIT $2",
        )
        .bind(after)
        .bind(i64::from(limit))
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|row| Ok((to_u64(row.get("idx"), "log idx")?, row.get("payload"))))
            .collect()
    }

    async fn load_snapshot(&self) -> Result<Option<(u64, Vec<u8>)>, StoreError> {
        let row = sqlx::query("SELECT applied_idx, payload FROM meta_snapshot WHERE id = 0")
            .fetch_optional(&self.pool)
            .await?;
        row.map(|row| {
            Ok((
                to_u64(row.get("applied_idx"), "snapshot idx")?,
                row.get("payload"),
            ))
        })
        .transpose()
    }

    async fn snapshot_idx(&self) -> Result<Option<u64>, StoreError> {
        let row = sqlx::query("SELECT applied_idx FROM meta_snapshot WHERE id = 0")
            .fetch_optional(&self.pool)
            .await?;
        row.map(|row| to_u64(row.get("applied_idx"), "snapshot idx"))
            .transpose()
    }

    async fn store_snapshot(&self, applied_idx: u64, payload: &[u8]) -> Result<(), StoreError> {
        let applied_idx = to_i64(applied_idx, "snapshot idx")?;
        sqlx::query(
            "INSERT INTO meta_snapshot (id, applied_idx, payload) VALUES (0, $1, $2) \
             ON CONFLICT (id) DO UPDATE SET \
                 applied_idx = excluded.applied_idx, payload = excluded.payload \
             WHERE excluded.applied_idx > meta_snapshot.applied_idx",
        )
        .bind(applied_idx)
        .bind(payload)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn truncate_log(&self, up_to: u64) -> Result<(), StoreError> {
        let up_to = to_i64(up_to, "truncate idx")?;
        sqlx::query("DELETE FROM meta_log WHERE idx <= $1")
            .bind(up_to)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn acquire_lease(
        &self,
        holder: &str,
        prev_epoch: Option<u64>,
        now_ms: i64,
        ttl_ms: i64,
    ) -> Result<Option<u64>, StoreError> {
        match prev_epoch {
            Some(epoch) => {
                let epoch_i = to_i64(epoch, "lease epoch")?;
                let done = sqlx::query(
                    "UPDATE meta_lease SET expires_at_ms = $1 \
                     WHERE id = 0 AND holder = $2 AND epoch = $3 AND expires_at_ms >= $4",
                )
                .bind(now_ms + ttl_ms)
                .bind(holder)
                .bind(epoch_i)
                .bind(now_ms)
                .execute(&self.pool)
                .await?;
                Ok((done.rows_affected() == 1).then_some(epoch))
            }
            None => {
                let row = sqlx::query(
                    "UPDATE meta_lease SET holder = $1, epoch = epoch + 1, expires_at_ms = $2 \
                     WHERE id = 0 AND (expires_at_ms < $3 OR holder = $1) \
                     RETURNING epoch",
                )
                .bind(holder)
                .bind(now_ms + ttl_ms)
                .bind(now_ms)
                .fetch_optional(&self.pool)
                .await?;
                row.map(|row| to_u64(row.get("epoch"), "lease epoch"))
                    .transpose()
            }
        }
    }

    async fn release_lease(&self, holder: &str, epoch: u64) -> Result<(), StoreError> {
        let epoch = to_i64(epoch, "lease epoch")?;
        sqlx::query(
            "UPDATE meta_lease SET expires_at_ms = 0 \
             WHERE id = 0 AND holder = $1 AND epoch = $2",
        )
        .bind(holder)
        .bind(epoch)
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}

/// Behavioral contract, run against every backend (SQLite always, Postgres
/// when `PICOMQ_PG_URL` is set. See `tests/pg_contract.rs`).
#[doc(hidden)]
pub async fn contract_suite(store: &dyn MetaStore) {
    // Empty store.
    assert_eq!(store.last_idx().await.unwrap(), 0);
    assert_eq!(store.fetch_after(0, 100).await.unwrap(), vec![]);
    assert_eq!(store.load_snapshot().await.unwrap(), None);
    assert_eq!(store.snapshot_idx().await.unwrap(), None);

    // Append + ordering + opaque bytes (include 0x00 and high bytes).
    assert!(store.append(1, b"one").await.unwrap());
    assert!(store.append(2, &[0u8, 255, 7, 0]).await.unwrap());
    assert!(store.append(3, b"").await.unwrap());
    assert_eq!(store.last_idx().await.unwrap(), 3);

    // Conflict loses and changes nothing.
    assert!(!store.append(2, b"usurper").await.unwrap());
    let rows = store.fetch_after(0, 100).await.unwrap();
    assert_eq!(
        rows,
        vec![
            (1, b"one".to_vec()),
            (2, vec![0u8, 255, 7, 0]),
            (3, Vec::new()),
        ]
    );

    assert_eq!(
        store.fetch_after(1, 1).await.unwrap(),
        vec![(2, vec![0u8, 255, 7, 0])]
    );
    assert_eq!(store.fetch_after(3, 100).await.unwrap(), vec![]);

    // Snapshot store/overwrite/load. Truncation keeps last_idx via snapshot.
    store.store_snapshot(2, b"snap-v1").await.unwrap();
    store.store_snapshot(3, b"snap-v2").await.unwrap();
    assert_eq!(
        store.load_snapshot().await.unwrap(),
        Some((3, b"snap-v2".to_vec()))
    );
    // A stale (older) snapshot never regresses the stored one.
    store.store_snapshot(2, b"stale").await.unwrap();
    assert_eq!(
        store.load_snapshot().await.unwrap(),
        Some((3, b"snap-v2".to_vec()))
    );
    assert_eq!(store.snapshot_idx().await.unwrap(), Some(3));
    store.truncate_log(3).await.unwrap();
    assert_eq!(store.fetch_after(0, 100).await.unwrap(), vec![]);
    assert_eq!(
        store.last_idx().await.unwrap(),
        3,
        "snapshot idx survives truncation"
    );
    assert!(store.append(4, b"post-truncate").await.unwrap());
    assert_eq!(store.last_idx().await.unwrap(), 4);

    // Lease: acquire on the seeded row bumps the epoch (fencing token).
    let now = 1_000;
    let ttl = 100;
    let e1 = store
        .acquire_lease("a", None, now, ttl)
        .await
        .unwrap()
        .unwrap();
    assert!(e1 >= 1);
    // Steal while valid fails. Renewal with the right epoch works.
    assert_eq!(
        store.acquire_lease("b", None, now + 50, ttl).await.unwrap(),
        None
    );
    assert_eq!(
        store
            .acquire_lease("a", Some(e1), now + 50, ttl)
            .await
            .unwrap(),
        Some(e1)
    );
    // Renewal extended the lease: still owned at now+120.
    assert_eq!(
        store
            .acquire_lease("b", None, now + 120, ttl)
            .await
            .unwrap(),
        None
    );
    // After expiry, takeover bumps the epoch and fences the old holder.
    let e2 = store
        .acquire_lease("b", None, now + 200, ttl)
        .await
        .unwrap()
        .unwrap();
    assert!(e2 > e1);
    assert_eq!(
        store
            .acquire_lease("a", Some(e1), now + 210, ttl)
            .await
            .unwrap(),
        None
    );
    // Re-acquiring one's own live lease is allowed (restart with same id)
    // and bumps the epoch.
    let e3 = store
        .acquire_lease("b", None, now + 220, ttl)
        .await
        .unwrap()
        .unwrap();
    assert!(e3 > e2);
    // Fenced release: stale (holder, epoch) is a no-op. Current one expires it.
    store.release_lease("b", e2).await.unwrap();
    assert_eq!(
        store
            .acquire_lease("a", None, now + 230, ttl)
            .await
            .unwrap(),
        None
    );
    store.release_lease("b", e3).await.unwrap();
    let e4 = store
        .acquire_lease("a", None, now + 230, ttl)
        .await
        .unwrap()
        .unwrap();
    assert!(e4 > e3);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn sqlite_memory_contract() {
        let store = SqliteStore::memory().await.unwrap();
        contract_suite(&store).await;
    }

    #[tokio::test]
    async fn sqlite_file_contract_and_reopen_durability() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("meta.db");
        {
            let store = SqliteStore::open(&path).await.unwrap();
            contract_suite(&store).await;
        }
        // Reopen: everything the suite left behind is still there.
        let store = SqliteStore::open(&path).await.unwrap();
        assert_eq!(store.last_idx().await.unwrap(), 4);
        assert_eq!(
            store.fetch_after(3, 10).await.unwrap(),
            vec![(4, b"post-truncate".to_vec())]
        );
        assert_eq!(
            store.load_snapshot().await.unwrap(),
            Some((3, b"snap-v2".to_vec()))
        );
    }

    /// Two writers racing for the same index: exactly one wins, the loser's
    /// payload is nowhere (the sink's conflict-retry correctness foundation).
    #[tokio::test]
    async fn sqlite_append_race_single_winner() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("race.db");
        let a = SqliteStore::open(&path).await.unwrap();
        let b = SqliteStore::open(&path).await.unwrap();
        let mut wins = 0;
        for idx in 1..=20u64 {
            let (pa, pb) = (format!("a-{idx}"), format!("b-{idx}"));
            let (ra, rb) = tokio::join!(a.append(idx, pa.as_bytes()), b.append(idx, pb.as_bytes()));
            let (ra, rb) = (ra.unwrap(), rb.unwrap());
            assert!(ra ^ rb, "exactly one writer must win idx {idx}");
            wins += u64::from(ra);
        }
        let rows = a.fetch_after(0, 100).await.unwrap();
        assert_eq!(rows.len(), 20);
        for (idx, payload) in rows {
            let text = String::from_utf8(payload).unwrap();
            assert!(text == format!("a-{idx}") || text == format!("b-{idx}"));
        }
        // Sanity: both writers won at least once across 20 raced slots is not
        // guaranteed, but the winner count must be within range.
        assert!(wins <= 20);
    }
}

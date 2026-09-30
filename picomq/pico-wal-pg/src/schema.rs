use sqlx::{AssertSqlSafe, PgPool};

use crate::error::Error;

pub(crate) const NODE: &str = "pico_wal_node";

const LOCK: i64 = 0x0070_6963_6f77_616c;

pub(crate) struct Schema {
    slots: Vec<String>,
}

impl Schema {
    pub(crate) fn new(cluster_id: &str, node_id: u32, count: u32) -> Self {
        let tag = crc32fast::hash(cluster_id.as_bytes());
        Self {
            slots: (0..count)
                .map(|index| format!("pico_wal_{tag:08x}_{node_id}_{index}"))
                .collect(),
        }
    }

    pub(crate) fn slot(&self, index: usize) -> &str {
        &self.slots[index]
    }

    pub(crate) fn slots(&self) -> &[String] {
        &self.slots
    }

    pub(crate) fn union(&self) -> String {
        self.slots
            .iter()
            .map(|table| format!("SELECT start_offset, end_offset, epoch, body FROM {table}"))
            .collect::<Vec<_>>()
            .join(" UNION ALL ")
    }

    pub(crate) async fn migrate(&self, pool: &PgPool) -> Result<(), Error> {
        let mut tx = pool.begin().await?;
        sqlx::query("SELECT pg_advisory_xact_lock($1)")
            .bind(LOCK)
            .execute(&mut *tx)
            .await?;
        sqlx::query(AssertSqlSafe(format!(
            "CREATE TABLE IF NOT EXISTS {NODE} (\
                 cluster_id TEXT NOT NULL, node_id BIGINT NOT NULL, \
                 epoch BIGINT NOT NULL DEFAULT 0, trim_offset BIGINT NOT NULL DEFAULT 0, \
                 timeline BIGINT NOT NULL DEFAULT 0, segments BIGINT NOT NULL DEFAULT 0, \
                 segment_bytes BIGINT NOT NULL DEFAULT 0, \
                 PRIMARY KEY (cluster_id, node_id))"
        )))
        .execute(&mut *tx)
        .await?;
        for table in &self.slots {
            sqlx::query(AssertSqlSafe(format!(
                "CREATE TABLE IF NOT EXISTS {table} (\
                     start_offset BIGINT PRIMARY KEY, end_offset BIGINT NOT NULL, \
                     epoch BIGINT NOT NULL, body BYTEA NOT NULL)"
            )))
            .execute(&mut *tx)
            .await?;
            sqlx::query(AssertSqlSafe(format!(
                "ALTER TABLE {table} ALTER COLUMN body SET STORAGE EXTERNAL"
            )))
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }
}

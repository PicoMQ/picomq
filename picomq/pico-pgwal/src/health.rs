use sqlx::PgPool;

use crate::error::Error;

const TIMELINE: &str =
    "SELECT (('x' || substr(pg_walfile_name(pg_current_wal_lsn()), 1, 8))::bit(32)::int)";

pub(crate) struct Health {
    pub(crate) timeline: u32,
    replicated: bool,
}

impl Health {
    pub(crate) async fn probe(pool: &PgPool, allow_unsafe: bool) -> Result<Self, Error> {
        let standby: bool = sqlx::query_scalar("SELECT pg_is_in_recovery()")
            .fetch_one(pool)
            .await?;
        if standby {
            return Err(Error::Unsafe(
                "the wal must point at a primary, not a standby".to_owned(),
            ));
        }
        let fsync: String = sqlx::query_scalar("SELECT current_setting('fsync')")
            .fetch_one(pool)
            .await?;
        if fsync != "on" {
            if !allow_unsafe {
                return Err(Error::Unsafe(
                    "fsync is off, so acknowledged records would not survive a crash; set allowUnsafe=true to run anyway".to_owned(),
                ));
            }
            tracing::warn!("postgres fsync is off: acknowledged records may be lost on a crash");
        }
        let standbys: String =
            sqlx::query_scalar("SELECT current_setting('synchronous_standby_names')")
                .fetch_one(pool)
                .await?;
        let replicated = !standbys.trim().is_empty();
        if !replicated {
            tracing::warn!(
                "synchronous_standby_names is empty: acknowledged records survive a postgres crash but not the loss of the primary"
            );
        }
        let timeline: i32 = sqlx::query_scalar(TIMELINE).fetch_one(pool).await?;
        Ok(Self {
            timeline: timeline as u32,
            replicated,
        })
    }

    pub(crate) fn compare(&self, previous: u32) {
        if previous == 0 || previous == self.timeline {
            return;
        }
        if self.timeline < previous {
            tracing::warn!(
                previous,
                current = self.timeline,
                "postgres timeline moved backwards since the last start: the database was restored"
            );
        } else if self.replicated {
            tracing::info!(
                previous,
                current = self.timeline,
                "postgres was promoted since the last start"
            );
        } else {
            tracing::error!(
                previous,
                current = self.timeline,
                "postgres was promoted without synchronous replication: records acknowledged after the standby's last replayed commit are lost"
            );
        }
    }
}

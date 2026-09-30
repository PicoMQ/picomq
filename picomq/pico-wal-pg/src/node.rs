use async_trait::async_trait;
use s3stream_wal::{ReservationService, WalError};
use sqlx::{AssertSqlSafe, PgPool, Row};

use crate::error::Error;
use crate::schema::NODE;

pub(crate) struct State {
    pub(crate) trim: u64,
    pub(crate) timeline: u32,
    pub(crate) segments: u32,
    pub(crate) segment_bytes: u64,
}

pub(crate) struct Node {
    pool: PgPool,
    cluster_id: String,
}

impl Node {
    pub(crate) fn new(pool: PgPool, cluster_id: String) -> Self {
        Self { pool, cluster_id }
    }

    pub(crate) async fn claim(&self, node_id: u32, epoch: u64) -> Result<(), Error> {
        let claimed = sqlx::query(AssertSqlSafe(format!(
            "INSERT INTO {NODE} (cluster_id, node_id, epoch) VALUES ($1, $2, $3) \
             ON CONFLICT (cluster_id, node_id) DO UPDATE SET epoch = EXCLUDED.epoch \
             WHERE {NODE}.epoch <= EXCLUDED.epoch"
        )))
        .bind(&self.cluster_id)
        .bind(i64::from(node_id))
        .bind(signed(epoch)?)
        .execute(&self.pool)
        .await?
        .rows_affected();
        if claimed == 0 {
            return Err(Error::Fenced { node_id, epoch });
        }
        Ok(())
    }

    pub(crate) async fn epoch(&self, node_id: u32) -> Result<Option<u64>, Error> {
        let epoch: Option<i64> = sqlx::query_scalar(AssertSqlSafe(format!(
            "SELECT epoch FROM {NODE} WHERE cluster_id = $1 AND node_id = $2"
        )))
        .bind(&self.cluster_id)
        .bind(i64::from(node_id))
        .fetch_optional(&self.pool)
        .await?;
        Ok(epoch.map(|epoch| epoch as u64))
    }

    pub(crate) async fn load(&self, node_id: u32) -> Result<State, Error> {
        let row = sqlx::query(AssertSqlSafe(format!(
            "SELECT trim_offset, timeline, segments, segment_bytes FROM {NODE} \
             WHERE cluster_id = $1 AND node_id = $2"
        )))
        .bind(&self.cluster_id)
        .bind(i64::from(node_id))
        .fetch_one(&self.pool)
        .await?;
        Ok(State {
            trim: row.try_get::<i64, _>(0)? as u64,
            timeline: row.try_get::<i64, _>(1)? as u32,
            segments: row.try_get::<i64, _>(2)? as u32,
            segment_bytes: row.try_get::<i64, _>(3)? as u64,
        })
    }

    pub(crate) async fn stamp(
        &self,
        node_id: u32,
        epoch: u64,
        timeline: u32,
        segments: u32,
        segment_bytes: u64,
    ) -> Result<(), Error> {
        let stamped = sqlx::query(AssertSqlSafe(format!(
            "UPDATE {NODE} SET timeline = $4, segments = $5, segment_bytes = $6 \
             WHERE cluster_id = $1 AND node_id = $2 AND epoch = $3"
        )))
        .bind(&self.cluster_id)
        .bind(i64::from(node_id))
        .bind(signed(epoch)?)
        .bind(i64::from(timeline))
        .bind(i64::from(segments))
        .bind(signed(segment_bytes)?)
        .execute(&self.pool)
        .await?
        .rows_affected();
        fenced(stamped, node_id, epoch)
    }

    pub(crate) async fn trim(&self, node_id: u32, epoch: u64, watermark: u64) -> Result<(), Error> {
        let trimmed = sqlx::query(AssertSqlSafe(format!(
            "UPDATE {NODE} SET trim_offset = GREATEST(trim_offset, $4) \
             WHERE cluster_id = $1 AND node_id = $2 AND epoch = $3"
        )))
        .bind(&self.cluster_id)
        .bind(i64::from(node_id))
        .bind(signed(epoch)?)
        .bind(signed(watermark)?)
        .execute(&self.pool)
        .await?
        .rows_affected();
        fenced(trimmed, node_id, epoch)
    }
}

#[async_trait]
impl ReservationService for Node {
    async fn acquire(&self, node_id: u32, epoch: u64, _failover: bool) -> Result<(), WalError> {
        Ok(self.claim(node_id, epoch).await?)
    }

    async fn verify(&self, node_id: u32, epoch: u64, _failover: bool) -> Result<bool, WalError> {
        Ok(self.epoch(node_id).await? == Some(epoch))
    }
}

pub(crate) fn signed(value: u64) -> Result<i64, Error> {
    i64::try_from(value).map_err(|_| Error::Corrupt(format!("{value} exceeds BIGINT")))
}

fn fenced(rows: u64, node_id: u32, epoch: u64) -> Result<(), Error> {
    if rows == 0 {
        return Err(Error::Fenced { node_id, epoch });
    }
    Ok(())
}

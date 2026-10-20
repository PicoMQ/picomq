use async_trait::async_trait;
use s3stream_wal::{ReservationService, WalError};
use sqlx::postgres::PgArguments;
use sqlx::query::Query;
use sqlx::{AssertSqlSafe, PgPool, Postgres, Row};

use crate::error::Error;
use crate::schema::NODE;

pub(crate) struct State {
    pub(crate) trim: u64,
    pub(crate) timeline: u32,
    pub(crate) segments: u32,
    pub(crate) segment_bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Claim {
    pub(crate) node_id: u32,
    pub(crate) epoch: u64,
    pub(crate) failover: bool,
}

pub(crate) const HELD: &str = "cluster_id = $1 AND node_id = $2 AND epoch = $3 AND failover = $4";

pub(crate) fn held<'q>(
    query: Query<'q, Postgres, PgArguments>,
    cluster_id: &'q str,
    claim: Claim,
) -> Result<Query<'q, Postgres, PgArguments>, Error> {
    Ok(query
        .bind(cluster_id)
        .bind(i64::from(claim.node_id))
        .bind(signed(claim.epoch)?)
        .bind(claim.failover))
}

pub(crate) struct Node {
    pool: PgPool,
    cluster_id: String,
}

impl Node {
    pub(crate) fn new(pool: PgPool, cluster_id: String) -> Self {
        Self { pool, cluster_id }
    }

    pub(crate) async fn claim(&self, claim: Claim) -> Result<(), Error> {
        let sql = format!(
            "INSERT INTO {NODE} (cluster_id, node_id, epoch, failover) VALUES ($1, $2, $3, $4) \
             ON CONFLICT (cluster_id, node_id) DO UPDATE \
             SET epoch = EXCLUDED.epoch, failover = EXCLUDED.failover \
             WHERE {NODE}.epoch < EXCLUDED.epoch \
             OR ({NODE}.epoch = EXCLUDED.epoch AND (EXCLUDED.failover OR NOT {NODE}.failover))"
        );
        let claimed = held(sqlx::query(AssertSqlSafe(sql)), &self.cluster_id, claim)?
            .execute(&self.pool)
            .await?
            .rows_affected();

        fenced(claimed, claim)
    }

    pub(crate) async fn claimed(&self, node_id: u32) -> Result<Option<Claim>, Error> {
        let claimed: Option<(i64, bool)> = sqlx::query_as(AssertSqlSafe(format!(
            "SELECT epoch, failover FROM {NODE} WHERE cluster_id = $1 AND node_id = $2"
        )))
        .bind(&self.cluster_id)
        .bind(i64::from(node_id))
        .fetch_optional(&self.pool)
        .await?;

        Ok(claimed.map(|(epoch, failover)| Claim {
            node_id,
            epoch: epoch as u64,
            failover,
        }))
    }

    pub(crate) async fn load(&self, node_id: u32) -> Result<Option<State>, Error> {
        let row = sqlx::query(AssertSqlSafe(format!(
            "SELECT trim_offset, timeline, segments, segment_bytes FROM {NODE} \
             WHERE cluster_id = $1 AND node_id = $2"
        )))
        .bind(&self.cluster_id)
        .bind(i64::from(node_id))
        .fetch_optional(&self.pool)
        .await?;

        row.map(|row| {
            Ok(State {
                trim: row.try_get::<i64, _>(0)? as u64,
                timeline: row.try_get::<i64, _>(1)? as u32,
                segments: row.try_get::<i64, _>(2)? as u32,
                segment_bytes: row.try_get::<i64, _>(3)? as u64,
            })
        })
        .transpose()
    }

    pub(crate) async fn stamp(
        &self,
        claim: Claim,
        timeline: u32,
        segments: u32,
        segment_bytes: u64,
    ) -> Result<(), Error> {
        let sql = format!(
            "UPDATE {NODE} SET timeline = $5, segments = $6, segment_bytes = $7 WHERE {HELD}"
        );
        let stamped = held(sqlx::query(AssertSqlSafe(sql)), &self.cluster_id, claim)?
            .bind(i64::from(timeline))
            .bind(i64::from(segments))
            .bind(signed(segment_bytes)?)
            .execute(&self.pool)
            .await?
            .rows_affected();

        fenced(stamped, claim)
    }

    pub(crate) async fn trim(&self, claim: Claim, watermark: u64) -> Result<(), Error> {
        let sql = format!("UPDATE {NODE} SET trim_offset = GREATEST(trim_offset, $5) WHERE {HELD}");
        let trimmed = held(sqlx::query(AssertSqlSafe(sql)), &self.cluster_id, claim)?
            .bind(signed(watermark)?)
            .execute(&self.pool)
            .await?
            .rows_affected();

        fenced(trimmed, claim)
    }
}

#[async_trait]
impl ReservationService for Node {
    async fn acquire(&self, node_id: u32, epoch: u64, failover: bool) -> Result<(), WalError> {
        let claim = Claim {
            node_id,
            epoch,
            failover,
        };

        Ok(self.claim(claim).await?)
    }

    async fn verify(&self, node_id: u32, epoch: u64, failover: bool) -> Result<bool, WalError> {
        let claim = Claim {
            node_id,
            epoch,
            failover,
        };

        Ok(self.claimed(node_id).await? == Some(claim))
    }
}

pub(crate) fn signed(value: u64) -> Result<i64, Error> {
    i64::try_from(value).map_err(|_| Error::Corrupt(format!("{value} exceeds BIGINT")))
}

fn fenced(rows: u64, claim: Claim) -> Result<(), Error> {
    if rows == 0 {
        return Err(Error::Fenced {
            node_id: claim.node_id,
            epoch: claim.epoch,
        });
    }
    Ok(())
}

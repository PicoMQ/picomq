use std::collections::VecDeque;
use std::sync::Arc;

use bytes::Bytes;
use futures::{StreamExt, TryStreamExt, stream};
use s3stream_codec::StreamRecordBatch;
use s3stream_wal::{RecordOffset, RecoverResult, RecoverStream, WalError};
use sqlx::{AssertSqlSafe, PgPool, Row};

use crate::error::Error;
use crate::frame::{self, Framed};
use crate::node::signed;
use crate::schema::Schema;

struct Batch {
    start: u64,
    end: u64,
    epoch: u64,
    body: Bytes,
}

impl Batch {
    fn decode(self) -> Result<(u64, Vec<Framed>), Error> {
        Ok((self.epoch, frame::decode(self.start, self.body)?))
    }
}

#[derive(Clone)]
pub(crate) struct Reader {
    pool: PgPool,
    schema: Arc<Schema>,
}

impl Reader {
    pub(crate) fn new(pool: PgPool, schema: Arc<Schema>) -> Self {
        Self { pool, schema }
    }

    pub(crate) async fn extent(&self, index: usize) -> Result<Option<(u64, u64)>, Error> {
        extent(&self.pool, self.schema.slot(index), 0).await
    }

    pub(crate) fn recover(&self, watermark: u64) -> RecoverStream {
        let recovery = Recovery {
            reader: self.clone(),
            watermark,
            slots: None,
            after: -1,
            expected: None,
        };
        stream::try_unfold(recovery, |mut recovery| async move {
            let results = recovery.next().await?;
            Ok::<_, WalError>(results.map(|results| (results, recovery)))
        })
        .map_ok(|results| stream::iter(results.into_iter().map(Ok)))
        .try_flatten()
        .boxed()
    }

    pub(crate) async fn get(&self, offset: RecordOffset) -> Result<StreamRecordBatch, Error> {
        let position = signed(offset.offset)?;
        let batches = self
            .select("start_offset <= $1 AND end_offset > $1", position, position)
            .await?;
        for batch in batches {
            let (_, framed) = batch.decode()?;
            if let Some(found) = framed.into_iter().find(|f| f.offset == offset.offset) {
                return Ok(found.record);
            }
        }
        Err(Error::Corrupt(format!(
            "no record at offset {}",
            offset.offset
        )))
    }

    pub(crate) async fn range(
        &self,
        start: u64,
        end: u64,
    ) -> Result<Vec<StreamRecordBatch>, Error> {
        if start >= end {
            return Ok(Vec::new());
        }
        let batches = self
            .select(
                "end_offset > $1 AND start_offset < $2",
                signed(start)?,
                signed(end)?,
            )
            .await?;
        let mut records = Vec::new();
        for batch in batches {
            let (_, framed) = batch.decode()?;
            records.extend(
                framed
                    .into_iter()
                    .filter(|f| f.offset >= start && f.offset < end)
                    .map(|f| f.record),
            );
        }
        Ok(records)
    }

    async fn select(&self, filter: &str, low: i64, high: i64) -> Result<Vec<Batch>, Error> {
        let rows = sqlx::query(AssertSqlSafe(format!(
            "SELECT start_offset, end_offset, epoch, body FROM ({}) wal \
             WHERE {filter} ORDER BY start_offset",
            self.schema.union()
        )))
        .bind(low)
        .bind(high)
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(batch).collect()
    }

    async fn first(
        &self,
        index: usize,
        watermark: u64,
        after: i64,
    ) -> Result<Option<Batch>, Error> {
        let row = sqlx::query(AssertSqlSafe(format!(
            "SELECT start_offset, end_offset, epoch, body FROM {} \
             WHERE end_offset > $1 AND start_offset > $2 ORDER BY start_offset LIMIT 1",
            self.schema.slot(index)
        )))
        .bind(signed(watermark)?)
        .bind(after)
        .fetch_optional(&self.pool)
        .await?;
        row.as_ref().map(batch).transpose()
    }
}

pub(crate) async fn residue(
    pool: &PgPool,
    tables: &[String],
    watermark: u64,
) -> Result<bool, Error> {
    for table in tables {
        if extent(pool, table, watermark).await?.is_some() {
            return Ok(true);
        }
    }
    Ok(false)
}

async fn extent(pool: &PgPool, table: &str, watermark: u64) -> Result<Option<(u64, u64)>, Error> {
    let row = sqlx::query(AssertSqlSafe(format!(
        "SELECT min(start_offset), max(end_offset) FROM {table} WHERE end_offset > $1"
    )))
    .bind(signed(watermark)?)
    .fetch_one(pool)
    .await?;
    let start: Option<i64> = row.try_get(0)?;
    let end: Option<i64> = row.try_get(1)?;
    Ok(start
        .zip(end)
        .map(|(start, end)| (start as u64, end as u64)))
}

fn batch(row: &sqlx::postgres::PgRow) -> Result<Batch, Error> {
    Ok(Batch {
        start: row.try_get::<i64, _>(0)? as u64,
        end: row.try_get::<i64, _>(1)? as u64,
        epoch: row.try_get::<i64, _>(2)? as u64,
        body: Bytes::from(row.try_get::<Vec<u8>, _>(3)?),
    })
}

struct Recovery {
    reader: Reader,
    watermark: u64,
    slots: Option<VecDeque<usize>>,
    after: i64,
    expected: Option<u64>,
}

impl Recovery {
    async fn next(&mut self) -> Result<Option<Vec<RecoverResult>>, Error> {
        if self.slots.is_none() {
            self.slots = Some(self.order().await?);
        }
        loop {
            let Some(&index) = self.slots.as_ref().and_then(VecDeque::front) else {
                return Ok(None);
            };
            let Some(batch) = self.reader.first(index, self.watermark, self.after).await? else {
                self.slots.as_mut().map(VecDeque::pop_front);
                self.after = -1;
                continue;
            };
            if let Some(expected) = self.expected
                && batch.start != expected
            {
                tracing::error!(
                    expected,
                    found = batch.start,
                    "wal has a gap: recovery stops at the last contiguous record"
                );
                return Ok(None);
            }
            self.expected = Some(batch.end);
            self.after = signed(batch.start)?;
            let (epoch, framed) = batch.decode()?;
            return Ok(Some(
                framed
                    .into_iter()
                    .filter(|f| f.offset >= self.watermark)
                    .map(|f| RecoverResult {
                        record: f.record,
                        record_offset: RecordOffset {
                            epoch,
                            offset: f.offset,
                            size: f.size,
                        },
                    })
                    .collect(),
            ));
        }
    }

    async fn order(&self) -> Result<VecDeque<usize>, Error> {
        let mut starts = Vec::new();
        for (index, table) in self.reader.schema.slots().iter().enumerate() {
            if let Some((start, _)) = extent(&self.reader.pool, table, self.watermark).await? {
                starts.push((start, index));
            }
        }
        starts.sort_unstable();
        Ok(starts.into_iter().map(|(_, index)| index).collect())
    }
}

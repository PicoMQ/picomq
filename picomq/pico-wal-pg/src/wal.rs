use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use s3stream_codec::StreamRecordBatch;
use s3stream_wal::{
    AppendListener, PendingAppend, RecordOffset, RecoverStream, WalError, WalMetadata,
    WriteAheadLog,
};
use sqlx::PgPool;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};

use crate::config::Config;
use crate::error::Error;
use crate::health::Health;
use crate::node::{Node, State};
use crate::reader::{self, Reader};
use crate::reclaim::{self, Reclaimer};
use crate::ring::Ring;
use crate::schema::Schema;
use crate::writer::Writer;

pub struct PgWal {
    config: Config,
    pool: PgPool,
    schema: Arc<Schema>,
    ring: Arc<Mutex<Ring>>,
    node: Node,
    writer: Writer,
    reader: Reader,
    reclaimer: Reclaimer,
    watermark: AtomicU64,
}

impl PgWal {
    pub fn new(config: Config) -> Result<Self, Error> {
        let options = PgConnectOptions::from_str(config.connection())?
            .application_name("picomq-wal")
            .options([("synchronous_commit", config.synchronous_commit.as_str())]);
        let pool = PgPoolOptions::new()
            .max_connections(config.max_inflight as u32 + 2)
            .connect_lazy_with(options);
        let schema = Arc::new(Schema::new(
            &config.cluster_id,
            config.node_id,
            config.segments,
        ));
        let ring = Arc::new(Mutex::new(Ring::new(
            config.segment_bytes,
            config.segments as usize,
        )));
        Ok(Self {
            node: Node::new(pool.clone(), config.cluster_id.clone()),
            writer: Writer::new(
                pool.clone(),
                Arc::clone(&schema),
                Arc::clone(&ring),
                &config,
            ),
            reader: Reader::new(pool.clone(), Arc::clone(&schema)),
            reclaimer: Reclaimer::new(pool.clone(), Arc::clone(&schema), Arc::clone(&ring)),
            watermark: AtomicU64::new(0),
            config,
            pool,
            schema,
            ring,
        })
    }

    async fn open(&self) -> Result<(), Error> {
        let config = &self.config;
        let health = Health::probe(&self.pool, config.allow_unsafe).await?;
        self.schema.migrate(&self.pool).await?;
        self.node.claim(config.node_id, config.epoch).await?;
        let state = self.node.load(config.node_id).await?;
        health.compare(state.timeline);
        self.reshape(&state).await?;
        let mut frontier = state.trim;
        for index in 0..self.schema.slots().len() {
            let Some((start, end)) = self.reader.extent(index).await? else {
                continue;
            };
            if !self
                .ring
                .lock()
                .expect("ring poisoned")
                .restore(index, start, end)
            {
                return Err(Error::Corrupt(format!(
                    "{} holds offset {start}, which belongs to another slot",
                    self.schema.slot(index)
                )));
            }
            frontier = frontier.max(end);
        }
        self.node
            .stamp(
                config.node_id,
                config.epoch,
                health.timeline,
                config.segments,
                config.segment_bytes,
            )
            .await?;
        self.watermark.store(state.trim, Ordering::SeqCst);
        self.writer.start(frontier);
        self.reclaimer.run(state.trim, frontier).await?;
        Ok(())
    }

    async fn reshape(&self, state: &State) -> Result<(), Error> {
        let config = &self.config;
        if state.segments == 0
            || (state.segments, state.segment_bytes) == (config.segments, config.segment_bytes)
        {
            return Ok(());
        }
        let previous = Schema::new(&config.cluster_id, config.node_id, state.segments);
        if reader::residue(&self.pool, previous.slots(), state.trim).await? {
            return Err(Error::Unsafe(format!(
                "the ring changed from {} x {} to {} x {} bytes while unflushed records remain; restart once with the previous segments and segmentBytes",
                state.segments, state.segment_bytes, config.segments, config.segment_bytes
            )));
        }
        reclaim::truncate(&self.pool, previous.slots()).await
    }

    async fn advance(&self, watermark: u64) -> Result<(), Error> {
        if watermark <= self.watermark.load(Ordering::SeqCst) {
            return Ok(());
        }
        self.node
            .trim(self.config.node_id, self.config.epoch, watermark)
            .await?;
        self.watermark.fetch_max(watermark, Ordering::SeqCst);
        if let Err(error) = self.reclaimer.run(watermark, self.writer.frontier()).await {
            tracing::warn!(%error, "wal reclaim failed, retrying on the next trim");
        }
        Ok(())
    }
}

#[async_trait]
impl WriteAheadLog for PgWal {
    async fn start(&self) -> Result<(), WalError> {
        tracing::info!(uri = self.config.redacted(), "start postgres wal");
        Ok(self.open().await?)
    }

    async fn shutdown_gracefully(&self) {
        tracing::info!("shutdown postgres wal");
        self.writer.close().await;
        self.pool.close().await;
    }

    fn metadata(&self) -> WalMetadata {
        WalMetadata {
            node_id: self.config.node_id,
            epoch: self.config.epoch,
        }
    }

    fn uri(&self) -> &str {
        self.config.redacted()
    }

    fn submit(&self, record: StreamRecordBatch) -> Result<PendingAppend, WalError> {
        self.writer.submit(record)
    }

    fn set_append_listener(&self, listener: AppendListener) {
        self.writer.listen(listener);
    }

    async fn get(&self, offset: RecordOffset) -> Result<StreamRecordBatch, WalError> {
        Ok(self.reader.get(offset).await?)
    }

    async fn get_range(
        &self,
        start: RecordOffset,
        end: RecordOffset,
    ) -> Result<Vec<StreamRecordBatch>, WalError> {
        Ok(self.reader.range(start.offset, end.offset).await?)
    }

    fn confirm_offset(&self) -> RecordOffset {
        RecordOffset {
            epoch: self.config.epoch,
            offset: self.writer.confirmed(),
            size: 0,
        }
    }

    fn recover(&self) -> RecoverStream {
        self.reader.recover(self.watermark.load(Ordering::SeqCst))
    }

    async fn reset(&self) -> Result<(), WalError> {
        Ok(self.advance(self.writer.confirmed()).await?)
    }

    async fn trim(&self, offset: RecordOffset) -> Result<(), WalError> {
        let watermark = (offset.offset + 1).min(self.writer.confirmed());
        Ok(self.advance(watermark).await?)
    }
}

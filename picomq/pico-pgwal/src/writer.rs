use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Mutex, RwLock, Weak};
use std::time::Duration;

use s3stream_codec::StreamRecordBatch;
use s3stream_wal::{AppendListener, AppendResult, PendingAppend, RecordOffset, WalError};
use sqlx::{AssertSqlSafe, PgPool};
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore, oneshot, watch};

use crate::config::Config;
use crate::frame;
use crate::node::signed;
use crate::ring::Ring;
use crate::schema::{NODE, Schema};

const RETRY_FLOOR: Duration = Duration::from_millis(50);
const RETRY_CEILING: Duration = Duration::from_secs(2);

struct Pending {
    record: StreamRecordBatch,
    offset: u64,
    size: u32,
    ack: oneshot::Sender<Result<AppendResult, WalError>>,
}

struct Batch {
    start: u64,
    end: u64,
    records: Vec<Pending>,
}

#[derive(Clone, Debug, PartialEq)]
enum Failure {
    Fenced,
    Shutdown,
    Broken(String),
}

#[derive(Default)]
struct State {
    running: bool,
    failure: Option<Failure>,
    next: u64,
    unconfirmed: u64,
    active: Option<Batch>,
    sealed: VecDeque<Batch>,
    done: BTreeMap<u64, Batch>,
}

struct Inner {
    pool: PgPool,
    schema: Arc<Schema>,
    ring: Arc<Mutex<Ring>>,
    cluster_id: String,
    node_id: u32,
    epoch: u64,
    batch_interval: Duration,
    max_bytes_in_batch: u64,
    max_unflushed_bytes: u64,
    max_record_bytes: u64,
    capacity: u64,
    state: Mutex<State>,
    listener: RwLock<Option<AppendListener>>,
    permits: Arc<Semaphore>,
    wake: Arc<Notify>,
    confirmed: watch::Sender<u64>,
    closing: watch::Sender<bool>,
}

pub(crate) struct Writer {
    inner: Arc<Inner>,
}

impl Writer {
    pub(crate) fn new(
        pool: PgPool,
        schema: Arc<Schema>,
        ring: Arc<Mutex<Ring>>,
        config: &Config,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                pool,
                schema,
                ring,
                cluster_id: config.cluster_id.clone(),
                node_id: config.node_id,
                epoch: config.epoch,
                batch_interval: config.batch_interval,
                max_bytes_in_batch: config.max_bytes_in_batch,
                max_unflushed_bytes: config.max_unflushed_bytes,
                max_record_bytes: config.segment_bytes,
                capacity: config.capacity(),
                state: Mutex::new(State::default()),
                listener: RwLock::new(None),
                permits: Arc::new(Semaphore::new(config.max_inflight)),
                wake: Arc::new(Notify::new()),
                confirmed: watch::Sender::new(0),
                closing: watch::Sender::new(false),
            }),
        }
    }

    pub(crate) fn start(&self, next: u64) {
        {
            let mut state = self.inner.state();
            state.running = true;
            state.next = next;
        }
        self.inner.confirmed.send_replace(next);
        tokio::spawn(flusher(
            Arc::downgrade(&self.inner),
            Arc::clone(&self.inner.wake),
        ));
    }

    pub(crate) fn listen(&self, listener: AppendListener) {
        *self.inner.listener.write().expect("listener poisoned") = Some(listener);
    }

    pub(crate) fn confirmed(&self) -> u64 {
        *self.inner.confirmed.borrow()
    }

    pub(crate) fn frontier(&self) -> u64 {
        self.inner.state().next
    }

    pub(crate) fn submit(&self, record: StreamRecordBatch) -> Result<PendingAppend, WalError> {
        let inner = &self.inner;
        let size = frame::size(&record);
        if size > inner.max_record_bytes {
            return Err(WalError::RecordTooLarge {
                size,
                max: inner.max_record_bytes,
            });
        }
        let (ack, durable) = oneshot::channel();
        let wake = {
            let mut state = inner.state();
            if let Some(failure) = &state.failure {
                return Err(inner.error(failure));
            }
            if !state.running {
                return Err(WalError::NotInitialized);
            }
            if state.unconfirmed >= inner.max_unflushed_bytes {
                return Err(WalError::OverCapacity {
                    unconfirmed_bytes: state.unconfirmed,
                    cap_bytes: inner.max_unflushed_bytes,
                });
            }
            let opened = state.active.is_none();
            if opened {
                if !inner.ring.lock().expect("ring poisoned").open(state.next) {
                    return Err(WalError::OverCapacity {
                        unconfirmed_bytes: state.unconfirmed,
                        cap_bytes: inner.capacity,
                    });
                }
                state.active = Some(Batch {
                    start: state.next,
                    end: state.next,
                    records: Vec::new(),
                });
            }
            let offset = state.next;
            state.next += size;
            state.unconfirmed += size;
            let end = state.next;
            let batch = state.active.as_mut().expect("active batch opened above");
            batch.records.push(Pending {
                record,
                offset,
                size: size as u32,
                ack,
            });
            batch.end = end;
            let full = batch.end - batch.start >= inner.max_bytes_in_batch;
            if full {
                let batch = state.active.take().expect("active batch opened above");
                state.sealed.push_back(batch);
            }
            opened || full
        };
        if wake {
            inner.wake.notify_one();
        }
        Ok(PendingAppend {
            durable: Box::pin(async move { durable.await.unwrap_or(Err(WalError::Shutdown)) }),
        })
    }

    pub(crate) async fn close(&self) {
        {
            let mut state = self.inner.state();
            state.running = false;
            if let Some(batch) = state.active.take() {
                state.sealed.push_back(batch);
            }
        }
        self.inner.closing.send_replace(true);
        self.inner.wake.notify_one();
        let mut confirmed = self.inner.confirmed.subscribe();
        loop {
            {
                let state = self.inner.state();
                if state.failure.is_some() || *confirmed.borrow_and_update() >= state.next {
                    return;
                }
            }
            if confirmed.changed().await.is_err() {
                return;
            }
        }
    }
}

impl Drop for Writer {
    fn drop(&mut self) {
        self.inner.wake.notify_one();
    }
}

impl Inner {
    fn state(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().expect("writer state poisoned")
    }

    fn error(&self, failure: &Failure) -> WalError {
        match failure {
            Failure::Fenced => WalError::Fenced {
                node_id: self.node_id,
                our_epoch: self.epoch,
            },
            Failure::Shutdown => WalError::Shutdown,
            Failure::Broken(reason) => WalError::Io(std::io::Error::other(reason.clone())),
        }
    }

    fn take(&self) -> Option<Batch> {
        let mut state = self.state();
        state.sealed.pop_front().or_else(|| state.active.take())
    }

    fn idle(&self) -> bool {
        let state = self.state();
        !state.running && state.active.is_none() && state.sealed.is_empty()
    }

    async fn persist(&self, batch: &Batch) -> Result<(), Failure> {
        let table = self
            .schema
            .slot(self.ring.lock().expect("ring poisoned").slot(batch.start));
        let body = frame::encode(batch.start, batch.records.iter().map(|p| &p.record));
        let sql = format!(
            "INSERT INTO {table} (start_offset, end_offset, epoch, body) \
             SELECT $1, $2, $3, $4 FROM {NODE} \
             WHERE cluster_id = $5 AND node_id = $6 AND epoch = $3 FOR SHARE \
             ON CONFLICT (start_offset) DO NOTHING"
        );
        let start = signed(batch.start).map_err(broken)?;
        let end = signed(batch.end).map_err(broken)?;
        let epoch = signed(self.epoch).map_err(broken)?;
        let mut closing = self.closing.subscribe();
        let mut delay = RETRY_FLOOR;
        let mut attempts: u32 = 0;
        loop {
            let attempt = async {
                let inserted = sqlx::query(AssertSqlSafe(sql.as_str()))
                    .bind(start)
                    .bind(end)
                    .bind(epoch)
                    .bind(body.as_slice())
                    .bind(&self.cluster_id)
                    .bind(i64::from(self.node_id))
                    .execute(&self.pool)
                    .await?
                    .rows_affected();
                if inserted == 1 {
                    return Ok(None);
                }
                self.stored(table, start).await.map(Some)
            }
            .await;
            match attempt {
                Ok(None) => return Ok(()),
                Ok(Some(stored)) => return reconcile(stored, start, end, epoch),
                Err(error) if retryable(&error) => {
                    attempts = attempts.saturating_add(1);
                    if attempts.is_power_of_two() {
                        tracing::warn!(%error, start = batch.start, attempts, "wal commit failed, retrying");
                    }
                    tokio::select! {
                        () = tokio::time::sleep(delay) => {}
                        _ = closing.wait_for(|closing| *closing) => return Err(Failure::Shutdown),
                    }
                    delay = (delay * 2).min(RETRY_CEILING);
                }
                Err(error) => return Err(broken(error)),
            }
        }
    }

    async fn stored(&self, table: &str, start: i64) -> Result<Option<(i64, i64)>, sqlx::Error> {
        sqlx::query_as(AssertSqlSafe(format!(
            "SELECT end_offset, epoch FROM {table} WHERE start_offset = $1"
        )))
        .bind(start)
        .fetch_optional(&self.pool)
        .await
    }

    fn complete(&self, batch: Batch, outcome: Result<(), Failure>) {
        let mut state = self.state();
        if let Err(failure) = outcome {
            state.failure.get_or_insert(failure);
        }
        if let Some(failure) = state.failure.clone() {
            let mut failed = vec![batch];
            failed.extend(std::mem::take(&mut state.done).into_values());
            failed.extend(std::mem::take(&mut state.sealed));
            failed.extend(state.active.take());
            drop(state);
            for pending in failed.into_iter().flat_map(|batch| batch.records) {
                let _ = pending.ack.send(Err(self.error(&failure)));
            }
            self.confirmed.send_modify(|_| {});
            return;
        }
        state.done.insert(batch.start, batch);
        let listener = self.listener.read().expect("listener poisoned").clone();
        let mut confirmed = *self.confirmed.borrow();
        while let Some(batch) = state.done.remove(&confirmed) {
            confirmed = batch.end;
            state.unconfirmed -= batch.end - batch.start;
            self.confirmed.send_replace(confirmed);
            for pending in batch.records {
                let record_offset = RecordOffset {
                    epoch: self.epoch,
                    offset: pending.offset,
                    size: pending.size,
                };
                let next_offset = RecordOffset {
                    epoch: self.epoch,
                    offset: record_offset.end_offset(),
                    size: 0,
                };
                if let Some(listener) = &listener {
                    listener(&pending.record, record_offset, next_offset);
                }
                let _ = pending.ack.send(Ok(AppendResult {
                    record_offset,
                    next_offset,
                }));
            }
        }
    }
}

async fn flusher(inner: Weak<Inner>, wake: Arc<Notify>) {
    loop {
        wake.notified().await;
        let Some(strong) = inner.upgrade() else {
            return;
        };
        if !strong.batch_interval.is_zero() {
            tokio::time::sleep(strong.batch_interval).await;
        }
        loop {
            let permit = Arc::clone(&strong.permits)
                .acquire_owned()
                .await
                .expect("permits are never closed");
            let Some(batch) = strong.take() else {
                break;
            };
            tokio::spawn(commit(Arc::clone(&strong), batch, permit));
        }
        if strong.idle() {
            return;
        }
    }
}

async fn commit(inner: Arc<Inner>, batch: Batch, permit: OwnedSemaphorePermit) {
    let outcome = inner.persist(&batch).await;
    drop(permit);
    inner
        .ring
        .lock()
        .expect("ring poisoned")
        .settle(batch.start, batch.end);
    inner.complete(batch, outcome);
}

fn broken(error: impl std::fmt::Display) -> Failure {
    Failure::Broken(error.to_string())
}

fn reconcile(stored: Option<(i64, i64)>, start: i64, end: i64, epoch: i64) -> Result<(), Failure> {
    match stored {
        Some(row) if row == (end, epoch) => Ok(()),
        Some((_, stored_epoch)) if stored_epoch != epoch => Err(Failure::Fenced),
        Some((stored_end, _)) => Err(Failure::Broken(format!(
            "wal row at {start} holds end {stored_end}, expected end {end} in epoch {epoch}"
        ))),
        None => Err(Failure::Fenced),
    }
}

fn retryable(error: &sqlx::Error) -> bool {
    match error {
        sqlx::Error::Io(_) | sqlx::Error::PoolTimedOut | sqlx::Error::Tls(_) => true,
        sqlx::Error::Database(error) => error.code().is_some_and(|code| {
            code.starts_with("08") || code.starts_with("57P") || code == "40001" || code == "40P01"
        }),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reconcile_tells_our_commit_from_a_fence_and_from_corruption() {
        assert_eq!(reconcile(Some((200, 3)), 100, 200, 3), Ok(()));
        assert_eq!(reconcile(None, 100, 200, 3), Err(Failure::Fenced));
        assert_eq!(reconcile(Some((180, 4)), 100, 200, 3), Err(Failure::Fenced));
        assert!(matches!(
            reconcile(Some((180, 3)), 100, 200, 3),
            Err(Failure::Broken(_))
        ));
    }
}

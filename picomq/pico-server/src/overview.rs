use std::collections::HashMap;
use std::sync::Arc;

use picomq_common::now_ms;
use picomq_metadata::{MetadataState, ViewPublisher};
use tokio::sync::Mutex;

use crate::error::{ErrorKind, ServiceError};
use crate::log::Log;
use crate::partition::Partition;
use crate::topic::{Topic, TopicId};
use crate::topics::live_topics;

const REFRESH_MS: i64 = 60_000;
const RANKED: usize = 10;

pub struct Overview {
    views: Arc<ViewPublisher>,
    latest: Mutex<Option<Pass>>,
}

#[derive(Debug, Clone)]
pub struct Rankings {
    pub computed_at_ms: i64,
    pub by_size: Vec<(Topic, u64)>,
    pub by_write_rate: Vec<(Topic, f64)>,
    pub write_rate_since_ms: Option<i64>,
}

struct Pass {
    rankings: Arc<Rankings>,
    ends: HashMap<TopicId, u64>,
}

struct Measured {
    topic: Topic,
    stored_bytes: u64,
    end: u64,
}

impl Overview {
    pub fn new(views: Arc<ViewPublisher>) -> Self {
        Self {
            views,
            latest: Mutex::new(None),
        }
    }

    pub async fn rankings(&self) -> Result<Arc<Rankings>, ServiceError> {
        self.rankings_at(now_ms()).await
    }

    pub async fn rankings_at(&self, now_ms: i64) -> Result<Arc<Rankings>, ServiceError> {
        let mut latest = self.latest.lock().await;
        if let Some(pass) = latest.as_ref()
            && now_ms - pass.rankings.computed_at_ms < REFRESH_MS
        {
            return Ok(pass.rankings.clone());
        }

        let view = self.views.load();
        let previous = latest.take();
        let pass = tokio::task::spawn_blocking(move || measure(&view.state, now_ms, previous))
            .await
            .map_err(|_| ServiceError::kind(ErrorKind::Internal))?;

        let rankings = pass.rankings.clone();
        *latest = Some(pass);
        Ok(rankings)
    }
}

fn measure(state: &MetadataState, now_ms: i64, previous: Option<Pass>) -> Pass {
    let mut measured: Vec<Measured> = live_topics(state, now_ms)
        .map(|topic| {
            let (stored_bytes, end) = (0..topic.partitions)
                .filter_map(|index| {
                    let log = Log::Partition(Partition::new(topic.id, index));
                    log.stream_id(state).ok().flatten()
                })
                .filter_map(|stream_id| state.streams().get(&stream_id))
                .fold((0, 0), |(bytes, end), row| {
                    (bytes + row.stored_bytes, end + row.end_offset)
                });
            Measured {
                topic,
                stored_bytes,
                end,
            }
        })
        .collect();
    let ends = measured
        .iter()
        .map(|measured| (measured.topic.id, measured.end))
        .collect();

    let write_rate_since_ms = previous
        .as_ref()
        .map(|previous| previous.rankings.computed_at_ms)
        .filter(|since| *since < now_ms);
    let mut by_write_rate = match (&previous, write_rate_since_ms) {
        (Some(previous), Some(since)) => {
            let seconds = (now_ms - since) as f64 / 1000.0;
            measured
                .iter()
                .filter_map(|measured| {
                    let before = previous.ends.get(&measured.topic.id)?;
                    let written = measured.end.saturating_sub(*before);
                    (written > 0).then(|| (measured.topic.clone(), written as f64 / seconds))
                })
                .collect()
        }
        _ => Vec::new(),
    };
    by_write_rate.sort_by(|a, b| {
        b.1.total_cmp(&a.1)
            .then_with(|| a.0.name.canonical().cmp(b.0.name.canonical()))
    });
    by_write_rate.truncate(RANKED);

    measured.sort_by(|a, b| {
        b.stored_bytes
            .cmp(&a.stored_bytes)
            .then_with(|| a.topic.name.canonical().cmp(b.topic.name.canonical()))
    });
    let by_size = measured
        .into_iter()
        .take(RANKED)
        .map(|measured| (measured.topic, measured.stored_bytes))
        .collect();

    Pass {
        rankings: Arc::new(Rankings {
            computed_at_ms: now_ms,
            by_size,
            by_write_rate,
            write_rate_since_ms,
        }),
        ends,
    }
}

use std::sync::{Arc, Mutex};
use std::time::Duration;

use picomq_protocol::groups::{
    DEFAULT_SESSION_TIMEOUT_MS, JoinRequest, JoinResponse, MemberFence, Offsets,
};
use picomq_protocol::pico::{E_ILLEGAL_GENERATION, E_REBALANCE_IN_PROGRESS, E_UNKNOWN_MEMBER};
use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::error::{ClientError, Result};
use crate::pico::PicoClient;
use crate::retry::RetryPolicy;

#[derive(Debug, Clone)]
pub struct GroupConfig {
    pub session_timeout: Duration,
    /// Defaults to a third of the session timeout.
    pub heartbeat_interval: Option<Duration>,
    pub rebalance_timeout: Option<Duration>,
    pub instance_id: Option<String>,
    pub client_id: Option<String>,
    pub retry: RetryPolicy,
}

impl Default for GroupConfig {
    fn default() -> Self {
        Self {
            session_timeout: Duration::from_millis(u64::from(DEFAULT_SESSION_TIMEOUT_MS)),
            heartbeat_interval: None,
            rebalance_timeout: None,
            instance_id: None,
            client_id: None,
            retry: RetryPolicy {
                max_attempts: u32::MAX,
                initial_backoff: Duration::from_millis(100),
                max_backoff: Duration::from_secs(5),
                multiplier: 2.0,
            },
        }
    }
}

impl GroupConfig {
    fn join_request(
        &self,
        group: &str,
        subscription: &[String],
        member_id: Option<String>,
    ) -> JoinRequest {
        JoinRequest {
            group: group.to_owned(),
            subscription: subscription.to_vec(),
            member_id,
            instance_id: self.instance_id.clone(),
            client_id: self.client_id.clone(),
            session_timeout_ms: Some(millis(self.session_timeout)),
            rebalance_timeout_ms: self.rebalance_timeout.map(millis),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Assignment {
    pub generation: i32,
    pub streams: Vec<String>,
}

#[derive(Debug, Default)]
struct Session {
    member_id: String,
    generation: i32,
    failed: Option<ClientError>,
}

impl Session {
    fn fence(&self, instance_id: Option<&String>) -> MemberFence {
        MemberFence {
            member_id: self.member_id.clone(),
            generation: self.generation,
            instance_id: instance_id.cloned(),
        }
    }
}

/// One member of a server-assigned consumer group. Joins on construction,
/// heartbeats in the background, rejoins when the coordinator rebalances,
/// and publishes each new assignment through [`GroupMember::assignments`].
pub struct GroupMember {
    client: Arc<PicoClient>,
    group: String,
    config: GroupConfig,
    session: Arc<Mutex<Session>>,
    assignments: watch::Receiver<Assignment>,
    heartbeats: JoinHandle<()>,
}

impl GroupMember {
    pub async fn join(
        client: Arc<PicoClient>,
        group: &str,
        subscription: Vec<String>,
        config: GroupConfig,
    ) -> Result<Self> {
        let request = config.join_request(group, &subscription, None);
        let joined = config.retry.run(|| client.join_group(&request)).await?;
        let session = Arc::new(Mutex::new(Session {
            member_id: joined.member_id,
            generation: joined.generation,
            failed: None,
        }));
        let (publish, assignments) = watch::channel(Assignment {
            generation: joined.generation,
            streams: joined.assignment,
        });
        let heartbeats = tokio::spawn(run(
            Arc::clone(&client),
            group.to_owned(),
            subscription,
            config.clone(),
            Arc::clone(&session),
            publish,
        ));
        Ok(Self {
            client,
            group: group.to_owned(),
            config,
            session,
            assignments,
            heartbeats,
        })
    }

    pub fn member_id(&self) -> String {
        self.session.lock().unwrap().member_id.clone()
    }

    pub fn assignment(&self) -> Assignment {
        self.assignments.borrow().clone()
    }

    /// Closes when the member stops; [`GroupMember::error`] then says why.
    pub fn assignments(&self) -> watch::Receiver<Assignment> {
        self.assignments.clone()
    }

    pub fn error(&self) -> Option<ClientError> {
        self.session.lock().unwrap().failed.clone()
    }

    pub async fn commit(&self, offsets: &Offsets) -> Result<()> {
        let fence = {
            let session = self.session.lock().unwrap();
            if let Some(error) = &session.failed {
                return Err(error.clone());
            }
            session.fence(self.config.instance_id.as_ref())
        };
        self.client
            .commit_offsets(&self.group, Some(&fence), offsets)
            .await
    }

    pub async fn fetch_offsets(&self, streams: &[String]) -> Result<Offsets> {
        self.client.fetch_offsets(&self.group, streams).await
    }

    pub async fn leave(self) -> Result<()> {
        self.heartbeats.abort();
        if self.error().is_some() {
            return Ok(());
        }
        self.client
            .leave_group(
                &self.group,
                &self.member_id(),
                self.config.instance_id.as_deref(),
            )
            .await
    }
}

impl Drop for GroupMember {
    fn drop(&mut self) {
        self.heartbeats.abort();
    }
}

async fn run(
    client: Arc<PicoClient>,
    group: String,
    subscription: Vec<String>,
    config: GroupConfig,
    session: Arc<Mutex<Session>>,
    publish: watch::Sender<Assignment>,
) {
    let interval = config
        .heartbeat_interval
        .unwrap_or(config.session_timeout / 3)
        .max(Duration::from_millis(1));
    let mut attempt = 0u32;
    loop {
        tokio::time::sleep(interval).await;
        let fence = session.lock().unwrap().fence(config.instance_id.as_ref());
        let outcome = match client.heartbeat(&group, &fence).await {
            Ok(()) => Ok(None),
            Err(error)
                if error.code == E_REBALANCE_IN_PROGRESS || error.code == E_ILLEGAL_GENERATION =>
            {
                rejoin(
                    &client,
                    &group,
                    &subscription,
                    &config,
                    Some(fence.member_id),
                )
                .await
            }
            Err(error) if error.code == E_UNKNOWN_MEMBER => {
                rejoin(&client, &group, &subscription, &config, None).await
            }
            Err(error) if error.retryable() => match config.retry.delay(attempt) {
                Some(delay) => {
                    attempt += 1;
                    tokio::time::sleep(delay).await;
                    continue;
                }
                None => Err(error),
            },
            Err(error) => Err(error),
        };
        match outcome {
            Ok(None) => attempt = 0,
            Ok(Some(joined)) => {
                attempt = 0;
                let mut session = session.lock().unwrap();
                session.member_id = joined.member_id;
                session.generation = joined.generation;
                publish.send_replace(Assignment {
                    generation: joined.generation,
                    streams: joined.assignment,
                });
            }
            Err(error) => {
                session.lock().unwrap().failed = Some(error);
                return;
            }
        }
    }
}

async fn rejoin(
    client: &PicoClient,
    group: &str,
    subscription: &[String],
    config: &GroupConfig,
    mut member_id: Option<String>,
) -> Result<Option<JoinResponse>> {
    let mut attempt = 0u32;
    loop {
        let request = config.join_request(group, subscription, member_id.clone());
        match client.join_group(&request).await {
            Ok(joined) => return Ok(Some(joined)),
            Err(error) if error.code == E_UNKNOWN_MEMBER => member_id = None,
            Err(error) if error.retryable() => {
                let Some(delay) = config.retry.delay(attempt) else {
                    return Err(error);
                };
                attempt += 1;
                tokio::time::sleep(delay).await;
            }
            Err(error) => return Err(error),
        }
    }
}

fn millis(duration: Duration) -> u32 {
    u32::try_from(duration.as_millis()).unwrap_or(u32::MAX)
}

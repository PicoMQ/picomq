use anyhow::anyhow;
use http::Extensions;
use humantime::Duration as HumanDuration;
use rand::RngExt as _;
use reqwest_middleware::{ClientBuilder, ClientWithMiddleware, Middleware, Next};
use std::str::FromStr;
use std::time::Duration;
use tokio::sync::Mutex;
use tracing::{info, warn};

#[derive(Debug)]
struct CircuitState {
    consecutive_failures: u32,
    open_until: Option<tokio::time::Instant>,
}

#[derive(Debug)]
pub struct CircuitBreaker {
    threshold: u32,
    cool_down: Duration,
    state: Mutex<CircuitState>,
}

impl CircuitBreaker {
    pub fn new(threshold: u32, cool_down: Duration) -> Self {
        Self {
            threshold,
            cool_down,
            state: Mutex::new(CircuitState {
                consecutive_failures: 0,
                open_until: None,
            }),
        }
    }

    pub fn record_success(&self) {
        if let Ok(mut s) = self.state.try_lock() {
            s.consecutive_failures = 0;
            s.open_until = None;
        }
    }

    pub async fn record_failure(&self) {
        let mut s = self.state.lock().await;
        s.consecutive_failures = s.consecutive_failures.saturating_add(1);
        if s.consecutive_failures >= self.threshold {
            let deadline = tokio::time::Instant::now() + self.cool_down;
            s.open_until = Some(deadline);
            warn!(
                "Circuit breaker OPENED after {} consecutive failures. \
                 Pausing for {:?}.",
                s.consecutive_failures, self.cool_down
            );
        }
    }

    pub async fn is_open(&self) -> bool {
        let mut s = self.state.lock().await;
        match s.open_until {
            None => false,
            Some(deadline) if tokio::time::Instant::now() < deadline => true,
            Some(_) => {
                s.open_until = None;
                s.consecutive_failures = 0;
                info!("Circuit breaker entering HALF-OPEN state.");
                false
            }
        }
    }
}

pub fn parse_duration(value: Option<&str>, default_value: &str) -> Duration {
    let raw = value.unwrap_or(default_value);
    HumanDuration::from_str(raw)
        .map(|d| d.into())
        .unwrap_or_else(|e| {
            if value.is_some() {
                warn!(
                    "Invalid duration {:?}: {e}. Falling back to 1s. \
                     Use humantime format, e.g. \"5s\", \"1m30s\", \"200ms\".",
                    raw
                );
            }
            Duration::from_secs(1)
        })
}

pub fn jitter(base: Duration) -> Duration {
    let millis = base.as_millis() as u64;
    let jitter_range = millis / 5;
    if jitter_range == 0 {
        return base;
    }
    let delta = rand::rng().random_range(0..=jitter_range * 2);
    Duration::from_millis(millis.saturating_sub(jitter_range).saturating_add(delta))
}

pub fn exponential_backoff(base: Duration, attempt: u32, max_delay: Duration) -> Duration {
    let factor = 2u64.saturating_pow(attempt);
    let millis = base
        .as_millis()
        .saturating_mul(factor as u128)
        .min(max_delay.as_millis());
    let millis_u64 = u64::try_from(millis).unwrap_or(u64::MAX);
    Duration::from_millis(millis_u64)
}

pub fn parse_retry_after(value: &str) -> Option<Duration> {
    if let Ok(secs) = value.trim().parse::<u64>() {
        return Some(Duration::from_secs(secs));
    }
    None
}

pub fn is_transient_status(status: reqwest::StatusCode) -> bool {
    status == reqwest::StatusCode::TOO_MANY_REQUESTS || status.is_server_error()
}

#[derive(Debug, Clone)]
pub struct HttpRetryMiddleware {
    max_retries: u32,
    retry_delay: Duration,
    max_delay: Duration,
    log_prefix: &'static str,
}

impl HttpRetryMiddleware {
    pub fn new(
        max_retries: u32,
        retry_delay: Duration,
        max_delay: Duration,
        log_prefix: &'static str,
    ) -> Self {
        Self {
            max_retries,
            retry_delay,
            max_delay,
            log_prefix,
        }
    }
}

#[async_trait::async_trait]
impl Middleware for HttpRetryMiddleware {
    async fn handle(
        &self,
        req: reqwest::Request,
        extensions: &mut Extensions,
        next: Next<'_>,
    ) -> reqwest_middleware::Result<reqwest::Response> {
        let mut current_req = req;
        let mut attempts = 0u32;

        loop {
            let next_req = current_req.try_clone();

            match next.clone().run(current_req, extensions).await {
                Ok(response) => {
                    let status = response.status();

                    if status.is_success() {
                        return Ok(response);
                    }

                    let retry_after = if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
                        response
                            .headers()
                            .get("Retry-After")
                            .and_then(|v| v.to_str().ok())
                            .and_then(parse_retry_after)
                    } else {
                        None
                    };

                    attempts += 1;
                    if is_transient_status(status) && attempts < self.max_retries {
                        let body_text = response.text().await.unwrap_or_default();
                        let delay = retry_after.unwrap_or_else(|| {
                            jitter(exponential_backoff(
                                self.retry_delay,
                                attempts,
                                self.max_delay,
                            ))
                        });
                        warn!(
                            "{} transient error {status} \
                             (attempt {attempts}/{}): {body_text}. \
                             Retrying in {delay:?}...",
                            self.log_prefix, self.max_retries
                        );
                        tokio::time::sleep(delay).await;
                        current_req = match next_req {
                            Some(r) => r,
                            None => {
                                return Err(reqwest_middleware::Error::Middleware(anyhow!(
                                    "request body is not cloneable, cannot retry"
                                )));
                            }
                        };
                        continue;
                    }

                    return Ok(response);
                }
                Err(e) => {
                    attempts += 1;
                    if attempts < self.max_retries {
                        let delay = jitter(exponential_backoff(
                            self.retry_delay,
                            attempts,
                            self.max_delay,
                        ));
                        warn!(
                            "{} network error (attempt {attempts}/{}): {e}. \
                             Retrying in {delay:?}...",
                            self.log_prefix, self.max_retries
                        );
                        tokio::time::sleep(delay).await;
                        current_req = match next_req {
                            Some(r) => r,
                            None => return Err(e),
                        };
                        continue;
                    }
                    return Err(e);
                }
            }
        }
    }
}

pub fn build_retry_client(
    client: reqwest::Client,
    max_retries: u32,
    retry_delay: Duration,
    max_delay: Duration,
    log_prefix: &'static str,
) -> ClientWithMiddleware {
    ClientBuilder::new(client)
        .with(HttpRetryMiddleware::new(
            max_retries,
            retry_delay,
            max_delay,
            log_prefix,
        ))
        .build()
}

pub struct ConnectivityConfig {
    pub max_open_retries: u32,
    pub open_retry_max_delay: Duration,
    pub retry_delay: Duration,
}

pub async fn check_connectivity(
    client: &reqwest::Client,
    url: reqwest::Url,
    connector_label: &str,
) -> Result<(), crate::Error> {
    let response = client.get(url).send().await.map_err(|e| {
        crate::Error::Connection(format!("{connector_label} health check failed: {e}"))
    })?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response
            .text()
            .await
            .unwrap_or_else(|_| "failed to read response body".to_string());
        return Err(crate::Error::Connection(format!(
            "{connector_label} health check returned status {status}: {body}"
        )));
    }
    Ok(())
}

pub async fn check_connectivity_with_retry(
    client: &reqwest::Client,
    url: reqwest::Url,
    connector_label: &str,
    connector_id: u32,
    cfg: &ConnectivityConfig,
) -> Result<(), crate::Error> {
    let max_open_retries = cfg.max_open_retries.max(1);
    let mut attempt = 0u32;

    loop {
        match check_connectivity(client, url.clone(), connector_label).await {
            Ok(()) => {
                if attempt > 0 {
                    tracing::info!(
                        "{connector_label} connectivity established after {attempt} retries \
                         for connector ID: {connector_id}"
                    );
                }
                return Ok(());
            }
            Err(e) => {
                attempt += 1;
                if attempt >= max_open_retries {
                    tracing::error!(
                        "{connector_label} connectivity check failed after {attempt} attempts \
                         for connector ID: {connector_id}. Giving up: {e}"
                    );
                    return Err(e);
                }
                let backoff = jitter(exponential_backoff(
                    cfg.retry_delay,
                    attempt,
                    cfg.open_retry_max_delay,
                ));
                tracing::warn!(
                    "{connector_label} health check failed \
                     (attempt {attempt}/{max_open_retries}) \
                     for connector ID: {connector_id}. Retrying in {backoff:?}: {e}"
                );
                tokio::time::sleep(backoff).await;
            }
        }
    }
}

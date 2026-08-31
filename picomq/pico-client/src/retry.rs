use std::future::Future;
use std::time::Duration;

use crate::error::Result;

#[derive(Debug, Clone)]
pub struct RetryPolicy {
    pub max_attempts: u32,
    pub initial_backoff: Duration,
    pub max_backoff: Duration,
    pub multiplier: f64,
}

impl RetryPolicy {
    pub fn none() -> Self {
        Self {
            max_attempts: 1,
            initial_backoff: Duration::ZERO,
            max_backoff: Duration::ZERO,
            multiplier: 1.0,
        }
    }

    pub fn attempts(max_attempts: u32) -> Self {
        Self {
            max_attempts: max_attempts.max(1),
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(30),
            multiplier: 2.0,
        }
    }

    fn backoff(&self, attempt: u32) -> Duration {
        if self.initial_backoff.is_zero() || attempt == 0 {
            return Duration::ZERO;
        }
        let millis = self.initial_backoff.as_millis() as f64
            * self
                .multiplier
                .powi(i32::try_from(attempt - 1).unwrap_or(i32::MAX));
        Duration::from_millis(millis as u64).min(self.max_backoff)
    }

    pub fn delay(&self, attempt: u32) -> Option<Duration> {
        (attempt + 1 < self.max_attempts).then(|| self.backoff(attempt + 1))
    }

    pub async fn run<T, F, Fut>(&self, mut operation: F) -> Result<T>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        let mut attempt = 1;
        loop {
            match operation().await {
                Ok(value) => return Ok(value),
                Err(error) if attempt < self.max_attempts && error.retryable() => {
                    tokio::time::sleep(self.backoff(attempt)).await;
                    attempt += 1;
                }
                Err(error) => return Err(error),
            }
        }
    }
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self::none()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::{ClientError, ErrorKind};
    use std::sync::atomic::{AtomicU32, Ordering};

    #[tokio::test]
    async fn retries_transport_failures_up_to_the_limit() {
        let calls = AtomicU32::new(0);
        let policy = RetryPolicy {
            initial_backoff: Duration::ZERO,
            ..RetryPolicy::attempts(3)
        };
        let result: Result<()> = policy
            .run(|| async {
                calls.fetch_add(1, Ordering::Relaxed);
                Err(ClientError::transport("connection refused"))
            })
            .await;
        assert!(result.is_err());
        assert_eq!(calls.load(Ordering::Relaxed), 3);
    }

    #[tokio::test]
    async fn does_not_retry_client_errors() {
        let calls = AtomicU32::new(0);
        let policy = RetryPolicy {
            initial_backoff: Duration::ZERO,
            ..RetryPolicy::attempts(3)
        };
        let result: Result<()> = policy
            .run(|| async {
                calls.fetch_add(1, Ordering::Relaxed);
                Err(ClientError::new(404, ErrorKind::NotFound, "not_found"))
            })
            .await;
        assert!(result.is_err());
        assert_eq!(calls.load(Ordering::Relaxed), 1, "404 is not retryable");
    }
}

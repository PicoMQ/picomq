//! Async size limiter gating block-cache memory.
//!
//! `s3.cache.blockcache.DataBlockCache#sizeLimiter`. Semantics preserved exactly:
//! an acquire succeeds whenever `permits >= 0` (permits may go negative afterwards,
//! so one oversized block can always load), otherwise the caller queues. A release
//! wakes one queued waiter once permits are positive again.
//!
//! The waiter gets a oneshot wake-up and retries `try_acquire` itself, which
//! composes with async/await instead of executor hops.

use std::collections::VecDeque;
use std::sync::Mutex;

use tokio::sync::oneshot;

pub struct AsyncSizeLimiter {
    state: Mutex<State>,
}

struct State {
    permits: i64,
    waiters: VecDeque<oneshot::Sender<()>>,
}

impl AsyncSizeLimiter {
    pub fn new(permits: u64) -> Self {
        Self {
            state: Mutex::new(State {
                permits: permits as i64,
                waiters: VecDeque::new(),
            }),
        }
    }

    pub fn try_acquire(&self, required: u64) -> Result<(), oneshot::Receiver<()>> {
        let mut state = self.state.lock().expect("limiter poisoned");
        if state.permits >= 0 {
            state.permits -= required as i64;
            Ok(())
        } else {
            let (tx, rx) = oneshot::channel();
            state.waiters.push_back(tx);
            Err(rx)
        }
    }

    pub fn release(&self, required: u64) {
        let mut state = self.state.lock().expect("limiter poisoned");
        state.permits += required as i64;
        if state.permits > 0
            && let Some(waiter) = state.waiters.pop_front()
        {
            let _ = waiter.send(());
        }
    }

    pub fn required_release(&self) -> bool {
        let state = self.state.lock().expect("limiter poisoned");
        state.permits <= 0 || !state.waiters.is_empty()
    }

    pub fn permits(&self) -> i64 {
        self.state.lock().expect("limiter poisoned").permits
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn permits_may_go_negative_once() {
        let limiter = AsyncSizeLimiter::new(1);
        // First oversized acquire succeeds (permits >= 0 before), going negative.
        assert!(limiter.try_acquire(10).is_ok());
        assert_eq!(limiter.permits(), -9);
        assert!(limiter.required_release());
        // Second acquire queues.
        let rx = limiter.try_acquire(1).unwrap_err();
        limiter.release(10);
        assert_eq!(limiter.permits(), 1);
        // Waiter woken. Retry succeeds.
        assert!(rx.blocking_recv().is_ok());
        assert!(limiter.try_acquire(1).is_ok());
        assert_eq!(limiter.permits(), 0);
    }
}

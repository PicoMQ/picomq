//! Long-poll waiter registry: readers park until an append passes their offset
//! or the stream closes.
//!
//! A waiter is a `tokio::sync::oneshot` in a mutex-guarded map. The lock is
//! held only for registry mutation, never across waits.
//! `notify_append(name, next)` wakes waiters with `wait_offset < next`,
//! `notify_closed` wakes a stream's waiters and `stop` wakes everyone for
//! good.

use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use tokio::sync::oneshot;

use crate::types::OffsetToken;

#[derive(Default)]
pub struct StreamWaiterRegistry {
    waiters: Mutex<HashMap<String, Vec<Waiter>>>,
    next_token: Mutex<u64>,
    stopped: AtomicBool,
}

struct Waiter {
    token: u64,
    wait_offset: u64,
    tx: oneshot::Sender<()>,
}

impl StreamWaiterRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Park until data past `offset` is durable, the stream closes, or the
    /// timeout lapses. Returns `true` when woken, `false` on timeout.
    pub async fn wait(&self, name: &str, offset: OffsetToken, timeout: Duration) -> bool {
        let (tx, rx) = oneshot::channel();
        let token = {
            let mut next = self.next_token.lock().unwrap();
            *next += 1;
            *next
        };
        {
            let mut map = self.waiters.lock().unwrap();
            if self.stopped.load(Ordering::SeqCst) {
                return true;
            }
            map.entry(name.to_owned()).or_default().push(Waiter {
                token,
                wait_offset: offset.record_offset(),
                tx,
            });
        }

        let woken = tokio::time::timeout(timeout, rx).await.is_ok();
        if !woken {
            let mut map = self.waiters.lock().unwrap();
            if let Some(queue) = map.get_mut(name) {
                queue.retain(|w| w.token != token);
                if queue.is_empty() {
                    map.remove(name);
                }
            }
        }
        woken
    }

    pub fn notify_append(&self, name: &str, next_record_offset: u64) {
        let mut map = self.waiters.lock().unwrap();
        if let Some(queue) = map.get_mut(name) {
            let mut kept = Vec::with_capacity(queue.len());
            for waiter in queue.drain(..) {
                if next_record_offset > waiter.wait_offset {
                    let _ = waiter.tx.send(());
                } else {
                    kept.push(waiter);
                }
            }
            if kept.is_empty() {
                map.remove(name);
            } else {
                *queue = kept;
            }
        }
    }

    pub fn notify_closed(&self, name: &str) {
        if let Some(queue) = self.waiters.lock().unwrap().remove(name) {
            for waiter in queue {
                let _ = waiter.tx.send(());
            }
        }
    }

    /// Wakes every waiter and makes later waits return at once: the node is
    /// stopping, so live reads end and their clients reconnect.
    pub fn stop(&self) {
        self.stopped.store(true, Ordering::SeqCst);
        for (_, queue) in self.waiters.lock().unwrap().drain() {
            for waiter in queue {
                let _ = waiter.tx.send(());
            }
        }
    }

    pub fn stopped(&self) -> bool {
        self.stopped.load(Ordering::SeqCst)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[tokio::test]
    async fn append_wakes_only_satisfied_waiters() {
        let registry = Arc::new(StreamWaiterRegistry::new());
        let r1 = registry.clone();
        let low = tokio::spawn(async move {
            r1.wait(
                "/s",
                OffsetToken::of_record_offset(0),
                Duration::from_secs(30),
            )
            .await
        });
        let r2 = registry.clone();
        let high = tokio::spawn(async move {
            r2.wait(
                "/s",
                OffsetToken::of_record_offset(5),
                Duration::from_secs(30),
            )
            .await
        });
        while registry
            .waiters
            .lock()
            .unwrap()
            .get("/s")
            .map(|q| q.len())
            .unwrap_or(0)
            < 2
        {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }

        registry.notify_append("/s", 1); // > 0, not > 5
        assert!(low.await.unwrap());
        assert!(!high.is_finished());

        registry.notify_append("/s", 6);
        assert!(high.await.unwrap());
    }

    #[tokio::test]
    async fn close_wakes_everyone_and_timeout_returns_false() {
        let registry = Arc::new(StreamWaiterRegistry::new());
        let r1 = registry.clone();
        let parked = tokio::spawn(async move {
            r1.wait(
                "/s",
                OffsetToken::of_record_offset(9),
                Duration::from_secs(30),
            )
            .await
        });
        while !registry.waiters.lock().unwrap().contains_key("/s") {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        registry.notify_closed("/s");
        assert!(parked.await.unwrap());

        // Nobody notifies: times out false and cleans up its registration.
        assert!(
            !registry
                .wait("/t", OffsetToken::beginning(), Duration::from_millis(10))
                .await
        );
        assert!(registry.waiters.lock().unwrap().is_empty());
    }
}

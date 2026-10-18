use std::time::Duration;

use tokio::sync::watch;

pub(crate) async fn tick(leadership: &mut watch::Receiver<bool>, period: Duration) -> Option<bool> {
    tokio::time::sleep(period).await;
    if leadership.has_changed().is_err() {
        return None;
    }

    Some(*leadership.borrow_and_update())
}

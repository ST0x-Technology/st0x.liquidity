//! Configuration for the offchain order status poller.

use std::time::Duration;

#[derive(Debug, Clone)]
pub struct OrderPollerCtx {
    pub polling_interval: Duration,
    pub max_jitter: Duration,
}

impl Default for OrderPollerCtx {
    fn default() -> Self {
        Self {
            polling_interval: Duration::from_secs(15),
            max_jitter: Duration::from_secs(5),
        }
    }
}

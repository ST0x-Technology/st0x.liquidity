//! Configuration for the offchain order status poller.

use std::time::Duration;

/// Configuration for the offchain order status poller.
///
/// Controls polling cadence when checking order status with the broker.
#[derive(Debug, Clone)]
pub struct OrderPollerCtx {
    /// Base interval between consecutive polls.
    pub polling_interval: Duration,
    /// Maximum random jitter added to `polling_interval` to avoid thundering
    /// herd when many orders are polled simultaneously.
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

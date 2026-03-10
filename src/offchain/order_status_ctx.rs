//! Shared context for the [`PollOrderStatus`] apalis job.
//!
//! [`PollOrderStatus`]: super::poll_order_status::PollOrderStatus

use std::sync::Arc;
use std::time::Duration;

use apalis_codec::json::JsonCodec;
use apalis_sqlite::fetcher::SqliteFetcher;
use apalis_sqlite::{CompactType, SqliteStorage};
use tokio::sync::Mutex;

use st0x_event_sorcery::Store;
use st0x_execution::Executor;

use super::poll_order_status::PollOrderStatus;
use crate::offchain_order::OffchainOrder;
use crate::position::Position;

/// Persistent job queue for polling order status.
pub(crate) type PollOrderStatusQueue =
    SqliteStorage<PollOrderStatus, JsonCodec<CompactType>, SqliteFetcher>;

/// Shared context injected into order status jobs via apalis `Data`.
pub(crate) struct OrderStatusCtx<E: Executor> {
    pub(crate) executor: E,
    pub(crate) offchain_order: Arc<Store<OffchainOrder>>,
    pub(crate) position: Arc<Store<Position>>,
    pub(crate) poll_queue: Mutex<PollOrderStatusQueue>,
    pub(crate) poll_interval: Duration,
}

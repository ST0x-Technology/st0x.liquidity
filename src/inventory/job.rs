use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tracing::{debug, warn};
use uuid::Uuid;

use super::snapshot::InventoryObservationSource;
use crate::conductor::job::{Job, JobQueue, Label, QueuePushError};

pub(crate) type PollInflightEquityJobQueue = JobQueue<PollInflightEquity>;
pub(crate) type PollOnchainEquityJobQueue = JobQueue<PollOnchainEquity>;
pub(crate) type PollOnchainUsdcJobQueue = JobQueue<PollOnchainUsdc>;
pub(crate) type PollEthereumWalletUsdcJobQueue = JobQueue<PollEthereumWalletUsdc>;
pub(crate) type PollBaseWalletUsdcJobQueue = JobQueue<PollBaseWalletUsdc>;
pub(crate) type PollBaseWalletUnwrappedEquityJobQueue = JobQueue<PollBaseWalletUnwrappedEquity>;
pub(crate) type PollBaseWalletWrappedEquityJobQueue = JobQueue<PollBaseWalletWrappedEquity>;
pub(crate) type PollOffchainInventoryJobQueue = JobQueue<PollOffchainInventory>;

#[async_trait]
pub(crate) trait InventorySourcePoller: Send + Sync {
    async fn poll_source(
        &self,
        source: InventoryObservationSource,
    ) -> Result<(), InventorySourcePollError>;
}

#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub(crate) struct InventorySourcePollError(pub(crate) Box<dyn std::error::Error + Send + Sync>);

#[derive(Clone)]
pub(crate) struct InventoryPollingJobCtx {
    pub(crate) poller: Arc<dyn InventorySourcePoller>,
    pub(crate) queues: InventoryPollingJobQueues,
    pub(crate) interval: Duration,
}

#[derive(Clone)]
pub(crate) struct InventoryPollingJobQueues {
    pub(crate) inflight_equity: PollInflightEquityJobQueue,
    pub(crate) onchain_equity: PollOnchainEquityJobQueue,
    pub(crate) onchain_usdc: PollOnchainUsdcJobQueue,
    pub(crate) ethereum_wallet_usdc: PollEthereumWalletUsdcJobQueue,
    pub(crate) base_wallet_usdc: PollBaseWalletUsdcJobQueue,
    pub(crate) base_wallet_unwrapped_equity: PollBaseWalletUnwrappedEquityJobQueue,
    pub(crate) base_wallet_wrapped_equity: PollBaseWalletWrappedEquityJobQueue,
    pub(crate) offchain_inventory: PollOffchainInventoryJobQueue,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ConfiguredInventorySources(BTreeSet<InventoryObservationSource>);

impl ConfiguredInventorySources {
    pub(crate) fn always_available() -> Self {
        Self(BTreeSet::from([
            InventoryObservationSource::OnchainEquity,
            InventoryObservationSource::OnchainUsdc,
            InventoryObservationSource::OffchainInventory,
        ]))
    }

    #[cfg(test)]
    fn all() -> Self {
        Self(BTreeSet::from([
            InventoryObservationSource::InflightEquity,
            InventoryObservationSource::OnchainEquity,
            InventoryObservationSource::OnchainUsdc,
            InventoryObservationSource::EthereumWalletUsdc,
            InventoryObservationSource::BaseWalletUsdc,
            InventoryObservationSource::BaseWalletUnwrappedEquity,
            InventoryObservationSource::BaseWalletWrappedEquity,
            InventoryObservationSource::OffchainInventory,
        ]))
    }

    pub(crate) fn enable(&mut self, source: InventoryObservationSource) {
        self.0.insert(source);
    }

    fn iter(&self) -> impl Iterator<Item = InventoryObservationSource> + '_ {
        self.0.iter().copied()
    }
}

impl InventoryPollingJobQueues {
    pub(crate) fn new(pool: &apalis_sqlite::SqlitePool) -> Self {
        Self {
            inflight_equity: PollInflightEquityJobQueue::new(pool),
            onchain_equity: PollOnchainEquityJobQueue::new(pool),
            onchain_usdc: PollOnchainUsdcJobQueue::new(pool),
            ethereum_wallet_usdc: PollEthereumWalletUsdcJobQueue::new(pool),
            base_wallet_usdc: PollBaseWalletUsdcJobQueue::new(pool),
            base_wallet_unwrapped_equity: PollBaseWalletUnwrappedEquityJobQueue::new(pool),
            base_wallet_wrapped_equity: PollBaseWalletWrappedEquityJobQueue::new(pool),
            offchain_inventory: PollOffchainInventoryJobQueue::new(pool),
        }
    }

    async fn push_with_delay(
        &self,
        source: InventoryObservationSource,
        delay: Duration,
        idempotency_key: &str,
    ) -> Result<(), QueuePushError> {
        match source {
            InventoryObservationSource::InflightEquity => {
                self.inflight_equity
                    .clone()
                    .push_with_delay_idempotent(
                        PollInflightEquity::default(),
                        delay,
                        idempotency_key,
                    )
                    .await
            }
            InventoryObservationSource::OnchainEquity => {
                self.onchain_equity
                    .clone()
                    .push_with_delay_idempotent(
                        PollOnchainEquity::default(),
                        delay,
                        idempotency_key,
                    )
                    .await
            }
            InventoryObservationSource::OnchainUsdc => {
                self.onchain_usdc
                    .clone()
                    .push_with_delay_idempotent(PollOnchainUsdc::default(), delay, idempotency_key)
                    .await
            }
            InventoryObservationSource::EthereumWalletUsdc => {
                self.ethereum_wallet_usdc
                    .clone()
                    .push_with_delay_idempotent(
                        PollEthereumWalletUsdc::default(),
                        delay,
                        idempotency_key,
                    )
                    .await
            }
            InventoryObservationSource::BaseWalletUsdc => {
                self.base_wallet_usdc
                    .clone()
                    .push_with_delay_idempotent(
                        PollBaseWalletUsdc::default(),
                        delay,
                        idempotency_key,
                    )
                    .await
            }
            InventoryObservationSource::BaseWalletUnwrappedEquity => {
                self.base_wallet_unwrapped_equity
                    .clone()
                    .push_with_delay_idempotent(
                        PollBaseWalletUnwrappedEquity::default(),
                        delay,
                        idempotency_key,
                    )
                    .await
            }
            InventoryObservationSource::BaseWalletWrappedEquity => {
                self.base_wallet_wrapped_equity
                    .clone()
                    .push_with_delay_idempotent(
                        PollBaseWalletWrappedEquity::default(),
                        delay,
                        idempotency_key,
                    )
                    .await
            }
            InventoryObservationSource::OffchainInventory => {
                self.offchain_inventory
                    .clone()
                    .push_with_delay_idempotent(
                        PollOffchainInventory::default(),
                        delay,
                        idempotency_key,
                    )
                    .await
            }
        }
    }

    async fn push(&self, source: InventoryObservationSource) -> Result<(), QueuePushError> {
        match source {
            InventoryObservationSource::InflightEquity => {
                self.inflight_equity
                    .clone()
                    .push(PollInflightEquity::default())
                    .await
            }
            InventoryObservationSource::OnchainEquity => {
                self.onchain_equity
                    .clone()
                    .push(PollOnchainEquity::default())
                    .await
            }
            InventoryObservationSource::OnchainUsdc => {
                self.onchain_usdc
                    .clone()
                    .push(PollOnchainUsdc::default())
                    .await
            }
            InventoryObservationSource::EthereumWalletUsdc => {
                self.ethereum_wallet_usdc
                    .clone()
                    .push(PollEthereumWalletUsdc::default())
                    .await
            }
            InventoryObservationSource::BaseWalletUsdc => {
                self.base_wallet_usdc
                    .clone()
                    .push(PollBaseWalletUsdc::default())
                    .await
            }
            InventoryObservationSource::BaseWalletUnwrappedEquity => {
                self.base_wallet_unwrapped_equity
                    .clone()
                    .push(PollBaseWalletUnwrappedEquity::default())
                    .await
            }
            InventoryObservationSource::BaseWalletWrappedEquity => {
                self.base_wallet_wrapped_equity
                    .clone()
                    .push(PollBaseWalletWrappedEquity::default())
                    .await
            }
            InventoryObservationSource::OffchainInventory => {
                self.offchain_inventory
                    .clone()
                    .push(PollOffchainInventory::default())
                    .await
            }
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum InventoryPollingJobError {
    #[error("Failed to enqueue the next inventory poll: {0}")]
    Enqueue(#[from] QueuePushError),
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum InventoryPollingBootstrapError {
    #[error("Apalis database error: {0}")]
    Database(#[from] sqlx_apalis::Error),
    #[error("Failed to enqueue an inventory polling job: {0}")]
    Enqueue(#[from] QueuePushError),
}

pub(crate) async fn bootstrap_inventory_polling_jobs(
    pool: &apalis_sqlite::SqlitePool,
    queues: &InventoryPollingJobQueues,
    configured_sources: &ConfiguredInventorySources,
) -> Result<(), InventoryPollingBootstrapError> {
    purge_active_jobs::<PollInflightEquity>(pool).await?;
    purge_active_jobs::<PollOnchainEquity>(pool).await?;
    purge_active_jobs::<PollOnchainUsdc>(pool).await?;
    purge_active_jobs::<PollEthereumWalletUsdc>(pool).await?;
    purge_active_jobs::<PollBaseWalletUsdc>(pool).await?;
    purge_active_jobs::<PollBaseWalletUnwrappedEquity>(pool).await?;
    purge_active_jobs::<PollBaseWalletWrappedEquity>(pool).await?;
    purge_active_jobs::<PollOffchainInventory>(pool).await?;

    for source in configured_sources.iter() {
        queues.push(source).await?;
    }

    Ok(())
}

async fn purge_active_jobs<Task: 'static>(
    pool: &apalis_sqlite::SqlitePool,
) -> Result<u64, sqlx_apalis::Error> {
    let deleted = sqlx_apalis::query(
        "DELETE FROM Jobs WHERE job_type = ? AND (status IN ('Pending', 'Queued', 'Running') \
         OR (status = 'Failed' AND attempts < max_attempts))",
    )
    .bind(std::any::type_name::<Task>())
    .execute(pool)
    .await?
    .rows_affected();

    Ok(deleted)
}

fn new_successor_idempotency_key() -> String {
    Uuid::new_v4().to_string()
}

macro_rules! inventory_polling_job {
    ($job:ident, $source:ident, $worker:literal, $label:literal) => {
        #[derive(Clone, Debug, Serialize, Deserialize)]
        pub(crate) struct $job {
            #[serde(default = "new_successor_idempotency_key")]
            successor_idempotency_key: String,
        }

        impl Default for $job {
            fn default() -> Self {
                Self {
                    successor_idempotency_key: new_successor_idempotency_key(),
                }
            }
        }

        impl Job<InventoryPollingJobCtx> for $job {
            type Output = ();
            type Error = InventoryPollingJobError;

            const WORKER_NAME: &'static str = $worker;

            #[cfg(any(test, feature = "test-support"))]
            const JOB_KIND: crate::conductor::job::JobKind =
                crate::conductor::job::JobKind::InventoryPolling;

            fn label(&self) -> Label {
                Label::new($label)
            }

            async fn perform(
                &self,
                ctx: &InventoryPollingJobCtx,
            ) -> Result<Self::Output, Self::Error> {
                perform_inventory_poll(
                    ctx,
                    InventoryObservationSource::$source,
                    &self.successor_idempotency_key,
                )
                .await
            }
        }
    };
}

inventory_polling_job!(
    PollInflightEquity,
    InflightEquity,
    "poll-inflight-equity-worker",
    "PollInflightEquity"
);
inventory_polling_job!(
    PollOnchainEquity,
    OnchainEquity,
    "poll-onchain-equity-worker",
    "PollOnchainEquity"
);
inventory_polling_job!(
    PollOnchainUsdc,
    OnchainUsdc,
    "poll-onchain-usdc-worker",
    "PollOnchainUsdc"
);
inventory_polling_job!(
    PollEthereumWalletUsdc,
    EthereumWalletUsdc,
    "poll-ethereum-wallet-usdc-worker",
    "PollEthereumWalletUsdc"
);
inventory_polling_job!(
    PollBaseWalletUsdc,
    BaseWalletUsdc,
    "poll-base-wallet-usdc-worker",
    "PollBaseWalletUsdc"
);
inventory_polling_job!(
    PollBaseWalletUnwrappedEquity,
    BaseWalletUnwrappedEquity,
    "poll-base-wallet-unwrapped-equity-worker",
    "PollBaseWalletUnwrappedEquity"
);
inventory_polling_job!(
    PollBaseWalletWrappedEquity,
    BaseWalletWrappedEquity,
    "poll-base-wallet-wrapped-equity-worker",
    "PollBaseWalletWrappedEquity"
);
inventory_polling_job!(
    PollOffchainInventory,
    OffchainInventory,
    "poll-offchain-inventory-worker",
    "PollOffchainInventory"
);

async fn perform_inventory_poll(
    ctx: &InventoryPollingJobCtx,
    source: InventoryObservationSource,
    successor_idempotency_key: &str,
) -> Result<(), InventoryPollingJobError> {
    let poll_result = ctx.poller.poll_source(source).await;

    ctx.queues
        .push_with_delay(source, ctx.interval, successor_idempotency_key)
        .await?;

    match poll_result {
        Ok(()) => debug!(target: "inventory", ?source, "Inventory source poll completed"),
        Err(error) => warn!(
            target: "inventory",
            ?source,
            %error,
            "Inventory source poll failed; scheduled source-local retry"
        ),
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;
    use crate::test_utils::setup_test_pools;

    #[derive(Default)]
    struct FailingPoller {
        sources: Mutex<Vec<InventoryObservationSource>>,
    }

    #[async_trait]
    impl InventorySourcePoller for FailingPoller {
        async fn poll_source(
            &self,
            source: InventoryObservationSource,
        ) -> Result<(), InventorySourcePollError> {
            self.sources.lock().unwrap().push(source);
            Err(InventorySourcePollError(Box::new(std::io::Error::other(
                "source unavailable",
            ))))
        }
    }

    #[tokio::test]
    async fn source_failure_still_schedules_its_follow_up() {
        let (_cqrs_pool, apalis_pool) = setup_test_pools().await;
        let queues = InventoryPollingJobQueues::new(&apalis_pool);
        let poller = Arc::new(FailingPoller::default());
        let ctx = InventoryPollingJobCtx {
            poller: poller.clone(),
            queues: queues.clone(),
            interval: Duration::from_secs(60),
        };

        PollOnchainEquity::default().perform(&ctx).await.unwrap();

        assert_eq!(
            *poller.sources.lock().unwrap(),
            vec![InventoryObservationSource::OnchainEquity],
        );
        assert!(queues.onchain_equity.has_in_flight().await.unwrap());
        assert!(!queues.onchain_usdc.has_in_flight().await.unwrap());
    }

    #[tokio::test]
    async fn retried_poll_enqueues_only_one_successor() {
        let (_cqrs_pool, apalis_pool) = setup_test_pools().await;
        let queues = InventoryPollingJobQueues::new(&apalis_pool);
        let poller = Arc::new(FailingPoller::default());
        let ctx = InventoryPollingJobCtx {
            poller,
            queues,
            interval: Duration::from_secs(60),
        };
        let job = PollOnchainEquity::default();

        job.perform(&ctx).await.unwrap();
        job.perform(&ctx).await.unwrap();

        assert_eq!(
            live_job_count::<PollOnchainEquity>(&apalis_pool).await,
            1,
            "an at-least-once retry must reuse the persisted successor key",
        );
    }

    #[tokio::test]
    async fn bootstrap_replaces_duplicate_loops_with_one_job_per_source() {
        let (_cqrs_pool, apalis_pool) = setup_test_pools().await;
        let queues = InventoryPollingJobQueues::new(&apalis_pool);

        queues
            .onchain_equity
            .clone()
            .push(PollOnchainEquity::default())
            .await
            .unwrap();
        queues
            .onchain_equity
            .clone()
            .push(PollOnchainEquity::default())
            .await
            .unwrap();

        bootstrap_inventory_polling_jobs(&apalis_pool, &queues, &ConfiguredInventorySources::all())
            .await
            .unwrap();

        assert_eq!(live_job_count::<PollOnchainEquity>(&apalis_pool).await, 1);
        assert_eq!(live_job_count::<PollInflightEquity>(&apalis_pool).await, 1);
        assert_eq!(live_job_count::<PollOnchainUsdc>(&apalis_pool).await, 1);
        assert_eq!(
            live_job_count::<PollEthereumWalletUsdc>(&apalis_pool).await,
            1
        );
        assert_eq!(live_job_count::<PollBaseWalletUsdc>(&apalis_pool).await, 1);
        assert_eq!(
            live_job_count::<PollBaseWalletUnwrappedEquity>(&apalis_pool).await,
            1
        );
        assert_eq!(
            live_job_count::<PollBaseWalletWrappedEquity>(&apalis_pool).await,
            1
        );
        assert_eq!(
            live_job_count::<PollOffchainInventory>(&apalis_pool).await,
            1
        );
    }

    #[tokio::test]
    async fn bootstrap_purges_unconfigured_sources_without_restarting_them() {
        let (_cqrs_pool, apalis_pool) = setup_test_pools().await;
        let queues = InventoryPollingJobQueues::new(&apalis_pool);

        queues
            .inflight_equity
            .clone()
            .push(PollInflightEquity::default())
            .await
            .unwrap();
        queues
            .base_wallet_wrapped_equity
            .clone()
            .push(PollBaseWalletWrappedEquity::default())
            .await
            .unwrap();

        bootstrap_inventory_polling_jobs(
            &apalis_pool,
            &queues,
            &ConfiguredInventorySources::always_available(),
        )
        .await
        .unwrap();

        assert_eq!(live_job_count::<PollInflightEquity>(&apalis_pool).await, 0);
        assert_eq!(live_job_count::<PollOnchainEquity>(&apalis_pool).await, 1);
        assert_eq!(live_job_count::<PollOnchainUsdc>(&apalis_pool).await, 1);
        assert_eq!(
            live_job_count::<PollEthereumWalletUsdc>(&apalis_pool).await,
            0
        );
        assert_eq!(live_job_count::<PollBaseWalletUsdc>(&apalis_pool).await, 0);
        assert_eq!(
            live_job_count::<PollBaseWalletUnwrappedEquity>(&apalis_pool).await,
            0
        );
        assert_eq!(
            live_job_count::<PollBaseWalletWrappedEquity>(&apalis_pool).await,
            0
        );
        assert_eq!(
            live_job_count::<PollOffchainInventory>(&apalis_pool).await,
            1
        );
    }

    async fn live_job_count<Task: 'static>(pool: &apalis_sqlite::SqlitePool) -> i64 {
        sqlx_apalis::query_scalar(
            "SELECT COUNT(*) FROM Jobs WHERE job_type = ? \
             AND status NOT IN ('Done', 'Failed', 'Killed')",
        )
        .bind(std::any::type_name::<Task>())
        .fetch_one(pool)
        .await
        .unwrap()
    }
}

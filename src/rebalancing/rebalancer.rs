//! Operation executor that routes triggered rebalancing operations to
//! cross-venue transfer implementations.

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::mpsc;
use tracing::{error, info, warn};
use uuid::Uuid;

use st0x_execution::{FractionalShares, Symbol};
use st0x_finance::Usdc;

use super::equity::{Equity, MintError, RedemptionError};
use super::transfer::{CrossVenueTransfer, HedgingVenue, MarketMakingVenue};
use super::trigger::TriggeredOperation;
use super::usdc::{TransferUsdcToHedging, TransferUsdcToHedgingJobQueue, UsdcTransferError};
use crate::usdc_rebalance::UsdcRebalanceId;

/// Type-erased equity transfer (hedging -> market-making).
type EquityToMarketMaking =
    dyn CrossVenueTransfer<HedgingVenue, MarketMakingVenue, Asset = Equity, Error = MintError>;

/// Type-erased equity transfer (market-making -> hedging).
type EquityToHedging = dyn CrossVenueTransfer<MarketMakingVenue, HedgingVenue, Asset = Equity, Error = RedemptionError>;

/// Type-erased USDC transfer (hedging -> market-making).
type UsdcToMarketMaking = dyn CrossVenueTransfer<HedgingVenue, MarketMakingVenue, Asset = Usdc, Error = UsdcTransferError>;

/// Receives triggered rebalancing operations and routes them to the
/// appropriate cross-venue transfer implementation.
///
/// USDC rebalances in the Base-to-Alpaca direction are enqueued onto a
/// persistent apalis job queue so a crash mid-bridge or mid-deposit can
/// resume from the persisted [`UsdcRebalance`] state; the Alpaca-to-Base
/// direction and equity transfers still run as direct calls into the
/// trait impl until they are converted to jobs.
///
/// [`UsdcRebalance`]: crate::usdc_rebalance::UsdcRebalance
pub(crate) struct Rebalancer {
    equity_to_mm: Arc<EquityToMarketMaking>,
    equity_to_hedging: Arc<EquityToHedging>,
    usdc_to_mm: Arc<UsdcToMarketMaking>,
    usdc_to_hedging_queue: TransferUsdcToHedgingJobQueue,
    receiver: mpsc::Receiver<TriggeredOperation>,
    equity_in_progress: Arc<std::sync::RwLock<HashSet<Symbol>>>,
    usdc_in_progress: Arc<AtomicBool>,
}

impl Rebalancer {
    pub(crate) fn new(
        equity_to_mm: Arc<EquityToMarketMaking>,
        equity_to_hedging: Arc<EquityToHedging>,
        usdc_to_mm: Arc<UsdcToMarketMaking>,
        usdc_to_hedging_queue: TransferUsdcToHedgingJobQueue,
        receiver: mpsc::Receiver<TriggeredOperation>,
        equity_in_progress: Arc<std::sync::RwLock<HashSet<Symbol>>>,
        usdc_in_progress: Arc<AtomicBool>,
    ) -> Self {
        Self {
            equity_to_mm,
            equity_to_hedging,
            usdc_to_mm,
            usdc_to_hedging_queue,
            receiver,
            equity_in_progress,
            usdc_in_progress,
        }
    }

    /// Runs the rebalancer loop, receiving and executing operations.
    /// Returns when the sender channel is closed.
    pub(crate) async fn run(mut self) {
        info!(target: "rebalance", "Rebalancer started");

        while let Some(operation) = self.receiver.recv().await {
            self.execute(operation).await;
        }

        info!(target: "rebalance", "Rebalancer stopped (channel closed)");
    }

    async fn execute(&mut self, operation: TriggeredOperation) {
        match operation {
            TriggeredOperation::Mint { symbol, quantity } => {
                self.execute_mint(symbol, quantity).await;
            }

            TriggeredOperation::Redemption {
                symbol, quantity, ..
            } => {
                self.execute_redemption(symbol, quantity).await;
            }

            TriggeredOperation::UsdcAlpacaToBase { amount } => {
                if let Err(error) = self.usdc_to_mm.transfer(amount).await {
                    error!(target: "rebalance", ?error, %amount, "USDC transfer to market-making venue failed");
                    self.clear_usdc_in_progress();
                }
            }

            TriggeredOperation::UsdcBaseToAlpaca { amount } => {
                self.enqueue_usdc_to_hedging(amount).await;
            }
        }
    }

    async fn execute_mint(&self, symbol: Symbol, quantity: FractionalShares) {
        let log_symbol = symbol.clone();

        if let Err(error) = self
            .equity_to_mm
            .transfer(Equity { symbol, quantity })
            .await
        {
            error!(target: "rebalance", ?error, symbol = %log_symbol, %quantity, "Equity transfer to market-making venue failed");
            self.clear_equity_in_progress(&log_symbol);
        }
    }

    async fn execute_redemption(&self, symbol: Symbol, quantity: FractionalShares) {
        let log_symbol = symbol.clone();

        if let Err(error) = self
            .equity_to_hedging
            .transfer(Equity { symbol, quantity })
            .await
        {
            error!(target: "rebalance", ?error, symbol = %log_symbol, %quantity, "Equity transfer to hedging venue failed");
            self.clear_equity_in_progress(&log_symbol);
        }
    }

    async fn enqueue_usdc_to_hedging(&mut self, amount: Usdc) {
        let id = UsdcRebalanceId(Uuid::new_v4());

        info!(
            target: "rebalance",
            %id,
            %amount,
            "Enqueuing Base-to-Alpaca USDC transfer job"
        );

        if let Err(error) = self
            .usdc_to_hedging_queue
            .push(TransferUsdcToHedging {
                id: id.clone(),
                amount,
            })
            .await
        {
            error!(
                target: "rebalance",
                ?error,
                %id,
                %amount,
                "Failed to enqueue USDC transfer to hedging job"
            );
            self.clear_usdc_in_progress();
        }
    }

    fn clear_equity_in_progress(&self, symbol: &Symbol) {
        {
            let mut guard = match self.equity_in_progress.write() {
                Ok(guard) => guard,
                Err(poison) => poison.into_inner(),
            };
            guard.remove(symbol);
        }

        warn!(
            target: "rebalance",
            %symbol,
            "Cleared equity in-progress flag after transfer failure"
        );
    }

    fn clear_usdc_in_progress(&self) {
        self.usdc_in_progress.store(false, Ordering::SeqCst);

        warn!(
            target: "rebalance",
            "Cleared USDC in-progress flag after transfer failure"
        );
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::address;
    use sqlx::SqlitePool;
    use std::sync::Arc;

    use st0x_execution::{FractionalShares, Symbol};
    use st0x_float_macro::float;

    use super::*;
    use crate::conductor::setup_apalis_tables;
    use crate::rebalancing::equity::mock::MockCrossVenueEquityTransfer;
    use crate::rebalancing::usdc::mock::MockUsdcRebalance;
    use crate::test_utils::setup_test_db;

    async fn execute(
        operations: Vec<TriggeredOperation>,
    ) -> (
        Arc<MockCrossVenueEquityTransfer>,
        Arc<MockUsdcRebalance>,
        SqlitePool,
    ) {
        let pool = setup_test_db().await;
        setup_apalis_tables(&pool).await.unwrap();

        let equity = Arc::new(MockCrossVenueEquityTransfer::new());
        let usdc = Arc::new(MockUsdcRebalance::new());
        let (sender, receiver) = mpsc::channel(10);

        let rebalancer = Rebalancer::new(
            Arc::clone(&equity) as Arc<EquityToMarketMaking>,
            Arc::clone(&equity) as Arc<EquityToHedging>,
            Arc::clone(&usdc) as Arc<UsdcToMarketMaking>,
            TransferUsdcToHedgingJobQueue::new(&pool),
            receiver,
            Arc::new(std::sync::RwLock::new(HashSet::new())),
            Arc::new(AtomicBool::new(false)),
        );

        for operation in operations {
            sender.send(operation).await.unwrap();
        }

        drop(sender);
        rebalancer.run().await;

        (equity, usdc, pool)
    }

    async fn count_jobs(pool: &SqlitePool) -> i64 {
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM Jobs")
            .fetch_one(pool)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn execute_mint_calls_equity_to_market_making() {
        let (equity, usdc, pool) = execute(vec![TriggeredOperation::Mint {
            symbol: Symbol::new("AAPL").unwrap(),
            quantity: FractionalShares::new(float!(10)),
        }])
        .await;

        assert_eq!(equity.mint_calls(), 1);
        assert_eq!(equity.redeem_calls(), 0);
        assert_eq!(usdc.alpaca_to_base_calls(), 0);
        assert_eq!(
            count_jobs(&pool).await,
            0,
            "Mint should not enqueue any USDC job"
        );
    }

    #[tokio::test]
    async fn execute_redemption_calls_equity_to_hedging() {
        let (equity, usdc, pool) = execute(vec![TriggeredOperation::Redemption {
            symbol: Symbol::new("AAPL").unwrap(),
            quantity: FractionalShares::new(float!(50)),
            wrapped_token: address!("0x1234567890123456789012345678901234567890"),
            unwrapped_token: address!("0xabcdef0123456789abcdef0123456789abcdef01"),
        }])
        .await;

        assert_eq!(equity.mint_calls(), 0);
        assert_eq!(equity.redeem_calls(), 1);
        assert_eq!(usdc.alpaca_to_base_calls(), 0);
        assert_eq!(
            count_jobs(&pool).await,
            0,
            "Redemption should not enqueue any USDC job"
        );
    }

    #[tokio::test]
    async fn execute_usdc_alpaca_to_base_calls_usdc_to_market_making() {
        let (equity, usdc, pool) = execute(vec![TriggeredOperation::UsdcAlpacaToBase {
            amount: Usdc::new(float!(1000)),
        }])
        .await;

        assert_eq!(equity.mint_calls(), 0);
        assert_eq!(equity.redeem_calls(), 0);
        assert_eq!(usdc.alpaca_to_base_calls(), 1);
        assert_eq!(
            count_jobs(&pool).await,
            0,
            "Alpaca-to-Base path should not enqueue a job"
        );
    }

    #[tokio::test]
    async fn execute_usdc_base_to_alpaca_enqueues_transfer_job() {
        let amount = Usdc::new(float!(2000));

        let (equity, usdc, pool) =
            execute(vec![TriggeredOperation::UsdcBaseToAlpaca { amount }]).await;

        assert_eq!(equity.mint_calls(), 0);
        assert_eq!(equity.redeem_calls(), 0);
        assert_eq!(
            usdc.base_to_alpaca_calls(),
            0,
            "Base-to-Alpaca should no longer call the trait impl directly"
        );
        assert_eq!(
            count_jobs(&pool).await,
            1,
            "Base-to-Alpaca should enqueue exactly one TransferUsdcToHedging job"
        );
    }

    #[tokio::test]
    async fn run_processes_multiple_operations() {
        let (equity, usdc, pool) = execute(vec![
            TriggeredOperation::Mint {
                symbol: Symbol::new("AAPL").unwrap(),
                quantity: FractionalShares::new(float!(10)),
            },
            TriggeredOperation::Mint {
                symbol: Symbol::new("TSLA").unwrap(),
                quantity: FractionalShares::new(float!(20)),
            },
            TriggeredOperation::Redemption {
                symbol: Symbol::new("GOOG").unwrap(),
                quantity: FractionalShares::new(float!(5)),
                wrapped_token: address!("0x1234567890123456789012345678901234567890"),
                unwrapped_token: address!("0xabcdef0123456789abcdef0123456789abcdef01"),
            },
            TriggeredOperation::UsdcAlpacaToBase {
                amount: Usdc::new(float!(500)),
            },
            TriggeredOperation::UsdcBaseToAlpaca {
                amount: Usdc::new(float!(300)),
            },
        ])
        .await;

        assert_eq!(equity.mint_calls(), 2);
        assert_eq!(equity.redeem_calls(), 1);
        assert_eq!(usdc.alpaca_to_base_calls(), 1);
        assert_eq!(count_jobs(&pool).await, 1);
    }

    #[tokio::test]
    async fn run_terminates_when_channel_closes() {
        execute(vec![]).await;
    }
}

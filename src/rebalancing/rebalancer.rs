//! Operation executor that routes triggered rebalancing operations to
//! cross-venue transfer implementations.

use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use st0x_execution::{FractionalShares, Symbol};

use super::equity::{Equity, RedemptionError};
use super::transfer::{CrossVenueTransfer, HedgingVenue, MarketMakingVenue};
use super::trigger::TriggeredOperation;

/// Type-erased equity transfer (market-making -> hedging).
type EquityToHedging = dyn CrossVenueTransfer<MarketMakingVenue, HedgingVenue, Asset = Equity, Error = RedemptionError>;

/// Receives triggered equity redemption operations and routes them to the
/// cross-venue transfer implementation. USDC rebalances and equity mints no
/// longer flow through this channel — they're enqueued as apalis jobs
/// directly from the trigger.
pub(crate) struct Rebalancer {
    equity_to_hedging: Arc<EquityToHedging>,
    receiver: mpsc::Receiver<TriggeredOperation>,
    equity_in_progress: Arc<std::sync::RwLock<HashSet<Symbol>>>,
    shutdown_token: CancellationToken,
}

impl Rebalancer {
    pub(crate) fn new(
        equity_to_hedging: Arc<EquityToHedging>,
        receiver: mpsc::Receiver<TriggeredOperation>,
        equity_in_progress: Arc<std::sync::RwLock<HashSet<Symbol>>>,
        shutdown_token: CancellationToken,
    ) -> Self {
        Self {
            equity_to_hedging,
            receiver,
            equity_in_progress,
            shutdown_token,
        }
    }

    /// Runs the rebalancer loop, receiving and executing operations.
    ///
    /// Stops accepting new operations when the shutdown token is
    /// cancelled, but always finishes the in-flight operation before
    /// returning. Also exits when the sender channel is closed.
    pub(crate) async fn run(mut self) {
        info!(target: "rebalance", "Rebalancer started");

        loop {
            let operation = tokio::select! {
                biased;
                () = self.shutdown_token.cancelled() => {
                    info!(target: "rebalance", "Rebalancer received shutdown signal");
                    break;
                }
                received = self.receiver.recv() => {
                    match received {
                        Some(operation) => operation,
                        None => break,
                    }
                }
            };

            self.execute(operation).await;
        }

        info!(target: "rebalance", "Rebalancer stopped");
    }

    async fn execute(&self, operation: TriggeredOperation) {
        match operation {
            TriggeredOperation::Mint { symbol, quantity } => {
                error!(
                    target: "rebalance",
                    %symbol,
                    %quantity,
                    "Mint operation received over the mpsc channel; mints are \
                     driven by TransferEquityToMarketMaking apalis jobs and \
                     must not be dispatched here"
                );
            }

            TriggeredOperation::Redemption {
                symbol, quantity, ..
            } => {
                self.execute_redemption(symbol, quantity).await;
            }
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
}

#[cfg(test)]
mod tests {
    use alloy::primitives::address;
    use std::sync::Arc;
    use std::time::Duration;

    use st0x_execution::{FractionalShares, Symbol};

    use super::*;
    use crate::rebalancing::equity::mock::MockCrossVenueEquityTransfer;
    use st0x_float_macro::float;

    async fn execute(operations: Vec<TriggeredOperation>) -> Arc<MockCrossVenueEquityTransfer> {
        let equity = Arc::new(MockCrossVenueEquityTransfer::new());
        let (sender, receiver) = mpsc::channel(10);

        let rebalancer = Rebalancer::new(
            Arc::clone(&equity) as Arc<EquityToHedging>,
            receiver,
            Arc::new(std::sync::RwLock::new(HashSet::new())),
            CancellationToken::new(),
        );

        for operation in operations {
            sender.send(operation).await.unwrap();
        }

        drop(sender);
        rebalancer.run().await;

        equity
    }

    #[tokio::test]
    async fn execute_redemption_calls_equity_to_hedging() {
        let equity = execute(vec![TriggeredOperation::Redemption {
            symbol: Symbol::new("AAPL").unwrap(),
            quantity: FractionalShares::new(float!(50)),
            wrapped_token: address!("0x1234567890123456789012345678901234567890"),
            unwrapped_token: address!("0xabcdef0123456789abcdef0123456789abcdef01"),
        }])
        .await;

        assert_eq!(equity.redeem_calls(), 1);
    }

    #[tokio::test]
    async fn run_processes_multiple_operations() {
        let equity = execute(vec![
            TriggeredOperation::Redemption {
                symbol: Symbol::new("AAPL").unwrap(),
                quantity: FractionalShares::new(float!(5)),
                wrapped_token: address!("0x1234567890123456789012345678901234567890"),
                unwrapped_token: address!("0xabcdef0123456789abcdef0123456789abcdef01"),
            },
            TriggeredOperation::Redemption {
                symbol: Symbol::new("GOOG").unwrap(),
                quantity: FractionalShares::new(float!(7)),
                wrapped_token: address!("0x1234567890123456789012345678901234567890"),
                unwrapped_token: address!("0xabcdef0123456789abcdef0123456789abcdef01"),
            },
        ])
        .await;

        assert_eq!(equity.redeem_calls(), 2);
    }

    /// A mint arriving over the channel is a wiring bug (mints are apalis
    /// jobs); the rebalancer must log and skip it, not execute a transfer.
    #[tokio::test]
    async fn mint_over_channel_is_ignored() {
        let equity = execute(vec![TriggeredOperation::Mint {
            symbol: Symbol::new("AAPL").unwrap(),
            quantity: FractionalShares::new(float!(10)),
        }])
        .await;

        assert_eq!(equity.redeem_calls(), 0);
    }

    #[tokio::test]
    async fn run_terminates_when_channel_closes() {
        tokio::time::timeout(Duration::from_secs(1), execute(vec![]))
            .await
            .expect("timed out waiting for execute");
    }
}

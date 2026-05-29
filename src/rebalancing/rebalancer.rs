//! Operation executor that routes triggered rebalancing operations to
//! cross-venue transfer implementations.

use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use st0x_execution::{FractionalShares, Symbol};

use super::equity::{Equity, MintTransferError, RedemptionError};
use super::transfer::{CrossVenueTransfer, HedgingVenue, MarketMakingVenue};
use super::trigger::TriggeredOperation;

/// Type-erased equity transfer (hedging -> market-making).
type EquityToMarketMaking = dyn CrossVenueTransfer<HedgingVenue, MarketMakingVenue, Asset = Equity, Error = MintTransferError>;

/// Type-erased equity transfer (market-making -> hedging).
type EquityToHedging = dyn CrossVenueTransfer<MarketMakingVenue, HedgingVenue, Asset = Equity, Error = RedemptionError>;

/// Receives triggered equity rebalancing operations and routes them to the
/// appropriate cross-venue transfer implementation. USDC rebalances no
/// longer flow through this channel — they're enqueued as apalis jobs
/// directly from the trigger.
pub(crate) struct Rebalancer {
    equity_to_mm: Arc<EquityToMarketMaking>,
    equity_to_hedging: Arc<EquityToHedging>,
    receiver: mpsc::Receiver<TriggeredOperation>,
    equity_in_progress: Arc<std::sync::RwLock<HashSet<Symbol>>>,
    shutdown_token: CancellationToken,
}

impl Rebalancer {
    pub(crate) fn new(
        equity_to_mm: Arc<EquityToMarketMaking>,
        equity_to_hedging: Arc<EquityToHedging>,
        receiver: mpsc::Receiver<TriggeredOperation>,
        equity_in_progress: Arc<std::sync::RwLock<HashSet<Symbol>>>,
        shutdown_token: CancellationToken,
    ) -> Self {
        Self {
            equity_to_mm,
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
                self.execute_mint(symbol, quantity).await;
            }

            TriggeredOperation::Redemption {
                symbol, quantity, ..
            } => {
                self.execute_redemption(symbol, quantity).await;
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
            match &error {
                MintTransferError::PreReceipt(_) => {
                    error!(target: "rebalance", ?error, symbol = %log_symbol, %quantity,
                        "Mint failed before tokens received; clearing guard for retry");
                    self.clear_equity_in_progress(&log_symbol);
                }

                MintTransferError::PostReceipt(_) => {
                    error!(target: "rebalance", ?error, symbol = %log_symbol, %quantity,
                        "Mint failed after tokens received; keeping guard set for recovery");
                }
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

    use st0x_execution::{FractionalShares, Symbol};

    use super::*;
    use crate::rebalancing::equity::mock::MockCrossVenueEquityTransfer;
    use st0x_float_macro::float;

    async fn execute(operations: Vec<TriggeredOperation>) -> Arc<MockCrossVenueEquityTransfer> {
        let equity = Arc::new(MockCrossVenueEquityTransfer::new());
        let (sender, receiver) = mpsc::channel(10);

        let rebalancer = Rebalancer::new(
            Arc::clone(&equity) as Arc<EquityToMarketMaking>,
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
    async fn execute_mint_calls_equity_to_market_making() {
        let equity = execute(vec![TriggeredOperation::Mint {
            symbol: Symbol::new("AAPL").unwrap(),
            quantity: FractionalShares::new(float!(10)),
        }])
        .await;

        assert_eq!(equity.mint_calls(), 1);
        assert_eq!(equity.redeem_calls(), 0);
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

        assert_eq!(equity.mint_calls(), 0);
        assert_eq!(equity.redeem_calls(), 1);
    }

    #[tokio::test]
    async fn run_processes_multiple_operations() {
        let equity = execute(vec![
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
        ])
        .await;

        assert_eq!(equity.mint_calls(), 2);
        assert_eq!(equity.redeem_calls(), 1);
    }

    #[tokio::test]
    async fn run_terminates_when_channel_closes() {
        execute(vec![]).await;
    }

    /// Mint transfer that always returns the configured error.
    struct FailingMintTransfer {
        error: std::sync::Mutex<Option<MintTransferError>>,
    }

    #[async_trait::async_trait]
    impl CrossVenueTransfer<HedgingVenue, MarketMakingVenue> for FailingMintTransfer {
        type Asset = Equity;
        type Error = MintTransferError;

        async fn transfer(&self, _asset: Self::Asset) -> Result<(), Self::Error> {
            Err(self.error.lock().unwrap().take().unwrap())
        }
    }

    fn make_pre_receipt_error() -> MintTransferError {
        use crate::rebalancing::equity::MintError;
        use crate::tokenized_equity_mint::IssuerRequestId;

        MintTransferError::PreReceipt(MintError::EntityNotFound {
            issuer_request_id: IssuerRequestId::new("test-pre".to_string()),
            expected_state: "test",
        })
    }

    fn make_post_receipt_error() -> MintTransferError {
        use crate::rebalancing::equity::MintError;
        use crate::tokenized_equity_mint::IssuerRequestId;

        MintTransferError::PostReceipt(MintError::EntityNotFound {
            issuer_request_id: IssuerRequestId::new("test-post".to_string()),
            expected_state: "test",
        })
    }

    #[tokio::test]
    async fn pre_receipt_mint_failure_clears_guard() {
        let symbol = Symbol::new("AAPL").unwrap();
        let equity_in_progress = Arc::new(std::sync::RwLock::new(HashSet::from([symbol.clone()])));
        let (sender, receiver) = mpsc::channel(10);

        let failing_mint = Arc::new(FailingMintTransfer {
            error: std::sync::Mutex::new(Some(make_pre_receipt_error())),
        });
        let equity = Arc::new(MockCrossVenueEquityTransfer::new());

        let rebalancer = Rebalancer::new(
            failing_mint,
            Arc::clone(&equity) as Arc<EquityToHedging>,
            receiver,
            Arc::clone(&equity_in_progress),
            CancellationToken::new(),
        );

        sender
            .send(TriggeredOperation::Mint {
                symbol: symbol.clone(),
                quantity: FractionalShares::new(float!(10)),
            })
            .await
            .unwrap();
        drop(sender);

        rebalancer.run().await;

        assert!(
            !equity_in_progress.read().unwrap().contains(&symbol),
            "guard should be cleared after pre-receipt failure"
        );
    }

    #[tokio::test]
    async fn post_receipt_mint_failure_keeps_guard() {
        let symbol = Symbol::new("AAPL").unwrap();
        let equity_in_progress = Arc::new(std::sync::RwLock::new(HashSet::from([symbol.clone()])));
        let (sender, receiver) = mpsc::channel(10);

        let failing_mint = Arc::new(FailingMintTransfer {
            error: std::sync::Mutex::new(Some(make_post_receipt_error())),
        });
        let equity = Arc::new(MockCrossVenueEquityTransfer::new());

        let rebalancer = Rebalancer::new(
            failing_mint,
            Arc::clone(&equity) as Arc<EquityToHedging>,
            receiver,
            Arc::clone(&equity_in_progress),
            CancellationToken::new(),
        );

        sender
            .send(TriggeredOperation::Mint {
                symbol: symbol.clone(),
                quantity: FractionalShares::new(float!(10)),
            })
            .await
            .unwrap();
        drop(sender);

        rebalancer.run().await;

        assert!(
            equity_in_progress.read().unwrap().contains(&symbol),
            "guard should remain set after post-receipt failure"
        );
    }
}

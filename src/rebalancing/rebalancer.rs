//! Operation executor that routes triggered rebalancing operations to
//! cross-venue transfer implementations.

use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{error, info};

use super::equity::{Equity, MintError, RedemptionError};
use super::transfer::{CrossVenueTransfer, HedgingVenue, MarketMakingVenue};
use super::trigger::TriggeredOperation;
use super::usdc::UsdcTransferError;
use crate::threshold::Usdc;

/// Type-erased equity transfer (hedging -> market-making).
type EquityToMarketMaking =
    dyn CrossVenueTransfer<HedgingVenue, MarketMakingVenue, Asset = Equity, Error = MintError>;

/// Type-erased equity transfer (market-making -> hedging).
type EquityToHedging = dyn CrossVenueTransfer<MarketMakingVenue, HedgingVenue, Asset = Equity, Error = RedemptionError>;

/// Type-erased USDC transfer (hedging -> market-making).
type UsdcToMarketMaking = dyn CrossVenueTransfer<HedgingVenue, MarketMakingVenue, Asset = Usdc, Error = UsdcTransferError>;

/// Type-erased USDC transfer (market-making -> hedging).
type UsdcToHedging = dyn CrossVenueTransfer<MarketMakingVenue, HedgingVenue, Asset = Usdc, Error = UsdcTransferError>;

/// Receives triggered rebalancing operations and routes them to the
/// appropriate cross-venue transfer implementation.
pub(crate) struct Rebalancer {
    equity_to_mm: Arc<EquityToMarketMaking>,
    equity_to_hedging: Arc<EquityToHedging>,
    usdc_to_mm: Arc<UsdcToMarketMaking>,
    usdc_to_hedging: Arc<UsdcToHedging>,
    receiver: mpsc::Receiver<TriggeredOperation>,
}

impl Rebalancer {
    pub(crate) fn new(
        equity_to_mm: Arc<EquityToMarketMaking>,
        equity_to_hedging: Arc<EquityToHedging>,
        usdc_to_mm: Arc<UsdcToMarketMaking>,
        usdc_to_hedging: Arc<UsdcToHedging>,
        receiver: mpsc::Receiver<TriggeredOperation>,
    ) -> Self {
        Self {
            equity_to_mm,
            equity_to_hedging,
            usdc_to_mm,
            usdc_to_hedging,
            receiver,
        }
    }

    /// Runs the rebalancer loop, receiving and executing operations.
    /// Returns when the sender channel is closed.
    pub(crate) async fn run(mut self) {
        info!("Rebalancer started");

        while let Some(operation) = self.receiver.recv().await {
            self.execute(operation).await;
        }

        info!("Rebalancer stopped (channel closed)");
    }

    async fn execute(&self, operation: TriggeredOperation) {
        match operation {
            TriggeredOperation::Mint { symbol, quantity } => {
                self.equity_to_mm
                    .transfer(Equity { symbol, quantity })
                    .await
                    .inspect_err(|error| {
                        error!(?error, "Equity transfer to market-making venue failed");
                    })
                    .ok();
            }

            TriggeredOperation::Redemption {
                symbol, quantity, ..
            } => {
                self.equity_to_hedging
                    .transfer(Equity { symbol, quantity })
                    .await
                    .inspect_err(|error| {
                        error!(?error, "Equity transfer to hedging venue failed");
                    })
                    .ok();
            }

            TriggeredOperation::UsdcAlpacaToBase { amount } => {
                self.usdc_to_mm
                    .transfer(amount)
                    .await
                    .inspect_err(|error| {
                        error!(?error, "USDC transfer to market-making venue failed");
                    })
                    .ok();
            }

            TriggeredOperation::UsdcBaseToAlpaca { amount } => {
                self.usdc_to_hedging
                    .transfer(amount)
                    .await
                    .inspect_err(|error| {
                        error!(?error, "USDC transfer to hedging venue failed");
                    })
                    .ok();
            }
        }
    }

    async fn execute_mint(&self, symbol: Symbol, quantity: FractionalShares) {
        let issuer_request_id = IssuerRequestId::new(Uuid::new_v4().to_string());

        info!(
            %symbol,
            ?quantity,
            id = %issuer_request_id.0,
            "Executing mint operation"
        );

        match self
            .mint_manager
            .execute_mint(&issuer_request_id, symbol.clone(), quantity, self.wallet)
            .await
        {
            Ok(()) => {
                info!(%symbol, "Mint operation completed successfully");
            }
            Err(error) => {
                error!(%symbol, error = %error, "Mint operation failed");
            }
        }
    }

    async fn execute_redemption(
        &self,
        symbol: Symbol,
        quantity: FractionalShares,
        wrapped_token: Address,
        unwrapped_token: Address,
    ) {
        let amount = match quantity.to_u256_18_decimals() {
            Ok(amount) => amount,
            Err(error) => {
                error!(
                    %symbol,
                    ?quantity,
                    error = %error,
                    "Redemption operation failed: share conversion error"
                );
                return;
            }
        };

        let aggregate_id = RedemptionAggregateId::new(Uuid::new_v4().to_string());

        log_redemption_start(
            &symbol,
            quantity,
            wrapped_token,
            unwrapped_token,
            amount,
            &aggregate_id,
        );

        match self
            .redemption_manager
            .execute_redemption(
                &aggregate_id,
                symbol.clone(),
                quantity,
                wrapped_token,
                unwrapped_token,
                amount,
            )
            .await
        {
            Ok(()) => info!(%symbol, "Redemption operation completed successfully"),
            Err(error) => {
                error!(%symbol, error = %error, "Redemption operation failed");
            }
        }
    }

    async fn execute_usdc_alpaca_to_base(&self, amount: crate::threshold::Usdc) {
        let id = UsdcRebalanceId::new(Uuid::new_v4().to_string());

        info!(?amount, id = %id.0, "Executing USDC Alpaca to Base rebalance");

        match self.usdc_manager.execute_alpaca_to_base(&id, amount).await {
            Ok(()) => {
                info!("USDC Alpaca to Base rebalance completed successfully");
            }
            Err(error) => {
                error!(error = %error, "USDC Alpaca to Base rebalance failed");
            }
        }
    }

    async fn execute_usdc_base_to_alpaca(&self, amount: crate::threshold::Usdc) {
        let id = UsdcRebalanceId::new(Uuid::new_v4().to_string());

        info!(?amount, id = %id.0, "Executing USDC Base to Alpaca rebalance");

        match self.usdc_manager.execute_base_to_alpaca(&id, amount).await {
            Ok(()) => {
                info!("USDC Base to Alpaca rebalance completed successfully");
            }
            Err(error) => {
                error!(error = %error, "USDC Base to Alpaca rebalance failed");
            }
        }
    }
}

fn log_redemption_start(
    symbol: &Symbol,
    quantity: FractionalShares,
    wrapped_token: Address,
    unwrapped_token: Address,
    amount: U256,
    aggregate_id: &RedemptionAggregateId,
) {
    info!(
        %symbol,
        ?quantity,
        %wrapped_token,
        %unwrapped_token,
        %amount,
        aggregate_id = %aggregate_id.0,
        "Executing redemption operation"
    );
}

#[cfg(test)]
mod tests {
    use alloy::primitives::address;
    use rust_decimal_macros::dec;
    use std::sync::Arc;

    use st0x_execution::{FractionalShares, Symbol};

    use super::*;
    use crate::rebalancing::equity::mock::MockCrossVenueEquityTransfer;
    use crate::rebalancing::usdc::mock::MockUsdcRebalance;

    async fn execute(
        operations: Vec<TriggeredOperation>,
    ) -> (Arc<MockCrossVenueEquityTransfer>, Arc<MockUsdcRebalance>) {
        let equity = Arc::new(MockCrossVenueEquityTransfer::new());
        let usdc = Arc::new(MockUsdcRebalance::new());
        let (sender, receiver) = mpsc::channel(10);

        let rebalancer = Rebalancer::new(
            Arc::clone(&equity) as Arc<EquityToMarketMaking>,
            Arc::clone(&equity) as Arc<EquityToHedging>,
            Arc::clone(&usdc) as Arc<UsdcToMarketMaking>,
            Arc::clone(&usdc) as Arc<UsdcToHedging>,
            receiver,
        );

        for operation in operations {
            sender.send(operation).await.unwrap();
        }

        drop(sender);
        rebalancer.run().await;

        (equity, usdc)
    }

    #[tokio::test]
    async fn execute_mint_calls_equity_to_market_making() {
        let (equity, usdc) = execute(vec![TriggeredOperation::Mint {
            symbol: Symbol::new("AAPL").unwrap(),
            quantity: FractionalShares::new(dec!(10)),
        }])
        .await;

        assert_eq!(equity.mint_calls(), 1);
        assert_eq!(equity.redeem_calls(), 0);
        assert_eq!(usdc.alpaca_to_base_calls(), 0);
        assert_eq!(usdc.base_to_alpaca_calls(), 0);
    }

    #[tokio::test]
    async fn execute_redemption_calls_equity_to_hedging() {
        let (equity, usdc) = execute(vec![TriggeredOperation::Redemption {
            symbol: Symbol::new("AAPL").unwrap(),
            quantity: FractionalShares::new(dec!(50)),
            wrapped_token: address!("0x1234567890123456789012345678901234567890"),
            unwrapped_token: address!("0xabcdef0123456789abcdef0123456789abcdef01"),
        }])
        .await;

        assert_eq!(equity.mint_calls(), 0);
        assert_eq!(equity.redeem_calls(), 1);
        assert_eq!(usdc.alpaca_to_base_calls(), 0);
        assert_eq!(usdc.base_to_alpaca_calls(), 0);
    }

    #[tokio::test]
    async fn execute_usdc_alpaca_to_base_calls_usdc_to_market_making() {
        let (equity, usdc) = execute(vec![TriggeredOperation::UsdcAlpacaToBase {
            amount: Usdc(dec!(1000)),
        }])
        .await;

        assert_eq!(equity.mint_calls(), 0);
        assert_eq!(equity.redeem_calls(), 0);
        assert_eq!(usdc.alpaca_to_base_calls(), 1);
        assert_eq!(usdc.base_to_alpaca_calls(), 0);
    }

    #[tokio::test]
    async fn execute_usdc_base_to_alpaca_calls_usdc_to_hedging() {
        let (equity, usdc) = execute(vec![TriggeredOperation::UsdcBaseToAlpaca {
            amount: Usdc(dec!(2000)),
        }])
        .await;

        assert_eq!(equity.mint_calls(), 0);
        assert_eq!(equity.redeem_calls(), 0);
        assert_eq!(usdc.alpaca_to_base_calls(), 0);
        assert_eq!(usdc.base_to_alpaca_calls(), 1);
    }

    #[tokio::test]
    async fn run_processes_multiple_operations() {
        let (equity, usdc) = execute(vec![
            TriggeredOperation::Mint {
                symbol: Symbol::new("AAPL").unwrap(),
                quantity: FractionalShares::new(dec!(10)),
            },
            TriggeredOperation::Mint {
                symbol: Symbol::new("TSLA").unwrap(),
                quantity: FractionalShares::new(dec!(20)),
            },
            TriggeredOperation::Redemption {
                symbol: Symbol::new("GOOG").unwrap(),
                quantity: FractionalShares::new(dec!(5)),
                wrapped_token: address!("0x1234567890123456789012345678901234567890"),
                unwrapped_token: address!("0xabcdef0123456789abcdef0123456789abcdef01"),
            },
            TriggeredOperation::UsdcAlpacaToBase {
                amount: Usdc(dec!(500)),
            },
            TriggeredOperation::UsdcBaseToAlpaca {
                amount: Usdc(dec!(300)),
            },
        ])
        .await;

        assert_eq!(equity.mint_calls(), 2);
        assert_eq!(equity.redeem_calls(), 1);
        assert_eq!(usdc.alpaca_to_base_calls(), 1);
        assert_eq!(usdc.base_to_alpaca_calls(), 1);
    }

    #[tokio::test]
    async fn run_terminates_when_channel_closes() {
        execute(vec![]).await;
    }
}

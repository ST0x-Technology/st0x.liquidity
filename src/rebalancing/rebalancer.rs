//! Operation executor that dispatches triggered rebalancing operations to
//! managers.

use alloy::primitives::{Address, U256};
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{error, info};
use uuid::Uuid;

use st0x_execution::{FractionalShares, Symbol};

use super::mint::Mint;
use super::redemption::Redeem;
use super::trigger::TriggeredOperation;
use super::usdc::UsdcRebalance;
use crate::equity_redemption::RedemptionAggregateId;
use crate::tokenized_equity_mint::IssuerRequestId;
use crate::usdc_rebalance::UsdcRebalanceId;

/// Receives triggered rebalancing operations and dispatches them to managers.
pub(crate) struct Rebalancer<M, R, U>
where
    M: Mint,
    R: Redeem,
    U: UsdcRebalance,
{
    mint_manager: Arc<M>,
    redemption_manager: Arc<R>,
    usdc_manager: Arc<U>,
    receiver: mpsc::Receiver<TriggeredOperation>,
    wallet: Address,
}

impl<M, R, U> Rebalancer<M, R, U>
where
    M: Mint,
    R: Redeem,
    U: UsdcRebalance,
{
    pub(crate) fn new(
        mint_manager: Arc<M>,
        redemption_manager: Arc<R>,
        usdc_manager: Arc<U>,
        receiver: mpsc::Receiver<TriggeredOperation>,
        wallet: Address,
    ) -> Self {
        Self {
            mint_manager,
            redemption_manager,
            usdc_manager,
            receiver,
            wallet,
        }
    }

    /// Runs the rebalancer loop, receiving operations and dispatching them.
    /// Returns when the sender channel is closed.
    pub(crate) async fn run(mut self) {
        info!("Rebalancer started");

        while let Some(operation) = self.receiver.recv().await {
            self.dispatch(operation).await;
        }

        info!("Rebalancer stopped (channel closed)");
    }

    async fn dispatch(&self, operation: TriggeredOperation) {
        match operation {
            TriggeredOperation::Mint { symbol, quantity } => {
                self.execute_mint(symbol, quantity).await;
            }

            TriggeredOperation::Redemption {
                symbol,
                quantity,
                token,
            } => {
                self.execute_redemption(symbol, quantity, token).await;
            }

            TriggeredOperation::UsdcAlpacaToBase { amount } => {
                self.execute_usdc_alpaca_to_base(amount).await;
            }

            TriggeredOperation::UsdcBaseToAlpaca { amount } => {
                self.execute_usdc_base_to_alpaca(amount).await;
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

    async fn execute_redemption(&self, symbol: Symbol, quantity: FractionalShares, token: Address) {
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

        log_redemption_start(&symbol, quantity, token, amount, &aggregate_id);

        let result = self
            .redemption_manager
            .execute_redemption(&aggregate_id, symbol.clone(), quantity, token, amount)
            .await;

        log_redemption_result(&symbol, result);
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
    token: Address,
    amount: U256,
    aggregate_id: &RedemptionAggregateId,
) {
    info!(
        %symbol,
        ?quantity,
        %token,
        %amount,
        aggregate_id = %aggregate_id.0,
        "Executing redemption operation"
    );
}

fn log_redemption_result<E: std::fmt::Display>(symbol: &Symbol, result: Result<(), E>) {
    match result {
        Ok(()) => info!(%symbol, "Redemption operation completed successfully"),
        Err(error) => error!(%symbol, error = %error, "Redemption operation failed"),
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::address;
    use rust_decimal_macros::dec;
    use std::sync::Arc;
    use tokio::sync::mpsc;

    use super::*;
    use crate::rebalancing::mint::mock::MockMint;
    use crate::rebalancing::redemption::mock::MockRedeem;
    use crate::rebalancing::usdc::mock::MockUsdcRebalance;
    use crate::threshold::Usdc;

    #[tokio::test]
    async fn dispatch_mint_calls_mint_manager() {
        let mint = Arc::new(MockMint::new());
        let redeem = Arc::new(MockRedeem::new());
        let usdc = Arc::new(MockUsdcRebalance::new());
        let (tx, rx) = mpsc::channel(10);

        let rebalancer = Rebalancer::new(
            Arc::clone(&mint),
            Arc::clone(&redeem),
            Arc::clone(&usdc),
            rx,
            Address::ZERO,
        );

        tx.send(TriggeredOperation::Mint {
            symbol: Symbol::new("AAPL").unwrap(),
            quantity: FractionalShares::new(dec!(100)),
        })
        .await
        .unwrap();

        drop(tx);
        rebalancer.run().await;

        assert_eq!(mint.calls(), 1);
        assert_eq!(redeem.calls(), 0);
        assert_eq!(usdc.alpaca_to_base_calls(), 0);
        assert_eq!(usdc.base_to_alpaca_calls(), 0);
    }

    #[tokio::test]
    async fn dispatch_redemption_calls_redemption_manager() {
        let mint = Arc::new(MockMint::new());
        let redeem = Arc::new(MockRedeem::new());
        let usdc = Arc::new(MockUsdcRebalance::new());
        let (tx, rx) = mpsc::channel(10);

        let rebalancer = Rebalancer::new(
            Arc::clone(&mint),
            Arc::clone(&redeem),
            Arc::clone(&usdc),
            rx,
            Address::ZERO,
        );

        tx.send(TriggeredOperation::Redemption {
            symbol: Symbol::new("AAPL").unwrap(),
            quantity: FractionalShares::new(dec!(50)),
            token: address!("0x1234567890123456789012345678901234567890"),
        })
        .await
        .unwrap();

        drop(tx);
        rebalancer.run().await;

        assert_eq!(mint.calls(), 0);
        assert_eq!(redeem.calls(), 1);
        assert_eq!(usdc.alpaca_to_base_calls(), 0);
        assert_eq!(usdc.base_to_alpaca_calls(), 0);
    }

    #[tokio::test]
    async fn dispatch_usdc_alpaca_to_base_calls_usdc_manager() {
        let mint = Arc::new(MockMint::new());
        let redeem = Arc::new(MockRedeem::new());
        let usdc = Arc::new(MockUsdcRebalance::new());
        let (tx, rx) = mpsc::channel(10);

        let rebalancer = Rebalancer::new(
            Arc::clone(&mint),
            Arc::clone(&redeem),
            Arc::clone(&usdc),
            rx,
            Address::ZERO,
        );

        tx.send(TriggeredOperation::UsdcAlpacaToBase {
            amount: Usdc(dec!(1000)),
        })
        .await
        .unwrap();

        drop(tx);
        rebalancer.run().await;

        assert_eq!(mint.calls(), 0);
        assert_eq!(redeem.calls(), 0);
        assert_eq!(usdc.alpaca_to_base_calls(), 1);
        assert_eq!(usdc.base_to_alpaca_calls(), 0);
    }

    #[tokio::test]
    async fn dispatch_usdc_base_to_alpaca_calls_usdc_manager() {
        let mint = Arc::new(MockMint::new());
        let redeem = Arc::new(MockRedeem::new());
        let usdc = Arc::new(MockUsdcRebalance::new());
        let (tx, rx) = mpsc::channel(10);

        let rebalancer = Rebalancer::new(
            Arc::clone(&mint),
            Arc::clone(&redeem),
            Arc::clone(&usdc),
            rx,
            Address::ZERO,
        );

        tx.send(TriggeredOperation::UsdcBaseToAlpaca {
            amount: Usdc(dec!(2000)),
        })
        .await
        .unwrap();

        drop(tx);
        rebalancer.run().await;

        assert_eq!(mint.calls(), 0);
        assert_eq!(redeem.calls(), 0);
        assert_eq!(usdc.alpaca_to_base_calls(), 0);
        assert_eq!(usdc.base_to_alpaca_calls(), 1);
    }

    #[tokio::test]
    async fn run_processes_multiple_operations() {
        let mint = Arc::new(MockMint::new());
        let redeem = Arc::new(MockRedeem::new());
        let usdc = Arc::new(MockUsdcRebalance::new());
        let (tx, rx) = mpsc::channel(10);

        let rebalancer = Rebalancer::new(
            Arc::clone(&mint),
            Arc::clone(&redeem),
            Arc::clone(&usdc),
            rx,
            Address::ZERO,
        );

        tx.send(TriggeredOperation::Mint {
            symbol: Symbol::new("AAPL").unwrap(),
            quantity: FractionalShares::new(dec!(10)),
        })
        .await
        .unwrap();

        tx.send(TriggeredOperation::Mint {
            symbol: Symbol::new("TSLA").unwrap(),
            quantity: FractionalShares::new(dec!(20)),
        })
        .await
        .unwrap();

        tx.send(TriggeredOperation::Redemption {
            symbol: Symbol::new("GOOG").unwrap(),
            quantity: FractionalShares::new(dec!(5)),
            token: address!("0x1234567890123456789012345678901234567890"),
        })
        .await
        .unwrap();

        tx.send(TriggeredOperation::UsdcAlpacaToBase {
            amount: Usdc(dec!(500)),
        })
        .await
        .unwrap();

        tx.send(TriggeredOperation::UsdcBaseToAlpaca {
            amount: Usdc(dec!(300)),
        })
        .await
        .unwrap();

        drop(tx);
        rebalancer.run().await;

        assert_eq!(mint.calls(), 2);
        assert_eq!(redeem.calls(), 1);
        assert_eq!(usdc.alpaca_to_base_calls(), 1);
        assert_eq!(usdc.base_to_alpaca_calls(), 1);
    }

    #[tokio::test]
    async fn run_terminates_when_channel_closes() {
        let mint = Arc::new(MockMint::new());
        let redeem = Arc::new(MockRedeem::new());
        let usdc = Arc::new(MockUsdcRebalance::new());
        let (tx, rx) = mpsc::channel(10);

        let rebalancer = Rebalancer::new(mint, redeem, usdc, rx, Address::ZERO);

        drop(tx);
        rebalancer.run().await;
    }

    #[tokio::test]
    async fn redemption_with_negative_shares_logs_error_and_continues() {
        let mint = Arc::new(MockMint::new());
        let redeem = Arc::new(MockRedeem::new());
        let usdc = Arc::new(MockUsdcRebalance::new());
        let (tx, rx) = mpsc::channel(10);

        let rebalancer = Rebalancer::new(
            Arc::clone(&mint),
            Arc::clone(&redeem),
            Arc::clone(&usdc),
            rx,
            Address::ZERO,
        );

        tx.send(TriggeredOperation::Redemption {
            symbol: Symbol::new("AAPL").unwrap(),
            quantity: FractionalShares::new(dec!(-10)),
            token: address!("0x1234567890123456789012345678901234567890"),
        })
        .await
        .unwrap();

        tx.send(TriggeredOperation::Mint {
            symbol: Symbol::new("TSLA").unwrap(),
            quantity: FractionalShares::new(dec!(50)),
        })
        .await
        .unwrap();

        drop(tx);
        rebalancer.run().await;

        // Redemption with negative shares should not call the manager
        assert_eq!(redeem.calls(), 0);
        // But the subsequent mint should still be processed
        assert_eq!(mint.calls(), 1);
    }
}

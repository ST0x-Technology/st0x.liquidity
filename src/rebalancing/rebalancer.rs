//! Operation executor that dispatches triggered rebalancing operations to managers.

use alloy::primitives::{Address, U256};
use rust_decimal::Decimal;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{error, info};
use uuid::Uuid;

use super::mint::Mint;
use super::redemption::Redeem;
use super::trigger::TriggeredOperation;
use super::usdc::UsdcRebalance;
use crate::equity_redemption::RedemptionAggregateId;
use crate::shares::FractionalShares;
use crate::tokenized_equity_mint::IssuerRequestId;
use crate::usdc_rebalance::UsdcRebalanceId;
use st0x_broker::Symbol;

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
            Err(e) => {
                error!(%symbol, error = %e, "Mint operation failed");
            }
        }
    }

    async fn execute_redemption(&self, symbol: Symbol, quantity: FractionalShares, token: Address) {
        let amount = match shares_to_u256_18_decimals(quantity) {
            Ok(amount) => amount,
            Err(e) => {
                error!(
                    %symbol,
                    ?quantity,
                    error = %e,
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
            Err(e) => {
                error!(error = %e, "USDC Alpaca to Base rebalance failed");
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
            Err(e) => {
                error!(error = %e, "USDC Base to Alpaca rebalance failed");
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
        Err(e) => error!(%symbol, error = %e, "Redemption operation failed"),
    }
}

/// 10^18 - scale factor for converting shares to 18-decimal token amounts.
/// Constructed via from_parts(lo, mid, hi, negative, scale) where lo/mid/hi
/// form a 96-bit integer: 10^18 = 0x0DE0B6B3A7640000 = (mid=232830643 << 32) | lo=2808348672
const ERC20_DECIMAL_SCALE: Decimal = Decimal::from_parts(2_808_348_672, 232_830_643, 0, false, 0);

/// Converts FractionalShares (Decimal) to U256 with 18 decimal places.
///
/// Underflow policy: values that scale to less than 1 (i.e., < 1e-18 shares)
/// return an error rather than silently becoming zero.
fn shares_to_u256_18_decimals(shares: FractionalShares) -> Result<U256, SharesConversionError> {
    let value = shares.0;

    if value.is_sign_negative() {
        return Err(SharesConversionError::NegativeValue(value));
    }

    if value.is_zero() {
        return Ok(U256::ZERO);
    }

    let scaled = value
        .checked_mul(ERC20_DECIMAL_SCALE)
        .ok_or(SharesConversionError::Overflow)?;

    let truncated = scaled.trunc();

    if truncated.is_zero() {
        return Err(SharesConversionError::Underflow(value));
    }

    let as_str = truncated.to_string();

    U256::from_str_radix(&as_str, 10).map_err(SharesConversionError::ParseError)
}

#[derive(Debug, thiserror::Error)]
enum SharesConversionError {
    #[error("shares value cannot be negative: {0}")]
    NegativeValue(Decimal),
    #[error("shares value too small to represent with 18 decimals: {0}")]
    Underflow(Decimal),
    #[error("overflow when scaling shares to 18 decimals")]
    Overflow,
    #[error("failed to parse U256: {0}")]
    ParseError(#[from] alloy::primitives::ruint::ParseError),
}

#[cfg(test)]
mod tests {
    use alloy::primitives::address;
    use rust_decimal_macros::dec;

    use super::*;
    use crate::rebalancing::mint::mock::MockMint;
    use crate::rebalancing::redemption::mock::MockRedeem;
    use crate::rebalancing::usdc::mock::MockUsdcRebalance;
    use crate::threshold::Usdc;

    #[test]
    fn erc20_decimal_scale_equals_1e18() {
        let expected = dec!(1_000_000_000_000_000_000);
        assert_eq!(ERC20_DECIMAL_SCALE, expected);
    }

    #[test]
    fn shares_to_u256_converts_whole_number() {
        let shares = FractionalShares(dec!(42));
        let result = shares_to_u256_18_decimals(shares).unwrap();

        let expected = U256::from(42_000_000_000_000_000_000_u128);
        assert_eq!(result, expected);
    }

    #[test]
    fn shares_to_u256_converts_fractional() {
        let shares = FractionalShares(dec!(100.5));
        let result = shares_to_u256_18_decimals(shares).unwrap();

        let expected = U256::from(100_500_000_000_000_000_000_u128);
        assert_eq!(result, expected);
    }

    #[test]
    fn shares_to_u256_converts_zero() {
        let shares = FractionalShares(dec!(0));
        let result = shares_to_u256_18_decimals(shares).unwrap();

        assert_eq!(result, U256::ZERO);
    }

    #[test]
    fn shares_to_u256_rejects_negative() {
        let shares = FractionalShares(dec!(-10));
        let result = shares_to_u256_18_decimals(shares);

        assert!(matches!(
            result,
            Err(SharesConversionError::NegativeValue(_))
        ));
    }

    #[test]
    fn shares_to_u256_rejects_underflow() {
        // Value smaller than 1e-18 should error
        let shares = FractionalShares(Decimal::from_parts(1, 0, 0, false, 28));
        let result = shares_to_u256_18_decimals(shares);

        assert!(matches!(result, Err(SharesConversionError::Underflow(_))));
    }

    #[test]
    fn shares_to_u256_rejects_overflow() {
        // Decimal::MAX * 10^18 would overflow
        let shares = FractionalShares(Decimal::MAX);
        let result = shares_to_u256_18_decimals(shares);

        assert!(matches!(result, Err(SharesConversionError::Overflow)));
    }

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
            quantity: FractionalShares(dec!(100)),
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
            quantity: FractionalShares(dec!(50)),
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
            quantity: FractionalShares(dec!(10)),
        })
        .await
        .unwrap();

        tx.send(TriggeredOperation::Mint {
            symbol: Symbol::new("TSLA").unwrap(),
            quantity: FractionalShares(dec!(20)),
        })
        .await
        .unwrap();

        tx.send(TriggeredOperation::Redemption {
            symbol: Symbol::new("GOOG").unwrap(),
            quantity: FractionalShares(dec!(5)),
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
            quantity: FractionalShares(dec!(-10)),
            token: address!("0x1234567890123456789012345678901234567890"),
        })
        .await
        .unwrap();

        tx.send(TriggeredOperation::Mint {
            symbol: Symbol::new("TSLA").unwrap(),
            quantity: FractionalShares(dec!(50)),
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

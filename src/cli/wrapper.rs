//! CLI commands for ERC-4626 wrapping and unwrapping operations.

use alloy::primitives::Address;
use std::io::Write;

use st0x_execution::{FractionalShares, Positive, SharesBlockchain, Symbol};

use crate::config::Ctx;
use crate::wrapper::{Wrapper, WrapperService};

pub(super) async fn wrap_equity_command<Writer: Write>(
    stdout: &mut Writer,
    symbol: Symbol,
    quantity: Positive<FractionalShares>,
    ctx: &Ctx,
) -> anyhow::Result<()> {
    let wallet_ctx = ctx.wallet()?;
    let base_wallet = wallet_ctx.base_wallet().clone();
    let owner = base_wallet.address();
    let wrapper = WrapperService::new(base_wallet, ctx.assets.equities.symbols.clone());

    wrap_equity_with_wrapper(stdout, &wrapper, owner, symbol, quantity).await
}

async fn wrap_equity_with_wrapper<Writer: Write, W: Wrapper + ?Sized>(
    stdout: &mut Writer,
    wrapper: &W,
    owner: Address,
    symbol: Symbol,
    quantity: Positive<FractionalShares>,
) -> anyhow::Result<()> {
    writeln!(
        stdout,
        "Wrapping tokenized equity into wrapped vault shares"
    )?;
    writeln!(stdout, "   Symbol: {symbol}")?;
    writeln!(stdout, "   Underlying quantity: {quantity}")?;
    writeln!(stdout, "   Liquidity wallet: {owner}")?;

    let wrapped_token = wrapper.lookup_derivative(&symbol)?;
    let underlying_token = wrapper.lookup_underlying(&symbol)?;

    writeln!(stdout, "   Wrapped token: {wrapped_token}")?;
    writeln!(stdout, "   Underlying token: {underlying_token}")?;

    let underlying_amount = quantity.inner().to_u256_18_decimals()?;
    writeln!(
        stdout,
        "   Underlying amount (smallest unit): {underlying_amount}"
    )?;
    writeln!(
        stdout,
        "   Depositing underlying shares into ERC-4626 vault..."
    )?;

    let (wrap_tx_hash, wrapped_amount_u256) = wrapper
        .to_wrapped(wrapped_token, underlying_amount, owner)
        .await?;
    let wrapped_amount = FractionalShares::from_u256_18_decimals(wrapped_amount_u256)?;

    writeln!(stdout, "   Transaction hash: {wrap_tx_hash}")?;
    writeln!(stdout, "   Wrapped amount received: {wrapped_amount}")?;
    writeln!(
        stdout,
        "   Wrapped amount received (smallest unit): {wrapped_amount_u256}"
    )?;
    writeln!(stdout, "Wrap completed successfully!")?;

    Ok(())
}

pub(super) async fn unwrap_equity_command<Writer: Write>(
    stdout: &mut Writer,
    symbol: Symbol,
    quantity: Positive<FractionalShares>,
    ctx: &Ctx,
) -> anyhow::Result<()> {
    let wallet_ctx = ctx.wallet()?;
    let base_wallet = wallet_ctx.base_wallet().clone();
    let owner = base_wallet.address();
    let wrapper = WrapperService::new(base_wallet, ctx.assets.equities.symbols.clone());

    unwrap_equity_with_wrapper(stdout, &wrapper, owner, symbol, quantity).await
}

async fn unwrap_equity_with_wrapper<Writer: Write, W: Wrapper + ?Sized>(
    stdout: &mut Writer,
    wrapper: &W,
    owner: Address,
    symbol: Symbol,
    quantity: Positive<FractionalShares>,
) -> anyhow::Result<()> {
    writeln!(stdout, "Unwrapping wrapped equity shares")?;
    writeln!(stdout, "   Symbol: {symbol}")?;
    writeln!(stdout, "   Wrapped quantity: {quantity}")?;
    writeln!(stdout, "   Liquidity wallet: {owner}")?;

    let wrapped_token = wrapper.lookup_derivative(&symbol)?;
    let underlying_token = wrapper.lookup_underlying(&symbol)?;

    writeln!(stdout, "   Wrapped token: {wrapped_token}")?;
    writeln!(stdout, "   Underlying token: {underlying_token}")?;

    let wrapped_amount = quantity.inner().to_u256_18_decimals()?;
    writeln!(
        stdout,
        "   Wrapped amount (smallest unit): {wrapped_amount}"
    )?;
    writeln!(stdout, "   Redeeming wrapped shares...")?;

    let (unwrap_tx_hash, underlying_amount_u256) = wrapper
        .to_underlying(wrapped_token, wrapped_amount, owner, owner)
        .await?;
    let underlying_amount = FractionalShares::from_u256_18_decimals(underlying_amount_u256)?;

    writeln!(stdout, "   Transaction hash: {unwrap_tx_hash}")?;
    writeln!(stdout, "   Underlying amount received: {underlying_amount}")?;
    writeln!(
        stdout,
        "   Underlying amount received (smallest unit): {underlying_amount_u256}"
    )?;
    writeln!(stdout, "Unwrap completed successfully!")?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use alloy::primitives::Address;
    use url::Url;

    use st0x_execution::Symbol;

    use super::{
        unwrap_equity_command, unwrap_equity_with_wrapper, wrap_equity_command,
        wrap_equity_with_wrapper,
    };
    use crate::config::{AssetsConfig, BrokerCtx, Ctx, EquitiesConfig, LogLevel, TradingMode};
    use crate::onchain::EvmCtx;
    use crate::test_utils::positive_shares;
    use crate::threshold::ExecutionThreshold;
    use crate::wrapper::mock::MockWrapper;

    fn create_ctx_without_rebalancing() -> Ctx {
        Ctx {
            database_url: ":memory:".to_string(),
            log_level: LogLevel::Debug,
            server_port: 8080,
            evm: EvmCtx {
                ws_rpc_url: Url::parse("ws://localhost:8545").unwrap(),
                orderbook: Address::random(),
                deployment_block: 1,
            },
            order_polling_interval: 15,
            order_polling_max_jitter: 5,
            position_check_interval: 60,
            inventory_poll_interval: 60,
            apalis_finished_job_cleanup_interval_secs: 3600,
            broker: BrokerCtx::DryRun,
            telemetry: None,
            trading_mode: TradingMode::Standalone {
                order_owner: Address::ZERO,
            },
            wallet: None,
            execution_threshold: ExecutionThreshold::whole_share(),
            assets: AssetsConfig {
                equities: EquitiesConfig::default(),
                cash: None,
            },
            travel_rule: None,
        }
    }

    #[tokio::test]
    async fn wrap_equity_requires_wallet_config() {
        let ctx = create_ctx_without_rebalancing();
        let mut stdout = Vec::new();

        let error = wrap_equity_command(
            &mut stdout,
            Symbol::new("AAPL").unwrap(),
            positive_shares("10.5"),
            &ctx,
        )
        .await
        .unwrap_err();

        assert!(
            error.to_string().contains("configured [wallet] section"),
            "expected wallet config error, got: {error}"
        );
    }

    #[tokio::test]
    async fn unwrap_equity_requires_wallet_config() {
        let ctx = create_ctx_without_rebalancing();
        let mut stdout = Vec::new();

        let error = unwrap_equity_command(
            &mut stdout,
            Symbol::new("AAPL").unwrap(),
            positive_shares("10.5"),
            &ctx,
        )
        .await
        .unwrap_err();

        assert!(
            error.to_string().contains("configured [wallet] section"),
            "expected wallet config error, got: {error}"
        );
    }

    #[tokio::test]
    async fn wrap_equity_success_prints_transaction_details() {
        let wrapped_token = Address::repeat_byte(0x22);
        let underlying_token = Address::repeat_byte(0x11);
        let wrapper = MockWrapper::new()
            .with_wrapped_token(wrapped_token)
            .with_tokenized_shares(underlying_token);
        let mut stdout = Vec::new();

        wrap_equity_with_wrapper(
            &mut stdout,
            &wrapper,
            Address::repeat_byte(0xaa),
            Symbol::new("AAPL").unwrap(),
            positive_shares("10.5"),
        )
        .await
        .unwrap();

        let output = String::from_utf8(stdout).unwrap();
        assert!(output.contains("Wrapping tokenized equity into wrapped vault shares"));
        assert!(output.contains("Symbol: AAPL"));
        assert!(output.contains("Underlying quantity: 10.5"));
        assert!(output.contains(&format!("Wrapped token: {wrapped_token}")));
        assert!(output.contains(&format!("Underlying token: {underlying_token}")));
        assert!(output.contains("Transaction hash:"));
        assert!(output.contains("Wrapped amount received: 10.5"));
        assert!(output.contains("Wrap completed successfully"));
    }

    #[tokio::test]
    async fn unwrap_equity_success_prints_transaction_details() {
        let wrapped_token = Address::repeat_byte(0x22);
        let underlying_token = Address::repeat_byte(0x11);
        let wrapper = MockWrapper::new()
            .with_wrapped_token(wrapped_token)
            .with_tokenized_shares(underlying_token);
        let mut stdout = Vec::new();

        unwrap_equity_with_wrapper(
            &mut stdout,
            &wrapper,
            Address::repeat_byte(0xaa),
            Symbol::new("AAPL").unwrap(),
            positive_shares("10.5"),
        )
        .await
        .unwrap();

        let output = String::from_utf8(stdout).unwrap();
        assert!(output.contains("Unwrapping wrapped equity shares"));
        assert!(output.contains("Symbol: AAPL"));
        assert!(output.contains("Wrapped quantity: 10.5"));
        assert!(output.contains(&format!("Wrapped token: {wrapped_token}")));
        assert!(output.contains(&format!("Underlying token: {underlying_token}")));
        assert!(output.contains("Transaction hash:"));
        assert!(output.contains("Underlying amount received: 10.5"));
        assert!(output.contains("Unwrap completed successfully"));
    }

    #[tokio::test]
    async fn wrap_equity_propagates_symbol_lookup_failure() {
        let wrapper = MockWrapper::failing_lookup();
        let mut stdout = Vec::new();

        let error = wrap_equity_with_wrapper(
            &mut stdout,
            &wrapper,
            Address::repeat_byte(0xaa),
            Symbol::new("AAPL").unwrap(),
            positive_shares("10.5"),
        )
        .await
        .unwrap_err();

        assert!(
            error.to_string().contains("Symbol not configured: AAPL"),
            "expected symbol lookup error, got: {error}"
        );
    }

    #[tokio::test]
    async fn unwrap_equity_propagates_symbol_lookup_failure() {
        let wrapper = MockWrapper::failing_derivative_lookup();
        let mut stdout = Vec::new();

        let error = unwrap_equity_with_wrapper(
            &mut stdout,
            &wrapper,
            Address::repeat_byte(0xaa),
            Symbol::new("AAPL").unwrap(),
            positive_shares("10.5"),
        )
        .await
        .unwrap_err();

        assert!(
            error.to_string().contains("Symbol not configured: AAPL"),
            "expected symbol lookup error, got: {error}"
        );
    }

    #[tokio::test]
    async fn wrap_equity_propagates_wrap_failure() {
        let wrapper = MockWrapper::failing();
        let mut stdout = Vec::new();

        let error = wrap_equity_with_wrapper(
            &mut stdout,
            &wrapper,
            Address::repeat_byte(0xaa),
            Symbol::new("AAPL").unwrap(),
            positive_shares("10.5"),
        )
        .await
        .unwrap_err();

        assert!(
            error.to_string().contains("Missing Deposit event"),
            "expected wrap error, got: {error}"
        );
    }

    #[tokio::test]
    async fn unwrap_equity_propagates_unwrap_failure() {
        let wrapper = MockWrapper::failing_unwrap();
        let mut stdout = Vec::new();

        let error = unwrap_equity_with_wrapper(
            &mut stdout,
            &wrapper,
            Address::repeat_byte(0xaa),
            Symbol::new("AAPL").unwrap(),
            positive_shares("10.5"),
        )
        .await
        .unwrap_err();

        assert!(
            error.to_string().contains("Missing Withdraw event"),
            "expected unwrap error, got: {error}"
        );
    }
}

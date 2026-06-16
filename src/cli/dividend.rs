//! Composite dividend NAV-bump command.
//!
//! Runs the three issuer steps of a dividend bump -- buy the equity offchain,
//! tokenize it onchain, donate the tokenized shares into the ERC-4626 wrapper --
//! in a single invocation. Each step waits for the previous one to settle (the
//! buy for its fill, the tokenize for tokens to land onchain, the donate for its
//! receipt), so the flow is reliable instead of a babysat three-command runbook.
//! Shares are tokenized to, and donated from, the configured `[wallet]`, so the
//! issuer config funds and signs the whole bump.

use std::io::Write;

use alloy::providers::Provider;

use st0x_config::Ctx;
use st0x_execution::{FractionalShares, Positive, Symbol};

use super::{rebalancing, trading, wrapper};

pub(super) async fn dividend_bump_command<Writer: Write, Prov: Provider + Clone + 'static>(
    stdout: &mut Writer,
    symbol: Symbol,
    quantity: Positive<FractionalShares>,
    ctx: &Ctx,
    provider: Prov,
) -> anyhow::Result<()> {
    writeln!(stdout, "Dividend NAV bump: {quantity} {symbol}")?;

    writeln!(
        stdout,
        "Step 1/3: buying {quantity} {symbol} and waiting for fill"
    )?;
    trading::execute_market_buy_until_filled(ctx, symbol.clone(), quantity, stdout).await?;

    writeln!(stdout, "Step 2/3: tokenizing {quantity} {symbol} onchain")?;
    rebalancing::alpaca_tokenize_command(
        stdout,
        symbol.clone(),
        quantity.inner(),
        None,
        ctx,
        provider,
    )
    .await?;

    writeln!(
        stdout,
        "Step 3/3: donating {quantity} {symbol} into the wrapper"
    )?;
    wrapper::donate_equity_command(stdout, symbol, quantity, ctx).await?;

    writeln!(stdout, "✅ Dividend NAV bump completed")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{Address, address};
    use alloy::providers::ProviderBuilder;
    use url::Url;

    use st0x_config::create_test_issuance_ctx;
    use st0x_config::{
        AssetsConfig, BrokerCtx, EquitiesConfig, EvmCtx, ExecutionThreshold, LogLevel, TradingMode,
    };

    use super::*;
    use crate::test_utils::positive_shares;

    fn dry_run_ctx() -> Ctx {
        Ctx {
            database_url: ":memory:".to_string(),
            log_level: LogLevel::Debug,
            log_dir: None,
            server_port: 8080,
            board_port: 8081,
            evm: EvmCtx {
                rpc_url: Url::parse("http://localhost:8545").unwrap(),
                orderbook: address!("0x1234567890123456789012345678901234567890"),
                deployment_block: 1,
                required_confirmations: 0,
            },
            order_polling_interval: 15,
            order_polling_max_jitter: 5,
            position_check_interval: 60,
            inventory_poll_interval: 60,
            order_fill_poll_interval: 5,
            apalis_finished_job_cleanup_interval_secs: 3600,
            broker: BrokerCtx::DryRun,
            telemetry: None,
            alerts: None,
            trading_mode: TradingMode::Standalone,
            order_owner: Address::ZERO,
            wallet: None,
            wallet_meta: None,
            execution_threshold: ExecutionThreshold::whole_share(),
            assets: AssetsConfig {
                equities: EquitiesConfig::default(),
                cash: None,
            },
            travel_rule: None,
            rest_api: None,
            issuance: create_test_issuance_ctx(),
            redemption_wallet: None,
        }
    }

    /// The bump must run buy -> tokenize -> donate in order and stop at the first
    /// failing step. With a DryRun broker the mock buy fills, but tokenization
    /// fails because the symbol is not configured, so the donate step must never
    /// run and the error must propagate to the caller.
    #[tokio::test]
    async fn dividend_bump_stops_after_buy_when_tokenize_fails() {
        let ctx = dry_run_ctx();
        let provider =
            ProviderBuilder::new().connect_http(Url::parse("http://localhost:8545").unwrap());
        let mut stdout = Vec::new();

        let error = dividend_bump_command(
            &mut stdout,
            Symbol::new("COIN").unwrap(),
            positive_shares("10"),
            &ctx,
            provider,
        )
        .await
        .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("equity COIN is not configured in [assets.equities]"),
            "tokenize must fail on the unconfigured symbol, got: {error}"
        );

        let output = String::from_utf8(stdout).unwrap();
        assert!(
            output.contains("Buy filled"),
            "the buy must complete before tokenize runs; output: {output}"
        );
        assert!(
            output.contains("Step 2/3"),
            "tokenize must be attempted after the buy; output: {output}"
        );
        assert!(
            !output.contains("Step 3/3"),
            "donate must not run after tokenize fails; output: {output}"
        );
    }
}

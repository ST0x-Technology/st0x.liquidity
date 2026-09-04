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
use std::sync::Arc;

use async_trait::async_trait;

use st0x_config::Ctx;
use st0x_execution::{FractionalShares, Positive, Symbol};
use st0x_hedge::alerts::{LogNotifier, Notifier};

use super::{TokenizationNetwork, rebalancing, trading, wrapper};

#[async_trait]
trait DividendBumpOperations: Sync {
    fn prepare_notifier(&self, ctx: &Ctx) -> anyhow::Result<Arc<dyn Notifier>>;

    async fn buy<Writer: Write + Send>(
        &self,
        stdout: &mut Writer,
        symbol: Symbol,
        quantity: Positive<FractionalShares>,
        ctx: &Ctx,
    ) -> anyhow::Result<Positive<FractionalShares>>;

    async fn tokenize<Writer: Write + Send>(
        &self,
        stdout: &mut Writer,
        symbol: Symbol,
        quantity: Positive<FractionalShares>,
        ctx: &Ctx,
    ) -> anyhow::Result<()>;

    async fn donate<Writer: Write + Send>(
        &self,
        stdout: &mut Writer,
        symbol: Symbol,
        quantity: Positive<FractionalShares>,
        notifier: &dyn Notifier,
        ctx: &Ctx,
    ) -> anyhow::Result<()>;
}

struct LiveDividendBumpOperations;

#[async_trait]
impl DividendBumpOperations for LiveDividendBumpOperations {
    fn prepare_notifier(&self, _ctx: &Ctx) -> anyhow::Result<Arc<dyn Notifier>> {
        Ok(Arc::new(LogNotifier))
    }

    async fn buy<Writer: Write + Send>(
        &self,
        stdout: &mut Writer,
        symbol: Symbol,
        quantity: Positive<FractionalShares>,
        ctx: &Ctx,
    ) -> anyhow::Result<Positive<FractionalShares>> {
        trading::execute_market_buy_until_filled(ctx, symbol, quantity, stdout).await
    }

    async fn tokenize<Writer: Write + Send>(
        &self,
        stdout: &mut Writer,
        symbol: Symbol,
        quantity: Positive<FractionalShares>,
        ctx: &Ctx,
    ) -> anyhow::Result<()> {
        rebalancing::alpaca_tokenize_command(
            stdout,
            symbol,
            quantity.inner(),
            None,
            TokenizationNetwork::Base,
            None,
            ctx,
        )
        .await
    }

    async fn donate<Writer: Write + Send>(
        &self,
        stdout: &mut Writer,
        symbol: Symbol,
        quantity: Positive<FractionalShares>,
        notifier: &dyn Notifier,
        ctx: &Ctx,
    ) -> anyhow::Result<()> {
        wrapper::donate_equity_command_with_notifier(stdout, symbol, quantity, notifier, ctx).await
    }
}

pub(super) async fn dividend_bump_command<Writer: Write + Send>(
    stdout: &mut Writer,
    symbol: Symbol,
    quantity: Positive<FractionalShares>,
    ctx: &Ctx,
) -> anyhow::Result<()> {
    dividend_bump_with_operations(stdout, symbol, quantity, ctx, &LiveDividendBumpOperations).await
}

async fn dividend_bump_with_operations<Writer: Write + Send, Operations: DividendBumpOperations>(
    stdout: &mut Writer,
    symbol: Symbol,
    quantity: Positive<FractionalShares>,
    ctx: &Ctx,
    operations: &Operations,
) -> anyhow::Result<()> {
    let notifier = operations.prepare_notifier(ctx)?;

    writeln!(stdout, "Dividend NAV bump: {quantity} {symbol}")?;

    writeln!(
        stdout,
        "Step 1/3: buying {quantity} {symbol} and waiting for fill"
    )?;
    let filled_quantity = operations
        .buy(stdout, symbol.clone(), quantity, ctx)
        .await?;

    writeln!(
        stdout,
        "Step 2/3: tokenizing {filled_quantity} {symbol} onchain"
    )?;
    operations
        .tokenize(stdout, symbol.clone(), filled_quantity, ctx)
        .await?;

    writeln!(
        stdout,
        "Step 3/3: donating {filled_quantity} {symbol} into the wrapper"
    )?;
    operations
        .donate(stdout, symbol, filled_quantity, notifier.as_ref(), ctx)
        .await?;

    writeln!(stdout, "✅ Dividend NAV bump completed")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use alloy::primitives::{Address, address};
    use url::Url;

    use st0x_config::ChainRegistry;
    use st0x_config::HedgingAssets;
    use st0x_config::create_test_issuance_ctx;
    use st0x_config::{
        BrokerCtx, ChainAssets, ExecutionThreshold, IngestionCutoff, InventoryAdapters,
        InventoryMode, LogFormat, LogLevel, TradingChain, TradingMode,
    };
    use st0x_evm::Chain;
    use st0x_hedge::operator::test_utils::try_positive_shares;

    use super::*;

    fn positive_shares(value: &str) -> Positive<FractionalShares> {
        try_positive_shares(value).expect("test shares must be valid and positive")
    }

    struct RecordingDividendBumpOperations {
        filled_quantity: Positive<FractionalShares>,
        notifier_setup_fails: bool,
        buy_calls: AtomicUsize,
        tokenized_quantities: Mutex<Vec<Positive<FractionalShares>>>,
        donated_quantities: Mutex<Vec<Positive<FractionalShares>>>,
    }

    #[async_trait]
    impl DividendBumpOperations for RecordingDividendBumpOperations {
        fn prepare_notifier(&self, _ctx: &Ctx) -> anyhow::Result<Arc<dyn Notifier>> {
            if self.notifier_setup_fails {
                anyhow::bail!("notifier setup failed");
            }

            Ok(Arc::new(LogNotifier))
        }

        async fn buy<Writer: Write + Send>(
            &self,
            _stdout: &mut Writer,
            _symbol: Symbol,
            _quantity: Positive<FractionalShares>,
            _ctx: &Ctx,
        ) -> anyhow::Result<Positive<FractionalShares>> {
            self.buy_calls.fetch_add(1, Ordering::SeqCst);
            Ok(self.filled_quantity)
        }

        async fn tokenize<Writer: Write + Send>(
            &self,
            _stdout: &mut Writer,
            _symbol: Symbol,
            quantity: Positive<FractionalShares>,
            _ctx: &Ctx,
        ) -> anyhow::Result<()> {
            self.tokenized_quantities.lock().unwrap().push(quantity);
            Ok(())
        }

        async fn donate<Writer: Write + Send>(
            &self,
            _stdout: &mut Writer,
            _symbol: Symbol,
            quantity: Positive<FractionalShares>,
            _notifier: &dyn Notifier,
            _ctx: &Ctx,
        ) -> anyhow::Result<()> {
            self.donated_quantities.lock().unwrap().push(quantity);
            Ok(())
        }
    }

    fn dry_run_ctx() -> Ctx {
        Ctx {
            database_url: ":memory:".to_string(),
            log_level: LogLevel::Debug,
            log_dir: None,
            log_format: LogFormat::Text,
            log_query_url_template: None,
            server_port: 8080,
            board_port: 8081,
            chains: ChainRegistry::single_trading_chain(TradingChain {
                redemption_wallet: None,
                assets: ChainAssets::default(),
                chain: Chain::Base,
                inventory_adapters: InventoryAdapters::default(),
                rpc_url: Url::parse("http://localhost:8545").unwrap(),
                orderbook: address!("0x1234567890123456789012345678901234567890"),
                inventory: InventoryMode::Managed {
                    inventory: address!("0x1234567890123456789012345678901234567890"),
                },
                vault_owner: Address::ZERO,
                deployment_block: 1,
                required_confirmations: 0,
                ingestion_cutoff: IngestionCutoff::Safe,
            }),
            order_polling_interval: 15,
            order_polling_max_jitter: 5,
            position_check_interval: 60,
            inventory_poll_interval: 60,
            inventory_divergence_threshold: std::num::NonZeroU32::MIN,
            hedge_order_gate_reconciliation_timeout_secs: std::num::NonZeroU64::MIN,
            order_fill_poll_interval: 5,
            extended_hours_reprice_timeout_secs: std::num::NonZeroU64::new(300),
            close_flatten_reprice_timeout_secs: 60,
            extended_hours_close_flatten_window_secs: 900,
            close_flatten_cross_max_bps: 400,
            apalis_finished_job_cleanup_interval_secs: 3600,
            broker: BrokerCtx::DryRun,
            telemetry: None,
            alerts: None,
            startup_notices: Vec::new(),
            pricing: None,
            trading_mode: TradingMode::Standalone,
            order_owner: Address::ZERO,
            wallet: None,
            wallet_meta: None,
            execution_threshold: ExecutionThreshold::whole_share(),
            assets: HedgingAssets::default(),
            travel_rule: None,
            rest_api: None,
            ops_api: None,
            issuance: create_test_issuance_ctx(),
            redemption_wallet: None,
            bot_gas_valuation: None,
            orchestrator: None,
        }
    }

    /// The bump must run buy -> tokenize -> donate in order and stop at the first
    /// failing step. With a DryRun broker the mock buy fills, but tokenization
    /// fails because the symbol is not configured, so the donate step must never
    /// run and the error must propagate to the caller.
    #[tokio::test]
    async fn dividend_bump_stops_after_buy_when_tokenize_fails() {
        let ctx = dry_run_ctx();
        let mut stdout = Vec::new();

        let error = dividend_bump_command(
            &mut stdout,
            Symbol::new("COIN").unwrap(),
            positive_shares("10"),
            &ctx,
        )
        .await
        .unwrap_err();

        assert!(
            error.to_string().contains(
                "equity COIN is not configured in [chains.<name>.trading.assets.equities]"
            ),
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

    #[tokio::test]
    async fn dividend_bump_refuses_to_buy_when_notifier_setup_fails() {
        let ctx = dry_run_ctx();
        let operations = RecordingDividendBumpOperations {
            filled_quantity: positive_shares("1"),
            notifier_setup_fails: true,
            buy_calls: AtomicUsize::new(0),
            tokenized_quantities: Mutex::new(Vec::new()),
            donated_quantities: Mutex::new(Vec::new()),
        };
        let mut stdout = Vec::new();

        let error = dividend_bump_with_operations(
            &mut stdout,
            Symbol::new("AAPL").unwrap(),
            positive_shares("1"),
            &ctx,
            &operations,
        )
        .await
        .unwrap_err();

        assert_eq!(error.to_string(), "notifier setup failed");
        assert_eq!(operations.buy_calls.load(Ordering::SeqCst), 0);
        assert!(stdout.is_empty());
    }

    #[tokio::test]
    async fn dividend_bump_tokenizes_and_donates_the_broker_filled_quantity() {
        let ctx = dry_run_ctx();
        let operations = RecordingDividendBumpOperations {
            filled_quantity: positive_shares("0.0041"),
            notifier_setup_fails: false,
            buy_calls: AtomicUsize::new(0),
            tokenized_quantities: Mutex::new(Vec::new()),
            donated_quantities: Mutex::new(Vec::new()),
        };
        let mut stdout = Vec::new();

        dividend_bump_with_operations(
            &mut stdout,
            Symbol::new("AAPL").unwrap(),
            positive_shares("0.004115451077565126"),
            &ctx,
            &operations,
        )
        .await
        .unwrap();

        assert_eq!(
            *operations.tokenized_quantities.lock().unwrap(),
            vec![positive_shares("0.0041")]
        );
        assert_eq!(
            *operations.donated_quantities.lock().unwrap(),
            vec![positive_shares("0.0041")]
        );
        let output = String::from_utf8(stdout).unwrap();
        assert!(output.contains("Step 2/3: tokenizing 0.0041 AAPL onchain"));
        assert!(output.contains("Step 3/3: donating 0.0041 AAPL into the wrapper"));
    }
}

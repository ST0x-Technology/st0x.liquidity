//! Composite dividend NAV-bump command.
//!
//! Runs the three issuer steps of a dividend bump -- buy the equity offchain,
//! tokenize it onchain, donate the tokenized shares into the ERC-4626 wrapper --
//! in a single invocation. Each step waits for the previous one to settle (the
//! buy for its fill, the tokenize for tokens to land onchain, the donate for its
//! receipt), so the flow is reliable instead of a babysat three-command runbook.
//! Shares are tokenized to, and donated from, the configured `[wallet]`, so the
//! issuer config funds and signs the whole bump.

use async_trait::async_trait;
use std::io::Write;
use std::path::Path;

use st0x_config::Ctx;
use st0x_evm::Chain;
use st0x_execution::{FractionalShares, Positive, Symbol};

use super::wrapper::DividendNavBumpNotifier;
use super::{TokenizationNetwork, rebalancing, trading, wrapper};

#[async_trait]
trait DividendBumpOperations: Sync {
    fn prepare_notifier(
        &self,
        bot_config: &Path,
    ) -> anyhow::Result<Box<dyn DividendNavBumpNotifier>>;

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
        network: TokenizationNetwork,
        ctx: &Ctx,
    ) -> anyhow::Result<()>;

    async fn donate<Writer: Write + Send>(
        &self,
        stdout: &mut Writer,
        symbol: Symbol,
        quantity: Positive<FractionalShares>,
        network: TokenizationNetwork,
        notifier: &dyn DividendNavBumpNotifier,
        ctx: &Ctx,
    ) -> anyhow::Result<()>;
}

struct LiveDividendBumpOperations;

#[async_trait]
impl DividendBumpOperations for LiveDividendBumpOperations {
    fn prepare_notifier(
        &self,
        bot_config: &Path,
    ) -> anyhow::Result<Box<dyn DividendNavBumpNotifier>> {
        Ok(Box::new(wrapper::bot_notice_client(bot_config)?))
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
        network: TokenizationNetwork,
        ctx: &Ctx,
    ) -> anyhow::Result<()> {
        rebalancing::alpaca_tokenize_command(
            stdout,
            symbol,
            quantity.inner(),
            None,
            network,
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
        network: TokenizationNetwork,
        notifier: &dyn DividendNavBumpNotifier,
        ctx: &Ctx,
    ) -> anyhow::Result<()> {
        wrapper::donate_equity_command_with_notifier(
            stdout, symbol, quantity, network, notifier, ctx,
        )
        .await
    }
}

pub(super) async fn dividend_bump_command<Writer: Write + Send>(
    stdout: &mut Writer,
    symbol: Symbol,
    quantity: Positive<FractionalShares>,
    network: TokenizationNetwork,
    bot_config: &Path,
    ctx: &Ctx,
) -> anyhow::Result<()> {
    dividend_bump_with_operations(
        stdout,
        symbol,
        quantity,
        network,
        bot_config,
        ctx,
        &LiveDividendBumpOperations,
    )
    .await
}

async fn dividend_bump_with_operations<Writer: Write + Send, Operations: DividendBumpOperations>(
    stdout: &mut Writer,
    symbol: Symbol,
    quantity: Positive<FractionalShares>,
    network: TokenizationNetwork,
    bot_config: &Path,
    ctx: &Ctx,
    operations: &Operations,
) -> anyhow::Result<()> {
    rebalancing::require_equity_mutation_network(network)?;
    let notifier = operations.prepare_notifier(bot_config)?;
    let chain = Chain::from(network);
    writeln!(stdout, "Dividend NAV bump: {quantity} {symbol} on {chain}")?;

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
        .tokenize(stdout, symbol.clone(), filled_quantity, network, ctx)
        .await?;

    writeln!(
        stdout,
        "Step 3/3: donating {filled_quantity} {symbol} into the wrapper"
    )?;
    operations
        .donate(
            stdout,
            symbol,
            filled_quantity,
            network,
            notifier.as_ref(),
            ctx,
        )
        .await?;

    wrapper::write_after_receipt(stdout, format_args!("✅ Dividend NAV bump completed"));
    Ok(())
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{Address, address};
    use httpmock::Method::POST;
    use httpmock::MockServer;
    use rain_math_float::Float;
    use std::io::Write as _;
    use std::path::Path;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tempfile::NamedTempFile;

    use st0x_config::ChainRegistry;
    use st0x_config::HedgingAssets;
    use st0x_config::create_test_issuance_ctx;
    use st0x_config::{
        BrokerCtx, ExecutionThreshold, HedgedChain, InventoryMode, LogFormat, LogLevel,
    };
    use st0x_execution::alpaca_broker_api::AlpacaBrokerMock;
    use st0x_hedge::api::{DIVIDEND_NAV_BUMP_NOTICE_PATH, DividendNavBumpNotice};
    use st0x_hedge::operator::test_utils::{mock_alpaca_broker_ctx, try_positive_shares};

    use super::*;

    fn positive_shares(value: &str) -> Positive<FractionalShares> {
        try_positive_shares(value).expect("test shares must be valid and positive")
    }

    fn unused_bot_config() -> &'static Path {
        Path::new("unused-by-test-operations")
    }

    /// The recording operations never reach the donation receipt, so they
    /// never deliver a notice.
    struct UnreachableNotifier;

    #[async_trait]
    impl DividendNavBumpNotifier for UnreachableNotifier {
        async fn notify(&self, _notice: &DividendNavBumpNotice) -> anyhow::Result<()> {
            anyhow::bail!("test operations never deliver notices")
        }
    }

    struct RecordingDividendBumpOperations {
        filled_quantity: Positive<FractionalShares>,
        notifier_setup_fails: bool,
        buy_calls: AtomicUsize,
        tokenized: Mutex<Vec<(Positive<FractionalShares>, TokenizationNetwork)>>,
        donated: Mutex<Vec<(Positive<FractionalShares>, TokenizationNetwork)>>,
    }

    #[async_trait]
    impl DividendBumpOperations for RecordingDividendBumpOperations {
        fn prepare_notifier(
            &self,
            _bot_config: &Path,
        ) -> anyhow::Result<Box<dyn DividendNavBumpNotifier>> {
            if self.notifier_setup_fails {
                anyhow::bail!("notifier setup failed");
            }

            Ok(Box::new(UnreachableNotifier))
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
            network: TokenizationNetwork,
            _ctx: &Ctx,
        ) -> anyhow::Result<()> {
            self.tokenized.lock().unwrap().push((quantity, network));
            Ok(())
        }

        async fn donate<Writer: Write + Send>(
            &self,
            _stdout: &mut Writer,
            _symbol: Symbol,
            quantity: Positive<FractionalShares>,
            network: TokenizationNetwork,
            _notifier: &dyn DividendNavBumpNotifier,
            _ctx: &Ctx,
        ) -> anyhow::Result<()> {
            self.donated.lock().unwrap().push((quantity, network));
            Ok(())
        }
    }
    #[derive(Default)]
    struct CountingDividendBumpOperations {
        buys: AtomicUsize,
        tokenizations: AtomicUsize,
        donations: AtomicUsize,
    }

    #[derive(Default)]
    struct CloseOnCompletionWriter {
        output: Vec<u8>,
        closed: bool,
    }

    impl Write for CloseOnCompletionWriter {
        fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
            if self.closed {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "writer is closed",
                ));
            }

            let mut candidate = self.output.clone();
            candidate.extend_from_slice(buffer);
            if String::from_utf8_lossy(&candidate).contains("Dividend NAV bump completed") {
                self.closed = true;
                return Err(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "writer closed before final status",
                ));
            }

            self.output.extend_from_slice(buffer);
            Ok(buffer.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[async_trait]
    impl DividendBumpOperations for CountingDividendBumpOperations {
        fn prepare_notifier(
            &self,
            _bot_config: &Path,
        ) -> anyhow::Result<Box<dyn DividendNavBumpNotifier>> {
            Ok(Box::new(UnreachableNotifier))
        }

        async fn buy<Writer: Write + Send>(
            &self,
            _stdout: &mut Writer,
            _symbol: Symbol,
            quantity: Positive<FractionalShares>,
            _ctx: &Ctx,
        ) -> anyhow::Result<Positive<FractionalShares>> {
            self.buys.fetch_add(1, Ordering::Relaxed);
            Ok(quantity)
        }

        async fn tokenize<Writer: Write + Send>(
            &self,
            _stdout: &mut Writer,
            _symbol: Symbol,
            _quantity: Positive<FractionalShares>,
            _network: TokenizationNetwork,
            _ctx: &Ctx,
        ) -> anyhow::Result<()> {
            self.tokenizations.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }

        async fn donate<Writer: Write + Send>(
            &self,
            _stdout: &mut Writer,
            _symbol: Symbol,
            _quantity: Positive<FractionalShares>,
            _network: TokenizationNetwork,
            _notifier: &dyn DividendNavBumpNotifier,
            _ctx: &Ctx,
        ) -> anyhow::Result<()> {
            self.donations.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
    }

    fn test_ctx(broker: BrokerCtx) -> Ctx {
        Ctx {
            database_url: ":memory:".to_string(),
            log_level: LogLevel::Debug,
            file_logging: None,
            log_format: LogFormat::Text,
            log_query_url_template: None,
            server_port: 8080,
            board_port: 8081,
            chains: ChainRegistry::single_hedged_chain(
                HedgedChain::test()
                    .orderbook(address!("0x1234567890123456789012345678901234567890"))
                    .inventory(InventoryMode::Managed {
                        inventory: address!("0x1234567890123456789012345678901234567890"),
                    })
                    .vault_owner(Address::ZERO)
                    .deployment_block(1)
                    .call(),
            ),
            order_polling_interval_secs: 15,
            order_polling_max_jitter_secs: 5,
            position_check_interval_secs: 60,
            inventory_poll_interval_secs: 60,
            inventory_divergence_threshold: std::num::NonZeroU32::MIN,
            hedge_order_gate_reconciliation_timeout_secs: std::num::NonZeroU64::MIN,
            extended_hours_reprice_timeout_secs: std::num::NonZeroU64::new(300),
            close_flatten_reprice_timeout_secs: 60,
            extended_hours_close_flatten_window_secs: 900,
            close_flatten_cross_max_bps: 400,
            apalis_finished_job_cleanup_interval_secs: 3600,
            broker,
            telemetry: None,
            alerts: None,
            startup_notices: Vec::new(),
            pricing: None,
            rebalancing: st0x_config::default_test_rebalancing_ctx(),
            order_owner: Address::ZERO,
            wallet: None,
            wallet_meta: None,
            execution_threshold: ExecutionThreshold::whole_share(),
            assets: HedgingAssets::default(),
            travel_rule: None,
            rest_api: None,
            ops_api: None,
            issuance: create_test_issuance_ctx(),
            bot_gas_valuation: None,
            orchestrator: None,
        }
    }

    #[tokio::test]
    async fn dividend_bump_rejects_robinhood_before_any_operation() {
        let ctx = test_ctx(st0x_config::test_alpaca_broker_ctx());
        let operations = CountingDividendBumpOperations::default();
        let mut stdout = Vec::new();

        let error = dividend_bump_with_operations(
            &mut stdout,
            Symbol::new("DNUT").unwrap(),
            positive_shares("1"),
            TokenizationNetwork::Robinhood,
            unused_bot_config(),
            &ctx,
            &operations,
        )
        .await
        .unwrap_err();

        assert_eq!(
            error.to_string(),
            "Robinhood Chain does not support automated equity transfers or donations"
        );
        assert_eq!(operations.buys.load(Ordering::Relaxed), 0);
        assert_eq!(operations.tokenizations.load(Ordering::Relaxed), 0);
        assert_eq!(operations.donations.load(Ordering::Relaxed), 0);
        assert!(stdout.is_empty());
    }

    /// The bump must run buy -> tokenize -> donate in order and stop at the first
    /// failing step. The mock broker fills the buy, but tokenization fails
    /// because the symbol is not configured, so the donate step must never
    /// run and the error must propagate to the caller.
    #[tokio::test]
    async fn dividend_bump_stops_after_buy_when_tokenize_fails() {
        let broker_mock = AlpacaBrokerMock::start()
            .symbol_fill_prices(vec![(
                Symbol::new("COIN").unwrap(),
                Float::parse("100".to_string()).unwrap(),
            )])
            .symbol_positions(vec![])
            .call()
            .await;
        let ctx = test_ctx(mock_alpaca_broker_ctx(broker_mock.base_url()));
        let bot = MockServer::start_async().await;
        let notice = bot
            .mock_async(|when, then| {
                when.method(POST).path(DIVIDEND_NAV_BUMP_NOTICE_PATH);
                then.status(204);
            })
            .await;
        let mut bot_config = NamedTempFile::new().unwrap();
        writeln!(bot_config, "server_port = {}", bot.port()).unwrap();
        let mut stdout = Vec::new();

        let error = dividend_bump_command(
            &mut stdout,
            Symbol::new("COIN").unwrap(),
            positive_shares("10"),
            TokenizationNetwork::Base,
            bot_config.path(),
            &ctx,
        )
        .await
        .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("equity COIN is not configured in [chains.base.trading.assets.equities]"),
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
        notice.assert_calls_async(0).await;
    }

    #[tokio::test]
    async fn dividend_bump_refuses_to_buy_when_notifier_setup_fails() {
        let ctx = test_ctx(st0x_config::test_alpaca_broker_ctx());
        let operations = RecordingDividendBumpOperations {
            filled_quantity: positive_shares("1"),
            notifier_setup_fails: true,
            buy_calls: AtomicUsize::new(0),
            tokenized: Mutex::new(Vec::new()),
            donated: Mutex::new(Vec::new()),
        };
        let mut stdout = Vec::new();

        let error = dividend_bump_with_operations(
            &mut stdout,
            Symbol::new("AAPL").unwrap(),
            positive_shares("1"),
            TokenizationNetwork::Base,
            unused_bot_config(),
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
        // The recording operations never touch the broker, so the plain
        // mode-less Alpaca fixture suffices (no mock server needed).
        let ctx = test_ctx(st0x_config::test_alpaca_broker_ctx());
        let operations = RecordingDividendBumpOperations {
            filled_quantity: positive_shares("0.0041"),
            notifier_setup_fails: false,
            buy_calls: AtomicUsize::new(0),
            tokenized: Mutex::new(Vec::new()),
            donated: Mutex::new(Vec::new()),
        };
        let mut stdout = Vec::new();

        dividend_bump_with_operations(
            &mut stdout,
            Symbol::new("AAPL").unwrap(),
            positive_shares("0.004115451077565126"),
            TokenizationNetwork::Base,
            unused_bot_config(),
            &ctx,
            &operations,
        )
        .await
        .unwrap();

        assert_eq!(
            *operations.tokenized.lock().unwrap(),
            vec![(positive_shares("0.0041"), TokenizationNetwork::Base)]
        );
        assert_eq!(
            *operations.donated.lock().unwrap(),
            vec![(positive_shares("0.0041"), TokenizationNetwork::Base)]
        );
        let output = String::from_utf8(stdout).unwrap();
        assert!(output.contains("Step 2/3: tokenizing 0.0041 AAPL onchain"));
        assert!(output.contains("Step 3/3: donating 0.0041 AAPL into the wrapper"));
    }

    #[tokio::test]
    async fn dividend_bump_succeeds_when_stdout_closes_after_donation() {
        let ctx = test_ctx(st0x_config::test_alpaca_broker_ctx());
        let operations = CountingDividendBumpOperations::default();
        let mut stdout = CloseOnCompletionWriter::default();

        dividend_bump_with_operations(
            &mut stdout,
            Symbol::new("AAPL").unwrap(),
            positive_shares("1"),
            TokenizationNetwork::Base,
            unused_bot_config(),
            &ctx,
            &operations,
        )
        .await
        .unwrap();

        assert!(stdout.closed);
        assert_eq!(operations.buys.load(Ordering::Relaxed), 1);
        assert_eq!(operations.tokenizations.load(Ordering::Relaxed), 1);
        assert_eq!(operations.donations.load(Ordering::Relaxed), 1);
    }

    /// The tokenize and donate steps must land on the same chain the bump
    /// was asked for: tokens minted on one chain cannot be donated on another.
    #[tokio::test]
    async fn dividend_bump_tokenizes_and_donates_on_the_selected_network() {
        let ctx = test_ctx(st0x_config::test_alpaca_broker_ctx());
        let operations = RecordingDividendBumpOperations {
            filled_quantity: positive_shares("2"),
            notifier_setup_fails: false,
            buy_calls: AtomicUsize::new(0),
            tokenized: Mutex::new(Vec::new()),
            donated: Mutex::new(Vec::new()),
        };
        let mut stdout = Vec::new();

        dividend_bump_with_operations(
            &mut stdout,
            Symbol::new("AAPL").unwrap(),
            positive_shares("2"),
            TokenizationNetwork::Ethereum,
            unused_bot_config(),
            &ctx,
            &operations,
        )
        .await
        .unwrap();

        assert_eq!(
            *operations.tokenized.lock().unwrap(),
            vec![(positive_shares("2"), TokenizationNetwork::Ethereum)]
        );
        assert_eq!(
            *operations.donated.lock().unwrap(),
            vec![(positive_shares("2"), TokenizationNetwork::Ethereum)]
        );
        let output = String::from_utf8(stdout).unwrap();
        assert!(
            output.contains("on ethereum"),
            "the bump must name its chain, got: {output}"
        );
    }
}

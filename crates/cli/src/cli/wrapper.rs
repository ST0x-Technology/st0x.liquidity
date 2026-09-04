//! CLI commands for ERC-4626 wrapping and unwrapping operations.

use alloy::primitives::Address;
use alloy::providers::RootProvider;
use std::collections::HashMap;
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;

use st0x_config::Ctx;
use st0x_evm::Wallet;
use st0x_execution::{FractionalShares, Positive, Symbol};
use st0x_hedge::operator::rebalancing::to_wrapped_equities;
use st0x_wrapper::{WrappedEquity, Wrapper, WrapperService};

use super::TokenizationNetwork;
use super::token_list::load_wrapped_equities;

pub(super) async fn wrap_equity_command<Writer: Write>(
    stdout: &mut Writer,
    symbol: Symbol,
    quantity: Positive<FractionalShares>,
    network: TokenizationNetwork,
    registry: Option<PathBuf>,
    ctx: &Ctx,
) -> anyhow::Result<()> {
    let WrapContext { wallet, equities } = wrap_context(ctx, network, registry.as_ref(), &symbol)?;
    let owner = wallet.address();
    let wrapper = WrapperService::new(wallet, equities);

    wrap_equity_with_wrapper(stdout, &wrapper, owner, symbol, quantity).await
}

async fn wrap_equity_with_wrapper<Writer: Write, WrapperImpl: Wrapper + ?Sized>(
    stdout: &mut Writer,
    wrapper: &WrapperImpl,
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
    network: TokenizationNetwork,
    registry: Option<PathBuf>,
    ctx: &Ctx,
) -> anyhow::Result<()> {
    let WrapContext { wallet, equities } = wrap_context(ctx, network, registry.as_ref(), &symbol)?;
    let owner = wallet.address();
    let wrapper = WrapperService::new(wallet, equities);

    unwrap_equity_with_wrapper(stdout, &wrapper, owner, symbol, quantity).await
}

async fn unwrap_equity_with_wrapper<Writer: Write, WrapperImpl: Wrapper + ?Sized>(
    stdout: &mut Writer,
    wrapper: &WrapperImpl,
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

pub(super) async fn donate_equity_command<Writer: Write>(
    stdout: &mut Writer,
    symbol: Symbol,
    quantity: Positive<FractionalShares>,
    ctx: &Ctx,
) -> anyhow::Result<()> {
    let wallet_ctx = ctx.wallet()?;
    let base_wallet = wallet_ctx.base_wallet().clone();
    let owner = base_wallet.address();
    let wrapper = WrapperService::new(
        base_wallet,
        to_wrapped_equities(&ctx.chains.primary().assets.equities.symbols),
    );

    donate_equity_with_wrapper(stdout, &wrapper, owner, symbol, quantity).await
}

async fn donate_equity_with_wrapper<Writer: Write, WrapperImpl: Wrapper + ?Sized>(
    stdout: &mut Writer,
    wrapper: &WrapperImpl,
    owner: Address,
    symbol: Symbol,
    quantity: Positive<FractionalShares>,
) -> anyhow::Result<()> {
    writeln!(
        stdout,
        "Donating tokenized equity into the wrapper to bump its NAV"
    )?;
    writeln!(stdout, "   Symbol: {symbol}")?;
    writeln!(stdout, "   Donation quantity: {quantity}")?;
    writeln!(stdout, "   Source wallet: {owner}")?;

    let wrapped_token = wrapper.lookup_derivative(&symbol)?;
    let underlying_token = wrapper.lookup_underlying(&symbol)?;

    writeln!(stdout, "   Wrapped token (NAV recipient): {wrapped_token}")?;
    writeln!(stdout, "   Underlying token: {underlying_token}")?;

    let underlying_amount = quantity.inner().to_u256_18_decimals()?;
    writeln!(
        stdout,
        "   Underlying amount (smallest unit): {underlying_amount}"
    )?;
    writeln!(
        stdout,
        "   Transferring underlying into the wrapper (no shares minted)..."
    )?;

    let donate_tx_hash = wrapper.donate(wrapped_token, underlying_amount).await?;

    writeln!(stdout, "   Transaction hash: {donate_tx_hash}")?;
    writeln!(stdout, "Donation completed successfully!")?;

    Ok(())
}

/// The wallet and symbol to address map a wrap or unwrap runs against.
struct WrapContext {
    wallet: Arc<dyn Wallet<Provider = RootProvider>>,
    equities: HashMap<Symbol, WrappedEquity>,
}

/// Resolves the wallet and the symbol to address map for a wrap or unwrap.
///
/// Base resolves from `[chains.<name>.trading.assets.equities]` exactly as before. Non Base
/// networks have no config source, so the registry token list is required
/// and a stray one on Base is rejected instead of silently ignored. The
/// resolved map must contain the requested symbol so a typo fails here with
/// the available symbols instead of deeper in the vault call.
fn wrap_context(
    ctx: &Ctx,
    network: TokenizationNetwork,
    registry: Option<&PathBuf>,
    symbol: &Symbol,
) -> anyhow::Result<WrapContext> {
    let wallet_ctx = ctx.wallet()?;
    let (wallet, _network_wire) =
        super::rebalancing::tokenization_network_context(wallet_ctx, network);

    let equities = match (network, registry) {
        (TokenizationNetwork::Base, None) => {
            to_wrapped_equities(&ctx.chains.primary().assets.equities.symbols)
        }
        (TokenizationNetwork::Base, Some(_)) => anyhow::bail!(
            "--registry only applies to non Base networks: Base resolves \
             from [chains.<name>.trading.assets.equities]"
        ),
        (TokenizationNetwork::Ethereum | TokenizationNetwork::HyperEvm, Some(path)) => {
            load_wrapped_equities(path, network.chain_id())?
        }
        (TokenizationNetwork::Ethereum | TokenizationNetwork::HyperEvm, None) => anyhow::bail!(
            "pass --registry with the st0x.registry token list for the \
             selected network (token-lists/<network>.json)"
        ),
    };

    if !equities.contains_key(symbol) {
        let mut available: Vec<String> = equities.keys().map(ToString::to_string).collect();
        available.sort();
        anyhow::bail!(
            "{symbol} is not in the resolved token set; available: [{}]",
            available.join(", ")
        );
    }

    Ok(WrapContext { wallet, equities })
}

#[cfg(test)]
mod tests {
    use alloy::primitives::Address;
    use std::io::Write as _;

    use st0x_config::ChainRegistry;
    use st0x_config::ExecutionThreshold;
    use st0x_config::HedgingAssets;
    use st0x_config::create_test_issuance_ctx;
    use st0x_config::{BrokerCtx, Ctx, LogFormat, LogLevel, TradingMode};
    use st0x_config::{InventoryMode, TradingChain};
    use st0x_execution::{FractionalShares, Positive, Symbol};
    use st0x_hedge::operator::test_utils::try_positive_shares;
    use st0x_wrapper::MockWrapper;

    use super::{
        TokenizationNetwork, donate_equity_command, donate_equity_with_wrapper,
        unwrap_equity_command, unwrap_equity_with_wrapper, wrap_equity_command,
        wrap_equity_with_wrapper,
    };

    fn positive_shares(value: &str) -> Positive<FractionalShares> {
        try_positive_shares(value).expect("test shares must be valid and positive")
    }

    fn create_ctx_without_rebalancing() -> Ctx {
        Ctx {
            database_url: ":memory:".to_string(),
            log_level: LogLevel::Debug,
            log_dir: None,
            log_format: LogFormat::Text,
            log_query_url_template: None,
            server_port: 8080,
            board_port: 8081,
            chains: ChainRegistry::single_trading_chain(
                TradingChain::test()
                    .orderbook(Address::random())
                    .inventory(InventoryMode::Managed {
                        inventory: Address::random(),
                    })
                    .vault_owner(Address::ZERO)
                    .deployment_block(1)
                    .call(),
            ),
            order_polling_interval: 15,
            order_polling_max_jitter: 5,
            position_check_interval: 60,
            inventory_poll_interval: 60,
            inventory_divergence_threshold: std::num::NonZeroU32::MIN,
            hedge_order_gate_reconciliation_timeout_secs: std::num::NonZeroU64::MIN,
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

    fn create_ctx_with_stub_wallet() -> Ctx {
        let mut ctx = create_ctx_without_rebalancing();
        ctx.wallet = Some(st0x_config::OnchainWalletCtx::stub());
        ctx
    }

    #[tokio::test]
    async fn wrap_on_base_rejects_a_registry() {
        let ctx = create_ctx_with_stub_wallet();
        let mut stdout = Vec::new();

        let error = wrap_equity_command(
            &mut stdout,
            Symbol::new("AAPL").unwrap(),
            positive_shares("1"),
            TokenizationNetwork::Base,
            Some(std::path::PathBuf::from("token-lists/base.json")),
            &ctx,
        )
        .await
        .unwrap_err();

        assert!(
            error.to_string().contains("only applies to non Base"),
            "expected registry rejection, got: {error}"
        );
    }

    #[tokio::test]
    async fn wrap_on_ethereum_requires_a_registry() {
        let ctx = create_ctx_with_stub_wallet();
        let mut stdout = Vec::new();

        let error = wrap_equity_command(
            &mut stdout,
            Symbol::new("RKLB").unwrap(),
            positive_shares("0.1"),
            TokenizationNetwork::Ethereum,
            None,
            &ctx,
        )
        .await
        .unwrap_err();

        assert!(
            error.to_string().contains("pass --registry"),
            "expected missing registry error, got: {error}"
        );
    }

    #[tokio::test]
    async fn wrap_rejects_a_symbol_missing_from_the_resolved_set() {
        let mut registry = tempfile::NamedTempFile::new().unwrap();
        registry
            .write_all(
                br#"{
                    "tokens": [{
                        "chainId": 1,
                        "address": "0xF4f8c66085910d583c01f3b4e44Bf731D4e2c565",
                        "symbol": "wtRKLB",
                        "extensions": {
                            "unwrappedAddress": "0xED0c085d92C262FB46937CB0B3C9763Af7fCCf30"
                        }
                    }]
                }"#,
            )
            .unwrap();

        let ctx = create_ctx_with_stub_wallet();
        let mut stdout = Vec::new();

        let error = wrap_equity_command(
            &mut stdout,
            Symbol::new("AAPL").unwrap(),
            positive_shares("1"),
            TokenizationNetwork::Ethereum,
            Some(registry.path().to_path_buf()),
            &ctx,
        )
        .await
        .unwrap_err();

        assert!(
            error.to_string().contains("not in the resolved token set"),
            "expected missing symbol error, got: {error}"
        );
        assert!(
            error.to_string().contains("RKLB"),
            "error must list available symbols, got: {error}"
        );
    }

    /// A valid registry resolves through wrap_context into the wrapper: the
    /// command selects the ethereum stub wallet and prints the registry's
    /// wrapped and underlying addresses before the chain call fails on the
    /// stub provider.
    #[tokio::test]
    async fn unwrap_on_ethereum_resolves_addresses_from_the_registry() {
        let mut registry = tempfile::NamedTempFile::new().unwrap();
        registry
            .write_all(
                br#"{
                    "tokens": [{
                        "chainId": 1,
                        "address": "0x8FC87Be766C0cB6f254F1FDc9351D4B85B560FB3",
                        "symbol": "wtRKLB",
                        "extensions": {
                            "unwrappedAddress": "0xED0c085d92C262FB46937CB0B3C9763Af7fCCf30"
                        }
                    }]
                }"#,
            )
            .unwrap();

        let ctx = create_ctx_with_stub_wallet();
        let mut stdout = Vec::new();

        let error = unwrap_equity_command(
            &mut stdout,
            Symbol::new("RKLB").unwrap(),
            positive_shares("0.1"),
            TokenizationNetwork::Ethereum,
            Some(registry.path().to_path_buf()),
            &ctx,
        )
        .await
        .unwrap_err();

        let output = String::from_utf8(stdout).unwrap();
        assert!(
            output.contains("Wrapped token: 0x8FC87Be766C0cB6f254F1FDc9351D4B85B560FB3"),
            "derivative must come from the registry, got: {output}"
        );
        assert!(
            output.contains("Underlying token: 0xED0c085d92C262FB46937CB0B3C9763Af7fCCf30"),
            "underlying must come from the registry, got: {output}"
        );
        assert!(
            output.contains("Liquidity wallet: 0x0000000000000000000000000000000000000E78"),
            "the ethereum marker wallet must be selected, got: {output}"
        );
        assert!(
            !error.to_string().contains("not in the resolved token set")
                && !error.to_string().contains("pass --registry"),
            "failure must be past resolution, got: {error}"
        );
    }

    #[tokio::test]
    async fn wrap_equity_requires_wallet_config() {
        let ctx = create_ctx_without_rebalancing();
        let mut stdout = Vec::new();

        let error = wrap_equity_command(
            &mut stdout,
            Symbol::new("AAPL").unwrap(),
            positive_shares("10.5"),
            TokenizationNetwork::Base,
            None,
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
            TokenizationNetwork::Base,
            None,
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

    #[tokio::test]
    async fn donate_equity_requires_wallet_config() {
        let ctx = create_ctx_without_rebalancing();
        let mut stdout = Vec::new();

        let error = donate_equity_command(
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
    async fn donate_equity_success_prints_transaction_details() {
        let wrapped_token = Address::repeat_byte(0x22);
        let underlying_token = Address::repeat_byte(0x11);
        let wrapper = MockWrapper::new()
            .with_wrapped_token(wrapped_token)
            .with_tokenized_shares(underlying_token);
        let mut stdout = Vec::new();

        donate_equity_with_wrapper(
            &mut stdout,
            &wrapper,
            Address::repeat_byte(0xaa),
            Symbol::new("AAPL").unwrap(),
            positive_shares("10.5"),
        )
        .await
        .unwrap();

        let output = String::from_utf8(stdout).unwrap();
        assert!(output.contains("Donating tokenized equity into the wrapper to bump its NAV"));
        assert!(output.contains("Symbol: AAPL"));
        assert!(output.contains("Donation quantity: 10.5"));
        assert!(output.contains(&format!("Wrapped token (NAV recipient): {wrapped_token}")));
        assert!(output.contains(&format!("Underlying token: {underlying_token}")));
        assert!(output.contains("no shares minted"));
        assert!(output.contains("Transaction hash:"));
        assert!(output.contains("Donation completed successfully"));
    }

    #[tokio::test]
    async fn donate_equity_propagates_symbol_lookup_failure() {
        let wrapper = MockWrapper::failing_derivative_lookup();
        let mut stdout = Vec::new();

        let error = donate_equity_with_wrapper(
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
    async fn donate_equity_propagates_transfer_failure() {
        let wrapper = MockWrapper::failing_donate();
        let mut stdout = Vec::new();

        let error = donate_equity_with_wrapper(
            &mut stdout,
            &wrapper,
            Address::repeat_byte(0xaa),
            Symbol::new("AAPL").unwrap(),
            positive_shares("10.5"),
        )
        .await
        .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("wrapper donation transfer failed"),
            "expected donate transfer error, got: {error}"
        );
    }
}

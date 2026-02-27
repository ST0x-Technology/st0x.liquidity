//! Rebalancing test helpers.
//!
//! Provides `build_rebalancing_ctx`, `build_usdc_rebalancing_ctx`, and the
//! assertion helpers (`assert_equity_rebalancing_flow`,
//! `assert_usdc_rebalancing_flow`) used by the rebalancing e2e tests.

use std::collections::HashMap;
use std::sync::Arc;
pub(crate) use std::time::Duration;

use alloy::network::EthereumWallet;
use alloy::primitives::{Address, B256};
pub(crate) use alloy::primitives::{U256, utils::parse_units};
pub(crate) use alloy::providers::Provider;
use alloy::providers::fillers::{
    BlobGasFiller, ChainIdFiller, FillProvider, GasFiller, JoinFill, NonceFiller, WalletFiller,
};
use alloy::providers::{Identity, ProviderBuilder, RootProvider};
use alloy::rpc::client::RpcClient;
use alloy::rpc::types::{TransactionReceipt, TransactionRequest};
use alloy::signers::local::PrivateKeySigner;
use async_trait::async_trait;
use rain_math_float::Float;
pub(crate) use st0x_exact_decimal::ExactDecimal;

pub(crate) fn ed(value: &str) -> ExactDecimal {
    match ExactDecimal::parse(value) {
        Ok(val) => val,
        Err(error) => panic!("ed({value:?}) failed: {error}"),
    }
}
use sqlx::SqlitePool;

use st0x_event_sorcery::Store;
use st0x_evm::{Evm, EvmError, Wallet};
use st0x_execution::{
    AlpacaAccountId, AlpacaBrokerApiCtx, AlpacaBrokerApiMode, Symbol, TimeInForce,
};
pub(crate) use st0x_hedge::UsdcRebalancing;
use st0x_hedge::bindings::IOrderBookV6;
use st0x_hedge::config::{BrokerCtx, Ctx, LogLevel, OperationalLimits};
use st0x_hedge::{EquityTokenAddresses, EvmCtx, ImbalanceThreshold, TradingMode};

pub(crate) use e2e_tests::common::ExpectedPosition;
use e2e_tests::common::{
    assert_broker_state, assert_cqrs_state, assert_event_subsequence,
    assert_single_clean_aggregate, connect_db, fetch_events_by_type,
};
pub(crate) use e2e_tests::common::{poll_for_events_with_timeout, spawn_bot};
pub(crate) use e2e_tests::services::TestInfra;
use e2e_tests::services::alpaca_broker::{self, AlpacaBrokerMock};
use e2e_tests::services::alpaca_tokenization::AlpacaTokenizationMock;
pub(crate) use e2e_tests::services::alpaca_tokenization::REDEMPTION_WALLET;
pub(crate) use e2e_tests::services::base_chain::TakeDirection;
use e2e_tests::services::base_chain::{self, TakeOrderResult};
use e2e_tests::services::cctp::CctpOverrides;
use e2e_tests::services::cctp::attestation::CctpAttestationMock;
pub(crate) use e2e_tests::services::cctp::{CctpInfra, USDC_ETHEREUM};

/// Local signing wallet for rebalancing e2e tests that exposes
/// `Provider = RootProvider`.
type SigningProvider = FillProvider<
    JoinFill<
        JoinFill<
            Identity,
            JoinFill<GasFiller, JoinFill<BlobGasFiller, JoinFill<NonceFiller, ChainIdFiller>>>,
        >,
        WalletFiller<EthereumWallet>,
    >,
    RootProvider,
    alloy::network::Ethereum,
>;

struct TestWallet {
    address: Address,
    read_provider: RootProvider,
    signing_provider: SigningProvider,
    required_confirmations: u64,
}

impl TestWallet {
    fn new(
        private_key: &B256,
        rpc_url: url::Url,
        required_confirmations: u64,
    ) -> anyhow::Result<Self> {
        let signer = PrivateKeySigner::from_bytes(private_key)?;
        let address = signer.address();
        let eth_wallet = EthereumWallet::from(signer);

        let read_provider = RootProvider::new(RpcClient::builder().http(rpc_url.clone()));
        let signing_provider = ProviderBuilder::new()
            .wallet(eth_wallet)
            .connect_http(rpc_url);

        Ok(Self {
            address,
            read_provider,
            signing_provider,
            required_confirmations,
        })
    }
}

#[async_trait]
impl Evm for TestWallet {
    type Provider = RootProvider;

    fn provider(&self) -> &RootProvider {
        &self.read_provider
    }
}

#[async_trait]
impl Wallet for TestWallet {
    fn address(&self) -> Address {
        self.address
    }

    async fn send(
        &self,
        contract: Address,
        calldata: alloy::primitives::Bytes,
        note: &str,
    ) -> Result<TransactionReceipt, EvmError> {
        tracing::info!(%contract, note, "Submitting local test wallet call");

        let tx = TransactionRequest::default()
            .to(contract)
            .input(calldata.into());

        let pending = self.signing_provider.send_transaction(tx).await?;
        let receipt = pending
            .with_required_confirmations(self.required_confirmations)
            .get_receipt()
            .await?;

        Ok(receipt)
    }
}

/// Builds a `Ctx` with rebalancing enabled.
pub(crate) fn build_rebalancing_ctx<P: Provider + Clone>(
    chain: &base_chain::BaseChain<P>,
    broker: &AlpacaBrokerMock,
    db_path: &std::path::Path,
    deployment_block: u64,
    equity_tokens: &[(String, Address, Address)],
    usdc_rebalancing: UsdcRebalancing,
    redemption_wallet: Address,
) -> anyhow::Result<Ctx> {
    let alpaca_auth = AlpacaBrokerApiCtx {
        api_key: alpaca_broker::TEST_API_KEY.to_owned(),
        api_secret: alpaca_broker::TEST_API_SECRET.to_owned(),
        account_id: AlpacaAccountId::new(uuid::uuid!("904837e3-3b76-47ec-b432-046db621571b")),
        mode: Some(AlpacaBrokerApiMode::Mock(broker.base_url())),
        asset_cache_ttl: Duration::from_secs(3600),
        time_in_force: TimeInForce::Day,
    };
    let broker_ctx = BrokerCtx::AlpacaBrokerApi(alpaca_auth.clone());
    let execution_threshold = broker_ctx.execution_threshold()?;

    let equities: HashMap<Symbol, EquityTokenAddresses> = equity_tokens
        .iter()
        .map(|&(ref symbol, wrapped, unwrapped)| {
            Ok((
                Symbol::new(symbol)?,
                EquityTokenAddresses {
                    wrapped,
                    unwrapped,
                    enabled: true,
                },
            ))
        })
        .collect::<anyhow::Result<_>>()?;

    let wallet: Arc<dyn st0x_evm::Wallet<Provider = RootProvider>> = Arc::new(TestWallet::new(
        &chain.owner_key,
        chain.endpoint().parse()?,
        1,
    )?);

    let rebalancing_ctx = st0x_hedge::RebalancingCtx::with_wallets()
        .equity(ImbalanceThreshold::new(ed("0.5"), ed("0.1"))?)
        .usdc(usdc_rebalancing)
        .redemption_wallet(redemption_wallet)
        .usdc_vault_id(B256::random())
        .alpaca_broker_auth(alpaca_auth)
        .base_wallet(wallet.clone())
        .ethereum_wallet(wallet)
        .call();

    Ok(Ctx {
        database_url: db_path.display().to_string(),
        log_level: LogLevel::Debug,
        server_port: 0,
        operational_limits: OperationalLimits::Disabled,
        evm: EvmCtx {
            ws_rpc_url: chain.ws_endpoint()?,
            orderbook: chain.orderbook_addr,
            deployment_block,
        },
        order_polling_interval: 1,
        order_polling_max_jitter: 0,
        position_check_interval: 2,
        inventory_poll_interval: 2,
        broker: broker_ctx,
        telemetry: None,
        trading_mode: TradingMode::Rebalancing(Box::new(rebalancing_ctx)),
        execution_threshold,
        equities,
    })
}

/// Builds a `Ctx` with USDC rebalancing enabled and both chain endpoints.
#[bon::builder]
pub(crate) fn build_usdc_rebalancing_ctx<BP>(
    base_chain: &base_chain::BaseChain<BP>,
    ethereum_endpoint: &str,
    broker: &AlpacaBrokerMock,
    db_path: &std::path::Path,
    deployment_block: u64,
    equity_tokens: &[(String, Address, Address)],
    usdc_vault_id: B256,
    cctp: CctpOverrides,
) -> anyhow::Result<Ctx>
where
    BP: Provider + Clone,
{
    let alpaca_auth = AlpacaBrokerApiCtx {
        api_key: alpaca_broker::TEST_API_KEY.to_owned(),
        api_secret: alpaca_broker::TEST_API_SECRET.to_owned(),
        account_id: AlpacaAccountId::new(uuid::uuid!("904837e3-3b76-47ec-b432-046db621571b")),
        mode: Some(AlpacaBrokerApiMode::Mock(broker.base_url())),
        asset_cache_ttl: Duration::from_secs(3600),
        time_in_force: TimeInForce::Day,
    };
    let broker_ctx = BrokerCtx::AlpacaBrokerApi(alpaca_auth.clone());
    let execution_threshold = broker_ctx.execution_threshold()?;

    let equities: HashMap<Symbol, EquityTokenAddresses> = equity_tokens
        .iter()
        .map(|(symbol, wrapped, unwrapped)| {
            Ok((
                Symbol::new(symbol)?,
                EquityTokenAddresses {
                    wrapped: *wrapped,
                    unwrapped: *unwrapped,
                    enabled: true,
                },
            ))
        })
        .collect::<anyhow::Result<_>>()?;

    let base_wallet: Arc<dyn st0x_evm::Wallet<Provider = RootProvider>> = Arc::new(
        TestWallet::new(&base_chain.owner_key, base_chain.endpoint().parse()?, 1)?,
    );

    let ethereum_wallet: Arc<dyn st0x_evm::Wallet<Provider = RootProvider>> = Arc::new(
        TestWallet::new(&base_chain.owner_key, ethereum_endpoint.parse()?, 1)?,
    );

    let rebalancing_ctx = st0x_hedge::RebalancingCtx::with_wallets()
        .equity(ImbalanceThreshold::new(ed("0.5"), ed("100"))?)
        .usdc(UsdcRebalancing::Enabled {
            target: ed("0.5"),
            deviation: ed("0.1"),
        })
        .redemption_wallet(Address::random())
        .usdc_vault_id(usdc_vault_id)
        .alpaca_broker_auth(alpaca_auth)
        .base_wallet(base_wallet)
        .ethereum_wallet(ethereum_wallet)
        .call()
        .with_circle_api_base(cctp.attestation_base_url)
        .with_cctp_addresses(cctp.token_messenger, cctp.message_transmitter);

    Ok(Ctx {
        database_url: db_path.display().to_string(),
        log_level: LogLevel::Debug,
        server_port: 0,
        operational_limits: OperationalLimits::Disabled,
        evm: EvmCtx {
            ws_rpc_url: base_chain.ws_endpoint()?,
            orderbook: base_chain.orderbook_addr,
            deployment_block,
        },
        order_polling_interval: 1,
        order_polling_max_jitter: 0,
        position_check_interval: 2,
        inventory_poll_interval: 15,
        broker: broker_ctx,
        telemetry: None,
        trading_mode: TradingMode::Rebalancing(Box::new(rebalancing_ctx)),
        execution_threshold,
        equities,
    })
}

pub(crate) enum EquityRebalanceType<'a> {
    Mint {
        symbol: &'a str,
        tokenization: &'a AlpacaTokenizationMock,
    },
    Redeem {
        symbol: &'a str,
        tokenization: &'a AlpacaTokenizationMock,
        redemption_wallet_balance_before: U256,
        redemption_wallet_balance_after: U256,
    },
}

async fn assert_inventory_snapshots(
    pool: &SqlitePool,
    orderbook_addr: Address,
    owner: Address,
    expected_symbols: &[&str],
    assert_cash: bool,
) -> anyhow::Result<()> {
    let events = fetch_events_by_type(pool, "InventorySnapshot").await?;
    assert!(
        !events.is_empty(),
        "Expected InventorySnapshot events, got 0"
    );

    let expected_aggregate_id = format!(
        "{}:{}",
        orderbook_addr.to_checksum(None),
        owner.to_checksum(None),
    );
    for event in &events {
        assert_eq!(
            event.aggregate_id, expected_aggregate_id,
            "InventorySnapshot aggregate_id mismatch: expected {expected_aggregate_id}, got {}",
            event.aggregate_id
        );
    }

    let event_types: Vec<&str> = events.iter().map(|ev| ev.event_type.as_str()).collect();

    assert!(
        event_types.contains(&"InventorySnapshotEvent::OnchainEquity"),
        "Missing OnchainEquity event, got types: {event_types:?}"
    );
    assert!(
        event_types.contains(&"InventorySnapshotEvent::OffchainEquity"),
        "Missing OffchainEquity event, got types: {event_types:?}"
    );

    if assert_cash {
        assert!(
            event_types.contains(&"InventorySnapshotEvent::OnchainCash"),
            "Missing OnchainCash event, got types: {event_types:?}"
        );
        assert!(
            event_types.contains(&"InventorySnapshotEvent::OffchainCash"),
            "Missing OffchainCash event, got types: {event_types:?}"
        );
    }

    let last_onchain_equity = events
        .iter()
        .rev()
        .find(|ev| ev.event_type == "InventorySnapshotEvent::OnchainEquity")
        .ok_or_else(|| anyhow::anyhow!("Missing OnchainEquity event"))?;
    let onchain_balances = last_onchain_equity
        .payload
        .get("OnchainEquity")
        .and_then(|val| val.get("balances"))
        .ok_or_else(|| anyhow::anyhow!("OnchainEquity payload missing balances"))?;

    for symbol in expected_symbols {
        let balance_str = onchain_balances
            .get(*symbol)
            .and_then(|val| val.as_str())
            .unwrap_or_else(|| {
                panic!("OnchainEquity missing symbol {symbol}, got: {onchain_balances}")
            });
        let _parsed_balance: ExactDecimal = balance_str
            .parse()
            .unwrap_or_else(|err| panic!("Failed to parse onchain balance for {symbol}: {err}"));
    }

    let last_offchain_equity = events
        .iter()
        .rev()
        .find(|ev| ev.event_type == "InventorySnapshotEvent::OffchainEquity")
        .ok_or_else(|| anyhow::anyhow!("Missing OffchainEquity event"))?;
    let offchain_positions = last_offchain_equity
        .payload
        .get("OffchainEquity")
        .and_then(|val| val.get("positions"))
        .ok_or_else(|| anyhow::anyhow!("OffchainEquity payload missing positions"))?;

    for symbol in expected_symbols {
        let position_str = offchain_positions
            .get(*symbol)
            .and_then(|val| val.as_str())
            .unwrap_or_else(|| {
                panic!("OffchainEquity missing symbol {symbol}, got: {offchain_positions}")
            });
        let _position: ExactDecimal = position_str
            .parse()
            .unwrap_or_else(|err| panic!("Failed to parse offchain position for {symbol}: {err}"));
    }

    if assert_cash {
        let last_onchain_cash = events
            .iter()
            .rev()
            .find(|ev| ev.event_type == "InventorySnapshotEvent::OnchainCash")
            .ok_or_else(|| anyhow::anyhow!("Missing OnchainCash event"))?;
        let usdc_balance_str = last_onchain_cash
            .payload
            .get("OnchainCash")
            .and_then(|val| val.get("usdc_balance"))
            .and_then(|val| val.as_str())
            .ok_or_else(|| anyhow::anyhow!("OnchainCash payload missing usdc_balance"))?;
        let _usdc_balance: ExactDecimal = usdc_balance_str
            .parse()
            .unwrap_or_else(|err| panic!("Failed to parse onchain USDC balance: {err}"));
    }

    Ok(())
}

async fn assert_equity_mint_rebalancing<P: Provider>(
    pool: &SqlitePool,
    take_results: &[TakeOrderResult],
    provider: &P,
    orderbook_addr: Address,
    owner: Address,
    symbol: &str,
    tokenization: &AlpacaTokenizationMock,
) -> anyhow::Result<()> {
    let mint_events = fetch_events_by_type(pool, "TokenizedEquityMint").await?;
    assert_eq!(
        mint_events.len(),
        5,
        "Expected exactly 5 TokenizedEquityMint success events",
    );
    assert_event_subsequence(
        &mint_events,
        &[
            "TokenizedEquityMintEvent::MintRequested",
            "TokenizedEquityMintEvent::MintAccepted",
            "TokenizedEquityMintEvent::TokensReceived",
            "TokenizedEquityMintEvent::TokensWrapped",
            "TokenizedEquityMintEvent::DepositedIntoRaindex",
        ],
    );
    assert_single_clean_aggregate(&mint_events, &["Failed", "Rejected"]);

    let mint_requests: Vec<_> = tokenization
        .tokenization_requests()
        .into_iter()
        .filter(|req| req.request_type == "mint" && req.symbol == symbol)
        .collect();
    assert_eq!(
        mint_requests.len(),
        1,
        "Expected exactly 1 mint request for {symbol}, got {}",
        mint_requests.len(),
    );

    let completed_mint = &mint_requests[0];
    assert_eq!(
        completed_mint.status.as_str(),
        "completed",
        "Mint request for {symbol} should complete"
    );

    let consumed_output_vaults: HashMap<(Address, B256), B256> = take_results
        .iter()
        .map(|result| {
            (
                (result.output_token, result.output_vault_id),
                result.output_vault_balance_after_take,
            )
        })
        .collect();

    let orderbook = IOrderBookV6::IOrderBookV6Instance::new(orderbook_addr, provider);
    let mut total_refilled_wrapped_shares_delta = U256::ZERO;
    for ((token, vault_id), pre_rebalance_balance) in consumed_output_vaults {
        let post_rebalance_balance = orderbook
            .vaultBalance2(owner, token, vault_id)
            .call()
            .await?;

        let pre_rebalance_shares = Float::from_raw(pre_rebalance_balance)
            .to_fixed_decimal_lossy(18)?
            .0;
        let post_rebalance_shares = Float::from_raw(post_rebalance_balance)
            .to_fixed_decimal_lossy(18)?
            .0;
        let refilled_shares_delta = post_rebalance_shares
            .checked_sub(pre_rebalance_shares)
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Mint should not reduce consumed output vault shares (pre={}, post={})",
                    Float::from_raw(pre_rebalance_balance)
                        .format_with_scientific(false)
                        .unwrap_or_else(|_| "???".to_string()),
                    Float::from_raw(post_rebalance_balance)
                        .format_with_scientific(false)
                        .unwrap_or_else(|_| "???".to_string())
                )
            })?;
        total_refilled_wrapped_shares_delta = total_refilled_wrapped_shares_delta
            .checked_add(refilled_shares_delta)
            .ok_or_else(|| anyhow::anyhow!("Refilled wrapped share delta overflow"))?;
    }

    let tokens_wrapped_event = mint_events
        .iter()
        .find(|event| event.event_type == "TokenizedEquityMintEvent::TokensWrapped")
        .ok_or_else(|| anyhow::anyhow!("Missing TokenizedEquityMintEvent::TokensWrapped"))?;
    let wrapped_payload = tokens_wrapped_event
        .payload
        .get("TokensWrapped")
        .ok_or_else(|| anyhow::anyhow!("TokensWrapped event payload missing wrapper"))?;
    let wrapped_shares_str = wrapped_payload
        .get("wrapped_shares")
        .and_then(|value| value.as_str())
        .ok_or_else(|| anyhow::anyhow!("TokensWrapped payload missing wrapped_shares"))?;
    let wrapped_shares: U256 = wrapped_shares_str.parse().map_err(|error| {
        anyhow::anyhow!("Invalid wrapped_shares '{wrapped_shares_str}': {error}")
    })?;
    let mint_qty_units: U256 = parse_units(&completed_mint.qty, 18)?.into();
    assert_eq!(
        mint_qty_units, wrapped_shares,
        "Tokenization completed mint qty should match TokensWrapped.wrapped_shares"
    );

    assert_eq!(
        total_refilled_wrapped_shares_delta, wrapped_shares,
        "Refilled Raindex vault share delta should match TokensWrapped.wrapped_shares"
    );

    Ok(())
}

#[bon::builder]
async fn assert_equity_redeem_rebalancing<P: Provider>(
    pool: &SqlitePool,
    take_results: &[TakeOrderResult],
    provider: &P,
    orderbook_addr: Address,
    owner: Address,
    symbol: &str,
    tokenization: &AlpacaTokenizationMock,
    redemption_wallet_balance_before: U256,
    redemption_wallet_balance_after: U256,
) -> anyhow::Result<()> {
    let redeem_events = fetch_events_by_type(pool, "EquityRedemption").await?;
    assert_eq!(
        redeem_events.len(),
        5,
        "Expected exactly 5 EquityRedemption success events",
    );
    assert_event_subsequence(
        &redeem_events,
        &[
            "EquityRedemptionEvent::WithdrawnFromRaindex",
            "EquityRedemptionEvent::TokensUnwrapped",
            "EquityRedemptionEvent::TokensSent",
            "EquityRedemptionEvent::Detected",
            "EquityRedemptionEvent::Completed",
        ],
    );
    assert_single_clean_aggregate(&redeem_events, &["Failed", "Rejected"]);

    let redeem_requests: Vec<_> = tokenization
        .tokenization_requests()
        .into_iter()
        .filter(|req| req.request_type == "redeem" && req.symbol == symbol)
        .collect();
    assert_eq!(
        redeem_requests.len(),
        1,
        "Expected exactly 1 redeem request for {symbol}, got {}",
        redeem_requests.len(),
    );

    let completed_redeem = &redeem_requests[0];
    assert_eq!(
        completed_redeem.status.as_str(),
        "completed",
        "Redeem request for {symbol} should complete"
    );

    let redemption_wallet_delta = redemption_wallet_balance_after
        .checked_sub(redemption_wallet_balance_before)
        .ok_or_else(|| anyhow::anyhow!("Redemption wallet balance underflow"))?;
    let expected_wallet_delta: U256 = parse_units(&completed_redeem.qty, 18)?.into();
    assert_eq!(
        redemption_wallet_delta, expected_wallet_delta,
        "Redemption wallet delta should match completed redeem qty (18 decimals)"
    );

    // Sum TokensWrapped.wrapped_shares only for mint aggregates matching this
    // symbol. The TokensWrapped event itself doesn't carry a symbol, so we
    // resolve it from the MintRequested event in the same aggregate.
    let concurrent_mint_events = fetch_events_by_type(pool, "TokenizedEquityMint").await?;

    let mint_aggregate_symbols: HashMap<&str, &str> = concurrent_mint_events
        .iter()
        .filter(|event| event.event_type == "TokenizedEquityMintEvent::MintRequested")
        .filter_map(|event| {
            let sym = event
                .payload
                .get("MintRequested")?
                .get("symbol")?
                .as_str()?;
            Some((event.aggregate_id.as_str(), sym))
        })
        .collect();

    let concurrent_mint_wrapped_shares = concurrent_mint_events
        .iter()
        .filter(|event| event.event_type == "TokenizedEquityMintEvent::TokensWrapped")
        .filter(|event| {
            mint_aggregate_symbols
                .get(event.aggregate_id.as_str())
                .is_some_and(|sym| *sym == symbol)
        })
        .try_fold(U256::ZERO, |acc, event| {
            let wrapped_payload = event
                .payload
                .get("TokensWrapped")
                .ok_or_else(|| anyhow::anyhow!("TokensWrapped event payload missing wrapper"))?;
            let wrapped_shares_str = wrapped_payload
                .get("wrapped_shares")
                .and_then(|value| value.as_str())
                .ok_or_else(|| anyhow::anyhow!("TokensWrapped payload missing wrapped_shares"))?;
            let wrapped_shares: U256 = wrapped_shares_str.parse().map_err(|error| {
                anyhow::anyhow!("Invalid wrapped_shares '{wrapped_shares_str}': {error}")
            })?;

            acc.checked_add(wrapped_shares)
                .ok_or_else(|| anyhow::anyhow!("Concurrent mint wrapped share overflow"))
        })?;

    let redeemed_input_vaults: HashMap<(Address, B256), B256> = take_results
        .iter()
        .map(|result| {
            (
                (result.input_token, result.input_vault_id),
                result.input_vault_balance_after_take,
            )
        })
        .collect();
    let orderbook = IOrderBookV6::IOrderBookV6Instance::new(orderbook_addr, provider);
    let mut total_withdrawn_wrapped_shares = U256::ZERO;
    for ((token, vault_id), pre_rebalance_balance) in redeemed_input_vaults {
        let post_rebalance_balance = orderbook
            .vaultBalance2(owner, token, vault_id)
            .call()
            .await?;

        let pre_rebalance_shares = Float::from_raw(pre_rebalance_balance)
            .to_fixed_decimal_lossy(18)?
            .0;
        let post_rebalance_shares = Float::from_raw(post_rebalance_balance)
            .to_fixed_decimal_lossy(18)?
            .0;
        let withdrawn_shares_delta = pre_rebalance_shares
            .checked_sub(post_rebalance_shares)
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Redemption should not increase redeemed input vault shares (pre={}, post={})",
                    Float::from_raw(pre_rebalance_balance)
                        .format_with_scientific(false)
                        .unwrap_or_else(|_| "???".to_string()),
                    Float::from_raw(post_rebalance_balance)
                        .format_with_scientific(false)
                        .unwrap_or_else(|_| "???".to_string())
                )
            })?;
        total_withdrawn_wrapped_shares = total_withdrawn_wrapped_shares
            .checked_add(withdrawn_shares_delta)
            .ok_or_else(|| anyhow::anyhow!("Withdrawn wrapped share delta overflow"))?;
    }
    let expected_net_vault_drain = expected_wallet_delta
        .checked_sub(concurrent_mint_wrapped_shares)
        .ok_or_else(|| {
            anyhow::anyhow!(
                "Concurrent mint wrapped shares exceed redeemed qty: mint={concurrent_mint_wrapped_shares}, redeem={expected_wallet_delta}"
            )
        })?;
    assert_eq!(
        total_withdrawn_wrapped_shares, expected_net_vault_drain,
        "Redeemed Raindex vault net share delta should match completed redeem qty minus concurrent mint wrapped shares"
    );

    let aggregate_id = redeem_events[0].aggregate_id.clone();
    let redemption_id = aggregate_id.parse::<st0x_hedge::RedemptionAggregateId>()?;
    let redemption =
        Store::<st0x_hedge::EquityRedemption>::load_from_pool(pool.clone(), &redemption_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("Redemption aggregate should exist after events"))?;
    let redemption_json = serde_json::to_value(&redemption)?;
    let completed = redemption_json.get("Completed").ok_or_else(|| {
        anyhow::anyhow!("Redemption should be in Completed state, got: {redemption_json}")
    })?;
    assert_eq!(
        completed.get("symbol").and_then(|val| val.as_str()),
        Some(symbol),
        "Completed redemption should be for {symbol}, got: {completed}"
    );

    Ok(())
}

#[bon::builder]
pub(crate) async fn assert_equity_rebalancing_flow<P: Provider>(
    expected_positions: &[ExpectedPosition],
    take_results: &[TakeOrderResult],
    provider: &P,
    orderbook_addr: Address,
    owner: Address,
    broker: &AlpacaBrokerMock,
    db_path: &std::path::Path,
    rebalance_type: EquityRebalanceType<'_>,
) -> anyhow::Result<()> {
    let database_url = &db_path.display().to_string();

    assert_broker_state(expected_positions, broker);
    assert_cqrs_state(expected_positions, take_results.len(), database_url).await?;

    let pool = connect_db(db_path).await?;

    let equity_symbol = match &rebalance_type {
        EquityRebalanceType::Mint { symbol, .. } | EquityRebalanceType::Redeem { symbol, .. } => {
            *symbol
        }
    };
    assert_inventory_snapshots(&pool, orderbook_addr, owner, &[equity_symbol], false).await?;

    match rebalance_type {
        EquityRebalanceType::Mint {
            symbol,
            tokenization,
        } => {
            assert_equity_mint_rebalancing(
                &pool,
                take_results,
                provider,
                orderbook_addr,
                owner,
                symbol,
                tokenization,
            )
            .await?;
        }
        EquityRebalanceType::Redeem {
            symbol,
            tokenization,
            redemption_wallet_balance_before,
            redemption_wallet_balance_after,
        } => {
            assert_equity_redeem_rebalancing()
                .pool(&pool)
                .take_results(take_results)
                .provider(provider)
                .orderbook_addr(orderbook_addr)
                .owner(owner)
                .symbol(symbol)
                .tokenization(tokenization)
                .redemption_wallet_balance_before(redemption_wallet_balance_before)
                .redemption_wallet_balance_after(redemption_wallet_balance_after)
                .call()
                .await?;
        }
    }

    pool.close().await;
    Ok(())
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum UsdcRebalanceType {
    AlpacaToBase,
    BaseToAlpaca,
}

struct UsdcRebalanceExpectations<'a> {
    event_sequence: &'a [&'a str],
    direction_str: &'a str,
    expected_broker_side: &'a str,
    expected_transfer_direction: &'a str,
}

struct UsdcRebalanceEventAmounts {
    initiated_amount_units: U256,
    bridged_amount_received_units: U256,
}

fn usdc_rebalance_expectations(
    rebalance_type: UsdcRebalanceType,
) -> UsdcRebalanceExpectations<'static> {
    match rebalance_type {
        UsdcRebalanceType::AlpacaToBase => UsdcRebalanceExpectations {
            event_sequence: &[
                "UsdcRebalanceEvent::ConversionInitiated",
                "UsdcRebalanceEvent::ConversionConfirmed",
                "UsdcRebalanceEvent::Initiated",
                "UsdcRebalanceEvent::WithdrawalConfirmed",
                "UsdcRebalanceEvent::BridgingInitiated",
                "UsdcRebalanceEvent::BridgeAttestationReceived",
                "UsdcRebalanceEvent::Bridged",
                "UsdcRebalanceEvent::DepositInitiated",
                "UsdcRebalanceEvent::DepositConfirmed",
            ],
            direction_str: "AlpacaToBase",
            expected_broker_side: "buy",
            expected_transfer_direction: "OUTGOING",
        },
        UsdcRebalanceType::BaseToAlpaca => UsdcRebalanceExpectations {
            event_sequence: &[
                "UsdcRebalanceEvent::Initiated",
                "UsdcRebalanceEvent::WithdrawalConfirmed",
                "UsdcRebalanceEvent::BridgingInitiated",
                "UsdcRebalanceEvent::BridgeAttestationReceived",
                "UsdcRebalanceEvent::Bridged",
                "UsdcRebalanceEvent::DepositInitiated",
                "UsdcRebalanceEvent::DepositConfirmed",
                "UsdcRebalanceEvent::ConversionInitiated",
                "UsdcRebalanceEvent::ConversionConfirmed",
            ],
            direction_str: "BaseToAlpaca",
            expected_broker_side: "sell",
            expected_transfer_direction: "INCOMING",
        },
    }
}

async fn assert_usdc_rebalancing_db_state(
    expected_positions: &[ExpectedPosition],
    pool: &SqlitePool,
    orderbook_addr: Address,
    owner: Address,
    expectations: &UsdcRebalanceExpectations<'_>,
) -> anyhow::Result<UsdcRebalanceEventAmounts> {
    let equity_symbols: Vec<&str> = expected_positions.iter().map(|pos| pos.symbol).collect();
    assert_inventory_snapshots(pool, orderbook_addr, owner, &equity_symbols, true).await?;

    let usdc_events = fetch_events_by_type(pool, "UsdcRebalance").await?;
    assert_eq!(
        usdc_events.len(),
        expectations.event_sequence.len(),
        "Expected exact USDC rebalance success event count",
    );

    assert_event_subsequence(&usdc_events, expectations.event_sequence);
    assert_single_clean_aggregate(&usdc_events, &["Failed"]);

    let initiated_event = usdc_events
        .iter()
        .find(|event| event.event_type == "UsdcRebalanceEvent::Initiated")
        .ok_or_else(|| anyhow::anyhow!("Missing Initiated event"))?;
    let initiated_payload = initiated_event
        .payload
        .get("Initiated")
        .ok_or_else(|| anyhow::anyhow!("Initiated event payload missing Initiated wrapper"))?;
    assert_eq!(
        initiated_payload
            .get("direction")
            .and_then(|val| val.as_str()),
        Some(expectations.direction_str),
        "Initiated event should record {} direction, got: {initiated_payload}",
        expectations.direction_str
    );
    let initiated_amount_str = initiated_payload
        .get("amount")
        .and_then(|val| val.as_str())
        .ok_or_else(|| anyhow::anyhow!("Initiated event missing amount"))?;

    let bridged_event = usdc_events
        .iter()
        .find(|event| event.event_type == "UsdcRebalanceEvent::Bridged")
        .ok_or_else(|| anyhow::anyhow!("Missing Bridged event"))?;
    let bridged_payload = bridged_event
        .payload
        .get("Bridged")
        .ok_or_else(|| anyhow::anyhow!("Bridged event payload missing Bridged wrapper"))?;
    let amount_received = bridged_payload
        .get("amount_received")
        .and_then(|val| val.as_str())
        .ok_or_else(|| anyhow::anyhow!("Bridged event missing amount_received"))?;

    Ok(UsdcRebalanceEventAmounts {
        initiated_amount_units: parse_units(initiated_amount_str, 6)?.into(),
        bridged_amount_received_units: parse_units(amount_received, 6)?.into(),
    })
}

fn assert_usdc_rebalancing_broker_state(
    broker: &AlpacaBrokerMock,
    rebalance_type: UsdcRebalanceType,
    expectations: &UsdcRebalanceExpectations<'_>,
    event_amounts: &UsdcRebalanceEventAmounts,
) -> anyhow::Result<()> {
    let usdcusd_orders: Vec<_> = broker
        .orders()
        .into_iter()
        .filter(|order| order.symbol == "USDCUSD")
        .collect();
    assert_eq!(
        usdcusd_orders.len(),
        1,
        "Expected exactly one USDCUSD conversion order, got {}",
        usdcusd_orders.len(),
    );
    let matched_order = &usdcusd_orders[0];
    assert_eq!(
        matched_order.side, expectations.expected_broker_side,
        "Unexpected USDCUSD order side"
    );
    let _parsed_order_qty: ExactDecimal = matched_order.qty.parse().unwrap_or_else(|err| {
        panic!(
            "Failed to parse USDCUSD order qty '{}': {err}",
            matched_order.qty
        )
    });

    let transfers: Vec<_> = broker
        .wallet_transfers()
        .into_iter()
        .filter(|transfer| transfer.direction == expectations.expected_transfer_direction)
        .collect();
    assert_eq!(
        transfers.len(),
        1,
        "Expected exactly one {} wallet transfer, got {}",
        expectations.expected_transfer_direction,
        transfers.len(),
    );
    let transfer = &transfers[0];
    assert_eq!(
        transfer.status, "COMPLETE",
        "{} transfer {} should be COMPLETE, got {}",
        expectations.expected_transfer_direction, transfer.transfer_id, transfer.status
    );

    let expected_rebalance_usdc_units = match rebalance_type {
        UsdcRebalanceType::AlpacaToBase => event_amounts.initiated_amount_units,
        UsdcRebalanceType::BaseToAlpaca => event_amounts.bridged_amount_received_units,
    };
    let order_qty_units: U256 = parse_units(&matched_order.qty, 6)?.into();
    assert_eq!(
        order_qty_units, expected_rebalance_usdc_units,
        "USDCUSD conversion order qty should match USDC rebalance amount"
    );
    let transfer_units: U256 = parse_units(&transfer.amount, 6)?.into();
    assert_eq!(
        transfer_units, expected_rebalance_usdc_units,
        "{} wallet transfer amount should match USDC rebalance event amount",
        expectations.expected_transfer_direction
    );

    Ok(())
}

#[bon::builder]
async fn assert_usdc_rebalancing_onchain_state<P: Provider>(
    provider: &P,
    orderbook_addr: Address,
    owner: Address,
    usdc_vault_id: B256,
    usdc_vault_balance_before_rebalance: B256,
    ethereum_usdc_balance_before_rebalance: U256,
    ethereum_usdc_balance_after_rebalance: U256,
    rebalance_type: UsdcRebalanceType,
    event_amounts: &UsdcRebalanceEventAmounts,
) -> anyhow::Result<()> {
    let orderbook = IOrderBookV6::IOrderBookV6Instance::new(orderbook_addr, provider);
    let vault_balance = orderbook
        .vaultBalance2(owner, base_chain::USDC_BASE, usdc_vault_id)
        .call()
        .await?;

    let pre_balance_float = Float::from_raw(usdc_vault_balance_before_rebalance);
    let post_balance_float = Float::from_raw(vault_balance);
    let pre_usdc_units = pre_balance_float.to_fixed_decimal_lossy(6)?.0;
    let post_usdc_units = post_balance_float.to_fixed_decimal_lossy(6)?.0;

    let expected_post_units = match rebalance_type {
        UsdcRebalanceType::AlpacaToBase => pre_usdc_units
            .checked_add(event_amounts.bridged_amount_received_units)
            .ok_or_else(|| anyhow::anyhow!("USDC expected post overflow on AlpacaToBase"))?,
        UsdcRebalanceType::BaseToAlpaca => pre_usdc_units
            .checked_sub(event_amounts.initiated_amount_units)
            .ok_or_else(|| anyhow::anyhow!("USDC expected post underflow on BaseToAlpaca"))?,
    };
    assert_eq!(
        post_usdc_units,
        expected_post_units,
        "USDC vault balance should reconcile exactly: pre_rebalance + transfer_delta = post (pre={}, post={})",
        pre_balance_float
            .format_with_scientific(false)
            .unwrap_or_else(|_| "???".to_string()),
        post_balance_float
            .format_with_scientific(false)
            .unwrap_or_else(|_| "???".to_string())
    );

    let expected_ethereum_post_units = match rebalance_type {
        UsdcRebalanceType::AlpacaToBase => ethereum_usdc_balance_before_rebalance
            .checked_sub(event_amounts.initiated_amount_units)
            .ok_or_else(|| anyhow::anyhow!("Ethereum USDC underflow on AlpacaToBase burn"))?,
        UsdcRebalanceType::BaseToAlpaca => ethereum_usdc_balance_before_rebalance
            .checked_add(event_amounts.bridged_amount_received_units)
            .ok_or_else(|| anyhow::anyhow!("Ethereum USDC overflow on BaseToAlpaca mint"))?,
    };
    assert_eq!(
        ethereum_usdc_balance_after_rebalance, expected_ethereum_post_units,
        "Ethereum USDC balance should reconcile exactly with CCTP transfer amount"
    );

    Ok(())
}

#[bon::builder]
pub(crate) async fn assert_usdc_rebalancing_flow<P: Provider>(
    expected_positions: &[ExpectedPosition],
    take_results: &[TakeOrderResult],
    provider: &P,
    orderbook_addr: Address,
    owner: Address,
    broker: &AlpacaBrokerMock,
    attestation: &CctpAttestationMock,
    db_path: &std::path::Path,
    usdc_vault_id: B256,
    usdc_vault_balance_before_rebalance: B256,
    ethereum_usdc_balance_before_rebalance: U256,
    ethereum_usdc_balance_after_rebalance: U256,
    rebalance_type: UsdcRebalanceType,
) -> anyhow::Result<()> {
    let database_url = &db_path.display().to_string();

    assert_broker_state(expected_positions, broker);
    assert_cqrs_state(expected_positions, take_results.len(), database_url).await?;

    let expectations = usdc_rebalance_expectations(rebalance_type);

    let pool = connect_db(db_path).await?;
    let event_amounts = assert_usdc_rebalancing_db_state(
        expected_positions,
        &pool,
        orderbook_addr,
        owner,
        &expectations,
    )
    .await?;

    pool.close().await;
    assert_usdc_rebalancing_broker_state(broker, rebalance_type, &expectations, &event_amounts)?;

    let attestation_count = attestation.processed_attestation_count();
    assert_eq!(
        attestation_count, 1,
        "Expected exactly 1 CCTP attestation processed, got {attestation_count}"
    );
    assert_usdc_rebalancing_onchain_state()
        .provider(provider)
        .orderbook_addr(orderbook_addr)
        .owner(owner)
        .usdc_vault_id(usdc_vault_id)
        .usdc_vault_balance_before_rebalance(usdc_vault_balance_before_rebalance)
        .ethereum_usdc_balance_before_rebalance(ethereum_usdc_balance_before_rebalance)
        .ethereum_usdc_balance_after_rebalance(ethereum_usdc_balance_after_rebalance)
        .rebalance_type(rebalance_type)
        .event_amounts(&event_amounts)
        .call()
        .await?;

    Ok(())
}

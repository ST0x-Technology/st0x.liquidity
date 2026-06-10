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
use sqlx::SqlitePool;
use tokio::task::JoinHandle;

use st0x_bridge::cctp::CctpAttestationMock;
use st0x_config::{BrokerCtx, Ctx};
use st0x_evm::{Evm, EvmError, Wallet};
use st0x_execution::alpaca_broker_api::{
    AlpacaBrokerMock, OrderSide, OrderStatus, TEST_API_KEY, TEST_API_SECRET, TransferDirection,
    TransferStatus,
};
use st0x_execution::{
    AlpacaAccountId, AlpacaBrokerApiCtx, AlpacaBrokerApiMode, FractionalShares, Symbol, TimeInForce,
};
use st0x_finance::{Positive, Usd};
pub(crate) use st0x_hedge::UsdcRebalancing;
use st0x_hedge::bindings::IOrderBookV6;
pub(crate) use st0x_hedge::mock_api::REDEMPTION_WALLET;
use st0x_hedge::mock_api::{AlpacaTokenizationMock, TokenizationStatus};
pub(crate) use st0x_hedge::mock_api::{RedemptionOutcome, TokenizationRequestType};
use st0x_hedge::{
    AssetsConfig, CashAssetConfig, EquitiesConfig, EquityAssetConfig, ImbalanceThreshold,
    OperationMode, TradingMode,
};

pub(crate) use crate::assert::{ExpectedPosition, assert_event_subsequence};
use crate::assert::{assert_broker_state, assert_cqrs_state, assert_single_clean_aggregate};
pub(crate) use crate::base_chain::TakeDirection;
use crate::base_chain::{self, TakeOrderResult};
pub(crate) use crate::cctp::{CctpInfra, CctpOverrides, USDC_ETHEREUM};
pub(crate) use crate::poll::{
    DEFAULT_POLL_TIMEOUT_SECS, connect_db, fetch_events_by_type, poll_for_events_with_timeout,
    poll_for_hedge_completion, poll_for_snapshot_field, sleep_or_crash, spawn_bot,
};
pub(crate) use crate::test_infra::TestInfra;
use st0x_float_macro::float;

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

pub(crate) struct TestWallet {
    address: Address,
    read_provider: RootProvider,
    signing_provider: SigningProvider,
    required_confirmations: u64,
}

impl TestWallet {
    pub(crate) fn new(
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

    async fn send_pending(
        &self,
        contract: Address,
        calldata: alloy::primitives::Bytes,
        note: &str,
    ) -> Result<alloy::primitives::TxHash, EvmError> {
        tracing::info!(%contract, note, "Submitting local test wallet call");

        let tx = TransactionRequest::default()
            .to(contract)
            .input(calldata.into());

        let pending = self.signing_provider.send_transaction(tx).await?;
        Ok(*pending.tx_hash())
    }

    async fn await_receipt(
        &self,
        tx_hash: alloy::primitives::TxHash,
    ) -> Result<TransactionReceipt, EvmError> {
        let timeout = Duration::from_secs(60);
        let poll_interval = Duration::from_secs(1);
        let start = tokio::time::Instant::now();

        let mut poll = tokio::time::interval(poll_interval);

        let (receipt, receipt_block) = loop {
            poll.tick().await;

            if start.elapsed() > timeout {
                return Err(EvmError::ReceiptTimeout {
                    tx_hash,
                    timeout_secs: 60,
                });
            }

            let Some(receipt) = self.read_provider.get_transaction_receipt(tx_hash).await? else {
                continue;
            };

            let Some(block) = receipt.block_number else {
                continue;
            };

            break (receipt, block);
        };

        // Wait for required confirmation depth.
        loop {
            let current_block = self.read_provider.get_block_number().await?;
            if current_block >= receipt_block + self.required_confirmations - 1 {
                break;
            }

            poll.tick().await;

            if start.elapsed() > timeout {
                return Err(EvmError::ReceiptTimeout {
                    tx_hash,
                    timeout_secs: 60,
                });
            }
        }

        Ok(receipt)
    }

    async fn send(
        &self,
        contract: Address,
        calldata: alloy::primitives::Bytes,
        note: &str,
    ) -> Result<TransactionReceipt, EvmError> {
        let tx_hash = self.send_pending(contract, calldata, note).await?;
        self.await_receipt(tx_hash).await
    }
}

/// Builds a `Ctx` with rebalancing enabled.
#[bon::builder]
pub(crate) fn build_rebalancing_ctx<P: Provider + Clone>(
    chain: &base_chain::BaseChain<P>,
    broker: &AlpacaBrokerMock,
    db_path: &std::path::Path,
    deployment_block: u64,
    equity_tokens: &[(String, Address, Address)],
    equity_vault_ids: &HashMap<String, B256>,
    cash_vault_id: B256,
    usdc_rebalancing: UsdcRebalancing,
    cash_rebalancing: OperationMode,
    wrapped_equity_recovery: OperationMode,
    // Equity imbalance threshold. Defaults to the standard (0.5 target, 0.1
    // deviation). Recovery tests pass a huge deviation to suppress equity
    // rebalancing transfers (which would churn the wallet/vault and starve a
    // second recovery) while keeping `rebalancing: Enabled` so the wallet
    // poller still emits the events that dispatch recovery jobs.
    equity_imbalance: Option<ImbalanceThreshold>,
    redemption_wallet: Address,
) -> anyhow::Result<Ctx> {
    let alpaca_auth = AlpacaBrokerApiCtx {
        api_key: TEST_API_KEY.to_owned(),
        api_secret: TEST_API_SECRET.to_owned(),
        account_id: AlpacaAccountId::new(uuid::uuid!("904837e3-3b76-47ec-b432-046db621571b")),
        mode: Some(AlpacaBrokerApiMode::Mock(broker.base_url())),
        asset_cache_ttl: Duration::from_secs(3600),
        time_in_force: TimeInForce::Day,
        counter_trade_slippage_bps: st0x_execution::DEFAULT_ALPACA_COUNTER_TRADE_SLIPPAGE_BPS,
    };
    let broker_ctx = BrokerCtx::AlpacaBrokerApi(alpaca_auth);

    let equities: HashMap<Symbol, EquityAssetConfig> = equity_tokens
        .iter()
        .map(|&(ref symbol, wrapped, unwrapped)| {
            Ok((
                Symbol::new(symbol)?,
                EquityAssetConfig {
                    tokenized_equity: unwrapped,
                    tokenized_equity_derivative: wrapped,
                    vault_ids: equity_vault_ids.get(symbol).copied().into_iter().collect(),
                    trading: OperationMode::Enabled,
                    rebalancing: OperationMode::Enabled,
                    wrapped_equity_recovery,
                    operational_limit: None,
                },
            ))
        })
        .collect::<anyhow::Result<_>>()?;

    let base_wallet: Arc<dyn st0x_evm::Wallet<Provider = RootProvider>> = Arc::new(
        TestWallet::new(&chain.owner_key, chain.endpoint().parse()?, 1)?,
    );

    let equity_threshold = match equity_imbalance {
        Some(threshold) => threshold,
        None => ImbalanceThreshold::new(float!(0.5), float!(0.1))?,
    };

    let rebalancing_ctx = st0x_hedge::RebalancingCtx::with_wallets()
        .equity(equity_threshold)
        .usdc(usdc_rebalancing)
        .call();

    let wallet_ctx = st0x_config::OnchainWalletCtx::from_wallets(base_wallet.clone(), base_wallet);

    let assets = AssetsConfig {
        equities: EquitiesConfig {
            symbols: equities,
            operational_limit: None,
        },
        cash: Some(CashAssetConfig {
            vault_ids: vec![cash_vault_id],
            rebalancing: cash_rebalancing,
            operational_limit: None,
            reserved: None,
        }),
    };

    Ctx::for_test()
        .database_url(db_path.display().to_string())
        .rpc_url(chain.endpoint().parse()?)
        .orderbook(chain.orderbook)
        .deployment_block(deployment_block)
        .broker(broker_ctx)
        .trading_mode(TradingMode::Rebalancing(Box::new(rebalancing_ctx)))
        .order_owner(chain.owner)
        .wallet(wallet_ctx)
        .assets(assets)
        .redemption_wallet(redemption_wallet)
        .call()
        .map_err(Into::into)
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
    reserved: Option<Positive<Usd>>,
    wrapped_equity_recovery: OperationMode,
) -> anyhow::Result<Ctx>
where
    BP: Provider + Clone,
{
    let alpaca_auth = AlpacaBrokerApiCtx {
        api_key: TEST_API_KEY.to_owned(),
        api_secret: TEST_API_SECRET.to_owned(),
        account_id: AlpacaAccountId::new(uuid::uuid!("904837e3-3b76-47ec-b432-046db621571b")),
        mode: Some(AlpacaBrokerApiMode::Mock(broker.base_url())),
        asset_cache_ttl: Duration::from_secs(3600),
        time_in_force: TimeInForce::Day,
        counter_trade_slippage_bps: st0x_execution::DEFAULT_ALPACA_COUNTER_TRADE_SLIPPAGE_BPS,
    };
    let broker_ctx = BrokerCtx::AlpacaBrokerApi(alpaca_auth);

    let equities: HashMap<Symbol, EquityAssetConfig> = equity_tokens
        .iter()
        .map(|(symbol, wrapped, unwrapped)| {
            Ok((
                Symbol::new(symbol)?,
                EquityAssetConfig {
                    tokenized_equity: *unwrapped,
                    tokenized_equity_derivative: *wrapped,
                    vault_ids: Vec::new(),
                    trading: OperationMode::Enabled,
                    rebalancing: OperationMode::Disabled,
                    wrapped_equity_recovery,
                    operational_limit: None,
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
        .equity(ImbalanceThreshold::new(float!(0.5), float!(100))?)
        .usdc(UsdcRebalancing::Enabled {
            target: float!(0.5),
            deviation: float!(0.1),
        })
        .call()
        .with_circle_api_base(cctp.attestation_base_url)
        .with_cctp_addresses(cctp.token_messenger, cctp.message_transmitter);

    let wallet_ctx = st0x_config::OnchainWalletCtx::from_wallets(base_wallet, ethereum_wallet);

    Ctx::for_test()
        .database_url(db_path.display().to_string())
        .rpc_url(base_chain.endpoint().parse()?)
        .orderbook(base_chain.orderbook)
        .deployment_block(deployment_block)
        .broker(broker_ctx)
        .trading_mode(TradingMode::Rebalancing(Box::new(rebalancing_ctx)))
        .order_owner(base_chain.owner)
        .wallet(wallet_ctx)
        .assets(AssetsConfig {
            equities: EquitiesConfig {
                symbols: equities,
                operational_limit: None,
            },
            cash: Some(CashAssetConfig {
                vault_ids: vec![usdc_vault_id],
                rebalancing: OperationMode::Enabled,
                operational_limit: None,
                reserved,
            }),
        })
        .inventory_poll_interval(15)
        .redemption_wallet(Address::random())
        .call()
        .map_err(Into::into)
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

/// Asserts inventory snapshot state by reading the CQRS snapshot table.
///
/// Events are compacted for `InventorySnapshot`, so querying the events
/// table is unreliable. The snapshot table always contains the latest
/// persisted state.
async fn assert_inventory_snapshots(
    pool: &SqlitePool,
    orderbook: Address,
    owner: Address,
    expected_symbols: &[&str],
    assert_cash: bool,
) -> anyhow::Result<()> {
    let expected_aggregate_id = format!(
        "{}:{}",
        orderbook.to_checksum(None),
        owner.to_checksum(None),
    );

    let snapshot_row = sqlx::query_as::<_, (String, String)>(
        "SELECT aggregate_id, payload FROM snapshots \
         WHERE aggregate_type = 'InventorySnapshot' AND aggregate_id = ?",
    )
    .bind(&expected_aggregate_id)
    .fetch_optional(pool)
    .await?;

    let (aggregate_id, payload_str) =
        snapshot_row.ok_or_else(|| anyhow::anyhow!("No InventorySnapshot snapshot found"))?;

    assert_eq!(
        aggregate_id, expected_aggregate_id,
        "InventorySnapshot aggregate_id mismatch"
    );

    let payload: serde_json::Value = serde_json::from_str(&payload_str)?;
    let live = payload
        .get("Live")
        .ok_or_else(|| anyhow::anyhow!("Snapshot not in Live state: {payload}"))?;

    let onchain_equity = live
        .get("onchain_equity")
        .ok_or_else(|| anyhow::anyhow!("Snapshot missing onchain_equity"))?;
    let offchain_equity = live
        .get("offchain_equity")
        .ok_or_else(|| anyhow::anyhow!("Snapshot missing offchain_equity"))?;

    for symbol in expected_symbols {
        let balance_str = onchain_equity
            .get(*symbol)
            .and_then(|val| val.as_str())
            .unwrap_or_else(|| {
                panic!("onchain_equity missing symbol {symbol}, got: {onchain_equity}")
            });
        let _parsed = Float::parse(balance_str.to_string())
            .unwrap_or_else(|err| panic!("Failed to parse onchain balance for {symbol}: {err:?}"));

        let position_str = offchain_equity
            .get(*symbol)
            .and_then(|val| val.as_str())
            .unwrap_or_else(|| {
                panic!("offchain_equity missing symbol {symbol}, got: {offchain_equity}")
            });
        let _parsed = Float::parse(position_str.to_string()).unwrap_or_else(|err| {
            panic!("Failed to parse offchain position for {symbol}: {err:?}")
        });
    }

    if assert_cash {
        let _usdc = live
            .get("onchain_usdc")
            .and_then(|val| val.as_str())
            .ok_or_else(|| anyhow::anyhow!("Snapshot missing onchain_usdc"))?;
    }

    Ok(())
}

async fn assert_equity_mint_rebalancing<P: Provider>(
    pool: &SqlitePool,
    take_results: &[TakeOrderResult],
    provider: &P,
    orderbook: Address,
    owner: Address,
    symbol: &str,
    tokenization: &AlpacaTokenizationMock,
) -> anyhow::Result<()> {
    let all_mint_events = fetch_events_by_type(pool, "TokenizedEquityMint").await?;

    // The inventory poller may fire a second time after the first mint cycle
    // completes, starting a new aggregate before the test reads events. Filter
    // to the first completed aggregate (the one that reached DepositedIntoRaindex)
    // to avoid flaky count assertions.
    let completed_aggregate_id = all_mint_events
        .iter()
        .find(|event| event.event_type == "TokenizedEquityMintEvent::DepositedIntoRaindex")
        .map(|event| event.aggregate_id.as_str())
        .expect("Expected at least one completed mint aggregate (DepositedIntoRaindex)");

    let mint_events: Vec<_> = all_mint_events
        .iter()
        .filter(|event| event.aggregate_id == completed_aggregate_id)
        .collect();

    assert_eq!(
        mint_events.len(),
        7,
        "Expected exactly 7 TokenizedEquityMint events in the completed aggregate, \
         got {} (total across all aggregates: {})",
        mint_events.len(),
        all_mint_events.len(),
    );
    assert_event_subsequence(
        &all_mint_events,
        &[
            "TokenizedEquityMintEvent::MintRequested",
            "TokenizedEquityMintEvent::MintAccepted",
            "TokenizedEquityMintEvent::TokensReceived",
            "TokenizedEquityMintEvent::WrapSubmitted",
            "TokenizedEquityMintEvent::TokensWrapped",
            "TokenizedEquityMintEvent::VaultDepositSubmitted",
            "TokenizedEquityMintEvent::DepositedIntoRaindex",
        ],
    );

    // Only check the completed aggregate for errors — spurious second cycles
    // may be incomplete but should not contain failures either.
    let error_events: Vec<_> = all_mint_events
        .iter()
        .filter(|event| {
            ["Failed", "Rejected"]
                .iter()
                .any(|sub| event.event_type.contains(sub))
        })
        .collect();
    assert!(
        error_events.is_empty(),
        "Expected no error events in any mint aggregate, found: {:?}",
        error_events
            .iter()
            .map(|event| &event.event_type)
            .collect::<Vec<_>>(),
    );

    let mint_requests: Vec<_> = tokenization
        .tokenization_requests()
        .into_iter()
        .filter(|req| req.request_type == TokenizationRequestType::Mint && req.symbol == symbol)
        .collect();
    assert!(
        !mint_requests.is_empty(),
        "Expected at least 1 mint request for {symbol}, got 0",
    );

    // At least one mint request should have completed. A spurious second
    // poller cycle may create additional requests that haven't completed yet.
    let completed_mint = mint_requests
        .iter()
        .find(|req| req.status == TokenizationStatus::Completed)
        .unwrap_or_else(|| {
            panic!("Expected at least 1 completed mint request for {symbol}, found none")
        });

    // Compute the expected vault balance after the take from the
    // before-take snapshot minus the take event delta. The live query
    // (`output_vault_balance_after_take`) is racey because the
    // inventory poller may detect the vault change and trigger a
    // deposit before the test re-queries the chain.
    let consumed_output_vaults: HashMap<(Address, B256), B256> = take_results
        .iter()
        .map(|result| {
            let before = Float::from_raw(result.output_vault_balance_before_take);
            let delta = Float::from_raw(result.output_vault_delta_from_take_event);
            let after_take = (before - delta)?.get_inner();
            Ok(((result.output_token, result.output_vault_id), after_take))
        })
        .collect::<anyhow::Result<_>>()?;

    let orderbook = IOrderBookV6::IOrderBookV6Instance::new(orderbook, provider);
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
    let mint_quantity_units: U256 = parse_units(
        &completed_mint
            .quantity
            .format_with_scientific(false)
            .unwrap_or_else(|err| panic!("Failed to format mint quantity: {err}")),
        18,
    )?
    .into();
    assert_eq!(
        mint_quantity_units, wrapped_shares,
        "Tokenization completed mint quantity should match TokensWrapped.wrapped_shares"
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
    orderbook: Address,
    owner: Address,
    symbol: &str,
    tokenization: &AlpacaTokenizationMock,
    redemption_wallet_balance_before: U256,
    redemption_wallet_balance_after: U256,
) -> anyhow::Result<()> {
    let redeem_events = fetch_events_by_type(pool, "EquityRedemption").await?;
    assert_eq!(
        redeem_events.len(),
        10,
        "Expected exactly 10 EquityRedemption success events",
    );
    assert_event_subsequence(
        &redeem_events,
        &[
            "EquityRedemptionEvent::VaultWithdrawPending",
            "EquityRedemptionEvent::VaultWithdrawSubmitted",
            "EquityRedemptionEvent::WithdrawnFromRaindex",
            "EquityRedemptionEvent::UnwrapPending",
            "EquityRedemptionEvent::UnwrapSubmitted",
            "EquityRedemptionEvent::TokensUnwrapped",
            "EquityRedemptionEvent::SendPending",
            "EquityRedemptionEvent::TokensSent",
            "EquityRedemptionEvent::Detected",
            "EquityRedemptionEvent::Completed",
        ],
    );
    assert_single_clean_aggregate(&redeem_events, &["Failed", "Rejected"]);

    let redeem_requests: Vec<_> = tokenization
        .tokenization_requests()
        .into_iter()
        .filter(|req| req.request_type == TokenizationRequestType::Redeem && req.symbol == symbol)
        .collect();
    assert_eq!(
        redeem_requests.len(),
        1,
        "Expected exactly 1 redeem request for {symbol}, got {}",
        redeem_requests.len(),
    );

    let completed_redeem = &redeem_requests[0];
    assert_eq!(
        completed_redeem.status,
        TokenizationStatus::Completed,
        "Redeem request for {symbol} should complete"
    );

    let redemption_wallet_delta = redemption_wallet_balance_after
        .checked_sub(redemption_wallet_balance_before)
        .ok_or_else(|| anyhow::anyhow!("Redemption wallet balance underflow"))?;
    let expected_wallet_delta: U256 = parse_units(
        &completed_redeem
            .quantity
            .format_with_scientific(false)
            .unwrap_or_else(|err| panic!("Failed to format redeem quantity: {err}")),
        18,
    )?
    .into();
    assert_eq!(
        redemption_wallet_delta, expected_wallet_delta,
        "Redemption wallet delta should match completed redeem quantity (18 decimals)"
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

    // Compute the expected vault balance after the take from the
    // before-take snapshot plus the take event delta. The live query
    // (`input_vault_balance_after_take`) is racey because the
    // inventory poller may detect the vault change and trigger a
    // withdrawal before the test re-queries the chain.
    let redeemed_input_vaults: HashMap<(Address, B256), B256> = take_results
        .iter()
        .map(|result| {
            let before = Float::from_raw(result.input_vault_balance_before_take);
            let delta = Float::from_raw(result.input_vault_delta_from_take_event);
            let after_take = (before + delta)?.get_inner();
            Ok(((result.input_token, result.input_vault_id), after_take))
        })
        .collect::<anyhow::Result<_>>()?;
    let orderbook = IOrderBookV6::IOrderBookV6Instance::new(orderbook, provider);
    let mut total_withdrawn_wrapped_shares = U256::ZERO;
    for ((token, vault_id), pre_rebalance_balance) in &redeemed_input_vaults {
        let post_rebalance_balance = orderbook
            .vaultBalance2(owner, *token, *vault_id)
            .call()
            .await?;

        let pre_rebalance_shares = Float::from_raw(*pre_rebalance_balance)
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
                    Float::from_raw(*pre_rebalance_balance)
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
        "Redeemed Raindex vault net share delta should match completed redeem qty \
         minus concurrent mint wrapped shares\n\
         redeemed_input_vaults: {redeemed_input_vaults:?}\n\
         expected_wallet_delta: {expected_wallet_delta}\n\
         concurrent_mint_wrapped_shares: {concurrent_mint_wrapped_shares}"
    );

    let last_event = redeem_events
        .last()
        .ok_or_else(|| anyhow::anyhow!("Expected at least one redemption event"))?;
    assert_eq!(
        last_event.event_type, "EquityRedemptionEvent::Completed",
        "Last redemption event should be Completed, got: {}",
        last_event.event_type,
    );

    // Verify the symbol from the VaultWithdrawPending event (the terminal
    // Completed event only has a timestamp). Find by payload key rather
    // than assuming a fixed index.
    let pending_event = redeem_events
        .iter()
        .find(|event| event.payload.get("VaultWithdrawPending").is_some())
        .ok_or_else(|| anyhow::anyhow!("No redemption event contains VaultWithdrawPending"))?;
    let submitted = pending_event
        .payload
        .get("VaultWithdrawPending")
        .expect("checked in find");
    assert_eq!(
        submitted.get("symbol").and_then(|val| val.as_str()),
        Some(symbol),
        "Redemption should be for {symbol}, got: {submitted:?}"
    );

    Ok(())
}

pub(crate) async fn assert_initial_base_wallet_unwrapped_and_wrapped_equity_snapshot(
    bot: &mut JoinHandle<anyhow::Result<()>>,
    db_path: &std::path::Path,
    symbol: &str,
    expected_unwrapped_balance: FractionalShares,
    expected_wrapped_balance: FractionalShares,
) -> anyhow::Result<()> {
    let timeout = Duration::from_secs(DEFAULT_POLL_TIMEOUT_SECS);
    let deadline = tokio::time::Instant::now() + timeout;
    let context = format!(
        "Base-wallet unwrapped and wrapped equity snapshots for {symbol} matching unwrapped \
             {expected_unwrapped_balance} and wrapped {expected_wrapped_balance}"
    );

    let parse_snapshot_balance = |events: &[crate::assert::StoredEvent],
                                  event_type: &str,
                                  payload_key: &str|
     -> anyhow::Result<Option<FractionalShares>> {
        events
            .iter()
            .rev()
            .find(|event| event.event_type == event_type)
            .and_then(|event| {
                event
                    .payload
                    .get(payload_key)
                    .and_then(|value| value.get("balances"))
                    .and_then(|balances| balances.get(symbol))
                    .and_then(|value| value.as_str())
            })
            .map(|balance_str| {
                balance_str.parse::<FractionalShares>().map_err(|error| {
                    anyhow::anyhow!(
                        "Failed to parse {event_type} balance '{balance_str}' for {symbol}: \
                         {error}"
                    )
                })
            })
            .transpose()
    };

    loop {
        sleep_or_crash(bot, &context).await;

        let Ok(pool) = connect_db(db_path).await else {
            assert!(
                tokio::time::Instant::now() < deadline,
                "Timed out after {timeout:?} waiting for {context} (database not ready)",
            );
            continue;
        };

        let events = fetch_events_by_type(&pool, "InventorySnapshot").await;
        pool.close().await;

        let (unwrapped_snapshot_balance, wrapped_snapshot_balance) = match events {
            Ok(events) => (
                parse_snapshot_balance(
                    &events,
                    "InventorySnapshotEvent::BaseWalletUnwrappedEquity",
                    "BaseWalletUnwrappedEquity",
                )?,
                parse_snapshot_balance(
                    &events,
                    "InventorySnapshotEvent::BaseWalletWrappedEquity",
                    "BaseWalletWrappedEquity",
                )?,
            ),
            Err(error) => {
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "Timed out after {timeout:?} waiting for {context} \
                     (query failed: {error})",
                );
                continue;
            }
        };

        if unwrapped_snapshot_balance == Some(expected_unwrapped_balance)
            && wrapped_snapshot_balance == Some(expected_wrapped_balance)
        {
            return Ok(());
        }

        assert!(
            tokio::time::Instant::now() < deadline,
            "Timed out after {timeout:?} waiting for {context} \
             (found latest unwrapped snapshot {unwrapped_snapshot_balance:?}, latest wrapped \
              snapshot {wrapped_snapshot_balance:?})",
        );
    }
}

#[bon::builder]
pub(crate) async fn assert_equity_rebalancing_flow<P: Provider>(
    expected_positions: &[ExpectedPosition],
    take_results: &[TakeOrderResult],
    provider: &P,
    orderbook: Address,
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
    assert_inventory_snapshots(&pool, orderbook, owner, &[equity_symbol], false).await?;

    match rebalance_type {
        EquityRebalanceType::Mint {
            symbol,
            tokenization,
            ..
        } => {
            assert_equity_mint_rebalancing(
                &pool,
                take_results,
                provider,
                orderbook,
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
            ..
        } => {
            assert_equity_redeem_rebalancing()
                .pool(&pool)
                .take_results(take_results)
                .provider(provider)
                .orderbook(orderbook)
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
    expected_broker_side: OrderSide,
    expected_transfer_direction: TransferDirection,
}

struct UsdcRebalanceEventAmounts {
    initiated_amount: U256,
    bridged_amount_received: U256,
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
            expected_broker_side: OrderSide::Buy,
            expected_transfer_direction: TransferDirection::Outgoing,
        },
        UsdcRebalanceType::BaseToAlpaca => UsdcRebalanceExpectations {
            event_sequence: &[
                "UsdcRebalanceEvent::WithdrawalSubmitting",
                "UsdcRebalanceEvent::Initiated",
                "UsdcRebalanceEvent::WithdrawalConfirmed",
                "UsdcRebalanceEvent::BridgingSubmitting",
                "UsdcRebalanceEvent::BridgingInitiated",
                "UsdcRebalanceEvent::BridgeAttestationReceived",
                "UsdcRebalanceEvent::Bridged",
                "UsdcRebalanceEvent::DepositInitiated",
                "UsdcRebalanceEvent::DepositConfirmed",
                "UsdcRebalanceEvent::ConversionInitiated",
                "UsdcRebalanceEvent::ConversionConfirmed",
            ],
            direction_str: "BaseToAlpaca",
            expected_broker_side: OrderSide::Sell,
            expected_transfer_direction: TransferDirection::Incoming,
        },
    }
}

async fn assert_usdc_rebalancing_db_state(
    expected_positions: &[ExpectedPosition],
    pool: &SqlitePool,
    orderbook: Address,
    owner: Address,
    expectations: &UsdcRebalanceExpectations<'_>,
) -> anyhow::Result<UsdcRebalanceEventAmounts> {
    let equity_symbols: Vec<&str> = expected_positions.iter().map(|pos| pos.symbol).collect();
    assert_inventory_snapshots(pool, orderbook, owner, &equity_symbols, true).await?;

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
        initiated_amount: parse_units(initiated_amount_str, 6)?.into(),
        bridged_amount_received: parse_units(amount_received, 6)?.into(),
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
    assert_eq!(
        matched_order.status,
        OrderStatus::Filled,
        "USDCUSD conversion order should be filled"
    );

    let transfers: Vec<_> = broker
        .wallet_transfers()
        .into_iter()
        .filter(|transfer| transfer.flow.direction() == expectations.expected_transfer_direction)
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
        transfer.status,
        TransferStatus::Complete,
        "{} transfer {} should be COMPLETE, got {}",
        expectations.expected_transfer_direction,
        transfer.transfer_id,
        transfer.status
    );

    let expected_rebalance_usdc_units = match rebalance_type {
        UsdcRebalanceType::AlpacaToBase => event_amounts.initiated_amount,
        UsdcRebalanceType::BaseToAlpaca => event_amounts.bridged_amount_received,
    };
    let order_quantity_units: U256 = parse_units(
        &matched_order
            .quantity
            .format_with_scientific(false)
            .unwrap_or_else(|err| panic!("Failed to format order quantity: {err}")),
        6,
    )?
    .into();
    assert_eq!(
        order_quantity_units, expected_rebalance_usdc_units,
        "USDCUSD conversion order quantity should match USDC rebalance amount"
    );
    let transfer_units: U256 = parse_units(
        &transfer
            .amount
            .format_with_scientific(false)
            .unwrap_or_else(|err| panic!("Failed to format transfer amount: {err}")),
        6,
    )?
    .into();
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
    orderbook: Address,
    owner: Address,
    usdc_vault_id: B256,
    usdc_vault_balance_before_rebalance: B256,
    ethereum_usdc_balance_before_rebalance: U256,
    ethereum_usdc_balance_after_rebalance: U256,
    rebalance_type: UsdcRebalanceType,
    event_amounts: &UsdcRebalanceEventAmounts,
    take_results: &[TakeOrderResult],
) -> anyhow::Result<()> {
    let orderbook = IOrderBookV6::IOrderBookV6Instance::new(orderbook, provider);
    let vault_balance = orderbook
        .vaultBalance2(owner, base_chain::USDC_BASE, usdc_vault_id)
        .call()
        .await?;

    let pre_balance_float = Float::from_raw(usdc_vault_balance_before_rebalance);
    let post_balance_float = Float::from_raw(vault_balance);
    let (pre_usdc_units, pre_lossless) = pre_balance_float.to_fixed_decimal_lossy(6)?;
    assert!(
        pre_lossless,
        "Pre-rebalance USDC balance should convert losslessly to 6 decimals"
    );
    let (post_usdc_units, post_lossless) = post_balance_float.to_fixed_decimal_lossy(6)?;
    assert!(
        post_lossless,
        "Post-rebalance USDC balance should convert losslessly to 6 decimals"
    );

    // Trades also modify the USDC vault concurrently with rebalancing,
    // but only if the trade uses the same vault_id. Filter to trades
    // that actually affected the shared USDC vault.
    let mut usdc_received = U256::ZERO;
    let mut usdc_spent = U256::ZERO;
    for result in take_results {
        if result.input_token == base_chain::USDC_BASE && result.input_vault_id == usdc_vault_id {
            let delta = Float::from_raw(result.input_vault_delta_from_take_event)
                .to_fixed_decimal_lossy(6)?
                .0;
            usdc_received = usdc_received
                .checked_add(delta)
                .ok_or_else(|| anyhow::anyhow!("USDC received overflow"))?;
        }
        if result.output_token == base_chain::USDC_BASE && result.output_vault_id == usdc_vault_id {
            let delta = Float::from_raw(result.output_vault_delta_from_take_event)
                .to_fixed_decimal_lossy(6)?
                .0;
            usdc_spent = usdc_spent
                .checked_add(delta)
                .ok_or_else(|| anyhow::anyhow!("USDC spent overflow"))?;
        }
    }

    let pre_plus_trades = pre_usdc_units
        .checked_add(usdc_received)
        .ok_or_else(|| anyhow::anyhow!("USDC pre + received overflow"))?
        .checked_sub(usdc_spent)
        .ok_or_else(|| anyhow::anyhow!("USDC pre + received - spent underflow"))?;

    let expected_post_units = match rebalance_type {
        UsdcRebalanceType::AlpacaToBase => pre_plus_trades
            .checked_add(event_amounts.bridged_amount_received)
            .ok_or_else(|| anyhow::anyhow!("USDC expected post overflow on AlpacaToBase"))?,
        UsdcRebalanceType::BaseToAlpaca => pre_plus_trades
            .checked_sub(event_amounts.initiated_amount)
            .ok_or_else(|| anyhow::anyhow!("USDC expected post underflow on BaseToAlpaca"))?,
    };
    assert_eq!(
        post_usdc_units,
        expected_post_units,
        "USDC vault balance should reconcile exactly: \
         pre + received - spent + rebalance_delta = post \
         (pre={}, received={usdc_received}, spent={usdc_spent}, post={})",
        pre_balance_float
            .format_with_scientific(false)
            .unwrap_or_else(|_| "???".to_string()),
        post_balance_float
            .format_with_scientific(false)
            .unwrap_or_else(|_| "???".to_string())
    );

    let expected_ethereum_post_units = match rebalance_type {
        UsdcRebalanceType::AlpacaToBase => {
            // The Alpaca withdrawal mints USDC to the bot's Ethereum wallet
            // (via the test fixture's withdrawal executor) and the CCTP
            // burn removes the same amount, so the wallet returns to its
            // pre-test baseline of zero. We can't compare against
            // `ethereum_usdc_balance_before_rebalance` because the test
            // captures it after the bot starts -- if the watcher has
            // already minted by that point, the "before" reading reflects
            // mid-flight state, not baseline.
            U256::ZERO
        }
        UsdcRebalanceType::BaseToAlpaca => ethereum_usdc_balance_before_rebalance
            .checked_add(event_amounts.bridged_amount_received)
            .ok_or_else(|| anyhow::anyhow!("Ethereum USDC overflow on BaseToAlpaca mint"))?,
    };
    assert_eq!(
        ethereum_usdc_balance_after_rebalance, expected_ethereum_post_units,
        "Ethereum USDC balance should reconcile exactly with CCTP transfer amount"
    );

    Ok(())
}

/// Asserts that the `InventorySnapshot` CQRS snapshot contains a non-null
/// `ethereum_usdc` field, proving that Ethereum wallet polling ran at least
/// once.
///
/// Reads from the snapshot table, not the events table, because
/// `InventorySnapshot` uses `CompactAfterSnapshot` with `SNAPSHOT_SIZE = 1`
/// -- the cleanup job can compact events before this assertion runs.
pub(crate) async fn assert_ethereum_usdc_event_exists(
    db_path: &std::path::Path,
) -> anyhow::Result<()> {
    let pool = connect_db(db_path).await?;

    let payload_str: String = sqlx::query_scalar(
        "SELECT payload FROM snapshots WHERE aggregate_type = 'InventorySnapshot' LIMIT 1",
    )
    .fetch_optional(&pool)
    .await?
    .ok_or_else(|| anyhow::anyhow!("No InventorySnapshot snapshot found"))?;

    let payload: serde_json::Value = serde_json::from_str(&payload_str)?;
    let ethereum_usdc = payload
        .pointer("/Live/ethereum_usdc")
        .ok_or_else(|| anyhow::anyhow!("Snapshot missing ethereum_usdc field: {payload}"))?;

    assert!(
        !ethereum_usdc.is_null(),
        "Expected ethereum_usdc to be populated from Ethereum wallet polling, got null"
    );

    pool.close().await;
    Ok(())
}

/// Asserts that the `InventorySnapshot` CQRS snapshot contains a non-null
/// `base_wallet_usdc` field, proving that Base wallet polling ran at least
/// once.
///
/// Reads from the snapshot table for the same compaction reason as
/// [`assert_ethereum_usdc_event_exists`].
pub(crate) async fn assert_base_wallet_usdc_event_exists(
    db_path: &std::path::Path,
) -> anyhow::Result<()> {
    let pool = connect_db(db_path).await?;

    let payload_str: String = sqlx::query_scalar(
        "SELECT payload FROM snapshots WHERE aggregate_type = 'InventorySnapshot' LIMIT 1",
    )
    .fetch_optional(&pool)
    .await?
    .ok_or_else(|| anyhow::anyhow!("No InventorySnapshot snapshot found"))?;

    let payload: serde_json::Value = serde_json::from_str(&payload_str)?;
    let base_wallet_usdc = payload
        .pointer("/Live/base_wallet_usdc")
        .ok_or_else(|| anyhow::anyhow!("Snapshot missing base_wallet_usdc field: {payload}"))?;

    assert!(
        !base_wallet_usdc.is_null(),
        "Expected base_wallet_usdc to be populated from Base wallet polling, got null"
    );

    pool.close().await;
    Ok(())
}

/// Polls `InventorySnapshot` events until the latest `OffchainUsd` reports
/// `broker.cash_balance() - reserved`, proving the inventory poller subtracts
/// the configured cash reserve before emitting the offchain balance.
///
/// Polls because the test may call this before the next inventory poll cycle
/// has run (poll interval is 15s, hedge fills settle asynchronously).
pub(crate) async fn assert_offchain_usd_reflects_reserve(
    bot: &mut JoinHandle<anyhow::Result<()>>,
    db_path: &std::path::Path,
    broker: &AlpacaBrokerMock,
    reserved: Positive<Usd>,
) -> anyhow::Result<()> {
    let reserved_cents = reserved.inner().to_cents()?;

    let timeout = Duration::from_secs(DEFAULT_POLL_TIMEOUT_SECS);
    let deadline = tokio::time::Instant::now() + timeout;
    let context = "OffchainUsd snapshot matching broker cash minus reserve".to_string();

    loop {
        sleep_or_crash(bot, &context).await;

        let cash_cents = Usd::new(broker.cash_balance()).to_cents()?;
        let expected_cents = cash_cents - reserved_cents;

        let pool = connect_db(db_path).await?;
        let events = fetch_events_by_type(&pool, "InventorySnapshot").await?;
        pool.close().await;

        let latest_reported_cents = events
            .iter()
            .rev()
            .find(|event| event.event_type == "InventorySnapshotEvent::OffchainUsd")
            .and_then(|event| {
                event
                    .payload
                    .get("OffchainUsd")
                    .and_then(|value| value.get("usd_balance_cents"))
                    .and_then(serde_json::Value::as_i64)
            });

        if latest_reported_cents == Some(expected_cents) {
            return Ok(());
        }

        assert!(
            tokio::time::Instant::now() < deadline,
            "Timed out after {timeout:?} waiting for {context} \
             (latest reported={latest_reported_cents:?}, expected={expected_cents}, \
              broker cash={cash_cents} cents, reserved={reserved_cents} cents)",
        );
    }
}

#[bon::builder]
pub(crate) async fn assert_usdc_rebalancing_flow<P: Provider>(
    expected_positions: &[ExpectedPosition],
    take_results: &[TakeOrderResult],
    provider: &P,
    orderbook: Address,
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
        orderbook,
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
        .orderbook(orderbook)
        .owner(owner)
        .usdc_vault_id(usdc_vault_id)
        .usdc_vault_balance_before_rebalance(usdc_vault_balance_before_rebalance)
        .ethereum_usdc_balance_before_rebalance(ethereum_usdc_balance_before_rebalance)
        .ethereum_usdc_balance_after_rebalance(ethereum_usdc_balance_after_rebalance)
        .rebalance_type(rebalance_type)
        .event_amounts(&event_amounts)
        .take_results(take_results)
        .call()
        .await?;

    Ok(())
}

//! Builds the rebalancing transfer infrastructure.

use alloy::primitives::Address;
use alloy::providers::RootProvider;
use std::collections::HashMap;
use std::hash::BuildHasher;
use std::sync::Arc;
use tracing::info;

use st0x_bridge::cctp::{CctpBridge, CctpCtx, CctpError};
use st0x_config::{ChainEquityAsset, OnchainWalletCtx};
use st0x_event_sorcery::Store;
use st0x_evm::{USDC_BASE, USDC_ETHEREUM, Wallet};
use st0x_execution::{AlpacaWalletService, EmptySymbolError, Symbol};
use st0x_raindex::{RaindexService, RaindexVaultId};
use st0x_wrapper::WrappedEquity;

use super::usdc::{
    CrossVenueCashTransfer, ResumeAlpacaToBase, ResumeBaseToAlpaca, UsdcSettlementParams,
};
use crate::bot_gas::BotGasReceiptCostEnqueuer;
use crate::native_gas::GasReadiness;
use crate::telemetry::broker::InstrumentedAlpacaBroker;
use crate::usdc_rebalance::UsdcRebalance;

/// Errors that can occur when spawning the rebalancer.
#[derive(Debug, thiserror::Error)]
pub(crate) enum SpawnRebalancerError {
    #[error("failed to create CCTP bridge: {0}")]
    Cctp(#[from] Box<CctpError>),
    #[error("failed to create wrapper service: {0}")]
    Wrapper(#[from] EmptySymbolError),
}

/// Adapts the config-layer equity asset map to the narrow per-symbol token pairs
/// `WrapperService` needs, keeping `st0x-wrapper` independent of `st0x-config`.
pub fn to_wrapped_equities<S: BuildHasher>(
    equities: &HashMap<Symbol, ChainEquityAsset, S>,
) -> HashMap<Symbol, WrappedEquity> {
    equities
        .iter()
        .map(|(symbol, asset)| {
            (
                symbol.clone(),
                WrappedEquity {
                    underlying: asset.tokenized_equity,
                    derivative: asset.tokenized_equity_derivative,
                },
            )
        })
        .collect()
}

/// Trait-erased resume entry points for the cash transfer, so the conductor
/// can build apalis job ctxs without leaking the wallet `Signer` generic
/// upstream.
pub(crate) struct UsdcTransferResumeHandles {
    pub(crate) resume_base_to_alpaca: Arc<dyn ResumeBaseToAlpaca>,
    pub(crate) resume_alpaca_to_base: Arc<dyn ResumeAlpacaToBase>,
}

#[derive(Clone)]
pub(crate) struct EthereumWallet<Signer>(pub(crate) Signer);

#[derive(Clone)]
pub(crate) struct BaseWallet<Signer>(pub(crate) Signer);

#[derive(Clone)]
pub(crate) struct ChainWallets<Signer> {
    ethereum: EthereumWallet<Signer>,
    base: BaseWallet<Signer>,
}

impl<Signer> ChainWallets<Signer> {
    pub(crate) fn base(&self) -> &BaseWallet<Signer> {
        &self.base
    }

    pub(crate) fn into_parts(self) -> (EthereumWallet<Signer>, BaseWallet<Signer>) {
        (self.ethereum, self.base)
    }
}

impl ChainWallets<Arc<dyn Wallet<Provider = RootProvider>>> {
    pub(crate) fn from_wallet_ctx(ctx: &OnchainWalletCtx) -> Self {
        Self {
            ethereum: EthereumWallet(ctx.ethereum_wallet().clone()),
            base: BaseWallet(ctx.base_wallet().clone()),
        }
    }
}

/// External service clients for rebalancing operations.
///
/// Holds connections to Alpaca APIs, CCTP bridge, and vault services.
/// Providers for both chains are obtained from the wallets on `RebalancingCtx`.
pub(crate) struct RebalancerServices<Signer: Wallet> {
    broker: InstrumentedAlpacaBroker,
    wallet: Arc<AlpacaWalletService>,
    cctp: Arc<CctpBridge<Signer, Signer>>,
    raindex: Arc<RaindexService<Signer>>,
    settlement: UsdcSettlementParams,
}

impl<Signer: Wallet + Clone> RebalancerServices<Signer> {
    /// Creates the services needed for rebalancing.
    ///
    /// RaindexService is passed in rather than created here because it is
    /// needed for CQRS framework initialization in the conductor, which
    /// must happen before this constructor is called.
    pub(crate) fn new(
        broker: InstrumentedAlpacaBroker,
        wallet: Arc<AlpacaWalletService>,
        wallets: ChainWallets<Signer>,
        raindex: Arc<RaindexService<Signer>>,
        settlement: UsdcSettlementParams,
    ) -> Result<Self, SpawnRebalancerError> {
        let ChainWallets {
            ethereum: EthereumWallet(ethereum_wallet),
            base: BaseWallet(base_wallet),
        } = wallets;
        let cctp = Arc::new(
            CctpBridge::try_from_ctx(CctpCtx {
                usdc_ethereum: USDC_ETHEREUM,
                usdc_base: USDC_BASE,
                ethereum_wallet,
                base_wallet,
                #[cfg(feature = "test-support")]
                circle_api_base: settlement.circle_api_base.clone(),
                #[cfg(feature = "test-support")]
                token_messenger: settlement.token_messenger,
                #[cfg(feature = "test-support")]
                message_transmitter: settlement.message_transmitter,
            })
            .map_err(|error| SpawnRebalancerError::Cctp(Box::new(error)))?,
        );

        Ok(Self {
            broker,
            wallet,
            cctp,
            raindex,
            settlement,
        })
    }

    /// Builds the cross-venue cash transfer and returns its trait-erased
    /// resume entry points for the conductor's apalis job ctxs.
    ///
    /// The `UsdcRebalance` CQRS framework is created in the conductor and
    /// passed here to ensure single-instance initialization with all
    /// required query processors.
    pub(crate) fn into_usdc_transfer_handles(
        self,
        market_maker_wallet: Address,
        usdc_vault_id: RaindexVaultId,
        usdc: Arc<Store<UsdcRebalance>>,
        bot_gas_enqueuer: BotGasReceiptCostEnqueuer,
        gas_readiness: Arc<GasReadiness>,
    ) -> UsdcTransferResumeHandles {
        let usdc = Arc::new(
            CrossVenueCashTransfer::new(
                self.broker,
                self.wallet,
                self.cctp,
                self.raindex,
                usdc,
                market_maker_wallet,
                usdc_vault_id,
                &self.settlement,
            )
            .with_gas_readiness(gas_readiness)
            .with_bot_gas_enqueuer(bot_gas_enqueuer),
        );

        let resume_base_to_alpaca: Arc<dyn ResumeBaseToAlpaca> = usdc.clone();
        let resume_alpaca_to_base: Arc<dyn ResumeAlpacaToBase> = usdc;

        info!(target: "rebalance", "Rebalancing infrastructure initialized");

        UsdcTransferResumeHandles {
            resume_base_to_alpaca,
            resume_alpaca_to_base,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::inventory::PollFreshness;
    use alloy::network::Ethereum;
    use alloy::node_bindings::Anvil;
    use alloy::primitives::{B256, U256, address, b256};
    use alloy::providers::ext::AnvilApi as _;
    use alloy::providers::fillers::{
        BlobGasFiller, ChainIdFiller, FillProvider, GasFiller, JoinFill, NonceFiller,
    };
    use alloy::providers::{Identity, ProviderBuilder, RootProvider};
    use httpmock::Method::GET;
    use httpmock::MockServer;
    use serde_json::json;
    use std::collections::HashMap;
    use uuid::Uuid;

    use st0x_config::{ChainAssets, OperationMode, RebalancingCtx};
    use st0x_event_sorcery::test_store;
    use st0x_evm::Evm;
    use st0x_evm::local::RawPrivateKeyWallet;
    use st0x_evm::test_chain::evm_mapping_slot;
    use st0x_execution::{
        AlpacaAccountId, AlpacaBrokerApi, AlpacaBrokerApiCtx, AlpacaBrokerApiMode,
        AlpacaWalletService, Executor, Symbol, TimeInForce,
    };
    use st0x_float_macro::float;
    use st0x_raindex::RaindexContracts;
    use st0x_wrapper::WrappedEquity;

    use super::*;
    use crate::bindings::DeployableERC20;
    use crate::inventory::ImbalanceThreshold;
    use crate::rebalancing::RebalancingServiceConfig;
    use crate::rebalancing::usdc::UsdcSettlementParams;
    use crate::telemetry::TelemetrySender;
    use crate::test_utils::spawn_anvil;

    #[test]
    fn to_wrapped_equities_maps_underlying_and_derivative() {
        let underlying = Address::random();
        let derivative = Address::random();
        let symbol: Symbol = "AAPL".parse().unwrap();

        let mut config = HashMap::new();
        config.insert(
            symbol.clone(),
            ChainEquityAsset {
                tokenized_equity: underlying,
                tokenized_equity_derivative: derivative,
                vault_ids: Vec::new(),
                trading: OperationMode::Enabled,
                rebalancing: OperationMode::Disabled,
                wrapped_equity_recovery: OperationMode::Disabled,
                operational_limit: None,
            },
        );

        let wrapped = to_wrapped_equities(&config);

        assert_eq!(
            wrapped.get(&symbol),
            Some(&WrappedEquity {
                underlying,
                derivative,
            }),
        );
    }

    type BaseProvider = FillProvider<
        JoinFill<
            Identity,
            JoinFill<GasFiller, JoinFill<BlobGasFiller, JoinFill<NonceFiller, ChainIdFiller>>>,
        >,
        RootProvider<Ethereum>,
        Ethereum,
    >;

    const TEST_ORDERBOOK: Address = address!("0xabcdefabcdefabcdefabcdefabcdefabcdefabcd");

    fn make_ctx() -> RebalancingCtx {
        RebalancingCtx::stub()
            .equity(ImbalanceThreshold {
                target: float!(0.5),
                deviation: float!(0.2),
            })
            .usdc(ImbalanceThreshold {
                target: float!(0.6),
                deviation: float!(0.15),
            })
            .call()
    }

    fn mock_alpaca_account(server: &MockServer) -> (AlpacaAccountId, httpmock::Mock<'_>) {
        let account_id = AlpacaAccountId::new(Uuid::nil());
        let account_mock = server.mock(|when, then| {
            when.method(GET)
                .path(format!("/v1/trading/accounts/{account_id}/account",));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "id": account_id.to_string(),
                    "status": "ACTIVE"
                }));
        });

        (account_id, account_mock)
    }

    async fn make_mock_alpaca_services(
        server: &MockServer,
        account_id: AlpacaAccountId,
    ) -> (InstrumentedAlpacaBroker, Arc<AlpacaWalletService>) {
        let broker_auth = AlpacaBrokerApiCtx {
            auth: st0x_execution::AlpacaBrokerAuth::Basic {
                api_key: "test_key".to_string(),
                api_secret: "test_secret".to_string(),
            },
            account_id,
            mode: Some(AlpacaBrokerApiMode::Mock(server.base_url())),
            asset_cache_ttl: std::time::Duration::from_secs(3600),
            time_in_force: TimeInForce::default(),
            counter_trade_slippage_bps: st0x_execution::DEFAULT_ALPACA_COUNTER_TRADE_SLIPPAGE_BPS,
        };
        let broker = InstrumentedAlpacaBroker::new(
            AlpacaBrokerApi::try_from_ctx(broker_auth)
                .await
                .expect("Failed to create test broker API"),
            TelemetrySender::disabled(),
        );
        let wallet = Arc::new(
            AlpacaWalletService::new(
                server.base_url(),
                account_id,
                st0x_execution::AlpacaBrokerAuth::Basic {
                    api_key: "test_key".to_string(),
                    api_secret: "test_secret".to_string(),
                },
            )
            .unwrap(),
        );

        (broker, wallet)
    }

    fn make_test_settlement(rebalancing_ctx: &RebalancingCtx) -> UsdcSettlementParams {
        UsdcSettlementParams {
            attestation_retry_deadline: rebalancing_ctx.attestation_retry_deadline,
            required_confirmations: 0,
            reserved_cash: None,
            #[cfg(feature = "test-support")]
            circle_api_base: st0x_bridge::cctp::CIRCLE_API_BASE.to_string(),
            #[cfg(feature = "test-support")]
            token_messenger: st0x_bridge::cctp::TOKEN_MESSENGER_V2,
            #[cfg(feature = "test-support")]
            message_transmitter: st0x_bridge::cctp::MESSAGE_TRANSMITTER_V2,
        }
    }

    #[test]
    fn trigger_config_uses_equity_from_ctx() {
        let ctx = make_ctx();

        let trigger_config = RebalancingServiceConfig {
            poll_freshness: PollFreshness::always_fresh(),
            inventory_staleness_bound: std::time::Duration::from_secs(300),
            cash_reserved: None,
            equity: ctx.equity,
            usdc: ctx.usdc,
            transfer_timeout: ctx.transfer_timeout,
            assets: ChainAssets::default(),
        };

        assert!(trigger_config.equity.target.eq(float!(0.5)).unwrap());
        assert!(trigger_config.equity.deviation.eq(float!(0.2)).unwrap());
    }

    #[test]
    fn trigger_config_uses_usdc_from_ctx() {
        let ctx = make_ctx();

        let trigger_config = RebalancingServiceConfig {
            poll_freshness: PollFreshness::always_fresh(),
            inventory_staleness_bound: std::time::Duration::from_secs(300),
            cash_reserved: None,
            equity: ctx.equity,
            usdc: ctx.usdc,
            transfer_timeout: ctx.transfer_timeout,
            assets: ChainAssets::default(),
        };

        let usdc_threshold = trigger_config.usdc.expect("USDC threshold should be Some");
        assert!(usdc_threshold.target.eq(float!(0.6)).unwrap());
        assert!(usdc_threshold.deviation.eq(float!(0.15)).unwrap());
    }

    async fn make_services_with_mock_wallet(
        server: &httpmock::MockServer,
    ) -> (
        RebalancerServices<RawPrivateKeyWallet<BaseProvider>>,
        RebalancingCtx,
    ) {
        let anvil = spawn_anvil(Anvil::new());
        let base_provider = ProviderBuilder::new().connect_http(anvil.endpoint_url());

        let rebalancing_ctx = make_ctx();
        let (account_id, _account_mock) = mock_alpaca_account(server);

        let evm_private_key =
            b256!("0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef");

        let base_wallet =
            RawPrivateKeyWallet::new(&evm_private_key, base_provider.clone(), 1).unwrap();
        let ethereum_wallet =
            RawPrivateKeyWallet::new(&evm_private_key, base_provider.clone(), 1).unwrap();

        let (broker, wallet) = make_mock_alpaca_services(server, account_id).await;

        let cctp = Arc::new(
            CctpBridge::try_from_ctx(CctpCtx {
                usdc_ethereum: USDC_ETHEREUM,
                usdc_base: USDC_BASE,
                ethereum_wallet,
                base_wallet: base_wallet.clone(),
                #[cfg(feature = "test-support")]
                circle_api_base: st0x_bridge::cctp::CIRCLE_API_BASE.to_string(),
                #[cfg(feature = "test-support")]
                token_messenger: st0x_bridge::cctp::TOKEN_MESSENGER_V2,
                #[cfg(feature = "test-support")]
                message_transmitter: st0x_bridge::cctp::MESSAGE_TRANSMITTER_V2,
            })
            .unwrap(),
        );

        let owner = base_wallet.address();
        let raindex = Arc::new(RaindexService::new(
            base_wallet,
            RaindexContracts {
                inventory: TEST_ORDERBOOK,
                orderbook: TEST_ORDERBOOK,
            },
            owner,
        ));

        let services = RebalancerServices {
            broker,
            wallet,
            cctp,
            raindex,
            settlement: make_test_settlement(&rebalancing_ctx),
        };

        (services, rebalancing_ctx)
    }

    #[tokio::test]
    async fn new_maps_ethereum_wallet_to_ethereum_cctp_endpoint() {
        let server = MockServer::start();
        let ethereum_anvil = spawn_anvil(Anvil::new());
        let base_anvil = spawn_anvil(Anvil::new());
        let ethereum_provider = ProviderBuilder::new().connect_http(ethereum_anvil.endpoint_url());
        let base_provider = ProviderBuilder::new().connect_http(base_anvil.endpoint_url());

        let evm_private_key =
            b256!("0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef");
        let ethereum_wallet =
            RawPrivateKeyWallet::new(&evm_private_key, ethereum_provider, 1).unwrap();
        let base_wallet = RawPrivateKeyWallet::new(&evm_private_key, base_provider, 1).unwrap();
        let ethereum_holder = ethereum_wallet.address();

        // Install a callable USDC contract only on Ethereum. If the named
        // wallet fields are reversed while constructing CctpCtx, this lookup
        // fails against Base instead of returning the known balance.
        ethereum_wallet
            .provider()
            .anvil_set_code(USDC_ETHEREUM, DeployableERC20::DEPLOYED_BYTECODE.clone())
            .await
            .unwrap();
        let expected_balance = U256::from(123_456u64);
        ethereum_wallet
            .provider()
            .anvil_set_storage_at(
                USDC_ETHEREUM,
                evm_mapping_slot(ethereum_holder, 0),
                expected_balance.into(),
            )
            .await
            .unwrap();

        let (account_id, _account_mock) = mock_alpaca_account(&server);

        let rebalancing_ctx = make_ctx();
        let (broker, wallet) = make_mock_alpaca_services(&server, account_id).await;
        let wallets = ChainWallets {
            ethereum: EthereumWallet(ethereum_wallet),
            base: BaseWallet(base_wallet.clone()),
        };
        let raindex = Arc::new(RaindexService::new(
            base_wallet,
            RaindexContracts {
                inventory: TEST_ORDERBOOK,
                orderbook: TEST_ORDERBOOK,
            },
            Address::random(),
        ));

        let services = RebalancerServices::new(
            broker,
            wallet,
            wallets,
            raindex,
            make_test_settlement(&rebalancing_ctx),
        )
        .unwrap();

        assert_eq!(
            services
                .cctp
                .ethereum_usdc_balance(ethereum_holder)
                .await
                .unwrap(),
            expected_balance
        );
    }

    #[tokio::test]
    async fn into_usdc_transfer_handles_produces_resume_handles() {
        let server = MockServer::start();
        let (services, _ctx) = make_services_with_mock_wallet(&server).await;

        let pool = crate::test_utils::setup_test_db().await;
        let usdc_store = Arc::new(test_store(pool, ()));

        let UsdcTransferResumeHandles {
            resume_base_to_alpaca: _,
            resume_alpaca_to_base: _,
        } = services.into_usdc_transfer_handles(
            Address::random(),
            RaindexVaultId(B256::ZERO),
            usdc_store,
            BotGasReceiptCostEnqueuer::Disabled,
            crate::native_gas::GasReadiness::always_ready_for_test(),
        );
    }
}

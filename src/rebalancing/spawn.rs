//! Builds the rebalancing transfer infrastructure.

use alloy::primitives::Address;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::info;

use st0x_bridge::cctp::{CctpBridge, CctpCtx, CctpError};
use st0x_config::EquityAssetConfig;
use st0x_event_sorcery::Store;
use st0x_evm::{USDC_BASE, USDC_ETHEREUM, Wallet};
use st0x_execution::{AlpacaWalletService, EmptySymbolError, Symbol};
use st0x_raindex::{RaindexService, RaindexVaultId};
use st0x_wrapper::WrappedEquity;

use super::usdc::{
    CrossVenueCashTransfer, ResumeAlpacaToBase, ResumeBaseToAlpaca, UsdcSettlementParams,
};
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
/// [`WrapperService`] needs, keeping `st0x-wrapper` independent of `st0x-config`.
pub(crate) fn to_wrapped_equities(
    equities: &HashMap<Symbol, EquityAssetConfig>,
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
/// can build apalis job ctxs without leaking the wallet `Chain` generic
/// upstream.
pub(crate) struct UsdcTransferResumeHandles {
    pub(crate) resume_base_to_alpaca: Arc<dyn ResumeBaseToAlpaca>,
    pub(crate) resume_alpaca_to_base: Arc<dyn ResumeAlpacaToBase>,
}

/// External service clients for rebalancing operations.
///
/// Holds connections to Alpaca APIs, CCTP bridge, and vault services.
/// Providers for both chains are obtained from the wallets on `RebalancingCtx`.
pub(crate) struct RebalancerServices<Chain: Wallet> {
    broker: InstrumentedAlpacaBroker,
    wallet: Arc<AlpacaWalletService>,
    cctp: Arc<CctpBridge<Chain, Chain>>,
    raindex: Arc<RaindexService<Chain>>,
    settlement: UsdcSettlementParams,
}

impl<Chain: Wallet + Clone> RebalancerServices<Chain> {
    /// Creates the services needed for rebalancing.
    ///
    /// RaindexService is passed in rather than created here because it is
    /// needed for CQRS framework initialization in the conductor, which
    /// must happen before this constructor is called.
    pub(crate) fn new(
        broker: InstrumentedAlpacaBroker,
        wallet: Arc<AlpacaWalletService>,
        ethereum_wallet: Chain,
        base_wallet: Chain,
        raindex: Arc<RaindexService<Chain>>,
        settlement: UsdcSettlementParams,
    ) -> Result<Self, SpawnRebalancerError> {
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
    ) -> UsdcTransferResumeHandles {
        let usdc = Arc::new(CrossVenueCashTransfer::new(
            self.broker,
            self.wallet,
            self.cctp,
            self.raindex,
            usdc,
            market_maker_wallet,
            usdc_vault_id,
            &self.settlement,
        ));

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
    use alloy::network::Ethereum;
    use alloy::node_bindings::Anvil;
    use alloy::primitives::{B256, address, b256};
    use alloy::providers::fillers::{
        BlobGasFiller, ChainIdFiller, FillProvider, GasFiller, JoinFill, NonceFiller,
    };
    use alloy::providers::{Identity, ProviderBuilder, RootProvider};
    use httpmock::Method::GET;
    use httpmock::MockServer;
    use serde_json::json;
    use std::collections::HashMap;
    use uuid::Uuid;

    use st0x_config::{AssetsConfig, EquitiesConfig, OperationMode, RebalancingCtx};
    use st0x_event_sorcery::test_store;
    use st0x_evm::local::RawPrivateKeyWallet;
    use st0x_execution::{
        AlpacaAccountId, AlpacaBrokerApi, AlpacaBrokerApiCtx, AlpacaBrokerApiMode,
        AlpacaWalletService, Executor, Symbol, TimeInForce,
    };
    use st0x_float_macro::float;
    use st0x_wrapper::WrappedEquity;

    use super::*;
    use crate::inventory::ImbalanceThreshold;
    use crate::rebalancing::RebalancingServiceConfig;
    use crate::rebalancing::usdc::UsdcSettlementParams;
    use crate::telemetry::TelemetrySender;

    #[test]
    fn to_wrapped_equities_maps_underlying_and_derivative() {
        let underlying = Address::random();
        let derivative = Address::random();
        let symbol: Symbol = "AAPL".parse().unwrap();

        let mut config = HashMap::new();
        config.insert(
            symbol.clone(),
            EquityAssetConfig {
                tokenized_equity: underlying,
                tokenized_equity_derivative: derivative,
                pyth_feed_id: None,
                vault_ids: Vec::new(),
                trading: OperationMode::Enabled,
                rebalancing: OperationMode::Disabled,
                wrapped_equity_recovery: OperationMode::Disabled,
                extended_hours_counter_trading: OperationMode::Disabled,
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

    #[test]
    fn trigger_config_uses_equity_from_ctx() {
        let ctx = make_ctx();

        let trigger_config = RebalancingServiceConfig {
            equity: ctx.equity,
            usdc: ctx.usdc,
            transfer_timeout: ctx.transfer_timeout,
            assets: AssetsConfig {
                equities: EquitiesConfig::default(),
                cash: None,
            },
        };

        assert!(trigger_config.equity.target.eq(float!(0.5)).unwrap());
        assert!(trigger_config.equity.deviation.eq(float!(0.2)).unwrap());
    }

    #[test]
    fn trigger_config_uses_usdc_from_ctx() {
        let ctx = make_ctx();

        let trigger_config = RebalancingServiceConfig {
            equity: ctx.equity,
            usdc: ctx.usdc,
            transfer_timeout: ctx.transfer_timeout,
            assets: AssetsConfig {
                equities: EquitiesConfig::default(),
                cash: None,
            },
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
        let anvil = Anvil::new().spawn();
        let base_provider = ProviderBuilder::new().connect_http(anvil.endpoint_url());

        let rebalancing_ctx = make_ctx();
        let account_id = AlpacaAccountId::new(Uuid::nil());

        let evm_private_key =
            b256!("0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef");

        let base_wallet =
            RawPrivateKeyWallet::new(&evm_private_key, base_provider.clone(), 1).unwrap();
        let ethereum_wallet =
            RawPrivateKeyWallet::new(&evm_private_key, base_provider.clone(), 1).unwrap();

        let _account_mock = server.mock(|when, then| {
            when.method(GET)
                .path(format!("/v1/trading/accounts/{account_id}/account",));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "id": account_id.to_string(),
                    "status": "ACTIVE"
                }));
        });

        let broker_auth = AlpacaBrokerApiCtx {
            api_key: "test_key".to_string(),
            api_secret: "test_secret".to_string(),
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

        let wallet = Arc::new(AlpacaWalletService::new(
            server.base_url(),
            account_id,
            "test_key".into(),
            "test_secret".into(),
        ));

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
        let raindex = Arc::new(RaindexService::new(base_wallet, TEST_ORDERBOOK, owner));

        let services = RebalancerServices {
            broker,
            wallet,
            cctp,
            raindex,
            settlement: UsdcSettlementParams {
                attestation_retry_deadline: rebalancing_ctx.attestation_retry_deadline,
                required_confirmations: 0,
                #[cfg(feature = "test-support")]
                circle_api_base: st0x_bridge::cctp::CIRCLE_API_BASE.to_string(),
                #[cfg(feature = "test-support")]
                token_messenger: st0x_bridge::cctp::TOKEN_MESSENGER_V2,
                #[cfg(feature = "test-support")]
                message_transmitter: st0x_bridge::cctp::MESSAGE_TRANSMITTER_V2,
            },
        };

        (services, rebalancing_ctx)
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
        );
    }
}

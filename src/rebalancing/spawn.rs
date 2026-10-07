//! Builds the rebalancing transfer infrastructure.

use alloy::providers::RootProvider;
use sqlx::SqlitePool;
use std::collections::{BTreeMap, HashMap};
use std::hash::BuildHasher;
use std::sync::Arc;
use tracing::info;

use st0x_bridge::cctp::{CctpBridge, CctpCorridor, CctpCtx, CctpError};
use st0x_bridge::corridor::{HopKind, UsdcCorridor};
use st0x_bridge::relay::{RelayBridge, RelayBridgeError, RelayClient, RelayCtx, RelayError};
use st0x_config::{ChainEquityAsset, OnchainWalletCtx, RelayHopCtx};
use st0x_event_sorcery::Store;
use st0x_evm::{Chain, Wallet};
use st0x_execution::{AlpacaWalletService, EmptySymbolError, Symbol};
use st0x_raindex::{RaindexContracts, RaindexService, RaindexVaultId};
use st0x_wrapper::WrappedEquity;

use super::usdc::{
    CorridorTransfer, CrossVenueCashTransfer, EthereumChainMissing, MarketMakingUsdcEndpoints,
    RecheckUsdcDeposit, RecoverCctpMint, RelayHop, RestorePreparedDepositSends, ResumeAlpacaToBase,
    ResumeBaseToAlpaca, UsdcBridgeHelper, UsdcCorridorTransfers, UsdcDriverGate,
    UsdcSettlementParams,
};
use crate::bot_gas::BotGasReceiptCostEnqueuer;
use crate::native_gas::{ConfiguredGasReadiness, GasReadiness};
use crate::telemetry::broker::InstrumentedAlpacaBroker;
use crate::usdc_rebalance::UsdcRebalance;

/// Errors that can occur when spawning the rebalancer.
#[derive(Debug, thiserror::Error)]
pub(crate) enum SpawnRebalancerError {
    #[error("failed to create CCTP bridge: {0}")]
    Cctp(#[from] Box<CctpError>),
    #[error("failed to create the Relay bridge: {0}")]
    RelayBridge(#[from] Box<RelayBridgeError>),
    #[error("failed to create the Relay API client: {0}")]
    RelayClient(#[from] Box<RelayError>),
    #[error("the {corridor} corridor has no Relay bounds or chain confirmations")]
    RelayHopUnconfigured { corridor: UsdcCorridor },
    #[error(transparent)]
    EthereumChainMissing(#[from] EthereumChainMissing),
    #[error("failed to create wrapper service: {0}")]
    Wrapper(#[from] EmptySymbolError),
    #[error("no cash transfer service can be built for the {corridor} corridor")]
    UnwiredCorridor { corridor: UsdcCorridor },
    #[error("no USDC corridor is served, so no cash transfer service can be built")]
    NoCorridor,
    #[error("the {corridor} corridor is listed twice")]
    DuplicateCorridor { corridor: UsdcCorridor },
    #[error(
        "the {corridor} corridor runs on {chain}, where another served corridor already runs: \
         {chain} has one USDC gas check"
    )]
    SharedCorridorChain {
        chain: Chain,
        corridor: UsdcCorridor,
    },
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
    /// Operator `transfer recheck` entry point for a failed USDC deposit,
    /// published on the recovery handle rather than a job ctx.
    pub(crate) recheck_deposit: Arc<dyn RecheckUsdcDeposit>,
    /// Startup hook reserving the nonces of persisted signed deposit sends.
    pub(crate) restore_deposit_sends: Arc<dyn RestorePreparedDepositSends>,
    /// Operator `cctp complete-mint` entry point, published on the recovery
    /// handle rather than a job ctx.
    pub(crate) recover_cctp_mint: Arc<dyn RecoverCctpMint>,
}

/// Where one served cash corridor's transfers run: the signer, orderbook
/// contracts and cash vault of its chain.
pub(crate) struct UsdcCorridorEndpoints<Signer> {
    pub(crate) corridor: UsdcCorridor,
    pub(crate) chain_wallet: Signer,
    pub(crate) contracts: RaindexContracts,
    pub(crate) vault_id: RaindexVaultId,
    /// The USDC route's gas check: this chain's wallet and the Ethereum hub.
    pub(crate) gas_readiness: Arc<GasReadiness>,
}

/// Each served corridor's gas check, keyed by its chain as the transfer
/// admission guard looks it up. A second corridor on one chain would replace
/// the first's check, so it is refused by name.
pub(crate) fn usdc_gas_readiness_by_chain<Signer>(
    corridors: &[UsdcCorridorEndpoints<Signer>],
) -> Result<BTreeMap<Chain, ConfiguredGasReadiness>, SpawnRebalancerError> {
    let mut by_chain = BTreeMap::new();

    for endpoints in corridors {
        let chain = endpoints.corridor.chain();
        let readiness = ConfiguredGasReadiness::Wired(endpoints.gas_readiness.clone());

        if by_chain.insert(chain, readiness).is_some() {
            return Err(SpawnRebalancerError::SharedCorridorChain {
                chain,
                corridor: endpoints.corridor,
            });
        }
    }

    Ok(by_chain)
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

/// What a Relay corridor's hop is built from: the corridor's bounds and the
/// confirmations its chain requires.
#[derive(Debug, Clone, Copy)]
pub(crate) struct RelayHopSetup {
    pub(crate) bounds: RelayHopCtx,
    pub(crate) chain_confirmations: u64,
}

/// External service clients for rebalancing operations: the Alpaca broker
/// and wallet every cash corridor shares, the Ethereum hub wallet, the CCTP
/// pair a CCTP corridor bridges over, and each Relay corridor's setup.
pub(crate) struct RebalancerServices<Signer: Wallet> {
    broker: InstrumentedAlpacaBroker,
    wallet: Arc<AlpacaWalletService>,
    /// The one Ethereum wallet every hop signs hub sends on: each bridge
    /// gets a clone, which shares its nonce reservations and in-flight
    /// record, so a Relay pair and a deposit send never take one nonce.
    ethereum_wallet: Signer,
    cctp_corridor: CctpCorridor,
    /// Settlement tuning shared by every corridor.
    settlement: UsdcSettlementParams,
    relay_hops: BTreeMap<Chain, RelayHopSetup>,
}

impl<Signer: Wallet + Clone + 'static> RebalancerServices<Signer> {
    pub(crate) fn new(
        broker: InstrumentedAlpacaBroker,
        wallet: Arc<AlpacaWalletService>,
        ethereum_wallet: Signer,
        cctp_corridor: CctpCorridor,
        settlement: UsdcSettlementParams,
        relay_hops: BTreeMap<Chain, RelayHopSetup>,
    ) -> Self {
        Self {
            broker,
            wallet,
            ethereum_wallet,
            cctp_corridor,
            settlement,
            relay_hops,
        }
    }

    /// Builds one cross-venue cash transfer per served corridor, each on its
    /// own chain's orderbook, vault, wallet and gas check, and
    /// returns the trait-erased entry points of the dispatcher that routes
    /// every call to the transfer's corridor.
    ///
    /// The `UsdcRebalance` CQRS framework is created in the conductor and
    /// passed here to ensure single-instance initialization with all
    /// required query processors.
    pub(crate) fn into_usdc_corridor_transfers(
        self,
        corridors: Vec<UsdcCorridorEndpoints<Signer>>,
        usdc: &Arc<Store<UsdcRebalance>>,
        pool: &SqlitePool,
        bot_gas_enqueuer: &BotGasReceiptCostEnqueuer,
        driver_gate: &UsdcDriverGate,
    ) -> Result<UsdcTransferResumeHandles, SpawnRebalancerError> {
        // Every service signs deposit sends on the one Ethereum wallet.
        let deposit_send_lock = Arc::new(tokio::sync::Mutex::new(()));
        let mut by_corridor = BTreeMap::new();
        for endpoints in corridors {
            let corridor = endpoints.corridor;
            let shared = SharedTransferParts {
                usdc,
                pool,
                bot_gas_enqueuer,
                driver_gate,
                deposit_send_lock: &deposit_send_lock,
            };
            let transfer = self.corridor_transfer(endpoints, &shared)?;

            if by_corridor.insert(corridor, transfer).is_some() {
                return Err(SpawnRebalancerError::DuplicateCorridor { corridor });
            }
            info!(target: "rebalance", %corridor, "Cash transfer service built");
        }

        let transfers = Arc::new(
            UsdcCorridorTransfers::new(by_corridor, usdc.clone())
                .ok_or(SpawnRebalancerError::NoCorridor)?,
        );

        Ok(UsdcTransferResumeHandles {
            resume_base_to_alpaca: transfers.clone(),
            resume_alpaca_to_base: transfers.clone(),
            recheck_deposit: transfers.clone(),
            restore_deposit_sends: transfers.clone(),
            recover_cctp_mint: transfers,
        })
    }

    /// The cash transfer service of one corridor, on the bridge its hop
    /// takes: CCTP for Base, Relay for a chain with a Relay depository. Any
    /// other corridor refuses startup by name.
    fn corridor_transfer(
        &self,
        endpoints: UsdcCorridorEndpoints<Signer>,
        shared: &SharedTransferParts<'_>,
    ) -> Result<Arc<dyn CorridorTransfer>, SpawnRebalancerError> {
        let corridor = endpoints.corridor;
        let chain_wallet = endpoints.chain_wallet.clone();

        match corridor {
            UsdcCorridor::HubRouted {
                chain: Chain::Base,
                hop: HopKind::Cctp,
            } => {
                let bridge = Arc::new(self.cctp_bridge(chain_wallet)?);
                Ok(Arc::new(self.cash_transfer(bridge, endpoints, shared)))
            }
            UsdcCorridor::HubRouted {
                chain,
                hop: HopKind::Relay,
            } => {
                let hop = Arc::new(self.relay_hop(corridor, chain, chain_wallet)?);
                Ok(Arc::new(self.cash_transfer(hop, endpoints, shared)))
            }
            UsdcCorridor::HubRouted {
                chain: Chain::Ethereum | Chain::HyperEvm | Chain::Robinhood,
                hop: HopKind::Cctp,
            } => Err(SpawnRebalancerError::UnwiredCorridor { corridor }),
        }
    }

    /// One corridor's service over `hop`, on its chain's orderbook, vault,
    /// wallet and gas check.
    fn cash_transfer<Hop>(
        &self,
        hop: Arc<Hop>,
        endpoints: UsdcCorridorEndpoints<Signer>,
        shared: &SharedTransferParts<'_>,
    ) -> CrossVenueCashTransfer<Signer, Hop>
    where
        Hop: UsdcBridgeHelper,
    {
        let chain_wallet = endpoints.chain_wallet;
        let raindex = Arc::new(RaindexService::new(
            chain_wallet.clone(),
            endpoints.contracts,
            chain_wallet.address(),
        ));

        CrossVenueCashTransfer::new(
            self.broker.clone(),
            self.wallet.clone(),
            hop,
            raindex,
            shared.usdc.clone(),
            MarketMakingUsdcEndpoints::new(
                endpoints.corridor,
                chain_wallet.address(),
                endpoints.vault_id,
            ),
            &self.settlement,
            shared.bot_gas_enqueuer.clone(),
        )
        .with_gas_readiness(endpoints.gas_readiness)
        .with_credit_ledger(shared.pool.clone())
        .with_driver_gate(shared.driver_gate.clone())
        .with_deposit_send_lock(Arc::clone(shared.deposit_send_lock))
    }

    /// The CCTP pair of Ethereum and Base, on the shared Ethereum wallet.
    fn cctp_bridge(
        &self,
        base_wallet: Signer,
    ) -> Result<CctpBridge<Signer, Signer>, SpawnRebalancerError> {
        CctpBridge::try_from_ctx(CctpCtx {
            corridor: self.cctp_corridor,
            ethereum_wallet: self.ethereum_wallet.clone(),
            base_wallet,
            #[cfg(feature = "test-support")]
            circle_api_base: self.settlement.circle_api_base.clone(),
            #[cfg(feature = "test-support")]
            token_messenger: self.settlement.token_messenger,
            #[cfg(feature = "test-support")]
            message_transmitter: self.settlement.message_transmitter,
        })
        .map_err(|error| SpawnRebalancerError::Cctp(Box::new(error)))
    }

    /// The Relay hop between `chain` and the hub, on the shared Ethereum
    /// wallet. Quotes run on Relay's unauthenticated limits.
    fn relay_hop(
        &self,
        corridor: UsdcCorridor,
        chain: Chain,
        chain_wallet: Signer,
    ) -> Result<RelayHop<Signer>, SpawnRebalancerError> {
        let setup = self
            .relay_hops
            .get(&chain)
            .ok_or(SpawnRebalancerError::RelayHopUnconfigured { corridor })?;
        let ethereum_confirmations = self
            .settlement
            .ethereum_required_confirmations
            .ok_or(EthereumChainMissing)?;
        let hub_wallet = self.ethereum_wallet.address();

        let bridge = RelayBridge::try_from_ctx(RelayCtx {
            chain,
            ethereum_wallet: self.ethereum_wallet.clone(),
            chain_wallet,
            ethereum_confirmations,
            chain_confirmations: setup.chain_confirmations,
        })
        .map_err(Box::new)?;
        let client = RelayClient::new(None).map_err(Box::new)?;

        Ok(RelayHop::new(bridge, client, setup.bounds, hub_wallet))
    }
}

/// What every corridor's service shares: the store, the pool, the gas
/// enqueuer, the driver gate and the Ethereum wallet's deposit-send lock.
struct SharedTransferParts<'parts> {
    usdc: &'parts Arc<Store<UsdcRebalance>>,
    pool: &'parts SqlitePool,
    bot_gas_enqueuer: &'parts BotGasReceiptCostEnqueuer,
    driver_gate: &'parts UsdcDriverGate,
    deposit_send_lock: &'parts Arc<tokio::sync::Mutex<()>>,
}

#[cfg(test)]
mod tests {
    use crate::inventory::PollFreshness;
    use alloy::network::Ethereum;
    use alloy::node_bindings::Anvil;
    use alloy::primitives::{Address, B256, U256, address, b256};
    use alloy::providers::ext::AnvilApi as _;
    use alloy::providers::fillers::{
        BlobGasFiller, ChainIdFiller, FillProvider, GasFiller, JoinFill, NonceFiller,
    };
    use alloy::providers::{Identity, ProviderBuilder, RootProvider};
    use httpmock::Method::GET;
    use httpmock::MockServer;
    use serde_json::json;
    use std::collections::{BTreeMap, HashMap};
    use uuid::Uuid;

    use st0x_config::{AllocationCtx, OperationMode, RebalancingCtx, RebalancingMode};
    use st0x_event_sorcery::test_store;
    use st0x_evm::local::RawPrivateKeyWallet;
    use st0x_evm::test_chain::evm_mapping_slot;
    use st0x_evm::{Chain, Evm, PreparedTransaction, USDC_ETHEREUM};
    use st0x_execution::{
        AlpacaAccountId, AlpacaBrokerApi, AlpacaBrokerApiCtx, AlpacaBrokerApiMode,
        AlpacaWalletService, Executor, Symbol, TimeInForce,
    };
    use st0x_finance::Usdc;
    use st0x_float_macro::float;
    use st0x_raindex::RaindexContracts;
    use st0x_wrapper::WrappedEquity;

    use super::*;
    use crate::bindings::DeployableERC20;
    use crate::inventory::ImbalanceThreshold;
    use crate::rebalancing::RebalancingServiceConfig;
    use crate::rebalancing::usdc::{UsdcRecheckError, UsdcSettlementParams, UsdcTransferError};
    use crate::telemetry::TelemetrySender;
    use crate::test_utils::spawn_anvil;
    use crate::usdc_rebalance::{
        RebalanceDirection, UsdcRebalanceCommand, UsdcRebalanceId, record_swap_pair_for_test,
    };

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
                rebalancing: RebalancingMode::Disabled,
                wrapped_equity_recovery: OperationMode::Disabled,
                operational_limit: None,
                target_share: None,
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
            .allocation(AllocationCtx::base_test())
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
            hedge_floor: st0x_execution::HedgeFloor::default(),
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
            settlement_retry_deadline: rebalancing_ctx.settlement_retry_deadline,
            ethereum_required_confirmations: Some(1),
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
    fn trigger_config_uses_allocation_from_ctx() {
        let ctx = make_ctx();

        let trigger_config = RebalancingServiceConfig {
            poll_freshness: PollFreshness::always_fresh(),
            inventory_staleness_bound: std::time::Duration::from_secs(300),
            cash_reserved: None,
            hedge_floor: st0x_execution::HedgeFloor::default(),
            allocation: ctx.allocation.clone(),
            usdc: ctx.usdc,
            transfer_timeout: ctx.transfer_timeout,
            recovery_hold_alert_after: ctx.recovery_hold_alert_after,
            chains: BTreeMap::new(),
        };

        assert!(
            trigger_config.allocation.targets[&Chain::Base]
                .inner()
                .eq(float!(0.5))
                .unwrap()
        );
        assert!(
            trigger_config
                .allocation
                .deviation
                .inner()
                .eq(float!(0.2))
                .unwrap()
        );
    }

    #[test]
    fn trigger_config_uses_usdc_from_ctx() {
        let ctx = make_ctx();

        let trigger_config = RebalancingServiceConfig {
            poll_freshness: PollFreshness::always_fresh(),
            inventory_staleness_bound: std::time::Duration::from_secs(300),
            cash_reserved: None,
            hedge_floor: st0x_execution::HedgeFloor::default(),
            allocation: ctx.allocation.clone(),
            usdc: ctx.usdc,
            transfer_timeout: ctx.transfer_timeout,
            recovery_hold_alert_after: ctx.recovery_hold_alert_after,
            chains: BTreeMap::new(),
        };

        let usdc_threshold = trigger_config
            .usdc
            .active()
            .next()
            .expect("one active corridor");
        assert!(usdc_threshold.threshold.target.eq(float!(0.6)).unwrap());
        assert!(usdc_threshold.threshold.deviation.eq(float!(0.15)).unwrap());
    }

    async fn make_services_with_mock_wallet(
        server: &httpmock::MockServer,
    ) -> (
        RebalancerServices<RawPrivateKeyWallet<BaseProvider>>,
        RawPrivateKeyWallet<BaseProvider>,
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

        let services = RebalancerServices {
            broker,
            wallet,
            ethereum_wallet,
            cctp_corridor: CctpCorridor::ethereum_base().unwrap(),
            settlement: make_test_settlement(&rebalancing_ctx),
            relay_hops: robinhood_relay_hops(),
        };

        (services, base_wallet)
    }

    const ROBINHOOD_RELAY: UsdcCorridor = UsdcCorridor::HubRouted {
        chain: Chain::Robinhood,
        hop: HopKind::Relay,
    };

    fn robinhood_relay_hops() -> BTreeMap<Chain, RelayHopSetup> {
        BTreeMap::from([(
            Chain::Robinhood,
            RelayHopSetup {
                bounds: RelayHopCtx {
                    slippage_bps: 30,
                    max_quote_loss_bps: 50,
                    min_transfer: Usdc::new(float!(500)),
                    max_transfer: Usdc::new(float!(50000)),
                    quote_max_age: std::time::Duration::from_secs(60),
                    fill_timeout: std::time::Duration::from_secs(1800),
                    max_refund_retries: 3,
                    max_deposit_revert_redrives: 5,
                },
                chain_confirmations: 1,
            },
        )])
    }

    #[tokio::test]
    async fn base_cctp_bridge_maps_ethereum_wallet_to_ethereum_cctp_endpoint() {
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
        let services = RebalancerServices::new(
            broker,
            wallet,
            ethereum_wallet,
            rebalancing_ctx.cctp_corridor,
            make_test_settlement(&rebalancing_ctx),
            BTreeMap::new(),
        );

        let bridge = services.cctp_bridge(base_wallet).unwrap();

        assert_eq!(
            bridge.ethereum_usdc_balance(ethereum_holder).await.unwrap(),
            expected_balance
        );
    }

    fn corridor_endpoints(
        corridor: UsdcCorridor,
        chain_wallet: RawPrivateKeyWallet<BaseProvider>,
    ) -> UsdcCorridorEndpoints<RawPrivateKeyWallet<BaseProvider>> {
        UsdcCorridorEndpoints {
            corridor,
            chain_wallet,
            contracts: RaindexContracts {
                inventory: TEST_ORDERBOOK,
                orderbook: TEST_ORDERBOOK,
            },
            vault_id: RaindexVaultId(B256::ZERO),
            gas_readiness: crate::native_gas::GasReadiness::always_ready_for_test(),
        }
    }

    /// The handles reach the built corridor's service and refuse a corridor
    /// no service carries. Only Base via CCTP has a bridge today, so the
    /// routing between two built services is covered in `usdc::corridors`.
    #[tokio::test]
    async fn into_usdc_corridor_transfers_routes_served_and_refuses_unserved() {
        let server = MockServer::start();
        let (services, base_wallet) = make_services_with_mock_wallet(&server).await;

        let pool = crate::test_utils::setup_test_db().await;
        let usdc_store = Arc::new(test_store(pool.clone(), ()));

        let handles = services
            .into_usdc_corridor_transfers(
                vec![corridor_endpoints(UsdcCorridor::BASE_CCTP, base_wallet)],
                &usdc_store,
                &pool,
                &BotGasReceiptCostEnqueuer::Disabled,
                &UsdcDriverGate::unpaused(),
            )
            .unwrap();

        // A transfer mid-withdrawal on Base reaches the Base service, whose
        // recheck refuses its state without any chain or Alpaca call.
        let base_transfer = UsdcRebalanceId(Uuid::new_v4());
        usdc_store
            .send(
                &base_transfer,
                UsdcRebalanceCommand::BeginWithdrawal {
                    direction: RebalanceDirection::BaseToAlpaca,
                    corridor: UsdcCorridor::BASE_CCTP,
                    amount: Usdc::new(float!(1)),
                    from_block: 0,
                },
            )
            .await
            .unwrap();
        let error = handles
            .recheck_deposit
            .recheck_deposit(&base_transfer, None)
            .await
            .unwrap_err();
        assert!(
            matches!(error, UsdcRecheckError::NotDepositFailed { ref id, .. } if *id == base_transfer),
            "got {error:?}"
        );

        let unserved = UsdcCorridor::HubRouted {
            chain: Chain::Robinhood,
            hop: HopKind::Relay,
        };
        let error = handles
            .resume_alpaca_to_base
            .resume_alpaca_to_base(
                &UsdcRebalanceId(Uuid::new_v4()),
                Usdc::new(float!(1)),
                unserved,
            )
            .await
            .unwrap_err();
        assert!(
            matches!(
                error,
                UsdcTransferError::CorridorNotServed { requested, .. } if requested == unserved
            ),
            "got {error:?}"
        );
    }

    /// Robinhood via Relay gets its own cash transfer service, which the
    /// handles route its transfers to.
    #[tokio::test]
    async fn relay_corridor_builds_a_service() {
        let server = MockServer::start();
        let (services, chain_wallet) = make_services_with_mock_wallet(&server).await;
        let robinhood_relay = UsdcCorridor::HubRouted {
            chain: Chain::Robinhood,
            hop: HopKind::Relay,
        };

        let pool = crate::test_utils::setup_test_db().await;
        let usdc_store = Arc::new(test_store(pool.clone(), ()));

        let handles = services
            .into_usdc_corridor_transfers(
                vec![corridor_endpoints(robinhood_relay, chain_wallet)],
                &usdc_store,
                &pool,
                &BotGasReceiptCostEnqueuer::Disabled,
                &UsdcDriverGate::unpaused(),
            )
            .unwrap();

        let transfer = UsdcRebalanceId(Uuid::new_v4());
        usdc_store
            .send(
                &transfer,
                UsdcRebalanceCommand::BeginWithdrawal {
                    direction: RebalanceDirection::BaseToAlpaca,
                    corridor: robinhood_relay,
                    amount: Usdc::new(float!(1)),
                    from_block: 0,
                },
            )
            .await
            .unwrap();
        let error = handles
            .recheck_deposit
            .recheck_deposit(&transfer, None)
            .await
            .unwrap_err();
        assert!(
            matches!(error, UsdcRecheckError::NotDepositFailed { ref id, .. } if *id == transfer),
            "got {error:?}"
        );
    }

    /// The CCTP pair and a Relay hop built from one set of services sign hub
    /// sends on one Ethereum wallet record, so a Relay pair and a deposit send
    /// take consecutive nonces, never the same one.
    #[tokio::test]
    async fn relay_and_cctp_hub_sends_share_one_nonce_reservation() {
        let server = MockServer::start();
        let anvil = spawn_anvil(Anvil::new());
        let provider = ProviderBuilder::new().connect_http(anvil.endpoint_url());
        let evm_private_key =
            b256!("0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef");
        let ethereum_wallet =
            RawPrivateKeyWallet::new(&evm_private_key, provider.clone(), 1).unwrap();
        let chain_wallet = RawPrivateKeyWallet::new(&evm_private_key, provider.clone(), 1).unwrap();
        let holder = ethereum_wallet.address();
        let (account_id, _account_mock) = mock_alpaca_account(&server);
        let (broker, wallet) = make_mock_alpaca_services(&server, account_id).await;
        let rebalancing_ctx = make_ctx();
        let services = RebalancerServices::new(
            broker,
            wallet,
            ethereum_wallet,
            rebalancing_ctx.cctp_corridor,
            make_test_settlement(&rebalancing_ctx),
            robinhood_relay_hops(),
        );

        provider
            .anvil_set_code(USDC_ETHEREUM, DeployableERC20::DEPLOYED_BYTECODE.clone())
            .await
            .unwrap();
        provider
            .anvil_set_storage_at(
                USDC_ETHEREUM,
                evm_mapping_slot(holder, 0),
                U256::from(10_000_000u64).into(),
            )
            .await
            .unwrap();

        let cctp = services.cctp_bridge(chain_wallet.clone()).unwrap();
        let relay = services
            .relay_hop(ROBINHOOD_RELAY, Chain::Robinhood, chain_wallet)
            .unwrap();
        let recipient = Address::repeat_byte(0xA1);

        let deposit_send = cctp
            .prepare_usdc_on_ethereum(recipient, U256::from(1u64))
            .await
            .unwrap();
        let relay_send = relay
            .prepare_usdc_on_ethereum(recipient, U256::from(1u64))
            .await
            .unwrap();

        assert_eq!(relay_send.nonce(), deposit_send.nonce() + 1);
    }

    /// The pair a Relay swap signed on the Ethereum wallet is restored once,
    /// by the hub restore, however many services run; the pair signed on
    /// Robinhood is restored by the Relay service alone.
    #[tokio::test]
    async fn prepared_pair_restored_once_on_startup() {
        let server = MockServer::start();
        let (services, chain_wallet) = make_services_with_mock_wallet(&server).await;
        let pool = crate::test_utils::setup_test_db().await;
        let usdc_store = Arc::new(test_store(pool.clone(), ()));

        let handles = services
            .into_usdc_corridor_transfers(
                vec![
                    corridor_endpoints(UsdcCorridor::BASE_CCTP, chain_wallet.clone()),
                    corridor_endpoints(ROBINHOOD_RELAY, chain_wallet),
                ],
                &usdc_store,
                &pool,
                &BotGasReceiptCostEnqueuer::Disabled,
                &UsdcDriverGate::unpaused(),
            )
            .unwrap();

        record_swap_pair_for_test(
            &usdc_store,
            &UsdcRebalanceId(Uuid::new_v4()),
            RebalanceDirection::AlpacaToBase,
            ROBINHOOD_RELAY,
            Some(PreparedTransaction::for_test(B256::repeat_byte(0xe1), 50)),
            PreparedTransaction::for_test(B256::repeat_byte(0xe2), 51),
        )
        .await;
        record_swap_pair_for_test(
            &usdc_store,
            &UsdcRebalanceId(Uuid::new_v4()),
            RebalanceDirection::BaseToAlpaca,
            ROBINHOOD_RELAY,
            Some(PreparedTransaction::for_test(B256::repeat_byte(0xc1), 60)),
            PreparedTransaction::for_test(B256::repeat_byte(0xc2), 61),
        )
        .await;

        let hub = handles
            .restore_deposit_sends
            .restore_prepared_deposit_sends(&pool)
            .await;
        let by_chain = handles
            .restore_deposit_sends
            .restore_chain_signed_swaps(&pool)
            .await;

        assert_eq!(hub.restored, 2, "the Ethereum-signed pair, restored once");
        assert_eq!(
            by_chain.keys().copied().collect::<Vec<_>>(),
            vec![Chain::Robinhood]
        );
        assert_eq!(by_chain[&Chain::Robinhood].restored, 2);
    }

    /// A corridor with no bridge wired refuses startup by name rather than
    /// building a transfer that could never move cash.
    #[tokio::test]
    async fn corridor_without_a_bridge_refuses_startup_by_name() {
        let server = MockServer::start();
        let (services, chain_wallet) = make_services_with_mock_wallet(&server).await;
        let corridor = UsdcCorridor::HubRouted {
            chain: Chain::Robinhood,
            hop: HopKind::Cctp,
        };

        let pool = crate::test_utils::setup_test_db().await;
        let usdc_store = Arc::new(test_store(pool.clone(), ()));

        let Err(error) = services.into_usdc_corridor_transfers(
            vec![corridor_endpoints(corridor, chain_wallet)],
            &usdc_store,
            &pool,
            &BotGasReceiptCostEnqueuer::Disabled,
            &UsdcDriverGate::unpaused(),
        ) else {
            panic!("a corridor without a bridge must be refused");
        };

        assert!(
            matches!(
                error,
                SpawnRebalancerError::UnwiredCorridor { corridor: refused } if refused == corridor
            ),
            "got {error:?}"
        );
    }

    /// A corridor listed twice would silently replace its first service, so
    /// startup refuses it by name.
    #[tokio::test]
    async fn duplicate_corridor_refuses_startup_by_name() {
        let server = MockServer::start();
        let (services, chain_wallet) = make_services_with_mock_wallet(&server).await;
        let pool = crate::test_utils::setup_test_db().await;
        let usdc_store = Arc::new(test_store(pool.clone(), ()));

        let Err(error) = services.into_usdc_corridor_transfers(
            vec![
                corridor_endpoints(UsdcCorridor::BASE_CCTP, chain_wallet.clone()),
                corridor_endpoints(UsdcCorridor::BASE_CCTP, chain_wallet),
            ],
            &usdc_store,
            &pool,
            &BotGasReceiptCostEnqueuer::Disabled,
            &UsdcDriverGate::unpaused(),
        ) else {
            panic!("a corridor listed twice must be refused");
        };

        assert!(
            matches!(
                error,
                SpawnRebalancerError::DuplicateCorridor {
                    corridor: UsdcCorridor::BASE_CCTP
                }
            ),
            "got {error:?}"
        );
    }

    /// The admission guard keeps one gas check per chain, so a second served
    /// corridor on a chain is refused rather than replacing the first's check.
    #[tokio::test]
    async fn second_corridor_on_one_chain_refuses_its_gas_check() {
        let server = MockServer::start();
        let (_services, chain_wallet) = make_services_with_mock_wallet(&server).await;
        let base_relay = UsdcCorridor::HubRouted {
            chain: Chain::Base,
            hop: HopKind::Relay,
        };

        let Err(error) = usdc_gas_readiness_by_chain(&[
            corridor_endpoints(UsdcCorridor::BASE_CCTP, chain_wallet.clone()),
            corridor_endpoints(base_relay, chain_wallet),
        ]) else {
            panic!("a second corridor on one chain must be refused");
        };

        assert!(
            matches!(
                error,
                SpawnRebalancerError::SharedCorridorChain {
                    chain: Chain::Base,
                    corridor,
                } if corridor == base_relay
            ),
            "got {error:?}"
        );
    }

    /// With no corridor served there is no service to restore the signed
    /// deposit sends on, so startup refuses rather than skipping the restore.
    #[tokio::test]
    async fn empty_corridor_list_refuses_startup() {
        let server = MockServer::start();
        let (services, _chain_wallet) = make_services_with_mock_wallet(&server).await;
        let pool = crate::test_utils::setup_test_db().await;
        let usdc_store = Arc::new(test_store(pool.clone(), ()));

        let Err(error) = services.into_usdc_corridor_transfers(
            Vec::new(),
            &usdc_store,
            &pool,
            &BotGasReceiptCostEnqueuer::Disabled,
            &UsdcDriverGate::unpaused(),
        ) else {
            panic!("an empty corridor list must be refused");
        };

        assert!(
            matches!(error, SpawnRebalancerError::NoCorridor),
            "got {error:?}"
        );
    }
}

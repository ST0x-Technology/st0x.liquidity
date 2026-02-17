//! Spawns the rebalancing infrastructure.

use alloy::network::{Ethereum, EthereumWallet};
use alloy::primitives::Address;
use alloy::providers::fillers::{
    BlobGasFiller, ChainIdFiller, FillProvider, GasFiller, JoinFill, NonceFiller, WalletFiller,
};
use alloy::providers::{Identity, Provider, ProviderBuilder, RootProvider};
use alloy::signers::local::PrivateKeySigner;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::info;

use st0x_bridge::cctp::{CctpBridge, CctpCtx, CctpError};
use st0x_event_sorcery::{Projection, Store};
use st0x_execution::{AlpacaBrokerApi, AlpacaBrokerApiError, EmptySymbolError, Executor};

use super::equity::CrossVenueEquityTransfer;
use super::usdc::CrossVenueCashTransfer;
use super::{Rebalancer, RebalancingCtx, TriggeredOperation};
use crate::alpaca_wallet::{AlpacaWalletError, AlpacaWalletService};
use crate::equity_redemption::EquityRedemption;
use crate::onchain::http_client_with_retry;
use crate::onchain::raindex::{RaindexService, RaindexVaultId};
use crate::onchain::{USDC_BASE, USDC_ETHEREUM};
use crate::tokenization::Tokenizer;
use crate::tokenized_equity_mint::TokenizedEquityMint;
use crate::usdc_rebalance::UsdcRebalance;
use crate::wrapper::WrapperService;

/// Errors that can occur when spawning the rebalancer.
#[derive(Debug, thiserror::Error)]
pub(crate) enum SpawnRebalancerError {
    #[error("invalid Ethereum private key: {0}")]
    InvalidPrivateKey(#[from] alloy::signers::k256::ecdsa::Error),
    #[error("failed to create Alpaca wallet service: {0}")]
    AlpacaWallet(#[from] AlpacaWalletError),
    #[error("failed to create Alpaca broker API: {0}")]
    AlpacaBrokerApi(#[from] AlpacaBrokerApiError),
    #[error("failed to create CCTP bridge: {0}")]
    Cctp(#[from] CctpError),
    #[error("failed to create wrapper service: {0}")]
    Wrapper(#[from] EmptySymbolError),
}

/// Provider type returned by `ProviderBuilder::connect_http` with wallet.
type HttpProvider = FillProvider<
    JoinFill<
        JoinFill<
            Identity,
            JoinFill<GasFiller, JoinFill<BlobGasFiller, JoinFill<NonceFiller, ChainIdFiller>>>,
        >,
        WalletFiller<EthereumWallet>,
    >,
    RootProvider<Ethereum>,
    Ethereum,
>;

pub(crate) struct RebalancingCqrsFrameworks {
    pub(crate) mint: Arc<Store<TokenizedEquityMint>>,
    pub(crate) redemption: Arc<Store<EquityRedemption>>,
    pub(crate) usdc: Arc<Store<UsdcRebalance>>,
}

/// Spawns the rebalancing infrastructure.
///
/// All CQRS frameworks are created in the conductor and passed here to ensure
/// single-instance initialization with all required query processors.
pub(crate) async fn spawn_rebalancer<BP>(
    ctx: &RebalancingCtx,
    base_provider: BP,
    market_maker_wallet: Address,
    operation_receiver: mpsc::Receiver<TriggeredOperation>,
    frameworks: RebalancingCqrsFrameworks,
    raindex_service: Arc<RaindexService<BP>>,
    tokenizer: Arc<dyn Tokenizer>,
) -> Result<JoinHandle<()>, SpawnRebalancerError>
where
    BP: Provider + Clone + Send + Sync + 'static,
{
    let signer = PrivateKeySigner::from_bytes(&ctx.evm_private_key)?;
    let ethereum_wallet = EthereumWallet::from(signer.clone());

    let services = Services::new(
        ctx,
        &ethereum_wallet,
        signer.address(),
        base_provider,
        raindex_service,
        tokenizer,
    )
    .await?;

    let rebalancer =
        services.into_rebalancer(ctx, market_maker_wallet, operation_receiver, frameworks);

    let handle = tokio::spawn(async move {
        rebalancer.run().await;
    });

    info!("Rebalancing infrastructure initialized");
    Ok(handle)
}

/// External service clients for rebalancing operations.
///
/// Holds connections to Alpaca APIs, CCTP bridge, and vault services.
///
/// Generic over `BP` (Base Provider) to allow reusing the existing WebSocket
/// connection from the main bot. The Ethereum provider uses a fixed HTTP type
/// since it's created from a URL in the rebalancing config.
struct Services<BP>
where
    BP: Provider + Clone,
{
    broker: Arc<AlpacaBrokerApi>,
    wallet: Arc<AlpacaWalletService>,
    cctp: Arc<CctpBridge<HttpProvider, BP>>,
    raindex: Arc<RaindexService<BP>>,
    tokenizer: Arc<dyn Tokenizer>,
    wrapper: Arc<WrapperService<BP>>,
}

impl<BP> Services<BP>
where
    BP: Provider + Clone + 'static,
{
    /// Creates the services needed for rebalancing.
    ///
    /// RaindexService is passed in rather than created here because it is needed
    /// for CQRS framework initialization in the conductor, which must happen
    /// before spawn_rebalancer is called.
    async fn new(
        ctx: &RebalancingCtx,
        ethereum_wallet: &EthereumWallet,
        owner: Address,
        base_provider: BP,
        raindex: Arc<RaindexService<BP>>,
        tokenizer: Arc<dyn Tokenizer>,
    ) -> Result<Self, SpawnRebalancerError> {
        let ethereum_provider = ProviderBuilder::new()
            .wallet(ethereum_wallet.clone())
            .connect_client(http_client_with_retry(ctx.ethereum_rpc_url.clone()));

        let broker_auth = &ctx.alpaca_broker_auth;

        let broker = Arc::new(AlpacaBrokerApi::try_from_ctx(broker_auth.clone()).await?);

        let wallet = Arc::new(AlpacaWalletService::new(
            broker_auth.base_url().to_string(),
            ctx.alpaca_broker_auth.account_id,
            broker_auth.api_key.clone(),
            broker_auth.api_secret.clone(),
        ));

        let cctp = Arc::new(CctpBridge::try_from_ctx(CctpCtx {
            ethereum_provider,
            base_provider: base_provider.clone(),
            owner,
            usdc_ethereum: USDC_ETHEREUM,
            usdc_base: USDC_BASE,
        })?);

        let wrapper = Arc::new(WrapperService::new(
            base_provider.clone(),
            owner,
            ctx.equities.clone(),
        ));

        Ok(Self {
            broker,
            wallet,
            cctp,
            raindex,
            tokenizer,
            wrapper,
        })
    }

    /// Converts Services into a configured Rebalancer.
    fn into_rebalancer(
        self,
        ctx: &RebalancingCtx,
        market_maker_wallet: Address,
        operation_receiver: mpsc::Receiver<TriggeredOperation>,
        frameworks: RebalancingCqrsFrameworks,
    ) -> Rebalancer
    where
        BP: Send + Sync + 'static,
    {
        let equity = Arc::new(CrossVenueEquityTransfer::new(
            self.raindex.clone(),
            self.tokenizer,
            market_maker_wallet,
            frameworks.mint,
            frameworks.redemption,
        ));

        let usdc = Arc::new(CrossVenueCashTransfer::new(
            self.broker,
            self.wallet,
            self.cctp,
            self.raindex,
            frameworks.usdc,
            market_maker_wallet,
            RaindexVaultId(ctx.usdc_vault_id),
        ));

        Rebalancer::new(
            Arc::clone(&equity) as _,
            equity as _,
            Arc::clone(&usdc) as _,
            usdc as _,
            operation_receiver,
        )
    }
}

#[cfg(test)]
mod tests {
    use alloy::node_bindings::Anvil;
    use alloy::primitives::{address, b256};
    use alloy::providers::ProviderBuilder;
    use httpmock::Method::GET;
    use httpmock::MockServer;
    use rust_decimal_macros::dec;
    use serde_json::json;
    use st0x_execution::{
        AlpacaAccountId, AlpacaBrokerApiCtx, AlpacaBrokerApiMode, FractionalShares, Symbol,
        TimeInForce,
    };
    use std::collections::HashMap;
    use uuid::Uuid;

    use st0x_event_sorcery::{Projection, test_store};

    use super::*;
    use crate::alpaca_wallet::{AlpacaTransferId, AlpacaWalletService};
    use crate::inventory::ImbalanceThreshold;
    use crate::onchain::mock::MockRaindex;
    use crate::rebalancing::RebalancingTriggerConfig;
    use crate::rebalancing::equity::EquityTransferServices;
    use crate::rebalancing::trigger::UsdcRebalancing;
    use crate::tokenization::alpaca::AlpacaTokenizationService;
    use crate::tokenization::mock::MockTokenizer;
    use crate::vault_registry::VaultRegistry;
    use crate::wrapper::mock::MockWrapper;

    /// Provider type returned by `ProviderBuilder::new().connect_http()` without wallet.
    type BaseTestProvider = FillProvider<
        JoinFill<
            Identity,
            JoinFill<GasFiller, JoinFill<BlobGasFiller, JoinFill<NonceFiller, ChainIdFiller>>>,
        >,
        RootProvider<Ethereum>,
        Ethereum,
    >;

    const TEST_ORDERBOOK: Address = address!("0xabcdefabcdefabcdefabcdefabcdefabcdefabcd");

    fn make_ctx() -> RebalancingCtx {
        let evm_private_key =
            b256!("0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef");
        let market_maker_wallet = PrivateKeySigner::from_bytes(&evm_private_key)
            .unwrap()
            .address();

        RebalancingCtx {
            equity: ImbalanceThreshold {
                target: dec!(0.5),
                deviation: dec!(0.2),
            },
            usdc: UsdcRebalancing::Enabled {
                target: dec!(0.6),
                deviation: dec!(0.15),
            },
            redemption_wallet: address!("0x1234567890123456789012345678901234567890"),
            market_maker_wallet,
            ethereum_rpc_url: "https://eth.example.com".parse().unwrap(),
            evm_private_key,
            usdc_vault_id: b256!(
                "0xfedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210"
            ),
            alpaca_broker_auth: AlpacaBrokerApiCtx {
                api_key: "test_key".to_string(),
                api_secret: "test_secret".to_string(),
                account_id: AlpacaAccountId::new(Uuid::nil()),
                mode: Some(AlpacaBrokerApiMode::Sandbox),
                asset_cache_ttl: std::time::Duration::from_secs(3600),
                time_in_force: TimeInForce::default(),
            },
            equities: HashMap::new(),
        }
    }

    #[test]
    fn spawn_rebalancer_error_display_invalid_private_key() {
        let err =
            SpawnRebalancerError::InvalidPrivateKey(alloy::signers::k256::ecdsa::Error::new());

        let display = format!("{err}");

        assert!(
            display.contains("invalid Ethereum private key"),
            "Expected error message to contain 'invalid Ethereum private key', got: {display}"
        );
    }

    #[test]
    fn spawn_rebalancer_error_display_alpaca_wallet() {
        let err = SpawnRebalancerError::AlpacaWallet(AlpacaWalletError::TransferNotFound {
            transfer_id: AlpacaTransferId::from(Uuid::nil()),
        });

        let display = format!("{err}");

        assert!(
            display.contains("failed to create Alpaca wallet service"),
            "Expected error message to contain 'failed to create Alpaca wallet service', got: {display}"
        );
    }

    #[test]
    fn trigger_config_uses_equity_from_ctx() {
        let ctx = make_ctx();

        let trigger_config = RebalancingTriggerConfig {
            equity: ctx.equity,
            usdc: ctx.usdc,
        };

        assert_eq!(trigger_config.equity.target, dec!(0.5));
        assert_eq!(trigger_config.equity.deviation, dec!(0.2));
    }

    #[test]
    fn trigger_config_uses_usdc_from_ctx() {
        let ctx = make_ctx();

        let trigger_config = RebalancingTriggerConfig {
            equity: ctx.equity,
            usdc: ctx.usdc,
        };

        let UsdcRebalancing::Enabled { target, deviation } = trigger_config.usdc else {
            panic!("expected enabled");
        };
        assert_eq!(target, dec!(0.6));
        assert_eq!(deviation, dec!(0.15));
    }

    #[test]
    fn private_key_signer_from_valid_bytes_succeeds() {
        let rebalancing_ctx = make_ctx();

        PrivateKeySigner::from_bytes(&rebalancing_ctx.evm_private_key).unwrap();
    }

    #[test]
    fn private_key_signer_from_zero_bytes_fails() {
        let zero_key = b256!("0x0000000000000000000000000000000000000000000000000000000000000000");

        PrivateKeySigner::from_bytes(&zero_key).unwrap_err();
    }

    async fn make_services_with_mock_wallet(
        server: &httpmock::MockServer,
    ) -> (Services<BaseTestProvider>, RebalancingCtx) {
        let anvil = Anvil::new().spawn();
        let base_provider = ProviderBuilder::new().connect_http(anvil.endpoint_url());

        let rebalancing_ctx = make_ctx();
        let signer = PrivateKeySigner::from_bytes(&rebalancing_ctx.evm_private_key).unwrap();
        let ethereum_wallet = EthereumWallet::from(signer.clone());

        let ethereum_provider = ProviderBuilder::new()
            .wallet(ethereum_wallet)
            .connect_http(rebalancing_ctx.ethereum_rpc_url.clone());

        let tokenization = Arc::new(AlpacaTokenizationService::new(
            server.base_url(),
            rebalancing_ctx.alpaca_broker_auth.account_id,
            "test_key".into(),
            "test_secret".into(),
            base_provider.clone(),
            rebalancing_ctx.redemption_wallet,
        ));

        // Mock the broker account verification endpoint
        let _account_mock = server.mock(|when, then| {
            when.method(GET).path(format!(
                "/v1/trading/accounts/{}/account",
                rebalancing_ctx.alpaca_broker_auth.account_id
            ));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "id": rebalancing_ctx.alpaca_broker_auth.account_id.to_string(),
                    "status": "ACTIVE"
                }));
        });

        let broker_auth = AlpacaBrokerApiCtx {
            api_key: "test_key".to_string(),
            api_secret: "test_secret".to_string(),
            account_id: rebalancing_ctx.alpaca_broker_auth.account_id,
            mode: Some(AlpacaBrokerApiMode::Mock(server.base_url())),
            asset_cache_ttl: std::time::Duration::from_secs(3600),
            time_in_force: TimeInForce::default(),
        };
        let broker = Arc::new(
            AlpacaBrokerApi::try_from_ctx(broker_auth)
                .await
                .expect("Failed to create test broker API"),
        );

        let wallet = Arc::new(AlpacaWalletService::new(
            server.base_url(),
            rebalancing_ctx.alpaca_broker_auth.account_id,
            "test_key".into(),
            "test_secret".into(),
        ));

        let owner = signer.address();

        let cctp = Arc::new(
            CctpBridge::try_from_ctx(CctpCtx {
                ethereum_provider,
                base_provider: base_provider.clone(),
                owner,
                usdc_ethereum: USDC_ETHEREUM,
                usdc_base: USDC_BASE,
            })
            .unwrap(),
        );

        let wrapper = Arc::new(WrapperService::new(
            base_provider.clone(),
            owner,
            rebalancing_ctx.equities.clone(),
        ));

        let vault_registry_projection = Arc::new(
            Projection::<VaultRegistry>::sqlite(crate::test_utils::setup_test_db().await).unwrap(),
        );
        let raindex = Arc::new(
            RaindexService::new(
                base_provider,
                TEST_ORDERBOOK,
                vault_registry_projection,
                owner,
            )
            .with_required_confirmations(1),
        );

        let services = Services {
            broker,
            wallet,
            cctp,
            raindex,
            tokenizer: tokenization,
            wrapper,
        };

        (services, rebalancing_ctx)
    }

    #[tokio::test]
    async fn broker_auth_failure_returns_spawn_error() {
        let server = MockServer::start();

        // Mock account endpoint to return 401 unauthorized
        let _account_mock = server.mock(|when, then| {
            when.method(GET).path_contains("/trading/accounts/");
            then.status(401)
                .header("content-type", "application/json")
                .json_body(json!({"message": "Invalid API credentials"}));
        });

        let rebalancing_ctx = make_ctx();
        let broker_auth = AlpacaBrokerApiCtx {
            api_key: "invalid_key".to_string(),
            api_secret: "invalid_secret".to_string(),
            account_id: rebalancing_ctx.alpaca_broker_auth.account_id,
            mode: Some(AlpacaBrokerApiMode::Mock(server.base_url())),
            asset_cache_ttl: std::time::Duration::from_secs(3600),
            time_in_force: TimeInForce::default(),
        };

        // Verify the error can be converted to SpawnRebalancerError
        let spawn_error: SpawnRebalancerError = AlpacaBrokerApi::try_from_ctx(broker_auth)
            .await
            .unwrap_err()
            .into();
        assert!(
            matches!(spawn_error, SpawnRebalancerError::AlpacaBrokerApi(_)),
            "Expected AlpacaBrokerApi error variant, got: {spawn_error:?}"
        );
    }

    #[tokio::test]
    async fn into_rebalancer_dispatches_mint_operation() {
        let server = MockServer::start();
        let (services, config) = make_services_with_mock_wallet(&server).await;

        let pool = crate::test_utils::setup_test_db().await;
        let mock_services = EquityTransferServices {
            tokenizer: Arc::new(MockTokenizer::new()),
            raindex: Arc::new(MockRaindex::new()),
            wrapper: Arc::new(MockWrapper::new()),
        };
        let mint_cqrs = Arc::new(test_store(pool.clone(), mock_services.clone()));
        let usdc_cqrs = Arc::new(test_store(pool.clone(), ()));

        let redemption_cqrs = Arc::new(test_store(pool.clone(), mock_services));

        let frameworks = RebalancingCqrsFrameworks {
            mint: mint_cqrs,
            redemption: redemption_cqrs,
            usdc: usdc_cqrs,
        };

        let (tx, rx) = tokio::sync::mpsc::channel(10);

        let rebalancer = services.into_rebalancer(&config, Address::random(), rx, frameworks);

        tx.send(TriggeredOperation::Mint {
            symbol: Symbol::new("AAPL").unwrap(),
            quantity: FractionalShares::new(dec!(10)),
        })
        .await
        .unwrap();

        drop(tx);
        rebalancer.run().await;
    }
}

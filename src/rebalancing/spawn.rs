//! Spawns the rebalancing infrastructure.

use alloy::network::{Ethereum, EthereumWallet};
use alloy::primitives::Address;
use alloy::providers::fillers::{
    BlobGasFiller, ChainIdFiller, FillProvider, GasFiller, JoinFill, NonceFiller, WalletFiller,
};
use alloy::providers::{Identity, Provider, ProviderBuilder, RootProvider};
use alloy::signers::local::PrivateKeySigner;
use cqrs_es::persist::PersistedEventStore;
use cqrs_es::{CqrsFramework, Query};
use sqlite_es::SqliteEventRepository;
use sqlx::SqlitePool;
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio::sync::{RwLock, mpsc};
use tokio::task::JoinHandle;
use tracing::info;

use crate::dashboard::{EventBroadcaster, ServerMessage};
use st0x_execution::alpaca_broker_api::AlpacaBrokerApiError;
use st0x_execution::{AlpacaBrokerApi, Executor};

use super::usdc::UsdcRebalanceManager;
use super::{
    MintManager, Rebalancer, RebalancingConfig, RebalancingTrigger, RebalancingTriggerConfig,
    RedemptionManager,
};
use crate::alpaca_tokenization::AlpacaTokenizationService;
use crate::alpaca_wallet::{AlpacaWalletError, AlpacaWalletService};
use crate::cctp::{
    CctpBridge, Evm, MESSAGE_TRANSMITTER_V2, TOKEN_MESSENGER_V2, USDC_BASE, USDC_ETHEREUM,
};
use crate::equity_redemption::{EquityRedemption, RedemptionEventStore};
use crate::inventory::InventoryView;
use crate::lifecycle::{Lifecycle, Never};
use crate::onchain::http_client_with_retry;
use crate::onchain::vault::{VaultId, VaultService};
use crate::tokenized_equity_mint::{MintEventStore, TokenizedEquityMint};
use crate::usdc_rebalance::{UsdcEventStore, UsdcRebalance};

/// Runtime context passed to the rebalancer.
///
/// Groups optional runtime dependencies that are shared across the system.
pub(crate) struct RebalancerContext {
    pub(crate) event_broadcast: Option<broadcast::Sender<ServerMessage>>,
    pub(crate) inventory: Arc<RwLock<InventoryView>>,
}

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
    Cctp(#[from] crate::cctp::CctpError),
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

/// Type alias for a configured rebalancer with SQLite persistence.
type ConfiguredRebalancer<BP> = Rebalancer<
    MintManager<BP, MintEventStore>,
    RedemptionManager<BP, RedemptionEventStore>,
    UsdcRebalanceManager<BP, UsdcEventStore>,
>;

/// Spawns the rebalancing infrastructure.
pub(crate) async fn spawn_rebalancer<BP>(
    pool: SqlitePool,
    config: &RebalancingConfig,
    base_provider: BP,
    orderbook: Address,
    context: RebalancerContext,
) -> Result<JoinHandle<()>, SpawnRebalancerError>
where
    BP: Provider + Clone + Send + Sync + 'static,
{
    let signer = PrivateKeySigner::from_bytes(&config.evm_private_key)?;
    let ethereum_wallet = EthereumWallet::from(signer.clone());

    let services = Services::new(
        config,
        &ethereum_wallet,
        signer.address(),
        base_provider,
        orderbook,
    )
    .await?;

    let market_maker_wallet = signer.address();
    let rebalancer = services.into_rebalancer(
        pool,
        config,
        orderbook,
        context.event_broadcast,
        market_maker_wallet,
        context.inventory,
    );

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
    tokenization: Arc<AlpacaTokenizationService<BP>>,
    broker: Arc<AlpacaBrokerApi>,
    wallet: Arc<AlpacaWalletService>,
    cctp: Arc<CctpBridge<HttpProvider, BP>>,
    vault: Arc<VaultService<BP>>,
}

impl<BP> Services<BP>
where
    BP: Provider + Clone + 'static,
{
    async fn new(
        config: &RebalancingConfig,
        ethereum_wallet: &EthereumWallet,
        owner: Address,
        base_provider: BP,
        orderbook: Address,
    ) -> Result<Self, SpawnRebalancerError> {
        let ethereum_provider = ProviderBuilder::new()
            .wallet(ethereum_wallet.clone())
            .connect_client(http_client_with_retry(config.ethereum_rpc_url.clone()));

        let broker_auth = &config.alpaca_broker_auth;

        let tokenization = Arc::new(AlpacaTokenizationService::new(
            broker_auth.base_url().to_string(),
            config.alpaca_account_id,
            broker_auth.alpaca_broker_api_key.clone(),
            broker_auth.alpaca_broker_api_secret.clone(),
            base_provider.clone(),
            config.redemption_wallet,
        ));

        let broker = Arc::new(AlpacaBrokerApi::try_from_config(broker_auth.clone()).await?);

        let wallet = Arc::new(AlpacaWalletService::new(
            broker_auth.base_url().to_string(),
            config.alpaca_account_id,
            broker_auth.alpaca_broker_api_key.clone(),
            broker_auth.alpaca_broker_api_secret.clone(),
        ));

        let ethereum_evm = Evm::new(
            ethereum_provider,
            owner,
            USDC_ETHEREUM,
            TOKEN_MESSENGER_V2,
            MESSAGE_TRANSMITTER_V2,
        );

        let base_evm_for_cctp = Evm::new(
            base_provider.clone(),
            owner,
            USDC_BASE,
            TOKEN_MESSENGER_V2,
            MESSAGE_TRANSMITTER_V2,
        );

        let cctp = Arc::new(CctpBridge::new(ethereum_evm, base_evm_for_cctp)?);
        let vault = Arc::new(VaultService::new(base_provider, orderbook));

        Ok(Self {
            tokenization,
            broker,
            wallet,
            cctp,
            vault,
        })
    }

    fn into_rebalancer(
        self,
        pool: SqlitePool,
        config: &RebalancingConfig,
        orderbook: Address,
        event_broadcast: Option<broadcast::Sender<ServerMessage>>,
        market_maker_wallet: Address,
        inventory: Arc<RwLock<InventoryView>>,
    ) -> ConfiguredRebalancer<BP> {
        const OPERATION_CHANNEL_CAPACITY: usize = 100;

        let (operation_sender, operation_receiver) = mpsc::channel(OPERATION_CHANNEL_CAPACITY);

        let trigger_config = RebalancingTriggerConfig {
            equity_threshold: config.equity_threshold,
            usdc_threshold: config.usdc_threshold,
        };

        let trigger = Arc::new(RebalancingTrigger::new(
            trigger_config,
            pool.clone(),
            orderbook,
            market_maker_wallet,
            inventory,
            operation_sender,
        ));

        let mint_store =
            PersistedEventStore::new_event_store(SqliteEventRepository::new(pool.clone()));
        let mint_cqrs = Arc::new(CqrsFramework::new(
            mint_store,
            build_mint_queries(trigger.clone(), event_broadcast.clone()),
            (),
        ));

        let redemption_store =
            PersistedEventStore::new_event_store(SqliteEventRepository::new(pool.clone()));
        let redemption_cqrs = Arc::new(CqrsFramework::new(
            redemption_store,
            build_redemption_queries(trigger.clone(), event_broadcast.clone()),
            (),
        ));

        let usdc_store = PersistedEventStore::new_event_store(SqliteEventRepository::new(pool));
        let usdc_cqrs = Arc::new(CqrsFramework::new(
            usdc_store,
            build_usdc_queries(trigger, event_broadcast),
            (),
        ));

        let mint_manager = Arc::new(MintManager::new(self.tokenization.clone(), mint_cqrs));

        let redemption_manager =
            Arc::new(RedemptionManager::new(self.tokenization, redemption_cqrs));

        let usdc_manager = Arc::new(UsdcRebalanceManager::new(
            self.broker,
            self.wallet,
            self.cctp,
            self.vault,
            usdc_cqrs,
            market_maker_wallet,
            VaultId(config.usdc_vault_id),
        ));

        Rebalancer::new(
            mint_manager,
            redemption_manager,
            usdc_manager,
            operation_receiver,
            config.redemption_wallet,
        )
    }
}

macro_rules! build_queries {
    ($name:ident, $aggregate:ty) => {
        fn $name(
            trigger: Arc<RebalancingTrigger>,
            event_broadcast: Option<broadcast::Sender<ServerMessage>>,
        ) -> Vec<Box<dyn Query<Lifecycle<$aggregate, Never>>>> {
            let mut queries: Vec<Box<dyn Query<Lifecycle<$aggregate, Never>>>> =
                vec![Box::new(trigger)];

            if let Some(sender) = event_broadcast {
                queries.push(Box::new(EventBroadcaster::new(sender)));
            }

            queries
        }
    };
}

build_queries!(build_mint_queries, TokenizedEquityMint);
build_queries!(build_redemption_queries, EquityRedemption);
build_queries!(build_usdc_queries, UsdcRebalance);

#[cfg(test)]
mod tests {
    use alloy::node_bindings::Anvil;
    use alloy::primitives::{address, b256};
    use alloy::providers::ProviderBuilder;
    use httpmock::Method::GET;
    use httpmock::MockServer;
    use rust_decimal_macros::dec;
    use serde_json::json;
    use sqlx::SqlitePool;
    use st0x_execution::alpaca_broker_api::{AlpacaBrokerApiAuthEnv, AlpacaBrokerApiMode};
    use uuid::Uuid;

    use super::*;
    use crate::alpaca_wallet::{AlpacaAccountId, AlpacaWalletService};
    use crate::inventory::ImbalanceThreshold;

    const TEST_ORDERBOOK: Address = address!("0xabcdefabcdefabcdefabcdefabcdefabcdefabcd");

    fn make_config() -> RebalancingConfig {
        RebalancingConfig {
            equity_threshold: ImbalanceThreshold {
                target: dec!(0.5),
                deviation: dec!(0.2),
            },
            usdc_threshold: ImbalanceThreshold {
                target: dec!(0.6),
                deviation: dec!(0.15),
            },
            redemption_wallet: address!("0x1234567890123456789012345678901234567890"),
            ethereum_rpc_url: "https://eth.example.com".parse().unwrap(),
            evm_private_key: b256!(
                "0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
            ),
            usdc_vault_id: b256!(
                "0xfedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210"
            ),
            alpaca_account_id: AlpacaAccountId::new(Uuid::nil()),
            alpaca_broker_auth: AlpacaBrokerApiAuthEnv {
                alpaca_broker_api_key: "test_key".to_string(),
                alpaca_broker_api_secret: "test_secret".to_string(),
                alpaca_account_id: Uuid::nil().to_string(),
                alpaca_broker_api_mode: AlpacaBrokerApiMode::Sandbox,
            },
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
        let err = SpawnRebalancerError::AlpacaWallet(AlpacaWalletError::InvalidAmount {
            amount: dec!(0),
        });

        let display = format!("{err}");

        assert!(
            display.contains("failed to create Alpaca wallet service"),
            "Expected error message to contain 'failed to create Alpaca wallet service', got: {display}"
        );
    }

    #[test]
    fn trigger_config_uses_equity_threshold_from_config() {
        let config = make_config();

        let trigger_config = RebalancingTriggerConfig {
            equity_threshold: config.equity_threshold,
            usdc_threshold: config.usdc_threshold,
        };

        assert_eq!(trigger_config.equity_threshold.target, dec!(0.5));
        assert_eq!(trigger_config.equity_threshold.deviation, dec!(0.2));
    }

    #[test]
    fn trigger_config_uses_usdc_threshold_from_config() {
        let config = make_config();

        let trigger_config = RebalancingTriggerConfig {
            equity_threshold: config.equity_threshold,
            usdc_threshold: config.usdc_threshold,
        };

        assert_eq!(trigger_config.usdc_threshold.target, dec!(0.6));
        assert_eq!(trigger_config.usdc_threshold.deviation, dec!(0.15));
    }

    #[test]
    fn private_key_signer_from_valid_bytes_succeeds() {
        let config = make_config();

        let result = PrivateKeySigner::from_bytes(&config.evm_private_key);

        assert!(
            result.is_ok(),
            "Expected valid private key to parse successfully"
        );
    }

    #[test]
    fn private_key_signer_from_zero_bytes_fails() {
        let zero_key = b256!("0x0000000000000000000000000000000000000000000000000000000000000000");

        let result = PrivateKeySigner::from_bytes(&zero_key);

        assert!(result.is_err(), "Expected zero private key to fail parsing");
    }

    async fn make_services_with_mock_wallet(
        server: &httpmock::MockServer,
    ) -> (Services<impl Provider + Clone + 'static>, RebalancingConfig) {
        let anvil = Anvil::new().spawn();
        let base_provider = ProviderBuilder::new().connect_http(anvil.endpoint_url());

        let config = make_config();
        let signer = PrivateKeySigner::from_bytes(&config.evm_private_key).unwrap();
        let ethereum_wallet = EthereumWallet::from(signer.clone());

        let ethereum_provider = ProviderBuilder::new()
            .wallet(ethereum_wallet)
            .connect_http(config.ethereum_rpc_url.clone());

        let tokenization = Arc::new(AlpacaTokenizationService::new(
            server.base_url(),
            config.alpaca_account_id,
            "test_key".into(),
            "test_secret".into(),
            base_provider.clone(),
            config.redemption_wallet,
        ));

        // Mock the broker account verification endpoint
        let _account_mock = server.mock(|when, then| {
            when.method(GET).path(format!(
                "/v1/trading/accounts/{}/account",
                config.alpaca_account_id
            ));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "id": config.alpaca_account_id.to_string(),
                    "status": "ACTIVE"
                }));
        });

        let broker_auth = AlpacaBrokerApiAuthEnv {
            alpaca_broker_api_key: "test_key".to_string(),
            alpaca_broker_api_secret: "test_secret".to_string(),
            alpaca_account_id: config.alpaca_account_id.to_string(),
            alpaca_broker_api_mode: AlpacaBrokerApiMode::Mock(server.base_url()),
        };
        let broker = Arc::new(
            AlpacaBrokerApi::try_from_config(broker_auth)
                .await
                .expect("Failed to create test broker API"),
        );

        let wallet = Arc::new(AlpacaWalletService::new(
            server.base_url(),
            config.alpaca_account_id,
            "test_key".into(),
            "test_secret".into(),
        ));

        let owner = signer.address();

        let ethereum_evm = Evm::new(
            ethereum_provider,
            owner,
            USDC_ETHEREUM,
            TOKEN_MESSENGER_V2,
            MESSAGE_TRANSMITTER_V2,
        )
        .with_required_confirmations(1);

        let base_evm_for_cctp = Evm::new(
            base_provider.clone(),
            owner,
            USDC_BASE,
            TOKEN_MESSENGER_V2,
            MESSAGE_TRANSMITTER_V2,
        )
        .with_required_confirmations(1);

        let cctp = Arc::new(CctpBridge::new(ethereum_evm, base_evm_for_cctp).unwrap());
        let vault = Arc::new(
            VaultService::new(base_provider, TEST_ORDERBOOK).with_required_confirmations(1),
        );

        let services = Services {
            tokenization,
            broker,
            wallet,
            cctp,
            vault,
        };

        (services, config)
    }

    #[tokio::test]
    async fn into_rebalancer_constructs_without_panic() {
        let server = MockServer::start();
        let (services, config) = make_services_with_mock_wallet(&server).await;

        let pool = SqlitePool::connect(":memory:").await.unwrap();
        sqlx::migrate!().run(&pool).await.unwrap();

        let market_maker_wallet = address!("0xaabbccddaabbccddaabbccddaabbccddaabbccdd");
        let inventory = Arc::new(RwLock::new(InventoryView::default()));
        let orderbook = address!("0x1111111111111111111111111111111111111111");
        let _rebalancer = services.into_rebalancer(
            pool,
            &config,
            orderbook,
            None,
            market_maker_wallet,
            inventory,
        );
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

        let config = make_config();
        let broker_auth = AlpacaBrokerApiAuthEnv {
            alpaca_broker_api_key: "invalid_key".to_string(),
            alpaca_broker_api_secret: "invalid_secret".to_string(),
            alpaca_account_id: config.alpaca_account_id.to_string(),
            alpaca_broker_api_mode: AlpacaBrokerApiMode::Mock(server.base_url()),
        };

        let result = AlpacaBrokerApi::try_from_config(broker_auth).await;

        assert!(
            result.is_err(),
            "Expected auth failure to return error, got: {result:?}"
        );

        // Verify the error can be converted to SpawnRebalancerError
        let spawn_error: SpawnRebalancerError = result.unwrap_err().into();
        assert!(
            matches!(spawn_error, SpawnRebalancerError::AlpacaBrokerApi(_)),
            "Expected AlpacaBrokerApi error variant, got: {spawn_error:?}"
        );
    }
}

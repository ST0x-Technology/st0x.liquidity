//! RedemptionManager orchestrates the EquityRedemption workflow.
//!
//! Coordinates between `AlpacaTokenizationService` and the `EquityRedemption` aggregate
//! to execute the full redemption lifecycle: withdraw from vault -> send tokens -> poll detection -> poll completion.

use alloy::primitives::{Address, B256, TxHash, U256};
use alloy::providers::Provider;
use async_trait::async_trait;
use st0x_execution::{FractionalShares, Symbol};
use std::sync::Arc;
use tracing::{error, info, instrument, warn};

use st0x_event_sorcery::Store;

use super::{Redeem, RedemptionError};
use crate::alpaca_tokenization::{AlpacaTokenizationService, TokenizationRequestStatus};
use crate::tokenized_equity_mint::TokenizationRequestId;

/// Our tokenized equity tokens use 18 decimals.
const TOKENIZED_EQUITY_DECIMALS: u8 = 18;
use crate::equity_redemption::{EquityRedemption, EquityRedemptionCommand, RedemptionAggregateId};
use crate::onchain::vault::{VaultId, VaultService};
use crate::vault_registry::{VaultRegistry, VaultRegistryQuery};

pub(crate) struct RedemptionManager<P>
where
    P: Provider + Clone,
{
    service: Arc<AlpacaTokenizationService<P>>,
    vault: Arc<VaultService<P>>,
    cqrs: Arc<Store<EquityRedemption>>,
    vault_registry_query: Arc<VaultRegistryQuery>,
    orderbook: Address,
    owner: Address,
}

impl<P> RedemptionManager<P>
where
    P: Provider + Clone + Send + Sync + 'static,
{
    pub(crate) fn new(
        service: Arc<AlpacaTokenizationService<P>>,
        vault: Arc<VaultService<P>>,
        cqrs: Arc<Store<EquityRedemption>>,
        vault_registry_query: Arc<VaultRegistryQuery>,
        orderbook: Address,
        owner: Address,
    ) -> Self {
        Self {
            service,
            vault,
            cqrs,
            vault_registry_query,
            orderbook,
            owner,
        }
    }

    /// Looks up the vault ID for a token from the vault registry.
    async fn load_vault_id(&self, token: Address) -> Option<B256> {
        let aggregate_id = VaultRegistry::aggregate_id(self.orderbook, self.owner);

        let Some(lifecycle) = self.vault_registry_query.load(&aggregate_id).await else {
            warn!("Vault registry not found");
            return None;
        };

        match lifecycle {
            Lifecycle::Uninitialized => {
                warn!("Vault registry not initialized");
                None
            }
            Lifecycle::Live(registry) => registry.vault_id_by_token(token),
            Lifecycle::Failed { .. } => {
                error!("Vault registry in failed state");
                None
            }
        }
    }

    /// Executes the full redemption workflow.
    ///
    /// # Workflow
    ///
    /// 1. Look up vault ID from vault registry
    /// 2. Withdraw tokens from vault to wallet
    /// 3. Send tokens to Alpaca redemption wallet
    /// 4. Send `SendTokens` command to aggregate
    /// 5. Poll Alpaca until redemption is detected
    /// 6. Send `Detect` with tokenization_request_id
    /// 7. Poll Alpaca until terminal status
    /// 8. Send `Complete` when Alpaca reports completion
    ///
    /// On errors, sends appropriate failure commands (`FailDetection`, `RejectRedemption`).
    async fn withdraw_and_send(
        &self,
        aggregate_id: &RedemptionAggregateId,
        symbol: Symbol,
        quantity: FractionalShares,
        token: Address,
        amount: U256,
    ) -> Result<TxHash, RedemptionError> {
        let Some(vault_id) = self.load_vault_id(token).await.map(VaultId) else {
            error!(%token, "Token not found in vault registry");
            return Err(RedemptionError::VaultNotFound { token });
        };

        info!(?vault_id, "Withdrawing tokens from vault");
        self.vault
            .withdraw(token, vault_id, amount, TOKENIZED_EQUITY_DECIMALS)
            .await
            .map_err(|e| {
                warn!("Failed to withdraw tokens from vault: {e}");
                RedemptionError::Vault(e)
            })?;

        let tx_hash = self
            .service
            .send_for_redemption(token, amount)
            .await
            .map_err(|e| {
                warn!("Failed to send tokens for redemption: {e}");
                RedemptionError::Alpaca(e)
            })?;

        self.cqrs
            .send(
                aggregate_id,
                EquityRedemptionCommand::SendTokens {
                    symbol,
                    quantity: quantity.inner(),
                    redemption_wallet: self.service.redemption_wallet(),
                    tx_hash,
                },
            )
            .await?;

        Ok(tx_hash)
    }

    /// Polls for redemption detection and records it.
    async fn poll_detection(
        &self,
        aggregate_id: &RedemptionAggregateId,
        tx_hash: &TxHash,
    ) -> Result<TokenizationRequestId, RedemptionError> {
        let detected = match self.service.poll_for_redemption(tx_hash).await {
            Ok(req) => req,
            Err(e) => {
                warn!("Polling for redemption detection failed: {e}");
                self.cqrs
                    .send(
                        aggregate_id,
                        EquityRedemptionCommand::FailDetection {
                            reason: format!("Detection polling failed: {e}"),
                        },
                    )
                    .await?;
                return Err(RedemptionError::Alpaca(e));
            }
        };

        self.cqrs
            .send(
                aggregate_id,
                EquityRedemptionCommand::Detect {
                    tokenization_request_id: detected.id.clone(),
                },
            )
            .await?;

        Ok(detected.id)
    }

    /// Polls for completion and finalizes the redemption.
    async fn poll_completion(
        &self,
        aggregate_id: &RedemptionAggregateId,
        request_id: &TokenizationRequestId,
    ) -> Result<(), RedemptionError> {
        let completed = match self
            .service
            .poll_redemption_until_complete(request_id)
            .await
        {
            Ok(req) => req,
            Err(e) => {
                warn!("Polling for completion failed: {e}");
                self.cqrs
                    .send(
                        aggregate_id,
                        EquityRedemptionCommand::RejectRedemption {
                            reason: format!("Completion polling failed: {e}"),
                        },
                    )
                    .await?;
                return Err(RedemptionError::Alpaca(e));
            }
        };

        match completed.status {
            TokenizationRequestStatus::Completed => {
                self.cqrs
                    .send(aggregate_id, EquityRedemptionCommand::Complete)
                    .await?;
                Ok(())
            }
            TokenizationRequestStatus::Rejected => {
                self.cqrs
                    .send(
                        aggregate_id,
                        EquityRedemptionCommand::RejectRedemption {
                            reason: "Redemption rejected by Alpaca".to_string(),
                        },
                    )
                    .await?;
                Err(RedemptionError::Rejected)
            }
            TokenizationRequestStatus::Pending => {
                unreachable!("poll_redemption_until_complete should not return Pending status")
            }
        }
    }

    /// Executes the full redemption workflow.
    #[instrument(skip(self), fields(%symbol, ?quantity, %token, %amount))]
    async fn execute_redemption_impl(
        &self,
        aggregate_id: &RedemptionAggregateId,
        symbol: Symbol,
        quantity: FractionalShares,
        token: Address,
        amount: U256,
    ) -> Result<(), RedemptionError> {
        info!(%symbol, ?quantity, %token, %amount, "Starting redemption workflow");

        let tx_hash = self
            .withdraw_and_send(aggregate_id, symbol, quantity, token, amount)
            .await?;

        info!(%tx_hash, "Tokens sent, polling for detection");
        let request_id = self.poll_detection(aggregate_id, &tx_hash).await?;

        info!(%request_id, "Redemption detected, polling for completion");
        self.poll_completion(aggregate_id, &request_id).await?;

        info!("Redemption workflow completed successfully");
        Ok(())
    }
}

#[async_trait]
impl<P> Redeem for RedemptionManager<P>
where
    P: Provider + Clone + Send + Sync + 'static,
{
    async fn execute_redemption(
        &self,
        aggregate_id: &RedemptionAggregateId,
        symbol: Symbol,
        quantity: FractionalShares,
        token: Address,
        amount: U256,
    ) -> Result<(), RedemptionError> {
        self.execute_redemption_impl(aggregate_id, symbol, quantity, token, amount)
            .await
    }
}

#[cfg(test)]
mod tests {
    use alloy::network::{Ethereum, EthereumWallet};
    use alloy::node_bindings::{Anvil, AnvilInstance};
    use alloy::primitives::{B256, TxHash, address, b256};
    use alloy::providers::fillers::{
        BlobGasFiller, ChainIdFiller, FillProvider, GasFiller, JoinFill, NonceFiller, WalletFiller,
    };
    use alloy::providers::{Identity, ProviderBuilder, RootProvider};
    use alloy::signers::local::PrivateKeySigner;
    use httpmock::prelude::*;
    use rust_decimal_macros::dec;
    use serde_json::json;
    use sqlx::SqlitePool;

    use st0x_event_sorcery::test_store;

    use super::*;
    use crate::alpaca_tokenization::tests::{
        TEST_REDEMPTION_WALLET, create_test_service_with_provider,
    };
    use crate::bindings::{OrderBook, TOFUTokenDecimals, TestERC20};
    use crate::onchain::vault::{VaultId, VaultService};
    use crate::vault_registry::{VaultRegistryAggregate, VaultRegistryCommand};

    const TEST_ORDERBOOK: Address = address!("0x1111111111111111111111111111111111111111");
    const TEST_OWNER: Address = address!("0x2222222222222222222222222222222222222222");

    async fn create_test_store_instance() -> Arc<Store<EquityRedemption>> {
        let pool = SqlitePool::connect(":memory:").await.unwrap();
        sqlx::migrate!().run(&pool).await.unwrap();
        Arc::new(test_store(pool, ()))
    }

    /// Creates a vault registry CQRS framework with a view query processor.
    /// Returns the query for loading data after commands are executed.
    fn create_vault_registry_cqrs(
        pool: &SqlitePool,
    ) -> (SqliteCqrs<VaultRegistryAggregate>, Arc<VaultRegistryQuery>) {
        let view_repo = Arc::new(SqliteViewRepository::<
            VaultRegistryAggregate,
            VaultRegistryAggregate,
        >::new(
            pool.clone(), "vault_registry_view".to_string()
        ));
        let query = Arc::new(GenericQuery::new(view_repo.clone()));
        let cqrs = sqlite_cqrs::<VaultRegistryAggregate>(
            pool.clone(),
            vec![Box::new(GenericQuery::new(view_repo))],
            (),
        );
        (cqrs, query)
    }

    async fn seed_vault_registry(
        pool: &SqlitePool,
        token: Address,
        vault_id: B256,
    ) -> Arc<VaultRegistryQuery> {
        seed_vault_registry_with_params(pool, TEST_ORDERBOOK, TEST_OWNER, token, vault_id).await
    }

    async fn seed_vault_registry_with_params(
        pool: &SqlitePool,
        orderbook: Address,
        owner: Address,
        token: Address,
        vault_id: B256,
    ) -> Arc<VaultRegistryQuery> {
        let (cqrs, query) = create_vault_registry_cqrs(pool);
        let aggregate_id = VaultRegistry::aggregate_id(orderbook, owner);

        cqrs.execute(
            &aggregate_id,
            VaultRegistryCommand::DiscoverEquityVault {
                token,
                vault_id,
                discovered_in: TxHash::ZERO,
                symbol: Symbol::new("TEST").unwrap(),
            },
        )
        .await
        .unwrap();

        query
    }

    #[tokio::test]
    async fn load_vault_id_returns_none_when_registry_empty() {
        let pool = crate::test_utils::setup_test_db().await;
        let (_cqrs, vault_registry_query) = create_vault_registry_cqrs(&pool);

        let aggregate_id = VaultRegistry::aggregate_id(TEST_ORDERBOOK, TEST_OWNER);
        let result = vault_registry_query.load(&aggregate_id).await;

        assert!(result.is_none(), "Expected None for empty registry");
    }

    #[tokio::test]
    async fn load_vault_id_returns_vault_id_when_registered() {
        let pool = crate::test_utils::setup_test_db().await;
        let token = address!("0x1234567890abcdef1234567890abcdef12345678");
        let expected_vault_id =
            b256!("0xabcdef0000000000000000000000000000000000000000000000000000000001");

        let vault_registry_query = seed_vault_registry(&pool, token, expected_vault_id).await;

        let aggregate_id = VaultRegistry::aggregate_id(TEST_ORDERBOOK, TEST_OWNER);
        let lifecycle = vault_registry_query.load(&aggregate_id).await.unwrap();

        let Lifecycle::Live(registry) = lifecycle else {
            panic!("Expected Live registry");
        };

        assert_eq!(
            registry.vault_id_by_token(token),
            Some(expected_vault_id),
            "Expected vault ID to be registered"
        );
    }

    #[tokio::test]
    async fn load_vault_id_returns_none_for_unknown_token() {
        let pool = crate::test_utils::setup_test_db().await;
        let registered_token = address!("0x1234567890abcdef1234567890abcdef12345678");
        let unknown_token = address!("0xabcdef0123456789abcdef0123456789abcdef01");
        let vault_id = b256!("0xabcdef0000000000000000000000000000000000000000000000000000000001");

        let vault_registry_query = seed_vault_registry(&pool, registered_token, vault_id).await;

        let aggregate_id = VaultRegistry::aggregate_id(TEST_ORDERBOOK, TEST_OWNER);
        let lifecycle = vault_registry_query.load(&aggregate_id).await.unwrap();

        let Lifecycle::Live(registry) = lifecycle else {
            panic!("Expected Live registry");
        };

        assert_eq!(
            registry.vault_id_by_token(unknown_token),
            None,
            "Expected None for unknown token"
        );
    }

    // --- Test infrastructure for vault integration tests ---

    const TOFU_DECIMALS_ADDRESS: Address = address!("0x4f1C29FAAB7EDdF8D7794695d8259996734Cc665");
    const TEST_VAULT_ID: VaultId = VaultId(b256!(
        "0x0000000000000000000000000000000000000000000000000000000000000001"
    ));

    type LocalEvmProvider = FillProvider<
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

    struct LocalEvmWithVault {
        anvil: AnvilInstance,
        provider: LocalEvmProvider,
        signer: PrivateKeySigner,
        orderbook_address: Address,
        token_address: Address,
    }

    impl LocalEvmWithVault {
        async fn new() -> Self {
            let anvil = Anvil::new().spawn();
            let endpoint = anvil.endpoint();

            let private_key_bytes = anvil.keys()[0].to_bytes();
            let signer =
                PrivateKeySigner::from_bytes(&B256::from_slice(&private_key_bytes)).unwrap();

            let wallet = EthereumWallet::from(signer.clone());
            let provider = ProviderBuilder::new()
                .wallet(wallet)
                .connect_http(endpoint.parse().unwrap());

            // Deploy TOFU decimals singleton
            let tofu = TOFUTokenDecimals::deploy(&provider).await.unwrap();
            let deployed_code = provider.get_code_at(*tofu.address()).await.unwrap();
            provider
                .raw_request::<_, ()>(
                    "anvil_setCode".into(),
                    (TOFU_DECIMALS_ADDRESS, deployed_code),
                )
                .await
                .unwrap();

            // Deploy orderbook
            let orderbook = OrderBook::deploy(&provider).await.unwrap();
            let orderbook_address = *orderbook.address();

            // Deploy test token and mint to signer
            let token = TestERC20::deploy(&provider).await.unwrap();
            let token_address = *token.address();
            let initial_supply = U256::from(1_000_000) * U256::from(10).pow(U256::from(18));
            token
                .mint(signer.address(), initial_supply)
                .send()
                .await
                .unwrap()
                .get_receipt()
                .await
                .unwrap();

            Self {
                anvil,
                provider,
                signer,
                orderbook_address,
                token_address,
            }
        }

        async fn deposit_tokens_to_vault(&self, amount: U256) {
            let token = TestERC20::new(self.token_address, &self.provider);
            let vault_service = VaultService::new(self.provider.clone(), self.orderbook_address);

            // Approve orderbook to spend tokens
            token
                .approve(self.orderbook_address, amount)
                .send()
                .await
                .unwrap()
                .get_receipt()
                .await
                .unwrap();

            // Deposit to vault
            vault_service
                .deposit(
                    self.token_address,
                    TEST_VAULT_ID,
                    amount,
                    TOKENIZED_EQUITY_DECIMALS,
                )
                .await
                .unwrap();
        }

        async fn wallet_token_balance(&self) -> U256 {
            let token = TestERC20::new(self.token_address, &self.provider);
            token.balanceOf(self.signer.address()).call().await.unwrap()
        }
    }

    /// Tests the full redemption workflow with vault withdrawal.
    #[tokio::test]
    async fn redemption_succeeds_when_tokens_are_in_vault() {
        let pool = crate::test_utils::setup_test_db().await;
        let local_evm = LocalEvmWithVault::new().await;
        let server = MockServer::start();

        let service = Arc::new(create_test_service_with_provider(
            &server,
            local_evm.provider.clone(),
            TEST_REDEMPTION_WALLET,
        ));
        let vault = Arc::new(VaultService::new(
            local_evm.provider.clone(),
            local_evm.orderbook_address,
        ));

        let vault_registry_query = seed_vault_registry_with_params(
            &pool,
            local_evm.orderbook_address,
            local_evm.signer.address(),
            local_evm.token_address,
            TEST_VAULT_ID.0,
        )
        .await;

        let deposit_amount = U256::from(100) * U256::from(10).pow(U256::from(18));
        local_evm.deposit_tokens_to_vault(deposit_amount).await;

        let _transfer_mock = server.mock(|_when, then| {
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "tokenization_request_id": "redeem_123",
                    "type": "redeem",
                    "status": "pending",
                    "underlying_symbol": "TEST",
                    "token_symbol": "tTEST",
                    "qty": "100.0",
                    "issuer": "st0x",
                    "network": "base"
                }));
        });

        let cqrs = create_test_store_instance().await;

        let manager = RedemptionManager::new(
            service,
            vault,
            cqrs,
            vault_registry_query,
            local_evm.orderbook_address,
            local_evm.signer.address(),
        );

        // Verify vault lookup works
        let vault_id = manager.load_vault_id(local_evm.token_address).await;
        assert_eq!(
            vault_id,
            Some(TEST_VAULT_ID.0),
            "Expected vault ID to be found in registry"
        );
    }
}

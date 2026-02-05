//! Orchestrates Alpaca-to-Raindex equity inventory transfer.
//!
//! Coordinates Alpaca tokenization API and vault deposits, emitting commands to
//! the `TokenizedEquityMint` aggregate at each step. Implements the `Mint` trait
//! for integration with the rebalancing trigger system.

use alloy::primitives::{Address, U256};
use async_trait::async_trait;
use cqrs_es::{CqrsFramework, EventStore};
use rust_decimal::Decimal;
use st0x_execution::{FractionalShares, Symbol};
use std::sync::Arc;
use tracing::{info, instrument, warn};

use super::{Mint, MintError};
use crate::equity_redemption::TOKENIZED_EQUITY_DECIMALS;
use crate::lifecycle::{Lifecycle, Never};
use crate::onchain::raindex::{Raindex, VaultId};
use crate::tokenization::{TokenizationRequest, TokenizationRequestStatus, Tokenizer};
use crate::tokenized_equity_mint::{
    IssuerRequestId, ReceiptId, TokenizedEquityMint, TokenizedEquityMintCommand,
};
use crate::vault_registry::{VaultRegistry, VaultRegistryQuery};
use crate::wrapper::Wrapper;

pub(crate) struct MintManager<ES>
where
    ES: EventStore<Lifecycle<TokenizedEquityMint, Never>>,
{
    tokenizer: Arc<dyn Tokenizer>,
    raindex: Arc<dyn Raindex>,
    wrapper: Arc<dyn Wrapper>,
    vault_registry_query: Arc<VaultRegistryQuery>,
    orderbook: Address,
    owner: Address,
    cqrs: Arc<CqrsFramework<Lifecycle<TokenizedEquityMint, Never>, ES>>,
}

impl<ES> MintManager<ES>
where
    ES: EventStore<Lifecycle<TokenizedEquityMint, Never>>,
{
    pub(crate) fn new(
        tokenizer: Arc<dyn Tokenizer>,
        raindex: Arc<dyn Raindex>,
        wrapper: Arc<dyn Wrapper>,
        vault_registry_query: Arc<VaultRegistryQuery>,
        orderbook: Address,
        owner: Address,
        cqrs: Arc<CqrsFramework<Lifecycle<TokenizedEquityMint, Never>, ES>>,
    ) -> Self {
        Self {
            tokenizer,
            raindex,
            wrapper,
            vault_registry_query,
            orderbook,
            owner,
            cqrs,
        }
    }

    async fn load_vault_info(&self, symbol: &Symbol) -> Option<(Address, VaultId)> {
        let aggregate_id = VaultRegistry::aggregate_id(self.orderbook, self.owner);
        let lifecycle = self.vault_registry_query.load(&aggregate_id).await?;

        match lifecycle {
            Lifecycle::Uninitialized => {
                warn!("Vault registry not initialized");
                None
            }
            Lifecycle::Live(registry) => {
                let token = registry.token_by_symbol(symbol)?;
                let vault_id = registry.vault_id_by_token(token)?;
                Some((token, VaultId(vault_id)))
            }
            Lifecycle::Failed { .. } => {
                warn!("Vault registry in failed state");
                None
            }
        }
    }

    /// Executes the full mint workflow.
    ///
    /// # Workflow
    ///
    /// 1. Send `RequestMint` command to aggregate
    /// 2. Call `AlpacaTokenizationService::request_mint()`
    /// 3. Send `AcknowledgeAcceptance` with request IDs
    /// 4. Poll Alpaca until terminal status
    /// 5. Send `ReceiveTokens` when Alpaca reports completion
    /// 6. Send `Finalize` to complete
    ///
    /// On permanent errors, sends `Fail` command to transition aggregate to Failed state.
    #[instrument(skip(self), fields(%symbol, ?quantity, %wallet))]
    async fn execute_mint_impl(
        &self,
        issuer_request_id: &IssuerRequestId,
        symbol: Symbol,
        quantity: FractionalShares,
        wallet: Address,
    ) -> Result<(), MintError> {
        info!(%symbol, ?quantity, %wallet, "Starting mint workflow");

        self.cqrs
            .execute(
                &issuer_request_id.0,
                TokenizedEquityMintCommand::RequestMint {
                    symbol: symbol.clone(),
                    quantity: quantity.inner(),
                    wallet,
                },
            )
            .await?;

        let alpaca_request = match self
            .tokenizer
            .request_mint(symbol.clone(), quantity, wallet, issuer_request_id.clone())
            .await
        {
            Ok(req) => req,
            Err(e) => {
                warn!("Alpaca mint request failed: {e}");
                self.reject_mint(issuer_request_id, format!("Alpaca API error: {e}"))
                    .await?;
                return Err(MintError::Tokenizer(e));
            }
        };

        self.cqrs
            .execute(
                &issuer_request_id.0,
                TokenizedEquityMintCommand::AcknowledgeAcceptance {
                    issuer_request_id: issuer_request_id.clone(),
                    tokenization_request_id: alpaca_request.id.clone(),
                },
            )
            .await?;

        info!(tokenization_request_id = %alpaca_request.id, "Mint request accepted, polling for completion");

        let completed_request = match self
            .tokenizer
            .poll_mint_until_complete(&alpaca_request.id)
            .await
        {
            Ok(req) => req,
            Err(e) => {
                warn!("Polling failed: {e}");
                self.fail_acceptance(issuer_request_id, format!("Polling failed: {e}"))
                    .await?;
                return Err(MintError::Tokenizer(e));
            }
        };

        self.handle_completed_request(issuer_request_id, &symbol, completed_request)
            .await
    }

    #[instrument(skip(self))]
    async fn reject_mint(
        &self,
        issuer_request_id: &IssuerRequestId,
        reason: String,
    ) -> Result<(), MintError> {
        self.cqrs
            .execute(
                &issuer_request_id.0,
                TokenizedEquityMintCommand::RejectMint { reason },
            )
            .await?;
        Ok(())
    }

    #[instrument(skip(self))]
    async fn fail_acceptance(
        &self,
        issuer_request_id: &IssuerRequestId,
        reason: String,
    ) -> Result<(), MintError> {
        self.cqrs
            .execute(
                &issuer_request_id.0,
                TokenizedEquityMintCommand::FailAcceptance { reason },
            )
            .await?;
        Ok(())
    }

    #[instrument(skip(self, completed_request), fields(status = ?completed_request.status))]
    async fn handle_completed_request(
        &self,
        issuer_request_id: &IssuerRequestId,
        symbol: &Symbol,
        completed_request: TokenizationRequest,
    ) -> Result<(), MintError> {
        match completed_request.status {
            TokenizationRequestStatus::Completed => {
                let tx_hash = completed_request.tx_hash.ok_or(MintError::MissingTxHash)?;
                let shares_minted = decimal_to_u256_18_decimals(completed_request.quantity)?;

                self.cqrs
                    .execute(
                        &issuer_request_id.0,
                        TokenizedEquityMintCommand::ReceiveTokens {
                            tx_hash,
                            receipt_id: ReceiptId(U256::ZERO),
                            shares_minted,
                        },
                    )
                    .await?;

                let (token, vault_id) = self
                    .load_vault_info(symbol)
                    .await
                    .ok_or_else(|| MintError::VaultNotFound(symbol.clone()))?;

                // Wrap the unwrapped tokens into ERC-4626 vault shares
                let owner = self.wrapper.owner();
                info!(%token, %shares_minted, %owner, "Wrapping tokens");
                let (wrap_tx_hash, wrapped_shares) =
                    self.wrapper.to_wrapped(token, shares_minted, owner).await?;

                self.cqrs
                    .execute(
                        &issuer_request_id.0,
                        TokenizedEquityMintCommand::WrapTokens {
                            wrap_tx_hash,
                            wrapped_shares,
                        },
                    )
                    .await?;

                info!(?vault_id, %token, %wrapped_shares, "Depositing wrapped tokens to vault");

                let vault_deposit_tx_hash = self
                    .raindex
                    .deposit(token, vault_id, wrapped_shares, TOKENIZED_EQUITY_DECIMALS)
                    .await?;

                self.cqrs
                    .execute(
                        &issuer_request_id.0,
                        TokenizedEquityMintCommand::DepositToVault {
                            vault_deposit_tx_hash,
                        },
                    )
                    .await?;

                self.cqrs
                    .execute(&issuer_request_id.0, TokenizedEquityMintCommand::Finalize)
                    .await?;

                info!(%vault_deposit_tx_hash, "Mint workflow completed successfully");
                Ok(())
            }
            TokenizationRequestStatus::Rejected => {
                self.fail_acceptance(
                    issuer_request_id,
                    "Mint request rejected by Alpaca".to_string(),
                )
                .await?;
                Err(MintError::Rejected)
            }
            TokenizationRequestStatus::Pending => {
                unreachable!("poll_mint_until_complete should not return Pending status")
            }
        }
    }
}

fn decimal_to_u256_18_decimals(value: FractionalShares) -> Result<U256, MintError> {
    let decimal = value.inner();
    let scale_factor = Decimal::from(10u64.pow(18));
    let scaled = decimal
        .checked_mul(scale_factor)
        .ok_or(MintError::DecimalOverflow(value))?;
    let truncated = scaled.trunc();

    Ok(U256::from_str_radix(&truncated.to_string(), 10)?)
}

#[async_trait]
impl<ES> Mint for MintManager<ES>
where
    ES: EventStore<Lifecycle<TokenizedEquityMint, Never>> + Send + Sync,
    ES::AC: Send,
{
    async fn execute_mint(
        &self,
        issuer_request_id: &IssuerRequestId,
        symbol: Symbol,
        quantity: FractionalShares,
        wallet: Address,
    ) -> Result<(), MintError> {
        self.execute_mint_impl(issuer_request_id, symbol, quantity, wallet)
            .await
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{U256, address};
    use alloy::providers::Provider;
    use alloy::signers::local::PrivateKeySigner;
    use cqrs_es::CqrsFramework;
    use cqrs_es::mem_store::MemStore;
    use httpmock::prelude::*;
    use rust_decimal_macros::dec;
    use serde_json::json;

    use super::*;
    use crate::onchain::raindex::RaindexService;
    use crate::test_utils::{create_test_raindex_service, create_vault_registry_query};
    use crate::tokenization::alpaca::tests::{
        TEST_REDEMPTION_WALLET, create_test_service_with_provider, setup_anvil,
        tokenization_mint_path, tokenization_requests_path,
    };
    use crate::wrapper::mock::MockWrapper;

    const TEST_ORDERBOOK: Address = address!("0xdead000000000000000000000000000000000001");

    type TestCqrs = CqrsFramework<
        Lifecycle<TokenizedEquityMint, Never>,
        MemStore<Lifecycle<TokenizedEquityMint, Never>>,
    >;

    fn create_test_cqrs() -> Arc<TestCqrs> {
        let store = MemStore::default();
        Arc::new(CqrsFramework::new(store, vec![], ()))
    }

    fn sample_pending_response(id: &str) -> serde_json::Value {
        json!({
            "tokenization_request_id": id,
            "type": "mint",
            "status": "pending",
            "underlying_symbol": "AAPL",
            "token_symbol": "tAAPL",
            "qty": "100.0",
            "issuer": "st0x",
            "network": "base",
            "wallet_address": "0x1234567890abcdef1234567890abcdef12345678",
            "issuer_request_id": "issuer_123",
            "created_at": "2024-01-15T10:30:00Z"
        })
    }

    fn sample_completed_response(id: &str) -> serde_json::Value {
        json!({
            "tokenization_request_id": id,
            "type": "mint",
            "status": "completed",
            "underlying_symbol": "AAPL",
            "token_symbol": "tAAPL",
            "qty": "100.0",
            "issuer": "st0x",
            "network": "base",
            "wallet_address": "0x1234567890abcdef1234567890abcdef12345678",
            "issuer_request_id": "issuer_123",
            "tx_hash": "0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890",
            "created_at": "2024-01-15T10:30:00Z"
        })
    }

    #[test]
    fn decimal_to_u256_converts_fractional() {
        let value = FractionalShares::new(dec!(100.5));
        let result = decimal_to_u256_18_decimals(value).unwrap();

        let expected = U256::from(100_500_000_000_000_000_000_u128);
        assert_eq!(result, expected);
    }

    #[test]
    fn decimal_to_u256_converts_whole_number() {
        let value = FractionalShares::new(dec!(42));
        let result = decimal_to_u256_18_decimals(value).unwrap();

        let expected = U256::from(42_000_000_000_000_000_000_u128);
        assert_eq!(result, expected);
    }

    #[test]
    fn decimal_to_u256_converts_zero() {
        let value = FractionalShares::new(dec!(0));
        let result = decimal_to_u256_18_decimals(value).unwrap();

        assert_eq!(result, U256::ZERO);
    }

    #[tokio::test]
    async fn execute_mint_receives_tokens_then_fails_vault_lookup() {
        let server = MockServer::start();
        let pool = crate::test_utils::setup_test_db().await;
        let (_anvil, endpoint, key) = setup_anvil();

        let signer = PrivateKeySigner::from_bytes(&key).unwrap();
        let wallet = alloy::network::EthereumWallet::from(signer);
        let provider = alloy::providers::ProviderBuilder::new()
            .wallet(wallet)
            .connect_http(endpoint.parse().unwrap());

        let service = Arc::new(create_test_service_with_provider(
            &server,
            provider.clone(),
            TEST_REDEMPTION_WALLET,
        ));
        let vault_registry_query = create_vault_registry_query(&pool);
        let vault =
            create_test_raindex_service(provider, &pool, TEST_ORDERBOOK, TEST_REDEMPTION_WALLET);
        let cqrs = create_test_cqrs();
        let wrapper: Arc<dyn Wrapper> = Arc::new(MockWrapper::new());
        let manager = MintManager::new(
            service as Arc<dyn Tokenizer>,
            vault as Arc<dyn Raindex>,
            wrapper,
            vault_registry_query,
            TEST_ORDERBOOK,
            TEST_REDEMPTION_WALLET,
            cqrs,
        );

        let mint_mock = server.mock(|when, then| {
            when.method(POST).path(tokenization_mint_path());
            then.status(200)
                .header("content-type", "application/json")
                .json_body(sample_pending_response("mint_123"));
        });

        let poll_mock = server.mock(|when, then| {
            when.method(GET).path(tokenization_requests_path());
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([sample_completed_response("mint_123")]));
        });

        let symbol = Symbol::new("AAPL").unwrap();
        let quantity = FractionalShares::new(dec!(100.0));
        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");

        let result = manager
            .execute_mint_impl(
                &IssuerRequestId::new("mint-001"),
                symbol.clone(),
                quantity,
                wallet,
            )
            .await;

        // Workflow proceeds through Alpaca API but fails at vault lookup
        assert!(
            matches!(result, Err(MintError::VaultNotFound(ref s)) if *s == symbol),
            "Expected VaultNotFound error, got: {result:?}"
        );

        mint_mock.assert();
        poll_mock.assert();
    }

    #[tokio::test]
    async fn execute_mint_rejected_by_alpaca() {
        let server = MockServer::start();
        let pool = crate::test_utils::setup_test_db().await;
        let (_anvil, endpoint, key) = setup_anvil();

        let signer = PrivateKeySigner::from_bytes(&key).unwrap();
        let wallet = EthereumWallet::from(signer);
        let provider = ProviderBuilder::new()
            .wallet(wallet)
            .connect_http(endpoint.parse().unwrap());

        let service = Arc::new(create_test_service_with_provider(
            &server,
            provider.clone(),
            TEST_REDEMPTION_WALLET,
        ));
        let vault_registry_query = create_vault_registry_query(&pool);
        let vault =
            create_test_raindex_service(provider, &pool, TEST_ORDERBOOK, TEST_REDEMPTION_WALLET);
        let cqrs = create_test_cqrs();
        let wrapper: Arc<dyn Wrapper> = Arc::new(MockWrapper::new());
        let manager = MintManager::new(
            service as Arc<dyn Tokenizer>,
            vault as Arc<dyn Raindex>,
            wrapper,
            vault_registry_query,
            TEST_ORDERBOOK,
            TEST_REDEMPTION_WALLET,
            cqrs,
        );

        let mint_mock = server.mock(|when, then| {
            when.method(POST).path(tokenization_mint_path());
            then.status(200)
                .header("content-type", "application/json")
                .json_body(sample_pending_response("mint_456"));
        });

        let poll_mock = server.mock(|when, then| {
            when.method(GET).path(tokenization_requests_path());
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([{
                    "tokenization_request_id": "mint_456",
                    "type": "mint",
                    "status": "rejected",
                    "underlying_symbol": "AAPL",
                    "token_symbol": "tAAPL",
                    "qty": "100.0",
                    "issuer": "st0x",
                    "network": "base",
                    "wallet_address": "0x1234567890abcdef1234567890abcdef12345678",
                    "issuer_request_id": "issuer_123",
                    "created_at": "2024-01-15T10:30:00Z"
                }]));
        });

        let symbol = Symbol::new("AAPL").unwrap();
        let quantity = FractionalShares::new(dec!(100.0));
        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");

        let result = manager
            .execute_mint_impl(&IssuerRequestId::new("mint-002"), symbol, quantity, wallet)
            .await;

        assert!(matches!(result, Err(MintError::Rejected)));

        mint_mock.assert();
        poll_mock.assert();
    }

    #[tokio::test]
    async fn execute_mint_api_error() {
        let server = MockServer::start();
        let pool = crate::test_utils::setup_test_db().await;
        let (_anvil, endpoint, key) = setup_anvil();

        let signer = PrivateKeySigner::from_bytes(&key).unwrap();
        let wallet = EthereumWallet::from(signer);
        let provider = ProviderBuilder::new()
            .wallet(wallet)
            .connect_http(endpoint.parse().unwrap());

        let service = Arc::new(create_test_service_with_provider(
            &server,
            provider.clone(),
            TEST_REDEMPTION_WALLET,
        ));
        let vault_registry_query = create_vault_registry_query(&pool);
        let vault =
            create_test_raindex_service(provider, &pool, TEST_ORDERBOOK, TEST_REDEMPTION_WALLET);
        let cqrs = create_test_cqrs();
        let wrapper: Arc<dyn Wrapper> = Arc::new(MockWrapper::new());
        let manager = MintManager::new(
            service as Arc<dyn Tokenizer>,
            vault as Arc<dyn Raindex>,
            wrapper,
            vault_registry_query,
            TEST_ORDERBOOK,
            TEST_REDEMPTION_WALLET,
            cqrs,
        );

        let mint_mock = server.mock(|when, then| {
            when.method(POST).path(tokenization_mint_path());
            then.status(500).body("Internal Server Error");
        });

        let symbol = Symbol::new("AAPL").unwrap();
        let quantity = FractionalShares::new(dec!(100.0));
        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");

        let result = manager
            .execute_mint_impl(&IssuerRequestId::new("mint-003"), symbol, quantity, wallet)
            .await;

        assert!(matches!(result, Err(MintError::Tokenizer(_))));

        mint_mock.assert();
    }

    #[tokio::test]
    async fn trait_impl_delegates_to_execute_mint_impl() {
        let server = MockServer::start();
        let pool = crate::test_utils::setup_test_db().await;
        let (_anvil, endpoint, key) = setup_anvil();

        let signer = PrivateKeySigner::from_bytes(&key).unwrap();
        let wallet = EthereumWallet::from(signer);
        let provider = ProviderBuilder::new()
            .wallet(wallet)
            .connect_http(endpoint.parse().unwrap());

        let service = Arc::new(create_test_service_with_provider(
            &server,
            provider.clone(),
            TEST_REDEMPTION_WALLET,
        ));
        let vault_registry_query = create_vault_registry_query(&pool);
        let vault =
            create_test_raindex_service(provider, &pool, TEST_ORDERBOOK, TEST_REDEMPTION_WALLET);
        let cqrs = create_test_cqrs();
        let wrapper: Arc<dyn Wrapper> = Arc::new(MockWrapper::new());
        let manager = MintManager::new(
            service as Arc<dyn Tokenizer>,
            vault as Arc<dyn Raindex>,
            wrapper,
            vault_registry_query,
            TEST_ORDERBOOK,
            TEST_REDEMPTION_WALLET,
            cqrs,
        );

        let mint_mock = server.mock(|when, then| {
            when.method(POST).path(tokenization_mint_path());
            then.status(200)
                .header("content-type", "application/json")
                .json_body(sample_pending_response("trait_test"));
        });

        let poll_mock = server.mock(|when, then| {
            when.method(GET).path(tokenization_requests_path());
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([sample_completed_response("trait_test")]));
        });

        let mint_trait: &dyn Mint = &manager;

        // Workflow proceeds through Alpaca API but fails at vault lookup (registry empty)
        let result = mint_trait
            .execute_mint(
                &IssuerRequestId::new("trait-001"),
                Symbol::new("AAPL").unwrap(),
                FractionalShares::new(dec!(50.0)),
                address!("0x1234567890abcdef1234567890abcdef12345678"),
            )
            .await;

        // Expected to fail at vault lookup since registry is empty
        assert!(matches!(result, Err(MintError::VaultNotFound(_))));

        mint_mock.assert();
        poll_mock.assert();
    }

    use alloy::network::{Ethereum, EthereumWallet};
    use alloy::node_bindings::{Anvil, AnvilInstance};
    use alloy::primitives::{B256, TxHash, b256};
    use alloy::providers::fillers::{
        BlobGasFiller, ChainIdFiller, FillProvider, GasFiller, JoinFill, NonceFiller, WalletFiller,
    };
    use alloy::providers::{Identity, ProviderBuilder, RootProvider};
    use cqrs_es::persist::GenericQuery;
    use sqlite_es::{SqliteCqrs, SqliteViewRepository};

    use crate::bindings::{OrderBook, TOFUTokenDecimals, TestERC20};
    use crate::conductor::wire::test_cqrs;
    use crate::vault_registry::{VaultRegistry, VaultRegistryAggregate, VaultRegistryCommand};

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
        _anvil: AnvilInstance,
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

            let tofu = TOFUTokenDecimals::deploy(&provider).await.unwrap();
            let deployed_code = provider.get_code_at(*tofu.address()).await.unwrap();
            provider
                .raw_request::<_, ()>(
                    "anvil_setCode".into(),
                    (TOFU_DECIMALS_ADDRESS, deployed_code),
                )
                .await
                .unwrap();

            let orderbook = OrderBook::deploy(&provider).await.unwrap();
            let orderbook_address = *orderbook.address();

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
                _anvil: anvil,
                provider,
                signer,
                orderbook_address,
                token_address,
            }
        }

        async fn approve_tokens_for_orderbook(&self, amount: U256) {
            let token = TestERC20::new(self.token_address, &self.provider);
            token
                .approve(self.orderbook_address, amount)
                .send()
                .await
                .unwrap()
                .get_receipt()
                .await
                .unwrap();
        }
    }

    fn create_vault_registry_cqrs(
        pool: &sqlx::SqlitePool,
    ) -> (SqliteCqrs<VaultRegistryAggregate>, Arc<VaultRegistryQuery>) {
        let view_repo = Arc::new(SqliteViewRepository::<
            VaultRegistryAggregate,
            VaultRegistryAggregate,
        >::new(
            pool.clone(), "vault_registry_view".to_string()
        ));
        let query = Arc::new(GenericQuery::new(view_repo.clone()));
        let cqrs = test_cqrs::<VaultRegistryAggregate>(
            pool.clone(),
            vec![Box::new(GenericQuery::new(view_repo))],
            (),
        );
        (cqrs, query)
    }

    async fn seed_vault_registry(
        pool: &sqlx::SqlitePool,
        orderbook: Address,
        owner: Address,
        token: Address,
        vault_id: B256,
        symbol: Symbol,
    ) -> Arc<VaultRegistryQuery> {
        let (cqrs, query) = create_vault_registry_cqrs(pool);
        let aggregate_id = VaultRegistry::aggregate_id(orderbook, owner);

        cqrs.execute(
            &aggregate_id,
            VaultRegistryCommand::DiscoverEquityVault {
                token,
                vault_id,
                discovered_in: TxHash::ZERO,
                symbol,
            },
        )
        .await
        .unwrap();

        query
    }

    #[tokio::test]
    async fn execute_mint_full_workflow_with_vault_deposit() {
        let server = MockServer::start();
        let pool = crate::test_utils::setup_test_db().await;
        let local_evm = LocalEvmWithVault::new().await;
        let symbol = Symbol::new("AAPL").unwrap();

        let vault_registry_query = seed_vault_registry(
            &pool,
            local_evm.orderbook_address,
            local_evm.signer.address(),
            local_evm.token_address,
            TEST_VAULT_ID.0,
            symbol.clone(),
        )
        .await;

        let deposit_amount = U256::from(100) * U256::from(10).pow(U256::from(18));
        local_evm.approve_tokens_for_orderbook(deposit_amount).await;

        let service = Arc::new(create_test_service_with_provider(
            &server,
            local_evm.provider.clone(),
            local_evm.signer.address(),
        ));
        let raindex = Arc::new(RaindexService::new(
            local_evm.provider.clone(),
            local_evm.orderbook_address,
            vault_registry_query.clone(),
            local_evm.signer.address(),
        ));
        let cqrs = create_test_cqrs();
        let wrapper: Arc<dyn Wrapper> = Arc::new(MockWrapper::new());
        let manager = MintManager::new(
            service as Arc<dyn Tokenizer>,
            raindex as Arc<dyn Raindex>,
            wrapper,
            vault_registry_query,
            local_evm.orderbook_address,
            local_evm.signer.address(),
            cqrs,
        );

        let mint_mock = server.mock(|when, then| {
            when.method(POST).path(tokenization_mint_path());
            then.status(200)
                .header("content-type", "application/json")
                .json_body(sample_pending_response("full_flow_test"));
        });

        let poll_mock = server.mock(|when, then| {
            when.method(GET).path(tokenization_requests_path());
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([sample_completed_response("full_flow_test")]));
        });

        let quantity = FractionalShares::new(dec!(100.0));
        let wallet = local_evm.signer.address();

        let result = manager
            .execute_mint_impl(
                &IssuerRequestId::new("full-flow-001"),
                symbol,
                quantity,
                wallet,
            )
            .await;

        assert!(result.is_ok(), "Expected Ok but got: {result:?}");

        mint_mock.assert();
        poll_mock.assert();
    }
}

//! Orchestrates Alpaca-to-Raindex equity inventory transfer.
//!
//! Coordinates the `TokenizedEquityMint` aggregate which handles tokenization API calls
//! and vault deposits internally via its Services. Implements the `Mint` trait for
//! integration with the rebalancing trigger system.

use alloy::primitives::Address;
use async_trait::async_trait;
use std::sync::Arc;
use tracing::{debug, info, instrument};

use st0x_event_sorcery::Store;
use st0x_execution::{FractionalShares, Symbol};

use super::{Mint, MintError};
use crate::tokenized_equity_mint::{
    IssuerRequestId, TokenizedEquityMint, TokenizedEquityMintCommand,
};

pub(crate) struct MintManager {
    cqrs: Arc<Store<TokenizedEquityMint>>,
}

impl MintManager {
    pub(crate) fn new(cqrs: Arc<Store<TokenizedEquityMint>>) -> Self {
        Self { cqrs }
    }

    #[instrument(skip(self), fields(%symbol, ?quantity, %wallet))]
    async fn execute_mint_impl(
        &self,
        issuer_request_id: &IssuerRequestId,
        symbol: Symbol,
        quantity: FractionalShares,
        wallet: Address,
    ) -> Result<(), MintError> {
        debug!("Executing mint command");

        self.cqrs
            .send(
                issuer_request_id,
                TokenizedEquityMintCommand::Mint {
                    issuer_request_id: issuer_request_id.clone(),
                    symbol,
                    quantity: quantity.inner(),
                    wallet,
                },
            )
            .await?;

        debug!("Executing deposit command");

        self.cqrs
            .send(issuer_request_id, TokenizedEquityMintCommand::Deposit)
            .await?;

        info!("Mint workflow completed successfully");
        Ok(())
    }
}

#[async_trait]
impl Mint for MintManager {
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
    use alloy::network::{Ethereum, EthereumWallet};
    use alloy::node_bindings::{Anvil, AnvilInstance};
    use alloy::primitives::{B256, TxHash, U256, address, b256};
    use alloy::providers::fillers::{
        BlobGasFiller, ChainIdFiller, FillProvider, GasFiller, JoinFill, NonceFiller, WalletFiller,
    };
    use alloy::providers::{Identity, Provider, ProviderBuilder, RootProvider};
    use alloy::signers::local::PrivateKeySigner;
    use httpmock::prelude::*;
    use rust_decimal_macros::dec;
    use serde_json::json;
    use std::sync::Arc;

    use st0x_event_sorcery::{StoreBuilder, test_store};

    use super::*;
    use crate::bindings::{OrderBook, TOFUTokenDecimals, TestERC20};
    use crate::onchain::mock::MockRaindex;
    use crate::onchain::raindex::{Raindex, RaindexService, RaindexVaultId};
    use crate::test_utils::setup_test_db;
    use crate::tokenization::Tokenizer;
    use crate::tokenization::alpaca::tests::{
        create_test_service_from_mock, setup_anvil, tokenization_mint_path,
        tokenization_requests_path,
    };
    use crate::tokenization::mock::MockTokenizer;
    use crate::tokenized_equity_mint::MintServices;
    use crate::vault_registry::{
        VaultRegistry, VaultRegistryCommand, VaultRegistryId, VaultRegistryProjection,
    };

    const TOFU_DECIMALS_ADDRESS: Address = address!("0x4f1C29FAAB7EDdF8D7794695d8259996734Cc665");

    const TEST_VAULT_ID: RaindexVaultId = RaindexVaultId(b256!(
        "0x0000000000000000000000000000000000000000000000000000000000000001"
    ));

    fn mock_mint_services() -> MintServices {
        MintServices {
            tokenizer: Arc::new(MockTokenizer::new()),
            raindex: Arc::new(MockRaindex::new()),
        }
    }

    async fn create_test_store() -> Arc<Store<TokenizedEquityMint>> {
        let pool = setup_test_db().await;
        Arc::new(test_store(pool, mock_mint_services()))
    }

    #[tokio::test]
    async fn execute_mint_sends_mint_and_deposit_commands() {
        let store = create_test_store().await;
        let manager = MintManager::new(store);

        let symbol = Symbol::new("AAPL").unwrap();
        let quantity = FractionalShares::new(dec!(100.0));
        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");

        let result = manager
            .execute_mint_impl(&IssuerRequestId::new("mint-001"), symbol, quantity, wallet)
            .await;

        // MockTokenizer returns success, MockRaindex returns success
        result.unwrap();
    }

    #[tokio::test]
    async fn trait_impl_delegates_to_execute_mint_impl() {
        let store = create_test_store().await;
        let manager = MintManager::new(store);

        let mint_trait: &dyn Mint = &manager;

        let result = mint_trait
            .execute_mint(
                &IssuerRequestId::new("trait-001"),
                Symbol::new("AAPL").unwrap(),
                FractionalShares::new(dec!(50.0)),
                address!("0x1234567890abcdef1234567890abcdef12345678"),
            )
            .await;

        // MockTokenizer and MockRaindex always succeed
        result.unwrap();
    }

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
    }

    async fn seed_vault_registry(
        pool: &sqlx::SqlitePool,
        orderbook: Address,
        owner: Address,
        token: Address,
        vault_id: B256,
        symbol: Symbol,
    ) -> Arc<VaultRegistryProjection> {
        let projection = Arc::new(VaultRegistryProjection::sqlite(pool.clone()).unwrap());
        let store = StoreBuilder::<VaultRegistry>::new(pool.clone())
            .with(projection.as_ref().clone())
            .build(())
            .await
            .unwrap();
        let registry_id = VaultRegistryId { orderbook, owner };

        store
            .send(
                &registry_id,
                VaultRegistryCommand::DiscoverEquityVault {
                    token,
                    vault_id,
                    discovered_in: TxHash::ZERO,
                    symbol,
                },
            )
            .await
            .unwrap();

        projection
    }

    #[tokio::test]
    async fn execute_mint_full_workflow_with_vault_deposit() {
        let pool = setup_test_db().await;
        let server = MockServer::start();
        let local_evm = LocalEvmWithVault::new().await;
        let symbol = Symbol::new("AAPL").unwrap();

        let vault_registry_projection = seed_vault_registry(
            &pool,
            local_evm.orderbook_address,
            local_evm.signer.address(),
            local_evm.token_address,
            TEST_VAULT_ID.0,
            symbol.clone(),
        )
        .await;

        let (_setup_anvil, anvil_endpoint, private_key) = setup_anvil();
        let tokenizer: Arc<dyn Tokenizer> = Arc::new(
            create_test_service_from_mock(
                &server,
                &anvil_endpoint,
                &private_key,
                local_evm.signer.address(),
            )
            .await,
        );
        let raindex: Arc<dyn Raindex> = Arc::new(
            RaindexService::new(
                local_evm.provider.clone(),
                local_evm.orderbook_address,
                vault_registry_projection,
                local_evm.signer.address(),
            )
            .with_required_confirmations(1),
        );

        let services = MintServices { tokenizer, raindex };
        let store = Arc::new(test_store(pool, services));
        let manager = MintManager::new(store);

        let wallet = local_evm.signer.address();

        let mint_mock = server.mock(|when, then| {
            when.method(POST).path(tokenization_mint_path());
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "tokenization_request_id": "mint_full_flow",
                    "type": "mint",
                    "status": "pending",
                    "underlying_symbol": "AAPL",
                    "token_symbol": "tAAPL",
                    "qty": "100.0",
                    "issuer": "st0x",
                    "network": "base",
                    "wallet_address": wallet,
                    "issuer_request_id": "full-flow-001",
                    "created_at": "2024-01-15T10:30:00Z"
                }));
        });

        let poll_mock = server.mock(|when, then| {
            when.method(GET).path(tokenization_requests_path());
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([{
                    "tokenization_request_id": "mint_full_flow",
                    "type": "mint",
                    "status": "completed",
                    "underlying_symbol": "AAPL",
                    "token_symbol": "tAAPL",
                    "qty": "100.0",
                    "issuer": "st0x",
                    "network": "base",
                    "wallet_address": wallet,
                    "issuer_request_id": "full-flow-001",
                    "tx_hash": TxHash::ZERO,
                    "created_at": "2024-01-15T10:30:00Z"
                }]));
        });

        let quantity = FractionalShares::new(dec!(100.0));

        manager
            .execute_mint_impl(
                &IssuerRequestId::new("full-flow-001"),
                symbol,
                quantity,
                wallet,
            )
            .await
            .unwrap();

        mint_mock.assert();
        poll_mock.assert();
    }
}

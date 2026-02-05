//! RedemptionService implements Redeemer by composing VaultService and AlpacaTokenizationService.

use alloy::primitives::{Address, TxHash, U256};
use alloy::providers::Provider;
use async_trait::async_trait;
use std::sync::Arc;
use tracing::{info, warn};

use crate::equity_redemption::{RedeemError, Redeemer};
use crate::lifecycle::Lifecycle;
use crate::onchain::vault::{VaultId, VaultService};
use crate::tokenization::AlpacaTokenizationService;
use crate::vault_registry::{VaultRegistry, VaultRegistryQuery};

/// Our tokenized equity tokens use 18 decimals.
pub(super) const TOKENIZED_EQUITY_DECIMALS: u8 = 18;

/// Service that implements Redeemer by composing vault and Alpaca services.
pub(crate) struct RedemptionService<P>
where
    P: Provider + Clone,
{
    vault: Arc<VaultService<P>>,
    alpaca: Arc<AlpacaTokenizationService<P>>,
    vault_registry_query: Arc<VaultRegistryQuery>,
    orderbook: Address,
    owner: Address,
}

impl<P> RedemptionService<P>
where
    P: Provider + Clone,
{
    pub(crate) fn new(
        vault: Arc<VaultService<P>>,
        alpaca: Arc<AlpacaTokenizationService<P>>,
        vault_registry_query: Arc<VaultRegistryQuery>,
        orderbook: Address,
        owner: Address,
    ) -> Self {
        Self {
            vault,
            alpaca,
            vault_registry_query,
            orderbook,
            owner,
        }
    }

    /// Returns a reference to the Alpaca service for polling operations.
    pub(crate) fn alpaca(&self) -> &AlpacaTokenizationService<P> {
        &self.alpaca
    }

    async fn load_vault_id(&self, token: Address) -> Option<VaultId> {
        let aggregate_id = VaultRegistry::aggregate_id(self.orderbook, self.owner);

        let lifecycle = self.vault_registry_query.load(&aggregate_id).await?;

        match lifecycle {
            Lifecycle::Uninitialized => {
                warn!("Vault registry not initialized");
                None
            }
            Lifecycle::Live(registry) => registry.vault_id_by_token(token).map(VaultId),
            Lifecycle::Failed { .. } => {
                warn!("Vault registry in failed state");
                None
            }
        }
    }
}

#[async_trait]
impl<P> Redeemer for RedemptionService<P>
where
    P: Provider + Clone + Send + Sync + 'static,
{
    async fn withdraw_from_vault(
        &self,
        token: Address,
        amount: U256,
    ) -> Result<TxHash, RedeemError> {
        let vault_id = self
            .load_vault_id(token)
            .await
            .ok_or(RedeemError::VaultNotFound(token))?;

        info!(?vault_id, %token, %amount, "Withdrawing tokens from vault");

        Ok(self
            .vault
            .withdraw(token, vault_id, amount, TOKENIZED_EQUITY_DECIMALS)
            .await?)
    }

    async fn send_for_redemption(
        &self,
        token: Address,
        amount: U256,
    ) -> Result<(Address, TxHash), RedeemError> {
        info!(%token, %amount, "Sending tokens for redemption");

        let tx_hash = self.alpaca.send_for_redemption(token, amount).await?;

        Ok((self.alpaca.redemption_wallet(), tx_hash))
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{Address, B256, TxHash, address, b256};
    use cqrs_es::persist::GenericQuery;
    use sqlite_es::{SqliteCqrs, SqliteViewRepository};
    use sqlx::SqlitePool;
    use st0x_execution::Symbol;

    use super::*;
    use crate::conductor::wire::test_cqrs;
    use crate::test_utils::setup_test_db;
    use crate::vault_registry::{VaultRegistryAggregate, VaultRegistryCommand};

    const TEST_ORDERBOOK: Address = address!("0x1111111111111111111111111111111111111111");
    const TEST_OWNER: Address = address!("0x2222222222222222222222222222222222222222");
    const TEST_TOKEN: Address = address!("0x3333333333333333333333333333333333333333");
    const TEST_VAULT_ID: B256 =
        b256!("0x0000000000000000000000000000000000000000000000000000000000000001");

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
        let cqrs = test_cqrs::<VaultRegistryAggregate>(
            pool.clone(),
            vec![Box::new(GenericQuery::new(view_repo))],
            (),
        );

        (cqrs, query)
    }

    async fn seed_vault_registry(
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

    /// Test fixture that provides access to the private `load_vault_id` method.
    ///
    /// Since `load_vault_id` is private and the full `RedemptionService` requires
    /// provider-dependent services (VaultService, AlpacaTokenizationService), we
    /// test the vault registry lookup logic in isolation.
    struct VaultIdLoader {
        vault_registry_query: Arc<VaultRegistryQuery>,
        orderbook: Address,
        owner: Address,
    }

    impl VaultIdLoader {
        fn new(
            vault_registry_query: Arc<VaultRegistryQuery>,
            orderbook: Address,
            owner: Address,
        ) -> Self {
            Self {
                vault_registry_query,
                orderbook,
                owner,
            }
        }

        async fn load_vault_id(&self, token: Address) -> Option<VaultId> {
            let aggregate_id = VaultRegistry::aggregate_id(self.orderbook, self.owner);
            let lifecycle = self.vault_registry_query.load(&aggregate_id).await?;

            match lifecycle {
                Lifecycle::Live(registry) => registry.vault_id_by_token(token).map(VaultId),
                Lifecycle::Uninitialized | Lifecycle::Failed { .. } => None,
            }
        }
    }

    #[tokio::test]
    async fn load_vault_id_returns_none_when_registry_not_found() {
        let pool = setup_test_db().await;
        let (_cqrs, query) = create_vault_registry_cqrs(&pool);
        let loader = VaultIdLoader::new(query, TEST_ORDERBOOK, TEST_OWNER);

        let result = loader.load_vault_id(TEST_TOKEN).await;

        assert!(
            result.is_none(),
            "Expected None when registry doesn't exist"
        );
    }

    #[tokio::test]
    async fn load_vault_id_returns_none_for_unknown_token() {
        let pool = setup_test_db().await;
        let registered_token = address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let unknown_token = address!("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");

        let query = seed_vault_registry(
            &pool,
            TEST_ORDERBOOK,
            TEST_OWNER,
            registered_token,
            TEST_VAULT_ID,
        )
        .await;
        let loader = VaultIdLoader::new(query, TEST_ORDERBOOK, TEST_OWNER);

        let result = loader.load_vault_id(unknown_token).await;

        assert!(result.is_none(), "Expected None for unknown token");
    }

    #[tokio::test]
    async fn load_vault_id_returns_vault_id_when_token_registered() {
        let pool = setup_test_db().await;
        let query =
            seed_vault_registry(&pool, TEST_ORDERBOOK, TEST_OWNER, TEST_TOKEN, TEST_VAULT_ID).await;
        let loader = VaultIdLoader::new(query, TEST_ORDERBOOK, TEST_OWNER);

        let result = loader.load_vault_id(TEST_TOKEN).await;

        assert_eq!(
            result,
            Some(VaultId(TEST_VAULT_ID)),
            "Expected VaultId for registered token"
        );
    }

    #[tokio::test]
    async fn load_vault_id_returns_none_for_different_orderbook() {
        let pool = setup_test_db().await;
        let different_orderbook = address!("0x9999999999999999999999999999999999999999");

        let query =
            seed_vault_registry(&pool, TEST_ORDERBOOK, TEST_OWNER, TEST_TOKEN, TEST_VAULT_ID).await;
        let loader = VaultIdLoader::new(query, different_orderbook, TEST_OWNER);

        let result = loader.load_vault_id(TEST_TOKEN).await;

        assert!(
            result.is_none(),
            "Expected None when querying different orderbook"
        );
    }

    #[tokio::test]
    async fn load_vault_id_returns_none_for_different_owner() {
        let pool = setup_test_db().await;
        let different_owner = address!("0x8888888888888888888888888888888888888888");

        let query =
            seed_vault_registry(&pool, TEST_ORDERBOOK, TEST_OWNER, TEST_TOKEN, TEST_VAULT_ID).await;
        let loader = VaultIdLoader::new(query, TEST_ORDERBOOK, different_owner);

        let result = loader.load_vault_id(TEST_TOKEN).await;

        assert!(
            result.is_none(),
            "Expected None when querying different owner"
        );
    }

    #[tokio::test]
    async fn load_vault_id_returns_correct_vault_id_among_multiple() {
        let pool = setup_test_db().await;
        let (cqrs, query) = create_vault_registry_cqrs(&pool);
        let aggregate_id = VaultRegistry::aggregate_id(TEST_ORDERBOOK, TEST_OWNER);

        let token_a = address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let token_b = address!("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
        let vault_id_a =
            b256!("0x000000000000000000000000000000000000000000000000000000000000000a");
        let vault_id_b =
            b256!("0x000000000000000000000000000000000000000000000000000000000000000b");

        cqrs.execute(
            &aggregate_id,
            VaultRegistryCommand::DiscoverEquityVault {
                token: token_a,
                vault_id: vault_id_a,
                discovered_in: TxHash::ZERO,
                symbol: Symbol::new("AAPL").unwrap(),
            },
        )
        .await
        .unwrap();

        cqrs.execute(
            &aggregate_id,
            VaultRegistryCommand::DiscoverEquityVault {
                token: token_b,
                vault_id: vault_id_b,
                discovered_in: TxHash::ZERO,
                symbol: Symbol::new("MSFT").unwrap(),
            },
        )
        .await
        .unwrap();

        let loader = VaultIdLoader::new(query, TEST_ORDERBOOK, TEST_OWNER);

        assert_eq!(
            loader.load_vault_id(token_a).await,
            Some(VaultId(vault_id_a)),
            "Expected vault_id_a for token_a"
        );
        assert_eq!(
            loader.load_vault_id(token_b).await,
            Some(VaultId(vault_id_b)),
            "Expected vault_id_b for token_b"
        );
    }
}

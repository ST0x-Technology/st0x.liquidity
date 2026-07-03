//! Vault resolution over the [`VaultRegistry`] projection.
//!
//! Separates "which vault holds this token/symbol" (a registry read) from the
//! Rain OrderBook chain operations in [`st0x_raindex`]. The chain
//! layer takes an explicit vault id; this module resolves that id from the
//! [`VaultRegistry`] aggregate so the chain layer never needs the projection
//! and can live in a standalone crate independent of `st0x-config`.

use alloy::primitives::Address;
use async_trait::async_trait;
#[cfg(test)]
use std::collections::BTreeMap;
use std::sync::Arc;

use st0x_event_sorcery::ProjectionError;
use st0x_execution::Symbol;
use st0x_raindex::RaindexVaultId;

use crate::vault_registry::{VaultRegistry, VaultRegistryId, VaultRegistryProjection};

/// Resolves Raindex vault ids from the [`VaultRegistry`] projection.
///
/// Consumers that must act on a vault (deposit, withdraw, recover) but only
/// know a token address or trading symbol use this to discover the primary
/// vault before invoking chain operations.
#[async_trait]
pub(crate) trait VaultLookup: Send + Sync {
    /// Returns the primary vault id registered for `token`.
    async fn vault_id_for_token(&self, token: Address) -> Result<RaindexVaultId, VaultLookupError>;

    /// Returns the wrapped equity token address registered for `symbol`.
    async fn vault_token_for_symbol(&self, symbol: &Symbol) -> Result<Address, VaultLookupError>;
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum VaultLookupError {
    #[error("Vault registry not found for aggregate {0}")]
    RegistryNotFound(VaultRegistryId),
    #[error(transparent)]
    Projection(#[from] ProjectionError<VaultRegistry>),
    #[error("Vault not found for token {0}")]
    VaultNotFound(Address),
    #[error("Token not found for symbol {0}")]
    TokenNotFound(Symbol),
    #[error("Multiple vault tokens found for symbol {symbol}: {tokens:?}")]
    AmbiguousTokenForSymbol {
        symbol: Symbol,
        tokens: Vec<Address>,
    },
}

/// [`VaultLookup`] backed by the live [`VaultRegistry`] projection for a single
/// orderbook/owner pair.
pub(crate) struct VaultRegistryLookup {
    projection: Arc<VaultRegistryProjection>,
    registry_id: VaultRegistryId,
}

impl VaultRegistryLookup {
    pub(crate) fn new(
        projection: Arc<VaultRegistryProjection>,
        registry_id: VaultRegistryId,
    ) -> Self {
        Self {
            projection,
            registry_id,
        }
    }

    async fn load_registry(&self) -> Result<VaultRegistry, VaultLookupError> {
        self.projection
            .load(&self.registry_id)
            .await?
            .ok_or_else(|| VaultLookupError::RegistryNotFound(self.registry_id.clone()))
    }
}

#[async_trait]
impl VaultLookup for VaultRegistryLookup {
    async fn vault_id_for_token(&self, token: Address) -> Result<RaindexVaultId, VaultLookupError> {
        let registry = self.load_registry().await?;
        registry
            .primary_vault_id_by_token(token)
            .map(RaindexVaultId)
            .ok_or(VaultLookupError::VaultNotFound(token))
    }

    async fn vault_token_for_symbol(&self, symbol: &Symbol) -> Result<Address, VaultLookupError> {
        let registry = self.load_registry().await?;
        let tokens: Vec<_> = registry
            .equity_vaults
            .iter()
            .filter_map(|(token, vaults)| {
                vaults
                    .values()
                    .any(|vault| vault.symbol == *symbol)
                    .then_some(*token)
            })
            .collect();

        match tokens.as_slice() {
            [] => Err(VaultLookupError::TokenNotFound(symbol.clone())),
            [token] => Ok(*token),
            _ => Err(VaultLookupError::AmbiguousTokenForSymbol {
                symbol: symbol.clone(),
                tokens,
            }),
        }
    }
}

#[cfg(test)]
pub(crate) struct MockVaultLookup {
    vaults: BTreeMap<Address, RaindexVaultId>,
    tokens: BTreeMap<Symbol, Address>,
    default_vault: Option<RaindexVaultId>,
}

#[cfg(test)]
impl MockVaultLookup {
    pub(crate) fn new() -> Self {
        Self {
            vaults: BTreeMap::new(),
            tokens: BTreeMap::new(),
            default_vault: None,
        }
    }

    pub(crate) fn with_symbol_token(mut self, symbol: Symbol, token: Address) -> Self {
        self.tokens.insert(symbol, token);
        self
    }

    pub(crate) fn with_vault(mut self, token: Address, vault_id: RaindexVaultId) -> Self {
        self.vaults.insert(token, vault_id);
        self
    }

    pub(crate) fn with_default_vault(mut self, vault_id: RaindexVaultId) -> Self {
        self.default_vault = Some(vault_id);
        self
    }
}

#[cfg(test)]
#[async_trait]
impl VaultLookup for MockVaultLookup {
    async fn vault_id_for_token(&self, token: Address) -> Result<RaindexVaultId, VaultLookupError> {
        self.vaults
            .get(&token)
            .copied()
            .or(self.default_vault)
            .ok_or(VaultLookupError::VaultNotFound(token))
    }

    async fn vault_token_for_symbol(&self, symbol: &Symbol) -> Result<Address, VaultLookupError> {
        self.tokens
            .get(symbol)
            .copied()
            .ok_or_else(|| VaultLookupError::TokenNotFound(symbol.clone()))
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{B256, address, b256};

    use st0x_event_sorcery::StoreBuilder;

    use super::*;
    use crate::test_utils::setup_test_db;
    use crate::vault_registry::VaultRegistryCommand;

    const TEST_ORDERBOOK: Address = address!("0x1234567890123456789012345678901234567890");
    const TEST_OWNER: Address = address!("0xabcdefabcdefabcdefabcdefabcdefabcdefabcd");
    const TEST_TOKEN: Address = address!("0x9876543210987654321098765432109876543210");
    const OTHER_TOKEN: Address = address!("0x2222222222222222222222222222222222222222");
    const TEST_VAULT_ID: B256 =
        b256!("0x0000000000000000000000000000000000000000000000000000000000000001");
    const EARLIER_SORTING_VAULT_ID: B256 =
        b256!("0x0000000000000000000000000000000000000000000000000000000000000000");

    fn test_symbol() -> Symbol {
        Symbol::new("AAPL").unwrap()
    }

    async fn seeded_lookup() -> VaultRegistryLookup {
        let pool = setup_test_db().await;
        let (store, projection) = StoreBuilder::<VaultRegistry>::new(pool)
            .build(())
            .await
            .unwrap();

        store
            .send(
                &VaultRegistryId {
                    orderbook: TEST_ORDERBOOK,
                    owner: TEST_OWNER,
                },
                VaultRegistryCommand::SeedEquityVaultFromConfig {
                    token: TEST_TOKEN,
                    vault_id: TEST_VAULT_ID,
                    symbol: test_symbol(),
                },
            )
            .await
            .unwrap();

        VaultRegistryLookup::new(
            projection,
            VaultRegistryId {
                orderbook: TEST_ORDERBOOK,
                owner: TEST_OWNER,
            },
        )
    }

    #[tokio::test]
    async fn vault_id_for_token_returns_primary_vault() {
        let pool = setup_test_db().await;
        let (store, projection) = StoreBuilder::<VaultRegistry>::new(pool)
            .build(())
            .await
            .unwrap();
        let registry_id = VaultRegistryId {
            orderbook: TEST_ORDERBOOK,
            owner: TEST_OWNER,
        };

        store
            .send(
                &registry_id,
                VaultRegistryCommand::SeedEquityVaultFromConfig {
                    token: TEST_TOKEN,
                    vault_id: TEST_VAULT_ID,
                    symbol: test_symbol(),
                },
            )
            .await
            .unwrap();
        store
            .send(
                &registry_id,
                VaultRegistryCommand::SeedEquityVaultFromConfig {
                    token: TEST_TOKEN,
                    vault_id: EARLIER_SORTING_VAULT_ID,
                    symbol: test_symbol(),
                },
            )
            .await
            .unwrap();
        let lookup = VaultRegistryLookup::new(
            projection,
            VaultRegistryId {
                orderbook: TEST_ORDERBOOK,
                owner: TEST_OWNER,
            },
        );

        assert_eq!(
            lookup.vault_id_for_token(TEST_TOKEN).await.unwrap(),
            RaindexVaultId(TEST_VAULT_ID),
            "primary vault must preserve first-seeded config order, not BTreeMap ordering",
        );
    }

    #[tokio::test]
    async fn vault_token_for_symbol_returns_registered_token() {
        let lookup = seeded_lookup().await;

        assert_eq!(
            lookup.vault_token_for_symbol(&test_symbol()).await.unwrap(),
            TEST_TOKEN
        );
    }

    #[tokio::test]
    async fn vault_token_for_symbol_rejects_ambiguous_tokens() {
        let pool = setup_test_db().await;
        let (store, projection) = StoreBuilder::<VaultRegistry>::new(pool)
            .build(())
            .await
            .unwrap();
        let registry_id = VaultRegistryId {
            orderbook: TEST_ORDERBOOK,
            owner: TEST_OWNER,
        };

        for (token, vault_id) in [
            (TEST_TOKEN, TEST_VAULT_ID),
            (OTHER_TOKEN, EARLIER_SORTING_VAULT_ID),
        ] {
            store
                .send(
                    &registry_id,
                    VaultRegistryCommand::SeedEquityVaultFromConfig {
                        token,
                        vault_id,
                        symbol: test_symbol(),
                    },
                )
                .await
                .unwrap();
        }

        let lookup = VaultRegistryLookup::new(
            projection,
            VaultRegistryId {
                orderbook: TEST_ORDERBOOK,
                owner: TEST_OWNER,
            },
        );
        let error = lookup
            .vault_token_for_symbol(&test_symbol())
            .await
            .unwrap_err();

        assert!(
            matches!(
                error,
                VaultLookupError::AmbiguousTokenForSymbol { ref symbol, ref tokens }
                    if symbol == &test_symbol()
                        && *tokens == vec![OTHER_TOKEN, TEST_TOKEN]
            ),
            "expected ambiguous token error with both tokens, got: {error:?}",
        );
    }

    #[tokio::test]
    async fn vault_id_for_unknown_token_is_vault_not_found() {
        let lookup = seeded_lookup().await;
        let other = address!("0x2222222222222222222222222222222222222222");

        let error = lookup.vault_id_for_token(other).await.unwrap_err();

        assert!(
            matches!(error, VaultLookupError::VaultNotFound(token) if token == other),
            "expected VaultNotFound for unregistered token, got: {error:?}"
        );
    }

    #[tokio::test]
    async fn token_for_unknown_symbol_is_token_not_found() {
        let lookup = seeded_lookup().await;
        let symbol = Symbol::new("MSFT").unwrap();

        let error = lookup.vault_token_for_symbol(&symbol).await.unwrap_err();

        assert!(
            matches!(&error, VaultLookupError::TokenNotFound(unknown) if unknown == &symbol),
            "expected TokenNotFound for unregistered symbol, got: {error:?}"
        );
    }

    #[tokio::test]
    async fn empty_registry_is_registry_not_found() {
        let pool = setup_test_db().await;
        let (_store, projection) = StoreBuilder::<VaultRegistry>::new(pool)
            .build(())
            .await
            .unwrap();
        let lookup = VaultRegistryLookup::new(
            projection,
            VaultRegistryId {
                orderbook: TEST_ORDERBOOK,
                owner: TEST_OWNER,
            },
        );

        let error = lookup.vault_id_for_token(TEST_TOKEN).await.unwrap_err();

        let VaultLookupError::RegistryNotFound(id) = error else {
            panic!("expected RegistryNotFound for empty registry, got: {error:?}");
        };
        assert_eq!(id.orderbook, TEST_ORDERBOOK);
        assert_eq!(id.owner, TEST_OWNER);
    }

    #[tokio::test]
    async fn mock_vault_lookup_rejects_unconfigured_inputs() {
        let lookup = MockVaultLookup::new()
            .with_symbol_token(test_symbol(), TEST_TOKEN)
            .with_vault(TEST_TOKEN, RaindexVaultId(TEST_VAULT_ID));

        assert_eq!(
            lookup.vault_token_for_symbol(&test_symbol()).await.unwrap(),
            TEST_TOKEN,
        );
        assert_eq!(
            lookup.vault_id_for_token(TEST_TOKEN).await.unwrap(),
            RaindexVaultId(TEST_VAULT_ID),
        );

        let other_symbol = Symbol::new("MSFT").unwrap();
        let token_error = lookup
            .vault_token_for_symbol(&other_symbol)
            .await
            .unwrap_err();
        assert!(
            matches!(&token_error, VaultLookupError::TokenNotFound(symbol) if symbol == &other_symbol),
            "mock must reject unconfigured symbols, got: {token_error:?}",
        );

        let vault_error = lookup.vault_id_for_token(OTHER_TOKEN).await.unwrap_err();
        assert!(
            matches!(vault_error, VaultLookupError::VaultNotFound(token) if token == OTHER_TOKEN),
            "mock must reject unconfigured tokens, got: {vault_error:?}",
        );

        let fallback_lookup =
            MockVaultLookup::new().with_default_vault(RaindexVaultId(TEST_VAULT_ID));
        assert_eq!(
            fallback_lookup
                .vault_id_for_token(OTHER_TOKEN)
                .await
                .unwrap(),
            RaindexVaultId(TEST_VAULT_ID),
            "explicit fallback is only for tests that are not asserting token-specific lookup",
        );
    }
}

//! `VaultRegistry` aggregate for tracking Raindex vaults.
//!
//! Vaults are either auto-discovered from onchain trade events
//! (ClearV3/TakeOrderV3) or pre-seeded from config. Each vault tracks its
//! provenance via [`VaultProvenance`]. The registry distinguishes between:
//! - **Equity Vaults**: Hold tokenized equities (token != USDC)
//! - **USDC Vaults**: Hold USDC for trading

use alloy::hex::FromHexError;
use alloy::primitives::{Address, B256, TxHash};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt;
use std::str::FromStr;
use std::sync::Arc;
use thiserror::Error;
use tracing::{debug, info};

use st0x_execution::Symbol;

use st0x_event_sorcery::{DomainEvent, EventSourced, Never, Projection, SendError, Store, Table};

use st0x_config::{Ctx, CtxError};

use crate::conductor::job::{Job, JobQueue, Label};

/// Typed identifier for VaultRegistry aggregates, keyed by
/// orderbook and owner address pair.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct VaultRegistryId {
    pub(crate) orderbook: Address,
    pub(crate) owner: Address,
}

impl fmt::Display for VaultRegistryId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.orderbook, self.owner)
    }
}

#[derive(Debug, Error)]
pub(crate) enum ParseVaultRegistryIdError {
    #[error("expected 'orderbook:owner', got '{id_provided}'")]
    MissingDelimiter { id_provided: String },

    #[error("invalid orderbook address: {0}")]
    Orderbook(FromHexError),

    #[error("invalid owner address: {0}")]
    Owner(FromHexError),
}

impl FromStr for VaultRegistryId {
    type Err = ParseVaultRegistryIdError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let (orderbook_str, owner_str) =
            value
                .split_once(':')
                .ok_or_else(|| ParseVaultRegistryIdError::MissingDelimiter {
                    id_provided: value.to_string(),
                })?;
        let orderbook = orderbook_str
            .parse()
            .map_err(ParseVaultRegistryIdError::Orderbook)?;
        let owner = owner_str
            .parse()
            .map_err(ParseVaultRegistryIdError::Owner)?;
        Ok(Self { orderbook, owner })
    }
}

#[async_trait]
impl EventSourced for VaultRegistry {
    type Id = VaultRegistryId;
    type Event = VaultRegistryEvent;
    type Command = VaultRegistryCommand;
    type Error = Never;
    type Services = ();
    type Materialized = Table;

    const AGGREGATE_TYPE: &'static str = "VaultRegistry";
    const PROJECTION: Table = Table("vault_registry_view");
    const SCHEMA_VERSION: u64 = 3;

    fn originate(event: &Self::Event) -> Option<Self> {
        let mut registry = Self::empty(event.timestamp());
        registry.apply_event(event);
        Some(registry)
    }

    fn evolve(entity: &Self, event: &Self::Event) -> Result<Option<Self>, Self::Error> {
        let mut new_registry = entity.clone();
        new_registry.apply_event(event);
        Ok(Some(new_registry))
    }

    async fn initialize(
        command: Self::Command,
        _services: &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        Ok(vec![Self::command_to_event(command)])
    }

    async fn transition(
        &self,
        command: Self::Command,
        _services: &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        match &command {
            VaultRegistryCommand::SeedEquityVaultFromConfig {
                token, vault_id, ..
            } => {
                if let Some(vaults_for_token) = self.equity_vaults.get(token)
                    && vaults_for_token.contains_key(vault_id)
                {
                    return Ok(vec![]);
                }
            }

            VaultRegistryCommand::SeedUsdcVaultFromConfig { vault_id } => {
                if self.usdc_vaults.contains_key(vault_id) {
                    return Ok(vec![]);
                }
            }

            VaultRegistryCommand::SetPrimaryEquityVaultFromConfig {
                token, vault_id, ..
            } => {
                if self.primary_equity_vault.get(token) == Some(vault_id) {
                    return Ok(vec![]);
                }
            }

            VaultRegistryCommand::SetPrimaryUsdcVaultFromConfig { vault_id } => {
                if self.primary_usdc_vault == Some(*vault_id) {
                    return Ok(vec![]);
                }
            }

            VaultRegistryCommand::DiscoverEquityVault { .. }
            | VaultRegistryCommand::DiscoverUsdcVault { .. } => {}
        }

        Ok(vec![Self::command_to_event(command)])
    }
}

/// Registry state tracking all discovered vaults for an orderbook/owner pair.
///
/// Supports multiple vaults per asset. Equity vaults are grouped by token
/// address, with each token mapping to a set of vaults keyed by vault ID.
/// Multiple USDC vaults are supported, keyed by vault ID.
///
/// Each asset also tracks a **primary** vault ID. Config seeding reasserts
/// the first vault currently configured for the asset; otherwise the first
/// discovered vault remains primary. The primary vault is used for
/// single-vault operations like deposits and withdrawals, preserving
/// operator intent from current config ordering.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct VaultRegistry {
    /// Equity vaults, grouped by token address then keyed by vault ID.
    pub(crate) equity_vaults: BTreeMap<Address, BTreeMap<B256, EquityVault>>,
    /// USDC vaults, keyed by vault ID.
    pub(crate) usdc_vaults: BTreeMap<B256, UsdcVault>,
    /// Primary equity vault per token, used by deposit/withdraw operations.
    primary_equity_vault: BTreeMap<Address, B256>,
    /// Primary USDC vault, used by deposit/withdraw operations.
    primary_usdc_vault: Option<B256>,
    pub(crate) last_updated: DateTime<Utc>,
}

pub(crate) type VaultRegistryProjection = Projection<VaultRegistry>;

/// How a vault was added to the registry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum VaultProvenance {
    Discovered {
        tx_hash: TxHash,
        discovered_at: DateTime<Utc>,
    },
    Seeded {
        seeded_at: DateTime<Utc>,
    },
}

/// Equity vault holding tokenized shares (base asset for a trading pair).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct EquityVault {
    pub(crate) token: Address,
    pub(crate) vault_id: B256,
    pub(crate) symbol: Symbol,
    pub(crate) provenance: VaultProvenance,
}

/// USDC vault holding the quote asset for all trading pairs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct UsdcVault {
    pub(crate) vault_id: B256,
    pub(crate) provenance: VaultProvenance,
}

impl VaultRegistry {
    pub(crate) fn token_by_symbol(&self, symbol: &Symbol) -> Option<Address> {
        self.equity_vaults
            .iter()
            .find(|(_, vaults)| vaults.values().any(|vault| vault.symbol == *symbol))
            .map(|(token, _)| *token)
    }

    /// Returns the primary vault ID for a token.
    pub(crate) fn primary_vault_id_by_token(&self, token: Address) -> Option<B256> {
        self.primary_equity_vault.get(&token).copied()
    }

    /// Returns all vault IDs registered for a token.
    #[cfg(test)]
    fn all_vault_ids_by_token(&self, token: Address) -> Vec<B256> {
        self.equity_vaults
            .get(&token)
            .map(|vaults| vaults.keys().copied().collect())
            .unwrap_or_default()
    }

    /// Returns the primary USDC vault ID.
    #[cfg(test)]
    fn primary_usdc_vault_id(&self) -> Option<B256> {
        self.primary_usdc_vault
    }

    fn empty(timestamp: DateTime<Utc>) -> Self {
        Self {
            equity_vaults: BTreeMap::new(),
            usdc_vaults: BTreeMap::new(),
            primary_equity_vault: BTreeMap::new(),
            primary_usdc_vault: None,
            last_updated: timestamp,
        }
    }

    fn insert_equity_vault(&mut self, token: Address, vault_id: B256, vault: EquityVault) {
        self.equity_vaults
            .entry(token)
            .or_default()
            .insert(vault_id, vault);
    }

    fn apply_event(&mut self, event: &VaultRegistryEvent) {
        self.last_updated = event.timestamp();

        match event {
            VaultRegistryEvent::EquityVaultDiscovered {
                token,
                vault_id,
                discovered_in,
                discovered_at,
                symbol,
            } => {
                self.primary_equity_vault.entry(*token).or_insert(*vault_id);

                self.insert_equity_vault(
                    *token,
                    *vault_id,
                    EquityVault {
                        token: *token,
                        vault_id: *vault_id,
                        symbol: symbol.clone(),
                        provenance: VaultProvenance::Discovered {
                            tx_hash: *discovered_in,
                            discovered_at: *discovered_at,
                        },
                    },
                );
            }

            VaultRegistryEvent::UsdcVaultDiscovered {
                vault_id,
                discovered_in,
                discovered_at,
            } => {
                self.primary_usdc_vault.get_or_insert(*vault_id);

                self.usdc_vaults.insert(
                    *vault_id,
                    UsdcVault {
                        vault_id: *vault_id,
                        provenance: VaultProvenance::Discovered {
                            tx_hash: *discovered_in,
                            discovered_at: *discovered_at,
                        },
                    },
                );
            }

            VaultRegistryEvent::EquityVaultSeededFromConfig {
                token,
                vault_id,
                seeded_at,
                symbol,
            } => {
                self.primary_equity_vault.entry(*token).or_insert(*vault_id);

                self.insert_equity_vault(
                    *token,
                    *vault_id,
                    EquityVault {
                        token: *token,
                        vault_id: *vault_id,
                        symbol: symbol.clone(),
                        provenance: VaultProvenance::Seeded {
                            seeded_at: *seeded_at,
                        },
                    },
                );
            }

            VaultRegistryEvent::UsdcVaultSeededFromConfig {
                vault_id,
                seeded_at,
            } => {
                self.primary_usdc_vault.get_or_insert(*vault_id);

                self.usdc_vaults.insert(
                    *vault_id,
                    UsdcVault {
                        vault_id: *vault_id,
                        provenance: VaultProvenance::Seeded {
                            seeded_at: *seeded_at,
                        },
                    },
                );
            }

            VaultRegistryEvent::PrimaryEquityVaultSetFromConfig {
                token, vault_id, ..
            } => {
                self.primary_equity_vault.insert(*token, *vault_id);
            }

            VaultRegistryEvent::PrimaryUsdcVaultSetFromConfig { vault_id, .. } => {
                self.primary_usdc_vault = Some(*vault_id);
            }
        }
    }

    fn command_to_event(command: VaultRegistryCommand) -> VaultRegistryEvent {
        let now = Utc::now();
        match command {
            VaultRegistryCommand::DiscoverEquityVault {
                token,
                vault_id,
                discovered_in,
                symbol,
            } => VaultRegistryEvent::EquityVaultDiscovered {
                token,
                vault_id,
                discovered_in,
                discovered_at: now,
                symbol,
            },
            VaultRegistryCommand::DiscoverUsdcVault {
                vault_id,
                discovered_in,
            } => VaultRegistryEvent::UsdcVaultDiscovered {
                vault_id,
                discovered_in,
                discovered_at: now,
            },

            VaultRegistryCommand::SeedEquityVaultFromConfig {
                token,
                vault_id,
                symbol,
            } => VaultRegistryEvent::EquityVaultSeededFromConfig {
                token,
                vault_id,
                seeded_at: now,
                symbol,
            },

            VaultRegistryCommand::SeedUsdcVaultFromConfig { vault_id } => {
                VaultRegistryEvent::UsdcVaultSeededFromConfig {
                    vault_id,
                    seeded_at: now,
                }
            }

            VaultRegistryCommand::SetPrimaryEquityVaultFromConfig {
                token,
                vault_id,
                symbol,
            } => VaultRegistryEvent::PrimaryEquityVaultSetFromConfig {
                token,
                vault_id,
                configured_at: now,
                symbol,
            },

            VaultRegistryCommand::SetPrimaryUsdcVaultFromConfig { vault_id } => {
                VaultRegistryEvent::PrimaryUsdcVaultSetFromConfig {
                    vault_id,
                    configured_at: now,
                }
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum VaultRegistryCommand {
    DiscoverEquityVault {
        token: Address,
        vault_id: B256,
        discovered_in: TxHash,
        symbol: Symbol,
    },
    DiscoverUsdcVault {
        vault_id: B256,
        discovered_in: TxHash,
    },
    /// Pre-seed an equity vault from config (no onchain discovery).
    SeedEquityVaultFromConfig {
        token: Address,
        vault_id: B256,
        symbol: Symbol,
    },
    /// Pre-seed the USDC vault from config (no onchain discovery).
    SeedUsdcVaultFromConfig { vault_id: B256 },
    /// Mark the currently configured primary equity vault.
    SetPrimaryEquityVaultFromConfig {
        token: Address,
        vault_id: B256,
        symbol: Symbol,
    },
    /// Mark the currently configured primary USDC vault.
    SetPrimaryUsdcVaultFromConfig { vault_id: B256 },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) enum VaultRegistryEvent {
    EquityVaultDiscovered {
        token: Address,
        vault_id: B256,
        discovered_in: TxHash,
        discovered_at: DateTime<Utc>,
        symbol: Symbol,
    },
    UsdcVaultDiscovered {
        vault_id: B256,
        discovered_in: TxHash,
        discovered_at: DateTime<Utc>,
    },
    /// Equity vault pre-seeded from config (no onchain transaction).
    EquityVaultSeededFromConfig {
        token: Address,
        vault_id: B256,
        seeded_at: DateTime<Utc>,
        symbol: Symbol,
    },
    /// USDC vault pre-seeded from config (no onchain transaction).
    UsdcVaultSeededFromConfig {
        vault_id: B256,
        seeded_at: DateTime<Utc>,
    },
    /// Primary equity vault selected from current config ordering.
    PrimaryEquityVaultSetFromConfig {
        token: Address,
        vault_id: B256,
        configured_at: DateTime<Utc>,
        symbol: Symbol,
    },
    /// Primary USDC vault selected from current config ordering.
    PrimaryUsdcVaultSetFromConfig {
        vault_id: B256,
        configured_at: DateTime<Utc>,
    },
}

impl VaultRegistryEvent {
    fn timestamp(&self) -> DateTime<Utc> {
        match self {
            Self::EquityVaultDiscovered { discovered_at, .. }
            | Self::UsdcVaultDiscovered { discovered_at, .. } => *discovered_at,
            Self::EquityVaultSeededFromConfig { seeded_at, .. }
            | Self::UsdcVaultSeededFromConfig { seeded_at, .. } => *seeded_at,
            Self::PrimaryEquityVaultSetFromConfig { configured_at, .. }
            | Self::PrimaryUsdcVaultSetFromConfig { configured_at, .. } => *configured_at,
        }
    }
}

impl DomainEvent for VaultRegistryEvent {
    fn event_type(&self) -> String {
        match self {
            Self::EquityVaultDiscovered { .. } => {
                "VaultRegistryEvent::EquityVaultDiscovered".to_string()
            }
            Self::UsdcVaultDiscovered { .. } => {
                "VaultRegistryEvent::UsdcVaultDiscovered".to_string()
            }
            Self::EquityVaultSeededFromConfig { .. } => {
                "VaultRegistryEvent::EquityVaultSeededFromConfig".to_string()
            }
            Self::UsdcVaultSeededFromConfig { .. } => {
                "VaultRegistryEvent::UsdcVaultSeededFromConfig".to_string()
            }
            Self::PrimaryEquityVaultSetFromConfig { .. } => {
                "VaultRegistryEvent::PrimaryEquityVaultSetFromConfig".to_string()
            }
            Self::PrimaryUsdcVaultSetFromConfig { .. } => {
                "VaultRegistryEvent::PrimaryUsdcVaultSetFromConfig".to_string()
            }
        }
    }

    fn event_version(&self) -> String {
        "1.0".to_string()
    }
}

/// A single equity vault to seed from config.
#[derive(Debug, Clone)]
pub(crate) struct EquityVaultSeed {
    pub(crate) token: Address,
    pub(crate) vault_id: B256,
    pub(crate) symbol: Symbol,
}

/// Apalis job that seeds the [`VaultRegistry`] aggregate from config.
///
/// Stateless payload: the registry id, store handle, and seed list all
/// live in [`SeedVaultRegistryCtx`]. Carries no data so the job survives
/// restarts without serializing config (which is reloaded at startup).
///
/// Idempotent: each seed command is a no-op if the vault is already
/// registered with the same id (see [`VaultRegistry::transition`]).
/// Re-running the job after a partial failure replays only the missing
/// seeds.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(crate) struct SeedVaultRegistry;

pub(crate) type SeedVaultRegistryJobQueue = JobQueue<SeedVaultRegistry>;

/// Bundled dependencies for [`SeedVaultRegistry`].
///
/// Only constructable via [`Self::from_config`], which performs the
/// pre-flight config validation. Holding an instance of this type is
/// proof that validation succeeded -- callers cannot bypass the check
/// by hand-rolling the fields.
pub(crate) struct SeedVaultRegistryCtx {
    vault_registry: Arc<Store<VaultRegistry>>,
    id: VaultRegistryId,
    equity_seeds: Vec<EquityVaultSeed>,
    equity_primary_seeds: Vec<EquityVaultSeed>,
    usdc_vault_ids: Vec<B256>,
    usdc_primary_vault_id: Option<B256>,
}

impl SeedVaultRegistryCtx {
    /// Builds a seeding context from configuration.
    ///
    /// Validates that every rebalancing-enabled equity has at least
    /// one configured `vault_id`. Returns [`CtxError::MissingEquityVaultId`]
    /// if any rebalancing-enabled equity lacks a vault, so config errors
    /// fail fast at construction rather than being retried by apalis.
    pub(crate) fn from_config(
        vault_registry: Arc<Store<VaultRegistry>>,
        ctx: &Ctx,
    ) -> Result<Self, Box<CtxError>> {
        for (symbol, equity_config) in &ctx.assets.equities.symbols {
            if equity_config.vault_ids.is_empty() && ctx.assets.is_rebalancing_enabled(symbol) {
                return Err(Box::new(CtxError::MissingEquityVaultId {
                    symbol: symbol.clone(),
                }));
            }
        }

        let id = VaultRegistryId {
            orderbook: ctx.evm.orderbook,
            owner: ctx.order_owner(),
        };

        let equity_seeds = ctx
            .assets
            .equities
            .symbols
            .iter()
            .flat_map(|(symbol, equity_config)| {
                equity_config
                    .vault_ids
                    .iter()
                    .map(move |vault_id| EquityVaultSeed {
                        token: equity_config.tokenized_equity_derivative,
                        vault_id: *vault_id,
                        symbol: symbol.clone(),
                    })
            })
            .collect();

        let equity_primary_seeds = ctx
            .assets
            .equities
            .symbols
            .iter()
            .filter_map(|(symbol, equity_config)| {
                equity_config
                    .vault_ids
                    .first()
                    .copied()
                    .map(|vault_id| EquityVaultSeed {
                        token: equity_config.tokenized_equity_derivative,
                        vault_id,
                        symbol: symbol.clone(),
                    })
            })
            .collect();

        let usdc_vault_ids = ctx
            .assets
            .cash
            .as_ref()
            .map(|cash| cash.vault_ids.clone())
            .unwrap_or_default();

        let usdc_primary_vault_id = ctx
            .assets
            .cash
            .as_ref()
            .and_then(|cash| cash.vault_ids.first().copied());

        Ok(Self {
            vault_registry,
            id,
            equity_seeds,
            equity_primary_seeds,
            usdc_vault_ids,
            usdc_primary_vault_id,
        })
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum SeedVaultRegistryError {
    #[error("VaultRegistry command failed: {0}")]
    VaultRegistry(#[from] SendError<VaultRegistry>),
}

impl Job<SeedVaultRegistryCtx> for SeedVaultRegistry {
    type Output = ();
    type Error = SeedVaultRegistryError;

    const WORKER_NAME: &'static str = "seed-vault-registry-worker";

    #[cfg(any(test, feature = "test-support"))]
    const JOB_KIND: crate::conductor::job::JobKind =
        crate::conductor::job::JobKind::SeedVaultRegistry;

    fn label(&self) -> Label {
        Label::new("SeedVaultRegistry")
    }

    async fn perform(&self, ctx: &SeedVaultRegistryCtx) -> Result<Self::Output, Self::Error> {
        for seed in &ctx.equity_seeds {
            debug!(
                symbol = %seed.symbol,
                vault_id = %seed.vault_id,
                token = %seed.token,
                "Seeding equity vault from config",
            );

            ctx.vault_registry
                .send(
                    &ctx.id,
                    VaultRegistryCommand::SeedEquityVaultFromConfig {
                        token: seed.token,
                        vault_id: seed.vault_id,
                        symbol: seed.symbol.clone(),
                    },
                )
                .await?;
        }

        for seed in &ctx.equity_primary_seeds {
            info!(
                symbol = %seed.symbol,
                vault_id = %seed.vault_id,
                token = %seed.token,
                "Setting configured primary equity vault",
            );

            ctx.vault_registry
                .send(
                    &ctx.id,
                    VaultRegistryCommand::SetPrimaryEquityVaultFromConfig {
                        token: seed.token,
                        vault_id: seed.vault_id,
                        symbol: seed.symbol.clone(),
                    },
                )
                .await?;
        }

        for vault_id in &ctx.usdc_vault_ids {
            info!(%vault_id, "Seeding USDC vault from config");

            ctx.vault_registry
                .send(
                    &ctx.id,
                    VaultRegistryCommand::SeedUsdcVaultFromConfig {
                        vault_id: *vault_id,
                    },
                )
                .await?;
        }

        if let Some(vault_id) = ctx.usdc_primary_vault_id {
            info!(%vault_id, "Setting configured primary USDC vault");

            ctx.vault_registry
                .send(
                    &ctx.id,
                    VaultRegistryCommand::SetPrimaryUsdcVaultFromConfig { vault_id },
                )
                .await?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{address, b256};
    use async_trait::async_trait;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use st0x_event_sorcery::{EntityList, Reactor, StoreBuilder, TestHarness, deps, replay};

    use super::*;
    use crate::test_utils::{setup_test_db, setup_test_pools};

    const TEST_ORDERBOOK: Address = address!("0x1234567890123456789012345678901234567890");
    const TEST_OWNER: Address = address!("0xabcdefabcdefabcdefabcdefabcdefabcdefabcd");
    const TEST_TOKEN: Address = address!("0x9876543210987654321098765432109876543210");
    const TEST_VAULT_ID: B256 =
        b256!("0x0000000000000000000000000000000000000000000000000000000000000001");
    const TEST_TX_HASH: TxHash =
        b256!("0x1111111111111111111111111111111111111111111111111111111111111111");

    fn test_symbol() -> Symbol {
        Symbol::new("AAPL").unwrap()
    }

    #[test]
    fn aggregate_id_format() {
        let id = VaultRegistryId {
            orderbook: TEST_ORDERBOOK,
            owner: TEST_OWNER,
        };
        let id_str = id.to_string();
        assert!(id_str.contains(&TEST_ORDERBOOK.to_string()));
        assert!(id_str.contains(&TEST_OWNER.to_string()));
    }

    #[tokio::test]
    async fn first_equity_discovery_initializes_registry() {
        let events = TestHarness::<VaultRegistry>::with(())
            .given_no_previous_events()
            .when(VaultRegistryCommand::DiscoverEquityVault {
                token: TEST_TOKEN,
                vault_id: TEST_VAULT_ID,
                discovered_in: TEST_TX_HASH,
                symbol: test_symbol(),
            })
            .await
            .events();

        assert_eq!(events.len(), 1);
        assert!(matches!(
            &events[0],
            VaultRegistryEvent::EquityVaultDiscovered {
                token,
                vault_id,
                symbol,
                ..
            } if *token == TEST_TOKEN
                && *vault_id == TEST_VAULT_ID
                && *symbol == test_symbol()
        ));
    }

    #[tokio::test]
    async fn first_usdc_discovery_initializes_registry() {
        let events = TestHarness::<VaultRegistry>::with(())
            .given_no_previous_events()
            .when(VaultRegistryCommand::DiscoverUsdcVault {
                vault_id: TEST_VAULT_ID,
                discovered_in: TEST_TX_HASH,
            })
            .await
            .events();

        assert_eq!(events.len(), 1);
        assert!(matches!(
            &events[0],
            VaultRegistryEvent::UsdcVaultDiscovered { vault_id, .. }
            if *vault_id == TEST_VAULT_ID
        ));
    }

    #[tokio::test]
    async fn discover_equity_vault_on_existing_registry() {
        let events = TestHarness::<VaultRegistry>::with(())
            .given(vec![VaultRegistryEvent::UsdcVaultDiscovered {
                vault_id: TEST_VAULT_ID,
                discovered_in: TEST_TX_HASH,
                discovered_at: Utc::now(),
            }])
            .when(VaultRegistryCommand::DiscoverEquityVault {
                token: TEST_TOKEN,
                vault_id: TEST_VAULT_ID,
                discovered_in: TEST_TX_HASH,
                symbol: test_symbol(),
            })
            .await
            .events();

        assert_eq!(events.len(), 1);
        assert!(matches!(
            &events[0],
            VaultRegistryEvent::EquityVaultDiscovered {
                token,
                vault_id,
                symbol,
                ..
            } if *token == TEST_TOKEN
                && *vault_id == TEST_VAULT_ID
                && *symbol == test_symbol()
        ));
    }

    #[tokio::test]
    async fn discover_usdc_vault_on_existing_registry() {
        let events = TestHarness::<VaultRegistry>::with(())
            .given(vec![VaultRegistryEvent::EquityVaultDiscovered {
                token: TEST_TOKEN,
                vault_id: TEST_VAULT_ID,
                discovered_in: TEST_TX_HASH,
                discovered_at: Utc::now(),
                symbol: test_symbol(),
            }])
            .when(VaultRegistryCommand::DiscoverUsdcVault {
                vault_id: TEST_VAULT_ID,
                discovered_in: TEST_TX_HASH,
            })
            .await
            .events();

        assert_eq!(events.len(), 1);
        assert!(matches!(
            &events[0],
            VaultRegistryEvent::UsdcVaultDiscovered { vault_id, .. }
            if *vault_id == TEST_VAULT_ID
        ));
    }

    #[tokio::test]
    async fn discovering_new_vault_id_for_same_token_adds_it() {
        let second_vault_id =
            b256!("0x0000000000000000000000000000000000000000000000000000000000000099");
        let new_tx_hash =
            b256!("0x2222222222222222222222222222222222222222222222222222222222222222");

        let events = TestHarness::<VaultRegistry>::with(())
            .given(vec![VaultRegistryEvent::EquityVaultDiscovered {
                token: TEST_TOKEN,
                vault_id: TEST_VAULT_ID,
                discovered_in: TEST_TX_HASH,
                discovered_at: Utc::now(),
                symbol: test_symbol(),
            }])
            .when(VaultRegistryCommand::DiscoverEquityVault {
                token: TEST_TOKEN,
                vault_id: second_vault_id,
                discovered_in: new_tx_hash,
                symbol: test_symbol(),
            })
            .await
            .events();

        assert_eq!(events.len(), 1, "Should emit event for new vault ID");
        assert!(matches!(
            &events[0],
            VaultRegistryEvent::EquityVaultDiscovered { vault_id, .. }
            if *vault_id == second_vault_id
        ));
    }

    #[test]
    fn multiple_vaults_per_token_are_tracked() {
        let vault_id_2 =
            b256!("0x0000000000000000000000000000000000000000000000000000000000000099");

        let registry = replay::<VaultRegistry>(vec![
            VaultRegistryEvent::EquityVaultDiscovered {
                token: TEST_TOKEN,
                vault_id: TEST_VAULT_ID,
                discovered_in: TEST_TX_HASH,
                discovered_at: Utc::now(),
                symbol: test_symbol(),
            },
            VaultRegistryEvent::EquityVaultDiscovered {
                token: TEST_TOKEN,
                vault_id: vault_id_2,
                discovered_in: TEST_TX_HASH,
                discovered_at: Utc::now(),
                symbol: test_symbol(),
            },
        ])
        .unwrap()
        .unwrap();

        // One token entry with two vaults
        assert_eq!(registry.equity_vaults.len(), 1);
        let vaults = &registry.equity_vaults[&TEST_TOKEN];
        assert_eq!(vaults.len(), 2);
        assert!(vaults.contains_key(&TEST_VAULT_ID));
        assert!(vaults.contains_key(&vault_id_2));

        let all_ids = registry.all_vault_ids_by_token(TEST_TOKEN);
        assert_eq!(all_ids.len(), 2);
    }

    #[test]
    fn primary_vault_preserves_config_order_not_btreemap_order() {
        // The second vault has a SMALLER B256 value than the first. Without
        // primary tracking, BTreeMap ordering would return the second one.
        let large_vault_id =
            b256!("0x00000000000000000000000000000000000000000000000000000000000000ff");
        let small_vault_id =
            b256!("0x0000000000000000000000000000000000000000000000000000000000000001");

        let registry = replay::<VaultRegistry>(vec![
            VaultRegistryEvent::EquityVaultSeededFromConfig {
                token: TEST_TOKEN,
                vault_id: large_vault_id,
                seeded_at: Utc::now(),
                symbol: test_symbol(),
            },
            VaultRegistryEvent::EquityVaultSeededFromConfig {
                token: TEST_TOKEN,
                vault_id: small_vault_id,
                seeded_at: Utc::now(),
                symbol: test_symbol(),
            },
        ])
        .unwrap()
        .unwrap();

        // Primary should be the FIRST seeded vault (large), not the
        // BTreeMap-ordered smallest (small).
        assert_eq!(
            registry.primary_vault_id_by_token(TEST_TOKEN),
            Some(large_vault_id),
            "Primary vault must preserve config order, not BTreeMap key order"
        );
    }

    #[test]
    fn configured_primary_equity_vault_can_replace_previous_primary() {
        let new_vault_id =
            b256!("0x0000000000000000000000000000000000000000000000000000000000000099");

        let registry = replay::<VaultRegistry>(vec![
            VaultRegistryEvent::EquityVaultSeededFromConfig {
                token: TEST_TOKEN,
                vault_id: TEST_VAULT_ID,
                seeded_at: Utc::now(),
                symbol: test_symbol(),
            },
            VaultRegistryEvent::PrimaryEquityVaultSetFromConfig {
                token: TEST_TOKEN,
                vault_id: TEST_VAULT_ID,
                configured_at: Utc::now(),
                symbol: test_symbol(),
            },
            VaultRegistryEvent::EquityVaultSeededFromConfig {
                token: TEST_TOKEN,
                vault_id: new_vault_id,
                seeded_at: Utc::now(),
                symbol: test_symbol(),
            },
            VaultRegistryEvent::PrimaryEquityVaultSetFromConfig {
                token: TEST_TOKEN,
                vault_id: new_vault_id,
                configured_at: Utc::now(),
                symbol: test_symbol(),
            },
        ])
        .unwrap()
        .unwrap();

        assert_eq!(
            registry.primary_vault_id_by_token(TEST_TOKEN),
            Some(new_vault_id),
            "current config must be able to replace the previous primary vault",
        );
        assert_eq!(
            registry.all_vault_ids_by_token(TEST_TOKEN),
            vec![TEST_VAULT_ID, new_vault_id],
            "retired vault remains registered for inventory polling",
        );
    }

    #[test]
    fn multiple_usdc_vaults_are_tracked() {
        let vault_id_2 =
            b256!("0x0000000000000000000000000000000000000000000000000000000000000099");

        let registry = replay::<VaultRegistry>(vec![
            VaultRegistryEvent::UsdcVaultDiscovered {
                vault_id: TEST_VAULT_ID,
                discovered_in: TEST_TX_HASH,
                discovered_at: Utc::now(),
            },
            VaultRegistryEvent::UsdcVaultDiscovered {
                vault_id: vault_id_2,
                discovered_in: TEST_TX_HASH,
                discovered_at: Utc::now(),
            },
        ])
        .unwrap()
        .unwrap();

        assert_eq!(registry.usdc_vaults.len(), 2);
        assert_eq!(registry.primary_usdc_vault_id(), Some(TEST_VAULT_ID));
    }

    #[test]
    fn replay_initializes_and_updates_state() {
        let registry = replay::<VaultRegistry>(vec![
            VaultRegistryEvent::EquityVaultDiscovered {
                token: TEST_TOKEN,
                vault_id: TEST_VAULT_ID,
                discovered_in: TEST_TX_HASH,
                discovered_at: Utc::now(),
                symbol: test_symbol(),
            },
            VaultRegistryEvent::UsdcVaultDiscovered {
                vault_id: TEST_VAULT_ID,
                discovered_in: TEST_TX_HASH,
                discovered_at: Utc::now(),
            },
        ])
        .unwrap()
        .unwrap();

        assert_eq!(registry.equity_vaults.len(), 1);
        assert!(registry.equity_vaults.contains_key(&TEST_TOKEN));
        assert!(!registry.usdc_vaults.is_empty());
    }

    #[test]
    fn replay_single_equity_discovery() {
        let registry = replay::<VaultRegistry>(vec![VaultRegistryEvent::EquityVaultDiscovered {
            token: TEST_TOKEN,
            vault_id: TEST_VAULT_ID,
            discovered_in: TEST_TX_HASH,
            discovered_at: Utc::now(),
            symbol: test_symbol(),
        }])
        .unwrap()
        .unwrap();

        assert_eq!(registry.equity_vaults.len(), 1);
        assert!(registry.equity_vaults.contains_key(&TEST_TOKEN));
        assert!(registry.usdc_vaults.is_empty());
    }

    #[test]
    fn multiple_equity_vaults_can_be_registered() {
        let token_2 = address!("0x2222222222222222222222222222222222222222");
        let vault_id_2 =
            b256!("0x0000000000000000000000000000000000000000000000000000000000000002");

        let registry = replay::<VaultRegistry>(vec![
            VaultRegistryEvent::EquityVaultDiscovered {
                token: TEST_TOKEN,
                vault_id: TEST_VAULT_ID,
                discovered_in: TEST_TX_HASH,
                discovered_at: Utc::now(),
                symbol: Symbol::new("AAPL").unwrap(),
            },
            VaultRegistryEvent::EquityVaultDiscovered {
                token: token_2,
                vault_id: vault_id_2,
                discovered_in: TEST_TX_HASH,
                discovered_at: Utc::now(),
                symbol: Symbol::new("MSFT").unwrap(),
            },
        ])
        .unwrap()
        .unwrap();

        assert_eq!(registry.equity_vaults.len(), 2);
    }

    #[test]
    fn token_by_symbol_returns_address_for_known_symbol() {
        let registry = replay::<VaultRegistry>(vec![VaultRegistryEvent::EquityVaultDiscovered {
            token: TEST_TOKEN,
            vault_id: TEST_VAULT_ID,
            discovered_in: TEST_TX_HASH,
            discovered_at: Utc::now(),
            symbol: test_symbol(),
        }])
        .unwrap()
        .unwrap();

        assert_eq!(registry.token_by_symbol(&test_symbol()), Some(TEST_TOKEN));
    }

    #[test]
    fn token_by_symbol_returns_none_for_unknown_symbol() {
        let registry = replay::<VaultRegistry>(vec![VaultRegistryEvent::EquityVaultDiscovered {
            token: TEST_TOKEN,
            vault_id: TEST_VAULT_ID,
            discovered_in: TEST_TX_HASH,
            discovered_at: Utc::now(),
            symbol: test_symbol(),
        }])
        .unwrap()
        .unwrap();

        assert_eq!(
            registry.token_by_symbol(&Symbol::new("MSFT").unwrap()),
            None
        );
    }

    #[test]
    fn token_by_symbol_distinguishes_multiple_equities() {
        let token_2 = address!("0x2222222222222222222222222222222222222222");
        let vault_id_2 =
            b256!("0x0000000000000000000000000000000000000000000000000000000000000002");
        let msft = Symbol::new("MSFT").unwrap();

        let registry = replay::<VaultRegistry>(vec![
            VaultRegistryEvent::EquityVaultDiscovered {
                token: TEST_TOKEN,
                vault_id: TEST_VAULT_ID,
                discovered_in: TEST_TX_HASH,
                discovered_at: Utc::now(),
                symbol: test_symbol(),
            },
            VaultRegistryEvent::EquityVaultDiscovered {
                token: token_2,
                vault_id: vault_id_2,
                discovered_in: TEST_TX_HASH,
                discovered_at: Utc::now(),
                symbol: msft.clone(),
            },
        ])
        .unwrap()
        .unwrap();

        assert_eq!(registry.token_by_symbol(&test_symbol()), Some(TEST_TOKEN));
        assert_eq!(registry.token_by_symbol(&msft), Some(token_2));
    }

    /// Tracks how many events a reactor receives.
    struct EventCounter(Arc<AtomicUsize>);

    deps!(EventCounter, [VaultRegistry]);

    #[async_trait]
    impl Reactor for EventCounter {
        type Error = st0x_event_sorcery::Never;

        async fn react(
            &self,
            event: <Self::Dependencies as EntityList>::Event,
        ) -> Result<(), Self::Error> {
            let (_id, _event) = event.into_inner();
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    #[tokio::test]
    async fn seed_equity_vault_deduplicates_on_same_token_and_vault_id() {
        let events = TestHarness::<VaultRegistry>::with(())
            .given(vec![VaultRegistryEvent::EquityVaultSeededFromConfig {
                token: TEST_TOKEN,
                vault_id: TEST_VAULT_ID,
                seeded_at: Utc::now(),
                symbol: test_symbol(),
            }])
            .when(VaultRegistryCommand::SeedEquityVaultFromConfig {
                token: TEST_TOKEN,
                vault_id: TEST_VAULT_ID,
                symbol: test_symbol(),
            })
            .await
            .events();

        assert_eq!(
            events.len(),
            0,
            "Should not emit event when vault already seeded with same ID"
        );
    }

    #[tokio::test]
    async fn seed_equity_vault_emits_when_vault_id_differs() {
        let new_vault_id =
            b256!("0x0000000000000000000000000000000000000000000000000000000000000099");

        let events = TestHarness::<VaultRegistry>::with(())
            .given(vec![VaultRegistryEvent::EquityVaultSeededFromConfig {
                token: TEST_TOKEN,
                vault_id: TEST_VAULT_ID,
                seeded_at: Utc::now(),
                symbol: test_symbol(),
            }])
            .when(VaultRegistryCommand::SeedEquityVaultFromConfig {
                token: TEST_TOKEN,
                vault_id: new_vault_id,
                symbol: test_symbol(),
            })
            .await
            .events();

        assert_eq!(
            events.len(),
            1,
            "Should emit event when vault ID changed in config"
        );
    }

    #[tokio::test]
    async fn seed_usdc_vault_deduplicates_on_same_vault_id() {
        let events = TestHarness::<VaultRegistry>::with(())
            .given(vec![VaultRegistryEvent::UsdcVaultSeededFromConfig {
                vault_id: TEST_VAULT_ID,
                seeded_at: Utc::now(),
            }])
            .when(VaultRegistryCommand::SeedUsdcVaultFromConfig {
                vault_id: TEST_VAULT_ID,
            })
            .await
            .events();

        assert_eq!(
            events.len(),
            0,
            "Should not emit event when USDC vault already seeded with same ID"
        );
    }

    #[tokio::test]
    async fn seed_usdc_vault_emits_when_vault_id_differs() {
        let new_vault_id =
            b256!("0x0000000000000000000000000000000000000000000000000000000000000099");

        let events = TestHarness::<VaultRegistry>::with(())
            .given(vec![VaultRegistryEvent::UsdcVaultSeededFromConfig {
                vault_id: TEST_VAULT_ID,
                seeded_at: Utc::now(),
            }])
            .when(VaultRegistryCommand::SeedUsdcVaultFromConfig {
                vault_id: new_vault_id,
            })
            .await
            .events();

        assert_eq!(
            events.len(),
            1,
            "Should emit event when USDC vault ID changed in config"
        );
    }

    #[tokio::test]
    async fn set_primary_equity_vault_deduplicates_when_config_unchanged() {
        let events = TestHarness::<VaultRegistry>::with(())
            .given(vec![
                VaultRegistryEvent::EquityVaultSeededFromConfig {
                    token: TEST_TOKEN,
                    vault_id: TEST_VAULT_ID,
                    seeded_at: Utc::now(),
                    symbol: test_symbol(),
                },
                VaultRegistryEvent::PrimaryEquityVaultSetFromConfig {
                    token: TEST_TOKEN,
                    vault_id: TEST_VAULT_ID,
                    configured_at: Utc::now(),
                    symbol: test_symbol(),
                },
            ])
            .when(VaultRegistryCommand::SetPrimaryEquityVaultFromConfig {
                token: TEST_TOKEN,
                vault_id: TEST_VAULT_ID,
                symbol: test_symbol(),
            })
            .await
            .events();

        assert_eq!(
            events.len(),
            0,
            "Should not emit event when configured primary equity vault is unchanged",
        );
    }

    #[tokio::test]
    async fn set_primary_equity_vault_emits_when_config_changes() {
        let new_vault_id =
            b256!("0x0000000000000000000000000000000000000000000000000000000000000099");

        let events = TestHarness::<VaultRegistry>::with(())
            .given(vec![
                VaultRegistryEvent::EquityVaultSeededFromConfig {
                    token: TEST_TOKEN,
                    vault_id: TEST_VAULT_ID,
                    seeded_at: Utc::now(),
                    symbol: test_symbol(),
                },
                VaultRegistryEvent::PrimaryEquityVaultSetFromConfig {
                    token: TEST_TOKEN,
                    vault_id: TEST_VAULT_ID,
                    configured_at: Utc::now(),
                    symbol: test_symbol(),
                },
                VaultRegistryEvent::EquityVaultSeededFromConfig {
                    token: TEST_TOKEN,
                    vault_id: new_vault_id,
                    seeded_at: Utc::now(),
                    symbol: test_symbol(),
                },
            ])
            .when(VaultRegistryCommand::SetPrimaryEquityVaultFromConfig {
                token: TEST_TOKEN,
                vault_id: new_vault_id,
                symbol: test_symbol(),
            })
            .await
            .events();

        assert_eq!(
            events.len(),
            1,
            "Should emit event when configured primary equity vault changes",
        );
        assert!(matches!(
            &events[0],
            VaultRegistryEvent::PrimaryEquityVaultSetFromConfig { vault_id, .. }
            if *vault_id == new_vault_id
        ));
    }

    #[tokio::test]
    async fn set_primary_usdc_vault_deduplicates_when_config_unchanged() {
        let events = TestHarness::<VaultRegistry>::with(())
            .given(vec![
                VaultRegistryEvent::UsdcVaultSeededFromConfig {
                    vault_id: TEST_VAULT_ID,
                    seeded_at: Utc::now(),
                },
                VaultRegistryEvent::PrimaryUsdcVaultSetFromConfig {
                    vault_id: TEST_VAULT_ID,
                    configured_at: Utc::now(),
                },
            ])
            .when(VaultRegistryCommand::SetPrimaryUsdcVaultFromConfig {
                vault_id: TEST_VAULT_ID,
            })
            .await
            .events();

        assert_eq!(
            events.len(),
            0,
            "Should not emit event when configured primary USDC vault is unchanged",
        );
    }

    #[tokio::test]
    async fn set_primary_usdc_vault_emits_when_config_changes() {
        let new_vault_id =
            b256!("0x0000000000000000000000000000000000000000000000000000000000000099");

        let events = TestHarness::<VaultRegistry>::with(())
            .given(vec![
                VaultRegistryEvent::UsdcVaultSeededFromConfig {
                    vault_id: TEST_VAULT_ID,
                    seeded_at: Utc::now(),
                },
                VaultRegistryEvent::PrimaryUsdcVaultSetFromConfig {
                    vault_id: TEST_VAULT_ID,
                    configured_at: Utc::now(),
                },
                VaultRegistryEvent::UsdcVaultSeededFromConfig {
                    vault_id: new_vault_id,
                    seeded_at: Utc::now(),
                },
            ])
            .when(VaultRegistryCommand::SetPrimaryUsdcVaultFromConfig {
                vault_id: new_vault_id,
            })
            .await
            .events();

        assert_eq!(
            events.len(),
            1,
            "Should emit event when configured primary USDC vault changes",
        );
        assert!(matches!(
            &events[0],
            VaultRegistryEvent::PrimaryUsdcVaultSetFromConfig { vault_id, .. }
            if *vault_id == new_vault_id
        ));
    }

    /// Proves that reactors only receive events from commands
    /// executed AFTER the framework is constructed -- existing events
    /// in the store are NOT replayed on construction.
    #[tokio::test]
    async fn reactors_only_see_new_events_not_historical() {
        let pool = setup_test_db().await;
        let id = VaultRegistryId {
            orderbook: TEST_ORDERBOOK,
            owner: TEST_OWNER,
        };

        // Phase 1: emit an event with NO reactors
        let (bare_store, _projection) = StoreBuilder::<VaultRegistry>::new(pool.clone())
            .build(())
            .await
            .unwrap();

        bare_store
            .send(
                &id,
                VaultRegistryCommand::DiscoverEquityVault {
                    token: TEST_TOKEN,
                    vault_id: TEST_VAULT_ID,
                    discovered_in: TEST_TX_HASH,
                    symbol: test_symbol(),
                },
            )
            .await
            .unwrap();

        // Phase 2: create a NEW framework with a counting reactor
        let counter = Arc::new(AtomicUsize::new(0));
        let reactor = EventCounter(counter.clone());
        let (observed_store, _projection) = StoreBuilder::<VaultRegistry>::new(pool.clone())
            .with(Arc::new(reactor))
            .build(())
            .await
            .unwrap();

        // Phase 3: emit one more event through the new framework
        let new_vault_id =
            b256!("0x0000000000000000000000000000000000000000000000000000000000000002");
        observed_store
            .send(
                &id,
                VaultRegistryCommand::DiscoverUsdcVault {
                    vault_id: new_vault_id,
                    discovered_in: TEST_TX_HASH,
                },
            )
            .await
            .unwrap();

        // The counter should be 1 (only the new event), not 2
        assert_eq!(
            counter.load(Ordering::SeqCst),
            1,
            "Reactor should only see events emitted after construction, not historical ones"
        );
    }

    fn test_usdc_vault_id() -> B256 {
        b256!("0x0000000000000000000000000000000000000000000000000000000000000002")
    }

    /// Builds a [`Ctx`] populated with the seeding fixtures defined at
    /// the top of this module so [`SeedVaultRegistryCtx::from_config`]
    /// can exercise the production construction path.
    fn ctx_with_seeded_assets() -> Ctx {
        use st0x_config::{
            AssetsConfig, CashAssetConfig, EquitiesConfig, EquityAssetConfig, OperationMode,
            create_test_ctx_with_order_owner,
        };
        use std::collections::HashMap;

        let mut symbols = HashMap::new();
        symbols.insert(
            test_symbol(),
            EquityAssetConfig {
                tokenized_equity: Address::ZERO,
                tokenized_equity_derivative: TEST_TOKEN,
                pyth_feed_id: None,
                vault_ids: vec![TEST_VAULT_ID],
                trading: OperationMode::Disabled,
                rebalancing: OperationMode::Enabled,
                wrapped_equity_recovery: OperationMode::Disabled,
                extended_hours_counter_trading: OperationMode::Disabled,
                operational_limit: None,
            },
        );

        Ctx {
            assets: AssetsConfig {
                equities: EquitiesConfig {
                    operational_limit: None,
                    symbols,
                },
                cash: Some(CashAssetConfig {
                    vault_ids: vec![test_usdc_vault_id()],
                    rebalancing: OperationMode::Disabled,
                    operational_limit: None,
                    reserved: None,
                }),
            },
            ..create_test_ctx_with_order_owner(TEST_OWNER)
        }
    }

    async fn seed_ctx_from(pool: sqlx::SqlitePool, ctx: &Ctx) -> Arc<SeedVaultRegistryCtx> {
        let (store, _projection) = StoreBuilder::<VaultRegistry>::new(pool)
            .build(())
            .await
            .unwrap();

        Arc::new(SeedVaultRegistryCtx::from_config(store, ctx).unwrap())
    }

    async fn loaded_registry(store: &Store<VaultRegistry>, id: &VaultRegistryId) -> VaultRegistry {
        store
            .load(id)
            .await
            .unwrap()
            .expect("registry should be initialized after seeding")
    }

    #[tokio::test]
    async fn from_config_rejects_missing_vault_id() {
        use st0x_config::{
            AssetsConfig, EquitiesConfig, EquityAssetConfig, OperationMode,
            create_test_ctx_with_order_owner,
        };
        use std::collections::HashMap;

        // Two equities with rebalancing enabled: one has a vault_id, one
        // does not. The smart constructor must reject before any state
        // is built, leaving callers with no way to skip the check.
        let mut symbols = HashMap::new();
        symbols.insert(
            Symbol::new("AAPL").unwrap(),
            EquityAssetConfig {
                tokenized_equity: Address::ZERO,
                tokenized_equity_derivative: Address::ZERO,
                pyth_feed_id: None,
                vault_ids: vec![TEST_VAULT_ID],
                trading: OperationMode::Disabled,
                rebalancing: OperationMode::Enabled,
                wrapped_equity_recovery: OperationMode::Disabled,
                extended_hours_counter_trading: OperationMode::Disabled,
                operational_limit: None,
            },
        );
        symbols.insert(
            Symbol::new("TSLA").unwrap(),
            EquityAssetConfig {
                tokenized_equity: Address::ZERO,
                tokenized_equity_derivative: Address::ZERO,
                pyth_feed_id: None,
                vault_ids: Vec::new(),
                trading: OperationMode::Disabled,
                rebalancing: OperationMode::Enabled,
                wrapped_equity_recovery: OperationMode::Disabled,
                extended_hours_counter_trading: OperationMode::Disabled,
                operational_limit: None,
            },
        );

        let ctx = Ctx {
            assets: AssetsConfig {
                equities: EquitiesConfig {
                    operational_limit: None,
                    symbols,
                },
                cash: None,
            },
            ..create_test_ctx_with_order_owner(Address::ZERO)
        };

        let pool = setup_test_db().await;
        let (store, _projection) = StoreBuilder::<VaultRegistry>::new(pool)
            .build(())
            .await
            .unwrap();

        let error = SeedVaultRegistryCtx::from_config(store, &ctx)
            .err()
            .expect("should fail when vault_id is missing for TSLA");

        assert!(
            matches!(&*error, CtxError::MissingEquityVaultId { symbol } if symbol.to_string() == "TSLA"),
            "expected MissingEquityVaultId for TSLA, got: {error:?}"
        );
    }

    #[tokio::test]
    async fn perform_seeds_configured_equity_and_usdc_vaults() {
        let pool = setup_test_db().await;
        let ctx = ctx_with_seeded_assets();
        let seed_ctx = seed_ctx_from(pool, &ctx).await;

        SeedVaultRegistry.perform(&seed_ctx).await.unwrap();

        let registry = loaded_registry(&seed_ctx.vault_registry, &seed_ctx.id).await;

        assert_eq!(
            registry.primary_vault_id_by_token(TEST_TOKEN),
            Some(TEST_VAULT_ID),
            "equity vault should be seeded as primary for the token",
        );
        assert_eq!(
            registry.primary_usdc_vault_id(),
            Some(test_usdc_vault_id()),
            "usdc vault should be seeded as primary",
        );
    }

    #[tokio::test]
    async fn perform_reasserts_configured_equity_primary_after_vault_id_change() {
        let pool = setup_test_db().await;
        let initial_ctx = ctx_with_seeded_assets();
        let initial_seed_ctx = seed_ctx_from(pool.clone(), &initial_ctx).await;
        SeedVaultRegistry.perform(&initial_seed_ctx).await.unwrap();

        let new_vault_id =
            b256!("0x0000000000000000000000000000000000000000000000000000000000000099");
        let mut updated_ctx = ctx_with_seeded_assets();
        updated_ctx
            .assets
            .equities
            .symbols
            .get_mut(&test_symbol())
            .unwrap()
            .vault_ids = vec![new_vault_id];

        let updated_seed_ctx = seed_ctx_from(pool, &updated_ctx).await;
        SeedVaultRegistry.perform(&updated_seed_ctx).await.unwrap();

        let registry =
            loaded_registry(&updated_seed_ctx.vault_registry, &updated_seed_ctx.id).await;

        assert_eq!(
            registry.primary_vault_id_by_token(TEST_TOKEN),
            Some(new_vault_id),
            "configured equity vault change must update the primary used by rebalancing",
        );
        assert_eq!(
            registry.all_vault_ids_by_token(TEST_TOKEN),
            vec![TEST_VAULT_ID, new_vault_id],
            "old equity vault remains registered so inventory polling can surface stranded funds",
        );
    }

    #[tokio::test]
    async fn perform_reasserts_configured_usdc_primary_after_vault_id_change() {
        let pool = setup_test_db().await;
        let initial_ctx = ctx_with_seeded_assets();
        let initial_seed_ctx = seed_ctx_from(pool.clone(), &initial_ctx).await;
        SeedVaultRegistry.perform(&initial_seed_ctx).await.unwrap();

        let new_vault_id =
            b256!("0x0000000000000000000000000000000000000000000000000000000000000099");
        let mut updated_ctx = ctx_with_seeded_assets();
        updated_ctx.assets.cash.as_mut().unwrap().vault_ids = vec![new_vault_id];

        let updated_seed_ctx = seed_ctx_from(pool, &updated_ctx).await;
        SeedVaultRegistry.perform(&updated_seed_ctx).await.unwrap();

        let registry =
            loaded_registry(&updated_seed_ctx.vault_registry, &updated_seed_ctx.id).await;

        assert_eq!(
            registry.primary_usdc_vault_id(),
            Some(new_vault_id),
            "configured USDC vault change must update the primary",
        );
        assert!(
            registry.usdc_vaults.contains_key(&test_usdc_vault_id()),
            "old USDC vault remains registered so inventory polling can surface stranded funds",
        );
    }

    #[tokio::test]
    async fn perform_is_idempotent_across_reruns() {
        let pool = setup_test_db().await;
        let ctx = ctx_with_seeded_assets();
        let seed_ctx = seed_ctx_from(pool, &ctx).await;

        SeedVaultRegistry.perform(&seed_ctx).await.unwrap();
        // Re-running with the same seeds must be a no-op: the transition
        // function returns `vec![]` for already-seeded vaults, so no
        // duplicate events accrue.
        SeedVaultRegistry.perform(&seed_ctx).await.unwrap();

        let registry = loaded_registry(&seed_ctx.vault_registry, &seed_ctx.id).await;
        assert_eq!(
            registry.all_vault_ids_by_token(TEST_TOKEN),
            vec![TEST_VAULT_ID]
        );
    }

    async fn job_attempts_for_seed(apalis_pool: &apalis_sqlite::SqlitePool) -> i64 {
        use sqlx_apalis::Row;
        let queue_name = std::any::type_name::<SeedVaultRegistry>();
        sqlx_apalis::query("SELECT attempts FROM Jobs WHERE job_type = ?")
            .bind(queue_name)
            .fetch_one(apalis_pool)
            .await
            .unwrap()
            .get::<i64, _>("attempts")
    }

    // Proves the gap is closed: enqueuing a SeedVaultRegistry job and
    // forcing the job to fail (via the FailureInjector armed for this
    // job type, which short-circuits before reaching the aggregate
    // command) results in apalis retrying the job before halting. The
    // `attempts` column on the Jobs row shows >1 when retries actually
    // happened.
    #[tokio::test]
    async fn enqueued_job_retries_on_aggregate_command_failure() {
        use apalis::layers::WorkerBuilderExt;
        use apalis::layers::retry::RetryPolicy;
        use apalis::prelude::{Monitor, WorkerBuilder};
        use apalis_core::worker::event::Event;
        use apalis_core::worker::ext::circuit_breaker::{
            CircuitBreaker, config::CircuitBreakerConfig,
        };
        use apalis_core::worker::ext::event_listener::EventListenerExt;
        use std::time::Duration;

        use crate::conductor::job::{FAIL_STOP_RECOVERY_TIMEOUT, FailureInjector, JobQueue, work};

        let (pool, apalis_pool) = setup_test_pools().await;

        let ctx = ctx_with_seeded_assets();
        let seed_ctx = seed_ctx_from(pool.clone(), &ctx).await;

        let mut queue: JobQueue<SeedVaultRegistry> = JobQueue::new(&apalis_pool);
        queue.push(SeedVaultRegistry).await.unwrap();

        let injector = FailureInjector::new();
        injector.arm(crate::conductor::job::JobKind::SeedVaultRegistry);

        let queue_for_worker = queue.clone();
        let ctx_for_worker = seed_ctx.clone();
        let injector_for_worker = injector.clone();

        let monitor_handle = tokio::spawn(async move {
            let monitor = Monitor::new()
                .should_restart(|_ctx, _error, _attempt| false)
                .register(move |index| {
                    let fail_stop = CircuitBreakerConfig::default()
                        .with_failure_threshold(1)
                        .with_recovery_timeout(FAIL_STOP_RECOVERY_TIMEOUT);

                    WorkerBuilder::new(format!("seed-vault-registry-test-{index}"))
                        .backend(queue_for_worker.clone().into_storage())
                        .data(ctx_for_worker.clone())
                        .data(injector_for_worker.clone())
                        .concurrency(1)
                        .retry(RetryPolicy::retries(3))
                        .break_circuit_with(fail_stop)
                        .on_event(|ctx, event| {
                            if let Event::Error(_) = event {
                                let _ = ctx.stop();
                            }
                        })
                        .build(work::<SeedVaultRegistryCtx, SeedVaultRegistry>)
                });

            monitor.run().await
        });

        tokio::time::timeout(Duration::from_secs(5), monitor_handle)
            .await
            .expect("Monitor should halt within 5s after retries exhaust")
            .expect("Monitor task should not panic")
            .ok();

        let attempts = job_attempts_for_seed(&apalis_pool).await;
        assert!(
            attempts > 1,
            "Job should have been retried at least once; attempts={attempts}",
        );

        // The retries never produced events because every attempt was
        // injected to fail before reaching the aggregate command.
        assert!(
            seed_ctx
                .vault_registry
                .load(&seed_ctx.id)
                .await
                .unwrap()
                .is_none(),
            "registry must remain empty when every retry attempt failed",
        );
    }
}

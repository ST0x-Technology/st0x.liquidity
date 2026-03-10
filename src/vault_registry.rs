//! VaultRegistry aggregate for tracking Raindex vaults.
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
use thiserror::Error;

use st0x_execution::Symbol;

use st0x_event_sorcery::{DomainEvent, EventSourced, Never, Projection, Table};

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
    const SCHEMA_VERSION: u64 = 2;

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
                if let Some(existing) = self.equity_vaults.get(token)
                    && existing.vault_id == *vault_id
                {
                    return Ok(vec![]);
                }
            }

            VaultRegistryCommand::SeedUsdcVaultFromConfig { vault_id } => {
                if let Some(ref existing) = self.usdc_vault
                    && existing.vault_id == *vault_id
                {
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
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct VaultRegistry {
    /// Equity vaults, keyed by token address
    pub(crate) equity_vaults: BTreeMap<Address, EquityVault>,
    /// USDC vault (at most one per owner)
    pub(crate) usdc_vault: Option<UsdcVault>,
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
            .values()
            .find(|vault| vault.symbol == *symbol)
            .map(|vault| vault.token)
    }

    pub(crate) fn vault_id_by_token(&self, token: Address) -> Option<B256> {
        self.equity_vaults.get(&token).map(|v| v.vault_id)
    }

    fn empty(timestamp: DateTime<Utc>) -> Self {
        Self {
            equity_vaults: BTreeMap::new(),
            usdc_vault: None,
            last_updated: timestamp,
        }
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
                self.equity_vaults.insert(
                    *token,
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
                self.usdc_vault = Some(UsdcVault {
                    vault_id: *vault_id,
                    provenance: VaultProvenance::Discovered {
                        tx_hash: *discovered_in,
                        discovered_at: *discovered_at,
                    },
                });
            }

            VaultRegistryEvent::EquityVaultSeededFromConfig {
                token,
                vault_id,
                seeded_at,
                symbol,
            } => {
                self.equity_vaults.insert(
                    *token,
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
                self.usdc_vault = Some(UsdcVault {
                    vault_id: *vault_id,
                    provenance: VaultProvenance::Seeded {
                        seeded_at: *seeded_at,
                    },
                });
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
}

impl VaultRegistryEvent {
    fn timestamp(&self) -> DateTime<Utc> {
        match self {
            Self::EquityVaultDiscovered { discovered_at, .. }
            | Self::UsdcVaultDiscovered { discovered_at, .. } => *discovered_at,
            Self::EquityVaultSeededFromConfig { seeded_at, .. }
            | Self::UsdcVaultSeededFromConfig { seeded_at, .. } => *seeded_at,
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
        }
    }

    fn event_version(&self) -> String {
        "1.0".to_string()
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
    use crate::test_utils::setup_test_db;

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
    async fn rediscovering_vault_updates_it() {
        let new_vault_id =
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
                vault_id: new_vault_id,
                discovered_in: new_tx_hash,
                symbol: test_symbol(),
            })
            .await
            .events();

        assert_eq!(events.len(), 1, "Should emit event to update vault");
        assert!(matches!(
            &events[0],
            VaultRegistryEvent::EquityVaultDiscovered { vault_id, .. }
            if *vault_id == new_vault_id
        ));
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
        assert!(registry.usdc_vault.is_some());
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
        assert!(registry.usdc_vault.is_none());
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
}

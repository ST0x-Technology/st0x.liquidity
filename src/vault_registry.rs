//! VaultRegistry aggregate for tracking discovered Raindex vaults.
//!
//! Vaults are auto-discovered from onchain trade events (ClearV3/TakeOrderV3).
//! The registry distinguishes between:
//! - **Equity Vaults**: Hold tokenized equities (token != USDC)
//! - **USDC Vaults**: Hold USDC for trading

use std::collections::BTreeMap;

use alloy::primitives::{Address, B256, TxHash};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use cqrs_es::{Aggregate, DomainEvent};
use serde::{Deserialize, Serialize};
use st0x_execution::Symbol;

use crate::lifecycle::{Lifecycle, LifecycleError, Never};

pub(crate) type VaultRegistryAggregate = Lifecycle<VaultRegistry, Never>;

/// Equity vault holding tokenized shares (base asset for a trading pair).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct DiscoveredEquityVault {
    pub(crate) token: Address,
    pub(crate) vault_id: B256,
    pub(crate) discovered_in: TxHash,
    pub(crate) discovered_at: DateTime<Utc>,
    pub(crate) symbol: Symbol,
}

/// USDC vault holding the quote asset for all trading pairs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct DiscoveredUsdcVault {
    pub(crate) vault_id: B256,
    pub(crate) discovered_in: TxHash,
    pub(crate) discovered_at: DateTime<Utc>,
}

/// Registry state tracking all discovered vaults for an orderbook/owner pair.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct VaultRegistry {
    /// Discovered equity vaults, keyed by token address
    pub(crate) equity_vaults: BTreeMap<Address, DiscoveredEquityVault>,
    /// Discovered USDC vault (at most one per owner)
    pub(crate) usdc_vault: Option<DiscoveredUsdcVault>,
    pub(crate) last_updated: DateTime<Utc>,
}

impl VaultRegistry {
    /// Creates the aggregate ID from orderbook and owner addresses.
    pub(crate) fn aggregate_id(orderbook: Address, owner: Address) -> String {
        format!("{orderbook}:{owner}")
    }

    pub(crate) fn token_by_symbol(&self, symbol: &Symbol) -> Option<Address> {
        self.equity_vaults
            .values()
            .find(|v| v.symbol == *symbol)
            .map(|v| v.token)
    }

    fn empty(timestamp: DateTime<Utc>) -> Self {
        Self {
            equity_vaults: BTreeMap::new(),
            usdc_vault: None,
            last_updated: timestamp,
        }
    }

    pub(crate) fn from_event(event: &VaultRegistryEvent) -> Self {
        let mut registry = Self::empty(event.timestamp());
        registry.apply_event(event);
        registry
    }

    pub(crate) fn apply_transition(event: &VaultRegistryEvent, registry: &Self) -> Self {
        let mut new_registry = registry.clone();
        new_registry.apply_event(event);
        new_registry
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
                    DiscoveredEquityVault {
                        token: *token,
                        vault_id: *vault_id,
                        discovered_in: *discovered_in,
                        discovered_at: *discovered_at,
                        symbol: symbol.clone(),
                    },
                );
            }

            VaultRegistryEvent::UsdcVaultDiscovered {
                vault_id,
                discovered_in,
                discovered_at,
            } => {
                self.usdc_vault = Some(DiscoveredUsdcVault {
                    vault_id: *vault_id,
                    discovered_in: *discovered_in,
                    discovered_at: *discovered_at,
                });
            }
        }
    }
}

#[async_trait]
impl Aggregate for Lifecycle<VaultRegistry, Never> {
    type Command = VaultRegistryCommand;
    type Event = VaultRegistryEvent;
    type Error = VaultRegistryError;
    type Services = ();

    fn aggregate_type() -> String {
        "VaultRegistry".to_string()
    }

    fn apply(&mut self, event: Self::Event) {
        *self = self
            .clone()
            .transition(&event, |event, state| {
                Ok(VaultRegistry::apply_transition(event, state))
            })
            .or_initialize(&event, |event| Ok(VaultRegistry::from_event(event)));
    }

    async fn handle(
        &self,
        command: Self::Command,
        _services: &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        let now = Utc::now();

        let event = match command {
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
        };

        Ok(vec![event])
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum VaultRegistryError {
    #[error(transparent)]
    State(#[from] LifecycleError<Never>),
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
}

impl VaultRegistryEvent {
    fn timestamp(&self) -> DateTime<Utc> {
        match self {
            Self::EquityVaultDiscovered { discovered_at, .. }
            | Self::UsdcVaultDiscovered { discovered_at, .. } => *discovered_at,
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
        }
    }

    fn event_version(&self) -> String {
        "1.0".to_string()
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{address, b256};
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use async_trait::async_trait;
    use cqrs_es::{EventEnvelope, Query};
    use sqlite_es::sqlite_cqrs;

    use super::*;
    use crate::test_utils::setup_test_db;

    const TEST_ORDERBOOK: Address = address!("0x1234567890123456789012345678901234567890");
    const TEST_OWNER: Address = address!("0xabcdefabcdefabcdefabcdefabcdefabcdefabcd");
    const TEST_TOKEN: Address = address!("0x9876543210987654321098765432109876543210");
    const TEST_VAULT_ID: B256 =
        b256!("0x0000000000000000000000000000000000000000000000000000000000000001");
    const TEST_TX_HASH: TxHash =
        b256!("0x1111111111111111111111111111111111111111111111111111111111111111");

    fn make_envelope(
        aggregate_id: &str,
        sequence: usize,
        event: VaultRegistryEvent,
    ) -> EventEnvelope<VaultRegistryAggregate> {
        EventEnvelope {
            aggregate_id: aggregate_id.to_string(),
            sequence,
            payload: event,
            metadata: HashMap::new(),
        }
    }

    fn test_symbol() -> Symbol {
        Symbol::new("AAPL").unwrap()
    }

    #[test]
    fn aggregate_id_format() {
        let id = VaultRegistry::aggregate_id(TEST_ORDERBOOK, TEST_OWNER);
        assert!(id.contains(&TEST_ORDERBOOK.to_string()));
        assert!(id.contains(&TEST_OWNER.to_string()));
    }

    #[tokio::test]
    async fn first_equity_discovery_initializes_registry() {
        let aggregate = VaultRegistryAggregate::default();

        let command = VaultRegistryCommand::DiscoverEquityVault {
            token: TEST_TOKEN,
            vault_id: TEST_VAULT_ID,
            discovered_in: TEST_TX_HASH,
            symbol: test_symbol(),
        };

        let events = aggregate.handle(command, &()).await.unwrap();

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
        let aggregate = VaultRegistryAggregate::default();

        let command = VaultRegistryCommand::DiscoverUsdcVault {
            vault_id: TEST_VAULT_ID,
            discovered_in: TEST_TX_HASH,
        };

        let events = aggregate.handle(command, &()).await.unwrap();

        assert_eq!(events.len(), 1);
        assert!(matches!(
            &events[0],
            VaultRegistryEvent::UsdcVaultDiscovered { vault_id, .. }
            if *vault_id == TEST_VAULT_ID
        ));
    }

    #[tokio::test]
    async fn discover_equity_vault_on_existing_registry() {
        let mut aggregate = VaultRegistryAggregate::default();
        aggregate.apply(VaultRegistryEvent::UsdcVaultDiscovered {
            vault_id: TEST_VAULT_ID,
            discovered_in: TEST_TX_HASH,
            discovered_at: Utc::now(),
        });

        let command = VaultRegistryCommand::DiscoverEquityVault {
            token: TEST_TOKEN,
            vault_id: TEST_VAULT_ID,
            discovered_in: TEST_TX_HASH,
            symbol: test_symbol(),
        };

        let events = aggregate.handle(command, &()).await.unwrap();

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
        let mut aggregate = VaultRegistryAggregate::default();
        aggregate.apply(VaultRegistryEvent::EquityVaultDiscovered {
            token: TEST_TOKEN,
            vault_id: TEST_VAULT_ID,
            discovered_in: TEST_TX_HASH,
            discovered_at: Utc::now(),
            symbol: test_symbol(),
        });

        let command = VaultRegistryCommand::DiscoverUsdcVault {
            vault_id: TEST_VAULT_ID,
            discovered_in: TEST_TX_HASH,
        };

        let events = aggregate.handle(command, &()).await.unwrap();

        assert_eq!(events.len(), 1);
        assert!(matches!(
            &events[0],
            VaultRegistryEvent::UsdcVaultDiscovered { vault_id, .. }
            if *vault_id == TEST_VAULT_ID
        ));
    }

    #[tokio::test]
    async fn rediscovering_vault_updates_it() {
        let mut aggregate = VaultRegistryAggregate::default();
        aggregate.apply(VaultRegistryEvent::EquityVaultDiscovered {
            token: TEST_TOKEN,
            vault_id: TEST_VAULT_ID,
            discovered_in: TEST_TX_HASH,
            discovered_at: Utc::now(),
            symbol: test_symbol(),
        });

        let new_vault_id =
            b256!("0x0000000000000000000000000000000000000000000000000000000000000099");
        let new_tx_hash =
            b256!("0x2222222222222222222222222222222222222222222222222222222222222222");

        let command = VaultRegistryCommand::DiscoverEquityVault {
            token: TEST_TOKEN,
            vault_id: new_vault_id,
            discovered_in: new_tx_hash,
            symbol: test_symbol(),
        };

        let events = aggregate.handle(command, &()).await.unwrap();

        assert_eq!(events.len(), 1, "Should emit event to update vault");
        assert!(matches!(
            &events[0],
            VaultRegistryEvent::EquityVaultDiscovered { vault_id, .. }
            if *vault_id == new_vault_id
        ));
    }

    #[test]
    fn apply_initializes_and_updates_state() {
        let mut aggregate = VaultRegistryAggregate::default();

        aggregate.apply(VaultRegistryEvent::EquityVaultDiscovered {
            token: TEST_TOKEN,
            vault_id: TEST_VAULT_ID,
            discovered_in: TEST_TX_HASH,
            discovered_at: Utc::now(),
            symbol: test_symbol(),
        });

        let Lifecycle::Live(registry) = &aggregate else {
            panic!("Expected Live state after first event");
        };
        assert_eq!(registry.equity_vaults.len(), 1);
        assert!(registry.equity_vaults.contains_key(&TEST_TOKEN));
        assert!(registry.usdc_vault.is_none());

        aggregate.apply(VaultRegistryEvent::UsdcVaultDiscovered {
            vault_id: TEST_VAULT_ID,
            discovered_in: TEST_TX_HASH,
            discovered_at: Utc::now(),
        });

        let Lifecycle::Live(registry) = &aggregate else {
            panic!("Expected Live state after second event");
        };
        assert_eq!(registry.equity_vaults.len(), 1);
        assert!(registry.usdc_vault.is_some());
    }

    #[test]
    fn view_update_applies_events() {
        let aggregate_id = VaultRegistry::aggregate_id(TEST_ORDERBOOK, TEST_OWNER);
        let mut view = VaultRegistryAggregate::default();

        let discover_envelope = make_envelope(
            &aggregate_id,
            1,
            VaultRegistryEvent::EquityVaultDiscovered {
                token: TEST_TOKEN,
                vault_id: TEST_VAULT_ID,
                discovered_in: TEST_TX_HASH,
                discovered_at: Utc::now(),
                symbol: test_symbol(),
            },
        );
        view.update(&discover_envelope);

        let Lifecycle::Live(registry) = &view else {
            panic!("Expected Live state after first discovery");
        };
        assert_eq!(registry.equity_vaults.len(), 1);
        assert!(registry.equity_vaults.contains_key(&TEST_TOKEN));
        assert!(registry.usdc_vault.is_none());
    }

    #[test]
    fn multiple_equity_vaults_can_be_registered() {
        let aggregate_id = VaultRegistry::aggregate_id(TEST_ORDERBOOK, TEST_OWNER);
        let mut view = VaultRegistryAggregate::default();

        let token_2 = address!("0x2222222222222222222222222222222222222222");
        let vault_id_2 =
            b256!("0x0000000000000000000000000000000000000000000000000000000000000002");

        let discover_1 = make_envelope(
            &aggregate_id,
            1,
            VaultRegistryEvent::EquityVaultDiscovered {
                token: TEST_TOKEN,
                vault_id: TEST_VAULT_ID,
                discovered_in: TEST_TX_HASH,
                discovered_at: Utc::now(),
                symbol: Symbol::new("AAPL").unwrap(),
            },
        );
        view.update(&discover_1);

        let discover_2 = make_envelope(
            &aggregate_id,
            2,
            VaultRegistryEvent::EquityVaultDiscovered {
                token: token_2,
                vault_id: vault_id_2,
                discovered_in: TEST_TX_HASH,
                discovered_at: Utc::now(),
                symbol: Symbol::new("MSFT").unwrap(),
            },
        );
        view.update(&discover_2);

        let Lifecycle::Live(registry) = &view else {
            panic!("Expected Live state");
        };
        assert_eq!(registry.equity_vaults.len(), 2);
    }

    #[test]
    fn token_by_symbol_returns_address_for_known_symbol() {
        let mut aggregate = VaultRegistryAggregate::default();
        aggregate.apply(VaultRegistryEvent::EquityVaultDiscovered {
            token: TEST_TOKEN,
            vault_id: TEST_VAULT_ID,
            discovered_in: TEST_TX_HASH,
            discovered_at: Utc::now(),
            symbol: test_symbol(),
        });

        let Lifecycle::Live(registry) = &aggregate else {
            panic!("Expected Live state");
        };

        assert_eq!(registry.token_by_symbol(&test_symbol()), Some(TEST_TOKEN));
    }

    #[test]
    fn token_by_symbol_returns_none_for_unknown_symbol() {
        let mut aggregate = VaultRegistryAggregate::default();
        aggregate.apply(VaultRegistryEvent::EquityVaultDiscovered {
            token: TEST_TOKEN,
            vault_id: TEST_VAULT_ID,
            discovered_in: TEST_TX_HASH,
            discovered_at: Utc::now(),
            symbol: test_symbol(),
        });

        let Lifecycle::Live(registry) = &aggregate else {
            panic!("Expected Live state");
        };

        assert_eq!(
            registry.token_by_symbol(&Symbol::new("MSFT").unwrap()),
            None
        );
    }

    #[test]
    fn token_by_symbol_distinguishes_multiple_equities() {
        let mut aggregate = VaultRegistryAggregate::default();
        let token_2 = address!("0x2222222222222222222222222222222222222222");
        let vault_id_2 =
            b256!("0x0000000000000000000000000000000000000000000000000000000000000002");
        let msft = Symbol::new("MSFT").unwrap();

        aggregate.apply(VaultRegistryEvent::EquityVaultDiscovered {
            token: TEST_TOKEN,
            vault_id: TEST_VAULT_ID,
            discovered_in: TEST_TX_HASH,
            discovered_at: Utc::now(),
            symbol: test_symbol(),
        });
        aggregate.apply(VaultRegistryEvent::EquityVaultDiscovered {
            token: token_2,
            vault_id: vault_id_2,
            discovered_in: TEST_TX_HASH,
            discovered_at: Utc::now(),
            symbol: msft.clone(),
        });

        let Lifecycle::Live(registry) = &aggregate else {
            panic!("Expected Live state");
        };

        assert_eq!(registry.token_by_symbol(&test_symbol()), Some(TEST_TOKEN));
        assert_eq!(registry.token_by_symbol(&msft), Some(token_2));
    }

    /// Tracks how many events a query processor receives via dispatch.
    struct EventCounter(Arc<AtomicUsize>);

    #[async_trait]
    impl Query<VaultRegistryAggregate> for EventCounter {
        async fn dispatch(&self, _id: &str, _events: &[EventEnvelope<VaultRegistryAggregate>]) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    /// Proves that query processors only receive events from commands
    /// executed AFTER the framework is constructed -- existing events
    /// in the store are NOT replayed on construction.
    #[tokio::test]
    async fn query_processors_only_see_new_events_not_historical() {
        let pool = setup_test_db().await;
        let aggregate_id = VaultRegistry::aggregate_id(TEST_ORDERBOOK, TEST_OWNER);

        // Phase 1: emit an event with NO query processors
        let bare_cqrs = sqlite_cqrs::<VaultRegistryAggregate>(pool.clone(), vec![], ());

        bare_cqrs
            .execute(
                &aggregate_id,
                VaultRegistryCommand::DiscoverEquityVault {
                    token: TEST_TOKEN,
                    vault_id: TEST_VAULT_ID,
                    discovered_in: TEST_TX_HASH,
                    symbol: test_symbol(),
                },
            )
            .await
            .unwrap();

        // Phase 2: create a NEW framework with a counting query processor
        let counter = Arc::new(AtomicUsize::new(0));
        let query = EventCounter(counter.clone());
        let observed_cqrs =
            sqlite_cqrs::<VaultRegistryAggregate>(pool.clone(), vec![Box::new(query)], ());

        // Phase 3: emit one more event through the new framework
        let new_vault_id =
            b256!("0x0000000000000000000000000000000000000000000000000000000000000002");
        observed_cqrs
            .execute(
                &aggregate_id,
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
            "Query processor should only see events emitted after construction, not historical ones"
        );
    }
}

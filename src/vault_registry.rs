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
use cqrs_es::{Aggregate, DomainEvent, EventEnvelope, View};
use serde::{Deserialize, Serialize};
use st0x_execution::Symbol;

use crate::cctp::USDC_BASE;
use crate::lifecycle::{Lifecycle, LifecycleError, Never};

/// Information about a discovered vault.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct DiscoveredVault {
    pub(crate) owner: Address,
    pub(crate) token: Address,
    pub(crate) vault_id: B256,
    pub(crate) discovered_from_tx: TxHash,
    pub(crate) discovered_at: DateTime<Utc>,
}

/// Type of vault based on the token it holds.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum VaultType {
    /// Vault holding tokenized shares (token != USDC)
    Equity { symbol: Symbol },
    /// Vault holding USDC
    Usdc,
}

/// Registry state tracking all discovered vaults for an orderbook/owner pair.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct VaultRegistry {
    pub(crate) orderbook: Address,
    pub(crate) owner: Address,
    /// Discovered equity vaults, keyed by token address
    pub(crate) equity_vaults: BTreeMap<Address, DiscoveredVault>,
    /// Discovered USDC vault (at most one per owner)
    pub(crate) usdc_vault: Option<DiscoveredVault>,
    pub(crate) last_updated: DateTime<Utc>,
}

impl VaultRegistry {
    /// Creates the aggregate ID from orderbook and owner addresses.
    pub(crate) fn aggregate_id(orderbook: Address, owner: Address) -> String {
        format!("{orderbook}:{owner}")
    }

    pub(crate) fn apply_transition(
        event: &VaultRegistryEvent,
        registry: &Self,
    ) -> Result<Self, LifecycleError<Never>> {
        match event {
            VaultRegistryEvent::EquityVaultDiscovered {
                owner,
                token,
                vault_id,
                discovered_from_tx,
                discovered_at,
                symbol: _,
            } => {
                let new_vault = DiscoveredVault {
                    owner: *owner,
                    token: *token,
                    vault_id: *vault_id,
                    discovered_from_tx: *discovered_from_tx,
                    discovered_at: *discovered_at,
                };

                // Insert or update: BTreeMap::collect overwrites existing keys
                let equity_vaults = registry
                    .equity_vaults
                    .iter()
                    .map(|(k, v)| (*k, v.clone()))
                    .chain(std::iter::once((*token, new_vault)))
                    .collect();

                Ok(Self {
                    equity_vaults,
                    last_updated: *discovered_at,
                    ..registry.clone()
                })
            }

            VaultRegistryEvent::UsdcVaultDiscovered {
                owner,
                vault_id,
                discovered_from_tx,
                discovered_at,
            } => Ok(Self {
                usdc_vault: Some(DiscoveredVault {
                    owner: *owner,
                    token: USDC_BASE,
                    vault_id: *vault_id,
                    discovered_from_tx: *discovered_from_tx,
                    discovered_at: *discovered_at,
                }),
                last_updated: *discovered_at,
                ..registry.clone()
            }),

            VaultRegistryEvent::Initialized { .. } => Err(LifecycleError::Mismatch {
                state: format!("{registry:?}"),
                event: event.event_type(),
            }),
        }
    }

    pub(crate) fn from_event(event: &VaultRegistryEvent) -> Result<Self, LifecycleError<Never>> {
        match event {
            VaultRegistryEvent::Initialized {
                orderbook,
                owner,
                initialized_at,
            } => Ok(Self {
                orderbook: *orderbook,
                owner: *owner,
                equity_vaults: BTreeMap::new(),
                usdc_vault: None,
                last_updated: *initialized_at,
            }),

            _ => Err(LifecycleError::Mismatch {
                state: "Uninitialized".into(),
                event: event.event_type(),
            }),
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
            .transition(&event, VaultRegistry::apply_transition)
            .or_initialize(&event, VaultRegistry::from_event);
    }

    async fn handle(
        &self,
        command: Self::Command,
        _services: &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        match (self.live(), &command) {
            (
                Err(LifecycleError::Uninitialized),
                VaultRegistryCommand::Initialize { orderbook, owner },
            ) => Ok(vec![VaultRegistryEvent::Initialized {
                orderbook: *orderbook,
                owner: *owner,
                initialized_at: Utc::now(),
            }]),

            (Ok(_), VaultRegistryCommand::Initialize { .. }) => {
                Err(LifecycleError::AlreadyInitialized.into())
            }

            (Err(e), _) => Err(e.into()),

            (
                Ok(_),
                VaultRegistryCommand::DiscoverVault {
                    owner,
                    token,
                    vault_id,
                    discovered_from_tx,
                    symbol,
                },
            ) => {
                if *token == USDC_BASE {
                    Ok(vec![VaultRegistryEvent::UsdcVaultDiscovered {
                        owner: *owner,
                        vault_id: *vault_id,
                        discovered_from_tx: *discovered_from_tx,
                        discovered_at: Utc::now(),
                    }])
                } else {
                    let symbol = symbol
                        .clone()
                        .ok_or(VaultRegistryError::MissingSymbol { token: *token })?;

                    Ok(vec![VaultRegistryEvent::EquityVaultDiscovered {
                        owner: *owner,
                        token: *token,
                        vault_id: *vault_id,
                        discovered_from_tx: *discovered_from_tx,
                        discovered_at: Utc::now(),
                        symbol,
                    }])
                }
            }
        }
    }
}

impl View<Self> for Lifecycle<VaultRegistry, Never> {
    fn update(&mut self, event: &EventEnvelope<Self>) {
        *self = self
            .clone()
            .transition(&event.payload, VaultRegistry::apply_transition)
            .or_initialize(&event.payload, VaultRegistry::from_event);
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum VaultRegistryError {
    #[error("Missing symbol for equity vault with token {token}")]
    MissingSymbol { token: Address },
    #[error(transparent)]
    State(#[from] LifecycleError<Never>),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum VaultRegistryCommand {
    Initialize {
        orderbook: Address,
        owner: Address,
    },
    DiscoverVault {
        owner: Address,
        token: Address,
        vault_id: B256,
        discovered_from_tx: TxHash,
        /// Symbol for equity vaults (None for USDC)
        symbol: Option<Symbol>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) enum VaultRegistryEvent {
    Initialized {
        orderbook: Address,
        owner: Address,
        initialized_at: DateTime<Utc>,
    },
    EquityVaultDiscovered {
        owner: Address,
        token: Address,
        vault_id: B256,
        discovered_from_tx: TxHash,
        discovered_at: DateTime<Utc>,
        symbol: Symbol,
    },
    UsdcVaultDiscovered {
        owner: Address,
        vault_id: B256,
        discovered_from_tx: TxHash,
        discovered_at: DateTime<Utc>,
    },
}

impl DomainEvent for VaultRegistryEvent {
    fn event_type(&self) -> String {
        match self {
            Self::Initialized { .. } => "VaultRegistryEvent::Initialized".to_string(),
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

    use super::*;

    const TEST_ORDERBOOK: Address = address!("0x1234567890123456789012345678901234567890");
    const TEST_OWNER: Address = address!("0xabcdefabcdefabcdefabcdefabcdefabcdefabcd");
    const TEST_TOKEN: Address = address!("0x9876543210987654321098765432109876543210");
    const TEST_VAULT_ID: B256 =
        b256!("0x0000000000000000000000000000000000000000000000000000000000000001");
    const TEST_TX_HASH: TxHash =
        b256!("0x1111111111111111111111111111111111111111111111111111111111111111");

    type VaultRegistryAggregate = Lifecycle<VaultRegistry, Never>;

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
    async fn initialize_creates_empty_registry() {
        let aggregate = VaultRegistryAggregate::default();
        let command = VaultRegistryCommand::Initialize {
            orderbook: TEST_ORDERBOOK,
            owner: TEST_OWNER,
        };

        let events = aggregate.handle(command, &()).await.unwrap();

        assert_eq!(events.len(), 1);
        assert!(matches!(
            &events[0],
            VaultRegistryEvent::Initialized { orderbook, owner, .. }
            if *orderbook == TEST_ORDERBOOK && *owner == TEST_OWNER
        ));
    }

    #[tokio::test]
    async fn discover_equity_vault_emits_event() {
        let mut aggregate = VaultRegistryAggregate::default();
        aggregate.apply(VaultRegistryEvent::Initialized {
            orderbook: TEST_ORDERBOOK,
            owner: TEST_OWNER,
            initialized_at: Utc::now(),
        });

        let command = VaultRegistryCommand::DiscoverVault {
            owner: TEST_OWNER,
            token: TEST_TOKEN,
            vault_id: TEST_VAULT_ID,
            discovered_from_tx: TEST_TX_HASH,
            symbol: Some(test_symbol()),
        };

        let events = aggregate.handle(command, &()).await.unwrap();

        assert_eq!(events.len(), 1);
        assert!(matches!(
            &events[0],
            VaultRegistryEvent::EquityVaultDiscovered {
                owner,
                token,
                vault_id,
                symbol,
                ..
            } if *owner == TEST_OWNER
                && *token == TEST_TOKEN
                && *vault_id == TEST_VAULT_ID
                && *symbol == test_symbol()
        ));
    }

    #[tokio::test]
    async fn discover_usdc_vault_emits_event() {
        let mut aggregate = VaultRegistryAggregate::default();
        aggregate.apply(VaultRegistryEvent::Initialized {
            orderbook: TEST_ORDERBOOK,
            owner: TEST_OWNER,
            initialized_at: Utc::now(),
        });

        let command = VaultRegistryCommand::DiscoverVault {
            owner: TEST_OWNER,
            token: USDC_BASE,
            vault_id: TEST_VAULT_ID,
            discovered_from_tx: TEST_TX_HASH,
            symbol: None,
        };

        let events = aggregate.handle(command, &()).await.unwrap();

        assert_eq!(events.len(), 1);
        assert!(matches!(
            &events[0],
            VaultRegistryEvent::UsdcVaultDiscovered {
                owner,
                vault_id,
                ..
            } if *owner == TEST_OWNER && *vault_id == TEST_VAULT_ID
        ));
    }

    #[tokio::test]
    async fn rediscovering_vault_updates_it() {
        let mut aggregate = VaultRegistryAggregate::default();
        aggregate.apply(VaultRegistryEvent::Initialized {
            orderbook: TEST_ORDERBOOK,
            owner: TEST_OWNER,
            initialized_at: Utc::now(),
        });
        aggregate.apply(VaultRegistryEvent::EquityVaultDiscovered {
            owner: TEST_OWNER,
            token: TEST_TOKEN,
            vault_id: TEST_VAULT_ID,
            discovered_from_tx: TEST_TX_HASH,
            discovered_at: Utc::now(),
            symbol: test_symbol(),
        });

        let new_vault_id =
            b256!("0x0000000000000000000000000000000000000000000000000000000000000099");
        let new_tx_hash =
            b256!("0x2222222222222222222222222222222222222222222222222222222222222222");

        let command = VaultRegistryCommand::DiscoverVault {
            owner: TEST_OWNER,
            token: TEST_TOKEN,
            vault_id: new_vault_id,
            discovered_from_tx: new_tx_hash,
            symbol: Some(test_symbol()),
        };

        let events = aggregate.handle(command, &()).await.unwrap();

        assert_eq!(events.len(), 1, "Should emit event to update vault");
        assert!(matches!(
            &events[0],
            VaultRegistryEvent::EquityVaultDiscovered { vault_id, .. }
            if *vault_id == new_vault_id
        ));
    }

    #[tokio::test]
    async fn discover_vault_without_symbol_for_equity_fails() {
        let mut aggregate = VaultRegistryAggregate::default();
        aggregate.apply(VaultRegistryEvent::Initialized {
            orderbook: TEST_ORDERBOOK,
            owner: TEST_OWNER,
            initialized_at: Utc::now(),
        });

        let command = VaultRegistryCommand::DiscoverVault {
            owner: TEST_OWNER,
            token: TEST_TOKEN,
            vault_id: TEST_VAULT_ID,
            discovered_from_tx: TEST_TX_HASH,
            symbol: None,
        };

        let error = aggregate.handle(command, &()).await.unwrap_err();

        assert!(
            matches!(error, VaultRegistryError::MissingSymbol { token } if token == TEST_TOKEN),
            "Expected MissingSymbol error, got: {error:?}"
        );
    }

    #[test]
    fn view_update_applies_events() {
        let aggregate_id = VaultRegistry::aggregate_id(TEST_ORDERBOOK, TEST_OWNER);

        let mut view = VaultRegistryAggregate::default();

        let init_envelope = make_envelope(
            &aggregate_id,
            1,
            VaultRegistryEvent::Initialized {
                orderbook: TEST_ORDERBOOK,
                owner: TEST_OWNER,
                initialized_at: Utc::now(),
            },
        );
        view.update(&init_envelope);

        let Lifecycle::Live(registry) = &view else {
            panic!("Expected Live state after initialization");
        };
        assert_eq!(registry.orderbook, TEST_ORDERBOOK);
        assert_eq!(registry.owner, TEST_OWNER);
        assert!(registry.equity_vaults.is_empty());
        assert!(registry.usdc_vault.is_none());

        let discover_envelope = make_envelope(
            &aggregate_id,
            2,
            VaultRegistryEvent::EquityVaultDiscovered {
                owner: TEST_OWNER,
                token: TEST_TOKEN,
                vault_id: TEST_VAULT_ID,
                discovered_from_tx: TEST_TX_HASH,
                discovered_at: Utc::now(),
                symbol: test_symbol(),
            },
        );
        view.update(&discover_envelope);

        let Lifecycle::Live(registry) = &view else {
            panic!("Expected Live state after discovery");
        };
        assert_eq!(registry.equity_vaults.len(), 1);
        assert!(registry.equity_vaults.contains_key(&TEST_TOKEN));
    }

    #[test]
    fn multiple_equity_vaults_can_be_registered() {
        let aggregate_id = VaultRegistry::aggregate_id(TEST_ORDERBOOK, TEST_OWNER);
        let mut view = VaultRegistryAggregate::default();

        let init_envelope = make_envelope(
            &aggregate_id,
            1,
            VaultRegistryEvent::Initialized {
                orderbook: TEST_ORDERBOOK,
                owner: TEST_OWNER,
                initialized_at: Utc::now(),
            },
        );
        view.update(&init_envelope);

        let token_2 = address!("0x2222222222222222222222222222222222222222");
        let vault_id_2 =
            b256!("0x0000000000000000000000000000000000000000000000000000000000000002");

        let discover_1 = make_envelope(
            &aggregate_id,
            2,
            VaultRegistryEvent::EquityVaultDiscovered {
                owner: TEST_OWNER,
                token: TEST_TOKEN,
                vault_id: TEST_VAULT_ID,
                discovered_from_tx: TEST_TX_HASH,
                discovered_at: Utc::now(),
                symbol: Symbol::new("AAPL").unwrap(),
            },
        );
        view.update(&discover_1);

        let discover_2 = make_envelope(
            &aggregate_id,
            3,
            VaultRegistryEvent::EquityVaultDiscovered {
                owner: TEST_OWNER,
                token: token_2,
                vault_id: vault_id_2,
                discovered_from_tx: TEST_TX_HASH,
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
}

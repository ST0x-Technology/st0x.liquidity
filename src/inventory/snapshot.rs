//! InventorySnapshot aggregate for recording fetched inventory.
//!
//! This aggregate records point-in-time snapshots of inventory fetched from
//! onchain vaults and offchain brokers. Events are consumed by InventoryView
//! to reconcile tracked inventory with actual balances.

use alloy::hex::FromHexError;
use alloy::primitives::Address;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::str::FromStr;
use thiserror::Error;

use st0x_event_sorcery::{DomainEvent, EventSourced, Never, Table};
use st0x_execution::{FractionalShares, Symbol};

use crate::threshold::Usdc;

/// Typed identifier for InventorySnapshot aggregates, keyed
/// by orderbook and owner address pair.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct InventorySnapshotId {
    pub(crate) orderbook: Address,
    pub(crate) owner: Address,
}

impl std::fmt::Display for InventorySnapshotId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.orderbook, self.owner)
    }
}

#[derive(Debug, Error)]
pub(crate) enum ParseInventorySnapshotIdError {
    #[error("expected 'orderbook:owner', got '{id_provided}'")]
    MissingDelimiter { id_provided: String },

    #[error("invalid orderbook address: {0}")]
    Orderbook(FromHexError),

    #[error("invalid owner address: {0}")]
    Owner(FromHexError),
}

impl FromStr for InventorySnapshotId {
    type Err = ParseInventorySnapshotIdError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let (orderbook_str, owner_str) = value.split_once(':').ok_or_else(|| {
            ParseInventorySnapshotIdError::MissingDelimiter {
                id_provided: value.to_string(),
            }
        })?;
        let orderbook = orderbook_str
            .parse()
            .map_err(ParseInventorySnapshotIdError::Orderbook)?;
        let owner = owner_str
            .parse()
            .map_err(ParseInventorySnapshotIdError::Owner)?;
        Ok(Self { orderbook, owner })
    }
}

/// State tracking the latest inventory snapshots.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct InventorySnapshot {
    /// Latest onchain equity balances by symbol
    pub(crate) onchain_equity: BTreeMap<Symbol, FractionalShares>,
    /// Latest onchain USDC balance
    pub(crate) onchain_cash: Option<Usdc>,
    /// Latest offchain equity positions by symbol
    pub(crate) offchain_equity: BTreeMap<Symbol, FractionalShares>,
    /// Latest offchain cash balance in cents
    pub(crate) offchain_cash_cents: Option<i64>,
    /// When this snapshot was last updated
    pub(crate) last_updated: DateTime<Utc>,
}

#[async_trait]
impl EventSourced for InventorySnapshot {
    type Id = InventorySnapshotId;
    type Event = InventorySnapshotEvent;
    type Command = InventorySnapshotCommand;
    type Error = Never;
    type Services = ();

    const AGGREGATE_TYPE: &'static str = "InventorySnapshot";
    const PROJECTION: Option<Table> = None;
    const SCHEMA_VERSION: u64 = 1;

    fn originate(event: &Self::Event) -> Option<Self> {
        let mut snapshot = Self {
            onchain_equity: BTreeMap::new(),
            onchain_cash: None,
            offchain_equity: BTreeMap::new(),
            offchain_cash_cents: None,
            last_updated: event.timestamp(),
        };
        snapshot.apply_event(event);
        Some(snapshot)
    }

    fn evolve(entity: &Self, event: &Self::Event) -> Result<Option<Self>, Self::Error> {
        let mut snapshot = entity.clone();
        snapshot.apply_event(event);
        Ok(Some(snapshot))
    }

    async fn initialize(
        command: Self::Command,
        _services: &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        use InventorySnapshotCommand::*;
        let now = Utc::now();
        Ok(vec![match command {
            OnchainEquity { balances } => InventorySnapshotEvent::OnchainEquity {
                balances,
                fetched_at: now,
            },
            OnchainCash { usdc_balance } => InventorySnapshotEvent::OnchainCash {
                usdc_balance,
                fetched_at: now,
            },
            OffchainEquity { positions } => InventorySnapshotEvent::OffchainEquity {
                positions,
                fetched_at: now,
            },
            OffchainCash { cash_balance_cents } => InventorySnapshotEvent::OffchainCash {
                cash_balance_cents,
                fetched_at: now,
            },
        }])
    }

    async fn transition(
        &self,
        command: Self::Command,
        _services: &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        use InventorySnapshotCommand::*;
        let now = Utc::now();
        Ok(vec![match command {
            OnchainEquity { balances } => InventorySnapshotEvent::OnchainEquity {
                balances,
                fetched_at: now,
            },
            OnchainCash { usdc_balance } => InventorySnapshotEvent::OnchainCash {
                usdc_balance,
                fetched_at: now,
            },
            OffchainEquity { positions } => InventorySnapshotEvent::OffchainEquity {
                positions,
                fetched_at: now,
            },
            OffchainCash { cash_balance_cents } => InventorySnapshotEvent::OffchainCash {
                cash_balance_cents,
                fetched_at: now,
            },
        }])
    }
}

impl InventorySnapshot {
    fn apply_event(&mut self, event: &InventorySnapshotEvent) {
        self.last_updated = event.timestamp();

        match event {
            InventorySnapshotEvent::OnchainEquity { balances, .. } => {
                self.onchain_equity = balances.clone();
            }
            InventorySnapshotEvent::OnchainCash { usdc_balance, .. } => {
                self.onchain_cash = Some(*usdc_balance);
            }
            InventorySnapshotEvent::OffchainEquity { positions, .. } => {
                self.offchain_equity = positions.clone();
            }
            InventorySnapshotEvent::OffchainCash {
                cash_balance_cents, ..
            } => {
                self.offchain_cash_cents = Some(*cash_balance_cents);
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum InventorySnapshotCommand {
    OnchainEquity {
        balances: BTreeMap<Symbol, FractionalShares>,
    },
    OnchainCash {
        usdc_balance: Usdc,
    },
    OffchainEquity {
        positions: BTreeMap<Symbol, FractionalShares>,
    },
    OffchainCash {
        cash_balance_cents: i64,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) enum InventorySnapshotEvent {
    OnchainEquity {
        balances: BTreeMap<Symbol, FractionalShares>,
        fetched_at: DateTime<Utc>,
    },
    OnchainCash {
        usdc_balance: Usdc,
        fetched_at: DateTime<Utc>,
    },
    OffchainEquity {
        positions: BTreeMap<Symbol, FractionalShares>,
        fetched_at: DateTime<Utc>,
    },
    OffchainCash {
        cash_balance_cents: i64,
        fetched_at: DateTime<Utc>,
    },
}

impl InventorySnapshotEvent {
    fn timestamp(&self) -> DateTime<Utc> {
        match self {
            Self::OnchainEquity { fetched_at, .. }
            | Self::OnchainCash { fetched_at, .. }
            | Self::OffchainEquity { fetched_at, .. }
            | Self::OffchainCash { fetched_at, .. } => *fetched_at,
        }
    }
}

impl DomainEvent for InventorySnapshotEvent {
    fn event_type(&self) -> String {
        match self {
            Self::OnchainEquity { .. } => "InventorySnapshotEvent::OnchainEquity".to_string(),
            Self::OnchainCash { .. } => "InventorySnapshotEvent::OnchainCash".to_string(),
            Self::OffchainEquity { .. } => "InventorySnapshotEvent::OffchainEquity".to_string(),
            Self::OffchainCash { .. } => "InventorySnapshotEvent::OffchainCash".to_string(),
        }
    }

    fn event_version(&self) -> String {
        "1.0".to_string()
    }
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;
    use std::str::FromStr;

    use super::*;
    use st0x_event_sorcery::{TestHarness, replay};

    #[test]
    fn inventory_snapshot_id_roundtrips_through_display_and_parse() {
        let id = InventorySnapshotId {
            orderbook: Address::repeat_byte(0xAB),
            owner: Address::repeat_byte(0xCD),
        };

        let parsed: InventorySnapshotId = id.to_string().parse().unwrap();

        assert_eq!(parsed, id);
    }

    #[test]
    fn inventory_snapshot_id_missing_delimiter() {
        let error = "0xdeadbeef".parse::<InventorySnapshotId>().unwrap_err();

        assert!(matches!(
            error,
            ParseInventorySnapshotIdError::MissingDelimiter { .. }
        ));
    }

    #[test]
    fn inventory_snapshot_id_invalid_orderbook() {
        let error = "not_hex:0xCdCdCdCdCdCdCdCdCdCdCdCdCdCdCdCdCdCdCdCd"
            .parse::<InventorySnapshotId>()
            .unwrap_err();

        assert!(matches!(error, ParseInventorySnapshotIdError::Orderbook(_)));
    }

    #[test]
    fn inventory_snapshot_id_invalid_owner() {
        let error = "0xAbAbAbAbAbAbAbAbAbAbAbAbAbAbAbAbAbAbAbAb:not_hex"
            .parse::<InventorySnapshotId>()
            .unwrap_err();

        assert!(matches!(error, ParseInventorySnapshotIdError::Owner(_)));
    }

    fn test_symbol(s: &str) -> Symbol {
        Symbol::new(s).unwrap()
    }

    fn test_shares(n: i64) -> FractionalShares {
        FractionalShares::new(Decimal::from(n))
    }

    #[tokio::test]
    async fn first_command_initializes_aggregate() {
        let mut balances = BTreeMap::new();
        balances.insert(test_symbol("AAPL"), test_shares(100));

        let events = TestHarness::<InventorySnapshot>::with(())
            .given_no_previous_events()
            .when(InventorySnapshotCommand::OnchainEquity {
                balances: balances.clone(),
            })
            .await
            .events();

        assert_eq!(events.len(), 1);
        match &events[0] {
            InventorySnapshotEvent::OnchainEquity {
                balances: event_balances,
                ..
            } => {
                assert_eq!(event_balances, &balances);
            }
            _ => panic!("Expected OnchainEquity event"),
        }
    }

    #[tokio::test]
    async fn record_onchain_equity_on_existing_aggregate() {
        let mut balances = BTreeMap::new();
        balances.insert(test_symbol("AAPL"), test_shares(100));

        let events = TestHarness::<InventorySnapshot>::with(())
            .given(vec![InventorySnapshotEvent::OnchainCash {
                usdc_balance: Usdc::from_str("1000").unwrap(),
                fetched_at: Utc::now(),
            }])
            .when(InventorySnapshotCommand::OnchainEquity {
                balances: balances.clone(),
            })
            .await
            .events();

        assert_eq!(events.len(), 1);
        match &events[0] {
            InventorySnapshotEvent::OnchainEquity {
                balances: event_balances,
                ..
            } => {
                assert_eq!(event_balances, &balances);
            }
            _ => panic!("Expected OnchainEquity event"),
        }
    }

    #[tokio::test]
    async fn record_onchain_cash_emits_event() {
        let usdc_balance = Usdc::from_str("10000.50").unwrap();

        let events = TestHarness::<InventorySnapshot>::with(())
            .given_no_previous_events()
            .when(InventorySnapshotCommand::OnchainCash { usdc_balance })
            .await
            .events();

        assert_eq!(events.len(), 1);
        match &events[0] {
            InventorySnapshotEvent::OnchainCash {
                usdc_balance: event_balance,
                ..
            } => {
                assert_eq!(*event_balance, usdc_balance);
            }
            _ => panic!("Expected OnchainCash event"),
        }
    }

    #[tokio::test]
    async fn record_offchain_equity_emits_event() {
        let mut positions = BTreeMap::new();
        positions.insert(test_symbol("AAPL"), test_shares(75));

        let events = TestHarness::<InventorySnapshot>::with(())
            .given_no_previous_events()
            .when(InventorySnapshotCommand::OffchainEquity {
                positions: positions.clone(),
            })
            .await
            .events();

        assert_eq!(events.len(), 1);
        match &events[0] {
            InventorySnapshotEvent::OffchainEquity {
                positions: event_positions,
                ..
            } => {
                assert_eq!(event_positions, &positions);
            }
            _ => panic!("Expected OffchainEquity event"),
        }
    }

    #[tokio::test]
    async fn record_offchain_cash_emits_event() {
        let cash_balance_cents = 50_000_000; // $500,000.00

        let events = TestHarness::<InventorySnapshot>::with(())
            .given_no_previous_events()
            .when(InventorySnapshotCommand::OffchainCash { cash_balance_cents })
            .await
            .events();

        assert_eq!(events.len(), 1);
        match &events[0] {
            InventorySnapshotEvent::OffchainCash {
                cash_balance_cents: event_cents,
                ..
            } => {
                assert_eq!(*event_cents, cash_balance_cents);
            }
            _ => panic!("Expected OffchainCash event"),
        }
    }

    #[test]
    fn apply_initializes_and_updates_state() {
        let mut balances = BTreeMap::new();
        balances.insert(test_symbol("AAPL"), test_shares(100));

        let usdc = Usdc::from_str("5000").unwrap();

        let snapshot = replay::<InventorySnapshot>(vec![
            InventorySnapshotEvent::OnchainEquity {
                balances: balances.clone(),
                fetched_at: Utc::now(),
            },
            InventorySnapshotEvent::OnchainCash {
                usdc_balance: usdc,
                fetched_at: Utc::now(),
            },
        ])
        .unwrap()
        .unwrap();

        assert_eq!(snapshot.onchain_equity, balances);
        assert_eq!(snapshot.onchain_cash, Some(usdc));
    }

    #[test]
    fn subsequent_fetches_replace_previous_values() {
        let mut first_balances = BTreeMap::new();
        first_balances.insert(test_symbol("AAPL"), test_shares(100));

        let mut second_balances = BTreeMap::new();
        second_balances.insert(test_symbol("MSFT"), test_shares(50));

        let snapshot = replay::<InventorySnapshot>(vec![
            InventorySnapshotEvent::OnchainEquity {
                balances: first_balances,
                fetched_at: Utc::now(),
            },
            InventorySnapshotEvent::OnchainEquity {
                balances: second_balances.clone(),
                fetched_at: Utc::now(),
            },
        ])
        .unwrap()
        .unwrap();

        assert_eq!(snapshot.onchain_equity, second_balances);
        assert!(!snapshot.onchain_equity.contains_key(&test_symbol("AAPL")));
    }
}

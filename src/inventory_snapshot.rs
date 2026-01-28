//! InventorySnapshot aggregate for recording fetched inventory.
//!
//! This aggregate records point-in-time snapshots of inventory fetched from
//! onchain vaults and offchain brokers. Events are consumed by InventoryView
//! to reconcile tracked inventory with actual balances.

use std::collections::BTreeMap;

use alloy::primitives::Address;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use cqrs_es::{Aggregate, DomainEvent};
use serde::{Deserialize, Serialize};
use st0x_execution::{FractionalShares, Symbol};

use crate::lifecycle::{Lifecycle, LifecycleError, Never};
use crate::threshold::Usdc;

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

impl InventorySnapshot {
    /// Creates the aggregate ID from orderbook and owner addresses.
    pub(crate) fn aggregate_id(orderbook: Address, owner: Address) -> String {
        format!("{orderbook}:{owner}")
    }

    fn empty(timestamp: DateTime<Utc>) -> Self {
        Self {
            onchain_equity: BTreeMap::new(),
            onchain_cash: None,
            offchain_equity: BTreeMap::new(),
            offchain_cash_cents: None,
            last_updated: timestamp,
        }
    }

    pub(crate) fn from_event(
        event: &InventorySnapshotEvent,
    ) -> Result<Self, LifecycleError<Never>> {
        let mut snapshot = Self::empty(event.timestamp());
        snapshot.apply_event(event);
        Ok(snapshot)
    }

    pub(crate) fn apply_transition(
        event: &InventorySnapshotEvent,
        snapshot: &Self,
    ) -> Result<Self, LifecycleError<Never>> {
        let mut new_snapshot = snapshot.clone();
        new_snapshot.apply_event(event);
        Ok(new_snapshot)
    }

    fn apply_event(&mut self, event: &InventorySnapshotEvent) {
        self.last_updated = event.timestamp();

        match event {
            InventorySnapshotEvent::OnchainEquityFetched { balances, .. } => {
                self.onchain_equity = balances.clone();
            }
            InventorySnapshotEvent::OnchainCashFetched { usdc_balance, .. } => {
                self.onchain_cash = Some(*usdc_balance);
            }
            InventorySnapshotEvent::OffchainEquityFetched { positions, .. } => {
                self.offchain_equity = positions.clone();
            }
            InventorySnapshotEvent::OffchainCashFetched {
                cash_balance_cents, ..
            } => {
                self.offchain_cash_cents = Some(*cash_balance_cents);
            }
        }
    }
}

#[async_trait]
impl Aggregate for Lifecycle<InventorySnapshot, Never> {
    type Command = InventorySnapshotCommand;
    type Event = InventorySnapshotEvent;
    type Error = InventorySnapshotError;
    type Services = ();

    fn aggregate_type() -> String {
        "InventorySnapshot".to_string()
    }

    fn apply(&mut self, event: Self::Event) {
        *self = self
            .clone()
            .transition(&event, InventorySnapshot::apply_transition)
            .or_initialize(&event, InventorySnapshot::from_event);
    }

    async fn handle(
        &self,
        command: Self::Command,
        _services: &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        let now = Utc::now();

        let event = match command {
            InventorySnapshotCommand::RecordOnchainEquity { balances } => {
                InventorySnapshotEvent::OnchainEquityFetched {
                    balances,
                    fetched_at: now,
                }
            }
            InventorySnapshotCommand::RecordOnchainCash { usdc_balance } => {
                InventorySnapshotEvent::OnchainCashFetched {
                    usdc_balance,
                    fetched_at: now,
                }
            }
            InventorySnapshotCommand::RecordOffchainEquity { positions } => {
                InventorySnapshotEvent::OffchainEquityFetched {
                    positions,
                    fetched_at: now,
                }
            }
            InventorySnapshotCommand::RecordOffchainCash { cash_balance_cents } => {
                InventorySnapshotEvent::OffchainCashFetched {
                    cash_balance_cents,
                    fetched_at: now,
                }
            }
        };

        Ok(vec![event])
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum InventorySnapshotError {
    #[error(transparent)]
    State(#[from] LifecycleError<Never>),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum InventorySnapshotCommand {
    RecordOnchainEquity {
        balances: BTreeMap<Symbol, FractionalShares>,
    },
    RecordOnchainCash {
        usdc_balance: Usdc,
    },
    RecordOffchainEquity {
        positions: BTreeMap<Symbol, FractionalShares>,
    },
    RecordOffchainCash {
        cash_balance_cents: i64,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) enum InventorySnapshotEvent {
    OnchainEquityFetched {
        balances: BTreeMap<Symbol, FractionalShares>,
        fetched_at: DateTime<Utc>,
    },
    OnchainCashFetched {
        usdc_balance: Usdc,
        fetched_at: DateTime<Utc>,
    },
    OffchainEquityFetched {
        positions: BTreeMap<Symbol, FractionalShares>,
        fetched_at: DateTime<Utc>,
    },
    OffchainCashFetched {
        cash_balance_cents: i64,
        fetched_at: DateTime<Utc>,
    },
}

impl InventorySnapshotEvent {
    fn timestamp(&self) -> DateTime<Utc> {
        match self {
            Self::OnchainEquityFetched { fetched_at, .. }
            | Self::OnchainCashFetched { fetched_at, .. }
            | Self::OffchainEquityFetched { fetched_at, .. }
            | Self::OffchainCashFetched { fetched_at, .. } => *fetched_at,
        }
    }
}

impl DomainEvent for InventorySnapshotEvent {
    fn event_type(&self) -> String {
        match self {
            Self::OnchainEquityFetched { .. } => {
                "InventorySnapshotEvent::OnchainEquityFetched".to_string()
            }
            Self::OnchainCashFetched { .. } => {
                "InventorySnapshotEvent::OnchainCashFetched".to_string()
            }
            Self::OffchainEquityFetched { .. } => {
                "InventorySnapshotEvent::OffchainEquityFetched".to_string()
            }
            Self::OffchainCashFetched { .. } => {
                "InventorySnapshotEvent::OffchainCashFetched".to_string()
            }
        }
    }

    fn event_version(&self) -> String {
        "1.0".to_string()
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use rust_decimal::Decimal;

    use super::*;

    type InventorySnapshotAggregate = Lifecycle<InventorySnapshot, Never>;

    fn test_symbol(s: &str) -> Symbol {
        Symbol::new(s).unwrap()
    }

    fn test_shares(n: i64) -> FractionalShares {
        FractionalShares::new(Decimal::from(n))
    }

    #[tokio::test]
    async fn first_command_initializes_aggregate() {
        let aggregate = InventorySnapshotAggregate::default();

        let mut balances = BTreeMap::new();
        balances.insert(test_symbol("AAPL"), test_shares(100));

        let events = aggregate
            .handle(
                InventorySnapshotCommand::RecordOnchainEquity {
                    balances: balances.clone(),
                },
                &(),
            )
            .await
            .unwrap();

        assert_eq!(events.len(), 1);
        match &events[0] {
            InventorySnapshotEvent::OnchainEquityFetched {
                balances: event_balances,
                ..
            } => {
                assert_eq!(event_balances, &balances);
            }
            _ => panic!("Expected OnchainEquityFetched event"),
        }
    }

    #[tokio::test]
    async fn record_onchain_equity_on_existing_aggregate() {
        let mut aggregate = InventorySnapshotAggregate::default();

        // First event initializes
        aggregate.apply(InventorySnapshotEvent::OnchainCashFetched {
            usdc_balance: Usdc::from_str("1000").unwrap(),
            fetched_at: Utc::now(),
        });

        // Second event updates
        let mut balances = BTreeMap::new();
        balances.insert(test_symbol("AAPL"), test_shares(100));

        let events = aggregate
            .handle(
                InventorySnapshotCommand::RecordOnchainEquity {
                    balances: balances.clone(),
                },
                &(),
            )
            .await
            .unwrap();

        assert_eq!(events.len(), 1);
        match &events[0] {
            InventorySnapshotEvent::OnchainEquityFetched {
                balances: event_balances,
                ..
            } => {
                assert_eq!(event_balances, &balances);
            }
            _ => panic!("Expected OnchainEquityFetched event"),
        }
    }

    #[tokio::test]
    async fn record_onchain_cash_emits_event() {
        let aggregate = InventorySnapshotAggregate::default();

        let usdc_balance = Usdc::from_str("10000.50").unwrap();

        let events = aggregate
            .handle(
                InventorySnapshotCommand::RecordOnchainCash { usdc_balance },
                &(),
            )
            .await
            .unwrap();

        assert_eq!(events.len(), 1);
        match &events[0] {
            InventorySnapshotEvent::OnchainCashFetched {
                usdc_balance: event_balance,
                ..
            } => {
                assert_eq!(*event_balance, usdc_balance);
            }
            _ => panic!("Expected OnchainCashFetched event"),
        }
    }

    #[tokio::test]
    async fn record_offchain_equity_emits_event() {
        let aggregate = InventorySnapshotAggregate::default();

        let mut positions = BTreeMap::new();
        positions.insert(test_symbol("AAPL"), test_shares(75));

        let events = aggregate
            .handle(
                InventorySnapshotCommand::RecordOffchainEquity {
                    positions: positions.clone(),
                },
                &(),
            )
            .await
            .unwrap();

        assert_eq!(events.len(), 1);
        match &events[0] {
            InventorySnapshotEvent::OffchainEquityFetched {
                positions: event_positions,
                ..
            } => {
                assert_eq!(event_positions, &positions);
            }
            _ => panic!("Expected OffchainEquityFetched event"),
        }
    }

    #[tokio::test]
    async fn record_offchain_cash_emits_event() {
        let aggregate = InventorySnapshotAggregate::default();

        let cash_balance_cents = 500_000_00; // $500,000.00

        let events = aggregate
            .handle(
                InventorySnapshotCommand::RecordOffchainCash { cash_balance_cents },
                &(),
            )
            .await
            .unwrap();

        assert_eq!(events.len(), 1);
        match &events[0] {
            InventorySnapshotEvent::OffchainCashFetched {
                cash_balance_cents: event_cents,
                ..
            } => {
                assert_eq!(*event_cents, cash_balance_cents);
            }
            _ => panic!("Expected OffchainCashFetched event"),
        }
    }

    #[test]
    fn apply_initializes_and_updates_state() {
        let mut aggregate = InventorySnapshotAggregate::default();

        // First event initializes
        let mut balances = BTreeMap::new();
        balances.insert(test_symbol("AAPL"), test_shares(100));

        aggregate.apply(InventorySnapshotEvent::OnchainEquityFetched {
            balances: balances.clone(),
            fetched_at: Utc::now(),
        });

        let Lifecycle::Live(snapshot) = &aggregate else {
            panic!("Expected Live state after first event");
        };
        assert_eq!(snapshot.onchain_equity, balances);
        assert!(snapshot.onchain_cash.is_none());

        // Second event updates
        let usdc = Usdc::from_str("5000").unwrap();
        aggregate.apply(InventorySnapshotEvent::OnchainCashFetched {
            usdc_balance: usdc,
            fetched_at: Utc::now(),
        });

        let Lifecycle::Live(snapshot) = &aggregate else {
            panic!("Expected Live state after second event");
        };
        assert_eq!(snapshot.onchain_equity, balances);
        assert_eq!(snapshot.onchain_cash, Some(usdc));
    }

    #[test]
    fn subsequent_fetches_replace_previous_values() {
        let mut aggregate = InventorySnapshotAggregate::default();

        let mut first_balances = BTreeMap::new();
        first_balances.insert(test_symbol("AAPL"), test_shares(100));

        aggregate.apply(InventorySnapshotEvent::OnchainEquityFetched {
            balances: first_balances,
            fetched_at: Utc::now(),
        });

        let mut second_balances = BTreeMap::new();
        second_balances.insert(test_symbol("MSFT"), test_shares(50));

        aggregate.apply(InventorySnapshotEvent::OnchainEquityFetched {
            balances: second_balances.clone(),
            fetched_at: Utc::now(),
        });

        let Lifecycle::Live(snapshot) = &aggregate else {
            panic!("Expected Live state");
        };
        assert_eq!(snapshot.onchain_equity, second_balances);
        assert!(!snapshot.onchain_equity.contains_key(&test_symbol("AAPL")));
    }
}

//! Reactor that dispatches InventorySnapshot events to InventoryView.

use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use tokio::sync::RwLock;
use tracing::warn;

use st0x_event_sorcery::Reactor;

use super::snapshot::InventorySnapshot;
use super::view::InventoryView;

/// Reactor that forwards InventorySnapshot events to a shared InventoryView.
///
/// This component implements the Reactor trait to receive events emitted by the
/// InventorySnapshot aggregate and apply them to the InventoryView for reconciliation.
pub(crate) struct InventorySnapshotReactor {
    inventory: Arc<RwLock<InventoryView>>,
}

impl InventorySnapshotReactor {
    pub(crate) fn new(inventory: Arc<RwLock<InventoryView>>) -> Self {
        Self { inventory }
    }
}

#[async_trait]
impl Reactor<InventorySnapshot> for InventorySnapshotReactor {
    async fn react(
        &self,
        _id: &<InventorySnapshot as st0x_event_sorcery::EventSourced>::Id,
        event: &<InventorySnapshot as st0x_event_sorcery::EventSourced>::Event,
    ) {
        let now = Utc::now();

        let mut inventory = self.inventory.write().await;

        match inventory.clone().apply_snapshot_event(event, now) {
            Ok(updated) => {
                *inventory = updated;
                drop(inventory);
            }
            Err(error) => {
                drop(inventory);
                warn!(%error, "Failed to apply inventory snapshot event");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use chrono::Utc;
    use rust_decimal::Decimal;

    use st0x_execution::{FractionalShares, Symbol};

    use alloy::primitives::Address;

    use super::*;
    use crate::inventory::snapshot::{InventorySnapshotEvent, InventorySnapshotId};
    use crate::inventory::view::{Imbalance, ImbalanceThreshold};
    use crate::threshold::Usdc;

    fn test_symbol() -> Symbol {
        Symbol::new("AAPL").unwrap()
    }

    fn test_shares(n: i64) -> FractionalShares {
        FractionalShares::new(Decimal::from(n))
    }

    fn balanced_threshold() -> ImbalanceThreshold {
        ImbalanceThreshold {
            target: Decimal::new(5, 1),    // 0.5
            deviation: Decimal::new(1, 1), // 0.1
        }
    }

    #[tokio::test]
    async fn dispatch_applies_onchain_equity_event_to_inventory() {
        let aapl = test_symbol();
        let inventory = Arc::new(RwLock::new(
            InventoryView::default().with_equity(aapl.clone()),
        ));
        let query = InventorySnapshotReactor::new(Arc::clone(&inventory));

        let mut balances = BTreeMap::new();
        balances.insert(aapl.clone(), test_shares(100));

        let event = InventorySnapshotEvent::OnchainEquity {
            balances,
            fetched_at: Utc::now(),
        };

        let id = InventorySnapshotId {
            orderbook: Address::ZERO,
            owner: Address::ZERO,
        };

        query.react(&id, &event).await;

        // 100 shares onchain, 0 offchain -> ratio = 1.0, target = 0.5 -> TooMuchOnchain
        let imbalance = inventory
            .read()
            .await
            .check_equity_imbalance(&aapl, &balanced_threshold())
            .expect("should detect imbalance after onchain equity snapshot");

        assert_eq!(
            imbalance,
            Imbalance::TooMuchOnchain {
                excess: test_shares(50)
            }
        );
    }

    #[tokio::test]
    async fn dispatch_applies_onchain_cash_event_to_inventory() {
        let inventory = Arc::new(RwLock::new(InventoryView::default()));
        let query = InventorySnapshotReactor::new(Arc::clone(&inventory));

        let event = InventorySnapshotEvent::OnchainCash {
            usdc_balance: Usdc(Decimal::from(1000)),
            fetched_at: Utc::now(),
        };

        let id = InventorySnapshotId {
            orderbook: Address::ZERO,
            owner: Address::ZERO,
        };

        query.react(&id, &event).await;

        // 1000 USDC onchain, 0 offchain -> ratio = 1.0, target = 0.5 -> TooMuchOnchain
        let imbalance = inventory
            .read()
            .await
            .check_usdc_imbalance(&balanced_threshold())
            .expect("should detect imbalance after onchain cash snapshot");

        assert_eq!(
            imbalance,
            Imbalance::TooMuchOnchain {
                excess: Usdc(Decimal::from(500))
            }
        );
    }

    #[tokio::test]
    async fn dispatch_applies_offchain_equity_event_to_inventory() {
        let aapl = test_symbol();
        let inventory = Arc::new(RwLock::new(
            InventoryView::default().with_equity(aapl.clone()),
        ));
        let query = InventorySnapshotReactor::new(Arc::clone(&inventory));

        let mut positions = BTreeMap::new();
        positions.insert(aapl.clone(), test_shares(50));

        let event = InventorySnapshotEvent::OffchainEquity {
            positions,
            fetched_at: Utc::now(),
        };

        let id = InventorySnapshotId {
            orderbook: Address::ZERO,
            owner: Address::ZERO,
        };

        query.react(&id, &event).await;

        // 0 onchain, 50 offchain -> ratio = 0.0, target = 0.5 -> TooMuchOffchain
        let imbalance = inventory
            .read()
            .await
            .check_equity_imbalance(&aapl, &balanced_threshold())
            .expect("should detect imbalance after offchain equity snapshot");

        assert_eq!(
            imbalance,
            Imbalance::TooMuchOffchain {
                excess: test_shares(25)
            }
        );
    }

    #[tokio::test]
    async fn dispatch_applies_offchain_cash_event_to_inventory() {
        let inventory = Arc::new(RwLock::new(InventoryView::default()));
        let query = InventorySnapshotReactor::new(Arc::clone(&inventory));

        let event = InventorySnapshotEvent::OffchainCash {
            cash_balance_cents: 50_000_000, // $500,000.00
            fetched_at: Utc::now(),
        };

        let id = InventorySnapshotId {
            orderbook: Address::ZERO,
            owner: Address::ZERO,
        };

        query.react(&id, &event).await;

        // 0 onchain, 500000 USDC offchain -> ratio = 0.0, target = 0.5 -> TooMuchOffchain
        let imbalance = inventory
            .read()
            .await
            .check_usdc_imbalance(&balanced_threshold())
            .expect("should detect imbalance after offchain cash snapshot");

        assert_eq!(
            imbalance,
            Imbalance::TooMuchOffchain {
                excess: Usdc(Decimal::from(250_000))
            }
        );
    }

    #[tokio::test]
    async fn dispatch_handles_multiple_events_sequentially() {
        let aapl = test_symbol();
        let inventory = Arc::new(RwLock::new(
            InventoryView::default().with_equity(aapl.clone()),
        ));
        let query = InventorySnapshotReactor::new(Arc::clone(&inventory));

        let mut balances = BTreeMap::new();
        balances.insert(aapl.clone(), test_shares(100));

        let id = InventorySnapshotId {
            orderbook: Address::ZERO,
            owner: Address::ZERO,
        };

        query
            .react(
                &id,
                &InventorySnapshotEvent::OnchainEquity {
                    balances,
                    fetched_at: Utc::now(),
                },
            )
            .await;

        query
            .react(
                &id,
                &InventorySnapshotEvent::OnchainCash {
                    usdc_balance: Usdc(Decimal::from(5000)),
                    fetched_at: Utc::now(),
                },
            )
            .await;

        let view = inventory.read().await;

        // Both events applied: equity 100 onchain/0 offchain, USDC 5000 onchain/0 offchain
        let equity_imbalance = view
            .check_equity_imbalance(&aapl, &balanced_threshold())
            .expect("should detect equity imbalance after multiple events");

        let usdc_imbalance = view
            .check_usdc_imbalance(&balanced_threshold())
            .expect("should detect USDC imbalance after multiple events");

        drop(view);

        assert_eq!(
            equity_imbalance,
            Imbalance::TooMuchOnchain {
                excess: test_shares(50)
            }
        );

        assert_eq!(
            usdc_imbalance,
            Imbalance::TooMuchOnchain {
                excess: Usdc(Decimal::from(2500))
            }
        );
    }
}

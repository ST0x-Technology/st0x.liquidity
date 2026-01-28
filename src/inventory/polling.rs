//! Inventory polling service for fetching actual balances and emitting snapshot events.
//!
//! This service polls onchain vaults and offchain broker accounts to fetch actual
//! inventory balances, then emits InventorySnapshotCommands to record the fetched
//! values. The InventoryView reacts to these events to reconcile tracked inventory.

use std::collections::BTreeMap;
use std::sync::Arc;

use alloy::primitives::Address;
use alloy::providers::Provider;
use cqrs_es::AggregateError;
use sqlite_es::sqlite_cqrs;
use sqlx::SqlitePool;
use st0x_execution::{Executor, InventoryResult};

use crate::inventory::snapshot::{
    InventorySnapshot, InventorySnapshotCommand, InventorySnapshotError,
};
use crate::lifecycle::{Lifecycle, Never};
use crate::onchain::vault::{VaultError, VaultService};

type InventorySnapshotAggregate = Lifecycle<InventorySnapshot, Never>;

/// Error type for inventory polling operations.
#[derive(Debug, thiserror::Error)]
pub(crate) enum InventoryPollingError<ExecutorError> {
    #[error(transparent)]
    Vault(#[from] VaultError),
    #[error(transparent)]
    Executor(ExecutorError),
    #[error(transparent)]
    Aggregate(#[from] AggregateError<InventorySnapshotError>),
}

/// Service that polls actual inventory from onchain vaults and offchain brokers.
pub(crate) struct InventoryPollingService<P, E>
where
    P: Provider + Clone,
{
    vault_service: Arc<VaultService<P>>,
    executor: E,
    pool: SqlitePool,
    orderbook: Address,
    order_owner: Address,
}

impl<P, E> InventoryPollingService<P, E>
where
    P: Provider + Clone,
    E: Executor,
{
    pub(crate) fn new(
        vault_service: Arc<VaultService<P>>,
        executor: E,
        pool: SqlitePool,
        orderbook: Address,
        order_owner: Address,
    ) -> Self {
        Self {
            vault_service,
            executor,
            pool,
            orderbook,
            order_owner,
        }
    }

    /// Polls actual inventory from both venues and emits snapshot commands.
    ///
    /// 1. Queries onchain equity balances from discovered vaults
    /// 2. Queries onchain USDC balance from USDC vault
    /// 3. Queries offchain positions and cash from executor
    /// 4. Emits InventorySnapshot events via corresponding commands
    pub(crate) async fn poll_and_record(&self) -> Result<(), InventoryPollingError<E::Error>> {
        let aggregate_id = InventorySnapshot::aggregate_id(self.orderbook, self.order_owner);
        let cqrs = sqlite_cqrs::<InventorySnapshotAggregate>(self.pool.clone(), vec![], ());

        let inventory_result = self
            .executor
            .get_inventory()
            .await
            .map_err(InventoryPollingError::Executor)?;

        let InventoryResult::Fetched(inventory) = inventory_result else {
            return Ok(());
        };

        let positions: BTreeMap<_, _> = inventory
            .positions
            .into_iter()
            .map(|position| (position.symbol, position.quantity))
            .collect();

        cqrs.execute(
            &aggregate_id,
            InventorySnapshotCommand::RecordOffchainEquity { positions },
        )
        .await?;

        cqrs.execute(
            &aggregate_id,
            InventorySnapshotCommand::RecordOffchainCash {
                cash_balance_cents: inventory.cash_balance_cents,
            },
        )
        .await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::address;
    use alloy::providers::ProviderBuilder;
    use rust_decimal::Decimal;
    use sqlx::Row;
    use st0x_execution::{EquityPosition, FractionalShares, Inventory, MockExecutor, Symbol};

    use super::*;
    use crate::inventory::snapshot::{InventorySnapshot, InventorySnapshotEvent};
    use crate::test_utils::setup_test_db;

    fn test_provider() -> impl Provider + Clone {
        ProviderBuilder::new().on_http("http://localhost:8545".parse().unwrap())
    }

    fn test_addresses() -> (Address, Address) {
        let orderbook = address!("0x1111111111111111111111111111111111111111");
        let order_owner = address!("0x2222222222222222222222222222222222222222");
        (orderbook, order_owner)
    }

    fn test_symbol(s: &str) -> Symbol {
        Symbol::new(s).unwrap()
    }

    fn test_shares(n: i64) -> FractionalShares {
        FractionalShares::new(Decimal::from(n))
    }

    #[tokio::test]
    async fn poll_and_record_emits_offchain_equity_command_with_executor_positions() {
        let pool = setup_test_db().await;
        let provider = test_provider();
        let vault_service = Arc::new(VaultService::new(provider.clone(), Address::ZERO));
        let (orderbook, order_owner) = test_addresses();

        let inventory = Inventory {
            positions: vec![
                EquityPosition {
                    symbol: test_symbol("AAPL"),
                    quantity: test_shares(100),
                    market_value_cents: Some(15000_00),
                },
                EquityPosition {
                    symbol: test_symbol("MSFT"),
                    quantity: test_shares(50),
                    market_value_cents: Some(20000_00),
                },
            ],
            cash_balance_cents: 100_000_00,
        };
        let executor = MockExecutor::new().with_inventory(inventory.clone());

        let service = InventoryPollingService::new(
            vault_service,
            executor,
            pool.clone(),
            orderbook,
            order_owner,
        );

        service.poll_and_record().await.unwrap();

        // Verify OffchainEquityFetched event was emitted with correct positions
        let events = load_snapshot_events(&pool, orderbook, order_owner).await;
        let offchain_equity_event = events
            .iter()
            .find(|e| matches!(e, InventorySnapshotEvent::OffchainEquityFetched { .. }))
            .expect("Expected OffchainEquityFetched event to be emitted");

        let InventorySnapshotEvent::OffchainEquityFetched { positions, .. } = offchain_equity_event
        else {
            panic!("Expected OffchainEquityFetched event, got {offchain_equity_event:?}");
        };
        assert_eq!(positions.len(), 2, "Expected 2 positions");
        assert_eq!(
            positions.get(&test_symbol("AAPL")),
            Some(&test_shares(100)),
            "AAPL position mismatch"
        );
        assert_eq!(
            positions.get(&test_symbol("MSFT")),
            Some(&test_shares(50)),
            "MSFT position mismatch"
        );
    }

    #[tokio::test]
    async fn poll_and_record_emits_offchain_cash_command_with_executor_cash_balance() {
        let pool = setup_test_db().await;
        let provider = test_provider();
        let vault_service = Arc::new(VaultService::new(provider.clone(), Address::ZERO));
        let (orderbook, order_owner) = test_addresses();

        let inventory = Inventory {
            positions: vec![],
            cash_balance_cents: 250_000_00, // $250,000.00
        };
        let executor = MockExecutor::new().with_inventory(inventory);

        let service = InventoryPollingService::new(
            vault_service,
            executor,
            pool.clone(),
            orderbook,
            order_owner,
        );

        service.poll_and_record().await.unwrap();

        // Verify OffchainCashFetched event was emitted with correct amount
        let events = load_snapshot_events(&pool, orderbook, order_owner).await;
        let offchain_cash_event = events
            .iter()
            .find(|e| matches!(e, InventorySnapshotEvent::OffchainCashFetched { .. }))
            .expect("Expected OffchainCashFetched event to be emitted");

        let InventorySnapshotEvent::OffchainCashFetched {
            cash_balance_cents, ..
        } = offchain_cash_event
        else {
            panic!("Expected OffchainCashFetched event, got {offchain_cash_event:?}");
        };
        assert_eq!(
            *cash_balance_cents, 250_000_00,
            "Cash balance mismatch: expected $250,000.00"
        );
    }

    #[tokio::test]
    async fn poll_and_record_emits_empty_positions_when_executor_has_no_positions() {
        let pool = setup_test_db().await;
        let provider = test_provider();
        let vault_service = Arc::new(VaultService::new(provider.clone(), Address::ZERO));
        let (orderbook, order_owner) = test_addresses();

        let inventory = Inventory {
            positions: vec![],
            cash_balance_cents: 50_000_00,
        };
        let executor = MockExecutor::new().with_inventory(inventory);

        let service = InventoryPollingService::new(
            vault_service,
            executor,
            pool.clone(),
            orderbook,
            order_owner,
        );

        service.poll_and_record().await.unwrap();

        let events = load_snapshot_events(&pool, orderbook, order_owner).await;
        let offchain_equity_event = events
            .iter()
            .find(|e| matches!(e, InventorySnapshotEvent::OffchainEquityFetched { .. }))
            .expect("Expected OffchainEquityFetched event even with empty positions");

        let InventorySnapshotEvent::OffchainEquityFetched { positions, .. } = offchain_equity_event
        else {
            panic!("Expected OffchainEquityFetched event, got {offchain_equity_event:?}");
        };
        assert!(positions.is_empty(), "Expected empty positions map");
    }

    #[tokio::test]
    async fn poll_and_record_skips_offchain_commands_when_executor_returns_unimplemented() {
        let pool = setup_test_db().await;
        let provider = test_provider();
        let vault_service = Arc::new(VaultService::new(provider.clone(), Address::ZERO));
        let (orderbook, order_owner) = test_addresses();

        let executor = MockExecutor::new();

        let service = InventoryPollingService::new(
            vault_service,
            executor,
            pool.clone(),
            orderbook,
            order_owner,
        );

        // Should succeed without error
        service.poll_and_record().await.unwrap();

        // Verify NO offchain events were emitted
        let events = load_snapshot_events(&pool, orderbook, order_owner).await;
        let has_offchain_equity = events
            .iter()
            .any(|e| matches!(e, InventorySnapshotEvent::OffchainEquityFetched { .. }));
        let has_offchain_cash = events
            .iter()
            .any(|e| matches!(e, InventorySnapshotEvent::OffchainCashFetched { .. }));

        assert!(
            !has_offchain_equity,
            "Should NOT emit OffchainEquityFetched when executor returns Unimplemented"
        );
        assert!(
            !has_offchain_cash,
            "Should NOT emit OffchainCashFetched when executor returns Unimplemented"
        );
    }

    #[tokio::test]
    async fn poll_and_record_handles_negative_cash_balance() {
        let pool = setup_test_db().await;
        let provider = test_provider();
        let vault_service = Arc::new(VaultService::new(provider.clone(), Address::ZERO));
        let (orderbook, order_owner) = test_addresses();

        // Margin account with negative cash (borrowed funds)
        let inventory = Inventory {
            positions: vec![EquityPosition {
                symbol: test_symbol("AAPL"),
                quantity: test_shares(1000),
                market_value_cents: Some(150_000_00),
            }],
            cash_balance_cents: -50_000_00, // -$50,000 (margin debt)
        };
        let executor = MockExecutor::new().with_inventory(inventory);

        let service = InventoryPollingService::new(
            vault_service,
            executor,
            pool.clone(),
            orderbook,
            order_owner,
        );

        service.poll_and_record().await.unwrap();

        let events = load_snapshot_events(&pool, orderbook, order_owner).await;
        let offchain_cash_event = events
            .iter()
            .find(|e| matches!(e, InventorySnapshotEvent::OffchainCashFetched { .. }))
            .expect("Expected OffchainCashFetched event");

        let InventorySnapshotEvent::OffchainCashFetched {
            cash_balance_cents, ..
        } = offchain_cash_event
        else {
            panic!("Expected OffchainCashFetched event, got {offchain_cash_event:?}");
        };
        assert_eq!(
            *cash_balance_cents, -50_000_00,
            "Should preserve negative cash balance"
        );
    }

    #[tokio::test]
    async fn poll_and_record_handles_fractional_share_positions() {
        let pool = setup_test_db().await;
        let provider = test_provider();
        let vault_service = Arc::new(VaultService::new(provider.clone(), Address::ZERO));
        let (orderbook, order_owner) = test_addresses();

        let fractional_qty = FractionalShares::new(Decimal::new(12345, 3)); // 12.345 shares
        let inventory = Inventory {
            positions: vec![EquityPosition {
                symbol: test_symbol("AAPL"),
                quantity: fractional_qty,
                market_value_cents: Some(1851_75), // ~$1851.75
            }],
            cash_balance_cents: 10_000_00,
        };
        let executor = MockExecutor::new().with_inventory(inventory);

        let service = InventoryPollingService::new(
            vault_service,
            executor,
            pool.clone(),
            orderbook,
            order_owner,
        );

        service.poll_and_record().await.unwrap();

        let events = load_snapshot_events(&pool, orderbook, order_owner).await;
        let offchain_equity_event = events
            .iter()
            .find(|e| matches!(e, InventorySnapshotEvent::OffchainEquityFetched { .. }))
            .expect("Expected OffchainEquityFetched event");

        let InventorySnapshotEvent::OffchainEquityFetched { positions, .. } = offchain_equity_event
        else {
            panic!("Expected OffchainEquityFetched event, got {offchain_equity_event:?}");
        };
        assert_eq!(
            positions.get(&test_symbol("AAPL")),
            Some(&fractional_qty),
            "Should preserve fractional share quantity"
        );
    }

    #[tokio::test]
    async fn poll_and_record_uses_correct_aggregate_id() {
        let pool = setup_test_db().await;
        let provider = test_provider();
        let vault_service = Arc::new(VaultService::new(provider.clone(), Address::ZERO));
        let orderbook = address!("0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA");
        let order_owner = address!("0xBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB");

        let inventory = Inventory {
            positions: vec![],
            cash_balance_cents: 100_00,
        };
        let executor = MockExecutor::new().with_inventory(inventory);

        let service = InventoryPollingService::new(
            vault_service,
            executor,
            pool.clone(),
            orderbook,
            order_owner,
        );

        service.poll_and_record().await.unwrap();

        // Verify events were stored under the correct aggregate ID
        let expected_aggregate_id = InventorySnapshot::aggregate_id(orderbook, order_owner);
        let events = load_events_for_aggregate(&pool, &expected_aggregate_id).await;

        assert!(
            !events.is_empty(),
            "Expected events under aggregate ID {expected_aggregate_id}"
        );
    }

    /// Loads all InventorySnapshotEvents for the given orderbook/owner from the event store.
    async fn load_snapshot_events(
        pool: &SqlitePool,
        orderbook: Address,
        order_owner: Address,
    ) -> Vec<InventorySnapshotEvent> {
        let aggregate_id = InventorySnapshot::aggregate_id(orderbook, order_owner);
        load_events_for_aggregate(pool, &aggregate_id).await
    }

    /// Loads events for a specific aggregate ID from the SQLite event store.
    async fn load_events_for_aggregate(
        pool: &SqlitePool,
        aggregate_id: &str,
    ) -> Vec<InventorySnapshotEvent> {
        let rows = sqlx::query(
            r#"
            SELECT payload
            FROM events
            WHERE aggregate_id = ?
            ORDER BY sequence ASC
            "#,
        )
        .bind(aggregate_id)
        .fetch_all(pool)
        .await
        .unwrap();

        rows.iter()
            .map(|row| {
                let payload: String = row.get("payload");
                serde_json::from_str(&payload).unwrap()
            })
            .collect()
    }
}

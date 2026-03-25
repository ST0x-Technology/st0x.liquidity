//! Reactor that broadcasts aggregate events to WebSocket dashboard
//! clients as [`Trade`] fills and [`TransferOperation`] updates.

use async_trait::async_trait;
use sqlx::SqlitePool;
use tokio::sync::broadcast;
use tracing::warn;

use st0x_dto::{Statement, Trade, TradeDirection, TradingVenue};
use st0x_event_sorcery::{EntityList, Never, Reactor, deps, load_entity};
use st0x_execution::SupportedExecutor;

use crate::equity_redemption::EquityRedemption;
use crate::offchain_order::OffchainOrder;
use crate::onchain_trade::{OnChainTrade, OnChainTradeEvent};
use crate::position::Position;
use crate::tokenized_equity_mint::TokenizedEquityMint;
use crate::usdc_rebalance::UsdcRebalance;

deps!(
    Broadcaster,
    [
        OnChainTrade,
        Position,
        OffchainOrder,
        TokenizedEquityMint,
        EquityRedemption,
        UsdcRebalance,
    ]
);

/// Reactor that broadcasts notifications and trade fills to connected
/// WebSocket clients.
pub(crate) struct Broadcaster {
    sender: broadcast::Sender<Statement>,
    pool: SqlitePool,
}

impl Broadcaster {
    pub(crate) fn new(sender: broadcast::Sender<Statement>, pool: SqlitePool) -> Self {
        Self { sender, pool }
    }

    fn broadcast_fill(&self, trade: Trade) {
        if let Err(error) = self.sender.send(Statement::TradeFill(trade)) {
            warn!("Failed to broadcast trade fill (no receivers): {error}");
        }
    }

    fn broadcast_position(&self, position: st0x_dto::Position) {
        if let Err(error) = self.sender.send(Statement::PositionUpdate(position)) {
            warn!("Failed to broadcast position update (no receivers): {error}");
        }
    }

    fn broadcast_transfer(&self, transfer: st0x_dto::TransferOperation) {
        if let Err(error) = self.sender.send(Statement::TransferUpdate(transfer)) {
            warn!("Failed to broadcast transfer update (no receivers): {error}");
        }
    }
}

/// Convert a [`SupportedExecutor`] to a [`TradingVenue`] for the dashboard.
pub(crate) fn executor_to_venue(executor: SupportedExecutor) -> TradingVenue {
    match executor {
        SupportedExecutor::AlpacaBrokerApi => TradingVenue::Alpaca,
        SupportedExecutor::DryRun => TradingVenue::DryRun,
    }
}

/// Convert an execution [`Direction`] to a [`TradeDirection`] for the dashboard.
pub(crate) fn direction_to_dto(direction: st0x_execution::Direction) -> TradeDirection {
    match direction {
        st0x_execution::Direction::Buy => TradeDirection::Buy,
        st0x_execution::Direction::Sell => TradeDirection::Sell,
    }
}

#[async_trait]
impl Reactor for Broadcaster {
    type Error = Never;

    async fn react(
        &self,
        event: <Self::Dependencies as EntityList>::Event,
    ) -> Result<(), Self::Error> {
        event
            .on(|id, event| async move {
                if let OnChainTradeEvent::Filled {
                    symbol,
                    amount,
                    direction,
                    filled_at,
                    ..
                } = &event
                {
                    self.broadcast_fill(Trade {
                        id: id.to_string(),
                        filled_at: *filled_at,
                        venue: TradingVenue::Raindex,
                        direction: direction_to_dto(*direction),
                        symbol: symbol.clone(),
                        shares: st0x_finance::FractionalShares::new(*amount),
                    });
                }
            })
            .on(|id, _event| async move {
                match load_entity::<Position>(&self.pool, &id).await {
                    Ok(Some(position)) => {
                        self.broadcast_position(st0x_dto::Position {
                            symbol: position.symbol,
                            net: position.net.inner(),
                        });
                    }
                    Ok(None) => warn!(%id, "Position not found after event"),
                    Err(error) => warn!(%id, ?error, "Failed to load position for broadcast"),
                }
            })
            .on(|id, event| async move {
                if matches!(
                    event,
                    crate::offchain_order::OffchainOrderEvent::Filled { .. }
                ) {
                    match load_entity::<OffchainOrder>(&self.pool, &id).await {
                        Ok(Some(OffchainOrder::Filled {
                            symbol,
                            shares,
                            direction,
                            executor,
                            filled_at,
                            ..
                        })) => {
                            self.broadcast_fill(Trade {
                                id: id.to_string(),
                                filled_at,
                                venue: executor_to_venue(executor),
                                direction: direction_to_dto(direction),
                                symbol,
                                shares: st0x_finance::FractionalShares::new(shares.inner().inner()),
                            });
                        }
                        Ok(_) => {
                            warn!(%id, "OffchainOrder not in Filled state after Filled event");
                        }
                        Err(error) => {
                            warn!(%id, ?error, "Failed to load OffchainOrder for fill broadcast");
                        }
                    }
                }
            })
            .on(|id, _event| async move {
                match load_entity::<TokenizedEquityMint>(&self.pool, &id).await {
                    Ok(Some(entity)) => self.broadcast_transfer(entity.to_dto(&id)),
                    Ok(None) => warn!(%id, "Mint entity not found for transfer broadcast"),
                    Err(error) => warn!(%id, ?error, "Failed to load mint for broadcast"),
                }
            })
            .on(|id, _event| async move {
                match load_entity::<EquityRedemption>(&self.pool, &id).await {
                    Ok(Some(entity)) => self.broadcast_transfer(entity.to_dto(&id)),
                    Ok(None) => warn!(%id, "Redemption entity not found for broadcast"),
                    Err(error) => warn!(%id, ?error, "Failed to load redemption for broadcast"),
                }
            })
            .on(|id, _event| async move {
                match load_entity::<UsdcRebalance>(&self.pool, &id).await {
                    Ok(Some(entity)) => self.broadcast_transfer(entity.to_dto(&id)),
                    Ok(None) => warn!(%id, "USDC rebalance entity not found for broadcast"),
                    Err(error) => warn!(%id, ?error, "Failed to load rebalance for broadcast"),
                }
            })
            .exhaustive()
            .await;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use st0x_event_sorcery::ReactorHarness;
    use st0x_execution::Symbol;

    use super::*;
    use crate::test_utils::setup_test_db;

    fn test_broadcaster(pool: SqlitePool) -> (Broadcaster, broadcast::Receiver<Statement>) {
        let (sender, receiver) = broadcast::channel(16);
        let broadcaster = Broadcaster::new(sender, pool);
        (broadcaster, receiver)
    }

    #[tokio::test]
    async fn no_broadcast_without_events() {
        let pool = setup_test_db().await;
        let (sender, mut receiver) = broadcast::channel(16);
        let _broadcaster = Broadcaster::new(sender, pool);

        let result =
            tokio::time::timeout(std::time::Duration::from_millis(10), receiver.recv()).await;

        assert!(result.is_err(), "should timeout with no messages");
    }

    #[tokio::test]
    async fn onchain_trade_filled_broadcasts_fill() {
        let pool = setup_test_db().await;
        let (broadcaster, mut receiver) = test_broadcaster(pool);
        let harness = ReactorHarness::new(broadcaster);

        let now = chrono::Utc::now();
        let id = crate::onchain_trade::OnChainTradeId {
            tx_hash: alloy::primitives::TxHash::ZERO,
            log_index: 0,
        };

        harness
            .receive::<OnChainTrade>(
                id,
                OnChainTradeEvent::Filled {
                    symbol: Symbol::new("AAPL").unwrap(),
                    amount: st0x_float_macro::float!(10),
                    direction: st0x_execution::Direction::Buy,
                    price_usdc: st0x_float_macro::float!(150),
                    block_number: 12345,
                    block_timestamp: now,
                    filled_at: now,
                },
            )
            .await
            .unwrap();

        let msg = receiver.recv().await.expect("should receive fill");

        match msg {
            Statement::TradeFill(trade) => {
                assert!(matches!(trade.venue, TradingVenue::Raindex));
                assert!(matches!(trade.direction, TradeDirection::Buy));
                assert_eq!(trade.symbol, Symbol::new("AAPL").unwrap());
            }
            other => panic!("expected TradeFill message, got {other:?}"),
        }
    }

    async fn insert_event(
        pool: &SqlitePool,
        aggregate_type: &str,
        aggregate_id: &str,
        sequence: i64,
        event_type: &str,
        payload: serde_json::Value,
    ) {
        sqlx::query(
            "INSERT INTO events (aggregate_type, aggregate_id, sequence,
             event_type, event_version, payload, metadata)
             VALUES (?1, ?2, ?3, ?4, '1.0', ?5, '{}')",
        )
        .bind(aggregate_type)
        .bind(aggregate_id)
        .bind(sequence)
        .bind(event_type)
        .bind(serde_json::to_string(&payload).unwrap())
        .execute(pool)
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn offchain_order_filled_broadcasts_fill() {
        use crate::offchain_order::OffchainOrderEvent;

        let pool = setup_test_db().await;
        let (broadcaster, mut receiver) = test_broadcaster(pool.clone());
        let harness = ReactorHarness::new(broadcaster);

        let now = chrono::Utc::now();
        let id = crate::offchain_order::OffchainOrderId::new();

        // Seed OffchainOrder aggregate to Filled state via direct event insertion
        let placed = OffchainOrderEvent::Placed {
            symbol: Symbol::new("TSLA").unwrap(),
            shares: st0x_execution::Positive::new(st0x_execution::FractionalShares::new(
                st0x_float_macro::float!(5),
            ))
            .unwrap(),
            direction: st0x_execution::Direction::Sell,
            executor: st0x_execution::SupportedExecutor::AlpacaBrokerApi,
            placed_at: now,
        };
        let submitted = OffchainOrderEvent::Submitted {
            executor_order_id: st0x_execution::ExecutorOrderId::new("test-order"),
            submitted_at: now,
        };
        let filled = OffchainOrderEvent::Filled {
            price: st0x_finance::Usd::new(st0x_float_macro::float!(245)),
            filled_at: now,
        };

        let id_str = id.to_string();
        insert_event(
            &pool,
            "OffchainOrder",
            &id_str,
            0,
            "OffchainOrderEvent::Placed",
            serde_json::to_value(&placed).unwrap(),
        )
        .await;
        insert_event(
            &pool,
            "OffchainOrder",
            &id_str,
            1,
            "OffchainOrderEvent::Submitted",
            serde_json::to_value(&submitted).unwrap(),
        )
        .await;
        insert_event(
            &pool,
            "OffchainOrder",
            &id_str,
            2,
            "OffchainOrderEvent::Filled",
            serde_json::to_value(&filled).unwrap(),
        )
        .await;

        // Fire the Filled event through the reactor
        harness.receive::<OffchainOrder>(id, filled).await.unwrap();

        let msg = receiver.recv().await.expect("should receive fill");

        match msg {
            Statement::TradeFill(trade) => {
                assert!(matches!(trade.venue, TradingVenue::Alpaca));
                assert!(matches!(trade.direction, TradeDirection::Sell));
                assert_eq!(trade.symbol, Symbol::new("TSLA").unwrap());
            }
            other => panic!("expected TradeFill message, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn position_update_broadcasts_net_position() {
        use crate::position::PositionEvent;

        let pool = setup_test_db().await;
        let (broadcaster, mut receiver) = test_broadcaster(pool.clone());
        let harness = ReactorHarness::new(broadcaster);

        let symbol = Symbol::new("AAPL").unwrap();
        let now = chrono::Utc::now();

        // Seed a Position aggregate via direct event insertion
        let initialized = PositionEvent::Initialized {
            symbol: symbol.clone(),
            threshold: crate::threshold::ExecutionThreshold::Shares(
                st0x_execution::Positive::new(st0x_execution::FractionalShares::new(
                    st0x_float_macro::float!(1),
                ))
                .unwrap(),
            ),
            initialized_at: now,
        };

        insert_event(
            &pool,
            "Position",
            &symbol.to_string(),
            0,
            "PositionEvent::Initialized",
            serde_json::to_value(&initialized).unwrap(),
        )
        .await;

        harness
            .receive::<Position>(symbol.clone(), initialized)
            .await
            .unwrap();

        let msg = receiver
            .recv()
            .await
            .expect("should receive position update");

        match msg {
            Statement::PositionUpdate(position) => {
                assert_eq!(position.symbol, symbol);
            }
            other => panic!("expected PositionUpdate message, got {other:?}"),
        }
    }
}

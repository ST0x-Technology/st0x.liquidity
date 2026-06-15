//! Reactor that broadcasts aggregate events to WebSocket dashboard
//! clients as [`Trade`] fills and [`TransferOperation`] updates.

use async_trait::async_trait;
use sqlx::SqlitePool;
use tokio::sync::broadcast;
use tracing::{debug, warn};

use st0x_dto::{Statement, Trade, TradingVenue};
use st0x_event_sorcery::{EntityList, Never, Reactor, deps, load_entity};

use crate::equity_redemption::EquityRedemption;
use crate::offchain::order::{OffchainOrder, OffchainOrderEvent};
use crate::onchain_trade::{OnChainTrade, OnChainTradeEvent};
use crate::position::{Position, PositionEvent};
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
            debug!(target: "dashboard", %error, "Failed to broadcast trade fill (no receivers)");
        }
    }

    fn broadcast_position(&self, position: st0x_dto::Position) {
        if let Err(error) = self.sender.send(Statement::PositionUpdate(position)) {
            debug!(target: "dashboard", %error, "Failed to broadcast position update (no receivers)");
        }
    }

    fn broadcast_transfer(&self, transfer: st0x_dto::TransferOperation) {
        if let Err(error) = self.sender.send(Statement::TransferUpdate(transfer)) {
            debug!(target: "dashboard", %error, "Failed to broadcast transfer update (no receivers)");
        }
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
                        direction: *direction,
                        symbol: symbol.clone(),
                        shares: st0x_finance::FractionalShares::new(*amount),
                    });
                }
            })

            .on(|id, event| async move {
                if !matches!(
                    event,
                    PositionEvent::OnChainOrderFilled { .. }
                        | PositionEvent::OffChainOrderFilled { .. }
                        | PositionEvent::ManualPositionAdjusted { .. }
                ) {
                    return;
                }

                match load_entity::<Position>(&self.pool, &id).await {
                    Ok(Some(position)) => {
                        self.broadcast_position(st0x_dto::Position {
                            symbol: position.symbol,
                            net: position.net.inner(),
                            last_price_usdc: position.last_price_usdc,
                        });
                    }
                    Ok(None) => warn!(target: "dashboard", %id, "Position not found after event"),
                    Err(error) => warn!(target: "dashboard", %id, ?error, "Failed to load position for broadcast"),
                }
            })

            .on(|id, event| async move {
                use OffchainOrderEvent::*;
                match event {
                    Filled { .. } => {
                        match load_entity::<OffchainOrder>(&self.pool, &id).await {
                            Ok(Some(order)) => match order.try_to_trade(&id) {
                                Ok(trade) => self.broadcast_fill(trade),
                                Err(error) => warn!(
                                    target: "dashboard",
                                    %id, %error,
                                    "OffchainOrder not in Filled state after Filled event"
                                ),
                            },
                            Ok(None) => warn!(
                                target: "dashboard",
                                %id,
                                "OffchainOrder replayed to empty state"
                            ),
                            Err(error) => warn!(
                                target: "dashboard",
                                %id, ?error,
                                "Failed to load OffchainOrder for fill broadcast"
                            ),
                        }
                    }
                    Placed { .. }
                    | Submitted { .. }
                    | Accepted { .. }
                    | PartiallyFilled { .. }
                    | CancelRequested { .. }
                    | Failed { .. }
                    | Cancelled { .. } => {}
                }
            })

            .on(|id, _event| async move {
                match load_entity::<TokenizedEquityMint>(&self.pool, &id).await {
                    Ok(Some(entity)) => self.broadcast_transfer(entity.to_dto(&id)),
                    Ok(None) => warn!(target: "dashboard", %id, "Mint entity not found for transfer broadcast"),
                    Err(error) => warn!(target: "dashboard", %id, ?error, "Failed to load mint for broadcast"),
                }
            })

            .on(|id, _event| async move {
                match load_entity::<EquityRedemption>(&self.pool, &id).await {
                    Ok(Some(entity)) => self.broadcast_transfer(entity.to_dto(&id)),
                    Ok(None) => warn!(target: "dashboard", %id, "Redemption entity not found for broadcast"),
                    Err(error) => warn!(target: "dashboard", %id, ?error, "Failed to load redemption for broadcast"),
                }
            })

            .on(|id, _event| async move {
                match load_entity::<UsdcRebalance>(&self.pool, &id).await {
                    Ok(Some(entity)) => self.broadcast_transfer(entity.to_dto(&id)),
                    Ok(None) => warn!(target: "dashboard", %id, "USDC rebalance entity not found for broadcast"),
                    Err(error) => warn!(target: "dashboard", %id, ?error, "Failed to load rebalance for broadcast"),
                }
            })
            .exhaustive()
            .await;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use st0x_event_sorcery::{ReactorHarness, StoreBuilder};
    use st0x_execution::Symbol;

    use super::*;
    use crate::offchain::order::{OffchainOrderCommand, OffchainOrderEvent};
    use crate::position::{PositionCommand, PositionEvent, TradeId};
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
                    block_hash: None,
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
                assert!(matches!(trade.direction, st0x_dto::Direction::Buy));
                assert_eq!(trade.symbol, Symbol::new("AAPL").unwrap());
            }
            other => panic!("expected TradeFill message, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn offchain_order_filled_broadcasts_fill() {
        let pool = setup_test_db().await;
        let (store, _projection) = StoreBuilder::<OffchainOrder>::new(pool.clone())
            .build(crate::offchain::order::noop_order_placer())
            .await
            .unwrap();
        let (broadcaster, mut receiver) = test_broadcaster(pool.clone());
        let harness = ReactorHarness::new(broadcaster);

        let now = chrono::Utc::now();
        let id = crate::offchain::order::OffchainOrderId::new();

        store
            .send(
                &id,
                OffchainOrderCommand::Place {
                    symbol: Symbol::new("TSLA").unwrap(),
                    shares: st0x_execution::Positive::new(st0x_execution::FractionalShares::new(
                        st0x_float_macro::float!(5),
                    ))
                    .unwrap(),
                    direction: st0x_execution::Direction::Sell,
                    executor: st0x_execution::SupportedExecutor::AlpacaBrokerApi,
                    client_order_id: st0x_execution::ClientOrderId::from_uuid(id.as_uuid()),
                    kind: crate::offchain::order::CounterTradeOrderKind::Market,
                },
            )
            .await
            .unwrap();
        store
            .send(
                &id,
                OffchainOrderCommand::MarkAccepted {
                    executor_order_id: st0x_execution::ExecutorOrderId::new("test"),
                    placed_shares: st0x_execution::Positive::new(
                        st0x_execution::FractionalShares::new(st0x_float_macro::float!(5)),
                    )
                    .unwrap(),
                    submitted_at: now,
                    market_session: st0x_execution::MarketSession::Regular,
                    limit_price: None,
                },
            )
            .await
            .unwrap();
        store
            .send(
                &id,
                OffchainOrderCommand::CompleteFill {
                    price: st0x_finance::Usd::new(st0x_float_macro::float!(245)),
                    filled_at: now,
                },
            )
            .await
            .unwrap();

        let filled = OffchainOrderEvent::Filled {
            price: st0x_finance::Usd::new(st0x_float_macro::float!(245)),
            filled_at: now,
        };

        harness.receive::<OffchainOrder>(id, filled).await.unwrap();

        let msg = receiver.recv().await.expect("should receive fill");

        match msg {
            Statement::TradeFill(trade) => {
                assert!(matches!(trade.venue, TradingVenue::Alpaca));
                assert!(matches!(trade.direction, st0x_dto::Direction::Sell));
                assert_eq!(trade.symbol, Symbol::new("TSLA").unwrap());
            }
            other => panic!("expected TradeFill message, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn position_update_broadcasts_net_position() {
        let pool = setup_test_db().await;
        let (store, _projection) = StoreBuilder::<Position>::new(pool.clone())
            .build(())
            .await
            .unwrap();
        let (broadcaster, mut receiver) = test_broadcaster(pool.clone());
        let harness = ReactorHarness::new(broadcaster);

        let symbol = Symbol::new("AAPL").unwrap();
        let now = chrono::Utc::now();
        let threshold = st0x_config::ExecutionThreshold::Shares(
            st0x_execution::Positive::new(st0x_execution::FractionalShares::new(
                st0x_float_macro::float!(1),
            ))
            .unwrap(),
        );

        store
            .send(
                &symbol,
                PositionCommand::AcknowledgeOnChainFill {
                    symbol: symbol.clone(),
                    threshold,
                    trade_id: TradeId {
                        tx_hash: alloy::primitives::TxHash::ZERO,
                        log_index: 0,
                    },
                    amount: st0x_execution::FractionalShares::new(st0x_float_macro::float!(1)),
                    direction: st0x_execution::Direction::Buy,
                    price_usdc: st0x_float_macro::float!(150),
                    block_timestamp: now,
                },
            )
            .await
            .unwrap();

        harness
            .receive::<Position>(
                symbol.clone(),
                PositionEvent::OnChainOrderFilled {
                    trade_id: TradeId {
                        tx_hash: alloy::primitives::TxHash::ZERO,
                        log_index: 0,
                    },
                    amount: st0x_execution::FractionalShares::new(st0x_float_macro::float!(1)),
                    direction: st0x_execution::Direction::Buy,
                    price_usdc: st0x_float_macro::float!(150),
                    block_timestamp: now,
                    seen_at: now,
                },
            )
            .await
            .unwrap();

        let msg = receiver
            .recv()
            .await
            .expect("should receive position update");

        match msg {
            Statement::PositionUpdate(position) => {
                assert_eq!(position.symbol, symbol);
                assert!(
                    position.net.eq(st0x_float_macro::float!(1)).unwrap(),
                    "expected net 1, got {:?}",
                    position.net
                );
            }
            other => panic!("expected PositionUpdate message, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn manual_position_adjustment_broadcasts_adjusted_net() {
        let pool = setup_test_db().await;
        let (store, _projection) = StoreBuilder::<Position>::new(pool.clone())
            .build(())
            .await
            .unwrap();
        let (broadcaster, mut receiver) = test_broadcaster(pool.clone());
        let harness = ReactorHarness::new(broadcaster);

        let symbol = Symbol::new("AAPL").unwrap();
        let now = chrono::Utc::now();
        let threshold = crate::ExecutionThreshold::Shares(
            st0x_execution::Positive::new(st0x_execution::FractionalShares::new(
                st0x_float_macro::float!(1),
            ))
            .unwrap(),
        );

        store
            .send(
                &symbol,
                PositionCommand::AcknowledgeOnChainFill {
                    symbol: symbol.clone(),
                    threshold,
                    trade_id: TradeId {
                        tx_hash: alloy::primitives::TxHash::ZERO,
                        log_index: 0,
                    },
                    amount: st0x_execution::FractionalShares::new(st0x_float_macro::float!(1)),
                    direction: st0x_execution::Direction::Buy,
                    price_usdc: st0x_float_macro::float!(150),
                    block_timestamp: now,
                },
            )
            .await
            .unwrap();

        store
            .send(
                &symbol,
                PositionCommand::ManuallyAdjustPosition {
                    symbol: symbol.clone(),
                    target_net: st0x_execution::FractionalShares::new(st0x_float_macro::float!(-3)),
                    reason: "operator repair".to_string(),
                    threshold,
                    expected_net: Some(st0x_execution::FractionalShares::new(
                        st0x_float_macro::float!(1),
                    )),
                    price_usdc: None,
                },
            )
            .await
            .unwrap();

        harness
            .receive::<Position>(
                symbol.clone(),
                PositionEvent::ManualPositionAdjusted {
                    previous_net: st0x_execution::FractionalShares::new(st0x_float_macro::float!(
                        1
                    )),
                    target_net: st0x_execution::FractionalShares::new(st0x_float_macro::float!(-3)),
                    reason: "operator repair".to_string(),
                    price_usdc: None,
                    adjusted_at: now,
                },
            )
            .await
            .unwrap();

        let msg = receiver
            .recv()
            .await
            .expect("should receive position update");

        match msg {
            Statement::PositionUpdate(position) => {
                assert_eq!(position.symbol, symbol);
                assert!(
                    position.net.eq(st0x_float_macro::float!(-3)).unwrap(),
                    "expected adjusted net -3, got {:?}",
                    position.net
                );
            }
            other => panic!("expected PositionUpdate message, got {other:?}"),
        }
    }
}

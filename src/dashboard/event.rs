//! Reactor that broadcasts aggregate events to WebSocket dashboard
//! clients as [`Statement`] notifications and [`Trade`] fills.

use async_trait::async_trait;
use sqlx::SqlitePool;
use tokio::sync::broadcast;
use tracing::warn;

use st0x_dto::{Concern, ServerMessage, Statement, Trade, TradeDirection, TradingVenue};
use st0x_event_sorcery::{EntityList, EventSourced, Never, Reactor, deps, load_entity};

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
    sender: broadcast::Sender<ServerMessage>,
    pool: SqlitePool,
}

impl Broadcaster {
    pub(crate) fn new(sender: broadcast::Sender<ServerMessage>, pool: SqlitePool) -> Self {
        Self { sender, pool }
    }

    fn notify<Entity: EventSourced>(&self, id: &Entity::Id, concern: Concern) {
        let msg = ServerMessage::Statement(Statement {
            id: id.to_string(),
            statement: concern,
        });

        if let Err(error) = self.sender.send(msg) {
            warn!("Failed to broadcast statement (no receivers): {error}");
        }
    }

    fn broadcast_fill(&self, trade: Trade) {
        if let Err(error) = self.sender.send(ServerMessage::Fill(trade)) {
            warn!("Failed to broadcast trade fill (no receivers): {error}");
        }
    }

    fn broadcast_transfer(&self, transfer: st0x_dto::TransferOperation) {
        if let Err(error) = self.sender.send(ServerMessage::Transfer(transfer)) {
            warn!("Failed to broadcast transfer update (no receivers): {error}");
        }
    }
}

/// Convert a [`SupportedExecutor`] to a [`TradingVenue`] for the dashboard.
pub(crate) fn executor_to_venue(executor: st0x_execution::SupportedExecutor) -> TradingVenue {
    use st0x_execution::SupportedExecutor;

    match executor {
        SupportedExecutor::Schwab => TradingVenue::Schwab,
        SupportedExecutor::AlpacaTradingApi | SupportedExecutor::AlpacaBrokerApi => {
            TradingVenue::Alpaca
        }
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
                        filled_at: *filled_at,
                        venue: TradingVenue::Raindex,
                        direction: direction_to_dto(*direction),
                        symbol: symbol.clone(),
                        shares: st0x_finance::FractionalShares::new(*amount),
                    });
                }

                self.notify::<OnChainTrade>(&id, Concern::Trading);
            })
            .on(|id, _event| async move {
                self.notify::<Position>(&id, Concern::Trading);
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

                self.notify::<OffchainOrder>(&id, Concern::Trading);
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
    use alloy::primitives::Address;
    use rain_math_float::Float;
    use uuid::Uuid;

    use st0x_event_sorcery::ReactorHarness;
    use st0x_execution::Symbol;

    use super::*;
    use crate::equity_redemption::{EquityRedemptionEvent, RedemptionAggregateId};
    use crate::test_utils::setup_test_db;
    use crate::tokenized_equity_mint::{IssuerRequestId, TokenizedEquityMintEvent};
    use crate::usdc_rebalance::{UsdcRebalanceEvent, UsdcRebalanceId};

    fn test_broadcaster(pool: SqlitePool) -> (Broadcaster, broadcast::Receiver<ServerMessage>) {
        let (sender, receiver) = broadcast::channel(16);
        let broadcaster = Broadcaster::new(sender, pool);
        (broadcaster, receiver)
    }

    fn make_mint_requested(symbol: &str) -> TokenizedEquityMintEvent {
        TokenizedEquityMintEvent::MintRequested {
            symbol: Symbol::new(symbol).unwrap(),
            quantity: Float::parse("100".to_string()).unwrap(),
            wallet: Address::ZERO,
            requested_at: chrono::Utc::now(),
        }
    }

    fn make_redemption_completed() -> EquityRedemptionEvent {
        EquityRedemptionEvent::Completed {
            completed_at: chrono::Utc::now(),
        }
    }

    fn make_usdc_withdrawal_confirmed() -> UsdcRebalanceEvent {
        UsdcRebalanceEvent::WithdrawalConfirmed {
            confirmed_at: chrono::Utc::now(),
        }
    }

    #[tokio::test]
    async fn broadcaster_sends_transfer_statement() {
        let pool = setup_test_db().await;
        let (broadcaster, mut receiver) = test_broadcaster(pool);

        let id = IssuerRequestId::new("mint-123".to_string());
        broadcaster.notify::<TokenizedEquityMint>(&id, Concern::Transfer);

        let msg = receiver.recv().await.expect("should receive message");

        match msg {
            ServerMessage::Statement(statement) => {
                assert_eq!(statement.id, "mint-123");
                assert!(matches!(statement.statement, Concern::Transfer));
            }
            other => panic!("expected Statement message, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn broadcaster_handles_no_receivers() {
        let pool = setup_test_db().await;
        let (sender, _) = broadcast::channel::<ServerMessage>(16);
        let broadcaster = Broadcaster::new(sender, pool);

        let id = IssuerRequestId::new("mint-456".to_string());
        broadcaster.notify::<TokenizedEquityMint>(&id, Concern::Transfer);
    }

    #[tokio::test]
    async fn reactor_broadcasts_mint_statement() {
        let pool = setup_test_db().await;
        let (broadcaster, mut receiver) = test_broadcaster(pool);
        let harness = ReactorHarness::new(broadcaster);

        let id = IssuerRequestId::new("mint-multi".to_string());

        harness
            .receive::<TokenizedEquityMint>(id, make_mint_requested("NVDA"))
            .await
            .unwrap();

        let msg = receiver.recv().await.expect("should receive message");

        match msg {
            ServerMessage::Statement(statement) => {
                assert_eq!(statement.id, "mint-multi");
                assert!(matches!(statement.statement, Concern::Transfer));
            }
            other => panic!("expected Statement message, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn reactor_broadcasts_equity_redemption() {
        let pool = setup_test_db().await;
        let (broadcaster, mut receiver) = test_broadcaster(pool);
        let harness = ReactorHarness::new(broadcaster);

        let id = RedemptionAggregateId::new("redemption-123".to_string());

        harness
            .receive::<EquityRedemption>(id, make_redemption_completed())
            .await
            .unwrap();

        let msg = receiver.recv().await.expect("should receive message");

        match msg {
            ServerMessage::Statement(statement) => {
                assert_eq!(statement.id, "redemption-123");
                assert!(matches!(statement.statement, Concern::Transfer));
            }
            other => panic!("expected Statement message, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn reactor_broadcasts_usdc_rebalance() {
        let pool = setup_test_db().await;
        let (broadcaster, mut receiver) = test_broadcaster(pool);
        let harness = ReactorHarness::new(broadcaster);

        let id = UsdcRebalanceId(Uuid::new_v4());
        let expected_id = id.to_string();

        harness
            .receive::<UsdcRebalance>(id, make_usdc_withdrawal_confirmed())
            .await
            .unwrap();

        let msg = receiver.recv().await.expect("should receive message");

        match msg {
            ServerMessage::Statement(statement) => {
                assert_eq!(statement.id, expected_id);
                assert!(matches!(statement.statement, Concern::Transfer));
            }
            other => panic!("expected Statement message, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn multiple_subscribers_receive_same_statement() {
        let pool = setup_test_db().await;
        let (sender, mut receiver1) = broadcast::channel(16);
        let mut receiver2 = sender.subscribe();
        let broadcaster = Broadcaster::new(sender, pool);

        let id = IssuerRequestId::new("multi-sub".to_string());
        broadcaster.notify::<TokenizedEquityMint>(&id, Concern::Transfer);

        let msg1 = receiver1
            .recv()
            .await
            .expect("receiver1 should get message");
        let msg2 = receiver2
            .recv()
            .await
            .expect("receiver2 should get message");

        for (idx, msg) in [msg1, msg2].into_iter().enumerate() {
            match msg {
                ServerMessage::Statement(statement) => {
                    assert_eq!(
                        statement.id,
                        "multi-sub",
                        "receiver {} got wrong id",
                        idx + 1
                    );
                }
                other => {
                    panic!("receiver {} expected Statement, got {other:?}", idx + 1);
                }
            }
        }
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
            ServerMessage::Fill(trade) => {
                assert!(matches!(trade.venue, TradingVenue::Raindex));
                assert!(matches!(trade.direction, TradeDirection::Buy));
                assert_eq!(trade.symbol, Symbol::new("AAPL").unwrap());
            }
            other => panic!("expected Fill message, got {other:?}"),
        }

        // Should also get a Statement for Trading concern
        let msg2 = receiver.recv().await.expect("should receive statement");
        assert!(matches!(msg2, ServerMessage::Statement(_)));
    }
}

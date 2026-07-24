//! Loads terminal trade outcomes from the event store for dashboard history.

use futures_util::{StreamExt, TryStreamExt, stream};
use sqlx::SqlitePool;

use st0x_dto::{Trade, sort_trades_newest_first};
use st0x_event_sorcery::{LoadAllIdsError, SendError, load_all_ids, load_entity};

use crate::offchain::order::{OffchainOrder, TradeConversionError};
use crate::onchain_trade::OnChainTrade;

const MAX_TRADES: usize = 100;

/// Load recent filled trades from both onchain and offchain sources.
///
/// Returns up to [`MAX_TRADES`] trades sorted by fill time (newest first).
pub(crate) async fn load_trades(pool: &SqlitePool) -> Result<Vec<Trade>, TradeHistoryError> {
    let mut trades: Vec<Trade> = load_onchain_trades(pool)
        .await?
        .into_iter()
        .chain(load_offchain_trades(pool).await?)
        .collect();

    sort_trades_newest_first(&mut trades);
    trades.truncate(MAX_TRADES);

    Ok(trades)
}

async fn load_onchain_trades(pool: &SqlitePool) -> Result<Vec<Trade>, TradeHistoryError> {
    let ids = load_all_ids::<OnChainTrade>(pool)
        .await
        .map_err(TradeHistoryError::OnchainIds)?;

    stream::iter(ids)
        .then(|id| async move {
            let entity = load_entity::<OnChainTrade>(pool, &id)
                .await
                .map_err(|source| TradeHistoryError::OnchainReplay {
                    id: id.to_string(),
                    source,
                })?
                .ok_or_else(|| TradeHistoryError::OnchainMissing { id: id.to_string() })?;

            Ok(entity.into_trade(&id))
        })
        .try_collect()
        .await
}

async fn load_offchain_trades(pool: &SqlitePool) -> Result<Vec<Trade>, TradeHistoryError> {
    let ids = load_all_ids::<OffchainOrder>(pool)
        .await
        .map_err(TradeHistoryError::OffchainIds)?;

    stream::iter(ids)
        .then(|id| async move {
            let order = load_entity::<OffchainOrder>(pool, &id)
                .await
                .map_err(|source| TradeHistoryError::OffchainReplay {
                    id: id.to_string(),
                    source,
                })?
                .ok_or_else(|| TradeHistoryError::OffchainMissing { id: id.to_string() })?;

            match order.try_into_trade(&id) {
                Ok(trade) => Ok(Some(trade)),
                Err(
                    TradeConversionError::Pending
                    | TradeConversionError::Submitted
                    | TradeConversionError::PartiallyFilled
                    | TradeConversionError::Cancelling
                    | TradeConversionError::Cancelled,
                ) => Ok(None),
                Err(source) => Err(TradeHistoryError::OffchainConversion {
                    id: id.to_string(),
                    source,
                }),
            }
        })
        .try_filter_map(|trade| async move { Ok(trade) })
        .try_collect()
        .await
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum TradeHistoryError {
    #[error("failed to load onchain trade IDs: {0}")]
    OnchainIds(#[source] LoadAllIdsError),
    #[error("failed to load offchain trade IDs: {0}")]
    OffchainIds(#[source] LoadAllIdsError),
    #[error("failed to replay onchain trade {id}: {source}")]
    OnchainReplay {
        id: String,
        #[source]
        source: SendError<OnChainTrade>,
    },
    #[error("onchain trade {id} replayed to empty state")]
    OnchainMissing { id: String },
    #[error("failed to replay offchain trade {id}: {source}")]
    OffchainReplay {
        id: String,
        #[source]
        source: SendError<OffchainOrder>,
    },
    #[error("offchain trade {id} replayed to empty state")]
    OffchainMissing { id: String },
    #[error("offchain trade {id} cannot be represented in history: {source}")]
    OffchainConversion {
        id: String,
        #[source]
        source: TradeConversionError,
    },
}

#[cfg(test)]
mod tests {
    use chrono::Utc;

    use st0x_execution::{ClientOrderId, Direction, FractionalShares, Positive, Symbol};
    use st0x_float_macro::float;

    use super::*;
    use crate::offchain::order::{OffchainOrderCommand, OffchainOrderId};
    use crate::onchain_trade::{OnChainTradeCommand, OnChainTradeId};
    use crate::test_utils::setup_test_db;

    #[tokio::test]
    async fn load_trades_empty_database() {
        let pool = setup_test_db().await;
        let trades = load_trades(&pool).await.unwrap();
        assert!(trades.is_empty());
    }

    #[tokio::test]
    async fn load_trades_includes_onchain_fills() {
        let pool = setup_test_db().await;
        let now = Utc::now();

        let id = OnChainTradeId {
            tx_hash: alloy::primitives::TxHash::ZERO,
            log_index: 0,
        };

        let store = st0x_event_sorcery::StoreBuilder::<OnChainTrade>::new(pool.clone())
            .build(())
            .await
            .unwrap();

        store
            .send(
                &id,
                OnChainTradeCommand::Witness {
                    symbol: Symbol::new("AAPL").unwrap(),
                    amount: float!(10),
                    direction: Direction::Buy,
                    price_usdc: float!(150),
                    block_number: 12345,
                    block_timestamp: now,
                },
            )
            .await
            .unwrap();

        let trades = load_trades(&pool).await.unwrap();
        assert_eq!(trades.len(), 1);
        assert_eq!(trades[0].symbol, Symbol::new("AAPL").unwrap());
        assert!(matches!(trades[0].venue, st0x_dto::TradingVenue::Raindex));
        assert!(matches!(trades[0].direction, st0x_dto::Direction::Buy));
    }

    #[tokio::test]
    async fn load_trades_includes_offchain_fills() {
        let pool = setup_test_db().await;

        let (store, _projection) =
            st0x_event_sorcery::StoreBuilder::<OffchainOrder>::new(pool.clone())
                .build(crate::offchain::order::noop_order_placer())
                .await
                .unwrap();

        let id = OffchainOrderId::new();

        store
            .send(
                &id,
                OffchainOrderCommand::Place {
                    symbol: Symbol::new("TSLA").unwrap(),
                    shares: Positive::new(FractionalShares::new(float!(50))).unwrap(),
                    direction: Direction::Sell,
                    executor: st0x_execution::SupportedExecutor::AlpacaBrokerApi,
                    client_order_id: ClientOrderId::from_uuid(id.as_uuid()),
                    kind: crate::offchain::order::CounterTradeOrderKind::Market,
                },
            )
            .await
            .unwrap();

        store
            .send(
                &id,
                OffchainOrderCommand::MarkAccepted {
                    executor_order_id: st0x_execution::ExecutorOrderId::new("TEST"),
                    placed_shares: Positive::new(FractionalShares::new(float!(50))).unwrap(),
                    submitted_at: Utc::now(),
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
                    price: st0x_finance::Usd::new(float!(200)),
                    filled_at: Utc::now(),
                },
            )
            .await
            .unwrap();

        let trades = load_trades(&pool).await.unwrap();
        assert_eq!(trades.len(), 1);
        assert_eq!(trades[0].symbol, Symbol::new("TSLA").unwrap());
        assert!(matches!(trades[0].venue, st0x_dto::TradingVenue::Alpaca));
        assert!(matches!(trades[0].direction, st0x_dto::Direction::Sell));
    }

    #[tokio::test]
    async fn load_trades_sorted_newest_first() {
        let pool = setup_test_db().await;

        let store = st0x_event_sorcery::StoreBuilder::<OnChainTrade>::new(pool.clone())
            .build(())
            .await
            .unwrap();

        let now = Utc::now();

        let older_id = OnChainTradeId {
            tx_hash: alloy::primitives::TxHash::ZERO,
            log_index: 0,
        };
        let newer_id = OnChainTradeId {
            tx_hash: alloy::primitives::TxHash::ZERO,
            log_index: 1,
        };
        let tied_id = OnChainTradeId {
            tx_hash: alloy::primitives::TxHash::ZERO,
            log_index: 2,
        };

        store
            .send(
                &older_id,
                OnChainTradeCommand::WitnessAt {
                    symbol: Symbol::new("AAPL").unwrap(),
                    amount: float!(10),
                    direction: Direction::Buy,
                    price_usdc: float!(150),
                    block_number: 1,
                    block_timestamp: now - chrono::Duration::seconds(1),
                    filled_at: now,
                },
            )
            .await
            .unwrap();

        store
            .send(
                &newer_id,
                OnChainTradeCommand::WitnessAt {
                    symbol: Symbol::new("TSLA").unwrap(),
                    amount: float!(5),
                    direction: Direction::Sell,
                    price_usdc: float!(200),
                    block_number: 2,
                    block_timestamp: now,
                    filled_at: now,
                },
            )
            .await
            .unwrap();

        store
            .send(
                &tied_id,
                OnChainTradeCommand::WitnessAt {
                    symbol: Symbol::new("NVDA").unwrap(),
                    amount: float!(3),
                    direction: Direction::Buy,
                    price_usdc: float!(300),
                    block_number: 2,
                    block_timestamp: now,
                    filled_at: now,
                },
            )
            .await
            .unwrap();

        let trades = load_trades(&pool).await.unwrap();
        assert_eq!(trades.len(), 3);
        assert_eq!(trades[0].id, newer_id.to_string());
        assert_eq!(trades[1].id, tied_id.to_string());
        assert_eq!(trades[2].id, older_id.to_string());
    }

    #[tokio::test]
    async fn load_trades_capped_at_max() {
        let pool = setup_test_db().await;

        let store = st0x_event_sorcery::StoreBuilder::<OnChainTrade>::new(pool.clone())
            .build(())
            .await
            .unwrap();

        let now = Utc::now();

        for log_index in 0..=(MAX_TRADES as u64) {
            let id = OnChainTradeId {
                tx_hash: alloy::primitives::TxHash::ZERO,
                log_index,
            };
            store
                .send(
                    &id,
                    OnChainTradeCommand::Witness {
                        symbol: Symbol::new("AAPL").unwrap(),
                        amount: float!(1),
                        direction: Direction::Buy,
                        price_usdc: float!(100),
                        block_number: log_index,
                        block_timestamp: now,
                    },
                )
                .await
                .unwrap();
        }

        let trades = load_trades(&pool).await.unwrap();
        assert_eq!(trades.len(), MAX_TRADES, "should be capped at {MAX_TRADES}");
    }

    #[tokio::test]
    async fn load_trades_includes_failed_offchain_orders() {
        let pool = setup_test_db().await;

        let (store, _projection) =
            st0x_event_sorcery::StoreBuilder::<OffchainOrder>::new(pool.clone())
                .build(crate::offchain::order::noop_order_placer())
                .await
                .unwrap();

        let id = OffchainOrderId::new();

        store
            .send(
                &id,
                OffchainOrderCommand::Place {
                    symbol: Symbol::new("NVDA").unwrap(),
                    shares: Positive::new(FractionalShares::new(float!(10))).unwrap(),
                    direction: Direction::Buy,
                    executor: st0x_execution::SupportedExecutor::AlpacaBrokerApi,
                    client_order_id: ClientOrderId::from_uuid(id.as_uuid()),
                    kind: crate::offchain::order::CounterTradeOrderKind::Market,
                },
            )
            .await
            .unwrap();

        store
            .send(
                &id,
                OffchainOrderCommand::MarkPlacementFailed {
                    error: "asset is not tradable".to_string(),
                },
            )
            .await
            .unwrap();

        let trades = load_trades(&pool).await.unwrap();
        assert_eq!(trades.len(), 1, "failed orders should appear");
        assert!(matches!(
            &trades[0].outcome,
            st0x_dto::TradeOutcome::Failed {
                error,
                filled_shares,
                remaining_shares,
                excess_shares,
            } if error == "asset is not tradable"
                && filled_shares.inner().inner().eq(float!(0)).unwrap()
                && remaining_shares.inner().inner().eq(float!(10)).unwrap()
                && excess_shares.inner().inner().is_zero().unwrap()
        ));
    }

    #[tokio::test]
    async fn load_trades_excludes_pending_offchain_orders() {
        let pool = setup_test_db().await;
        let (store, _projection) =
            st0x_event_sorcery::StoreBuilder::<OffchainOrder>::new(pool.clone())
                .build(crate::offchain::order::noop_order_placer())
                .await
                .unwrap();
        let id = OffchainOrderId::new();

        store
            .send(
                &id,
                OffchainOrderCommand::Place {
                    symbol: Symbol::new("NVDA").unwrap(),
                    shares: Positive::new(FractionalShares::new(float!(10))).unwrap(),
                    direction: Direction::Buy,
                    executor: st0x_execution::SupportedExecutor::AlpacaBrokerApi,
                    client_order_id: ClientOrderId::from_uuid(id.as_uuid()),
                    kind: crate::offchain::order::CounterTradeOrderKind::Market,
                },
            )
            .await
            .unwrap();

        let trades = load_trades(&pool).await.unwrap();
        assert!(trades.is_empty(), "pending orders should not appear");
    }
}

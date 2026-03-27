//! Loads filled trades from the event store for the initial dashboard state.

use sqlx::SqlitePool;
use tracing::warn;

use st0x_dto::Trade;
use st0x_event_sorcery::{load_all_ids, load_entity};

use crate::offchain_order::OffchainOrder;
use crate::onchain_trade::OnChainTrade;

use super::event::{direction_to_dto, executor_to_venue};

const MAX_TRADES: usize = 100;

/// Load recent filled trades from both onchain and offchain sources.
///
/// Returns up to [`MAX_TRADES`] trades sorted by fill time (newest first).
pub(crate) async fn load_trades(pool: &SqlitePool) -> Vec<Trade> {
    let mut trades = Vec::new();

    trades.extend(load_onchain_trades(pool).await);
    trades.extend(load_offchain_trades(pool).await);

    trades.sort_by(|lhs, rhs| rhs.filled_at.cmp(&lhs.filled_at));
    trades.truncate(MAX_TRADES);

    trades
}

async fn load_onchain_trades(pool: &SqlitePool) -> Vec<Trade> {
    let ids = match load_all_ids::<OnChainTrade>(pool).await {
        Ok(ids) => ids,
        Err(error) => {
            warn!(?error, "Failed to load OnChainTrade IDs for trade history");
            return Vec::new();
        }
    };

    let mut trades = Vec::with_capacity(ids.len());

    for id in &ids {
        match load_entity::<OnChainTrade>(pool, id).await {
            Ok(Some(entity)) => {
                trades.push(Trade {
                    filled_at: entity.filled_at,
                    venue: st0x_dto::TradingVenue::Raindex,
                    direction: direction_to_dto(entity.direction),
                    symbol: entity.symbol,
                    shares: st0x_finance::FractionalShares::new(entity.amount),
                });
            }
            Ok(None) => {
                warn!(?id, "OnChainTrade replayed to empty state");
            }
            Err(error) => {
                warn!(?error, ?id, "Failed to load OnChainTrade");
            }
        }
    }

    trades
}

async fn load_offchain_trades(pool: &SqlitePool) -> Vec<Trade> {
    let ids = match load_all_ids::<OffchainOrder>(pool).await {
        Ok(ids) => ids,
        Err(error) => {
            warn!(?error, "Failed to load OffchainOrder IDs for trade history");
            return Vec::new();
        }
    };

    let mut trades = Vec::new();

    for id in &ids {
        match load_entity::<OffchainOrder>(pool, id).await {
            Ok(Some(OffchainOrder::Filled {
                symbol,
                shares,
                direction,
                executor,
                filled_at,
                ..
            })) => {
                trades.push(Trade {
                    filled_at,
                    venue: executor_to_venue(executor),
                    direction: direction_to_dto(direction),
                    symbol,
                    shares: st0x_finance::FractionalShares::new(shares.inner().inner()),
                });
            }

            Ok(Some(_)) => {}
            Ok(None) => {
                warn!(?id, "OffchainOrder replayed to empty state");
            }
            Err(error) => {
                warn!(?error, ?id, "Failed to load OffchainOrder");
            }
        }
    }

    trades
}

#[cfg(test)]
mod tests {
    use chrono::Utc;

    use st0x_execution::{Direction, FractionalShares, Positive, Symbol};
    use st0x_float_macro::float;

    use super::*;
    use crate::offchain_order::{OffchainOrderCommand, OffchainOrderId, noop_order_placer};
    use crate::onchain_trade::{OnChainTradeCommand, OnChainTradeId};
    use crate::test_utils::setup_test_db;

    #[tokio::test]
    async fn load_trades_empty_database() {
        let pool = setup_test_db().await;
        let trades = load_trades(&pool).await;
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

        let trades = load_trades(&pool).await;
        assert_eq!(trades.len(), 1);
        assert_eq!(trades[0].symbol, Symbol::new("AAPL").unwrap());
        assert!(matches!(trades[0].venue, st0x_dto::TradingVenue::Raindex));
        assert!(matches!(trades[0].direction, st0x_dto::TradeDirection::Buy));
    }

    #[tokio::test]
    async fn load_trades_includes_offchain_fills() {
        let pool = setup_test_db().await;
        let order_placer = noop_order_placer();

        let (store, _projection) = st0x_event_sorcery::StoreBuilder::<OffchainOrder>::new(pool.clone())
            .build(order_placer.clone())
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
                    executor: st0x_execution::SupportedExecutor::AlpacaTradingApi,
                },
            )
            .await
            .unwrap();

        store
            .send(
                &id,
                OffchainOrderCommand::CompleteFill {
                    price: st0x_finance::Usd::new(float!(200)),
                },
            )
            .await
            .unwrap();

        let trades = load_trades(&pool).await;
        assert_eq!(trades.len(), 1);
        assert_eq!(trades[0].symbol, Symbol::new("TSLA").unwrap());
        assert!(matches!(trades[0].venue, st0x_dto::TradingVenue::Alpaca));
        assert!(matches!(
            trades[0].direction,
            st0x_dto::TradeDirection::Sell
        ));
    }

    #[tokio::test]
    async fn load_trades_excludes_unfilled_offchain_orders() {
        let pool = setup_test_db().await;
        let order_placer = noop_order_placer();

        let (store, _projection) = st0x_event_sorcery::StoreBuilder::<OffchainOrder>::new(pool.clone())
            .build(order_placer)
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
                    executor: st0x_execution::SupportedExecutor::AlpacaTradingApi,
                },
            )
            .await
            .unwrap();

        let trades = load_trades(&pool).await;
        assert!(trades.is_empty(), "unfilled orders should not appear");
    }
}

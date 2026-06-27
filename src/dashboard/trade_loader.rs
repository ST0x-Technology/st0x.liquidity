//! Loads filled trades from the event store for the initial dashboard state.

use futures_util::{StreamExt, stream};
use sqlx::SqlitePool;
use tracing::warn;

use st0x_dto::Trade;
use st0x_event_sorcery::{load_all_ids, load_entity};

use crate::offchain::order::OffchainOrder;
use crate::onchain_trade::OnChainTrade;

const MAX_TRADES: usize = 100;

/// Load recent filled trades from both onchain and offchain sources.
///
/// Returns up to [`MAX_TRADES`] trades sorted by fill time (newest first).
pub(crate) async fn load_trades(pool: &SqlitePool) -> Vec<Trade> {
    let mut trades: Vec<Trade> = load_onchain_trades(pool)
        .await
        .into_iter()
        .chain(load_offchain_trades(pool).await)
        .collect();

    trades.sort_by(|lhs, rhs| rhs.filled_at.cmp(&lhs.filled_at));
    trades.truncate(MAX_TRADES);

    trades
}

async fn load_onchain_trades(pool: &SqlitePool) -> Vec<Trade> {
    let ids = load_all_ids::<OnChainTrade>(pool)
        .await
        .inspect_err(|error| warn!(?error, "Failed to load OnChainTrade IDs for trade history"))
        .unwrap_or_default();

    stream::iter(ids)
        .filter_map(|id| async move {
            match load_entity::<OnChainTrade>(pool, &id).await {
                Ok(Some(entity)) => Some(entity.to_trade(&id)),
                Ok(None) => {
                    warn!(?id, "OnChainTrade replayed to empty state");
                    None
                }
                Err(error) => {
                    warn!(?error, ?id, "Failed to load OnChainTrade");
                    None
                }
            }
        })
        .collect()
        .await
}

async fn load_offchain_trades(pool: &SqlitePool) -> Vec<Trade> {
    let ids = load_all_ids::<OffchainOrder>(pool)
        .await
        .inspect_err(|error| warn!(?error, "Failed to load OffchainOrder IDs for trade history"))
        .unwrap_or_default();

    stream::iter(ids)
        .filter_map(|id| async move {
            match load_entity::<OffchainOrder>(pool, &id).await {
                Ok(Some(order)) => order.try_to_trade(&id).ok(),
                Ok(None) => {
                    warn!(?id, "OffchainOrder replayed to empty state");
                    None
                }
                Err(error) => {
                    warn!(?error, ?id, "Failed to load OffchainOrder");
                    None
                }
            }
        })
        .collect()
        .await
}

#[cfg(test)]
mod tests {
    use chrono::Utc;

    use st0x_execution::{ClientOrderId, Direction, FractionalShares, Positive, Symbol};
    use st0x_float_macro::float;

    use super::*;
    use crate::offchain::order::{OffchainOrderCommand, OffchainOrderId, noop_order_placer};
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
        assert!(matches!(trades[0].direction, st0x_dto::Direction::Buy));
    }

    #[tokio::test]
    async fn load_trades_includes_offchain_fills() {
        let pool = setup_test_db().await;
        let order_placer = noop_order_placer();

        let (store, _projection) =
            st0x_event_sorcery::StoreBuilder::<OffchainOrder>::new(pool.clone())
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
                    executor: st0x_execution::SupportedExecutor::AlpacaBrokerApi,
                    client_order_id: ClientOrderId::from_uuid(id.as_uuid()),
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

        store
            .send(
                &older_id,
                OnChainTradeCommand::Witness {
                    symbol: Symbol::new("AAPL").unwrap(),
                    amount: float!(10),
                    direction: Direction::Buy,
                    price_usdc: float!(150),
                    block_number: 1,
                    block_timestamp: now,
                },
            )
            .await
            .unwrap();

        store
            .send(
                &newer_id,
                OnChainTradeCommand::Witness {
                    symbol: Symbol::new("TSLA").unwrap(),
                    amount: float!(5),
                    direction: Direction::Sell,
                    price_usdc: float!(200),
                    block_number: 2,
                    block_timestamp: now,
                },
            )
            .await
            .unwrap();

        let trades = load_trades(&pool).await;
        assert_eq!(trades.len(), 2);
        assert!(
            trades[0].filled_at >= trades[1].filled_at,
            "trades should be sorted newest first: {:?} >= {:?}",
            trades[0].filled_at,
            trades[1].filled_at
        );
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

        let trades = load_trades(&pool).await;
        assert_eq!(trades.len(), MAX_TRADES, "should be capped at {MAX_TRADES}");
    }

    #[tokio::test]
    async fn load_trades_excludes_unfilled_offchain_orders() {
        let pool = setup_test_db().await;
        let order_placer = noop_order_placer();

        let (store, _projection) =
            st0x_event_sorcery::StoreBuilder::<OffchainOrder>::new(pool.clone())
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
                    executor: st0x_execution::SupportedExecutor::AlpacaBrokerApi,
                    client_order_id: ClientOrderId::from_uuid(id.as_uuid()),
                },
            )
            .await
            .unwrap();

        let trades = load_trades(&pool).await;
        assert!(trades.is_empty(), "unfilled orders should not appear");
    }
}

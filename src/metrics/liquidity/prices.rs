//! The `Prices` family: the live price of each symbol the bot holds a
//! position in, and the position's dollar exposure at that price.
//!
//! Refreshed every 60 seconds by `liq-state-refresh`, like the exporter. A
//! symbol with a price but no position, or a position without a live price,
//! publishes nothing for the missing half.

use std::collections::HashMap;

use rain_math_float::Float;
use sqlx::SqlitePool;
use tracing::warn;

use st0x_event_sorcery::{LoadAllIdsError, SendError, load_all_ids, load_entity};
use st0x_finance::Symbol;

use super::{LiqMetric, LiqSample, float_value, push_sample, strip_prefix};
use crate::position::Position;

/// One position as the prices builder reads it, owned by the metrics so it
/// does not depend on the dashboard DTO.
#[derive(Debug, Clone)]
pub(crate) struct PositionInput {
    pub(crate) symbol: Symbol,
    pub(crate) net: Float,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum PositionLoadError {
    #[error("failed to list positions")]
    Ids(#[from] LoadAllIdsError),
    #[error("failed to load position {id}")]
    Position {
        id: Symbol,
        #[source]
        source: Box<SendError<Position>>,
    },
}

/// Every position, or an error. The dashboard's loader turns errors into a
/// shorter list, which would drop exposure series; here a failure keeps the
/// last published family instead.
pub(crate) async fn load_positions(
    pool: &SqlitePool,
) -> Result<Vec<PositionInput>, PositionLoadError> {
    let mut positions = Vec::new();

    for id in load_all_ids::<Position>(pool).await? {
        match load_entity::<Position>(pool, &id).await {
            Ok(Some(position)) => positions.push(PositionInput {
                symbol: position.symbol,
                net: position.net.inner(),
            }),
            // Listed but not loadable as a live aggregate: the dashboard
            // skips it too, so the exporter never saw it.
            Ok(None) => warn!(%id, "Position listed without a live aggregate"),
            Err(source) => {
                return Err(PositionLoadError::Position {
                    id,
                    source: Box::new(source),
                });
            }
        }
    }

    Ok(positions)
}

/// `liq_position_last_price_usd` and `liq_equity_exposure_usd` per position.
/// Positions and prices join on the label symbol, as the exporter joined
/// them, so `tAAPL` prices `AAPL`.
pub(crate) fn price_samples(
    positions: &[PositionInput],
    live_prices: &[(Symbol, Float)],
) -> Vec<LiqSample> {
    let prices: HashMap<&str, Float> = live_prices
        .iter()
        .map(|(symbol, price)| (strip_prefix(symbol.as_str()), *price))
        .collect();

    let mut samples = Vec::new();
    for position in positions {
        let symbol = strip_prefix(position.symbol.as_str());
        let Some(price) = prices.get(symbol).copied() else {
            continue;
        };

        let labels = vec![("symbol", symbol.to_string())];
        push_sample(
            &mut samples,
            LiqMetric::PositionLastPriceUsd,
            labels.clone(),
            float_value(price),
        );
        push_sample(
            &mut samples,
            LiqMetric::EquityExposureUsd,
            labels,
            (position.net * price)
                .map_err(Into::into)
                .and_then(float_value),
        );
    }

    samples
}

#[cfg(test)]
pub(crate) mod tests {
    use std::collections::BTreeMap;

    use alloy::primitives::TxHash;
    use chrono::{DateTime, Utc};

    use st0x_config::ExecutionThreshold;
    use st0x_event_sorcery::StoreBuilder;
    use st0x_evm::Chain;
    use st0x_execution::{Direction, FractionalShares};
    use st0x_float_macro::float;

    use super::*;
    use crate::dashboard::equity_price::EquityPriceStore;
    use crate::metrics::liquidity::LiqFamily;
    use crate::metrics::liquidity::inventory::tests::{
        golden_for, render_family, state_fixture, state_reserved_fixture,
    };
    use crate::metrics::liquidity::tests::series;
    use crate::position::{PositionCommand, TradeId};
    use crate::test_utils::setup_test_db;

    fn position(symbol: &str, net: Float) -> PositionInput {
        PositionInput {
            symbol: Symbol::new(symbol).unwrap(),
            net,
        }
    }

    fn price(symbol: &str, price: Float) -> (Symbol, Float) {
        (Symbol::new(symbol).unwrap(), price)
    }

    /// The prices come from [`EquityPriceStore::live_prices`], the call the
    /// refresh task makes, so a change to its expiry or ordering rule is a
    /// golden change.
    #[tokio::test]
    async fn prices_match_the_exporter_goldens() {
        let now: DateTime<Utc> = "2026-10-08T12:00:00Z".parse().unwrap();

        for (fixture, golden) in [
            (state_fixture(), include_str!("testdata/state.prom")),
            (
                state_reserved_fixture(),
                include_str!("testdata/state-reserved.prom"),
            ),
        ] {
            let positions: Vec<PositionInput> = fixture
                .positions
                .iter()
                .map(|position| PositionInput {
                    symbol: position.symbol.clone(),
                    net: position.net,
                })
                .collect();
            let live = EquityPriceStore::with_prices(&fixture.equity_prices)
                .live_prices(now)
                .await;

            assert_eq!(
                render_family(LiqFamily::Prices, price_samples(&positions, &live)),
                golden_for(LiqFamily::Prices, golden)
            );
        }
    }

    #[test]
    fn only_symbols_with_both_a_position_and_a_live_price_publish() {
        let samples = price_samples(
            &[position("tAAPL", float!(10)), position("TSLA", float!(-3))],
            &[price("AAPL", float!(2.5)), price("tSPYM", float!(7))],
        );

        assert_eq!(
            render_family(LiqFamily::Prices, samples),
            BTreeMap::from([
                (
                    series("liq_position_last_price_usd", &[("symbol", "AAPL")]),
                    2.5
                ),
                (
                    series("liq_equity_exposure_usd", &[("symbol", "AAPL")]),
                    25.0
                ),
            ])
        );
    }

    #[tokio::test]
    async fn load_positions_reads_every_position_aggregate() {
        let pool = setup_test_db().await;
        let (store, _projection) = StoreBuilder::<Position>::new(pool.clone())
            .build(())
            .await
            .unwrap();
        for (name, direction) in [("AAPL", Direction::Buy), ("TSLA", Direction::Sell)] {
            let symbol = Symbol::new(name).unwrap();
            store
                .send(
                    &symbol,
                    PositionCommand::AcknowledgeOnChainFill {
                        symbol: symbol.clone(),
                        threshold: ExecutionThreshold::whole_share(),
                        trade_id: TradeId {
                            chain: Chain::Base,
                            tx_hash: TxHash::random(),
                            log_index: 1,
                        },
                        amount: FractionalShares::new(float!(0.5)),
                        direction,
                        price_usdc: float!(150),
                        block_timestamp: Utc::now(),
                        block_number: None,
                    },
                )
                .await
                .unwrap();
        }

        let mut loaded: Vec<(String, String)> = load_positions(&pool)
            .await
            .unwrap()
            .into_iter()
            .map(|position| (position.symbol.to_string(), position.net.format().unwrap()))
            .collect();
        loaded.sort();

        assert_eq!(
            loaded,
            [
                ("AAPL".to_string(), "0.5".to_string()),
                ("TSLA".to_string(), "-0.5".to_string()),
            ]
        );
    }

    #[tokio::test]
    async fn load_positions_fails_instead_of_returning_a_shorter_list() {
        let pool = setup_test_db().await;
        pool.close().await;

        assert!(matches!(
            load_positions(&pool).await,
            Err(PositionLoadError::Ids(_))
        ));
    }
}

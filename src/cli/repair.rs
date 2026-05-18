//! Repair CLI commands for manually recovering stuck local CQRS state.

use anyhow::{Context, bail};
use sqlx::SqlitePool;
use std::io::Write;

use st0x_event_sorcery::StoreBuilder;
use st0x_execution::Symbol;

use crate::offchain::order::OffchainOrderId;
use crate::position::{Position, PositionCommand};

pub(super) async fn fail_pending_offchain_order_command<W: Write>(
    stdout: &mut W,
    pool: &SqlitePool,
    symbol: &Symbol,
    offchain_order_id: OffchainOrderId,
    reason: String,
) -> anyhow::Result<()> {
    let (position, projection) = StoreBuilder::<Position>::new(pool.clone())
        .build(())
        .await
        .context("failed to build position store")?;

    let Some(view) = projection
        .load(symbol)
        .await
        .context("failed to load position view")?
    else {
        bail!("position {symbol} not found");
    };

    match view.pending_offchain_order_id {
        Some(pending) if pending == offchain_order_id => {}
        Some(pending) => {
            bail!("position {symbol} pending offchain order is {pending}, not {offchain_order_id}");
        }
        None => bail!("position {symbol} has no pending offchain order"),
    }

    position
        .send(
            symbol,
            PositionCommand::FailOffChainOrder {
                offchain_order_id,
                error: reason,
            },
        )
        .await
        .context("failed to fail pending offchain order")?;

    writeln!(
        stdout,
        "Failed pending offchain order {offchain_order_id} for {symbol}"
    )?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use alloy::primitives::TxHash;

    use st0x_config::ExecutionThreshold;
    use st0x_execution::{Direction, FractionalShares};
    use st0x_float_macro::float;

    use super::*;
    use crate::position::TradeId;
    use crate::test_utils::{positive_shares, setup_test_db};

    async fn seed_pending_position(
        pool: &SqlitePool,
        symbol: &Symbol,
        offchain_order_id: OffchainOrderId,
    ) {
        let (position, _projection) = StoreBuilder::<Position>::new(pool.clone())
            .build(())
            .await
            .unwrap();
        let threshold = ExecutionThreshold::whole_share();

        position
            .send(
                symbol,
                PositionCommand::AcknowledgeOnChainFill {
                    symbol: symbol.clone(),
                    threshold,
                    trade_id: TradeId {
                        tx_hash: TxHash::random(),
                        log_index: 0,
                    },
                    amount: FractionalShares::new(float!(1)),
                    direction: Direction::Buy,
                    price_usdc: float!(420),
                    block_timestamp: chrono::Utc::now(),
                },
            )
            .await
            .unwrap();

        position
            .send(
                symbol,
                PositionCommand::PlaceOffChainOrder {
                    offchain_order_id,
                    shares: positive_shares("0.5"),
                    direction: Direction::Sell,
                    executor: st0x_execution::SupportedExecutor::AlpacaBrokerApi,
                    threshold,
                },
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn fail_pending_offchain_order_clears_matching_pending_order() {
        let pool = setup_test_db().await;
        let symbol = Symbol::new("MSTR").unwrap();
        let order_id = OffchainOrderId::new();
        seed_pending_position(&pool, &symbol, order_id).await;

        let mut stdout_buffer = Vec::new();
        fail_pending_offchain_order_command(
            &mut stdout_buffer,
            &pool,
            &symbol,
            order_id,
            "operator repair".to_string(),
        )
        .await
        .unwrap();

        let (_position, projection) = StoreBuilder::<Position>::new(pool.clone())
            .build(())
            .await
            .unwrap();
        let view = projection.load(&symbol).await.unwrap().unwrap();
        assert_eq!(view.pending_offchain_order_id, None);
        let output = String::from_utf8(stdout_buffer).unwrap();
        assert!(
            output.contains(&order_id.to_string()),
            "unexpected output: {output}"
        );
    }

    #[tokio::test]
    async fn fail_pending_offchain_order_rejects_mismatched_order_id() {
        let pool = setup_test_db().await;
        let symbol = Symbol::new("MSTR").unwrap();
        let pending_order_id = OffchainOrderId::new();
        let requested_order_id = OffchainOrderId::new();
        seed_pending_position(&pool, &symbol, pending_order_id).await;

        let mut stdout_buffer = Vec::new();
        let error = fail_pending_offchain_order_command(
            &mut stdout_buffer,
            &pool,
            &symbol,
            requested_order_id,
            "operator repair".to_string(),
        )
        .await
        .unwrap_err();

        assert!(
            error.to_string().contains("not"),
            "unexpected error: {error}"
        );

        let (_position, projection) = StoreBuilder::<Position>::new(pool.clone())
            .build(())
            .await
            .unwrap();
        let view = projection.load(&symbol).await.unwrap().unwrap();
        assert_eq!(view.pending_offchain_order_id, Some(pending_order_id));
    }

    #[tokio::test]
    async fn fail_pending_offchain_order_rejects_position_without_pending_order() {
        let pool = setup_test_db().await;
        let symbol = Symbol::new("MSTR").unwrap();
        let (position, _projection) = StoreBuilder::<Position>::new(pool.clone())
            .build(())
            .await
            .unwrap();

        position
            .send(
                &symbol,
                PositionCommand::AcknowledgeOnChainFill {
                    symbol: symbol.clone(),
                    threshold: ExecutionThreshold::whole_share(),
                    trade_id: TradeId {
                        tx_hash: TxHash::random(),
                        log_index: 0,
                    },
                    amount: FractionalShares::new(float!(1)),
                    direction: Direction::Buy,
                    price_usdc: float!(420),
                    block_timestamp: chrono::Utc::now(),
                },
            )
            .await
            .unwrap();

        let mut stdout_buffer = Vec::new();
        let error = fail_pending_offchain_order_command(
            &mut stdout_buffer,
            &pool,
            &symbol,
            OffchainOrderId::new(),
            "operator repair".to_string(),
        )
        .await
        .unwrap_err();

        assert!(
            error.to_string().contains("no pending offchain order"),
            "unexpected error: {error}"
        );
    }
}

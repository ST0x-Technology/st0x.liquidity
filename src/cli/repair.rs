//! Repair CLI commands for manually recovering stuck local CQRS state.

use anyhow::{Context, bail};
use rain_math_float::Float;
use sqlx::SqlitePool;
use std::io::Write;

use st0x_config::ExecutionThreshold;
use st0x_event_sorcery::StoreBuilder;
use st0x_execution::{FractionalShares, Symbol};

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

pub(super) async fn set_position_command<W: Write>(
    stdout: &mut W,
    pool: &SqlitePool,
    symbol: &Symbol,
    target_net: FractionalShares,
    reason: String,
    threshold: ExecutionThreshold,
    price_usdc: Option<Float>,
) -> anyhow::Result<()> {
    if reason.trim().is_empty() {
        bail!(
            "--reason must not be empty; it is persisted as the audit record for this adjustment"
        );
    }

    let (position, projection) = StoreBuilder::<Position>::new(pool.clone())
        .build(())
        .await
        .context("failed to build position store")?;

    let current = projection
        .load(symbol)
        .await
        .context("failed to load position view")?;

    if let Some(view) = &current
        && let Some(pending) = view.pending_offchain_order_id.as_ref()
    {
        bail!(
            "position {symbol} has pending offchain order {pending}; \
             run repair fail-pending-offchain-order before setting position"
        );
    }

    let previous_net = current
        .as_ref()
        .map_or(FractionalShares::ZERO, |view| view.net);

    position
        .send(
            symbol,
            PositionCommand::ManuallyAdjustPosition {
                symbol: symbol.clone(),
                target_net,
                reason: reason.clone(),
                threshold,
                expected_net: Some(previous_net),
                price_usdc,
            },
        )
        .await
        .context("failed to set position")?;

    writeln!(
        stdout,
        "Set {symbol} position from {previous_net} to {target_net} because \"{reason}\""
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

    #[tokio::test]
    async fn set_position_initializes_missing_position() {
        let pool = setup_test_db().await;
        let symbol = Symbol::new("SPYM").unwrap();
        let target_net = FractionalShares::new(float!(100));

        let mut stdout_buffer = Vec::new();
        set_position_command(
            &mut stdout_buffer,
            &pool,
            &symbol,
            target_net,
            "manual long correction".to_string(),
            ExecutionThreshold::whole_share(),
            None,
        )
        .await
        .unwrap();

        let (_position, projection) = StoreBuilder::<Position>::new(pool.clone())
            .build(())
            .await
            .unwrap();
        let view = projection.load(&symbol).await.unwrap().unwrap();
        assert_eq!(view.net, target_net);
        assert_eq!(view.threshold, ExecutionThreshold::whole_share());

        let output = String::from_utf8(stdout_buffer).unwrap();
        assert!(
            output.contains("because \"manual long correction\""),
            "unexpected output: {output}"
        );
    }

    #[tokio::test]
    async fn set_position_updates_existing_position() {
        let pool = setup_test_db().await;
        let symbol = Symbol::new("SPYM").unwrap();
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
                    amount: FractionalShares::new(float!(5)),
                    direction: Direction::Buy,
                    price_usdc: float!(420),
                    block_timestamp: chrono::Utc::now(),
                },
            )
            .await
            .unwrap();

        let target_net = FractionalShares::new(float!(-3.25));
        let mut stdout_buffer = Vec::new();
        set_position_command(
            &mut stdout_buffer,
            &pool,
            &symbol,
            target_net,
            "manual short correction".to_string(),
            ExecutionThreshold::whole_share(),
            None,
        )
        .await
        .unwrap();

        let (_position, projection) = StoreBuilder::<Position>::new(pool.clone())
            .build(())
            .await
            .unwrap();
        let view = projection.load(&symbol).await.unwrap().unwrap();
        assert_eq!(view.net, target_net);
        assert_eq!(view.accumulated_long, FractionalShares::new(float!(5)));

        let output = String::from_utf8(stdout_buffer).unwrap();
        assert!(
            output.contains("from 5 to -3.25"),
            "unexpected output: {output}"
        );
    }

    #[tokio::test]
    async fn set_position_rejects_nonzero_dollar_target_without_price() {
        let pool = setup_test_db().await;
        let symbol = Symbol::new("SPYM").unwrap();
        let threshold =
            ExecutionThreshold::dollar_value(st0x_finance::Usdc::new(float!(1000))).unwrap();

        let mut stdout_buffer = Vec::new();
        let error = set_position_command(
            &mut stdout_buffer,
            &pool,
            &symbol,
            FractionalShares::new(float!(100)),
            "manual long correction".to_string(),
            threshold,
            None,
        )
        .await
        .unwrap_err();

        assert!(
            format!("{error:#}").contains("without a price"),
            "unexpected error: {error:#}"
        );

        let (_position, projection) = StoreBuilder::<Position>::new(pool.clone())
            .build(())
            .await
            .unwrap();
        assert!(
            projection.load(&symbol).await.unwrap().is_none(),
            "rejected adjustment must not persist a position"
        );
    }

    #[tokio::test]
    async fn set_position_rejects_whitespace_only_reason() {
        let pool = setup_test_db().await;
        let symbol = Symbol::new("SPYM").unwrap();

        let mut stdout_buffer = Vec::new();
        let error = set_position_command(
            &mut stdout_buffer,
            &pool,
            &symbol,
            FractionalShares::ZERO,
            "   ".to_string(),
            ExecutionThreshold::whole_share(),
            None,
        )
        .await
        .unwrap_err();

        assert!(
            format!("{error:#}").contains("--reason must not be empty"),
            "unexpected error: {error:#}"
        );

        let (_position, projection) = StoreBuilder::<Position>::new(pool.clone())
            .build(())
            .await
            .unwrap();
        assert!(
            projection.load(&symbol).await.unwrap().is_none(),
            "rejected adjustment must not persist a position"
        );
    }

    #[tokio::test]
    async fn set_position_initializes_dollar_position_with_price() {
        let pool = setup_test_db().await;
        let symbol = Symbol::new("SPYM").unwrap();
        let threshold =
            ExecutionThreshold::dollar_value(st0x_finance::Usdc::new(float!(1000))).unwrap();
        let target_net = FractionalShares::new(float!(100));

        let mut stdout_buffer = Vec::new();
        set_position_command(
            &mut stdout_buffer,
            &pool,
            &symbol,
            target_net,
            "manual long correction".to_string(),
            threshold,
            Some(float!(200)),
        )
        .await
        .unwrap();

        let (_position, projection) = StoreBuilder::<Position>::new(pool.clone())
            .build(())
            .await
            .unwrap();
        let view = projection.load(&symbol).await.unwrap().unwrap();
        assert_eq!(view.net, target_net);
        let (direction, shares) = view.is_ready_for_execution(None).unwrap().unwrap();
        assert_eq!(direction, Direction::Sell);
        assert_eq!(shares, target_net);
    }

    #[tokio::test]
    async fn set_position_rejects_position_with_pending_order() {
        let pool = setup_test_db().await;
        let symbol = Symbol::new("SPYM").unwrap();
        let pending_order_id = OffchainOrderId::new();
        seed_pending_position(&pool, &symbol, pending_order_id).await;

        let mut stdout_buffer = Vec::new();
        let error = set_position_command(
            &mut stdout_buffer,
            &pool,
            &symbol,
            FractionalShares::ZERO,
            "manual rebalance completed".to_string(),
            ExecutionThreshold::whole_share(),
            None,
        )
        .await
        .unwrap_err();

        assert!(
            error.to_string().contains("pending offchain order"),
            "unexpected error: {error}"
        );

        let (_position, projection) = StoreBuilder::<Position>::new(pool.clone())
            .build(())
            .await
            .unwrap();
        let view = projection.load(&symbol).await.unwrap().unwrap();
        assert_eq!(view.pending_offchain_order_id, Some(pending_order_id));
        assert_eq!(view.net, FractionalShares::new(float!(1)));
    }
}

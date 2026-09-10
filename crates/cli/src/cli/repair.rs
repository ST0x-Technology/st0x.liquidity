//! Repair CLI commands for manually recovering stuck local CQRS state.

use std::io::Write;

use rain_math_float::Float;
use sqlx::SqlitePool;

use st0x_config::{Ctx, ExecutionThreshold};
use st0x_execution::{FractionalShares, Symbol};
use st0x_hedge::operator::offchain::order::OffchainOrderId;
use st0x_hedge::operator::portfolio_snapshot::{EquityMarkCorrection, set_equity_mark};
use st0x_hedge::operator::position::{
    OffchainOrderOutcome, PointerOutcome, release_pending_offchain_order, set_position,
};

use super::{AuditReason, PortfolioSnapshotRecoveryCommand};

pub(super) async fn set_portfolio_snapshot_mark_command<W: Write>(
    stdout: &mut W,
    pool: &SqlitePool,
    command: PortfolioSnapshotRecoveryCommand,
    ctx: &Ctx,
) -> anyhow::Result<()> {
    let PortfolioSnapshotRecoveryCommand::Set {
        day,
        symbol,
        usd_mark,
        observed_at,
        source,
        reason,
    } = command;

    let correction = EquityMarkCorrection {
        day,
        symbol: symbol.clone(),
        usd_mark,
        observed_at,
        source: source.to_string(),
        reason: reason.to_string(),
    };
    let formatted_mark = set_equity_mark(pool, ctx, &correction)
        .await?
        .formatted_mark;

    writeln!(
        stdout,
        "Set {day} {symbol} portfolio mark to ${formatted_mark} observed at {observed_at} from \
         {source} because \"{reason}\""
    )?;

    Ok(())
}

/// Fails a position's pending offchain order pointer and drives the orphaned
/// `OffchainOrder` aggregate to `Failed`, then reports what happened.
///
/// Operates directly on the database: the operator must ensure the bot is not
/// concurrently driving the same order (per the recovery CLI execution-mode
/// contract), since a fill landing between the state read and the commands
/// cannot be guarded against.
pub(super) async fn fail_pending_offchain_order_command<W: Write>(
    stdout: &mut W,
    pool: &SqlitePool,
    symbol: &Symbol,
    offchain_order_id: OffchainOrderId,
    reason: AuditReason,
) -> anyhow::Result<()> {
    let outcome =
        release_pending_offchain_order(pool, symbol, offchain_order_id, reason.as_ref()).await?;

    match outcome.pointer {
        PointerOutcome::WasAlreadyClear => {
            writeln!(
                stdout,
                "Position {symbol} pointer already clear; repairing OffchainOrder \
                 {offchain_order_id}"
            )?;
            write_offchain_order_repair_line(stdout, outcome.offchain_order, offchain_order_id)?;
        }
        PointerOutcome::ClearedNow => {
            write_offchain_order_repair_line(stdout, outcome.offchain_order, offchain_order_id)?;
            writeln!(
                stdout,
                "Failed pending offchain order {offchain_order_id} for {symbol}"
            )?;
        }
    }

    Ok(())
}

/// Renders the operator-facing line describing what the repair did with the
/// orphaned `OffchainOrder` aggregate.
fn write_offchain_order_repair_line<W: Write>(
    stdout: &mut W,
    outcome: OffchainOrderOutcome,
    offchain_order_id: OffchainOrderId,
) -> std::io::Result<()> {
    match outcome {
        OffchainOrderOutcome::MarkedFailed => writeln!(
            stdout,
            "Also marked OffchainOrder {offchain_order_id} as failed"
        ),
        OffchainOrderOutcome::AlreadyTerminal => writeln!(
            stdout,
            "OffchainOrder {offchain_order_id} already terminal; left as-is"
        ),
        OffchainOrderOutcome::NoAggregate => writeln!(
            stdout,
            "No OffchainOrder aggregate {offchain_order_id} found; pointer cleared only"
        ),
        OffchainOrderOutcome::TerminalConcurrently => writeln!(
            stdout,
            "OffchainOrder {offchain_order_id} reached a terminal state concurrently; left as-is"
        ),
    }
}

pub(super) async fn set_position_command<W: Write>(
    stdout: &mut W,
    pool: &SqlitePool,
    symbol: &Symbol,
    target_net: FractionalShares,
    reason: AuditReason,
    threshold: ExecutionThreshold,
    price_usdc: Option<Float>,
) -> anyhow::Result<()> {
    let previous_net = set_position(
        pool,
        symbol,
        target_net,
        reason.as_ref(),
        threshold,
        price_usdc,
    )
    .await?
    .previous_net;

    writeln!(
        stdout,
        "Set {symbol} position from {previous_net} to {target_net} because \"{reason}\""
    )?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use alloy::primitives::TxHash;
    use chrono::{TimeZone, Utc};
    use st0x_config::ExecutionThreshold;
    use st0x_event_sorcery::StoreBuilder;
    use st0x_evm::Chain;
    use st0x_execution::{ClientOrderId, Direction, ExecutorOrderId, FractionalShares};
    use st0x_finance::{Positive, Usd};
    use st0x_float_macro::float;
    use st0x_hedge::operator::inventory::{PortfolioAsset, PortfolioBalanceRow, PortfolioLocation};
    use st0x_hedge::operator::offchain::order::{OffchainOrder, OffchainOrderCommand, OrderPlacer};
    use st0x_hedge::operator::portfolio_snapshot::{
        PortfolioBalanceRowWithMark, PortfolioSnapshot, PortfolioSnapshotCommand,
        PortfolioSnapshotId, PortfolioSnapshotProjection,
    };
    use st0x_hedge::operator::position::{
        AnchorDisposition, Position, PositionCommand, ReloadOutcome, RepairOrderPlacer, TradeId,
        classify_reloaded_state, fail_offchain_order_aggregate,
    };
    use st0x_hedge::operator::test_utils::{
        try_positive_shares, try_rebalancing_enabled_equities, try_setup_test_db,
    };
    use uuid::Uuid;

    use super::*;

    fn positive_shares(value: &str) -> Positive<FractionalShares> {
        try_positive_shares(value).expect("test shares must be valid and positive")
    }

    async fn setup_test_db() -> SqlitePool {
        try_setup_test_db()
            .await
            .expect("test database setup must succeed")
    }

    fn repair_order_placer() -> Arc<dyn OrderPlacer> {
        Arc::new(RepairOrderPlacer)
    }

    /// Ctx configuring `symbols` as equities, so the repair's
    /// unconverted-wrapped-rows guard sees them as configured.
    fn ctx_with_equities(symbols: &[&str]) -> Ctx {
        let mut ctx =
            st0x_config::create_test_ctx_with_order_owner(alloy::primitives::Address::ZERO);
        ctx.chains.primary_mut().assets.equities =
            try_rebalancing_enabled_equities(symbols).expect("test equity symbols must be valid");
        ctx
    }

    async fn seed_missing_portfolio_marks(
        pool: &SqlitePool,
        day: chrono::NaiveDate,
        symbol: &Symbol,
    ) {
        seed_missing_portfolio_marks_at(
            pool,
            day,
            symbol,
            &[
                PortfolioLocation::MarketMaking(Chain::Base),
                PortfolioLocation::Hedging,
            ],
        )
        .await;
    }

    async fn seed_missing_portfolio_marks_at(
        pool: &SqlitePool,
        day: chrono::NaiveDate,
        symbol: &Symbol,
        locations: &[PortfolioLocation],
    ) {
        let captured_at = Utc.with_ymd_and_hms(2026, 7, 20, 4, 5, 0).unwrap();
        let rows = locations
            .iter()
            .copied()
            .map(|location| PortfolioBalanceRowWithMark {
                row: PortfolioBalanceRow {
                    location,
                    asset: PortfolioAsset::Equity(symbol.clone()),
                    available: float!(10),
                    inflight: float!(0),
                },
                usd_mark: None,
                mark_captured_at: None,
            })
            .collect::<Vec<_>>();
        let store = StoreBuilder::<PortfolioSnapshot>::new(pool.clone())
            .with(Arc::new(PortfolioSnapshotProjection::new(pool.clone())))
            .build(())
            .await
            .unwrap();
        store
            .send(
                &PortfolioSnapshotId(day),
                PortfolioSnapshotCommand::Capture { captured_at, rows },
            )
            .await
            .unwrap();
    }

    /// The refusal keys on the LOCATION holding vault shares, not on
    /// `MarketMaking` specifically: `BaseWalletWrapped` is the other
    /// unconverted location, and a symbol whose only wrapped rows sit there
    /// must be refused the same way.
    #[tokio::test]
    async fn portfolio_snapshot_repair_refuses_unconfigured_symbol_with_base_wallet_wrapped_rows() {
        let pool = setup_test_db().await;
        let day = chrono::NaiveDate::from_ymd_opt(2026, 7, 20).unwrap();
        let symbol = Symbol::new("QSEP").unwrap();
        seed_missing_portfolio_marks_at(
            &pool,
            day,
            &symbol,
            &[
                PortfolioLocation::BaseWalletWrapped,
                PortfolioLocation::Hedging,
            ],
        )
        .await;

        let ctx = ctx_with_equities(&["AAPL"]);
        let error = set_portfolio_snapshot_mark_command(
            &mut Vec::new(),
            &pool,
            PortfolioSnapshotRecoveryCommand::Set {
                day,
                symbol: symbol.clone(),
                usd_mark: Positive::new(float!(150)).unwrap(),
                observed_at: Utc.with_ymd_and_hms(2026, 7, 17, 20, 0, 0).unwrap(),
                source: "Nasdaq historical close".parse().unwrap(),
                reason: "repair missing mark".parse().unwrap(),
            },
            &ctx,
        )
        .await
        .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("vault shares, not underlying shares"),
            "a BaseWalletWrapped-only holding must refuse like MarketMaking; got: {error}"
        );
    }

    /// The guard must not over-block: an unconfigured symbol whose wrapped
    /// rows are gone holds only underlying-unit balances (`Hedging` is never
    /// converted), so a mark values them correctly and the repair proceeds.
    /// This is what makes reconciling the on-chain side restore repairability.
    #[tokio::test]
    async fn portfolio_snapshot_repair_allows_unconfigured_symbol_without_wrapped_rows() {
        let pool = setup_test_db().await;
        let day = chrono::NaiveDate::from_ymd_opt(2026, 7, 20).unwrap();
        let symbol = Symbol::new("QSEP").unwrap();
        seed_missing_portfolio_marks_at(&pool, day, &symbol, &[PortfolioLocation::Hedging]).await;

        let ctx = ctx_with_equities(&["AAPL"]);
        set_portfolio_snapshot_mark_command(
            &mut Vec::new(),
            &pool,
            PortfolioSnapshotRecoveryCommand::Set {
                day,
                symbol: symbol.clone(),
                usd_mark: Positive::new(float!(150)).unwrap(),
                observed_at: Utc.with_ymd_and_hms(2026, 7, 17, 20, 0, 0).unwrap(),
                source: "Nasdaq historical close".parse().unwrap(),
                reason: "repair missing mark".parse().unwrap(),
            },
            &ctx,
        )
        .await
        .unwrap();

        let (location, mark): (String, Option<String>) = sqlx::query_as(
            "SELECT location, usd_mark FROM portfolio_snapshot WHERE et_day = ? AND asset = ?",
        )
        .bind(day.to_string())
        .bind(symbol.to_string())
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(location, "hedging");
        assert_eq!(mark.as_deref(), Some("150"));
    }

    /// `EquityMarkSet` prices every row of the symbol, and a symbol with no
    /// config entry has its wrapped-location rows captured in vault-share
    /// units. Pricing those as underlying is the misvaluation the capture's
    /// forced-absent mark exists to prevent, so the repair must refuse rather
    /// than reintroduce it through the operator path.
    #[tokio::test]
    async fn portfolio_snapshot_repair_refuses_unconfigured_symbol_with_wrapped_rows() {
        let pool = setup_test_db().await;
        let day = chrono::NaiveDate::from_ymd_opt(2026, 7, 20).unwrap();
        let symbol = Symbol::new("QSEP").unwrap();
        seed_missing_portfolio_marks(&pool, day, &symbol).await;

        // AAPL configured, QSEP retired -- the state after a config removal.
        let ctx = ctx_with_equities(&["AAPL"]);
        let error = set_portfolio_snapshot_mark_command(
            &mut Vec::new(),
            &pool,
            PortfolioSnapshotRecoveryCommand::Set {
                day,
                symbol: symbol.clone(),
                usd_mark: Positive::new(float!(150)).unwrap(),
                observed_at: Utc.with_ymd_and_hms(2026, 7, 17, 20, 0, 0).unwrap(),
                source: "Nasdaq historical close".parse().unwrap(),
                reason: "repair missing mark".parse().unwrap(),
            },
            &ctx,
        )
        .await
        .unwrap_err();

        let message = error.to_string();
        assert!(
            message.contains("vault shares, not underlying shares"),
            "the refusal must name the unit mismatch; got: {message}"
        );

        let marked: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM portfolio_snapshot \
             WHERE et_day = ? AND asset = ? AND usd_mark IS NOT NULL",
        )
        .bind(day.to_string())
        .bind(symbol.to_string())
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(marked, 0, "a refused repair must not price any row");
    }

    /// The guard derives the market-making location from the configured
    /// primary chain: a snapshot captured on a non-Base chain persists
    /// `market_making:<chain>` rows, and a guard hardcoded to
    /// `market_making:base` would miss them and let the repair price vault
    /// shares as underlying.
    #[tokio::test]
    async fn portfolio_snapshot_repair_refuses_wrapped_rows_on_a_non_base_primary_chain() {
        let pool = setup_test_db().await;
        let day = chrono::NaiveDate::from_ymd_opt(2026, 7, 20).unwrap();
        let symbol = Symbol::new("QSEP").unwrap();
        seed_missing_portfolio_marks_at(
            &pool,
            day,
            &symbol,
            &[
                PortfolioLocation::MarketMaking(Chain::Ethereum),
                PortfolioLocation::Hedging,
            ],
        )
        .await;

        let mut ctx = ctx_with_equities(&["AAPL"]);
        ctx.chains.primary_mut().chain = Chain::Ethereum;
        let error = set_portfolio_snapshot_mark_command(
            &mut Vec::new(),
            &pool,
            PortfolioSnapshotRecoveryCommand::Set {
                day,
                symbol: symbol.clone(),
                usd_mark: Positive::new(float!(150)).unwrap(),
                observed_at: Utc.with_ymd_and_hms(2026, 7, 17, 20, 0, 0).unwrap(),
                source: "Nasdaq historical close".parse().unwrap(),
                reason: "repair missing mark".parse().unwrap(),
            },
            &ctx,
        )
        .await
        .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("vault shares, not underlying shares"),
            "wrapped rows on the configured non-Base chain must refuse; got: {error}"
        );
    }

    #[tokio::test]
    async fn portfolio_snapshot_repair_updates_all_symbol_rows_without_touching_position() {
        let pool = setup_test_db().await;
        let day = chrono::NaiveDate::from_ymd_opt(2026, 7, 20).unwrap();
        let symbol = Symbol::new("AAPL").unwrap();
        seed_missing_portfolio_marks(&pool, day, &symbol).await;

        let ctx = ctx_with_equities(&["AAPL"]);
        let mut output = Vec::new();
        set_portfolio_snapshot_mark_command(
            &mut output,
            &pool,
            PortfolioSnapshotRecoveryCommand::Set {
                day,
                symbol: symbol.clone(),
                usd_mark: Positive::new(float!(150)).unwrap(),
                observed_at: Utc.with_ymd_and_hms(2026, 7, 17, 20, 0, 0).unwrap(),
                source: "Nasdaq historical close".parse().unwrap(),
                reason: "repair missing mark".parse().unwrap(),
            },
            &ctx,
        )
        .await
        .unwrap();

        let marks: Vec<String> = sqlx::query_scalar(
            "SELECT usd_mark FROM portfolio_snapshot WHERE et_day = ? AND asset = 'AAPL'",
        )
        .bind(day.to_string())
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(marks, vec!["150", "150"]);
        assert!(
            st0x_event_sorcery::load_entity::<Position>(&pool, &symbol)
                .await
                .unwrap()
                .is_none(),
            "historical repair must not create or update live Position state"
        );
        let payloads: Vec<String> = sqlx::query_scalar(
            "SELECT payload FROM events WHERE aggregate_type = 'PortfolioSnapshot' \
             AND aggregate_id = ? AND event_type = 'PortfolioSnapshotEvent::EquityMarkSet'",
        )
        .bind(day.to_string())
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(payloads.len(), 1);

        // `source` and `reason` are adjacent `String` fields on the event, so
        // both are asserted: swapping them at the call site would otherwise
        // compile and satisfy every other assertion here.
        let event: serde_json::Value = serde_json::from_str(&payloads[0]).unwrap();
        let fields = &event["EquityMarkSet"];
        assert_eq!(
            fields["source"],
            serde_json::json!("Nasdaq historical close")
        );
        assert_eq!(fields["reason"], serde_json::json!("repair missing mark"));
    }

    #[tokio::test]
    async fn portfolio_snapshot_repair_rejects_partial_projection_rows() {
        let pool = setup_test_db().await;
        let day = chrono::NaiveDate::from_ymd_opt(2026, 7, 20).unwrap();
        let symbol = Symbol::new("AAPL").unwrap();
        seed_missing_portfolio_marks(&pool, day, &symbol).await;
        sqlx::query(
            "DELETE FROM portfolio_snapshot WHERE et_day = ? AND asset = ? AND location = ?",
        )
        .bind(day.to_string())
        .bind(symbol.to_string())
        .bind(PortfolioLocation::Hedging.to_string())
        .execute(&pool)
        .await
        .unwrap();

        let ctx = ctx_with_equities(&["AAPL"]);
        let error = set_portfolio_snapshot_mark_command(
            &mut Vec::new(),
            &pool,
            PortfolioSnapshotRecoveryCommand::Set {
                day,
                symbol,
                usd_mark: Positive::new(float!(150)).unwrap(),
                observed_at: Utc.with_ymd_and_hms(2026, 7, 17, 20, 0, 0).unwrap(),
                source: "Nasdaq historical close".parse().unwrap(),
                reason: "repair missing mark".parse().unwrap(),
            },
            &ctx,
        )
        .await
        .unwrap_err();

        assert!(
            error.to_string().contains("did not update every"),
            "unexpected error: {error:#}"
        );
    }

    /// Seeds a standalone OffchainOrder aggregate for `order_id` into the
    /// non-terminal `Submitted` state (Place -> Placed + Submitted via the noop
    /// placer), so repair can drive it to `Failed`.
    async fn seed_offchain_order(pool: &SqlitePool, order_id: OffchainOrderId, symbol: &Symbol) {
        st0x_event_sorcery::send_command::<OffchainOrder>(
            pool,
            &order_id,
            OffchainOrderCommand::Place {
                symbol: symbol.clone(),
                shares: positive_shares("0.5"),
                direction: Direction::Sell,
                executor: st0x_execution::SupportedExecutor::AlpacaBrokerApi,
                client_order_id: ClientOrderId::from_uuid(Uuid::new_v4()),
                kind: st0x_hedge::operator::offchain::order::CounterTradeOrderKind::Market,
            },
            st0x_hedge::operator::offchain::order::noop_order_placer(),
        )
        .await
        .unwrap();

        // The pure `Place` handler only records the order as `Pending`; broker
        // acceptance is a separate step (a job in production). Seed it directly
        // so the order reaches `Submitted`, the state these repair tests exercise.
        st0x_event_sorcery::send_command::<OffchainOrder>(
            pool,
            &order_id,
            OffchainOrderCommand::MarkAccepted {
                executor_order_id: ExecutorOrderId::new("seed-accept"),
                placed_shares: positive_shares("0.5"),
                submitted_at: chrono::Utc::now(),
                market_session: st0x_execution::MarketSession::Regular,
                limit_price: None,
            },
            st0x_hedge::operator::offchain::order::noop_order_placer(),
        )
        .await
        .unwrap();
    }

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
                        chain: Chain::Base,
                        tx_hash: TxHash::random(),
                        log_index: 0,
                    },
                    amount: FractionalShares::new(float!(1)),
                    direction: Direction::Buy,
                    price_usdc: float!(420),
                    block_timestamp: chrono::Utc::now(),
                    block_number: None,
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
            "operator repair".parse().unwrap(),
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
    async fn fail_pending_also_fails_offchain_order_aggregate() {
        let pool = setup_test_db().await;
        let symbol = Symbol::new("MSTR").unwrap();
        let order_id = OffchainOrderId::new();
        seed_pending_position(&pool, &symbol, order_id).await;
        seed_offchain_order(&pool, order_id, &symbol).await;

        let mut stdout_buffer = Vec::new();
        fail_pending_offchain_order_command(
            &mut stdout_buffer,
            &pool,
            &symbol,
            order_id,
            "operator repair".parse().unwrap(),
        )
        .await
        .unwrap();

        // Position pointer cleared.
        let (_position, projection) = StoreBuilder::<Position>::new(pool.clone())
            .build(())
            .await
            .unwrap();
        assert_eq!(
            projection
                .load(&symbol)
                .await
                .unwrap()
                .unwrap()
                .pending_offchain_order_id,
            None
        );

        // The OffchainOrder aggregate itself is now Failed -- no orphan -- and
        // the failure carries the operator's audit reason verbatim.
        let order = st0x_event_sorcery::load_entity::<OffchainOrder>(&pool, &order_id)
            .await
            .unwrap()
            .unwrap();
        let OffchainOrder::Failed { error, .. } = order else {
            panic!("OffchainOrder must be Failed, got {order:?}");
        };
        assert_eq!(
            error, "operator repair",
            "the audit reason must be persisted on the failed order",
        );

        // The read-side view must update immediately -- a stale live-looking
        // row is the symptom this command exists to repair.
        let (status,): (String,) =
            sqlx::query_as("SELECT status FROM offchain_order_view WHERE view_id = ?")
                .bind(order_id.to_string())
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(status, "Failed", "offchain_order_view must show Failed");

        let output = String::from_utf8(stdout_buffer).unwrap();
        assert!(
            output.contains("Also marked OffchainOrder"),
            "unexpected output: {output}"
        );
    }

    /// `seed_offchain_order` seeds a broker `executor_order_id`, which is not
    /// terminality evidence and must not flip the disposition to `Release`.
    /// Guards against a refactor that derives the disposition from it instead
    /// of the hardcoded `Preserve`.
    #[tokio::test]
    async fn fail_pending_preserves_anchor_despite_executor_order_id_evidence() {
        let pool = setup_test_db().await;
        let symbol = Symbol::new("MSTR").unwrap();
        let order_id = OffchainOrderId::new();
        seed_pending_position(&pool, &symbol, order_id).await;
        seed_offchain_order(&pool, order_id, &symbol).await;

        fail_pending_offchain_order_command(
            &mut Vec::new(),
            &pool,
            &symbol,
            order_id,
            "operator repair".parse().unwrap(),
        )
        .await
        .unwrap();

        let (_position, projection) = StoreBuilder::<Position>::new(pool.clone())
            .build(())
            .await
            .unwrap();
        let view = projection.load(&symbol).await.unwrap().unwrap();
        assert_eq!(
            view.last_failed_offchain_order_id,
            Some(order_id),
            "repair must preserve the anchor: a force-failed order is typically \
             still live at the broker, and an executor_order_id proves nothing \
             about terminality"
        );
    }

    /// A Filled order with the pointer still set means the fill was never
    /// accounted: the repair must refuse and leave the pointer for the fill
    /// reconciliation path.
    #[tokio::test]
    async fn fail_pending_refuses_filled_order() {
        let pool = setup_test_db().await;
        let symbol = Symbol::new("MSTR").unwrap();
        let order_id = OffchainOrderId::new();
        seed_pending_position(&pool, &symbol, order_id).await;
        seed_offchain_order(&pool, order_id, &symbol).await;

        st0x_event_sorcery::send_command::<OffchainOrder>(
            &pool,
            &order_id,
            OffchainOrderCommand::CompleteFill {
                price: Usd::new(float!(100)),
                filled_at: chrono::Utc::now(),
            },
            repair_order_placer(),
        )
        .await
        .unwrap();

        let mut stdout_buffer = Vec::new();
        let error = fail_pending_offchain_order_command(
            &mut stdout_buffer,
            &pool,
            &symbol,
            order_id,
            "operator repair".parse().unwrap(),
        )
        .await
        .unwrap_err();

        assert!(
            error.to_string().contains("is Filled"),
            "expected a filled-order refusal; got: {error}"
        );
        let (_position, projection) = StoreBuilder::<Position>::new(pool.clone())
            .build(())
            .await
            .unwrap();
        assert_eq!(
            projection
                .load(&symbol)
                .await
                .unwrap()
                .unwrap()
                .pending_offchain_order_id,
            Some(order_id),
            "position pointer must remain set when the repair is refused",
        );
    }

    /// A concurrent FILL between the command's state snapshot and MarkFailed
    /// must surface as a hard error: the pointer was cleared for an order
    /// that actually executed.
    #[tokio::test]
    async fn fail_offchain_order_aggregate_errors_on_concurrent_fill() {
        let pool = setup_test_db().await;
        let symbol = Symbol::new("MSTR").unwrap();
        let order_id = OffchainOrderId::new();
        seed_offchain_order(&pool, order_id, &symbol).await;

        // Snapshot the order while it is still Submitted...
        let stale = st0x_event_sorcery::load_entity::<OffchainOrder>(&pool, &order_id)
            .await
            .unwrap();

        // ...then the bot fills it concurrently.
        st0x_event_sorcery::send_command::<OffchainOrder>(
            &pool,
            &order_id,
            OffchainOrderCommand::CompleteFill {
                price: Usd::new(float!(100)),
                filled_at: chrono::Utc::now(),
            },
            repair_order_placer(),
        )
        .await
        .unwrap();

        let error = fail_offchain_order_aggregate(&pool, stale, order_id, "operator repair")
            .await
            .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("acquired executed shares concurrently"),
            "expected the concurrent-execution error; got: {error}"
        );
    }

    /// The exact interleaving the live route must survive: the bot commits a
    /// PARTIAL fill after the repair's last read of the order and before its
    /// `MarkFailedUnfilled` send. The fill must be refused by the aggregate
    /// (evaluated on the state the store loads for the send, not on the stale
    /// snapshot), the partial fill must survive, and because the repair is
    /// aggregate-first the position pointer must still be set so the fill is
    /// accounted through the normal flow. This is the case the bot's own
    /// `MarkFailed` would erase silently.
    #[tokio::test]
    async fn fail_offchain_order_aggregate_refuses_a_partial_fill_landing_after_the_read() {
        let pool = setup_test_db().await;
        let symbol = Symbol::new("MSTR").unwrap();
        let order_id = OffchainOrderId::new();
        seed_pending_position(&pool, &symbol, order_id).await;
        seed_offchain_order(&pool, order_id, &symbol).await;

        // The repair's last read of the order, while still Submitted...
        let stale = st0x_event_sorcery::load_entity::<OffchainOrder>(&pool, &order_id)
            .await
            .unwrap();
        assert!(matches!(stale, Some(OffchainOrder::Submitted { .. })));

        // ...then the bot commits a partial fill through its own store.
        st0x_event_sorcery::send_command::<OffchainOrder>(
            &pool,
            &order_id,
            OffchainOrderCommand::UpdatePartialFill {
                shares_filled: FractionalShares::new(float!(0.25)),
                avg_price: Usd::new(float!(100)),
                partially_filled_at: chrono::Utc::now(),
            },
            repair_order_placer(),
        )
        .await
        .unwrap();

        let error = fail_offchain_order_aggregate(&pool, stale, order_id, "operator repair")
            .await
            .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("acquired executed shares concurrently"),
            "expected the concurrent-execution error; got: {error}"
        );

        let order = st0x_event_sorcery::load_entity::<OffchainOrder>(&pool, &order_id)
            .await
            .unwrap()
            .unwrap();
        assert!(
            matches!(
                order,
                OffchainOrder::PartiallyFilled { shares_filled, .. }
                    if shares_filled == FractionalShares::new(float!(0.25))
            ),
            "the partial fill must not be erased, got {order:?}"
        );

        let (_position, projection) = StoreBuilder::<Position>::new(pool.clone())
            .build(())
            .await
            .unwrap();
        assert_eq!(
            projection
                .load(&symbol)
                .await
                .unwrap()
                .unwrap()
                .pending_offchain_order_id,
            Some(order_id),
            "the pointer must stay set so the fill is accounted through the normal flow",
        );
    }

    /// The escalation classifier is the single source of the executed-shares
    /// rule shared by the `AlreadyCompleted` and `AggregateConflict` recovery
    /// arms; every state must map to the right outcome so the two cannot
    /// silently diverge. Covers all three `ReloadOutcome` variants -- the
    /// branches those arms depend on but cannot exercise deterministically in
    /// situ.
    #[tokio::test]
    async fn classify_reloaded_state_routes_every_variant() {
        let pool = setup_test_db().await;
        let symbol = Symbol::new("MSTR").unwrap();

        // Absent aggregate -> Proceed.
        assert!(matches!(
            classify_reloaded_state(None),
            ReloadOutcome::Proceed
        ));

        // Submitted (no executed shares, not terminal) -> Proceed.
        let submitted_id = OffchainOrderId::new();
        seed_offchain_order(&pool, submitted_id, &symbol).await;
        let submitted = st0x_event_sorcery::load_entity::<OffchainOrder>(&pool, &submitted_id)
            .await
            .unwrap();
        assert!(matches!(submitted, Some(OffchainOrder::Submitted { .. })));
        assert!(matches!(
            classify_reloaded_state(submitted.as_ref()),
            ReloadOutcome::Proceed
        ));

        // PartiallyFilled (executed shares) -> Escalate.
        let partial_id = OffchainOrderId::new();
        seed_offchain_order(&pool, partial_id, &symbol).await;
        st0x_event_sorcery::send_command::<OffchainOrder>(
            &pool,
            &partial_id,
            OffchainOrderCommand::UpdatePartialFill {
                shares_filled: FractionalShares::new(float!(0.25)),
                avg_price: Usd::new(float!(100)),
                partially_filled_at: chrono::Utc::now(),
            },
            repair_order_placer(),
        )
        .await
        .unwrap();
        let partial = st0x_event_sorcery::load_entity::<OffchainOrder>(&pool, &partial_id)
            .await
            .unwrap();
        assert!(matches!(
            classify_reloaded_state(partial.as_ref()),
            ReloadOutcome::Escalate
        ));

        // Filled (executed shares) -> Escalate.
        let filled_id = OffchainOrderId::new();
        seed_offchain_order(&pool, filled_id, &symbol).await;
        st0x_event_sorcery::send_command::<OffchainOrder>(
            &pool,
            &filled_id,
            OffchainOrderCommand::CompleteFill {
                price: Usd::new(float!(100)),
                filled_at: chrono::Utc::now(),
            },
            repair_order_placer(),
        )
        .await
        .unwrap();
        let filled = st0x_event_sorcery::load_entity::<OffchainOrder>(&pool, &filled_id)
            .await
            .unwrap();
        assert!(matches!(
            classify_reloaded_state(filled.as_ref()),
            ReloadOutcome::Escalate
        ));

        // Failed (benign concurrent terminal) -> BenignTerminal.
        let failed_id = OffchainOrderId::new();
        seed_offchain_order(&pool, failed_id, &symbol).await;
        st0x_event_sorcery::send_command::<OffchainOrder>(
            &pool,
            &failed_id,
            OffchainOrderCommand::MarkFailed {
                error: "bot failed it".to_string(),
                filled_shares: None,
                failed_at: chrono::Utc::now(),
            },
            repair_order_placer(),
        )
        .await
        .unwrap();
        let failed = st0x_event_sorcery::load_entity::<OffchainOrder>(&pool, &failed_id)
            .await
            .unwrap();
        assert!(matches!(
            classify_reloaded_state(failed.as_ref()),
            ReloadOutcome::BenignTerminal
        ));
    }

    /// The public command must propagate the executed-shares refusal when a
    /// fill landed after a partial prior run cleared the pointer.
    #[tokio::test]
    async fn fail_pending_command_errors_when_orphan_filled_after_pointer_clear() {
        let pool = setup_test_db().await;
        let symbol = Symbol::new("MSTR").unwrap();
        let order_id = OffchainOrderId::new();
        seed_pending_position(&pool, &symbol, order_id).await;
        seed_offchain_order(&pool, order_id, &symbol).await;

        // Partial prior run cleared the pointer...
        let (position, _projection) = StoreBuilder::<Position>::new(pool.clone())
            .build(())
            .await
            .unwrap();
        position
            .send(
                &symbol,
                PositionCommand::FailOffChainOrder {
                    offchain_order_id: order_id,
                    error: "partial prior run".to_string(),
                    anchor: AnchorDisposition::Preserve,
                },
            )
            .await
            .unwrap();

        // ...and the order then filled at the broker.
        st0x_event_sorcery::send_command::<OffchainOrder>(
            &pool,
            &order_id,
            OffchainOrderCommand::CompleteFill {
                price: Usd::new(float!(100)),
                filled_at: chrono::Utc::now(),
            },
            repair_order_placer(),
        )
        .await
        .unwrap();

        let mut stdout_buffer = Vec::new();
        let error = fail_pending_offchain_order_command(
            &mut stdout_buffer,
            &pool,
            &symbol,
            order_id,
            "operator repair".parse().unwrap(),
        )
        .await
        .unwrap_err();

        assert!(
            error.to_string().contains("is Filled"),
            "the filled orphan must be refused through the public command; got: {error}"
        );
    }

    /// A concurrent FAIL between the snapshot and MarkFailed is equivalent to
    /// finding the order terminal up front: clean no-op.
    #[tokio::test]
    async fn fail_offchain_order_aggregate_tolerates_concurrent_fail() {
        let pool = setup_test_db().await;
        let symbol = Symbol::new("MSTR").unwrap();
        let order_id = OffchainOrderId::new();
        seed_offchain_order(&pool, order_id, &symbol).await;

        let stale = st0x_event_sorcery::load_entity::<OffchainOrder>(&pool, &order_id)
            .await
            .unwrap();

        st0x_event_sorcery::send_command::<OffchainOrder>(
            &pool,
            &order_id,
            OffchainOrderCommand::MarkFailed {
                error: "bot failed it concurrently".to_string(),
                filled_shares: None,
                failed_at: chrono::Utc::now(),
            },
            repair_order_placer(),
        )
        .await
        .unwrap();

        let outcome = fail_offchain_order_aggregate(&pool, stale, order_id, "operator repair")
            .await
            .unwrap();

        assert_eq!(
            outcome,
            OffchainOrderOutcome::TerminalConcurrently,
            "a concurrent fail must be reported as a benign terminal outcome"
        );

        // The bot's own failure record must be untouched by the repair.
        let order = st0x_event_sorcery::load_entity::<OffchainOrder>(&pool, &order_id)
            .await
            .unwrap()
            .unwrap();
        let OffchainOrder::Failed { error, .. } = order else {
            panic!("order must remain Failed, got {order:?}");
        };
        assert_eq!(
            error, "bot failed it concurrently",
            "the bot's original failure reason must not be overwritten",
        );
    }

    #[tokio::test]
    async fn fail_pending_leaves_already_terminal_offchain_order_untouched() {
        let pool = setup_test_db().await;
        let symbol = Symbol::new("MSTR").unwrap();
        let order_id = OffchainOrderId::new();
        seed_pending_position(&pool, &symbol, order_id).await;
        seed_offchain_order(&pool, order_id, &symbol).await;

        // Pre-fail the OffchainOrder so repair must treat it as idempotent.
        st0x_event_sorcery::send_command::<OffchainOrder>(
            &pool,
            &order_id,
            OffchainOrderCommand::MarkFailed {
                error: "pre-failed".to_string(),
                filled_shares: None,
                failed_at: chrono::Utc::now(),
            },
            repair_order_placer(),
        )
        .await
        .unwrap();

        let mut stdout_buffer = Vec::new();
        fail_pending_offchain_order_command(
            &mut stdout_buffer,
            &pool,
            &symbol,
            order_id,
            "operator repair".parse().unwrap(),
        )
        .await
        .unwrap();

        let output = String::from_utf8(stdout_buffer).unwrap();
        assert!(
            output.contains("already terminal"),
            "unexpected output: {output}"
        );

        // The pointer must still be cleared even when the order needed no fix.
        let (_position, projection) = StoreBuilder::<Position>::new(pool.clone())
            .build(())
            .await
            .unwrap();
        assert_eq!(
            projection
                .load(&symbol)
                .await
                .unwrap()
                .unwrap()
                .pending_offchain_order_id,
            None,
            "position pointer must be cleared alongside the terminal report",
        );
    }

    /// Partial-failure recovery: a prior run cleared the Position pointer but
    /// never failed the OffchainOrder (crash between the two non-atomic
    /// aggregate commands, or an orphan created by the pre-RAI-984 command).
    /// Re-running must repair the orphaned order instead of bailing, and a
    /// further re-run after full success must be a clean no-op.
    #[tokio::test]
    async fn fail_pending_rerun_repairs_orphaned_order_after_partial_failure() {
        let pool = setup_test_db().await;
        let symbol = Symbol::new("MSTR").unwrap();
        let order_id = OffchainOrderId::new();
        seed_pending_position(&pool, &symbol, order_id).await;
        seed_offchain_order(&pool, order_id, &symbol).await;

        // Simulate the partial prior run: pointer cleared, order untouched.
        let (position, _projection) = StoreBuilder::<Position>::new(pool.clone())
            .build(())
            .await
            .unwrap();
        position
            .send(
                &symbol,
                PositionCommand::FailOffChainOrder {
                    offchain_order_id: order_id,
                    error: "partial prior run".to_string(),
                    anchor: AnchorDisposition::Preserve,
                },
            )
            .await
            .unwrap();

        let mut stdout_buffer = Vec::new();
        fail_pending_offchain_order_command(
            &mut stdout_buffer,
            &pool,
            &symbol,
            order_id,
            "operator repair".parse().unwrap(),
        )
        .await
        .unwrap();

        let order = st0x_event_sorcery::load_entity::<OffchainOrder>(&pool, &order_id)
            .await
            .unwrap()
            .unwrap();
        assert!(
            matches!(order, OffchainOrder::Failed { .. }),
            "orphaned OffchainOrder must be repaired to Failed, got {order:?}"
        );
        let output = String::from_utf8(stdout_buffer).unwrap();
        assert!(
            output.contains("pointer already clear"),
            "unexpected output: {output}"
        );

        // Full re-run after complete success: clean no-op, not an error.
        let mut rerun_buffer = Vec::new();
        fail_pending_offchain_order_command(
            &mut rerun_buffer,
            &pool,
            &symbol,
            order_id,
            "operator repair".parse().unwrap(),
        )
        .await
        .unwrap();
        let rerun_output = String::from_utf8(rerun_buffer).unwrap();
        assert!(
            rerun_output.contains("already terminal"),
            "unexpected output: {rerun_output}"
        );
    }

    /// An order id that belongs to a different symbol must be refused -- the
    /// pointer-already-clear branch must not fail an unrelated symbol's live
    /// order.
    #[tokio::test]
    async fn fail_pending_refuses_order_belonging_to_another_symbol() {
        let pool = setup_test_db().await;
        let symbol_a = Symbol::new("MSTR").unwrap();
        let symbol_b = Symbol::new("TSLA").unwrap();
        let order_id = OffchainOrderId::new();

        // Symbol A has a position with a clear pointer; the order belongs to B.
        seed_pending_position(&pool, &symbol_a, OffchainOrderId::new()).await;
        let (position, _projection) = StoreBuilder::<Position>::new(pool.clone())
            .build(())
            .await
            .unwrap();
        let view = {
            let (_pos, projection) = StoreBuilder::<Position>::new(pool.clone())
                .build(())
                .await
                .unwrap();
            projection.load(&symbol_a).await.unwrap().unwrap()
        };
        let pointed = view.pending_offchain_order_id.unwrap();
        position
            .send(
                &symbol_a,
                PositionCommand::FailOffChainOrder {
                    offchain_order_id: pointed,
                    error: "clears the pointer".to_string(),
                    anchor: AnchorDisposition::Preserve,
                },
            )
            .await
            .unwrap();
        seed_offchain_order(&pool, order_id, &symbol_b).await;

        let mut stdout_buffer = Vec::new();
        let error = fail_pending_offchain_order_command(
            &mut stdout_buffer,
            &pool,
            &symbol_a,
            order_id,
            "operator repair".parse().unwrap(),
        )
        .await
        .unwrap_err();

        assert!(
            error.to_string().contains("belongs to"),
            "expected a symbol-mismatch refusal; got: {error}"
        );

        // Symbol B's order must be untouched.
        let order = st0x_event_sorcery::load_entity::<OffchainOrder>(&pool, &order_id)
            .await
            .unwrap()
            .unwrap();
        assert!(
            matches!(order, OffchainOrder::Submitted { .. }),
            "symbol B's order must remain Submitted, got {order:?}"
        );
    }

    /// A PartiallyFilled order has real executed shares behind it: the repair
    /// must refuse to erase that hedge, leaving both aggregates untouched.
    #[tokio::test]
    async fn fail_pending_refuses_partially_filled_order() {
        let pool = setup_test_db().await;
        let symbol = Symbol::new("MSTR").unwrap();
        let order_id = OffchainOrderId::new();
        seed_pending_position(&pool, &symbol, order_id).await;
        seed_offchain_order(&pool, order_id, &symbol).await;

        st0x_event_sorcery::send_command::<OffchainOrder>(
            &pool,
            &order_id,
            OffchainOrderCommand::UpdatePartialFill {
                shares_filled: FractionalShares::new(float!(0.25)),
                avg_price: Usd::new(float!(100)),
                partially_filled_at: chrono::Utc::now(),
            },
            repair_order_placer(),
        )
        .await
        .unwrap();

        let mut stdout_buffer = Vec::new();
        let error = fail_pending_offchain_order_command(
            &mut stdout_buffer,
            &pool,
            &symbol,
            order_id,
            "operator repair".parse().unwrap(),
        )
        .await
        .unwrap_err();

        assert!(
            error.to_string().contains("PartiallyFilled"),
            "expected a partial-fill refusal; got: {error}"
        );

        // Neither aggregate was touched: order still PartiallyFilled, pointer
        // still set.
        let order = st0x_event_sorcery::load_entity::<OffchainOrder>(&pool, &order_id)
            .await
            .unwrap()
            .unwrap();
        assert!(
            matches!(order, OffchainOrder::PartiallyFilled { .. }),
            "order must remain PartiallyFilled, got {order:?}"
        );
        let (_position, projection) = StoreBuilder::<Position>::new(pool.clone())
            .build(())
            .await
            .unwrap();
        assert_eq!(
            projection
                .load(&symbol)
                .await
                .unwrap()
                .unwrap()
                .pending_offchain_order_id,
            Some(order_id),
            "position pointer must remain set when the repair is refused",
        );
    }

    /// A clear pointer with no OffchainOrder aggregate at all (likely a typo'd
    /// id) must refuse rather than silently succeed.
    #[tokio::test]
    async fn fail_pending_refuses_when_pointer_clear_and_no_order_exists() {
        let pool = setup_test_db().await;
        let symbol = Symbol::new("MSTR").unwrap();
        let order_id = OffchainOrderId::new();
        seed_pending_position(&pool, &symbol, order_id).await;

        let (position, _projection) = StoreBuilder::<Position>::new(pool.clone())
            .build(())
            .await
            .unwrap();
        position
            .send(
                &symbol,
                PositionCommand::FailOffChainOrder {
                    offchain_order_id: order_id,
                    error: "clears the pointer".to_string(),
                    anchor: AnchorDisposition::Preserve,
                },
            )
            .await
            .unwrap();

        let mut stdout_buffer = Vec::new();
        let result = fail_pending_offchain_order_command(
            &mut stdout_buffer,
            &pool,
            &symbol,
            OffchainOrderId::new(),
            "operator repair".parse().unwrap(),
        )
        .await;

        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("nothing to repair"),
            "expected a nothing-to-repair refusal; got: {err_msg}"
        );
    }

    /// The pointer-clearing path must keep working when no OffchainOrder
    /// aggregate exists yet for the pointed-at order.
    #[tokio::test]
    async fn fail_pending_clears_pointer_when_no_order_aggregate_exists() {
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
            "operator repair".parse().unwrap(),
        )
        .await
        .unwrap();

        let (_position, projection) = StoreBuilder::<Position>::new(pool.clone())
            .build(())
            .await
            .unwrap();
        assert_eq!(
            projection
                .load(&symbol)
                .await
                .unwrap()
                .unwrap()
                .pending_offchain_order_id,
            None
        );
        let output = String::from_utf8(stdout_buffer).unwrap();
        assert!(
            output.contains("pointer cleared only"),
            "unexpected output: {output}"
        );
    }

    /// Re-running a fully successful pointer-only clear (the pointed-at order
    /// never had an OffchainOrder aggregate) must surface "nothing to repair":
    /// the first run clears the pointer, and the re-run with the SAME id finds
    /// the system already consistent with nothing left to fix.
    #[tokio::test]
    async fn fail_pending_rerun_after_pointer_only_clear_reports_nothing_to_repair() {
        let pool = setup_test_db().await;
        let symbol = Symbol::new("MSTR").unwrap();
        let order_id = OffchainOrderId::new();
        seed_pending_position(&pool, &symbol, order_id).await;

        // First run: clears the pointer; there is no aggregate to repair.
        let mut first_buffer = Vec::new();
        fail_pending_offchain_order_command(
            &mut first_buffer,
            &pool,
            &symbol,
            order_id,
            "operator repair".parse().unwrap(),
        )
        .await
        .unwrap();
        let first_output = String::from_utf8(first_buffer).unwrap();
        assert!(
            first_output.contains("pointer cleared only"),
            "unexpected first-run output: {first_output}"
        );

        // Re-run with the SAME id: pointer already clear, still no aggregate.
        let mut rerun_buffer = Vec::new();
        let error = fail_pending_offchain_order_command(
            &mut rerun_buffer,
            &pool,
            &symbol,
            order_id,
            "operator repair".parse().unwrap(),
        )
        .await
        .unwrap_err();
        assert!(
            error.to_string().contains("nothing to repair"),
            "expected a nothing-to-repair refusal on re-run; got: {error}"
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
            "operator repair".parse().unwrap(),
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
                        chain: Chain::Base,
                        tx_hash: TxHash::random(),
                        log_index: 0,
                    },
                    amount: FractionalShares::new(float!(1)),
                    direction: Direction::Buy,
                    price_usdc: float!(420),
                    block_timestamp: chrono::Utc::now(),
                    block_number: None,
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
            "operator repair".parse().unwrap(),
        )
        .await
        .unwrap_err();

        assert!(
            error.to_string().contains("nothing to repair"),
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
            "manual long correction".parse().unwrap(),
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
                        chain: Chain::Base,
                        tx_hash: TxHash::random(),
                        log_index: 0,
                    },
                    amount: FractionalShares::new(float!(5)),
                    direction: Direction::Buy,
                    price_usdc: float!(420),
                    block_timestamp: chrono::Utc::now(),
                    block_number: None,
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
            "manual short correction".parse().unwrap(),
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
            "manual long correction".parse().unwrap(),
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
            "manual long correction".parse().unwrap(),
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
            "manual rebalance completed".parse().unwrap(),
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

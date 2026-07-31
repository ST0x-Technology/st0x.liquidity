//! Repair CLI commands for manually recovering stuck local CQRS state.

use anyhow::{Context, bail};
use async_trait::async_trait;
use rain_math_float::Float;
use sqlx::SqlitePool;
use std::io::Write;
use std::sync::Arc;

use st0x_config::ExecutionThreshold;
use st0x_event_sorcery::{AggregateError, LifecycleError, StoreBuilder};
use st0x_execution::{
    CancellationOutcome, ExecutorOrderId, FractionalShares, LimitOrder, MarketOrder, Symbol,
};

use crate::offchain::order::{
    OffchainOrder, OffchainOrderCommand, OffchainOrderError, OffchainOrderId, OrderPlacementResult,
    OrderPlacer,
};
use crate::position::{Position, PositionCommand};

/// An [`OrderPlacer`] for repair commands that must never place or cancel an
/// order: `MarkFailed` is a pure terminal transition that never touches the
/// placer. Returns an error on the unreachable placement/cancellation paths
/// rather than panicking.
struct RepairOrderPlacer;

#[async_trait]
impl OrderPlacer for RepairOrderPlacer {
    async fn place_market_order(
        &self,
        _order: MarketOrder,
    ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>> {
        Err("repair must not place offchain orders; MarkFailed is terminal-only".into())
    }

    async fn place_limit_order(
        &self,
        _order: LimitOrder,
    ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>> {
        Err("repair must not place offchain orders; MarkFailed is terminal-only".into())
    }

    async fn cancel_order(
        &self,
        _executor_order_id: &ExecutorOrderId,
    ) -> Result<CancellationOutcome, Box<dyn std::error::Error + Send + Sync>> {
        Err("repair must not cancel offchain orders; MarkFailed is terminal-only".into())
    }
}

/// Fails a position's pending offchain order pointer and drives the orphaned
/// `OffchainOrder` aggregate to `Failed`.
///
/// Operates directly on the database: the operator must ensure the bot is not
/// concurrently driving the same order (per the recovery CLI execution-mode
/// contract), since a fill landing between the state read and the commands
/// here cannot be guarded against.
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

    let order = st0x_event_sorcery::load_entity::<OffchainOrder>(pool, &offchain_order_id)
        .await
        .context("failed to load offchain order aggregate")?;

    if let Some(existing) = &order {
        if existing.symbol() != symbol {
            bail!(
                "OffchainOrder {offchain_order_id} belongs to {}, not {symbol} -- refusing \
                 to repair",
                existing.symbol()
            );
        }
        match existing {
            OffchainOrder::PartiallyFilled { .. } => {
                bail!(
                    "OffchainOrder {offchain_order_id} is PartiallyFilled: shares already \
                     executed offchain, and failing it would erase that hedge from the \
                     position. Reconcile the partial fill first."
                );
            }
            OffchainOrder::Filled { .. } => {
                bail!(
                    "OffchainOrder {offchain_order_id} is Filled: the hedge executed. This \
                     command cannot repair a filled order -- reconcile the fill into the \
                     position instead of failing it."
                );
            }
            OffchainOrder::Cancelling { .. } | OffchainOrder::Cancelled { .. } => {
                bail!(
                    "OffchainOrder {offchain_order_id} is in a cancellation lifecycle state: \
                     this command fails stuck Pending/Submitted orders, not cancellations -- \
                     refusing. Confirm the intended recovery path for cancellation states."
                );
            }
            OffchainOrder::Pending { .. }
            | OffchainOrder::Submitted { .. }
            | OffchainOrder::Failed { .. } => {}
        }
    }

    match view.pending_offchain_order_id {
        Some(pending) if pending == offchain_order_id => {}
        Some(pending) => {
            bail!("position {symbol} pending offchain order is {pending}, not {offchain_order_id}");
        }
        // The pointer is already clear: either a partial prior run (pointer
        // cleared, order still live) or a completed one. Skip the Position
        // command and repair the orphaned order so re-runs are safe.
        None => {
            // Re-running a fully successful repair lands here. When the
            // pointed-at order never had an aggregate (a pointer-only clear),
            // there is nothing left to fix: the system is already consistent,
            // so "nothing to repair" is the intended terminal outcome rather
            // than a silent success.
            if order.is_none() {
                bail!(
                    "position {symbol} has no pending offchain order and no OffchainOrder \
                     aggregate {offchain_order_id} exists -- nothing to repair"
                );
            }

            writeln!(
                stdout,
                "Position {symbol} pointer already clear; repairing OffchainOrder \
                 {offchain_order_id}"
            )?;
            return fail_offchain_order_aggregate(stdout, pool, order, offchain_order_id, reason)
                .await;
        }
    }

    position
        .send(
            symbol,
            PositionCommand::FailOffChainOrder {
                offchain_order_id,
                error: reason.clone(),
            },
        )
        .await
        .context("failed to fail pending offchain order")?;

    // Pointer-first: the position's pending pointer is cleared above. Now drive
    // the OffchainOrder aggregate itself to its Failed terminal so it does not
    // linger as a live-looking order in the view. The two aggregates are not
    // transactionally atomic (separate CQRS boundaries); this second step is
    // idempotent -- an already-terminal or absent order is left as-is.
    fail_offchain_order_aggregate(stdout, pool, order, offchain_order_id, reason).await?;

    // Print the pointer-clear summary only after both steps succeed: if the
    // aggregate step errors (for example a fill detected on re-load), the
    // command exits non-zero without ever having shown the operator a success
    // line -- misleading mid-incident output that masks the cleared pointer.
    writeln!(
        stdout,
        "Failed pending offchain order {offchain_order_id} for {symbol}"
    )?;

    Ok(())
}

/// What the repair must do with a freshly re-loaded `OffchainOrder` state.
/// The "executed shares always escalate" rule is the load-bearing
/// financial-safety invariant of this command; classifying the state in one
/// place keeps the pre-send guard and the post-send `AlreadyCompleted`
/// recovery from encoding it differently and silently diverging.
enum ReloadOutcome {
    /// Executed shares present (`Filled`/`PartiallyFilled`): failing the order
    /// would erase a hedge the position no longer accounts for. The caller must
    /// refuse and route the operator to manual reconciliation.
    Escalate,
    /// Already `Failed`: a benign concurrent terminal transition. Report it and
    /// leave the existing failure record untouched.
    BenignTerminal,
    /// No executed shares and not terminal (`Pending`/`Submitted`/absent). The
    /// pre-send guard proceeds to `MarkFailed`; the post-`AlreadyCompleted` site
    /// treats it as an unreachable invariant violation.
    Proceed,
}

/// Single source of the executed-shares-escalate rule shared by both re-load
/// sites in [`fail_offchain_order_aggregate`].
fn classify_reloaded_state(state: Option<&OffchainOrder>) -> ReloadOutcome {
    use OffchainOrder::{
        Cancelled, Cancelling, Failed, Filled, PartiallyFilled, Pending, Submitted,
    };

    match state {
        // Executed shares (Filled/PartiallyFilled) would erase a hedge; and a
        // concurrent transition into a cancellation lifecycle state during a
        // fail must route to manual reconciliation rather than being failed
        // blind (a Cancelled order may carry a partial fill). Confirm the
        // intended recovery path for cancellation states.
        Some(Filled { .. } | PartiallyFilled { .. } | Cancelling { .. } | Cancelled { .. }) => {
            ReloadOutcome::Escalate
        }
        Some(Failed { .. }) => ReloadOutcome::BenignTerminal,
        Some(Pending { .. } | Submitted { .. }) | None => ReloadOutcome::Proceed,
    }
}

/// Drives the standalone `OffchainOrder` aggregate (pre-loaded by the caller)
/// to its `Failed` terminal via `MarkFailed`, after its position pointer has
/// been cleared. Routed through the wired store so `offchain_order_view`
/// updates immediately. Idempotent: an already-`Failed` or absent order is
/// reported and left untouched rather than erroring, so a partial prior run
/// can be re-run safely; `Filled`/`PartiallyFilled` orders are refused because
/// failing them would erase executed hedge shares.
async fn fail_offchain_order_aggregate<W: Write>(
    stdout: &mut W,
    pool: &SqlitePool,
    order: Option<OffchainOrder>,
    offchain_order_id: OffchainOrderId,
    reason: String,
) -> anyhow::Result<()> {
    use OffchainOrder::{
        Cancelled, Cancelling, Failed, Filled, PartiallyFilled, Pending, Submitted,
    };

    let Some(order) = order else {
        writeln!(
            stdout,
            "No OffchainOrder aggregate {offchain_order_id} found; pointer cleared only"
        )?;
        return Ok(());
    };

    match order {
        Failed { .. } => {
            writeln!(
                stdout,
                "OffchainOrder {offchain_order_id} already terminal; left as-is"
            )?;
            return Ok(());
        }
        // The caller refuses executed orders before clearing the pointer;
        // refuse here too so the invariant cannot rot if a new caller skips
        // that check.
        Filled { .. } | PartiallyFilled { .. } => {
            bail!(
                "OffchainOrder {offchain_order_id} has executed shares (state {order:?}) -- \
                 refusing to erase the executed hedge"
            );
        }
        Cancelling { .. } | Cancelled { .. } => {
            bail!(
                "OffchainOrder {offchain_order_id} is in a cancellation lifecycle state \
                 (state {order:?}): this command fails Pending/Submitted orders, not \
                 cancellations -- refusing. Confirm the intended recovery path."
            );
        }
        Pending { .. } | Submitted { .. } => {}
    }

    // Re-load immediately before sending: the caller's snapshot may be stale,
    // and MarkFailed is a legal transition from PartiallyFilled at the
    // aggregate level (the bot's own post-partial-fill rejection path needs
    // it), so a partial fill landing since the snapshot would otherwise be
    // erased SILENTLY. This narrows the race window to the load->send gap.
    //
    // CONTRACT: this command requires that the bot is not concurrently driving
    // this order (see the module docstring). Honoring that contract means no
    // event can land in the load->send gap, so the re-load is exact. The
    // defenses for a contract violation are best-effort and ASYMMETRIC:
    //   - a complete fill in the gap makes MarkFailed return AlreadyCompleted,
    //     which the post-send handler below escalates;
    //   - a PARTIAL fill in the gap does NOT -- MarkFailed succeeds from
    //     PartiallyFilled -- so it would be erased silently and is UNGUARDED.
    // That sliver is closed only by honoring the no-concurrent-bot contract;
    // there is no in-process guard for it because the aggregate must keep
    // MarkFailed legal from PartiallyFilled for the bot's own rejection path.
    let current = st0x_event_sorcery::load_entity::<OffchainOrder>(pool, &offchain_order_id)
        .await
        .context("failed to re-load offchain order before MarkFailed")?;
    match classify_reloaded_state(current.as_ref()) {
        ReloadOutcome::Escalate => {
            bail!(
                "OffchainOrder {offchain_order_id} acquired executed shares concurrently; \
                 the position pointer may already be cleared -- reconcile the position \
                 manually instead of failing the order"
            );
        }
        ReloadOutcome::BenignTerminal => {
            writeln!(
                stdout,
                "OffchainOrder {offchain_order_id} reached a terminal state concurrently; \
                 left as-is"
            )?;
            return Ok(());
        }
        ReloadOutcome::Proceed => {}
    }

    // The wired store (not bare send_command) so the offchain_order_view
    // projection updates immediately -- a stale 'Submitted' row in the view is
    // the very symptom this command exists to repair.
    let (store, _projection) = StoreBuilder::<OffchainOrder>::new(pool.clone())
        .build(Arc::new(RepairOrderPlacer))
        .await
        .context("failed to build offchain order store")?;
    let send_result = store
        .send(
            &offchain_order_id,
            OffchainOrderCommand::MarkFailed {
                error: reason,
                filled_shares: None,
                failed_at: chrono::Utc::now(),
            },
        )
        .await;

    match send_result {
        Ok(()) => {
            writeln!(
                stdout,
                "Also marked OffchainOrder {offchain_order_id} as failed"
            )?;
            Ok(())
        }
        // The bot can transition the order to a terminal state in the sliver
        // between the re-load above and this command; a concurrent FAIL is
        // equivalent to finding it terminal up front, but a concurrent FILL
        // means the pointer was cleared for an order that actually executed --
        // surface that as a hard error so the operator reconciles the
        // position instead of trusting a clean exit.
        Err(AggregateError::UserError(LifecycleError::Apply(
            OffchainOrderError::AlreadyCompleted,
        ))) => {
            let terminal =
                st0x_event_sorcery::load_entity::<OffchainOrder>(pool, &offchain_order_id)
                    .await
                    .context("failed to re-load offchain order after concurrent transition")?;
            match classify_reloaded_state(terminal.as_ref()) {
                // Executed shares always escalate: PartiallyFilled cannot
                // produce AlreadyCompleted today, but if it ever does, the
                // same pointer-cleared-without-accounting hazard applies.
                ReloadOutcome::Escalate => {
                    bail!(
                        "OffchainOrder {offchain_order_id} acquired executed shares \
                         concurrently: the position pointer was cleared without accounting \
                         the fill -- reconcile the position manually"
                    );
                }
                ReloadOutcome::BenignTerminal => {
                    writeln!(
                        stdout,
                        "OffchainOrder {offchain_order_id} reached a terminal state \
                         concurrently; left as-is"
                    )?;
                    Ok(())
                }
                // MarkFailed only returns AlreadyCompleted from a terminal
                // aggregate (Filled or Failed), so a non-terminal or absent
                // state here means the order regressed out of a terminal state
                // -- impossible under the append-only lifecycle. Bail loudly as
                // an invariant violation rather than silently reporting a clean
                // "left as-is".
                ReloadOutcome::Proceed => {
                    bail!(
                        "OffchainOrder {offchain_order_id} returned AlreadyCompleted from \
                         MarkFailed but re-loaded as a non-terminal state -- aggregate \
                         lifecycle invariant violated"
                    );
                }
            }
        }
        Err(error) => {
            Err(anyhow::Error::new(error).context("failed to mark offchain order failed"))
        }
    }
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
             run position release-hedge before setting position"
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
    use st0x_execution::{ClientOrderId, Direction, ExecutorOrderId, FractionalShares};
    use st0x_finance::Usd;
    use st0x_float_macro::float;
    use uuid::Uuid;

    use super::*;
    use crate::position::TradeId;
    use crate::test_utils::{positive_shares, setup_test_db};

    fn repair_order_placer() -> Arc<dyn OrderPlacer> {
        Arc::new(RepairOrderPlacer)
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
                kind: crate::offchain::order::CounterTradeOrderKind::Market,
            },
            crate::offchain::order::noop_order_placer(),
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
            crate::offchain::order::noop_order_placer(),
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
            "operator repair".to_string(),
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
            "operator repair".to_string(),
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

        let mut stdout_buffer = Vec::new();
        let error = fail_offchain_order_aggregate(
            &mut stdout_buffer,
            &pool,
            stale,
            order_id,
            "operator repair".to_string(),
        )
        .await
        .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("acquired executed shares concurrently"),
            "expected the concurrent-execution error; got: {error}"
        );
    }

    /// The in-contract guarantee for partial fills: a partial fill that lands
    /// before the pre-send re-load must be caught. MarkFailed is legal from
    /// PartiallyFilled at the aggregate level, so without the re-load the
    /// executed shares would be erased silently. The remaining load->send
    /// sliver is UNGUARDED by design -- it is closed only by honoring the
    /// no-concurrent-bot contract (documented at the re-load site), since the
    /// aggregate must keep MarkFailed legal from PartiallyFilled for the bot.
    #[tokio::test]
    async fn fail_offchain_order_aggregate_errors_on_concurrent_partial_fill() {
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
        let error = fail_offchain_order_aggregate(
            &mut stdout_buffer,
            &pool,
            stale,
            order_id,
            "operator repair".to_string(),
        )
        .await
        .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("acquired executed shares concurrently"),
            "expected the concurrent-execution error; got: {error}"
        );

        // The partial fill must survive untouched.
        let order = st0x_event_sorcery::load_entity::<OffchainOrder>(&pool, &order_id)
            .await
            .unwrap()
            .unwrap();
        assert!(
            matches!(order, OffchainOrder::PartiallyFilled { .. }),
            "the partial fill must not be erased, got {order:?}"
        );
    }

    /// The escalation classifier is the single source of the executed-shares
    /// rule shared by both re-load sites; every state must map to the right
    /// outcome so the two sites cannot silently diverge. Covers all three
    /// `ReloadOutcome` variants -- the branches the `AlreadyCompleted` recovery
    /// arm depends on but cannot exercise deterministically in situ.
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
            "operator repair".to_string(),
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

        let mut stdout_buffer = Vec::new();
        fail_offchain_order_aggregate(
            &mut stdout_buffer,
            &pool,
            stale,
            order_id,
            "operator repair".to_string(),
        )
        .await
        .unwrap();

        let output = String::from_utf8(stdout_buffer).unwrap();
        assert!(
            output.contains("reached a terminal state concurrently"),
            "unexpected output: {output}"
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
            "operator repair".to_string(),
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
            "operator repair".to_string(),
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
            "operator repair".to_string(),
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
            "operator repair".to_string(),
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
            "operator repair".to_string(),
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
            "operator repair".to_string(),
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
            "operator repair".to_string(),
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
            "operator repair".to_string(),
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
            "operator repair".to_string(),
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

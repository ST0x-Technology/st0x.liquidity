//! Position CQRS/ES aggregate for tracking per-symbol
//! onchain/offchain exposure.
//!
//! Accumulates long and short fills, tracks the net
//! position, and decides when the imbalance exceeds the
//! threshold to trigger an offsetting broker order.

use std::collections::BTreeSet;

use alloy::primitives::TxHash;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use rain_math_float::{Float, FloatError};
use serde::{Deserialize, Serialize};
use tracing::{debug, warn};

use st0x_config::ExecutionThreshold;
use st0x_event_sorcery::{DomainEvent, EventSourced, Table};
use st0x_execution::{
    Direction, ExecutorOrderId, FractionalShares, HasZero, Positive, SupportedExecutor, Symbol,
};
use st0x_finance::{Usd, Usdc};
use st0x_float_macro::float;
use st0x_float_serde::{DebugFloat, DebugOptionFloat};

use crate::offchain::order::OffchainOrderId;

#[derive(Clone, Serialize, Deserialize)]
pub struct Position {
    pub symbol: Symbol,
    pub net: FractionalShares,
    pub accumulated_long: FractionalShares,
    pub accumulated_short: FractionalShares,
    pub pending_offchain_order_id: Option<OffchainOrderId>,
    /// Idempotency anchor: the `OffchainOrderId` from the last failed
    /// placement that has not yet been followed by a successful fill.
    /// Subsequent placement attempts reuse this id as their broker-side
    /// `client_order_id` so the broker dedupes the duplicate submission
    /// when the first attempt's response was lost in flight (e.g. 5xx
    /// after the broker recorded the order).
    ///
    /// Cleared on any successful `OffChainOrderFilled` event so the next
    /// rebalance cycle gets a fresh idempotency key.
    #[serde(default)]
    pub last_failed_offchain_order_id: Option<OffchainOrderId>,
    /// The most recently applied onchain fill (single slot, ADR 0005).
    /// Retained as the zero-migration cross-upgrade bridge (ADR 0010): a
    /// fill applied under pre-`pending_acknowledged_trade_ids` code but
    /// left unmarked when the deploy restarts has no `OnChainFillApplied`
    /// event in its history, so the rebuilt set cannot guard it -- this
    /// slot, written by the unchanged `OnChainOrderFilled` evolve, still
    /// equals that trade and rejects its re-drive.
    #[serde(default)]
    pub last_acknowledged_trade_id: Option<TradeId>,
    /// Trade ids applied to the position but whose `OnChainTrade`
    /// acknowledgement marker is not yet settle-pruned (ADR 0010).
    /// Closes the cross-process / out-of-order double-count the single
    /// slot cannot: the slot only remembers the LAST applied trade, so a
    /// re-drive of an older fill after a newer one displaced the slot --
    /// reachable once serialization breaks (the process-tx CLI runs in a
    /// separate process from the bot) -- slips through. This set, fed by a
    /// dedicated `OnChainFillApplied` event in the same atomic batch as the
    /// position move and pruned by `SettleOnChainFill` after
    /// the marker is durable, rejects such a re-drive regardless of how
    /// many fills intervened. Bounded: empty at rest, one entry per
    /// in-flight job. `OnChainFillApplied` has zero pre-`ADR 0010`
    /// occurrences, so a full event replay rebuilds it empty rather than
    /// re-accumulating every historical trade id.
    #[serde(default)]
    pub pending_acknowledged_trade_ids: BTreeSet<TradeId>,
    /// Reorg reversals applied to the position but whose `OnChainTrade`
    /// reorg-acknowledgement marker is not yet settle-pruned (ADR 0012). The
    /// reorg twin of `pending_acknowledged_trade_ids`: the single slot
    /// `last_reorged_trade_id` only remembers the LAST reversed trade, so a
    /// crash-resumed re-drive of an older reorg -- after a newer reorg of the
    /// same symbol advanced the slot -- slips past the slot and double-reverses
    /// `net`. This set, fed by the `Reorged` evolve and pruned by `SettleReorg`
    /// once the reorg-ack marker is durable, rejects such a re-drive regardless
    /// of how many reorgs intervened. Bounded: empty at rest, one entry per
    /// in-flight reversal, and reorgs are rare.
    #[serde(default)]
    pub pending_reorged_trade_ids: BTreeSet<TradeId>,
    pub threshold: ExecutionThreshold,
    #[serde(
        serialize_with = "st0x_float_serde::serialize_option_float",
        deserialize_with = "st0x_float_serde::deserialize_option_float_from_number_or_string"
    )]
    pub last_price_usdc: Option<Float>,
    pub last_updated: Option<DateTime<Utc>>,
    /// Set when a reorg reversal has been applied to this position. Append-only
    /// marker -- the reversal is a new event, the original fill events are
    /// preserved, and `position_view` surfaces that a reorg struck without
    /// deleting history. Absent on positions persisted before the
    /// field existed, which is the never-reorged default.
    #[serde(default)]
    pub last_reorged_at: Option<DateTime<Utc>>,
    /// The most recently reversed fill (single slot). Retained as the
    /// zero-migration cross-upgrade bridge alongside `pending_reorged_trade_ids`
    /// (ADR 0012, mirroring ADR 0010 for fills): a reorg reversed under
    /// pre-`pending_reorged_trade_ids` code but left unsettled at the deploy
    /// restart has no set entry to guard it, so this slot -- still written by the
    /// `Reorged` evolve -- equals that trade and rejects its re-drive. The set
    /// closes the out-of-order re-drive the slot alone cannot, once a newer reorg
    /// of the same symbol advances the slot past the older trade.
    #[serde(default)]
    pub last_reorged_trade_id: Option<TradeId>,
}

impl std::fmt::Debug for Position {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Position")
            .field("symbol", &self.symbol)
            .field("net", &self.net)
            .field("accumulated_long", &self.accumulated_long)
            .field("accumulated_short", &self.accumulated_short)
            .field("pending_offchain_order_id", &self.pending_offchain_order_id)
            .field(
                "last_failed_offchain_order_id",
                &self.last_failed_offchain_order_id,
            )
            .field(
                "last_acknowledged_trade_id",
                &self.last_acknowledged_trade_id,
            )
            .field(
                "pending_acknowledged_trade_ids",
                &self.pending_acknowledged_trade_ids,
            )
            .field("pending_reorged_trade_ids", &self.pending_reorged_trade_ids)
            .field("threshold", &self.threshold)
            .field("last_price_usdc", &DebugOptionFloat(&self.last_price_usdc))
            .field("last_updated", &self.last_updated)
            .field("last_reorged_at", &self.last_reorged_at)
            .field("last_reorged_trade_id", &self.last_reorged_trade_id)
            .finish()
    }
}

#[async_trait]
impl EventSourced for Position {
    type Id = Symbol;
    type Event = PositionEvent;
    type Command = PositionCommand;
    type Error = PositionError;
    type Services = ();
    type Materialized = Table;

    const AGGREGATE_TYPE: &'static str = "Position";
    const PROJECTION: Table = Table("position_view");
    const SCHEMA_VERSION: u64 = 5;

    fn originate(event: &Self::Event) -> Option<Self> {
        use PositionEvent::*;
        match event {
            Initialized {
                symbol,
                threshold,
                initialized_at,
            } => Some(Self {
                symbol: symbol.clone(),
                net: FractionalShares::ZERO,
                accumulated_long: FractionalShares::ZERO,
                accumulated_short: FractionalShares::ZERO,
                pending_offchain_order_id: None,
                last_failed_offchain_order_id: None,
                last_acknowledged_trade_id: None,
                pending_acknowledged_trade_ids: BTreeSet::new(),
                pending_reorged_trade_ids: BTreeSet::new(),
                threshold: *threshold,
                last_price_usdc: None,
                last_updated: Some(*initialized_at),
                last_reorged_at: None,
                last_reorged_trade_id: None,
            }),

            _ => None,
        }
    }

    fn evolve(entity: &Self, event: &Self::Event) -> Result<Option<Self>, Self::Error> {
        use Direction::{Buy, Sell};
        use PositionEvent::*;

        match event {
            OnChainOrderFilled {
                trade_id,
                amount,
                direction: Buy,
                price_usdc,
                seen_at,
                ..
            } => Ok(Some(Self {
                net: (entity.net + *amount)?,
                accumulated_long: (entity.accumulated_long + *amount)?,
                last_acknowledged_trade_id: Some(trade_id.clone()),
                last_price_usdc: Some(*price_usdc),
                last_updated: Some(*seen_at),
                ..entity.clone()
            })),

            OnChainOrderFilled {
                trade_id,
                direction: Sell,
                amount,
                price_usdc,
                seen_at,
                ..
            } => Ok(Some(Self {
                net: (entity.net - *amount)?,
                accumulated_short: (entity.accumulated_short + *amount)?,
                last_acknowledged_trade_id: Some(trade_id.clone()),
                last_price_usdc: Some(*price_usdc),
                last_updated: Some(*seen_at),
                ..entity.clone()
            })),

            // Bookkeeping only (ADR 0010): track the applied fill in the
            // pending-acknowledgement set without touching net or
            // last_updated -- the position move already happened in the
            // `OnChainOrderFilled` of the same atomic batch.
            OnChainFillApplied { trade_id, .. } => {
                let mut pending_acknowledged_trade_ids =
                    entity.pending_acknowledged_trade_ids.clone();
                pending_acknowledged_trade_ids.insert(trade_id.clone());
                Ok(Some(Self {
                    pending_acknowledged_trade_ids,
                    ..entity.clone()
                }))
            }

            OnChainFillSettled { trade_id, .. } => {
                let mut pending_acknowledged_trade_ids =
                    entity.pending_acknowledged_trade_ids.clone();
                pending_acknowledged_trade_ids.remove(trade_id);
                Ok(Some(Self {
                    pending_acknowledged_trade_ids,
                    ..entity.clone()
                }))
            }

            // A reorg reverses the original fill's impact exactly: undo the net
            // delta and the accumulator the fill grew. The fill events stay in
            // history; this reversal is appended on top.
            //
            // Clear `last_acknowledged_trade_id` only when it points at the trade
            // being reorged, and remove the reorged trade_id from
            // `pending_acknowledged_trade_ids`. The fill-dedupe guard absorbs
            // crash-window replay of the same event, not a permanent tombstone: if
            // this exact `(tx_hash, log_index)` re-confirms on the canonical chain
            // after the reorg resolves, `AcknowledgeOnChainFill` must accept it
            // instead of rejecting it as a `DuplicateTrade`. That guard ORs both
            // slots, so the bounded pending set must be pruned too -- clearing only
            // the anchor would leave the trade_id in the set and keep rejecting the
            // re-confirmation. Clearing the anchor unconditionally would instead
            // drop a more recent fill's anchor when an older fill is reorged,
            // reopening that newer fill's crash-window replay. Set
            // `last_reorged_trade_id` and insert into `pending_reorged_trade_ids`
            // so a re-driven reorg of the same trade is rejected -- the slot
            // covers the most-recent re-drive, the bounded set covers an
            // out-of-order one after a newer reorg advanced the slot (ADR 0012).
            Reorged {
                trade_id,
                direction: Buy,
                amount,
                reorged_at,
                ..
            } => {
                let mut pending_acknowledged_trade_ids =
                    entity.pending_acknowledged_trade_ids.clone();
                pending_acknowledged_trade_ids.remove(trade_id);
                let mut pending_reorged_trade_ids = entity.pending_reorged_trade_ids.clone();
                pending_reorged_trade_ids.insert(trade_id.clone());
                Ok(Some(Self {
                    net: (entity.net - *amount)?,
                    accumulated_long: (entity.accumulated_long - *amount)?,
                    last_reorged_at: Some(*reorged_at),
                    last_acknowledged_trade_id: entity
                        .last_acknowledged_trade_id
                        .clone()
                        .filter(|id| id != trade_id),
                    last_reorged_trade_id: Some(trade_id.clone()),
                    pending_acknowledged_trade_ids,
                    pending_reorged_trade_ids,
                    last_updated: Some(*reorged_at),
                    ..entity.clone()
                }))
            }

            Reorged {
                trade_id,
                direction: Sell,
                amount,
                reorged_at,
                ..
            } => {
                let mut pending_acknowledged_trade_ids =
                    entity.pending_acknowledged_trade_ids.clone();
                pending_acknowledged_trade_ids.remove(trade_id);
                let mut pending_reorged_trade_ids = entity.pending_reorged_trade_ids.clone();
                pending_reorged_trade_ids.insert(trade_id.clone());
                Ok(Some(Self {
                    net: (entity.net + *amount)?,
                    accumulated_short: (entity.accumulated_short - *amount)?,
                    last_reorged_at: Some(*reorged_at),
                    last_acknowledged_trade_id: entity
                        .last_acknowledged_trade_id
                        .clone()
                        .filter(|id| id != trade_id),
                    last_reorged_trade_id: Some(trade_id.clone()),
                    pending_acknowledged_trade_ids,
                    pending_reorged_trade_ids,
                    last_updated: Some(*reorged_at),
                    ..entity.clone()
                }))
            }

            // Bookkeeping only (ADR 0012): prune the settled reversal from the
            // pending-reorg set without touching net -- the reversal already
            // happened in the `Reorged` event. Mirrors `OnChainFillSettled`.
            ReorgSettled { trade_id, .. } => {
                let mut pending_reorged_trade_ids = entity.pending_reorged_trade_ids.clone();
                pending_reorged_trade_ids.remove(trade_id);
                Ok(Some(Self {
                    pending_reorged_trade_ids,
                    ..entity.clone()
                }))
            }

            OffChainOrderPlaced { .. } if entity.pending_offchain_order_id.is_some() => Ok(None),

            OffChainOrderPlaced {
                offchain_order_id,
                placed_at,
                ..
            } => Ok(Some(Self {
                pending_offchain_order_id: Some(*offchain_order_id),
                last_updated: Some(*placed_at),
                ..entity.clone()
            })),

            OffChainOrderFilled {
                offchain_order_id, ..
            } if entity.pending_offchain_order_id != Some(*offchain_order_id) => Ok(None),

            OffChainOrderFilled {
                direction: Buy,
                shares_filled,
                broker_timestamp,
                ..
            } => Ok(Some(Self {
                net: (entity.net + shares_filled.inner())?,
                pending_offchain_order_id: None,
                // Successful fill: drop the failed-attempt anchor so the
                // next rebalance cycle gets a fresh idempotency key.
                last_failed_offchain_order_id: None,
                last_updated: Some(*broker_timestamp),
                ..entity.clone()
            })),

            OffChainOrderFilled {
                direction: Sell,
                shares_filled,
                broker_timestamp,
                ..
            } => Ok(Some(Self {
                net: (entity.net - shares_filled.inner())?,
                pending_offchain_order_id: None,
                last_failed_offchain_order_id: None,
                last_updated: Some(*broker_timestamp),
                ..entity.clone()
            })),

            OffChainOrderFailed {
                offchain_order_id, ..
            } if entity.pending_offchain_order_id != Some(*offchain_order_id) => Ok(None),

            OffChainOrderFailed {
                offchain_order_id,
                failed_at,
                ..
            } => Ok(Some(Self {
                pending_offchain_order_id: None,
                // Stash the failed OID so the next placement attempt can
                // reuse it as `client_order_id` and let the broker dedupe.
                //
                // Preserve the *first* failed anchor across a chain of
                // failures: the broker recorded the original attempt under
                // that key, so a later attempt whose own response was also
                // lost must keep deduping against the original key. Overwriting
                // with each new OID would point the next retry at a key the
                // broker never saw, double-submitting the order. Cleared only
                // by a successful fill.
                last_failed_offchain_order_id: entity
                    .last_failed_offchain_order_id
                    .or(Some(*offchain_order_id)),
                last_updated: Some(*failed_at),
                ..entity.clone()
            })),

            ThresholdUpdated {
                new_threshold,
                updated_at,
                ..
            } => Ok(Some(Self {
                threshold: *new_threshold,
                last_updated: Some(*updated_at),
                ..entity.clone()
            })),

            ManualPositionAdjusted {
                target_net,
                price_usdc,
                adjusted_at,
                ..
            } => Ok(Some(Self {
                net: *target_net,
                last_price_usdc: (*price_usdc).or(entity.last_price_usdc),
                last_updated: Some(*adjusted_at),
                // A manual reconciliation supersedes any prior failed hedge, so
                // clear the broker-idempotency anchor. Otherwise the next hedge
                // could reuse the failed order's client_order_id and be deduped
                // by the broker against a stale order.
                last_failed_offchain_order_id: None,
                ..entity.clone()
            })),

            Initialized { .. } => Ok(None),
        }
    }

    async fn initialize(
        command: Self::Command,
        _services: &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        use PositionCommand::*;
        match command {
            AcknowledgeOnChainFill {
                symbol,
                threshold,
                trade_id,
                amount,
                direction,
                price_usdc,
                block_timestamp,
            } => {
                let now = Utc::now();
                Ok(vec![
                    PositionEvent::Initialized {
                        symbol,
                        threshold,
                        initialized_at: now,
                    },
                    PositionEvent::OnChainOrderFilled {
                        trade_id: trade_id.clone(),
                        amount,
                        direction,
                        price_usdc,
                        block_timestamp,
                        seen_at: now,
                    },
                    PositionEvent::OnChainFillApplied {
                        trade_id,
                        applied_at: now,
                    },
                ])
            }

            // Pruning a fill or reorg the position never applied/reversed is a
            // no-op (ADR 0010 fills, ADR 0012 reorgs); never initialize the
            // aggregate just to settle an absent trade.
            SettleOnChainFill { .. } | SettleReorg { .. } => Ok(vec![]),

            ManuallyAdjustPosition {
                symbol,
                target_net,
                reason,
                threshold,
                expected_net,
                price_usdc,
            } => {
                Self::validate_manual_adjustment(
                    expected_net,
                    FractionalShares::ZERO,
                    target_net,
                    &threshold,
                    None,
                    price_usdc,
                )?;

                let now = Utc::now();

                warn!(
                    target: "hedge",
                    %symbol, target_net = %target_net, %reason,
                    "Manually adjusted uninitialized position"
                );

                Ok(vec![
                    PositionEvent::Initialized {
                        symbol,
                        threshold,
                        initialized_at: now,
                    },
                    PositionEvent::ManualPositionAdjusted {
                        previous_net: FractionalShares::ZERO,
                        target_net,
                        reason,
                        price_usdc,
                        adjusted_at: now,
                    },
                ])
            }

            _ => Err(PositionError::Uninitialized),
        }
    }

    async fn transition(
        &self,
        command: Self::Command,
        _services: &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        use PositionCommand::*;
        match command {
            AcknowledgeOnChainFill {
                trade_id,
                amount,
                direction,
                price_usdc,
                block_timestamp,
                ..
            } => {
                // Dual guard (ADR 0010): the retained slot bridges a fill
                // applied under pre-set code and left unmarked at the
                // deploy restart; the set rejects an out-of-order /
                // cross-process re-drive the single slot cannot, because a
                // newer fill displaces the slot but never the set entry.
                if self.last_acknowledged_trade_id.as_ref() == Some(&trade_id)
                    || self.pending_acknowledged_trade_ids.contains(&trade_id)
                {
                    return Err(PositionError::DuplicateTrade { trade_id });
                }

                let now = Utc::now();
                Ok(vec![
                    PositionEvent::OnChainOrderFilled {
                        trade_id: trade_id.clone(),
                        amount,
                        direction,
                        price_usdc,
                        block_timestamp,
                        seen_at: now,
                    },
                    PositionEvent::OnChainFillApplied {
                        trade_id,
                        applied_at: now,
                    },
                ])
            }

            SettleOnChainFill { trade_id } => {
                if self.pending_acknowledged_trade_ids.contains(&trade_id) {
                    Ok(vec![PositionEvent::OnChainFillSettled {
                        trade_id,
                        settled_at: Utc::now(),
                    }])
                } else {
                    Ok(vec![])
                }
            }

            RecordReorg {
                trade_id,
                amount,
                direction,
                price_usdc,
                reorg_depth,
            } => {
                // A reversal must undo a real (strictly positive) fill amount.
                // Reject a zero or negative amount rather than appending a
                // reversal that would corrupt the net position.
                Positive::new(amount)
                    .map_err(|_| PositionError::NonPositiveReorgAmount { amount })?;

                // Dual guard (ADR 0012, mirroring ADR 0010 for fills): the
                // retained slot bridges a reorg reversed under pre-set code and
                // left unsettled at the deploy restart; the set rejects an
                // out-of-order re-drive the single slot cannot, because a newer
                // reorg advances the slot but never the set entry.
                if self.last_reorged_trade_id.as_ref() == Some(&trade_id)
                    || self.pending_reorged_trade_ids.contains(&trade_id)
                {
                    warn!(
                        target: "hedge",
                        symbol = %self.symbol, %trade_id,
                        "Rejecting re-driven reorg: fill already reversed on this position"
                    );
                    return Err(PositionError::DuplicateReorg { trade_id });
                }

                warn!(
                    target: "hedge",
                    symbol = %self.symbol,
                    %trade_id,
                    ?direction,
                    %amount,
                    reorg_depth,
                    "Reversing position impact of a reorged fill"
                );

                Ok(vec![PositionEvent::Reorged {
                    trade_id,
                    amount,
                    direction,
                    price_usdc: Some(price_usdc),
                    reorg_depth,
                    reorged_at: Utc::now(),
                }])
            }

            SettleReorg { trade_id } => {
                if self.pending_reorged_trade_ids.contains(&trade_id) {
                    Ok(vec![PositionEvent::ReorgSettled {
                        trade_id,
                        settled_at: Utc::now(),
                    }])
                } else {
                    Ok(vec![])
                }
            }

            PlaceOffChainOrder {
                offchain_order_id,
                shares,
                direction,
                executor,
                ..
            } => {
                if let Some(pending) = self.pending_offchain_order_id {
                    return Err(PositionError::PendingExecution {
                        offchain_order_id: pending,
                    });
                }

                let trigger_reason = self
                    .create_trigger_reason(&self.threshold)?
                    .ok_or(PositionError::ThresholdNotMet {
                        net_position: self.net,
                        threshold: self.threshold,
                    })
                    .inspect_err(|error| {
                        warn!(
                            target: "hedge",
                            %offchain_order_id, symbol = %self.symbol,
                            "Order placement rejected: {error}",
                        );
                    })?;

                Ok(vec![PositionEvent::OffChainOrderPlaced {
                    offchain_order_id,
                    shares,
                    direction,
                    executor,
                    trigger_reason,
                    placed_at: Utc::now(),
                }])
            }

            CompleteOffChainOrder {
                offchain_order_id,
                shares_filled,
                direction,
                executor_order_id,
                price,
                broker_timestamp,
            } => {
                self.validate_pending_execution(offchain_order_id)?;

                Ok(vec![PositionEvent::OffChainOrderFilled {
                    offchain_order_id,
                    shares_filled,
                    direction,
                    executor_order_id,
                    price,
                    broker_timestamp,
                }])
            }

            FailOffChainOrder {
                offchain_order_id,
                error,
            } => {
                self.validate_pending_execution(offchain_order_id)?;

                warn!(
                    target: "hedge",
                    %offchain_order_id, symbol = %self.symbol, %error,
                    "Offchain venue rejected"
                );

                Ok(vec![PositionEvent::OffChainOrderFailed {
                    offchain_order_id,
                    error,
                    failed_at: Utc::now(),
                }])
            }

            UpdateThreshold { threshold } => Ok(vec![PositionEvent::ThresholdUpdated {
                old_threshold: self.threshold,
                new_threshold: threshold,
                updated_at: Utc::now(),
            }]),

            ManuallyAdjustPosition {
                target_net,
                reason,
                expected_net,
                price_usdc,
                ..
            } => {
                if let Some(pending) = self.pending_offchain_order_id {
                    return Err(PositionError::ManualAdjustmentBlockedByPendingExecution {
                        offchain_order_id: pending,
                    });
                }

                Self::validate_manual_adjustment(
                    expected_net,
                    self.net,
                    target_net,
                    &self.threshold,
                    self.last_price_usdc,
                    price_usdc,
                )?;

                warn!(
                    target: "hedge",
                    symbol = %self.symbol,
                    previous_net = %self.net,
                    target_net = %target_net,
                    %reason,
                    "Manually adjusted position"
                );

                Ok(vec![PositionEvent::ManualPositionAdjusted {
                    previous_net: self.net,
                    target_net,
                    reason,
                    price_usdc,
                    adjusted_at: Utc::now(),
                }])
            }
        }
    }
}

impl Position {
    fn create_trigger_reason(
        &self,
        threshold: &ExecutionThreshold,
    ) -> Result<Option<TriggerReason>, PositionError> {
        match threshold {
            ExecutionThreshold::Shares(threshold_shares) => {
                let net_abs = self.net.abs()?;
                let threshold_value = threshold_shares.inner().inner();
                let meets_threshold = net_abs.inner().gte(threshold_value)?;
                Ok(meets_threshold.then_some(TriggerReason::SharesThreshold {
                    net_position_shares: net_abs.inner(),
                    threshold_shares: threshold_value,
                }))
            }
            ExecutionThreshold::DollarValue(threshold_dollars) => {
                let Some(price) = self.last_price_usdc else {
                    debug!(
                        target: "hedge",
                        net_position = %self.net,
                        threshold_dollars = %threshold_dollars,
                        "Cannot evaluate ExecutionThreshold::DollarValue: last_price_usdc is None"
                    );
                    return Ok(None);
                };

                let net_abs = self.net.abs()?;
                let dollar_value = (Usdc::new(price) * net_abs.inner())?;

                if dollar_value.inner().gte(threshold_dollars.inner())? {
                    Ok(Some(TriggerReason::DollarThreshold {
                        net_position_shares: self.net.inner(),
                        dollar_value: dollar_value.inner(),
                        price_usdc: price,
                        threshold_dollars: threshold_dollars.inner(),
                    }))
                } else {
                    Ok(None)
                }
            }
        }
    }

    fn validate_pending_execution(
        &self,
        offchain_order_id: OffchainOrderId,
    ) -> Result<(), PositionError> {
        let Some(pending_id) = self.pending_offchain_order_id else {
            return Err(PositionError::NoPendingExecution);
        };

        if pending_id != offchain_order_id {
            return Err(PositionError::OffchainOrderIdMismatch {
                expected: pending_id,
                actual: offchain_order_id,
            });
        }

        Ok(())
    }

    /// Validates a manual position adjustment before emitting events.
    ///
    /// Enforces optimistic concurrency (the live net must still match what the
    /// operator observed) and ensures a nonzero target under a dollar-value
    /// threshold has a usable price, so the adjusted exposure can actually be
    /// hedged instead of stalling on a missing `last_price_usdc`.
    fn validate_manual_adjustment(
        expected_net: Option<FractionalShares>,
        current_net: FractionalShares,
        target_net: FractionalShares,
        threshold: &ExecutionThreshold,
        existing_price: Option<Float>,
        provided_price: Option<Float>,
    ) -> Result<(), PositionError> {
        if let Some(expected) = expected_net
            && expected != current_net
        {
            return Err(PositionError::ManualAdjustmentStateChanged {
                expected,
                actual: current_net,
            });
        }

        // The aggregate is the real domain boundary: a non-positive price
        // would value the repaired exposure at zero or a negative dollar
        // amount, stalling or corrupting dollar-threshold hedging. Reject the
        // price that will actually drive valuation (the provided override, or
        // the persisted price it falls back to).
        if let Some(price) = provided_price.or(existing_price)
            && !price.gt(float!(0))?
        {
            return Err(PositionError::ManualAdjustmentInvalidPrice {
                price: Usdc::new(price),
            });
        }

        let needs_price = match threshold {
            ExecutionThreshold::DollarValue(_) => {
                !target_net.is_zero()? && existing_price.is_none() && provided_price.is_none()
            }
            ExecutionThreshold::Shares(_) => false,
        };

        if needs_price {
            return Err(PositionError::ManualAdjustmentRequiresPrice { target_net });
        }

        Ok(())
    }

    /// Checks if this position is ready for execution
    /// based on its configured threshold.
    ///
    /// Returns `Ok(Some((direction, shares)))` if met:
    /// - `direction`: Sell for long, Buy for short
    /// - `shares`: Full fractional amount, optionally
    ///   capped by `shares_limit`
    ///
    /// Returns `Ok(None)` if threshold is not met or no
    /// price available for dollar-value threshold.
    ///
    /// Returns `Err` on arithmetic overflow.
    pub(crate) fn is_ready_for_execution(
        &self,
        shares_limit: Option<Positive<FractionalShares>>,
    ) -> Result<Option<(Direction, FractionalShares)>, PositionError> {
        if self.pending_offchain_order_id.is_some() {
            return Ok(None);
        }

        let trigger = self.create_trigger_reason(&self.threshold)?;

        match trigger {
            Some(TriggerReason::SharesThreshold { .. } | TriggerReason::DollarThreshold { .. }) => {
                let raw_shares = self.net.abs()?;

                let capped_shares = shares_limit.map_or(raw_shares, |cap| {
                    let cap = cap.inner();
                    if raw_shares > cap {
                        debug!(
                            target: "hedge",
                            symbol = %self.symbol,
                            computed = %raw_shares,
                            limit = %cap,
                            "Counter trade shares capped by operational limit"
                        );
                        cap
                    } else {
                        raw_shares
                    }
                });

                if capped_shares.is_zero()? {
                    return Ok(None);
                }

                let direction = if self.net.is_negative()? {
                    Direction::Buy
                } else {
                    Direction::Sell
                };

                Ok(Some((direction, capped_shares)))
            }
            None => Ok(None),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, thiserror::Error)]
pub enum PositionError {
    #[error("Position has not been initialized")]
    Uninitialized,
    #[error("Reorg reversal amount must be strictly positive, got {amount:?}")]
    NonPositiveReorgAmount { amount: FractionalShares },
    #[error(
        "Cannot place offchain order: position \
         {net_position:?} does not meet threshold \
         {threshold:?}"
    )]
    ThresholdNotMet {
        net_position: FractionalShares,
        threshold: ExecutionThreshold,
    },
    #[error(
        "Cannot place offchain order: already have \
         pending execution {offchain_order_id:?}"
    )]
    PendingExecution { offchain_order_id: OffchainOrderId },
    #[error(
        "Cannot acknowledge onchain fill: trade {trade_id} \
         was already applied to this position"
    )]
    DuplicateTrade { trade_id: TradeId },
    #[error(
        "Cannot record reorg: trade {trade_id} \
         was already reversed on this position"
    )]
    DuplicateReorg { trade_id: TradeId },
    #[error(
        "Cannot manually adjust position: already have \
         pending execution {offchain_order_id:?}"
    )]
    ManualAdjustmentBlockedByPendingExecution { offchain_order_id: OffchainOrderId },
    #[error(
        "Cannot manually adjust position: expected net \
         {expected:?} but live net is {actual:?}; reload \
         and retry"
    )]
    ManualAdjustmentStateChanged {
        expected: FractionalShares,
        actual: FractionalShares,
    },
    #[error(
        "Cannot manually adjust nonzero position \
         {target_net:?} under a dollar-value threshold \
         without a price; provide --price"
    )]
    ManualAdjustmentRequiresPrice { target_net: FractionalShares },
    #[error(
        "Cannot manually adjust position with non-positive price \
         {price:?}; the price must be strictly positive to value the \
         repaired exposure"
    )]
    ManualAdjustmentInvalidPrice { price: Usdc },
    #[error("Cannot complete offchain order: no pending execution")]
    NoPendingExecution,
    #[error(
        "Offchain order ID mismatch: expected \
         {expected:?}, got {actual:?}"
    )]
    OffchainOrderIdMismatch {
        expected: OffchainOrderId,
        actual: OffchainOrderId,
    },
    // Stores the error as String rather than the typed FloatError because
    // PositionError must implement Serialize/Deserialize (it's a CQRS error
    // type stored in the event log), and FloatError from rain_float does not
    // implement those traits. This is a conscious trade-off, not an oversight.
    #[error("Float arithmetic error: {0}")]
    Float(String),
}

// FloatError cannot be stored directly in PositionError (see comment on the
// Float variant above), so we convert to String here at the boundary.
impl From<FloatError> for PositionError {
    fn from(error: FloatError) -> Self {
        Self::Float(error.to_string())
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub enum PositionCommand {
    AcknowledgeOnChainFill {
        symbol: Symbol,
        threshold: ExecutionThreshold,
        trade_id: TradeId,
        amount: FractionalShares,
        direction: Direction,
        price_usdc: Float,
        block_timestamp: DateTime<Utc>,
    },
    /// Prunes `trade_id` from the pending-acknowledgement set after its
    /// `OnChainTrade` marker is durable (ADR 0010). A no-op (emits no
    /// events) when the trade is not in the set, so a re-driven prune is
    /// idempotent and optimistic-concurrency-safe.
    SettleOnChainFill {
        trade_id: TradeId,
    },
    /// Reverses an onchain fill that a reorg invalidated. Carries the original
    /// fill's `amount`, `direction`, and `price_usdc` so the reversal undoes
    /// exactly its position impact and the reactor can reverse the inventory's
    /// equity and USDC legs; `reorg_depth` is retained for the audit trail.
    RecordReorg {
        trade_id: TradeId,
        amount: FractionalShares,
        direction: Direction,
        price_usdc: Float,
        reorg_depth: u64,
    },
    /// Prunes `trade_id` from `pending_reorged_trade_ids` after its
    /// `OnChainTrade` reorg-ack marker is durable (ADR 0012). A no-op (emits no
    /// events) when the trade is not in the set, so a re-driven prune is
    /// idempotent and optimistic-concurrency-safe.
    SettleReorg {
        trade_id: TradeId,
    },
    PlaceOffChainOrder {
        offchain_order_id: OffchainOrderId,
        shares: Positive<FractionalShares>,
        direction: Direction,
        executor: SupportedExecutor,
        threshold: ExecutionThreshold,
    },
    CompleteOffChainOrder {
        offchain_order_id: OffchainOrderId,
        shares_filled: Positive<FractionalShares>,
        direction: Direction,
        executor_order_id: ExecutorOrderId,
        price: Usd,
        broker_timestamp: DateTime<Utc>,
    },
    FailOffChainOrder {
        offchain_order_id: OffchainOrderId,
        error: String,
    },
    UpdateThreshold {
        threshold: ExecutionThreshold,
    },
    ManuallyAdjustPosition {
        symbol: Symbol,
        target_net: FractionalShares,
        reason: String,
        threshold: ExecutionThreshold,
        /// Net exposure the operator observed; rejects the command if the live
        /// aggregate has since changed. `None` skips the concurrency check.
        expected_net: Option<FractionalShares>,
        /// USDC price per share to record, required for nonzero adjustments
        /// under a dollar-value threshold when no price is already known.
        price_usdc: Option<Float>,
    },
}

#[derive(Clone, Serialize, Deserialize)]
pub enum PositionEvent {
    Initialized {
        symbol: Symbol,
        threshold: ExecutionThreshold,
        initialized_at: DateTime<Utc>,
    },
    OnChainOrderFilled {
        trade_id: TradeId,
        amount: FractionalShares,
        direction: Direction,
        #[serde(
            serialize_with = "st0x_float_serde::serialize_float_as_string",
            deserialize_with = "st0x_float_serde::deserialize_float_from_number_or_string"
        )]
        price_usdc: Float,
        block_timestamp: DateTime<Utc>,
        seen_at: DateTime<Utc>,
    },
    /// Records that `trade_id` was applied to the position, in the same
    /// atomic batch as the `OnChainOrderFilled` move (ADR 0010). A
    /// dedicated event -- not folded into `OnChainOrderFilled` -- so that
    /// pre-`ADR 0010` history (which has none) rebuilds the pending set
    /// empty on full replay instead of re-accumulating every trade id.
    OnChainFillApplied {
        trade_id: TradeId,
        applied_at: DateTime<Utc>,
    },
    /// Prunes `trade_id` from the pending-acknowledgement set once its
    /// `OnChainTrade` marker is durable (ADR 0010).
    OnChainFillSettled {
        trade_id: TradeId,
        settled_at: DateTime<Utc>,
    },
    Reorged {
        trade_id: TradeId,
        amount: FractionalShares,
        direction: Direction,
        /// `Option` for replay safety: a `Reorged` event persisted before the
        /// reactor carried the price has no `price_usdc`, and
        /// deserializes to `None`. The reactor falls back to nudging an equity
        /// check for those rather than reversing the inventory's USDC leg with a
        /// fabricated price.
        #[serde(
            default,
            serialize_with = "st0x_float_serde::serialize_option_float",
            deserialize_with = "st0x_float_serde::deserialize_option_float_from_number_or_string"
        )]
        price_usdc: Option<Float>,
        reorg_depth: u64,
        reorged_at: DateTime<Utc>,
    },
    /// Prunes `trade_id` from `pending_reorged_trade_ids` once its `OnChainTrade`
    /// reorg-ack marker is durable (ADR 0012). The reorg twin of
    /// `OnChainFillSettled`.
    ReorgSettled {
        trade_id: TradeId,
        settled_at: DateTime<Utc>,
    },
    OffChainOrderPlaced {
        offchain_order_id: OffchainOrderId,
        shares: Positive<FractionalShares>,
        direction: Direction,
        executor: SupportedExecutor,
        trigger_reason: TriggerReason,
        placed_at: DateTime<Utc>,
    },
    OffChainOrderFilled {
        offchain_order_id: OffchainOrderId,
        shares_filled: Positive<FractionalShares>,
        direction: Direction,
        executor_order_id: ExecutorOrderId,
        price: Usd,
        broker_timestamp: DateTime<Utc>,
    },
    OffChainOrderFailed {
        offchain_order_id: OffchainOrderId,
        error: String,
        failed_at: DateTime<Utc>,
    },
    ThresholdUpdated {
        old_threshold: ExecutionThreshold,
        new_threshold: ExecutionThreshold,
        updated_at: DateTime<Utc>,
    },
    ManualPositionAdjusted {
        previous_net: FractionalShares,
        target_net: FractionalShares,
        reason: String,
        #[serde(
            default,
            serialize_with = "st0x_float_serde::serialize_option_float",
            deserialize_with = "st0x_float_serde::deserialize_option_float_from_number_or_string"
        )]
        price_usdc: Option<Float>,
        adjusted_at: DateTime<Utc>,
    },
}

impl PositionEvent {
    pub(crate) fn timestamp(&self) -> DateTime<Utc> {
        use PositionEvent::*;
        match self {
            Initialized { initialized_at, .. } => *initialized_at,
            OnChainOrderFilled { seen_at, .. } => *seen_at,
            OnChainFillApplied { applied_at, .. } => *applied_at,
            OnChainFillSettled { settled_at, .. } | ReorgSettled { settled_at, .. } => *settled_at,
            Reorged { reorged_at, .. } => *reorged_at,
            OffChainOrderPlaced { placed_at, .. } => *placed_at,
            OffChainOrderFilled {
                broker_timestamp, ..
            } => *broker_timestamp,
            OffChainOrderFailed { failed_at, .. } => *failed_at,
            ThresholdUpdated { updated_at, .. } => *updated_at,
            ManualPositionAdjusted { adjusted_at, .. } => *adjusted_at,
        }
    }
}

impl DomainEvent for PositionEvent {
    fn event_type(&self) -> String {
        use PositionEvent::*;
        match self {
            Initialized { .. } => "PositionEvent::Initialized".to_string(),
            OnChainOrderFilled { .. } => "PositionEvent::OnChainOrderFilled".to_string(),
            OnChainFillApplied { .. } => "PositionEvent::OnChainFillApplied".to_string(),
            OnChainFillSettled { .. } => "PositionEvent::OnChainFillSettled".to_string(),
            Reorged { .. } => "PositionEvent::Reorged".to_string(),
            ReorgSettled { .. } => "PositionEvent::ReorgSettled".to_string(),
            OffChainOrderPlaced { .. } => "PositionEvent::OffChainOrderPlaced".to_string(),
            OffChainOrderFilled { .. } => "PositionEvent::OffChainOrderFilled".to_string(),
            OffChainOrderFailed { .. } => "PositionEvent::OffChainOrderFailed".to_string(),
            ThresholdUpdated { .. } => "PositionEvent::ThresholdUpdated".to_string(),
            ManualPositionAdjusted { .. } => "PositionEvent::ManualPositionAdjusted".to_string(),
        }
    }

    fn event_version(&self) -> String {
        "1.0".to_string()
    }
}

/// Compares two optional `Float` prices, treating both-absent as equal.
/// `Float` has no `PartialEq`, so this routes through its fallible `eq`.
fn option_float_eq(left: Option<Float>, right: Option<Float>) -> bool {
    match (left, right) {
        (None, None) => true,
        (Some(left), Some(right)) => left.eq(right).unwrap_or(false),
        _ => false,
    }
}

/// Required by `cqrs_es::DomainEvent`.
impl PartialEq for PositionEvent {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (
                Self::Initialized {
                    symbol: s1,
                    threshold: t1,
                    initialized_at: i1,
                },
                Self::Initialized {
                    symbol: s2,
                    threshold: t2,
                    initialized_at: i2,
                },
            ) => s1 == s2 && t1 == t2 && i1 == i2,
            (
                Self::OnChainOrderFilled {
                    trade_id: t1,
                    amount: a1,
                    direction: d1,
                    price_usdc: p1,
                    block_timestamp: bt1,
                    seen_at: sa1,
                },
                Self::OnChainOrderFilled {
                    trade_id: t2,
                    amount: a2,
                    direction: d2,
                    price_usdc: p2,
                    block_timestamp: bt2,
                    seen_at: sa2,
                },
            ) => {
                t1 == t2
                    && a1 == a2
                    && d1 == d2
                    && p1.eq(*p2).unwrap_or(false)
                    && bt1 == bt2
                    && sa1 == sa2
            }
            (
                Self::OnChainFillApplied {
                    trade_id: t1,
                    applied_at: a1,
                },
                Self::OnChainFillApplied {
                    trade_id: t2,
                    applied_at: a2,
                },
            ) => t1 == t2 && a1 == a2,
            (
                Self::OnChainFillSettled {
                    trade_id: t1,
                    settled_at: c1,
                },
                Self::OnChainFillSettled {
                    trade_id: t2,
                    settled_at: c2,
                },
            ) => t1 == t2 && c1 == c2,
            (
                Self::Reorged {
                    trade_id: left_trade_id,
                    amount: left_amount,
                    direction: left_direction,
                    price_usdc: left_price_usdc,
                    reorg_depth: left_reorg_depth,
                    reorged_at: left_reorged_at,
                },
                Self::Reorged {
                    trade_id: right_trade_id,
                    amount: right_amount,
                    direction: right_direction,
                    price_usdc: right_price_usdc,
                    reorg_depth: right_reorg_depth,
                    reorged_at: right_reorged_at,
                },
            ) => {
                left_trade_id == right_trade_id
                    && left_amount == right_amount
                    && left_direction == right_direction
                    && option_float_eq(*left_price_usdc, *right_price_usdc)
                    && left_reorg_depth == right_reorg_depth
                    && left_reorged_at == right_reorged_at
            }
            (
                Self::ReorgSettled {
                    trade_id: left_trade_id,
                    settled_at: left_settled_at,
                },
                Self::ReorgSettled {
                    trade_id: right_trade_id,
                    settled_at: right_settled_at,
                },
            ) => left_trade_id == right_trade_id && left_settled_at == right_settled_at,
            (
                Self::OffChainOrderPlaced {
                    offchain_order_id: o1,
                    shares: s1,
                    direction: d1,
                    executor: e1,
                    trigger_reason: tr1,
                    placed_at: pa1,
                },
                Self::OffChainOrderPlaced {
                    offchain_order_id: o2,
                    shares: s2,
                    direction: d2,
                    executor: e2,
                    trigger_reason: tr2,
                    placed_at: pa2,
                },
            ) => o1 == o2 && s1 == s2 && d1 == d2 && e1 == e2 && tr1 == tr2 && pa1 == pa2,
            (
                Self::OffChainOrderFilled {
                    offchain_order_id: o1,
                    shares_filled: sf1,
                    direction: d1,
                    executor_order_id: eo1,
                    price: p1,
                    broker_timestamp: bt1,
                },
                Self::OffChainOrderFilled {
                    offchain_order_id: o2,
                    shares_filled: sf2,
                    direction: d2,
                    executor_order_id: eo2,
                    price: p2,
                    broker_timestamp: bt2,
                },
            ) => o1 == o2 && sf1 == sf2 && d1 == d2 && eo1 == eo2 && p1 == p2 && bt1 == bt2,
            (
                Self::OffChainOrderFailed {
                    offchain_order_id: o1,
                    error: e1,
                    failed_at: f1,
                },
                Self::OffChainOrderFailed {
                    offchain_order_id: o2,
                    error: e2,
                    failed_at: f2,
                },
            ) => o1 == o2 && e1 == e2 && f1 == f2,
            (
                Self::ThresholdUpdated {
                    old_threshold: ot1,
                    new_threshold: nt1,
                    updated_at: u1,
                },
                Self::ThresholdUpdated {
                    old_threshold: ot2,
                    new_threshold: nt2,
                    updated_at: u2,
                },
            ) => ot1 == ot2 && nt1 == nt2 && u1 == u2,
            (
                Self::ManualPositionAdjusted {
                    previous_net: pn1,
                    target_net: tn1,
                    reason: r1,
                    price_usdc: pr1,
                    adjusted_at: a1,
                },
                Self::ManualPositionAdjusted {
                    previous_net: pn2,
                    target_net: tn2,
                    reason: r2,
                    price_usdc: pr2,
                    adjusted_at: a2,
                },
            ) => pn1 == pn2 && tn1 == tn2 && r1 == r2 && option_float_eq(*pr1, *pr2) && a1 == a2,
            _ => false,
        }
    }
}

impl Eq for PositionEvent {}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct TradeId {
    pub tx_hash: TxHash,
    pub log_index: u64,
}

impl std::fmt::Display for TradeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.tx_hash, self.log_index)
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub enum TriggerReason {
    SharesThreshold {
        #[serde(
            serialize_with = "st0x_float_serde::serialize_float_as_string",
            deserialize_with = "st0x_float_serde::deserialize_float_from_number_or_string"
        )]
        net_position_shares: Float,
        #[serde(
            serialize_with = "st0x_float_serde::serialize_float_as_string",
            deserialize_with = "st0x_float_serde::deserialize_float_from_number_or_string"
        )]
        threshold_shares: Float,
    },
    DollarThreshold {
        #[serde(
            serialize_with = "st0x_float_serde::serialize_float_as_string",
            deserialize_with = "st0x_float_serde::deserialize_float_from_number_or_string"
        )]
        net_position_shares: Float,
        #[serde(
            serialize_with = "st0x_float_serde::serialize_float_as_string",
            deserialize_with = "st0x_float_serde::deserialize_float_from_number_or_string"
        )]
        dollar_value: Float,
        #[serde(
            serialize_with = "st0x_float_serde::serialize_float_as_string",
            deserialize_with = "st0x_float_serde::deserialize_float_from_number_or_string"
        )]
        price_usdc: Float,
        #[serde(
            serialize_with = "st0x_float_serde::serialize_float_as_string",
            deserialize_with = "st0x_float_serde::deserialize_float_from_number_or_string"
        )]
        threshold_dollars: Float,
    },
}

/// Required by `cqrs_es::DomainEvent` (via `PositionEvent`).
impl PartialEq for TriggerReason {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (
                Self::SharesThreshold {
                    net_position_shares: n1,
                    threshold_shares: t1,
                },
                Self::SharesThreshold {
                    net_position_shares: n2,
                    threshold_shares: t2,
                },
            ) => n1.eq(*n2).unwrap_or(false) && t1.eq(*t2).unwrap_or(false),
            (
                Self::DollarThreshold {
                    net_position_shares: n1,
                    dollar_value: d1,
                    price_usdc: p1,
                    threshold_dollars: t1,
                },
                Self::DollarThreshold {
                    net_position_shares: n2,
                    dollar_value: d2,
                    price_usdc: p2,
                    threshold_dollars: t2,
                },
            ) => {
                n1.eq(*n2).unwrap_or(false)
                    && d1.eq(*d2).unwrap_or(false)
                    && p1.eq(*p2).unwrap_or(false)
                    && t1.eq(*t2).unwrap_or(false)
            }
            _ => false,
        }
    }
}

impl Eq for TriggerReason {}

impl std::fmt::Debug for PositionCommand {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AcknowledgeOnChainFill {
                symbol,
                threshold,
                trade_id,
                amount,
                direction,
                price_usdc,
                block_timestamp,
            } => f
                .debug_struct("AcknowledgeOnChainFill")
                .field("symbol", symbol)
                .field("threshold", threshold)
                .field("trade_id", trade_id)
                .field("amount", amount)
                .field("direction", direction)
                .field("price_usdc", &DebugFloat(price_usdc))
                .field("block_timestamp", block_timestamp)
                .finish(),
            Self::SettleOnChainFill { trade_id } => f
                .debug_struct("SettleOnChainFill")
                .field("trade_id", trade_id)
                .finish(),
            Self::RecordReorg {
                trade_id,
                amount,
                direction,
                price_usdc,
                reorg_depth,
            } => f
                .debug_struct("RecordReorg")
                .field("trade_id", trade_id)
                .field("amount", amount)
                .field("direction", direction)
                .field("price_usdc", &DebugFloat(price_usdc))
                .field("reorg_depth", reorg_depth)
                .finish(),
            Self::SettleReorg { trade_id } => f
                .debug_struct("SettleReorg")
                .field("trade_id", trade_id)
                .finish(),
            Self::PlaceOffChainOrder {
                offchain_order_id,
                shares,
                direction,
                executor,
                threshold,
            } => f
                .debug_struct("PlaceOffChainOrder")
                .field("offchain_order_id", offchain_order_id)
                .field("shares", shares)
                .field("direction", direction)
                .field("executor", executor)
                .field("threshold", threshold)
                .finish(),
            Self::CompleteOffChainOrder {
                offchain_order_id,
                shares_filled,
                direction,
                executor_order_id,
                price,
                broker_timestamp,
            } => f
                .debug_struct("CompleteOffChainOrder")
                .field("offchain_order_id", offchain_order_id)
                .field("shares_filled", shares_filled)
                .field("direction", direction)
                .field("executor_order_id", executor_order_id)
                .field("price", price)
                .field("broker_timestamp", broker_timestamp)
                .finish(),
            Self::FailOffChainOrder {
                offchain_order_id,
                error,
            } => f
                .debug_struct("FailOffChainOrder")
                .field("offchain_order_id", offchain_order_id)
                .field("error", error)
                .finish(),
            Self::UpdateThreshold { threshold } => f
                .debug_struct("UpdateThreshold")
                .field("threshold", threshold)
                .finish(),
            Self::ManuallyAdjustPosition {
                symbol,
                target_net,
                reason,
                threshold,
                expected_net,
                price_usdc,
            } => f
                .debug_struct("ManuallyAdjustPosition")
                .field("symbol", symbol)
                .field("target_net", target_net)
                .field("reason", reason)
                .field("threshold", threshold)
                .field("expected_net", expected_net)
                .field("price_usdc", &DebugOptionFloat(price_usdc))
                .finish(),
        }
    }
}

impl std::fmt::Debug for PositionEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use st0x_float_serde::DebugFloat;

        match self {
            Self::Initialized {
                symbol,
                threshold,
                initialized_at,
            } => f
                .debug_struct("Initialized")
                .field("symbol", symbol)
                .field("threshold", threshold)
                .field("initialized_at", initialized_at)
                .finish(),
            Self::OnChainOrderFilled {
                trade_id,
                amount,
                direction,
                price_usdc,
                block_timestamp,
                seen_at,
            } => f
                .debug_struct("OnChainOrderFilled")
                .field("trade_id", trade_id)
                .field("amount", amount)
                .field("direction", direction)
                .field("price_usdc", &DebugFloat(price_usdc))
                .field("block_timestamp", block_timestamp)
                .field("seen_at", seen_at)
                .finish(),
            Self::OnChainFillApplied {
                trade_id,
                applied_at,
            } => f
                .debug_struct("OnChainFillApplied")
                .field("trade_id", trade_id)
                .field("applied_at", applied_at)
                .finish(),
            Self::OnChainFillSettled {
                trade_id,
                settled_at,
            } => f
                .debug_struct("OnChainFillSettled")
                .field("trade_id", trade_id)
                .field("settled_at", settled_at)
                .finish(),
            Self::Reorged {
                trade_id,
                amount,
                direction,
                price_usdc,
                reorg_depth,
                reorged_at,
            } => f
                .debug_struct("Reorged")
                .field("trade_id", trade_id)
                .field("amount", amount)
                .field("direction", direction)
                .field("price_usdc", &DebugOptionFloat(price_usdc))
                .field("reorg_depth", reorg_depth)
                .field("reorged_at", reorged_at)
                .finish(),
            Self::ReorgSettled {
                trade_id,
                settled_at,
            } => f
                .debug_struct("ReorgSettled")
                .field("trade_id", trade_id)
                .field("settled_at", settled_at)
                .finish(),
            Self::OffChainOrderPlaced {
                offchain_order_id,
                shares,
                direction,
                executor,
                trigger_reason,
                placed_at,
            } => f
                .debug_struct("OffChainOrderPlaced")
                .field("offchain_order_id", offchain_order_id)
                .field("shares", shares)
                .field("direction", direction)
                .field("executor", executor)
                .field("trigger_reason", trigger_reason)
                .field("placed_at", placed_at)
                .finish(),
            Self::OffChainOrderFilled {
                offchain_order_id,
                shares_filled,
                direction,
                executor_order_id,
                price,
                broker_timestamp,
            } => f
                .debug_struct("OffChainOrderFilled")
                .field("offchain_order_id", offchain_order_id)
                .field("shares_filled", shares_filled)
                .field("direction", direction)
                .field("executor_order_id", executor_order_id)
                .field("price", price)
                .field("broker_timestamp", broker_timestamp)
                .finish(),
            Self::OffChainOrderFailed {
                offchain_order_id,
                error,
                failed_at,
            } => f
                .debug_struct("OffChainOrderFailed")
                .field("offchain_order_id", offchain_order_id)
                .field("error", error)
                .field("failed_at", failed_at)
                .finish(),
            Self::ThresholdUpdated {
                old_threshold,
                new_threshold,
                updated_at,
            } => f
                .debug_struct("ThresholdUpdated")
                .field("old_threshold", old_threshold)
                .field("new_threshold", new_threshold)
                .field("updated_at", updated_at)
                .finish(),
            Self::ManualPositionAdjusted {
                previous_net,
                target_net,
                reason,
                price_usdc,
                adjusted_at,
            } => f
                .debug_struct("ManualPositionAdjusted")
                .field("previous_net", previous_net)
                .field("target_net", target_net)
                .field("reason", reason)
                .field("price_usdc", &DebugOptionFloat(price_usdc))
                .field("adjusted_at", adjusted_at)
                .finish(),
        }
    }
}

impl std::fmt::Debug for TriggerReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use st0x_float_serde::DebugFloat;

        match self {
            Self::SharesThreshold {
                net_position_shares,
                threshold_shares,
            } => f
                .debug_struct("SharesThreshold")
                .field("net_position_shares", &DebugFloat(net_position_shares))
                .field("threshold_shares", &DebugFloat(threshold_shares))
                .finish(),
            Self::DollarThreshold {
                net_position_shares,
                dollar_value,
                price_usdc,
                threshold_dollars,
            } => f
                .debug_struct("DollarThreshold")
                .field("net_position_shares", &DebugFloat(net_position_shares))
                .field("dollar_value", &DebugFloat(dollar_value))
                .field("price_usdc", &DebugFloat(price_usdc))
                .field("threshold_dollars", &DebugFloat(threshold_dollars))
                .finish(),
        }
    }
}

#[cfg(test)]
mod tests {
    use st0x_execution::Positive;

    use st0x_event_sorcery::{LifecycleError, StoreBuilder, TestHarness, replay};

    use st0x_finance::Usdc;

    use super::*;
    use st0x_float_macro::float;

    fn one_share_threshold() -> ExecutionThreshold {
        ExecutionThreshold::shares(Positive::new(FractionalShares::new(float!(1))).unwrap())
    }

    #[tokio::test]
    async fn first_fill_initializes_and_accumulates() {
        let events = TestHarness::<Position>::with(())
            .given_no_previous_events()
            .when(PositionCommand::AcknowledgeOnChainFill {
                symbol: Symbol::new("AAPL").unwrap(),
                threshold: one_share_threshold(),
                trade_id: TradeId {
                    tx_hash: TxHash::random(),
                    log_index: 1,
                },
                amount: FractionalShares::new(float!(0.5)),
                direction: Direction::Buy,
                price_usdc: float!(150),
                block_timestamp: Utc::now(),
            })
            .await
            .events();

        assert_eq!(
            events.len(),
            3,
            "Expected Initialized + OnChainOrderFilled + OnChainFillApplied"
        );
        assert!(matches!(events[0], PositionEvent::Initialized { .. }));
        assert!(matches!(
            events[1],
            PositionEvent::OnChainOrderFilled { .. }
        ));
        assert!(matches!(
            events[2],
            PositionEvent::OnChainFillApplied { .. }
        ));
    }

    #[tokio::test]
    async fn acknowledge_onchain_fill_accumulates_position() {
        let threshold = one_share_threshold();

        let events = TestHarness::<Position>::with(())
            .given(vec![PositionEvent::Initialized {
                symbol: Symbol::new("AAPL").unwrap(),
                threshold,
                initialized_at: Utc::now(),
            }])
            .when(PositionCommand::AcknowledgeOnChainFill {
                symbol: Symbol::new("AAPL").unwrap(),
                threshold,
                trade_id: TradeId {
                    tx_hash: TxHash::random(),
                    log_index: 1,
                },
                amount: FractionalShares::new(float!(0.5)),
                direction: Direction::Buy,
                price_usdc: float!(150),
                block_timestamp: Utc::now(),
            })
            .await
            .events();

        assert_eq!(
            events.len(),
            2,
            "Expected OnChainOrderFilled + OnChainFillApplied"
        );
        assert!(matches!(
            events[0],
            PositionEvent::OnChainOrderFilled { .. }
        ));
        assert!(matches!(
            events[1],
            PositionEvent::OnChainFillApplied { .. }
        ));
    }

    #[tokio::test]
    async fn record_reorg_on_filled_position_emits_reorged_event() {
        let symbol = Symbol::new("AAPL").unwrap();
        let now = Utc::now();
        let trade_id = TradeId {
            tx_hash: TxHash::random(),
            log_index: 1,
        };

        let events = TestHarness::<Position>::with(())
            .given(vec![
                PositionEvent::Initialized {
                    symbol,
                    threshold: one_share_threshold(),
                    initialized_at: now,
                },
                PositionEvent::OnChainOrderFilled {
                    trade_id: trade_id.clone(),
                    amount: FractionalShares::new(float!(5)),
                    direction: Direction::Buy,
                    price_usdc: float!(150),
                    block_timestamp: now,
                    seen_at: now,
                },
            ])
            .when(PositionCommand::RecordReorg {
                trade_id: trade_id.clone(),
                amount: FractionalShares::new(float!(5)),
                direction: Direction::Buy,
                price_usdc: float!(150),
                reorg_depth: 2,
            })
            .await
            .events();

        let [
            PositionEvent::Reorged {
                trade_id: reorged_trade_id,
                amount,
                direction,
                price_usdc,
                reorg_depth,
                ..
            },
        ] = events.as_slice()
        else {
            panic!("RecordReorg must emit exactly one Reorged event; got {events:?}");
        };

        assert_eq!(
            *reorged_trade_id, trade_id,
            "the reversal must target the reorged fill"
        );
        assert_eq!(
            *amount,
            FractionalShares::new(float!(5)),
            "amount drives the reversal"
        );
        assert_eq!(
            *direction,
            Direction::Buy,
            "direction drives the reversal sign"
        );
        assert_eq!(*reorg_depth, 2);
        assert!(
            option_float_eq(*price_usdc, Some(float!(150))),
            "the reorged event must carry the fill price for the USDC reversal"
        );
    }

    #[tokio::test]
    async fn record_reorg_with_non_positive_amount_is_rejected() {
        let symbol = Symbol::new("AAPL").unwrap();
        let now = Utc::now();
        let trade_id = TradeId {
            tx_hash: TxHash::random(),
            log_index: 1,
        };

        let error = TestHarness::<Position>::with(())
            .given(vec![
                PositionEvent::Initialized {
                    symbol,
                    threshold: one_share_threshold(),
                    initialized_at: now,
                },
                PositionEvent::OnChainOrderFilled {
                    trade_id: trade_id.clone(),
                    amount: FractionalShares::new(float!(5)),
                    direction: Direction::Buy,
                    price_usdc: float!(150),
                    block_timestamp: now,
                    seen_at: now,
                },
            ])
            .when(PositionCommand::RecordReorg {
                trade_id,
                amount: FractionalShares::ZERO,
                direction: Direction::Buy,
                price_usdc: float!(150),
                reorg_depth: 2,
            })
            .await
            .then_expect_error();

        assert!(
            matches!(
                error,
                LifecycleError::Apply(PositionError::NonPositiveReorgAmount { .. })
            ),
            "a zero reorg amount must be rejected; got {error:?}",
        );
    }

    /// A reorg of a buy fill must return `net` and `accumulated_long` to the
    /// exact pre-fill state while preserving the original fill events.
    #[test]
    fn reorg_reverses_buy_fill_to_pre_fill_state() {
        let symbol = Symbol::new("AAPL").unwrap();
        let now = Utc::now();
        let trade_id = TradeId {
            tx_hash: TxHash::random(),
            log_index: 1,
        };

        let position = replay::<Position>(vec![
            PositionEvent::Initialized {
                symbol,
                threshold: one_share_threshold(),
                initialized_at: now,
            },
            PositionEvent::OnChainOrderFilled {
                trade_id: trade_id.clone(),
                amount: FractionalShares::new(float!(5)),
                direction: Direction::Buy,
                price_usdc: float!(150),
                block_timestamp: now,
                seen_at: now,
            },
            PositionEvent::Reorged {
                trade_id,
                amount: FractionalShares::new(float!(5)),
                direction: Direction::Buy,
                price_usdc: Some(float!(150)),
                reorg_depth: 2,
                reorged_at: now,
            },
        ])
        .unwrap()
        .expect("replay must produce a live position");

        assert_eq!(position.net, FractionalShares::ZERO);
        assert_eq!(position.accumulated_long, FractionalShares::ZERO);
        assert_eq!(position.accumulated_short, FractionalShares::ZERO);
        assert_eq!(position.last_reorged_at, Some(now));
    }

    /// The sell-side mirror: reversing a sell returns `net` to zero and undoes
    /// the `accumulated_short` the fill grew.
    #[test]
    fn reorg_reverses_sell_fill_to_pre_fill_state() {
        let symbol = Symbol::new("AAPL").unwrap();
        let now = Utc::now();
        let trade_id = TradeId {
            tx_hash: TxHash::random(),
            log_index: 1,
        };

        let position = replay::<Position>(vec![
            PositionEvent::Initialized {
                symbol,
                threshold: one_share_threshold(),
                initialized_at: now,
            },
            PositionEvent::OnChainOrderFilled {
                trade_id: trade_id.clone(),
                amount: FractionalShares::new(float!(5)),
                direction: Direction::Sell,
                price_usdc: float!(150),
                block_timestamp: now,
                seen_at: now,
            },
            PositionEvent::Reorged {
                trade_id,
                amount: FractionalShares::new(float!(5)),
                direction: Direction::Sell,
                price_usdc: Some(float!(150)),
                reorg_depth: 1,
                reorged_at: now,
            },
        ])
        .unwrap()
        .expect("replay must produce a live position");

        assert_eq!(position.net, FractionalShares::ZERO);
        assert_eq!(position.accumulated_short, FractionalShares::ZERO);
        assert_eq!(position.accumulated_long, FractionalShares::ZERO);
        assert_eq!(position.last_reorged_at, Some(now));
    }

    /// `evolve` reverses a `Reorged` by its `amount`/`direction`, NOT by
    /// `trade_id` (which is audit metadata the aggregate does not match on). A
    /// 5-share buy reversed out of two buys (5 + 3) leaves the 3-share net --
    /// the reversal matches the amount, not the named trade.
    #[test]
    fn reorg_reverses_by_amount_and_direction_not_trade_id() {
        let symbol = Symbol::new("AAPL").unwrap();
        let now = Utc::now();
        let reorged_trade = TradeId {
            tx_hash: TxHash::random(),
            log_index: 1,
        };
        let surviving_trade = TradeId {
            tx_hash: TxHash::random(),
            log_index: 2,
        };

        let position = replay::<Position>(vec![
            PositionEvent::Initialized {
                symbol,
                threshold: one_share_threshold(),
                initialized_at: now,
            },
            PositionEvent::OnChainOrderFilled {
                trade_id: reorged_trade.clone(),
                amount: FractionalShares::new(float!(5)),
                direction: Direction::Buy,
                price_usdc: float!(150),
                block_timestamp: now,
                seen_at: now,
            },
            PositionEvent::OnChainOrderFilled {
                trade_id: surviving_trade,
                amount: FractionalShares::new(float!(3)),
                direction: Direction::Buy,
                price_usdc: float!(150),
                block_timestamp: now,
                seen_at: now,
            },
            PositionEvent::Reorged {
                trade_id: reorged_trade,
                amount: FractionalShares::new(float!(5)),
                direction: Direction::Buy,
                price_usdc: Some(float!(150)),
                reorg_depth: 1,
                reorged_at: now,
            },
        ])
        .unwrap()
        .expect("replay must produce a live position");

        assert_eq!(position.net, FractionalShares::new(float!(3)));
        assert_eq!(position.accumulated_long, FractionalShares::new(float!(3)));
    }

    #[tokio::test]
    async fn reorged_fill_can_be_re_acknowledged_after_canonical_reconfirmation() {
        // The fill-dedupe guard absorbs crash-window replay but must not
        // permanently tombstone a fill: if the same (tx, log) re-confirms on the
        // canonical chain after the reorg resolves, re-acknowledgement must
        // succeed, not be rejected as a DuplicateTrade.
        //
        // The seeded history is the real post-ADR-0010 production state:
        // `AcknowledgeOnChainFill` always emits `OnChainOrderFilled` +
        // `OnChainFillApplied` as an atomic pair, so the applied trade_id sits
        // in `pending_acknowledged_trade_ids`. The `Reorged` evolve must remove
        // it from that set (mirroring `OnChainFillSettled`); otherwise the dedup
        // guard's set check would keep rejecting the canonical re-confirmation.
        let symbol = Symbol::new("AAPL").unwrap();
        let now = Utc::now();
        let trade_id = TradeId {
            tx_hash: TxHash::random(),
            log_index: 1,
        };
        let amount = FractionalShares::new(float!(5));

        let events = TestHarness::<Position>::with(())
            .given(vec![
                PositionEvent::Initialized {
                    symbol: symbol.clone(),
                    threshold: one_share_threshold(),
                    initialized_at: now,
                },
                PositionEvent::OnChainOrderFilled {
                    trade_id: trade_id.clone(),
                    amount,
                    direction: Direction::Buy,
                    price_usdc: float!(150),
                    block_timestamp: now,
                    seen_at: now,
                },
                PositionEvent::OnChainFillApplied {
                    trade_id: trade_id.clone(),
                    applied_at: now,
                },
                PositionEvent::Reorged {
                    trade_id: trade_id.clone(),
                    amount,
                    direction: Direction::Buy,
                    price_usdc: Some(float!(150)),
                    reorg_depth: 1,
                    reorged_at: now,
                },
            ])
            .when(PositionCommand::AcknowledgeOnChainFill {
                symbol,
                threshold: one_share_threshold(),
                trade_id,
                amount,
                direction: Direction::Buy,
                price_usdc: float!(150),
                block_timestamp: now,
            })
            .await
            .events();

        assert!(
            matches!(
                events.as_slice(),
                [
                    PositionEvent::OnChainOrderFilled { .. },
                    PositionEvent::OnChainFillApplied { .. }
                ]
            ),
            "re-confirmation of a reorged fill must be accepted, not rejected as \
             DuplicateTrade; got {events:?}",
        );
    }

    #[tokio::test]
    async fn reorging_an_older_fill_preserves_a_newer_fills_dedupe_anchor() {
        // Reversing one fill must not reopen another fill's crash-window guard.
        // After two fills are acknowledged, reorging the OLDER one must leave the
        // NEWER fill's dedupe anchor intact, so a re-delivered acknowledge of the
        // newer fill is still rejected as a duplicate rather than double-counted.
        let symbol = Symbol::new("AAPL").unwrap();
        let now = Utc::now();
        let threshold = one_share_threshold();
        let older = TradeId {
            tx_hash: TxHash::random(),
            log_index: 1,
        };
        let newer = TradeId {
            tx_hash: TxHash::random(),
            log_index: 2,
        };
        let amount = FractionalShares::new(float!(5));

        let error = TestHarness::<Position>::with(())
            .given(vec![
                PositionEvent::Initialized {
                    symbol: symbol.clone(),
                    threshold,
                    initialized_at: now,
                },
                PositionEvent::OnChainOrderFilled {
                    trade_id: older.clone(),
                    amount,
                    direction: Direction::Buy,
                    price_usdc: float!(150),
                    block_timestamp: now,
                    seen_at: now,
                },
                PositionEvent::OnChainOrderFilled {
                    trade_id: newer.clone(),
                    amount,
                    direction: Direction::Buy,
                    price_usdc: float!(150),
                    block_timestamp: now,
                    seen_at: now,
                },
                PositionEvent::Reorged {
                    trade_id: older.clone(),
                    amount,
                    direction: Direction::Buy,
                    price_usdc: Some(float!(150)),
                    reorg_depth: 1,
                    reorged_at: now,
                },
            ])
            .when(PositionCommand::AcknowledgeOnChainFill {
                symbol,
                threshold,
                trade_id: newer.clone(),
                amount,
                direction: Direction::Buy,
                price_usdc: float!(150),
                block_timestamp: now,
            })
            .await
            .then_expect_error();

        assert!(
            matches!(
                error,
                LifecycleError::Apply(PositionError::DuplicateTrade {
                    trade_id: ref rejected
                }) if *rejected == newer
            ),
            "reorging an older fill must not clear a newer fill's dedupe anchor; \
             re-acknowledging the newer fill must still be rejected as a \
             duplicate; got: {error:?}",
        );
    }

    #[tokio::test]
    async fn cannot_reorg_uninitialized_position() {
        let error = TestHarness::<Position>::with(())
            .given_no_previous_events()
            .when(PositionCommand::RecordReorg {
                trade_id: TradeId {
                    tx_hash: TxHash::random(),
                    log_index: 1,
                },
                amount: FractionalShares::new(float!(5)),
                direction: Direction::Buy,
                price_usdc: float!(150),
                reorg_depth: 1,
            })
            .await
            .then_expect_error();

        assert!(matches!(
            error,
            LifecycleError::Apply(PositionError::Uninitialized)
        ));
    }

    /// Defense in depth (ADR 0012): a reorg re-driven for a `trade_id` the
    /// position already reversed must be rejected, not double-reverse `net`.
    #[tokio::test]
    async fn cannot_reorg_same_trade_twice() {
        let symbol = Symbol::new("AAPL").unwrap();
        let now = Utc::now();
        let trade_id = TradeId {
            tx_hash: TxHash::random(),
            log_index: 1,
        };

        let error = TestHarness::<Position>::with(())
            .given(vec![
                PositionEvent::Initialized {
                    symbol,
                    threshold: one_share_threshold(),
                    initialized_at: now,
                },
                PositionEvent::OnChainOrderFilled {
                    trade_id: trade_id.clone(),
                    amount: FractionalShares::new(float!(5)),
                    direction: Direction::Buy,
                    price_usdc: float!(150),
                    block_timestamp: now,
                    seen_at: now,
                },
                PositionEvent::Reorged {
                    trade_id: trade_id.clone(),
                    amount: FractionalShares::new(float!(5)),
                    direction: Direction::Buy,
                    price_usdc: Some(float!(150)),
                    reorg_depth: 1,
                    reorged_at: now,
                },
            ])
            .when(PositionCommand::RecordReorg {
                trade_id,
                amount: FractionalShares::new(float!(5)),
                direction: Direction::Buy,
                price_usdc: float!(150),
                reorg_depth: 1,
            })
            .await
            .then_expect_error();

        assert!(matches!(
            error,
            LifecycleError::Apply(PositionError::DuplicateReorg { .. })
        ));
    }

    /// The pending-reorg set rejects an out-of-order re-drive the single slot
    /// cannot (ADR 0012): reorg B is reversed, then a newer reorg C of the same
    /// symbol completes and advances the slot to C, but the set still holds B. A
    /// re-driven reorg of B is then caught by the SET, not the slot. Remove the
    /// set check from the RecordReorg guard and this fails.
    #[tokio::test]
    async fn out_of_order_reorg_redrive_rejected_by_pending_set() {
        let symbol = Symbol::new("AAPL").unwrap();
        let now = Utc::now();
        let trade_b = TradeId {
            tx_hash: TxHash::random(),
            log_index: 1,
        };
        let trade_c = TradeId {
            tx_hash: TxHash::random(),
            log_index: 2,
        };
        let amount = FractionalShares::new(float!(5));

        let error = TestHarness::<Position>::with(())
            .given(vec![
                PositionEvent::Initialized {
                    symbol: symbol.clone(),
                    threshold: one_share_threshold(),
                    initialized_at: now,
                },
                PositionEvent::OnChainOrderFilled {
                    trade_id: trade_b.clone(),
                    amount,
                    direction: Direction::Buy,
                    price_usdc: float!(150),
                    block_timestamp: now,
                    seen_at: now,
                },
                PositionEvent::OnChainOrderFilled {
                    trade_id: trade_c.clone(),
                    amount,
                    direction: Direction::Buy,
                    price_usdc: float!(150),
                    block_timestamp: now,
                    seen_at: now,
                },
                PositionEvent::Reorged {
                    trade_id: trade_b.clone(),
                    amount,
                    direction: Direction::Buy,
                    price_usdc: Some(float!(150)),
                    reorg_depth: 1,
                    reorged_at: now,
                },
                PositionEvent::Reorged {
                    trade_id: trade_c.clone(),
                    amount,
                    direction: Direction::Buy,
                    price_usdc: Some(float!(150)),
                    reorg_depth: 1,
                    reorged_at: now,
                },
            ])
            .when(PositionCommand::RecordReorg {
                trade_id: trade_b.clone(),
                amount,
                direction: Direction::Buy,
                price_usdc: float!(150),
                reorg_depth: 1,
            })
            .await
            .then_expect_error();

        assert!(
            matches!(
                error,
                LifecycleError::Apply(PositionError::DuplicateReorg {
                    trade_id: ref rejected
                }) if *rejected == trade_b
            ),
            "an older reorged trade must be rejected by the pending-reorg set even \
             after a newer reorg advanced the slot; got: {error:?}",
        );
    }

    /// Replay safety (ADR 0012): a `Reorged` event persisted before `price_usdc`
    /// was added (by the basic detect-and-reverse slice) must deserialize with
    /// `price_usdc: None`, not fail replay. The reactor then nudges instead of
    /// reversing the inventory's USDC leg with a fabricated price.
    #[test]
    fn reorged_event_without_price_usdc_deserializes_to_none() {
        let now = Utc::now();
        let event = PositionEvent::Reorged {
            trade_id: TradeId {
                tx_hash: TxHash::ZERO,
                log_index: 1,
            },
            amount: FractionalShares::new(float!(5)),
            direction: Direction::Buy,
            price_usdc: Some(float!(150)),
            reorg_depth: 1,
            reorged_at: now,
        };

        let mut json = serde_json::to_value(&event).unwrap();
        json["Reorged"]
            .as_object_mut()
            .unwrap()
            .remove("price_usdc")
            .expect("premise: a current Reorged event carries price_usdc");

        let restored: PositionEvent = serde_json::from_value(json).unwrap();
        let PositionEvent::Reorged { price_usdc, .. } = restored else {
            panic!("expected Reorged, got {restored:?}");
        };

        assert!(
            price_usdc.is_none(),
            "a legacy Reorged event without price_usdc must replay as None",
        );
    }

    /// The `position_view.last_reorged_at` column is `json_extract(payload,
    /// '$.Live.last_reorged_at')`. `Position` serializes flat (the field at the
    /// top level); the framework's `Lifecycle<Position>` wrapper nests that flat
    /// object under the externally-tagged `Live` key, so the projection reads
    /// `$.Live.last_reorged_at`. This pins the exact RFC3339 string the column
    /// receives for a known `reorged_at`, independent of the serialization under
    /// test.
    #[test]
    fn reorged_marker_serializes_for_view() {
        let symbol = Symbol::new("AAPL").unwrap();
        let now = Utc::now();
        let reorged_at = DateTime::parse_from_rfc3339("2026-06-15T19:49:15Z")
            .unwrap()
            .with_timezone(&Utc);
        let trade_id = TradeId {
            tx_hash: TxHash::random(),
            log_index: 1,
        };

        let position = replay::<Position>(vec![
            PositionEvent::Initialized {
                symbol,
                threshold: one_share_threshold(),
                initialized_at: now,
            },
            PositionEvent::OnChainOrderFilled {
                trade_id: trade_id.clone(),
                amount: FractionalShares::new(float!(5)),
                direction: Direction::Buy,
                price_usdc: float!(150),
                block_timestamp: now,
                seen_at: now,
            },
            PositionEvent::Reorged {
                trade_id,
                amount: FractionalShares::new(float!(5)),
                direction: Direction::Buy,
                price_usdc: Some(float!(150)),
                reorg_depth: 1,
                reorged_at,
            },
        ])
        .unwrap()
        .expect("replay must produce a live position");

        let payload = serde_json::to_value(&position).unwrap();

        assert_eq!(
            payload["last_reorged_at"],
            serde_json::json!("2026-06-15T19:49:15Z"),
            "last_reorged_at must serialize to the exact RFC3339 string the \
             position_view projection extracts via $.Live.last_reorged_at",
        );
    }

    /// Reproduction for the double-count half of the Witness/Acknowledge
    /// gap (ADR 0005): the position must reject a trade id it has
    /// already applied. Here the seeded history has only `OnChainOrderFilled`
    /// (no `OnChainFillApplied`), mirroring a fill applied under pre-ADR-0010
    /// code and left unmarked at the deploy restart, so the rejection comes
    /// from the retained single slot -- the cross-upgrade bridge (ADR 0010).
    #[tokio::test]
    async fn duplicate_acknowledge_on_chain_fill_is_rejected() {
        let threshold = one_share_threshold();
        let trade_id = TradeId {
            tx_hash: TxHash::random(),
            log_index: 1,
        };

        let error = TestHarness::<Position>::with(())
            .given(vec![
                PositionEvent::Initialized {
                    symbol: Symbol::new("AAPL").unwrap(),
                    threshold,
                    initialized_at: Utc::now(),
                },
                PositionEvent::OnChainOrderFilled {
                    trade_id: trade_id.clone(),
                    amount: FractionalShares::new(float!(0.5)),
                    direction: Direction::Buy,
                    price_usdc: float!(150),
                    block_timestamp: Utc::now(),
                    seen_at: Utc::now(),
                },
            ])
            .when(PositionCommand::AcknowledgeOnChainFill {
                symbol: Symbol::new("AAPL").unwrap(),
                threshold,
                trade_id: trade_id.clone(),
                amount: FractionalShares::new(float!(0.5)),
                direction: Direction::Buy,
                price_usdc: float!(150),
                block_timestamp: Utc::now(),
            })
            .await
            .then_expect_error();

        assert!(
            matches!(
                error,
                LifecycleError::Apply(PositionError::DuplicateTrade {
                    trade_id: ref rejected
                }) if *rejected == trade_id
            ),
            "Re-driving an already-applied trade must be rejected as a \
             duplicate, not double-counted; got: {error:?}",
        );
    }

    /// The pending-acknowledgement set rejects an out-of-order re-drive the
    /// single slot cannot (ADR 0010): once a newer fill (B) displaces the slot,
    /// the slot no longer equals an older applied-but-unmarked fill (A), but the
    /// set still holds A. This is the cross-process / process-tx-CLI double-count
    /// the slot alone misses. Remove the set check from the dedup and this fails.
    #[tokio::test]
    async fn out_of_order_redrive_rejected_by_pending_set() {
        let threshold = one_share_threshold();
        let trade_a = TradeId {
            tx_hash: TxHash::random(),
            log_index: 1,
        };
        let trade_b = TradeId {
            tx_hash: TxHash::random(),
            log_index: 2,
        };
        let now = Utc::now();

        let error = TestHarness::<Position>::with(())
            .given(vec![
                PositionEvent::Initialized {
                    symbol: Symbol::new("AAPL").unwrap(),
                    threshold,
                    initialized_at: now,
                },
                PositionEvent::OnChainOrderFilled {
                    trade_id: trade_a.clone(),
                    amount: FractionalShares::new(float!(0.5)),
                    direction: Direction::Buy,
                    price_usdc: float!(150),
                    block_timestamp: now,
                    seen_at: now,
                },
                PositionEvent::OnChainFillApplied {
                    trade_id: trade_a.clone(),
                    applied_at: now,
                },
                PositionEvent::OnChainOrderFilled {
                    trade_id: trade_b.clone(),
                    amount: FractionalShares::new(float!(0.5)),
                    direction: Direction::Buy,
                    price_usdc: float!(150),
                    block_timestamp: now,
                    seen_at: now,
                },
                PositionEvent::OnChainFillApplied {
                    trade_id: trade_b,
                    applied_at: now,
                },
            ])
            .when(PositionCommand::AcknowledgeOnChainFill {
                symbol: Symbol::new("AAPL").unwrap(),
                threshold,
                trade_id: trade_a.clone(),
                amount: FractionalShares::new(float!(0.5)),
                direction: Direction::Buy,
                price_usdc: float!(150),
                block_timestamp: now,
            })
            .await
            .then_expect_error();

        assert!(
            matches!(
                error,
                LifecycleError::Apply(PositionError::DuplicateTrade {
                    trade_id: ref rejected
                }) if *rejected == trade_a
            ),
            "An older applied-but-unmarked fill must be rejected by the pending \
             set even after a newer fill displaced the slot; got: {error:?}",
        );
    }

    /// A full replay of pre-ADR-0010 history (only `OnChainOrderFilled`, no
    /// `OnChainFillApplied`) rebuilds the pending set EMPTY -- the regression
    /// guard against re-accumulating every historical trade id on the
    /// snapshot-clearing schema bump.
    #[test]
    fn replay_of_pre_migration_history_yields_empty_pending_set() {
        let now = Utc::now();

        let position = replay::<Position>(vec![
            PositionEvent::Initialized {
                symbol: Symbol::new("AAPL").unwrap(),
                threshold: one_share_threshold(),
                initialized_at: now,
            },
            PositionEvent::OnChainOrderFilled {
                trade_id: TradeId {
                    tx_hash: TxHash::random(),
                    log_index: 1,
                },
                amount: FractionalShares::new(float!(0.5)),
                direction: Direction::Buy,
                price_usdc: float!(150),
                block_timestamp: now,
                seen_at: now,
            },
            PositionEvent::OnChainOrderFilled {
                trade_id: TradeId {
                    tx_hash: TxHash::random(),
                    log_index: 2,
                },
                amount: FractionalShares::new(float!(0.5)),
                direction: Direction::Buy,
                price_usdc: float!(150),
                block_timestamp: now,
                seen_at: now,
            },
        ])
        .unwrap()
        .unwrap();

        assert!(
            position.pending_acknowledged_trade_ids.is_empty(),
            "Pre-ADR-0010 history has no OnChainFillApplied events, so replay must \
             rebuild an empty pending set; got: {:?}",
            position.pending_acknowledged_trade_ids,
        );
    }

    /// Applied/Settled pairs cancel on replay, leaving only the genuinely
    /// in-flight tail (ADR 0010): A is applied then settled (pruned); B is
    /// applied but not yet settled (crash before its mark), so the rebuilt set
    /// is exactly {B} -- bounded, and B stays durably guarded.
    #[test]
    fn replay_applied_settled_pairs_converge_to_inflight_tail() {
        let now = Utc::now();
        let trade_a = TradeId {
            tx_hash: TxHash::random(),
            log_index: 1,
        };
        let trade_b = TradeId {
            tx_hash: TxHash::random(),
            log_index: 2,
        };

        let position = replay::<Position>(vec![
            PositionEvent::Initialized {
                symbol: Symbol::new("AAPL").unwrap(),
                threshold: one_share_threshold(),
                initialized_at: now,
            },
            PositionEvent::OnChainOrderFilled {
                trade_id: trade_a.clone(),
                amount: FractionalShares::new(float!(0.5)),
                direction: Direction::Buy,
                price_usdc: float!(150),
                block_timestamp: now,
                seen_at: now,
            },
            PositionEvent::OnChainFillApplied {
                trade_id: trade_a.clone(),
                applied_at: now,
            },
            PositionEvent::OnChainFillSettled {
                trade_id: trade_a,
                settled_at: now,
            },
            PositionEvent::OnChainOrderFilled {
                trade_id: trade_b.clone(),
                amount: FractionalShares::new(float!(0.5)),
                direction: Direction::Buy,
                price_usdc: float!(150),
                block_timestamp: now,
                seen_at: now,
            },
            PositionEvent::OnChainFillApplied {
                trade_id: trade_b.clone(),
                applied_at: now,
            },
        ])
        .unwrap()
        .unwrap();

        assert_eq!(
            position.pending_acknowledged_trade_ids,
            BTreeSet::from([trade_b]),
            "Settled fills must prune; only the unsettled tail remains",
        );
    }

    /// Settle emits the prune event when the trade is in the pending set.
    #[tokio::test]
    async fn settle_emits_settled_when_member() {
        let threshold = one_share_threshold();
        let trade_id = TradeId {
            tx_hash: TxHash::random(),
            log_index: 1,
        };
        let now = Utc::now();

        let events = TestHarness::<Position>::with(())
            .given(vec![
                PositionEvent::Initialized {
                    symbol: Symbol::new("AAPL").unwrap(),
                    threshold,
                    initialized_at: now,
                },
                PositionEvent::OnChainOrderFilled {
                    trade_id: trade_id.clone(),
                    amount: FractionalShares::new(float!(0.5)),
                    direction: Direction::Buy,
                    price_usdc: float!(150),
                    block_timestamp: now,
                    seen_at: now,
                },
                PositionEvent::OnChainFillApplied {
                    trade_id: trade_id.clone(),
                    applied_at: now,
                },
            ])
            .when(PositionCommand::SettleOnChainFill {
                trade_id: trade_id.clone(),
            })
            .await
            .events();

        assert_eq!(events.len(), 1);
        assert!(matches!(
            &events[0],
            PositionEvent::OnChainFillSettled { trade_id: settled, .. }
                if *settled == trade_id
        ));
    }

    /// Settle for a trade not in the set is a no-op: zero events, so a
    /// re-driven prune is idempotent and optimistic-concurrency-safe.
    #[tokio::test]
    async fn settle_is_noop_when_not_member() {
        let threshold = one_share_threshold();

        let events = TestHarness::<Position>::with(())
            .given(vec![PositionEvent::Initialized {
                symbol: Symbol::new("AAPL").unwrap(),
                threshold,
                initialized_at: Utc::now(),
            }])
            .when(PositionCommand::SettleOnChainFill {
                trade_id: TradeId {
                    tx_hash: TxHash::random(),
                    log_index: 99,
                },
            })
            .await
            .events();

        assert!(
            events.is_empty(),
            "Settling an absent trade must emit no events"
        );
    }

    /// SettleReorg emits ReorgSettled when the trade is in the pending-reorg set
    /// (ADR 0012, the reorg twin of `settle_emits_settled_when_member`).
    #[tokio::test]
    async fn settle_reorg_emits_reorg_settled_when_member() {
        let symbol = Symbol::new("AAPL").unwrap();
        let now = Utc::now();
        let trade_id = TradeId {
            tx_hash: TxHash::random(),
            log_index: 1,
        };
        let amount = FractionalShares::new(float!(5));

        let events = TestHarness::<Position>::with(())
            .given(vec![
                PositionEvent::Initialized {
                    symbol: symbol.clone(),
                    threshold: one_share_threshold(),
                    initialized_at: now,
                },
                PositionEvent::OnChainOrderFilled {
                    trade_id: trade_id.clone(),
                    amount,
                    direction: Direction::Buy,
                    price_usdc: float!(150),
                    block_timestamp: now,
                    seen_at: now,
                },
                PositionEvent::Reorged {
                    trade_id: trade_id.clone(),
                    amount,
                    direction: Direction::Buy,
                    price_usdc: Some(float!(150)),
                    reorg_depth: 1,
                    reorged_at: now,
                },
            ])
            .when(PositionCommand::SettleReorg {
                trade_id: trade_id.clone(),
            })
            .await
            .events();

        assert_eq!(events.len(), 1);
        assert!(matches!(
            &events[0],
            PositionEvent::ReorgSettled { trade_id: settled, .. }
                if *settled == trade_id
        ));
    }

    /// The ReorgSettled evolve prunes the trade from the bounded pending-reorg
    /// set (ADR 0012); the retained slot still guards the most-recent reversal.
    #[test]
    fn reorg_settled_prunes_pending_reorged_set() {
        let symbol = Symbol::new("AAPL").unwrap();
        let now = Utc::now();
        let trade_id = TradeId {
            tx_hash: TxHash::random(),
            log_index: 1,
        };
        let amount = FractionalShares::new(float!(5));

        let position = replay::<Position>(vec![
            PositionEvent::Initialized {
                symbol,
                threshold: one_share_threshold(),
                initialized_at: now,
            },
            PositionEvent::OnChainOrderFilled {
                trade_id: trade_id.clone(),
                amount,
                direction: Direction::Buy,
                price_usdc: float!(150),
                block_timestamp: now,
                seen_at: now,
            },
            PositionEvent::Reorged {
                trade_id: trade_id.clone(),
                amount,
                direction: Direction::Buy,
                price_usdc: Some(float!(150)),
                reorg_depth: 1,
                reorged_at: now,
            },
            PositionEvent::ReorgSettled {
                trade_id: trade_id.clone(),
                settled_at: now,
            },
        ])
        .unwrap()
        .unwrap();

        assert!(
            position.pending_reorged_trade_ids.is_empty(),
            "ReorgSettled must prune the reversed trade from the pending-reorg \
             set; got: {:?}",
            position.pending_reorged_trade_ids,
        );
        assert_eq!(
            position.last_reorged_trade_id,
            Some(trade_id),
            "the retained slot still guards the most-recent reversal after settle",
        );
    }

    /// SettleReorg for a trade not in the pending-reorg set is a no-op: zero
    /// events, so a re-driven prune is idempotent and optimistic-concurrency-safe.
    #[tokio::test]
    async fn settle_reorg_is_noop_when_not_member() {
        let threshold = one_share_threshold();

        let events = TestHarness::<Position>::with(())
            .given(vec![PositionEvent::Initialized {
                symbol: Symbol::new("AAPL").unwrap(),
                threshold,
                initialized_at: Utc::now(),
            }])
            .when(PositionCommand::SettleReorg {
                trade_id: TradeId {
                    tx_hash: TxHash::random(),
                    log_index: 99,
                },
            })
            .await
            .events();

        assert!(
            events.is_empty(),
            "Settling an absent reorg must emit no events"
        );
    }

    #[tokio::test]
    async fn shares_threshold_triggers_execution() {
        let threshold = one_share_threshold();

        let events = TestHarness::<Position>::with(())
            .given(vec![
                PositionEvent::Initialized {
                    symbol: Symbol::new("AAPL").unwrap(),
                    threshold,
                    initialized_at: Utc::now(),
                },
                PositionEvent::OnChainOrderFilled {
                    trade_id: TradeId {
                        tx_hash: TxHash::random(),
                        log_index: 1,
                    },
                    amount: FractionalShares::new(float!(0.6)),
                    direction: Direction::Buy,
                    price_usdc: float!(150),
                    block_timestamp: Utc::now(),
                    seen_at: Utc::now(),
                },
                PositionEvent::OnChainOrderFilled {
                    trade_id: TradeId {
                        tx_hash: TxHash::random(),
                        log_index: 2,
                    },
                    amount: FractionalShares::new(float!(0.5)),
                    direction: Direction::Buy,
                    price_usdc: float!(151),
                    block_timestamp: Utc::now(),
                    seen_at: Utc::now(),
                },
            ])
            .when(PositionCommand::PlaceOffChainOrder {
                offchain_order_id: OffchainOrderId::new(),
                shares: Positive::new(FractionalShares::new(float!(1))).unwrap(),
                direction: Direction::Sell,
                executor: SupportedExecutor::DryRun,
                threshold,
            })
            .await
            .events();

        assert_eq!(events.len(), 1);
    }

    #[tokio::test]
    async fn place_offchain_order_below_threshold_fails() {
        let threshold = one_share_threshold();

        let error = TestHarness::<Position>::with(())
            .given(vec![
                PositionEvent::Initialized {
                    symbol: Symbol::new("AAPL").unwrap(),
                    threshold,
                    initialized_at: Utc::now(),
                },
                PositionEvent::OnChainOrderFilled {
                    trade_id: TradeId {
                        tx_hash: TxHash::random(),
                        log_index: 1,
                    },
                    amount: FractionalShares::new(float!(0.5)),
                    direction: Direction::Buy,
                    price_usdc: float!(150),
                    block_timestamp: Utc::now(),
                    seen_at: Utc::now(),
                },
            ])
            .when(PositionCommand::PlaceOffChainOrder {
                offchain_order_id: OffchainOrderId::new(),
                shares: Positive::new(FractionalShares::new(float!(1))).unwrap(),
                direction: Direction::Sell,
                executor: SupportedExecutor::DryRun,
                threshold,
            })
            .await
            .then_expect_error();

        assert!(matches!(
            error,
            LifecycleError::Apply(PositionError::ThresholdNotMet { .. })
        ));
    }

    #[tokio::test]
    async fn pending_execution_prevents_new_execution() {
        let threshold = one_share_threshold();
        let offchain_order_id = OffchainOrderId::new();

        let error = TestHarness::<Position>::with(())
            .given(vec![
                PositionEvent::Initialized {
                    symbol: Symbol::new("AAPL").unwrap(),
                    threshold,
                    initialized_at: Utc::now(),
                },
                PositionEvent::OnChainOrderFilled {
                    trade_id: TradeId {
                        tx_hash: TxHash::random(),
                        log_index: 1,
                    },
                    amount: FractionalShares::new(float!(1.5)),
                    direction: Direction::Buy,
                    price_usdc: float!(150),
                    block_timestamp: Utc::now(),
                    seen_at: Utc::now(),
                },
                PositionEvent::OffChainOrderPlaced {
                    offchain_order_id,
                    shares: Positive::new(FractionalShares::new(float!(1))).unwrap(),
                    direction: Direction::Sell,
                    executor: SupportedExecutor::DryRun,
                    trigger_reason: TriggerReason::SharesThreshold {
                        net_position_shares: float!(1.5),
                        threshold_shares: float!(1),
                    },
                    placed_at: Utc::now(),
                },
            ])
            .when(PositionCommand::PlaceOffChainOrder {
                offchain_order_id: OffchainOrderId::new(),
                shares: Positive::new(FractionalShares::new(float!(0.5))).unwrap(),
                direction: Direction::Sell,
                executor: SupportedExecutor::DryRun,
                threshold,
            })
            .await
            .then_expect_error();

        assert!(matches!(
            error,
            LifecycleError::Apply(PositionError::PendingExecution { .. })
        ));
    }

    #[tokio::test]
    async fn complete_offchain_order_clears_pending() {
        let threshold = one_share_threshold();
        let offchain_order_id = OffchainOrderId::new();

        let events = TestHarness::<Position>::with(())
            .given(vec![
                PositionEvent::Initialized {
                    symbol: Symbol::new("AAPL").unwrap(),
                    threshold,
                    initialized_at: Utc::now(),
                },
                PositionEvent::OnChainOrderFilled {
                    trade_id: TradeId {
                        tx_hash: TxHash::random(),
                        log_index: 1,
                    },
                    amount: FractionalShares::new(float!(1.5)),
                    direction: Direction::Buy,
                    price_usdc: float!(150),
                    block_timestamp: Utc::now(),
                    seen_at: Utc::now(),
                },
                PositionEvent::OffChainOrderPlaced {
                    offchain_order_id,
                    shares: Positive::new(FractionalShares::new(float!(1))).unwrap(),
                    direction: Direction::Sell,
                    executor: SupportedExecutor::DryRun,
                    trigger_reason: TriggerReason::SharesThreshold {
                        net_position_shares: float!(1.5),
                        threshold_shares: float!(1),
                    },
                    placed_at: Utc::now(),
                },
            ])
            .when(PositionCommand::CompleteOffChainOrder {
                offchain_order_id,
                shares_filled: Positive::new(FractionalShares::new(float!(1))).unwrap(),
                direction: Direction::Sell,
                executor_order_id: ExecutorOrderId::new("ORDER123"),
                price: Usd::new(float!(150.50)),
                broker_timestamp: Utc::now(),
            })
            .await
            .events();

        assert_eq!(events.len(), 1);
    }

    #[tokio::test]
    async fn fail_offchain_order_clears_pending() {
        let threshold = one_share_threshold();
        let offchain_order_id = OffchainOrderId::new();

        let events = TestHarness::<Position>::with(())
            .given(vec![
                PositionEvent::Initialized {
                    symbol: Symbol::new("AAPL").unwrap(),
                    threshold,
                    initialized_at: Utc::now(),
                },
                PositionEvent::OnChainOrderFilled {
                    trade_id: TradeId {
                        tx_hash: TxHash::random(),
                        log_index: 1,
                    },
                    amount: FractionalShares::new(float!(1.5)),
                    direction: Direction::Buy,
                    price_usdc: float!(150),
                    block_timestamp: Utc::now(),
                    seen_at: Utc::now(),
                },
                PositionEvent::OffChainOrderPlaced {
                    offchain_order_id,
                    shares: Positive::new(FractionalShares::new(float!(1))).unwrap(),
                    direction: Direction::Sell,
                    executor: SupportedExecutor::DryRun,
                    trigger_reason: TriggerReason::SharesThreshold {
                        net_position_shares: float!(1.5),
                        threshold_shares: float!(1),
                    },
                    placed_at: Utc::now(),
                },
            ])
            .when(PositionCommand::FailOffChainOrder {
                offchain_order_id,
                error: "Broker API timeout".to_string(),
            })
            .await
            .events();

        assert_eq!(events.len(), 1);
    }

    #[tokio::test]
    async fn update_threshold_creates_audit_trail() {
        let events = TestHarness::<Position>::with(())
            .given(vec![PositionEvent::Initialized {
                symbol: Symbol::new("AAPL").unwrap(),
                threshold: one_share_threshold(),
                initialized_at: Utc::now(),
            }])
            .when(PositionCommand::UpdateThreshold {
                threshold: ExecutionThreshold::shares(
                    Positive::new(FractionalShares::new(float!(5))).unwrap(),
                ),
            })
            .await
            .events();

        assert_eq!(events.len(), 1);
    }

    #[tokio::test]
    async fn manual_position_adjustment_initializes_position() {
        let symbol = Symbol::new("SPYM").unwrap();
        let threshold = one_share_threshold();
        let target_net = FractionalShares::new(float!(100));

        let events = TestHarness::<Position>::with(())
            .given_no_previous_events()
            .when(PositionCommand::ManuallyAdjustPosition {
                symbol: symbol.clone(),
                target_net,
                reason: "manual long correction".to_string(),
                threshold,
                expected_net: Some(FractionalShares::ZERO),
                price_usdc: None,
            })
            .await
            .events();

        assert_eq!(
            events.len(),
            2,
            "Expected Initialized + ManualPositionAdjusted"
        );
        assert!(matches!(
            events.first(),
            Some(PositionEvent::Initialized {
                symbol: initialized_symbol,
                threshold: initialized_threshold,
                ..
            }) if *initialized_symbol == symbol && *initialized_threshold == threshold
        ));

        match events.get(1) {
            Some(PositionEvent::ManualPositionAdjusted {
                previous_net,
                target_net: adjusted_target,
                reason,
                ..
            }) => {
                assert_eq!(*previous_net, FractionalShares::ZERO);
                assert_eq!(*adjusted_target, target_net);
                assert_eq!(reason, "manual long correction");
            }
            other => panic!("expected ManualPositionAdjusted event, got: {other:?}"),
        }
    }

    #[tokio::test]
    async fn manual_position_adjustment_sets_existing_position_to_zero() {
        let symbol = Symbol::new("SPYM").unwrap();
        let threshold = one_share_threshold();

        let events = TestHarness::<Position>::with(())
            .given(vec![
                PositionEvent::Initialized {
                    symbol: symbol.clone(),
                    threshold,
                    initialized_at: Utc::now(),
                },
                PositionEvent::OnChainOrderFilled {
                    trade_id: TradeId {
                        tx_hash: TxHash::random(),
                        log_index: 1,
                    },
                    amount: FractionalShares::new(float!(2.5)),
                    direction: Direction::Sell,
                    price_usdc: float!(150),
                    block_timestamp: Utc::now(),
                    seen_at: Utc::now(),
                },
            ])
            .when(PositionCommand::ManuallyAdjustPosition {
                symbol,
                target_net: FractionalShares::ZERO,
                reason: "manual rebalance completed".to_string(),
                threshold,
                expected_net: Some(FractionalShares::new(float!(-2.5))),
                price_usdc: None,
            })
            .await
            .events();

        assert_eq!(events.len(), 1);

        match events.first() {
            Some(PositionEvent::ManualPositionAdjusted {
                previous_net,
                target_net,
                reason,
                ..
            }) => {
                assert_eq!(*previous_net, FractionalShares::new(float!(-2.5)));
                assert_eq!(*target_net, FractionalShares::ZERO);
                assert_eq!(reason, "manual rebalance completed");
            }
            other => panic!("expected ManualPositionAdjusted event, got: {other:?}"),
        }
    }

    #[tokio::test]
    async fn manual_position_adjustment_rejects_pending_offchain_order() {
        let symbol = Symbol::new("SPYM").unwrap();
        let threshold = one_share_threshold();
        let offchain_order_id = OffchainOrderId::new();

        let error = TestHarness::<Position>::with(())
            .given(vec![
                PositionEvent::Initialized {
                    symbol: symbol.clone(),
                    threshold,
                    initialized_at: Utc::now(),
                },
                PositionEvent::OnChainOrderFilled {
                    trade_id: TradeId {
                        tx_hash: TxHash::random(),
                        log_index: 1,
                    },
                    amount: FractionalShares::new(float!(2)),
                    direction: Direction::Buy,
                    price_usdc: float!(150),
                    block_timestamp: Utc::now(),
                    seen_at: Utc::now(),
                },
                PositionEvent::OffChainOrderPlaced {
                    offchain_order_id,
                    shares: Positive::new(FractionalShares::new(float!(1))).unwrap(),
                    direction: Direction::Sell,
                    executor: SupportedExecutor::DryRun,
                    trigger_reason: TriggerReason::SharesThreshold {
                        net_position_shares: float!(2),
                        threshold_shares: float!(1),
                    },
                    placed_at: Utc::now(),
                },
            ])
            .when(PositionCommand::ManuallyAdjustPosition {
                symbol,
                target_net: FractionalShares::ZERO,
                reason: "operator repair".to_string(),
                threshold,
                expected_net: None,
                price_usdc: None,
            })
            .await
            .then_expect_error();

        assert!(matches!(
            error,
            LifecycleError::Apply(PositionError::ManualAdjustmentBlockedByPendingExecution {
                offchain_order_id: blocked_order_id
            }) if blocked_order_id == offchain_order_id
        ));
    }

    #[test]
    fn manual_position_adjustment_replay_sets_net_without_rewriting_volume() {
        let adjusted_at = Utc::now();

        let position = replay::<Position>(vec![
            PositionEvent::Initialized {
                symbol: Symbol::new("SPYM").unwrap(),
                threshold: one_share_threshold(),
                initialized_at: Utc::now(),
            },
            PositionEvent::OnChainOrderFilled {
                trade_id: TradeId {
                    tx_hash: TxHash::random(),
                    log_index: 1,
                },
                amount: FractionalShares::new(float!(5)),
                direction: Direction::Buy,
                price_usdc: float!(150),
                block_timestamp: Utc::now(),
                seen_at: Utc::now(),
            },
            PositionEvent::ManualPositionAdjusted {
                previous_net: FractionalShares::new(float!(5)),
                target_net: FractionalShares::new(float!(-2.5)),
                reason: "manual short correction".to_string(),
                price_usdc: None,
                adjusted_at,
            },
        ])
        .unwrap()
        .unwrap();

        assert_eq!(position.net, FractionalShares::new(float!(-2.5)));
        assert_eq!(position.accumulated_long, FractionalShares::new(float!(5)));
        assert_eq!(position.accumulated_short, FractionalShares::ZERO);
        assert_eq!(position.last_updated, Some(adjusted_at));
    }

    #[tokio::test]
    async fn manual_adjustment_rejects_nonzero_dollar_target_without_price() {
        let symbol = Symbol::new("SPYM").unwrap();
        let threshold = ExecutionThreshold::dollar_value(Usdc::new(float!(1000))).unwrap();

        let error = TestHarness::<Position>::with(())
            .given_no_previous_events()
            .when(PositionCommand::ManuallyAdjustPosition {
                symbol,
                target_net: FractionalShares::new(float!(100)),
                reason: "manual long correction".to_string(),
                threshold,
                expected_net: Some(FractionalShares::ZERO),
                price_usdc: None,
            })
            .await
            .then_expect_error();

        assert!(matches!(
            error,
            LifecycleError::Apply(PositionError::ManualAdjustmentRequiresPrice { target_net })
                if target_net == FractionalShares::new(float!(100))
        ));
    }

    #[tokio::test]
    async fn manual_adjustment_rejects_non_positive_price() {
        for price in [float!(0), float!(-5)] {
            let symbol = Symbol::new("SPYM").unwrap();
            let threshold = ExecutionThreshold::dollar_value(Usdc::new(float!(1000))).unwrap();

            let error = TestHarness::<Position>::with(())
                .given_no_previous_events()
                .when(PositionCommand::ManuallyAdjustPosition {
                    symbol,
                    target_net: FractionalShares::new(float!(100)),
                    reason: "manual long correction".to_string(),
                    threshold,
                    expected_net: Some(FractionalShares::ZERO),
                    price_usdc: Some(price),
                })
                .await
                .then_expect_error();

            assert!(
                matches!(
                    error,
                    LifecycleError::Apply(PositionError::ManualAdjustmentInvalidPrice {
                        price: rejected,
                    }) if rejected == Usdc::new(price)
                ),
                "expected ManualAdjustmentInvalidPrice for price {price:?}, got {error:?}"
            );
        }
    }

    #[tokio::test]
    async fn manual_adjustment_allows_zero_dollar_target_without_price() {
        let symbol = Symbol::new("SPYM").unwrap();
        let threshold = ExecutionThreshold::dollar_value(Usdc::new(float!(1000))).unwrap();

        let events = TestHarness::<Position>::with(())
            .given_no_previous_events()
            .when(PositionCommand::ManuallyAdjustPosition {
                symbol,
                target_net: FractionalShares::ZERO,
                reason: "manual rebalance completed".to_string(),
                threshold,
                expected_net: Some(FractionalShares::ZERO),
                price_usdc: None,
            })
            .await
            .events();

        assert_eq!(
            events.len(),
            2,
            "Expected Initialized + ManualPositionAdjusted"
        );
    }

    #[test]
    fn manual_adjustment_with_price_is_hedge_ready_under_dollar_threshold() {
        let now = Utc::now();

        let position = replay::<Position>(vec![
            PositionEvent::Initialized {
                symbol: Symbol::new("SPYM").unwrap(),
                threshold: ExecutionThreshold::dollar_value(Usdc::new(float!(1000))).unwrap(),
                initialized_at: now,
            },
            PositionEvent::ManualPositionAdjusted {
                previous_net: FractionalShares::ZERO,
                target_net: FractionalShares::new(float!(100)),
                reason: "manual long correction".to_string(),
                price_usdc: Some(float!(200)),
                adjusted_at: now,
            },
        ])
        .unwrap()
        .unwrap();

        assert!(
            option_float_eq(position.last_price_usdc, Some(float!(200))),
            "manual adjustment should persist the supplied price"
        );

        let (direction, shares) = position.is_ready_for_execution(None).unwrap().unwrap();
        assert_eq!(direction, Direction::Sell);
        assert_eq!(shares, FractionalShares::new(float!(100)));
    }

    #[tokio::test]
    async fn manual_adjustment_allows_nonzero_dollar_target_when_price_known() {
        let symbol = Symbol::new("SPYM").unwrap();
        let threshold = ExecutionThreshold::dollar_value(Usdc::new(float!(1000))).unwrap();

        let events = TestHarness::<Position>::with(())
            .given(vec![
                PositionEvent::Initialized {
                    symbol: symbol.clone(),
                    threshold,
                    initialized_at: Utc::now(),
                },
                PositionEvent::OnChainOrderFilled {
                    trade_id: TradeId {
                        tx_hash: TxHash::random(),
                        log_index: 1,
                    },
                    amount: FractionalShares::new(float!(2)),
                    direction: Direction::Buy,
                    price_usdc: float!(150),
                    block_timestamp: Utc::now(),
                    seen_at: Utc::now(),
                },
            ])
            .when(PositionCommand::ManuallyAdjustPosition {
                symbol,
                target_net: FractionalShares::new(float!(50)),
                reason: "manual correction".to_string(),
                threshold,
                expected_net: Some(FractionalShares::new(float!(2))),
                price_usdc: None,
            })
            .await
            .events();

        assert_eq!(events.len(), 1);

        match events.first() {
            Some(PositionEvent::ManualPositionAdjusted {
                target_net,
                price_usdc,
                ..
            }) => {
                assert_eq!(*target_net, FractionalShares::new(float!(50)));
                assert!(price_usdc.is_none());
            }
            other => panic!("expected ManualPositionAdjusted event, got: {other:?}"),
        }
    }

    #[tokio::test]
    async fn manual_adjustment_rejects_stale_expected_net() {
        let symbol = Symbol::new("SPYM").unwrap();
        let threshold = one_share_threshold();

        let error = TestHarness::<Position>::with(())
            .given(vec![
                PositionEvent::Initialized {
                    symbol: symbol.clone(),
                    threshold,
                    initialized_at: Utc::now(),
                },
                PositionEvent::OnChainOrderFilled {
                    trade_id: TradeId {
                        tx_hash: TxHash::random(),
                        log_index: 1,
                    },
                    amount: FractionalShares::new(float!(2)),
                    direction: Direction::Buy,
                    price_usdc: float!(150),
                    block_timestamp: Utc::now(),
                    seen_at: Utc::now(),
                },
            ])
            .when(PositionCommand::ManuallyAdjustPosition {
                symbol,
                target_net: FractionalShares::ZERO,
                reason: "operator repair".to_string(),
                threshold,
                expected_net: Some(FractionalShares::new(float!(5))),
                price_usdc: None,
            })
            .await
            .then_expect_error();

        assert!(matches!(
            error,
            LifecycleError::Apply(PositionError::ManualAdjustmentStateChanged { expected, actual })
                if expected == FractionalShares::new(float!(5))
                    && actual == FractionalShares::new(float!(2))
        ));
    }

    #[test]
    fn offchain_sell_reduces_net_position() {
        let offchain_order_id = OffchainOrderId::new();

        let position = replay::<Position>(vec![
            PositionEvent::Initialized {
                symbol: Symbol::new("AAPL").unwrap(),
                threshold: one_share_threshold(),
                initialized_at: Utc::now(),
            },
            PositionEvent::OnChainOrderFilled {
                trade_id: TradeId {
                    tx_hash: TxHash::random(),
                    log_index: 1,
                },
                amount: FractionalShares::new(float!(2)),
                direction: Direction::Buy,
                price_usdc: float!(150),
                block_timestamp: Utc::now(),
                seen_at: Utc::now(),
            },
            PositionEvent::OffChainOrderPlaced {
                offchain_order_id,
                shares: Positive::new(FractionalShares::new(float!(1.5))).unwrap(),
                direction: Direction::Sell,
                executor: SupportedExecutor::DryRun,
                trigger_reason: TriggerReason::SharesThreshold {
                    net_position_shares: float!(2),
                    threshold_shares: float!(1),
                },
                placed_at: Utc::now(),
            },
            PositionEvent::OffChainOrderFilled {
                offchain_order_id,
                shares_filled: Positive::new(FractionalShares::new(float!(1.5))).unwrap(),
                direction: Direction::Sell,
                executor_order_id: ExecutorOrderId::new("ORDER123"),
                price: Usd::new(float!(150.50)),
                broker_timestamp: Utc::now(),
            },
        ])
        .unwrap()
        .unwrap();

        assert_eq!(position.net, FractionalShares::new(float!(0.5)));
        assert!(
            position.pending_offchain_order_id.is_none(),
            "pending_offchain_order_id should be cleared \
             after OffChainOrderFilled"
        );
    }

    #[test]
    fn offchain_buy_increases_net_position() {
        let offchain_order_id = OffchainOrderId::new();

        let position = replay::<Position>(vec![
            PositionEvent::Initialized {
                symbol: Symbol::new("AAPL").unwrap(),
                threshold: one_share_threshold(),
                initialized_at: Utc::now(),
            },
            PositionEvent::OnChainOrderFilled {
                trade_id: TradeId {
                    tx_hash: TxHash::random(),
                    log_index: 1,
                },
                amount: FractionalShares::new(float!(2)),
                direction: Direction::Sell,
                price_usdc: float!(150),
                block_timestamp: Utc::now(),
                seen_at: Utc::now(),
            },
            PositionEvent::OffChainOrderPlaced {
                offchain_order_id,
                shares: Positive::new(FractionalShares::new(float!(1.5))).unwrap(),
                direction: Direction::Buy,
                executor: SupportedExecutor::DryRun,
                trigger_reason: TriggerReason::SharesThreshold {
                    net_position_shares: float!(2),
                    threshold_shares: float!(1),
                },
                placed_at: Utc::now(),
            },
            PositionEvent::OffChainOrderFilled {
                offchain_order_id,
                shares_filled: Positive::new(FractionalShares::new(float!(1.5))).unwrap(),
                direction: Direction::Buy,
                executor_order_id: ExecutorOrderId::new("ORDER456"),
                price: Usd::new(float!(150.50)),
                broker_timestamp: Utc::now(),
            },
        ])
        .unwrap()
        .unwrap();

        assert_eq!(position.net, FractionalShares::new(float!(-0.5)));
        assert!(
            position.pending_offchain_order_id.is_none(),
            "pending_offchain_order_id should be cleared \
             after OffChainOrderFilled"
        );
    }

    #[test]
    fn offchain_failed_clears_pending() {
        use PositionEvent::*;

        let offchain_order_id = OffchainOrderId::new();

        let position = replay::<Position>(vec![
            Initialized {
                symbol: Symbol::new("AAPL").unwrap(),
                threshold: one_share_threshold(),
                initialized_at: Utc::now(),
            },
            OnChainOrderFilled {
                trade_id: TradeId {
                    tx_hash: TxHash::random(),
                    log_index: 1,
                },
                amount: FractionalShares::new(float!(1.5)),
                direction: Direction::Buy,
                price_usdc: float!(150),
                block_timestamp: Utc::now(),
                seen_at: Utc::now(),
            },
            OffChainOrderPlaced {
                offchain_order_id,
                shares: Positive::new(FractionalShares::new(float!(1))).unwrap(),
                direction: Direction::Sell,
                executor: SupportedExecutor::DryRun,
                trigger_reason: TriggerReason::SharesThreshold {
                    net_position_shares: float!(1.5),
                    threshold_shares: float!(1),
                },
                placed_at: Utc::now(),
            },
            OffChainOrderFailed {
                offchain_order_id,
                error: "Market closed".to_string(),
                failed_at: Utc::now(),
            },
        ])
        .unwrap()
        .unwrap();

        assert_eq!(position.net, FractionalShares::new(float!(1.5)));
        assert!(
            position.pending_offchain_order_id.is_none(),
            "pending_offchain_order_id should be cleared \
             after OffChainOrderFailed"
        );
        assert_eq!(
            position.last_failed_offchain_order_id,
            Some(offchain_order_id),
            "the failed order id should be stashed as the idempotency anchor \
             so the next placement attempt reuses it as client_order_id"
        );
    }

    #[test]
    fn offchain_failed_preserves_first_failed_anchor() {
        use PositionEvent::*;

        let first_order_id = OffchainOrderId::new();
        let second_order_id = OffchainOrderId::new();

        let position = replay::<Position>(vec![
            Initialized {
                symbol: Symbol::new("AAPL").unwrap(),
                threshold: one_share_threshold(),
                initialized_at: Utc::now(),
            },
            OnChainOrderFilled {
                trade_id: TradeId {
                    tx_hash: TxHash::random(),
                    log_index: 1,
                },
                amount: FractionalShares::new(float!(1.5)),
                direction: Direction::Buy,
                price_usdc: float!(150),
                block_timestamp: Utc::now(),
                seen_at: Utc::now(),
            },
            OffChainOrderPlaced {
                offchain_order_id: first_order_id,
                shares: Positive::new(FractionalShares::new(float!(1))).unwrap(),
                direction: Direction::Sell,
                executor: SupportedExecutor::DryRun,
                trigger_reason: TriggerReason::SharesThreshold {
                    net_position_shares: float!(1.5),
                    threshold_shares: float!(1),
                },
                placed_at: Utc::now(),
            },
            OffChainOrderFailed {
                offchain_order_id: first_order_id,
                error: "Market closed".to_string(),
                failed_at: Utc::now(),
            },
            OffChainOrderPlaced {
                offchain_order_id: second_order_id,
                shares: Positive::new(FractionalShares::new(float!(1))).unwrap(),
                direction: Direction::Sell,
                executor: SupportedExecutor::DryRun,
                trigger_reason: TriggerReason::SharesThreshold {
                    net_position_shares: float!(1.5),
                    threshold_shares: float!(1),
                },
                placed_at: Utc::now(),
            },
            OffChainOrderFailed {
                offchain_order_id: second_order_id,
                error: "Market closed".to_string(),
                failed_at: Utc::now(),
            },
        ])
        .unwrap()
        .unwrap();

        assert_eq!(
            position.last_failed_offchain_order_id,
            Some(first_order_id),
            "a chain of failures must keep the first anchor: the broker \
             recorded the original attempt under that key, so later retries \
             must keep deduping against it rather than a key the broker \
             never saw"
        );
    }

    #[test]
    fn offchain_filled_clears_failed_anchor() {
        use PositionEvent::*;

        let failed_order_id = OffchainOrderId::new();
        let retry_order_id = OffchainOrderId::new();

        let position = replay::<Position>(vec![
            Initialized {
                symbol: Symbol::new("AAPL").unwrap(),
                threshold: one_share_threshold(),
                initialized_at: Utc::now(),
            },
            OnChainOrderFilled {
                trade_id: TradeId {
                    tx_hash: TxHash::random(),
                    log_index: 1,
                },
                amount: FractionalShares::new(float!(1.5)),
                direction: Direction::Buy,
                price_usdc: float!(150),
                block_timestamp: Utc::now(),
                seen_at: Utc::now(),
            },
            OffChainOrderPlaced {
                offchain_order_id: failed_order_id,
                shares: Positive::new(FractionalShares::new(float!(1))).unwrap(),
                direction: Direction::Sell,
                executor: SupportedExecutor::DryRun,
                trigger_reason: TriggerReason::SharesThreshold {
                    net_position_shares: float!(1.5),
                    threshold_shares: float!(1),
                },
                placed_at: Utc::now(),
            },
            OffChainOrderFailed {
                offchain_order_id: failed_order_id,
                error: "Market closed".to_string(),
                failed_at: Utc::now(),
            },
            OffChainOrderPlaced {
                offchain_order_id: retry_order_id,
                shares: Positive::new(FractionalShares::new(float!(1))).unwrap(),
                direction: Direction::Sell,
                executor: SupportedExecutor::DryRun,
                trigger_reason: TriggerReason::SharesThreshold {
                    net_position_shares: float!(1.5),
                    threshold_shares: float!(1),
                },
                placed_at: Utc::now(),
            },
            OffChainOrderFilled {
                offchain_order_id: retry_order_id,
                shares_filled: Positive::new(FractionalShares::new(float!(1))).unwrap(),
                direction: Direction::Sell,
                executor_order_id: ExecutorOrderId::new("ORDER-RETRY"),
                price: Usd::new(float!(150.50)),
                broker_timestamp: Utc::now(),
            },
        ])
        .unwrap()
        .unwrap();

        assert!(
            position.last_failed_offchain_order_id.is_none(),
            "a successful fill must clear the failed-attempt anchor so the \
             next rebalance cycle starts from a fresh idempotency key"
        );
    }

    #[test]
    fn manual_adjustment_clears_failed_offchain_order_anchor() {
        let offchain_order_id = OffchainOrderId::new();

        // Drive the position to a failed offchain order (which records the
        // broker-idempotency anchor), then manually reconcile. The manual
        // adjustment must clear the anchor so the next hedge does not reuse the
        // failed order's client_order_id and get deduped by the broker.
        let position = replay::<Position>(vec![
            PositionEvent::Initialized {
                symbol: Symbol::new("AAPL").unwrap(),
                threshold: one_share_threshold(),
                initialized_at: Utc::now(),
            },
            PositionEvent::OnChainOrderFilled {
                trade_id: TradeId {
                    tx_hash: TxHash::random(),
                    log_index: 1,
                },
                amount: FractionalShares::new(float!(1.5)),
                direction: Direction::Buy,
                price_usdc: float!(150),
                block_timestamp: Utc::now(),
                seen_at: Utc::now(),
            },
            PositionEvent::OffChainOrderPlaced {
                offchain_order_id,
                shares: Positive::new(FractionalShares::new(float!(1))).unwrap(),
                direction: Direction::Sell,
                executor: SupportedExecutor::DryRun,
                trigger_reason: TriggerReason::SharesThreshold {
                    net_position_shares: float!(1.5),
                    threshold_shares: float!(1),
                },
                placed_at: Utc::now(),
            },
            PositionEvent::OffChainOrderFailed {
                offchain_order_id,
                error: "Market closed".to_string(),
                failed_at: Utc::now(),
            },
            PositionEvent::ManualPositionAdjusted {
                previous_net: FractionalShares::new(float!(1.5)),
                target_net: FractionalShares::ZERO,
                reason: "operator reconciliation".to_string(),
                price_usdc: None,
                adjusted_at: Utc::now(),
            },
        ])
        .unwrap()
        .unwrap();

        assert_eq!(position.net, FractionalShares::ZERO);
        assert_eq!(
            position.last_failed_offchain_order_id, None,
            "manual adjustment must clear the stale broker-idempotency anchor"
        );
    }

    #[test]
    fn threshold_updated_changes_threshold() {
        let new_threshold = ExecutionThreshold::dollar_value(Usdc::new(float!(10000))).unwrap();

        let position = replay::<Position>(vec![
            PositionEvent::Initialized {
                symbol: Symbol::new("AAPL").unwrap(),
                threshold: one_share_threshold(),
                initialized_at: Utc::now(),
            },
            PositionEvent::ThresholdUpdated {
                old_threshold: one_share_threshold(),
                new_threshold,
                updated_at: Utc::now(),
            },
        ])
        .unwrap()
        .unwrap();

        assert_eq!(position.threshold, new_threshold);
    }

    #[test]
    fn non_genesis_event_on_uninitialized_fails() {
        let error = replay::<Position>(vec![PositionEvent::OnChainOrderFilled {
            trade_id: TradeId {
                tx_hash: TxHash::random(),
                log_index: 0,
            },
            direction: Direction::Buy,
            amount: FractionalShares::new(float!(10)),
            price_usdc: float!(150),
            block_timestamp: Utc::now(),
            seen_at: Utc::now(),
        }])
        .unwrap_err();

        assert!(matches!(error, LifecycleError::EventCantOriginate { .. }));
    }

    #[test]
    fn timestamp_returns_initialized_at_for_initialized_event() {
        let timestamp = Utc::now();
        let event = PositionEvent::Initialized {
            symbol: Symbol::new("AAPL").unwrap(),
            threshold: ExecutionThreshold::whole_share(),
            initialized_at: timestamp,
        };

        assert_eq!(event.timestamp(), timestamp);
    }

    #[test]
    fn timestamp_returns_seen_at_for_onchain_order_filled_event() {
        let block_timestamp = Utc::now();
        let seen_at = block_timestamp + chrono::Duration::seconds(5);
        let event = PositionEvent::OnChainOrderFilled {
            trade_id: TradeId {
                tx_hash: TxHash::random(),
                log_index: 0,
            },
            amount: FractionalShares::new(float!(1)),
            direction: Direction::Buy,
            price_usdc: float!(150),
            block_timestamp,
            seen_at,
        };

        assert_eq!(event.timestamp(), seen_at);
    }

    #[test]
    fn timestamp_returns_reorged_at_for_reorged_event() {
        let reorged_at = Utc::now();
        let event = PositionEvent::Reorged {
            trade_id: TradeId {
                tx_hash: TxHash::random(),
                log_index: 1,
            },
            amount: FractionalShares::new(float!(5)),
            direction: Direction::Buy,
            price_usdc: Some(float!(150)),
            reorg_depth: 1,
            reorged_at,
        };

        assert_eq!(event.timestamp(), reorged_at);
    }

    #[test]
    fn timestamp_returns_placed_at_for_offchain_order_placed_event() {
        let timestamp = Utc::now();
        let event = PositionEvent::OffChainOrderPlaced {
            offchain_order_id: OffchainOrderId::new(),
            shares: Positive::new(FractionalShares::new(float!(1))).unwrap(),
            direction: Direction::Sell,
            executor: SupportedExecutor::DryRun,
            trigger_reason: TriggerReason::SharesThreshold {
                net_position_shares: float!(1),
                threshold_shares: float!(1),
            },
            placed_at: timestamp,
        };

        assert_eq!(event.timestamp(), timestamp);
    }

    #[test]
    fn timestamp_returns_broker_timestamp_for_offchain_order_filled_event() {
        let timestamp = Utc::now();
        let event = PositionEvent::OffChainOrderFilled {
            offchain_order_id: OffchainOrderId::new(),
            shares_filled: Positive::new(FractionalShares::new(float!(1))).unwrap(),
            direction: Direction::Sell,
            executor_order_id: ExecutorOrderId::new("ORD123"),
            price: Usd::new(float!(150.00)),
            broker_timestamp: timestamp,
        };

        assert_eq!(event.timestamp(), timestamp);
    }

    #[test]
    fn timestamp_returns_failed_at_for_offchain_order_failed_event() {
        let timestamp = Utc::now();
        let event = PositionEvent::OffChainOrderFailed {
            offchain_order_id: OffchainOrderId::new(),
            error: "Market closed".to_string(),
            failed_at: timestamp,
        };

        assert_eq!(event.timestamp(), timestamp);
    }

    #[test]
    fn timestamp_returns_updated_at_for_threshold_updated_event() {
        let timestamp = Utc::now();
        let event = PositionEvent::ThresholdUpdated {
            old_threshold: ExecutionThreshold::whole_share(),
            new_threshold: ExecutionThreshold::shares(
                Positive::new(FractionalShares::new(float!(5))).unwrap(),
            ),
            updated_at: timestamp,
        };

        assert_eq!(event.timestamp(), timestamp);
    }

    #[test]
    fn timestamp_returns_adjusted_at_for_manual_position_adjusted_event() {
        let timestamp = Utc::now();
        let event = PositionEvent::ManualPositionAdjusted {
            previous_net: FractionalShares::new(float!(2)),
            target_net: FractionalShares::ZERO,
            reason: "operator repair".to_string(),
            price_usdc: None,
            adjusted_at: timestamp,
        };

        assert_eq!(event.timestamp(), timestamp);
    }

    #[test]
    fn is_ready_for_execution_returns_fractional_shares() {
        let position = Position {
            symbol: Symbol::new("AAPL").unwrap(),
            net: FractionalShares::new(float!(1.212)),
            accumulated_long: FractionalShares::new(float!(1.212)),
            accumulated_short: FractionalShares::ZERO,
            pending_offchain_order_id: None,
            last_failed_offchain_order_id: None,
            last_acknowledged_trade_id: None,
            pending_acknowledged_trade_ids: BTreeSet::new(),
            pending_reorged_trade_ids: BTreeSet::new(),
            threshold: ExecutionThreshold::whole_share(),
            last_price_usdc: Some(float!(150)),
            last_updated: Some(Utc::now()),
            last_reorged_at: None,
            last_reorged_trade_id: None,
        };

        let (direction, shares) = position
            .is_ready_for_execution(None)
            .unwrap()
            .expect("should be ready for execution");

        assert_eq!(direction, Direction::Sell);
        assert_eq!(
            shares,
            FractionalShares::new(float!(1.212)),
            "Expected 1.212 shares, got {shares}",
        );
    }

    #[test]
    fn is_ready_for_execution_returns_fractional_buy_for_negative_position() {
        let position = Position {
            symbol: Symbol::new("AAPL").unwrap(),
            net: FractionalShares::new(float!(-2.567)),
            accumulated_long: FractionalShares::ZERO,
            accumulated_short: FractionalShares::new(float!(2.567)),
            pending_offchain_order_id: None,
            last_failed_offchain_order_id: None,
            last_acknowledged_trade_id: None,
            pending_acknowledged_trade_ids: BTreeSet::new(),
            pending_reorged_trade_ids: BTreeSet::new(),
            threshold: ExecutionThreshold::whole_share(),
            last_price_usdc: Some(float!(150)),
            last_updated: Some(Utc::now()),
            last_reorged_at: None,
            last_reorged_trade_id: None,
        };

        let (direction, shares) = position
            .is_ready_for_execution(None)
            .unwrap()
            .expect("should be ready for execution");

        assert_eq!(direction, Direction::Buy);
        assert_eq!(
            shares,
            FractionalShares::new(float!(2.567)),
            "Expected 2.567 shares, got {shares}",
        );
    }

    #[test]
    fn operational_limits_cap_shares() {
        let position = Position {
            symbol: Symbol::new("AAPL").unwrap(),
            net: FractionalShares::new(float!(100)),
            accumulated_long: FractionalShares::new(float!(100)),
            accumulated_short: FractionalShares::ZERO,
            pending_offchain_order_id: None,
            last_failed_offchain_order_id: None,
            last_acknowledged_trade_id: None,
            pending_acknowledged_trade_ids: BTreeSet::new(),
            pending_reorged_trade_ids: BTreeSet::new(),
            threshold: ExecutionThreshold::whole_share(),
            last_price_usdc: Some(float!(150)),
            last_updated: Some(Utc::now()),
            last_reorged_at: None,
            last_reorged_trade_id: None,
        };

        let shares_limit = Positive::new(FractionalShares::new(float!(50))).unwrap();

        let (direction, shares) = position
            .is_ready_for_execution(Some(shares_limit))
            .unwrap()
            .expect("should be ready for execution");

        assert_eq!(direction, Direction::Sell);
        assert_eq!(
            shares,
            FractionalShares::new(float!(50)),
            "Shares should be capped by operational \
             limit: expected 50, got {shares}",
        );
    }

    #[test]
    fn operational_limits_do_not_cap_below_limit() {
        let position = Position {
            symbol: Symbol::new("AAPL").unwrap(),
            net: FractionalShares::new(float!(30)),
            accumulated_long: FractionalShares::new(float!(30)),
            accumulated_short: FractionalShares::ZERO,
            pending_offchain_order_id: None,
            last_failed_offchain_order_id: None,
            last_acknowledged_trade_id: None,
            pending_acknowledged_trade_ids: BTreeSet::new(),
            pending_reorged_trade_ids: BTreeSet::new(),
            threshold: ExecutionThreshold::whole_share(),
            last_price_usdc: Some(float!(150)),
            last_updated: Some(Utc::now()),
            last_reorged_at: None,
            last_reorged_trade_id: None,
        };

        let shares_limit = Positive::new(FractionalShares::new(float!(50))).unwrap();

        let (direction, shares) = position
            .is_ready_for_execution(Some(shares_limit))
            .unwrap()
            .expect("should be ready for execution");

        assert_eq!(direction, Direction::Sell);
        assert_eq!(
            shares,
            FractionalShares::new(float!(30)),
            "Shares should not be capped when below \
             limit: expected 30, got {shares}",
        );
    }

    #[test]
    fn operational_limits_preserve_fractional_cap() {
        let position = Position {
            symbol: Symbol::new("AAPL").unwrap(),
            net: FractionalShares::new(float!(100)),
            accumulated_long: FractionalShares::new(float!(100)),
            accumulated_short: FractionalShares::ZERO,
            pending_offchain_order_id: None,
            last_failed_offchain_order_id: None,
            last_acknowledged_trade_id: None,
            pending_acknowledged_trade_ids: BTreeSet::new(),
            pending_reorged_trade_ids: BTreeSet::new(),
            threshold: ExecutionThreshold::whole_share(),
            last_price_usdc: Some(float!(150)),
            last_updated: Some(Utc::now()),
            last_reorged_at: None,
            last_reorged_trade_id: None,
        };

        let shares_limit = Positive::new(FractionalShares::new(float!(50.7))).unwrap();

        let (_, shares) = position
            .is_ready_for_execution(Some(shares_limit))
            .unwrap()
            .expect("should be ready for execution");

        assert_eq!(
            shares,
            FractionalShares::new(float!(50.7)),
            "Fractional executor should preserve the exact cap: expected 50.7, got {shares}",
        );
    }

    #[test]
    fn operational_limits_fractional_cap_constrains_position() {
        // Fractional shares cap of 7.5 should constrain a 10-share position
        let position = Position {
            symbol: Symbol::new("AAPL").unwrap(),
            net: FractionalShares::new(float!(10)),
            accumulated_long: FractionalShares::new(float!(10)),
            accumulated_short: FractionalShares::ZERO,
            pending_offchain_order_id: None,
            last_failed_offchain_order_id: None,
            last_acknowledged_trade_id: None,
            pending_acknowledged_trade_ids: BTreeSet::new(),
            pending_reorged_trade_ids: BTreeSet::new(),
            threshold: ExecutionThreshold::whole_share(),
            last_price_usdc: Some(float!(150)),
            last_updated: Some(Utc::now()),
            last_reorged_at: None,
            last_reorged_trade_id: None,
        };

        let shares_limit = Positive::new(FractionalShares::new(float!(7.5))).unwrap();

        let (direction, shares) = position
            .is_ready_for_execution(Some(shares_limit))
            .unwrap()
            .expect("should be ready for execution");

        assert_eq!(direction, Direction::Sell);
        assert_eq!(
            shares,
            FractionalShares::new(float!(7.5)),
            "Fractional shares cap should limit to 7.5 shares, got {shares}",
        );
    }

    #[test]
    fn operational_limits_cap_fractional_executor_to_exact_limit() {
        // Fractional executor with shares cap (5) tighter than position (10)
        let position = Position {
            symbol: Symbol::new("AAPL").unwrap(),
            net: FractionalShares::new(float!(10)),
            accumulated_long: FractionalShares::new(float!(10)),
            accumulated_short: FractionalShares::ZERO,
            pending_offchain_order_id: None,
            last_failed_offchain_order_id: None,
            last_acknowledged_trade_id: None,
            pending_acknowledged_trade_ids: BTreeSet::new(),
            pending_reorged_trade_ids: BTreeSet::new(),
            threshold: ExecutionThreshold::whole_share(),
            last_price_usdc: Some(float!(150)),
            last_updated: Some(Utc::now()),
            last_reorged_at: None,
            last_reorged_trade_id: None,
        };

        let shares_limit = Positive::new(FractionalShares::new(float!(5))).unwrap();

        let (direction, shares) = position
            .is_ready_for_execution(Some(shares_limit))
            .unwrap()
            .expect("should be ready for execution");

        assert_eq!(direction, Direction::Sell);
        assert_eq!(
            shares,
            FractionalShares::new(float!(5)),
            "Shares cap should limit to 5 shares, got {shares}",
        );
    }

    #[test]
    fn operational_limits_cap_applies_without_price() {
        // Shares cap works regardless of whether last_price_usdc is available
        let position = Position {
            symbol: Symbol::new("AAPL").unwrap(),
            net: FractionalShares::new(float!(100)),
            accumulated_long: FractionalShares::new(float!(100)),
            accumulated_short: FractionalShares::ZERO,
            pending_offchain_order_id: None,
            last_failed_offchain_order_id: None,
            last_acknowledged_trade_id: None,
            pending_acknowledged_trade_ids: BTreeSet::new(),
            pending_reorged_trade_ids: BTreeSet::new(),
            threshold: ExecutionThreshold::whole_share(),
            last_price_usdc: None,
            last_updated: Some(Utc::now()),
            last_reorged_at: None,
            last_reorged_trade_id: None,
        };

        let shares_limit = Positive::new(FractionalShares::new(float!(50))).unwrap();

        let (direction, shares) = position
            .is_ready_for_execution(Some(shares_limit))
            .unwrap()
            .expect("should be ready for execution");

        assert_eq!(direction, Direction::Sell);
        assert_eq!(
            shares,
            FractionalShares::new(float!(50)),
            "Without price, only shares cap applies: expected 50, got {shares}",
        );
    }

    #[tokio::test]
    async fn load_position_returns_none_when_no_aggregate_exists() {
        let pool = crate::test_utils::setup_test_db().await;
        let (_store, projection) = StoreBuilder::<Position>::new(pool).build(()).await.unwrap();

        let result = projection
            .load(&Symbol::new("AAPL").unwrap())
            .await
            .unwrap();

        assert!(
            result.is_none(),
            "Should return None for non-existent aggregate"
        );
    }

    #[tokio::test]
    async fn load_position_returns_position_for_live_lifecycle() {
        let pool = crate::test_utils::setup_test_db().await;
        let (store, projection) = StoreBuilder::<Position>::new(pool).build(()).await.unwrap();

        let symbol = Symbol::new("AAPL").unwrap();

        store
            .send(
                &symbol,
                PositionCommand::AcknowledgeOnChainFill {
                    symbol: symbol.clone(),
                    threshold: one_share_threshold(),
                    trade_id: TradeId {
                        tx_hash: TxHash::random(),
                        log_index: 1,
                    },
                    amount: FractionalShares::new(float!(1)),
                    direction: Direction::Buy,
                    price_usdc: float!(150),
                    block_timestamp: Utc::now(),
                },
            )
            .await
            .unwrap();

        let result = projection.load(&symbol).await.unwrap();

        let position = result.expect("Should return Some for live lifecycle");
        assert_eq!(position.symbol, symbol);
        assert_eq!(position.net, FractionalShares::new(float!(1)));
    }

    #[test]
    fn capped_execution_leaves_remaining_exposure_triggerable() {
        let shares_limit = Positive::new(FractionalShares::new(float!(50))).unwrap();

        let position = Position {
            symbol: Symbol::new("AAPL").unwrap(),
            net: FractionalShares::new(float!(120)),
            accumulated_long: FractionalShares::new(float!(120)),
            accumulated_short: FractionalShares::ZERO,
            pending_offchain_order_id: None,
            last_failed_offchain_order_id: None,
            last_acknowledged_trade_id: None,
            pending_acknowledged_trade_ids: BTreeSet::new(),
            pending_reorged_trade_ids: BTreeSet::new(),
            threshold: ExecutionThreshold::whole_share(),
            last_price_usdc: Some(float!(150)),
            last_updated: Some(Utc::now()),
            last_reorged_at: None,
            last_reorged_trade_id: None,
        };

        let (_, first_shares) = position
            .is_ready_for_execution(Some(shares_limit))
            .unwrap()
            .expect("first check should trigger");
        assert_eq!(
            first_shares,
            FractionalShares::new(float!(50)),
            "First execution capped to 50"
        );

        // Simulate executing 50 shares: net goes from 120 to 70
        let after_first = Position {
            net: FractionalShares::new(float!(70)),
            accumulated_long: FractionalShares::new(float!(70)),
            ..position.clone()
        };

        let (_, second_shares) = after_first
            .is_ready_for_execution(Some(shares_limit))
            .unwrap()
            .expect("remaining 70 shares still exceeds threshold");
        assert_eq!(
            second_shares,
            FractionalShares::new(float!(50)),
            "Second execution capped to 50"
        );

        // Simulate executing another 50: net goes from 70 to 20
        let after_second = Position {
            net: FractionalShares::new(float!(20)),
            accumulated_long: FractionalShares::new(float!(20)),
            ..position
        };

        let (_, third_shares) = after_second
            .is_ready_for_execution(Some(shares_limit))
            .unwrap()
            .expect("remaining 20 shares still exceeds threshold");
        assert_eq!(
            third_shares,
            FractionalShares::new(float!(20)),
            "Third execution returns remaining 20 (below cap)"
        );
    }
}

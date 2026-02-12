//! Position CQRS/ES aggregate for tracking per-symbol
//! onchain/offchain exposure.
//!
//! Accumulates long and short fills, tracks the net
//! position, and decides when the imbalance exceeds the
//! threshold to trigger an offsetting broker order.

use alloy::primitives::TxHash;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use cqrs_es::{Aggregate, DomainEvent};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sqlite_es::SqliteCqrs;
use tracing::warn;

use st0x_execution::{
    ArithmeticError, Direction, ExecutorOrderId, FractionalShares, Positive, SupportedExecutor,
    Symbol,
};

use crate::lifecycle::{EventSourced, Lifecycle, LifecycleError, SqliteQuery};
use crate::offchain_order::{OffchainOrderId, PriceCents};
use crate::threshold::{ExecutionThreshold, Usdc};

pub(crate) type PositionAggregate = Lifecycle<Position, ArithmeticError<FractionalShares>>;
pub(crate) type PositionCqrs = SqliteCqrs<PositionAggregate>;
pub(crate) type PositionQuery = SqliteQuery<Position, ArithmeticError<FractionalShares>>;

pub(crate) async fn load_position(
    query: &PositionQuery,
    symbol: &Symbol,
) -> Result<Option<Position>, PositionError> {
    let aggregate_id = Position::aggregate_id(symbol);
    let Some(lifecycle) = query.load(&aggregate_id).await else {
        return Ok(None);
    };
    match lifecycle.live() {
        Ok(position) => Ok(Some(position.clone())),
        Err(LifecycleError::Uninitialized) => Ok(None),
        Err(error) => Err(error.into()),
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct Position {
    pub(crate) symbol: Symbol,
    pub(crate) net: FractionalShares,
    pub(crate) accumulated_long: FractionalShares,
    pub(crate) accumulated_short: FractionalShares,
    pub(crate) pending_offchain_order_id: Option<OffchainOrderId>,
    pub(crate) threshold: ExecutionThreshold,
    pub(crate) last_price_usdc: Option<Decimal>,
    pub(crate) last_updated: Option<DateTime<Utc>>,
}

impl EventSourced for Position {
    type Event = PositionEvent;
}

impl Position {
    pub(crate) fn aggregate_id(symbol: &Symbol) -> String {
        symbol.to_string()
    }

    pub(crate) fn apply_transition(
        event: &PositionEvent,
        position: &Self,
    ) -> Result<Self, LifecycleError<Position, ArithmeticError<FractionalShares>>> {
        match event {
            PositionEvent::OnChainOrderFilled {
                amount,
                direction,
                price_usdc,
                seen_at,
                ..
            } => Self::apply_onchain_fill(position, *amount, *direction, *price_usdc, *seen_at),

            PositionEvent::OffChainOrderPlaced {
                offchain_order_id,
                placed_at,
                ..
            } => Self::apply_offchain_placed(position, event, *offchain_order_id, *placed_at),

            PositionEvent::OffChainOrderFilled {
                offchain_order_id,
                shares_filled,
                direction,
                broker_timestamp,
                ..
            } => Self::apply_offchain_filled(
                position,
                event,
                *offchain_order_id,
                *shares_filled,
                *direction,
                *broker_timestamp,
            ),

            PositionEvent::OffChainOrderFailed {
                offchain_order_id,
                failed_at,
                ..
            } => Self::apply_offchain_failed(position, event, *offchain_order_id, *failed_at),

            PositionEvent::ThresholdUpdated {
                new_threshold,
                updated_at,
                ..
            } => Ok(Self {
                threshold: *new_threshold,
                last_updated: Some(*updated_at),
                ..position.clone()
            }),

            PositionEvent::Initialized { .. } => Err(Self::mismatch_error(position, event)),
        }
    }

    fn apply_onchain_fill(
        position: &Self,
        amount: FractionalShares,
        direction: Direction,
        price_usdc: Decimal,
        seen_at: DateTime<Utc>,
    ) -> Result<Self, LifecycleError<Position, ArithmeticError<FractionalShares>>> {
        match direction {
            Direction::Buy => Ok(Self {
                net: (position.net + amount)?,
                accumulated_long: (position.accumulated_long + amount)?,
                last_price_usdc: Some(price_usdc),
                last_updated: Some(seen_at),
                ..position.clone()
            }),
            Direction::Sell => Ok(Self {
                net: (position.net - amount)?,
                accumulated_short: (position.accumulated_short + amount)?,
                last_price_usdc: Some(price_usdc),
                last_updated: Some(seen_at),
                ..position.clone()
            }),
        }
    }

    fn apply_offchain_placed(
        position: &Self,
        event: &PositionEvent,
        offchain_order_id: OffchainOrderId,
        placed_at: DateTime<Utc>,
    ) -> Result<Self, LifecycleError<Position, ArithmeticError<FractionalShares>>> {
        if position.pending_offchain_order_id.is_some() {
            return Err(Self::mismatch_error(position, event));
        }

        Ok(Self {
            pending_offchain_order_id: Some(offchain_order_id),
            last_updated: Some(placed_at),
            ..position.clone()
        })
    }

    fn apply_offchain_filled(
        position: &Self,
        event: &PositionEvent,
        offchain_order_id: OffchainOrderId,
        shares_filled: Positive<FractionalShares>,
        direction: Direction,
        broker_timestamp: DateTime<Utc>,
    ) -> Result<Self, LifecycleError<Position, ArithmeticError<FractionalShares>>> {
        if position.pending_offchain_order_id != Some(offchain_order_id) {
            return Err(Self::mismatch_error(position, event));
        }

        match direction {
            Direction::Sell => Ok(Self {
                net: (position.net - shares_filled.inner())?,
                pending_offchain_order_id: None,
                last_updated: Some(broker_timestamp),
                ..position.clone()
            }),
            Direction::Buy => Ok(Self {
                net: (position.net + shares_filled.inner())?,
                pending_offchain_order_id: None,
                last_updated: Some(broker_timestamp),
                ..position.clone()
            }),
        }
    }

    fn apply_offchain_failed(
        position: &Self,
        event: &PositionEvent,
        offchain_order_id: OffchainOrderId,
        failed_at: DateTime<Utc>,
    ) -> Result<Self, LifecycleError<Position, ArithmeticError<FractionalShares>>> {
        if position.pending_offchain_order_id != Some(offchain_order_id) {
            return Err(Self::mismatch_error(position, event));
        }

        Ok(Self {
            pending_offchain_order_id: None,
            last_updated: Some(failed_at),
            ..position.clone()
        })
    }

    fn mismatch_error(
        position: &Self,
        event: &PositionEvent,
    ) -> LifecycleError<Position, ArithmeticError<FractionalShares>> {
        LifecycleError::Mismatch {
            state: Box::new(Lifecycle::Live(position.clone())),
            event: event.clone(),
        }
    }

    pub(crate) fn from_event(
        event: &PositionEvent,
    ) -> Result<Self, LifecycleError<Position, ArithmeticError<FractionalShares>>> {
        match event {
            PositionEvent::Initialized {
                symbol,
                threshold,
                initialized_at,
            } => Ok(Self {
                symbol: symbol.clone(),
                net: FractionalShares::ZERO,
                accumulated_long: FractionalShares::ZERO,
                accumulated_short: FractionalShares::ZERO,
                pending_offchain_order_id: None,
                threshold: *threshold,
                last_price_usdc: None,
                last_updated: Some(*initialized_at),
            }),

            _ => Err(LifecycleError::Mismatch {
                state: Box::new(Lifecycle::Uninitialized),
                event: event.clone(),
            }),
        }
    }

    fn create_trigger_reason(
        &self,
        threshold: &ExecutionThreshold,
    ) -> Result<Option<TriggerReason>, ArithmeticError<Usdc>> {
        match threshold {
            ExecutionThreshold::Shares(threshold_shares) => {
                let net_abs = self.net.abs();
                let threshold_value = threshold_shares.inner().inner();
                Ok(
                    (net_abs.inner() >= threshold_value).then_some(
                        TriggerReason::SharesThreshold {
                            net_position_shares: net_abs.inner(),
                            threshold_shares: threshold_value,
                        },
                    ),
                )
            }
            ExecutionThreshold::DollarValue(threshold_dollars) => {
                let Some(price) = self.last_price_usdc else {
                    warn!(
                        net_position = %self.net,
                        threshold_dollars = %threshold_dollars,
                        "Cannot evaluate ExecutionThreshold::DollarValue: last_price_usdc is None"
                    );
                    return Ok(None);
                };

                let net_abs = self.net.abs();
                let dollar_value = (Usdc(price) * net_abs.inner())?;

                if dollar_value.0 >= threshold_dollars.0 {
                    Ok(Some(TriggerReason::DollarThreshold {
                        net_position_shares: self.net.inner(),
                        dollar_value: dollar_value.0,
                        price_usdc: price,
                        threshold_dollars: threshold_dollars.0,
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

    fn handle_complete_offchain_order(
        &self,
        offchain_order_id: OffchainOrderId,
        shares_filled: Positive<FractionalShares>,
        direction: Direction,
        executor_order_id: ExecutorOrderId,
        price_cents: PriceCents,
        broker_timestamp: DateTime<Utc>,
    ) -> Result<Vec<PositionEvent>, PositionError> {
        self.validate_pending_execution(offchain_order_id)?;

        Ok(vec![PositionEvent::OffChainOrderFilled {
            offchain_order_id,
            shares_filled,
            direction,
            executor_order_id,
            price_cents,
            broker_timestamp,
        }])
    }

    fn handle_fail_offchain_order(
        &self,
        offchain_order_id: OffchainOrderId,
        error: String,
    ) -> Result<Vec<PositionEvent>, PositionError> {
        self.validate_pending_execution(offchain_order_id)?;

        Ok(vec![PositionEvent::OffChainOrderFailed {
            offchain_order_id,
            error,
            failed_at: Utc::now(),
        }])
    }

    /// Checks if this position is ready for execution
    /// based on its configured threshold.
    ///
    /// Returns `Ok(Some((direction, shares)))` if met:
    /// - `direction`: Sell for long, Buy for short
    /// - `shares`: Full fractional amount if executor
    ///   supports it, otherwise floored to whole shares
    ///
    /// Returns `Ok(None)` if threshold is not met or no
    /// price available for dollar-value threshold.
    ///
    /// Returns `Err` on arithmetic overflow.
    pub(crate) fn is_ready_for_execution(
        &self,
        executor: SupportedExecutor,
    ) -> Result<Option<(Direction, FractionalShares)>, PositionError> {
        if self.pending_offchain_order_id.is_some() {
            return Ok(None);
        }

        let trigger = self.create_trigger_reason(&self.threshold)?;

        match trigger {
            Some(TriggerReason::SharesThreshold { .. } | TriggerReason::DollarThreshold { .. }) => {
                let raw_shares = self.net.abs();

                let executable_shares = if executor.supports_fractional_shares() {
                    raw_shares
                } else {
                    FractionalShares::new(raw_shares.inner().trunc())
                };

                if executable_shares.is_zero() {
                    return Ok(None);
                }

                let direction = if self.net.inner() > Decimal::ZERO {
                    Direction::Sell
                } else {
                    Direction::Buy
                };

                Ok(Some((direction, executable_shares)))
            }
            None => Ok(None),
        }
    }

    fn handle_place_offchain_order(
        &self,
        offchain_order_id: OffchainOrderId,
        shares: Positive<FractionalShares>,
        direction: Direction,
        executor: SupportedExecutor,
    ) -> Result<Vec<PositionEvent>, PositionError> {
        if let Some(pending) = self.pending_offchain_order_id {
            return Err(PositionError::PendingExecution {
                offchain_order_id: pending,
            });
        }

        let trigger_reason =
            self.create_trigger_reason(&self.threshold)?
                .ok_or(PositionError::ThresholdNotMet {
                    net_position: self.net,
                    threshold: self.threshold,
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
}

#[async_trait]
impl Aggregate for Lifecycle<Position, ArithmeticError<FractionalShares>> {
    type Command = PositionCommand;
    type Event = PositionEvent;
    type Error = PositionError;
    type Services = ();

    fn aggregate_type() -> String {
        "Position".to_string()
    }

    fn apply(&mut self, event: Self::Event) {
        *self = self
            .clone()
            .transition(&event, Position::apply_transition)
            .or_initialize(&event, Position::from_event);
    }

    async fn handle(
        &self,
        command: Self::Command,
        _services: &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        match (self.live(), &command) {
            (
                Err(_),
                PositionCommand::AcknowledgeOnChainFill {
                    symbol,
                    threshold,
                    trade_id,
                    amount,
                    direction,
                    price_usdc,
                    block_timestamp,
                },
            ) => {
                let now = Utc::now();
                Ok(vec![
                    PositionEvent::Initialized {
                        symbol: symbol.clone(),
                        threshold: *threshold,
                        initialized_at: now,
                    },
                    PositionEvent::OnChainOrderFilled {
                        trade_id: trade_id.clone(),
                        amount: *amount,
                        direction: *direction,
                        price_usdc: *price_usdc,
                        block_timestamp: *block_timestamp,
                        seen_at: now,
                    },
                ])
            }

            (Err(error), _) => Err(error.into()),

            (
                Ok(_),
                PositionCommand::AcknowledgeOnChainFill {
                    trade_id,
                    amount,
                    direction,
                    price_usdc,
                    block_timestamp,
                    ..
                },
            ) => Ok(vec![PositionEvent::OnChainOrderFilled {
                trade_id: trade_id.clone(),
                amount: *amount,
                direction: *direction,
                price_usdc: *price_usdc,
                block_timestamp: *block_timestamp,
                seen_at: Utc::now(),
            }]),

            (
                Ok(position),
                PositionCommand::PlaceOffChainOrder {
                    offchain_order_id,
                    shares,
                    direction,
                    executor,
                    ..
                },
            ) => position.handle_place_offchain_order(
                *offchain_order_id,
                *shares,
                *direction,
                *executor,
            ),

            (
                Ok(position),
                PositionCommand::CompleteOffChainOrder {
                    offchain_order_id,
                    shares_filled,
                    direction,
                    executor_order_id,
                    price_cents,
                    broker_timestamp,
                },
            ) => position.handle_complete_offchain_order(
                *offchain_order_id,
                *shares_filled,
                *direction,
                executor_order_id.clone(),
                *price_cents,
                *broker_timestamp,
            ),

            (
                Ok(position),
                PositionCommand::FailOffChainOrder {
                    offchain_order_id,
                    error,
                },
            ) => position.handle_fail_offchain_order(*offchain_order_id, error.clone()),

            (Ok(position), PositionCommand::UpdateThreshold { threshold }) => {
                Ok(vec![PositionEvent::ThresholdUpdated {
                    old_threshold: position.threshold,
                    new_threshold: *threshold,
                    updated_at: Utc::now(),
                }])
            }
        }
    }
}

#[derive(Debug, thiserror::Error, PartialEq)]
pub(crate) enum PositionError {
    #[error(
        "Cannot place offchain order: position {net_position:?} does not meet threshold {threshold:?}"
    )]
    ThresholdNotMet {
        net_position: FractionalShares,
        threshold: ExecutionThreshold,
    },
    #[error("Cannot place offchain order: already have pending execution {offchain_order_id:?}")]
    PendingExecution { offchain_order_id: OffchainOrderId },
    #[error("Cannot complete offchain order: no pending execution")]
    NoPendingExecution,
    #[error("Offchain order ID mismatch: expected {expected:?}, got {actual:?}")]
    OffchainOrderIdMismatch {
        expected: OffchainOrderId,
        actual: OffchainOrderId,
    },
    #[error(transparent)]
    State(#[from] LifecycleError<Position, ArithmeticError<FractionalShares>>),
    #[error("Arithmetic error calculating threshold: {0}")]
    ThresholdCalculation(#[from] ArithmeticError<Usdc>),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum PositionCommand {
    AcknowledgeOnChainFill {
        symbol: Symbol,
        threshold: ExecutionThreshold,
        trade_id: TradeId,
        amount: FractionalShares,
        direction: Direction,
        price_usdc: Decimal,
        block_timestamp: DateTime<Utc>,
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
        price_cents: PriceCents,
        broker_timestamp: DateTime<Utc>,
    },
    FailOffChainOrder {
        offchain_order_id: OffchainOrderId,
        error: String,
    },
    UpdateThreshold {
        threshold: ExecutionThreshold,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) enum PositionEvent {
    Initialized {
        symbol: Symbol,
        threshold: ExecutionThreshold,
        initialized_at: DateTime<Utc>,
    },
    OnChainOrderFilled {
        trade_id: TradeId,
        amount: FractionalShares,
        direction: Direction,
        price_usdc: Decimal,
        block_timestamp: DateTime<Utc>,
        seen_at: DateTime<Utc>,
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
        price_cents: PriceCents,
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
}

impl PositionEvent {
    pub(crate) fn timestamp(&self) -> DateTime<Utc> {
        match self {
            Self::Initialized { initialized_at, .. } => *initialized_at,
            Self::OnChainOrderFilled { seen_at, .. } => *seen_at,
            Self::OffChainOrderPlaced { placed_at, .. } => *placed_at,
            Self::OffChainOrderFilled {
                broker_timestamp, ..
            } => *broker_timestamp,
            Self::OffChainOrderFailed { failed_at, .. } => *failed_at,
            Self::ThresholdUpdated { updated_at, .. } => *updated_at,
        }
    }
}

impl DomainEvent for PositionEvent {
    fn event_type(&self) -> String {
        match self {
            Self::Initialized { .. } => "PositionEvent::Initialized".to_string(),
            Self::OnChainOrderFilled { .. } => "PositionEvent::OnChainOrderFilled".to_string(),
            Self::OffChainOrderPlaced { .. } => "PositionEvent::OffChainOrderPlaced".to_string(),
            Self::OffChainOrderFilled { .. } => "PositionEvent::OffChainOrderFilled".to_string(),
            Self::OffChainOrderFailed { .. } => "PositionEvent::OffChainOrderFailed".to_string(),
            Self::ThresholdUpdated { .. } => "PositionEvent::ThresholdUpdated".to_string(),
        }
    }

    fn event_version(&self) -> String {
        "1.0".to_string()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct TradeId {
    pub(crate) tx_hash: TxHash,
    pub(crate) log_index: u64,
}

impl std::fmt::Display for TradeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.tx_hash, self.log_index)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) enum TriggerReason {
    SharesThreshold {
        net_position_shares: Decimal,
        threshold_shares: Decimal,
    },
    DollarThreshold {
        net_position_shares: Decimal,
        dollar_value: Decimal,
        price_usdc: Decimal,
        threshold_dollars: Decimal,
    },
}

#[cfg(test)]
mod tests {
    use cqrs_es::EventEnvelope;
    use cqrs_es::View;
    use cqrs_es::test::TestFramework;
    use rust_decimal_macros::dec;
    use std::collections::HashMap;
    use std::str::FromStr;

    use st0x_execution::Positive;

    use super::*;
    use crate::threshold::Usdc;

    fn one_share_threshold() -> ExecutionThreshold {
        ExecutionThreshold::shares(Positive::<FractionalShares>::ONE)
    }

    fn make_envelope(
        aggregate_id: &str,
        sequence: usize,
        event: PositionEvent,
    ) -> EventEnvelope<Lifecycle<Position, ArithmeticError<FractionalShares>>> {
        EventEnvelope {
            aggregate_id: aggregate_id.to_string(),
            sequence,
            payload: event,
            metadata: HashMap::new(),
        }
    }

    #[test]
    fn first_fill_initializes_and_accumulates() {
        let threshold = one_share_threshold();
        let trade_id = TradeId {
            tx_hash: TxHash::random(),
            log_index: 1,
        };

        let result =
            TestFramework::<Lifecycle<Position, ArithmeticError<FractionalShares>>>::with(())
                .given_no_previous_events()
                .when(PositionCommand::AcknowledgeOnChainFill {
                    symbol: Symbol::new("AAPL").unwrap(),
                    threshold,
                    trade_id,
                    amount: FractionalShares::new(dec!(0.5)),
                    direction: Direction::Buy,
                    price_usdc: dec!(150.0),
                    block_timestamp: Utc::now(),
                })
                .inspect_result();

        let events = result.unwrap();
        assert_eq!(events.len(), 2, "Expected Initialized + OnChainOrderFilled");
    }

    #[test]
    fn acknowledge_onchain_fill_accumulates_position() {
        let threshold = one_share_threshold();
        let trade_id = TradeId {
            tx_hash: TxHash::random(),
            log_index: 1,
        };
        let amount = FractionalShares::new(dec!(0.5));
        let price_usdc = dec!(150.0);
        let block_timestamp = Utc::now();

        let result =
            TestFramework::<Lifecycle<Position, ArithmeticError<FractionalShares>>>::with(())
                .given(vec![PositionEvent::Initialized {
                    symbol: Symbol::new("AAPL").unwrap(),
                    threshold,
                    initialized_at: Utc::now(),
                }])
                .when(PositionCommand::AcknowledgeOnChainFill {
                    symbol: Symbol::new("AAPL").unwrap(),
                    threshold,
                    trade_id,
                    amount,
                    direction: Direction::Buy,
                    price_usdc,
                    block_timestamp,
                })
                .inspect_result();

        assert_eq!(result.unwrap().len(), 1);
    }

    #[test]
    fn shares_threshold_triggers_execution() {
        let threshold = one_share_threshold();
        let trade_id1 = TradeId {
            tx_hash: TxHash::random(),
            log_index: 1,
        };
        let trade_id2 = TradeId {
            tx_hash: TxHash::random(),
            log_index: 2,
        };

        let offchain_order_id = OffchainOrderId::new();
        let shares = Positive::new(FractionalShares::ONE).unwrap();

        let result =
            TestFramework::<Lifecycle<Position, ArithmeticError<FractionalShares>>>::with(())
                .given(vec![
                    PositionEvent::Initialized {
                        symbol: Symbol::new("AAPL").unwrap(),
                        threshold,
                        initialized_at: Utc::now(),
                    },
                    PositionEvent::OnChainOrderFilled {
                        trade_id: trade_id1,
                        amount: FractionalShares::new(dec!(0.6)),
                        direction: Direction::Buy,
                        price_usdc: dec!(150.0),
                        block_timestamp: Utc::now(),
                        seen_at: Utc::now(),
                    },
                    PositionEvent::OnChainOrderFilled {
                        trade_id: trade_id2,
                        amount: FractionalShares::new(dec!(0.5)),
                        direction: Direction::Buy,
                        price_usdc: dec!(151.0),
                        block_timestamp: Utc::now(),
                        seen_at: Utc::now(),
                    },
                ])
                .when(PositionCommand::PlaceOffChainOrder {
                    offchain_order_id,
                    shares,
                    direction: Direction::Sell,
                    executor: SupportedExecutor::Schwab,
                    threshold,
                })
                .inspect_result();

        assert_eq!(result.unwrap().len(), 1);
    }

    #[test]
    fn place_offchain_order_below_threshold_fails() {
        let threshold = one_share_threshold();
        let trade_id = TradeId {
            tx_hash: TxHash::random(),
            log_index: 1,
        };
        let offchain_order_id = OffchainOrderId::new();

        TestFramework::<Lifecycle<Position, ArithmeticError<FractionalShares>>>::with(())
            .given(vec![
                PositionEvent::Initialized {
                    symbol: Symbol::new("AAPL").unwrap(),
                    threshold,
                    initialized_at: Utc::now(),
                },
                PositionEvent::OnChainOrderFilled {
                    trade_id,
                    amount: FractionalShares::new(dec!(0.5)),
                    direction: Direction::Buy,
                    price_usdc: dec!(150.0),
                    block_timestamp: Utc::now(),
                    seen_at: Utc::now(),
                },
            ])
            .when(PositionCommand::PlaceOffChainOrder {
                offchain_order_id,
                shares: Positive::new(FractionalShares::ONE).unwrap(),
                direction: Direction::Sell,
                executor: SupportedExecutor::Schwab,
                threshold,
            })
            .then_expect_error(PositionError::ThresholdNotMet {
                net_position: FractionalShares::new(dec!(0.5)),
                threshold,
            });
    }

    #[test]
    fn pending_execution_prevents_new_execution() {
        let threshold = one_share_threshold();
        let trade_id = TradeId {
            tx_hash: TxHash::random(),
            log_index: 1,
        };
        let offchain_order_id = OffchainOrderId::new();

        TestFramework::<Lifecycle<Position, ArithmeticError<FractionalShares>>>::with(())
            .given(vec![
                PositionEvent::Initialized {
                    symbol: Symbol::new("AAPL").unwrap(),
                    threshold,
                    initialized_at: Utc::now(),
                },
                PositionEvent::OnChainOrderFilled {
                    trade_id,
                    amount: FractionalShares::new(dec!(1.5)),
                    direction: Direction::Buy,
                    price_usdc: dec!(150.0),
                    block_timestamp: Utc::now(),
                    seen_at: Utc::now(),
                },
                PositionEvent::OffChainOrderPlaced {
                    offchain_order_id,
                    shares: Positive::new(FractionalShares::ONE).unwrap(),
                    direction: Direction::Sell,
                    executor: SupportedExecutor::Schwab,
                    trigger_reason: TriggerReason::SharesThreshold {
                        net_position_shares: dec!(1.5),
                        threshold_shares: dec!(1.0),
                    },
                    placed_at: Utc::now(),
                },
            ])
            .when(PositionCommand::PlaceOffChainOrder {
                offchain_order_id: OffchainOrderId::new(),
                shares: Positive::new(FractionalShares::new(dec!(0.5))).unwrap(),
                direction: Direction::Sell,
                executor: SupportedExecutor::Schwab,
                threshold,
            })
            .then_expect_error(PositionError::PendingExecution { offchain_order_id });
    }

    #[test]
    fn complete_offchain_order_clears_pending() {
        let threshold = one_share_threshold();
        let trade_id = TradeId {
            tx_hash: TxHash::random(),
            log_index: 1,
        };
        let offchain_order_id = OffchainOrderId::new();
        let executor_order_id = ExecutorOrderId::new("ORDER123");
        let price_cents = PriceCents(15050);

        let result =
            TestFramework::<Lifecycle<Position, ArithmeticError<FractionalShares>>>::with(())
                .given(vec![
                    PositionEvent::Initialized {
                        symbol: Symbol::new("AAPL").unwrap(),
                        threshold,
                        initialized_at: Utc::now(),
                    },
                    PositionEvent::OnChainOrderFilled {
                        trade_id,
                        amount: FractionalShares::new(dec!(1.5)),
                        direction: Direction::Buy,
                        price_usdc: dec!(150.0),
                        block_timestamp: Utc::now(),
                        seen_at: Utc::now(),
                    },
                    PositionEvent::OffChainOrderPlaced {
                        offchain_order_id,
                        shares: Positive::new(FractionalShares::ONE).unwrap(),
                        direction: Direction::Sell,
                        executor: SupportedExecutor::Schwab,
                        trigger_reason: TriggerReason::SharesThreshold {
                            net_position_shares: dec!(1.5),
                            threshold_shares: dec!(1.0),
                        },
                        placed_at: Utc::now(),
                    },
                ])
                .when(PositionCommand::CompleteOffChainOrder {
                    offchain_order_id,
                    shares_filled: Positive::new(FractionalShares::ONE).unwrap(),
                    direction: Direction::Sell,
                    executor_order_id,
                    price_cents,
                    broker_timestamp: Utc::now(),
                })
                .inspect_result();

        assert_eq!(result.unwrap().len(), 1);
    }

    #[test]
    fn fail_offchain_order_clears_pending() {
        let threshold = one_share_threshold();
        let trade_id = TradeId {
            tx_hash: TxHash::random(),
            log_index: 1,
        };
        let offchain_order_id = OffchainOrderId::new();

        let result =
            TestFramework::<Lifecycle<Position, ArithmeticError<FractionalShares>>>::with(())
                .given(vec![
                    PositionEvent::Initialized {
                        symbol: Symbol::new("AAPL").unwrap(),
                        threshold,
                        initialized_at: Utc::now(),
                    },
                    PositionEvent::OnChainOrderFilled {
                        trade_id,
                        amount: FractionalShares::new(dec!(1.5)),
                        direction: Direction::Buy,
                        price_usdc: dec!(150.0),
                        block_timestamp: Utc::now(),
                        seen_at: Utc::now(),
                    },
                    PositionEvent::OffChainOrderPlaced {
                        offchain_order_id,
                        shares: Positive::new(FractionalShares::ONE).unwrap(),
                        direction: Direction::Sell,
                        executor: SupportedExecutor::Schwab,
                        trigger_reason: TriggerReason::SharesThreshold {
                            net_position_shares: dec!(1.5),
                            threshold_shares: dec!(1.0),
                        },
                        placed_at: Utc::now(),
                    },
                ])
                .when(PositionCommand::FailOffChainOrder {
                    offchain_order_id,
                    error: "Broker API timeout".to_string(),
                })
                .inspect_result();

        assert_eq!(result.unwrap().len(), 1);
    }

    #[test]
    fn offchain_sell_reduces_net_position() {
        let threshold = one_share_threshold();
        let offchain_order_id = OffchainOrderId::new();
        let executor_order_id = ExecutorOrderId::new("ORDER123");
        let price_cents = PriceCents(15050);

        let events = vec![
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
                amount: FractionalShares::new(dec!(2.0)),
                direction: Direction::Buy,
                price_usdc: dec!(150.0),
                block_timestamp: Utc::now(),
                seen_at: Utc::now(),
            },
            PositionEvent::OffChainOrderPlaced {
                offchain_order_id,
                shares: Positive::new(FractionalShares::new(dec!(1.5))).unwrap(),
                direction: Direction::Sell,
                executor: SupportedExecutor::Schwab,
                trigger_reason: TriggerReason::SharesThreshold {
                    net_position_shares: dec!(2.0),
                    threshold_shares: dec!(1.0),
                },
                placed_at: Utc::now(),
            },
            PositionEvent::OffChainOrderFilled {
                offchain_order_id,
                shares_filled: Positive::new(FractionalShares::new(dec!(1.5))).unwrap(),
                direction: Direction::Sell,
                executor_order_id,
                price_cents,
                broker_timestamp: Utc::now(),
            },
        ];

        let mut aggregate = Lifecycle::<Position, ArithmeticError<FractionalShares>>::default();
        for event in events {
            aggregate.apply(event);
        }

        let Lifecycle::Live(position) = aggregate else {
            panic!("Expected Active state");
        };

        // OnChain buy of 2.0 + OffChain sell of 1.5 = net position of 0.5
        assert_eq!(position.net, FractionalShares::new(dec!(0.5)));
        assert!(
            position.pending_offchain_order_id.is_none(),
            "pending_offchain_order_id should be cleared after OffChainOrderFilled"
        );
    }

    #[test]
    fn offchain_buy_increases_net_position() {
        let threshold = one_share_threshold();
        let offchain_order_id = OffchainOrderId::new();
        let executor_order_id = ExecutorOrderId::new("ORDER456");
        let price_cents = PriceCents(15050);

        let events = vec![
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
                amount: FractionalShares::new(dec!(2.0)),
                direction: Direction::Sell,
                price_usdc: dec!(150.0),
                block_timestamp: Utc::now(),
                seen_at: Utc::now(),
            },
            PositionEvent::OffChainOrderPlaced {
                offchain_order_id,
                shares: Positive::new(FractionalShares::new(dec!(1.5))).unwrap(),
                direction: Direction::Buy,
                executor: SupportedExecutor::Schwab,
                trigger_reason: TriggerReason::SharesThreshold {
                    net_position_shares: dec!(2.0),
                    threshold_shares: dec!(1.0),
                },
                placed_at: Utc::now(),
            },
            PositionEvent::OffChainOrderFilled {
                offchain_order_id,
                shares_filled: Positive::new(FractionalShares::new(dec!(1.5))).unwrap(),
                direction: Direction::Buy,
                executor_order_id,
                price_cents,
                broker_timestamp: Utc::now(),
            },
        ];

        let mut aggregate = Lifecycle::<Position, ArithmeticError<FractionalShares>>::default();
        for event in events {
            aggregate.apply(event);
        }

        let Lifecycle::Live(position) = aggregate else {
            panic!("Expected Active state");
        };

        // OnChain sell of 2.0 + OffChain buy of 1.5 = net position of -0.5
        assert_eq!(position.net, FractionalShares::new(dec!(-0.5)));
        assert!(
            position.pending_offchain_order_id.is_none(),
            "pending_offchain_order_id should be cleared after OffChainOrderFilled"
        );
    }

    #[test]
    fn update_threshold_creates_audit_trail() {
        let old_threshold = one_share_threshold();
        let new_threshold =
            ExecutionThreshold::shares(Positive::new(FractionalShares::new(dec!(5.0))).unwrap());

        let result =
            TestFramework::<Lifecycle<Position, ArithmeticError<FractionalShares>>>::with(())
                .given(vec![PositionEvent::Initialized {
                    symbol: Symbol::new("AAPL").unwrap(),
                    threshold: old_threshold,
                    initialized_at: Utc::now(),
                }])
                .when(PositionCommand::UpdateThreshold {
                    threshold: new_threshold,
                })
                .inspect_result();

        assert_eq!(result.unwrap().len(), 1);
    }

    #[test]
    fn initialized_creates_active_state() {
        let symbol = Symbol::new("AAPL").unwrap();
        let initialized_at = Utc::now();

        let event = PositionEvent::Initialized {
            symbol: symbol.clone(),
            threshold: ExecutionThreshold::shares(
                Positive::new(FractionalShares::new(dec!(100))).unwrap(),
            ),
            initialized_at,
        };

        let mut view = Lifecycle::<Position, ArithmeticError<FractionalShares>>::default();
        assert!(matches!(view, Lifecycle::Uninitialized));

        view.update(&make_envelope(&symbol.to_string(), 1, event));

        let Lifecycle::Live(position) = view else {
            panic!("Expected Active state");
        };

        assert_eq!(position.symbol, symbol);
        assert_eq!(position.net, FractionalShares::ZERO);
        assert_eq!(position.accumulated_long, FractionalShares::ZERO);
        assert_eq!(position.accumulated_short, FractionalShares::ZERO);
        assert_eq!(position.pending_offchain_order_id, None);
        assert_eq!(position.last_updated, Some(initialized_at));
    }

    #[test]
    fn onchain_buy_increases_net_and_accumulated_long() {
        let symbol = Symbol::new("AAPL").unwrap();
        let initialized_at = Utc::now();
        let seen_at = Utc::now();

        let mut view = Lifecycle::Live(Position {
            symbol: symbol.clone(),
            net: FractionalShares::ZERO,
            accumulated_long: FractionalShares::ZERO,
            accumulated_short: FractionalShares::ZERO,
            pending_offchain_order_id: None,
            threshold: ExecutionThreshold::whole_share(),
            last_price_usdc: None,
            last_updated: Some(initialized_at),
        });

        let event = PositionEvent::OnChainOrderFilled {
            trade_id: TradeId {
                tx_hash: TxHash::from_str(
                    "0x1234567890123456789012345678901234567890123456789012345678901234",
                )
                .unwrap(),
                log_index: 0,
            },
            direction: Direction::Buy,
            amount: FractionalShares::new(dec!(10.5)),
            price_usdc: dec!(150.25),
            block_timestamp: seen_at,
            seen_at,
        };

        view.update(&make_envelope(&symbol.to_string(), 2, event));

        let Lifecycle::Live(position) = view else {
            panic!("Expected Active state");
        };

        assert_eq!(position.net, FractionalShares::new(dec!(10.5)));
        assert_eq!(position.accumulated_long, FractionalShares::new(dec!(10.5)));
        assert_eq!(position.accumulated_short, FractionalShares::ZERO);
        assert_eq!(position.last_updated, Some(seen_at));
    }

    #[test]
    fn onchain_sell_decreases_net_and_increases_accumulated_short() {
        let symbol = Symbol::new("AAPL").unwrap();
        let initialized_at = Utc::now();
        let seen_at = Utc::now();

        let mut view = Lifecycle::Live(Position {
            symbol: symbol.clone(),
            net: FractionalShares::new(dec!(20.0)),
            accumulated_long: FractionalShares::new(dec!(20.0)),
            accumulated_short: FractionalShares::ZERO,
            pending_offchain_order_id: None,
            threshold: ExecutionThreshold::whole_share(),
            last_price_usdc: None,
            last_updated: Some(initialized_at),
        });

        let event = PositionEvent::OnChainOrderFilled {
            trade_id: TradeId {
                tx_hash: TxHash::from_str(
                    "0x2234567890123456789012345678901234567890123456789012345678901234",
                )
                .unwrap(),
                log_index: 1,
            },
            direction: Direction::Sell,
            amount: FractionalShares::new(dec!(5.5)),
            price_usdc: dec!(149.75),
            block_timestamp: seen_at,
            seen_at,
        };

        view.update(&make_envelope(&symbol.to_string(), 3, event));

        let Lifecycle::Live(position) = view else {
            panic!("Expected Active state");
        };

        assert_eq!(position.net, FractionalShares::new(dec!(14.5)));
        assert_eq!(position.accumulated_long, FractionalShares::new(dec!(20.0)));
        assert_eq!(position.accumulated_short, FractionalShares::new(dec!(5.5)));
        assert_eq!(position.last_updated, Some(seen_at));
    }

    #[test]
    fn offchain_placed_sets_pending_execution() {
        let symbol = Symbol::new("AAPL").unwrap();
        let initialized_at = Utc::now();
        let placed_at = Utc::now();
        let offchain_order_id = OffchainOrderId::new();

        let mut view = Lifecycle::Live(Position {
            symbol: symbol.clone(),
            net: FractionalShares::new(dec!(100.0)),
            accumulated_long: FractionalShares::new(dec!(100.0)),
            accumulated_short: FractionalShares::ZERO,
            pending_offchain_order_id: None,
            threshold: ExecutionThreshold::whole_share(),
            last_price_usdc: None,
            last_updated: Some(initialized_at),
        });

        let event = PositionEvent::OffChainOrderPlaced {
            offchain_order_id,
            shares: Positive::new(FractionalShares::new(dec!(100))).unwrap(),
            direction: Direction::Sell,
            executor: SupportedExecutor::Schwab,
            trigger_reason: TriggerReason::SharesThreshold {
                net_position_shares: dec!(100),
                threshold_shares: dec!(100),
            },
            placed_at,
        };

        view.update(&make_envelope(&symbol.to_string(), 4, event));

        let Lifecycle::Live(position) = view else {
            panic!("Expected Active state");
        };

        assert_eq!(position.pending_offchain_order_id, Some(offchain_order_id));
        assert_eq!(position.last_updated, Some(placed_at));
    }

    #[test]
    fn offchain_filled_sell_reduces_net_and_clears_pending() {
        let symbol = Symbol::new("AAPL").unwrap();
        let initialized_at = Utc::now();
        let broker_timestamp = Utc::now();
        let offchain_order_id = OffchainOrderId::new();

        let mut view = Lifecycle::Live(Position {
            symbol: symbol.clone(),
            net: FractionalShares::new(dec!(100.0)),
            accumulated_long: FractionalShares::new(dec!(100.0)),
            accumulated_short: FractionalShares::ZERO,
            pending_offchain_order_id: Some(offchain_order_id),
            threshold: ExecutionThreshold::whole_share(),
            last_price_usdc: None,
            last_updated: Some(initialized_at),
        });

        let event = PositionEvent::OffChainOrderFilled {
            offchain_order_id,
            shares_filled: Positive::new(FractionalShares::new(dec!(100))).unwrap(),
            direction: Direction::Sell,
            executor_order_id: ExecutorOrderId::new("ORD123"),
            price_cents: PriceCents(15025),
            broker_timestamp,
        };

        view.update(&make_envelope(&symbol.to_string(), 5, event));

        let Lifecycle::Live(position) = view else {
            panic!("Expected Active state");
        };

        assert_eq!(position.net, FractionalShares::ZERO);
        assert_eq!(position.pending_offchain_order_id, None);
        assert_eq!(position.last_updated, Some(broker_timestamp));
    }

    #[test]
    fn offchain_filled_buy_increases_net() {
        let symbol = Symbol::new("AAPL").unwrap();
        let initialized_at = Utc::now();
        let broker_timestamp = Utc::now();
        let offchain_order_id = OffchainOrderId::new();

        let mut view = Lifecycle::Live(Position {
            symbol: symbol.clone(),
            net: FractionalShares::new(dec!(50.0)),
            accumulated_long: FractionalShares::new(dec!(50.0)),
            accumulated_short: FractionalShares::ZERO,
            pending_offchain_order_id: Some(offchain_order_id),
            threshold: ExecutionThreshold::whole_share(),
            last_price_usdc: None,
            last_updated: Some(initialized_at),
        });

        let event = PositionEvent::OffChainOrderFilled {
            offchain_order_id,
            shares_filled: Positive::new(FractionalShares::new(dec!(25))).unwrap(),
            direction: Direction::Buy,
            executor_order_id: ExecutorOrderId::new("ORD456"),
            price_cents: PriceCents(14500),
            broker_timestamp,
        };

        view.update(&make_envelope(&symbol.to_string(), 6, event));

        let Lifecycle::Live(position) = view else {
            panic!("Expected Active state");
        };

        assert_eq!(position.net, FractionalShares::new(dec!(75.0)));
        assert_eq!(position.pending_offchain_order_id, None);
        assert_eq!(position.last_updated, Some(broker_timestamp));
    }

    #[test]
    fn offchain_failed_clears_pending() {
        let symbol = Symbol::new("AAPL").unwrap();
        let initialized_at = Utc::now();
        let failed_at = Utc::now();
        let offchain_order_id = OffchainOrderId::new();

        let mut view = Lifecycle::Live(Position {
            symbol: symbol.clone(),
            net: FractionalShares::new(dec!(100.0)),
            accumulated_long: FractionalShares::new(dec!(100.0)),
            accumulated_short: FractionalShares::ZERO,
            pending_offchain_order_id: Some(offchain_order_id),
            threshold: ExecutionThreshold::whole_share(),
            last_price_usdc: None,
            last_updated: Some(initialized_at),
        });

        let event = PositionEvent::OffChainOrderFailed {
            offchain_order_id,
            error: "Market closed".to_string(),
            failed_at,
        };

        view.update(&make_envelope(&symbol.to_string(), 7, event));

        let Lifecycle::Live(position) = view else {
            panic!("Expected Active state");
        };

        assert_eq!(position.net, FractionalShares::new(dec!(100.0)));
        assert_eq!(position.pending_offchain_order_id, None);
        assert_eq!(position.last_updated, Some(failed_at));
    }

    #[test]
    fn threshold_updated_changes_last_updated() {
        let symbol = Symbol::new("AAPL").unwrap();
        let initialized_at = Utc::now();
        let updated_at = Utc::now();

        let mut view = Lifecycle::Live(Position {
            symbol: symbol.clone(),
            net: FractionalShares::new(dec!(100.0)),
            accumulated_long: FractionalShares::new(dec!(100.0)),
            accumulated_short: FractionalShares::ZERO,
            pending_offchain_order_id: None,
            threshold: ExecutionThreshold::shares(
                Positive::new(FractionalShares::new(dec!(100))).unwrap(),
            ),
            last_price_usdc: None,
            last_updated: Some(initialized_at),
        });

        let event = PositionEvent::ThresholdUpdated {
            old_threshold: ExecutionThreshold::shares(
                Positive::new(FractionalShares::new(dec!(100))).unwrap(),
            ),
            new_threshold: ExecutionThreshold::dollar_value(Usdc(dec!(10000))).unwrap(),
            updated_at,
        };

        view.update(&make_envelope(&symbol.to_string(), 8, event));

        let Lifecycle::Live(position) = view else {
            panic!("Expected Active state");
        };

        assert_eq!(position.last_updated, Some(updated_at));
    }

    #[test]
    fn transition_on_uninitialized_corrupts_state() {
        let mut view = Lifecycle::<Position, ArithmeticError<FractionalShares>>::default();

        let event = PositionEvent::OnChainOrderFilled {
            trade_id: TradeId {
                tx_hash: TxHash::from_str(
                    "0x3234567890123456789012345678901234567890123456789012345678901234",
                )
                .unwrap(),
                log_index: 0,
            },
            direction: Direction::Buy,
            amount: FractionalShares::new(dec!(10)),
            price_usdc: dec!(150.00),
            block_timestamp: Utc::now(),
            seen_at: Utc::now(),
        };

        view.update(&make_envelope("AAPL", 1, event));

        assert!(matches!(view, Lifecycle::Failed { .. }));
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
            amount: FractionalShares::ONE,
            direction: Direction::Buy,
            price_usdc: dec!(150.0),
            block_timestamp,
            seen_at,
        };

        assert_eq!(event.timestamp(), seen_at);
    }

    #[test]
    fn timestamp_returns_placed_at_for_offchain_order_placed_event() {
        let timestamp = Utc::now();
        let event = PositionEvent::OffChainOrderPlaced {
            offchain_order_id: OffchainOrderId::new(),
            shares: Positive::new(FractionalShares::ONE).unwrap(),
            direction: Direction::Sell,
            executor: SupportedExecutor::Schwab,
            trigger_reason: TriggerReason::SharesThreshold {
                net_position_shares: dec!(1.0),
                threshold_shares: dec!(1.0),
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
            shares_filled: Positive::new(FractionalShares::ONE).unwrap(),
            direction: Direction::Sell,
            executor_order_id: ExecutorOrderId::new("ORD123"),
            price_cents: PriceCents(15000),
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
                Positive::new(FractionalShares::new(dec!(5.0))).unwrap(),
            ),
            updated_at: timestamp,
        };

        assert_eq!(event.timestamp(), timestamp);
    }

    #[test]
    fn is_ready_for_execution_floors_shares_for_non_fractional_executor() {
        let position = Position {
            symbol: Symbol::new("AAPL").unwrap(),
            net: FractionalShares::new(dec!(1.212)),
            accumulated_long: FractionalShares::new(dec!(1.212)),
            accumulated_short: FractionalShares::ZERO,
            pending_offchain_order_id: None,
            threshold: ExecutionThreshold::whole_share(),
            last_price_usdc: Some(dec!(150.0)),
            last_updated: Some(Utc::now()),
        };

        let (direction, shares) = position
            .is_ready_for_execution(SupportedExecutor::Schwab)
            .unwrap()
            .expect("should be ready for execution");

        assert_eq!(direction, Direction::Sell);
        assert_eq!(
            shares.inner(),
            dec!(1),
            "Schwab executor should floor to whole shares: expected 1, got {}",
            shares.inner()
        );
    }

    #[test]
    fn is_ready_for_execution_floors_shares_for_negative_position_with_non_fractional_executor() {
        let position = Position {
            symbol: Symbol::new("AAPL").unwrap(),
            net: FractionalShares::new(dec!(-2.567)),
            accumulated_long: FractionalShares::ZERO,
            accumulated_short: FractionalShares::new(dec!(2.567)),
            pending_offchain_order_id: None,
            threshold: ExecutionThreshold::whole_share(),
            last_price_usdc: Some(dec!(150.0)),
            last_updated: Some(Utc::now()),
        };

        let (direction, shares) = position
            .is_ready_for_execution(SupportedExecutor::Schwab)
            .unwrap()
            .expect("should be ready for execution");

        assert_eq!(direction, Direction::Buy);
        assert_eq!(
            shares.inner(),
            dec!(2),
            "Schwab executor should floor to whole shares: expected 2, got {}",
            shares.inner()
        );
    }

    #[test]
    fn is_ready_for_execution_returns_fractional_shares_for_fractional_executor() {
        let position = Position {
            symbol: Symbol::new("AAPL").unwrap(),
            net: FractionalShares::new(dec!(1.212)),
            accumulated_long: FractionalShares::new(dec!(1.212)),
            accumulated_short: FractionalShares::ZERO,
            pending_offchain_order_id: None,
            threshold: ExecutionThreshold::dollar_value(Usdc(dec!(1))).unwrap(),
            last_price_usdc: Some(dec!(150.0)),
            last_updated: Some(Utc::now()),
        };

        let (direction, shares) = position
            .is_ready_for_execution(SupportedExecutor::AlpacaTradingApi)
            .unwrap()
            .expect("should be ready for execution");

        assert_eq!(direction, Direction::Sell);
        assert_eq!(
            shares.inner(),
            dec!(1.212),
            "Alpaca executor should return full fractional shares: expected 1.212, got {}",
            shares.inner()
        );
    }

    #[test]
    fn is_ready_for_execution_returns_fractional_for_negative_position_with_fractional_executor() {
        let position = Position {
            symbol: Symbol::new("AAPL").unwrap(),
            net: FractionalShares::new(dec!(-2.567)),
            accumulated_long: FractionalShares::ZERO,
            accumulated_short: FractionalShares::new(dec!(2.567)),
            pending_offchain_order_id: None,
            threshold: ExecutionThreshold::dollar_value(Usdc(dec!(1))).unwrap(),
            last_price_usdc: Some(dec!(150.0)),
            last_updated: Some(Utc::now()),
        };

        let (direction, shares) = position
            .is_ready_for_execution(SupportedExecutor::AlpacaTradingApi)
            .unwrap()
            .expect("should be ready for execution");

        assert_eq!(direction, Direction::Buy);
        assert_eq!(
            shares.inner(),
            dec!(2.567),
            "Alpaca executor should return full fractional shares: expected 2.567, got {}",
            shares.inner()
        );
    }

    #[tokio::test]
    async fn load_position_returns_none_when_no_aggregate_exists() {
        let pool = crate::test_utils::setup_test_db().await;
        let view_repo =
            std::sync::Arc::new(sqlite_es::SqliteViewRepository::<
                PositionAggregate,
                PositionAggregate,
            >::new(pool.clone(), "position_view".to_string()));
        let query = cqrs_es::persist::GenericQuery::new(view_repo);

        let result = load_position(&query, &Symbol::new("AAPL").unwrap())
            .await
            .unwrap();

        assert!(
            result.is_none(),
            "Should return None for non-existent aggregate"
        );
    }

    #[tokio::test]
    async fn load_position_returns_none_for_uninitialized_lifecycle() {
        let pool = crate::test_utils::setup_test_db().await;

        // Manually insert an Uninitialized lifecycle into the view table
        sqlx::query("INSERT INTO position_view (view_id, version, payload) VALUES (?, ?, ?)")
            .bind("AAPL")
            .bind(0i64)
            .bind(
                serde_json::to_string(
                    &Lifecycle::<Position, ArithmeticError<FractionalShares>>::default(),
                )
                .unwrap(),
            )
            .execute(&pool)
            .await
            .unwrap();

        let view_repo =
            std::sync::Arc::new(sqlite_es::SqliteViewRepository::<
                PositionAggregate,
                PositionAggregate,
            >::new(pool.clone(), "position_view".to_string()));
        let query = cqrs_es::persist::GenericQuery::new(view_repo);

        let result = load_position(&query, &Symbol::new("AAPL").unwrap())
            .await
            .unwrap();

        assert!(
            result.is_none(),
            "Should return None for uninitialized lifecycle"
        );
    }

    #[tokio::test]
    async fn load_position_returns_position_for_live_lifecycle() {
        let pool = crate::test_utils::setup_test_db().await;
        let view_repo =
            std::sync::Arc::new(sqlite_es::SqliteViewRepository::<
                PositionAggregate,
                PositionAggregate,
            >::new(pool.clone(), "position_view".to_string()));
        let position_query =
            std::sync::Arc::new(cqrs_es::persist::GenericQuery::new(view_repo.clone()));

        let cqrs = sqlite_es::sqlite_cqrs(
            pool.clone(),
            vec![Box::new(cqrs_es::persist::GenericQuery::new(view_repo))],
            (),
        );

        let symbol = Symbol::new("AAPL").unwrap();

        cqrs.execute(
            &Position::aggregate_id(&symbol),
            PositionCommand::AcknowledgeOnChainFill {
                symbol: symbol.clone(),
                threshold: one_share_threshold(),
                trade_id: TradeId {
                    tx_hash: TxHash::random(),
                    log_index: 1,
                },
                amount: FractionalShares::new(dec!(1)),
                direction: Direction::Buy,
                price_usdc: dec!(150),
                block_timestamp: Utc::now(),
            },
        )
        .await
        .unwrap();

        let result = load_position(&position_query, &symbol).await.unwrap();

        let position = result.expect("Should return Some for live lifecycle");
        assert_eq!(position.symbol, symbol);
        assert_eq!(position.net.inner(), dec!(1));
    }
}

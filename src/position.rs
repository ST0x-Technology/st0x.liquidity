//! Position CQRS/ES aggregate for tracking per-symbol
//! onchain/offchain exposure.
//!
//! Accumulates long and short fills, tracks the net
//! position, and decides when the imbalance exceeds the
//! threshold to trigger an offsetting broker order.

use alloy::primitives::TxHash;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use rain_math_float::{Float, FloatError};
use serde::{Deserialize, Serialize};
use tracing::warn;

use st0x_execution::{
    Direction, ExecutorOrderId, FractionalShares, HasZero, Positive, SupportedExecutor, Symbol,
};
use st0x_finance::{Usd, Usdc};

use st0x_event_sorcery::{DomainEvent, EventSourced, Table};

use crate::offchain_order::OffchainOrderId;
use crate::threshold::ExecutionThreshold;

#[derive(Clone, Serialize, Deserialize)]
pub struct Position {
    pub symbol: Symbol,
    pub net: FractionalShares,
    pub accumulated_long: FractionalShares,
    pub accumulated_short: FractionalShares,
    pub pending_offchain_order_id: Option<OffchainOrderId>,
    pub threshold: ExecutionThreshold,
    #[serde(
        serialize_with = "st0x_float_serde::serialize_option_float",
        deserialize_with = "st0x_float_serde::deserialize_option_float_from_number_or_string"
    )]
    pub last_price_usdc: Option<Float>,
    pub last_updated: Option<DateTime<Utc>>,
}

impl std::fmt::Debug for Position {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use st0x_float_serde::DebugOptionFloat;

        f.debug_struct("Position")
            .field("symbol", &self.symbol)
            .field("net", &self.net)
            .field("accumulated_long", &self.accumulated_long)
            .field("accumulated_short", &self.accumulated_short)
            .field("pending_offchain_order_id", &self.pending_offchain_order_id)
            .field("threshold", &self.threshold)
            .field("last_price_usdc", &DebugOptionFloat(&self.last_price_usdc))
            .field("last_updated", &self.last_updated)
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
    const SCHEMA_VERSION: u64 = 1;

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
                threshold: *threshold,
                last_price_usdc: None,
                last_updated: Some(*initialized_at),
            }),

            _ => None,
        }
    }

    fn evolve(entity: &Self, event: &Self::Event) -> Result<Option<Self>, Self::Error> {
        use Direction::{Buy, Sell};
        use PositionEvent::*;

        match event {
            OnChainOrderFilled {
                amount,
                direction: Buy,
                price_usdc,
                seen_at,
                ..
            } => Ok(Some(Self {
                net: (entity.net + *amount)?,
                accumulated_long: (entity.accumulated_long + *amount)?,
                last_price_usdc: Some(*price_usdc),
                last_updated: Some(*seen_at),
                ..entity.clone()
            })),

            OnChainOrderFilled {
                direction: Sell,
                amount,
                price_usdc,
                seen_at,
                ..
            } => Ok(Some(Self {
                net: (entity.net - *amount)?,
                accumulated_short: (entity.accumulated_short + *amount)?,
                last_price_usdc: Some(*price_usdc),
                last_updated: Some(*seen_at),
                ..entity.clone()
            })),

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
                last_updated: Some(*broker_timestamp),
                ..entity.clone()
            })),

            OffChainOrderFailed {
                offchain_order_id, ..
            } if entity.pending_offchain_order_id != Some(*offchain_order_id) => Ok(None),

            OffChainOrderFailed { failed_at, .. } => Ok(Some(Self {
                pending_offchain_order_id: None,
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
                        trade_id,
                        amount,
                        direction,
                        price_usdc,
                        block_timestamp,
                        seen_at: now,
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
            } => Ok(vec![PositionEvent::OnChainOrderFilled {
                trade_id,
                amount,
                direction,
                price_usdc,
                block_timestamp,
                seen_at: Utc::now(),
            }]),

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
                            %offchain_order_id, %self.symbol,
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
                    warn!(
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
        if let Some(pending_id) = self.pending_offchain_order_id
            && pending_id != offchain_order_id
        {
            return Err(PositionError::OffchainOrderIdMismatch {
                expected: pending_id,
                actual: offchain_order_id,
            });
        }

        Ok(())
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
                        warn!(
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

                let executable_shares = if executor.supports_fractional_shares() {
                    capped_shares
                } else {
                    FractionalShares::new(capped_shares.inner().integer()?)
                };

                if executable_shares.is_zero()? {
                    return Ok(None);
                }

                let direction = if self.net.is_negative()? {
                    Direction::Buy
                } else {
                    Direction::Sell
                };

                Ok(Some((direction, executable_shares)))
            }
            None => Ok(None),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, thiserror::Error)]
pub enum PositionError {
    #[error("Position has not been initialized")]
    Uninitialized,
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
}

impl PositionEvent {
    pub(crate) fn timestamp(&self) -> DateTime<Utc> {
        use PositionEvent::*;
        match self {
            Initialized { initialized_at, .. } => *initialized_at,
            OnChainOrderFilled { seen_at, .. } => *seen_at,
            OffChainOrderPlaced { placed_at, .. } => *placed_at,
            OffChainOrderFilled {
                broker_timestamp, ..
            } => *broker_timestamp,
            OffChainOrderFailed { failed_at, .. } => *failed_at,
            ThresholdUpdated { updated_at, .. } => *updated_at,
        }
    }
}

impl DomainEvent for PositionEvent {
    fn event_type(&self) -> String {
        use PositionEvent::*;
        match self {
            Initialized { .. } => "PositionEvent::Initialized".to_string(),
            OnChainOrderFilled { .. } => "PositionEvent::OnChainOrderFilled".to_string(),
            OffChainOrderPlaced { .. } => "PositionEvent::OffChainOrderPlaced".to_string(),
            OffChainOrderFilled { .. } => "PositionEvent::OffChainOrderFilled".to_string(),
            OffChainOrderFailed { .. } => "PositionEvent::OffChainOrderFailed".to_string(),
            ThresholdUpdated { .. } => "PositionEvent::ThresholdUpdated".to_string(),
        }
    }

    fn event_version(&self) -> String {
        "1.0".to_string()
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
            _ => false,
        }
    }
}

impl Eq for PositionEvent {}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
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
        use st0x_float_serde::DebugFloat;

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

        assert_eq!(events.len(), 2, "Expected Initialized + OnChainOrderFilled");
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

        assert_eq!(events.len(), 1);
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
                executor: SupportedExecutor::Schwab,
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
                executor: SupportedExecutor::Schwab,
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
                    executor: SupportedExecutor::Schwab,
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
                executor: SupportedExecutor::Schwab,
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
                    executor: SupportedExecutor::Schwab,
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
                    executor: SupportedExecutor::Schwab,
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
                executor: SupportedExecutor::Schwab,
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
                executor: SupportedExecutor::Schwab,
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
                executor: SupportedExecutor::Schwab,
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
        ])
        .unwrap()
        .unwrap();

        assert_eq!(position.net, FractionalShares::new(float!(1.5)));
        assert!(
            position.pending_offchain_order_id.is_none(),
            "pending_offchain_order_id should be cleared \
             after OffChainOrderFailed"
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
    fn timestamp_returns_placed_at_for_offchain_order_placed_event() {
        let timestamp = Utc::now();
        let event = PositionEvent::OffChainOrderPlaced {
            offchain_order_id: OffchainOrderId::new(),
            shares: Positive::new(FractionalShares::new(float!(1))).unwrap(),
            direction: Direction::Sell,
            executor: SupportedExecutor::Schwab,
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
    fn is_ready_for_execution_floors_shares_for_non_fractional_executor() {
        let position = Position {
            symbol: Symbol::new("AAPL").unwrap(),
            net: FractionalShares::new(float!(1.212)),
            accumulated_long: FractionalShares::new(float!(1.212)),
            accumulated_short: FractionalShares::ZERO,
            pending_offchain_order_id: None,
            threshold: ExecutionThreshold::whole_share(),
            last_price_usdc: Some(float!(150)),
            last_updated: Some(Utc::now()),
        };

        let (direction, shares) = position
            .is_ready_for_execution(SupportedExecutor::Schwab, None)
            .unwrap()
            .expect("should be ready for execution");

        assert_eq!(direction, Direction::Sell);
        assert_eq!(
            shares,
            FractionalShares::new(float!(1)),
            "Schwab executor should floor to whole \
             shares: expected 1, got {shares}",
        );
    }

    #[test]
    fn is_ready_for_execution_floors_shares_for_negative_position_with_non_fractional_executor() {
        let position = Position {
            symbol: Symbol::new("AAPL").unwrap(),
            net: FractionalShares::new(float!(-2.567)),
            accumulated_long: FractionalShares::ZERO,
            accumulated_short: FractionalShares::new(float!(2.567)),
            pending_offchain_order_id: None,
            threshold: ExecutionThreshold::whole_share(),
            last_price_usdc: Some(float!(150)),
            last_updated: Some(Utc::now()),
        };

        let (direction, shares) = position
            .is_ready_for_execution(SupportedExecutor::Schwab, None)
            .unwrap()
            .expect("should be ready for execution");

        assert_eq!(direction, Direction::Buy);
        assert_eq!(
            shares,
            FractionalShares::new(float!(2)),
            "Schwab executor should floor to whole \
             shares: expected 2, got {shares}",
        );
    }

    #[test]
    fn is_ready_for_execution_returns_fractional_shares_for_fractional_executor() {
        let position = Position {
            symbol: Symbol::new("AAPL").unwrap(),
            net: FractionalShares::new(float!(1.212)),
            accumulated_long: FractionalShares::new(float!(1.212)),
            accumulated_short: FractionalShares::ZERO,
            pending_offchain_order_id: None,
            threshold: ExecutionThreshold::dollar_value(Usdc::new(float!(1))).unwrap(),
            last_price_usdc: Some(float!(150)),
            last_updated: Some(Utc::now()),
        };

        let (direction, shares) = position
            .is_ready_for_execution(SupportedExecutor::AlpacaTradingApi, None)
            .unwrap()
            .expect("should be ready for execution");

        assert_eq!(direction, Direction::Sell);
        assert_eq!(
            shares,
            FractionalShares::new(float!(1.212)),
            "Alpaca executor should return full \
             fractional shares: expected 1.212, got {shares}",
        );
    }

    #[test]
    fn is_ready_for_execution_returns_fractional_for_negative_position_with_fractional_executor() {
        let position = Position {
            symbol: Symbol::new("AAPL").unwrap(),
            net: FractionalShares::new(float!(-2.567)),
            accumulated_long: FractionalShares::ZERO,
            accumulated_short: FractionalShares::new(float!(2.567)),
            pending_offchain_order_id: None,
            threshold: ExecutionThreshold::dollar_value(Usdc::new(float!(1))).unwrap(),
            last_price_usdc: Some(float!(150)),
            last_updated: Some(Utc::now()),
        };

        let (direction, shares) = position
            .is_ready_for_execution(SupportedExecutor::AlpacaTradingApi, None)
            .unwrap()
            .expect("should be ready for execution");

        assert_eq!(direction, Direction::Buy);
        assert_eq!(
            shares,
            FractionalShares::new(float!(2.567)),
            "Alpaca executor should return full \
             fractional shares: expected 2.567, got {shares}",
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
            threshold: ExecutionThreshold::whole_share(),
            last_price_usdc: Some(float!(150)),
            last_updated: Some(Utc::now()),
        };

        let shares_limit = Positive::new(FractionalShares::new(float!(50))).unwrap();

        let (direction, shares) = position
            .is_ready_for_execution(SupportedExecutor::DryRun, Some(shares_limit))
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
            threshold: ExecutionThreshold::whole_share(),
            last_price_usdc: Some(float!(150)),
            last_updated: Some(Utc::now()),
        };

        let shares_limit = Positive::new(FractionalShares::new(float!(50))).unwrap();

        let (direction, shares) = position
            .is_ready_for_execution(SupportedExecutor::DryRun, Some(shares_limit))
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
    fn operational_limits_cap_floors_for_non_fractional_executor() {
        let position = Position {
            symbol: Symbol::new("AAPL").unwrap(),
            net: FractionalShares::new(float!(100)),
            accumulated_long: FractionalShares::new(float!(100)),
            accumulated_short: FractionalShares::ZERO,
            pending_offchain_order_id: None,
            threshold: ExecutionThreshold::whole_share(),
            last_price_usdc: Some(float!(150)),
            last_updated: Some(Utc::now()),
        };

        let shares_limit = Positive::new(FractionalShares::new(float!(50.7))).unwrap();

        let (_, shares) = position
            .is_ready_for_execution(SupportedExecutor::Schwab, Some(shares_limit))
            .unwrap()
            .expect("should be ready for execution");

        assert_eq!(
            shares,
            FractionalShares::new(float!(50)),
            "Non-fractional executor should floor capped shares: \
             cap is 50.7, floored to 50, got {shares}",
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
            threshold: ExecutionThreshold::whole_share(),
            last_price_usdc: Some(float!(150)),
            last_updated: Some(Utc::now()),
        };

        let shares_limit = Positive::new(FractionalShares::new(float!(7.5))).unwrap();

        let (direction, shares) = position
            .is_ready_for_execution(SupportedExecutor::AlpacaTradingApi, Some(shares_limit))
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
            threshold: ExecutionThreshold::whole_share(),
            last_price_usdc: Some(float!(150)),
            last_updated: Some(Utc::now()),
        };

        let shares_limit = Positive::new(FractionalShares::new(float!(5))).unwrap();

        let (direction, shares) = position
            .is_ready_for_execution(SupportedExecutor::AlpacaTradingApi, Some(shares_limit))
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
            threshold: ExecutionThreshold::whole_share(),
            last_price_usdc: None,
            last_updated: Some(Utc::now()),
        };

        let shares_limit = Positive::new(FractionalShares::new(float!(50))).unwrap();

        let (direction, shares) = position
            .is_ready_for_execution(SupportedExecutor::DryRun, Some(shares_limit))
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
            threshold: ExecutionThreshold::whole_share(),
            last_price_usdc: Some(float!(150)),
            last_updated: Some(Utc::now()),
        };

        let (_, first_shares) = position
            .is_ready_for_execution(SupportedExecutor::DryRun, Some(shares_limit))
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
            .is_ready_for_execution(SupportedExecutor::DryRun, Some(shares_limit))
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
            .is_ready_for_execution(SupportedExecutor::DryRun, Some(shares_limit))
            .unwrap()
            .expect("remaining 20 shares still exceeds threshold");
        assert_eq!(
            third_shares,
            FractionalShares::new(float!(20)),
            "Third execution returns remaining 20 (below cap)"
        );
    }
}

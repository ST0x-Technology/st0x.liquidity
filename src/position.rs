//! Position CQRS/ES aggregate for tracking per-symbol
//! onchain/offchain exposure.
//!
//! Accumulates long and short fills, tracks the net
//! position, and decides when the imbalance exceeds the
//! threshold to trigger an offsetting broker order.

use alloy::primitives::TxHash;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use tracing::warn;

use st0x_execution::{
    ArithmeticError, Direction, ExecutorOrderId, FractionalShares, Positive, SupportedExecutor,
    Symbol,
};

use st0x_event_sorcery::{DomainEvent, EventSourced, Table};

use crate::config::OperationalLimits;
use crate::offchain_order::{Dollars, OffchainOrderId};
use crate::threshold::{ExecutionThreshold, Usdc};

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

#[async_trait]
impl EventSourced for Position {
    type Id = Symbol;
    type Event = PositionEvent;
    type Command = PositionCommand;
    type Error = PositionError;
    type Services = ();

    const AGGREGATE_TYPE: &'static str = "Position";
    const PROJECTION: Option<Table> = Some(Table("position_view"));
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
        use PositionEvent::*;
        match event {
            OnChainOrderFilled {
                amount,
                direction,
                price_usdc,
                seen_at,
                ..
            } => match direction {
                Direction::Buy => Ok(Some(Self {
                    net: (entity.net + *amount)?,
                    accumulated_long: (entity.accumulated_long + *amount)?,
                    last_price_usdc: Some(*price_usdc),
                    last_updated: Some(*seen_at),
                    ..entity.clone()
                })),
                Direction::Sell => Ok(Some(Self {
                    net: (entity.net - *amount)?,
                    accumulated_short: (entity.accumulated_short + *amount)?,
                    last_price_usdc: Some(*price_usdc),
                    last_updated: Some(*seen_at),
                    ..entity.clone()
                })),
            },

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
                shares_filled,
                direction,
                broker_timestamp,
                ..
            } => match direction {
                Direction::Sell => Ok(Some(Self {
                    net: (entity.net - shares_filled.inner())?,
                    pending_offchain_order_id: None,
                    last_updated: Some(*broker_timestamp),
                    ..entity.clone()
                })),
                Direction::Buy => Ok(Some(Self {
                    net: (entity.net + shares_filled.inner())?,
                    pending_offchain_order_id: None,
                    last_updated: Some(*broker_timestamp),
                    ..entity.clone()
                })),
            },

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

                let trigger_reason = self.create_trigger_reason(&self.threshold)?.ok_or(
                    PositionError::ThresholdNotMet {
                        net_position: self.net,
                        threshold: self.threshold,
                    },
                )?;

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
        limits: &OperationalLimits,
    ) -> Result<Option<(Direction, FractionalShares)>, PositionError> {
        if self.pending_offchain_order_id.is_some() {
            return Ok(None);
        }

        let trigger = self.create_trigger_reason(&self.threshold)?;

        match trigger {
            Some(TriggerReason::SharesThreshold { .. } | TriggerReason::DollarThreshold { .. }) => {
                let raw_shares = self.net.abs();

                let floored_shares = if executor.supports_fractional_shares() {
                    raw_shares
                } else {
                    FractionalShares::new(raw_shares.inner().trunc())
                };

                let executable_shares =
                    if let OperationalLimits::Enabled { max_shares, .. } = limits {
                        let cap = max_shares.inner();
                        if floored_shares > cap {
                            warn!(
                                symbol = %self.symbol,
                                computed = %floored_shares,
                                limit = %cap,
                                "Counter trade shares capped by operational limit"
                            );
                            cap
                        } else {
                            floored_shares
                        }
                    } else {
                        floored_shares
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
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, thiserror::Error)]
pub(crate) enum PositionError {
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
    #[error(transparent)]
    Arithmetic(#[from] ArithmeticError<FractionalShares>),
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
        price: Dollars,
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
        price: Dollars,
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
    use rust_decimal_macros::dec;
    use std::sync::Arc;

    use st0x_execution::Positive;

    use st0x_event_sorcery::{LifecycleError, Projection, StoreBuilder, TestHarness, replay};

    use super::*;
    use crate::config::OperationalLimits;
    use crate::threshold::Usdc;

    fn one_share_threshold() -> ExecutionThreshold {
        ExecutionThreshold::shares(Positive::<FractionalShares>::ONE)
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
                amount: FractionalShares::new(dec!(0.5)),
                direction: Direction::Buy,
                price_usdc: dec!(150.0),
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
                amount: FractionalShares::new(dec!(0.5)),
                direction: Direction::Buy,
                price_usdc: dec!(150.0),
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
                    amount: FractionalShares::new(dec!(0.6)),
                    direction: Direction::Buy,
                    price_usdc: dec!(150.0),
                    block_timestamp: Utc::now(),
                    seen_at: Utc::now(),
                },
                PositionEvent::OnChainOrderFilled {
                    trade_id: TradeId {
                        tx_hash: TxHash::random(),
                        log_index: 2,
                    },
                    amount: FractionalShares::new(dec!(0.5)),
                    direction: Direction::Buy,
                    price_usdc: dec!(151.0),
                    block_timestamp: Utc::now(),
                    seen_at: Utc::now(),
                },
            ])
            .when(PositionCommand::PlaceOffChainOrder {
                offchain_order_id: OffchainOrderId::new(),
                shares: Positive::new(FractionalShares::ONE).unwrap(),
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
                    amount: FractionalShares::new(dec!(0.5)),
                    direction: Direction::Buy,
                    price_usdc: dec!(150.0),
                    block_timestamp: Utc::now(),
                    seen_at: Utc::now(),
                },
            ])
            .when(PositionCommand::PlaceOffChainOrder {
                offchain_order_id: OffchainOrderId::new(),
                shares: Positive::new(FractionalShares::ONE).unwrap(),
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
                executor_order_id: ExecutorOrderId::new("ORDER123"),
                price: Dollars(dec!(150.50)),
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
                    Positive::new(FractionalShares::new(dec!(5.0))).unwrap(),
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
                executor_order_id: ExecutorOrderId::new("ORDER123"),
                price: Dollars(dec!(150.50)),
                broker_timestamp: Utc::now(),
            },
        ])
        .unwrap()
        .unwrap();

        assert_eq!(position.net, FractionalShares::new(dec!(0.5)));
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
                executor_order_id: ExecutorOrderId::new("ORDER456"),
                price: Dollars(dec!(150.50)),
                broker_timestamp: Utc::now(),
            },
        ])
        .unwrap()
        .unwrap();

        assert_eq!(position.net, FractionalShares::new(dec!(-0.5)));
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
            PositionEvent::OffChainOrderFailed {
                offchain_order_id,
                error: "Market closed".to_string(),
                failed_at: Utc::now(),
            },
        ])
        .unwrap()
        .unwrap();

        assert_eq!(position.net, FractionalShares::new(dec!(1.5)));
        assert!(
            position.pending_offchain_order_id.is_none(),
            "pending_offchain_order_id should be cleared \
             after OffChainOrderFailed"
        );
    }

    #[test]
    fn threshold_updated_changes_threshold() {
        let new_threshold = ExecutionThreshold::dollar_value(Usdc(dec!(10000))).unwrap();

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
            amount: FractionalShares::new(dec!(10)),
            price_usdc: dec!(150.00),
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
            price: Dollars(dec!(150.00)),
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
            .is_ready_for_execution(SupportedExecutor::Schwab, &OperationalLimits::Disabled)
            .unwrap()
            .expect("should be ready for execution");

        assert_eq!(direction, Direction::Sell);
        assert_eq!(
            shares.inner(),
            dec!(1),
            "Schwab executor should floor to whole \
             shares: expected 1, got {}",
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
            .is_ready_for_execution(SupportedExecutor::Schwab, &OperationalLimits::Disabled)
            .unwrap()
            .expect("should be ready for execution");

        assert_eq!(direction, Direction::Buy);
        assert_eq!(
            shares.inner(),
            dec!(2),
            "Schwab executor should floor to whole \
             shares: expected 2, got {}",
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
            .is_ready_for_execution(
                SupportedExecutor::AlpacaTradingApi,
                &OperationalLimits::Disabled,
            )
            .unwrap()
            .expect("should be ready for execution");

        assert_eq!(direction, Direction::Sell);
        assert_eq!(
            shares.inner(),
            dec!(1.212),
            "Alpaca executor should return full \
             fractional shares: expected 1.212, got {}",
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
            .is_ready_for_execution(
                SupportedExecutor::AlpacaTradingApi,
                &OperationalLimits::Disabled,
            )
            .unwrap()
            .expect("should be ready for execution");

        assert_eq!(direction, Direction::Buy);
        assert_eq!(
            shares.inner(),
            dec!(2.567),
            "Alpaca executor should return full \
             fractional shares: expected 2.567, got {}",
            shares.inner()
        );
    }

    #[test]
    fn operational_limits_cap_shares() {
        let position = Position {
            symbol: Symbol::new("AAPL").unwrap(),
            net: FractionalShares::new(dec!(100)),
            accumulated_long: FractionalShares::new(dec!(100)),
            accumulated_short: FractionalShares::ZERO,
            pending_offchain_order_id: None,
            threshold: ExecutionThreshold::whole_share(),
            last_price_usdc: Some(dec!(150.0)),
            last_updated: Some(Utc::now()),
        };

        let limits = OperationalLimits::Enabled {
            max_shares: Positive::new(FractionalShares::new(dec!(50))).unwrap(),
            max_amount: Positive::new(Usdc(dec!(1000))).unwrap(),
        };

        let (direction, shares) = position
            .is_ready_for_execution(SupportedExecutor::DryRun, &limits)
            .unwrap()
            .expect("should be ready for execution");

        assert_eq!(direction, Direction::Sell);
        assert_eq!(
            shares.inner(),
            dec!(50),
            "Shares should be capped by operational \
             limit: expected 50, got {}",
            shares.inner()
        );
    }

    #[test]
    fn operational_limits_do_not_cap_below_limit() {
        let position = Position {
            symbol: Symbol::new("AAPL").unwrap(),
            net: FractionalShares::new(dec!(30)),
            accumulated_long: FractionalShares::new(dec!(30)),
            accumulated_short: FractionalShares::ZERO,
            pending_offchain_order_id: None,
            threshold: ExecutionThreshold::whole_share(),
            last_price_usdc: Some(dec!(150.0)),
            last_updated: Some(Utc::now()),
        };

        let limits = OperationalLimits::Enabled {
            max_shares: Positive::new(FractionalShares::new(dec!(50))).unwrap(),
            max_amount: Positive::new(Usdc(dec!(1000))).unwrap(),
        };

        let (direction, shares) = position
            .is_ready_for_execution(SupportedExecutor::DryRun, &limits)
            .unwrap()
            .expect("should be ready for execution");

        assert_eq!(direction, Direction::Sell);
        assert_eq!(
            shares.inner(),
            dec!(30),
            "Shares should not be capped when below \
             limit: expected 30, got {}",
            shares.inner()
        );
    }

    #[tokio::test]
    async fn load_position_returns_none_when_no_aggregate_exists() {
        let pool = crate::test_utils::setup_test_db().await;
        let projection = Projection::<Position>::sqlite(pool.clone()).unwrap();

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
        let projection = Projection::<Position>::sqlite(pool.clone()).unwrap();

        let store = StoreBuilder::<Position>::new(pool.clone())
            .with(Arc::new(projection.clone()))
            .build(())
            .await
            .unwrap();

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
                    amount: FractionalShares::new(dec!(1)),
                    direction: Direction::Buy,
                    price_usdc: dec!(150),
                    block_timestamp: Utc::now(),
                },
            )
            .await
            .unwrap();

        let result = projection.load(&symbol).await.unwrap();

        let position = result.expect("Should return Some for live lifecycle");
        assert_eq!(position.symbol, symbol);
        assert_eq!(position.net.inner(), dec!(1));
    }

    #[test]
    fn capped_execution_leaves_remaining_exposure_triggerable() {
        let limits = OperationalLimits::Enabled {
            max_shares: Positive::new(FractionalShares::new(dec!(50))).unwrap(),
            max_amount: Positive::new(Usdc(dec!(1000))).unwrap(),
        };

        let position = Position {
            symbol: Symbol::new("AAPL").unwrap(),
            net: FractionalShares::new(dec!(120)),
            accumulated_long: FractionalShares::new(dec!(120)),
            accumulated_short: FractionalShares::ZERO,
            pending_offchain_order_id: None,
            threshold: ExecutionThreshold::whole_share(),
            last_price_usdc: Some(dec!(150.0)),
            last_updated: Some(Utc::now()),
        };

        let (_, first_shares) = position
            .is_ready_for_execution(SupportedExecutor::DryRun, &limits)
            .unwrap()
            .expect("first check should trigger");
        assert_eq!(
            first_shares.inner(),
            dec!(50),
            "First execution capped to 50"
        );

        // Simulate executing 50 shares: net goes from 120 to 70
        let after_first = Position {
            net: FractionalShares::new(dec!(70)),
            accumulated_long: FractionalShares::new(dec!(70)),
            ..position.clone()
        };

        let (_, second_shares) = after_first
            .is_ready_for_execution(SupportedExecutor::DryRun, &limits)
            .unwrap()
            .expect("remaining 70 shares still exceeds threshold");
        assert_eq!(
            second_shares.inner(),
            dec!(50),
            "Second execution capped to 50"
        );

        // Simulate executing another 50: net goes from 70 to 20
        let after_second = Position {
            net: FractionalShares::new(dec!(20)),
            accumulated_long: FractionalShares::new(dec!(20)),
            ..position.clone()
        };

        let (_, third_shares) = after_second
            .is_ready_for_execution(SupportedExecutor::DryRun, &limits)
            .unwrap()
            .expect("remaining 20 shares still exceeds threshold");
        assert_eq!(
            third_shares.inner(),
            dec!(20),
            "Third execution returns remaining 20 (below cap)"
        );
    }
}

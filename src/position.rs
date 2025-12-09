//! Position aggregate for tracking onchain/offchain exposure.

use alloy::primitives::TxHash;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use cqrs_es::{Aggregate, DomainEvent, EventEnvelope, View};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use st0x_broker::{Direction, SupportedBroker, Symbol};

use crate::lifecycle::{Lifecycle, LifecycleError};
use crate::offchain_order::{BrokerOrderId, ExecutionId, PriceCents};
use crate::shares::{ArithmeticError, FractionalShares};
use crate::threshold::ExecutionThreshold;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct Position {
    pub(crate) symbol: Symbol,
    pub(crate) net: FractionalShares,
    pub(crate) accumulated_long: FractionalShares,
    pub(crate) accumulated_short: FractionalShares,
    pub(crate) pending_execution_id: Option<ExecutionId>,
    pub(crate) threshold: ExecutionThreshold,
    pub(crate) last_updated: Option<DateTime<Utc>>,
}

impl Position {
    pub(crate) fn aggregate_id(symbol: &Symbol) -> String {
        symbol.to_string()
    }

    pub(crate) fn apply_transition(
        event: &PositionEvent,
        position: &Self,
    ) -> Result<Self, LifecycleError<ArithmeticError>> {
        match event {
            PositionEvent::OnChainOrderFilled {
                amount,
                direction,
                seen_at,
                ..
            } => match direction {
                Direction::Buy => Ok(Self {
                    net: (position.net + *amount).map_err(LifecycleError::from)?,
                    accumulated_long: (position.accumulated_long + *amount)
                        .map_err(LifecycleError::from)?,
                    last_updated: Some(*seen_at),
                    ..position.clone()
                }),
                Direction::Sell => Ok(Self {
                    net: (position.net - *amount).map_err(LifecycleError::from)?,
                    accumulated_short: (position.accumulated_short + *amount)
                        .map_err(LifecycleError::from)?,
                    last_updated: Some(*seen_at),
                    ..position.clone()
                }),
            },

            PositionEvent::OffChainOrderPlaced {
                execution_id,
                placed_at,
                ..
            } => {
                if position.pending_execution_id.is_some() {
                    return Err(LifecycleError::Mismatch {
                        state: format!("{position:?}"),
                        event: event.event_type(),
                    });
                }

                Ok(Self {
                    pending_execution_id: Some(*execution_id),
                    last_updated: Some(*placed_at),
                    ..position.clone()
                })
            }

            PositionEvent::OffChainOrderFilled {
                execution_id,
                shares_filled,
                direction,
                broker_timestamp,
                ..
            } => {
                if position.pending_execution_id != Some(*execution_id) {
                    return Err(LifecycleError::Mismatch {
                        state: format!("{position:?}"),
                        event: event.event_type(),
                    });
                }

                match direction {
                    Direction::Sell => Ok(Self {
                        net: (position.net - *shares_filled).map_err(LifecycleError::from)?,
                        pending_execution_id: None,
                        last_updated: Some(*broker_timestamp),
                        ..position.clone()
                    }),
                    Direction::Buy => Ok(Self {
                        net: (position.net + *shares_filled).map_err(LifecycleError::from)?,
                        pending_execution_id: None,
                        last_updated: Some(*broker_timestamp),
                        ..position.clone()
                    }),
                }
            }

            PositionEvent::OffChainOrderFailed {
                execution_id,
                failed_at,
                ..
            } => {
                if position.pending_execution_id != Some(*execution_id) {
                    return Err(LifecycleError::Mismatch {
                        state: format!("{position:?}"),
                        event: event.event_type(),
                    });
                }

                Ok(Self {
                    pending_execution_id: None,
                    last_updated: Some(*failed_at),
                    ..position.clone()
                })
            }

            PositionEvent::ThresholdUpdated {
                new_threshold,
                updated_at,
                ..
            } => Ok(Self {
                threshold: *new_threshold,
                last_updated: Some(*updated_at),
                ..position.clone()
            }),

            PositionEvent::Initialized { .. } | PositionEvent::Migrated { .. } => {
                Err(LifecycleError::Mismatch {
                    state: format!("{position:?}"),
                    event: event.event_type(),
                })
            }
        }
    }

    pub(crate) fn from_event(
        event: &PositionEvent,
    ) -> Result<Self, LifecycleError<ArithmeticError>> {
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
                pending_execution_id: None,
                threshold: *threshold,
                last_updated: Some(*initialized_at),
            }),

            PositionEvent::Migrated {
                symbol,
                net_position,
                accumulated_long,
                accumulated_short,
                threshold,
                migrated_at,
            } => Ok(Self {
                symbol: symbol.clone(),
                net: *net_position,
                accumulated_long: *accumulated_long,
                accumulated_short: *accumulated_short,
                pending_execution_id: None,
                threshold: *threshold,
                last_updated: Some(*migrated_at),
            }),

            _ => Err(LifecycleError::Mismatch {
                state: "Uninitialized".into(),
                event: event.event_type(),
            }),
        }
    }

    fn create_trigger_reason(&self, threshold: &ExecutionThreshold) -> Option<TriggerReason> {
        match threshold {
            ExecutionThreshold::Shares(threshold_shares) => {
                let net_abs = self.net.abs();
                (net_abs.0 >= threshold_shares.0).then_some(TriggerReason::SharesThreshold {
                    net_position_shares: net_abs.0,
                    threshold_shares: threshold_shares.0,
                })
            }
            ExecutionThreshold::DollarValue(_threshold_dollars) => None,
        }
    }

    fn validate_pending_execution(&self, execution_id: ExecutionId) -> Result<(), PositionError> {
        let Some(pending_id) = self.pending_execution_id else {
            return Err(PositionError::NoPendingExecution);
        };

        if pending_id != execution_id {
            return Err(PositionError::ExecutionIdMismatch {
                expected: pending_id,
                actual: execution_id,
            });
        }

        Ok(())
    }

    fn handle_place_offchain_order(
        &self,
        execution_id: ExecutionId,
        shares: FractionalShares,
        direction: Direction,
        broker: SupportedBroker,
    ) -> Result<Vec<PositionEvent>, PositionError> {
        if let Some(pending) = self.pending_execution_id {
            return Err(PositionError::PendingExecution {
                execution_id: pending,
            });
        }

        let trigger_reason =
            self.create_trigger_reason(&self.threshold)
                .ok_or(PositionError::ThresholdNotMet {
                    net_position: self.net,
                    threshold: self.threshold,
                })?;

        Ok(vec![PositionEvent::OffChainOrderPlaced {
            execution_id,
            shares,
            direction,
            broker,
            trigger_reason,
            placed_at: Utc::now(),
        }])
    }
}

#[async_trait]
impl Aggregate for Lifecycle<Position, ArithmeticError> {
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
                Err(LifecycleError::Uninitialized),
                PositionCommand::Migrate {
                    symbol,
                    net_position,
                    accumulated_long,
                    accumulated_short,
                    threshold,
                },
            ) => Ok(vec![PositionEvent::Migrated {
                symbol: symbol.clone(),
                net_position: *net_position,
                accumulated_long: *accumulated_long,
                accumulated_short: *accumulated_short,
                threshold: *threshold,
                migrated_at: Utc::now(),
            }]),

            (Ok(_), PositionCommand::Migrate { .. } | PositionCommand::Initialize { .. }) => {
                Err(LifecycleError::AlreadyInitialized.into())
            }

            (Err(_), PositionCommand::Initialize { symbol, threshold }) => {
                Ok(vec![PositionEvent::Initialized {
                    symbol: symbol.clone(),
                    threshold: *threshold,
                    initialized_at: Utc::now(),
                }])
            }

            (Err(e), _) => Err(e.into()),

            (
                Ok(_),
                PositionCommand::AcknowledgeOnChainFill {
                    trade_id,
                    amount,
                    direction,
                    price_usdc,
                    block_timestamp,
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
                    execution_id,
                    shares,
                    direction,
                    broker,
                },
            ) => position.handle_place_offchain_order(*execution_id, *shares, *direction, *broker),

            (
                Ok(position),
                PositionCommand::CompleteOffChainOrder {
                    execution_id,
                    shares_filled,
                    direction,
                    broker_order_id,
                    price_cents,
                    broker_timestamp,
                },
            ) => {
                position.validate_pending_execution(*execution_id)?;

                Ok(vec![PositionEvent::OffChainOrderFilled {
                    execution_id: *execution_id,
                    shares_filled: *shares_filled,
                    direction: *direction,
                    broker_order_id: broker_order_id.clone(),
                    price_cents: *price_cents,
                    broker_timestamp: *broker_timestamp,
                }])
            }

            (
                Ok(position),
                PositionCommand::FailOffChainOrder {
                    execution_id,
                    error,
                },
            ) => {
                position.validate_pending_execution(*execution_id)?;

                Ok(vec![PositionEvent::OffChainOrderFailed {
                    execution_id: *execution_id,
                    error: error.clone(),
                    failed_at: Utc::now(),
                }])
            }

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

impl View<Self> for Lifecycle<Position, ArithmeticError> {
    fn update(&mut self, event: &EventEnvelope<Self>) {
        *self = self
            .clone()
            .transition(&event.payload, Position::apply_transition)
            .or_initialize(&event.payload, Position::from_event);
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
    #[error("Cannot place offchain order: already have pending execution {execution_id:?}")]
    PendingExecution { execution_id: ExecutionId },
    #[error("Cannot complete offchain order: no pending execution")]
    NoPendingExecution,
    #[error("Execution ID mismatch: expected {expected:?}, got {actual:?}")]
    ExecutionIdMismatch {
        expected: ExecutionId,
        actual: ExecutionId,
    },
    #[error(transparent)]
    State(#[from] LifecycleError<ArithmeticError>),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum PositionCommand {
    Migrate {
        symbol: Symbol,
        net_position: FractionalShares,
        accumulated_long: FractionalShares,
        accumulated_short: FractionalShares,
        threshold: ExecutionThreshold,
    },
    Initialize {
        symbol: Symbol,
        threshold: ExecutionThreshold,
    },
    AcknowledgeOnChainFill {
        trade_id: TradeId,
        amount: FractionalShares,
        direction: Direction,
        price_usdc: Decimal,
        block_timestamp: DateTime<Utc>,
    },
    PlaceOffChainOrder {
        execution_id: ExecutionId,
        shares: FractionalShares,
        direction: Direction,
        broker: SupportedBroker,
    },
    CompleteOffChainOrder {
        execution_id: ExecutionId,
        shares_filled: FractionalShares,
        direction: Direction,
        broker_order_id: BrokerOrderId,
        price_cents: PriceCents,
        broker_timestamp: DateTime<Utc>,
    },
    FailOffChainOrder {
        execution_id: ExecutionId,
        error: String,
    },
    UpdateThreshold {
        threshold: ExecutionThreshold,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) enum PositionEvent {
    Migrated {
        symbol: Symbol,
        net_position: FractionalShares,
        accumulated_long: FractionalShares,
        accumulated_short: FractionalShares,
        threshold: ExecutionThreshold,
        migrated_at: DateTime<Utc>,
    },
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
        execution_id: ExecutionId,
        shares: FractionalShares,
        direction: Direction,
        broker: SupportedBroker,
        trigger_reason: TriggerReason,
        placed_at: DateTime<Utc>,
    },
    OffChainOrderFilled {
        execution_id: ExecutionId,
        shares_filled: FractionalShares,
        direction: Direction,
        broker_order_id: BrokerOrderId,
        price_cents: PriceCents,
        broker_timestamp: DateTime<Utc>,
    },
    OffChainOrderFailed {
        execution_id: ExecutionId,
        error: String,
        failed_at: DateTime<Utc>,
    },
    ThresholdUpdated {
        old_threshold: ExecutionThreshold,
        new_threshold: ExecutionThreshold,
        updated_at: DateTime<Utc>,
    },
}

impl DomainEvent for PositionEvent {
    fn event_type(&self) -> String {
        match self {
            Self::Migrated { .. } => "PositionEvent::Migrated".to_string(),
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
    use super::*;
    use cqrs_es::test::TestFramework;
    use rust_decimal_macros::dec;
    use std::collections::HashMap;
    use std::str::FromStr;

    use crate::threshold::Usdc;

    fn one_share_threshold() -> ExecutionThreshold {
        ExecutionThreshold::shares(FractionalShares::ONE).unwrap()
    }

    fn make_envelope(
        aggregate_id: &str,
        sequence: usize,
        event: PositionEvent,
    ) -> EventEnvelope<Lifecycle<Position, ArithmeticError>> {
        EventEnvelope {
            aggregate_id: aggregate_id.to_string(),
            sequence,
            payload: event,
            metadata: HashMap::new(),
        }
    }

    #[test]
    fn initialize_sets_threshold() {
        let threshold = one_share_threshold();

        let result = TestFramework::<Lifecycle<Position, ArithmeticError>>::with(())
            .given_no_previous_events()
            .when(PositionCommand::Initialize {
                symbol: Symbol::new("AAPL").unwrap(),
                threshold,
            })
            .inspect_result();

        assert_eq!(result.unwrap().len(), 1);
    }

    #[test]
    fn acknowledge_onchain_fill_accumulates_position() {
        let threshold = one_share_threshold();
        let trade_id = TradeId {
            tx_hash: TxHash::random(),
            log_index: 1,
        };
        let amount = FractionalShares(dec!(0.5));
        let price_usdc = dec!(150.0);
        let block_timestamp = Utc::now();

        let result = TestFramework::<Lifecycle<Position, ArithmeticError>>::with(())
            .given(vec![PositionEvent::Initialized {
                symbol: Symbol::new("AAPL").unwrap(),
                threshold,
                initialized_at: Utc::now(),
            }])
            .when(PositionCommand::AcknowledgeOnChainFill {
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

        let execution_id = ExecutionId(1);
        let shares = FractionalShares::ONE;

        let result = TestFramework::<Lifecycle<Position, ArithmeticError>>::with(())
            .given(vec![
                PositionEvent::Initialized {
                    symbol: Symbol::new("AAPL").unwrap(),
                    threshold,
                    initialized_at: Utc::now(),
                },
                PositionEvent::OnChainOrderFilled {
                    trade_id: trade_id1,
                    amount: FractionalShares(dec!(0.6)),
                    direction: Direction::Buy,
                    price_usdc: dec!(150.0),
                    block_timestamp: Utc::now(),
                    seen_at: Utc::now(),
                },
                PositionEvent::OnChainOrderFilled {
                    trade_id: trade_id2,
                    amount: FractionalShares(dec!(0.5)),
                    direction: Direction::Buy,
                    price_usdc: dec!(151.0),
                    block_timestamp: Utc::now(),
                    seen_at: Utc::now(),
                },
            ])
            .when(PositionCommand::PlaceOffChainOrder {
                execution_id,
                shares,
                direction: Direction::Sell,
                broker: SupportedBroker::Schwab,
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
        let execution_id = ExecutionId(1);

        TestFramework::<Lifecycle<Position, ArithmeticError>>::with(())
            .given(vec![
                PositionEvent::Initialized {
                    symbol: Symbol::new("AAPL").unwrap(),
                    threshold,
                    initialized_at: Utc::now(),
                },
                PositionEvent::OnChainOrderFilled {
                    trade_id,
                    amount: FractionalShares(dec!(0.5)),
                    direction: Direction::Buy,
                    price_usdc: dec!(150.0),
                    block_timestamp: Utc::now(),
                    seen_at: Utc::now(),
                },
            ])
            .when(PositionCommand::PlaceOffChainOrder {
                execution_id,
                shares: FractionalShares::ONE,
                direction: Direction::Sell,
                broker: SupportedBroker::Schwab,
            })
            .then_expect_error(PositionError::ThresholdNotMet {
                net_position: FractionalShares(dec!(0.5)),
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
        let execution_id = ExecutionId(1);

        TestFramework::<Lifecycle<Position, ArithmeticError>>::with(())
            .given(vec![
                PositionEvent::Initialized {
                    symbol: Symbol::new("AAPL").unwrap(),
                    threshold,
                    initialized_at: Utc::now(),
                },
                PositionEvent::OnChainOrderFilled {
                    trade_id,
                    amount: FractionalShares(dec!(1.5)),
                    direction: Direction::Buy,
                    price_usdc: dec!(150.0),
                    block_timestamp: Utc::now(),
                    seen_at: Utc::now(),
                },
                PositionEvent::OffChainOrderPlaced {
                    execution_id,
                    shares: FractionalShares::ONE,
                    direction: Direction::Sell,
                    broker: SupportedBroker::Schwab,
                    trigger_reason: TriggerReason::SharesThreshold {
                        net_position_shares: dec!(1.5),
                        threshold_shares: dec!(1.0),
                    },
                    placed_at: Utc::now(),
                },
            ])
            .when(PositionCommand::PlaceOffChainOrder {
                execution_id: ExecutionId(2),
                shares: FractionalShares(dec!(0.5)),
                direction: Direction::Sell,
                broker: SupportedBroker::Schwab,
            })
            .then_expect_error(PositionError::PendingExecution { execution_id });
    }

    #[test]
    fn complete_offchain_order_clears_pending() {
        let threshold = one_share_threshold();
        let trade_id = TradeId {
            tx_hash: TxHash::random(),
            log_index: 1,
        };
        let execution_id = ExecutionId(1);
        let broker_order_id = BrokerOrderId("ORDER123".to_string());
        let price_cents = PriceCents(15050);

        let result = TestFramework::<Lifecycle<Position, ArithmeticError>>::with(())
            .given(vec![
                PositionEvent::Initialized {
                    symbol: Symbol::new("AAPL").unwrap(),
                    threshold,
                    initialized_at: Utc::now(),
                },
                PositionEvent::OnChainOrderFilled {
                    trade_id,
                    amount: FractionalShares(dec!(1.5)),
                    direction: Direction::Buy,
                    price_usdc: dec!(150.0),
                    block_timestamp: Utc::now(),
                    seen_at: Utc::now(),
                },
                PositionEvent::OffChainOrderPlaced {
                    execution_id,
                    shares: FractionalShares::ONE,
                    direction: Direction::Sell,
                    broker: SupportedBroker::Schwab,
                    trigger_reason: TriggerReason::SharesThreshold {
                        net_position_shares: dec!(1.5),
                        threshold_shares: dec!(1.0),
                    },
                    placed_at: Utc::now(),
                },
            ])
            .when(PositionCommand::CompleteOffChainOrder {
                execution_id,
                shares_filled: FractionalShares::ONE,
                direction: Direction::Sell,
                broker_order_id,
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
        let execution_id = ExecutionId(1);

        let result = TestFramework::<Lifecycle<Position, ArithmeticError>>::with(())
            .given(vec![
                PositionEvent::Initialized {
                    symbol: Symbol::new("AAPL").unwrap(),
                    threshold,
                    initialized_at: Utc::now(),
                },
                PositionEvent::OnChainOrderFilled {
                    trade_id,
                    amount: FractionalShares(dec!(1.5)),
                    direction: Direction::Buy,
                    price_usdc: dec!(150.0),
                    block_timestamp: Utc::now(),
                    seen_at: Utc::now(),
                },
                PositionEvent::OffChainOrderPlaced {
                    execution_id,
                    shares: FractionalShares::ONE,
                    direction: Direction::Sell,
                    broker: SupportedBroker::Schwab,
                    trigger_reason: TriggerReason::SharesThreshold {
                        net_position_shares: dec!(1.5),
                        threshold_shares: dec!(1.0),
                    },
                    placed_at: Utc::now(),
                },
            ])
            .when(PositionCommand::FailOffChainOrder {
                execution_id,
                error: "Broker API timeout".to_string(),
            })
            .inspect_result();

        assert_eq!(result.unwrap().len(), 1);
    }

    #[test]
    fn offchain_sell_reduces_net_position() {
        let threshold = one_share_threshold();
        let execution_id = ExecutionId(1);
        let broker_order_id = BrokerOrderId("ORDER123".to_string());
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
                amount: FractionalShares(dec!(2.0)),
                direction: Direction::Buy,
                price_usdc: dec!(150.0),
                block_timestamp: Utc::now(),
                seen_at: Utc::now(),
            },
            PositionEvent::OffChainOrderPlaced {
                execution_id,
                shares: FractionalShares(dec!(1.5)),
                direction: Direction::Sell,
                broker: SupportedBroker::Schwab,
                trigger_reason: TriggerReason::SharesThreshold {
                    net_position_shares: dec!(2.0),
                    threshold_shares: dec!(1.0),
                },
                placed_at: Utc::now(),
            },
            PositionEvent::OffChainOrderFilled {
                execution_id,
                shares_filled: FractionalShares(dec!(1.5)),
                direction: Direction::Sell,
                broker_order_id,
                price_cents,
                broker_timestamp: Utc::now(),
            },
        ];

        let mut aggregate = Lifecycle::<Position, ArithmeticError>::default();
        for event in events {
            aggregate.apply(event);
        }

        let Lifecycle::Live(position) = aggregate else {
            panic!("Expected Active state");
        };

        // OnChain buy of 2.0 + OffChain sell of 1.5 = net position of 0.5
        assert_eq!(position.net, FractionalShares(dec!(0.5)));
        assert!(
            position.pending_execution_id.is_none(),
            "pending_execution_id should be cleared after OffChainOrderFilled"
        );
    }

    #[test]
    fn offchain_buy_increases_net_position() {
        let threshold = one_share_threshold();
        let execution_id = ExecutionId(1);
        let broker_order_id = BrokerOrderId("ORDER456".to_string());
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
                amount: FractionalShares(dec!(2.0)),
                direction: Direction::Sell,
                price_usdc: dec!(150.0),
                block_timestamp: Utc::now(),
                seen_at: Utc::now(),
            },
            PositionEvent::OffChainOrderPlaced {
                execution_id,
                shares: FractionalShares(dec!(1.5)),
                direction: Direction::Buy,
                broker: SupportedBroker::Schwab,
                trigger_reason: TriggerReason::SharesThreshold {
                    net_position_shares: dec!(2.0),
                    threshold_shares: dec!(1.0),
                },
                placed_at: Utc::now(),
            },
            PositionEvent::OffChainOrderFilled {
                execution_id,
                shares_filled: FractionalShares(dec!(1.5)),
                direction: Direction::Buy,
                broker_order_id,
                price_cents,
                broker_timestamp: Utc::now(),
            },
        ];

        let mut aggregate = Lifecycle::<Position, ArithmeticError>::default();
        for event in events {
            aggregate.apply(event);
        }

        let Lifecycle::Live(position) = aggregate else {
            panic!("Expected Active state");
        };

        // OnChain sell of 2.0 + OffChain buy of 1.5 = net position of -0.5
        assert_eq!(position.net, FractionalShares(dec!(-0.5)));
        assert!(
            position.pending_execution_id.is_none(),
            "pending_execution_id should be cleared after OffChainOrderFilled"
        );
    }

    #[test]
    fn update_threshold_creates_audit_trail() {
        let old_threshold = one_share_threshold();
        let new_threshold = ExecutionThreshold::shares(FractionalShares(dec!(5.0))).unwrap();

        let result = TestFramework::<Lifecycle<Position, ArithmeticError>>::with(())
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
            threshold: ExecutionThreshold::shares(FractionalShares(dec!(100))).unwrap(),
            initialized_at,
        };

        let mut view = Lifecycle::<Position, ArithmeticError>::default();
        assert!(matches!(view, Lifecycle::Uninitialized));

        view.update(&make_envelope(&symbol.to_string(), 1, event));

        let Lifecycle::Live(position) = view else {
            panic!("Expected Active state");
        };

        assert_eq!(position.symbol, symbol);
        assert_eq!(position.net, FractionalShares::ZERO);
        assert_eq!(position.accumulated_long, FractionalShares::ZERO);
        assert_eq!(position.accumulated_short, FractionalShares::ZERO);
        assert_eq!(position.pending_execution_id, None);
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
            pending_execution_id: None,
            threshold: ExecutionThreshold::whole_share(),
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
            amount: FractionalShares(dec!(10.5)),
            price_usdc: dec!(150.25),
            block_timestamp: seen_at,
            seen_at,
        };

        view.update(&make_envelope(&symbol.to_string(), 2, event));

        let Lifecycle::Live(position) = view else {
            panic!("Expected Active state");
        };

        assert_eq!(position.net, FractionalShares(dec!(10.5)));
        assert_eq!(position.accumulated_long, FractionalShares(dec!(10.5)));
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
            net: FractionalShares(dec!(20.0)),
            accumulated_long: FractionalShares(dec!(20.0)),
            accumulated_short: FractionalShares::ZERO,
            pending_execution_id: None,
            threshold: ExecutionThreshold::whole_share(),
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
            amount: FractionalShares(dec!(5.5)),
            price_usdc: dec!(149.75),
            block_timestamp: seen_at,
            seen_at,
        };

        view.update(&make_envelope(&symbol.to_string(), 3, event));

        let Lifecycle::Live(position) = view else {
            panic!("Expected Active state");
        };

        assert_eq!(position.net, FractionalShares(dec!(14.5)));
        assert_eq!(position.accumulated_long, FractionalShares(dec!(20.0)));
        assert_eq!(position.accumulated_short, FractionalShares(dec!(5.5)));
        assert_eq!(position.last_updated, Some(seen_at));
    }

    #[test]
    fn offchain_placed_sets_pending_execution() {
        let symbol = Symbol::new("AAPL").unwrap();
        let initialized_at = Utc::now();
        let placed_at = Utc::now();
        let execution_id = ExecutionId(42);

        let mut view = Lifecycle::Live(Position {
            symbol: symbol.clone(),
            net: FractionalShares(dec!(100.0)),
            accumulated_long: FractionalShares(dec!(100.0)),
            accumulated_short: FractionalShares::ZERO,
            pending_execution_id: None,
            threshold: ExecutionThreshold::whole_share(),
            last_updated: Some(initialized_at),
        });

        let event = PositionEvent::OffChainOrderPlaced {
            execution_id,
            shares: FractionalShares(dec!(100)),
            direction: Direction::Sell,
            broker: SupportedBroker::Schwab,
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

        assert_eq!(position.pending_execution_id, Some(execution_id));
        assert_eq!(position.last_updated, Some(placed_at));
    }

    #[test]
    fn offchain_filled_sell_reduces_net_and_clears_pending() {
        let symbol = Symbol::new("AAPL").unwrap();
        let initialized_at = Utc::now();
        let broker_timestamp = Utc::now();
        let execution_id = ExecutionId(42);

        let mut view = Lifecycle::Live(Position {
            symbol: symbol.clone(),
            net: FractionalShares(dec!(100.0)),
            accumulated_long: FractionalShares(dec!(100.0)),
            accumulated_short: FractionalShares::ZERO,
            pending_execution_id: Some(execution_id),
            threshold: ExecutionThreshold::whole_share(),
            last_updated: Some(initialized_at),
        });

        let event = PositionEvent::OffChainOrderFilled {
            execution_id,
            shares_filled: FractionalShares(dec!(100)),
            direction: Direction::Sell,
            broker_order_id: BrokerOrderId("ORD123".to_string()),
            price_cents: PriceCents(15025),
            broker_timestamp,
        };

        view.update(&make_envelope(&symbol.to_string(), 5, event));

        let Lifecycle::Live(position) = view else {
            panic!("Expected Active state");
        };

        assert_eq!(position.net, FractionalShares::ZERO);
        assert_eq!(position.pending_execution_id, None);
        assert_eq!(position.last_updated, Some(broker_timestamp));
    }

    #[test]
    fn offchain_filled_buy_increases_net() {
        let symbol = Symbol::new("AAPL").unwrap();
        let initialized_at = Utc::now();
        let broker_timestamp = Utc::now();
        let execution_id = ExecutionId(43);

        let mut view = Lifecycle::Live(Position {
            symbol: symbol.clone(),
            net: FractionalShares(dec!(50.0)),
            accumulated_long: FractionalShares(dec!(50.0)),
            accumulated_short: FractionalShares::ZERO,
            pending_execution_id: Some(execution_id),
            threshold: ExecutionThreshold::whole_share(),
            last_updated: Some(initialized_at),
        });

        let event = PositionEvent::OffChainOrderFilled {
            execution_id,
            shares_filled: FractionalShares(dec!(25)),
            direction: Direction::Buy,
            broker_order_id: BrokerOrderId("ORD456".to_string()),
            price_cents: PriceCents(14500),
            broker_timestamp,
        };

        view.update(&make_envelope(&symbol.to_string(), 6, event));

        let Lifecycle::Live(position) = view else {
            panic!("Expected Active state");
        };

        assert_eq!(position.net, FractionalShares(dec!(75.0)));
        assert_eq!(position.pending_execution_id, None);
        assert_eq!(position.last_updated, Some(broker_timestamp));
    }

    #[test]
    fn offchain_failed_clears_pending() {
        let symbol = Symbol::new("AAPL").unwrap();
        let initialized_at = Utc::now();
        let failed_at = Utc::now();
        let execution_id = ExecutionId(42);

        let mut view = Lifecycle::Live(Position {
            symbol: symbol.clone(),
            net: FractionalShares(dec!(100.0)),
            accumulated_long: FractionalShares(dec!(100.0)),
            accumulated_short: FractionalShares::ZERO,
            pending_execution_id: Some(execution_id),
            threshold: ExecutionThreshold::whole_share(),
            last_updated: Some(initialized_at),
        });

        let event = PositionEvent::OffChainOrderFailed {
            execution_id,
            error: "Market closed".to_string(),
            failed_at,
        };

        view.update(&make_envelope(&symbol.to_string(), 7, event));

        let Lifecycle::Live(position) = view else {
            panic!("Expected Active state");
        };

        assert_eq!(position.net, FractionalShares(dec!(100.0)));
        assert_eq!(position.pending_execution_id, None);
        assert_eq!(position.last_updated, Some(failed_at));
    }

    #[test]
    fn threshold_updated_changes_last_updated() {
        let symbol = Symbol::new("AAPL").unwrap();
        let initialized_at = Utc::now();
        let updated_at = Utc::now();

        let mut view = Lifecycle::Live(Position {
            symbol: symbol.clone(),
            net: FractionalShares(dec!(100.0)),
            accumulated_long: FractionalShares(dec!(100.0)),
            accumulated_short: FractionalShares::ZERO,
            pending_execution_id: None,
            threshold: ExecutionThreshold::shares(FractionalShares(dec!(100))).unwrap(),
            last_updated: Some(initialized_at),
        });

        let event = PositionEvent::ThresholdUpdated {
            old_threshold: ExecutionThreshold::shares(FractionalShares(dec!(100))).unwrap(),
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
    fn migrated_creates_active_state_with_values() {
        let symbol = Symbol::new("AAPL").unwrap();
        let migrated_at = Utc::now();

        let event = PositionEvent::Migrated {
            symbol: symbol.clone(),
            net_position: FractionalShares(dec!(150.5)),
            accumulated_long: FractionalShares(dec!(200.0)),
            accumulated_short: FractionalShares(dec!(49.5)),
            threshold: ExecutionThreshold::shares(FractionalShares(dec!(100))).unwrap(),
            migrated_at,
        };

        let mut view = Lifecycle::<Position, ArithmeticError>::default();
        assert!(matches!(view, Lifecycle::Uninitialized));

        view.update(&make_envelope(&symbol.to_string(), 1, event));

        let Lifecycle::Live(position) = view else {
            panic!("Expected Active state");
        };

        assert_eq!(position.symbol, symbol);
        assert_eq!(position.net, FractionalShares(dec!(150.5)));
        assert_eq!(position.accumulated_long, FractionalShares(dec!(200.0)));
        assert_eq!(position.accumulated_short, FractionalShares(dec!(49.5)));
        assert_eq!(position.pending_execution_id, None);
        assert_eq!(position.last_updated, Some(migrated_at));
    }

    #[test]
    fn transition_on_uninitialized_corrupts_state() {
        let mut view = Lifecycle::<Position, ArithmeticError>::default();

        let event = PositionEvent::OnChainOrderFilled {
            trade_id: TradeId {
                tx_hash: TxHash::from_str(
                    "0x3234567890123456789012345678901234567890123456789012345678901234",
                )
                .unwrap(),
                log_index: 0,
            },
            direction: Direction::Buy,
            amount: FractionalShares(dec!(10)),
            price_usdc: dec!(150.00),
            block_timestamp: Utc::now(),
            seen_at: Utc::now(),
        };

        view.update(&make_envelope("AAPL", 1, event));

        assert!(matches!(view, Lifecycle::Failed { .. }));
    }

    #[tokio::test]
    async fn test_migrate_command_creates_migrated_event() {
        let position = Lifecycle::<Position, ArithmeticError>::default();
        let symbol = Symbol::new("AAPL").unwrap();
        let net_position = FractionalShares(dec!(5.5));
        let accumulated_long = FractionalShares(dec!(10.0));
        let accumulated_short = FractionalShares(dec!(4.5));
        let threshold = ExecutionThreshold::whole_share();

        let command = PositionCommand::Migrate {
            symbol: symbol.clone(),
            net_position,
            accumulated_long,
            accumulated_short,
            threshold,
        };

        let events = position.handle(command, &()).await.unwrap();

        assert_eq!(events.len(), 1);
        match &events[0] {
            PositionEvent::Migrated {
                symbol: event_symbol,
                net_position: event_net,
                accumulated_long: event_long,
                accumulated_short: event_short,
                threshold: event_threshold,
                ..
            } => {
                assert_eq!(event_symbol, &symbol);
                assert_eq!(event_net, &net_position);
                assert_eq!(event_long, &accumulated_long);
                assert_eq!(event_short, &accumulated_short);
                assert_eq!(event_threshold, &threshold);
            }
            _ => panic!("Expected Migrated event"),
        }
    }

    #[tokio::test]
    async fn test_migrate_with_zero_position() {
        let position = Lifecycle::<Position, ArithmeticError>::default();
        let symbol = Symbol::new("MSFT").unwrap();

        let command = PositionCommand::Migrate {
            symbol,
            net_position: FractionalShares::ZERO,
            accumulated_long: FractionalShares::ZERO,
            accumulated_short: FractionalShares::ZERO,
            threshold: ExecutionThreshold::whole_share(),
        };

        let events = position.handle(command, &()).await.unwrap();

        assert_eq!(events.len(), 1);
        match &events[0] {
            PositionEvent::Migrated {
                net_position,
                accumulated_long,
                accumulated_short,
                ..
            } => {
                assert_eq!(net_position, &FractionalShares::ZERO);
                assert_eq!(accumulated_long, &FractionalShares::ZERO);
                assert_eq!(accumulated_short, &FractionalShares::ZERO);
            }
            _ => panic!("Expected Migrated event"),
        }
    }

    #[tokio::test]
    async fn test_migrate_preserves_negative_position() {
        let position = Lifecycle::<Position, ArithmeticError>::default();
        let symbol = Symbol::new("GOOGL").unwrap();
        let net_position = FractionalShares(dec!(-10.5));

        let command = PositionCommand::Migrate {
            symbol,
            net_position,
            accumulated_long: FractionalShares(dec!(5.0)),
            accumulated_short: FractionalShares(dec!(15.5)),
            threshold: ExecutionThreshold::whole_share(),
        };

        let events = position.handle(command, &()).await.unwrap();

        assert_eq!(events.len(), 1);
        match &events[0] {
            PositionEvent::Migrated {
                net_position: event_net,
                ..
            } => {
                assert_eq!(event_net.0, dec!(-10.5));
                assert!(event_net.0 < Decimal::ZERO);
            }
            _ => panic!("Expected Migrated event"),
        }
    }

    #[tokio::test]
    async fn test_cannot_migrate_when_already_initialized() {
        let mut position = Lifecycle::<Position, ArithmeticError>::default();
        let symbol = Symbol::new("NVDA").unwrap();

        let initialized_event = PositionEvent::Initialized {
            symbol: symbol.clone(),
            threshold: ExecutionThreshold::whole_share(),
            initialized_at: Utc::now(),
        };
        position.apply(initialized_event);

        let command = PositionCommand::Migrate {
            symbol,
            net_position: FractionalShares(dec!(1.5)),
            accumulated_long: FractionalShares(dec!(1.5)),
            accumulated_short: FractionalShares::ZERO,
            threshold: ExecutionThreshold::whole_share(),
        };

        let result = position.handle(command, &()).await;

        assert!(matches!(
            result,
            Err(PositionError::State(LifecycleError::AlreadyInitialized))
        ));
    }
}

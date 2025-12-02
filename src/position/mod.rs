use async_trait::async_trait;
use chrono::{DateTime, Utc};
use cqrs_es::Aggregate;
use serde::{Deserialize, Serialize};
use st0x_broker::Direction;

use crate::state::{State, StateError};

mod cmd;
mod event;
pub(crate) mod view;

pub(crate) use cmd::PositionCommand;
pub(crate) use event::{
    ArithmeticError, ExecutionId, ExecutionThreshold, FractionalShares, PositionEvent,
    TriggerReason,
};

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
}

impl Default for State<Position, ArithmeticError> {
    fn default() -> Self {
        Self::Active(Position::default())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct Position {
    pub(crate) net: FractionalShares,
    pub(crate) accumulated_long: FractionalShares,
    pub(crate) accumulated_short: FractionalShares,
    pub(crate) pending_execution_id: Option<ExecutionId>,
    pub(crate) threshold: ExecutionThreshold,
    pub(crate) last_updated: Option<DateTime<Utc>>,
}

impl Default for Position {
    fn default() -> Self {
        Self {
            net: FractionalShares::ZERO,
            accumulated_long: FractionalShares::ZERO,
            accumulated_short: FractionalShares::ZERO,
            pending_execution_id: None,
            threshold: ExecutionThreshold::whole_share(),
            last_updated: None,
        }
    }
}

#[async_trait]
impl Aggregate for State<Position, ArithmeticError> {
    type Command = PositionCommand;
    type Event = PositionEvent;
    type Error = StateError<PositionError>;
    type Services = ();

    fn aggregate_type() -> String {
        "Position".to_string()
    }

    async fn handle(
        &self,
        command: Self::Command,
        _services: &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        let position = self.active()?;

        match command {
            PositionCommand::Initialize { threshold } => Ok(vec![PositionEvent::Initialized {
                threshold,
                initialized_at: Utc::now(),
            }]),
            PositionCommand::AcknowledgeOnChainFill {
                trade_id,
                amount,
                direction,
                price_usdc,
                block_timestamp,
            } => Ok(vec![PositionEvent::OnChainOrderFilled {
                trade_id,
                amount,
                direction,
                price_usdc,
                block_timestamp,
                seen_at: Utc::now(),
            }]),
            PositionCommand::PlaceOffChainOrder {
                execution_id,
                shares,
                direction,
                broker,
            } => {
                if let Some(pending) = position.pending_execution_id {
                    return Err(PositionError::PendingExecution {
                        execution_id: pending,
                    }
                    .into());
                }

                let trigger_reason = position.create_trigger_reason(&position.threshold).ok_or(
                    PositionError::ThresholdNotMet {
                        net_position: position.net,
                        threshold: position.threshold,
                    },
                )?;

                Ok(vec![PositionEvent::OffChainOrderPlaced {
                    execution_id,
                    shares,
                    direction,
                    broker,
                    trigger_reason,
                    placed_at: Utc::now(),
                }])
            }
            PositionCommand::CompleteOffChainOrder {
                execution_id,
                shares_filled,
                direction,
                broker_order_id,
                price_cents,
                broker_timestamp,
            } => {
                position.validate_pending_execution(execution_id)?;

                Ok(vec![PositionEvent::OffChainOrderFilled {
                    execution_id,
                    shares_filled,
                    direction,
                    broker_order_id,
                    price_cents,
                    broker_timestamp,
                }])
            }
            PositionCommand::FailOffChainOrder {
                execution_id,
                error,
            } => {
                position.validate_pending_execution(execution_id)?;

                Ok(vec![PositionEvent::OffChainOrderFailed {
                    execution_id,
                    error,
                    failed_at: Utc::now(),
                }])
            }
            PositionCommand::UpdateThreshold { threshold } => {
                Ok(vec![PositionEvent::ThresholdUpdated {
                    old_threshold: position.threshold,
                    new_threshold: threshold,
                    updated_at: Utc::now(),
                }])
            }
        }
    }

    fn apply(&mut self, event: Self::Event) {
        self.transition(event, |event, pos| match &event {
            PositionEvent::Migrated {
                net_position,
                accumulated_long,
                accumulated_short,
                threshold,
                migrated_at,
                ..
            } => Ok(Position {
                net: *net_position,
                accumulated_long: *accumulated_long,
                accumulated_short: *accumulated_short,
                threshold: *threshold,
                last_updated: Some(*migrated_at),
                ..pos.clone()
            }),

            PositionEvent::Initialized {
                threshold,
                initialized_at,
            } => Ok(Position {
                threshold: *threshold,
                last_updated: Some(*initialized_at),
                ..pos.clone()
            }),

            PositionEvent::OnChainOrderFilled {
                amount,
                direction,
                seen_at,
                ..
            } => {
                let (net, accumulated_long, accumulated_short) = match direction {
                    Direction::Buy => (
                        (pos.net + *amount).map_err(StateError::from)?,
                        (pos.accumulated_long + *amount).map_err(StateError::from)?,
                        pos.accumulated_short,
                    ),
                    Direction::Sell => (
                        (pos.net - *amount).map_err(StateError::from)?,
                        pos.accumulated_long,
                        (pos.accumulated_short + *amount).map_err(StateError::from)?,
                    ),
                };

                Ok(Position {
                    net,
                    accumulated_long,
                    accumulated_short,
                    last_updated: Some(*seen_at),
                    ..pos.clone()
                })
            }

            PositionEvent::OffChainOrderPlaced {
                execution_id,
                placed_at,
                ..
            } => Ok(Position {
                pending_execution_id: Some(*execution_id),
                last_updated: Some(*placed_at),
                ..pos.clone()
            }),

            PositionEvent::OffChainOrderFilled {
                shares_filled,
                direction,
                broker_timestamp,
                ..
            } => {
                let net = match direction {
                    Direction::Sell => (pos.net - *shares_filled).map_err(StateError::from)?,
                    Direction::Buy => (pos.net + *shares_filled).map_err(StateError::from)?,
                };

                Ok(Position {
                    net,
                    pending_execution_id: None,
                    last_updated: Some(*broker_timestamp),
                    ..pos.clone()
                })
            }

            PositionEvent::OffChainOrderFailed { failed_at, .. } => Ok(Position {
                pending_execution_id: None,
                last_updated: Some(*failed_at),
                ..pos.clone()
            }),

            PositionEvent::ThresholdUpdated {
                new_threshold,
                updated_at,
                ..
            } => Ok(Position {
                threshold: *new_threshold,
                last_updated: Some(*updated_at),
                ..pos.clone()
            }),
        });
    }
}

impl Position {
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

    fn validate_pending_execution(
        &self,
        execution_id: ExecutionId,
    ) -> Result<(), StateError<PositionError>> {
        let Some(pending_id) = self.pending_execution_id else {
            return Err(PositionError::NoPendingExecution.into());
        };

        if pending_id != execution_id {
            return Err(PositionError::ExecutionIdMismatch {
                expected: pending_id,
                actual: execution_id,
            }
            .into());
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::TxHash;
    use cqrs_es::Aggregate;
    use cqrs_es::test::TestFramework;
    use rust_decimal_macros::dec;
    use st0x_broker::{Direction, SupportedBroker};

    use super::*;
    use crate::position::event::{BrokerOrderId, PriceCents, TradeId};

    fn one_share_threshold() -> ExecutionThreshold {
        ExecutionThreshold::shares(FractionalShares::ONE).unwrap()
    }

    #[test]
    fn initialize_sets_threshold() {
        let threshold = one_share_threshold();

        let result = TestFramework::<State<Position, ArithmeticError>>::with(())
            .given_no_previous_events()
            .when(PositionCommand::Initialize { threshold })
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

        let result = TestFramework::<State<Position, ArithmeticError>>::with(())
            .given(vec![PositionEvent::Initialized {
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

        let result = TestFramework::<State<Position, ArithmeticError>>::with(())
            .given(vec![
                PositionEvent::Initialized {
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

        TestFramework::<State<Position, ArithmeticError>>::with(())
            .given(vec![
                PositionEvent::Initialized {
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
            .then_expect_error(StateError::Custom(PositionError::ThresholdNotMet {
                net_position: FractionalShares(dec!(0.5)),
                threshold,
            }));
    }

    #[test]
    fn pending_execution_prevents_new_execution() {
        let threshold = one_share_threshold();
        let trade_id = TradeId {
            tx_hash: TxHash::random(),
            log_index: 1,
        };
        let execution_id = ExecutionId(1);

        TestFramework::<State<Position, ArithmeticError>>::with(())
            .given(vec![
                PositionEvent::Initialized {
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
            .then_expect_error(StateError::Custom(PositionError::PendingExecution {
                execution_id,
            }));
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

        let result = TestFramework::<State<Position, ArithmeticError>>::with(())
            .given(vec![
                PositionEvent::Initialized {
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

        let result = TestFramework::<State<Position, ArithmeticError>>::with(())
            .given(vec![
                PositionEvent::Initialized {
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

        let mut aggregate = State::<Position, ArithmeticError>::default();
        for event in events {
            aggregate.apply(event);
        }

        let State::Active(position) = aggregate else {
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

        let mut aggregate = State::<Position, ArithmeticError>::default();
        for event in events {
            aggregate.apply(event);
        }

        let State::Active(position) = aggregate else {
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

        let result = TestFramework::<State<Position, ArithmeticError>>::with(())
            .given(vec![PositionEvent::Initialized {
                threshold: old_threshold,
                initialized_at: Utc::now(),
            }])
            .when(PositionCommand::UpdateThreshold {
                threshold: new_threshold,
            })
            .inspect_result();

        assert_eq!(result.unwrap().len(), 1);
    }
}

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use cqrs_es::{Aggregate, DomainEvent};
use serde::{Deserialize, Serialize};
use st0x_broker::{Direction, Symbol};

use crate::lifecycle::{Lifecycle, LifecycleError};

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
    #[error(transparent)]
    State(#[from] LifecycleError<ArithmeticError>),
}

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

#[async_trait]
impl Aggregate for Lifecycle<Position, ArithmeticError> {
    type Command = PositionCommand;
    type Event = PositionEvent;
    type Error = PositionError;
    type Services = ();

    fn aggregate_type() -> String {
        "Position".to_string()
    }

    async fn handle(
        &self,
        command: Self::Command,
        _services: &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        match (&command, self.live()) {
            (PositionCommand::Initialize { symbol, threshold }, _) => {
                Ok(vec![PositionEvent::Initialized {
                    symbol: symbol.clone(),
                    threshold: *threshold,
                    initialized_at: Utc::now(),
                }])
            }

            (_, Err(e)) => Err(e.into()),

            (
                PositionCommand::AcknowledgeOnChainFill {
                    trade_id,
                    amount,
                    direction,
                    price_usdc,
                    block_timestamp,
                },
                Ok(_),
            ) => Ok(vec![PositionEvent::OnChainOrderFilled {
                trade_id: trade_id.clone(),
                amount: *amount,
                direction: *direction,
                price_usdc: *price_usdc,
                block_timestamp: *block_timestamp,
                seen_at: Utc::now(),
            }]),

            (
                PositionCommand::PlaceOffChainOrder {
                    execution_id,
                    shares,
                    direction,
                    broker,
                },
                Ok(position),
            ) => {
                if let Some(pending) = position.pending_execution_id {
                    return Err(PositionError::PendingExecution {
                        execution_id: pending,
                    });
                }

                let trigger_reason = position.create_trigger_reason(&position.threshold).ok_or(
                    PositionError::ThresholdNotMet {
                        net_position: position.net,
                        threshold: position.threshold,
                    },
                )?;

                Ok(vec![PositionEvent::OffChainOrderPlaced {
                    execution_id: *execution_id,
                    shares: *shares,
                    direction: *direction,
                    broker: *broker,
                    trigger_reason,
                    placed_at: Utc::now(),
                }])
            }

            (
                PositionCommand::CompleteOffChainOrder {
                    execution_id,
                    shares_filled,
                    direction,
                    broker_order_id,
                    price_cents,
                    broker_timestamp,
                },
                Ok(position),
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
                PositionCommand::FailOffChainOrder {
                    execution_id,
                    error,
                },
                Ok(position),
            ) => {
                position.validate_pending_execution(*execution_id)?;

                Ok(vec![PositionEvent::OffChainOrderFailed {
                    execution_id: *execution_id,
                    error: error.clone(),
                    failed_at: Utc::now(),
                }])
            }

            (PositionCommand::UpdateThreshold { threshold }, Ok(position)) => {
                Ok(vec![PositionEvent::ThresholdUpdated {
                    old_threshold: position.threshold,
                    new_threshold: *threshold,
                    updated_at: Utc::now(),
                }])
            }
        }
    }

    fn apply(&mut self, event: Self::Event) {
        *self = self
            .clone()
            .transition(&event, Position::apply_transition)
            .or_initialize(&event, Position::from_event);
    }
}

impl Position {
    fn apply_transition(
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
            } => Ok(Self {
                pending_execution_id: Some(*execution_id),
                last_updated: Some(*placed_at),
                ..position.clone()
            }),

            PositionEvent::OffChainOrderFilled {
                shares_filled,
                direction,
                broker_timestamp,
                ..
            } => match direction {
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
            },

            PositionEvent::OffChainOrderFailed { failed_at, .. } => Ok(Self {
                pending_execution_id: None,
                last_updated: Some(*failed_at),
                ..position.clone()
            }),

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

    fn from_event(event: &PositionEvent) -> Result<Self, LifecycleError<ArithmeticError>> {
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
}

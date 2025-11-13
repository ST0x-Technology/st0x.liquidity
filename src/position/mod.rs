use async_trait::async_trait;
use chrono::{DateTime, Utc};
use cqrs_es::Aggregate;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use st0x_broker::{Direction, SupportedBroker, Symbol};

use crate::offchain_order::ExecutionId;

mod cmd;
mod event;
mod view;

pub(crate) use cmd::PositionCommand;
pub(crate) use event::{ExecutionThreshold, PositionEvent, TriggerReason};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct FractionalShares(pub(crate) Decimal);

impl FractionalShares {
    pub(crate) const ZERO: Self = Self(Decimal::ZERO);
    pub(crate) const ONE: Self = Self(Decimal::ONE);

    pub(crate) fn abs(self) -> Self {
        Self(self.0.abs())
    }
}

impl std::ops::Add for FractionalShares {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        Self(self.0 + rhs.0)
    }
}

impl std::ops::Sub for FractionalShares {
    type Output = Self;

    fn sub(self, rhs: Self) -> Self::Output {
        Self(self.0 - rhs.0)
    }
}

impl std::ops::AddAssign for FractionalShares {
    fn add_assign(&mut self, rhs: Self) {
        self.0 += rhs.0;
    }
}

impl std::ops::SubAssign for FractionalShares {
    fn sub_assign(&mut self, rhs: Self) {
        self.0 -= rhs.0;
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
impl Aggregate for Position {
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
        match command {
            PositionCommand::Migrate {
                symbol,
                net_position,
                accumulated_long,
                accumulated_short,
                threshold,
            } => Ok(vec![PositionEvent::Migrated {
                symbol,
                net_position,
                accumulated_long,
                accumulated_short,
                threshold,
                migrated_at: Utc::now(),
            }]),
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
            } => self.handle_place_offchain_order(execution_id, shares, direction, broker),
            PositionCommand::CompleteOffChainOrder {
                execution_id,
                shares_filled,
                direction,
                broker_order_id,
                price_cents,
                broker_timestamp,
            } => self.handle_complete_offchain_order(
                execution_id,
                shares_filled,
                direction,
                broker_order_id,
                price_cents,
                broker_timestamp,
            ),
            PositionCommand::FailOffChainOrder {
                execution_id,
                error,
            } => self.handle_fail_offchain_order(execution_id, error),
            PositionCommand::UpdateThreshold { threshold } => {
                Ok(vec![PositionEvent::ThresholdUpdated {
                    old_threshold: self.threshold,
                    new_threshold: threshold,
                    updated_at: Utc::now(),
                }])
            }
        }
    }

    fn apply(&mut self, event: Self::Event) {
        match event {
            PositionEvent::Migrated {
                symbol: _,
                net_position,
                accumulated_long,
                accumulated_short,
                threshold,
                migrated_at,
            } => {
                self.net = net_position;
                self.accumulated_long = accumulated_long;
                self.accumulated_short = accumulated_short;
                self.threshold = threshold;
                self.last_updated = Some(migrated_at);
            }
            PositionEvent::Initialized {
                threshold,
                initialized_at,
            } => {
                self.threshold = threshold;
                self.last_updated = Some(initialized_at);
            }
            PositionEvent::OnChainOrderFilled {
                amount,
                direction,
                seen_at,
                ..
            } => {
                self.net = match direction {
                    Direction::Buy => self.net + amount,
                    Direction::Sell => self.net - amount,
                };

                match direction {
                    Direction::Buy => self.accumulated_long += amount,
                    Direction::Sell => self.accumulated_short += amount,
                }

                self.last_updated = Some(seen_at);
            }
            PositionEvent::OffChainOrderPlaced {
                execution_id,
                placed_at,
                ..
            } => {
                self.pending_execution_id = Some(execution_id);
                self.last_updated = Some(placed_at);
            }
            PositionEvent::OffChainOrderFilled {
                shares_filled,
                direction,
                broker_timestamp,
                ..
            } => {
                self.net = match direction {
                    Direction::Sell => self.net - shares_filled,
                    Direction::Buy => self.net + shares_filled,
                };
                self.pending_execution_id = None;
                self.last_updated = Some(broker_timestamp);
            }
            PositionEvent::OffChainOrderFailed { failed_at, .. } => {
                self.pending_execution_id = None;
                self.last_updated = Some(failed_at);
            }
            PositionEvent::ThresholdUpdated {
                new_threshold,
                updated_at,
                ..
            } => {
                self.threshold = new_threshold;
                self.last_updated = Some(updated_at);
            }
        }
    }
}

impl Position {
    pub(crate) fn aggregate_id(symbol: &Symbol) -> String {
        symbol.to_string()
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

        let trigger_reason = self.create_trigger_reason(&self.threshold).ok_or_else(|| {
            PositionError::ThresholdNotMet {
                net_position: self.net,
                threshold: self.threshold.clone(),
            }
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

    fn handle_complete_offchain_order(
        &self,
        execution_id: ExecutionId,
        shares_filled: FractionalShares,
        direction: Direction,
        broker_order_id: BrokerOrderId,
        price_cents: PriceCents,
        broker_timestamp: DateTime<Utc>,
    ) -> Result<Vec<PositionEvent>, PositionError> {
        let Some(pending_id) = self.pending_execution_id else {
            return Err(PositionError::NoPendingExecution);
        };

        if pending_id != execution_id {
            return Err(PositionError::ExecutionIdMismatch {
                expected: pending_id,
                actual: execution_id,
            });
        }

        Ok(vec![PositionEvent::OffChainOrderFilled {
            execution_id,
            shares_filled,
            direction,
            broker_order_id,
            price_cents,
            broker_timestamp,
        }])
    }

    fn handle_fail_offchain_order(
        &self,
        execution_id: ExecutionId,
        error: String,
    ) -> Result<Vec<PositionEvent>, PositionError> {
        let Some(pending_id) = self.pending_execution_id else {
            return Err(PositionError::NoPendingExecution);
        };

        if pending_id != execution_id {
            return Err(PositionError::ExecutionIdMismatch {
                expected: pending_id,
                actual: execution_id,
            });
        }

        Ok(vec![PositionEvent::OffChainOrderFailed {
            execution_id,
            error,
            failed_at: Utc::now(),
        }])
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::TxHash;
    use cqrs_es::test::TestFramework;
    use rust_decimal_macros::dec;
    use st0x_broker::{Direction, SupportedBroker};

    use super::event::TradeId;
    use super::*;
    use crate::offchain_order::{BrokerOrderId, PriceCents};

    #[test]
    fn test_initialize_sets_threshold() {
        let threshold = ExecutionThreshold::shares(dec!(1.0)).unwrap();

        let result = TestFramework::<Position>::with(())
            .given_no_previous_events()
            .when(PositionCommand::Initialize { threshold })
            .inspect_result();

        assert_eq!(result.unwrap().len(), 1);
    }

    #[test]
    fn test_acknowledge_onchain_fill_accumulates_position() {
        let threshold = ExecutionThreshold::shares(dec!(1.0)).unwrap();
        let trade_id = TradeId {
            tx_hash: TxHash::random(),
            log_index: 1,
        };
        let amount = FractionalShares(dec!(0.5));
        let price_usdc = dec!(150.0);
        let block_timestamp = Utc::now();

        let result = TestFramework::<Position>::with(())
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
    fn test_shares_threshold_triggers_execution() {
        let threshold = ExecutionThreshold::shares(dec!(1.0)).unwrap();
        let trade_id1 = TradeId {
            tx_hash: TxHash::random(),
            log_index: 1,
        };
        let trade_id2 = TradeId {
            tx_hash: TxHash::random(),
            log_index: 2,
        };

        let execution_id = ExecutionId(1);
        let shares = FractionalShares(dec!(1.0));

        let result = TestFramework::<Position>::with(())
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
    fn test_place_offchain_order_below_threshold_fails() {
        let threshold = ExecutionThreshold::shares(dec!(1.0)).unwrap();
        let trade_id = TradeId {
            tx_hash: TxHash::random(),
            log_index: 1,
        };
        let execution_id = ExecutionId(1);

        TestFramework::<Position>::with(())
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
                shares: FractionalShares(dec!(1.0)),
                direction: Direction::Sell,
                broker: SupportedBroker::Schwab,
            })
            .then_expect_error(PositionError::ThresholdNotMet {
                net_position: FractionalShares(dec!(0.5)),
                threshold,
            });
    }

    #[test]
    fn test_pending_execution_prevents_new_execution() {
        let threshold = ExecutionThreshold::shares(dec!(1.0)).unwrap();
        let trade_id = TradeId {
            tx_hash: TxHash::random(),
            log_index: 1,
        };
        let execution_id = ExecutionId(1);

        TestFramework::<Position>::with(())
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
                    shares: FractionalShares(dec!(1.0)),
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
    fn test_complete_offchain_order_clears_pending() {
        let threshold = ExecutionThreshold::shares(dec!(1.0)).unwrap();
        let trade_id = TradeId {
            tx_hash: TxHash::random(),
            log_index: 1,
        };
        let execution_id = ExecutionId(1);
        let broker_order_id = BrokerOrderId("ORDER123".to_string());
        let price_cents = PriceCents(15050);

        let result = TestFramework::<Position>::with(())
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
                    shares: FractionalShares(dec!(1.0)),
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
                shares_filled: FractionalShares(dec!(1.0)),
                direction: Direction::Sell,
                broker_order_id,
                price_cents,
                broker_timestamp: Utc::now(),
            })
            .inspect_result();

        assert_eq!(result.unwrap().len(), 1);
    }

    #[test]
    fn test_fail_offchain_order_clears_pending() {
        let threshold = ExecutionThreshold::shares(dec!(1.0)).unwrap();
        let trade_id = TradeId {
            tx_hash: TxHash::random(),
            log_index: 1,
        };
        let execution_id = ExecutionId(1);

        let result = TestFramework::<Position>::with(())
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
                    shares: FractionalShares(dec!(1.0)),
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
    fn test_offchain_sell_reduces_net_position() {
        let threshold = ExecutionThreshold::shares(dec!(1.0)).unwrap();
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

        let mut position = Position::default();
        for event in events {
            position.apply(event);
        }

        // OnChain buy of 2.0 + OffChain sell of 1.5 = net position of 0.5
        assert_eq!(position.net, FractionalShares(dec!(0.5)));
        assert!(
            position.pending_execution_id.is_none(),
            "pending_execution_id should be cleared after OffChainOrderFilled"
        );
    }

    #[test]
    fn test_offchain_buy_increases_net_position() {
        let threshold = ExecutionThreshold::shares(dec!(1.0)).unwrap();
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

        let mut position = Position::default();
        for event in events {
            position.apply(event);
        }

        // OnChain sell of 2.0 + OffChain buy of 1.5 = net position of -0.5
        assert_eq!(position.net, FractionalShares(dec!(-0.5)));
        assert!(
            position.pending_execution_id.is_none(),
            "pending_execution_id should be cleared after OffChainOrderFilled"
        );
    }

    #[test]
    fn test_update_threshold_creates_audit_trail() {
        let old_threshold = ExecutionThreshold::shares(dec!(1.0)).unwrap();
        let new_threshold = ExecutionThreshold::shares(dec!(5.0)).unwrap();

        let result = TestFramework::<Position>::with(())
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

    #[test]
    fn test_aggregate_id_format() {
        let symbol = Symbol::new("AAPL").unwrap();
        let aggregate_id = Position::aggregate_id(&symbol);

        assert_eq!(aggregate_id, "AAPL");
    }

    #[test]
    fn test_migrate_command_creates_migrated_event() {
        let symbol = Symbol::new("AAPL").unwrap();
        let net_position = FractionalShares(dec!(5.5));
        let accumulated_long = FractionalShares(dec!(10.0));
        let accumulated_short = FractionalShares(dec!(4.5));
        let threshold = ExecutionThreshold::Shares(dec!(1.0));

        let result = TestFramework::<Position>::with(())
            .given_no_previous_events()
            .when(PositionCommand::Migrate {
                symbol: symbol.clone(),
                net_position,
                accumulated_long,
                accumulated_short,
                threshold: threshold.clone(),
            })
            .inspect_result();

        let events = result.unwrap();
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

    #[test]
    fn test_migrated_event_sets_position_state() {
        let symbol = Symbol::new("TSLA").unwrap();
        let net_position = FractionalShares(dec!(-2.0));
        let accumulated_long = FractionalShares(dec!(3.0));
        let accumulated_short = FractionalShares(dec!(5.0));
        let threshold = ExecutionThreshold::Shares(dec!(2.0));

        let result = TestFramework::<Position>::with(())
            .given_no_previous_events()
            .when(PositionCommand::Migrate {
                symbol: symbol.clone(),
                net_position,
                accumulated_long,
                accumulated_short,
                threshold: threshold.clone(),
            })
            .inspect_result();

        assert!(result.is_ok());
        let events = result.unwrap();
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

    #[test]
    fn test_migrate_with_zero_position() {
        let symbol = Symbol::new("MSFT").unwrap();
        let net_position = FractionalShares::ZERO;
        let accumulated_long = FractionalShares::ZERO;
        let accumulated_short = FractionalShares::ZERO;
        let threshold = ExecutionThreshold::Shares(dec!(1.0));

        let result = TestFramework::<Position>::with(())
            .given_no_previous_events()
            .when(PositionCommand::Migrate {
                symbol,
                net_position,
                accumulated_long,
                accumulated_short,
                threshold,
            })
            .inspect_result();

        assert!(result.is_ok());
        let events = result.unwrap();
        assert_eq!(events.len(), 1);

        match &events[0] {
            PositionEvent::Migrated {
                net_position: event_net,
                accumulated_long: event_long,
                accumulated_short: event_short,
                ..
            } => {
                assert_eq!(event_net, &FractionalShares::ZERO);
                assert_eq!(event_long, &FractionalShares::ZERO);
                assert_eq!(event_short, &FractionalShares::ZERO);
            }
            _ => panic!("Expected Migrated event"),
        }
    }

    #[test]
    fn test_migrate_preserves_negative_position() {
        let symbol = Symbol::new("GOOGL").unwrap();
        let net_position = FractionalShares(dec!(-10.5));
        let accumulated_long = FractionalShares(dec!(5.0));
        let accumulated_short = FractionalShares(dec!(15.5));
        let threshold = ExecutionThreshold::Shares(dec!(1.0));

        let result = TestFramework::<Position>::with(())
            .given_no_previous_events()
            .when(PositionCommand::Migrate {
                symbol,
                net_position,
                accumulated_long,
                accumulated_short,
                threshold,
            })
            .inspect_result();

        assert!(result.is_ok());
        let events = result.unwrap();
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

    #[test]
    fn test_operations_after_migrate() {
        let symbol = Symbol::new("NVDA").unwrap();
        let net_position = FractionalShares(dec!(1.5));
        let accumulated_long = FractionalShares(dec!(1.5));
        let accumulated_short = FractionalShares::ZERO;
        let threshold = ExecutionThreshold::Shares(dec!(1.0));

        let trade_id = TradeId {
            tx_hash: TxHash::random(),
            log_index: 1,
        };

        let result = TestFramework::<Position>::with(())
            .given(vec![PositionEvent::Migrated {
                symbol,
                net_position,
                accumulated_long,
                accumulated_short,
                threshold,
                migrated_at: Utc::now(),
            }])
            .when(PositionCommand::AcknowledgeOnChainFill {
                trade_id,
                amount: FractionalShares(dec!(0.5)),
                direction: Direction::Buy,
                price_usdc: dec!(500.0),
                block_timestamp: Utc::now(),
            })
            .inspect_result();

        assert!(result.is_ok());
        let events = result.unwrap();
        assert_eq!(events.len(), 1);
        assert!(matches!(
            events[0],
            PositionEvent::OnChainOrderFilled { .. }
        ));
    }
}

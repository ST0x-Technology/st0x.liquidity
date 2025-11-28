use async_trait::async_trait;
use chrono::{DateTime, Utc};
use cqrs_es::Aggregate;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

mod cmd;
mod event;

pub(crate) use cmd::UsdcRebalanceCommand;
pub(crate) use event::UsdcRebalanceEvent;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct UsdcRebalanceId(pub(crate) String);

impl UsdcRebalanceId {
    pub(crate) fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) enum RebalanceDirection {
    AlpacaToRaindex,
    RaindexToAlpaca,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) enum UsdcRebalance {
    NotStarted,

    WithdrawalInitiated {
        direction: RebalanceDirection,
        amount: Decimal,
        initiated_at: DateTime<Utc>,
    },

    Completed {
        direction: RebalanceDirection,
        amount: Decimal,
        completed_at: DateTime<Utc>,
    },

    Failed {
        direction: RebalanceDirection,
        amount: Decimal,
        reason: String,
        failed_at: DateTime<Utc>,
    },
}

impl Default for UsdcRebalance {
    fn default() -> Self {
        Self::NotStarted
    }
}

#[derive(Debug, thiserror::Error, PartialEq)]
pub(crate) enum UsdcRebalanceError {
    #[error("Withdrawal not initiated")]
    WithdrawalNotInitiated,

    #[error("Already completed")]
    AlreadyCompleted,

    #[error("Already failed")]
    AlreadyFailed,
}

#[async_trait]
impl Aggregate for UsdcRebalance {
    type Command = UsdcRebalanceCommand;
    type Event = UsdcRebalanceEvent;
    type Error = UsdcRebalanceError;
    type Services = ();

    fn aggregate_type() -> String {
        "UsdcRebalance".to_string()
    }

    async fn handle(
        &self,
        command: Self::Command,
        _services: &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        match (self, command) {
            (Self::NotStarted, UsdcRebalanceCommand::InitiateWithdrawal { direction, amount }) => {
                Ok(vec![UsdcRebalanceEvent::WithdrawalInitiated {
                    direction,
                    amount,
                    initiated_at: Utc::now(),
                }])
            }

            (Self::WithdrawalInitiated { .. }, UsdcRebalanceCommand::InitiateWithdrawal { .. }) => {
                Err(UsdcRebalanceError::WithdrawalNotInitiated)
            }

            (
                Self::NotStarted | Self::WithdrawalInitiated { .. },
                UsdcRebalanceCommand::Fail { reason },
            ) => Ok(vec![UsdcRebalanceEvent::Failed {
                reason,
                failed_at: Utc::now(),
            }]),

            (Self::Completed { .. }, UsdcRebalanceCommand::Fail { .. }) => {
                Err(UsdcRebalanceError::AlreadyCompleted)
            }

            (Self::Failed { .. }, UsdcRebalanceCommand::Fail { .. }) => {
                Err(UsdcRebalanceError::AlreadyFailed)
            }

            (Self::Completed { .. }, _) => Err(UsdcRebalanceError::AlreadyCompleted),

            (Self::Failed { .. }, _) => Err(UsdcRebalanceError::AlreadyFailed),
        }
    }

    fn apply(&mut self, event: Self::Event) {
        match event {
            UsdcRebalanceEvent::WithdrawalInitiated {
                direction,
                amount,
                initiated_at,
            } => {
                self.apply_withdrawal_initiated(direction, amount, initiated_at);
            }
            UsdcRebalanceEvent::Failed { reason, failed_at } => {
                self.apply_failed(reason, failed_at);
            }
        }
    }
}

impl UsdcRebalance {
    fn apply_withdrawal_initiated(
        &mut self,
        direction: RebalanceDirection,
        amount: Decimal,
        initiated_at: DateTime<Utc>,
    ) {
        *self = Self::WithdrawalInitiated {
            direction,
            amount,
            initiated_at,
        };
    }

    fn apply_failed(&mut self, reason: String, failed_at: DateTime<Utc>) {
        let (direction, amount) = match self {
            Self::WithdrawalInitiated {
                direction, amount, ..
            } => (direction.clone(), *amount),
            Self::NotStarted | Self::Completed { .. } | Self::Failed { .. } => {
                return;
            }
        };

        *self = Self::Failed {
            direction,
            amount,
            reason,
            failed_at,
        };
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[tokio::test]
    async fn test_initiate_withdrawal_alpaca_to_raindex() {
        let aggregate = UsdcRebalance::default();

        let events = aggregate
            .handle(
                UsdcRebalanceCommand::InitiateWithdrawal {
                    direction: RebalanceDirection::AlpacaToRaindex,
                    amount: dec!(1000.00),
                },
                &(),
            )
            .await
            .unwrap();

        assert_eq!(events.len(), 1);
        assert!(matches!(
            events[0],
            UsdcRebalanceEvent::WithdrawalInitiated { .. }
        ));
    }

    #[tokio::test]
    async fn test_initiate_withdrawal_raindex_to_alpaca() {
        let aggregate = UsdcRebalance::default();

        let events = aggregate
            .handle(
                UsdcRebalanceCommand::InitiateWithdrawal {
                    direction: RebalanceDirection::RaindexToAlpaca,
                    amount: dec!(500.50),
                },
                &(),
            )
            .await
            .unwrap();

        assert_eq!(events.len(), 1);
        assert!(matches!(
            events[0],
            UsdcRebalanceEvent::WithdrawalInitiated { .. }
        ));
    }

    #[tokio::test]
    async fn test_fail_from_withdrawal_initiated() {
        let mut aggregate = UsdcRebalance::default();

        let withdrawal_event = UsdcRebalanceEvent::WithdrawalInitiated {
            direction: RebalanceDirection::AlpacaToRaindex,
            amount: dec!(1000.00),
            initiated_at: Utc::now(),
        };
        aggregate.apply(withdrawal_event);

        let events = aggregate
            .handle(
                UsdcRebalanceCommand::Fail {
                    reason: "Withdrawal failed".to_string(),
                },
                &(),
            )
            .await
            .unwrap();

        assert_eq!(events.len(), 1);
        assert!(matches!(events[0], UsdcRebalanceEvent::Failed { .. }));
    }

    #[tokio::test]
    async fn test_cannot_fail_when_completed() {
        let aggregate = UsdcRebalance::Completed {
            direction: RebalanceDirection::AlpacaToRaindex,
            amount: dec!(1000.00),
            completed_at: Utc::now(),
        };

        let result = aggregate
            .handle(
                UsdcRebalanceCommand::Fail {
                    reason: "Cannot fail".to_string(),
                },
                &(),
            )
            .await;

        assert!(matches!(result, Err(UsdcRebalanceError::AlreadyCompleted)));
    }

    #[tokio::test]
    async fn test_cannot_fail_when_already_failed() {
        let mut aggregate = UsdcRebalance::default();

        aggregate.apply(UsdcRebalanceEvent::WithdrawalInitiated {
            direction: RebalanceDirection::AlpacaToRaindex,
            amount: dec!(1000.00),
            initiated_at: Utc::now(),
        });

        aggregate.apply(UsdcRebalanceEvent::Failed {
            reason: "First failure".to_string(),
            failed_at: Utc::now(),
        });

        let result = aggregate
            .handle(
                UsdcRebalanceCommand::Fail {
                    reason: "Second failure".to_string(),
                },
                &(),
            )
            .await;

        assert!(matches!(result, Err(UsdcRebalanceError::AlreadyFailed)));
    }
}

use alloy::primitives::TxHash;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use cqrs_es::Aggregate;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::alpaca_wallet::AlpacaTransferId;

mod cmd;
mod event;

pub(crate) use cmd::UsdcRebalanceCommand;
pub(crate) use event::UsdcRebalanceEvent;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) enum UsdcRebalance {
    NotStarted,

    WithdrawalInitiated {
        direction: RebalanceDirection,
        amount: Decimal,
        initiated_at: DateTime<Utc>,
    },

    AlpacaWithdrawalCompleted {
        amount: Decimal,
        reference: AlpacaTransferId,
        initiated_at: DateTime<Utc>,
        completed_at: DateTime<Utc>,
    },

    AlpacaWithdrawalFailed {
        amount: Decimal,
        reference: Option<AlpacaTransferId>,
        initiated_at: DateTime<Utc>,
        reason: String,
        failed_at: DateTime<Utc>,
    },

    RaindexWithdrawalCompleted {
        amount: Decimal,
        reference: TxHash,
        initiated_at: DateTime<Utc>,
        completed_at: DateTime<Utc>,
    },

    RaindexWithdrawalFailed {
        amount: Decimal,
        reference: Option<TxHash>,
        initiated_at: DateTime<Utc>,
        reason: String,
        failed_at: DateTime<Utc>,
    },

    Completed {
        direction: RebalanceDirection,
        amount: Decimal,
        completed_at: DateTime<Utc>,
    },
}

impl Default for UsdcRebalance {
    fn default() -> Self {
        Self::NotStarted
    }
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
            (Self::NotStarted, UsdcRebalanceCommand::Initiate { direction, amount }) => {
                Ok(vec![UsdcRebalanceEvent::WithdrawalInitiated {
                    direction,
                    amount,
                    initiated_at: Utc::now(),
                }])
            }

            (
                Self::WithdrawalInitiated {
                    direction: RebalanceDirection::AlpacaToRaindex,
                    ..
                },
                UsdcRebalanceCommand::CompleteAlpaca { transfer_id },
            ) => Ok(vec![UsdcRebalanceEvent::AlpacaWithdrawalCompleted {
                transfer_id,
                completed_at: Utc::now(),
            }]),

            (
                Self::WithdrawalInitiated {
                    direction: RebalanceDirection::RaindexToAlpaca,
                    ..
                },
                UsdcRebalanceCommand::CompleteRaindex { tx_hash },
            ) => Ok(vec![UsdcRebalanceEvent::RaindexWithdrawalCompleted {
                withdrawal_tx_hash: tx_hash,
                completed_at: Utc::now(),
            }]),

            (
                Self::WithdrawalInitiated {
                    direction: RebalanceDirection::AlpacaToRaindex,
                    ..
                },
                UsdcRebalanceCommand::CompleteRaindex { .. },
            )
            | (
                Self::WithdrawalInitiated {
                    direction: RebalanceDirection::RaindexToAlpaca,
                    ..
                },
                UsdcRebalanceCommand::CompleteAlpaca { .. },
            ) => Err(UsdcRebalanceError::TransferRefMismatch),

            (
                Self::NotStarted,
                UsdcRebalanceCommand::CompleteAlpaca { .. }
                | UsdcRebalanceCommand::CompleteRaindex { .. },
            )
            | (Self::WithdrawalInitiated { .. }, UsdcRebalanceCommand::Initiate { .. }) => {
                Err(UsdcRebalanceError::WithdrawalNotInitiated)
            }

            (Self::Completed { .. }, _) => Err(UsdcRebalanceError::AlreadyCompleted),

            (Self::AlpacaWithdrawalFailed { .. } | Self::RaindexWithdrawalFailed { .. }, _) => {
                Err(UsdcRebalanceError::AlreadyFailed)
            }

            (_, UsdcRebalanceCommand::FailAlpaca { reference, reason }) => {
                Ok(vec![UsdcRebalanceEvent::AlpacaWithdrawalFailed {
                    reference,
                    reason,
                    failed_at: Utc::now(),
                }])
            }

            (_, UsdcRebalanceCommand::FailRaindex { reference, reason }) => {
                Ok(vec![UsdcRebalanceEvent::RaindexWithdrawalFailed {
                    reference,
                    reason,
                    failed_at: Utc::now(),
                }])
            }

            (
                Self::AlpacaWithdrawalCompleted { .. } | Self::RaindexWithdrawalCompleted { .. },
                _,
            ) => Err(UsdcRebalanceError::WithdrawalNotInitiated),
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
            UsdcRebalanceEvent::AlpacaWithdrawalCompleted {
                transfer_id,
                completed_at,
            } => {
                self.apply_alpaca_withdrawal_completed(transfer_id, completed_at);
            }
            UsdcRebalanceEvent::RaindexWithdrawalCompleted {
                withdrawal_tx_hash,
                completed_at,
            } => {
                self.apply_raindex_withdrawal_completed(withdrawal_tx_hash, completed_at);
            }
            UsdcRebalanceEvent::AlpacaWithdrawalFailed {
                reference,
                reason,
                failed_at,
            } => {
                self.apply_alpaca_withdrawal_failed(reference, reason, failed_at);
            }
            UsdcRebalanceEvent::RaindexWithdrawalFailed {
                reference,
                reason,
                failed_at,
            } => {
                self.apply_raindex_withdrawal_failed(reference, reason, failed_at);
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

    fn apply_alpaca_withdrawal_completed(
        &mut self,
        transfer_id: AlpacaTransferId,
        completed_at: DateTime<Utc>,
    ) {
        let (amount, initiated_at) = match self {
            Self::WithdrawalInitiated {
                amount,
                initiated_at,
                ..
            } => (*amount, *initiated_at),
            _ => return,
        };

        *self = Self::AlpacaWithdrawalCompleted {
            amount,
            reference: transfer_id,
            initiated_at,
            completed_at,
        };
    }

    fn apply_raindex_withdrawal_completed(
        &mut self,
        withdrawal_tx_hash: TxHash,
        completed_at: DateTime<Utc>,
    ) {
        let (amount, initiated_at) = match self {
            Self::WithdrawalInitiated {
                amount,
                initiated_at,
                ..
            } => (*amount, *initiated_at),
            _ => return,
        };

        *self = Self::RaindexWithdrawalCompleted {
            amount,
            reference: withdrawal_tx_hash,
            initiated_at,
            completed_at,
        };
    }

    fn apply_alpaca_withdrawal_failed(
        &mut self,
        reference: Option<AlpacaTransferId>,
        reason: String,
        failed_at: DateTime<Utc>,
    ) {
        let (amount, initiated_at) = match self {
            Self::WithdrawalInitiated {
                amount,
                initiated_at,
                ..
            }
            | Self::AlpacaWithdrawalCompleted {
                amount,
                initiated_at,
                ..
            } => (*amount, *initiated_at),
            _ => return,
        };

        *self = Self::AlpacaWithdrawalFailed {
            amount,
            reference,
            initiated_at,
            reason,
            failed_at,
        };
    }

    fn apply_raindex_withdrawal_failed(
        &mut self,
        reference: Option<TxHash>,
        reason: String,
        failed_at: DateTime<Utc>,
    ) {
        let (amount, initiated_at) = match self {
            Self::WithdrawalInitiated {
                amount,
                initiated_at,
                ..
            }
            | Self::RaindexWithdrawalCompleted {
                amount,
                initiated_at,
                ..
            } => (*amount, *initiated_at),
            _ => return,
        };

        *self = Self::RaindexWithdrawalFailed {
            amount,
            reference,
            initiated_at,
            reason,
            failed_at,
        };
    }
}

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

#[derive(Debug, thiserror::Error, PartialEq)]
pub(crate) enum UsdcRebalanceError {
    #[error("Withdrawal not initiated")]
    WithdrawalNotInitiated,

    #[error("Transfer reference type does not match rebalance direction")]
    TransferRefMismatch,

    #[error("Already completed")]
    AlreadyCompleted,

    #[error("Already failed")]
    AlreadyFailed,
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::primitives::Address;
    use rust_decimal_macros::dec;
    use uuid::Uuid;

    #[tokio::test]
    async fn test_initiate_withdrawal_alpaca_to_raindex() {
        let aggregate = UsdcRebalance::default();

        let events = aggregate
            .handle(
                UsdcRebalanceCommand::Initiate {
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
                UsdcRebalanceCommand::Initiate {
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

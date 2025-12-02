use alloy::primitives::TxHash;
use chrono::{DateTime, Utc};
use cqrs_es::DomainEvent;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use st0x_broker::{Direction, SupportedBroker, Symbol};

pub(crate) use crate::shares::{ArithmeticError, FractionalShares};

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

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ExecutionId(pub(crate) i64);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct BrokerOrderId(pub(crate) String);

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct PriceCents(pub(crate) u64);

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct Usdc(pub(crate) Decimal);

impl Usdc {
    pub(crate) fn is_negative(self) -> bool {
        self.0.is_sign_negative()
    }

    pub(crate) fn is_zero(self) -> bool {
        self.0.is_zero()
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) enum ExecutionThreshold {
    Shares(FractionalShares),
    DollarValue(Usdc),
}

impl ExecutionThreshold {
    pub(crate) fn shares(value: FractionalShares) -> Result<Self, InvalidThresholdError> {
        if value.is_negative() {
            return Err(InvalidThresholdError::NegativeShares(value));
        }

        if value.is_zero() {
            return Err(InvalidThresholdError::ZeroShares);
        }

        Ok(Self::Shares(value))
    }

    pub(crate) fn dollar_value(value: Usdc) -> Result<Self, InvalidThresholdError> {
        if value.is_negative() {
            return Err(InvalidThresholdError::NegativeDollarValue(value));
        }

        if value.is_zero() {
            return Err(InvalidThresholdError::ZeroDollarValue);
        }

        Ok(Self::DollarValue(value))
    }

    pub(crate) fn whole_share() -> Self {
        Self::Shares(FractionalShares::ONE)
    }
}

#[derive(Debug, thiserror::Error, PartialEq)]
pub(crate) enum InvalidThresholdError {
    #[error("Shares threshold cannot be negative: {0:?}")]
    NegativeShares(FractionalShares),
    #[error("Dollar threshold cannot be negative: {0:?}")]
    NegativeDollarValue(Usdc),
    #[error("Shares threshold cannot be zero")]
    ZeroShares,
    #[error("Dollar threshold cannot be zero")]
    ZeroDollarValue,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn whole_share_matches_smart_constructor() {
        let from_whole_share = ExecutionThreshold::whole_share();
        let from_constructor = ExecutionThreshold::Shares(FractionalShares::ONE);
        assert_eq!(from_whole_share, from_constructor);
    }

    #[test]
    fn shares_threshold_rejects_zero() {
        let result = ExecutionThreshold::shares(FractionalShares::ZERO);
        assert_eq!(result.unwrap_err(), InvalidThresholdError::ZeroShares);
    }

    #[test]
    fn shares_threshold_rejects_negative() {
        let negative = FractionalShares(Decimal::NEGATIVE_ONE);
        let result = ExecutionThreshold::shares(negative);
        assert_eq!(
            result.unwrap_err(),
            InvalidThresholdError::NegativeShares(negative)
        );
    }

    #[test]
    fn shares_threshold_accepts_positive() {
        let result = ExecutionThreshold::shares(FractionalShares::ONE);
        assert!(result.is_ok());
    }

    #[test]
    fn dollar_threshold_rejects_zero() {
        let result = ExecutionThreshold::dollar_value(Usdc(Decimal::ZERO));
        assert_eq!(result.unwrap_err(), InvalidThresholdError::ZeroDollarValue);
    }

    #[test]
    fn dollar_threshold_rejects_negative() {
        let negative = Usdc(Decimal::NEGATIVE_ONE);
        let result = ExecutionThreshold::dollar_value(negative);
        assert_eq!(
            result.unwrap_err(),
            InvalidThresholdError::NegativeDollarValue(negative)
        );
    }

    #[test]
    fn dollar_threshold_accepts_positive() {
        let result = ExecutionThreshold::dollar_value(Usdc(Decimal::ONE));
        assert!(result.is_ok());
    }
}

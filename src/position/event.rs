use alloy::primitives::TxHash;
use chrono::{DateTime, Utc};
use cqrs_es::DomainEvent;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use st0x_broker::{Direction, SupportedBroker, Symbol};

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
pub(crate) struct FractionalShares(pub(crate) Decimal);

impl FractionalShares {
    pub(crate) const ZERO: Self = Self(Decimal::ZERO);

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

impl FractionalShares {
    pub(crate) fn new(value: Decimal) -> Result<Self, InvalidThresholdError> {
        if value.is_sign_negative() {
            return Err(InvalidThresholdError::Negative(value));
        }

        if value.is_zero() {
            return Err(InvalidThresholdError::Zero);
        }

        Ok(Self(value))
    }

    pub(crate) fn one() -> Self {
        Self(Decimal::ONE)
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct Usdc(Decimal);

impl Usdc {
    pub(crate) fn new(value: Decimal) -> Result<Self, InvalidThresholdError> {
        if value.is_sign_negative() {
            return Err(InvalidThresholdError::Negative(value));
        }

        if value.is_zero() {
            return Err(InvalidThresholdError::Zero);
        }

        Ok(Self(value))
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) enum ExecutionThreshold {
    Shares(FractionalShares),
    DollarValue(Usdc),
}

impl ExecutionThreshold {
    pub(crate) fn whole_share() -> Self {
        Self::Shares(FractionalShares::one())
    }
}

#[derive(Debug, thiserror::Error, PartialEq)]
pub(crate) enum InvalidThresholdError {
    #[error("Threshold value cannot be negative: {0}")]
    Negative(Decimal),
    #[error("Threshold value cannot be zero")]
    Zero,
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
    fn test_whole_share_matches_smart_constructor() {
        let from_whole_share = ExecutionThreshold::whole_share();
        let from_constructor = ExecutionThreshold::Shares(FractionalShares::one());
        assert_eq!(from_whole_share, from_constructor);
    }

    #[test]
    fn test_fractional_shares_validation_rejects_zero() {
        let result = FractionalShares::new(Decimal::ZERO);
        assert_eq!(result.unwrap_err(), InvalidThresholdError::Zero);
    }

    #[test]
    fn test_fractional_shares_validation_rejects_negative() {
        let result = FractionalShares::new(Decimal::NEGATIVE_ONE);
        assert_eq!(
            result.unwrap_err(),
            InvalidThresholdError::Negative(Decimal::NEGATIVE_ONE)
        );
    }

    #[test]
    fn test_fractional_shares_validation_accepts_positive() {
        let result = FractionalShares::new(Decimal::ONE);
        assert!(result.is_ok());
    }

    #[test]
    fn test_usdc_validation_rejects_zero() {
        let result = Usdc::new(Decimal::ZERO);
        assert_eq!(result.unwrap_err(), InvalidThresholdError::Zero);
    }

    #[test]
    fn test_usdc_validation_rejects_negative() {
        let result = Usdc::new(Decimal::NEGATIVE_ONE);
        assert_eq!(
            result.unwrap_err(),
            InvalidThresholdError::Negative(Decimal::NEGATIVE_ONE)
        );
    }

    #[test]
    fn test_usdc_validation_accepts_positive() {
        let result = Usdc::new(Decimal::ONE);
        assert!(result.is_ok());
    }
}

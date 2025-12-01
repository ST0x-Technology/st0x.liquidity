use alloy::primitives::TxHash;
use chrono::{DateTime, Utc};
use cqrs_es::DomainEvent;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use st0x_broker::{Direction, SupportedBroker, Symbol};

use super::FractionalShares;
use crate::offchain_order::{BrokerOrderId, ExecutionId, InvalidThresholdError, PriceCents, Usdc};

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
pub(crate) enum ExecutionThreshold {
    Shares(FractionalShares),
    DollarValue(Usdc),
}

impl ExecutionThreshold {
    pub(crate) fn shares(value: Decimal) -> Result<Self, InvalidThresholdError> {
        if value.is_sign_negative() {
            return Err(InvalidThresholdError::Negative(value));
        }

        if value.is_zero() {
            return Err(InvalidThresholdError::Zero);
        }

        Ok(Self::Shares(FractionalShares(value)))
    }

    pub(crate) fn whole_share() -> Self {
        Self::Shares(FractionalShares::ONE)
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
    use rust_decimal_macros::dec;

    use super::*;

    #[test]
    fn test_whole_share_matches_smart_constructor() {
        let from_whole_share = ExecutionThreshold::whole_share();
        let from_shares = ExecutionThreshold::shares(Decimal::ONE).unwrap();
        assert_eq!(from_whole_share, from_shares);
    }

    #[test]
    fn test_shares_positive_value_succeeds() {
        let value = dec!(1.5);
        let threshold = ExecutionThreshold::shares(value).unwrap();
        assert_eq!(
            threshold,
            ExecutionThreshold::Shares(FractionalShares(value))
        );
    }

    #[test]
    fn test_shares_zero_fails() {
        let result = ExecutionThreshold::shares(Decimal::ZERO);
        assert_eq!(result.unwrap_err(), InvalidThresholdError::Zero);
    }

    #[test]
    fn test_shares_negative_fails() {
        let negative = dec!(-1.0);
        let result = ExecutionThreshold::shares(negative);
        assert_eq!(
            result.unwrap_err(),
            InvalidThresholdError::Negative(negative)
        );
    }
}

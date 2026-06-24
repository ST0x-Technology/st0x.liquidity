use chrono::{DateTime, Utc};

use crate::{ExecutorOrderId, FractionalShares, Usd};

use super::OrderStatus;

/// Runtime representation of an offchain order's lifecycle state.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OrderState {
    Pending,
    Submitted {
        order_id: ExecutorOrderId,
    },
    /// Broker reports a partial fill. Carries the executed quantity so far
    /// and the average fill price so the local aggregate can record the
    /// fill via `UpdatePartialFill` before the order is either filled,
    /// cancelled, or fails.
    PartiallyFilled {
        order_id: ExecutorOrderId,
        shares_filled: FractionalShares,
        avg_price: Option<Usd>,
        partially_filled_at: DateTime<Utc>,
    },
    Filled {
        executed_at: DateTime<Utc>,
        order_id: ExecutorOrderId,
        price: Usd,
    },
    Cancelled {
        cancelled_at: DateTime<Utc>,
        order_id: ExecutorOrderId,
        shares_filled: FractionalShares,
        avg_price: Option<Usd>,
    },
    Failed {
        failed_at: DateTime<Utc>,
        error_reason: Option<String>,
        shares_filled: Option<FractionalShares>,
        avg_price: Option<Usd>,
    },
}

impl OrderState {
    pub const fn status(&self) -> OrderStatus {
        match self {
            Self::Pending => OrderStatus::Pending,
            Self::Submitted { .. } => OrderStatus::Submitted,
            Self::PartiallyFilled { .. } => OrderStatus::PartiallyFilled,
            Self::Filled { .. } => OrderStatus::Filled,
            Self::Cancelled { .. } => OrderStatus::Cancelled,
            Self::Failed { .. } => OrderStatus::Failed,
        }
    }
}

#[cfg(test)]
mod tests {
    use rain_math_float::Float;

    use super::*;

    #[test]
    fn filled_debug_formats_price_as_decimal() {
        let state = OrderState::Filled {
            executed_at: Utc::now(),
            order_id: ExecutorOrderId::new("ORDER123"),
            price: Usd::new(Float::parse("150.00".to_string()).unwrap()),
        };

        let debug_output = format!("{state:?}");
        assert!(
            debug_output.contains("150"),
            "Debug output should contain decimal price, got: {debug_output}"
        );
        assert!(
            !debug_output.contains("0x"),
            "Debug output should not contain hex representation, got: {debug_output}"
        );
    }

    #[test]
    fn test_status_extraction() {
        assert_eq!(OrderState::Pending.status(), OrderStatus::Pending);
        assert_eq!(
            OrderState::Submitted {
                order_id: ExecutorOrderId::new("ORDER123")
            }
            .status(),
            OrderStatus::Submitted
        );
        assert_eq!(
            OrderState::Filled {
                executed_at: Utc::now(),
                order_id: ExecutorOrderId::new("ORDER123"),
                price: Usd::new(Float::parse("150.00".to_string()).unwrap()),
            }
            .status(),
            OrderStatus::Filled
        );
        assert_eq!(
            OrderState::Cancelled {
                cancelled_at: Utc::now(),
                order_id: ExecutorOrderId::new("ORDER123"),
                shares_filled: FractionalShares::ZERO,
                avg_price: None,
            }
            .status(),
            OrderStatus::Cancelled
        );
        assert_eq!(
            OrderState::Failed {
                failed_at: Utc::now(),
                error_reason: None,
                shares_filled: None,
                avg_price: None,
            }
            .status(),
            OrderStatus::Failed
        );
    }
}

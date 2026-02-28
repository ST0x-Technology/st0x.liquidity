use chrono::{DateTime, Utc};
use st0x_exact_decimal::ExactDecimal;

use super::OrderStatus;

/// Runtime representation of an offchain order's lifecycle state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OrderState {
    Pending,
    Submitted {
        order_id: String,
    },
    Filled {
        executed_at: DateTime<Utc>,
        order_id: String,
        price: ExactDecimal,
    },
    Failed {
        failed_at: DateTime<Utc>,
        error_reason: Option<String>,
    },
}

impl OrderState {
    pub const fn status(&self) -> OrderStatus {
        match self {
            Self::Pending => OrderStatus::Pending,
            Self::Submitted { .. } => OrderStatus::Submitted,
            Self::Filled { .. } => OrderStatus::Filled,
            Self::Failed { .. } => OrderStatus::Failed,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_status_extraction() {
        assert_eq!(OrderState::Pending.status(), OrderStatus::Pending);
        assert_eq!(
            OrderState::Submitted {
                order_id: "ORDER123".to_string()
            }
            .status(),
            OrderStatus::Submitted
        );
        assert_eq!(
            OrderState::Filled {
                executed_at: Utc::now(),
                order_id: "ORDER123".to_string(),
                price: ExactDecimal::parse("150.00").unwrap(),
            }
            .status(),
            OrderStatus::Filled
        );
        assert_eq!(
            OrderState::Failed {
                failed_at: Utc::now(),
                error_reason: None
            }
            .status(),
            OrderStatus::Failed
        );
    }
}

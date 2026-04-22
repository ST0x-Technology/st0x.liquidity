use chrono::{DateTime, Utc};

use rain_math_float::Float;

use super::OrderStatus;

/// Runtime representation of an offchain order's lifecycle state.
#[derive(Clone)]
pub enum OrderState {
    Pending,
    Submitted {
        order_id: String,
    },
    Filled {
        executed_at: DateTime<Utc>,
        order_id: String,
        price: Float,
    },
    Failed {
        failed_at: DateTime<Utc>,
        error_reason: Option<String>,
    },
}

impl std::fmt::Debug for OrderState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Pending => write!(f, "Pending"),
            Self::Submitted { order_id } => f
                .debug_struct("Submitted")
                .field("order_id", order_id)
                .finish(),
            Self::Filled {
                executed_at,
                order_id,
                price,
            } => f
                .debug_struct("Filled")
                .field("executed_at", executed_at)
                .field("order_id", order_id)
                .field("price", &st0x_float_serde::DebugFloat(price))
                .finish(),
            Self::Failed {
                failed_at,
                error_reason,
            } => f
                .debug_struct("Failed")
                .field("failed_at", failed_at)
                .field("error_reason", error_reason)
                .finish(),
        }
    }
}

impl PartialEq for OrderState {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Pending, Self::Pending) => true,
            (Self::Submitted { order_id: lhs }, Self::Submitted { order_id: rhs }) => lhs == rhs,
            (
                Self::Filled {
                    executed_at: lhs_executed_at,
                    order_id: lhs_order_id,
                    price: lhs_price,
                },
                Self::Filled {
                    executed_at: rhs_executed_at,
                    order_id: rhs_order_id,
                    price: rhs_price,
                },
            ) => {
                lhs_executed_at == rhs_executed_at
                    && lhs_order_id == rhs_order_id
                    && lhs_price.eq(*rhs_price).unwrap_or(false)
            }
            (
                Self::Failed {
                    failed_at: lhs_failed_at,
                    error_reason: lhs_error_reason,
                },
                Self::Failed {
                    failed_at: rhs_failed_at,
                    error_reason: rhs_error_reason,
                },
            ) => lhs_failed_at == rhs_failed_at && lhs_error_reason == rhs_error_reason,
            _ => false,
        }
    }
}

impl Eq for OrderState {}

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
    fn filled_debug_formats_price_as_decimal() {
        let state = OrderState::Filled {
            executed_at: Utc::now(),
            order_id: "ORDER123".to_string(),
            price: Float::parse("150.00".to_string()).unwrap(),
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
                order_id: "ORDER123".to_string()
            }
            .status(),
            OrderStatus::Submitted
        );
        assert_eq!(
            OrderState::Filled {
                executed_at: Utc::now(),
                order_id: "ORDER123".to_string(),
                price: Float::parse("150.00".to_string()).unwrap(),
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

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
    /// Broker reports a partial fill. Carries the executed quantity so far
    /// and the average fill price so the local aggregate can record the
    /// fill via `UpdatePartialFill` before the order is either filled,
    /// cancelled, or fails.
    PartiallyFilled {
        order_id: String,
        shares_filled: Float,
        avg_price: Option<Float>,
        partially_filled_at: DateTime<Utc>,
    },
    Filled {
        executed_at: DateTime<Utc>,
        order_id: String,
        price: Float,
    },
    Cancelled {
        cancelled_at: DateTime<Utc>,
        order_id: String,
        shares_filled: Option<Float>,
        avg_price: Option<Float>,
    },
    Failed {
        failed_at: DateTime<Utc>,
        error_reason: Option<String>,
        shares_filled: Option<Float>,
        avg_price: Option<Float>,
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
            Self::PartiallyFilled {
                order_id,
                shares_filled,
                avg_price,
                partially_filled_at,
            } => f
                .debug_struct("PartiallyFilled")
                .field("order_id", order_id)
                .field(
                    "shares_filled",
                    &st0x_float_serde::DebugFloat(shares_filled),
                )
                .field("avg_price", &st0x_float_serde::DebugOptionFloat(avg_price))
                .field("partially_filled_at", partially_filled_at)
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
            Self::Cancelled {
                cancelled_at,
                order_id,
                shares_filled,
                avg_price,
            } => f
                .debug_struct("Cancelled")
                .field("cancelled_at", cancelled_at)
                .field("order_id", order_id)
                .field(
                    "shares_filled",
                    &st0x_float_serde::DebugOptionFloat(shares_filled),
                )
                .field("avg_price", &st0x_float_serde::DebugOptionFloat(avg_price))
                .finish(),
            Self::Failed {
                failed_at,
                error_reason,
                shares_filled,
                avg_price,
            } => f
                .debug_struct("Failed")
                .field("failed_at", failed_at)
                .field("error_reason", error_reason)
                .field(
                    "shares_filled",
                    &st0x_float_serde::DebugOptionFloat(shares_filled),
                )
                .field("avg_price", &st0x_float_serde::DebugOptionFloat(avg_price))
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
                Self::PartiallyFilled {
                    order_id: lhs_id,
                    shares_filled: lhs_filled,
                    avg_price: lhs_price,
                    partially_filled_at: lhs_at,
                },
                Self::PartiallyFilled {
                    order_id: rhs_id,
                    shares_filled: rhs_filled,
                    avg_price: rhs_price,
                    partially_filled_at: rhs_at,
                },
            ) => {
                lhs_id == rhs_id
                    && lhs_filled.eq(*rhs_filled).unwrap_or(false)
                    && lhs_at == rhs_at
                    && match (lhs_price, rhs_price) {
                        (Some(l), Some(r)) => l.eq(*r).unwrap_or(false),
                        (None, None) => true,
                        _ => false,
                    }
            }
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
                Self::Cancelled {
                    cancelled_at: lhs_cancelled_at,
                    order_id: lhs_order_id,
                    shares_filled: lhs_shares_filled,
                    avg_price: lhs_avg_price,
                },
                Self::Cancelled {
                    cancelled_at: rhs_cancelled_at,
                    order_id: rhs_order_id,
                    shares_filled: rhs_shares_filled,
                    avg_price: rhs_avg_price,
                },
            ) => {
                lhs_cancelled_at == rhs_cancelled_at
                    && lhs_order_id == rhs_order_id
                    && match (lhs_shares_filled, rhs_shares_filled) {
                        (Some(l), Some(r)) => l.eq(*r).unwrap_or(false),
                        (None, None) => true,
                        _ => false,
                    }
                    && match (lhs_avg_price, rhs_avg_price) {
                        (Some(l), Some(r)) => l.eq(*r).unwrap_or(false),
                        (None, None) => true,
                        _ => false,
                    }
            }
            (
                Self::Failed {
                    failed_at: lhs_failed_at,
                    error_reason: lhs_error_reason,
                    shares_filled: lhs_shares_filled,
                    avg_price: lhs_avg_price,
                },
                Self::Failed {
                    failed_at: rhs_failed_at,
                    error_reason: rhs_error_reason,
                    shares_filled: rhs_shares_filled,
                    avg_price: rhs_avg_price,
                },
            ) => {
                lhs_failed_at == rhs_failed_at
                    && lhs_error_reason == rhs_error_reason
                    && match (lhs_shares_filled, rhs_shares_filled) {
                        (Some(l), Some(r)) => l.eq(*r).unwrap_or(false),
                        (None, None) => true,
                        _ => false,
                    }
                    && match (lhs_avg_price, rhs_avg_price) {
                        (Some(l), Some(r)) => l.eq(*r).unwrap_or(false),
                        (None, None) => true,
                        _ => false,
                    }
            }
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
            Self::PartiallyFilled { .. } => OrderStatus::PartiallyFilled,
            Self::Filled { .. } => OrderStatus::Filled,
            Self::Cancelled { .. } => OrderStatus::Cancelled,
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
            OrderState::Cancelled {
                cancelled_at: Utc::now(),
                order_id: "ORDER123".to_string(),
                shares_filled: None,
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

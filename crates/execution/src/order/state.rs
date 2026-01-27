use chrono::{DateTime, TimeZone, Utc};
use num_traits::ToPrimitive;

use super::OrderStatus;
use crate::{Direction, ExecutionError, FractionalShares, Positive, SupportedExecutor, Symbol};

/// Database fields extracted from OrderState for storage
#[derive(Debug)]
pub(crate) struct OrderStateDbFields {
    pub(crate) order_id: Option<String>,
    pub(crate) price_cents: Option<i64>,
    pub(crate) executed_at: Option<chrono::NaiveDateTime>,
}

// Stateful enum with associated data for runtime use
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OrderState {
    Pending,
    Submitted {
        order_id: String,
    },
    Filled {
        executed_at: DateTime<Utc>,
        order_id: String,
        price_cents: u64,
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

    /// Converts database row data to an OrderState instance with proper validation.
    /// This centralizes the conversion logic and ensures database consistency.
    pub fn from_db_row(
        status: OrderStatus,
        order_id: Option<String>,
        price_cents: Option<i64>,
        executed_at: Option<chrono::NaiveDateTime>,
    ) -> Result<Self, ExecutionError> {
        match status {
            OrderStatus::Pending => Ok(Self::Pending),
            OrderStatus::Submitted => {
                let order_id = order_id.ok_or(ExecutionError::MissingOrderId { status })?;
                Ok(Self::Submitted { order_id })
            }
            OrderStatus::Filled => {
                let order_id = order_id.ok_or(ExecutionError::MissingOrderId { status })?;
                let price_cents =
                    price_cents.ok_or(ExecutionError::MissingPriceCents { status })?;
                let executed_at =
                    executed_at.ok_or(ExecutionError::MissingExecutedAt { status })?;
                Ok(Self::Filled {
                    executed_at: Utc.from_utc_datetime(&executed_at),
                    order_id,
                    price_cents: price_cents.try_into()?,
                })
            }
            OrderStatus::Failed => {
                let failed_at = executed_at.ok_or(ExecutionError::MissingExecutedAt { status })?;
                Ok(Self::Failed {
                    failed_at: Utc.from_utc_datetime(&failed_at),
                    error_reason: None, // We don't store error_reason in database yet
                })
            }
        }
    }

    pub async fn store_update(
        &self,
        sql_tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        execution_id: i64,
    ) -> Result<(), crate::PersistenceError> {
        let status_str = self.status().as_str();
        let db_fields = self.to_db_fields()?;

        let result = sqlx::query!(
            "
            UPDATE offchain_trades
            SET status = ?1, order_id = ?2, price_cents = ?3, executed_at = ?4
            WHERE id = ?5
            ",
            status_str,
            db_fields.order_id,
            db_fields.price_cents,
            db_fields.executed_at,
            execution_id
        )
        .execute(&mut **sql_tx)
        .await?;

        if result.rows_affected() == 0 {
            return Err(crate::PersistenceError::RowNotFound { execution_id });
        }

        Ok(())
    }

    pub async fn store(
        &self,
        sql_tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        symbol: &Symbol,
        shares: Positive<FractionalShares>,
        direction: Direction,
        executor: SupportedExecutor,
    ) -> Result<i64, crate::PersistenceError> {
        let status_str = self.status().as_str();
        let db_fields = self.to_db_fields()?;

        let symbol_str = symbol.to_string();
        let shares_f64 = shares
            .value()
            .value()
            .to_f64()
            .ok_or(crate::PersistenceError::InvalidShareQuantity(0.0))?;
        let direction_str = direction.as_str();
        let executor_str = executor.to_string();

        let result = sqlx::query!(
            r#"
            INSERT INTO offchain_trades (
                symbol,
                shares,
                direction,
                broker,
                order_id,
                price_cents,
                status,
                executed_at
            )
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
            "#,
            symbol_str,
            shares_f64,
            direction_str,
            executor_str,
            db_fields.order_id,
            db_fields.price_cents,
            status_str,
            db_fields.executed_at
        )
        .execute(&mut **sql_tx)
        .await?;

        Ok(result.last_insert_rowid())
    }

    pub(crate) fn to_db_fields(&self) -> Result<OrderStateDbFields, ExecutionError> {
        match self {
            Self::Pending => Ok(OrderStateDbFields {
                order_id: None,
                price_cents: None,
                executed_at: None,
            }),
            Self::Submitted { order_id } => Ok(OrderStateDbFields {
                order_id: Some(order_id.clone()),
                price_cents: None,
                executed_at: None,
            }),
            Self::Filled {
                executed_at,
                order_id,
                price_cents,
            } => Ok(OrderStateDbFields {
                order_id: Some(order_id.clone()),
                price_cents: Some((*price_cents).try_into()?),
                executed_at: Some(executed_at.naive_utc()),
            }),
            Self::Failed {
                failed_at,
                error_reason: _,
            } => Ok(OrderStateDbFields {
                order_id: None,
                price_cents: None,
                executed_at: Some(failed_at.naive_utc()),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    #[test]
    fn test_from_db_row_pending() {
        let result = OrderState::from_db_row(OrderStatus::Pending, None, None, None).unwrap();
        assert_eq!(result, OrderState::Pending);
    }

    #[test]
    fn test_from_db_row_submitted() {
        let result = OrderState::from_db_row(
            OrderStatus::Submitted,
            Some("ORDER123".to_string()),
            None,
            None,
        )
        .unwrap();
        assert_eq!(
            result,
            OrderState::Submitted {
                order_id: "ORDER123".to_string()
            }
        );
    }

    #[test]
    fn test_from_db_row_filled() {
        let timestamp = Utc::now().naive_utc();
        let result = OrderState::from_db_row(
            OrderStatus::Filled,
            Some("ORDER123".to_string()),
            Some(15000),
            Some(timestamp),
        )
        .unwrap();

        match result {
            OrderState::Filled {
                executed_at,
                order_id,
                price_cents,
            } => {
                assert_eq!(order_id, "ORDER123");
                assert_eq!(price_cents, 15000);
                assert_eq!(executed_at.naive_utc(), timestamp);
            }
            _ => panic!("Expected Filled variant"),
        }
    }

    #[test]
    fn test_from_db_row_failed() {
        let timestamp = Utc::now().naive_utc();
        let result =
            OrderState::from_db_row(OrderStatus::Failed, None, None, Some(timestamp)).unwrap();

        match result {
            OrderState::Failed {
                failed_at,
                error_reason,
            } => {
                assert_eq!(failed_at.naive_utc(), timestamp);
                assert_eq!(error_reason, None);
            }
            _ => panic!("Expected Failed variant"),
        }
    }

    #[test]
    fn test_from_db_row_submitted_missing_order_id() {
        let result = OrderState::from_db_row(OrderStatus::Submitted, None, None, None);
        assert!(matches!(
            result.unwrap_err(),
            ExecutionError::MissingOrderId {
                status: OrderStatus::Submitted
            }
        ));
    }

    #[test]
    fn test_from_db_row_filled_missing_order_id() {
        let timestamp = Utc::now().naive_utc();
        let result =
            OrderState::from_db_row(OrderStatus::Filled, None, Some(15000), Some(timestamp));
        assert!(matches!(
            result.unwrap_err(),
            ExecutionError::MissingOrderId {
                status: OrderStatus::Filled
            }
        ));
    }

    #[test]
    fn test_from_db_row_filled_missing_price_cents() {
        let timestamp = Utc::now().naive_utc();
        let result = OrderState::from_db_row(
            OrderStatus::Filled,
            Some("ORDER123".to_string()),
            None,
            Some(timestamp),
        );
        assert!(matches!(
            result.unwrap_err(),
            ExecutionError::MissingPriceCents {
                status: OrderStatus::Filled
            }
        ));
    }

    #[test]
    fn test_from_db_row_filled_missing_executed_at() {
        let result = OrderState::from_db_row(
            OrderStatus::Filled,
            Some("ORDER123".to_string()),
            Some(15000),
            None,
        );
        assert!(matches!(
            result.unwrap_err(),
            ExecutionError::MissingExecutedAt {
                status: OrderStatus::Filled
            }
        ));
    }

    #[test]
    fn test_from_db_row_failed_missing_executed_at() {
        let result = OrderState::from_db_row(OrderStatus::Failed, None, None, None);
        assert!(matches!(
            result.unwrap_err(),
            ExecutionError::MissingExecutedAt {
                status: OrderStatus::Failed
            }
        ));
    }

    #[test]
    fn test_to_db_fields_pending() {
        let state = OrderState::Pending;
        let db_fields = state.to_db_fields().unwrap();
        assert_eq!(db_fields.order_id, None);
        assert_eq!(db_fields.price_cents, None);
        assert_eq!(db_fields.executed_at, None);
    }

    #[test]
    fn test_to_db_fields_submitted() {
        let state = OrderState::Submitted {
            order_id: "ORDER123".to_string(),
        };
        let db_fields = state.to_db_fields().unwrap();
        assert_eq!(db_fields.order_id, Some("ORDER123".to_string()));
        assert_eq!(db_fields.price_cents, None);
        assert_eq!(db_fields.executed_at, None);
    }

    #[test]
    fn test_to_db_fields_filled() {
        let timestamp = Utc::now();
        let state = OrderState::Filled {
            executed_at: timestamp,
            order_id: "ORDER123".to_string(),
            price_cents: 15000,
        };
        let db_fields = state.to_db_fields().unwrap();
        assert_eq!(db_fields.order_id, Some("ORDER123".to_string()));
        assert_eq!(db_fields.price_cents, Some(15000));
        assert_eq!(db_fields.executed_at, Some(timestamp.naive_utc()));
    }

    #[test]
    fn test_to_db_fields_failed() {
        let timestamp = Utc::now();
        let state = OrderState::Failed {
            failed_at: timestamp,
            error_reason: Some("Test error".to_string()),
        };
        let db_fields = state.to_db_fields().unwrap();
        assert_eq!(db_fields.order_id, None);
        assert_eq!(db_fields.price_cents, None);
        assert_eq!(db_fields.executed_at, Some(timestamp.naive_utc()));
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
                price_cents: 15000,
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

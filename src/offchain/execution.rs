use serde::Serialize;
use sqlx::SqlitePool;
use st0x_execution::{OrderStatus, SupportedExecutor, Symbol};

use crate::offchain_order::{OffchainOrderAggregate, OffchainOrderId};
use crate::onchain::OnChainError;

pub(crate) async fn find_executions_by_symbol_status_and_broker(
    pool: &SqlitePool,
    symbol: Option<Symbol>,
    status: OrderStatus,
    broker: Option<SupportedExecutor>,
) -> Result<Vec<(OffchainOrderId, OffchainOrderAggregate)>, OnChainError> {
    let status_str = to_json_str(&status)?;

    let rows = match (symbol, broker) {
        (None, None) => {
            sqlx::query_as(
                "SELECT view_id, payload FROM offchain_order_view \
                 WHERE status = ?1 ORDER BY view_id ASC",
            )
            .bind(&status_str)
            .fetch_all(pool)
            .await?
        }
        (None, Some(broker)) => {
            let broker_str = to_json_str(&broker)?;
            sqlx::query_as(
                "SELECT view_id, payload FROM offchain_order_view \
                 WHERE status = ?1 AND executor = ?2 ORDER BY view_id ASC",
            )
            .bind(&status_str)
            .bind(&broker_str)
            .fetch_all(pool)
            .await?
        }
        (Some(symbol), None) => {
            sqlx::query_as(
                "SELECT view_id, payload FROM offchain_order_view \
                 WHERE symbol = ?1 AND status = ?2 ORDER BY view_id ASC",
            )
            .bind(symbol.to_string())
            .bind(&status_str)
            .fetch_all(pool)
            .await?
        }
        (Some(symbol), Some(broker)) => {
            let broker_str = to_json_str(&broker)?;
            sqlx::query_as(
                "SELECT view_id, payload FROM offchain_order_view \
                 WHERE symbol = ?1 AND status = ?2 AND executor = ?3 ORDER BY view_id ASC",
            )
            .bind(symbol.to_string())
            .bind(&status_str)
            .bind(&broker_str)
            .fetch_all(pool)
            .await?
        }
    };

    rows.into_iter()
        .map(|(view_id, payload): (String, String)| {
            let id: OffchainOrderId = view_id.parse()?;
            let aggregate = serde_json::from_str(&payload)?;
            Ok((id, aggregate))
        })
        .collect()
}

fn to_json_str<T: Serialize>(value: &T) -> Result<String, serde_json::Error> {
    let json = serde_json::to_string(value)?;
    Ok(json.trim_matches('"').to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn to_json_str_strips_quotes() {
        assert_eq!(to_json_str(&OrderStatus::Pending).unwrap(), "Pending");
        assert_eq!(to_json_str(&OrderStatus::Submitted).unwrap(), "Submitted");
        assert_eq!(to_json_str(&OrderStatus::Filled).unwrap(), "Filled");
        assert_eq!(to_json_str(&OrderStatus::Failed).unwrap(), "Failed");

        assert_eq!(to_json_str(&SupportedExecutor::Schwab).unwrap(), "Schwab");
        assert_eq!(
            to_json_str(&SupportedExecutor::AlpacaTradingApi).unwrap(),
            "AlpacaTradingApi"
        );
        assert_eq!(to_json_str(&SupportedExecutor::DryRun).unwrap(), "DryRun");
    }
}

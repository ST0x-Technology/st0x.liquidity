use sqlx::SqlitePool;
use tracing::warn;

use st0x_execution::OrderStatus;

use st0x_event_sorcery::Lifecycle;

use crate::offchain_order::{OffchainOrder, OffchainOrderId};
use crate::onchain::OnChainError;

/// Loads all live offchain orders matching the given status from the
/// view table.
///
/// The `status` generated column on `offchain_order_view` extracts the
/// OffchainOrder variant name (Pending, Submitted, Filled, Failed) from
/// the Lifecycle JSON payload. Additional filtering (by symbol, executor,
/// etc.) is done in Rust after deserialization.
///
/// Non-live aggregates (Uninitialized, Failed) are skipped with a warning.
pub(crate) async fn find_orders_by_status(
    pool: &SqlitePool,
    status: OrderStatus,
) -> Result<Vec<(OffchainOrderId, OffchainOrder)>, OnChainError> {
    let status_str = format!("{status:?}");

    let rows: Vec<(String, String)> = sqlx::query_as(
        "SELECT view_id, payload FROM offchain_order_view \
         WHERE status = ?1 ORDER BY view_id ASC",
    )
    .bind(&status_str)
    .fetch_all(pool)
    .await?;

    rows.into_iter()
        .map(|(view_id, payload)| {
            let id: OffchainOrderId = view_id.parse()?;
            let lifecycle: Lifecycle<OffchainOrder> = serde_json::from_str(&payload)?;
            Ok((id, lifecycle))
        })
        .filter_map(|result: Result<_, OnChainError>| match result {
            Ok((id, Lifecycle::Live(order))) => Some(Ok((id, order))),
            Ok((id, other)) => {
                warn!(%id, state = ?other, "Skipping non-live aggregate in view");
                None
            }
            Err(error) => Some(Err(error)),
        })
        .collect()
}

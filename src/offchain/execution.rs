use sqlx::SqlitePool;

use st0x_event_sorcery::Lifecycle;
use st0x_execution::OrderStatus;

use crate::offchain_order::{OffchainOrder, OffchainOrderId};
use crate::onchain::OnChainError;

/// Loads all offchain orders matching the given status from the view table.
///
/// The `status` generated column on `offchain_order_view` extracts the
/// OffchainOrder variant name (Pending, Submitted, Filled, Failed) from
/// the Lifecycle JSON payload. Additional filtering (by symbol, executor,
/// etc.) is done in Rust after deserialization.
pub(crate) async fn find_orders_by_status(
    pool: &SqlitePool,
    status: OrderStatus,
) -> Result<Vec<(OffchainOrderId, Lifecycle<OffchainOrder>)>, OnChainError> {
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
            let aggregate = serde_json::from_str(&payload)?;
            Ok((id, aggregate))
        })
        .collect()
}

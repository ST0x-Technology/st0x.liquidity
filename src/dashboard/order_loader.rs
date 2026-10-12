//! Live broker orders and the bot's Raindex orders, as `GET /orders/pending`
//! and `GET /orders/raindex` serve them and the `liq_*` order series count
//! them.

use serde::{Deserialize, Serialize};
use sqlx::SqliteExecutor;
use tracing::warn;

use st0x_config::Ctx;

const DEFAULT_RAINDEX_ORDERS_PAGE_SIZE: u32 = 50;
const MAX_RAINDEX_ORDERS_PAGE_SIZE: u32 = 100;

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct PendingOrderResponse {
    pub(crate) view_id: String,
    pub(crate) status: String,
    pub(crate) symbol: String,
    pub(crate) direction: String,
    pub(crate) shares: String,
    pub(crate) executor: String,
    pub(crate) placed_at: String,
    pub(crate) submitted_at: Option<String>,
    pub(crate) shares_filled: Option<String>,
    pub(crate) avg_price: Option<String>,
}

/// The `offchain_order_view` statuses of an order that is still live.
macro_rules! pending_order_statuses {
    () => {
        "status IN ('Pending', 'Submitted', 'PartiallyFilled', 'Cancelling')"
    };
}

const PENDING_ORDERS_QUERY: &str = concat!(
    "SELECT view_id, status, payload FROM offchain_order_view WHERE ",
    pending_order_statuses!(),
    " ORDER BY rowid DESC LIMIT 100"
);

/// Counts every live offchain order row, with no limit and no payload check.
const PENDING_ORDERS_COUNT_QUERY: &str = concat!(
    "SELECT COUNT(*) FROM offchain_order_view WHERE ",
    pending_order_statuses!()
);

/// The newest 100 non-terminal offchain order rows, without the ones whose
/// payload does not parse (a malformed row still takes one of the 100), or no
/// orders when the query fails. `GET /orders/pending` returns exactly this.
pub(crate) async fn load_pending_orders<'conn>(
    executor: impl SqliteExecutor<'conn>,
) -> Vec<PendingOrderResponse> {
    try_load_pending_orders(executor)
        .await
        .unwrap_or_else(|error| {
            warn!(target: "dashboard", %error, "Failed to load pending orders");
            Vec::new()
        })
}

/// [`load_pending_orders`] with the query error kept, for a caller that must
/// tell a failed read from no orders.
pub(crate) async fn try_load_pending_orders<'conn>(
    executor: impl SqliteExecutor<'conn>,
) -> Result<Vec<PendingOrderResponse>, sqlx::Error> {
    let rows: Vec<(String, String, String)> = sqlx::query_as(PENDING_ORDERS_QUERY)
        .fetch_all(executor)
        .await?;

    Ok(rows
        .into_iter()
        .filter_map(|(view_id, status, payload_str)| {
            parse_pending_order(view_id, status, &payload_str)
        })
        .collect())
}

/// Every non-terminal `offchain_order_view` row, with no limit and no payload
/// check.
pub(crate) async fn count_pending_orders<'conn>(
    executor: impl SqliteExecutor<'conn>,
) -> Result<i64, sqlx::Error> {
    sqlx::query_scalar(PENDING_ORDERS_COUNT_QUERY)
        .fetch_one(executor)
        .await
}

fn parse_pending_order(
    view_id: String,
    status: String,
    payload_str: &str,
) -> Option<PendingOrderResponse> {
    let payload: serde_json::Value = serde_json::from_str(payload_str).ok()?;
    let inner = payload.get("Live")?.get(&status)?;

    Some(PendingOrderResponse {
        view_id,
        symbol: inner["symbol"].as_str()?.to_string(),
        direction: inner["direction"].as_str()?.to_string(),
        shares: inner["shares"].as_str().unwrap_or("0").to_string(),
        executor: inner["executor"].as_str().unwrap_or("unknown").to_string(),
        placed_at: inner["placed_at"].as_str().unwrap_or("").to_string(),
        submitted_at: inner["submitted_at"].as_str().map(String::from),
        shares_filled: inner["shares_filled"].as_str().map(String::from),
        avg_price: inner["avg_price"].as_str().map(String::from),
        status,
    })
}

/// The st0x REST API's answer for the bot's Raindex orders.
pub(crate) enum RaindexOrders {
    /// The upstream JSON body, unchanged.
    Available(serde_json::Value),
    /// Why there is no body; `GET /orders/raindex` returns this reason.
    Unavailable { reason: &'static str },
}

/// One page of the bot's Raindex orders from the st0x REST API. `page`
/// defaults to 1 and `page_size` to 50, clamped to 1..=100.
pub(crate) async fn fetch_raindex_orders(
    ctx: &Ctx,
    page: Option<u32>,
    page_size: Option<u32>,
) -> RaindexOrders {
    let Some(rest_api) = &ctx.rest_api else {
        return RaindexOrders::Unavailable {
            reason: "REST API not configured (simulate mode)",
        };
    };

    let url = format!(
        "{}/v1/orders/owner/{:#x}",
        rest_api.url.trim_end_matches('/'),
        ctx.vault_owner()
    );

    let page = page.unwrap_or(1).max(1);
    let page_size = page_size
        .unwrap_or(DEFAULT_RAINDEX_ORDERS_PAGE_SIZE)
        .clamp(1, MAX_RAINDEX_ORDERS_PAGE_SIZE);

    let mut request = rest_api
        .http_client
        .get(&url)
        .query(&[("page", page), ("pageSize", page_size)]);

    if let (Some(key_id), Some(key_secret)) = (&rest_api.key_id, &rest_api.key_secret) {
        request = request.basic_auth(key_id, Some(key_secret));
    }

    let response = match request.send().await {
        Ok(response) => response,
        Err(error) => {
            warn!(target: "dashboard", %error, %url, "Failed to reach st0x REST API");
            return RaindexOrders::Unavailable {
                reason: "REST API unreachable",
            };
        }
    };

    read_raindex_orders(response, &url).await
}

async fn read_raindex_orders(response: reqwest::Response, url: &str) -> RaindexOrders {
    if !response.status().is_success() {
        let status = response.status();
        warn!(target: "dashboard", %status, %url, "st0x REST API returned error");
        return RaindexOrders::Unavailable {
            reason: "REST API returned an error",
        };
    }

    match response.text().await {
        Ok(body) => match serde_json::from_str(&body) {
            Ok(value) => RaindexOrders::Available(value),
            Err(error) => {
                warn!(target: "dashboard", %error, "st0x REST API returned non-JSON body");
                RaindexOrders::Unavailable {
                    reason: "REST API returned non-JSON",
                }
            }
        },
        Err(error) => {
            warn!(target: "dashboard", %error, "Failed to read st0x REST API response body");
            RaindexOrders::Unavailable {
                reason: "Failed to read REST API response",
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_pending_order_handles_cancelling_status() {
        // The cancel-and-replace flow introduced the non-terminal `Cancelling`
        // state; the /orders/pending endpoint must surface it like any other
        // live order rather than dropping it on the floor.
        let payload = r#"{"Live":{"Cancelling":{"symbol":"AAPL","direction":"Sell","shares":"1.5","executor":"DryRun","placed_at":"2026-01-01T00:00:00Z","submitted_at":"2026-01-01T00:00:01Z","shares_filled":"0.5","avg_price":"195.25"}}}"#;

        let parsed = parse_pending_order("order-1".to_string(), "Cancelling".to_string(), payload)
            .expect("Cancelling order should parse");

        assert_eq!(parsed.view_id, "order-1");
        assert_eq!(parsed.status, "Cancelling");
        assert_eq!(parsed.symbol, "AAPL");
        assert_eq!(parsed.direction, "Sell");
        assert_eq!(parsed.shares, "1.5");
        assert_eq!(parsed.executor, "DryRun");
        assert_eq!(parsed.placed_at, "2026-01-01T00:00:00Z");
        assert_eq!(parsed.submitted_at.as_deref(), Some("2026-01-01T00:00:01Z"));
        assert_eq!(parsed.shares_filled.as_deref(), Some("0.5"));
        assert_eq!(parsed.avg_price.as_deref(), Some("195.25"));
    }
}

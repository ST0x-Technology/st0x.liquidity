use chrono::Utc;
use num_traits::ToPrimitive;
use tracing::debug;
use uuid::Uuid;

use super::AlpacaBrokerApiError;
use super::auth::{AlpacaBrokerApiClient, BrokerOrderStatus, OrderRequest, OrderSide};
use crate::{Direction, MarketOrder, OrderPlacement, OrderStatus, OrderUpdate};

pub(super) async fn place_market_order(
    client: &AlpacaBrokerApiClient,
    market_order: MarketOrder,
) -> Result<OrderPlacement<String>, AlpacaBrokerApiError> {
    debug!(
        "Placing Alpaca Broker API market order: {} {} shares of {}",
        market_order.direction, market_order.shares, market_order.symbol
    );

    let side = match market_order.direction {
        Direction::Buy => OrderSide::Buy,
        Direction::Sell => OrderSide::Sell,
    };

    let request = OrderRequest {
        symbol: market_order.symbol.clone(),
        quantity: market_order.shares,
        side,
        order_type: "market",
        time_in_force: "day",
    };

    let response = client.place_order(&request).await?;

    Ok(OrderPlacement {
        order_id: response.id.to_string(),
        symbol: market_order.symbol,
        shares: market_order.shares,
        direction: market_order.direction,
        placed_at: Utc::now(),
    })
}

pub(super) async fn get_order_status(
    client: &AlpacaBrokerApiClient,
    order_id: &str,
) -> Result<OrderUpdate<String>, AlpacaBrokerApiError> {
    debug!(
        "Querying Alpaca Broker API order status for order ID: {}",
        order_id
    );

    let order_uuid = Uuid::parse_str(order_id)?;
    let response = client.get_order(order_uuid).await?;

    let direction = match response.side {
        OrderSide::Buy => Direction::Buy,
        OrderSide::Sell => Direction::Sell,
    };

    let status = map_broker_status_to_order_status(response.status);
    let price_cents = convert_price_to_cents(response.filled_average_price)?;

    Ok(OrderUpdate {
        order_id: order_id.to_string(),
        symbol: response.symbol,
        shares: response.quantity,
        direction,
        status,
        updated_at: Utc::now(),
        price_cents,
    })
}

pub(super) async fn poll_pending_orders(
    client: &AlpacaBrokerApiClient,
) -> Result<Vec<OrderUpdate<String>>, AlpacaBrokerApiError> {
    debug!("Polling all pending Alpaca Broker API orders");

    let orders = client.list_open_orders().await?;

    let order_updates = orders
        .into_iter()
        .map(|response| {
            let direction = match response.side {
                OrderSide::Buy => Direction::Buy,
                OrderSide::Sell => Direction::Sell,
            };

            let status = map_broker_status_to_order_status(response.status);
            let price_cents = convert_price_to_cents(response.filled_average_price)?;

            Ok(OrderUpdate {
                order_id: response.id.to_string(),
                symbol: response.symbol,
                shares: response.quantity,
                direction,
                status,
                updated_at: Utc::now(),
                price_cents,
            })
        })
        .collect::<Result<Vec<_>, AlpacaBrokerApiError>>()?;

    debug!("Found {} pending orders", order_updates.len());
    Ok(order_updates)
}

fn map_broker_status_to_order_status(status: BrokerOrderStatus) -> OrderStatus {
    match status {
        // Submitted to broker and in progress
        BrokerOrderStatus::New
        | BrokerOrderStatus::Accepted
        | BrokerOrderStatus::PendingNew
        | BrokerOrderStatus::PartiallyFilled
        | BrokerOrderStatus::AcceptedForBidding
        | BrokerOrderStatus::PendingCancel
        | BrokerOrderStatus::PendingReplace
        | BrokerOrderStatus::Stopped => OrderStatus::Submitted,

        // Successfully filled
        BrokerOrderStatus::Filled => OrderStatus::Filled,

        // Failed/terminal statuses
        BrokerOrderStatus::Canceled
        | BrokerOrderStatus::Expired
        | BrokerOrderStatus::DoneForDay
        | BrokerOrderStatus::Rejected
        | BrokerOrderStatus::Replaced
        | BrokerOrderStatus::Suspended
        | BrokerOrderStatus::Calculated => OrderStatus::Failed,
    }
}

fn convert_price_to_cents(price: Option<f64>) -> Result<Option<u64>, AlpacaBrokerApiError> {
    price.map_or(Ok(None), |p| {
        let cents_float = (p * 100.0).round();
        let cents = cents_float
            .to_u64()
            .ok_or(AlpacaBrokerApiError::PriceConversion(p))?;
        Ok(Some(cents))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::alpaca_broker_api::auth::{AlpacaBrokerApiAuthEnv, AlpacaBrokerApiMode};
    use httpmock::prelude::*;
    use serde_json::json;

    fn create_test_config(base_url: &str) -> AlpacaBrokerApiAuthEnv {
        AlpacaBrokerApiAuthEnv {
            alpaca_broker_api_key: "test_key".to_string(),
            alpaca_broker_api_secret: "test_secret".to_string(),
            alpaca_account_id: "test_account_123".to_string(),
            alpaca_broker_api_mode: AlpacaBrokerApiMode::Mock(base_url.to_string()),
        }
    }

    #[tokio::test]
    async fn test_place_market_order_buy_success() {
        let server = MockServer::start();
        let config = create_test_config(&server.base_url());

        let mock = server.mock(|when, then| {
            when.method(POST)
                .path("/v1/trading/accounts/test_account_123/orders")
                .json_body(json!({
                    "symbol": "AAPL",
                    "qty": "100",
                    "side": "buy",
                    "type": "market",
                    "time_in_force": "day"
                }));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "id": "904837e3-3b76-47ec-b432-046db621571b",
                    "symbol": "AAPL",
                    "qty": "100",
                    "side": "buy",
                    "status": "new",
                    "filled_avg_price": null
                }));
        });

        let client = AlpacaBrokerApiClient::new(&config).unwrap();
        let market_order = MarketOrder {
            symbol: Symbol::new("AAPL").unwrap(),
            shares: Shares::new(100).unwrap(),
            direction: Direction::Buy,
        };

        let result = place_market_order(&client, market_order).await;

        mock.assert();
        let placement = result.unwrap();
        assert_eq!(placement.order_id, "904837e3-3b76-47ec-b432-046db621571b");
        assert_eq!(placement.symbol.to_string(), "AAPL");
        assert_eq!(placement.shares.value(), 100);
        assert_eq!(placement.direction, Direction::Buy);
    }

    #[tokio::test]
    async fn test_place_market_order_sell_success() {
        let server = MockServer::start();
        let config = create_test_config(&server.base_url());

        let mock = server.mock(|when, then| {
            when.method(POST)
                .path("/v1/trading/accounts/test_account_123/orders")
                .json_body(json!({
                    "symbol": "TSLA",
                    "qty": "50",
                    "side": "sell",
                    "type": "market",
                    "time_in_force": "day"
                }));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "id": "61e7b016-9c91-4a97-b912-615c9d365c9d",
                    "symbol": "TSLA",
                    "qty": "50",
                    "side": "sell",
                    "status": "new",
                    "filled_avg_price": null
                }));
        });

        let client = AlpacaBrokerApiClient::new(&config).unwrap();
        let market_order = MarketOrder {
            symbol: Symbol::new("TSLA").unwrap(),
            shares: Shares::new(50).unwrap(),
            direction: Direction::Sell,
        };

        let result = place_market_order(&client, market_order).await;

        mock.assert();
        let placement = result.unwrap();
        assert_eq!(placement.order_id, "61e7b016-9c91-4a97-b912-615c9d365c9d");
        assert_eq!(placement.symbol.to_string(), "TSLA");
        assert_eq!(placement.shares.value(), 50);
        assert_eq!(placement.direction, Direction::Sell);
    }

    #[tokio::test]
    async fn test_get_order_status_pending() {
        let server = MockServer::start();
        let config = create_test_config(&server.base_url());
        let order_id = "904837e3-3b76-47ec-b432-046db621571b";

        let mock = server.mock(|when, then| {
            when.method(GET).path(format!(
                "/v1/trading/accounts/test_account_123/orders/{order_id}"
            ));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "id": order_id,
                    "symbol": "AAPL",
                    "qty": "100",
                    "side": "buy",
                    "status": "new",
                    "filled_avg_price": null
                }));
        });

        let client = AlpacaBrokerApiClient::new(&config).unwrap();
        let result = get_order_status(&client, order_id).await;

        mock.assert();
        let order_update = result.unwrap();
        assert_eq!(order_update.order_id, order_id);
        assert_eq!(order_update.symbol.to_string(), "AAPL");
        assert_eq!(order_update.shares.value(), 100);
        assert_eq!(order_update.direction, Direction::Buy);
        assert_eq!(order_update.status, OrderStatus::Submitted);
        assert_eq!(order_update.price_cents, None);
    }

    #[tokio::test]
    async fn test_get_order_status_filled() {
        let server = MockServer::start();
        let config = create_test_config(&server.base_url());
        let order_id = "61e7b016-9c91-4a97-b912-615c9d365c9d";

        let mock = server.mock(|when, then| {
            when.method(GET).path(format!(
                "/v1/trading/accounts/test_account_123/orders/{order_id}"
            ));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "id": order_id,
                    "symbol": "TSLA",
                    "qty": "50",
                    "side": "sell",
                    "status": "filled",
                    "filled_avg_price": "245.67"
                }));
        });

        let client = AlpacaBrokerApiClient::new(&config).unwrap();
        let result = get_order_status(&client, order_id).await;

        mock.assert();
        let order_update = result.unwrap();
        assert_eq!(order_update.order_id, order_id);
        assert_eq!(order_update.symbol.to_string(), "TSLA");
        assert_eq!(order_update.shares.value(), 50);
        assert_eq!(order_update.direction, Direction::Sell);
        assert_eq!(order_update.status, OrderStatus::Filled);
        assert_eq!(order_update.price_cents, Some(24567));
    }

    #[tokio::test]
    async fn test_get_order_status_rejected() {
        let server = MockServer::start();
        let config = create_test_config(&server.base_url());
        let order_id = "c7ca82d4-3c95-4f89-9b42-abc123def456";

        let mock = server.mock(|when, then| {
            when.method(GET).path(format!(
                "/v1/trading/accounts/test_account_123/orders/{order_id}"
            ));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "id": order_id,
                    "symbol": "MSFT",
                    "qty": "25",
                    "side": "buy",
                    "status": "rejected",
                    "filled_avg_price": null
                }));
        });

        let client = AlpacaBrokerApiClient::new(&config).unwrap();
        let result = get_order_status(&client, order_id).await;

        mock.assert();
        let order_update = result.unwrap();
        assert_eq!(order_update.order_id, order_id);
        assert_eq!(order_update.status, OrderStatus::Failed);
    }

    #[tokio::test]
    async fn test_poll_pending_orders_multiple() {
        let server = MockServer::start();
        let config = create_test_config(&server.base_url());

        let mock = server.mock(|when, then| {
            when.method(GET)
                .path("/v1/trading/accounts/test_account_123/orders")
                .query_param("status", "open");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([
                    {
                        "id": "904837e3-3b76-47ec-b432-046db621571b",
                        "symbol": "AAPL",
                        "qty": "100",
                        "side": "buy",
                        "status": "new",
                        "filled_avg_price": null
                    },
                    {
                        "id": "61e7b016-9c91-4a97-b912-615c9d365c9d",
                        "symbol": "TSLA",
                        "qty": "50",
                        "side": "sell",
                        "status": "partially_filled",
                        "filled_avg_price": null
                    }
                ]));
        });

        let client = AlpacaBrokerApiClient::new(&config).unwrap();
        let result = poll_pending_orders(&client).await;

        mock.assert();
        let order_updates = result.unwrap();
        assert_eq!(order_updates.len(), 2);

        assert_eq!(
            order_updates[0].order_id,
            "904837e3-3b76-47ec-b432-046db621571b"
        );
        assert_eq!(order_updates[0].symbol.to_string(), "AAPL");
        assert_eq!(order_updates[0].status, OrderStatus::Submitted);

        assert_eq!(
            order_updates[1].order_id,
            "61e7b016-9c91-4a97-b912-615c9d365c9d"
        );
        assert_eq!(order_updates[1].symbol.to_string(), "TSLA");
        assert_eq!(order_updates[1].status, OrderStatus::Submitted);
    }

    #[tokio::test]
    async fn test_poll_pending_orders_empty() {
        let server = MockServer::start();
        let config = create_test_config(&server.base_url());

        let mock = server.mock(|when, then| {
            when.method(GET)
                .path("/v1/trading/accounts/test_account_123/orders")
                .query_param("status", "open");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([]));
        });

        let client = AlpacaBrokerApiClient::new(&config).unwrap();
        let result = poll_pending_orders(&client).await;

        mock.assert();
        let order_updates = result.unwrap();
        assert!(order_updates.is_empty());
    }

    #[test]
    fn test_convert_price_to_cents_some() {
        let result = convert_price_to_cents(Some(245.67)).unwrap();
        assert_eq!(result, Some(24567));
    }

    #[test]
    fn test_convert_price_to_cents_none() {
        let result = convert_price_to_cents(None).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_convert_price_to_cents_negative_fails() {
        let result = convert_price_to_cents(Some(-100.0));
        assert!(matches!(
            result.unwrap_err(),
            AlpacaBrokerApiError::PriceConversion(_)
        ));
    }

    #[test]
    fn test_map_broker_status_new() {
        assert_eq!(
            map_broker_status_to_order_status(BrokerOrderStatus::New),
            OrderStatus::Submitted
        );
    }

    #[test]
    fn test_map_broker_status_filled() {
        assert_eq!(
            map_broker_status_to_order_status(BrokerOrderStatus::Filled),
            OrderStatus::Filled
        );
    }

    #[test]
    fn test_map_broker_status_rejected() {
        assert_eq!(
            map_broker_status_to_order_status(BrokerOrderStatus::Rejected),
            OrderStatus::Failed
        );
    }
}

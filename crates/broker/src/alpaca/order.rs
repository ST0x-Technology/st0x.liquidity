use apca::api::v2::{order, orders};
use apca::{Client, RequestError};
use chrono::Utc;
use num_traits::ToPrimitive;
use tracing::debug;
use uuid::Uuid;

use crate::{
    BrokerError, Direction, MarketOrder, OrderPlacement, OrderStatus, OrderUpdate, Shares, Symbol,
};

pub(super) async fn place_market_order(
    client: &Client,
    market_order: MarketOrder,
) -> Result<OrderPlacement<String>, BrokerError> {
    debug!(
        "Placing Alpaca market order: {} {} shares of {}",
        market_order.direction, market_order.shares, market_order.symbol
    );

    let alpaca_side = match market_order.direction {
        Direction::Buy => order::Side::Buy,
        Direction::Sell => order::Side::Sell,
    };

    let order_init = order::CreateReqInit {
        class: order::Class::Simple,
        type_: order::Type::Market,
        time_in_force: order::TimeInForce::Day,
        extended_hours: false,
        ..Default::default()
    };

    let order_request = order_init.init(
        market_order.symbol.to_string(),
        alpaca_side,
        order::Amount::quantity(market_order.shares.value()),
    );

    let order_response = client
        .issue::<order::Create>(&order_request)
        .await
        .map_err(|e| match e {
            RequestError::Endpoint(endpoint_error) => {
                BrokerError::AlpacaRequest(format!("Order placement failed: {endpoint_error}"))
            }
            RequestError::Hyper(hyper_error) => {
                BrokerError::AlpacaRequest(format!("HTTP error: {hyper_error}"))
            }
            RequestError::HyperUtil(hyper_util_error) => {
                BrokerError::AlpacaRequest(format!("HTTP util error: {hyper_util_error}"))
            }
            RequestError::Io(io_error) => {
                BrokerError::AlpacaRequest(format!("IO error: {io_error}"))
            }
        })?;

    let order_id = order_response.id.to_string();

    Ok(OrderPlacement {
        order_id,
        symbol: market_order.symbol,
        shares: market_order.shares,
        direction: market_order.direction,
        placed_at: chrono::Utc::now(),
    })
}

pub(super) async fn get_order_status(
    client: &Client,
    order_id: &str,
) -> Result<OrderUpdate<String>, BrokerError> {
    debug!("Querying Alpaca order status for order ID: {}", order_id);

    let order_uuid = Uuid::parse_str(order_id)
        .map_err(|e| BrokerError::AlpacaRequest(format!("Invalid order ID format: {e}")))?;

    let alpaca_order_id = order::Id(order_uuid);

    let order_response =
        client
            .issue::<order::Get>(&alpaca_order_id)
            .await
            .map_err(|e| match e {
                RequestError::Endpoint(endpoint_error) => BrokerError::AlpacaRequest(format!(
                    "Order status query failed: {endpoint_error}"
                )),
                RequestError::Hyper(hyper_error) => {
                    BrokerError::AlpacaRequest(format!("HTTP error: {hyper_error}"))
                }
                RequestError::HyperUtil(hyper_util_error) => {
                    BrokerError::AlpacaRequest(format!("HTTP util error: {hyper_util_error}"))
                }
                RequestError::Io(io_error) => {
                    BrokerError::AlpacaRequest(format!("IO error: {io_error}"))
                }
            })?;

    let symbol = Symbol::new(order_response.symbol.clone())
        .map_err(|e| BrokerError::AlpacaRequest(format!("Invalid symbol: {e}")))?;

    let shares = extract_shares_from_amount(&order_response.amount)?;

    let direction = match order_response.side {
        order::Side::Buy => Direction::Buy,
        order::Side::Sell => Direction::Sell,
    };

    let status = map_alpaca_status_to_order_status(order_response.status);

    let price_cents = extract_price_cents_from_order(&order_response)?;

    Ok(OrderUpdate {
        order_id: order_id.to_string(),
        symbol,
        shares,
        direction,
        status,
        updated_at: Utc::now(),
        price_cents,
    })
}

pub(super) async fn poll_pending_orders(
    client: &Client,
) -> Result<Vec<OrderUpdate<String>>, BrokerError> {
    debug!("Polling all pending Alpaca orders");

    let request = orders::ListReq {
        status: orders::Status::Open,
        limit: Some(500), // Maximum limit to get all pending orders
        ..Default::default()
    };

    let alpaca_orders = client
        .issue::<orders::List>(&request)
        .await
        .map_err(|e| match e {
            RequestError::Endpoint(endpoint_error) => {
                BrokerError::AlpacaRequest(format!("Order listing failed: {endpoint_error}"))
            }
            RequestError::Hyper(hyper_error) => {
                BrokerError::AlpacaRequest(format!("HTTP error: {hyper_error}"))
            }
            RequestError::HyperUtil(hyper_util_error) => {
                BrokerError::AlpacaRequest(format!("HTTP util error: {hyper_util_error}"))
            }
            RequestError::Io(io_error) => {
                BrokerError::AlpacaRequest(format!("IO error: {io_error}"))
            }
        })?;

    let order_updates = alpaca_orders
        .into_iter()
        .map(|alpaca_order| {
            let symbol = Symbol::new(alpaca_order.symbol.clone())
                .map_err(|e| BrokerError::AlpacaRequest(format!("Invalid symbol: {e}")))?;

            let shares = extract_shares_from_amount(&alpaca_order.amount)?;

            let direction = match alpaca_order.side {
                order::Side::Buy => Direction::Buy,
                order::Side::Sell => Direction::Sell,
            };

            let status = map_alpaca_status_to_order_status(alpaca_order.status);

            let price_cents = extract_price_cents_from_order(&alpaca_order)?;

            Ok(OrderUpdate {
                order_id: alpaca_order.id.to_string(),
                symbol,
                shares,
                direction,
                status,
                updated_at: Utc::now(),
                price_cents,
            })
        })
        .collect::<Result<Vec<_>, BrokerError>>()?;

    debug!("Found {} pending orders", order_updates.len());
    Ok(order_updates)
}

/// Maps Alpaca order status to our simplified OrderStatus enum
///
/// Note: All Alpaca in-progress statuses map to `OrderStatus::Submitted` because
/// they represent orders that have been submitted to and acknowledged by the broker.
/// `OrderStatus::Pending` is reserved for orders in our system that haven't been
/// sent to the broker yet (not applicable here since we're mapping broker responses).
fn map_alpaca_status_to_order_status(status: order::Status) -> OrderStatus {
    match status {
        // Submitted to broker and in progress (New, Accepted, working, etc.)
        order::Status::New
        | order::Status::Accepted
        | order::Status::PendingNew
        | order::Status::PartiallyFilled
        | order::Status::AcceptedForBidding
        | order::Status::PendingCancel
        | order::Status::PendingReplace
        | order::Status::Stopped => OrderStatus::Submitted,

        // Successfully filled
        order::Status::Filled => OrderStatus::Filled,

        // Failed/terminal statuses
        order::Status::Canceled
        | order::Status::Expired
        | order::Status::DoneForDay
        | order::Status::Rejected
        | order::Status::Replaced
        | order::Status::Suspended
        | order::Status::Calculated => OrderStatus::Failed,

        // Future-proofing: Alpaca's Status enum is marked #[non_exhaustive]
        // so new statuses may be added. We conservatively treat unknown statuses
        // as Failed to avoid incorrect handling. This will log a warning when
        // we encounter an unknown status, prompting us to update this mapping.
        #[allow(unreachable_patterns)]
        unknown => {
            debug!("Unknown Alpaca order status encountered: {unknown:?}, treating as Failed");
            OrderStatus::Failed
        }
    }
}

/// Extracts price in cents from Alpaca order
fn extract_price_cents_from_order(order: &order::Order) -> Result<Option<u64>, BrokerError> {
    if let Some(avg_fill_price) = &order.average_fill_price {
        let price_str = format!("{avg_fill_price}");
        let price_f64 = price_str
            .parse::<f64>()
            .map_err(|e| BrokerError::AlpacaRequest(format!("Invalid fill price: {e}")))?;

        let price_cents_float = (price_f64 * 100.0).round();

        let price_cents = price_cents_float.to_u64().ok_or_else(|| {
            BrokerError::AlpacaRequest(format!("Invalid price value: {price_f64}"))
        })?;

        Ok(Some(price_cents))
    } else {
        Ok(None)
    }
}

/// Extracts shares from Alpaca Amount enum
fn extract_shares_from_amount(amount: &order::Amount) -> Result<Shares, BrokerError> {
    match amount {
        order::Amount::Quantity { quantity } => {
            let qty_u64 = quantity
                .to_string()
                .parse::<u64>()
                .map_err(|e| BrokerError::AlpacaRequest(format!("Invalid quantity: {e}")))?;

            Shares::new(qty_u64)
                .map_err(|e| BrokerError::AlpacaRequest(format!("Invalid shares: {e}")))
        }
        order::Amount::Notional { notional: _ } => Err(BrokerError::AlpacaRequest(
            "Notional orders are not supported - cannot determine exact share quantity".to_string(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use apca::api::v2::order::Amount;
    use httpmock::prelude::*;
    use serde_json::json;

    fn create_test_client(mock_server: &MockServer) -> Client {
        let api_info =
            apca::ApiInfo::from_parts(mock_server.base_url(), "test_key", "test_secret").unwrap();
        Client::new(api_info)
    }

    #[tokio::test]
    async fn test_place_market_order_buy_success() {
        let server = MockServer::start();

        let mock = server.mock(|when, then| {
            when.method(POST).path("/v2/orders").json_body(json!({
                "symbol": "AAPL",
                "qty": "100",
                "side": "buy",
                "type": "market",
                "time_in_force": "day",
                "order_class": "simple",
                "extended_hours": false,
                "client_order_id": null,
                "limit_price": null,
                "stop_price": null,
                "trail_price": null,
                "trail_percent": null,
                "take_profit": null,
                "stop_loss": null
            }));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "id": "904837e3-3b76-47ec-b432-046db621571b",
                    "client_order_id": "",
                    "symbol": "AAPL",
                    "asset_id": "904837e3-3b76-47ec-b432-046db621571b",
                    "asset_class": "us_equity",
                    "qty": "100",
                    "filled_qty": "0",
                    "side": "buy",
                    "order_class": "simple",
                    "type": "market",
                    "time_in_force": "day",
                    "limit_price": null,
                    "stop_price": null,
                    "trail_price": null,
                    "trail_percent": null,
                    "status": "new",
                    "extended_hours": false,
                    "legs": [],
                    "created_at": "2030-01-15T09:30:00.000Z",
                    "updated_at": null,
                    "submitted_at": null,
                    "filled_at": null,
                    "expired_at": null,
                    "canceled_at": null,
                    "average_fill_price": null
                }));
        });

        let client = create_test_client(&server);
        let market_order = MarketOrder {
            symbol: Symbol::new("AAPL".to_string()).unwrap(),
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

        let mock = server.mock(|when, then| {
            when.method(POST).path("/v2/orders").json_body(json!({
                "symbol": "TSLA",
                "qty": "50",
                "side": "sell",
                "type": "market",
                "time_in_force": "day",
                "order_class": "simple",
                "extended_hours": false,
                "client_order_id": null,
                "limit_price": null,
                "stop_price": null,
                "trail_price": null,
                "trail_percent": null,
                "take_profit": null,
                "stop_loss": null
            }));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "id": "61e7b016-9c91-4a97-b912-615c9d365c9d",
                    "client_order_id": "",
                    "symbol": "TSLA",
                    "asset_id": "61e7b016-9c91-4a97-b912-615c9d365c9d",
                    "asset_class": "us_equity",
                    "qty": "50",
                    "filled_qty": "0",
                    "side": "sell",
                    "order_class": "simple",
                    "type": "market",
                    "time_in_force": "day",
                    "limit_price": null,
                    "stop_price": null,
                    "trail_price": null,
                    "trail_percent": null,
                    "status": "new",
                    "extended_hours": false,
                    "legs": [],
                    "created_at": "2030-01-15T09:30:00.000Z",
                    "updated_at": null,
                    "submitted_at": null,
                    "filled_at": null,
                    "expired_at": null,
                    "canceled_at": null,
                    "average_fill_price": null
                }));
        });

        let client = create_test_client(&server);
        let market_order = MarketOrder {
            symbol: Symbol::new("TSLA".to_string()).unwrap(),
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
    async fn test_place_market_order_invalid_symbol() {
        let server = MockServer::start();

        let mock = server.mock(|when, then| {
            when.method(POST).path("/v2/orders");
            then.status(422)
                .header("content-type", "application/json")
                .json_body(json!({
                    "code": 40010001,
                    "message": "symbol INVALID is not supported"
                }));
        });

        let client = create_test_client(&server);
        let market_order = MarketOrder {
            symbol: Symbol::new("INVALID".to_string()).unwrap(),
            shares: Shares::new(10).unwrap(),
            direction: Direction::Buy,
        };

        let result = place_market_order(&client, market_order).await;

        mock.assert();
        let error = result.unwrap_err();
        assert!(matches!(error, BrokerError::AlpacaRequest(_)));
    }

    #[tokio::test]
    async fn test_place_market_order_authentication_failure() {
        let server = MockServer::start();

        let mock = server.mock(|when, then| {
            when.method(POST).path("/v2/orders");
            then.status(401)
                .header("content-type", "application/json")
                .json_body(json!({
                    "code": 40110000,
                    "message": "Invalid credentials"
                }));
        });

        let client = create_test_client(&server);
        let market_order = MarketOrder {
            symbol: Symbol::new("AAPL".to_string()).unwrap(),
            shares: Shares::new(100).unwrap(),
            direction: Direction::Buy,
        };

        let result = place_market_order(&client, market_order).await;

        mock.assert();
        let error = result.unwrap_err();
        assert!(matches!(error, BrokerError::AlpacaRequest(_)));
    }

    #[tokio::test]
    async fn test_place_market_order_server_error() {
        let server = MockServer::start();

        let mock = server.mock(|when, then| {
            when.method(POST).path("/v2/orders");
            then.status(500)
                .header("content-type", "application/json")
                .json_body(json!({
                    "message": "Internal server error"
                }));
        });

        let client = create_test_client(&server);
        let market_order = MarketOrder {
            symbol: Symbol::new("SPY".to_string()).unwrap(),
            shares: Shares::new(25).unwrap(),
            direction: Direction::Buy,
        };

        let result = place_market_order(&client, market_order).await;

        mock.assert();
        let error = result.unwrap_err();
        assert!(matches!(error, BrokerError::AlpacaRequest(_)));
    }

    #[tokio::test]
    async fn test_get_order_status_pending_order() {
        let server = MockServer::start();
        let order_id = "904837e3-3b76-47ec-b432-046db621571b";

        let mock = server.mock(|when, then| {
            when.method(GET)
                .path(format!("/v2/orders/{}", order_id.replace("-", "")));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "id": order_id,
                    "client_order_id": "",
                    "symbol": "AAPL",
                    "asset_id": "904837e3-3b76-47ec-b432-046db621571b",
                    "asset_class": "us_equity",
                    "qty": "100",
                    "filled_qty": "0",
                    "side": "buy",
                    "order_class": "simple",
                    "type": "market",
                    "time_in_force": "day",
                    "status": "new",
                    "extended_hours": false,
                    "legs": [],
                    "created_at": "2030-01-15T09:30:00.000Z",
                    "updated_at": null,
                    "submitted_at": null,
                    "filled_at": null,
                    "expired_at": null,
                    "canceled_at": null,
                    "average_fill_price": null,
                    "limit_price": null,
                    "stop_price": null,
                    "trail_price": null,
                    "trail_percent": null
                }));
        });

        let client = create_test_client(&server);
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
    async fn test_get_order_status_filled_order() {
        let server = MockServer::start();
        let order_id = "61e7b016-9c91-4a97-b912-615c9d365c9d";

        let mock = server.mock(|when, then| {
            when.method(GET)
                .path(format!("/v2/orders/{}", order_id.replace("-", "")));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "id": order_id,
                    "client_order_id": "",
                    "symbol": "TSLA",
                    "asset_id": "61e7b016-9c91-4a97-b912-615c9d365c9d",
                    "asset_class": "us_equity",
                    "qty": "50",
                    "filled_qty": "50",
                    "side": "sell",
                    "order_class": "simple",
                    "type": "market",
                    "time_in_force": "day",
                    "status": "filled",
                    "extended_hours": false,
                    "legs": [],
                    "created_at": "2030-01-15T09:30:00.000Z",
                    "updated_at": "2030-01-15T09:31:00.000Z",
                    "submitted_at": "2030-01-15T09:30:00.000Z",
                    "filled_at": "2030-01-15T09:31:00.000Z",
                    "expired_at": null,
                    "canceled_at": null,
                    "filled_avg_price": "245.67",
                    "limit_price": null,
                    "stop_price": null,
                    "trail_price": null,
                    "trail_percent": null
                }));
        });

        let client = create_test_client(&server);
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
    async fn test_get_order_status_rejected_order() {
        let server = MockServer::start();
        let order_id = "c7ca82d4-3c95-4f89-9b42-abc123def456";

        let mock = server.mock(|when, then| {
            when.method(GET)
                .path(format!("/v2/orders/{}", order_id.replace("-", "")));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "id": order_id,
                    "client_order_id": "",
                    "symbol": "MSFT",
                    "asset_id": "c7ca82d4-3c95-4f89-9b42-abc123def456",
                    "asset_class": "us_equity",
                    "qty": "25",
                    "filled_qty": "0",
                    "side": "buy",
                    "order_class": "simple",
                    "type": "market",
                    "time_in_force": "day",
                    "status": "rejected",
                    "extended_hours": false,
                    "legs": [],
                    "created_at": "2030-01-15T09:30:00.000Z",
                    "updated_at": "2030-01-15T09:30:05.000Z",
                    "submitted_at": "2030-01-15T09:30:00.000Z",
                    "filled_at": null,
                    "expired_at": null,
                    "canceled_at": null,
                    "average_fill_price": null,
                    "limit_price": null,
                    "stop_price": null,
                    "trail_price": null,
                    "trail_percent": null
                }));
        });

        let client = create_test_client(&server);
        let result = get_order_status(&client, order_id).await;

        mock.assert();
        let order_update = result.unwrap();
        assert_eq!(order_update.order_id, order_id);
        assert_eq!(order_update.symbol.to_string(), "MSFT");
        assert_eq!(order_update.shares.value(), 25);
        assert_eq!(order_update.direction, Direction::Buy);
        assert_eq!(order_update.status, OrderStatus::Failed);
        assert_eq!(order_update.price_cents, None);
    }

    #[tokio::test]
    async fn test_get_order_status_partially_filled() {
        let server = MockServer::start();
        let order_id = "f9e8d7c6-b5a4-9382-7160-543210987654";

        let mock = server.mock(|when, then| {
            when.method(GET)
                .path(format!("/v2/orders/{}", order_id.replace("-", "")));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "id": order_id,
                    "client_order_id": "",
                    "symbol": "GOOGL",
                    "asset_id": "f9e8d7c6-b5a4-9382-7160-543210987654",
                    "asset_class": "us_equity",
                    "qty": "200",
                    "filled_qty": "75",
                    "side": "buy",
                    "order_class": "simple",
                    "type": "market",
                    "time_in_force": "day",
                    "status": "partially_filled",
                    "extended_hours": false,
                    "legs": [],
                    "created_at": "2030-01-15T09:30:00.000Z",
                    "updated_at": "2030-01-15T09:30:45.000Z",
                    "submitted_at": "2030-01-15T09:30:00.000Z",
                    "filled_at": null,
                    "expired_at": null,
                    "canceled_at": null,
                    "average_fill_price": null,
                    "limit_price": null,
                    "stop_price": null,
                    "trail_price": null,
                    "trail_percent": null
                }));
        });

        let client = create_test_client(&server);
        let result = get_order_status(&client, order_id).await;

        mock.assert();
        let order_update = result.unwrap();
        assert_eq!(order_update.order_id, order_id);
        assert_eq!(order_update.symbol.to_string(), "GOOGL");
        assert_eq!(order_update.shares.value(), 200);
        assert_eq!(order_update.direction, Direction::Buy);
        assert_eq!(order_update.status, OrderStatus::Submitted);
        assert_eq!(order_update.price_cents, None);
    }

    #[test]
    fn test_extract_shares_from_notional_amount_returns_error() {
        let quantity_amount = Amount::Quantity {
            quantity: 100.into(),
        };

        let result = extract_shares_from_amount(&quantity_amount);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().value(), 100);
    }

    #[tokio::test]
    async fn test_poll_pending_orders_multiple_orders() {
        let server = MockServer::start();

        let mock = server.mock(|when, then| {
            when.method(GET)
                .path("/v2/orders")
                .query_param("status", "open")
                .query_param("limit", "500");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([
                    {
                        "id": "904837e3-3b76-47ec-b432-046db621571b",
                        "client_order_id": "",
                        "symbol": "AAPL",
                        "asset_id": "904837e3-3b76-47ec-b432-046db621571b",
                        "asset_class": "us_equity",
                        "qty": "100",
                        "filled_qty": "0",
                        "side": "buy",
                        "order_class": "simple",
                        "type": "market",
                        "time_in_force": "day",
                        "status": "new",
                        "extended_hours": false,
                        "legs": [],
                        "created_at": "2030-01-15T09:30:00.000Z",
                        "updated_at": null,
                        "submitted_at": null,
                        "filled_at": null,
                        "expired_at": null,
                        "canceled_at": null,
                        "average_fill_price": null,
                        "limit_price": null,
                        "stop_price": null,
                        "trail_price": null,
                        "trail_percent": null
                    },
                    {
                        "id": "61e7b016-9c91-4a97-b912-615c9d365c9d",
                        "client_order_id": "",
                        "symbol": "TSLA",
                        "asset_id": "61e7b016-9c91-4a97-b912-615c9d365c9d",
                        "asset_class": "us_equity",
                        "qty": "50",
                        "filled_qty": "25",
                        "side": "sell",
                        "order_class": "simple",
                        "type": "market",
                        "time_in_force": "day",
                        "status": "partially_filled",
                        "extended_hours": false,
                        "legs": [],
                        "created_at": "2030-01-15T09:30:00.000Z",
                        "updated_at": "2030-01-15T09:30:30.000Z",
                        "submitted_at": "2030-01-15T09:30:00.000Z",
                        "filled_at": null,
                        "expired_at": null,
                        "canceled_at": null,
                        "average_fill_price": null,
                        "limit_price": null,
                        "stop_price": null,
                        "trail_price": null,
                        "trail_percent": null
                    }
                ]));
        });

        let client = create_test_client(&server);
        let result = poll_pending_orders(&client).await;

        mock.assert();
        let order_updates = result.unwrap();

        assert_eq!(order_updates.len(), 2);

        // Check first order (AAPL buy)
        let aapl_order = &order_updates[0];
        assert_eq!(aapl_order.order_id, "904837e3-3b76-47ec-b432-046db621571b");
        assert_eq!(aapl_order.symbol.to_string(), "AAPL");
        assert_eq!(aapl_order.shares.value(), 100);
        assert_eq!(aapl_order.direction, Direction::Buy);
        assert_eq!(aapl_order.status, OrderStatus::Submitted);
        assert_eq!(aapl_order.price_cents, None);

        // Check second order (TSLA sell)
        let tsla_order = &order_updates[1];
        assert_eq!(tsla_order.order_id, "61e7b016-9c91-4a97-b912-615c9d365c9d");
        assert_eq!(tsla_order.symbol.to_string(), "TSLA");
        assert_eq!(tsla_order.shares.value(), 50);
        assert_eq!(tsla_order.direction, Direction::Sell);
        assert_eq!(tsla_order.status, OrderStatus::Submitted); // partially_filled maps to Submitted
        assert_eq!(tsla_order.price_cents, None);
    }

    #[tokio::test]
    async fn test_poll_pending_orders_empty_result() {
        let server = MockServer::start();

        let mock = server.mock(|when, then| {
            when.method(GET)
                .path("/v2/orders")
                .query_param("status", "open")
                .query_param("limit", "500");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([]));
        });

        let client = create_test_client(&server);
        let result = poll_pending_orders(&client).await;

        mock.assert();
        let order_updates = result.unwrap();
        assert_eq!(order_updates.len(), 0);
    }

    #[tokio::test]
    async fn test_poll_pending_orders_with_filled_order() {
        let server = MockServer::start();

        let mock = server.mock(|when, then| {
            when.method(GET)
                .path("/v2/orders")
                .query_param("status", "open")
                .query_param("limit", "500");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([
                    {
                        "id": "c7ca82d4-3c95-4f89-9b42-abc123def456",
                        "client_order_id": "",
                        "symbol": "MSFT",
                        "asset_id": "c7ca82d4-3c95-4f89-9b42-abc123def456",
                        "asset_class": "us_equity",
                        "qty": "75",
                        "filled_qty": "75",
                        "side": "buy",
                        "order_class": "simple",
                        "type": "market",
                        "time_in_force": "day",
                        "status": "filled",
                        "extended_hours": false,
                        "legs": [],
                        "created_at": "2030-01-15T09:30:00.000Z",
                        "updated_at": "2030-01-15T09:31:00.000Z",
                        "submitted_at": "2030-01-15T09:30:00.000Z",
                        "filled_at": "2030-01-15T09:31:00.000Z",
                        "expired_at": null,
                        "canceled_at": null,
                        "filled_avg_price": "335.42",
                        "limit_price": null,
                        "stop_price": null,
                        "trail_price": null,
                        "trail_percent": null
                    }
                ]));
        });

        let client = create_test_client(&server);
        let result = poll_pending_orders(&client).await;

        mock.assert();
        let order_updates = result.unwrap();

        assert_eq!(order_updates.len(), 1);

        let filled_order = &order_updates[0];
        assert_eq!(
            filled_order.order_id,
            "c7ca82d4-3c95-4f89-9b42-abc123def456"
        );
        assert_eq!(filled_order.symbol.to_string(), "MSFT");
        assert_eq!(filled_order.shares.value(), 75);
        assert_eq!(filled_order.direction, Direction::Buy);
        assert_eq!(filled_order.status, OrderStatus::Filled);
        assert_eq!(filled_order.price_cents, Some(33542)); // $335.42 in cents
    }

    #[tokio::test]
    async fn test_poll_pending_orders_api_error() {
        let server = MockServer::start();

        let mock = server.mock(|when, then| {
            when.method(GET)
                .path("/v2/orders")
                .query_param("status", "open")
                .query_param("limit", "500");
            then.status(500)
                .header("content-type", "application/json")
                .json_body(json!({
                    "message": "Internal server error"
                }));
        });

        let client = create_test_client(&server);
        let result = poll_pending_orders(&client).await;

        mock.assert();
        let error = result.unwrap_err();
        assert!(matches!(error, BrokerError::AlpacaRequest(_)));
    }

    #[tokio::test]
    async fn test_poll_pending_orders_authentication_failure() {
        let server = MockServer::start();

        let mock = server.mock(|when, then| {
            when.method(GET)
                .path("/v2/orders")
                .query_param("status", "open")
                .query_param("limit", "500");
            then.status(401)
                .header("content-type", "application/json")
                .json_body(json!({
                    "code": 40110000,
                    "message": "Invalid credentials"
                }));
        });

        let client = create_test_client(&server);
        let result = poll_pending_orders(&client).await;

        mock.assert();
        let error = result.unwrap_err();
        assert!(matches!(error, BrokerError::AlpacaRequest(_)));
    }
}

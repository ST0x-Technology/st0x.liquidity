use apca::Client;
use apca::api::v2::order;
use chrono::Utc;
use num_decimal::Num;
use rain_math_float::Float;
use tracing::{debug, warn};
use uuid::Uuid;

use super::AlpacaTradingApiError;
use crate::{
    Direction, FractionalShares, MarketOrder, OrderPlacement, OrderStatus, OrderUpdate, Positive,
    Symbol,
};

pub(super) async fn place_market_order(
    client: &Client,
    market_order: MarketOrder,
) -> Result<OrderPlacement<String>, AlpacaTradingApiError> {
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

    // Alpaca supports max 9 decimal places; truncate to avoid rejection.
    let original = market_order.shares.inner().inner();
    let truncated_float =
        crate::truncate_to_decimal_places(original, crate::ALPACA_MAX_DECIMAL_PLACES)?.ok_or(
            AlpacaTradingApiError::BelowPrecision {
                shares: market_order.shares,
                max_decimals: crate::ALPACA_MAX_DECIMAL_PLACES,
            },
        )?;

    if !truncated_float.eq(original)? {
        warn!(
            original = %original.format_with_scientific(false)?,
            truncated = %truncated_float.format_with_scientific(false)?,
            "Truncated order quantity to {} decimal places for Alpaca",
            crate::ALPACA_MAX_DECIMAL_PLACES,
        );
    }

    let formatted = truncated_float.format_with_scientific(false)?;
    let quantity: Num = formatted
        .parse()
        .map_err(|source| AlpacaTradingApiError::NumConversion { formatted, source })?;

    let order_request = order_init.init(
        market_order.symbol.to_string(),
        alpaca_side,
        order::Amount::quantity(quantity),
    );

    let order_response = client.issue::<order::Create>(&order_request).await?;

    let order_id = order_response.id.to_string();

    let placed_shares = Positive::new(FractionalShares::new(truncated_float))?;

    Ok(OrderPlacement {
        order_id,
        symbol: market_order.symbol,
        shares: placed_shares,
        direction: market_order.direction,
        placed_at: chrono::Utc::now(),
    })
}

pub(super) async fn get_order_status(
    client: &Client,
    order_id: &str,
) -> Result<OrderUpdate<String>, AlpacaTradingApiError> {
    debug!("Querying Alpaca order status for order ID: {}", order_id);

    let order_uuid = Uuid::parse_str(order_id)?;

    let alpaca_order_id = order::Id(order_uuid);

    let order_response = client.issue::<order::Get>(&alpaca_order_id).await?;

    let symbol = Symbol::new(order_response.symbol.clone())?;

    let shares = extract_shares_from_amount(&order_response.amount)?;

    let direction = match order_response.side {
        order::Side::Buy => Direction::Buy,
        order::Side::Sell => Direction::Sell,
    };

    let status = map_alpaca_status_to_order_status(order_response.status);

    let price = extract_price(&order_response)?;

    Ok(OrderUpdate {
        order_id: order_id.to_string(),
        symbol,
        shares,
        direction,
        status,
        updated_at: Utc::now(),
        price,
    })
}

/// Maps Alpaca order status to our simplified `OrderStatus` enum.
///
/// Note: All Alpaca in-progress statuses map to
/// `OrderStatus::Submitted` because they represent orders that have
/// been submitted to and acknowledged by the broker.
/// `OrderStatus::Pending` is reserved for orders in our system that
/// haven't been sent to the broker yet (not applicable here since
/// we're mapping broker responses).
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

/// Extracts fill price as Float from Alpaca order
fn extract_price(order: &order::Order) -> Result<Option<Float>, AlpacaTradingApiError> {
    let Some(avg_fill_price) = &order.average_fill_price else {
        return Ok(None);
    };

    let price = Float::parse(format!("{avg_fill_price}"))?;
    Ok(Some(price))
}

/// Extracts shares from Alpaca Amount enum
fn extract_shares_from_amount(
    amount: &order::Amount,
) -> Result<Positive<FractionalShares>, AlpacaTradingApiError> {
    match amount {
        order::Amount::Quantity { quantity } => {
            let exact = Float::parse(quantity.to_string())?;
            Ok(Positive::new(FractionalShares::new(exact))?)
        }
        order::Amount::Notional { .. } => Err(AlpacaTradingApiError::NotionalOrdersNotSupported),
    }
}

#[cfg(test)]
mod tests {
    use apca::api::v2::order::Amount;
    use httpmock::prelude::*;
    use proptest::prelude::*;
    use serde_json::json;

    use super::*;
    use st0x_float_macro::float;

    fn option_float_eq(lhs: Option<Float>, rhs: Option<Float>) -> bool {
        match (lhs, rhs) {
            (Some(lhs), Some(rhs)) => lhs.eq(rhs).unwrap(),
            (None, None) => true,
            _ => false,
        }
    }

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
            shares: Positive::new(FractionalShares::new(float!(100))).unwrap(),
            direction: Direction::Buy,
        };

        let placement = place_market_order(&client, market_order).await.unwrap();
        mock.assert();
        assert_eq!(placement.order_id, "904837e3-3b76-47ec-b432-046db621571b");
        assert_eq!(placement.symbol.to_string(), "AAPL");
        assert_eq!(placement.shares.inner(), FractionalShares::new(float!(100)));
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
            shares: Positive::new(FractionalShares::new(float!(50))).unwrap(),
            direction: Direction::Sell,
        };

        let placement = place_market_order(&client, market_order).await.unwrap();
        mock.assert();
        assert_eq!(placement.order_id, "61e7b016-9c91-4a97-b912-615c9d365c9d");
        assert_eq!(placement.symbol.to_string(), "TSLA");
        assert_eq!(placement.shares.inner(), FractionalShares::new(float!(50)));
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
                    "code": 40_010_001,
                    "message": "symbol INVALID is not supported"
                }));
        });

        let client = create_test_client(&server);
        let market_order = MarketOrder {
            symbol: Symbol::new("INVALID".to_string()).unwrap(),
            shares: Positive::new(FractionalShares::new(float!(10))).unwrap(),
            direction: Direction::Buy,
        };

        let error = place_market_order(&client, market_order).await.unwrap_err();
        mock.assert();
        assert!(matches!(error, AlpacaTradingApiError::OrderCreate(_)));
    }

    #[tokio::test]
    async fn test_place_market_order_authentication_failure() {
        let server = MockServer::start();

        let mock = server.mock(|when, then| {
            when.method(POST).path("/v2/orders");
            then.status(401)
                .header("content-type", "application/json")
                .json_body(json!({
                    "code": 40_110_000,
                    "message": "Invalid credentials"
                }));
        });

        let client = create_test_client(&server);
        let market_order = MarketOrder {
            symbol: Symbol::new("AAPL".to_string()).unwrap(),
            shares: Positive::new(FractionalShares::new(float!(100))).unwrap(),
            direction: Direction::Buy,
        };

        let error = place_market_order(&client, market_order).await.unwrap_err();
        mock.assert();
        assert!(matches!(error, AlpacaTradingApiError::OrderCreate(_)));
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
            shares: Positive::new(FractionalShares::new(float!(25))).unwrap(),
            direction: Direction::Buy,
        };

        let error = place_market_order(&client, market_order).await.unwrap_err();
        mock.assert();
        assert!(matches!(error, AlpacaTradingApiError::OrderCreate(_)));
    }

    #[tokio::test]
    async fn test_get_order_status_pending_order() {
        let server = MockServer::start();
        let order_id = "904837e3-3b76-47ec-b432-046db621571b";

        let mock = server.mock(|when, then| {
            when.method(GET)
                .path(format!("/v2/orders/{}", order_id.replace('-', "")));
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
        let order_update = get_order_status(&client, order_id).await.unwrap();

        mock.assert();
        assert_eq!(order_update.order_id, order_id);
        assert_eq!(order_update.symbol.to_string(), "AAPL");
        assert_eq!(
            order_update.shares.inner(),
            FractionalShares::new(float!(100))
        );
        assert_eq!(order_update.direction, Direction::Buy);
        assert_eq!(order_update.status, OrderStatus::Submitted);
        assert!(option_float_eq(order_update.price, None));
    }

    #[tokio::test]
    async fn test_get_order_status_filled_order() {
        let server = MockServer::start();
        let order_id = "61e7b016-9c91-4a97-b912-615c9d365c9d";

        let mock = server.mock(|when, then| {
            when.method(GET)
                .path(format!("/v2/orders/{}", order_id.replace('-', "")));
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
        let order_update = get_order_status(&client, order_id).await.unwrap();

        mock.assert();
        assert_eq!(order_update.order_id, order_id);
        assert_eq!(order_update.symbol.to_string(), "TSLA");
        assert_eq!(
            order_update.shares.inner(),
            FractionalShares::new(float!(50))
        );
        assert_eq!(order_update.direction, Direction::Sell);
        assert_eq!(order_update.status, OrderStatus::Filled);
        assert!(option_float_eq(
            order_update.price,
            Some(Float::parse("245.67".to_string()).unwrap())
        ));
    }

    #[tokio::test]
    async fn test_get_order_status_rejected_order() {
        let server = MockServer::start();
        let order_id = "c7ca82d4-3c95-4f89-9b42-abc123def456";

        let mock = server.mock(|when, then| {
            when.method(GET)
                .path(format!("/v2/orders/{}", order_id.replace('-', "")));
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
        let order_update = get_order_status(&client, order_id).await.unwrap();

        mock.assert();
        assert_eq!(order_update.order_id, order_id);
        assert_eq!(order_update.symbol.to_string(), "MSFT");
        assert_eq!(
            order_update.shares.inner(),
            FractionalShares::new(float!(25))
        );
        assert_eq!(order_update.direction, Direction::Buy);
        assert_eq!(order_update.status, OrderStatus::Failed);
        assert!(option_float_eq(order_update.price, None));
    }

    #[tokio::test]
    async fn test_get_order_status_partially_filled() {
        let server = MockServer::start();
        let order_id = "f9e8d7c6-b5a4-9382-7160-543210987654";

        let mock = server.mock(|when, then| {
            when.method(GET)
                .path(format!("/v2/orders/{}", order_id.replace('-', "")));
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
        let order_update = get_order_status(&client, order_id).await.unwrap();

        mock.assert();
        assert_eq!(order_update.order_id, order_id);
        assert_eq!(order_update.symbol.to_string(), "GOOGL");
        assert_eq!(
            order_update.shares.inner(),
            FractionalShares::new(float!(200))
        );
        assert_eq!(order_update.direction, Direction::Buy);
        assert_eq!(order_update.status, OrderStatus::Submitted);
        assert!(option_float_eq(order_update.price, None));
    }

    #[test]
    fn test_extract_shares_from_notional_amount_returns_error() {
        let quantity_amount = Amount::Quantity {
            quantity: 100.into(),
        };

        let shares = extract_shares_from_amount(&quantity_amount).unwrap();
        assert_eq!(shares.inner(), FractionalShares::new(float!(100)));
    }

    proptest! {
        #[test]
        fn float_to_num_via_string_roundtrips(
            integer in 0u64..1_000_000,
            fractional in 0u64..1_000_000,
        ) {
            let value_str = format!("{integer}.{fractional:06}");
            if let Ok(exact) = Float::parse(value_str)
                && let Ok(formatted) = exact.format_with_scientific(false)
                && let Ok(num) = formatted.parse::<Num>()
            {
                let num_str = num.to_string();
                if let Ok(roundtripped) = Float::parse(num_str) {
                    prop_assert!(exact.eq(roundtripped).unwrap());
                }
            }
        }
    }

    #[tokio::test]
    async fn tiny_shares_below_precision_returns_error() {
        let server = MockServer::start();
        let client = create_test_client(&server);

        let tiny = Float::parse("0.0000000001".to_string()).unwrap();
        let market_order = MarketOrder {
            symbol: Symbol::new("AAPL").unwrap(),
            shares: Positive::new(FractionalShares::new(tiny)).unwrap(),
            direction: Direction::Buy,
        };

        let err = place_market_order(&client, market_order).await.unwrap_err();

        assert!(
            matches!(
                err,
                AlpacaTradingApiError::BelowPrecision {
                    max_decimals,
                    ..
                } if max_decimals == crate::ALPACA_MAX_DECIMAL_PLACES
            ),
            "Expected BelowPrecision error, got: {err:?}"
        );
    }
}

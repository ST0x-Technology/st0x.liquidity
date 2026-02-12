use chrono::{DateTime, Utc};
use num_traits::ToPrimitive;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use tracing::debug;
use uuid::Uuid;

use super::AlpacaBrokerApiError;
use super::client::AlpacaBrokerApiClient;
use crate::order::OrderUpdate;
use crate::{
    Direction, FractionalShares, MarketOrder, OrderPlacement, OrderStatus, Positive, Symbol,
};

/// Order side
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OrderSide {
    Buy,
    Sell,
}

/// Order status from Alpaca Broker API
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum BrokerOrderStatus {
    New,
    PendingNew,
    PartiallyFilled,
    Filled,
    DoneForDay,
    Canceled,
    Expired,
    Replaced,
    PendingCancel,
    PendingReplace,
    Rejected,
    Suspended,
    Calculated,
    Stopped,
    AcceptedForBidding,
    Accepted,
}

/// Direction for USDC/USD conversion
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConversionDirection {
    /// Convert USDC to USD buying power (sell USDC/USD)
    UsdcToUsd,
    /// Convert USD buying power to USDC (buy USDC/USD)
    UsdToUsdc,
}

/// Order request for placing market orders
#[derive(Debug, Serialize)]
pub(super) struct OrderRequest {
    #[serde(serialize_with = "serialize_symbol")]
    pub symbol: Symbol,
    #[serde(rename = "qty", serialize_with = "serialize_positive_shares")]
    pub quantity: Positive<FractionalShares>,
    pub side: OrderSide,
    #[serde(rename = "type")]
    pub order_type: &'static str,
    pub time_in_force: &'static str,
    pub extended_hours: bool,
}

fn serialize_symbol<S>(symbol: &Symbol, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    serializer.serialize_str(&symbol.to_string())
}

// serde's serialize_with requires the field to be passed by reference
#[allow(clippy::trivially_copy_pass_by_ref)]
fn serialize_positive_shares<S>(
    shares: &Positive<FractionalShares>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    serializer.serialize_str(&shares.inner().inner().to_string())
}

/// Order response from the Alpaca Broker API
#[derive(Debug, Deserialize)]
pub(super) struct OrderResponse {
    pub id: Uuid,
    pub symbol: Symbol,
    #[serde(
        rename = "qty",
        deserialize_with = "deserialize_positive_shares_from_string"
    )]
    pub quantity: Positive<FractionalShares>,
    #[serde(
        rename = "filled_qty",
        default,
        deserialize_with = "deserialize_optional_decimal"
    )]
    pub filled_quantity: Option<Decimal>,
    pub side: OrderSide,
    pub status: BrokerOrderStatus,
    #[serde(
        rename = "filled_avg_price",
        default,
        deserialize_with = "deserialize_optional_price"
    )]
    pub filled_average_price: Option<f64>,
}

/// Order request for crypto trading (e.g., USDC/USD conversion).
/// Uses decimal quantity and trading pair symbol format.
#[derive(Debug, Serialize)]
pub(crate) struct CryptoOrderRequest {
    /// Trading pair symbol (e.g., "USDCUSD" for USDC/USD)
    pub symbol: String,
    /// Quantity of the base asset (e.g., USDC amount)
    #[serde(rename = "qty")]
    pub quantity: Decimal,
    pub side: OrderSide,
    #[serde(rename = "type")]
    pub order_type: &'static str,
    pub time_in_force: &'static str,
}

/// Response from a crypto order placement
#[derive(Debug, Clone, Deserialize)]
pub struct CryptoOrderResponse {
    pub id: Uuid,
    pub symbol: String,
    #[serde(rename = "qty", deserialize_with = "deserialize_decimal_from_string")]
    pub quantity: Decimal,
    status: BrokerOrderStatus,
    #[serde(
        rename = "filled_avg_price",
        default,
        deserialize_with = "deserialize_optional_price"
    )]
    pub filled_average_price: Option<f64>,
    #[serde(
        rename = "filled_qty",
        default,
        deserialize_with = "deserialize_optional_decimal"
    )]
    pub filled_quantity: Option<Decimal>,
    pub created_at: DateTime<Utc>,
}

impl CryptoOrderResponse {
    /// Returns the status as a display-friendly string.
    pub fn status_display(&self) -> &'static str {
        match self.status {
            BrokerOrderStatus::Filled => "filled",
            BrokerOrderStatus::New => "new",
            BrokerOrderStatus::PendingNew => "pending_new",
            BrokerOrderStatus::PartiallyFilled => "partially_filled",
            BrokerOrderStatus::Canceled => "canceled",
            BrokerOrderStatus::Expired => "expired",
            BrokerOrderStatus::Rejected => "rejected",
            BrokerOrderStatus::Accepted => "accepted",
            _ => "other",
        }
    }
}

fn deserialize_decimal_from_string<'de, D>(deserializer: D) -> Result<Decimal, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    s.parse::<Decimal>().map_err(serde::de::Error::custom)
}

fn deserialize_positive_shares_from_string<'de, D>(
    deserializer: D,
) -> Result<Positive<FractionalShares>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    let value: Decimal = s.parse().map_err(serde::de::Error::custom)?;
    Positive::new(FractionalShares::new(value)).map_err(serde::de::Error::custom)
}

fn deserialize_optional_price<'de, D>(deserializer: D) -> Result<Option<f64>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let opt: Option<String> = Option::deserialize(deserializer)?;
    opt.map_or(Ok(None), |s| {
        s.parse::<f64>().map(Some).map_err(serde::de::Error::custom)
    })
}

fn deserialize_optional_decimal<'de, D>(deserializer: D) -> Result<Option<Decimal>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let opt: Option<String> = Option::deserialize(deserializer)?;
    opt.map_or(Ok(None), |s| {
        s.parse::<Decimal>()
            .map(Some)
            .map_err(serde::de::Error::custom)
    })
}

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
        // Alpaca only allows extended_hours=true for limit orders, not market orders
        extended_hours: false,
    };

    let response = client.place_order(&request).await?;

    if response.side != side {
        return Err(AlpacaBrokerApiError::SideMismatch {
            requested_side: side,
            response_side: response.side,
        });
    }

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
) -> Result<OrderUpdate, AlpacaBrokerApiError> {
    debug!(
        "Querying Alpaca Broker API order status for order ID: {}",
        order_id
    );

    let order_uuid = Uuid::parse_str(order_id)?;
    let response = client.get_order(order_uuid).await?;

    let status = map_broker_status_to_order_status(response.status);
    let price_cents = convert_price_to_cents(response.filled_average_price)?;

    if response.status == BrokerOrderStatus::PartiallyFilled {
        debug!(
            order_id,
            symbol = %response.symbol,
            ordered_qty = %response.quantity.inner(),
            filled_qty = ?response.filled_quantity,
            "Order is partially filled"
        );
    }

    Ok(OrderUpdate {
        status,
        updated_at: Utc::now(),
        price_cents,
    })
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

/// Convert USDC to/from USD on Alpaca.
///
/// This uses the USDC/USD trading pair:
/// - To convert USDC to USD buying power: sell USDC/USD
/// - To convert USD buying power to USDC: buy USDC/USD
pub(crate) async fn convert_usdc_usd(
    client: &AlpacaBrokerApiClient,
    amount: Decimal,
    direction: ConversionDirection,
) -> Result<CryptoOrderResponse, AlpacaBrokerApiError> {
    let side = match direction {
        ConversionDirection::UsdcToUsd => OrderSide::Sell,
        ConversionDirection::UsdToUsdc => OrderSide::Buy,
    };

    debug!(
        "Placing USDC/USD conversion order: {} {} USDC",
        if side == OrderSide::Sell {
            "sell"
        } else {
            "buy"
        },
        amount
    );

    let request = CryptoOrderRequest {
        symbol: "USDCUSD".to_string(),
        quantity: amount,
        side,
        order_type: "market",
        time_in_force: "gtc",
    };

    client.place_crypto_order(&request).await
}

/// Poll for a crypto order's status until it reaches a terminal state.
pub(crate) async fn poll_crypto_order_until_filled(
    client: &AlpacaBrokerApiClient,
    order_id: Uuid,
) -> Result<CryptoOrderResponse, AlpacaBrokerApiError> {
    loop {
        let order = client.get_crypto_order(order_id).await?;

        match order.status {
            BrokerOrderStatus::Filled => return Ok(order),
            BrokerOrderStatus::Canceled => {
                return Err(AlpacaBrokerApiError::CryptoOrderFailed {
                    order_id,
                    reason: super::CryptoOrderFailureReason::Canceled,
                });
            }
            BrokerOrderStatus::Expired => {
                return Err(AlpacaBrokerApiError::CryptoOrderFailed {
                    order_id,
                    reason: super::CryptoOrderFailureReason::Expired,
                });
            }
            BrokerOrderStatus::Rejected => {
                return Err(AlpacaBrokerApiError::CryptoOrderFailed {
                    order_id,
                    reason: super::CryptoOrderFailureReason::Rejected,
                });
            }
            _ => {
                debug!(
                    order_id = %order_id,
                    status = ?order.status,
                    "Crypto order still pending, waiting..."
                );
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use httpmock::prelude::*;
    use serde_json::json;

    use super::*;
    use crate::alpaca_broker_api::auth::{AlpacaBrokerApiCtx, AlpacaBrokerApiMode};

    fn create_test_ctx(mode: AlpacaBrokerApiMode) -> AlpacaBrokerApiCtx {
        AlpacaBrokerApiCtx {
            api_key: "test_key".to_string(),
            api_secret: "test_secret".to_string(),
            account_id: "test_account_123".to_string(),
            mode: Some(mode),
        }
    }

    #[tokio::test]
    async fn test_place_market_order_buy_success() {
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));

        let mock = server.mock(|when, then| {
            when.method(POST)
                .path("/v1/trading/accounts/test_account_123/orders")
                .json_body(json!({
                    "symbol": "AAPL",
                    "qty": "100",
                    "side": "buy",
                    "type": "market",
                    "time_in_force": "day",
                    "extended_hours": false
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

        let client = AlpacaBrokerApiClient::new(&ctx).unwrap();
        let market_order = MarketOrder {
            symbol: Symbol::new("AAPL").unwrap(),
            shares: Positive::new(FractionalShares::new(Decimal::from(100))).unwrap(),
            direction: Direction::Buy,
        };

        let placement = place_market_order(&client, market_order).await.unwrap();

        mock.assert();
        assert_eq!(placement.order_id, "904837e3-3b76-47ec-b432-046db621571b");
        assert_eq!(placement.symbol.to_string(), "AAPL");
        assert_eq!(placement.shares.inner().inner(), Decimal::from(100));
        assert_eq!(placement.direction, Direction::Buy);
    }

    #[tokio::test]
    async fn test_place_market_order_sell_success() {
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));

        let mock = server.mock(|when, then| {
            when.method(POST)
                .path("/v1/trading/accounts/test_account_123/orders")
                .json_body(json!({
                    "symbol": "TSLA",
                    "qty": "50",
                    "side": "sell",
                    "type": "market",
                    "time_in_force": "day",
                    "extended_hours": false
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

        let client = AlpacaBrokerApiClient::new(&ctx).unwrap();
        let market_order = MarketOrder {
            symbol: Symbol::new("TSLA").unwrap(),
            shares: Positive::new(FractionalShares::new(Decimal::from(50))).unwrap(),
            direction: Direction::Sell,
        };

        let placement = place_market_order(&client, market_order).await.unwrap();

        mock.assert();
        assert_eq!(placement.order_id, "61e7b016-9c91-4a97-b912-615c9d365c9d");
        assert_eq!(placement.symbol.to_string(), "TSLA");
        assert_eq!(placement.shares.inner().inner(), Decimal::from(50));
        assert_eq!(placement.direction, Direction::Sell);
    }

    #[tokio::test]
    async fn test_get_order_status_pending() {
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));
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

        let client = AlpacaBrokerApiClient::new(&ctx).unwrap();
        let order_update = get_order_status(&client, order_id).await.unwrap();

        mock.assert();
        assert_eq!(order_update.status, OrderStatus::Submitted);
        assert_eq!(order_update.price_cents, None);
    }

    #[tokio::test]
    async fn test_get_order_status_filled() {
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));
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

        let client = AlpacaBrokerApiClient::new(&ctx).unwrap();
        let order_update = get_order_status(&client, order_id).await.unwrap();

        mock.assert();
        assert_eq!(order_update.status, OrderStatus::Filled);
        assert_eq!(order_update.price_cents, Some(24567));
    }

    #[tokio::test]
    async fn test_get_order_status_rejected() {
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));
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

        let client = AlpacaBrokerApiClient::new(&ctx).unwrap();
        let order_update = get_order_status(&client, order_id).await.unwrap();

        mock.assert();
        assert_eq!(order_update.status, OrderStatus::Failed);
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

    #[tokio::test]
    async fn test_convert_usdc_to_usd() {
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));

        let mock = server.mock(|when, then| {
            when.method(POST)
                .path("/v1/trading/accounts/test_account_123/orders")
                .json_body(json!({
                    "symbol": "USDCUSD",
                    "qty": "1000.50",
                    "side": "sell",
                    "type": "market",
                    "time_in_force": "gtc"
                }));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "id": "904837e3-3b76-47ec-b432-046db621571b",
                    "symbol": "USDCUSD",
                    "qty": "1000.50",
                    "side": "sell",
                    "status": "filled",
                    "filled_avg_price": "1.0001",
                    "filled_qty": "1000.50",
                    "created_at": "2025-01-06T12:00:00Z"
                }));
        });

        let client = AlpacaBrokerApiClient::new(&ctx).unwrap();
        let amount = Decimal::from_str("1000.50").unwrap();

        let order = convert_usdc_usd(&client, amount, ConversionDirection::UsdcToUsd)
            .await
            .unwrap();

        mock.assert();
        assert_eq!(order.id.to_string(), "904837e3-3b76-47ec-b432-046db621571b");
        assert_eq!(order.symbol, "USDCUSD");
        assert_eq!(order.quantity, Decimal::from_str("1000.50").unwrap());
        assert_eq!(order.status_display(), "filled");
    }

    #[tokio::test]
    async fn test_convert_usd_to_usdc() {
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));

        let mock = server.mock(|when, then| {
            when.method(POST)
                .path("/v1/trading/accounts/test_account_123/orders")
                .json_body(json!({
                    "symbol": "USDCUSD",
                    "qty": "500",
                    "side": "buy",
                    "type": "market",
                    "time_in_force": "gtc"
                }));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "id": "61e7b016-9c91-4a97-b912-615c9d365c9d",
                    "symbol": "USDCUSD",
                    "qty": "500",
                    "side": "buy",
                    "status": "filled",
                    "filled_avg_price": "0.9999",
                    "filled_qty": "500",
                    "created_at": "2025-01-06T12:30:00Z"
                }));
        });

        let client = AlpacaBrokerApiClient::new(&ctx).unwrap();
        let amount = Decimal::from_str("500").unwrap();

        let order = convert_usdc_usd(&client, amount, ConversionDirection::UsdToUsdc)
            .await
            .unwrap();

        mock.assert();
        assert_eq!(order.id.to_string(), "61e7b016-9c91-4a97-b912-615c9d365c9d");
        assert_eq!(order.symbol, "USDCUSD");
        assert_eq!(order.quantity, Decimal::from_str("500").unwrap());
        assert_eq!(order.status_display(), "filled");
    }

    #[test]
    fn test_crypto_order_response_status_display() {
        let make_order = |status: BrokerOrderStatus| CryptoOrderResponse {
            id: Uuid::new_v4(),
            symbol: "USDCUSD".to_string(),
            quantity: Decimal::from(100),
            status,
            filled_average_price: None,
            filled_quantity: None,
            created_at: Utc::now(),
        };

        assert_eq!(
            make_order(BrokerOrderStatus::Filled).status_display(),
            "filled"
        );
        assert_eq!(make_order(BrokerOrderStatus::New).status_display(), "new");
        assert_eq!(
            make_order(BrokerOrderStatus::Rejected).status_display(),
            "rejected"
        );
        assert_eq!(
            make_order(BrokerOrderStatus::Canceled).status_display(),
            "canceled"
        );
    }

    #[tokio::test]
    async fn test_place_market_order_side_mismatch() {
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));

        let mock = server.mock(|when, then| {
            when.method(POST)
                .path("/v1/trading/accounts/test_account_123/orders")
                .json_body(json!({
                    "symbol": "AAPL",
                    "qty": "100",
                    "side": "buy",
                    "type": "market",
                    "time_in_force": "day",
                    "extended_hours": false
                }));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "id": "904837e3-3b76-47ec-b432-046db621571b",
                    "symbol": "AAPL",
                    "qty": "100",
                    "side": "sell",
                    "status": "new",
                    "filled_avg_price": null
                }));
        });

        let client = AlpacaBrokerApiClient::new(&ctx).unwrap();
        let market_order = MarketOrder {
            symbol: Symbol::new("AAPL").unwrap(),
            shares: Positive::new(FractionalShares::new(Decimal::from(100))).unwrap(),
            direction: Direction::Buy,
        };

        let result = place_market_order(&client, market_order).await;

        mock.assert();
        assert!(matches!(
            result.unwrap_err(),
            AlpacaBrokerApiError::SideMismatch {
                requested_side: OrderSide::Buy,
                response_side: OrderSide::Sell
            }
        ));
    }
}

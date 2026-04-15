use chrono::{DateTime, Utc};
use rain_math_float::Float;
use serde::{Deserialize, Serialize};
use st0x_float_macro::float;
use std::str::FromStr;
use tracing::{debug, warn};
use uuid::Uuid;

use super::client::AlpacaBrokerApiClient;
use super::{AlpacaBrokerApiError, TimeInForce};
use crate::{
    Direction, FractionalShares, MarketOrder, OrderPlacement, OrderStatus, OrderUpdate, Positive,
    Symbol, Usd, deserialize_float_from_number_or_string,
    deserialize_option_float_from_number_or_string, serialize_float_as_string,
};

const ALPACA_CRYPTO_MAX_DECIMAL_PLACES: u8 = 6;

/// Order side
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub(super) enum OrderSide {
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

#[derive(Debug, Clone)]
pub struct AlpacaLimitOrder {
    pub symbol: Symbol,
    pub shares: Positive<FractionalShares>,
    pub direction: Direction,
    pub limit_price: AlpacaLimitPrice,
    pub extended_hours: bool,
}

#[derive(Debug, Clone, Serialize)]
#[serde(transparent)]
pub struct AlpacaLimitPrice(Positive<Usd>);

#[derive(Debug, thiserror::Error)]
pub enum ParseAlpacaLimitPriceError {
    #[error(transparent)]
    Float(#[from] rain_math_float::FloatError),

    #[error("limit price must be positive")]
    NotPositive,

    #[error(transparent)]
    Validation(#[from] AlpacaBrokerApiError),
}

impl AlpacaLimitPrice {
    pub fn try_new(limit_price: Positive<Usd>) -> Result<Self, AlpacaBrokerApiError> {
        validate_limit_price_precision(limit_price)?;
        Ok(Self(limit_price))
    }

    pub fn as_price(&self) -> &Positive<Usd> {
        &self.0
    }

    pub fn into_inner(self) -> Positive<Usd> {
        self.0
    }
}

impl FromStr for AlpacaLimitPrice {
    type Err = ParseAlpacaLimitPriceError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let price = value.parse::<Usd>()?;
        let positive_price = Positive::new(price).map_err(|_| Self::Err::NotPositive)?;
        Self::try_new(positive_price).map_err(Self::Err::from)
    }
}

/// Order request for placing market orders.
///
/// The `quantity` field must already be truncated to Alpaca's decimal precision
/// before constructing this struct.
#[derive(Debug, Serialize)]
pub(super) struct OrderRequest {
    #[serde(serialize_with = "serialize_symbol")]
    pub symbol: Symbol,
    #[serde(rename = "qty", serialize_with = "serialize_shares_as_string")]
    pub quantity: Positive<FractionalShares>,
    pub side: OrderSide,
    #[serde(rename = "type")]
    pub order_type: &'static str,
    pub time_in_force: &'static str,
    pub extended_hours: bool,
}

/// Order request for placing limit orders.
///
/// The `quantity` field must already be truncated to Alpaca's decimal precision
/// before constructing this struct.
#[derive(Debug, Serialize)]
pub(super) struct LimitOrderRequest {
    #[serde(serialize_with = "serialize_symbol")]
    pub symbol: Symbol,
    #[serde(rename = "qty", serialize_with = "serialize_shares_as_string")]
    pub quantity: Positive<FractionalShares>,
    pub side: OrderSide,
    #[serde(rename = "type")]
    pub order_type: &'static str,
    pub limit_price: AlpacaLimitPrice,
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
fn serialize_shares_as_string<S>(
    shares: &Positive<FractionalShares>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    let formatted = shares
        .inner()
        .inner()
        .format_with_scientific(false)
        .map_err(serde::ser::Error::custom)?;
    serializer.serialize_str(&formatted)
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
        deserialize_with = "deserialize_option_float_from_number_or_string"
    )]
    pub filled_quantity: Option<Float>,
    pub side: OrderSide,
    pub status: BrokerOrderStatus,
    #[serde(
        rename = "filled_avg_price",
        default,
        deserialize_with = "deserialize_option_float_from_number_or_string"
    )]
    pub filled_average_price: Option<Float>,
}

/// Order request for crypto trading (e.g., USDC/USD conversion).
/// Uses decimal quantity and trading pair symbol format.
#[derive(Debug, Serialize)]
pub(crate) struct CryptoOrderRequest {
    /// Trading pair symbol (e.g., "USDCUSD" for USDC/USD)
    pub symbol: String,
    /// Quantity of the base asset (e.g., USDC amount)
    #[serde(rename = "qty", serialize_with = "serialize_float_as_string")]
    pub quantity: Float,
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
    #[serde(
        rename = "qty",
        deserialize_with = "deserialize_float_from_number_or_string"
    )]
    pub quantity: Float,
    status: BrokerOrderStatus,
    #[serde(
        rename = "filled_avg_price",
        default,
        deserialize_with = "deserialize_option_float_from_number_or_string"
    )]
    pub filled_average_price: Option<Float>,
    #[serde(
        rename = "filled_qty",
        default,
        deserialize_with = "deserialize_option_float_from_number_or_string"
    )]
    pub filled_quantity: Option<Float>,
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

fn deserialize_positive_shares_from_string<'de, D>(
    deserializer: D,
) -> Result<Positive<FractionalShares>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let exact = deserialize_float_from_number_or_string(deserializer)?;
    Positive::new(FractionalShares::new(exact)).map_err(serde::de::Error::custom)
}

fn validate_limit_price_precision(limit_price: Positive<Usd>) -> Result<(), AlpacaBrokerApiError> {
    let max_decimals = if limit_price.inner().inner().lt(float!(1))? {
        4
    } else {
        2
    };

    let (_, lossless) = limit_price
        .inner()
        .inner()
        .to_fixed_decimal_lossy(max_decimals)?;

    if !lossless {
        return Err(AlpacaBrokerApiError::InvalidLimitPricePrecision {
            limit_price,
            max_decimals,
        });
    }

    Ok(())
}

pub(super) async fn place_market_order(
    client: &AlpacaBrokerApiClient,
    market_order: MarketOrder,
    time_in_force: TimeInForce,
) -> Result<OrderPlacement<String>, AlpacaBrokerApiError> {
    debug!(
        "Placing Alpaca Broker API market order: {} {} shares of {} (time_in_force: {:?})",
        market_order.direction, market_order.shares, market_order.symbol, time_in_force
    );

    let placed_shares = truncate_shares_to_alpaca_precision(market_order.shares)?;

    let side = match market_order.direction {
        Direction::Buy => OrderSide::Buy,
        Direction::Sell => OrderSide::Sell,
    };

    let request = OrderRequest {
        symbol: market_order.symbol.clone(),
        quantity: placed_shares,
        side,
        order_type: "market",
        time_in_force: time_in_force.as_api_str(),
        // Alpaca only allows extended_hours=true for limit orders, not market orders
        extended_hours: false,
    };

    let response = client.place_order(&request).await?;

    Ok(OrderPlacement {
        order_id: response.id.to_string(),
        symbol: market_order.symbol,
        shares: placed_shares,
        direction: market_order.direction,
        placed_at: Utc::now(),
    })
}

pub(super) async fn place_limit_order(
    client: &AlpacaBrokerApiClient,
    limit_order: AlpacaLimitOrder,
) -> Result<OrderPlacement<String>, AlpacaBrokerApiError> {
    debug!(
        direction = ?limit_order.direction,
        shares = %limit_order.shares,
        symbol = %limit_order.symbol,
        limit_price = ?limit_order.limit_price,
        extended_hours = limit_order.extended_hours,
        "Placing Alpaca Broker API limit order"
    );

    let placed_shares = truncate_shares_to_alpaca_precision(limit_order.shares)?;

    let side = match limit_order.direction {
        Direction::Buy => OrderSide::Buy,
        Direction::Sell => OrderSide::Sell,
    };

    let request = LimitOrderRequest {
        symbol: limit_order.symbol.clone(),
        quantity: placed_shares,
        side,
        order_type: "limit",
        limit_price: limit_order.limit_price.clone(),
        time_in_force: TimeInForce::Day.as_api_str(),
        extended_hours: limit_order.extended_hours,
    };

    let response = client.place_limit_order(&request).await?;

    Ok(OrderPlacement {
        order_id: response.id.to_string(),
        symbol: limit_order.symbol,
        shares: placed_shares,
        direction: limit_order.direction,
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
    let price = response.filled_average_price;

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
        order_id: order_id.to_string(),
        symbol: response.symbol,
        shares: response.quantity,
        direction,
        status,
        updated_at: Utc::now(),
        price,
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

fn truncate_shares_to_alpaca_precision(
    shares: Positive<FractionalShares>,
) -> Result<Positive<FractionalShares>, AlpacaBrokerApiError> {
    let original = shares.inner().inner();
    let truncated_float =
        crate::truncate_to_decimal_places(original, crate::ALPACA_MAX_DECIMAL_PLACES)?.ok_or(
            AlpacaBrokerApiError::BelowPrecision {
                shares,
                max_decimals: crate::ALPACA_MAX_DECIMAL_PLACES,
            },
        )?;

    if !truncated_float.eq(original)? {
        warn!(
            original = %shares,
            truncated = %FractionalShares::new(truncated_float),
            "Truncated order quantity to {} decimal places for Alpaca",
            crate::ALPACA_MAX_DECIMAL_PLACES,
        );
    }

    Ok(Positive::new(FractionalShares::new(truncated_float))?)
}

fn validate_usdc_amount_for_alpaca_precision(amount: Float) -> Result<Float, AlpacaBrokerApiError> {
    let truncated_amount =
        crate::truncate_to_decimal_places(amount, ALPACA_CRYPTO_MAX_DECIMAL_PLACES)?.ok_or(
            AlpacaBrokerApiError::UsdcBelowPrecision {
                amount,
                max_decimals: ALPACA_CRYPTO_MAX_DECIMAL_PLACES,
            },
        )?;

    if !truncated_amount.eq(amount)? {
        return Err(AlpacaBrokerApiError::UsdcPrecisionExceeded {
            amount,
            max_decimals: ALPACA_CRYPTO_MAX_DECIMAL_PLACES,
        });
    }

    Ok(amount)
}

/// Convert USDC to/from USD on Alpaca.
///
/// This uses the USDC/USD trading pair:
/// - To convert USDC to USD buying power: sell USDC/USD
/// - To convert USD buying power to USDC: buy USDC/USD
pub(crate) async fn convert_usdc_usd(
    client: &AlpacaBrokerApiClient,
    amount: Float,
    direction: ConversionDirection,
) -> Result<CryptoOrderResponse, AlpacaBrokerApiError> {
    let placed_amount = validate_usdc_amount_for_alpaca_precision(amount)?;
    let side = match direction {
        ConversionDirection::UsdcToUsd => OrderSide::Sell,
        ConversionDirection::UsdToUsdc => OrderSide::Buy,
    };

    debug!(?side, amount = ?placed_amount, "Placing USDC/USD conversion order");

    let request = CryptoOrderRequest {
        symbol: "USDCUSD".to_string(),
        quantity: placed_amount,
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
    use httpmock::prelude::*;
    use serde_json::json;

    use super::*;
    use crate::alpaca_broker_api::auth::{
        AlpacaAccountId, AlpacaBrokerApiCtx, AlpacaBrokerApiMode,
    };
    use st0x_float_macro::float;

    const TEST_ACCOUNT_ID: AlpacaAccountId =
        AlpacaAccountId::new(uuid::uuid!("904837e3-3b76-47ec-b432-046db621571b"));

    fn create_test_ctx(mode: AlpacaBrokerApiMode) -> AlpacaBrokerApiCtx {
        AlpacaBrokerApiCtx {
            api_key: "test_key".to_string(),
            api_secret: "test_secret".to_string(),
            account_id: TEST_ACCOUNT_ID,
            mode: Some(mode),
            asset_cache_ttl: std::time::Duration::from_secs(3600),
            time_in_force: TimeInForce::Day,
            counter_trade_slippage_bps: crate::DEFAULT_ALPACA_COUNTER_TRADE_SLIPPAGE_BPS,
        }
    }

    #[tokio::test]
    async fn test_place_market_order_buy_success() {
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));

        let mock = server.mock(|when, then| {
            when.method(POST)
                .path("/v1/trading/accounts/904837e3-3b76-47ec-b432-046db621571b/orders")
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
            shares: Positive::new(FractionalShares::new(float!(100))).unwrap(),
            direction: Direction::Buy,
        };

        let placement = place_market_order(&client, market_order, TimeInForce::Day)
            .await
            .unwrap();

        mock.assert();
        assert_eq!(placement.order_id, "904837e3-3b76-47ec-b432-046db621571b");
        assert_eq!(placement.symbol.to_string(), "AAPL");
        assert_eq!(placement.shares.inner(), FractionalShares::new(float!(100)));
        assert_eq!(placement.direction, Direction::Buy);
    }

    #[tokio::test]
    async fn test_place_market_order_sell_success() {
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));

        let mock = server.mock(|when, then| {
            when.method(POST)
                .path("/v1/trading/accounts/904837e3-3b76-47ec-b432-046db621571b/orders")
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
            shares: Positive::new(FractionalShares::new(float!(50))).unwrap(),
            direction: Direction::Sell,
        };

        let placement = place_market_order(&client, market_order, TimeInForce::Day)
            .await
            .unwrap();

        mock.assert();
        assert_eq!(placement.order_id, "61e7b016-9c91-4a97-b912-615c9d365c9d");
        assert_eq!(placement.symbol.to_string(), "TSLA");
        assert_eq!(placement.shares.inner(), FractionalShares::new(float!(50)));
        assert_eq!(placement.direction, Direction::Sell);
    }

    #[tokio::test]
    async fn test_place_limit_order_buy_success() {
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));

        let mock = server.mock(|when, then| {
            when.method(POST)
                .path("/v1/trading/accounts/904837e3-3b76-47ec-b432-046db621571b/orders")
                .json_body(json!({
                    "symbol": "AAPL",
                    "qty": "100",
                    "side": "buy",
                    "type": "limit",
                    "limit_price": "195.25",
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
        let limit_order = AlpacaLimitOrder {
            symbol: Symbol::new("AAPL").unwrap(),
            shares: Positive::new(FractionalShares::new(float!(100))).unwrap(),
            direction: Direction::Buy,
            limit_price: AlpacaLimitPrice::try_new(
                Positive::new(Usd::new(float!(195.25))).unwrap(),
            )
            .unwrap(),
            extended_hours: false,
        };

        let placement = place_limit_order(&client, limit_order).await.unwrap();

        mock.assert();
        assert_eq!(placement.order_id, "904837e3-3b76-47ec-b432-046db621571b");
        assert_eq!(placement.symbol.to_string(), "AAPL");
        assert_eq!(placement.shares.inner(), FractionalShares::new(float!(100)));
        assert_eq!(placement.direction, Direction::Buy);
    }

    #[tokio::test]
    async fn test_place_limit_order_sell_success_with_extended_hours() {
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));

        let mock = server.mock(|when, then| {
            when.method(POST)
                .path("/v1/trading/accounts/904837e3-3b76-47ec-b432-046db621571b/orders")
                .json_body(json!({
                    "symbol": "TSLA",
                    "qty": "50",
                    "side": "sell",
                    "type": "limit",
                    "limit_price": "210",
                    "time_in_force": "day",
                    "extended_hours": true
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
        let limit_order = AlpacaLimitOrder {
            symbol: Symbol::new("TSLA").unwrap(),
            shares: Positive::new(FractionalShares::new(float!(50))).unwrap(),
            direction: Direction::Sell,
            limit_price: AlpacaLimitPrice::try_new(Positive::new(Usd::new(float!(210))).unwrap())
                .unwrap(),
            extended_hours: true,
        };

        let placement = place_limit_order(&client, limit_order).await.unwrap();

        mock.assert();
        assert_eq!(placement.order_id, "61e7b016-9c91-4a97-b912-615c9d365c9d");
        assert_eq!(placement.symbol.to_string(), "TSLA");
        assert_eq!(placement.shares.inner(), FractionalShares::new(float!(50)));
        assert_eq!(placement.direction, Direction::Sell);
    }

    #[test]
    fn test_alpaca_limit_price_rejects_more_than_two_decimals_at_or_above_one() {
        let error = AlpacaLimitPrice::try_new(Positive::new(Usd::new(float!(195.255))).unwrap())
            .unwrap_err();

        assert!(
            matches!(
                error,
                AlpacaBrokerApiError::InvalidLimitPricePrecision {
                    limit_price,
                    max_decimals: 2,
                } if limit_price == Positive::new(Usd::new(float!(195.255))).unwrap()
            ),
            "Expected InvalidLimitPricePrecision error, got: {error:?}"
        );
    }

    #[test]
    fn test_alpaca_limit_price_rejects_more_than_four_decimals_below_one() {
        let error = AlpacaLimitPrice::try_new(Positive::new(Usd::new(float!(0.12345))).unwrap())
            .unwrap_err();

        assert!(
            matches!(
                error,
                AlpacaBrokerApiError::InvalidLimitPricePrecision {
                    limit_price,
                    max_decimals: 4,
                } if limit_price == Positive::new(Usd::new(float!(0.12345))).unwrap()
            ),
            "Expected InvalidLimitPricePrecision error, got: {error:?}"
        );
    }

    #[tokio::test]
    async fn test_place_limit_order_accepts_price_with_four_decimals_below_one() {
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));

        let mock = server.mock(|when, then| {
            when.method(POST)
                .path("/v1/trading/accounts/904837e3-3b76-47ec-b432-046db621571b/orders")
                .json_body(json!({
                    "symbol": "AAPL",
                    "qty": "1",
                    "side": "buy",
                    "type": "limit",
                    "limit_price": "0.1234",
                    "time_in_force": "day",
                    "extended_hours": false
                }));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "id": "904837e3-3b76-47ec-b432-046db621571b",
                    "symbol": "AAPL",
                    "qty": "1",
                    "side": "buy",
                    "status": "new",
                    "filled_avg_price": null
                }));
        });

        let client = AlpacaBrokerApiClient::new(&ctx).unwrap();
        let limit_order = AlpacaLimitOrder {
            symbol: Symbol::new("AAPL").unwrap(),
            shares: Positive::new(FractionalShares::new(float!(1))).unwrap(),
            direction: Direction::Buy,
            limit_price: AlpacaLimitPrice::try_new(
                Positive::new(Usd::new(float!(0.1234))).unwrap(),
            )
            .unwrap(),
            extended_hours: false,
        };

        let placement = place_limit_order(&client, limit_order).await.unwrap();

        mock.assert();
        assert_eq!(placement.order_id, "904837e3-3b76-47ec-b432-046db621571b");
    }

    #[tokio::test]
    async fn test_get_order_status_pending() {
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));
        let order_id = "904837e3-3b76-47ec-b432-046db621571b";

        let mock = server.mock(|when, then| {
            when.method(GET).path(format!(
                "/v1/trading/accounts/904837e3-3b76-47ec-b432-046db621571b/orders/{order_id}"
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
        assert_eq!(order_update.order_id, order_id);
        assert_eq!(order_update.symbol.to_string(), "AAPL");
        assert_eq!(
            order_update.shares.inner(),
            FractionalShares::new(float!(100))
        );
        assert_eq!(order_update.direction, Direction::Buy);
        assert_eq!(order_update.status, OrderStatus::Submitted);
        assert!(order_update.price.is_none());
    }

    #[tokio::test]
    async fn test_get_order_status_filled() {
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));
        let order_id = "61e7b016-9c91-4a97-b912-615c9d365c9d";

        let mock = server.mock(|when, then| {
            when.method(GET).path(format!(
                "/v1/trading/accounts/904837e3-3b76-47ec-b432-046db621571b/orders/{order_id}"
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
        assert_eq!(order_update.order_id, order_id);
        assert_eq!(order_update.symbol.to_string(), "TSLA");
        assert_eq!(
            order_update.shares.inner(),
            FractionalShares::new(float!(50))
        );
        assert_eq!(order_update.direction, Direction::Sell);
        assert_eq!(order_update.status, OrderStatus::Filled);
        assert!(order_update.price.is_some_and(|price| {
            price
                .eq(Float::parse("245.67".to_string()).unwrap())
                .unwrap()
        }));
    }

    #[tokio::test]
    async fn test_get_order_status_rejected() {
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));
        let order_id = "c7ca82d4-3c95-4f89-9b42-abc123def456";

        let mock = server.mock(|when, then| {
            when.method(GET).path(format!(
                "/v1/trading/accounts/904837e3-3b76-47ec-b432-046db621571b/orders/{order_id}"
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
        assert_eq!(order_update.order_id, order_id);
        assert_eq!(order_update.status, OrderStatus::Failed);
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
                .path("/v1/trading/accounts/904837e3-3b76-47ec-b432-046db621571b/orders")
                .json_body(json!({
                    "symbol": "USDCUSD",
                    "qty": "1000.5",
                    "side": "sell",
                    "type": "market",
                    "time_in_force": "gtc"
                }));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "id": "904837e3-3b76-47ec-b432-046db621571b",
                    "symbol": "USDCUSD",
                    "qty": "1000.5",
                    "side": "sell",
                    "status": "filled",
                    "filled_avg_price": "1.0001",
                    "filled_qty": "1000.5",
                    "created_at": "2025-01-06T12:00:00Z"
                }));
        });

        let client = AlpacaBrokerApiClient::new(&ctx).unwrap();
        let amount = float!(1000.5);

        let order = convert_usdc_usd(&client, amount, ConversionDirection::UsdcToUsd)
            .await
            .unwrap();

        mock.assert();
        assert_eq!(order.id.to_string(), "904837e3-3b76-47ec-b432-046db621571b");
        assert_eq!(order.symbol, "USDCUSD");
        assert!(order.quantity.eq(float!(1000.5)).unwrap());
        assert_eq!(order.status_display(), "filled");
    }

    #[tokio::test]
    async fn test_convert_usd_to_usdc() {
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));

        let mock = server.mock(|when, then| {
            when.method(POST)
                .path("/v1/trading/accounts/904837e3-3b76-47ec-b432-046db621571b/orders")
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
        let amount = float!(500);

        let order = convert_usdc_usd(&client, amount, ConversionDirection::UsdToUsdc)
            .await
            .unwrap();

        mock.assert();
        assert_eq!(order.id.to_string(), "61e7b016-9c91-4a97-b912-615c9d365c9d");
        assert_eq!(order.symbol, "USDCUSD");
        assert!(order.quantity.eq(float!(500)).unwrap());
        assert_eq!(order.status_display(), "filled");
    }

    #[tokio::test]
    async fn test_convert_usdc_usd_rejects_excess_precision() {
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));
        let client = AlpacaBrokerApiClient::new(&ctx).unwrap();

        let error = convert_usdc_usd(
            &client,
            float!(1000.1234567),
            ConversionDirection::UsdToUsdc,
        )
        .await
        .unwrap_err();

        assert!(
            matches!(
                error,
                AlpacaBrokerApiError::UsdcPrecisionExceeded {
                    amount,
                    max_decimals: 6,
                } if amount.eq(float!(1000.1234567)).unwrap()
            ),
            "Expected UsdcPrecisionExceeded error, got: {error:?}"
        );
    }

    #[tokio::test]
    async fn truncates_18_decimal_quantity_to_9() {
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));

        let mock = server.mock(|when, then| {
            when.method(POST)
                .path("/v1/trading/accounts/904837e3-3b76-47ec-b432-046db621571b/orders")
                .json_body(json!({
                    "symbol": "RKLB",
                    "qty": "0.996350331",
                    "side": "sell",
                    "type": "market",
                    "time_in_force": "day",
                    "extended_hours": false
                }));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "id": "904837e3-3b76-47ec-b432-046db621571b",
                    "symbol": "RKLB",
                    "qty": "0.996350331",
                    "side": "sell",
                    "status": "new",
                    "filled_avg_price": null
                }));
        });

        let client = AlpacaBrokerApiClient::new(&ctx).unwrap();

        // Simulate an onchain value with 18 decimal places
        let onchain_shares = Float::parse("0.996350331351928059".to_string()).unwrap();
        let market_order = MarketOrder {
            symbol: Symbol::new("RKLB").unwrap(),
            shares: Positive::new(FractionalShares::new(onchain_shares)).unwrap(),
            direction: Direction::Sell,
        };

        let placement = place_market_order(&client, market_order, TimeInForce::Day)
            .await
            .unwrap();

        mock.assert();
        assert_eq!(placement.symbol.to_string(), "RKLB");
        assert_eq!(placement.direction, Direction::Sell);
        assert!(
            placement
                .shares
                .inner()
                .inner()
                .eq(float!(0.996350331))
                .unwrap()
        );
    }

    #[tokio::test]
    async fn tiny_shares_below_precision_returns_error() {
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));
        let client = AlpacaBrokerApiClient::new(&ctx).unwrap();

        let tiny = Float::parse("0.0000000001".to_string()).unwrap();
        let market_order = MarketOrder {
            symbol: Symbol::new("AAPL").unwrap(),
            shares: Positive::new(FractionalShares::new(tiny)).unwrap(),
            direction: Direction::Buy,
        };

        let err = place_market_order(&client, market_order, TimeInForce::Day)
            .await
            .unwrap_err();

        assert!(
            matches!(
                err,
                AlpacaBrokerApiError::BelowPrecision {
                    max_decimals,
                    ..
                } if max_decimals == crate::ALPACA_MAX_DECIMAL_PLACES
            ),
            "Expected BelowPrecision error, got: {err:?}"
        );
    }

    #[tokio::test]
    async fn truncates_limit_order_quantity_to_9() {
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));

        let mock = server.mock(|when, then| {
            when.method(POST)
                .path("/v1/trading/accounts/904837e3-3b76-47ec-b432-046db621571b/orders")
                .json_body(json!({
                    "symbol": "RKLB",
                    "qty": "0.996350331",
                    "side": "sell",
                    "type": "limit",
                    "limit_price": "17.45",
                    "time_in_force": "day",
                    "extended_hours": false
                }));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "id": "904837e3-3b76-47ec-b432-046db621571b",
                    "symbol": "RKLB",
                    "qty": "0.996350331",
                    "side": "sell",
                    "status": "new",
                    "filled_avg_price": null
                }));
        });

        let client = AlpacaBrokerApiClient::new(&ctx).unwrap();
        let limit_order = AlpacaLimitOrder {
            symbol: Symbol::new("RKLB").unwrap(),
            shares: Positive::new(FractionalShares::new(
                Float::parse("0.996350331351928059".to_string()).unwrap(),
            ))
            .unwrap(),
            direction: Direction::Sell,
            limit_price: AlpacaLimitPrice::try_new(Positive::new(Usd::new(float!(17.45))).unwrap())
                .unwrap(),
            extended_hours: false,
        };

        let placement = place_limit_order(&client, limit_order).await.unwrap();

        mock.assert();
        assert!(
            placement
                .shares
                .inner()
                .inner()
                .eq(float!(0.996350331))
                .unwrap()
        );
    }

    #[tokio::test]
    async fn tiny_limit_order_shares_below_precision_returns_error() {
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));
        let client = AlpacaBrokerApiClient::new(&ctx).unwrap();

        let limit_order = AlpacaLimitOrder {
            symbol: Symbol::new("AAPL").unwrap(),
            shares: Positive::new(FractionalShares::new(
                Float::parse("0.0000000001".to_string()).unwrap(),
            ))
            .unwrap(),
            direction: Direction::Buy,
            limit_price: AlpacaLimitPrice::try_new(
                Positive::new(Usd::new(float!(195.25))).unwrap(),
            )
            .unwrap(),
            extended_hours: false,
        };

        let err = place_limit_order(&client, limit_order).await.unwrap_err();

        assert!(
            matches!(
                err,
                AlpacaBrokerApiError::BelowPrecision {
                    max_decimals,
                    ..
                } if max_decimals == crate::ALPACA_MAX_DECIMAL_PLACES
            ),
            "Expected BelowPrecision error, got: {err:?}"
        );
    }

    #[test]
    fn test_crypto_order_response_status_display() {
        let make_order = |status: BrokerOrderStatus| CryptoOrderResponse {
            id: Uuid::new_v4(),
            symbol: "USDCUSD".to_string(),
            quantity: float!(100),
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
}

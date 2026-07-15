use chrono::{DateTime, Utc};
use rain_math_float::Float;
use st0x_alpaca::broker::{
    BrokerApiError as SharedBrokerApiError, ClientOrderId as SharedClientOrderId,
    LimitOrderRequest as SharedLimitOrderRequest, OrderRequest as SharedOrderRequest,
    OrderSide as SharedOrderSide, OrderStatus as SharedOrderStatus,
    TimeInForce as SharedTimeInForce,
};
use std::str::FromStr;
use tracing::{debug, warn};
use uuid::Uuid;

use super::client::AlpacaBrokerApiClient;
use super::{AlpacaBrokerApiError, MissingOrderField, TimeInForce};
use crate::{
    ClientOrderId, Direction, ExecutorOrderId, FractionalShares, MarketOrder, OrderPlacement,
    OrderStatus, OrderUpdate, Positive, Symbol, Usd,
};

#[derive(Debug, Clone)]
pub struct AlpacaLimitOrder {
    pub symbol: Symbol,
    pub shares: Positive<FractionalShares>,
    pub direction: Direction,
    pub limit_price: AlpacaLimitPrice,
    pub extended_hours: bool,
    pub client_order_id: ClientOrderId,
}

#[derive(Debug, Clone)]
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

    pub const fn as_price(&self) -> &Positive<Usd> {
        &self.0
    }

    pub const fn into_inner(self) -> Positive<Usd> {
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

fn validate_limit_price_precision(limit_price: Positive<Usd>) -> Result<(), AlpacaBrokerApiError> {
    let one_dollar = Float::from_fixed_decimal(alloy::primitives::U256::from(1_u8), 0)?;
    let max_decimals = if limit_price.inner().inner().lt(one_dollar)? {
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
    let request = SharedOrderRequest {
        symbol: market_order.symbol.clone(),
        quantity: placed_shares.inner(),
        side: shared_order_side(market_order.direction),
        order_type: "market",
        time_in_force: shared_time_in_force(time_in_force),
        extended_hours: false,
        client_order_id: shared_client_order_id(&market_order.client_order_id),
    };

    let (order_id, shares, extended_hours, limit_price) = match st0x_alpaca::broker::place_order(
        client.broker(),
        &request,
    )
    .await
    {
        Ok(response) => (response.id, placed_shares, false, None),
        Err(error) if is_duplicate_client_order_id(&error) => {
            debug!(
                client_order_id = %market_order.client_order_id,
                "Broker rejected duplicate client_order_id; reconciling the order it already accepted"
            );
            let existing = st0x_alpaca::broker::get_order_by_client_order_id(
                client.broker(),
                &request.client_order_id,
            )
            .await?
            .ok_or_else(|| AlpacaBrokerApiError::DuplicateOrderNotFound {
                client_order_id: market_order.client_order_id.clone(),
            })?;
            (
                existing.id,
                Positive::new(existing.quantity)?,
                existing.extended_hours.unwrap_or(false),
                parse_limit_price(existing.limit_price)?,
            )
        }
        Err(error) => return Err(error.into()),
    };

    Ok(OrderPlacement {
        order_id: order_id.to_string(),
        symbol: market_order.symbol,
        shares,
        direction: market_order.direction,
        placed_at: Utc::now(),
        extended_hours,
        limit_price,
    })
}

fn parse_limit_price(
    limit_price: Option<Usd>,
) -> Result<Option<Positive<Usd>>, AlpacaBrokerApiError> {
    limit_price
        .map(Positive::new)
        .transpose()
        .map_err(Into::into)
}

fn is_duplicate_client_order_id(error: &SharedBrokerApiError) -> bool {
    matches!(
        error,
        SharedBrokerApiError::Alpaca(st0x_alpaca::AlpacaError::Api {
            status_code: 422,
            body,
        }) if body.contains("client_order_id must be unique")
    )
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
    let request = SharedLimitOrderRequest {
        symbol: limit_order.symbol.clone(),
        quantity: placed_shares.inner(),
        side: shared_order_side(limit_order.direction),
        order_type: "limit",
        limit_price: limit_order.limit_price.as_price().inner(),
        time_in_force: SharedTimeInForce::Day,
        extended_hours: limit_order.extended_hours,
        client_order_id: shared_client_order_id(&limit_order.client_order_id),
    };

    let (order_id, shares, extended_hours, limit_price) =
        match st0x_alpaca::broker::place_limit_order(client.broker(), &request).await {
            Ok(response) => (
                response.id,
                placed_shares,
                limit_order.extended_hours,
                Some(*limit_order.limit_price.as_price()),
            ),
            Err(error) if is_duplicate_client_order_id(&error) => {
                debug!(
                    client_order_id = %limit_order.client_order_id,
                    "Broker rejected duplicate client_order_id; reconciling the order it already accepted"
                );
                let existing = st0x_alpaca::broker::get_order_by_client_order_id(
                    client.broker(),
                    &request.client_order_id,
                )
                .await?
                .ok_or_else(|| AlpacaBrokerApiError::DuplicateOrderNotFound {
                    client_order_id: limit_order.client_order_id.clone(),
                })?;
                (
                    existing.id,
                    Positive::new(existing.quantity)?,
                    existing
                        .extended_hours
                        .unwrap_or(limit_order.extended_hours),
                    parse_limit_price(existing.limit_price)?
                        .or(Some(*limit_order.limit_price.as_price())),
                )
            }
            Err(error) => return Err(error.into()),
        };

    Ok(OrderPlacement {
        order_id: order_id.to_string(),
        symbol: limit_order.symbol,
        shares,
        direction: limit_order.direction,
        placed_at: Utc::now(),
        extended_hours,
        limit_price,
    })
}

pub(super) async fn get_order_status(
    client: &AlpacaBrokerApiClient,
    order_id: &str,
) -> Result<OrderUpdate<String>, AlpacaBrokerApiError> {
    debug!("Querying Alpaca Broker API order status for order ID: {order_id}");

    let order_uuid = Uuid::parse_str(order_id)?;
    let response = st0x_alpaca::broker::get_order(client.broker(), order_uuid).await?;
    let direction = match response.side {
        SharedOrderSide::Buy => Direction::Buy,
        SharedOrderSide::Sell => Direction::Sell,
    };
    let status = map_broker_status_to_order_status(response.status);
    let shares_filled = response.filled_quantity;

    if response.status == SharedOrderStatus::PartiallyFilled {
        debug!(
            order_id,
            symbol = %response.symbol,
            ordered_qty = %response.quantity,
            filled_qty = ?shares_filled,
            "Order is partially filled"
        );
    }

    let updated_at = match status {
        OrderStatus::Filled => terminal_broker_time(
            response.filled_at,
            response.updated_at,
            order_id,
            MissingOrderField::FilledAt,
        )?,
        OrderStatus::Cancelled => terminal_broker_time(
            response.canceled_at,
            response.updated_at,
            order_id,
            MissingOrderField::CanceledAt,
        )?,
        OrderStatus::Failed => {
            broker_time_or_observation(response.failed_at.or(response.updated_at), order_id)
        }
        OrderStatus::Pending | OrderStatus::Submitted | OrderStatus::PartiallyFilled => {
            broker_time_or_observation(response.updated_at, order_id)
        }
    };

    Ok(OrderUpdate {
        order_id: order_id.to_string(),
        symbol: response.symbol,
        shares: Positive::new(response.quantity)?,
        direction,
        status,
        updated_at,
        price: response.filled_average_price.map(Usd::inner),
        shares_filled,
    })
}

fn terminal_broker_time(
    specific: Option<DateTime<Utc>>,
    updated_at: Option<DateTime<Utc>>,
    order_id: &str,
    field: MissingOrderField,
) -> Result<DateTime<Utc>, AlpacaBrokerApiError> {
    if let Some(time) = specific {
        return Ok(time);
    }
    if let Some(time) = updated_at {
        warn!(
            order_id,
            ?field,
            "Terminal order response omitted its status timestamp; using updated_at"
        );
        return Ok(time);
    }
    Err(AlpacaBrokerApiError::IncompleteOrder {
        order_id: ExecutorOrderId::new(order_id),
        field,
    })
}

fn broker_time_or_observation(broker_time: Option<DateTime<Utc>>, order_id: &str) -> DateTime<Utc> {
    broker_time.unwrap_or_else(|| {
        warn!(
            order_id,
            "Broker order response omitted updated_at; using observation time"
        );
        Utc::now()
    })
}

fn map_broker_status_to_order_status(status: SharedOrderStatus) -> OrderStatus {
    match status {
        SharedOrderStatus::New | SharedOrderStatus::Accepted => OrderStatus::Pending,
        SharedOrderStatus::PendingNew
        | SharedOrderStatus::PendingCancel
        | SharedOrderStatus::PendingReplace
        | SharedOrderStatus::AcceptedForBidding => OrderStatus::Submitted,
        SharedOrderStatus::PartiallyFilled => OrderStatus::PartiallyFilled,
        SharedOrderStatus::Filled => OrderStatus::Filled,
        SharedOrderStatus::Canceled => OrderStatus::Cancelled,
        SharedOrderStatus::DoneForDay
        | SharedOrderStatus::Expired
        | SharedOrderStatus::Replaced
        | SharedOrderStatus::Rejected
        | SharedOrderStatus::Suspended
        | SharedOrderStatus::Calculated
        | SharedOrderStatus::Stopped => OrderStatus::Failed,
    }
}

fn truncate_shares_to_alpaca_precision(
    shares: Positive<FractionalShares>,
) -> Result<Positive<FractionalShares>, AlpacaBrokerApiError> {
    let Some(truncated) = crate::truncate_to_decimal_places(
        shares.inner().inner(),
        crate::ALPACA_MAX_DECIMAL_PLACES,
    )?
    else {
        return Err(AlpacaBrokerApiError::BelowPrecision {
            shares,
            max_decimals: crate::ALPACA_MAX_DECIMAL_PLACES,
        });
    };

    Ok(Positive::new(FractionalShares::new(truncated))?)
}

pub(crate) async fn convert_usdc_usd(
    client: &AlpacaBrokerApiClient,
    amount: Float,
    direction: st0x_alpaca::broker::ConversionDirection,
    client_order_id: &ClientOrderId,
) -> Result<st0x_alpaca::broker::CryptoOrderResponse, AlpacaBrokerApiError> {
    st0x_alpaca::broker::convert_usdc_usd(
        client.broker(),
        crate::Usdc::new(amount),
        direction,
        &shared_client_order_id(client_order_id),
    )
    .await
    .map_err(Into::into)
}

fn shared_order_side(direction: Direction) -> SharedOrderSide {
    match direction {
        Direction::Buy => SharedOrderSide::Buy,
        Direction::Sell => SharedOrderSide::Sell,
    }
}

fn shared_time_in_force(time_in_force: TimeInForce) -> SharedTimeInForce {
    match time_in_force {
        TimeInForce::Day => SharedTimeInForce::Day,
        TimeInForce::MarketOnClose => SharedTimeInForce::MarketOnClose,
    }
}

fn shared_client_order_id(client_order_id: &ClientOrderId) -> SharedClientOrderId {
    SharedClientOrderId(client_order_id.to_string())
}

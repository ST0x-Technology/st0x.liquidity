use std::fmt::{self, Debug, Display};

use chrono::{DateTime, Utc};
use rain_math_float::Float;
use serde::{Deserialize, Serialize};
use st0x_float_serde::DebugOptionFloat;

use crate::{Direction, FractionalShares, Positive, Symbol};

pub mod state;
pub mod status;

pub use state::OrderState;
pub use status::OrderStatus;

/// Caller-supplied idempotency key forwarded to the broker so retries of
/// the same logical order do not double-submit.
///
/// Alpaca accepts a `client_order_id` of up to 128 characters and dedupes
/// placements on it. Other brokers expose equivalent fields.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ClientOrderId(String);

impl ClientOrderId {
    /// Constructs a `ClientOrderId` from a non-empty string, trimming to
    /// the Alpaca 128-char limit.
    ///
    /// # Errors
    /// Returns an error if the input is empty.
    pub fn new(value: impl Into<String>) -> Result<Self, ClientOrderIdError> {
        let raw: String = value.into();
        if raw.is_empty() {
            return Err(ClientOrderIdError::Empty);
        }
        let truncated = if raw.len() > 128 {
            raw[..128].to_owned()
        } else {
            raw
        };
        Ok(Self(truncated))
    }

    /// Constructs a `ClientOrderId` from a UUID. Infallible because the
    /// hyphenated UUID string is always non-empty and shorter than the
    /// 128-char broker limit.
    pub fn from_uuid(uuid: uuid::Uuid) -> Self {
        Self(uuid.to_string())
    }

    /// Constructs a `ClientOrderId` by concatenating a non-empty prefix
    /// (e.g. `"cli-"`, `"preflight-"`) with a UUID. Infallible because the
    /// prefix is taken as a non-empty static string and the suffix is a
    /// hyphenated UUID; the combined length stays well under 128 chars.
    pub fn from_prefixed_uuid(prefix: &'static str, uuid: uuid::Uuid) -> Self {
        debug_assert!(!prefix.is_empty(), "prefix must be non-empty");
        Self(format!("{prefix}{uuid}"))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Display for ClientOrderId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ClientOrderIdError {
    #[error("client_order_id must not be empty")]
    Empty,
}

#[derive(Debug)]
pub struct OrderPlacement<OrderId> {
    pub order_id: OrderId,
    pub symbol: Symbol,
    pub shares: Positive<FractionalShares>,
    pub direction: Direction,
    pub placed_at: DateTime<Utc>,
}

pub struct OrderUpdate<OrderId> {
    pub order_id: OrderId,
    pub symbol: Symbol,
    pub shares: Positive<FractionalShares>,
    pub direction: Direction,
    pub status: OrderStatus,
    pub updated_at: DateTime<Utc>,
    pub price: Option<Float>,
}

impl<OrderId: Debug> Debug for OrderUpdate<OrderId> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OrderUpdate")
            .field("order_id", &self.order_id)
            .field("symbol", &self.symbol)
            .field("shares", &self.shares)
            .field("direction", &self.direction)
            .field("status", &self.status)
            .field("updated_at", &self.updated_at)
            .field("price", &DebugOptionFloat(&self.price))
            .finish()
    }
}

#[derive(Debug, Clone)]
pub struct MarketOrder {
    pub symbol: Symbol,
    pub shares: Positive<FractionalShares>,
    pub direction: Direction,
    /// Forwarded to the broker so that retries of a logically identical
    /// placement (e.g. apalis re-running a job after a transient 5xx whose
    /// response was lost in flight) are deduped by the broker rather than
    /// submitting a second order.
    pub client_order_id: ClientOrderId,
}

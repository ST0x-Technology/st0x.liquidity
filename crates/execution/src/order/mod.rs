use std::fmt::{self, Debug, Display};
use std::str::FromStr;

use chrono::{DateTime, Utc};
use rain_math_float::Float;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use st0x_float_serde::DebugOptionFloat;

use crate::{Direction, FractionalShares, Positive, Symbol, Usd};

pub mod state;
pub mod status;

pub use state::OrderState;
pub use status::OrderStatus;

/// Caller-supplied idempotency key forwarded to the broker so retries of
/// the same logical order do not double-submit.
///
/// The underlying value is always a UUID; the broker's `client_order_id`
/// string is *derived* from it via [`Display`]. The variant records the
/// order's provenance, which selects the rendered prefix.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ClientOrderId {
    /// Automated placement (e.g. a conversion correlation id or an offchain
    /// order id). Renders as the bare hyphenated UUID.
    Automated(Uuid),
    /// Operator-initiated CLI placement. Renders as `cli-{uuid}` so manual
    /// orders are distinguishable in the broker dashboard.
    Cli(Uuid),
}

/// Prefix rendered for [`ClientOrderId::Cli`].
const CLI_PREFIX: &str = "cli-";

impl ClientOrderId {
    /// Idempotency key for an automated placement keyed by `uuid` (e.g. a
    /// conversion correlation id or an offchain order id).
    pub fn from_uuid(uuid: Uuid) -> Self {
        Self::Automated(uuid)
    }

    /// Idempotency key for an operator-initiated CLI placement; renders with
    /// the `cli-` prefix.
    pub fn cli(uuid: Uuid) -> Self {
        Self::Cli(uuid)
    }
}

impl Display for ClientOrderId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Automated(uuid) => Display::fmt(uuid, f),
            Self::Cli(uuid) => write!(f, "{CLI_PREFIX}{uuid}"),
        }
    }
}

impl FromStr for ClientOrderId {
    type Err = ClientOrderIdError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.strip_prefix(CLI_PREFIX) {
            Some(rest) => Ok(Self::Cli(rest.parse()?)),
            None => Ok(Self::Automated(value.parse()?)),
        }
    }
}

impl Serialize for ClientOrderId {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for ClientOrderId {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = String::deserialize(deserializer)?;
        raw.parse().map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ClientOrderIdError {
    #[error("client_order_id is not a valid UUID")]
    InvalidUuid(#[from] uuid::Error),
}

#[derive(Debug)]
pub struct OrderPlacement<OrderId> {
    pub order_id: OrderId,
    pub symbol: Symbol,
    pub shares: Positive<FractionalShares>,
    pub direction: Direction,
    pub placed_at: DateTime<Utc>,
    /// Whether the broker holds this as an extended-hours order. For a fresh
    /// placement this echoes the request; when a duplicate `client_order_id`
    /// placement adopts an order from a prior attempt, it is the ADOPTED
    /// order's flag, which can differ from the request (e.g. a regular-hours
    /// market retry adopting a still-live extended-hours limit order after a
    /// lost placement response). Callers must record this value -- not the
    /// request's -- so session-convergence logic sees broker reality.
    pub extended_hours: bool,
    /// The broker-held limit price, if the order is a limit order. Same
    /// adoption semantics as `extended_hours`.
    pub limit_price: Option<Positive<Usd>>,
}

pub struct OrderUpdate<OrderId> {
    pub order_id: OrderId,
    pub symbol: Symbol,
    pub shares: Positive<FractionalShares>,
    pub direction: Direction,
    pub status: OrderStatus,
    pub updated_at: DateTime<Utc>,
    pub price: Option<Float>,
    /// Cumulative quantity filled at the broker so far. `None` when the
    /// broker response omitted the field; brokers that always report it
    /// (Alpaca sends `filled_qty: "0"`) yield `Some(0)` for unfilled orders,
    /// so consumers MUST treat `Some(0)` and `None` alike as "no fill" --
    /// never branch on `is_some()` to detect a fill. Required for
    /// `OrderStatus::PartiallyFilled` so the caller can reconcile the local
    /// aggregate via `UpdatePartialFill`.
    pub shares_filled: Option<FractionalShares>,
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
            .field("shares_filled", &self.shares_filled)
            .finish()
    }
}

/// Outcome of a broker cancel request.
///
/// Distinguishes "the broker accepted the cancel request" from "the broker
/// no longer recognises the order id". The latter must not be reported as
/// plain success (the caller would wait forever for a cancellation
/// confirmation the broker will never produce) nor as a retryable error
/// (re-sending the cancel can never succeed). Callers resolve `OrderNotFound`
/// by treating the order as already terminal at the broker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CancellationOutcome {
    /// The broker accepted the cancel request; the order will reach a
    /// terminal state observable via `get_order_status`.
    Requested,
    /// The broker does not recognise the order id (e.g. HTTP 404). No live
    /// order exists under this id, so no further fills can occur.
    OrderNotFound,
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

/// Broker-agnostic limit order for automated counter-trading during extended
/// hours. The executor implementation converts this to its broker-specific
/// limit order type (e.g. `AlpacaLimitOrder`).
#[derive(Debug, Clone)]
pub struct LimitOrder {
    pub symbol: Symbol,
    pub shares: Positive<FractionalShares>,
    pub direction: Direction,
    pub limit_price: Positive<Usd>,
    pub extended_hours: bool,
    /// Forwarded to the broker so that retries of a logically identical
    /// extended-hours placement (e.g. apalis re-running a job after a lost
    /// placement response) are deduped by the broker rather than submitting a
    /// second live limit order. Mirrors [`MarketOrder::client_order_id`].
    pub client_order_id: ClientOrderId,
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;
    use serde_json::json;
    use uuid::{Uuid, uuid};

    use super::*;

    const ID: Uuid = uuid!("11111111-1111-4111-8111-111111111111");
    const BARE: &str = "11111111-1111-4111-8111-111111111111";
    const PREFIXED: &str = "cli-11111111-1111-4111-8111-111111111111";

    #[test]
    fn automated_renders_as_bare_uuid() {
        assert_eq!(ClientOrderId::from_uuid(ID).to_string(), BARE);
    }

    #[test]
    fn cli_renders_with_prefix() {
        assert_eq!(ClientOrderId::cli(ID).to_string(), PREFIXED);
    }

    #[test]
    fn parses_bare_uuid_as_automated() {
        let parsed: ClientOrderId = BARE.parse().unwrap();
        assert_eq!(parsed, ClientOrderId::Automated(ID));
    }

    #[test]
    fn parses_cli_prefixed_uuid_as_cli() {
        let parsed: ClientOrderId = PREFIXED.parse().unwrap();
        assert_eq!(parsed, ClientOrderId::Cli(ID));
    }

    #[test]
    fn rejects_non_uuid() {
        let error = "not-a-uuid".parse::<ClientOrderId>().unwrap_err();
        assert!(
            matches!(error, ClientOrderIdError::InvalidUuid(_)),
            "expected InvalidUuid, got {error:?}"
        );
    }

    #[test]
    fn serializes_to_derived_string() {
        assert_eq!(
            serde_json::to_value(ClientOrderId::from_uuid(ID)).unwrap(),
            json!(BARE)
        );
        assert_eq!(
            serde_json::to_value(ClientOrderId::cli(ID)).unwrap(),
            json!(PREFIXED)
        );
    }

    #[test]
    fn deserializes_from_string_form() {
        let automated: ClientOrderId = serde_json::from_value(json!(BARE)).unwrap();
        assert_eq!(automated, ClientOrderId::Automated(ID));

        let cli: ClientOrderId = serde_json::from_value(json!(PREFIXED)).unwrap();
        assert_eq!(cli, ClientOrderId::Cli(ID));
    }

    proptest! {
        #[test]
        fn display_parse_roundtrip(uuid in prop::array::uniform16(any::<u8>()).prop_map(Uuid::from_bytes)) {
            for id in [ClientOrderId::from_uuid(uuid), ClientOrderId::cli(uuid)] {
                let rendered = id.to_string();
                let parsed: ClientOrderId = rendered.parse().unwrap();
                prop_assert_eq!(parsed, id);
            }
        }

        #[test]
        fn serde_roundtrip(uuid in prop::array::uniform16(any::<u8>()).prop_map(Uuid::from_bytes)) {
            for id in [ClientOrderId::from_uuid(uuid), ClientOrderId::cli(uuid)] {
                let json = serde_json::to_value(&id).unwrap();
                let parsed: ClientOrderId = serde_json::from_value(json).unwrap();
                prop_assert_eq!(parsed, id);
            }
        }
    }
}

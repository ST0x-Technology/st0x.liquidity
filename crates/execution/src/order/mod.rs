use std::fmt::{self, Debug, Display};
use std::str::FromStr;

use chrono::{DateTime, Utc};
use rain_math_float::Float;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use st0x_float_serde::DebugOptionFloat;

use crate::{Direction, FractionalShares, Positive, Symbol};

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
}

#[cfg(test)]
mod tests {
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
}

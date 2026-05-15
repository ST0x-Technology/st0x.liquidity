use std::fmt::Debug;

use chrono::{DateTime, Utc};
use rain_math_float::Float;
use st0x_float_serde::DebugOptionFloat;

use crate::{Direction, FractionalShares, Positive, Symbol, Usd};

pub mod state;
pub mod status;

pub use state::OrderState;
pub use status::OrderStatus;

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
    /// Cumulative quantity filled at the broker so far. `None` when no
    /// fills have occurred. Required for `OrderStatus::PartiallyFilled`
    /// so the caller can reconcile the local aggregate via
    /// `UpdatePartialFill`.
    pub shares_filled: Option<Float>,
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
            .field("shares_filled", &DebugOptionFloat(&self.shares_filled))
            .finish()
    }
}

#[derive(Debug, Clone)]
pub struct MarketOrder {
    pub symbol: Symbol,
    pub shares: Positive<FractionalShares>,
    pub direction: Direction,
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
}

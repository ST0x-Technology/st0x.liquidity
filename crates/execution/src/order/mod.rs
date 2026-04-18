use std::fmt::Debug;

use chrono::{DateTime, Utc};
use rain_math_float::Float;
use st0x_float_serde::DebugOptionFloat;

use crate::{Direction, FractionalShares, Positive, Symbol};

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

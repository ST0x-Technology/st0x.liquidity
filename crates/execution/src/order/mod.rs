use std::fmt::Debug;

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;

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

#[derive(Debug)]
pub struct OrderUpdate<OrderId> {
    pub order_id: OrderId,
    pub symbol: Symbol,
    pub shares: Positive<FractionalShares>,
    pub direction: Direction,
    pub status: OrderStatus,
    pub updated_at: DateTime<Utc>,
    pub price: Option<Decimal>,
}

#[derive(Debug, Clone)]
pub struct MarketOrder {
    pub symbol: Symbol,
    pub shares: Positive<FractionalShares>,
    pub direction: Direction,
}

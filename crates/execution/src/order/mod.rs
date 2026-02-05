use std::fmt::Debug;

use chrono::{DateTime, Utc};

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
pub(crate) struct OrderUpdate {
    pub(crate) status: OrderStatus,
    pub(crate) updated_at: DateTime<Utc>,
    pub(crate) price_cents: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct MarketOrder {
    pub symbol: Symbol,
    pub shares: Positive<FractionalShares>,
    pub direction: Direction,
}

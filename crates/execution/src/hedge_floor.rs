//! Shares the bot keeps in the broker account per symbol so the position
//! never goes flat.
//!
//! Pricing derives every mark from the broker's `/positions`, so a symbol
//! with no position has no mark. Every path that moves shares out of the
//! account (sell hedges, equity mints) caps its quantity at
//! `available - floor` instead of `available`. The residual is a deliberate,
//! bounded unhedged exposure; a floor of zero disables it.

use std::collections::HashMap;

use crate::{FractionalShares, Symbol};

/// The configured floor: one global default plus per-symbol overrides.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HedgeFloor {
    default_shares: FractionalShares,
    per_symbol: HashMap<Symbol, FractionalShares>,
}

impl Default for HedgeFloor {
    fn default() -> Self {
        Self::new(FractionalShares::ZERO, HashMap::new())
    }
}

impl HedgeFloor {
    pub fn new(
        default_shares: FractionalShares,
        per_symbol: HashMap<Symbol, FractionalShares>,
    ) -> Self {
        Self {
            default_shares,
            per_symbol,
        }
    }

    /// Floor for `symbol`: its override when configured, else the default.
    pub fn for_symbol(&self, symbol: &Symbol) -> FractionalShares {
        self.per_symbol
            .get(symbol)
            .copied()
            .unwrap_or(self.default_shares)
    }
}

#[cfg(test)]
mod tests {
    use rain_math_float::Float;

    use super::*;

    fn shares(value: &str) -> FractionalShares {
        FractionalShares::new(Float::parse(value.to_string()).unwrap())
    }

    #[test]
    fn for_symbol_prefers_the_override_over_the_default() {
        let aapl = Symbol::new("AAPL").unwrap();
        let floor = HedgeFloor::new(shares("1"), HashMap::from([(aapl.clone(), shares("3"))]));

        assert_eq!(floor.for_symbol(&aapl), shares("3"));
        assert_eq!(floor.for_symbol(&Symbol::new("MSFT").unwrap()), shares("1"));
    }

    #[test]
    fn default_floor_is_zero() {
        assert_eq!(
            HedgeFloor::default().for_symbol(&Symbol::new("AAPL").unwrap()),
            FractionalShares::ZERO
        );
    }
}

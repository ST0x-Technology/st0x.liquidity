use rust_decimal::Decimal;
use st0x_broker::Direction;
use std::cmp::Ordering;
use std::collections::VecDeque;
use std::fmt;
use std::str::FromStr;
use thiserror::Error;

#[derive(Debug, Error)]
pub(super) enum PnlError {
    #[error("Invalid quantity: {0}")]
    InvalidQuantity(String),
    #[error("Invalid price: {0}")]
    InvalidPrice(String),
    #[error("Arithmetic overflow in P&L calculation")]
    ArithmeticOverflow,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum TradeType {
    Onchain,
    Offchain,
}

impl FromStr for TradeType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "ONCHAIN" => Ok(Self::Onchain),
            "OFFCHAIN" => Ok(Self::Offchain),
            _ => Err(format!("Invalid trade type: '{s}'")),
        }
    }
}

impl fmt::Display for TradeType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Onchain => write!(f, "ONCHAIN"),
            Self::Offchain => write!(f, "OFFCHAIN"),
        }
    }
}

#[derive(Debug, Clone)]
struct InventoryLot {
    quantity_remaining: Decimal,
    cost_basis_per_share: Decimal,
    direction: Direction,
}

/// Maintains FIFO (First-In-First-Out) inventory tracking for a single symbol.
///
/// Manages a queue of position lots and calculates realized P&L when lots are
/// consumed. The FIFO algorithm consumes oldest lots first when closing
/// positions, which is a standard accounting method for cost basis calculation.
pub(super) struct FifoInventory {
    lots: VecDeque<InventoryLot>,
    cumulative_pnl: Decimal,
}

pub(super) struct PnlResult {
    pub(super) realized_pnl: Option<Decimal>,
    pub(super) cumulative_pnl: Decimal,
    pub(super) net_position_after: Decimal,
}

impl FifoInventory {
    pub(super) fn new() -> Self {
        Self {
            lots: VecDeque::new(),
            cumulative_pnl: Decimal::ZERO,
        }
    }

    /// Processes a trade and updates FIFO inventory, returning P&L metrics.
    ///
    /// The trade either increases the position (same direction as current
    /// position or opening new position) or decreases the position (opposite
    /// direction). When decreasing, FIFO lots are consumed and P&L is
    /// realized. When increasing, a new lot is added with no P&L realization.
    pub(super) fn process_trade(
        &mut self,
        quantity: Decimal,
        price_per_share: Decimal,
        direction: Direction,
    ) -> Result<PnlResult, PnlError> {
        if quantity <= Decimal::ZERO {
            return Err(PnlError::InvalidQuantity(quantity.to_string()));
        }

        if price_per_share <= Decimal::ZERO {
            return Err(PnlError::InvalidPrice(price_per_share.to_string()));
        }

        match self.current_direction() {
            None => {
                self.add_lot(quantity, price_per_share, direction);
                Ok(PnlResult {
                    realized_pnl: None,
                    cumulative_pnl: self.cumulative_pnl,
                    net_position_after: self.net_position(),
                })
            }
            Some(current) if current == direction => {
                self.add_lot(quantity, price_per_share, direction);
                Ok(PnlResult {
                    realized_pnl: None,
                    cumulative_pnl: self.cumulative_pnl,
                    net_position_after: self.net_position(),
                })
            }
            Some(_) => {
                let pnl = self.consume_lots(quantity, price_per_share, direction)?;
                self.cumulative_pnl = self
                    .cumulative_pnl
                    .checked_add(pnl)
                    .ok_or(PnlError::ArithmeticOverflow)?;

                Ok(PnlResult {
                    realized_pnl: Some(pnl),
                    cumulative_pnl: self.cumulative_pnl,
                    net_position_after: self.net_position(),
                })
            }
        }
    }

    /// Consumes FIFO lots (oldest first) to close or reduce a position,
    /// calculating realized P&L.
    ///
    /// For long positions (Direction::Buy lots), P&L = (sell_price -
    /// cost_basis) * shares For short positions (Direction::Sell lots), P&L =
    /// (cost_basis - buy_price) * shares
    ///
    /// If the trade quantity exceeds available lots, the position reverses:
    /// all existing lots are consumed and a new lot opens in the opposite
    /// direction.
    fn consume_lots(
        &mut self,
        quantity: Decimal,
        execution_price: Decimal,
        direction: Direction,
    ) -> Result<Decimal, PnlError> {
        let (total_pnl, remaining) = self.lots.iter_mut().try_fold(
            (Decimal::ZERO, quantity),
            |(pnl_acc, qty_remaining), lot| {
                if qty_remaining == Decimal::ZERO {
                    return Ok((pnl_acc, qty_remaining));
                }

                let consumed = qty_remaining.min(lot.quantity_remaining);

                let pnl = match lot.direction {
                    Direction::Buy => (execution_price - lot.cost_basis_per_share)
                        .checked_mul(consumed)
                        .ok_or(PnlError::ArithmeticOverflow)?,
                    Direction::Sell => (lot.cost_basis_per_share - execution_price)
                        .checked_mul(consumed)
                        .ok_or(PnlError::ArithmeticOverflow)?,
                };

                lot.quantity_remaining = lot
                    .quantity_remaining
                    .checked_sub(consumed)
                    .ok_or(PnlError::ArithmeticOverflow)?;

                let new_pnl = pnl_acc
                    .checked_add(pnl)
                    .ok_or(PnlError::ArithmeticOverflow)?;

                let new_remaining = qty_remaining
                    .checked_sub(consumed)
                    .ok_or(PnlError::ArithmeticOverflow)?;

                Ok((new_pnl, new_remaining))
            },
        )?;

        self.lots
            .retain(|lot| lot.quantity_remaining > Decimal::ZERO);

        if remaining > Decimal::ZERO {
            self.add_lot(remaining, execution_price, direction);
        }

        Ok(total_pnl)
    }

    fn add_lot(&mut self, quantity: Decimal, price: Decimal, direction: Direction) {
        self.lots.push_back(InventoryLot {
            quantity_remaining: quantity,
            cost_basis_per_share: price,
            direction,
        });
    }

    fn net_position(&self) -> Decimal {
        self.lots
            .iter()
            .fold(Decimal::ZERO, |acc, lot| match lot.direction {
                Direction::Buy => acc + lot.quantity_remaining,
                Direction::Sell => acc - lot.quantity_remaining,
            })
    }

    fn current_direction(&self) -> Option<Direction> {
        match self.net_position().cmp(&Decimal::ZERO) {
            Ordering::Greater => Some(Direction::Buy),
            Ordering::Less => Some(Direction::Sell),
            Ordering::Equal => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::sample::select;
    use rust_decimal_macros::dec;

    #[test]
    fn test_simple_buy_sell() {
        let mut fifo = FifoInventory::new();

        let result = fifo
            .process_trade(dec!(100), dec!(10.00), Direction::Buy)
            .unwrap();
        assert_eq!(result.realized_pnl, None);
        assert_eq!(result.net_position_after, dec!(100));

        let result = fifo
            .process_trade(dec!(100), dec!(11.00), Direction::Sell)
            .unwrap();
        assert_eq!(result.realized_pnl, Some(dec!(100.00)));
        assert_eq!(result.cumulative_pnl, dec!(100.00));
        assert_eq!(result.net_position_after, dec!(0));
    }

    #[test]
    fn test_multiple_lots_fifo() {
        let mut fifo = FifoInventory::new();

        fifo.process_trade(dec!(100), dec!(10.00), Direction::Buy)
            .unwrap();
        fifo.process_trade(dec!(50), dec!(12.00), Direction::Buy)
            .unwrap();

        let result = fifo
            .process_trade(dec!(80), dec!(11.00), Direction::Sell)
            .unwrap();
        assert_eq!(result.realized_pnl, Some(dec!(80.00)));
        assert_eq!(result.net_position_after, dec!(70));
    }

    #[test]
    fn test_position_reversal_long_to_short() {
        let mut fifo = FifoInventory::new();

        fifo.process_trade(dec!(100), dec!(10.00), Direction::Buy)
            .unwrap();

        let result = fifo
            .process_trade(dec!(150), dec!(11.00), Direction::Sell)
            .unwrap();
        assert_eq!(result.realized_pnl, Some(dec!(100.00)));
        assert_eq!(result.net_position_after, dec!(-50));
    }

    #[test]
    fn test_position_reversal_short_to_long() {
        let mut fifo = FifoInventory::new();

        fifo.process_trade(dec!(100), dec!(10.00), Direction::Sell)
            .unwrap();

        let result = fifo
            .process_trade(dec!(150), dec!(11.00), Direction::Buy)
            .unwrap();
        assert_eq!(result.realized_pnl, Some(dec!(-100.00)));
        assert_eq!(result.net_position_after, dec!(50));
    }

    #[test]
    fn test_short_position_pnl() {
        let mut fifo = FifoInventory::new();

        let result = fifo
            .process_trade(dec!(100), dec!(10.00), Direction::Sell)
            .unwrap();
        assert_eq!(result.realized_pnl, None);
        assert_eq!(result.net_position_after, dec!(-100));

        let result = fifo
            .process_trade(dec!(100), dec!(9.00), Direction::Buy)
            .unwrap();
        assert_eq!(result.realized_pnl, Some(dec!(100.00)));
        assert_eq!(result.cumulative_pnl, dec!(100.00));
        assert_eq!(result.net_position_after, dec!(0));
    }

    #[test]
    fn test_requirements_doc_example() {
        let mut fifo = FifoInventory::new();

        fifo.process_trade(dec!(100), dec!(10.00), Direction::Buy)
            .unwrap();
        fifo.process_trade(dec!(50), dec!(12.00), Direction::Buy)
            .unwrap();

        let result = fifo
            .process_trade(dec!(80), dec!(11.00), Direction::Sell)
            .unwrap();
        assert_eq!(result.realized_pnl, Some(dec!(80.00)));
        assert_eq!(result.cumulative_pnl, dec!(80.00));

        let result = fifo
            .process_trade(dec!(60), dec!(9.50), Direction::Sell)
            .unwrap();
        assert_eq!(result.realized_pnl, Some(dec!(-110.00)));
        assert_eq!(result.cumulative_pnl, dec!(-30.00));

        fifo.process_trade(dec!(30), dec!(12.20), Direction::Buy)
            .unwrap();

        let result = fifo
            .process_trade(dec!(70), dec!(12.00), Direction::Sell)
            .unwrap();
        assert_eq!(result.realized_pnl, Some(dec!(-6.00)));
        assert_eq!(result.cumulative_pnl, dec!(-36.00));

        let result = fifo
            .process_trade(dec!(20), dec!(11.50), Direction::Buy)
            .unwrap();
        assert_eq!(result.realized_pnl, Some(dec!(10.00)));
        assert_eq!(result.cumulative_pnl, dec!(-26.00));
        assert_eq!(result.net_position_after, dec!(-10));
    }

    #[test]
    fn test_fractional_share_handling() {
        let mut fifo = FifoInventory::new();

        fifo.process_trade(dec!(10.5), dec!(100.00), Direction::Buy)
            .unwrap();

        let result = fifo
            .process_trade(dec!(5.25), dec!(110.00), Direction::Sell)
            .unwrap();
        assert_eq!(result.realized_pnl, Some(dec!(52.50)));
        assert_eq!(result.net_position_after, dec!(5.25));
    }

    #[test]
    fn test_precision_with_rust_decimal() {
        let mut fifo = FifoInventory::new();

        fifo.process_trade(dec!(0.1), dec!(0.3), Direction::Buy)
            .unwrap();
        fifo.process_trade(dec!(0.2), dec!(0.3), Direction::Buy)
            .unwrap();

        let result = fifo
            .process_trade(dec!(0.3), dec!(0.6), Direction::Sell)
            .unwrap();

        assert_eq!(result.realized_pnl, Some(dec!(0.09)));
        assert_eq!(result.net_position_after, dec!(0));
    }

    #[test]
    fn test_invalid_quantity() {
        let mut fifo = FifoInventory::new();

        let result = fifo.process_trade(dec!(0), dec!(10.00), Direction::Buy);
        assert!(matches!(result, Err(PnlError::InvalidQuantity(_))));

        let result = fifo.process_trade(dec!(-10), dec!(10.00), Direction::Buy);
        assert!(matches!(result, Err(PnlError::InvalidQuantity(_))));
    }

    #[test]
    fn test_invalid_price() {
        let mut fifo = FifoInventory::new();

        let result = fifo.process_trade(dec!(10), dec!(0), Direction::Buy);
        assert!(matches!(result, Err(PnlError::InvalidPrice(_))));

        let result = fifo.process_trade(dec!(10), dec!(-5.00), Direction::Buy);
        assert!(matches!(result, Err(PnlError::InvalidPrice(_))));
    }

    #[test]
    fn test_multiple_reversals() {
        let mut fifo = FifoInventory::new();

        fifo.process_trade(dec!(100), dec!(10.00), Direction::Buy)
            .unwrap();

        let result = fifo
            .process_trade(dec!(150), dec!(11.00), Direction::Sell)
            .unwrap();
        assert_eq!(result.net_position_after, dec!(-50));

        let result = fifo
            .process_trade(dec!(100), dec!(10.50), Direction::Buy)
            .unwrap();
        assert_eq!(result.realized_pnl, Some(dec!(25.00)));
        assert_eq!(result.net_position_after, dec!(50));

        let result = fifo
            .process_trade(dec!(75), dec!(11.00), Direction::Sell)
            .unwrap();
        assert_eq!(result.realized_pnl, Some(dec!(25.00)));
        assert_eq!(result.net_position_after, dec!(-25));
    }

    #[test]
    fn test_partial_lot_consumption() {
        let mut fifo = FifoInventory::new();

        fifo.process_trade(dec!(100), dec!(10.00), Direction::Buy)
            .unwrap();

        let result = fifo
            .process_trade(dec!(30), dec!(11.00), Direction::Sell)
            .unwrap();
        assert_eq!(result.realized_pnl, Some(dec!(30.00)));
        assert_eq!(result.net_position_after, dec!(70));

        let result = fifo
            .process_trade(dec!(40), dec!(12.00), Direction::Sell)
            .unwrap();
        assert_eq!(result.realized_pnl, Some(dec!(80.00)));
        assert_eq!(result.cumulative_pnl, dec!(110.00));
        assert_eq!(result.net_position_after, dec!(30));
    }

    #[test]
    fn test_trade_type_from_str() {
        assert_eq!("ONCHAIN".parse::<TradeType>().unwrap(), TradeType::Onchain);
        assert_eq!(
            "OFFCHAIN".parse::<TradeType>().unwrap(),
            TradeType::Offchain
        );
        assert!("invalid".parse::<TradeType>().is_err());
    }

    #[test]
    fn test_trade_type_display() {
        assert_eq!(TradeType::Onchain.to_string(), "ONCHAIN");
        assert_eq!(TradeType::Offchain.to_string(), "OFFCHAIN");
    }

    proptest::proptest! {
        #[test]
        fn test_trade_type_roundtrip(trade_type in select(vec![TradeType::Onchain, TradeType::Offchain])) {
            let display_str = trade_type.to_string();
            let parsed: TradeType = display_str.parse().unwrap();
            assert_eq!(parsed, trade_type);
        }
    }
}

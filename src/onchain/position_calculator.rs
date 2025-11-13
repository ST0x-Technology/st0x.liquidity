use num_traits::ToPrimitive;

const SCHWAB_MINIMUM_WHOLE_SHARES: f64 = 1.0;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccumulationBucket {
    LongExposure,
    ShortExposure,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum ConversionError {
    #[error("Failed to convert u64 {value} to f64: precision loss would occur")]
    U64ToF64PrecisionLoss { value: u64 },

    #[error("Failed to convert f64 {value} to u64: value out of range or invalid")]
    F64ToU64OutOfRange { value: f64 },
}

/// Handles position tracking and threshold checking logic.
/// Separated from TradeAccumulator to follow single responsibility principle.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct PositionCalculator {
    pub(crate) accumulated_long: f64,
    pub(crate) accumulated_short: f64,
}

impl Default for PositionCalculator {
    fn default() -> Self {
        Self::new()
    }
}

impl PositionCalculator {
    pub(crate) const fn new() -> Self {
        Self {
            accumulated_long: 0.0,
            accumulated_short: 0.0,
        }
    }

    pub(crate) const fn with_positions(accumulated_long: f64, accumulated_short: f64) -> Self {
        Self {
            accumulated_long,
            accumulated_short,
        }
    }

    pub(crate) fn net_position(&self) -> f64 {
        self.accumulated_long - self.accumulated_short
    }

    pub(crate) fn determine_execution_type(&self) -> Option<AccumulationBucket> {
        let net = self.net_position();
        if net.abs() >= SCHWAB_MINIMUM_WHOLE_SHARES {
            if net > 0.0 {
                Some(AccumulationBucket::LongExposure) // Net long, need to SELL
            } else {
                Some(AccumulationBucket::ShortExposure) // Net short, need to BUY
            }
        } else {
            None
        }
    }

    pub(crate) fn add_trade(&mut self, amount: f64, direction: AccumulationBucket) {
        match direction {
            AccumulationBucket::LongExposure => {
                // Long exposure from onchain BUY -> accumulate for Schwab SELL to offset
                self.accumulated_long += amount;
            }
            AccumulationBucket::ShortExposure => {
                // Short exposure from onchain SELL -> accumulate for Schwab BUY to offset
                self.accumulated_short += amount;
            }
        }
    }

    pub(crate) fn reduce_accumulation(
        &mut self,
        execution_type: AccumulationBucket,
        shares: u64,
    ) -> Result<(), ConversionError> {
        let shares_f64 = shares
            .to_f64()
            .ok_or(ConversionError::U64ToF64PrecisionLoss { value: shares })?;

        match execution_type {
            AccumulationBucket::LongExposure => {
                self.accumulated_long -= shares_f64;
            }
            AccumulationBucket::ShortExposure => {
                self.accumulated_short -= shares_f64;
            }
        }

        Ok(())
    }

    pub(crate) fn calculate_executable_shares(&self) -> Result<u64, ConversionError> {
        let net = self.net_position().abs().floor();
        net.to_u64()
            .ok_or(ConversionError::F64ToU64OutOfRange { value: net })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_position_calculator_new() {
        let calc = PositionCalculator::new();
        assert!((calc.net_position() - 0.0).abs() < f64::EPSILON);
        assert!((calc.accumulated_long - 0.0).abs() < f64::EPSILON);
        assert!((calc.accumulated_short - 0.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_net_position_below_threshold_no_trigger() {
        // net=0.7 (long=1.5, short=0.8): Should NOT trigger
        let calc = PositionCalculator::with_positions(1.5, 0.8);
        assert!((calc.net_position() - 0.7).abs() < f64::EPSILON);
        assert!(calc.determine_execution_type().is_none());
    }

    #[test]
    fn test_net_position_negative_triggers_buy() {
        // net=-1.2 (long=0.3, short=1.5): Should trigger BUY
        let calc = PositionCalculator::with_positions(0.3, 1.5);
        assert!((calc.net_position() - (-1.2)).abs() < f64::EPSILON);
        assert_eq!(
            calc.determine_execution_type(),
            Some(AccumulationBucket::ShortExposure)
        );
        assert_eq!(calc.calculate_executable_shares().unwrap(), 1);
    }

    #[test]
    fn test_net_position_positive_triggers_sell() {
        // net=1.5 (long=2.0, short=0.5): Should trigger SELL
        let calc = PositionCalculator::with_positions(2.0, 0.5);
        assert!((calc.net_position() - 1.5).abs() < f64::EPSILON);
        assert_eq!(
            calc.determine_execution_type(),
            Some(AccumulationBucket::LongExposure)
        );
        assert_eq!(calc.calculate_executable_shares().unwrap(), 1);
    }

    #[test]
    fn test_net_position_large_negative_multiple_shares() {
        // net=-2.5: Should trigger BUY for 2 shares
        let calc = PositionCalculator::with_positions(0.5, 3.0);
        assert!((calc.net_position() - (-2.5)).abs() < f64::EPSILON);
        assert_eq!(
            calc.determine_execution_type(),
            Some(AccumulationBucket::ShortExposure)
        );
        assert_eq!(calc.calculate_executable_shares().unwrap(), 2);
    }

    #[test]
    fn test_net_position_exactly_one() {
        // net=1.0 exactly: Should trigger
        let calc = PositionCalculator::with_positions(1.0, 0.0);
        assert!((calc.net_position() - 1.0).abs() < f64::EPSILON);
        assert_eq!(
            calc.determine_execution_type(),
            Some(AccumulationBucket::LongExposure)
        );
        assert_eq!(calc.calculate_executable_shares().unwrap(), 1);
    }

    #[test]
    fn test_net_position_just_below_threshold() {
        // net=0.999: Should NOT trigger
        let calc = PositionCalculator::with_positions(0.999, 0.0);
        assert!((calc.net_position() - 0.999).abs() < f64::EPSILON);
        assert!(calc.determine_execution_type().is_none());
    }

    #[test]
    fn test_net_position_zero() {
        // net=0.0: Should NOT trigger
        let calc = PositionCalculator::with_positions(1.0, 1.0);
        assert!((calc.net_position() - 0.0).abs() < f64::EPSILON);
        assert!(calc.determine_execution_type().is_none());
    }

    #[test]
    fn test_net_position_large_positive_multiple_shares() {
        // net=3.7: Should trigger SELL for 3 shares
        let calc = PositionCalculator::with_positions(4.0, 0.3);
        assert!((calc.net_position() - 3.7).abs() < f64::EPSILON);
        assert_eq!(
            calc.determine_execution_type(),
            Some(AccumulationBucket::LongExposure)
        );
        assert_eq!(calc.calculate_executable_shares().unwrap(), 3);
    }

    #[test]
    fn test_add_trade_long_accumulation() {
        let mut calc = PositionCalculator::new();
        calc.add_trade(1.5, AccumulationBucket::LongExposure); // Long exposure from onchain BUY -> accumulate for Schwab SELL
        assert!((calc.accumulated_long - 1.5).abs() < f64::EPSILON);
        assert!((calc.accumulated_short - 0.0).abs() < f64::EPSILON);
        assert!((calc.net_position() - 1.5).abs() < f64::EPSILON);
    }

    #[test]
    fn test_add_trade_short_accumulation() {
        let mut calc = PositionCalculator::new();
        calc.add_trade(2.0, AccumulationBucket::ShortExposure); // Short exposure from onchain SELL -> accumulate for Schwab BUY
        assert!((calc.accumulated_long - 0.0).abs() < f64::EPSILON);
        assert!((calc.accumulated_short - 2.0).abs() < f64::EPSILON);
        assert!((calc.net_position() - (-2.0)).abs() < f64::EPSILON);
    }

    #[test]
    fn test_add_trade_zero_amount() {
        let mut calc = PositionCalculator::new();
        calc.add_trade(0.0, AccumulationBucket::LongExposure); // Zero amount but still affects direction
        assert!((calc.accumulated_long - 0.0).abs() < f64::EPSILON);
        assert!((calc.accumulated_short - 0.0).abs() < f64::EPSILON);
        assert!((calc.net_position() - 0.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_add_trade_mixed_directions() {
        let mut calc = PositionCalculator::new();
        calc.add_trade(1.5, AccumulationBucket::LongExposure); // Long accumulation
        calc.add_trade(2.0, AccumulationBucket::ShortExposure); // Short accumulation
        calc.add_trade(0.3, AccumulationBucket::LongExposure); // More long accumulation

        assert!((calc.accumulated_long - 1.8).abs() < f64::EPSILON); // 1.5 + 0.3
        assert!((calc.accumulated_short - 2.0).abs() < f64::EPSILON); // 2.0
        assert!((calc.net_position() - (-0.2)).abs() < f64::EPSILON); // 1.8 - 2.0 = -0.2
    }

    #[test]
    fn test_reduce_accumulation() {
        let mut calc = PositionCalculator::with_positions(2.5, 3.0);
        calc.reduce_accumulation(AccumulationBucket::LongExposure, 2)
            .unwrap();
        assert!((calc.accumulated_long - 0.5).abs() < f64::EPSILON);
        assert!((calc.net_position() - (-2.5)).abs() < f64::EPSILON); // 0.5 - 3.0 = -2.5

        calc.reduce_accumulation(AccumulationBucket::ShortExposure, 1)
            .unwrap();
        assert!((calc.accumulated_short - 2.0).abs() < f64::EPSILON);
        assert!((calc.net_position() - (-1.5)).abs() < f64::EPSILON); // 0.5 - 2.0 = -1.5
    }

    #[test]
    fn test_calculate_executable_shares() {
        // Test positive net position
        let calc = PositionCalculator::with_positions(2.7, 0.0);
        assert_eq!(calc.calculate_executable_shares().unwrap(), 2);

        // Test negative net position
        let calc = PositionCalculator::with_positions(0.0, 3.2);
        assert_eq!(calc.calculate_executable_shares().unwrap(), 3);

        // Test zero net position
        let calc = PositionCalculator::with_positions(1.0, 1.0);
        assert_eq!(calc.calculate_executable_shares().unwrap(), 0);
    }
}

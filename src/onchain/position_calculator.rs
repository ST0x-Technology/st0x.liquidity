//! Classifies onchain trades into long/short exposure buckets
//! for position accumulation.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccumulationBucket {
    LongExposure,
    ShortExposure,
}

/// Legacy position calculator for tracking accumulated positions.
///
/// This struct tracks accumulated long and short positions. Execution decisions
/// are now made by the event-sourced `Position` aggregate which supports
/// configurable thresholds (shares-based or dollar-value-based).
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

    pub(crate) fn reduce_accumulation(&mut self, execution_type: AccumulationBucket, shares: f64) {
        match execution_type {
            AccumulationBucket::LongExposure => {
                self.accumulated_long -= shares;
            }
            AccumulationBucket::ShortExposure => {
                self.accumulated_short -= shares;
            }
        }
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
    fn test_net_position_calculation() {
        let calc = PositionCalculator::with_positions(2.0, 0.5);
        assert!((calc.net_position() - 1.5).abs() < f64::EPSILON);

        let calc = PositionCalculator::with_positions(0.3, 1.5);
        assert!((calc.net_position() - (-1.2)).abs() < f64::EPSILON);
    }

    #[test]
    fn test_add_trade_long_accumulation() {
        let mut calc = PositionCalculator::new();
        calc.add_trade(1.5, AccumulationBucket::LongExposure);
        assert!((calc.accumulated_long - 1.5).abs() < f64::EPSILON);
        assert!((calc.accumulated_short - 0.0).abs() < f64::EPSILON);
        assert!((calc.net_position() - 1.5).abs() < f64::EPSILON);
    }

    #[test]
    fn test_add_trade_short_accumulation() {
        let mut calc = PositionCalculator::new();
        calc.add_trade(2.0, AccumulationBucket::ShortExposure);
        assert!((calc.accumulated_long - 0.0).abs() < f64::EPSILON);
        assert!((calc.accumulated_short - 2.0).abs() < f64::EPSILON);
        assert!((calc.net_position() - (-2.0)).abs() < f64::EPSILON);
    }

    #[test]
    fn test_add_trade_mixed_directions() {
        let mut calc = PositionCalculator::new();
        calc.add_trade(1.5, AccumulationBucket::LongExposure);
        calc.add_trade(2.0, AccumulationBucket::ShortExposure);
        calc.add_trade(0.3, AccumulationBucket::LongExposure);

        assert!((calc.accumulated_long - 1.8).abs() < f64::EPSILON);
        assert!((calc.accumulated_short - 2.0).abs() < f64::EPSILON);
        assert!((calc.net_position() - (-0.2)).abs() < f64::EPSILON);
    }

    #[test]
    fn test_reduce_accumulation() {
        let mut calc = PositionCalculator::with_positions(2.5, 3.0);
        calc.reduce_accumulation(AccumulationBucket::LongExposure, 2.0);
        assert!((calc.accumulated_long - 0.5).abs() < f64::EPSILON);
        assert!((calc.net_position() - (-2.5)).abs() < f64::EPSILON);

        calc.reduce_accumulation(AccumulationBucket::ShortExposure, 1.0);
        assert!((calc.accumulated_short - 2.0).abs() < f64::EPSILON);
        assert!((calc.net_position() - (-1.5)).abs() < f64::EPSILON);
    }
}

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(transparent)]
pub(crate) struct FractionalShares(pub(crate) Decimal);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, thiserror::Error)]
#[error("arithmetic overflow: {lhs:?} {operation} {rhs:?}")]
pub(crate) struct ArithmeticError<T> {
    pub(crate) operation: String,
    pub(crate) lhs: T,
    pub(crate) rhs: T,
}

pub(crate) trait HasZero: PartialOrd + Sized {
    const ZERO: Self;

    fn is_zero(&self) -> bool {
        self == &Self::ZERO
    }

    fn is_negative(&self) -> bool {
        self < &Self::ZERO
    }
}

impl HasZero for FractionalShares {
    const ZERO: Self = Self(Decimal::ZERO);
}

impl From<FractionalShares> for Decimal {
    fn from(value: FractionalShares) -> Self {
        value.0
    }
}

impl std::ops::Mul<Decimal> for FractionalShares {
    type Output = Result<Self, ArithmeticError<Self>>;

    fn mul(self, rhs: Decimal) -> Self::Output {
        self.0
            .checked_mul(rhs)
            .map(Self)
            .ok_or_else(|| ArithmeticError {
                operation: "*".to_string(),
                lhs: self,
                rhs: Self(rhs),
            })
    }
}

impl FractionalShares {
    #[cfg(test)]
    pub(crate) const ONE: Self = Self(Decimal::ONE);

    pub(crate) fn abs(self) -> Self {
        Self(self.0.abs())
    }
}

impl std::ops::Add for FractionalShares {
    type Output = Result<Self, ArithmeticError<Self>>;

    fn add(self, rhs: Self) -> Self::Output {
        self.0
            .checked_add(rhs.0)
            .map(Self)
            .ok_or_else(|| ArithmeticError {
                operation: "+".to_string(),
                lhs: self,
                rhs,
            })
    }
}

impl std::ops::Sub for FractionalShares {
    type Output = Result<Self, ArithmeticError<Self>>;

    fn sub(self, rhs: Self) -> Self::Output {
        self.0
            .checked_sub(rhs.0)
            .map(Self)
            .ok_or_else(|| ArithmeticError {
                operation: "-".to_string(),
                lhs: self,
                rhs,
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn add_succeeds() {
        let a = FractionalShares(Decimal::ONE);
        let b = FractionalShares(Decimal::TWO);

        let result = (a + b).unwrap();

        assert_eq!(result.0, Decimal::from(3));
    }

    #[test]
    fn sub_succeeds() {
        let a = FractionalShares(Decimal::from(5));
        let b = FractionalShares(Decimal::TWO);

        let result = (a - b).unwrap();

        assert_eq!(result.0, Decimal::from(3));
    }

    #[test]
    fn add_overflow_returns_error() {
        let max = FractionalShares(Decimal::MAX);
        let one = FractionalShares(Decimal::ONE);

        let result = max + one;

        let err = result.unwrap_err();
        assert_eq!(err.operation, "+");
        assert_eq!(err.lhs, max);
        assert_eq!(err.rhs, one);
    }

    #[test]
    fn sub_overflow_returns_error() {
        let min = FractionalShares(Decimal::MIN);
        let one = FractionalShares(Decimal::ONE);

        let result = min - one;

        let err = result.unwrap_err();
        assert_eq!(err.operation, "-");
        assert_eq!(err.lhs, min);
        assert_eq!(err.rhs, one);
    }

    #[test]
    fn abs_returns_absolute_value() {
        let negative = FractionalShares(Decimal::NEGATIVE_ONE);
        assert_eq!(negative.abs().0, Decimal::ONE);
    }

    #[test]
    fn into_decimal_extracts_inner_value() {
        let shares = FractionalShares(Decimal::from(42));
        let decimal: Decimal = shares.into();
        assert_eq!(decimal, Decimal::from(42));
    }

    #[test]
    fn mul_decimal_succeeds() {
        let shares = FractionalShares(Decimal::from(100));
        let ratio = Decimal::new(5, 1); // 0.5

        let result = (shares * ratio).unwrap();

        assert_eq!(result.0, Decimal::from(50));
    }

    #[test]
    fn mul_decimal_overflow_returns_error() {
        let max = FractionalShares(Decimal::MAX);
        let two = Decimal::TWO;

        let err = (max * two).unwrap_err();

        assert_eq!(err.operation, "*");
        assert_eq!(err.lhs, max);
        assert_eq!(err.rhs, FractionalShares(two));
    }
}

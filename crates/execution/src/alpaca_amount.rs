//! Crypto amounts received from Alpaca API responses.

use rain_math_float::{Float, FloatError};
use serde::Deserialize;
use std::fmt::{Debug, Display};

use st0x_finance::{HasZero, Usdc, UsdcConversionError};

/// A crypto amount normalized at the Alpaca response boundary.
///
/// Alpaca reports crypto quantities with up to nine decimals while on-chain
/// USDC has six. Construction always floors onto that grid, and only the
/// normalized amount can leave the boundary. The raw amount remains private
/// for exact broker-side cash valuation.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct AlpacaAmount {
    raw: Usdc,
    normalized: Usdc,
}

impl AlpacaAmount {
    #[must_use]
    pub fn into_normalized(self) -> Float {
        self.normalized.inner()
    }

    pub fn is_zero(&self) -> Result<bool, FloatError> {
        self.normalized.is_zero()
    }

    pub fn cash_value_at(self, price: Float) -> Result<Usdc, FloatError> {
        self.raw * price
    }
}

impl TryFrom<Float> for AlpacaAmount {
    type Error = UsdcConversionError;

    fn try_from(amount: Float) -> Result<Self, Self::Error> {
        let raw = Usdc::new(amount);
        let normalized = raw.floor_to_6_decimals()?;

        Ok(Self { raw, normalized })
    }
}

impl<'de> Deserialize<'de> for AlpacaAmount {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let amount = Usdc::deserialize(deserializer).map_err(|error| {
            serde::de::Error::custom(format_args!("Invalid Alpaca crypto Float: {error}"))
        })?;
        Self::try_from(amount.inner()).map_err(serde::de::Error::custom)
    }
}

impl Debug for AlpacaAmount {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_tuple("AlpacaAmount")
            .field(&self.normalized)
            .finish()
    }
}

impl Display for AlpacaAmount {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.normalized, formatter)
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use st0x_finance::Usdc;
    use st0x_float_macro::float;

    use super::AlpacaAmount;

    #[test]
    fn string_amount_is_floored_to_six_decimals() {
        let amount: AlpacaAmount = serde_json::from_value(json!("9.794019706")).unwrap();

        assert_eq!(
            Usdc::new(amount.into_normalized()),
            Usdc::new(float!(9.794019))
        );
    }

    #[test]
    fn numeric_amount_is_floored_to_six_decimals() {
        let amount: AlpacaAmount = serde_json::from_value(json!(9.794_019_706)).unwrap();

        assert_eq!(
            Usdc::new(amount.into_normalized()),
            Usdc::new(float!(9.794019))
        );
    }

    #[test]
    fn display_uses_the_normalized_amount() {
        let amount: AlpacaAmount = serde_json::from_value(json!("9.794019706")).unwrap();

        assert_eq!(amount.to_string(), "9.794019");
    }

    #[test]
    fn negative_amount_is_rejected() {
        let error = serde_json::from_value::<AlpacaAmount>(json!("-1.000000001")).unwrap_err();

        assert!(error.to_string().contains("cannot be negative"));
    }

    #[test]
    fn cash_valuation_preserves_the_raw_broker_amount() {
        let amount: AlpacaAmount = serde_json::from_value(json!("9.794019706")).unwrap();

        assert_eq!(
            amount.cash_value_at(float!(1.00101001)).unwrap(),
            Usdc::new(float!(9.80391176384325706))
        );
    }
}

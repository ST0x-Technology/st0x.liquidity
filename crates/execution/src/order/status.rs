/// Flat enum for database storage (matches CHECK constraint pattern)
#[derive(Debug, Clone, Copy, PartialEq, Eq, sqlx::Type)]
pub enum OrderStatus {
    Pending,
    Submitted,
    PartiallyFilled,
    Filled,
    Cancelled,
    Failed,
}

impl OrderStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "PENDING",
            Self::Submitted => "SUBMITTED",
            Self::PartiallyFilled => "PARTIALLY_FILLED",
            Self::Filled => "FILLED",
            Self::Cancelled => "CANCELLED",
            Self::Failed => "FAILED",
        }
    }
}

impl std::fmt::Display for OrderStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ParseOrderStatusError {
    #[error(
        "invalid order status: '{status_provided}'. Expected one of: \
         PENDING, SUBMITTED, PARTIALLY_FILLED, FILLED, CANCELLED, FAILED"
    )]
    InvalidStatus { status_provided: String },
}

impl std::str::FromStr for OrderStatus {
    type Err = ParseOrderStatusError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "PENDING" => Ok(Self::Pending),
            "SUBMITTED" => Ok(Self::Submitted),
            "PARTIALLY_FILLED" => Ok(Self::PartiallyFilled),
            "FILLED" => Ok(Self::Filled),
            "CANCELLED" => Ok(Self::Cancelled),
            "FAILED" => Ok(Self::Failed),
            _ => Err(ParseOrderStatusError::InvalidStatus {
                status_provided: s.to_string(),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_all_variants() {
        let pairs = [
            ("PENDING", OrderStatus::Pending),
            ("SUBMITTED", OrderStatus::Submitted),
            ("PARTIALLY_FILLED", OrderStatus::PartiallyFilled),
            ("FILLED", OrderStatus::Filled),
            ("CANCELLED", OrderStatus::Cancelled),
            ("FAILED", OrderStatus::Failed),
        ];

        for (string, variant) in pairs {
            assert_eq!(
                string.parse::<OrderStatus>().unwrap(),
                variant,
                "parse failed for {string}"
            );
            assert_eq!(variant.as_str(), string, "as_str failed for {variant:?}");
        }
    }

    #[test]
    fn unknown_string_returns_invalid_status_error() {
        let error = "NONSENSE".parse::<OrderStatus>().unwrap_err();
        assert!(
            matches!(error, ParseOrderStatusError::InvalidStatus { .. }),
            "expected InvalidStatus, got {error:?}"
        );
    }
}

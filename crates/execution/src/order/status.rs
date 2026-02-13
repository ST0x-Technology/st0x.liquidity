/// Flat enum for database storage (matches CHECK constraint pattern)
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum OrderStatus {
    Pending,
    Submitted,
    Filled,
    Failed,
}

impl OrderStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "PENDING",
            Self::Submitted => "SUBMITTED",
            Self::Filled => "FILLED",
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
        "Invalid order status: '{0}'. Expected one of: \
         PENDING, SUBMITTED, FILLED, FAILED"
    )]
    InvalidStatus(String),
}

impl std::str::FromStr for OrderStatus {
    type Err = ParseOrderStatusError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "PENDING" => Ok(Self::Pending),
            "SUBMITTED" => Ok(Self::Submitted),
            "FILLED" => Ok(Self::Filled),
            "FAILED" => Ok(Self::Failed),
            _ => Err(ParseOrderStatusError::InvalidStatus(s.to_string())),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    #[test]
    fn serde_round_trip() {
        for status in [
            OrderStatus::Pending,
            OrderStatus::Submitted,
            OrderStatus::Filled,
            OrderStatus::Failed,
        ] {
            let json = serde_json::to_string(&status).unwrap();
            let deserialized: OrderStatus = serde_json::from_str(&json).unwrap();
            assert_eq!(status, deserialized, "round-trip failed for {status}");
        }
    }

    #[test]
    fn display_from_str_round_trip() {
        for status in [
            OrderStatus::Pending,
            OrderStatus::Submitted,
            OrderStatus::Filled,
            OrderStatus::Failed,
        ] {
            let displayed = status.to_string();
            let parsed = OrderStatus::from_str(&displayed).unwrap();
            assert_eq!(status, parsed, "round-trip failed for {displayed}");
        }
    }

    #[test]
    fn from_str_invalid() {
        let err = OrderStatus::from_str("UNKNOWN").unwrap_err();
        assert!(matches!(err, ParseOrderStatusError::InvalidStatus(s) if s == "UNKNOWN"));
    }
}

//! Validated, byte-preserving reasons for persisted operator recovery events.

use std::fmt;
use std::str::FromStr;

/// A non-blank operator-supplied reason retained for the persisted audit trail.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuditReason(String);

impl FromStr for AuditReason {
    type Err = String;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        if input.trim().is_empty() {
            return Err(
                "--reason must not be blank; it is persisted as the audit record".to_string(),
            );
        }

        Ok(Self(input.to_string()))
    }
}

impl AsRef<str> for AuditReason {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for AuditReason {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl From<AuditReason> for String {
    fn from(reason: AuditReason) -> Self {
        reason.0
    }
}

#[cfg(test)]
mod tests {
    use super::AuditReason;

    #[test]
    fn rejects_empty_and_whitespace_only_values() {
        for value in ["", " ", "\t\n"] {
            assert_eq!(
                value.parse::<AuditReason>(),
                Err("--reason must not be blank; it is persisted as the audit record".to_string())
            );
        }
    }

    #[test]
    fn preserves_accepted_text_byte_for_byte() {
        let input = "  provider case #42  ";
        let reason = input.parse::<AuditReason>().unwrap();

        assert_eq!(reason.as_ref(), input);
        assert_eq!(reason.to_string(), input);
        assert_eq!(String::from(reason), input);
    }
}

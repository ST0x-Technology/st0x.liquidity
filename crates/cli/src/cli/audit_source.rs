//! Validated, byte-preserving provenance for persisted operator corrections.

use std::fmt;
use std::str::FromStr;

/// A non-blank operator-supplied source retained as the provenance of a
/// persisted correction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuditSource(String);

impl FromStr for AuditSource {
    type Err = String;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        if input.trim().is_empty() {
            return Err(
                "--source must not be blank; it is persisted as audit provenance".to_string(),
            );
        }

        Ok(Self(input.to_string()))
    }
}

impl AsRef<str> for AuditSource {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for AuditSource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl From<AuditSource> for String {
    fn from(source: AuditSource) -> Self {
        source.0
    }
}

#[cfg(test)]
mod tests {
    use super::AuditSource;

    #[test]
    fn rejects_empty_and_whitespace_only_values() {
        for value in ["", " ", "\t\n"] {
            assert_eq!(
                value.parse::<AuditSource>(),
                Err("--source must not be blank; it is persisted as audit provenance".to_string())
            );
        }
    }

    #[test]
    fn preserves_accepted_text_byte_for_byte() {
        let input = "  Nasdaq historical close  ";
        let source = input.parse::<AuditSource>().unwrap();

        assert_eq!(source.as_ref(), input);
        assert_eq!(source.to_string(), input);
        assert_eq!(String::from(source), input);
    }
}

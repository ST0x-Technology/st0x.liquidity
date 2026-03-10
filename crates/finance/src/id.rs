//! Phantom-tagged identifier for type-safe IDs across domain boundaries.

use serde::{Deserialize, Serialize};
use std::fmt::{Debug, Display};
use std::marker::PhantomData;
use std::str::FromStr;

/// Validation error for blank identifier strings.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("identifier cannot be blank")]
pub struct BlankIdError;

/// Phantom-tagged identifier. Type-safe in Rust, serializes as plain string.
///
/// Tag types are defined by consumers (DTO crate, domain crate), not here.
#[derive(Serialize)]
#[serde(transparent)]
pub struct Id<Tag: ?Sized> {
    value: String,
    #[serde(skip)]
    _tag: PhantomData<fn() -> Tag>,
}

impl<Tag: ?Sized> Clone for Id<Tag> {
    fn clone(&self) -> Self {
        Self {
            value: self.value.clone(),
            _tag: PhantomData,
        }
    }
}

impl<Tag: ?Sized> PartialEq for Id<Tag> {
    fn eq(&self, other: &Self) -> bool {
        self.value == other.value
    }
}

impl<Tag: ?Sized> Eq for Id<Tag> {}

impl<Tag: ?Sized> std::hash::Hash for Id<Tag> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.value.hash(state);
    }
}

impl<Tag: ?Sized> Id<Tag> {
    pub fn new(value: impl Into<String>) -> Self {
        Self {
            value: value.into(),
            _tag: PhantomData,
        }
    }
}

impl<Tag: ?Sized> Debug for Id<Tag> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Id({})", self.value)
    }
}

impl<Tag: ?Sized> Display for Id<Tag> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.value)
    }
}

impl<Tag: ?Sized> AsRef<str> for Id<Tag> {
    fn as_ref(&self) -> &str {
        &self.value
    }
}

impl<Tag: ?Sized> FromStr for Id<Tag> {
    type Err = BlankIdError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.trim().is_empty() {
            return Err(BlankIdError);
        }
        Ok(Self::new(s))
    }
}

impl<'de, Tag: ?Sized> Deserialize<'de> for Id<Tag> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        if value.trim().is_empty() {
            return Err(serde::de::Error::custom(BlankIdError));
        }
        Ok(Self::new(value))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    enum TestTag {}

    #[test]
    fn new_and_display() {
        let id: Id<TestTag> = Id::new("abc-123");
        assert_eq!(id.to_string(), "abc-123");
    }

    #[test]
    fn as_ref_str() {
        let id: Id<TestTag> = Id::new("hello");
        assert_eq!(id.as_ref(), "hello");
    }

    #[test]
    fn from_str_roundtrip() {
        let id: Id<TestTag> = "test-id".parse().unwrap();
        assert_eq!(id.to_string(), "test-id");
    }

    #[test]
    fn from_str_rejects_empty() {
        let result = "".parse::<Id<TestTag>>();
        assert_eq!(result.unwrap_err(), BlankIdError);
    }

    #[test]
    fn from_str_rejects_whitespace_only() {
        let result = "   ".parse::<Id<TestTag>>();
        assert_eq!(result.unwrap_err(), BlankIdError);
    }

    #[test]
    fn serde_roundtrip_json() {
        let id: Id<TestTag> = Id::new("ser-test");
        let json = serde_json::to_string(&id).unwrap();
        assert_eq!(json, r#""ser-test""#);

        let roundtripped: Id<TestTag> = serde_json::from_str(&json).unwrap();
        assert_eq!(id, roundtripped);
    }

    #[test]
    fn deserialize_rejects_empty() {
        let result: Result<Id<TestTag>, _> = serde_json::from_str(r#""""#);
        assert!(result.is_err());
    }

    #[test]
    fn deserialize_rejects_whitespace_only() {
        let result: Result<Id<TestTag>, _> = serde_json::from_str(r#""   ""#);
        assert!(result.is_err());
    }

    #[test]
    fn different_tags_are_distinct_types() {
        enum OtherTag {}

        let id_a: Id<TestTag> = Id::new("same-value");
        let id_b: Id<OtherTag> = Id::new("same-value");

        // They hold the same string but are different types
        assert_eq!(id_a.as_ref(), id_b.as_ref());
        // This wouldn't compile: assert_eq!(id_a, id_b);
    }

    #[test]
    fn equality_and_hash() {
        use std::collections::HashSet;

        let id1: Id<TestTag> = Id::new("x");
        let id2: Id<TestTag> = Id::new("x");
        let id3: Id<TestTag> = Id::new("y");

        assert_eq!(id1, id2);
        assert_ne!(id1, id3);

        let mut set = HashSet::new();
        set.insert(id1);
        assert!(set.contains(&id2));
        assert!(!set.contains(&id3));
    }
}

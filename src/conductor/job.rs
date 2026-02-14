//! Job trait and supporting types for apalis-based task processing.
//!
//! Each job is a serializable struct representing a unit of work. Its
//! associated `Ctx` type bundles all dependencies into a single struct
//! rather than using multiple `Data<T>` extractors. See
//! [naming.md](../docs/naming.md) for the `*Job` naming convention.

use apalis::prelude::Data;
use serde::{Serialize, de::DeserializeOwned};
use std::fmt::Debug;

pub(crate) trait Job:
    Serialize + DeserializeOwned + Send + Sync + Unpin + Debug + 'static
{
    type Ctx: Clone + Send + 'static;
    type Error: std::error::Error + Send + Sync + 'static;

    fn label(&self) -> Label;

    async fn run(self, ctx: Data<Self::Ctx>) -> Result<(), Self::Error>;
}

pub(crate) struct Label(String);

impl Label {
    pub(crate) fn new(label: impl Into<String>) -> Self {
        Self(label.into())
    }
}

impl std::fmt::Display for Label {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

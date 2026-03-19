//! [`Job`] implementation for [`MockBrokerCommand`].
//!
//! Bridges the execution crate's mock broker commands with the conductor's
//! job infrastructure, enabling e2e tests to control mock behavior through
//! the same Apalis job queue the production system uses.

use tracing::debug;

use st0x_execution::alpaca_broker_api::{AlpacaBrokerMock, MockBrokerCommand};

use crate::conductor::job::{Job, Label};

impl Job<AlpacaBrokerMock> for MockBrokerCommand {
    type Error = std::convert::Infallible;

    fn label(&self) -> Label {
        Label::new(format!("mock:{self:?}"))
    }

    async fn perform(&self, broker: &AlpacaBrokerMock) -> Result<(), Self::Error> {
        debug!(?self, "Applying mock broker command");
        self.clone().apply(broker);
        Ok(())
    }
}

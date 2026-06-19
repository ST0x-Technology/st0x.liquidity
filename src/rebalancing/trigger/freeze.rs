//! Dividend freeze-status gate for the rebalancing trigger.
//!
//! The liquidity bot must not initiate a rebalancing flow for an asset that
//! issuance has frozen for a dividend: the equity redemption saga sends tokens
//! on-chain before issuance is consulted, so a flow started for a frozen asset
//! strands funds. The trigger therefore reads the asset's freeze status before
//! dispatching and skips frozen assets, failing closed when the status cannot
//! be confirmed.

use async_trait::async_trait;

use st0x_execution::Symbol;
use st0x_issuance_client::{ClientError, IssuanceClient};
use st0x_issuance_dto::{TokenizedAssetStatus, UnderlyingSymbol};

/// Reads an asset's dividend freeze status so the rebalancing trigger can skip
/// frozen assets before starting a flow.
#[async_trait]
pub(crate) trait FreezeStatusReader: Send + Sync {
    /// Returns whether `symbol` is currently frozen for a dividend.
    ///
    /// # Errors
    ///
    /// Returns [`FreezeCheckError`] when the status cannot be determined (the
    /// service is unreachable/errored, or the asset is unknown to issuance);
    /// callers fail closed on any error rather than risk rebalancing a frozen
    /// asset.
    async fn is_frozen(&self, symbol: &Symbol) -> Result<bool, FreezeCheckError>;
}

/// Why a freeze-status check could not produce a definitive answer. The trigger
/// fails closed (skips and alerts) on any of these.
#[derive(Debug, thiserror::Error)]
pub(crate) enum FreezeCheckError {
    /// Issuance returned 404 -- it does not recognise an asset the bot
    /// rebalances, so the bot cannot confirm the asset is not frozen.
    #[error("issuance does not recognize asset {symbol}")]
    AssetUnknown { symbol: Symbol },
    /// The status request itself failed (transport error, unexpected status).
    #[error("issuance freeze-status request failed")]
    Client(#[from] ClientError),
}

#[async_trait]
impl FreezeStatusReader for IssuanceClient {
    async fn is_frozen(&self, symbol: &Symbol) -> Result<bool, FreezeCheckError> {
        let underlying = UnderlyingSymbol::new(symbol.to_string());
        match self.tokenized_asset_status(&underlying).await? {
            Some(response) => Ok(match response.status {
                TokenizedAssetStatus::Frozen => true,
                TokenizedAssetStatus::Enabled => false,
            }),
            None => Err(FreezeCheckError::AssetUnknown {
                symbol: symbol.clone(),
            }),
        }
    }
}

/// Test/`test-support` freeze reader with a fixed outcome, so trigger tests can
/// exercise the gate without a live issuance service.
#[cfg(test)]
#[derive(Clone, Copy)]
pub(crate) enum StubFreezeReader {
    /// Asset is not frozen -- rebalancing proceeds.
    NotFrozen,
    /// Asset is frozen -- the trigger skips it.
    Frozen,
    /// The status cannot be confirmed -- the trigger fails closed.
    Indeterminate,
}

#[cfg(test)]
#[async_trait]
impl FreezeStatusReader for StubFreezeReader {
    async fn is_frozen(&self, symbol: &Symbol) -> Result<bool, FreezeCheckError> {
        match self {
            Self::NotFrozen => Ok(false),
            Self::Frozen => Ok(true),
            Self::Indeterminate => Err(FreezeCheckError::AssetUnknown {
                symbol: symbol.clone(),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use httpmock::{Method::GET, MockServer};
    use serde_json::json;
    use url::Url;

    use super::*;

    fn client_for(server: &MockServer) -> IssuanceClient {
        IssuanceClient::new(
            Url::parse(&server.base_url()).expect("valid mock URL"),
            "test-key",
        )
        .expect("client must build")
    }

    #[tokio::test]
    async fn is_frozen_maps_a_frozen_asset_to_true() {
        let server = MockServer::start_async().await;
        let mock = server.mock(|when, then| {
            when.method(GET).path("/tokenized-assets/AAPL/status");
            then.status(200).json_body(json!({
                "underlying": "AAPL",
                "status": "frozen",
            }));
        });

        let frozen = client_for(&server)
            .is_frozen(&Symbol::new("AAPL").unwrap())
            .await
            .expect("a 200 response must resolve");

        mock.assert();
        assert!(frozen, "issuance reporting frozen must map to Ok(true)");
    }

    #[tokio::test]
    async fn is_frozen_maps_an_unfrozen_asset_to_false() {
        let server = MockServer::start_async().await;
        server.mock(|when, then| {
            when.method(GET).path("/tokenized-assets/AAPL/status");
            then.status(200).json_body(json!({
                "underlying": "AAPL",
                "status": "enabled",
            }));
        });

        let frozen = client_for(&server)
            .is_frozen(&Symbol::new("AAPL").unwrap())
            .await
            .expect("a 200 response must resolve");

        assert!(
            !frozen,
            "issuance reporting not-frozen must map to Ok(false)"
        );
    }

    #[tokio::test]
    async fn is_frozen_fails_closed_when_issuance_does_not_recognize_the_asset() {
        let server = MockServer::start_async().await;
        server.mock(|when, then| {
            when.method(GET).path("/tokenized-assets/AAPL/status");
            then.status(404);
        });

        let error = client_for(&server)
            .is_frozen(&Symbol::new("AAPL").unwrap())
            .await
            .expect_err("a 404 must fail closed, never resolve to Ok(false)");

        assert!(
            matches!(
                error,
                FreezeCheckError::AssetUnknown { ref symbol } if *symbol == Symbol::new("AAPL").unwrap()
            ),
            "a 404 must map to AssetUnknown so the trigger fails closed, got: {error:?}"
        );
    }

    #[tokio::test]
    async fn is_frozen_fails_closed_on_a_status_error() {
        let server = MockServer::start_async().await;
        server.mock(|when, then| {
            when.method(GET).path("/tokenized-assets/AAPL/status");
            then.status(500);
        });

        let error = client_for(&server)
            .is_frozen(&Symbol::new("AAPL").unwrap())
            .await
            .expect_err("a 500 must fail closed, never resolve to Ok(false)");

        assert!(
            matches!(error, FreezeCheckError::Client(_)),
            "a server error must map to Client so the trigger fails closed, got: {error:?}"
        );
    }
}

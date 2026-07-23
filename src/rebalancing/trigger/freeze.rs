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
    #[error("issuance freeze-status request failed: {0}")]
    Client(FreezeClientFailure),
}

/// Coarse, endpoint-free classification of a failed issuance status request.
///
/// The issuance endpoint lives in the encrypted secrets (issuance is not on
/// the internal network), and [`ClientError`] embeds the request URL and
/// socket address in both `Debug` and `Display`. The trigger logs freeze-check
/// errors, so the raw client error is reduced to this classification at the
/// boundary and never reaches logs.
#[derive(Debug, thiserror::Error)]
pub(crate) enum FreezeClientFailure {
    #[error("HTTP client could not be built")]
    Build,
    #[error("configured base URL is not a valid base")]
    InvalidBaseUrl,
    #[error("request timed out")]
    Timeout,
    #[error("connection failed")]
    Connect,
    #[error("transport error")]
    Transport,
    #[error("failed to parse the response body")]
    ParseResponse,
    /// Raw status code: the client does not re-export its `StatusCode` type,
    /// and naming either of the workspace's two reqwest majors here would tie
    /// this enum to the client's private dependency version.
    #[error("unexpected status {0}")]
    UnexpectedStatus(u16),
}

impl From<ClientError> for FreezeCheckError {
    fn from(error: ClientError) -> Self {
        Self::Client(match error {
            ClientError::Build(_) => FreezeClientFailure::Build,
            ClientError::NotABase { .. } => FreezeClientFailure::InvalidBaseUrl,
            ClientError::Http(http) if http.is_timeout() => FreezeClientFailure::Timeout,
            ClientError::Http(http) if http.is_connect() => FreezeClientFailure::Connect,
            ClientError::Http(_) => FreezeClientFailure::Transport,
            ClientError::ParseResponse(_) => FreezeClientFailure::ParseResponse,
            ClientError::Status { status, .. } => {
                FreezeClientFailure::UnexpectedStatus(status.as_u16())
            }
        })
    }
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

    /// The issuance endpoint lives in the encrypted secrets (issuance is not
    /// on the internal network), and the trigger logs freeze-check errors with
    /// `?error`. A connect failure must therefore never surface the endpoint
    /// host in the error's debug representation.
    #[tokio::test]
    async fn freeze_check_error_never_leaks_the_endpoint_on_connect_failure() {
        let client = IssuanceClient::new(
            Url::parse("http://127.0.0.1:9").expect("valid URL"),
            "test-key",
        )
        .expect("client must build");

        let error = client
            .is_frozen(&Symbol::new("AAPL").unwrap())
            .await
            .expect_err("port 9 must refuse the connection");

        let debug = format!("{error:?}");
        assert!(
            !debug.contains("127.0.0.1"),
            "freeze-check error debug must not leak the issuance endpoint, \
             got: {debug}"
        );
    }

    /// Same redaction requirement for the unexpected-status path, whose client
    /// error carries the request URL as a field.
    #[tokio::test]
    async fn freeze_check_error_never_leaks_the_endpoint_on_status_error() {
        let server = MockServer::start_async().await;
        server.mock(|when, then| {
            when.method(GET).path("/tokenized-assets/AAPL/status");
            then.status(500);
        });

        let error = client_for(&server)
            .is_frozen(&Symbol::new("AAPL").unwrap())
            .await
            .expect_err("a 500 must fail closed");

        let debug = format!("{error:?}");
        assert!(
            !debug.contains("127.0.0.1") && !debug.contains(&server.address().port().to_string()),
            "freeze-check error debug must not leak the issuance endpoint, \
             got: {debug}"
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

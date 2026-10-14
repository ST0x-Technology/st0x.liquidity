//! JSON-RPC clients for the configured chains, built through `st0x-rpc`.
//!
//! A bare `https://<network>.g.alchemy.com/v2` `rpc_url` is called with an
//! ES256 JWT signed by the Cloud KMS key named in `ALCHEMY_JWT_KMS_KEY_VERSION`,
//! under the Alchemy key id in `ALCHEMY_JWT_KID`. Any other URL is called as
//! plain HTTP. Either way redirects are not followed and errors never carry
//! the URL, which can hold an API key.

use alloy::rpc::client::{ClientBuilder, RpcClient};
use alloy::transports::{BoxTransport, IntoBoxTransport};
use st0x_rpc::JwtEnv;
use tower::Layer;
use url::Url;

pub use st0x_rpc::{Error as ChainRpcError, Timeouts as ChainRpcTimeouts};

/// A client for the chain RPC at `url`, reading the JWT settings from the
/// process environment, with `layer` wrapped around the HTTP transport.
///
/// A bare Alchemy URL with either env var missing is an error, so the
/// process refuses to start instead of calling Alchemy without credentials.
/// With both set, the client is returned only after an authenticated
/// `eth_chainId` call succeeds.
pub async fn chain_rpc_client<L>(
    url: &Url,
    timeouts: ChainRpcTimeouts,
    layer: L,
) -> Result<RpcClient, ChainRpcError>
where
    L: Layer<BoxTransport>,
    L::Service: IntoBoxTransport,
{
    chain_rpc_client_with(url, JwtEnv::from_env(), timeouts, layer).await
}

async fn chain_rpc_client_with<L>(
    url: &Url,
    jwt: JwtEnv,
    timeouts: ChainRpcTimeouts,
    layer: L,
) -> Result<RpcClient, ChainRpcError>
where
    L: Layer<BoxTransport>,
    L::Service: IntoBoxTransport,
{
    let base = st0x_rpc::connect_with_timeouts(url.as_str(), jwt, timeouts).await?;

    Ok(ClientBuilder::default()
        .layer(layer)
        .transport(base.transport().clone(), base.is_local()))
}

#[cfg(test)]
mod tests {
    use httpmock::MockServer;
    use st0x_rpc::{AlchemyJwtConfig, KEY_VERSION_VAR, KID_VAR};
    use std::time::{Duration, Instant};
    use tower::layer::util::Identity;

    use super::*;

    const BARE_ALCHEMY_URL: &str = "https://base-mainnet.g.alchemy.com/v2";

    #[tokio::test]
    async fn plain_url_sends_no_authorization() {
        let server = MockServer::start_async().await;
        let unauthenticated = server.mock(|when, then| {
            when.method("POST")
                .path("/rpc")
                .header_missing("authorization");
            then.status(200)
                .header("content-type", "application/json")
                .body(r#"{"jsonrpc":"2.0","id":0,"result":"0x2105"}"#);
        });
        let both = JwtEnv::Both(AlchemyJwtConfig {
            key_version: "projects/p/locations/l/keyRings/r/cryptoKeys/k/cryptoKeyVersions/1"
                .to_string(),
            kid: "kid".to_string(),
        });

        for jwt in [JwtEnv::Unset, both] {
            let client = chain_rpc_client_with(
                &server.url("/rpc").parse().unwrap(),
                jwt,
                ChainRpcTimeouts::default(),
                Identity::new(),
            )
            .await
            .unwrap();
            let chain_id: String = client.request_noparams("eth_chainId").await.unwrap();
            assert_eq!(chain_id, "0x2105");
        }

        unauthenticated.assert_calls(2);
    }

    #[tokio::test]
    async fn bare_alchemy_url_without_jwt_settings_is_refused() {
        let url: Url = BARE_ALCHEMY_URL.parse().unwrap();
        let cases = [
            (
                JwtEnv::Unset,
                "ALCHEMY_JWT_KMS_KEY_VERSION or ALCHEMY_JWT_KID",
            ),
            (JwtEnv::Partial { missing: KID_VAR }, "ALCHEMY_JWT_KID"),
            (
                JwtEnv::Partial {
                    missing: KEY_VERSION_VAR,
                },
                "ALCHEMY_JWT_KMS_KEY_VERSION",
            ),
        ];

        for (jwt, expected_missing) in cases {
            let error =
                chain_rpc_client_with(&url, jwt, ChainRpcTimeouts::default(), Identity::new())
                    .await
                    .unwrap_err();
            assert!(
                matches!(
                    error,
                    ChainRpcError::MissingJwtConfig { missing } if missing == expected_missing
                ),
                "{error:?}"
            );
        }
    }

    #[tokio::test]
    async fn request_timeout_bounds_a_hung_endpoint() {
        let server = MockServer::start_async().await;
        server.mock(|when, then| {
            when.method("POST").path("/v2/secret-api-key");
            then.status(200).delay(Duration::from_secs(5));
        });
        let timeouts = ChainRpcTimeouts {
            request: Some(Duration::from_millis(200)),
            ..ChainRpcTimeouts::default()
        };
        let client = chain_rpc_client_with(
            &server.url("/v2/secret-api-key").parse().unwrap(),
            JwtEnv::Unset,
            timeouts,
            Identity::new(),
        )
        .await
        .unwrap();

        let started = Instant::now();
        let error = client
            .request_noparams::<String>("eth_chainId")
            .await
            .unwrap_err();

        assert!(started.elapsed() < Duration::from_secs(2), "{error:?}");
        let rendered = format!("{error} {error:?}");
        assert!(!rendered.contains("secret-api-key"), "{rendered}");
        assert!(
            !rendered.contains(&server.address().to_string()),
            "{rendered}"
        );
    }
}

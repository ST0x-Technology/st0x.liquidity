//! Ambient service-account identity for authenticated pricing connections.

use std::time::Duration;

use thiserror::Error;

pub(crate) const METADATA_IDENTITY_URL: &str =
    "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/identity";

#[derive(Debug, Error)]
pub(crate) enum PricingIdentityError {
    #[error("metadata identity request failed")]
    Request(#[from] reqwest::Error),
    #[error("metadata identity returned HTTP {0}")]
    Status(u16),
}

pub(crate) async fn fetch_identity_from(
    endpoint: &str,
    audience: &str,
    timeout: Duration,
) -> Result<String, PricingIdentityError> {
    let response = reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(timeout)
        .build()?
        .get(endpoint)
        .query(&[("audience", audience)])
        .header("Metadata-Flavor", "Google")
        .send()
        .await?;
    if !response.status().is_success() {
        return Err(PricingIdentityError::Status(response.status().as_u16()));
    }
    Ok(response.text().await?)
}

#[cfg(test)]
mod tests {
    use httpmock::MockServer;

    use super::*;

    #[tokio::test]
    async fn metadata_redirect_cannot_forward_an_identity_request() {
        let server = MockServer::start_async().await;
        let redirect = server
            .mock_async(|when, then| {
                when.path("/identity")
                    .header("Metadata-Flavor", "Google")
                    .query_param("audience", "https://pricing.example");
                then.status(302)
                    .header("Location", format!("{}/other", server.base_url()));
            })
            .await;
        let target = server
            .mock_async(|when, then| {
                when.path("/other");
                then.status(200).body("must-not-be-read");
            })
            .await;
        let error = fetch_identity_from(
            &format!("{}/identity", server.base_url()),
            "https://pricing.example",
            Duration::from_secs(1),
        )
        .await
        .unwrap_err();
        assert!(matches!(error, PricingIdentityError::Status(302)));
        redirect.assert_async().await;
        target.assert_calls_async(0).await;
    }
}

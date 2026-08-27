use reqwest::StatusCode;
use url::Url;

use crate::error::Error;

/// Thin HTTP wrapper over the liquidity bot API. Holds no domain logic: it
/// builds a request, sends it, and returns the decoded JSON or a mapped error.
pub struct Client {
    http: reqwest::Client,
    base_url: Url,
}

impl Client {
    pub fn new(base_url: Url) -> anyhow::Result<Self> {
        let http = reqwest::Client::builder().build()?;
        Ok(Self { http, base_url })
    }

    fn url(&self, path: &str, params: &[(String, String)]) -> Url {
        let mut url = self.base_url.clone();
        url.set_path(path);
        url.set_query(None);
        if !params.is_empty() {
            let mut query = url.query_pairs_mut();
            for (key, value) in params {
                query.append_pair(key, value);
            }
        }
        url
    }

    pub async fn get(
        &self,
        path: &str,
        params: &[(String, String)],
    ) -> Result<serde_json::Value, Error> {
        let url = self.url(path, params);
        let target = url.to_string();
        let response = self
            .http
            .get(url)
            .send()
            .await
            .map_err(|source| Error::Transport(target, source))?;
        let status = response.status();
        if status.is_success() {
            return response
                .json::<serde_json::Value>()
                .await
                .map_err(Error::Decode);
        }
        let body = response.text().await.unwrap_or_default();
        Err(match status {
            StatusCode::UNAUTHORIZED => Error::Unauthorized(body),
            StatusCode::FORBIDDEN => Error::Forbidden(body),
            other => Error::Http(other, body),
        })
    }
}

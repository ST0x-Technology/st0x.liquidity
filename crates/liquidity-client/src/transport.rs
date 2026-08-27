use reqwest::StatusCode;
use url::Url;

use crate::auth::TokenSource;
use crate::error::Error;

/// Thin HTTP wrapper over the liquidity bot API. Holds no domain logic: it
/// mints a bearer token, builds the request, sends it, and returns the decoded
/// JSON or a mapped error.
pub struct Client<A> {
    http: reqwest::Client,
    base_url: Url,
    auth: A,
}

impl<A: TokenSource + Sync> Client<A> {
    pub fn new(base_url: Url, auth: A) -> anyhow::Result<Self> {
        let http = reqwest::Client::builder().build()?;
        Ok(Self {
            http,
            base_url,
            auth,
        })
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

    async fn dispatch(
        &self,
        request: reqwest::RequestBuilder,
        target: String,
    ) -> Result<serde_json::Value, Error> {
        let token = self.auth.bearer().await?;
        let response = request
            .bearer_auth(token)
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

    pub async fn get(
        &self,
        path: &str,
        params: &[(String, String)],
    ) -> Result<serde_json::Value, Error> {
        let url = self.url(path, params);
        let target = url.to_string();
        self.dispatch(self.http.get(url), target).await
    }

    pub async fn post(&self, path: &str) -> Result<serde_json::Value, Error> {
        let url = self.url(path, &[]);
        let target = url.to_string();
        self.dispatch(self.http.post(url), target).await
    }
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::mpsc::{Receiver, channel};

    use super::{Client, Error, TokenSource};

    struct FakeToken(&'static str);

    impl TokenSource for FakeToken {
        async fn bearer(&self) -> Result<String, Error> {
            Ok(self.0.to_owned())
        }
    }

    /// Accepts one connection, captures the raw request bytes, and replies with
    /// an empty JSON object. Returns the bound port and a channel of the
    /// captured request.
    fn capture_server() -> std::io::Result<(u16, Receiver<String>)> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let port = listener.local_addr()?.port();
        let (sender, receiver) = channel();
        std::thread::spawn(move || {
            if let Ok((mut stream, _)) = listener.accept() {
                let mut buffer = [0u8; 4096];
                let read = stream.read(&mut buffer).unwrap_or(0);
                let request = String::from_utf8_lossy(&buffer[..read]).into_owned();
                let _ = sender.send(request);
                let response = "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}";
                let _ = stream.write_all(response.as_bytes());
            }
        });
        Ok((port, receiver))
    }

    #[tokio::test]
    async fn attaches_bearer_token_and_builds_request() -> Result<(), Box<dyn std::error::Error>> {
        let (port, requests) = capture_server()?;
        let base = url::Url::parse(&format!("http://127.0.0.1:{port}/"))?;
        let client = Client::new(base, FakeToken("test-token"))?;

        let value = client
            .get(
                "/performance/latencies",
                &[("from".to_owned(), "x".to_owned())],
            )
            .await?;
        assert_eq!(value, serde_json::json!({}));

        let request = requests.recv()?;
        let lower = request.to_lowercase();
        assert!(
            lower.contains("authorization: bearer test-token"),
            "bearer token not attached: {request}"
        );
        assert!(
            request.contains("GET /performance/latencies?from=x "),
            "unexpected request line: {request}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn post_sends_bearer_and_method() -> Result<(), Box<dyn std::error::Error>> {
        let (port, requests) = capture_server()?;
        let base = url::Url::parse(&format!("http://127.0.0.1:{port}/"))?;
        let client = Client::new(base, FakeToken("test-token"))?;

        let value = client.post("/transfers/resume").await?;
        assert_eq!(value, serde_json::json!({}));

        let request = requests.recv()?;
        let lower = request.to_lowercase();
        assert!(
            lower.contains("authorization: bearer test-token"),
            "bearer token not attached: {request}"
        );
        assert!(
            request.contains("POST /transfers/resume "),
            "unexpected request line: {request}"
        );
        Ok(())
    }
}

//! Thin HTTP transport for the liquidity ops API: builds role-prefixed
//! requests, attaches the per-role bearer token, and maps each response to the
//! client's `Error` type.

use reqwest::StatusCode;
use url::Url;

use crate::auth::TokenSource;
use crate::error::Error;

/// Role prefix the load balancer routes to the read IAP backend.
const READ_PREFIX: &str = "/liquidity-read";
/// Role prefix the load balancer routes to the write IAP backend.
const WRITE_PREFIX: &str = "/liquidity-write";

/// Thin HTTP wrapper over the liquidity bot ops API. Holds no domain logic: it
/// mints the token for the role, builds the request under the role prefix,
/// sends it, and returns the decoded JSON or a mapped error.
///
/// Reads go to `/liquidity-read/*` with a token minted for the read audience;
/// writes go to `/liquidity-write/*` with a token minted for the write
/// audience. IAP admits each prefix per Workspace group and the bot pins the
/// audience, so a read-audience token presented to the write path is rejected.
pub struct Client<A> {
    http: reqwest::Client,
    base_url: Url,
    read_auth: A,
    write_auth: A,
}

impl<A: TokenSource + Sync> Client<A> {
    pub fn new(
        base_url: Url,
        read_auth: A,
        write_auth: A,
        request_timeout: std::time::Duration,
        connect_timeout: std::time::Duration,
    ) -> anyhow::Result<Self> {
        let http = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(request_timeout)
            .connect_timeout(connect_timeout)
            .build()?;
        Ok(Self {
            http,
            base_url,
            read_auth,
            write_auth,
        })
    }

    fn url(&self, prefix: &str, path: &str, params: &[(String, String)]) -> Url {
        let mut url = self.base_url.clone();
        url.set_path(&format!("{prefix}{path}"));
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
        let url = self.url(READ_PREFIX, path, params);
        let target = url.to_string();
        let token = self.read_auth.bearer().await?;
        self.dispatch(self.http.get(url), target, token).await
    }

    pub async fn post(&self, path: &str) -> Result<serde_json::Value, Error> {
        let url = self.url(WRITE_PREFIX, path, &[]);
        let target = url.to_string();
        let token = self.write_auth.bearer().await?;
        self.dispatch(
            self.http
                .post(url)
                .header(reqwest::header::CONTENT_LENGTH, "0"),
            target,
            token,
        )
        .await
    }

    async fn dispatch(
        &self,
        request: reqwest::RequestBuilder,
        target: String,
        token: String,
    ) -> Result<serde_json::Value, Error> {
        let response = request
            .bearer_auth(token)
            .send()
            .await
            .map_err(|source| Error::Transport(target.clone(), source))?;
        let status = response.status();
        let content_type = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default()
            .to_owned();
        if status.is_redirection() {
            let location = response
                .headers()
                .get(reqwest::header::LOCATION)
                .and_then(|value| value.to_str().ok())
                .unwrap_or_default()
                .to_owned();
            return Err(Error::Unauthorized(format!(
                "IAP redirected to sign-in (status {status}, location {location}); the token was missing, expired, or not accepted"
            )));
        }
        let body = response
            .text()
            .await
            .map_err(|source| Error::Transport(target, source))?;
        if status.is_success() {
            return serde_json::from_str::<serde_json::Value>(&body).map_err(|source| {
                Error::Decode(format!(
                    "expected JSON but the endpoint returned {content_type} ({source}); this usually means the ops API is not deployed at this path. Body starts: {}",
                    body_prefix(&body)
                ))
            });
        }
        Err(match status {
            StatusCode::UNAUTHORIZED => Error::Unauthorized(body),
            StatusCode::FORBIDDEN => Error::Forbidden(body),
            other => Error::Http(other, body),
        })
    }
}

/// A short, single-line preview of a response body, for error messages.
fn body_prefix(body: &str) -> String {
    body.split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .chars()
        .take(120)
        .collect()
}

/// Percent-encodes one path segment so an interpolated value (a venue, kind,
/// or aggregate id) cannot inject extra `/` segments or a `?`/`#` that would
/// change routing. Keeps the RFC 3986 unreserved set; encodes everything else.
pub(crate) fn encode_segment(segment: &str) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let mut out = String::with_capacity(segment.len());
    for &byte in segment.as_bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                out.push(byte as char);
            }
            _ => {
                out.push('%');
                out.push(HEX[(byte >> 4) as usize] as char);
                out.push(HEX[(byte & 0x0f) as usize] as char);
            }
        }
    }
    out
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

    fn fake_client(port: u16) -> Result<Client<FakeToken>, Box<dyn std::error::Error>> {
        let base = url::Url::parse(&format!("http://127.0.0.1:{port}/"))?;
        Ok(Client::new(
            base,
            FakeToken("read-token"),
            FakeToken("write-token"),
            std::time::Duration::from_secs(5),
            std::time::Duration::from_secs(5),
        )?)
    }

    #[tokio::test]
    async fn read_uses_read_prefix_and_read_token() -> Result<(), Box<dyn std::error::Error>> {
        let (port, requests) = capture_server()?;
        let client = fake_client(port)?;

        let value = client
            .get(
                "/transfers/interrupted",
                &[("since".to_owned(), "0".to_owned())],
            )
            .await?;
        assert_eq!(value, serde_json::json!({}));

        let request = requests.recv()?;
        assert!(
            request
                .to_lowercase()
                .contains("authorization: bearer read-token"),
            "read did not use the read token: {request}"
        );
        assert!(
            request.contains("GET /liquidity-read/transfers/interrupted?since=0 "),
            "read did not use the read prefix: {request}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn write_uses_write_prefix_and_write_token() -> Result<(), Box<dyn std::error::Error>> {
        let (port, requests) = capture_server()?;
        let client = fake_client(port)?;

        let value = client.post("/transfers/recheck/mint/abc").await?;
        assert_eq!(value, serde_json::json!({}));

        let request = requests.recv()?;
        assert!(
            request
                .to_lowercase()
                .contains("authorization: bearer write-token"),
            "write did not use the write token: {request}"
        );
        assert!(
            request.contains("POST /liquidity-write/transfers/recheck/mint/abc "),
            "write did not use the write prefix: {request}"
        );
        Ok(())
    }

    #[test]
    fn encode_segment_escapes_path_separators() {
        assert_eq!(super::encode_segment("a/b"), "a%2Fb");
        assert_eq!(super::encode_segment("x?y#z"), "x%3Fy%23z");
        assert_eq!(
            super::encode_segment("7fc7c900-aa6d-4911"),
            "7fc7c900-aa6d-4911"
        );
    }
}

//! Thin HTTP transport for the liquidity ops API: builds role-prefixed
//! requests, attaches the per-role bearer token, and maps each response to the
//! module's own `TransportError`.

use reqwest::StatusCode;
use url::Url;

use crate::auth::{AuthError, TokenSource};
/// Failure talking to the liquidity ops API over HTTP.
#[derive(Debug)]
pub enum TransportError {
    /// The HTTP request could not be sent, or its body could not be read.
    Transport(String, reqwest::Error),
    /// HTTP 401: the identity was missing, expired, or not accepted.
    Unauthorized(String),
    /// HTTP 403: authenticated, but not on the command's access list.
    Forbidden(String),
    /// Any other non-success status.
    Http(StatusCode, String),
    /// A success response whose body was not the expected JSON.
    Decode(String),
    /// The bearer token could not be minted for the request.
    Auth(AuthError),
}

fn server_said(body: &str) -> String {
    let trimmed = body.trim();
    if trimmed.is_empty() {
        String::new()
    } else {
        format!("\nServer said: {trimmed}")
    }
}

impl std::fmt::Display for TransportError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Transport(url, source) => {
                write!(
                    formatter,
                    "could not reach the liquidity API at {url}: {source}"
                )
            }
            Self::Unauthorized(body) => write!(
                formatter,
                "HTTP 401 Unauthorized: the T0 Google identity was missing, expired, or invalid.\nRefresh the T0 login (gcloud auth login) or, in CI, the T0 workload identity, then retry.{}",
                server_said(body)
            ),
            Self::Forbidden(body) => write!(
                formatter,
                "HTTP 403 Forbidden: authenticated, but your T0 Workspace group is not on this command's access list.{}",
                server_said(body)
            ),
            Self::Http(status, body) => {
                write!(
                    formatter,
                    "HTTP {status}: the request failed.{}",
                    server_said(body)
                )
            }
            Self::Decode(detail) => {
                write!(formatter, "the API response could not be decoded: {detail}")
            }
            Self::Auth(source) => write!(formatter, "{source}"),
        }
    }
}

impl std::error::Error for TransportError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Transport(_, source) => Some(source),
            Self::Auth(source) => Some(source),
            _ => None,
        }
    }
}

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
    ) -> Result<serde_json::Value, TransportError> {
        let url = self.url(READ_PREFIX, path, params);
        let target = url.to_string();
        let token = self
            .read_auth
            .bearer()
            .await
            .map_err(TransportError::Auth)?;
        self.dispatch(self.http.get(url), target, token).await
    }

    pub async fn post(&self, path: &str) -> Result<serde_json::Value, TransportError> {
        let url = self.url(WRITE_PREFIX, path, &[]);
        let target = url.to_string();
        let token = self
            .write_auth
            .bearer()
            .await
            .map_err(TransportError::Auth)?;
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
    ) -> Result<serde_json::Value, TransportError> {
        let response = request
            .bearer_auth(token)
            .send()
            .await
            .map_err(|source| TransportError::Transport(target.clone(), source))?;
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
            return Err(TransportError::Unauthorized(format!(
                "IAP redirected to sign-in (status {status}, location {location}); the token was missing, expired, or not accepted"
            )));
        }
        let body = response
            .text()
            .await
            .map_err(|source| TransportError::Transport(target, source))?;
        if status.is_success() {
            return serde_json::from_str::<serde_json::Value>(&body).map_err(|source| {
                TransportError::Decode(format!(
                    "expected JSON but the endpoint returned {content_type} ({source}); this usually means the ops API is not deployed at this path. Body starts: {}",
                    body_prefix(&body)
                ))
            });
        }
        Err(match status {
            StatusCode::UNAUTHORIZED => TransportError::Unauthorized(body),
            StatusCode::FORBIDDEN => TransportError::Forbidden(body),
            other => TransportError::Http(other, body),
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
    //! Tests for request routing, token attachment, and response classification.
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::mpsc::{Receiver, channel};

    use super::{Client, TokenSource, TransportError};
    use crate::auth::AuthError;

    struct FakeToken(&'static str);

    impl TokenSource for FakeToken {
        async fn bearer(&self) -> Result<String, AuthError> {
            Ok(self.0.to_owned())
        }
    }

    /// Serves one connection: accumulates the request headers up to the
    /// terminator, hands them back over the channel, then writes `response`.
    fn serve(response: &'static str) -> std::io::Result<(u16, Receiver<String>)> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let port = listener.local_addr()?.port();
        let (sender, receiver) = channel();
        std::thread::spawn(move || {
            if let Ok((mut stream, _)) = listener.accept() {
                let mut request = Vec::new();
                let mut buffer = [0u8; 1024];
                while let Ok(read) = stream.read(&mut buffer) {
                    if read == 0 {
                        break;
                    }
                    request.extend_from_slice(&buffer[..read]);
                    if request.windows(4).any(|window| window == &b"\r\n\r\n"[..]) {
                        break;
                    }
                }
                let _ = sender.send(String::from_utf8_lossy(&request).into_owned());
                let _ = stream.write_all(response.as_bytes());
            }
        });
        Ok((port, receiver))
    }

    const OK_JSON: &str = "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}";

    fn capture_server() -> std::io::Result<(u16, Receiver<String>)> {
        serve(OK_JSON)
    }

    /// The HTTP request line (method, target, version) of a captured request.
    fn request_line(request: &str) -> &str {
        request.lines().next().unwrap_or_default()
    }

    /// The value of a captured request header, matched case-insensitively.
    fn header<'a>(request: &'a str, name: &str) -> Option<&'a str> {
        request.lines().find_map(|line| {
            let (key, value) = line.split_once(':')?;
            key.trim()
                .eq_ignore_ascii_case(name)
                .then_some(value.trim())
        })
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
        assert_eq!(
            request_line(&request),
            "GET /liquidity-read/transfers/interrupted?since=0 HTTP/1.1"
        );
        assert_eq!(header(&request, "authorization"), Some("Bearer read-token"));
        Ok(())
    }

    #[tokio::test]
    async fn write_uses_write_prefix_and_write_token() -> Result<(), Box<dyn std::error::Error>> {
        let (port, requests) = capture_server()?;
        let client = fake_client(port)?;

        let value = client.post("/transfers/recheck/mint/abc").await?;
        assert_eq!(value, serde_json::json!({}));

        let request = requests.recv()?;
        assert_eq!(
            request_line(&request),
            "POST /liquidity-write/transfers/recheck/mint/abc HTTP/1.1"
        );
        assert_eq!(
            header(&request, "authorization"),
            Some("Bearer write-token")
        );
        Ok(())
    }

    #[tokio::test]
    async fn redirect_maps_to_unauthorized_with_location() -> Result<(), Box<dyn std::error::Error>>
    {
        let (port, _requests) = serve(
            "HTTP/1.1 302 Found\r\nLocation: https://accounts.google.com/signin\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
        )?;
        let client = fake_client(port)?;
        match client.get("/pnl", &[]).await {
            Err(TransportError::Unauthorized(message)) => {
                assert!(
                    message.contains("https://accounts.google.com/signin"),
                    "{message}"
                );
            }
            other => panic!("expected Unauthorized, got {other:?}"),
        }
        Ok(())
    }

    #[tokio::test]
    async fn status_401_maps_to_unauthorized() -> Result<(), Box<dyn std::error::Error>> {
        let (port, _requests) = serve(
            "HTTP/1.1 401 Unauthorized\r\nContent-Length: 6\r\nConnection: close\r\n\r\ndenied",
        )?;
        let client = fake_client(port)?;
        match client.get("/pnl", &[]).await {
            Err(TransportError::Unauthorized(body)) => assert_eq!(body, "denied"),
            other => panic!("expected Unauthorized, got {other:?}"),
        }
        Ok(())
    }

    #[tokio::test]
    async fn status_403_maps_to_forbidden() -> Result<(), Box<dyn std::error::Error>> {
        let (port, _requests) =
            serve("HTTP/1.1 403 Forbidden\r\nContent-Length: 4\r\nConnection: close\r\n\r\nnope")?;
        let client = fake_client(port)?;
        match client.get("/pnl", &[]).await {
            Err(TransportError::Forbidden(body)) => assert_eq!(body, "nope"),
            other => panic!("expected Forbidden, got {other:?}"),
        }
        Ok(())
    }

    #[tokio::test]
    async fn other_status_maps_to_http() -> Result<(), Box<dyn std::error::Error>> {
        let (port, _requests) = serve(
            "HTTP/1.1 500 Internal Server Error\r\nContent-Length: 4\r\nConnection: close\r\n\r\nboom",
        )?;
        let client = fake_client(port)?;
        match client.get("/pnl", &[]).await {
            Err(TransportError::Http(status, body)) => {
                assert_eq!(status.as_u16(), 500);
                assert_eq!(body, "boom");
            }
            other => panic!("expected Http, got {other:?}"),
        }
        Ok(())
    }

    #[tokio::test]
    async fn success_non_json_maps_to_decode() -> Result<(), Box<dyn std::error::Error>> {
        let (port, _requests) = serve(
            "HTTP/1.1 200 OK\r\nContent-Type: text/html\r\nContent-Length: 13\r\nConnection: close\r\n\r\n<html></html>",
        )?;
        let client = fake_client(port)?;
        match client.get("/pnl", &[]).await {
            Err(TransportError::Decode(message)) => {
                assert!(message.contains("text/html"), "{message}");
            }
            other => panic!("expected Decode, got {other:?}"),
        }
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

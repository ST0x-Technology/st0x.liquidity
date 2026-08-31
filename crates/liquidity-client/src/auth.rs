//! Token sources for the IAP audience: non-interactive ADC, a fixed
//! `StaticToken`, and the interactive Google Desktop OAuth flow (loopback +
//! PKCE) whose refresh token is cached for silent reuse.

use base64::Engine;
use google_cloud_auth::credentials::idtoken;
use rand::RngCore;
use sha2::{Digest, Sha256};
use url::Url;

/// Failure obtaining a T0 Google identity. `Display` explains the failure and,
/// per environment, how to fix it.
#[derive(Debug)]
pub enum AuthError {
    /// ADC / Google credential failure; the original typed error is retained
    /// as the source.
    Credentials(Box<dyn std::error::Error + Send + Sync + 'static>),
    /// Desktop OAuth sign-in flow failure, with an operator-facing reason.
    Flow(String),
}

impl std::fmt::Display for AuthError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let reason: &dyn std::fmt::Display = match self {
            Self::Credentials(source) => source,
            Self::Flow(reason) => reason,
        };
        write!(
            f,
            "could not obtain a T0 Google identity: {reason}\nFor staging, complete the browser sign-in when prompted. For production, ensure Application Default Credentials (a service account, workload identity, or impersonation) can mint an ID token for the configured audience."
        )
    }
}

impl std::error::Error for AuthError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Credentials(source) => Some(&**source),
            Self::Flow(_) => None,
        }
    }
}

/// Supplies the bearer token attached to each API request. Abstracted so the
/// transport can be exercised in tests without live Google credentials.
pub trait TokenSource {
    fn bearer(&self) -> impl Future<Output = Result<String, AuthError>> + Send;
}

/// Mints Google OIDC ID tokens for the IAP audience from Application Default
/// Credentials. Holding the credential lets the library cache and refresh the
/// token across calls, so a caller need not cache the returned string. This is
/// the non-interactive path: it needs a service account, workload identity, or
/// impersonation, since a plain user login cannot mint an audience-bound token.
pub struct Adc {
    credentials: idtoken::IDTokenCredentials,
}

impl Adc {
    pub fn new(audience: &str) -> Result<Self, AuthError> {
        let credentials = idtoken::Builder::new(audience.to_owned())
            .build()
            .map_err(|source| AuthError::Credentials(Box::new(source)))?;
        Ok(Self { credentials })
    }
}

impl TokenSource for Adc {
    async fn bearer(&self) -> Result<String, AuthError> {
        self.credentials
            .id_token()
            .await
            .map_err(|source| AuthError::Credentials(Box::new(source)))
    }
}

/// A fixed, already-minted bearer token. The desktop OAuth flow yields one ID
/// token whose audience is the OAuth client id, and IAP admits both the read
/// and write prefixes on that single identity (the Workspace group decides
/// which paths succeed), so the same token backs both roles.
pub struct StaticToken(pub String);

impl TokenSource for StaticToken {
    async fn bearer(&self) -> Result<String, AuthError> {
        Ok(self.0.clone())
    }
}

/// Google OAuth 2.0 endpoints for the installed-application (desktop) flow.
const AUTH_ENDPOINT: &str = "https://accounts.google.com/o/oauth2/v2/auth";
const TOKEN_ENDPOINT: &str = "https://oauth2.googleapis.com/token";

/// Obtains a Google OIDC ID token for the desktop OAuth client, whose `aud` is
/// the client id IAP expects. A cached refresh token drives a silent refresh
/// when present; otherwise a one-time browser sign-in (loopback + PKCE) runs
/// and its refresh token is cached so later invocations stay silent.
pub async fn desktop_id_token(client_id: &str, client_secret: &str) -> Result<String, AuthError> {
    if let Some(refresh_token) = load_refresh_token()
        && let Ok(id_token) =
            refresh_id_token(TOKEN_ENDPOINT, client_id, client_secret, &refresh_token).await
    {
        return Ok(id_token);
    }
    interactive_id_token(client_id, client_secret).await
}

/// Runs the browser loopback + PKCE authorization once, exchanges the returned
/// code for an ID token, and caches the refresh token for silent reuse.
async fn interactive_id_token(client_id: &str, client_secret: &str) -> Result<String, AuthError> {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").map_err(|source| {
        AuthError::Flow(format!("could not open the sign-in listener: {source}"))
    })?;
    let port = listener
        .local_addr()
        .map_err(|source| AuthError::Flow(format!("could not read the sign-in port: {source}")))?
        .port();
    let redirect_uri = format!("http://127.0.0.1:{port}");
    let verifier = random_token(32);
    let challenge = code_challenge(&verifier);
    let state = random_token(16);

    let mut auth_url = Url::parse(AUTH_ENDPOINT)
        .map_err(|source| AuthError::Flow(format!("invalid authorization endpoint: {source}")))?;
    auth_url
        .query_pairs_mut()
        .append_pair("client_id", client_id)
        .append_pair("redirect_uri", &redirect_uri)
        .append_pair("response_type", "code")
        .append_pair("scope", "openid email")
        .append_pair("code_challenge", &challenge)
        .append_pair("code_challenge_method", "S256")
        .append_pair("state", &state)
        .append_pair("access_type", "offline")
        .append_pair("prompt", "consent");

    eprintln!(
        "Open this URL in your browser to sign in with your T0 Google account:\n\n{auth_url}\n"
    );

    let expected_state = state.clone();
    let code = tokio::task::spawn_blocking(move || capture_code(&listener, &expected_state))
        .await
        .map_err(|source| AuthError::Flow(format!("the sign-in listener panicked: {source}")))??;

    let token = post_token(
        TOKEN_ENDPOINT,
        &[
            ("grant_type", "authorization_code"),
            ("code", &code),
            ("code_verifier", &verifier),
            ("client_id", client_id),
            ("client_secret", client_secret),
            ("redirect_uri", &redirect_uri),
        ],
    )
    .await?;

    if let Some(refresh_token) = token
        .get("refresh_token")
        .and_then(serde_json::Value::as_str)
    {
        store_refresh_token(refresh_token);
    }
    extract_id_token(&token)
}

/// Exchanges a cached refresh token for a fresh ID token, no browser needed.
async fn refresh_id_token(
    endpoint: &str,
    client_id: &str,
    client_secret: &str,
    refresh_token: &str,
) -> Result<String, AuthError> {
    let token = post_token(
        endpoint,
        &[
            ("grant_type", "refresh_token"),
            ("refresh_token", refresh_token),
            ("client_id", client_id),
            ("client_secret", client_secret),
        ],
    )
    .await?;
    extract_id_token(&token)
}

/// POSTs a form to the Google token endpoint and returns the decoded JSON.
async fn post_token(endpoint: &str, form: &[(&str, &str)]) -> Result<serde_json::Value, AuthError> {
    let response = reqwest::Client::new()
        .post(endpoint)
        .form(form)
        .send()
        .await
        .map_err(|source| AuthError::Flow(format!("token request failed: {source}")))?;
    let status = response.status();
    let body = response.text().await.map_err(|source| {
        AuthError::Flow(format!("could not read the token response: {source}"))
    })?;
    if !status.is_success() {
        return Err(AuthError::Flow(format!(
            "the token endpoint returned {status}: {body}"
        )));
    }
    serde_json::from_str(&body)
        .map_err(|source| AuthError::Flow(format!("could not decode the token response: {source}")))
}

/// Pulls the `id_token` out of a token response; this JWT is what IAP validates.
fn extract_id_token(token: &serde_json::Value) -> Result<String, AuthError> {
    token
        .get("id_token")
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| AuthError::Flow("the token response carried no id_token".to_owned()))
}

/// Accepts the single loopback redirect, returns the authorization code, and
/// serves a small page telling the operator the sign-in is done. Blocking, so
/// it runs on a blocking task off the async runtime.
fn capture_code(
    listener: &std::net::TcpListener,
    expected_state: &str,
) -> Result<String, AuthError> {
    let (mut stream, _) = listener.accept().map_err(|source| {
        AuthError::Flow(format!("the sign-in redirect never arrived: {source}"))
    })?;
    let cloned = stream
        .try_clone()
        .map_err(|source| AuthError::Flow(format!("could not read the redirect: {source}")))?;
    let mut reader = std::io::BufReader::new(cloned);
    let mut request_line = String::new();
    std::io::BufRead::read_line(&mut reader, &mut request_line)
        .map_err(|source| AuthError::Flow(format!("could not read the redirect: {source}")))?;

    let query = request_line
        .split_whitespace()
        .nth(1)
        .and_then(|target| target.split_once('?').map(|(_, query)| query.to_owned()))
        .unwrap_or_default();
    let params: std::collections::HashMap<String, String> =
        url::form_urlencoded::parse(query.as_bytes())
            .into_owned()
            .collect();

    let body = "<html><body>Sign-in complete. You can close this tab and return to the terminal.</body></html>";
    let response = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: text/html\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    let _ = std::io::Write::write_all(&mut stream, response.as_bytes());

    if let Some(reason) = params.get("error") {
        return Err(AuthError::Flow(format!(
            "authorization was denied: {reason}"
        )));
    }
    if params.get("state").map(String::as_str) != Some(expected_state) {
        return Err(AuthError::Flow(
            "the sign-in redirect state did not match; ignoring a possible forgery".to_owned(),
        ));
    }
    params
        .get("code")
        .cloned()
        .ok_or_else(|| AuthError::Flow("the sign-in redirect carried no code".to_owned()))
}

/// A URL-safe, unpadded base64 string of `bytes` random bytes, for the PKCE
/// verifier and the CSRF state.
fn random_token(bytes: usize) -> String {
    let mut buffer = vec![0u8; bytes];
    rand::thread_rng().fill_bytes(&mut buffer);
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(&buffer)
}

/// The S256 PKCE challenge for a verifier.
fn code_challenge(verifier: &str) -> String {
    let digest = Sha256::digest(verifier.as_bytes());
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(digest)
}

/// Path of the cached refresh token: `$XDG_CONFIG_HOME` (or `~/.config`) under
/// the client's own directory.
fn refresh_token_path() -> Option<std::path::PathBuf> {
    let base = std::env::var_os("XDG_CONFIG_HOME")
        .map(std::path::PathBuf::from)
        .or_else(|| {
            std::env::var_os("HOME").map(|home| std::path::PathBuf::from(home).join(".config"))
        })?;
    Some(
        base.join("st0x-liquidity-client")
            .join("oauth-staging.json"),
    )
}

/// Reads the cached refresh token, if any.
fn load_refresh_token() -> Option<String> {
    let path = refresh_token_path()?;
    let contents = std::fs::read_to_string(path).ok()?;
    let parsed: serde_json::Value = serde_json::from_str(&contents).ok()?;
    parsed
        .get("refresh_token")
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned)
}

/// Persists the refresh token with owner-only permissions. Best effort: a cache
/// write failure must not fail the command, only cost the next run a sign-in.
fn store_refresh_token(refresh_token: &str) {
    let Some(path) = refresh_token_path() else {
        return;
    };
    if let Some(directory) = path.parent() {
        let _ = std::fs::create_dir_all(directory);
    }
    let body = serde_json::json!({ "refresh_token": refresh_token }).to_string();
    #[cfg(unix)]
    {
        use std::io::Write;
        use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
        // Create with owner-only mode from the outset so the token is never
        // briefly world-readable between creation and a later chmod; re-assert
        // 0600 to also cover an already-existing file, whose mode open keeps.
        if let Ok(mut file) = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .open(&path)
        {
            let _ = std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600));
            let _ = file.write_all(body.as_bytes());
        }
    }
    #[cfg(not(unix))]
    {
        let _ = std::fs::write(&path, body);
    }
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};

    use super::{AuthError, capture_code, extract_id_token, refresh_id_token};

    #[test]
    fn extract_id_token_reads_the_jwt() {
        let token = serde_json::json!({ "id_token": "jwt-value" });
        assert_eq!(extract_id_token(&token).ok(), Some("jwt-value".to_owned()));
    }

    #[test]
    fn extract_id_token_rejects_a_missing_jwt() {
        let token = serde_json::json!({ "access_token": "no-id-here" });
        assert!(matches!(extract_id_token(&token), Err(AuthError::Flow(_))));
    }

    /// Connects to the loopback listener and sends one raw HTTP GET whose
    /// target carries `query`, then drains the reply so `capture_code` can
    /// finish writing its page.
    fn send_redirect(port: u16, query: &str) {
        if let Ok(mut stream) = TcpStream::connect(("127.0.0.1", port)) {
            let request = format!("GET /?{query} HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n");
            let _ = stream.write_all(request.as_bytes());
            let mut sink = Vec::new();
            let _ = stream.read_to_end(&mut sink);
        }
    }

    /// Drives `capture_code` against a single loopback redirect carrying
    /// `query`, returning what it parsed.
    fn capture(
        query: &'static str,
        expected_state: &str,
    ) -> Result<Result<String, AuthError>, Box<dyn std::error::Error>> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let port = listener.local_addr()?.port();
        let sender = std::thread::spawn(move || send_redirect(port, query));
        let result = capture_code(&listener, expected_state);
        let _ = sender.join();
        Ok(result)
    }

    #[test]
    fn capture_code_returns_the_authorization_code() -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!(
            capture("code=abc&state=xyz", "xyz")?.ok(),
            Some("abc".to_owned())
        );
        Ok(())
    }

    #[test]
    fn capture_code_rejects_a_mismatched_state() -> Result<(), Box<dyn std::error::Error>> {
        assert!(matches!(
            capture("code=abc&state=wrong", "xyz")?,
            Err(AuthError::Flow(_))
        ));
        Ok(())
    }

    #[test]
    fn capture_code_rejects_a_missing_code() -> Result<(), Box<dyn std::error::Error>> {
        assert!(matches!(
            capture("state=xyz", "xyz")?,
            Err(AuthError::Flow(_))
        ));
        Ok(())
    }

    #[test]
    fn capture_code_surfaces_a_denial() -> Result<(), Box<dyn std::error::Error>> {
        assert!(matches!(
            capture("error=access_denied&state=xyz", "xyz")?,
            Err(AuthError::Flow(_))
        ));
        Ok(())
    }

    /// Serves one token-endpoint response over loopback and returns its URL.
    fn token_server(response: &'static str) -> Result<String, Box<dyn std::error::Error>> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let port = listener.local_addr()?.port();
        std::thread::spawn(move || {
            if let Ok((mut stream, _)) = listener.accept() {
                let mut buffer = [0u8; 2048];
                let _ = stream.read(&mut buffer);
                let _ = stream.write_all(response.as_bytes());
            }
        });
        Ok(format!("http://127.0.0.1:{port}/token"))
    }

    #[tokio::test]
    async fn refresh_decodes_a_fresh_id_token() -> Result<(), Box<dyn std::error::Error>> {
        let endpoint = token_server(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n{\"id_token\":\"fresh\"}",
        )?;
        let result = refresh_id_token(&endpoint, "cid", "secret", "rtok").await;
        assert_eq!(result.ok(), Some("fresh".to_owned()));
        Ok(())
    }

    #[tokio::test]
    async fn refresh_fails_on_a_rejected_token_request() -> Result<(), Box<dyn std::error::Error>> {
        let endpoint =
            token_server("HTTP/1.1 400 Bad Request\r\nConnection: close\r\n\r\ninvalid_grant")?;
        assert!(matches!(
            refresh_id_token(&endpoint, "cid", "secret", "rtok").await,
            Err(AuthError::Flow(_))
        ));
        Ok(())
    }
}

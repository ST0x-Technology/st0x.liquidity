use google_cloud_auth::credentials::idtoken;

use crate::error::Error;

/// Supplies the bearer token attached to each API request. Abstracted so the
/// transport can be exercised in tests without live Google credentials.
pub trait TokenSource {
    fn bearer(&self) -> impl Future<Output = Result<String, Error>> + Send;
}

/// Mints Google OIDC ID tokens for the IAP audience from Application Default
/// Credentials. Holding the credential lets the library cache and refresh the
/// token across calls, so a caller need not cache the returned string.
pub struct Adc {
    credentials: idtoken::IDTokenCredentials,
}

impl Adc {
    pub fn new(audience: &str) -> Result<Self, Error> {
        let credentials = idtoken::Builder::new(audience.to_owned())
            .build()
            .map_err(|source| Error::Auth(source.to_string()))?;
        Ok(Self { credentials })
    }
}

impl TokenSource for Adc {
    async fn bearer(&self) -> Result<String, Error> {
        self.credentials
            .id_token()
            .await
            .map_err(|source| Error::Auth(source.to_string()))
    }
}

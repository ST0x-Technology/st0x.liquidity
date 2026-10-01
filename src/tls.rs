//! Process-wide TLS setup.

/// Installs `ring` as the process-level rustls crypto provider.
///
/// The dependency graph enables both the `ring` and `aws-lc-rs` rustls
/// providers, so rustls cannot pick a default on its own and panics when a
/// client such as `tokio-tungstenite` builds a config without naming one.
/// `reqwest` clients that use rustls, such as the Alpaca client, also pick up
/// this default.
///
/// Call it before any TLS client is built. Calling it again is harmless.
pub fn install_tls_crypto_provider() {
    // `Err` means a provider is already installed for this process, which is
    // all TLS clients need.
    let _ = rustls::crypto::ring::default_provider().install_default();
}

#[cfg(test)]
mod tests {
    use rustls::crypto::CryptoProvider;
    use rustls::{ClientConfig, RootCertStore};

    use super::*;

    #[test]
    fn tls_client_config_builds_after_installing_provider() {
        install_tls_crypto_provider();
        install_tls_crypto_provider();

        assert!(CryptoProvider::get_default().is_some());

        ClientConfig::builder()
            .with_root_certificates(RootCertStore::empty())
            .with_no_client_auth();
    }
}

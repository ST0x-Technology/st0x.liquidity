use aes_gcm::aead::{Aead, OsRng};
use aes_gcm::{Aes256Gcm, KeyInit};
use alloy::primitives::FixedBytes;
use rand::RngCore;
use serde::{Deserialize, Serialize};

const NONCE_SIZE: usize = 12;

pub type EncryptionKey = FixedBytes<32>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EncryptedToken(#[serde(with = "alloy::hex")] Vec<u8>);

impl EncryptedToken {
    pub(crate) fn new(ciphertext: Vec<u8>) -> Self {
        Self(ciphertext)
    }

    pub(crate) fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl AsRef<[u8]> for EncryptedToken {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl From<Vec<u8>> for EncryptedToken {
    fn from(bytes: Vec<u8>) -> Self {
        Self(bytes)
    }
}

pub fn encrypt_token(
    key: &EncryptionKey,
    plaintext: &str,
) -> Result<EncryptedToken, EncryptionError> {
    let cipher = Aes256Gcm::new(key.as_slice().into());

    let mut nonce_bytes = [0u8; NONCE_SIZE];
    OsRng.fill_bytes(&mut nonce_bytes);
    let nonce = &nonce_bytes.into();

    let ciphertext = cipher
        .encrypt(nonce, plaintext.as_bytes())
        .map_err(|_| EncryptionError::EncryptionFailed)?;

    let combined: Vec<u8> = nonce_bytes
        .iter()
        .chain(ciphertext.iter())
        .copied()
        .collect();

    Ok(EncryptedToken::new(combined))
}

pub fn decrypt_token(
    key: &EncryptionKey,
    token: &EncryptedToken,
) -> Result<String, EncryptionError> {
    let ciphertext = token.as_bytes();

    if ciphertext.len() < NONCE_SIZE {
        return Err(EncryptionError::InvalidCiphertext(format!(
            "Ciphertext too short: expected at least {} bytes, got {}",
            NONCE_SIZE,
            ciphertext.len()
        )));
    }

    let (nonce_bytes, encrypted_data) = ciphertext.split_at(NONCE_SIZE);
    let nonce = nonce_bytes.into();

    let cipher = Aes256Gcm::new(key.as_slice().into());

    let plaintext_bytes = cipher
        .decrypt(nonce, encrypted_data)
        .map_err(|_| EncryptionError::DecryptionFailed)?;

    String::from_utf8(plaintext_bytes).map_err(|e| EncryptionError::Utf8Error(e.to_string()))
}

#[derive(Debug, thiserror::Error)]
pub enum EncryptionError {
    #[error("Encryption failed")]
    EncryptionFailed,
    #[error("Decryption failed (wrong key or corrupted data)")]
    DecryptionFailed,
    #[error("Invalid ciphertext: {0}")]
    InvalidCiphertext(String),
    #[error("Decrypted data is not valid UTF-8: {0}")]
    Utf8Error(String),
    #[error("Hex decoding error: {0}")]
    Hex(#[from] alloy::hex::FromHexError),
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::primitives::b256;

    fn create_test_key() -> EncryptionKey {
        b256!("0x0000000000000000000000000000000000000000000000000000000000000000")
    }

    #[test]
    fn test_encryption_decryption_round_trip() {
        let key = create_test_key();
        let plaintext = "test_access_token_12345";

        let token = encrypt_token(&key, plaintext).unwrap();
        let decrypted = decrypt_token(&key, &token).unwrap();

        assert_eq!(plaintext, decrypted);
    }

    #[test]
    fn test_encryption_produces_different_ciphertext() {
        let key = create_test_key();
        let plaintext = "same_plaintext";

        let token1 = encrypt_token(&key, plaintext).unwrap();
        let token2 = encrypt_token(&key, plaintext).unwrap();

        assert_ne!(token1, token2);
    }

    #[test]
    fn test_decryption_with_wrong_key() {
        let key1 = b256!("0x0101010101010101010101010101010101010101010101010101010101010101");
        let key2 = b256!("0x0202020202020202020202020202020202020202020202020202020202020202");

        let plaintext = "secret_token";
        let token = encrypt_token(&key1, plaintext).unwrap();

        let result = decrypt_token(&key2, &token);
        assert!(matches!(
            result.unwrap_err(),
            EncryptionError::DecryptionFailed
        ));
    }

    #[test]
    fn test_decrypt_short_ciphertext() {
        let key = create_test_key();
        let short_data = vec![0u8; NONCE_SIZE - 1];
        let token = EncryptedToken::new(short_data);

        let result = decrypt_token(&key, &token);
        assert!(matches!(
            result.unwrap_err(),
            EncryptionError::InvalidCiphertext(_)
        ));
    }

    #[test]
    fn test_decrypt_corrupted_ciphertext() {
        let key = create_test_key();
        let plaintext = "test_token";

        let token = encrypt_token(&key, plaintext).unwrap();
        let mut bytes = token.0;
        bytes[NONCE_SIZE] ^= 0xFF;
        let corrupted_token = EncryptedToken::new(bytes);

        let result = decrypt_token(&key, &corrupted_token);
        assert!(matches!(
            result.unwrap_err(),
            EncryptionError::DecryptionFailed
        ));
    }

    #[test]
    fn test_encrypt_empty_string() {
        let key = create_test_key();
        let plaintext = "";

        let token = encrypt_token(&key, plaintext).unwrap();
        let decrypted = decrypt_token(&key, &token).unwrap();

        assert_eq!(plaintext, decrypted);
    }

    #[test]
    fn test_encrypt_long_string() {
        let key = create_test_key();
        let plaintext = "a".repeat(10000);

        let token = encrypt_token(&key, &plaintext).unwrap();
        let decrypted = decrypt_token(&key, &token).unwrap();

        assert_eq!(plaintext, decrypted);
    }

    #[test]
    fn test_encrypt_unicode() {
        let key = create_test_key();
        let plaintext = "Hello 世界 🌍";

        let token = encrypt_token(&key, plaintext).unwrap();
        let decrypted = decrypt_token(&key, &token).unwrap();

        assert_eq!(plaintext, decrypted);
    }

    #[test]
    fn test_encrypted_token_serialization() {
        let key = create_test_key();
        let plaintext = "test_token";

        let token = encrypt_token(&key, plaintext).unwrap();

        let json = serde_json::to_string(&token).unwrap();

        assert!(
            json.starts_with('"'),
            "JSON should be a hex string, not an array"
        );
        assert!(
            !json.contains('['),
            "JSON should not contain array brackets"
        );

        let deserialized: EncryptedToken = serde_json::from_str(&json).unwrap();

        let decrypted = decrypt_token(&key, &deserialized).unwrap();
        assert_eq!(plaintext, decrypted);
    }
}

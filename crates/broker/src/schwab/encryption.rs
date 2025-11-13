use aes_gcm::aead::{Aead, OsRng};
use aes_gcm::{Aes256Gcm, KeyInit};
use alloy::primitives::FixedBytes;
use rand::RngCore;

const NONCE_SIZE: usize = 12;

pub(crate) type EncryptionKey = FixedBytes<32>;

pub(crate) fn encrypt_token(
    key: &EncryptionKey,
    plaintext: &str,
) -> Result<Vec<u8>, EncryptionError> {
    let cipher = Aes256Gcm::new(key.as_slice().into());

    let mut nonce_bytes = [0u8; NONCE_SIZE];
    OsRng.fill_bytes(&mut nonce_bytes);
    let nonce = &nonce_bytes.into();

    let ciphertext = cipher
        .encrypt(nonce, plaintext.as_bytes())
        .map_err(|_| EncryptionError::EncryptionFailed)?;

    Ok(nonce_bytes
        .iter()
        .chain(ciphertext.iter())
        .copied()
        .collect())
}

pub(crate) fn decrypt_token(
    key: &EncryptionKey,
    ciphertext: &[u8],
) -> Result<String, EncryptionError> {
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

    fn create_test_key() -> EncryptionKey {
        FixedBytes::from([0u8; 32])
    }

    #[test]
    fn test_encryption_decryption_round_trip() {
        let key = create_test_key();
        let plaintext = "test_access_token_12345";

        let ciphertext = encrypt_token(&key, plaintext).unwrap();
        let decrypted = decrypt_token(&key, &ciphertext).unwrap();

        assert_eq!(plaintext, decrypted);
    }

    #[test]
    fn test_encryption_produces_different_ciphertext() {
        let key = create_test_key();
        let plaintext = "same_plaintext";

        let ciphertext1 = encrypt_token(&key, plaintext).unwrap();
        let ciphertext2 = encrypt_token(&key, plaintext).unwrap();

        assert_ne!(ciphertext1, ciphertext2);
    }

    #[test]
    fn test_decryption_with_wrong_key() {
        let key1 = FixedBytes::from([1u8; 32]);
        let key2 = FixedBytes::from([2u8; 32]);

        let plaintext = "secret_token";
        let ciphertext = encrypt_token(&key1, plaintext).unwrap();

        let result = decrypt_token(&key2, &ciphertext);
        assert!(matches!(
            result.unwrap_err(),
            EncryptionError::DecryptionFailed
        ));
    }

    #[test]
    fn test_decrypt_short_ciphertext() {
        let key = create_test_key();
        let short_data = vec![0u8; NONCE_SIZE - 1];

        let result = decrypt_token(&key, &short_data);
        assert!(matches!(
            result.unwrap_err(),
            EncryptionError::InvalidCiphertext(_)
        ));
    }

    #[test]
    fn test_decrypt_corrupted_ciphertext() {
        let key = create_test_key();
        let plaintext = "test_token";

        let mut ciphertext = encrypt_token(&key, plaintext).unwrap();
        ciphertext[NONCE_SIZE] ^= 0xFF;

        let result = decrypt_token(&key, &ciphertext);
        assert!(matches!(
            result.unwrap_err(),
            EncryptionError::DecryptionFailed
        ));
    }

    #[test]
    fn test_encrypt_empty_string() {
        let key = create_test_key();
        let plaintext = "";

        let ciphertext = encrypt_token(&key, plaintext).unwrap();
        let decrypted = decrypt_token(&key, &ciphertext).unwrap();

        assert_eq!(plaintext, decrypted);
    }

    #[test]
    fn test_encrypt_long_string() {
        let key = create_test_key();
        let plaintext = "a".repeat(10000);

        let ciphertext = encrypt_token(&key, &plaintext).unwrap();
        let decrypted = decrypt_token(&key, &ciphertext).unwrap();

        assert_eq!(plaintext, decrypted);
    }

    #[test]
    fn test_encrypt_unicode() {
        let key = create_test_key();
        let plaintext = "Hello 世界 🌍";

        let ciphertext = encrypt_token(&key, plaintext).unwrap();
        let decrypted = decrypt_token(&key, &ciphertext).unwrap();

        assert_eq!(plaintext, decrypted);
    }
}

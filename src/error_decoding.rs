//! Contract error decoding utilities.
//!
//! Provides helpers to decode Solidity revert data into human-readable error messages
//! using the OpenChain selector registry.

use rain_error_decoding::AbiDecodedErrorType;
use tracing::debug;

/// Handles a contract error by attempting to decode revert data.
///
/// If the error contains revert data and decoding succeeds, converts via
/// `From<AbiDecodedErrorType>`. Otherwise converts via `From<alloy::contract::Error>`.
pub(crate) async fn handle_contract_error<E>(err: alloy::contract::Error) -> E
where
    E: From<AbiDecodedErrorType> + From<alloy::contract::Error>,
{
    if let Some(revert_data) = err.as_revert_data() {
        if let Ok(decoded) =
            AbiDecodedErrorType::selector_registry_abi_decode(revert_data.as_ref(), None).await
        {
            return decoded.into();
        }
        debug!("Failed to decode revert data");
    }

    err.into()
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{Bytes, hex};
    use alloy::rpc::json_rpc::ErrorPayload;
    use alloy::sol_types::SolError;
    use alloy::transports::TransportError;
    use thiserror::Error;

    use super::*;

    alloy::sol! {
        #[derive(Debug)]
        error Error(string message);
    }

    #[derive(Debug, Error)]
    enum TestError {
        #[error("decoded: {0}")]
        Decoded(#[from] AbiDecodedErrorType),
        #[error("contract: {0}")]
        Contract(#[from] alloy::contract::Error),
    }

    fn create_error_with_revert_data(data: &Bytes) -> alloy::contract::Error {
        let hex = hex::encode_prefixed(data);
        let raw = serde_json::value::to_raw_value(&hex).expect("valid json");
        let payload = ErrorPayload {
            code: 3,
            message: "execution reverted".into(),
            data: Some(raw),
        };
        alloy::contract::Error::TransportError(TransportError::ErrorResp(payload))
    }

    #[tokio::test]
    async fn returns_contract_error_when_no_revert_data() {
        let err = alloy::contract::Error::TransportError(TransportError::local_usage_str(
            "connection refused",
        ));

        let result: TestError = handle_contract_error(err).await;

        assert!(matches!(result, TestError::Contract(_)));
    }

    #[tokio::test]
    async fn returns_decoded_error_when_revert_data_decodes() {
        // Standard Error(string) selector 0x08c379a0 is well-known
        let revert_data = Bytes::from(
            Error {
                message: "insufficient balance".into(),
            }
            .abi_encode(),
        );
        let err = create_error_with_revert_data(&revert_data);

        let result: TestError = handle_contract_error(err).await;

        let TestError::Decoded(decoded) = result else {
            panic!("expected Decoded, got {result:?}");
        };
        let msg = decoded.to_string();
        assert!(
            msg.contains("Error"),
            "expected Error in message, got: {msg}"
        );
        assert!(
            msg.contains("insufficient balance"),
            "expected 'insufficient balance' in message, got: {msg}"
        );
    }

    #[tokio::test]
    async fn returns_contract_error_when_revert_data_malformed() {
        // Data too short to be a valid selector - decoding fails
        let too_short = Bytes::from(vec![0x12, 0x34]);
        let err = create_error_with_revert_data(&too_short);

        let result: TestError = handle_contract_error(err).await;

        let TestError::Contract(contract_err) = result else {
            panic!("expected Contract, got {result:?}");
        };
        // Verify the original error is preserved
        assert!(contract_err.to_string().contains("execution reverted"));
    }

    #[tokio::test]
    async fn returns_decoded_unknown_for_unrecognized_selector() {
        // Valid 4-byte selector but unknown - returns Decoded with raw data
        let unknown_selector = Bytes::from(vec![0x12, 0x34, 0x56, 0x78, 0x00, 0x00, 0x00, 0x00]);
        let err = create_error_with_revert_data(&unknown_selector);

        let result: TestError = handle_contract_error(err).await;

        let TestError::Decoded(decoded) = result else {
            panic!("expected Decoded, got {result:?}");
        };
        let msg = decoded.to_string();
        // Unknown selectors show "unknown error" with raw data
        assert!(
            msg.contains("unknown error"),
            "expected 'unknown error' in message, got: {msg}"
        );
        assert!(
            msg.contains("12345678"),
            "expected raw selector in message, got: {msg}"
        );
    }
}

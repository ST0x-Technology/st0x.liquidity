//! Contract error decoding utilities.
//!
//! Provides helpers to decode Solidity revert data into
//! human-readable error messages using the OpenChain selector
//! registry.

use rain_error_decoding::{AbiDecodedErrorType, ErrorRegistry};
use tracing::debug;

/// Handles a contract error by attempting to decode revert data.
///
/// If the error contains revert data and decoding succeeds, converts via
/// `From<AbiDecodedErrorType>`. Otherwise converts via `From<alloy::contract::Error>`.
pub async fn handle_contract_error<E>(err: alloy::contract::Error) -> E
where
    E: From<AbiDecodedErrorType> + From<alloy::contract::Error>,
{
    handle_contract_error_with(err, None).await
}

/// Handles a contract error using an optional custom registry.
///
/// This variant allows injecting a mock registry for testing without making
/// live HTTP requests to the OpenChain selector registry.
pub async fn handle_contract_error_with<E>(
    err: alloy::contract::Error,
    registry: Option<&dyn ErrorRegistry>,
) -> E
where
    E: From<AbiDecodedErrorType> + From<alloy::contract::Error>,
{
    if let Some(revert_data) = err.as_revert_data() {
        if let Ok(decoded) =
            AbiDecodedErrorType::selector_registry_abi_decode(revert_data.as_ref(), registry).await
        {
            return decoded.into();
        }
        debug!("Failed to decode revert data");
    }

    err.into()
}

#[cfg(test)]
mod tests {
    use alloy::json_abi::Error as AlloyError;
    use alloy::primitives::{Bytes, hex};
    use alloy::rpc::json_rpc::ErrorPayload;
    use alloy::sol_types::SolError;
    use alloy::transports::TransportError;
    use async_trait::async_trait;
    use rain_error_decoding::AbiDecodeFailedErrors;
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

    /// Mock registry that returns configurable error candidates.
    struct MockRegistry {
        candidates: Vec<AlloyError>,
    }

    impl MockRegistry {
        fn empty() -> Self {
            Self { candidates: vec![] }
        }

        fn with_error(sig: &str) -> Self {
            Self {
                candidates: vec![sig.parse().expect("valid error signature")],
            }
        }
    }

    #[async_trait]
    impl ErrorRegistry for MockRegistry {
        async fn lookup(
            &self,
            _selector: [u8; 4],
        ) -> Result<Vec<AlloyError>, AbiDecodeFailedErrors> {
            Ok(self.candidates.clone())
        }
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
        let error = alloy::contract::Error::TransportError(TransportError::local_usage_str(
            "connection refused",
        ));
        let registry = MockRegistry::empty();

        let result: TestError = handle_contract_error_with(error, Some(&registry)).await;

        assert!(matches!(result, TestError::Contract(_)));
    }

    #[tokio::test]
    async fn returns_decoded_error_when_revert_data_decodes() {
        let revert_data = Bytes::from(
            Error {
                message: "insufficient balance".into(),
            }
            .abi_encode(),
        );
        let error = create_error_with_revert_data(&revert_data);
        let registry = MockRegistry::with_error("Error(string)");

        let result: TestError = handle_contract_error_with(error, Some(&registry)).await;

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
        let too_short = Bytes::from(vec![0x12, 0x34]);
        let error = create_error_with_revert_data(&too_short);
        let registry = MockRegistry::empty();

        let result: TestError = handle_contract_error_with(error, Some(&registry)).await;

        let TestError::Contract(contract_error) = result else {
            panic!("expected Contract, got {result:?}");
        };
        assert!(contract_error.to_string().contains("execution reverted"));
    }

    #[tokio::test]
    async fn returns_decoded_unknown_for_unrecognized_selector() {
        let unknown_selector = Bytes::from(vec![0x12, 0x34, 0x56, 0x78, 0x00, 0x00, 0x00, 0x00]);
        let error = create_error_with_revert_data(&unknown_selector);
        let registry = MockRegistry::empty();

        let result: TestError = handle_contract_error_with(error, Some(&registry)).await;

        let TestError::Decoded(decoded) = result else {
            panic!("expected Decoded, got {result:?}");
        };
        let msg = decoded.to_string();
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

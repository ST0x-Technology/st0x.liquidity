//! Contract error decoding utilities.
//!
//! Provides helpers to decode Solidity revert data into human-readable error messages
//! using the OpenChain selector registry.

use rain_error_decoding::{AbiDecodedErrorType, ErrorRegistry};
use tracing::debug;

/// Handles a contract error by attempting to decode revert data.
///
/// If the error contains revert data and decoding succeeds, converts via
/// `From<AbiDecodedErrorType>`. Otherwise converts via `From<alloy::contract::Error>`.
pub(crate) async fn handle_contract_error<E>(err: alloy::contract::Error) -> E
where
    E: From<AbiDecodedErrorType> + From<alloy::contract::Error>,
{
    handle_contract_error_with(err, None).await
}

/// Handles a contract error using an optional custom registry.
///
/// This variant allows injecting a mock registry for testing without making
/// live HTTP requests to the OpenChain selector registry.
pub(crate) async fn handle_contract_error_with<E>(
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
    use alloy::contract::Error as ContractError;
    use alloy::json_abi::Error as AlloyError;
    use alloy::primitives::hex;
    use alloy::rpc::json_rpc::{ErrorPayload, RpcError};
    use async_trait::async_trait;
    use rain_error_decoding::{AbiDecodeFailedErrors, AbiDecodedErrorType, ErrorRegistry};
    use serde_json::value::RawValue;
    use std::borrow::Cow;

    use super::*;

    /// Tracks which conversion path was taken so tests can assert the right branch.
    #[derive(Debug, PartialEq)]
    enum TestError {
        Decoded(AbiDecodedErrorType),
        Contract(String),
    }

    impl From<AbiDecodedErrorType> for TestError {
        fn from(decoded: AbiDecodedErrorType) -> Self {
            Self::Decoded(decoded)
        }
    }

    impl From<ContractError> for TestError {
        fn from(err: ContractError) -> Self {
            Self::Contract(err.to_string())
        }
    }

    /// Registry that returns a known error signature for selector 0x1ac66908.
    struct SuccessRegistry;

    #[async_trait]
    impl ErrorRegistry for SuccessRegistry {
        async fn lookup(
            &self,
            selector: [u8; 4],
        ) -> Result<Vec<AlloyError>, AbiDecodeFailedErrors> {
            if selector == [0x1a, 0xc6, 0x69, 0x08] {
                let e: AlloyError = "UnexpectedOperandValue()".parse().unwrap();
                Ok(vec![e])
            } else {
                Ok(vec![])
            }
        }
    }

    /// Registry that always fails lookup.
    struct FailRegistry;

    #[async_trait]
    impl ErrorRegistry for FailRegistry {
        async fn lookup(
            &self,
            _selector: [u8; 4],
        ) -> Result<Vec<AlloyError>, AbiDecodeFailedErrors> {
            Err(AbiDecodeFailedErrors::NoData)
        }
    }

    /// Builds a ContractError whose `as_revert_data()` returns Some(revert_bytes).
    fn contract_error_with_revert(revert_bytes: &[u8]) -> ContractError {
        let hex_data = format!("\"0x{}\"", hex::encode(revert_bytes));
        let raw = RawValue::from_string(hex_data).unwrap();

        let payload = ErrorPayload {
            code: 3,
            message: Cow::Borrowed("execution reverted"),
            data: Some(raw),
        };

        ContractError::TransportError(RpcError::ErrorResp(payload))
    }

    /// Builds a ContractError whose `as_revert_data()` returns None.
    fn contract_error_without_revert() -> ContractError {
        ContractError::UnknownFunction("test_fn".to_string())
    }

    #[tokio::test]
    async fn successful_decode_returns_decoded_error() {
        // 0x1ac66908 is the selector for UnexpectedOperandValue()
        let err = contract_error_with_revert(&[0x1a, 0xc6, 0x69, 0x08]);

        let result: TestError = handle_contract_error_with(err, Some(&SuccessRegistry)).await;

        let expected = AbiDecodedErrorType::Known {
            name: "UnexpectedOperandValue".to_string(),
            args: vec![],
            sig: "UnexpectedOperandValue()".to_string(),
            data: vec![0x1a, 0xc6, 0x69, 0x08],
        };
        assert_eq!(result, TestError::Decoded(expected));
    }

    #[tokio::test]
    async fn failed_decode_falls_back_to_contract_error() {
        let err = contract_error_with_revert(&[0x1a, 0xc6, 0x69, 0x08]);

        let result: TestError = handle_contract_error_with(err, Some(&FailRegistry)).await;

        assert!(
            matches!(result, TestError::Contract(_)),
            "Expected Contract fallback, got: {result:?}"
        );
    }

    #[tokio::test]
    async fn no_revert_data_converts_contract_error_directly() {
        let err = contract_error_without_revert();

        let result: TestError = handle_contract_error_with(err, None).await;

        assert!(
            matches!(result, TestError::Contract(ref msg) if msg.contains("test_fn")),
            "Expected Contract error mentioning test_fn, got: {result:?}"
        );
    }

    #[tokio::test]
    async fn handle_contract_error_delegates_to_with() {
        // No revert data, no registry — exercises the wrapper function
        let err = contract_error_without_revert();

        let result: TestError = handle_contract_error(err).await;

        assert!(
            matches!(result, TestError::Contract(ref msg) if msg.contains("test_fn")),
            "Expected Contract error mentioning test_fn, got: {result:?}"
        );
    }
}

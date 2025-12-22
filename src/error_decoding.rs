//! Contract error decoding utilities.
//!
//! Provides helpers to decode Solidity revert data into human-readable error messages
//! using the OpenChain signature database.

use rain_error_decoding::AbiDecodedErrorType;
use tracing::debug;

/// Attempts to decode an alloy contract error into a human-readable form.
///
/// If the error contains revert data, queries the OpenChain signature database
/// to decode the error selector into a named error with arguments.
///
/// Returns `Some(AbiDecodedErrorType)` if revert data was present and decoding succeeded,
/// `None` if no revert data was available.
pub(crate) async fn decode_contract_error(
    err: &alloy::contract::Error,
) -> Option<AbiDecodedErrorType> {
    let Some(revert_data) = err.as_revert_data() else {
        debug!("No revert data found in error: {err:?}");
        return None;
    };

    match AbiDecodedErrorType::selector_registry_abi_decode(revert_data.as_ref(), None).await {
        Ok(decoded) => Some(decoded),
        Err(e) => {
            debug!("Failed to decode revert data: {e}");
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{Bytes, U256, address, hex};
    use alloy::rpc::json_rpc::ErrorPayload;
    use alloy::sol_types::SolError;
    use alloy::transports::TransportError;

    use super::*;

    alloy::sol! {
        #[derive(Debug)]
        error InsufficientBalance(address account, uint256 balance, uint256 required);
    }

    fn create_error_with_revert_data(data: &Bytes) -> alloy::contract::Error {
        let err_payload = ErrorPayload {
            code: 3,
            message: "execution reverted".into(),
            data: Some(serde_json::value::to_raw_value(&hex::encode_prefixed(data)).unwrap()),
        };
        alloy::contract::Error::TransportError(TransportError::ErrorResp(err_payload))
    }

    #[tokio::test]
    async fn returns_none_when_no_revert_data() {
        let err = alloy::contract::Error::TransportError(TransportError::local_usage_str(
            "connection refused",
        ));

        assert!(decode_contract_error(&err).await.is_none());
    }

    #[tokio::test]
    async fn decodes_known_error_selector() {
        let error_data = Bytes::from(
            InsufficientBalance {
                account: address!("0x1234567890123456789012345678901234567890"),
                balance: U256::from(100),
                required: U256::from(200),
            }
            .abi_encode(),
        );
        let err = create_error_with_revert_data(&error_data);

        let decoded = decode_contract_error(&err).await;

        // InsufficientBalance is a well-known error signature in OpenChain
        let decoded = decoded.expect("should decode InsufficientBalance error");
        assert!(decoded.to_string().contains("InsufficientBalance"));
    }
}

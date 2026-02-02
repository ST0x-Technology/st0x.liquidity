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

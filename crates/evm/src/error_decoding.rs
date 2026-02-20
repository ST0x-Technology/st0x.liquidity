//! Contract error decoding utilities.
//!
//! Provides type-level error registry injection for decoding Solidity
//! revert data. Production code uses [`OpenChainErrorRegistry`] (backed by
//! the OpenChain selector API). Tests use [`NoOpErrorRegistry`] to
//! avoid HTTP calls.

use alloy::eips::BlockId;
use alloy::primitives::{Address, Bytes};
use alloy::providers::Provider;
use alloy::rpc::types::{TransactionReceipt, TransactionRequest};
use alloy::transports::{RpcError, TransportErrorKind};
use async_trait::async_trait;
use rain_error_decoding::{AbiDecodedErrorType, DEFAULT_REGISTRY, ErrorRegistry};
use tracing::{debug, warn};

use crate::EvmError;

/// Type-level selector for which error registry to use.
///
/// Implemented by zero-sized marker types ([`OpenChainErrorRegistry`],
/// [`NoOpErrorRegistry`]) so callers choose the registry at the type
/// level without passing values.
pub trait IntoErrorRegistry: Send + Sync {
    fn error_registry() -> &'static dyn ErrorRegistry;
}

/// Uses the OpenChain selector registry API via the static
/// `DEFAULT_REGISTRY` instance from `rain_error_decoding`.
pub struct OpenChainErrorRegistry;

impl IntoErrorRegistry for OpenChainErrorRegistry {
    fn error_registry() -> &'static dyn ErrorRegistry {
        &*DEFAULT_REGISTRY
    }
}

/// Noop registry for tests -- never makes HTTP calls. Errors decode
/// as "unknown" since no candidates are returned.
pub struct NoOpErrorRegistry;

impl IntoErrorRegistry for NoOpErrorRegistry {
    fn error_registry() -> &'static dyn ErrorRegistry {
        &NoOpRegistryImpl
    }
}

struct NoOpRegistryImpl;

#[async_trait]
impl ErrorRegistry for NoOpRegistryImpl {
    async fn lookup(
        &self,
        _selector: [u8; 4],
    ) -> Result<Vec<alloy::json_abi::Error>, rain_error_decoding::AbiDecodeFailedErrors> {
        Ok(vec![])
    }
}

/// Handles a contract error by attempting to decode revert data.
///
/// If the error contains revert data and decoding succeeds, converts via
/// `From<AbiDecodedErrorType>`. Otherwise converts via
/// `From<alloy::contract::Error>`.
pub async fn handle_contract_error<E>(err: alloy::contract::Error) -> E
where
    E: From<AbiDecodedErrorType> + From<alloy::contract::Error>,
{
    handle_contract_error_with(err, None).await
}

/// Handles a contract error using an optional custom registry.
///
/// This variant allows injecting a mock registry for testing without
/// making live HTTP requests to the OpenChain selector registry.
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

/// Decode a Solidity revert from an RPC error. Falls back to
/// wrapping as a contract error if decoding fails.
pub(crate) async fn decode_rpc_revert<Registry: IntoErrorRegistry>(
    rpc_err: RpcError<TransportErrorKind>,
) -> EvmError {
    let contract_err = alloy::contract::Error::TransportError(rpc_err);
    handle_contract_error_with(contract_err, Some(Registry::error_registry())).await
}

/// Check a transaction receipt for revert, and if reverted, replay
/// the call at the reverted block to extract and decode the revert
/// reason. Returns the receipt unchanged on success.
pub(crate) async fn decode_reverted_receipt<Registry: IntoErrorRegistry>(
    provider: &impl Provider,
    from: Address,
    contract: Address,
    calldata: Bytes,
    receipt: TransactionReceipt,
) -> Result<TransactionReceipt, EvmError> {
    if !receipt.status() {
        if let Some(block_number) = receipt.block_number {
            let tx = TransactionRequest::default()
                .to(contract)
                .from(from)
                .input(calldata.into());

            let replay = provider.call(tx).block(BlockId::number(block_number));

            match replay.await {
                Err(rpc_err) => {
                    return Err(decode_rpc_revert::<Registry>(rpc_err).await);
                }
                Ok(_) => {
                    warn!(
                        tx_hash = %receipt.transaction_hash,
                        "Transaction reverted but replay succeeded -- \
                         state may have changed between blocks"
                    );
                }
            }
        }

        return Err(EvmError::Reverted {
            tx_hash: receipt.transaction_hash,
        });
    }

    Ok(receipt)
}

#[cfg(test)]
mod tests {
    use alloy::consensus::{Receipt, ReceiptEnvelope, ReceiptWithBloom};
    use alloy::json_abi::Error as AlloyError;
    use alloy::network::EthereumWallet;
    use alloy::network::TransactionBuilder;
    use alloy::node_bindings::Anvil;
    use alloy::primitives::{Bloom, Bytes, TxHash, hex};
    use alloy::providers::ProviderBuilder;
    use alloy::providers::mock::Asserter;
    use alloy::rpc::json_rpc::ErrorPayload;
    use alloy::rpc::types::TransactionRequest;
    use alloy::signers::local::PrivateKeySigner;
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

    fn create_rpc_error_with_revert_data(data: &Bytes) -> RpcError<TransportErrorKind> {
        let hex = hex::encode_prefixed(data);
        let raw = serde_json::value::to_raw_value(&hex).expect("valid json");
        let payload = ErrorPayload {
            code: 3,
            message: "execution reverted".into(),
            data: Some(raw),
        };
        TransportError::ErrorResp(payload)
    }

    fn successful_receipt() -> TransactionReceipt {
        TransactionReceipt {
            inner: ReceiptEnvelope::Eip1559(ReceiptWithBloom {
                receipt: Receipt {
                    status: true.into(),
                    cumulative_gas_used: 0,
                    logs: vec![],
                },
                logs_bloom: Bloom::default(),
            }),
            transaction_hash: TxHash::ZERO,
            transaction_index: Some(0),
            block_hash: None,
            block_number: Some(42),
            gas_used: 21000,
            effective_gas_price: 1,
            blob_gas_used: None,
            blob_gas_price: None,
            from: Address::ZERO,
            to: Some(Address::ZERO),
            contract_address: None,
        }
    }

    fn reverted_receipt(block_number: Option<u64>) -> TransactionReceipt {
        TransactionReceipt {
            inner: ReceiptEnvelope::Eip1559(ReceiptWithBloom {
                receipt: Receipt {
                    status: false.into(),
                    cumulative_gas_used: 0,
                    logs: vec![],
                },
                logs_bloom: Bloom::default(),
            }),
            transaction_hash: TxHash::random(),
            transaction_index: Some(0),
            block_hash: None,
            block_number,
            gas_used: 21000,
            effective_gas_price: 1,
            blob_gas_used: None,
            blob_gas_price: None,
            from: Address::ZERO,
            to: Some(Address::ZERO),
            contract_address: None,
        }
    }

    #[tokio::test]
    async fn handle_contract_error_returns_contract_when_no_revert_data() {
        let error = alloy::contract::Error::TransportError(TransportError::local_usage_str(
            "connection refused",
        ));
        let registry = MockRegistry::empty();

        let result: TestError = handle_contract_error_with(error, Some(&registry)).await;

        assert!(matches!(result, TestError::Contract(_)));
    }

    #[tokio::test]
    async fn handle_contract_error_decodes_known_revert() {
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
    async fn handle_contract_error_returns_contract_when_malformed() {
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
    async fn handle_contract_error_decodes_unknown_selector() {
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

    #[tokio::test]
    async fn decode_rpc_revert_returns_contract_for_non_revert() {
        let rpc_err = TransportError::local_usage_str("connection refused");

        let result = decode_rpc_revert::<NoOpErrorRegistry>(rpc_err).await;

        assert!(
            matches!(result, EvmError::Contract(_)),
            "expected Contract, got {result:?}"
        );
    }

    #[tokio::test]
    async fn decode_rpc_revert_returns_decoded_for_revert_data() {
        let revert_data = Bytes::from(vec![0x12, 0x34, 0x56, 0x78, 0x00, 0x00, 0x00, 0x00]);
        let rpc_err = create_rpc_error_with_revert_data(&revert_data);

        let result = decode_rpc_revert::<NoOpErrorRegistry>(rpc_err).await;

        assert!(
            matches!(result, EvmError::DecodedRevert(_)),
            "expected DecodedRevert, got {result:?}"
        );
    }

    #[tokio::test]
    async fn decode_reverted_receipt_passes_through_successful() {
        let provider = ProviderBuilder::new().connect_mocked_client(Asserter::new());
        let receipt = successful_receipt();
        let tx_hash = receipt.transaction_hash;

        let result = decode_reverted_receipt::<NoOpErrorRegistry>(
            &provider,
            Address::ZERO,
            Address::ZERO,
            Bytes::new(),
            receipt,
        )
        .await;

        let returned = result.unwrap();
        assert_eq!(returned.transaction_hash, tx_hash);
    }

    #[tokio::test]
    async fn decode_reverted_receipt_returns_reverted_without_block() {
        let provider = ProviderBuilder::new().connect_mocked_client(Asserter::new());
        let receipt = reverted_receipt(None);
        let tx_hash = receipt.transaction_hash;

        let result = decode_reverted_receipt::<NoOpErrorRegistry>(
            &provider,
            Address::ZERO,
            Address::ZERO,
            Bytes::new(),
            receipt,
        )
        .await;

        let error = result.unwrap_err();
        assert!(
            matches!(error, EvmError::Reverted { tx_hash: hash } if hash == tx_hash),
            "expected Reverted with matching hash, got {error:?}"
        );
    }

    #[tokio::test]
    async fn decode_reverted_receipt_decodes_revert_when_replay_fails() {
        let anvil = Anvil::new().spawn();
        let signer: PrivateKeySigner = anvil.keys()[0].clone().into();
        let wallet = EthereumWallet::from(signer);
        let provider = ProviderBuilder::new()
            .wallet(wallet)
            .connect_http(anvil.endpoint_url());

        // Deploy a contract whose runtime code is PUSH0 PUSH0 REVERT (0x5f5ffd).
        // Init code copies the 3-byte runtime from bytecode offset 10 into memory
        // and returns it.
        let deploy_tx =
            TransactionRequest::default().with_deploy_code(hex!("0x6003600a5f3960035ff35f5ffd"));
        let deploy_receipt = provider
            .send_transaction(deploy_tx)
            .await
            .unwrap()
            .get_receipt()
            .await
            .unwrap();
        let reverting_address = deploy_receipt.contract_address.unwrap();
        let block_number = deploy_receipt.block_number.unwrap();

        let receipt = reverted_receipt(Some(block_number));

        let error = decode_reverted_receipt::<NoOpErrorRegistry>(
            &provider,
            Address::ZERO,
            reverting_address,
            Bytes::new(),
            receipt,
        )
        .await
        .unwrap_err();

        // The replay call hits the reverting contract, producing an RPC error
        // that gets decoded via decode_rpc_revert (empty revert data ->
        // Contract variant since NoOpErrorRegistry returns no candidates).
        assert!(
            matches!(error, EvmError::Contract(_)),
            "expected Contract from decoded RPC revert, got {error:?}"
        );
    }

    #[tokio::test]
    async fn decode_reverted_receipt_returns_reverted_when_replay_succeeds() {
        let anvil = Anvil::new().spawn();
        let provider = ProviderBuilder::new()
            .disable_recommended_fillers()
            .connect_http(anvil.endpoint_url());

        // Calling an address with no code succeeds (returns empty bytes),
        // exercising the Ok(_) branch that logs a warning and falls through
        // to EvmError::Reverted.
        let receipt = reverted_receipt(Some(0));
        let tx_hash = receipt.transaction_hash;

        let error = decode_reverted_receipt::<NoOpErrorRegistry>(
            &provider,
            Address::ZERO,
            Address::ZERO,
            Bytes::new(),
            receipt,
        )
        .await
        .unwrap_err();

        assert!(
            matches!(error, EvmError::Reverted { tx_hash: hash } if hash == tx_hash),
            "expected Reverted with matching tx_hash, got {error:?}"
        );
    }
}

//! Contract call submission abstraction.
//!
//! Provides a unified trait for submitting contract calls, with implementations
//! for both local wallet signing (via alloy provider) and Fireblocks CONTRACT_CALL
//! transactions. This enables Fireblocks TAP policy engine rules (contract/method
//! whitelisting) which are not possible with RAW signing.

use alloy::primitives::{Address, B256, Bytes};
use alloy::providers::Provider;
use alloy::rpc::types::TransactionReceipt;
use async_trait::async_trait;
use fireblocks_sdk::apis::transactions_api::CreateTransactionParams;
use fireblocks_sdk::models;
use fireblocks_sdk::models::TransactionStatus;
use fireblocks_sdk::{Client, ClientBuilder};
use std::time::Duration;
use tracing::debug;

use super::config::{ChainAssetIds, FireblocksEnv};

/// Submits encoded calldata to a contract and returns a transaction receipt.
///
/// Two implementations exist:
/// - `AlloyContractCaller`: wraps a wallet-enabled alloy provider
/// - `FireblocksContractCaller`: submits via Fireblocks CONTRACT_CALL API
#[async_trait]
pub(crate) trait ContractCallSubmitter: Send + Sync {
    async fn submit_contract_call(
        &self,
        contract: Address,
        calldata: &Bytes,
        note: &str,
    ) -> Result<TransactionReceipt, ContractCallError>;
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum ContractCallError {
    #[error("RPC transport error")]
    Transport(#[source] alloy::transports::RpcError<alloy::transports::TransportErrorKind>),

    #[error("transaction confirmation failed")]
    PendingTransaction(#[source] alloy::providers::PendingTransactionError),

    #[error("Fireblocks contract call failed")]
    Fireblocks(#[from] FireblocksContractCallError),

    #[error("failed to fetch receipt for tx {tx_hash}")]
    ReceiptFetch { tx_hash: B256 },

    #[error("receipt not found for tx {tx_hash}")]
    MissingReceipt { tx_hash: B256 },
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum FireblocksContractCallError {
    #[error("failed to read Fireblocks secret key from {path}")]
    ReadSecret {
        path: std::path::PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("failed to build Fireblocks client")]
    ClientBuild(#[source] Box<fireblocks_sdk::FireblocksError>),

    #[error("failed to fetch vault deposit addresses")]
    FetchAddresses(#[source] Box<fireblocks_sdk::FireblocksError>),

    #[error("no deposit address found for vault {vault_id}, asset {asset_id}")]
    NoAddress { vault_id: String, asset_id: String },

    #[error("invalid deposit address from Fireblocks: {address}")]
    InvalidAddress {
        address: String,
        #[source]
        source: alloy::hex::FromHexError,
    },

    #[error("failed to create Fireblocks transaction")]
    CreateTransaction(
        #[source]
        Box<
            fireblocks_sdk::apis::Error<
                fireblocks_sdk::apis::transactions_api::CreateTransactionError,
            >,
        >,
    ),

    #[error("Fireblocks transaction did not return an ID")]
    MissingTransactionId,

    #[error("failed to poll Fireblocks transaction {tx_id}")]
    PollTransaction {
        tx_id: String,
        #[source]
        source: Box<fireblocks_sdk::FireblocksError>,
    },

    #[error("Fireblocks transaction {tx_id} reached terminal status: {status:?}")]
    TransactionFailed {
        tx_id: String,
        status: TransactionStatus,
    },

    #[error("Fireblocks transaction {tx_id} returned no tx_hash")]
    MissingTxHash { tx_id: String },

    #[error("invalid tx hash from Fireblocks: {hash}")]
    InvalidTxHash {
        hash: String,
        #[source]
        source: alloy::hex::FromHexError,
    },

    #[error("no asset ID configured for chain {chain_id}")]
    UnknownChain { chain_id: u64 },
}

/// Submits contract calls via a wallet-enabled alloy provider.
///
/// Alloy's filler chain handles gas estimation, nonce, chain_id, and signing.
pub(crate) struct AlloyContractCaller<P> {
    provider: P,
    required_confirmations: u64,
}

impl<P> AlloyContractCaller<P>
where
    P: Provider + Clone + Send + Sync + 'static,
{
    pub(crate) fn new(provider: P, required_confirmations: u64) -> Self {
        Self {
            provider,
            required_confirmations,
        }
    }
}

#[async_trait]
impl<P> ContractCallSubmitter for AlloyContractCaller<P>
where
    P: Provider + Clone + Send + Sync + 'static,
{
    async fn submit_contract_call(
        &self,
        contract: Address,
        calldata: &Bytes,
        _note: &str,
    ) -> Result<TransactionReceipt, ContractCallError> {
        let tx = alloy::rpc::types::TransactionRequest::default()
            .to(contract)
            .input(calldata.clone().into());

        let pending = self
            .provider
            .send_transaction(tx)
            .await
            .map_err(ContractCallError::Transport)?;

        let receipt = pending
            .with_required_confirmations(self.required_confirmations)
            .get_receipt()
            .await
            .map_err(ContractCallError::PendingTransaction)?;

        Ok(receipt)
    }
}

/// Submits contract calls via the Fireblocks CONTRACT_CALL API.
///
/// Fireblocks handles signing and broadcasting. After submission, polls for
/// completion, then fetches the receipt from a read-only provider.
pub(crate) struct FireblocksContractCaller<P> {
    client: Client,
    vault_account_id: String,
    chain_asset_ids: ChainAssetIds,
    read_provider: P,
}

impl<P> FireblocksContractCaller<P>
where
    P: Provider + Clone + Send + Sync + 'static,
{
    pub(crate) fn new(
        env: &FireblocksEnv,
        read_provider: P,
    ) -> Result<Self, FireblocksContractCallError> {
        let secret = std::fs::read(&env.secret_path).map_err(|e| {
            FireblocksContractCallError::ReadSecret {
                path: env.secret_path.clone(),
                source: e,
            }
        })?;

        let mut builder = ClientBuilder::new(&env.api_key, &secret);
        if env.sandbox {
            builder = builder.use_sandbox();
        }
        let client = builder
            .build()
            .map_err(|e| FireblocksContractCallError::ClientBuild(Box::new(e)))?;

        Ok(Self {
            client,
            vault_account_id: env.vault_account_id.clone(),
            chain_asset_ids: env.chain_asset_ids.clone(),
            read_provider,
        })
    }

    /// Determines the asset ID by querying the provider's chain ID.
    async fn resolve_asset_id(&self) -> Result<String, FireblocksContractCallError> {
        let chain_id = self
            .read_provider
            .get_chain_id()
            .await
            .map_err(|_| FireblocksContractCallError::UnknownChain { chain_id: 0 })?;

        self.chain_asset_ids
            .get(chain_id)
            .map(ToString::to_string)
            .ok_or(FireblocksContractCallError::UnknownChain { chain_id })
    }

    async fn submit(
        &self,
        contract_address: Address,
        calldata: &Bytes,
        note: &str,
        asset_id: &str,
    ) -> Result<String, FireblocksContractCallError> {
        let tx_request = build_contract_call_request(
            asset_id,
            &self.vault_account_id,
            contract_address,
            calldata,
            note,
        );

        let params = CreateTransactionParams::builder()
            .transaction_request(tx_request)
            .build();

        let create_response = self
            .client
            .transactions_api()
            .create_transaction(params)
            .await
            .map_err(|e| FireblocksContractCallError::CreateTransaction(Box::new(e)))?;

        create_response
            .id
            .ok_or(FireblocksContractCallError::MissingTransactionId)
    }

    async fn wait_for_completion(&self, tx_id: &str) -> Result<B256, FireblocksContractCallError> {
        debug!(fireblocks_tx_id = %tx_id, "Polling Fireblocks CONTRACT_CALL transaction...");

        let result = self
            .client
            .poll_transaction(
                tx_id,
                Duration::from_secs(300),
                Duration::from_millis(500),
                |tx| {
                    debug!(
                        fireblocks_tx_id = %tx_id,
                        status = ?tx.status,
                        "Polling Fireblocks transaction"
                    );
                },
            )
            .await
            .map_err(|e| FireblocksContractCallError::PollTransaction {
                tx_id: tx_id.to_string(),
                source: Box::new(e),
            })?;

        if result.status != TransactionStatus::Completed {
            return Err(FireblocksContractCallError::TransactionFailed {
                tx_id: tx_id.to_string(),
                status: result.status,
            });
        }

        let tx_hash_str =
            result
                .tx_hash
                .ok_or_else(|| FireblocksContractCallError::MissingTxHash {
                    tx_id: tx_id.to_string(),
                })?;

        let tx_hash_hex = tx_hash_str.strip_prefix("0x").unwrap_or(&tx_hash_str);
        let tx_hash_bytes: [u8; 32] = alloy::hex::decode(tx_hash_hex)
            .map_err(|e| FireblocksContractCallError::InvalidTxHash {
                hash: tx_hash_str.clone(),
                source: e,
            })?
            .try_into()
            .map_err(|_| FireblocksContractCallError::InvalidTxHash {
                hash: tx_hash_str.clone(),
                source: alloy::hex::FromHexError::InvalidStringLength,
            })?;

        Ok(B256::from(tx_hash_bytes))
    }

    async fn fetch_receipt(&self, tx_hash: B256) -> Result<TransactionReceipt, ContractCallError> {
        self.read_provider
            .get_transaction_receipt(tx_hash)
            .await
            .map_err(|_| ContractCallError::ReceiptFetch { tx_hash })?
            .ok_or(ContractCallError::MissingReceipt { tx_hash })
    }
}

#[async_trait]
impl<P> ContractCallSubmitter for FireblocksContractCaller<P>
where
    P: Provider + Clone + Send + Sync + 'static,
{
    async fn submit_contract_call(
        &self,
        contract: Address,
        calldata: &Bytes,
        note: &str,
    ) -> Result<TransactionReceipt, ContractCallError> {
        let asset_id = self.resolve_asset_id().await?;

        let tx_id = self.submit(contract, calldata, note, &asset_id).await?;

        let tx_hash = self.wait_for_completion(&tx_id).await?;

        self.fetch_receipt(tx_hash).await
    }
}

/// Builds a Fireblocks CONTRACT_CALL transaction request.
fn build_contract_call_request(
    asset_id: &str,
    vault_account_id: &str,
    contract_address: Address,
    calldata: &Bytes,
    note: &str,
) -> models::TransactionRequest {
    let extra_parameters = models::ExtraParameters {
        contract_call_data: Some(alloy::hex::encode(calldata)),
        raw_message_data: None,
        inputs_selection: None,
        node_controls: None,
        program_call_data: None,
    };

    models::TransactionRequest {
        operation: Some(models::TransactionOperation::ContractCall),
        asset_id: Some(asset_id.to_string()),
        source: Some(models::SourceTransferPeerPath {
            r#type: models::TransferPeerPathType::VaultAccount,
            id: Some(vault_account_id.to_string()),
            sub_type: None,
            name: None,
            wallet_id: None,
            is_collateral: None,
        }),
        destination: Some(models::DestinationTransferPeerPath {
            r#type: models::TransferPeerPathType::OneTimeAddress,
            one_time_address: Some(models::OneTimeAddress::new(format!("{contract_address:?}"))),
            sub_type: None,
            id: None,
            name: None,
            wallet_id: None,
            is_collateral: None,
        }),
        amount: Some(models::TransactionRequestAmount::String("0".to_string())),
        extra_parameters: Some(extra_parameters),
        external_tx_id: Some(uuid::Uuid::new_v4().to_string()),
        note: Some(note.to_string()),
        fee_level: Some(models::transaction_request::FeeLevel::Medium),
        destinations: None,
        treat_as_gross_amount: None,
        force_sweep: None,
        fee: None,
        priority_fee: None,
        fail_on_low_fee: None,
        max_fee: None,
        gas_limit: None,
        gas_price: None,
        network_fee: None,
        replace_tx_by_hash: None,
        customer_ref_id: None,
        travel_rule_message: None,
        auto_staking: None,
        network_staking: None,
        cpu_staking: None,
        use_gasless: None,
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::bytes;

    use super::*;

    #[test]
    fn build_contract_call_request_sets_operation_to_contract_call() {
        let calldata = Bytes::from(vec![0xab, 0xcd, 0xef, 0x12]);
        let contract = Address::ZERO;

        let req = build_contract_call_request(
            "BASECHAIN_ETH",
            "vault_0",
            contract,
            &calldata,
            "test deposit",
        );

        assert_eq!(
            req.operation,
            Some(models::TransactionOperation::ContractCall)
        );
    }

    #[test]
    fn build_contract_call_request_sets_asset_id() {
        let req =
            build_contract_call_request("ETH", "vault_0", Address::ZERO, &Bytes::new(), "test");

        assert_eq!(req.asset_id.as_deref(), Some("ETH"));
    }

    #[test]
    fn build_contract_call_request_sets_source_vault() {
        let req =
            build_contract_call_request("ETH", "my_vault", Address::ZERO, &Bytes::new(), "test");

        let source = req.source.unwrap();
        assert_eq!(source.r#type, models::TransferPeerPathType::VaultAccount);
        assert_eq!(source.id.as_deref(), Some("my_vault"));
    }

    #[test]
    fn build_contract_call_request_sets_destination_as_one_time_address() {
        let contract = "0x1234567890abcdef1234567890abcdef12345678"
            .parse::<Address>()
            .unwrap();

        let req = build_contract_call_request("ETH", "vault_0", contract, &Bytes::new(), "test");

        let dest = req.destination.unwrap();
        assert_eq!(dest.r#type, models::TransferPeerPathType::OneTimeAddress);
        let ota = dest.one_time_address.unwrap();
        assert!(
            ota.address.contains("1234567890abcdef"),
            "expected contract address in one-time address, got: {ota:?}"
        );
    }

    #[test]
    fn build_contract_call_request_encodes_calldata() {
        let calldata = bytes!("a9059cbb0000000000000000000000001234");

        let req = build_contract_call_request("ETH", "vault_0", Address::ZERO, &calldata, "test");

        let extra = req.extra_parameters.unwrap();
        let encoded = extra.contract_call_data.unwrap();
        assert_eq!(
            encoded,
            alloy::hex::encode(&calldata),
            "calldata should be hex-encoded without 0x prefix"
        );
    }

    #[test]
    fn build_contract_call_request_sets_zero_amount() {
        let req =
            build_contract_call_request("ETH", "vault_0", Address::ZERO, &Bytes::new(), "test");

        assert_eq!(
            req.amount,
            Some(models::TransactionRequestAmount::String("0".to_string()))
        );
    }

    #[test]
    fn build_contract_call_request_sets_medium_fee_level() {
        let req =
            build_contract_call_request("ETH", "vault_0", Address::ZERO, &Bytes::new(), "test");

        assert_eq!(
            req.fee_level,
            Some(models::transaction_request::FeeLevel::Medium)
        );
    }

    #[test]
    fn build_contract_call_request_includes_note() {
        let req = build_contract_call_request(
            "ETH",
            "vault_0",
            Address::ZERO,
            &Bytes::new(),
            "vault deposit 1000 USDC",
        );

        assert_eq!(req.note.as_deref(), Some("vault deposit 1000 USDC"));
    }

    #[test]
    fn build_contract_call_request_sets_external_tx_id() {
        let req =
            build_contract_call_request("ETH", "vault_0", Address::ZERO, &Bytes::new(), "test");

        assert!(
            req.external_tx_id.is_some(),
            "should have a UUID external_tx_id"
        );
    }
}

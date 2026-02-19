//! Fireblocks MPC-based transaction submission.
//!
//! `FireblocksWallet` submits CONTRACT_CALL transactions via the
//! Fireblocks API and polls until completion. A read-only provider
//! fetches the transaction receipt after Fireblocks confirms. This
//! module is only compiled when the `fireblocks` feature is enabled.

use alloy::primitives::{Address, Bytes, TxHash};
use alloy::providers::{PendingTransactionBuilder, Provider};
use alloy::rpc::types::TransactionReceipt;
use alloy::transports::{RpcError, TransportErrorKind};
use async_trait::async_trait;
use fireblocks_sdk::apis::transactions_api::{CreateTransactionError, CreateTransactionParams};
use fireblocks_sdk::apis::vaults_api::{
    GetVaultAccountAssetAddressesPaginatedError, GetVaultAccountAssetAddressesPaginatedParams,
};
use fireblocks_sdk::models::{self, TransactionOperation, TransactionStatus};
use fireblocks_sdk::{Client, ClientBuilder};
use serde::Deserialize;
use std::collections::BTreeMap;
use std::time::Duration;
use tracing::{debug, info, warn};

use crate::{Evm, EvmError, Wallet};

/// Polling timeout for Fireblocks transaction completion.
const POLL_TIMEOUT: Duration = Duration::from_secs(600);

/// Polling interval between status checks.
const POLL_INTERVAL: Duration = Duration::from_millis(500);

/// Fireblocks API user ID for authentication.
#[derive(Debug, Clone, Deserialize)]
#[serde(transparent)]
pub struct FireblocksApiUserId(String);

impl FireblocksApiUserId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for FireblocksApiUserId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// Fireblocks vault account identifier.
#[derive(Debug, Clone, Deserialize)]
#[serde(transparent)]
pub struct FireblocksVaultAccountId(String);

impl FireblocksVaultAccountId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for FireblocksVaultAccountId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// Fireblocks asset identifier (e.g. "ETH", "BASECHAIN_ETH").
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(transparent)]
pub struct AssetId(String);

impl AssetId {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for AssetId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// Chain ID to Fireblocks asset ID mapping.
///
/// Deserialized from a TOML table:
/// ```toml
/// [rebalancing.fireblocks_chain_asset_ids]
/// 1 = "ETH"
/// 8453 = "BASECHAIN_ETH"
/// ```
#[derive(Debug, Clone, Deserialize)]
#[serde(transparent)]
pub struct ChainAssetIds(BTreeMap<u64, AssetId>);

impl ChainAssetIds {
    pub fn get(&self, chain_id: u64) -> Option<&AssetId> {
        self.0.get(&chain_id)
    }
}

/// Fireblocks environment selector.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum FireblocksEnvironment {
    Production,
    Sandbox,
}

/// Construction context for `FireblocksCaller`.
///
/// Contains everything needed to construct a caller: Fireblocks API
/// credentials, chain-specific asset ID, and a read-only provider
/// for fetching receipts. The main crate assembles this from its own
/// config/secrets split.
pub struct FireblocksCtx<P> {
    pub api_user_id: FireblocksApiUserId,
    pub secret: Vec<u8>,
    pub vault_account_id: FireblocksVaultAccountId,
    pub environment: FireblocksEnvironment,
    pub asset_id: AssetId,
    pub provider: P,
    pub required_confirmations: u64,
}

impl<P> std::fmt::Debug for FireblocksCtx<P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FireblocksCtx")
            .field("api_user_id", &self.api_user_id)
            .field("secret", &"[REDACTED]")
            .field("vault_account_id", &self.vault_account_id)
            .field("environment", &self.environment)
            .field("asset_id", &self.asset_id)
            .finish_non_exhaustive()
    }
}

impl<P> FireblocksCtx<P> {
    fn build_client(&self) -> Result<Client, FireblocksError> {
        let mut builder = ClientBuilder::new(self.api_user_id.as_str(), &self.secret);
        if self.environment == FireblocksEnvironment::Sandbox {
            builder = builder.use_sandbox();
        }
        Ok(builder.build()?)
    }
}

/// Errors specific to the Fireblocks signing backend.
#[derive(Debug, thiserror::Error)]
pub enum FireblocksError {
    #[error("Fireblocks SDK error: {0}")]
    Sdk(#[from] fireblocks_sdk::FireblocksError),
    #[error("Fireblocks create transaction API error: {0:?}")]
    CreateTransaction(#[from] fireblocks_sdk::apis::Error<CreateTransactionError>),
    #[error("Fireblocks vault addresses API error: {0:?}")]
    VaultAddresses(
        #[from] fireblocks_sdk::apis::Error<GetVaultAccountAssetAddressesPaginatedError>,
    ),
    #[error("RPC error: {0}")]
    Rpc(#[from] RpcError<TransportErrorKind>),
    #[error("invalid hex from Fireblocks: {0}")]
    Hex(#[from] alloy::hex::FromHexError),
    #[error("Fireblocks response did not return a transaction ID")]
    MissingTransactionId,
    #[error(
        "Fireblocks transaction {tx_id} reached terminal \
         status: {status:?}"
    )]
    TransactionFailed {
        tx_id: String,
        status: TransactionStatus,
    },
    #[error("Fireblocks transaction {tx_id} did not include a transaction hash")]
    MissingTxHash { tx_id: String },
    #[error("no deposit address found for vault {vault_account_id} asset {asset_id}")]
    NoDepositAddress {
        vault_account_id: FireblocksVaultAccountId,
        asset_id: AssetId,
    },
}

/// Wallet that submits transactions via Fireblocks MPC.
pub struct FireblocksWallet<P> {
    client: Client,
    vault_account_id: FireblocksVaultAccountId,
    asset_id: AssetId,
    provider: P,
    address: Address,
    required_confirmations: u64,
}

impl<P> FireblocksWallet<P> {
    /// Builds a Fireblocks SDK client, resolves the vault deposit address
    /// from the Fireblocks API, and returns a ready-to-use caller.
    pub async fn new(ctx: FireblocksCtx<P>) -> Result<Self, FireblocksError> {
        let client = ctx.build_client()?;

        let params = GetVaultAccountAssetAddressesPaginatedParams {
            vault_account_id: ctx.vault_account_id.0.clone(),
            asset_id: ctx.asset_id.0.clone(),
            limit: None,
            before: None,
            after: None,
        };

        let addresses = client
            .vaults_api()
            .get_vault_account_asset_addresses_paginated(params)
            .await?;

        let address_str = addresses
            .addresses
            .and_then(|addrs| addrs.into_iter().next())
            .and_then(|entry| entry.address)
            .ok_or_else(|| {
                warn!(
                    vault_id = ctx.vault_account_id.as_str(),
                    asset_id = ctx.asset_id.as_str(),
                    "No deposit address found"
                );
                FireblocksError::NoDepositAddress {
                    vault_account_id: ctx.vault_account_id.clone(),
                    asset_id: ctx.asset_id.clone(),
                }
            })?;

        let address = address_str.parse()?;

        Ok(Self {
            client,
            vault_account_id: ctx.vault_account_id,
            asset_id: ctx.asset_id,
            provider: ctx.provider,
            address,
            required_confirmations: ctx.required_confirmations,
        })
    }
}

#[async_trait]
impl<P> Evm for FireblocksWallet<P>
where
    P: Provider + Clone + Send + Sync + 'static,
{
    type Provider = P;

    fn provider(&self) -> &P {
        &self.provider
    }
}

#[async_trait]
impl<P> Wallet for FireblocksWallet<P>
where
    P: Provider + Clone + Send + Sync + 'static,
{
    fn address(&self) -> Address {
        self.address
    }

    async fn send(
        &self,
        contract: Address,
        calldata: Bytes,
        note: &str,
    ) -> Result<TransactionReceipt, EvmError> {
        let external_tx_id = generate_external_tx_id(note);

        let tx_request = build_contract_call_request(
            self.asset_id.as_str(),
            self.vault_account_id.as_str(),
            contract,
            &calldata,
            note,
            &external_tx_id,
        );

        info!(
            %contract,
            note,
            %external_tx_id,
            "Submitting Fireblocks CONTRACT_CALL"
        );

        let params = CreateTransactionParams::builder()
            .transaction_request(tx_request)
            .build();

        let create_response = self
            .client
            .transactions_api()
            .create_transaction(params)
            .await?;

        let tx_id = create_response
            .id
            .ok_or(EvmError::Fireblocks(FireblocksError::MissingTransactionId))?;

        info!(
            fireblocks_tx_id = %tx_id,
            %contract,
            note,
            "Fireblocks transaction created, polling for completion"
        );

        let result = self
            .client
            .poll_transaction(&tx_id, POLL_TIMEOUT, POLL_INTERVAL, |tx| {
                debug!(
                    fireblocks_tx_id = %tx_id,
                    status = ?tx.status,
                    "Polling Fireblocks transaction"
                );
            })
            .await?;

        if result.status != TransactionStatus::Completed {
            if is_still_pending(result.status) {
                warn!(
                    fireblocks_tx_id = %tx_id,
                    status = ?result.status,
                    "Polling timed out but transaction may still confirm on-chain"
                );
            }
            return Err(EvmError::Fireblocks(FireblocksError::TransactionFailed {
                tx_id,
                status: result.status,
            }));
        }

        let tx_hash_str = result.tx_hash.ok_or_else(|| {
            EvmError::Fireblocks(FireblocksError::MissingTxHash {
                tx_id: tx_id.clone(),
            })
        })?;

        let tx_hash: TxHash = tx_hash_str.parse()?;

        info!(
            fireblocks_tx_id = %tx_id,
            %tx_hash,
            note,
            required_confirmations = self.required_confirmations,
            "Fireblocks transaction completed, waiting for confirmations"
        );

        let receipt = PendingTransactionBuilder::new(self.provider.root().clone(), tx_hash)
            .with_required_confirmations(self.required_confirmations)
            .get_receipt()
            .await?;

        if !receipt.status() {
            return Err(EvmError::Reverted {
                tx_hash: receipt.transaction_hash,
            });
        }

        info!(
            %tx_hash,
            note,
            "Fireblocks contract call confirmed"
        );

        Ok(receipt)
    }
}

fn build_contract_call_request(
    asset_id: &str,
    vault_account_id: &str,
    contract_address: Address,
    calldata: &Bytes,
    note: &str,
    external_tx_id: &str,
) -> models::TransactionRequest {
    let extra_parameters = models::ExtraParameters {
        contract_call_data: Some(alloy::hex::encode(calldata)),
        raw_message_data: None,
        inputs_selection: None,
        node_controls: None,
        program_call_data: None,
    };

    models::TransactionRequest {
        operation: Some(TransactionOperation::ContractCall),
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
            one_time_address: Some(models::OneTimeAddress::new(contract_address.to_string())),
            sub_type: None,
            id: None,
            name: None,
            wallet_id: None,
            is_collateral: None,
        }),
        amount: Some(models::TransactionRequestAmount::String("0".to_string())),
        extra_parameters: Some(extra_parameters),
        external_tx_id: Some(external_tx_id.to_string()),
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

fn generate_external_tx_id(note: &str) -> String {
    let timestamp = chrono::Utc::now().format("%Y%m%dT%H%M%SZ");
    let uuid = uuid::Uuid::new_v4();
    format!("{timestamp}-{note}-{uuid}")
}

/// Returns `true` if the transaction is still in a non-terminal state
/// and may eventually confirm on-chain.
fn is_still_pending(status: TransactionStatus) -> bool {
    use TransactionStatus::*;

    match status {
        Submitted
        | PendingAmlScreening
        | PendingEnrichment
        | PendingAuthorization
        | Queued
        | PendingSignature
        | Pending3RdPartyManualApproval
        | Pending3RdParty
        | Broadcasting
        | Confirming => true,

        Completed | Cancelling | Cancelled | Blocked | Rejected | Failed => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generate_external_tx_id_contains_note() {
        let tx_id = generate_external_tx_id("ERC20 approve");
        assert!(
            tx_id.contains("ERC20 approve"),
            "Expected note in external_tx_id, got: {tx_id}"
        );
    }

    #[test]
    fn generate_external_tx_id_contains_timestamp() {
        let tx_id = generate_external_tx_id("test");
        // Should contain a timestamp-like pattern YYYYMMDD
        let year = chrono::Utc::now().format("%Y").to_string();
        assert!(
            tx_id.contains(&year),
            "Expected year in external_tx_id, got: {tx_id}"
        );
    }

    #[test]
    fn generate_external_tx_id_is_unique() {
        let id1 = generate_external_tx_id("test");
        let id2 = generate_external_tx_id("test");
        assert_ne!(id1, id2, "External tx IDs should be unique");
    }

    #[test]
    fn build_contract_call_request_sets_operation() {
        let request = build_contract_call_request(
            "ETH",
            "0",
            Address::ZERO,
            &Bytes::from(vec![0x12, 0x34]),
            "test",
            "ext-123",
        );

        assert_eq!(request.operation, Some(TransactionOperation::ContractCall));
    }

    #[test]
    fn build_contract_call_request_sets_calldata_without_0x() {
        let calldata = Bytes::from(vec![0xab, 0xcd, 0xef]);
        let request =
            build_contract_call_request("ETH", "0", Address::ZERO, &calldata, "test", "ext-123");

        let contract_call_data = request
            .extra_parameters
            .unwrap()
            .contract_call_data
            .unwrap();

        assert_eq!(contract_call_data, "abcdef");
        assert!(
            !contract_call_data.starts_with("0x"),
            "contract_call_data should not have 0x prefix"
        );
    }

    #[test]
    fn build_contract_call_request_sets_zero_amount() {
        let request = build_contract_call_request(
            "ETH",
            "0",
            Address::ZERO,
            &Bytes::new(),
            "test",
            "ext-123",
        );

        assert!(matches!(
            request.amount,
            Some(models::TransactionRequestAmount::String(ref amount)) if amount == "0"
        ));
    }

    #[test]
    fn is_still_pending_for_submitted() {
        assert!(is_still_pending(TransactionStatus::Submitted));
    }

    #[test]
    fn is_still_pending_for_confirming() {
        assert!(is_still_pending(TransactionStatus::Confirming));
    }

    #[test]
    fn is_not_pending_for_completed() {
        assert!(!is_still_pending(TransactionStatus::Completed));
    }

    #[test]
    fn is_not_pending_for_failed() {
        assert!(!is_still_pending(TransactionStatus::Failed));
    }

    #[test]
    fn is_not_pending_for_cancelled() {
        assert!(!is_still_pending(TransactionStatus::Cancelled));
    }

    #[test]
    fn is_not_pending_for_rejected() {
        assert!(!is_still_pending(TransactionStatus::Rejected));
    }

    #[test]
    fn chain_asset_ids_deserializes_from_json_table() {
        let json = serde_json::json!({"1": "ETH", "8453": "BASECHAIN_ETH"});
        let ids: ChainAssetIds = serde_json::from_value(json).unwrap();
        assert_eq!(ids.get(1).unwrap().as_str(), "ETH");
        assert_eq!(ids.get(8453).unwrap().as_str(), "BASECHAIN_ETH");
        assert!(ids.get(42).is_none());
    }

    #[test]
    fn fireblocks_environment_deserializes_lowercase() {
        let prod: FireblocksEnvironment = serde_json::from_str("\"production\"").unwrap();
        assert_eq!(prod, FireblocksEnvironment::Production);

        let sandbox: FireblocksEnvironment = serde_json::from_str("\"sandbox\"").unwrap();
        assert_eq!(sandbox, FireblocksEnvironment::Sandbox);
    }

    #[test]
    fn fireblocks_api_user_id_deserializes_from_string() {
        let user_id: FireblocksApiUserId = serde_json::from_str("\"my-api-user\"").unwrap();
        assert_eq!(user_id.as_str(), "my-api-user");
    }

    #[test]
    fn fireblocks_vault_account_id_deserializes_from_string() {
        let vault_id: FireblocksVaultAccountId = serde_json::from_str("\"42\"").unwrap();
        assert_eq!(vault_id.as_str(), "42");
    }
}

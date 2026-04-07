//! Fireblocks-backed contract call submission.
//!
//! `FireblocksWallet` resolves its onchain address from the configured
//! Fireblocks vault and submits EVM contract calls through the Fireblocks API.

use alloy::primitives::{Address, Bytes, TxHash};
use alloy::providers::{PendingTransactionBuilder, Provider};
use alloy::rpc::types::TransactionReceipt;
use async_trait::async_trait;
use chrono::Utc;
use fireblocks_sdk::Client;
use fireblocks_sdk::ClientBuilder;
use fireblocks_sdk::apis::transactions_api::{CreateTransactionError, CreateTransactionParams};
use fireblocks_sdk::apis::whitelisted_contracts_api::GetContractsError;
use fireblocks_sdk::models;
use serde::Deserialize;
use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::{Evm, EvmError, TryIntoWallet, Wallet, WalletCtx};

const FIREBLOCKS_POLL_TIMEOUT: Duration = Duration::from_secs(600);
const FIREBLOCKS_POLL_INTERVAL: Duration = Duration::from_millis(500);
const FIREBLOCKS_EXTERNAL_TX_ID_MAX_LEN: usize = 255;
const FIREBLOCKS_EXTERNAL_TX_ID_ELLIPSIS: &str = "...";

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum FireblocksEnvironment {
    Production,
    Sandbox,
}

#[derive(Clone, Deserialize)]
#[serde(transparent)]
pub struct FireblocksApiKey(String);

impl FireblocksApiKey {
    fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Debug for FireblocksApiKey {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("FireblocksApiKey(<redacted>)")
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(transparent)]
pub struct FireblocksVaultAccountId(String);

impl FireblocksVaultAccountId {
    fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for FireblocksVaultAccountId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(transparent)]
pub struct FireblocksAssetId(String);

impl FireblocksAssetId {
    fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for FireblocksAssetId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct FireblocksSettings {
    pub vault_account_id: FireblocksVaultAccountId,
    pub environment: FireblocksEnvironment,
    pub chain_asset_ids: HashMap<u64, FireblocksAssetId>,
}

#[derive(Clone, Deserialize)]
pub struct FireblocksCredentials {
    pub api_key: FireblocksApiKey,
    pub api_private_key_path: PathBuf,
}

impl std::fmt::Debug for FireblocksCredentials {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("FireblocksCredentials")
            .field("api_key", &self.api_key)
            .field("api_private_key_path", &self.api_private_key_path)
            .finish()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum FireblocksError {
    #[error("failed to read Fireblocks API private key from {path}: {source}")]
    ApiPrivateKeyRead {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("Fireblocks SDK error: {0}")]
    Sdk(#[from] fireblocks_sdk::FireblocksError),
    #[error("Fireblocks create transaction API error: {0:?}")]
    CreateTransaction(#[from] fireblocks_sdk::apis::Error<CreateTransactionError>),
    #[error("Fireblocks get contracts API error: {0:?}")]
    GetContracts(#[from] fireblocks_sdk::apis::Error<GetContractsError>),
    #[error("invalid chain id {chain_id} for Fireblocks asset mapping")]
    UnsupportedChain { chain_id: u64 },
    #[error("no Fireblocks address found for vault {vault_account_id} asset {asset_id}")]
    MissingVaultAddress {
        vault_account_id: FireblocksVaultAccountId,
        asset_id: FireblocksAssetId,
    },
    #[error("contract {contract} is not whitelisted in Fireblocks contract wallets")]
    ContractNotWhitelisted { contract: Address },
    #[error("invalid Fireblocks address {address}: {source}")]
    InvalidVaultAddress {
        address: String,
        source: alloy::hex::FromHexError,
    },
    #[error("invalid Fireblocks transaction hash {tx_hash}: {source}")]
    InvalidTxHash {
        tx_hash: String,
        source: alloy::hex::FromHexError,
    },
    #[error("Fireblocks response did not return a transaction ID")]
    MissingTransactionId,
    #[error("Fireblocks transaction {tx_id} polling timed out with status: {status:?}")]
    TransactionStillPending {
        tx_id: String,
        status: models::TransactionStatus,
    },
    #[error("Fireblocks transaction {tx_id} reached terminal status: {status:?}")]
    TransactionFailed {
        tx_id: String,
        status: models::TransactionStatus,
        tx_hash: Option<String>,
    },
    #[error("Fireblocks transaction {tx_id} did not include a transaction hash")]
    MissingTxHash { tx_id: String },
}

pub struct FireblocksWallet<P: Provider> {
    client: Client,
    provider: P,
    address: Address,
    asset_id: FireblocksAssetId,
    vault_account_id: FireblocksVaultAccountId,
    required_confirmations: u64,
}

impl<P: Provider> std::fmt::Debug for FireblocksWallet<P> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("FireblocksWallet")
            .field("address", &self.address)
            .field("asset_id", &self.asset_id)
            .field("vault_account_id", &self.vault_account_id)
            .field("required_confirmations", &self.required_confirmations)
            .finish_non_exhaustive()
    }
}

impl<P: Provider + Clone + Send + Sync + 'static> FireblocksWallet<P> {
    pub async fn new(
        ctx: WalletCtx<FireblocksSettings, FireblocksCredentials, P>,
    ) -> Result<Self, EvmError> {
        let chain_id = ctx.provider.get_chain_id().await?;
        let asset_id = asset_id_for_chain(&ctx.settings.chain_asset_ids, chain_id).cloned()?;
        let client = fireblocks_client(&ctx.credentials, &ctx.settings.environment)?;
        let address =
            fetch_vault_address(&client, &ctx.settings.vault_account_id, &asset_id).await?;

        Ok(Self {
            client,
            provider: ctx.provider,
            address,
            asset_id,
            vault_account_id: ctx.settings.vault_account_id,
            required_confirmations: ctx.required_confirmations,
        })
    }

    async fn resolve_contract_wallet(&self, contract: Address) -> Result<String, FireblocksError> {
        let contract_address = contract.to_string().to_lowercase();
        let expected_asset_id = self.asset_id.as_str();

        self.client
            .wallet_contract_api()
            .get_contracts()
            .await?
            .into_iter()
            .find_map(|wallet| {
                wallet
                    .assets
                    .iter()
                    .any(|asset| {
                        asset.id.as_ref().is_some_and(|id| id == expected_asset_id)
                            && asset
                                .address
                                .as_ref()
                                .is_some_and(|address| address.to_lowercase() == contract_address)
                    })
                    .then_some(wallet.id)
            })
            .ok_or(FireblocksError::ContractNotWhitelisted { contract })
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
        let wallet_id = self.resolve_contract_wallet(contract).await?;
        let external_tx_id = generate_external_tx_id(note);
        let request = build_contract_call_request(
            &self.asset_id,
            &self.vault_account_id,
            &wallet_id,
            &calldata,
            note,
            &external_tx_id,
        );
        let params = CreateTransactionParams::builder()
            .transaction_request(request)
            .build();

        info!(
            %contract,
            note,
            %external_tx_id,
            "Submitting Fireblocks contract call"
        );

        let create_response = self
            .client
            .transactions_api()
            .create_transaction(params)
            .await
            .map_err(FireblocksError::CreateTransaction)?;
        let tx_id = create_response
            .id
            .ok_or(FireblocksError::MissingTransactionId)?;

        info!(
            fireblocks_tx_id = %tx_id,
            %contract,
            note,
            "Fireblocks transaction created, polling for completion"
        );

        let result = poll_until_terminal(
            &self.client,
            &tx_id,
            FIREBLOCKS_POLL_TIMEOUT,
            FIREBLOCKS_POLL_INTERVAL,
        )
        .await;

        let tx_hash = match result {
            Ok(response) => transaction_hash_for_response(&tx_id, response.tx_hash)?,
            Err(FireblocksError::TransactionFailed {
                tx_id: failed_tx_id,
                status,
                tx_hash,
            }) => {
                let terminal_error = FireblocksError::TransactionFailed {
                    tx_id: failed_tx_id.clone(),
                    status,
                    tx_hash: tx_hash.clone(),
                };
                let Some(tx_hash_str) = tx_hash else {
                    return Err(terminal_error.into());
                };
                let tx_hash = parse_tx_hash(tx_hash_str)?;

                info!(
                    fireblocks_tx_id = %failed_tx_id,
                    %tx_hash,
                    note,
                    "Fireblocks reported terminal failure after broadcast, fetching receipt"
                );

                match wait_for_receipt(&self.provider, tx_hash, self.required_confirmations).await {
                    Ok(receipt) => return Ok(receipt),
                    Err(error) => {
                        warn!(
                            fireblocks_tx_id = %failed_tx_id,
                            %tx_hash,
                            ?error,
                            "Failed to fetch receipt for terminal Fireblocks transaction"
                        );

                        return Err(terminal_error.into());
                    }
                }
            }
            Err(error) => return Err(error.into()),
        };

        info!(
            fireblocks_tx_id = %tx_id,
            %tx_hash,
            note,
            "Watching Fireblocks transaction for receipt"
        );

        let receipt =
            wait_for_receipt(&self.provider, tx_hash, self.required_confirmations).await?;

        info!(%tx_hash, note, "Fireblocks transaction confirmed");

        Ok(receipt)
    }
}

#[async_trait]
impl<P> TryIntoWallet for FireblocksWallet<P>
where
    P: Provider + Clone + Send + Sync + 'static,
{
    type Settings = FireblocksSettings;
    type Credentials = FireblocksCredentials;

    async fn try_from_ctx(
        ctx: WalletCtx<FireblocksSettings, FireblocksCredentials, P>,
    ) -> Result<Self, EvmError> {
        Self::new(ctx).await
    }
}

fn fireblocks_client(
    credentials: &FireblocksCredentials,
    environment: &FireblocksEnvironment,
) -> Result<Client, FireblocksError> {
    let secret_path = credentials.api_private_key_path.clone();
    let secret =
        std::fs::read(&secret_path).map_err(|source| FireblocksError::ApiPrivateKeyRead {
            path: secret_path,
            source,
        })?;

    let builder = ClientBuilder::new(credentials.api_key.as_str(), &secret)
        .with_timeout(Duration::from_secs(15))
        .with_connect_timeout(Duration::from_secs(5));

    let builder = match environment {
        FireblocksEnvironment::Production => builder,
        FireblocksEnvironment::Sandbox => builder.with_sandbox(),
    };

    builder.build().map_err(FireblocksError::from)
}

fn asset_id_for_chain(
    chain_asset_ids: &HashMap<u64, FireblocksAssetId>,
    chain_id: u64,
) -> Result<&FireblocksAssetId, FireblocksError> {
    chain_asset_ids
        .get(&chain_id)
        .ok_or(FireblocksError::UnsupportedChain { chain_id })
}

async fn fetch_vault_address(
    client: &Client,
    vault_account_id: &FireblocksVaultAccountId,
    asset_id: &FireblocksAssetId,
) -> Result<Address, FireblocksError> {
    let addresses = client
        .addresses(vault_account_id.as_str(), asset_id.as_str())
        .await?;

    let address = addresses
        .into_iter()
        .find_map(|entry| entry.address)
        .ok_or_else(|| FireblocksError::MissingVaultAddress {
            vault_account_id: vault_account_id.clone(),
            asset_id: asset_id.clone(),
        })?;

    address
        .parse()
        .map_err(|source| FireblocksError::InvalidVaultAddress { address, source })
}

fn build_contract_call_request(
    asset_id: &FireblocksAssetId,
    vault_account_id: &FireblocksVaultAccountId,
    wallet_id: &str,
    calldata: &Bytes,
    note: &str,
    external_tx_id: &str,
) -> models::TransactionRequest {
    models::TransactionRequest {
        operation: Some(models::TransactionOperation::ContractCall),
        note: Some(note.to_string()),
        external_tx_id: Some(external_tx_id.to_string()),
        asset_id: Some(asset_id.as_str().to_string()),
        source: Some(models::SourceTransferPeerPath {
            r#type: models::TransferPeerPathType::VaultAccount,
            id: Some(vault_account_id.as_str().to_string()),
            ..Default::default()
        }),
        destination: Some(models::DestinationTransferPeerPath {
            r#type: models::TransferPeerPathType::ExternalWallet,
            id: Some(wallet_id.to_string()),
            ..Default::default()
        }),
        amount: Some(models::TransactionRequestAmount::String("0".to_string())),
        fee_level: Some(models::transaction_request::FeeLevel::Medium),
        extra_parameters: Some(models::ExtraParameters {
            contract_call_data: Some(alloy::hex::encode(calldata)),
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn generate_external_tx_id(note: &str) -> String {
    let timestamp = Utc::now().format("%Y%m%dT%H%M%SZ").to_string();
    let request_id = Uuid::new_v4().to_string();
    let separator_count = 2;
    let available_for_note =
        FIREBLOCKS_EXTERNAL_TX_ID_MAX_LEN - timestamp.len() - request_id.len() - separator_count;

    let truncated_note = if note.len() <= available_for_note {
        note.to_string()
    } else {
        let truncation_point = available_for_note - FIREBLOCKS_EXTERNAL_TX_ID_ELLIPSIS.len();
        format!(
            "{}{FIREBLOCKS_EXTERNAL_TX_ID_ELLIPSIS}",
            &note[..truncation_point]
        )
    };

    format!("{timestamp}-{truncated_note}-{request_id}")
}

fn transaction_hash_for_response(
    tx_id: &str,
    tx_hash: Option<String>,
) -> Result<TxHash, FireblocksError> {
    let tx_hash = tx_hash.ok_or_else(|| FireblocksError::MissingTxHash {
        tx_id: tx_id.to_string(),
    })?;

    parse_tx_hash(tx_hash)
}

fn parse_tx_hash(tx_hash: String) -> Result<TxHash, FireblocksError> {
    tx_hash
        .parse()
        .map_err(|source| FireblocksError::InvalidTxHash { tx_hash, source })
}

async fn wait_for_receipt<P: Provider>(
    provider: &P,
    tx_hash: TxHash,
    required_confirmations: u64,
) -> Result<TransactionReceipt, EvmError> {
    PendingTransactionBuilder::new(provider.root().clone(), tx_hash)
        .with_required_confirmations(required_confirmations)
        .get_receipt()
        .await
        .map_err(EvmError::from)
}

async fn poll_until_terminal(
    client: &Client,
    tx_id: &str,
    timeout: Duration,
    interval: Duration,
) -> Result<models::TransactionResponse, FireblocksError> {
    let deadline = tokio::time::Instant::now() + timeout;

    loop {
        let response = client.get_transaction(tx_id).await?;

        debug!(
            fireblocks_tx_id = %tx_id,
            status = ?response.status,
            "Polling Fireblocks transaction"
        );

        use models::TransactionStatus::{
            Blocked, Broadcasting, Cancelled, Cancelling, Completed, Confirming, Failed,
            Pending3RdParty, Pending3RdPartyManualApproval, PendingAmlScreening,
            PendingAuthorization, PendingEnrichment, PendingSignature, Queued, Rejected, Submitted,
        };

        match response.status {
            Completed => return Ok(response),
            Failed | Cancelled | Cancelling | Blocked | Rejected => {
                return Err(FireblocksError::TransactionFailed {
                    tx_id: tx_id.to_string(),
                    status: response.status,
                    tx_hash: response.tx_hash.clone(),
                });
            }
            Submitted
            | PendingAmlScreening
            | PendingEnrichment
            | PendingAuthorization
            | Queued
            | PendingSignature
            | Pending3RdPartyManualApproval
            | Pending3RdParty
            | Broadcasting
            | Confirming => {
                if tokio::time::Instant::now() >= deadline {
                    warn!(
                        fireblocks_tx_id = %tx_id,
                        status = ?response.status,
                        "Fireblocks transaction polling timed out before confirmation"
                    );

                    return Err(FireblocksError::TransactionStillPending {
                        tx_id: tx_id.to_string(),
                        status: response.status,
                    });
                }

                tokio::time::sleep(interval).await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use alloy::network::EthereumWallet;
    use alloy::network::TransactionBuilder;
    use alloy::node_bindings::Anvil;
    use alloy::primitives::address;
    use alloy::providers::ProviderBuilder;
    use alloy::rpc::types::TransactionRequest;
    use alloy::signers::local::PrivateKeySigner;
    use fireblocks_sdk::ClientBuilder;
    use httpmock::Method::GET;
    use httpmock::Method::POST;
    use httpmock::MockServer;
    use serde_json::json;

    use super::*;

    const TEST_FIREBLOCKS_RSA_KEY: &str = "\
-----BEGIN RSA PRIVATE KEY-----\n\
MIIEpAIBAAKCAQEA0A5Ruq8fIPvGNCOkGkCW6fTDQJHyOaWvEbPZ4h0vCsRpU85p\n\
m+T38xAGeLkloyETZbOZursuaBQNJH0Krpynhz06B+LkDhpKk0hl/NLIVmaW9/cn\n\
Wdo8kafVwBiqe5WCxGYt7DJBbAthwDe7mRVtWQpzzYi2nqC0e0IfTW6DYGAywcEY\n\
bj4S6x5GCTNACwU4HDrj4++xst3jwsAwwRXOcN+1hoGvpJ+NixdKf3rBCPzyWGqE\n\
sHQGhp1MIFOCIoWHCE3iQWgCtBr+hY+GTCiCemd+T+fuZF9Ppnk1QY9ajIJFzhtX\n\
VjbGMKpS+W10ohhg/CYw9L/nW7fRht6dOJwGrQIDAQABAoIBAGMjL5aA85hH4D3f\n\
7cYEmOSKGK+f24fUhwMsa9nuhgffZ0DjzjbWd79+F1dXAbQlgpSna80qfAZj549H\n\
dduWEc3DQu+XEYwWwwpDKTW7SnLBF5a4UivibZIKIzQRjMquh00GU0OE/t943O82\n\
n1FmgGA5NsztI/eaI+XHsBq5WiiFUixvwPGc9Amim5IpflT7ZI70jMpq7v8OngLI\n\
VSKJZxrrR1Ey370Bvuls4XCJQhRCU2W0ei63J12F5G7Ha2BHiA5V+6z8iwzU3Rcx\n\
1wGAs1FkVwRY32EJ/e5KipE/8BpsxHpddKHjqFe3HdNJX7p4fxhb5RwD3NofR7Bx\n\
0JlWEAECgYEA69OBDCz2uPQMfjHNKa/FYBaPk+T8Ng1z/ssDRyLm+laD8ZEm3qus\n\
h9h+uDPzsg/UC2ExfWAoy1v466eFyv4z9Jg1Xy1cIoC7ZpVJ3g7V/NieMcUyze3K\n\
neFY2cZP1+1537C1tv0pZ6o4a1dx6tTlf6tw8YBEImprOYDmPJSY3a0CgYEA4dqo\n\
eeWte2xwmkFnVE+xWpwbCKaloBfwjwLE8nKuX1TOMCHd+ROx1ZEu/lR9fq4ZAFiD\n\
03Hcablv3L6d9DOK677X3UMh8aJfN5CpfO2LRtHkxkocKJzN0y6LvbmpahL5lhhl\n\
mg6t/pVm8KFugwxBENr2p2+wISrpe/08R38y7QECgYEAgHj16aMpOfNCRxFsepRW\n\
S5We3Gw39l533cvNWliqSqENOnHgIhiWYl9QEZvD3DKBSz3Ez0+uibLuNbtKyR6Z\n\
QVwWX2Z/thA5h5Y26rFcZGXCMM0Ec2ljI4WbePBvmOu3pGRJaooan239VIUL+2nG\n\
KTpFylsdQz6EcYxGYaR2bvUCgYAh0/ZYC6aoTd2vvQwU/Lq7r5X8/bjg+bT2Npv5\n\
FGQ5syIO44Ozwtdn/Y7kWRNS3hCwlJFqIiu5SiUPEx3lbbLpDHSYl22GG6PXVruM\n\
EqhBuz5g1MjS7GmHr/kOObi+QolPieft4zT8ZLQ+Zm9/AV7df25iNcBTt3GxOITG\n\
0K54AQKBgQDAOaaQ0cndy9Xn33JmIlp8UbExboRjfFPmjoPy3fOLEiG007fCKb2V\n\
9sZrExj1gfn0ogl5D6eOdMxMO2+HkPnT5mRypGRikdj17MJ767QPWc9EJcKma9AX\n\
7IRi1ur1lSBwzROZVSeGNW9za2iZXxFrAcS3Y66xDCbDBft7KehLuQ==\n\
-----END RSA PRIVATE KEY-----\n";

    #[test]
    fn asset_id_for_chain_returns_matching_entry() {
        let chain_asset_ids =
            HashMap::from([(8453_u64, FireblocksAssetId("BASECHAIN_ETH".into()))]);

        let asset_id = asset_id_for_chain(&chain_asset_ids, 8453).unwrap();

        assert_eq!(asset_id, &FireblocksAssetId("BASECHAIN_ETH".into()));
    }

    #[test]
    fn build_contract_call_request_sets_contract_call_fields() {
        let request = build_contract_call_request(
            &FireblocksAssetId("BASECHAIN_ETH".into()),
            &FireblocksVaultAccountId("0".into()),
            "fireblocks-wallet-id",
            &Bytes::from(vec![0xab, 0xcd, 0xef]),
            "contract-call",
            "ext-123",
        );

        assert_eq!(
            request.operation,
            Some(models::TransactionOperation::ContractCall)
        );
        assert_eq!(request.asset_id.as_deref(), Some("BASECHAIN_ETH"));
        assert_eq!(request.external_tx_id.as_deref(), Some("ext-123"));
        assert_eq!(
            request
                .destination
                .as_ref()
                .map(|destination| destination.r#type),
            Some(models::TransferPeerPathType::ExternalWallet)
        );
        assert_eq!(
            request
                .destination
                .as_ref()
                .and_then(|destination| destination.id.as_deref()),
            Some("fireblocks-wallet-id")
        );
        assert_eq!(
            request
                .extra_parameters
                .as_ref()
                .and_then(|params| params.contract_call_data.as_deref()),
            Some("abcdef")
        );
    }

    #[test]
    fn generate_external_tx_id_contains_note() {
        let transaction_id = generate_external_tx_id("contract-call");

        assert!(transaction_id.contains("contract-call"));
    }

    #[test]
    fn generate_external_tx_id_truncates_long_note() {
        let transaction_id = generate_external_tx_id(&"contract-call".repeat(40));

        assert!(
            transaction_id.len() <= FIREBLOCKS_EXTERNAL_TX_ID_MAX_LEN,
            "expected external tx id to be capped at {} chars, got {}",
            FIREBLOCKS_EXTERNAL_TX_ID_MAX_LEN,
            transaction_id.len()
        );
    }

    #[test]
    fn fireblocks_api_key_debug_redacts_secret() {
        let api_key = FireblocksApiKey("super-secret-api-key".into());

        let debug_output = format!("{api_key:?}");

        assert_eq!(debug_output, "FireblocksApiKey(<redacted>)");
    }

    #[test]
    fn fireblocks_credentials_debug_redacts_api_key() {
        let credentials = FireblocksCredentials {
            api_key: FireblocksApiKey("super-secret-api-key".into()),
            api_private_key_path: "/tmp/fireblocks-key.pem".into(),
        };

        let debug_output = format!("{credentials:?}");

        assert!(
            !debug_output.contains("super-secret-api-key"),
            "debug output leaked the API key: {debug_output}"
        );
        assert!(
            debug_output.contains("FireblocksApiKey(<redacted>)"),
            "debug output should show the redacted placeholder: {debug_output}"
        );
    }

    #[tokio::test]
    async fn fetch_vault_address_returns_first_address() {
        let server = MockServer::start();
        let client = ClientBuilder::new("api-key", TEST_FIREBLOCKS_RSA_KEY.as_bytes())
            .with_url(&server.base_url())
            .build()
            .unwrap();

        server.mock(|when, then| {
            when.method(GET)
                .path("/vault/accounts/0/BASECHAIN_ETH/addresses_paginated");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "addresses": [
                        { "address": "0xbd41F40D91eE4E816Ada1Aa842e94aEb6B6385a6" }
                    ]
                }));
        });

        let address = fetch_vault_address(
            &client,
            &FireblocksVaultAccountId("0".into()),
            &FireblocksAssetId("BASECHAIN_ETH".into()),
        )
        .await
        .unwrap();

        assert_eq!(
            address,
            address!("0xbd41F40D91eE4E816Ada1Aa842e94aEb6B6385a6")
        );
    }

    #[tokio::test]
    async fn resolve_contract_wallet_returns_matching_whitelisted_contract() {
        let server = MockServer::start();
        let client = ClientBuilder::new("api-key", TEST_FIREBLOCKS_RSA_KEY.as_bytes())
            .with_url(&server.base_url())
            .build()
            .unwrap();
        let contract = address!("0xbd41F40D91eE4E816Ada1Aa842e94aEb6B6385a6");

        server.mock(|when, then| {
            when.method(GET).path("/contracts");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([{
                    "id": "wallet-id",
                    "name": "contract-wallet",
                    "assets": [{
                        "id": "BASECHAIN_ETH",
                        "address": contract.to_string()
                    }]
                }]));
        });

        let wallet = FireblocksWallet {
            client,
            provider: alloy::providers::ProviderBuilder::new()
                .disable_recommended_fillers()
                .connect_http(alloy::node_bindings::Anvil::new().spawn().endpoint_url()),
            address: Address::ZERO,
            asset_id: FireblocksAssetId("BASECHAIN_ETH".into()),
            vault_account_id: FireblocksVaultAccountId("0".into()),
            required_confirmations: 1,
        };

        let wallet_id = wallet.resolve_contract_wallet(contract).await.unwrap();

        assert_eq!(wallet_id, "wallet-id");
    }

    #[tokio::test]
    async fn send_returns_receipt_when_fireblocks_reports_failed_after_broadcast() {
        let anvil = Anvil::new().spawn();
        let signer: PrivateKeySigner = anvil.keys()[0].clone().into();
        let signing_provider = ProviderBuilder::new()
            .wallet(EthereumWallet::from(signer))
            .connect_http(anvil.endpoint_url());
        let read_only_provider = ProviderBuilder::new()
            .disable_recommended_fillers()
            .connect_http(anvil.endpoint_url());

        let contract = anvil.addresses()[1];
        let onchain_receipt = signing_provider
            .send_transaction(TransactionRequest::default().with_to(contract))
            .await
            .unwrap()
            .get_receipt()
            .await
            .unwrap();
        let tx_hash = onchain_receipt.transaction_hash.to_string();

        let server = MockServer::start();
        let client = ClientBuilder::new("api-key", TEST_FIREBLOCKS_RSA_KEY.as_bytes())
            .with_url(&server.base_url())
            .build()
            .unwrap();

        server.mock(|when, then| {
            when.method(GET).path("/contracts");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([{
                    "id": "wallet-id",
                    "name": "contract-wallet",
                    "assets": [{
                        "id": "BASECHAIN_ETH",
                        "address": contract.to_string()
                    }]
                }]));
        });

        server.mock(|when, then| {
            when.method(POST).path("/transactions");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "id": "tx-123"
                }));
        });

        server.mock(|when, then| {
            when.method(GET).path("/transactions/tx-123");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "id": "tx-123",
                    "status": "FAILED",
                    "txHash": tx_hash
                }));
        });

        let wallet = FireblocksWallet {
            client,
            provider: read_only_provider,
            address: Address::ZERO,
            asset_id: FireblocksAssetId("BASECHAIN_ETH".into()),
            vault_account_id: FireblocksVaultAccountId("0".into()),
            required_confirmations: 1,
        };

        let receipt = wallet
            .send(contract, Bytes::new(), "fireblocks failed tx")
            .await
            .unwrap();

        assert_eq!(receipt.transaction_hash, onchain_receipt.transaction_hash);
    }
}

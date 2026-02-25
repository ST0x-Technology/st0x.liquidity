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
use fireblocks_sdk::apis::whitelisted_contracts_api::GetContractsError;
use fireblocks_sdk::models::{self, TransactionOperation, TransactionStatus};
use fireblocks_sdk::{Client, ClientBuilder};
use serde::Deserialize;
use std::collections::BTreeMap;
use std::time::Duration;
use tracing::{debug, info, warn};
use url::Url;

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

/// Construction context for `FireblocksWallet`.
///
/// Contains everything needed to construct a wallet: Fireblocks API
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
    /// Override the Fireblocks API base URL. When `None`, the URL is
    /// determined by [`FireblocksEnvironment`] (production or sandbox).
    pub base_url: Option<Url>,
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

        if let Some(ref url) = self.base_url {
            builder = builder.with_url(url.as_str().trim_end_matches('/'));
        } else if self.environment == FireblocksEnvironment::Sandbox {
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
    #[error("Fireblocks get contracts API error: {0:?}")]
    GetContracts(#[from] fireblocks_sdk::apis::Error<GetContractsError>),
    #[error(
        "contract {contract} is not whitelisted in Fireblocks \
         contract wallets"
    )]
    ContractNotWhitelisted { contract: Address },
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

impl<P> std::fmt::Debug for FireblocksWallet<P> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("FireblocksWallet")
            .field("vault_account_id", &self.vault_account_id)
            .field("asset_id", &self.asset_id)
            .field("address", &self.address)
            .finish_non_exhaustive()
    }
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

impl<P> FireblocksWallet<P>
where
    P: Provider + Clone + Send + Sync + 'static,
{
    /// Queries the Fireblocks `GET /contracts` API to find the
    /// whitelisted contract wallet matching the given on-chain address.
    /// Returns the Fireblocks wallet ID.
    async fn resolve_contract_wallet(&self, contract: Address) -> Result<String, FireblocksError> {
        let needle = contract.to_string().to_lowercase();

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
                        asset
                            .address
                            .as_ref()
                            .is_some_and(|addr| addr.to_lowercase() == needle)
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

        let tx_request = build_contract_call_request(
            self.asset_id.as_str(),
            self.vault_account_id.as_str(),
            &wallet_id,
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

        let result =
            Client::poll_transaction(&self.client, &tx_id, POLL_TIMEOUT, POLL_INTERVAL, |tx| {
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
    wallet_id: &str,
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
            r#type: models::TransferPeerPathType::ExternalWallet,
            id: Some(wallet_id.to_string()),
            one_time_address: None,
            sub_type: None,
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

const EXTERNAL_TX_ID_MAX_LEN: usize = 255;
const ELLIPSIS: &str = "...";

fn generate_external_tx_id(note: &str) -> String {
    let timestamp = chrono::Utc::now().format("%Y%m%dT%H%M%SZ").to_string();
    let uuid = uuid::Uuid::new_v4().to_string();
    let separator_count = 2;
    let available_for_note =
        EXTERNAL_TX_ID_MAX_LEN - timestamp.len() - uuid.len() - separator_count;

    let truncated_note = if note.len() <= available_for_note {
        note.to_string()
    } else {
        let truncation_point = available_for_note - ELLIPSIS.len();
        format!("{}{ELLIPSIS}", &note[..truncation_point])
    };

    format!("{timestamp}-{truncated_note}-{uuid}")
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
    use alloy::network::EthereumWallet;
    use alloy::node_bindings::{Anvil, AnvilInstance};
    use alloy::providers::ProviderBuilder;
    use alloy::rpc::types::TransactionRequest;
    use alloy::signers::local::PrivateKeySigner;
    use httpmock::MockServer;
    use rsa::RsaPrivateKey;
    use rsa::pkcs8::EncodePrivateKey;
    use std::sync::LazyLock;

    use super::*;

    /// RSA private key generated at test time for Fireblocks JWT
    /// signing. The mock server does not validate signatures.
    static TEST_RSA_PEM: LazyLock<Vec<u8>> = LazyLock::new(|| {
        let mut rng = rand::thread_rng();
        let key = RsaPrivateKey::new(&mut rng, 2048).unwrap();
        let pem = key.to_pkcs8_pem(rsa::pkcs8::LineEnding::LF).unwrap();
        pem.as_bytes().to_vec()
    });

    fn anvil_signer(anvil: &AnvilInstance) -> EthereumWallet {
        let signer: PrivateKeySigner = anvil.keys()[0].clone().into();
        EthereumWallet::from(signer)
    }

    /// Build a Fireblocks [`Client`] that sends requests to the mock
    /// server instead of the real Fireblocks API.
    fn mock_client(server: &MockServer) -> Client {
        ClientBuilder::new("test-api-key", &TEST_RSA_PEM)
            .with_url(&server.base_url())
            .build()
            .unwrap()
    }

    const TEST_CONTRACT_WALLET_ID: &str = "test-contract-wallet-id";

    fn mock_whitelisted_contracts(server: &MockServer, contract: Address) -> httpmock::Mock<'_> {
        server.mock(|when, then| {
            when.method("GET").path("/contracts");
            then.status(200)
                .header("Content-Type", "application/json")
                .json_body(serde_json::json!([{
                    "id": TEST_CONTRACT_WALLET_ID,
                    "name": "Test Contract",
                    "assets": [{
                        "id": "ETH",
                        "address": contract.to_string()
                    }]
                }]));
        })
    }

    fn test_ctx<P>(server: &MockServer, provider: P) -> FireblocksCtx<P> {
        FireblocksCtx {
            api_user_id: FireblocksApiUserId::new("test-api-key"),
            secret: TEST_RSA_PEM.to_vec(),
            vault_account_id: FireblocksVaultAccountId::new("0"),
            environment: FireblocksEnvironment::Sandbox,
            asset_id: AssetId("ETH".to_string()),
            provider,
            required_confirmations: 1,
            base_url: Some(server.base_url().parse().unwrap()),
        }
    }

    /// Build a wallet using a pre-built mock client, bypassing
    /// `FireblocksWallet::new` (which calls `build_client` internally).
    /// This lets tests control the mock server URL.
    fn build_wallet<P>(client: Client, address: Address, provider: P) -> FireblocksWallet<P> {
        FireblocksWallet {
            client,
            vault_account_id: FireblocksVaultAccountId::new("0"),
            asset_id: AssetId("ETH".to_string()),
            provider,
            address,
            required_confirmations: 1,
        }
    }

    // -- FireblocksWallet::new tests --

    #[tokio::test]
    async fn new_resolves_address_from_vault() {
        let server = MockServer::start();
        let anvil = Anvil::new().spawn();
        let provider = ProviderBuilder::new()
            .disable_recommended_fillers()
            .connect_http(anvil.endpoint_url());

        server.mock(|when, then| {
            when.method("GET")
                .path("/vault/accounts/0/ETH/addresses_paginated");
            then.status(200)
                .header("Content-Type", "application/json")
                .json_body(serde_json::json!({
                    "addresses": [{
                        "assetId": "ETH",
                        "address": "0x1111111111111111111111111111111111111111"
                    }]
                }));
        });

        let ctx = test_ctx(&server, provider);

        let wallet = FireblocksWallet::new(ctx).await.unwrap();

        assert_eq!(
            wallet.address(),
            "0x1111111111111111111111111111111111111111"
                .parse::<Address>()
                .unwrap()
        );
    }

    #[tokio::test]
    async fn new_returns_no_deposit_address_when_empty() {
        let server = MockServer::start();
        let anvil = Anvil::new().spawn();
        let provider = ProviderBuilder::new()
            .disable_recommended_fillers()
            .connect_http(anvil.endpoint_url());

        server.mock(|when, then| {
            when.method("GET")
                .path("/vault/accounts/0/ETH/addresses_paginated");
            then.status(200)
                .header("Content-Type", "application/json")
                .json_body(serde_json::json!({
                    "addresses": []
                }));
        });

        let ctx = test_ctx(&server, provider);

        let error = FireblocksWallet::new(ctx).await.unwrap_err();

        assert!(
            matches!(error, FireblocksError::NoDepositAddress { .. }),
            "expected NoDepositAddress, got: {error:?}"
        );
    }

    // -- FireblocksWallet::send tests --

    #[tokio::test]
    async fn send_returns_missing_transaction_id_when_none() {
        let server = MockServer::start();
        let anvil = Anvil::new().spawn();
        let provider = ProviderBuilder::new()
            .disable_recommended_fillers()
            .connect_http(anvil.endpoint_url());

        let contracts_mock = mock_whitelisted_contracts(&server, Address::ZERO);

        let create_mock = server.mock(|when, then| {
            when.method("POST").path("/transactions");
            then.status(200)
                .header("Content-Type", "application/json")
                .json_body(serde_json::json!({}));
        });

        let client = mock_client(&server);
        let wallet = build_wallet(client, Address::ZERO, provider);

        let error = wallet
            .send(Address::ZERO, Bytes::new(), "test")
            .await
            .unwrap_err();

        contracts_mock.assert();
        create_mock.assert();
        assert!(
            matches!(
                error,
                EvmError::Fireblocks(FireblocksError::MissingTransactionId)
            ),
            "expected MissingTransactionId, got: {error:?}"
        );
    }

    #[tokio::test]
    async fn send_returns_transaction_failed_on_non_completed_status() {
        let server = MockServer::start();
        let anvil = Anvil::new().spawn();
        let provider = ProviderBuilder::new()
            .disable_recommended_fillers()
            .connect_http(anvil.endpoint_url());

        let contracts_mock = mock_whitelisted_contracts(&server, Address::ZERO);

        let create_mock = server.mock(|when, then| {
            when.method("POST").path("/transactions");
            then.status(200)
                .header("Content-Type", "application/json")
                .json_body(serde_json::json!({
                    "id": "tx-123",
                    "status": "SUBMITTED"
                }));
        });

        let poll_mock = server.mock(|when, then| {
            when.method("GET").path("/transactions/tx-123");
            then.status(200)
                .header("Content-Type", "application/json")
                .json_body(serde_json::json!({
                    "id": "tx-123",
                    "status": "FAILED",
                    "assetId": "ETH"
                }));
        });

        let client = mock_client(&server);
        let wallet = build_wallet(client, Address::ZERO, provider);

        let error = wallet
            .send(Address::ZERO, Bytes::new(), "test")
            .await
            .unwrap_err();

        contracts_mock.assert();
        create_mock.assert();
        assert!(
            poll_mock.hits() >= 1,
            "poll endpoint should be hit at least once"
        );
        assert!(
            matches!(
                error,
                EvmError::Fireblocks(FireblocksError::TransactionFailed {
                    ref tx_id,
                    status: TransactionStatus::Failed,
                }) if tx_id == "tx-123"
            ),
            "expected TransactionFailed with Failed status, got: {error:?}"
        );
    }

    #[tokio::test]
    async fn send_returns_missing_tx_hash_when_completed_without_hash() {
        let server = MockServer::start();
        let anvil = Anvil::new().spawn();
        let provider = ProviderBuilder::new()
            .disable_recommended_fillers()
            .connect_http(anvil.endpoint_url());

        let contracts_mock = mock_whitelisted_contracts(&server, Address::ZERO);

        let create_mock = server.mock(|when, then| {
            when.method("POST").path("/transactions");
            then.status(200)
                .header("Content-Type", "application/json")
                .json_body(serde_json::json!({
                    "id": "tx-456",
                    "status": "SUBMITTED"
                }));
        });

        let poll_mock = server.mock(|when, then| {
            when.method("GET").path("/transactions/tx-456");
            then.status(200)
                .header("Content-Type", "application/json")
                .json_body(serde_json::json!({
                    "id": "tx-456",
                    "status": "COMPLETED",
                    "assetId": "ETH"
                }));
        });

        let client = mock_client(&server);
        let wallet = build_wallet(client, Address::ZERO, provider);

        let error = wallet
            .send(Address::ZERO, Bytes::new(), "test")
            .await
            .unwrap_err();

        contracts_mock.assert();
        create_mock.assert();
        assert!(
            poll_mock.hits() >= 1,
            "poll endpoint should be hit at least once"
        );
        assert!(
            matches!(
                error,
                EvmError::Fireblocks(FireblocksError::MissingTxHash { ref tx_id })
                    if tx_id == "tx-456"
            ),
            "expected MissingTxHash, got: {error:?}"
        );
    }

    #[tokio::test]
    async fn send_happy_path_returns_receipt() {
        let server = MockServer::start();
        let anvil = Anvil::new().spawn();
        let url = anvil.endpoint_url();
        let signer = anvil_signer(&anvil);
        let signer_address = signer.clone().default_signer().address();

        let signing_provider = ProviderBuilder::new()
            .wallet(signer)
            .connect_http(url.clone());

        let tx = TransactionRequest::default()
            .to(signer_address)
            .value(alloy::primitives::U256::ZERO);

        let pending = signing_provider.send_transaction(tx).await.unwrap();
        let receipt = pending.get_receipt().await.unwrap();
        let tx_hash = receipt.transaction_hash;

        let contracts_mock = mock_whitelisted_contracts(&server, Address::ZERO);

        let create_mock = server.mock(|when, then| {
            when.method("POST").path("/transactions");
            then.status(200)
                .header("Content-Type", "application/json")
                .json_body(serde_json::json!({
                    "id": "tx-789",
                    "status": "SUBMITTED"
                }));
        });

        let poll_mock = server.mock(|when, then| {
            when.method("GET").path("/transactions/tx-789");
            then.status(200)
                .header("Content-Type", "application/json")
                .json_body(serde_json::json!({
                    "id": "tx-789",
                    "status": "COMPLETED",
                    "txHash": format!("{tx_hash}"),
                    "assetId": "ETH"
                }));
        });

        let bare_provider = ProviderBuilder::new()
            .disable_recommended_fillers()
            .connect_http(url);

        let client = mock_client(&server);
        let wallet = build_wallet(client, Address::ZERO, bare_provider);

        let result = wallet
            .send(Address::ZERO, Bytes::new(), "test")
            .await
            .unwrap();

        contracts_mock.assert();
        create_mock.assert();
        assert!(
            poll_mock.hits() >= 1,
            "poll endpoint should be hit at least once"
        );
        assert_eq!(result.transaction_hash, tx_hash);
    }

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
    fn generate_external_tx_id_truncates_long_note() {
        let long_note = "a".repeat(300);
        let tx_id = generate_external_tx_id(&long_note);

        assert!(
            tx_id.len() <= EXTERNAL_TX_ID_MAX_LEN,
            "Expected tx_id length <= {EXTERNAL_TX_ID_MAX_LEN}, got: {}",
            tx_id.len()
        );
        assert_eq!(tx_id.len(), EXTERNAL_TX_ID_MAX_LEN);
        assert!(
            tx_id.contains(ELLIPSIS),
            "Expected ellipsis in truncated tx_id, got: {tx_id}"
        );
    }

    #[test]
    fn generate_external_tx_id_does_not_truncate_short_note() {
        let short_note = "ERC20 approve";
        let tx_id = generate_external_tx_id(short_note);

        assert!(
            tx_id.contains(short_note),
            "Expected full note in tx_id, got: {tx_id}"
        );
        assert!(!tx_id.contains(ELLIPSIS));
    }

    #[test]
    fn build_contract_call_request_sets_operation() {
        let request = build_contract_call_request(
            "ETH",
            "0",
            TEST_CONTRACT_WALLET_ID,
            &Bytes::from(vec![0x12, 0x34]),
            "test",
            "ext-123",
        );

        assert_eq!(request.operation, Some(TransactionOperation::ContractCall));
    }

    #[test]
    fn build_contract_call_request_uses_whitelisted_wallet() {
        let request = build_contract_call_request(
            "ETH",
            "0",
            TEST_CONTRACT_WALLET_ID,
            &Bytes::new(),
            "test",
            "ext-123",
        );

        let destination = request.destination.unwrap();
        assert_eq!(
            destination.r#type,
            models::TransferPeerPathType::ExternalWallet
        );
        assert_eq!(destination.id.as_deref(), Some(TEST_CONTRACT_WALLET_ID));
        assert!(
            destination.one_time_address.is_none(),
            "whitelisted contract should not use one_time_address"
        );
    }

    #[test]
    fn build_contract_call_request_sets_calldata_without_0x() {
        let calldata = Bytes::from(vec![0xab, 0xcd, 0xef]);
        let request = build_contract_call_request(
            "ETH",
            "0",
            TEST_CONTRACT_WALLET_ID,
            &calldata,
            "test",
            "ext-123",
        );

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
            TEST_CONTRACT_WALLET_ID,
            &Bytes::new(),
            "test",
            "ext-123",
        );

        assert!(matches!(
            request.amount,
            Some(models::TransactionRequestAmount::String(ref amount)) if amount == "0"
        ));
    }

    #[tokio::test]
    async fn send_rejects_non_whitelisted_contract() {
        let server = MockServer::start();
        let anvil = Anvil::new().spawn();
        let provider = ProviderBuilder::new()
            .disable_recommended_fillers()
            .connect_http(anvil.endpoint_url());

        let contracts_mock = server.mock(|when, then| {
            when.method("GET").path("/contracts");
            then.status(200)
                .header("Content-Type", "application/json")
                .json_body(serde_json::json!([]));
        });

        let client = mock_client(&server);
        let wallet = build_wallet(client, Address::ZERO, provider);

        let non_whitelisted = Address::random();
        let error = wallet
            .send(non_whitelisted, Bytes::new(), "test")
            .await
            .unwrap_err();

        contracts_mock.assert();
        assert!(
            matches!(
                error,
                EvmError::Fireblocks(FireblocksError::ContractNotWhitelisted {
                    contract,
                }) if contract == non_whitelisted
            ),
            "expected ContractNotWhitelisted, got: {error:?}"
        );
    }

    #[tokio::test]
    async fn resolve_contract_wallet_finds_whitelisted_contract() {
        let server = MockServer::start();
        let anvil = Anvil::new().spawn();
        let provider = ProviderBuilder::new()
            .disable_recommended_fillers()
            .connect_http(anvil.endpoint_url());

        let contract = Address::random();
        let contracts_mock = mock_whitelisted_contracts(&server, contract);

        let client = mock_client(&server);
        let wallet = build_wallet(client, contract, provider);

        let resolved = wallet.resolve_contract_wallet(contract).await.unwrap();

        contracts_mock.assert();
        assert_eq!(resolved, TEST_CONTRACT_WALLET_ID);
    }

    #[tokio::test]
    async fn resolve_contract_wallet_returns_not_whitelisted_for_unknown() {
        let server = MockServer::start();
        let anvil = Anvil::new().spawn();
        let provider = ProviderBuilder::new()
            .disable_recommended_fillers()
            .connect_http(anvil.endpoint_url());

        let contracts_mock = mock_whitelisted_contracts(&server, Address::ZERO);

        let client = mock_client(&server);
        let wallet = build_wallet(client, Address::ZERO, provider);

        let unknown = Address::random();
        let error = wallet.resolve_contract_wallet(unknown).await.unwrap_err();

        contracts_mock.assert();
        assert!(
            matches!(error, FireblocksError::ContractNotWhitelisted { contract } if contract == unknown),
            "expected ContractNotWhitelisted, got: {error:?}"
        );
    }

    #[test]
    fn is_still_pending_matches_expected_statuses() {
        use TransactionStatus::*;

        let pending = [
            Submitted,
            PendingAmlScreening,
            PendingEnrichment,
            PendingAuthorization,
            Queued,
            PendingSignature,
            Pending3RdPartyManualApproval,
            Pending3RdParty,
            Broadcasting,
            Confirming,
        ];

        let terminal = [Completed, Cancelling, Cancelled, Blocked, Rejected, Failed];

        for status in pending {
            assert!(is_still_pending(status), "{status:?} should be pending");
        }

        for status in terminal {
            assert!(
                !is_still_pending(status),
                "{status:?} should not be pending"
            );
        }
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

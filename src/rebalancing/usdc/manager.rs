//! UsdcRebalanceManager orchestrates the USDC rebalancing workflow.
//!
//! Coordinates between `AlpacaWalletService`, `CctpBridge`, `VaultService`, and the
//! `UsdcRebalance` aggregate to execute USDC transfers between Alpaca and Base.

use alloy::primitives::{Address, TxHash, U256};
use alloy::providers::Provider;
use alloy::signers::Signer;
use async_trait::async_trait;
use cqrs_es::{CqrsFramework, EventStore};
use std::sync::Arc;
use tracing::{info, instrument, warn};

use super::{UsdcRebalance as UsdcRebalanceTrait, UsdcRebalanceManagerError};
use crate::alpaca_wallet::{
    AlpacaTransferId, AlpacaWalletService, TokenSymbol, Transfer, TransferStatus,
};
use crate::cctp::{AttestationResponse, BridgeDirection, BurnReceipt, CctpBridge};
use crate::lifecycle::{Lifecycle, Never};
use crate::onchain::vault::{VaultId, VaultService};
use crate::threshold::Usdc;
use crate::usdc_rebalance::{
    RebalanceDirection, TransferRef, UsdcRebalance, UsdcRebalanceCommand, UsdcRebalanceId,
};

/// Orchestrates USDC rebalancing between Alpaca (Ethereum) and Rain (Base).
///
/// # Type Parameters
///
/// * `BP` - Base provider type
/// * `S` - Signer type
/// * `ES` - Event store type for the USDC rebalance aggregate
pub(crate) struct UsdcRebalanceManager<BP, S, ES>
where
    BP: Provider + Clone,
    S: Signer + Clone + Sync,
    ES: EventStore<Lifecycle<UsdcRebalance, Never>>,
{
    alpaca_wallet: Arc<AlpacaWalletService>,
    cctp_bridge: Arc<CctpBridge<EthereumHttpProvider, BP, S>>,
    vault: Arc<VaultService<BP, S>>,
    cqrs: Arc<CqrsFramework<Lifecycle<UsdcRebalance, Never>, ES>>,
    /// Market maker's (our) wallet address
    /// Used for Alpaca withdrawals, CCTP bridging, and vault deposits.
    market_maker_wallet: Address,
    /// Vault ID for Rain OrderBook deposits
    vault_id: VaultId,
}

use alloy::network::{Ethereum, EthereumWallet};
use alloy::providers::fillers::{
    BlobGasFiller, ChainIdFiller, FillProvider, GasFiller, JoinFill, NonceFiller, WalletFiller,
};
use alloy::providers::{Identity, RootProvider};

/// Provider type for Ethereum HTTP connections (used for CCTP Ethereum side).
type EthereumHttpProvider = FillProvider<
    JoinFill<
        JoinFill<
            Identity,
            JoinFill<GasFiller, JoinFill<BlobGasFiller, JoinFill<NonceFiller, ChainIdFiller>>>,
        >,
        WalletFiller<EthereumWallet>,
    >,
    RootProvider<Ethereum>,
    Ethereum,
>;

impl<BP, S, ES> UsdcRebalanceManager<BP, S, ES>
where
    BP: Provider + Clone + Send + Sync + 'static,
    S: Signer + Clone + Send + Sync + 'static,
    ES: EventStore<Lifecycle<UsdcRebalance, Never>>,
{
    pub(crate) fn new(
        alpaca_wallet: Arc<AlpacaWalletService>,
        cctp_bridge: Arc<CctpBridge<EthereumHttpProvider, BP, S>>,
        vault: Arc<VaultService<BP, S>>,
        cqrs: Arc<CqrsFramework<Lifecycle<UsdcRebalance, Never>, ES>>,
        market_maker_wallet: Address,
        vault_id: VaultId,
    ) -> Self {
        Self {
            alpaca_wallet,
            cctp_bridge,
            vault,
            cqrs,
            market_maker_wallet,
            vault_id,
        }
    }

    /// Executes the full Alpaca to Base rebalancing workflow.
    ///
    /// # Workflow
    ///
    /// 1. Initiate Alpaca withdrawal -> `Initiate` command
    /// 2. Poll Alpaca until complete -> `ConfirmWithdrawal` command
    /// 3. Execute CCTP burn on Ethereum -> `InitiateBridging` command
    /// 4. Poll Circle API for attestation -> `ReceiveAttestation` command
    /// 5. Execute CCTP mint on Base -> `ConfirmBridging` command
    /// 6. Deposit to Rain vault -> `InitiateDeposit` command
    /// 7. Confirm deposit -> `ConfirmDeposit` command
    ///
    /// On errors, sends appropriate `Fail*` command to transition aggregate to failed state.
    #[instrument(skip(self), fields(?id, ?amount))]
    pub(crate) async fn execute_alpaca_to_base(
        &self,
        id: &UsdcRebalanceId,
        amount: Usdc,
    ) -> Result<(), UsdcRebalanceManagerError> {
        info!(?amount, "Starting Alpaca to Base rebalance");

        let transfer = self.initiate_alpaca_withdrawal(id, amount).await?;

        self.poll_and_confirm_withdrawal(id, &transfer.id).await?;

        let burn_amount = usdc_to_u256(amount)?;
        let burn_receipt = self.execute_cctp_burn(id, burn_amount).await?;

        let attestation_response = self.poll_attestation(id, &burn_receipt).await?;

        self.execute_cctp_mint(id, attestation_response).await?;

        self.deposit_to_vault(id, burn_receipt.amount).await?;

        self.confirm_deposit(id).await?;

        info!("Alpaca to Base rebalance completed successfully");
        Ok(())
    }

    #[instrument(skip(self), fields(?id, ?amount))]
    async fn initiate_alpaca_withdrawal(
        &self,
        id: &UsdcRebalanceId,
        amount: Usdc,
    ) -> Result<Transfer, UsdcRebalanceManagerError> {
        let usdc = TokenSymbol::new("USDC");
        let decimal_amount = amount.0;

        let transfer = match self
            .alpaca_wallet
            .initiate_withdrawal(decimal_amount, &usdc, &self.market_maker_wallet)
            .await
        {
            Ok(t) => t,
            Err(e) => {
                warn!("Alpaca withdrawal initiation failed: {e}");
                return Err(UsdcRebalanceManagerError::AlpacaWallet(e));
            }
        };

        self.cqrs
            .execute(
                &id.0,
                UsdcRebalanceCommand::Initiate {
                    direction: RebalanceDirection::AlpacaToBase,
                    amount,
                    withdrawal: TransferRef::AlpacaId(transfer.id),
                },
            )
            .await?;

        info!(transfer_id = %transfer.id, "Alpaca withdrawal initiated");
        Ok(transfer)
    }

    #[instrument(skip(self), fields(?id, %transfer_id))]
    async fn poll_and_confirm_withdrawal(
        &self,
        id: &UsdcRebalanceId,
        transfer_id: &AlpacaTransferId,
    ) -> Result<(), UsdcRebalanceManagerError> {
        let transfer = match self
            .alpaca_wallet
            .poll_transfer_until_complete(transfer_id)
            .await
        {
            Ok(t) => t,
            Err(e) => {
                warn!("Alpaca withdrawal polling failed: {e}");
                self.cqrs
                    .execute(
                        &id.0,
                        UsdcRebalanceCommand::FailWithdrawal {
                            reason: format!("Polling failed: {e}"),
                        },
                    )
                    .await?;
                return Err(UsdcRebalanceManagerError::AlpacaWallet(e));
            }
        };

        if transfer.status != TransferStatus::Complete {
            let status = format!("{:?}", transfer.status);
            self.cqrs
                .execute(
                    &id.0,
                    UsdcRebalanceCommand::FailWithdrawal {
                        reason: format!("Transfer ended in status: {status}"),
                    },
                )
                .await?;
            return Err(UsdcRebalanceManagerError::WithdrawalFailed { status });
        }

        self.cqrs
            .execute(&id.0, UsdcRebalanceCommand::ConfirmWithdrawal)
            .await?;

        info!("Alpaca withdrawal confirmed");
        Ok(())
    }

    #[instrument(skip(self), fields(?id, %amount))]
    async fn execute_cctp_burn(
        &self,
        id: &UsdcRebalanceId,
        amount: U256,
    ) -> Result<BurnReceipt, UsdcRebalanceManagerError> {
        let burn_receipt = match self
            .cctp_bridge
            .burn(
                BridgeDirection::EthereumToBase,
                amount,
                self.market_maker_wallet,
            )
            .await
        {
            Ok(r) => r,
            Err(e) => {
                warn!("CCTP burn failed: {e}");
                self.cqrs
                    .execute(
                        &id.0,
                        UsdcRebalanceCommand::FailBridging {
                            reason: format!("Burn failed: {e}"),
                        },
                    )
                    .await?;
                return Err(UsdcRebalanceManagerError::Cctp(e));
            }
        };

        let nonce_bytes = burn_receipt.nonce.as_slice();
        let cctp_nonce = u64::from_be_bytes(nonce_bytes[24..32].try_into().unwrap_or([0u8; 8]));

        self.cqrs
            .execute(
                &id.0,
                UsdcRebalanceCommand::InitiateBridging {
                    burn_tx: burn_receipt.tx,
                    cctp_nonce,
                },
            )
            .await?;

        info!(burn_tx = %burn_receipt.tx, "CCTP burn executed");
        Ok(burn_receipt)
    }

    #[instrument(skip(self, burn_receipt), fields(?id, burn_tx = %burn_receipt.tx))]
    async fn poll_attestation(
        &self,
        id: &UsdcRebalanceId,
        burn_receipt: &BurnReceipt,
    ) -> Result<AttestationResponse, UsdcRebalanceManagerError> {
        let response = match self
            .cctp_bridge
            .poll_attestation(BridgeDirection::EthereumToBase, burn_receipt.tx)
            .await
        {
            Ok(r) => r,
            Err(e) => {
                warn!("Attestation polling failed: {e}");
                self.cqrs
                    .execute(
                        &id.0,
                        UsdcRebalanceCommand::FailBridging {
                            reason: format!("Attestation polling failed: {e}"),
                        },
                    )
                    .await?;
                return Err(UsdcRebalanceManagerError::Cctp(e));
            }
        };

        self.cqrs
            .execute(
                &id.0,
                UsdcRebalanceCommand::ReceiveAttestation {
                    attestation: response.attestation.to_vec(),
                },
            )
            .await?;

        info!("Circle attestation received");
        Ok(response)
    }

    #[instrument(skip(self, attestation_response), fields(?id))]
    async fn execute_cctp_mint(
        &self,
        id: &UsdcRebalanceId,
        attestation_response: AttestationResponse,
    ) -> Result<TxHash, UsdcRebalanceManagerError> {
        let mint_tx = match self
            .cctp_bridge
            .mint(
                BridgeDirection::EthereumToBase,
                attestation_response.message,
                attestation_response.attestation,
            )
            .await
        {
            Ok(tx) => tx,
            Err(e) => {
                warn!("CCTP mint failed: {e}");
                self.cqrs
                    .execute(
                        &id.0,
                        UsdcRebalanceCommand::FailBridging {
                            reason: format!("Mint failed: {e}"),
                        },
                    )
                    .await?;
                return Err(UsdcRebalanceManagerError::Cctp(e));
            }
        };

        self.cqrs
            .execute(&id.0, UsdcRebalanceCommand::ConfirmBridging { mint_tx })
            .await?;

        info!(%mint_tx, "CCTP mint executed");
        Ok(mint_tx)
    }

    #[instrument(skip(self), fields(?id, ?amount))]
    async fn deposit_to_vault(
        &self,
        id: &UsdcRebalanceId,
        amount: U256,
    ) -> Result<(), UsdcRebalanceManagerError> {
        let deposit_tx = match self.vault.deposit_usdc(self.vault_id, amount).await {
            Ok(tx) => tx,
            Err(e) => {
                warn!("Vault deposit failed: {e}");
                self.cqrs
                    .execute(
                        &id.0,
                        UsdcRebalanceCommand::FailDeposit {
                            reason: format!("Vault deposit failed: {e}"),
                        },
                    )
                    .await?;
                return Err(UsdcRebalanceManagerError::Vault(e));
            }
        };

        self.cqrs
            .execute(
                &id.0,
                UsdcRebalanceCommand::InitiateDeposit {
                    deposit: TransferRef::OnchainTx(deposit_tx),
                },
            )
            .await?;

        info!(%deposit_tx, "Vault deposit initiated");
        Ok(())
    }

    #[instrument(skip(self), fields(?id))]
    async fn confirm_deposit(&self, id: &UsdcRebalanceId) -> Result<(), UsdcRebalanceManagerError> {
        self.cqrs
            .execute(&id.0, UsdcRebalanceCommand::ConfirmDeposit)
            .await?;

        info!("Vault deposit confirmed");
        Ok(())
    }

    /// Executes the full Base to Alpaca rebalancing workflow.
    ///
    /// # Workflow
    ///
    /// 1. Withdraw from Rain vault on Base -> `Initiate` command
    /// 2. Confirm vault withdrawal -> `ConfirmWithdrawal` command
    /// 3. Execute CCTP burn on Base -> `InitiateBridging` command
    /// 4. Poll Circle API for attestation -> `ReceiveAttestation` command
    /// 5. Execute CCTP mint on Ethereum -> `ConfirmBridging` command
    /// 6. Initiate Alpaca deposit (mint directly to Alpaca address) -> `InitiateDeposit` command
    /// 7. Poll Alpaca until deposit credited -> `ConfirmDeposit` command
    ///
    /// On errors, sends appropriate `Fail*` command to transition aggregate to failed state.
    #[instrument(skip(self), fields(?id, ?amount))]
    pub(crate) async fn execute_base_to_alpaca(
        &self,
        id: &UsdcRebalanceId,
        amount: Usdc,
    ) -> Result<(), UsdcRebalanceManagerError> {
        info!(?amount, "Starting Base to Alpaca rebalance");

        let amount_u256 = usdc_to_u256(amount)?;

        self.withdraw_from_vault(id, amount, amount_u256).await?;

        let burn_receipt = self.execute_cctp_burn_on_base(id, amount_u256).await?;

        let attestation_response = self
            .poll_attestation_for_base_burn(id, &burn_receipt)
            .await?;

        let mint_tx = self
            .execute_cctp_mint_on_ethereum(id, attestation_response)
            .await?;

        self.poll_and_confirm_alpaca_deposit(id, mint_tx).await?;

        info!("Base to Alpaca rebalance completed successfully");
        Ok(())
    }

    #[instrument(skip(self), fields(?id, ?amount))]
    async fn withdraw_from_vault(
        &self,
        id: &UsdcRebalanceId,
        amount: Usdc,
        amount_u256: U256,
    ) -> Result<(), UsdcRebalanceManagerError> {
        let withdraw_tx = match self.vault.withdraw_usdc(self.vault_id, amount_u256).await {
            Ok(tx) => tx,
            Err(e) => {
                warn!("Vault withdrawal failed: {e}");
                return Err(UsdcRebalanceManagerError::Vault(e));
            }
        };

        self.cqrs
            .execute(
                &id.0,
                UsdcRebalanceCommand::Initiate {
                    direction: RebalanceDirection::BaseToAlpaca,
                    amount,
                    withdrawal: TransferRef::OnchainTx(withdraw_tx),
                },
            )
            .await?;

        // Vault withdrawal function already waits for block inclusion, so confirming immediately
        self.cqrs
            .execute(&id.0, UsdcRebalanceCommand::ConfirmWithdrawal)
            .await?;

        info!(%withdraw_tx, "Vault withdrawal completed");
        Ok(())
    }

    #[instrument(skip(self), fields(?id, ?amount))]
    async fn execute_cctp_burn_on_base(
        &self,
        id: &UsdcRebalanceId,
        amount: U256,
    ) -> Result<BurnReceipt, UsdcRebalanceManagerError> {
        let burn_receipt = match self
            .cctp_bridge
            .burn(
                BridgeDirection::BaseToEthereum,
                amount,
                self.market_maker_wallet,
            )
            .await
        {
            Ok(r) => r,
            Err(e) => {
                warn!("CCTP burn on Base failed: {e}");
                self.cqrs
                    .execute(
                        &id.0,
                        UsdcRebalanceCommand::FailBridging {
                            reason: format!("Burn on Base failed: {e}"),
                        },
                    )
                    .await?;
                return Err(UsdcRebalanceManagerError::Cctp(e));
            }
        };

        let nonce_bytes = burn_receipt.nonce.as_slice();
        let cctp_nonce = u64::from_be_bytes(nonce_bytes[24..32].try_into().unwrap_or([0u8; 8]));

        self.cqrs
            .execute(
                &id.0,
                UsdcRebalanceCommand::InitiateBridging {
                    burn_tx: burn_receipt.tx,
                    cctp_nonce,
                },
            )
            .await?;

        info!(burn_tx = %burn_receipt.tx, "CCTP burn on Base executed");
        Ok(burn_receipt)
    }

    #[instrument(skip(self, burn_receipt), fields(?id, burn_tx = %burn_receipt.tx))]
    async fn poll_attestation_for_base_burn(
        &self,
        id: &UsdcRebalanceId,
        burn_receipt: &BurnReceipt,
    ) -> Result<AttestationResponse, UsdcRebalanceManagerError> {
        let response = match self
            .cctp_bridge
            .poll_attestation(BridgeDirection::BaseToEthereum, burn_receipt.tx)
            .await
        {
            Ok(r) => r,
            Err(e) => {
                warn!("Attestation polling failed: {e}");
                self.cqrs
                    .execute(
                        &id.0,
                        UsdcRebalanceCommand::FailBridging {
                            reason: format!("Attestation polling failed: {e}"),
                        },
                    )
                    .await?;
                return Err(UsdcRebalanceManagerError::Cctp(e));
            }
        };

        self.cqrs
            .execute(
                &id.0,
                UsdcRebalanceCommand::ReceiveAttestation {
                    attestation: response.attestation.to_vec(),
                },
            )
            .await?;

        info!("Circle attestation received for Base burn");
        Ok(response)
    }

    #[instrument(skip(self, attestation_response), fields(?id))]
    async fn execute_cctp_mint_on_ethereum(
        &self,
        id: &UsdcRebalanceId,
        attestation_response: AttestationResponse,
    ) -> Result<TxHash, UsdcRebalanceManagerError> {
        let mint_tx = match self
            .cctp_bridge
            .mint(
                BridgeDirection::BaseToEthereum,
                attestation_response.message,
                attestation_response.attestation,
            )
            .await
        {
            Ok(tx) => tx,
            Err(e) => {
                warn!("CCTP mint on Ethereum failed: {e}");
                self.cqrs
                    .execute(
                        &id.0,
                        UsdcRebalanceCommand::FailBridging {
                            reason: format!("Mint on Ethereum failed: {e}"),
                        },
                    )
                    .await?;
                return Err(UsdcRebalanceManagerError::Cctp(e));
            }
        };

        self.cqrs
            .execute(&id.0, UsdcRebalanceCommand::ConfirmBridging { mint_tx })
            .await?;

        info!(%mint_tx, "CCTP mint on Ethereum executed");
        Ok(mint_tx)
    }

    #[instrument(skip(self), fields(?id, %mint_tx))]
    async fn poll_and_confirm_alpaca_deposit(
        &self,
        id: &UsdcRebalanceId,
        mint_tx: TxHash,
    ) -> Result<(), UsdcRebalanceManagerError> {
        // Record the deposit initiation with the mint tx
        self.cqrs
            .execute(
                &id.0,
                UsdcRebalanceCommand::InitiateDeposit {
                    deposit: TransferRef::OnchainTx(mint_tx),
                },
            )
            .await?;

        info!(%mint_tx, "Polling Alpaca for deposit detection");

        let transfer = match self.alpaca_wallet.poll_deposit_by_tx_hash(&mint_tx).await {
            Ok(t) => t,
            Err(e) => {
                warn!("Alpaca deposit polling failed: {e}");
                self.cqrs
                    .execute(
                        &id.0,
                        UsdcRebalanceCommand::FailDeposit {
                            reason: format!("Deposit polling failed: {e}"),
                        },
                    )
                    .await?;
                return Err(UsdcRebalanceManagerError::AlpacaWallet(e));
            }
        };

        if transfer.status != TransferStatus::Complete {
            let status = format!("{:?}", transfer.status);
            self.cqrs
                .execute(
                    &id.0,
                    UsdcRebalanceCommand::FailDeposit {
                        reason: format!("Deposit ended in status: {status}"),
                    },
                )
                .await?;
            return Err(UsdcRebalanceManagerError::DepositFailed { status });
        }

        self.cqrs
            .execute(&id.0, UsdcRebalanceCommand::ConfirmDeposit)
            .await?;

        info!("Alpaca deposit confirmed");
        Ok(())
    }
}

/// Converts a USDC decimal amount to U256 with 6 decimals.
///
/// # Errors
///
/// Returns an error if the decimal cannot be represented as U256 (e.g., negative values
/// or values exceeding U256::MAX).
fn usdc_to_u256(usdc: Usdc) -> Result<U256, UsdcRebalanceManagerError> {
    if usdc.0.is_sign_negative() {
        return Err(UsdcRebalanceManagerError::InvalidAmount(format!(
            "USDC amount cannot be negative: {}",
            usdc.0
        )));
    }

    // USDC has 6 decimals
    let scaled = usdc
        .0
        .checked_mul(rust_decimal::Decimal::from(1_000_000u64))
        .ok_or_else(|| {
            UsdcRebalanceManagerError::ArithmeticOverflow(format!(
                "USDC amount overflow during scaling: {}",
                usdc.0
            ))
        })?;

    let integer = scaled.trunc().to_string();

    Ok(U256::from_str_radix(&integer, 10)?)
}

#[async_trait]
impl<BP, S, ES> UsdcRebalanceTrait for UsdcRebalanceManager<BP, S, ES>
where
    BP: Provider + Clone + Send + Sync + 'static,
    S: Signer + Clone + Send + Sync + 'static,
    ES: EventStore<Lifecycle<UsdcRebalance, Never>> + Send + Sync,
    ES::AC: Send,
{
    async fn execute_alpaca_to_base(
        &self,
        id: &UsdcRebalanceId,
        amount: Usdc,
    ) -> Result<(), UsdcRebalanceManagerError> {
        Self::execute_alpaca_to_base(self, id, amount).await
    }

    async fn execute_base_to_alpaca(
        &self,
        id: &UsdcRebalanceId,
        amount: Usdc,
    ) -> Result<(), UsdcRebalanceManagerError> {
        Self::execute_base_to_alpaca(self, id, amount).await
    }
}

#[cfg(test)]
mod tests {
    use alloy::network::{Ethereum, EthereumWallet};
    use alloy::node_bindings::Anvil;
    use alloy::primitives::{B256, address, b256};
    use alloy::providers::fillers::{
        BlobGasFiller, ChainIdFiller, FillProvider, GasFiller, JoinFill, NonceFiller, WalletFiller,
    };
    use alloy::providers::{Identity, ProviderBuilder, RootProvider};
    use alloy::signers::local::PrivateKeySigner;
    use cqrs_es::mem_store::MemStore;
    use cqrs_es::{AggregateError, CqrsFramework};
    use httpmock::prelude::*;
    use reqwest::StatusCode;
    use rust_decimal_macros::dec;
    use serde_json::json;

    use super::*;
    use crate::alpaca_wallet::{AlpacaWalletClient, AlpacaWalletError, create_account_mock};
    use crate::cctp::{CctpBridge, Evm};
    use crate::onchain::vault::VaultService;
    use crate::usdc_rebalance::UsdcRebalanceError;

    const TOKEN_MESSENGER_V2: Address = address!("0x28b5a0e9C621a5BadaA536219b3a228C8168cf5d");
    const MESSAGE_TRANSMITTER_V2: Address = address!("0x81D40F21F12A8F0E3252Bccb954D722d4c464B64");
    const USDC_ADDRESS: Address = address!("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48");
    const ORDERBOOK_ADDRESS: Address = address!("0x1234567890123456789012345678901234567890");
    const TEST_VAULT_ID: VaultId = VaultId(b256!(
        "0x0000000000000000000000000000000000000000000000000000000000000001"
    ));

    type TestProvider = FillProvider<
        JoinFill<
            JoinFill<
                Identity,
                JoinFill<GasFiller, JoinFill<BlobGasFiller, JoinFill<NonceFiller, ChainIdFiller>>>,
            >,
            WalletFiller<EthereumWallet>,
        >,
        RootProvider<Ethereum>,
        Ethereum,
    >;

    type TestCqrs =
        CqrsFramework<Lifecycle<UsdcRebalance, Never>, MemStore<Lifecycle<UsdcRebalance, Never>>>;

    fn create_test_cqrs() -> Arc<TestCqrs> {
        let store = MemStore::default();
        Arc::new(CqrsFramework::new(store, vec![], ()))
    }

    fn setup_anvil() -> (alloy::node_bindings::AnvilInstance, String, B256) {
        let anvil = Anvil::new().spawn();
        let endpoint = anvil.endpoint();
        let private_key = B256::from_slice(&anvil.keys()[0].to_bytes());
        (anvil, endpoint, private_key)
    }

    async fn create_test_wallet_service(server: &MockServer) -> AlpacaWalletService {
        let account_mock = create_account_mock(server, "test-account-id");

        let client = AlpacaWalletClient::new(
            server.base_url(),
            "test_key".to_string(),
            "test_secret".to_string(),
        )
        .await
        .unwrap();

        account_mock.assert();

        AlpacaWalletService::new_with_client(client, None)
    }

    fn create_test_provider(
        endpoint: &str,
        private_key: &B256,
    ) -> (TestProvider, PrivateKeySigner) {
        let signer = PrivateKeySigner::from_bytes(private_key).unwrap();
        let wallet = EthereumWallet::from(signer.clone());

        let provider = ProviderBuilder::new()
            .wallet(wallet)
            .connect_http(endpoint.parse().unwrap());

        (provider, signer)
    }

    fn create_test_onchain_services(
        provider: TestProvider,
        signer: PrivateKeySigner,
    ) -> (
        CctpBridge<TestProvider, TestProvider, PrivateKeySigner>,
        VaultService<TestProvider, PrivateKeySigner>,
    ) {
        let ethereum = Evm::new(
            provider.clone(),
            signer.clone(),
            USDC_ADDRESS,
            TOKEN_MESSENGER_V2,
            MESSAGE_TRANSMITTER_V2,
        );

        let base = Evm::new(
            provider.clone(),
            signer.clone(),
            USDC_ADDRESS,
            TOKEN_MESSENGER_V2,
            MESSAGE_TRANSMITTER_V2,
        );

        let cctp_bridge = CctpBridge::new(ethereum, base);

        let vault_evm = Evm::new(
            provider,
            signer,
            USDC_ADDRESS,
            TOKEN_MESSENGER_V2,
            MESSAGE_TRANSMITTER_V2,
        );

        let vault_service = VaultService::new(vault_evm, ORDERBOOK_ADDRESS);

        (cctp_bridge, vault_service)
    }

    #[test]
    fn test_error_display_withdrawal_failed() {
        let err = UsdcRebalanceManagerError::WithdrawalFailed {
            status: "Cancelled".to_string(),
        };
        assert_eq!(
            err.to_string(),
            "Withdrawal failed with terminal status: Cancelled"
        );
    }

    #[test]
    fn test_error_display_deposit_failed() {
        let err = UsdcRebalanceManagerError::DepositFailed {
            status: "Rejected".to_string(),
        };
        assert_eq!(
            err.to_string(),
            "Deposit failed with terminal status: Rejected"
        );
    }

    #[tokio::test]
    async fn test_execute_alpaca_to_base_withdrawal_not_whitelisted() {
        let server = MockServer::start();
        let (_anvil, endpoint, private_key) = setup_anvil();

        let alpaca_wallet = Arc::new(create_test_wallet_service(&server).await);
        let (provider, signer) = create_test_provider(&endpoint, &private_key);
        let (cctp_bridge, vault_service) = create_test_onchain_services(provider, signer);
        let cqrs = create_test_cqrs();

        let market_maker_wallet = address!("0x1111111111111111111111111111111111111111");

        let manager = UsdcRebalanceManager::new(
            alpaca_wallet,
            Arc::new(cctp_bridge),
            Arc::new(vault_service),
            cqrs,
            market_maker_wallet,
            TEST_VAULT_ID,
        );

        let whitelist_mock = server.mock(|when, then| {
            when.method(GET)
                .path("/v1/accounts/test-account-id/wallets/whitelists");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([]));
        });

        let id = UsdcRebalanceId::new("rebalance-001");
        let amount = Usdc(dec!(1000));

        let result = manager.execute_alpaca_to_base(&id, amount).await;

        assert!(
            matches!(
                result,
                Err(UsdcRebalanceManagerError::AlpacaWallet(
                    AlpacaWalletError::AddressNotWhitelisted { .. }
                ))
            ),
            "Expected AddressNotWhitelisted error, got: {result:?}"
        );
        whitelist_mock.assert();
    }

    #[tokio::test]
    async fn test_execute_alpaca_to_base_withdrawal_pending_whitelist() {
        let server = MockServer::start();
        let (_anvil, endpoint, private_key) = setup_anvil();

        let alpaca_wallet = Arc::new(create_test_wallet_service(&server).await);
        let (provider, signer) = create_test_provider(&endpoint, &private_key);
        let (cctp_bridge, vault_service) = create_test_onchain_services(provider, signer);
        let cqrs = create_test_cqrs();

        let market_maker_wallet = address!("0x1111111111111111111111111111111111111111");

        let manager = UsdcRebalanceManager::new(
            alpaca_wallet,
            Arc::new(cctp_bridge),
            Arc::new(vault_service),
            cqrs,
            market_maker_wallet,
            TEST_VAULT_ID,
        );

        let whitelist_mock = server.mock(|when, then| {
            when.method(GET)
                .path("/v1/accounts/test-account-id/wallets/whitelists");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([{
                    "id": "whitelist-123",
                    "address": "0x1111111111111111111111111111111111111111",
                    "asset": "USDC",
                    "chain": "ethereum",
                    "status": "PENDING",
                    "created_at": "2024-01-01T00:00:00Z"
                }]));
        });

        let id = UsdcRebalanceId::new("rebalance-002");
        let amount = Usdc(dec!(500));

        let result = manager.execute_alpaca_to_base(&id, amount).await;

        assert!(
            matches!(
                result,
                Err(UsdcRebalanceManagerError::AlpacaWallet(
                    AlpacaWalletError::AddressNotWhitelisted { .. }
                ))
            ),
            "Expected AddressNotWhitelisted error for pending whitelist, got: {result:?}"
        );
        whitelist_mock.assert();
    }

    #[tokio::test]
    async fn test_execute_alpaca_to_base_api_error() {
        let server = MockServer::start();
        let (_anvil, endpoint, private_key) = setup_anvil();

        let alpaca_wallet = Arc::new(create_test_wallet_service(&server).await);
        let (provider, signer) = create_test_provider(&endpoint, &private_key);
        let (cctp_bridge, vault_service) = create_test_onchain_services(provider, signer);
        let cqrs = create_test_cqrs();

        let market_maker_wallet = address!("0x1111111111111111111111111111111111111111");

        let manager = UsdcRebalanceManager::new(
            alpaca_wallet,
            Arc::new(cctp_bridge),
            Arc::new(vault_service),
            cqrs,
            market_maker_wallet,
            TEST_VAULT_ID,
        );

        let whitelist_mock = server.mock(|when, then| {
            when.method(GET)
                .path("/v1/accounts/test-account-id/wallets/whitelists");
            then.status(500).body("Internal Server Error");
        });

        let id = UsdcRebalanceId::new("rebalance-003");
        let amount = Usdc(dec!(100));

        let result = manager.execute_alpaca_to_base(&id, amount).await;

        assert!(
            matches!(
                result,
                Err(UsdcRebalanceManagerError::AlpacaWallet(
                    AlpacaWalletError::ApiError {
                        status: StatusCode::INTERNAL_SERVER_ERROR,
                        ..
                    }
                ))
            ),
            "Expected ApiError with INTERNAL_SERVER_ERROR, got: {result:?}"
        );
        whitelist_mock.assert();
    }

    #[test]
    fn test_usdc_to_u256_positive_amount() {
        let amount = Usdc(dec!(1000.50));
        let result = usdc_to_u256(amount).unwrap();
        assert_eq!(result, U256::from(1_000_500_000u64));
    }

    #[test]
    fn test_usdc_to_u256_negative_amount() {
        let amount = Usdc(dec!(-100));
        let result = usdc_to_u256(amount);
        assert!(
            matches!(
                &result,
                Err(UsdcRebalanceManagerError::InvalidAmount(msg)) if msg.contains("-100")
            ),
            "Expected InvalidAmount error mentioning -100, got: {result:?}"
        );
    }

    #[test]
    fn test_usdc_to_u256_zero_amount() {
        let amount = Usdc(dec!(0));
        let result = usdc_to_u256(amount).unwrap();
        assert_eq!(result, U256::ZERO);
    }

    #[test]
    fn test_usdc_to_u256_fractional_truncation() {
        let amount = Usdc(dec!(100.1234567));
        let result = usdc_to_u256(amount).unwrap();
        assert_eq!(result, U256::from(100_123_456u64));
    }

    #[tokio::test]
    async fn test_execute_base_to_alpaca_negative_amount() {
        let server = MockServer::start();
        let (_anvil, endpoint, private_key) = setup_anvil();

        let alpaca_wallet = Arc::new(create_test_wallet_service(&server).await);
        let (provider, signer) = create_test_provider(&endpoint, &private_key);
        let (cctp_bridge, vault_service) = create_test_onchain_services(provider, signer);
        let cqrs = create_test_cqrs();

        let market_maker_wallet = address!("0x1111111111111111111111111111111111111111");

        let manager = UsdcRebalanceManager::new(
            alpaca_wallet,
            Arc::new(cctp_bridge),
            Arc::new(vault_service),
            cqrs,
            market_maker_wallet,
            TEST_VAULT_ID,
        );

        let id = UsdcRebalanceId::new("rebalance-base-001");
        let amount = Usdc(dec!(-500));

        let result = manager.execute_base_to_alpaca(&id, amount).await;

        assert!(
            matches!(
                &result,
                Err(UsdcRebalanceManagerError::InvalidAmount(msg)) if msg.contains("-500")
            ),
            "Expected InvalidAmount error mentioning -500, got: {result:?}"
        );
    }

    #[tokio::test]
    async fn test_execute_base_to_alpaca_cctp_burn_fails_with_contract_error() {
        let server = MockServer::start();
        let (_anvil, endpoint, private_key) = setup_anvil();

        let alpaca_wallet = Arc::new(create_test_wallet_service(&server).await);
        let (provider, signer) = create_test_provider(&endpoint, &private_key);
        let (cctp_bridge, vault_service) = create_test_onchain_services(provider, signer);
        let cqrs = create_test_cqrs();

        let market_maker_wallet = address!("0x1111111111111111111111111111111111111111");

        let manager = UsdcRebalanceManager::new(
            alpaca_wallet,
            Arc::new(cctp_bridge),
            Arc::new(vault_service),
            cqrs,
            market_maker_wallet,
            TEST_VAULT_ID,
        );

        let id = UsdcRebalanceId::new("rebalance-base-002");
        let amount = Usdc(dec!(1000));

        let result = manager.execute_base_to_alpaca(&id, amount).await;

        assert!(
            matches!(
                result,
                Err(UsdcRebalanceManagerError::Aggregate(
                    AggregateError::UserError(UsdcRebalanceError::BridgingNotInitiated)
                ))
            ),
            "Expected Aggregate(UserError(BridgingNotInitiated)) error, got: {result:?}"
        );
    }
}

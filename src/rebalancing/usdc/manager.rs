//! [`CrossVenueCashTransfer`] orchestrates USDC cross-venue transfers.
//!
//! Coordinates between `AlpacaBrokerApi`, `AlpacaWalletService`,
//! `CctpBridge`, `RaindexService`, and the `UsdcRebalance` aggregate to
//! execute USDC transfers between Alpaca and Base.

use alloy::primitives::{Address, TxHash, U256};
use async_trait::async_trait;
use rust_decimal::Decimal;
use std::sync::Arc;
use tracing::{info, instrument, warn};
use uuid::Uuid;

use st0x_bridge::cctp::{AttestationResponse, CctpBridge};
use st0x_bridge::{Attestation, Bridge, BridgeDirection, BurnReceipt, MintReceipt};
use st0x_event_sorcery::Store;
use st0x_evm::Wallet;
use st0x_execution::{AlpacaBrokerApi, ConversionDirection, Positive};

use super::UsdcTransferError;
use crate::alpaca_wallet::{
    AlpacaTransferId, AlpacaWalletService, TokenSymbol, Transfer, TransferStatus,
};
use crate::onchain::raindex::{RaindexService, RaindexVaultId};
use crate::rebalancing::transfer::{CrossVenueTransfer, HedgingVenue, MarketMakingVenue};
use crate::threshold::Usdc;
use crate::usdc_rebalance::{
    RebalanceDirection, TransferRef, UsdcRebalance, UsdcRebalanceCommand, UsdcRebalanceId,
};

/// Orchestrates USDC rebalancing between Alpaca (Ethereum) and Rain (Base).
///
/// # Type Parameters
///
/// * `Chain` - Wallet type used for both Ethereum and Base chains
pub(crate) struct CrossVenueCashTransfer<Chain: Wallet> {
    alpaca_broker: Arc<AlpacaBrokerApi>,
    alpaca_wallet: Arc<AlpacaWalletService>,
    cctp_bridge: Arc<CctpBridge<Chain, Chain>>,
    raindex: Arc<RaindexService<Chain>>,
    cqrs: Arc<Store<UsdcRebalance>>,
    market_maker_wallet: Address,
    vault_id: RaindexVaultId,
}

impl<Chain: Wallet> CrossVenueCashTransfer<Chain> {
    pub(crate) fn new(
        alpaca_broker: Arc<AlpacaBrokerApi>,
        alpaca_wallet: Arc<AlpacaWalletService>,
        cctp_bridge: Arc<CctpBridge<Chain, Chain>>,
        raindex: Arc<RaindexService<Chain>>,
        cqrs: Arc<Store<UsdcRebalance>>,
        market_maker_wallet: Address,
        vault_id: RaindexVaultId,
    ) -> Self {
        Self {
            alpaca_broker,
            alpaca_wallet,
            cctp_bridge,
            raindex,
            cqrs,
            market_maker_wallet,
            vault_id,
        }
    }

    /// Converts USD buying power to USDC in the crypto wallet.
    ///
    /// Used at the start of AlpacaToBase flow, before withdrawal.
    /// Places a buy order on USDC/USD and polls until filled.
    ///
    /// Returns the actual filled USDC amount (may differ from requested
    /// due to slippage).
    ///
    /// # Event Sourcing Flow
    ///
    /// 1. Record intent via `InitiateConversion` (aggregate enters
    ///    `Converting` state)
    /// 2. Place Alpaca order
    /// 3. If order fails: emit `FailConversion` (aggregate enters
    ///    `ConversionFailed` state)
    /// 4. If order succeeds: emit `ConfirmConversion` (aggregate enters
    ///    `ConversionComplete` state)
    ///
    /// The `order_id` in `InitiateConversion` is a correlation UUID
    /// generated upfront, not the actual Alpaca order ID.
    #[instrument(skip(self), fields(?id, ?amount))]
    pub(crate) async fn execute_usd_to_usdc_conversion(
        &self,
        id: &UsdcRebalanceId,
        amount: Usdc,
    ) -> Result<Usdc, UsdcTransferError> {
        let Usdc(decimal_amount) = amount;
        let correlation_id = Uuid::new_v4();

        info!(?amount, %correlation_id, "Starting USD to USDC conversion");

        // Record intent BEFORE placing order so we can track failures
        self.cqrs
            .send(
                id,
                UsdcRebalanceCommand::InitiateConversion {
                    direction: RebalanceDirection::AlpacaToBase,
                    amount,
                    order_id: correlation_id,
                },
            )
            .await?;

        let order = match self
            .alpaca_broker
            .convert_usdc_usd(decimal_amount, ConversionDirection::UsdToUsdc)
            .await
        {
            Ok(order) => order,
            Err(error) => {
                warn!("USD to USDC conversion failed: {error}");
                self.cqrs
                    .send(
                        id,
                        UsdcRebalanceCommand::FailConversion {
                            reason: error.to_string(),
                        },
                    )
                    .await?;
                return Err(UsdcTransferError::AlpacaBrokerApi(error));
            }
        };

        let filled_qty = order
            .filled_quantity
            .ok_or_else(|| UsdcTransferError::MissingFilledQuantity { order_id: order.id })?;
        let filled_amount = Usdc(filled_qty);

        self.cqrs
            .send(
                id,
                UsdcRebalanceCommand::ConfirmConversion { filled_amount },
            )
            .await?;

        info!(
            order_id = %order.id,
            requested = %amount.0,
            filled = %filled_qty,
            "USD to USDC conversion completed"
        );
        Ok(filled_amount)
    }

    /// Converts USDC to USD buying power.
    ///
    /// Used at the end of BaseToAlpaca flow, after deposit is confirmed.
    /// Places a sell order on USDC/USD and polls until filled.
    ///
    /// Returns the actual filled USDC amount (the USDC sold, which may
    /// differ from requested if there's a partial fill).
    ///
    /// # Event Sourcing Flow
    ///
    /// 1. Record intent via `InitiatePostDepositConversion` (aggregate
    ///    enters `Converting` state)
    /// 2. Place Alpaca order
    /// 3. If order fails: emit `FailConversion` (aggregate enters
    ///    `ConversionFailed` state)
    /// 4. If order succeeds: emit `ConfirmConversion` (aggregate enters
    ///    `ConversionComplete` state)
    ///
    /// The `order_id` in `InitiatePostDepositConversion` is a correlation
    /// UUID generated upfront, not the actual Alpaca order ID.
    #[instrument(skip(self), fields(?id, ?amount))]
    pub(crate) async fn execute_usdc_to_usd_conversion(
        &self,
        id: &UsdcRebalanceId,
        amount: Usdc,
    ) -> Result<Usdc, UsdcTransferError> {
        let Usdc(decimal_amount) = amount;
        let correlation_id = Uuid::new_v4();

        info!(?amount, %correlation_id, "Starting USDC to USD conversion");

        // Record intent BEFORE placing order so we can track failures
        self.cqrs
            .send(
                id,
                UsdcRebalanceCommand::InitiatePostDepositConversion {
                    order_id: correlation_id,
                    amount,
                },
            )
            .await?;

        let order = match self
            .alpaca_broker
            .convert_usdc_usd(decimal_amount, ConversionDirection::UsdcToUsd)
            .await
        {
            Ok(order) => order,
            Err(error) => {
                warn!("USDC to USD conversion failed: {error}");
                self.cqrs
                    .send(
                        id,
                        UsdcRebalanceCommand::FailConversion {
                            reason: error.to_string(),
                        },
                    )
                    .await?;
                return Err(UsdcTransferError::AlpacaBrokerApi(error));
            }
        };

        let filled_amount = order
            .filled_quantity
            .ok_or_else(|| UsdcTransferError::MissingFilledQuantity { order_id: order.id })?;
        let filled_usdc = Usdc(filled_amount);

        self.cqrs
            .send(
                id,
                UsdcRebalanceCommand::ConfirmConversion {
                    filled_amount: filled_usdc,
                },
            )
            .await?;

        info!(
            order_id = %order.id,
            requested = %amount.0,
            filled = %filled_amount,
            "USDC to USD conversion completed"
        );
        Ok(filled_usdc)
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
    /// On errors, sends appropriate `Fail*` command to transition
    /// aggregate to failed state.
    #[instrument(skip(self), fields(?id, ?amount))]
    pub(crate) async fn execute_alpaca_to_base(
        &self,
        id: &UsdcRebalanceId,
        amount: Usdc,
    ) -> Result<(), UsdcTransferError> {
        info!(?amount, "Starting Alpaca to Base rebalance");

        // Convert USD to USDC - use actual filled amount for subsequent steps
        let usdc_amount = self.execute_usd_to_usdc_conversion(id, amount).await?;

        let transfer = self.initiate_alpaca_withdrawal(id, usdc_amount).await?;

        self.poll_and_confirm_withdrawal(id, &transfer.id).await?;

        let burn_amount = usdc_to_u256(usdc_amount)?;
        let burn_receipt = self.execute_cctp_burn(id, burn_amount).await?;

        let attestation_response = self.poll_attestation(id, &burn_receipt).await?;

        // Use the actual minted amount (net of CCTP fee) for vault deposit
        let mint_receipt = self.execute_cctp_mint(id, attestation_response).await?;

        self.deposit_to_vault(id, mint_receipt.amount).await?;

        self.confirm_deposit(id).await?;

        info!("Alpaca to Base rebalance completed successfully");
        Ok(())
    }

    #[instrument(skip(self), fields(?id, ?amount))]
    async fn initiate_alpaca_withdrawal(
        &self,
        id: &UsdcRebalanceId,
        amount: Usdc,
    ) -> Result<Transfer, UsdcTransferError> {
        let usdc = TokenSymbol::new("USDC");
        let positive_amount = Positive::new(amount)?;

        let transfer = match self
            .alpaca_wallet
            .initiate_withdrawal(positive_amount, &usdc, &self.market_maker_wallet)
            .await
        {
            Ok(transfer) => transfer,
            Err(error) => {
                warn!("Alpaca withdrawal initiation failed: {error}");
                return Err(UsdcTransferError::AlpacaWallet(error));
            }
        };

        self.cqrs
            .send(
                id,
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
    ) -> Result<(), UsdcTransferError> {
        let transfer = match self
            .alpaca_wallet
            .poll_transfer_until_complete(transfer_id)
            .await
        {
            Ok(transfer) => transfer,
            Err(error) => {
                warn!("Alpaca withdrawal polling failed: {error}");
                self.cqrs
                    .send(
                        id,
                        UsdcRebalanceCommand::FailWithdrawal {
                            reason: format!("Polling failed: {error}"),
                        },
                    )
                    .await?;
                return Err(UsdcTransferError::AlpacaWallet(error));
            }
        };

        if transfer.status != TransferStatus::Complete {
            let status = format!("{:?}", transfer.status);
            self.cqrs
                .send(
                    id,
                    UsdcRebalanceCommand::FailWithdrawal {
                        reason: format!("Transfer ended in status: {status}"),
                    },
                )
                .await?;
            return Err(UsdcTransferError::WithdrawalFailed { status });
        }

        self.cqrs
            .send(id, UsdcRebalanceCommand::ConfirmWithdrawal)
            .await?;

        info!("Alpaca withdrawal confirmed");
        Ok(())
    }

    #[instrument(skip(self), fields(?id, %amount))]
    async fn execute_cctp_burn(
        &self,
        id: &UsdcRebalanceId,
        amount: U256,
    ) -> Result<BurnReceipt, UsdcTransferError> {
        let burn_receipt = match self
            .cctp_bridge
            .burn(
                BridgeDirection::EthereumToBase,
                amount,
                self.market_maker_wallet,
            )
            .await
        {
            Ok(receipt) => receipt,
            Err(error) => {
                warn!("CCTP burn failed: {error}");
                self.cqrs
                    .send(
                        id,
                        UsdcRebalanceCommand::FailBridging {
                            reason: format!("Burn failed: {error}"),
                        },
                    )
                    .await?;
                return Err(UsdcTransferError::Cctp(Box::new(error)));
            }
        };

        self.cqrs
            .send(
                id,
                UsdcRebalanceCommand::InitiateBridging {
                    burn_tx: burn_receipt.tx,
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
    ) -> Result<AttestationResponse, UsdcTransferError> {
        let response = match self
            .cctp_bridge
            .poll_attestation(BridgeDirection::EthereumToBase, burn_receipt.tx)
            .await
        {
            Ok(response) => response,
            Err(error) => {
                warn!("Attestation polling failed: {error}");
                self.cqrs
                    .send(
                        id,
                        UsdcRebalanceCommand::FailBridging {
                            reason: format!("Attestation polling failed: {error}"),
                        },
                    )
                    .await?;
                return Err(UsdcTransferError::Cctp(Box::new(error)));
            }
        };

        self.cqrs
            .send(
                id,
                UsdcRebalanceCommand::ReceiveAttestation {
                    attestation: response.as_bytes().to_vec(),
                    cctp_nonce: response.nonce(),
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
    ) -> Result<MintReceipt, UsdcTransferError> {
        let mint_receipt = match self
            .cctp_bridge
            .mint(BridgeDirection::EthereumToBase, &attestation_response)
            .await
        {
            Ok(receipt) => receipt,
            Err(error) => {
                warn!("CCTP mint failed: {error}");
                self.cqrs
                    .send(
                        id,
                        UsdcRebalanceCommand::FailBridging {
                            reason: format!("Mint failed: {error}"),
                        },
                    )
                    .await?;
                return Err(UsdcTransferError::Cctp(Box::new(error)));
            }
        };

        self.cqrs
            .send(
                id,
                UsdcRebalanceCommand::ConfirmBridging {
                    mint_tx: mint_receipt.tx,
                    amount_received: u256_to_usdc(mint_receipt.amount)?,
                    fee_collected: u256_to_usdc(mint_receipt.fee)?,
                },
            )
            .await?;

        info!(
            mint_tx = %mint_receipt.tx,
            amount = %mint_receipt.amount,
            fee = %mint_receipt.fee,
            "CCTP mint executed"
        );
        Ok(mint_receipt)
    }

    #[instrument(skip(self), fields(?id, ?amount))]
    async fn deposit_to_vault(
        &self,
        id: &UsdcRebalanceId,
        amount: U256,
    ) -> Result<(), UsdcTransferError> {
        let deposit_tx = match self.raindex.deposit_usdc(self.vault_id, amount).await {
            Ok(tx) => tx,
            Err(error) => {
                warn!("Vault deposit failed: {error}");
                self.cqrs
                    .send(
                        id,
                        UsdcRebalanceCommand::FailDeposit {
                            reason: format!("Vault deposit failed: {error}"),
                        },
                    )
                    .await?;
                return Err(UsdcTransferError::Vault(error));
            }
        };

        self.cqrs
            .send(
                id,
                UsdcRebalanceCommand::InitiateDeposit {
                    deposit: TransferRef::OnchainTx(deposit_tx),
                },
            )
            .await?;

        info!(%deposit_tx, "Vault deposit initiated");
        Ok(())
    }

    #[instrument(skip(self), fields(?id))]
    async fn confirm_deposit(&self, id: &UsdcRebalanceId) -> Result<(), UsdcTransferError> {
        self.cqrs
            .send(id, UsdcRebalanceCommand::ConfirmDeposit)
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
    /// 6. Initiate Alpaca deposit (mint directly to Alpaca address)
    ///    -> `InitiateDeposit` command
    /// 7. Poll Alpaca until deposit credited -> `ConfirmDeposit` command
    ///
    /// On errors, sends appropriate `Fail*` command to transition
    /// aggregate to failed state.
    #[instrument(skip(self), fields(?id, ?amount))]
    pub(crate) async fn execute_base_to_alpaca(
        &self,
        id: &UsdcRebalanceId,
        amount: Usdc,
    ) -> Result<(), UsdcTransferError> {
        info!(?amount, "Starting Base to Alpaca rebalance");

        let amount_u256 = usdc_to_u256(amount)?;

        self.withdraw_from_vault(id, amount, amount_u256).await?;

        let burn_receipt = self.execute_cctp_burn_on_base(id, amount_u256).await?;

        let attestation_response = self
            .poll_attestation_for_base_burn(id, &burn_receipt)
            .await?;

        // Use the actual minted amount (net of CCTP fee) for downstream operations
        let mint_receipt = self
            .execute_cctp_mint_on_ethereum(id, attestation_response)
            .await?;

        self.poll_and_confirm_alpaca_deposit(id, mint_receipt.tx)
            .await?;

        // Convert deposited USDC to USD buying power using actual received amount
        let amount_received = u256_to_usdc(mint_receipt.amount)?;
        self.execute_usdc_to_usd_conversion(id, amount_received)
            .await?;

        info!("Base to Alpaca rebalance completed successfully");
        Ok(())
    }

    #[instrument(skip(self), fields(?id, ?amount))]
    async fn withdraw_from_vault(
        &self,
        id: &UsdcRebalanceId,
        amount: Usdc,
        amount_u256: U256,
    ) -> Result<(), UsdcTransferError> {
        let withdraw_tx = match self.raindex.withdraw_usdc(self.vault_id, amount_u256).await {
            Ok(tx) => tx,
            Err(error) => {
                warn!("Vault withdrawal failed: {error}");
                return Err(UsdcTransferError::Vault(error));
            }
        };

        self.cqrs
            .send(
                id,
                UsdcRebalanceCommand::Initiate {
                    direction: RebalanceDirection::BaseToAlpaca,
                    amount,
                    withdrawal: TransferRef::OnchainTx(withdraw_tx),
                },
            )
            .await?;

        // Vault withdrawal function already waits for block inclusion, so confirming immediately
        self.cqrs
            .send(id, UsdcRebalanceCommand::ConfirmWithdrawal)
            .await?;

        info!(%withdraw_tx, "Vault withdrawal completed");
        Ok(())
    }

    #[instrument(skip(self), fields(?id, ?amount))]
    async fn execute_cctp_burn_on_base(
        &self,
        id: &UsdcRebalanceId,
        amount: U256,
    ) -> Result<BurnReceipt, UsdcTransferError> {
        let burn_receipt = match self
            .cctp_bridge
            .burn(
                BridgeDirection::BaseToEthereum,
                amount,
                self.market_maker_wallet,
            )
            .await
        {
            Ok(receipt) => receipt,
            Err(error) => {
                warn!("CCTP burn on Base failed: {error}");
                self.cqrs
                    .send(
                        id,
                        UsdcRebalanceCommand::FailBridging {
                            reason: format!("Burn on Base failed: {error}"),
                        },
                    )
                    .await?;
                return Err(UsdcTransferError::Cctp(Box::new(error)));
            }
        };

        self.cqrs
            .send(
                id,
                UsdcRebalanceCommand::InitiateBridging {
                    burn_tx: burn_receipt.tx,
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
    ) -> Result<AttestationResponse, UsdcTransferError> {
        let response = match self
            .cctp_bridge
            .poll_attestation(BridgeDirection::BaseToEthereum, burn_receipt.tx)
            .await
        {
            Ok(response) => response,
            Err(error) => {
                warn!("Attestation polling failed: {error}");
                self.cqrs
                    .send(
                        id,
                        UsdcRebalanceCommand::FailBridging {
                            reason: format!("Attestation polling failed: {error}"),
                        },
                    )
                    .await?;
                return Err(UsdcTransferError::Cctp(Box::new(error)));
            }
        };

        self.cqrs
            .send(
                id,
                UsdcRebalanceCommand::ReceiveAttestation {
                    attestation: response.as_bytes().to_vec(),
                    cctp_nonce: response.nonce(),
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
    ) -> Result<MintReceipt, UsdcTransferError> {
        let mint_receipt = match self
            .cctp_bridge
            .mint(BridgeDirection::BaseToEthereum, &attestation_response)
            .await
        {
            Ok(receipt) => receipt,
            Err(error) => {
                warn!("CCTP mint on Ethereum failed: {error}");
                self.cqrs
                    .send(
                        id,
                        UsdcRebalanceCommand::FailBridging {
                            reason: format!("Mint on Ethereum failed: {error}"),
                        },
                    )
                    .await?;
                return Err(UsdcTransferError::Cctp(Box::new(error)));
            }
        };

        self.cqrs
            .send(
                id,
                UsdcRebalanceCommand::ConfirmBridging {
                    mint_tx: mint_receipt.tx,
                    amount_received: u256_to_usdc(mint_receipt.amount)?,
                    fee_collected: u256_to_usdc(mint_receipt.fee)?,
                },
            )
            .await?;

        info!(
            mint_tx = %mint_receipt.tx,
            amount = %mint_receipt.amount,
            fee = %mint_receipt.fee,
            "CCTP mint on Ethereum executed"
        );
        Ok(mint_receipt)
    }

    #[instrument(skip(self), fields(?id, %mint_tx))]
    async fn poll_and_confirm_alpaca_deposit(
        &self,
        id: &UsdcRebalanceId,
        mint_tx: TxHash,
    ) -> Result<(), UsdcTransferError> {
        // Record the deposit initiation with the mint tx
        self.cqrs
            .send(
                id,
                UsdcRebalanceCommand::InitiateDeposit {
                    deposit: TransferRef::OnchainTx(mint_tx),
                },
            )
            .await?;

        info!(%mint_tx, "Polling Alpaca for deposit detection");

        let transfer = match self.alpaca_wallet.poll_deposit_by_tx_hash(&mint_tx).await {
            Ok(transfer) => transfer,
            Err(error) => {
                warn!("Alpaca deposit polling failed: {error}");
                self.cqrs
                    .send(
                        id,
                        UsdcRebalanceCommand::FailDeposit {
                            reason: format!("Deposit polling failed: {error}"),
                        },
                    )
                    .await?;
                return Err(UsdcTransferError::AlpacaWallet(error));
            }
        };

        if transfer.status != TransferStatus::Complete {
            let status = format!("{:?}", transfer.status);
            self.cqrs
                .send(
                    id,
                    UsdcRebalanceCommand::FailDeposit {
                        reason: format!("Deposit ended in status: {status}"),
                    },
                )
                .await?;
            return Err(UsdcTransferError::DepositFailed { status });
        }

        self.cqrs
            .send(id, UsdcRebalanceCommand::ConfirmDeposit)
            .await?;

        info!("Alpaca deposit confirmed");
        Ok(())
    }
}

/// Converts a USDC decimal amount to U256 with 6 decimals.
///
/// # Errors
///
/// Returns an error if the decimal cannot be represented as U256
/// (e.g., negative values or values exceeding U256::MAX).
fn usdc_to_u256(usdc: Usdc) -> Result<U256, UsdcTransferError> {
    if usdc.0.is_sign_negative() {
        return Err(UsdcTransferError::NegativeAmount { amount: usdc });
    }

    // USDC has 6 decimals
    let scaled = usdc
        .0
        .checked_mul(Decimal::from(1_000_000u64))
        .ok_or_else(|| UsdcTransferError::ArithmeticOverflow { amount: usdc })?;

    let integer = scaled.trunc().to_string();

    Ok(U256::from_str_radix(&integer, 10)?)
}

/// Converts a U256 amount (with 6 decimals) to USDC decimal.
fn u256_to_usdc(amount: U256) -> Result<Usdc, UsdcTransferError> {
    let amount_u128: u128 = amount.try_into()?;
    let decimal = Decimal::from(amount_u128) / Decimal::from(1_000_000u64);
    Ok(Usdc(decimal))
}

/// Alpaca -> Base (hedging -> market-making): convert USD to USDC,
/// withdraw, bridge via CCTP, deposit to vault.
#[async_trait]
impl<Chain: Wallet> CrossVenueTransfer<HedgingVenue, MarketMakingVenue>
    for CrossVenueCashTransfer<Chain>
{
    type Asset = Usdc;
    type Error = UsdcTransferError;

    async fn transfer(&self, asset: Self::Asset) -> Result<(), Self::Error> {
        let id = UsdcRebalanceId(Uuid::new_v4());
        self.execute_alpaca_to_base(&id, asset).await
    }
}

/// Base -> Alpaca (market-making -> hedging): withdraw from vault,
/// bridge via CCTP, deposit USDC, convert to USD.
#[async_trait]
impl<Chain: Wallet> CrossVenueTransfer<MarketMakingVenue, HedgingVenue>
    for CrossVenueCashTransfer<Chain>
{
    type Asset = Usdc;
    type Error = UsdcTransferError;

    async fn transfer(&self, asset: Self::Asset) -> Result<(), Self::Error> {
        let id = UsdcRebalanceId(Uuid::new_v4());
        self.execute_base_to_alpaca(&id, asset).await
    }
}

#[cfg(test)]
mod tests {
    use alloy::node_bindings::Anvil;
    use alloy::primitives::{B256, address, b256, fixed_bytes};
    use alloy::providers::ProviderBuilder;
    use httpmock::prelude::*;
    use reqwest::StatusCode;
    use rust_decimal_macros::dec;
    use serde_json::json;
    use sqlx::SqlitePool;
    use std::sync::Arc;
    use uuid::{Uuid, uuid};

    use st0x_execution::alpaca_broker_api::CryptoOrderFailureReason;
    use st0x_execution::{
        AlpacaAccountId, AlpacaBrokerApiCtx, AlpacaBrokerApiError, AlpacaBrokerApiMode, Executor,
        TimeInForce,
    };

    use st0x_bridge::cctp::{CctpBridge, CctpCtx};
    use st0x_event_sorcery::{AggregateError, LifecycleError, StoreBuilder, test_store};
    use st0x_evm::Wallet;
    use st0x_evm::local::RawPrivateKeyWallet;

    use super::*;
    use crate::alpaca_wallet::{AlpacaTransferId, AlpacaWalletClient, AlpacaWalletError};
    use crate::onchain::raindex::RaindexService;
    use crate::rebalancing::usdc::mock::MockUsdcRebalance;
    use crate::usdc_rebalance::{RebalanceDirection, TransferRef, UsdcRebalanceError};
    use crate::vault_registry::VaultRegistry;

    const USDC_ADDRESS: Address = address!("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48");
    const ORDERBOOK_ADDRESS: Address = address!("0x1234567890123456789012345678901234567890");
    const TEST_VAULT_ID: RaindexVaultId = RaindexVaultId(b256!(
        "0x0000000000000000000000000000000000000000000000000000000000000001"
    ));

    async fn create_test_store_instance() -> Arc<Store<UsdcRebalance>> {
        let pool = SqlitePool::connect(":memory:").await.unwrap();
        sqlx::migrate!().run(&pool).await.unwrap();

        Arc::new(test_store(pool, ()))
    }

    /// Advances aggregate through: Initiate -> ConfirmWithdrawal ->
    /// InitiateBridging -> ReceiveAttestation -> ConfirmBridging ->
    /// InitiateDeposit -> ConfirmDeposit
    async fn advance_to_deposit_confirmed_base_to_alpaca(
        cqrs: &Store<UsdcRebalance>,
        id: &UsdcRebalanceId,
        amount: Usdc,
    ) {
        let burn_tx =
            fixed_bytes!("0xbbbb000000000000000000000000000000000000000000000000000000000001");
        let mint_tx =
            fixed_bytes!("0xbbbb111111111111111111111111111111111111111111111111111111111111");

        cqrs.send(
            id,
            UsdcRebalanceCommand::Initiate {
                direction: RebalanceDirection::BaseToAlpaca,
                amount,
                withdrawal: TransferRef::OnchainTx(burn_tx),
            },
        )
        .await
        .unwrap();

        cqrs.send(id, UsdcRebalanceCommand::ConfirmWithdrawal)
            .await
            .unwrap();

        cqrs.send(id, UsdcRebalanceCommand::InitiateBridging { burn_tx })
            .await
            .unwrap();

        cqrs.send(
            id,
            UsdcRebalanceCommand::ReceiveAttestation {
                attestation: vec![0x01],
                cctp_nonce: 99999,
            },
        )
        .await
        .unwrap();

        cqrs.send(
            id,
            UsdcRebalanceCommand::ConfirmBridging {
                mint_tx,
                amount_received: Usdc(dec!(99.99)),
                fee_collected: Usdc(dec!(0.01)),
            },
        )
        .await
        .unwrap();

        cqrs.send(
            id,
            UsdcRebalanceCommand::InitiateDeposit {
                deposit: TransferRef::AlpacaId(AlpacaTransferId::from(Uuid::new_v4())),
            },
        )
        .await
        .unwrap();

        cqrs.send(id, UsdcRebalanceCommand::ConfirmDeposit)
            .await
            .unwrap();
    }

    fn setup_anvil() -> (alloy::node_bindings::AnvilInstance, String, B256) {
        let anvil = Anvil::new().spawn();
        let endpoint = anvil.endpoint();
        let private_key = B256::from_slice(&anvil.keys()[0].to_bytes());
        (anvil, endpoint, private_key)
    }

    const TEST_ACCOUNT_ID: AlpacaAccountId =
        AlpacaAccountId::new(uuid!("904837e3-3b76-47ec-b432-046db621571b"));

    fn create_broker_account_mock(server: &MockServer) -> httpmock::Mock<'_> {
        server.mock(|when, then| {
            when.method(GET)
                .path("/v1/trading/accounts/904837e3-3b76-47ec-b432-046db621571b/account");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "id": "904837e3-3b76-47ec-b432-046db621571b",
                    "status": "ACTIVE"
                }));
        })
    }

    async fn create_test_broker_service(server: &MockServer) -> AlpacaBrokerApi {
        let _account_mock = create_broker_account_mock(server);

        let auth = AlpacaBrokerApiCtx {
            api_key: "test_key".to_string(),
            api_secret: "test_secret".to_string(),
            account_id: AlpacaAccountId::new(uuid!("904837e3-3b76-47ec-b432-046db621571b")),
            mode: Some(AlpacaBrokerApiMode::Mock(server.base_url())),
            asset_cache_ttl: std::time::Duration::from_secs(3600),
            time_in_force: TimeInForce::default(),
        };

        AlpacaBrokerApi::try_from_ctx(auth)
            .await
            .expect("Failed to create test broker API")
    }

    fn create_test_wallet_service(server: &MockServer) -> AlpacaWalletService {
        let client = AlpacaWalletClient::new(
            server.base_url(),
            TEST_ACCOUNT_ID,
            "test_key".to_string(),
            "test_secret".to_string(),
        );

        AlpacaWalletService::new_with_client(client, None)
    }

    fn create_test_wallet(
        endpoint: &str,
        private_key: &B256,
    ) -> RawPrivateKeyWallet<impl alloy::providers::Provider + Clone + use<>> {
        let base_provider = ProviderBuilder::new().connect_http(endpoint.parse().unwrap());

        RawPrivateKeyWallet::new(private_key, base_provider, 1).unwrap()
    }

    async fn create_test_onchain_services<Chain: Wallet + Clone>(
        wallet: Chain,
    ) -> (CctpBridge<Chain, Chain>, RaindexService<Chain>) {
        let cctp_bridge = CctpBridge::try_from_ctx(CctpCtx {
            usdc_ethereum: USDC_ADDRESS,
            usdc_base: USDC_ADDRESS,
            ethereum_wallet: wallet.clone(),
            base_wallet: wallet.clone(),
            circle_api_base: None,
            token_messenger: None,
            message_transmitter: None,
        })
        .unwrap();

        let pool = crate::test_utils::setup_test_db().await;

        let (_vault_registry_store, vault_registry_projection) =
            StoreBuilder::<VaultRegistry>::new(pool)
                .build(())
                .await
                .unwrap();

        let owner = wallet.address();

        let vault_service =
            RaindexService::new(wallet, ORDERBOOK_ADDRESS, vault_registry_projection, owner);

        (cctp_bridge, vault_service)
    }

    fn create_conversion_order_mock<'a>(
        server: &'a MockServer,
        amount: &str,
    ) -> httpmock::Mock<'a> {
        server.mock(|when, then| {
            when.method(POST)
                .path("/v1/trading/accounts/904837e3-3b76-47ec-b432-046db621571b/orders");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "id": "61e7b016-9c91-4a97-b912-615c9d365c9d",
                    "symbol": "USDCUSD",
                    "qty": amount,
                    "status": "filled",
                    "filled_avg_price": "1.0001",
                    "filled_qty": amount,
                    "created_at": "2024-01-15T10:30:00Z"
                }));
        })
    }

    fn create_conversion_order_pending_mock<'a>(
        server: &'a MockServer,
        amount: &str,
    ) -> httpmock::Mock<'a> {
        server.mock(|when, then| {
            when.method(POST)
                .path("/v1/trading/accounts/904837e3-3b76-47ec-b432-046db621571b/orders");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "id": "61e7b016-9c91-4a97-b912-615c9d365c9d",
                    "symbol": "USDCUSD",
                    "qty": amount,
                    "status": "new",
                    "created_at": "2024-01-15T10:30:00Z"
                }));
        })
    }

    fn create_get_order_mock<'a>(
        server: &'a MockServer,
        order_id: &str,
        status: &str,
        amount: &str,
    ) -> httpmock::Mock<'a> {
        server.mock(|when, then| {
            when.method(GET).path(format!(
                "/v1/trading/accounts/904837e3-3b76-47ec-b432-046db621571b/orders/{order_id}"
            ));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "id": order_id,
                    "symbol": "USDCUSD",
                    "qty": amount,
                    "status": status,
                    "filled_avg_price": if status == "filled" { Some("1.0001") } else { None },
                    "filled_qty": if status == "filled" { Some(amount) } else { None },
                    "created_at": "2024-01-15T10:30:00Z"
                }));
        })
    }

    /// Creates a mock where filled_qty differs from requested qty to
    /// simulate slippage.
    fn create_get_order_mock_with_slippage<'a>(
        server: &'a MockServer,
        order_id: &str,
        requested_qty: &str,
        filled_qty: &str,
    ) -> httpmock::Mock<'a> {
        server.mock(|when, then| {
            when.method(GET).path(format!(
                "/v1/trading/accounts/904837e3-3b76-47ec-b432-046db621571b/orders/{order_id}"
            ));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "id": order_id,
                    "symbol": "USDCUSD",
                    "qty": requested_qty,
                    "status": "filled",
                    "filled_avg_price": "1.0001",
                    "filled_qty": filled_qty,
                    "created_at": "2024-01-15T10:30:00Z"
                }));
        })
    }

    #[test]
    fn mock_tracks_call_counts() {
        let mock = MockUsdcRebalance::new();
        assert_eq!(mock.alpaca_to_base_calls(), 0);
        assert_eq!(mock.base_to_alpaca_calls(), 0);
    }

    #[tokio::test]
    async fn mock_alpaca_to_base_increments_count() {
        let mock = Arc::new(MockUsdcRebalance::new());

        CrossVenueTransfer::<HedgingVenue, MarketMakingVenue>::transfer(&*mock, Usdc(dec!(1000)))
            .await
            .unwrap();

        assert_eq!(mock.alpaca_to_base_calls(), 1);
        assert_eq!(mock.base_to_alpaca_calls(), 0);
    }

    #[tokio::test]
    async fn mock_base_to_alpaca_increments_count() {
        let mock = Arc::new(MockUsdcRebalance::new());

        CrossVenueTransfer::<MarketMakingVenue, HedgingVenue>::transfer(&*mock, Usdc(dec!(2000)))
            .await
            .unwrap();

        assert_eq!(mock.alpaca_to_base_calls(), 0);
        assert_eq!(mock.base_to_alpaca_calls(), 1);
    }

    #[tokio::test]
    async fn mock_captures_last_alpaca_to_base_call() {
        let mock = Arc::new(MockUsdcRebalance::new());
        let amount = Usdc(dec!(5000.50));

        CrossVenueTransfer::<HedgingVenue, MarketMakingVenue>::transfer(&*mock, amount)
            .await
            .unwrap();

        let last = mock.last_alpaca_to_base_call().unwrap();
        assert_eq!(last.amount, amount);
    }

    #[tokio::test]
    async fn mock_captures_last_base_to_alpaca_call() {
        let mock = Arc::new(MockUsdcRebalance::new());
        let amount = Usdc(dec!(7500.25));

        CrossVenueTransfer::<MarketMakingVenue, HedgingVenue>::transfer(&*mock, amount)
            .await
            .unwrap();

        let last = mock.last_base_to_alpaca_call().unwrap();
        assert_eq!(last.amount, amount);
    }

    #[tokio::test]
    async fn failing_mock_returns_error_for_alpaca_to_base() {
        let mock = Arc::new(MockUsdcRebalance::failing_alpaca_to_base());

        let result = CrossVenueTransfer::<HedgingVenue, MarketMakingVenue>::transfer(
            &*mock,
            Usdc(dec!(100)),
        )
        .await;

        assert!(matches!(
            result,
            Err(UsdcTransferError::WithdrawalFailed { .. })
        ));
    }

    #[tokio::test]
    async fn failing_mock_returns_error_for_base_to_alpaca() {
        let mock = Arc::new(MockUsdcRebalance::failing_base_to_alpaca());

        let result = CrossVenueTransfer::<MarketMakingVenue, HedgingVenue>::transfer(
            &*mock,
            Usdc(dec!(100)),
        )
        .await;

        assert!(matches!(
            result,
            Err(UsdcTransferError::DepositFailed { .. })
        ));
    }

    #[test]
    fn test_error_display_withdrawal_failed() {
        let err = UsdcTransferError::WithdrawalFailed {
            status: "Cancelled".to_string(),
        };
        assert_eq!(
            err.to_string(),
            "Withdrawal failed with terminal status: Cancelled"
        );
    }

    #[test]
    fn test_error_display_deposit_failed() {
        let err = UsdcTransferError::DepositFailed {
            status: "Rejected".to_string(),
        };
        assert_eq!(
            err.to_string(),
            "Deposit failed with terminal status: Rejected"
        );
    }

    #[test]
    fn test_usdc_to_u256_positive_amount() {
        let amount = Usdc(dec!(1000.50));
        assert_eq!(usdc_to_u256(amount).unwrap(), U256::from(1_000_500_000u64));
    }

    #[test]
    fn test_usdc_to_u256_negative_amount() {
        let amount = Usdc(dec!(-100));
        let error = usdc_to_u256(amount).unwrap_err();
        assert!(
            matches!(error, UsdcTransferError::NegativeAmount { amount } if amount == Usdc(dec!(-100))),
            "Expected NegativeAmount error, got: {error:?}"
        );
    }

    #[test]
    fn test_usdc_to_u256_zero_amount() {
        let amount = Usdc(dec!(0));
        assert_eq!(usdc_to_u256(amount).unwrap(), U256::ZERO);
    }

    #[test]
    fn test_usdc_to_u256_fractional_truncation() {
        let amount = Usdc(dec!(100.1234567));
        assert_eq!(usdc_to_u256(amount).unwrap(), U256::from(100_123_456u64));
    }

    #[test]
    fn test_usdc_to_u256_fractional_precision() {
        // Test with precise fractional amounts (6 decimals for USDC)
        let amount = Usdc(dec!(1000.123456));
        assert_eq!(usdc_to_u256(amount).unwrap(), U256::from(1_000_123_456u64));
    }

    #[test]
    fn test_usdc_to_u256_minimum_amount() {
        // Test near-minimum amounts (smallest USDC unit is 0.000001)
        let amount = Usdc(dec!(0.000001));
        assert_eq!(usdc_to_u256(amount).unwrap(), U256::from(1u64));
    }

    #[test]
    fn test_usdc_to_u256_large_amount_no_overflow() {
        // Test large amounts that should work without overflow
        // $1 trillion in USDC
        let amount = Usdc(dec!(1_000_000_000_000));
        assert_eq!(
            usdc_to_u256(amount).unwrap(),
            U256::from(1_000_000_000_000_000_000u64)
        );
    }

    #[test]
    fn test_usdc_to_u256_truncates_beyond_6_decimals() {
        // USDC has 6 decimals, anything beyond should be truncated
        let amount = Usdc(dec!(100.1234567890));
        let result = usdc_to_u256(amount).unwrap();
        // Should truncate to 100.123456 (6 decimals)
        assert_eq!(result, U256::from(100_123_456u64));
    }

    #[tokio::test]
    async fn test_execute_alpaca_to_base_withdrawal_not_whitelisted() {
        let server = MockServer::start();
        let (_anvil, endpoint, private_key) = setup_anvil();

        let alpaca_broker = Arc::new(create_test_broker_service(&server).await);
        let alpaca_wallet = Arc::new(create_test_wallet_service(&server));
        let wallet = create_test_wallet(&endpoint, &private_key);
        let (cctp_bridge, vault_service) = create_test_onchain_services(wallet).await;
        let cqrs = create_test_store_instance().await;

        let market_maker_wallet = address!("0x1111111111111111111111111111111111111111");

        let manager = CrossVenueCashTransfer::new(
            alpaca_broker,
            alpaca_wallet,
            Arc::new(cctp_bridge),
            Arc::new(vault_service),
            cqrs,
            market_maker_wallet,
            TEST_VAULT_ID,
        );

        // Mock conversion order (conversion happens before withdrawal)
        let _conversion_mock = create_conversion_order_mock(&server, "1000");
        let _get_order_mock = create_get_order_mock(
            &server,
            "61e7b016-9c91-4a97-b912-615c9d365c9d",
            "filled",
            "1000",
        );

        let whitelist_mock = server.mock(|when, then| {
            when.method(GET)
                .path("/v1/accounts/904837e3-3b76-47ec-b432-046db621571b/wallets/whitelists");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([]));
        });

        let id = UsdcRebalanceId(Uuid::new_v4());
        let amount = Usdc(dec!(1000));

        assert!(
            matches!(
                manager.execute_alpaca_to_base(&id, amount).await,
                Err(UsdcTransferError::AlpacaWallet(
                    AlpacaWalletError::AddressNotWhitelisted { .. }
                ))
            ),
            "Expected AddressNotWhitelisted error"
        );
        whitelist_mock.assert();
    }

    #[tokio::test]
    async fn test_execute_alpaca_to_base_withdrawal_pending_whitelist() {
        let server = MockServer::start();
        let (_anvil, endpoint, private_key) = setup_anvil();

        let alpaca_broker = Arc::new(create_test_broker_service(&server).await);
        let alpaca_wallet = Arc::new(create_test_wallet_service(&server));
        let wallet = create_test_wallet(&endpoint, &private_key);
        let (cctp_bridge, vault_service) = create_test_onchain_services(wallet).await;
        let cqrs = create_test_store_instance().await;

        let market_maker_wallet = address!("0x1111111111111111111111111111111111111111");

        let manager = CrossVenueCashTransfer::new(
            alpaca_broker,
            alpaca_wallet,
            Arc::new(cctp_bridge),
            Arc::new(vault_service),
            cqrs,
            market_maker_wallet,
            TEST_VAULT_ID,
        );

        // Mock conversion order (conversion happens before withdrawal)
        let _conversion_mock = create_conversion_order_mock(&server, "500");
        let _get_order_mock = create_get_order_mock(
            &server,
            "61e7b016-9c91-4a97-b912-615c9d365c9d",
            "filled",
            "500",
        );

        let whitelist_mock = server.mock(|when, then| {
            when.method(GET)
                .path("/v1/accounts/904837e3-3b76-47ec-b432-046db621571b/wallets/whitelists");
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

        let id = UsdcRebalanceId(Uuid::new_v4());
        let amount = Usdc(dec!(500));

        assert!(
            matches!(
                manager.execute_alpaca_to_base(&id, amount).await,
                Err(UsdcTransferError::AlpacaWallet(
                    AlpacaWalletError::AddressNotWhitelisted { .. }
                ))
            ),
            "Expected AddressNotWhitelisted error for pending whitelist"
        );
        whitelist_mock.assert();
    }

    #[tokio::test]
    async fn test_execute_alpaca_to_base_api_error() {
        let server = MockServer::start();
        let (_anvil, endpoint, private_key) = setup_anvil();

        let alpaca_broker = Arc::new(create_test_broker_service(&server).await);
        let alpaca_wallet = Arc::new(create_test_wallet_service(&server));
        let wallet = create_test_wallet(&endpoint, &private_key);
        let (cctp_bridge, vault_service) = create_test_onchain_services(wallet).await;
        let cqrs = create_test_store_instance().await;

        let market_maker_wallet = address!("0x1111111111111111111111111111111111111111");

        let manager = CrossVenueCashTransfer::new(
            alpaca_broker,
            alpaca_wallet,
            Arc::new(cctp_bridge),
            Arc::new(vault_service),
            cqrs,
            market_maker_wallet,
            TEST_VAULT_ID,
        );

        // Mock conversion order (conversion happens before withdrawal)
        let _conversion_mock = create_conversion_order_mock(&server, "100");
        let _get_order_mock = create_get_order_mock(
            &server,
            "61e7b016-9c91-4a97-b912-615c9d365c9d",
            "filled",
            "100",
        );

        let whitelist_mock = server.mock(|when, then| {
            when.method(GET)
                .path("/v1/accounts/904837e3-3b76-47ec-b432-046db621571b/wallets/whitelists");
            then.status(500).body("Internal Server Error");
        });

        let id = UsdcRebalanceId(Uuid::new_v4());
        let amount = Usdc(dec!(100));

        assert!(
            matches!(
                manager.execute_alpaca_to_base(&id, amount).await,
                Err(UsdcTransferError::AlpacaWallet(
                    AlpacaWalletError::ApiError {
                        status: StatusCode::INTERNAL_SERVER_ERROR,
                        ..
                    }
                ))
            ),
            "Expected ApiError with INTERNAL_SERVER_ERROR"
        );
        whitelist_mock.assert();
    }

    #[tokio::test]
    async fn test_execute_base_to_alpaca_negative_amount() {
        let server = MockServer::start();
        let (_anvil, endpoint, private_key) = setup_anvil();

        let alpaca_broker = Arc::new(create_test_broker_service(&server).await);
        let alpaca_wallet = Arc::new(create_test_wallet_service(&server));
        let wallet = create_test_wallet(&endpoint, &private_key);
        let (cctp_bridge, vault_service) = create_test_onchain_services(wallet).await;
        let cqrs = create_test_store_instance().await;

        let market_maker_wallet = address!("0x1111111111111111111111111111111111111111");

        let manager = CrossVenueCashTransfer::new(
            alpaca_broker,
            alpaca_wallet,
            Arc::new(cctp_bridge),
            Arc::new(vault_service),
            cqrs,
            market_maker_wallet,
            TEST_VAULT_ID,
        );

        let id = UsdcRebalanceId(Uuid::new_v4());
        let amount = Usdc(dec!(-500));

        let error = manager
            .execute_base_to_alpaca(&id, amount)
            .await
            .unwrap_err();
        assert!(
            matches!(error, UsdcTransferError::NegativeAmount { amount } if amount == Usdc(dec!(-500))),
            "Expected NegativeAmount error, got: {error:?}"
        );
    }

    #[tokio::test]
    async fn test_execute_base_to_alpaca_cctp_burn_fails_with_contract_error() {
        let server = MockServer::start();
        let (_anvil, endpoint, private_key) = setup_anvil();

        let alpaca_broker = Arc::new(create_test_broker_service(&server).await);
        let alpaca_wallet = Arc::new(create_test_wallet_service(&server));
        let wallet = create_test_wallet(&endpoint, &private_key);
        let (cctp_bridge, vault_service) = create_test_onchain_services(wallet).await;
        let cqrs = create_test_store_instance().await;

        let market_maker_wallet = address!("0x1111111111111111111111111111111111111111");

        let manager = CrossVenueCashTransfer::new(
            alpaca_broker,
            alpaca_wallet,
            Arc::new(cctp_bridge),
            Arc::new(vault_service),
            cqrs,
            market_maker_wallet,
            TEST_VAULT_ID,
        );

        let id = UsdcRebalanceId(Uuid::new_v4());
        let amount = Usdc(dec!(1000));

        assert!(
            matches!(
                manager.execute_base_to_alpaca(&id, amount).await,
                Err(UsdcTransferError::Aggregate(AggregateError::UserError(
                    LifecycleError::Apply(UsdcRebalanceError::BridgingNotInitiated)
                )))
            ),
            "Expected Aggregate(UserError(BridgingNotInitiated)) error"
        );
    }

    #[tokio::test]
    async fn test_execute_usd_to_usdc_conversion_places_buy_order() {
        let server = MockServer::start();
        let (_anvil, endpoint, private_key) = setup_anvil();

        let _account_mock = create_broker_account_mock(&server);
        let alpaca_broker = Arc::new(create_test_broker_service(&server).await);
        let alpaca_wallet = Arc::new(create_test_wallet_service(&server));
        let wallet = create_test_wallet(&endpoint, &private_key);
        let (cctp_bridge, vault_service) = create_test_onchain_services(wallet).await;
        let cqrs = create_test_store_instance().await;

        let market_maker_wallet = address!("0x1111111111111111111111111111111111111111");

        let manager = CrossVenueCashTransfer::new(
            alpaca_broker,
            alpaca_wallet,
            Arc::new(cctp_bridge),
            Arc::new(vault_service),
            cqrs,
            market_maker_wallet,
            TEST_VAULT_ID,
        );

        // Mock the conversion order placement (POST)
        let order_mock = create_conversion_order_mock(&server, "1000");
        // Mock the polling endpoint (convert_usdc_usd always polls after placement)
        let _get_mock = create_get_order_mock(
            &server,
            "61e7b016-9c91-4a97-b912-615c9d365c9d",
            "filled",
            "1000",
        );

        let id = UsdcRebalanceId(Uuid::new_v4());
        let amount = Usdc(dec!(1000));

        manager
            .execute_usd_to_usdc_conversion(&id, amount)
            .await
            .unwrap();

        order_mock.assert();
    }

    #[tokio::test]
    async fn test_execute_usdc_to_usd_conversion_requires_deposit_confirmed_state() {
        let server = MockServer::start();
        let (_anvil, endpoint, private_key) = setup_anvil();

        let _account_mock = create_broker_account_mock(&server);
        let alpaca_broker = Arc::new(create_test_broker_service(&server).await);
        let alpaca_wallet = Arc::new(create_test_wallet_service(&server));
        let wallet = create_test_wallet(&endpoint, &private_key);
        let (cctp_bridge, vault_service) = create_test_onchain_services(wallet).await;
        let cqrs = create_test_store_instance().await;

        let market_maker_wallet = address!("0x1111111111111111111111111111111111111111");

        let manager = CrossVenueCashTransfer::new(
            alpaca_broker,
            alpaca_wallet,
            Arc::new(cctp_bridge),
            Arc::new(vault_service),
            cqrs,
            market_maker_wallet,
            TEST_VAULT_ID,
        );

        // Mock the conversion order - will be called but CQRS command will fail
        let _order_mock = create_conversion_order_mock(&server, "500");
        let _get_mock = create_get_order_mock(
            &server,
            "61e7b016-9c91-4a97-b912-615c9d365c9d",
            "filled",
            "500",
        );

        let id = UsdcRebalanceId(Uuid::new_v4());
        let amount = Usdc(dec!(500));

        // execute_usdc_to_usd_conversion requires aggregate to be in DepositConfirmed state
        // (after a BaseToAlpaca deposit completes). With a fresh aggregate, it should fail.
        assert!(
            matches!(
                manager.execute_usdc_to_usd_conversion(&id, amount).await,
                Err(UsdcTransferError::Aggregate(AggregateError::UserError(
                    LifecycleError::Apply(UsdcRebalanceError::DepositNotConfirmed)
                )))
            ),
            "Expected DepositNotConfirmed error when aggregate not in correct state"
        );
    }

    #[tokio::test]
    async fn test_conversion_polls_until_filled() {
        let server = MockServer::start();
        let (_anvil, endpoint, private_key) = setup_anvil();

        let _account_mock = create_broker_account_mock(&server);
        let alpaca_broker = Arc::new(create_test_broker_service(&server).await);
        let alpaca_wallet = Arc::new(create_test_wallet_service(&server));
        let wallet = create_test_wallet(&endpoint, &private_key);
        let (cctp_bridge, vault_service) = create_test_onchain_services(wallet).await;
        let cqrs = create_test_store_instance().await;

        let market_maker_wallet = address!("0x1111111111111111111111111111111111111111");

        let manager = CrossVenueCashTransfer::new(
            alpaca_broker,
            alpaca_wallet,
            Arc::new(cctp_bridge),
            Arc::new(vault_service),
            cqrs,
            market_maker_wallet,
            TEST_VAULT_ID,
        );

        // Order starts as pending
        let _place_mock = create_conversion_order_pending_mock(&server, "1000");

        // First poll returns filled
        let _get_mock_1 = create_get_order_mock(
            &server,
            "61e7b016-9c91-4a97-b912-615c9d365c9d",
            "filled",
            "1000",
        );

        let id = UsdcRebalanceId(Uuid::new_v4());
        let amount = Usdc(dec!(1000));

        manager
            .execute_usd_to_usdc_conversion(&id, amount)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_conversion_fails_on_canceled_order() {
        let server = MockServer::start();
        let (_anvil, endpoint, private_key) = setup_anvil();

        let _account_mock = create_broker_account_mock(&server);
        let alpaca_broker = Arc::new(create_test_broker_service(&server).await);
        let alpaca_wallet = Arc::new(create_test_wallet_service(&server));
        let wallet = create_test_wallet(&endpoint, &private_key);
        let (cctp_bridge, vault_service) = create_test_onchain_services(wallet).await;
        let cqrs = create_test_store_instance().await;

        let market_maker_wallet = address!("0x1111111111111111111111111111111111111111");

        let manager = CrossVenueCashTransfer::new(
            alpaca_broker,
            alpaca_wallet,
            Arc::new(cctp_bridge),
            Arc::new(vault_service),
            cqrs,
            market_maker_wallet,
            TEST_VAULT_ID,
        );

        // Order starts as pending
        let _place_mock = create_conversion_order_pending_mock(&server, "1000");

        // Poll returns canceled
        let _get_mock = create_get_order_mock(
            &server,
            "61e7b016-9c91-4a97-b912-615c9d365c9d",
            "canceled",
            "1000",
        );

        let id = UsdcRebalanceId(Uuid::new_v4());
        let amount = Usdc(dec!(1000));

        assert!(
            matches!(
                manager.execute_usd_to_usdc_conversion(&id, amount).await,
                Err(UsdcTransferError::AlpacaBrokerApi(
                    AlpacaBrokerApiError::CryptoOrderFailed {
                        reason: CryptoOrderFailureReason::Canceled,
                        ..
                    }
                ))
            ),
            "Expected CryptoOrderFailed with Canceled reason"
        );
    }

    #[tokio::test]
    async fn test_conversion_fails_on_rejected_order() {
        let server = MockServer::start();
        let (_anvil, endpoint, private_key) = setup_anvil();

        let _account_mock = create_broker_account_mock(&server);
        let alpaca_broker = Arc::new(create_test_broker_service(&server).await);
        let alpaca_wallet = Arc::new(create_test_wallet_service(&server));
        let wallet = create_test_wallet(&endpoint, &private_key);
        let (cctp_bridge, vault_service) = create_test_onchain_services(wallet).await;
        let cqrs = create_test_store_instance().await;

        let market_maker_wallet = address!("0x1111111111111111111111111111111111111111");

        let id = UsdcRebalanceId(Uuid::new_v4());
        let amount = Usdc(dec!(1000));

        // Set up aggregate in DepositConfirmed state (required for execute_usdc_to_usd_conversion)
        let burn_tx =
            fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000000000001");
        let mint_tx =
            fixed_bytes!("0x1111111111111111111111111111111111111111111111111111111111111111");

        cqrs.send(
            &id,
            UsdcRebalanceCommand::Initiate {
                direction: RebalanceDirection::BaseToAlpaca,
                amount,
                withdrawal: TransferRef::OnchainTx(burn_tx),
            },
        )
        .await
        .unwrap();

        cqrs.send(&id, UsdcRebalanceCommand::ConfirmWithdrawal)
            .await
            .unwrap();

        cqrs.send(&id, UsdcRebalanceCommand::InitiateBridging { burn_tx })
            .await
            .unwrap();

        cqrs.send(
            &id,
            UsdcRebalanceCommand::ReceiveAttestation {
                attestation: vec![0x01],
                cctp_nonce: 12345,
            },
        )
        .await
        .unwrap();

        cqrs.send(
            &id,
            UsdcRebalanceCommand::ConfirmBridging {
                mint_tx,
                amount_received: Usdc(dec!(99.99)),
                fee_collected: Usdc(dec!(0.01)),
            },
        )
        .await
        .unwrap();

        cqrs.send(
            &id,
            UsdcRebalanceCommand::InitiateDeposit {
                deposit: TransferRef::AlpacaId(AlpacaTransferId::from(Uuid::new_v4())),
            },
        )
        .await
        .unwrap();

        cqrs.send(&id, UsdcRebalanceCommand::ConfirmDeposit)
            .await
            .unwrap();

        let manager = CrossVenueCashTransfer::new(
            alpaca_broker,
            alpaca_wallet,
            Arc::new(cctp_bridge),
            Arc::new(vault_service),
            cqrs,
            market_maker_wallet,
            TEST_VAULT_ID,
        );

        // Order starts as pending then gets rejected
        let _place_mock = create_conversion_order_pending_mock(&server, "1000");

        let _get_mock = create_get_order_mock(
            &server,
            "61e7b016-9c91-4a97-b912-615c9d365c9d",
            "rejected",
            "1000",
        );

        assert!(
            matches!(
                manager.execute_usdc_to_usd_conversion(&id, amount).await,
                Err(UsdcTransferError::AlpacaBrokerApi(
                    AlpacaBrokerApiError::CryptoOrderFailed {
                        reason: CryptoOrderFailureReason::Rejected,
                        ..
                    }
                ))
            ),
            "Expected CryptoOrderFailed with Rejected reason"
        );
    }

    #[tokio::test]
    async fn test_usd_to_usdc_conversion_emits_fail_conversion_on_api_error() {
        let server = MockServer::start();
        let (_anvil, endpoint, private_key) = setup_anvil();

        let _account_mock = create_broker_account_mock(&server);
        let alpaca_broker = Arc::new(create_test_broker_service(&server).await);
        let alpaca_wallet = Arc::new(create_test_wallet_service(&server));
        let wallet = create_test_wallet(&endpoint, &private_key);
        let (cctp_bridge, vault_service) = create_test_onchain_services(wallet).await;
        let cqrs = create_test_store_instance().await;

        let market_maker_wallet = address!("0x1111111111111111111111111111111111111111");

        let manager = CrossVenueCashTransfer::new(
            alpaca_broker,
            alpaca_wallet,
            Arc::new(cctp_bridge),
            Arc::new(vault_service),
            Arc::clone(&cqrs),
            market_maker_wallet,
            TEST_VAULT_ID,
        );

        // Mock order placement to fail with 500 error
        let _order_mock = server.mock(|when, then| {
            when.method(POST)
                .path("/v1/trading/accounts/904837e3-3b76-47ec-b432-046db621571b/orders");
            then.status(500).body("Internal Server Error");
        });

        let id = UsdcRebalanceId(Uuid::new_v4());
        let amount = Usdc(dec!(1000));

        manager
            .execute_usd_to_usdc_conversion(&id, amount)
            .await
            .unwrap_err();

        // Verify aggregate is in ConversionFailed state (not uninitialized) by attempting
        // InitiateConversion which should fail because aggregate is no longer uninitialized
        let second_result = cqrs
            .send(
                &id,
                UsdcRebalanceCommand::InitiateConversion {
                    direction: RebalanceDirection::AlpacaToBase,
                    amount,
                    order_id: Uuid::new_v4(),
                },
            )
            .await;

        assert!(
            matches!(
                &second_result,
                Err(AggregateError::UserError(LifecycleError::Apply(
                    UsdcRebalanceError::AlreadyInitiated
                )))
            ),
            "Expected AlreadyInitiated error (aggregate should be in \
             ConversionFailed state), got: {second_result:?}"
        );
    }

    /// AlpacaToBase workflow MUST call USD-to-USDC conversion before
    /// withdrawal.
    ///
    /// Flow: Convert USD to USDC, then Withdraw, Bridge, Deposit
    #[tokio::test]
    async fn alpaca_to_base_calls_usd_to_usdc_conversion() {
        let server = MockServer::start();
        let (_anvil, endpoint, private_key) = setup_anvil();

        let alpaca_broker = Arc::new(create_test_broker_service(&server).await);
        let alpaca_wallet = Arc::new(create_test_wallet_service(&server));
        let wallet = create_test_wallet(&endpoint, &private_key);
        let (cctp_bridge, vault_service) = create_test_onchain_services(wallet).await;
        let cqrs = create_test_store_instance().await;

        let market_maker_wallet = address!("0x1111111111111111111111111111111111111111");

        let manager = CrossVenueCashTransfer::new(
            alpaca_broker,
            alpaca_wallet,
            Arc::new(cctp_bridge),
            Arc::new(vault_service),
            cqrs,
            market_maker_wallet,
            TEST_VAULT_ID,
        );

        // Mock conversion order - MUST be called before withdrawal
        let conversion_mock = server.mock(|when, then| {
            when.method(POST)
                .path("/v1/trading/accounts/904837e3-3b76-47ec-b432-046db621571b/orders")
                .json_body_partial(r#"{"symbol":"USDCUSD"}"#);
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "id": "61e7b016-9c91-4a97-b912-615c9d365c9d",
                    "symbol": "USDCUSD",
                    "qty": "1000",
                    "status": "filled",
                    "filled_avg_price": "1.0001",
                    "filled_qty": "1000",
                    "created_at": "2024-01-15T10:30:00Z"
                }));
        });

        // Mock whitelist - will fail, but conversion should be called FIRST
        let _whitelist_mock = server.mock(|when, then| {
            when.method(GET)
                .path("/v1/accounts/904837e3-3b76-47ec-b432-046db621571b/wallets/whitelists");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([]));
        });

        let id = UsdcRebalanceId(Uuid::new_v4());
        let amount = Usdc(dec!(1000));

        manager
            .execute_alpaca_to_base(&id, amount)
            .await
            .unwrap_err();

        // Conversion MUST be called before withdrawal
        assert!(
            conversion_mock.hits() >= 1,
            "execute_alpaca_to_base MUST call USD-to-USDC conversion \
             before withdrawal"
        );
    }

    /// BaseToAlpaca workflow MUST call USDC-to-USD conversion after
    /// deposit is confirmed.
    ///
    /// Flow: Vault Withdraw, CCTP Bridge, Alpaca Deposit, then Convert
    /// USDC to USD
    #[tokio::test]
    async fn base_to_alpaca_calls_usdc_to_usd_conversion() {
        let server = MockServer::start();
        let (_anvil, endpoint, private_key) = setup_anvil();

        let alpaca_broker = Arc::new(create_test_broker_service(&server).await);
        let alpaca_wallet = Arc::new(create_test_wallet_service(&server));
        let wallet = create_test_wallet(&endpoint, &private_key);
        let (cctp_bridge, vault_service) = create_test_onchain_services(wallet).await;
        let cqrs = create_test_store_instance().await;

        let market_maker_wallet = address!("0x1111111111111111111111111111111111111111");

        let manager = CrossVenueCashTransfer::new(
            alpaca_broker,
            alpaca_wallet,
            Arc::new(cctp_bridge),
            Arc::new(vault_service),
            Arc::clone(&cqrs),
            market_maker_wallet,
            TEST_VAULT_ID,
        );

        let conversion_mock = server.mock(|when, then| {
            when.method(POST)
                .path("/v1/trading/accounts/904837e3-3b76-47ec-b432-046db621571b/orders")
                .json_body_partial(r#"{"symbol":"USDCUSD"}"#);
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "id": "61e7b016-9c91-4a97-b912-615c9d365c9d",
                    "symbol": "USDCUSD",
                    "qty": "1000",
                    "status": "filled",
                    "side": "sell",
                    "filled_avg_price": "0.9999",
                    "filled_qty": "1000",
                    "created_at": "2024-01-15T10:30:00Z"
                }));
        });

        let _get_order_mock = create_get_order_mock(
            &server,
            "61e7b016-9c91-4a97-b912-615c9d365c9d",
            "filled",
            "1000",
        );

        let id = UsdcRebalanceId(Uuid::new_v4());
        let amount = Usdc(dec!(1000));

        advance_to_deposit_confirmed_base_to_alpaca(&cqrs, &id, amount).await;

        manager
            .execute_usdc_to_usd_conversion(&id, amount)
            .await
            .unwrap();

        assert!(
            conversion_mock.hits() >= 1,
            "execute_base_to_alpaca MUST call USDC-to-USD conversion \
             after deposit confirmation"
        );
    }

    #[tokio::test]
    async fn test_conversion_fails_on_expired_order() {
        let server = MockServer::start();
        let (_anvil, endpoint, private_key) = setup_anvil();

        let _account_mock = create_broker_account_mock(&server);
        let alpaca_broker = Arc::new(create_test_broker_service(&server).await);
        let alpaca_wallet = Arc::new(create_test_wallet_service(&server));
        let wallet = create_test_wallet(&endpoint, &private_key);
        let (cctp_bridge, vault_service) = create_test_onchain_services(wallet).await;
        let cqrs = create_test_store_instance().await;

        let market_maker_wallet = address!("0x1111111111111111111111111111111111111111");

        let manager = CrossVenueCashTransfer::new(
            alpaca_broker,
            alpaca_wallet,
            Arc::new(cctp_bridge),
            Arc::new(vault_service),
            cqrs,
            market_maker_wallet,
            TEST_VAULT_ID,
        );

        // Order starts as pending
        let _place_mock = create_conversion_order_pending_mock(&server, "1000");

        // Poll returns expired
        let _get_mock = server.mock(|when, then| {
            when.method(GET).path(
                "/v1/trading/accounts/904837e3-3b76-47ec-b432-046db621571b/orders/61e7b016-9c91-4a97-b912-615c9d365c9d"
            );
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "id": "61e7b016-9c91-4a97-b912-615c9d365c9d",
                    "symbol": "USDCUSD",
                    "qty": "1000",
                    "status": "expired",
                    "created_at": "2024-01-15T10:30:00Z"
                }));
        });

        let id = UsdcRebalanceId(Uuid::new_v4());
        let amount = Usdc(dec!(1000));

        assert!(
            matches!(
                manager.execute_usd_to_usdc_conversion(&id, amount).await,
                Err(UsdcTransferError::AlpacaBrokerApi(
                    AlpacaBrokerApiError::CryptoOrderFailed {
                        reason: CryptoOrderFailureReason::Expired,
                        ..
                    }
                ))
            ),
            "Expected CryptoOrderFailed with Expired reason"
        );
    }

    #[tokio::test]
    async fn initiate_conversion_failure_prevents_order_placement() {
        let server = MockServer::start();
        let (_anvil, endpoint, private_key) = setup_anvil();

        let _account_mock = create_broker_account_mock(&server);
        let alpaca_broker = Arc::new(create_test_broker_service(&server).await);
        let alpaca_wallet = Arc::new(create_test_wallet_service(&server));
        let wallet = create_test_wallet(&endpoint, &private_key);
        let (cctp_bridge, vault_service) = create_test_onchain_services(wallet).await;
        let cqrs = create_test_store_instance().await;

        let market_maker_wallet = address!("0x1111111111111111111111111111111111111111");
        let id = UsdcRebalanceId(Uuid::new_v4());
        let amount = Usdc(dec!(1000));

        // Pre-initialize aggregate to make InitiateConversion fail
        cqrs.send(
            &id,
            UsdcRebalanceCommand::InitiateConversion {
                direction: RebalanceDirection::AlpacaToBase,
                amount,
                order_id: Uuid::new_v4(),
            },
        )
        .await
        .unwrap();

        let manager = CrossVenueCashTransfer::new(
            alpaca_broker,
            alpaca_wallet,
            Arc::new(cctp_bridge),
            Arc::new(vault_service),
            cqrs,
            market_maker_wallet,
            TEST_VAULT_ID,
        );

        // Mock that should NOT be called - if InitiateConversion fails, no order should be placed
        let order_mock = server.mock(|when, then| {
            when.method(POST)
                .path("/v1/trading/accounts/904837e3-3b76-47ec-b432-046db621571b/orders");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "id": "should-not-be-called",
                    "symbol": "USDCUSD",
                    "qty": "1000",
                    "status": "filled",
                    "created_at": "2024-01-15T10:30:00Z"
                }));
        });

        // Second call should fail because aggregate is already initialized
        assert!(
            matches!(
                manager.execute_usd_to_usdc_conversion(&id, amount).await,
                Err(UsdcTransferError::Aggregate(AggregateError::UserError(
                    LifecycleError::Apply(UsdcRebalanceError::AlreadyInitiated)
                )))
            ),
            "Expected AlreadyInitiated error"
        );

        // Verify no order was placed - CQRS failure should prevent side effects
        assert_eq!(
            order_mock.hits(),
            0,
            "Order API should not be called when InitiateConversion fails"
        );
    }

    #[tokio::test]
    async fn aggregate_reaches_conversion_failed_state_on_order_failure() {
        let server = MockServer::start();
        let (_anvil, endpoint, private_key) = setup_anvil();

        let _account_mock = create_broker_account_mock(&server);
        let alpaca_broker = Arc::new(create_test_broker_service(&server).await);
        let alpaca_wallet = Arc::new(create_test_wallet_service(&server));
        let wallet = create_test_wallet(&endpoint, &private_key);
        let (cctp_bridge, vault_service) = create_test_onchain_services(wallet).await;
        let cqrs = create_test_store_instance().await;

        let market_maker_wallet = address!("0x1111111111111111111111111111111111111111");
        let id = UsdcRebalanceId(Uuid::new_v4());
        let amount = Usdc(dec!(1000));

        let manager = CrossVenueCashTransfer::new(
            alpaca_broker,
            alpaca_wallet,
            Arc::new(cctp_bridge),
            Arc::new(vault_service),
            Arc::clone(&cqrs),
            market_maker_wallet,
            TEST_VAULT_ID,
        );

        // Mock order placement to fail with API error
        let _order_mock = server.mock(|when, then| {
            when.method(POST)
                .path("/v1/trading/accounts/904837e3-3b76-47ec-b432-046db621571b/orders");
            then.status(500);
        });

        manager
            .execute_usd_to_usdc_conversion(&id, amount)
            .await
            .unwrap_err();

        // Verify aggregate is in ConversionFailed state by attempting InitiateConversion
        // which should fail with AlreadyInitiated (not succeed with Uninitialized)
        let reinit_result = cqrs
            .send(
                &id,
                UsdcRebalanceCommand::InitiateConversion {
                    direction: RebalanceDirection::AlpacaToBase,
                    amount,
                    order_id: Uuid::new_v4(),
                },
            )
            .await;

        assert!(
            matches!(
                &reinit_result,
                Err(AggregateError::UserError(LifecycleError::Apply(
                    UsdcRebalanceError::AlreadyInitiated
                )))
            ),
            "Aggregate should be in ConversionFailed (not Uninitialized), \
             got: {reinit_result:?}"
        );
    }

    #[tokio::test]
    async fn aggregate_reaches_conversion_complete_state_on_success() {
        let server = MockServer::start();
        let (_anvil, endpoint, private_key) = setup_anvil();

        let _account_mock = create_broker_account_mock(&server);
        let alpaca_broker = Arc::new(create_test_broker_service(&server).await);
        let alpaca_wallet = Arc::new(create_test_wallet_service(&server));
        let wallet = create_test_wallet(&endpoint, &private_key);
        let (cctp_bridge, vault_service) = create_test_onchain_services(wallet).await;
        let cqrs = create_test_store_instance().await;

        let market_maker_wallet = address!("0x1111111111111111111111111111111111111111");
        let id = UsdcRebalanceId(Uuid::new_v4());
        let amount = Usdc(dec!(1000));

        let manager = CrossVenueCashTransfer::new(
            alpaca_broker,
            alpaca_wallet,
            Arc::new(cctp_bridge),
            Arc::new(vault_service),
            Arc::clone(&cqrs),
            market_maker_wallet,
            TEST_VAULT_ID,
        );

        let _order_mock = create_conversion_order_mock(&server, "1000");
        let _get_mock = create_get_order_mock(
            &server,
            "61e7b016-9c91-4a97-b912-615c9d365c9d",
            "filled",
            "1000",
        );

        manager
            .execute_usd_to_usdc_conversion(&id, amount)
            .await
            .unwrap();

        // Verify aggregate is in ConversionComplete state by attempting to start withdrawal
        // which should succeed from ConversionComplete but fail from other states
        let withdrawal_result = cqrs
            .send(
                &id,
                UsdcRebalanceCommand::Initiate {
                    direction: RebalanceDirection::AlpacaToBase,
                    amount,
                    withdrawal: TransferRef::AlpacaId(AlpacaTransferId::from(Uuid::new_v4())),
                },
            )
            .await;

        assert!(
            withdrawal_result.is_ok(),
            "Initiate should succeed from ConversionComplete state, \
             got: {withdrawal_result:?}"
        );
    }

    #[tokio::test]
    async fn usd_to_usdc_conversion_returns_actual_filled_amount() {
        let server = MockServer::start();
        let (_anvil, endpoint, private_key) = setup_anvil();

        let _account_mock = create_broker_account_mock(&server);
        let alpaca_broker = Arc::new(create_test_broker_service(&server).await);
        let alpaca_wallet = Arc::new(create_test_wallet_service(&server));
        let wallet = create_test_wallet(&endpoint, &private_key);
        let (cctp_bridge, vault_service) = create_test_onchain_services(wallet).await;
        let cqrs = create_test_store_instance().await;

        let market_maker_wallet = address!("0x1111111111111111111111111111111111111111");

        let manager = CrossVenueCashTransfer::new(
            alpaca_broker,
            alpaca_wallet,
            Arc::new(cctp_bridge),
            Arc::new(vault_service),
            cqrs,
            market_maker_wallet,
            TEST_VAULT_ID,
        );

        // Request 1000, but only 999.5 fills due to slippage
        let _order_mock = create_conversion_order_pending_mock(&server, "1000");
        let _get_mock = create_get_order_mock_with_slippage(
            &server,
            "61e7b016-9c91-4a97-b912-615c9d365c9d",
            "1000",
            "999.5",
        );

        let id = UsdcRebalanceId(Uuid::new_v4());
        let requested_amount = Usdc(dec!(1000));

        let filled_amount = manager
            .execute_usd_to_usdc_conversion(&id, requested_amount)
            .await
            .unwrap();

        // Should return the actual filled amount, not the requested amount
        assert_eq!(
            filled_amount,
            Usdc(dec!(999.5)),
            "Should return actual filled amount, not requested amount"
        );
    }
}

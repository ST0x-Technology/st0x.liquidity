//! [`CrossVenueCashTransfer`] orchestrates USDC cross-venue transfers.
//!
//! Coordinates between `AlpacaBrokerApi`, `AlpacaWalletService`,
//! `CctpBridge`, `RaindexService`, and the `UsdcRebalance` aggregate to
//! execute USDC transfers between Alpaca and Base.

use alloy::primitives::{Address, TxHash, U256};
use std::sync::Arc;
use tracing::{info, instrument, warn};
use uuid::Uuid;

use rain_math_float::Float;
use st0x_bridge::cctp::{AttestationResponse, CctpBridge};
use st0x_bridge::{Attestation, Bridge, BridgeDirection, BurnReceipt, MintReceipt};
use st0x_event_sorcery::Store;
use st0x_evm::Wallet;
use st0x_execution::{AlpacaBrokerApi, ConversionDirection, CryptoOrderOutcome, Positive};
use st0x_finance::Usdc;

use super::UsdcTransferError;
use crate::alpaca_wallet::{
    AlpacaTransferId, AlpacaWalletService, TokenSymbol, Transfer, TransferStatus,
};
use crate::onchain::raindex::{Raindex, RaindexService, RaindexVaultId, USDC_BASE};
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
    #[instrument(target = "rebalance", skip(self), fields(%id, %amount), level = tracing::Level::DEBUG)]
    pub(crate) async fn execute_usd_to_usdc_conversion(
        &self,
        id: &UsdcRebalanceId,
        amount: Usdc,
    ) -> Result<Usdc, UsdcTransferError> {
        let correlation_id = Uuid::new_v4();

        info!(target: "rebalance", %amount, %correlation_id, "Starting USD to USDC conversion");

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

        let alpaca_amount = amount.inner();

        let order = match self
            .alpaca_broker
            .convert_usdc_usd(
                alpaca_amount,
                ConversionDirection::UsdToUsdc,
                correlation_id,
            )
            .await
        {
            Ok(order) => order,
            Err(error) => {
                warn!(target: "rebalance", "USD to USDC conversion failed: {error}");
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
        let filled_amount = Usdc::new(filled_qty);

        self.cqrs
            .send(
                id,
                UsdcRebalanceCommand::ConfirmConversion { filled_amount },
            )
            .await?;

        info!(target: "rebalance",
            order_id = %order.id,
            requested = %amount,
            filled = %filled_amount,
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
    #[instrument(target = "rebalance", skip(self), fields(%id, %amount), level = tracing::Level::DEBUG)]
    pub(crate) async fn execute_usdc_to_usd_conversion(
        &self,
        id: &UsdcRebalanceId,
        amount: Usdc,
    ) -> Result<Usdc, UsdcTransferError> {
        let correlation_id = Uuid::new_v4();

        info!(target: "rebalance", %amount, %correlation_id, "Starting USDC to USD conversion");

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

        let alpaca_amount = amount.inner();

        let order = match self
            .alpaca_broker
            .convert_usdc_usd(
                alpaca_amount,
                ConversionDirection::UsdcToUsd,
                correlation_id,
            )
            .await
        {
            Ok(order) => order,
            Err(error) => {
                warn!(target: "rebalance", "USDC to USD conversion failed: {error}");
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
        let filled_usdc = Usdc::new(filled_amount);

        self.cqrs
            .send(
                id,
                UsdcRebalanceCommand::ConfirmConversion {
                    filled_amount: filled_usdc,
                },
            )
            .await?;

        info!(target: "rebalance",
            order_id = %order.id,
            requested = %amount,
            filled = %filled_usdc,
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
    #[instrument(target = "rebalance", skip(self), fields(%id, %amount), level = tracing::Level::DEBUG)]
    pub(crate) async fn resume_alpaca_to_base(
        &self,
        id: &UsdcRebalanceId,
        amount: Usdc,
    ) -> Result<(), UsdcTransferError> {
        let state = self.cqrs.load(id).await?;

        info!(
            target: "rebalance",
            ?state,
            "Resuming Alpaca->Base transfer from aggregate state",
        );

        match state {
            None => self.execute_alpaca_to_base(id, amount).await,

            Some(UsdcRebalance::Converting {
                direction: RebalanceDirection::AlpacaToBase,
                ..
            }) => {
                self.cqrs
                    .send(
                        id,
                        UsdcRebalanceCommand::FailConversion {
                            reason: "resume from Converting state requires manual \
                                     reconciliation: broker order ID not persisted"
                                .into(),
                        },
                    )
                    .await?;
                Err(UsdcTransferError::ResumeIndeterminateConversion { id: id.clone() })
            }

            Some(UsdcRebalance::ConversionComplete {
                direction: RebalanceDirection::AlpacaToBase,
                filled_amount,
                ..
            }) => {
                self.continue_alpaca_to_base_from_conversion_complete(id, filled_amount)
                    .await
            }

            Some(UsdcRebalance::Withdrawing {
                direction: RebalanceDirection::AlpacaToBase,
                amount,
                withdrawal_ref,
                ..
            }) => {
                let TransferRef::AlpacaId(transfer_id) = withdrawal_ref else {
                    return Err(UsdcTransferError::WithdrawalRefMustBeAlpacaId { id: id.clone() });
                };

                self.poll_and_confirm_withdrawal(id, &transfer_id).await?;
                self.continue_alpaca_to_base_from_withdrawal_complete(id, amount)
                    .await
            }

            Some(UsdcRebalance::WithdrawalComplete {
                direction: RebalanceDirection::AlpacaToBase,
                amount,
                ..
            }) => {
                self.continue_alpaca_to_base_from_withdrawal_complete(id, amount)
                    .await
            }

            Some(
                UsdcRebalance::Bridging {
                    direction: RebalanceDirection::AlpacaToBase,
                    amount,
                    burn_tx_hash,
                    ..
                }
                | UsdcRebalance::Attested {
                    direction: RebalanceDirection::AlpacaToBase,
                    amount,
                    burn_tx_hash,
                    ..
                },
            ) => {
                self.continue_alpaca_to_base_from_bridging(id, amount, burn_tx_hash)
                    .await
            }

            Some(UsdcRebalance::Bridged {
                direction: RebalanceDirection::AlpacaToBase,
                amount_received,
                ..
            }) => {
                self.continue_alpaca_to_base_from_bridged(id, amount_received)
                    .await
            }

            Some(UsdcRebalance::DepositInitiated {
                direction: RebalanceDirection::AlpacaToBase,
                deposit_ref,
                ..
            }) => {
                // Re-verify the persisted deposit tx on chain before
                // transitioning to `DepositConfirmed`. The aggregate
                // records the deposit hash at `InitiateDeposit` time,
                // but a process restart between that event and the
                // confirmation can land on a chain where the tx has
                // been reorged out, dropped, or never mined. Trusting
                // the persisted state alone would mark the rebalance
                // complete while the funds aren't actually deposited.
                let TransferRef::OnchainTx(deposit_tx) = deposit_ref else {
                    return Err(UsdcTransferError::DepositRefMustBeOnchain { id: id.clone() });
                };
                self.raindex.confirm_tx(deposit_tx).await?;
                self.confirm_deposit(id).await
            }

            Some(UsdcRebalance::DepositConfirmed {
                direction: RebalanceDirection::AlpacaToBase,
                ..
            }) => Ok(()),

            Some(
                UsdcRebalance::Converting {
                    direction: RebalanceDirection::BaseToAlpaca,
                    ..
                }
                | UsdcRebalance::ConversionComplete {
                    direction: RebalanceDirection::BaseToAlpaca,
                    ..
                }
                | UsdcRebalance::Withdrawing {
                    direction: RebalanceDirection::BaseToAlpaca,
                    ..
                }
                | UsdcRebalance::WithdrawalComplete {
                    direction: RebalanceDirection::BaseToAlpaca,
                    ..
                }
                | UsdcRebalance::Bridging {
                    direction: RebalanceDirection::BaseToAlpaca,
                    ..
                }
                | UsdcRebalance::Attested {
                    direction: RebalanceDirection::BaseToAlpaca,
                    ..
                }
                | UsdcRebalance::Bridged {
                    direction: RebalanceDirection::BaseToAlpaca,
                    ..
                }
                | UsdcRebalance::DepositInitiated {
                    direction: RebalanceDirection::BaseToAlpaca,
                    ..
                }
                | UsdcRebalance::DepositConfirmed {
                    direction: RebalanceDirection::BaseToAlpaca,
                    ..
                },
            ) => Err(UsdcTransferError::ResumeDirectionMismatch {
                id: id.clone(),
                direction: RebalanceDirection::BaseToAlpaca,
            }),

            // `WithdrawalSubmitting`/`BridgingSubmitting` are crash-safe
            // submission states produced only by the Base->Alpaca flow (on-chain
            // vault withdrawal and burn-on-Base). The Alpaca->Base flow withdraws
            // off-chain and burns via `InitiateBridging` straight to `Bridging`,
            // so it never enters these states -- reaching one here means the
            // aggregate belongs to the other direction.
            Some(
                UsdcRebalance::WithdrawalSubmitting { direction, .. }
                | UsdcRebalance::BridgingSubmitting { direction, .. },
            ) => Err(UsdcTransferError::ResumeDirectionMismatch {
                id: id.clone(),
                direction,
            }),

            Some(
                UsdcRebalance::WithdrawalFailed { .. }
                | UsdcRebalance::BridgingFailed { .. }
                | UsdcRebalance::DepositFailed { .. }
                | UsdcRebalance::ConversionFailed { .. },
            ) => Err(UsdcTransferError::PreviouslyFailedAggregate { id: id.clone() }),
        }
    }

    /// Drives an Alpaca->Base transfer from `ConversionComplete` through to
    /// terminal: withdraw -> burn -> attestation -> mint -> vault deposit.
    async fn continue_alpaca_to_base_from_conversion_complete(
        &self,
        id: &UsdcRebalanceId,
        filled_amount: Usdc,
    ) -> Result<(), UsdcTransferError> {
        let transfer = self.initiate_alpaca_withdrawal(id, filled_amount).await?;

        self.poll_and_confirm_withdrawal(id, &transfer.id).await?;
        self.continue_alpaca_to_base_from_withdrawal_complete(id, filled_amount)
            .await
    }

    /// Drives an Alpaca->Base transfer from `WithdrawalComplete` through to
    /// terminal: burn -> attestation -> mint -> vault deposit.
    async fn continue_alpaca_to_base_from_withdrawal_complete(
        &self,
        id: &UsdcRebalanceId,
        amount: Usdc,
    ) -> Result<(), UsdcTransferError> {
        let burn_amount = match usdc_to_u256(amount) {
            Ok(burn_amount) => burn_amount,
            Err(error) => {
                warn!(target: "rebalance", %error, "USDC to U256 conversion failed after withdrawal");
                self.cqrs
                    .send(
                        id,
                        UsdcRebalanceCommand::FailBridging {
                            reason: format!("USDC conversion failed: {error}"),
                        },
                    )
                    .await?;
                return Err(error);
            }
        };

        let burn_receipt = self.execute_cctp_burn(id, burn_amount).await?;

        self.continue_alpaca_to_base_from_bridging(id, amount, burn_receipt.tx)
            .await
    }

    /// Drives an Alpaca->Base transfer from `Bridging`/`Attested` through to
    /// terminal. Re-polls the Circle attestation (idempotent for completed
    /// attestations) so we obtain a fresh [`AttestationResponse`] suitable
    /// for [`Bridge::mint`].
    async fn continue_alpaca_to_base_from_bridging(
        &self,
        id: &UsdcRebalanceId,
        amount: Usdc,
        burn_tx_hash: TxHash,
    ) -> Result<(), UsdcTransferError> {
        let burn_receipt = BurnReceipt {
            tx: burn_tx_hash,
            amount: usdc_to_u256(amount)?,
        };

        let attestation_response = self.poll_attestation(id, &burn_receipt).await?;
        let mint_receipt = self.execute_cctp_mint(id, attestation_response).await?;

        self.continue_alpaca_to_base_from_bridged(id, u256_to_usdc(mint_receipt.amount)?)
            .await
    }

    /// Drives an Alpaca->Base transfer from `Bridged` to terminal: vault
    /// deposit + `ConfirmDeposit`.
    async fn continue_alpaca_to_base_from_bridged(
        &self,
        id: &UsdcRebalanceId,
        amount_received: Usdc,
    ) -> Result<(), UsdcTransferError> {
        let amount_u256 = usdc_to_u256(amount_received)?;

        self.deposit_to_vault(id, amount_u256).await?;
        self.confirm_deposit(id).await?;

        Ok(())
    }

    pub(crate) async fn execute_alpaca_to_base(
        &self,
        id: &UsdcRebalanceId,
        amount: Usdc,
    ) -> Result<(), UsdcTransferError> {
        info!(target: "rebalance", %amount, "Starting Alpaca to Base rebalance");

        // Convert USD to USDC - use actual filled amount for subsequent steps
        let usdc_amount = self.execute_usd_to_usdc_conversion(id, amount).await?;

        let transfer = self.initiate_alpaca_withdrawal(id, usdc_amount).await?;

        self.poll_and_confirm_withdrawal(id, &transfer.id).await?;

        let burn_amount = match usdc_to_u256(usdc_amount) {
            Ok(amount) => amount,
            Err(error) => {
                warn!(target: "rebalance", %error, "USDC to U256 conversion failed after withdrawal");
                self.cqrs
                    .send(
                        id,
                        UsdcRebalanceCommand::FailBridging {
                            reason: format!("USDC conversion failed: {error}"),
                        },
                    )
                    .await?;
                return Err(error);
            }
        };
        let burn_receipt = self.execute_cctp_burn(id, burn_amount).await?;

        let attestation_response = self.poll_attestation(id, &burn_receipt).await?;

        // Use the actual minted amount (net of CCTP fee) for vault deposit
        let mint_receipt = self.execute_cctp_mint(id, attestation_response).await?;

        self.deposit_to_vault(id, mint_receipt.amount).await?;

        self.confirm_deposit(id).await?;

        info!(target: "rebalance", "Alpaca to Base rebalance completed successfully");
        Ok(())
    }

    #[instrument(target = "rebalance", skip(self), fields(%id, %amount), level = tracing::Level::DEBUG)]
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
                warn!(target: "rebalance", "Alpaca withdrawal initiation failed: {error}");
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

        info!(target: "rebalance", transfer_id = %transfer.id, "Alpaca withdrawal initiated");
        Ok(transfer)
    }

    #[instrument(target = "rebalance", skip(self), fields(%id, %transfer_id), level = tracing::Level::DEBUG)]
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
                warn!(target: "rebalance", "Alpaca withdrawal polling failed: {error}");
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

        info!(target: "rebalance", "Alpaca withdrawal confirmed");
        Ok(())
    }

    #[instrument(target = "rebalance", skip(self), fields(%id, %amount), level = tracing::Level::DEBUG)]
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
                warn!(target: "rebalance", "CCTP burn failed: {error}");
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

        info!(target: "rebalance", burn_tx = %burn_receipt.tx, "CCTP burn executed");
        Ok(burn_receipt)
    }

    #[instrument(target = "rebalance", skip(self, burn_receipt), fields(%id, burn_tx = %burn_receipt.tx), level = tracing::Level::DEBUG)]
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
                warn!(target: "rebalance", "Attestation polling failed: {error}");
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

        // Capture the destination (Base) head before minting so a crash before
        // `ConfirmBridging` resumes by scanning for the already-submitted mint
        // instead of re-minting, which reverts on the already-used CCTP nonce.
        // AlpacaToBase has no resume entry point, so a failure here records
        // `FailBridging` rather than stranding the aggregate in `Bridging`.
        let mint_scan_from_block = match self
            .cctp_bridge
            .destination_block(BridgeDirection::EthereumToBase)
            .await
        {
            Ok(block) => block,
            Err(error) => {
                warn!(target: "rebalance", "Destination head lookup failed: {error}");
                self.cqrs
                    .send(
                        id,
                        UsdcRebalanceCommand::FailBridging {
                            reason: format!("Destination block lookup failed: {error}"),
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
                    mint_scan_from_block,
                },
            )
            .await?;

        info!(target: "rebalance", "Circle attestation received");
        Ok(response)
    }

    #[instrument(target = "rebalance", skip(self, attestation_response), fields(%id), level = tracing::Level::DEBUG)]
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
                warn!(target: "rebalance", "CCTP mint failed: {error}");
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

        info!(target: "rebalance",
            mint_tx = %mint_receipt.tx,
            amount = %mint_receipt.amount,
            fee = %mint_receipt.fee,
            "CCTP mint executed"
        );
        Ok(mint_receipt)
    }

    #[instrument(target = "rebalance", skip(self), fields(%id, %amount), level = tracing::Level::DEBUG)]
    async fn deposit_to_vault(
        &self,
        id: &UsdcRebalanceId,
        amount: U256,
    ) -> Result<(), UsdcTransferError> {
        let deposit_tx = match self.raindex.deposit_usdc(self.vault_id, amount).await {
            Ok(tx) => tx,
            Err(error) => {
                warn!(target: "rebalance", "Vault deposit failed: {error}");
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

        info!(target: "rebalance", %deposit_tx, "Vault deposit initiated");
        Ok(())
    }

    #[instrument(target = "rebalance", skip(self), fields(%id), level = tracing::Level::DEBUG)]
    async fn confirm_deposit(&self, id: &UsdcRebalanceId) -> Result<(), UsdcTransferError> {
        self.cqrs
            .send(id, UsdcRebalanceCommand::ConfirmDeposit)
            .await?;

        info!(target: "rebalance", "Vault deposit confirmed");
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
    #[instrument(target = "rebalance", skip(self), fields(%id, %amount), level = tracing::Level::DEBUG)]
    pub(crate) async fn execute_base_to_alpaca(
        &self,
        id: &UsdcRebalanceId,
        amount: Usdc,
    ) -> Result<(), UsdcTransferError> {
        info!(target: "rebalance", %amount, "Starting Base to Alpaca rebalance");

        let amount_u256 = usdc_to_u256(amount)?;

        self.withdraw_from_vault(id, amount, amount_u256).await?;

        let burn_receipt = self.execute_cctp_burn_on_base(id, amount_u256).await?;

        // From Bridging onward the fresh-start and resume paths are identical:
        // poll Circle, record the attestation, mint, deposit, and convert.
        self.continue_from_bridging(id, amount, burn_receipt.tx)
            .await?;

        info!(target: "rebalance", "Base to Alpaca rebalance completed successfully");
        Ok(())
    }

    /// Non-obvious points the match body below cannot express on its own:
    /// - `Attested` re-polls rather than reconstructing an `AttestationResponse`
    ///   because the aggregate stores attestation bytes + nonce but not the
    ///   full message envelope [`Bridge::mint`] needs; Circle's API is
    ///   idempotent for completed attestations so the cost is negligible.
    /// - `Converting` is unrecoverable: the aggregate persists the correlation
    ///   UUID but not the broker order ID, so a polling-based resume cannot
    ///   identify the order. Operator reconciles manually.
    /// - `Withdrawing` only happens on a crash between `Initiate` and
    ///   `ConfirmWithdrawal`; the vault withdrawal is synchronous (waits for
    ///   block inclusion before `Initiate`), so `ConfirmWithdrawal` is always
    ///   safe to send.
    #[instrument(target = "rebalance", skip(self), fields(%id, %amount), level = tracing::Level::DEBUG)]
    pub(crate) async fn resume_base_to_alpaca(
        &self,
        id: &UsdcRebalanceId,
        amount: Usdc,
    ) -> Result<(), UsdcTransferError> {
        let state = self.cqrs.load(id).await?;

        info!(
            target: "rebalance",
            ?state,
            "Resuming Base->Alpaca transfer from aggregate state",
        );

        match state {
            None => self.execute_base_to_alpaca(id, amount).await,

            Some(UsdcRebalance::WithdrawalSubmitting {
                direction,
                amount,
                from_block,
                ..
            }) => {
                Self::require_base_to_alpaca(id, &direction)?;
                let amount_u256 = usdc_to_u256(amount)?;
                self.resume_withdrawal_submitting(id, amount, amount_u256, from_block)
                    .await?;
                self.continue_from_withdrawal_complete(id, amount).await
            }

            Some(UsdcRebalance::Withdrawing {
                direction, amount, ..
            }) => {
                Self::require_base_to_alpaca(id, &direction)?;
                self.cqrs
                    .send(id, UsdcRebalanceCommand::ConfirmWithdrawal)
                    .await?;
                self.continue_from_withdrawal_complete(id, amount).await
            }

            Some(UsdcRebalance::WithdrawalComplete {
                direction, amount, ..
            }) => {
                Self::require_base_to_alpaca(id, &direction)?;
                self.continue_from_withdrawal_complete(id, amount).await
            }

            Some(UsdcRebalance::BridgingSubmitting {
                direction,
                amount,
                from_block,
                ..
            }) => {
                Self::require_base_to_alpaca(id, &direction)?;
                let amount_u256 = usdc_to_u256(amount)?;
                let burn_receipt = self
                    .resume_bridging_submitting(id, amount_u256, from_block)
                    .await?;
                self.continue_from_bridging(id, amount, burn_receipt.tx)
                    .await
            }

            Some(UsdcRebalance::Bridging {
                direction,
                amount,
                burn_tx_hash,
                ..
            }) => {
                Self::require_base_to_alpaca(id, &direction)?;
                self.continue_from_bridging(id, amount, burn_tx_hash).await
            }

            Some(UsdcRebalance::Attested {
                direction,
                amount,
                burn_tx_hash,
                mint_scan_from_block,
                ..
            }) => {
                Self::require_base_to_alpaca(id, &direction)?;
                self.continue_from_attested(id, amount, burn_tx_hash, mint_scan_from_block)
                    .await
            }

            Some(UsdcRebalance::Bridged {
                direction,
                amount_received,
                mint_tx_hash,
                ..
            }) => {
                Self::require_base_to_alpaca(id, &direction)?;
                self.continue_from_bridged(id, amount_received, mint_tx_hash)
                    .await
            }

            Some(UsdcRebalance::DepositInitiated {
                direction,
                amount,
                deposit_ref,
                ..
            }) => {
                Self::require_base_to_alpaca(id, &direction)?;
                let TransferRef::OnchainTx(mint_tx) = deposit_ref else {
                    return Err(UsdcTransferError::DepositRefMustBeOnchain { id: id.clone() });
                };
                self.poll_alpaca_deposit_and_confirm(id, mint_tx).await?;
                self.execute_usdc_to_usd_conversion(id, amount).await?;
                Ok(())
            }

            Some(UsdcRebalance::DepositConfirmed {
                direction: RebalanceDirection::BaseToAlpaca,
                amount,
                ..
            }) => {
                self.execute_usdc_to_usd_conversion(id, amount).await?;
                Ok(())
            }

            Some(UsdcRebalance::DepositConfirmed {
                direction: RebalanceDirection::AlpacaToBase,
                ..
            }) => {
                warn!(
                    target: "rebalance",
                    %id,
                    "resume_base_to_alpaca called on AlpacaToBase DepositConfirmed; \
                     treating as no-op terminal",
                );
                Ok(())
            }

            Some(UsdcRebalance::Converting {
                direction,
                order_id,
                ..
            }) => {
                Self::require_base_to_alpaca(id, &direction)?;
                self.resume_converting(id, order_id).await
            }

            Some(UsdcRebalance::ConversionComplete { direction, .. }) => {
                Self::require_base_to_alpaca(id, &direction)?;
                Ok(())
            }

            Some(
                UsdcRebalance::WithdrawalFailed { .. }
                | UsdcRebalance::BridgingFailed { .. }
                | UsdcRebalance::DepositFailed { .. }
                | UsdcRebalance::ConversionFailed { .. },
            ) => Err(UsdcTransferError::PreviouslyFailedAggregate { id: id.clone() }),
        }
    }

    fn require_base_to_alpaca(
        id: &UsdcRebalanceId,
        direction: &RebalanceDirection,
    ) -> Result<(), UsdcTransferError> {
        if matches!(direction, RebalanceDirection::BaseToAlpaca) {
            Ok(())
        } else {
            Err(UsdcTransferError::ResumeDirectionMismatch {
                id: id.clone(),
                direction: direction.clone(),
            })
        }
    }

    /// Resumes a transfer stalled at `Converting` (the post-deposit USDC->USD
    /// conversion). The conversion's correlation id is the Alpaca
    /// `client_order_id`, recorded before the order was placed, so resume looks
    /// the order up and resolves deterministically instead of blindly failing:
    /// a filled order confirms the conversion, a terminally-failed order fails
    /// it, a still-settling order is retried, and an order that never reached
    /// Alpaca (crash before placement) fails for operator reconciliation.
    async fn resume_converting(
        &self,
        id: &UsdcRebalanceId,
        correlation_id: Uuid,
    ) -> Result<(), UsdcTransferError> {
        let Some(order) = self
            .alpaca_broker
            .find_conversion_order(correlation_id)
            .await?
        else {
            warn!(target: "rebalance", %correlation_id, "Conversion order never reached Alpaca on resume; failing for reconciliation");
            self.cqrs
                .send(
                    id,
                    UsdcRebalanceCommand::FailConversion {
                        reason: format!("conversion order {correlation_id} never reached Alpaca"),
                    },
                )
                .await?;
            return Err(UsdcTransferError::ResumeIndeterminateConversion { id: id.clone() });
        };

        match order.classify() {
            CryptoOrderOutcome::Filled => {
                let filled_qty = order
                    .filled_quantity
                    .ok_or(UsdcTransferError::MissingFilledQuantity { order_id: order.id })?;
                let filled_amount = Usdc::new(filled_qty);
                self.cqrs
                    .send(
                        id,
                        UsdcRebalanceCommand::ConfirmConversion { filled_amount },
                    )
                    .await?;
                info!(target: "rebalance", order_id = %order.id, %filled_amount, "Resumed conversion confirmed from already-filled order");
                Ok(())
            }
            CryptoOrderOutcome::Pending => {
                warn!(target: "rebalance", order_id = %order.id, "Resumed conversion order still settling; retrying");
                Err(UsdcTransferError::ConversionStillSettling { id: id.clone() })
            }
            CryptoOrderOutcome::Failed(reason) => {
                warn!(target: "rebalance", order_id = %order.id, ?reason, "Resumed conversion order failed terminally");
                self.cqrs
                    .send(
                        id,
                        UsdcRebalanceCommand::FailConversion {
                            reason: format!("conversion order failed: {reason:?}"),
                        },
                    )
                    .await?;
                Err(UsdcTransferError::ResumeIndeterminateConversion { id: id.clone() })
            }
        }
    }

    /// Drives the transfer from `WithdrawalComplete` through to terminal:
    /// burn -> attestation -> mint -> deposit -> USDC->USD conversion.
    async fn continue_from_withdrawal_complete(
        &self,
        id: &UsdcRebalanceId,
        amount: Usdc,
    ) -> Result<(), UsdcTransferError> {
        let amount_u256 = usdc_to_u256(amount)?;
        let burn_receipt = self.execute_cctp_burn_on_base(id, amount_u256).await?;
        self.continue_from_bridging(id, amount, burn_receipt.tx)
            .await
    }

    /// Drives the transfer from `Bridging` through to terminal: poll Circle,
    /// record the attestation (`Bridging` -> `Attested`), then mint and continue.
    async fn continue_from_bridging(
        &self,
        id: &UsdcRebalanceId,
        amount: Usdc,
        burn_tx_hash: TxHash,
    ) -> Result<(), UsdcTransferError> {
        let burn_receipt = BurnReceipt {
            tx: burn_tx_hash,
            amount: usdc_to_u256(amount)?,
        };
        let attestation_response = self.poll_circle_attestation(id, &burn_receipt).await?;

        // Capture the destination (Ethereum) head before minting so a crash
        // before `ConfirmBridging` resumes by scanning for the already-submitted
        // mint (`continue_from_attested`) instead of re-minting, which reverts on
        // the already-used CCTP nonce.
        let mint_scan_from_block = self
            .cctp_bridge
            .destination_block(BridgeDirection::BaseToEthereum)
            .await
            .map_err(|error| UsdcTransferError::Cctp(Box::new(error)))?;

        self.cqrs
            .send(
                id,
                UsdcRebalanceCommand::ReceiveAttestation {
                    attestation: attestation_response.as_bytes().to_vec(),
                    cctp_nonce: attestation_response.nonce(),
                    mint_scan_from_block,
                },
            )
            .await?;

        info!(target: "rebalance", "Circle attestation received for Base burn");
        self.mint_and_continue(id, attestation_response).await
    }

    /// Drives the transfer from `Attested` through to terminal.
    ///
    /// The mint may already have been submitted before a crash, so this first
    /// scans the destination chain (bounded by `mint_scan_from_block`) for an
    /// already-submitted mint to the market maker wallet and adopts it via
    /// `ConfirmBridging` -- re-minting would revert on the already-used CCTP
    /// nonce and fail a transfer whose USDC was in fact minted. When no mint is
    /// found, it re-polls Circle for a fresh [`AttestationResponse`] (the
    /// aggregate stores the attestation bytes and nonce but not the full message
    /// envelope [`Bridge::mint`] needs) and mints, WITHOUT re-emitting
    /// `ReceiveAttestation` -- which the aggregate rejects from `Attested`.
    async fn continue_from_attested(
        &self,
        id: &UsdcRebalanceId,
        amount: Usdc,
        burn_tx_hash: TxHash,
        mint_scan_from_block: u64,
    ) -> Result<(), UsdcTransferError> {
        if let Some(mint_receipt) = self
            .cctp_bridge
            .find_recent_mint(
                BridgeDirection::BaseToEthereum,
                self.market_maker_wallet,
                mint_scan_from_block,
            )
            .await
            .map_err(|error| UsdcTransferError::Cctp(Box::new(error)))?
        {
            let amount_received = u256_to_usdc(mint_receipt.amount)?;

            info!(target: "rebalance", mint_tx = %mint_receipt.tx, %amount_received, "Adopting already-submitted CCTP mint on resume");

            self.cqrs
                .send(
                    id,
                    UsdcRebalanceCommand::ConfirmBridging {
                        mint_tx: mint_receipt.tx,
                        amount_received,
                        fee_collected: u256_to_usdc(mint_receipt.fee)?,
                    },
                )
                .await?;

            return self
                .continue_from_bridged(id, amount_received, mint_receipt.tx)
                .await;
        }

        let burn_receipt = BurnReceipt {
            tx: burn_tx_hash,
            amount: usdc_to_u256(amount)?,
        };
        let attestation_response = self.poll_circle_attestation(id, &burn_receipt).await?;
        self.mint_and_continue(id, attestation_response).await
    }

    /// Mints on the destination chain from a fresh attestation and continues to
    /// the deposit phase. Shared by the `Bridging` and `Attested` resume paths.
    async fn mint_and_continue(
        &self,
        id: &UsdcRebalanceId,
        attestation_response: AttestationResponse,
    ) -> Result<(), UsdcTransferError> {
        let mint_receipt = self
            .execute_cctp_mint_on_ethereum(id, attestation_response)
            .await?;
        self.continue_from_bridged(id, u256_to_usdc(mint_receipt.amount)?, mint_receipt.tx)
            .await
    }

    /// Drives the transfer from `Bridged` through to terminal: re-record the
    /// Alpaca deposit intent, poll Alpaca for the credit, then convert USDC->USD.
    ///
    /// # Crash-safety invariant (load-bearing)
    ///
    /// Unlike the `DepositInitiated` resume arm, this path has no guard
    /// preventing a re-send of `InitiateDeposit`. It is safe to resume from
    /// `Bridged` only because the BaseToAlpaca deposit is effected by the CCTP
    /// mint itself: [`execute_cctp_burn_on_base`](Self::execute_cctp_burn_on_base)
    /// sets the burn's `mintRecipient` to `self.market_maker_wallet`, so the
    /// Ethereum-side mint credits USDC directly to the address Alpaca monitors as
    /// the deposit source. No Alpaca deposit API call moves funds. This function
    /// therefore only RE-RECORDS intent via `InitiateDeposit` (an idempotent
    /// aggregate event) and POLLS Alpaca read-only via `poll_deposit_by_tx_hash`
    /// (a GET). Resuming from `Bridged` issues no new fund-moving call, so
    /// re-execution cannot double-spend and the missing guard is safe. If the
    /// mint-deposits-directly assumption ever changes (e.g. an explicit Alpaca
    /// deposit POST is introduced), this arm must gain a guard or idempotency key.
    async fn continue_from_bridged(
        &self,
        id: &UsdcRebalanceId,
        amount_received: Usdc,
        mint_tx: TxHash,
    ) -> Result<(), UsdcTransferError> {
        self.poll_and_confirm_alpaca_deposit(id, mint_tx).await?;
        self.execute_usdc_to_usd_conversion(id, amount_received)
            .await?;
        Ok(())
    }

    #[instrument(target = "rebalance", skip(self), fields(%id, %amount), level = tracing::Level::DEBUG)]
    async fn withdraw_from_vault(
        &self,
        id: &UsdcRebalanceId,
        amount: Usdc,
        amount_u256: U256,
    ) -> Result<(), UsdcTransferError> {
        // Record withdrawal intent and the chain head BEFORE the irreversible
        // on-chain withdraw, so a crash mid-withdrawal resumes via a chain scan
        // (`resume_withdrawal_submitting`) instead of blindly re-withdrawing.
        let from_block = self.raindex.current_block().await?;
        self.cqrs
            .send(
                id,
                UsdcRebalanceCommand::BeginWithdrawal {
                    direction: RebalanceDirection::BaseToAlpaca,
                    amount,
                    from_block,
                },
            )
            .await?;

        let withdraw_tx = match self.raindex.withdraw_usdc(self.vault_id, amount_u256).await {
            Ok(tx) => tx,
            Err(error) => {
                warn!(target: "rebalance", "Vault withdrawal failed: {error}");
                return Err(UsdcTransferError::Vault(error));
            }
        };

        self.record_vault_withdrawal(id, amount, withdraw_tx).await
    }

    /// Resumes a transfer stalled at `WithdrawalSubmitting`: scans the chain for
    /// an already-submitted withdrawal (adopting it to avoid a double-withdraw)
    /// and otherwise issues the withdrawal, then records and confirms it.
    ///
    /// The scan is finality-gated: it returns `Ok(None)` (safe to issue the
    /// withdrawal) only when the queried node is confirmations-deep past
    /// `from_block`; otherwise it yields a retryable error and this resume re-runs
    /// rather than risking a double-withdraw off a stale empty `eth_getLogs`.
    async fn resume_withdrawal_submitting(
        &self,
        id: &UsdcRebalanceId,
        amount: Usdc,
        amount_u256: U256,
        from_block: u64,
    ) -> Result<(), UsdcTransferError> {
        if let Some((existing_tx, withdrawn)) = self
            .raindex
            .find_recent_withdrawal(USDC_BASE, self.vault_id, from_block)
            .await?
        {
            // The withdrawal for this transfer already landed on-chain; adopt it
            // instead of re-withdrawing. If it realized a different amount than
            // requested (vault under-funded -> partial fill), fail fast for
            // operator reconciliation -- never burn more on Base than was actually
            // withdrawn.
            if withdrawn != amount_u256 {
                warn!(target: "rebalance", %existing_tx, %withdrawn, requested = %amount_u256,
                    "Adopted withdrawal realized a different amount than requested; failing for reconciliation");
                self.cqrs
                    .send(
                        id,
                        UsdcRebalanceCommand::Initiate {
                            direction: RebalanceDirection::BaseToAlpaca,
                            amount,
                            withdrawal: TransferRef::OnchainTx(existing_tx),
                        },
                    )
                    .await?;
                self.cqrs
                    .send(
                        id,
                        UsdcRebalanceCommand::FailWithdrawal {
                            reason: format!(
                                "adopted withdrawal {existing_tx} realized {withdrawn}, \
                                 requested {amount_u256}"
                            ),
                        },
                    )
                    .await?;
                return Err(UsdcTransferError::AdoptedWithdrawalAmountMismatch {
                    id: id.clone(),
                    withdrawn,
                    requested: amount_u256,
                });
            }

            info!(target: "rebalance", %existing_tx, "Adopting already-submitted vault withdrawal on resume");
            return self.record_vault_withdrawal(id, amount, existing_tx).await;
        }

        let withdraw_tx = match self.raindex.withdraw_usdc(self.vault_id, amount_u256).await {
            Ok(tx) => tx,
            Err(error) => {
                warn!(target: "rebalance", "Vault withdrawal failed: {error}");
                return Err(UsdcTransferError::Vault(error));
            }
        };

        self.record_vault_withdrawal(id, amount, withdraw_tx).await
    }

    /// Records the submitted withdrawal transaction and confirms it. The vault
    /// withdrawal waits for block inclusion, so confirmation is immediate.
    async fn record_vault_withdrawal(
        &self,
        id: &UsdcRebalanceId,
        amount: Usdc,
        withdraw_tx: TxHash,
    ) -> Result<(), UsdcTransferError> {
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
        self.cqrs
            .send(id, UsdcRebalanceCommand::ConfirmWithdrawal)
            .await?;

        info!(target: "rebalance", %withdraw_tx, "Vault withdrawal completed");
        Ok(())
    }

    #[instrument(target = "rebalance", skip(self), fields(%id, %amount), level = tracing::Level::DEBUG)]
    async fn execute_cctp_burn_on_base(
        &self,
        id: &UsdcRebalanceId,
        amount: U256,
    ) -> Result<BurnReceipt, UsdcTransferError> {
        // Record bridging intent and the chain head BEFORE the irreversible burn,
        // so a crash mid-burn resumes via a chain scan (`resume_bridging_submitting`)
        // instead of re-burning (which would burn USDC twice with at most one mint).
        let from_block = self.raindex.current_block().await?;
        self.cqrs
            .send(id, UsdcRebalanceCommand::BeginBridging { from_block })
            .await?;

        let burn_receipt = self.burn_on_base(amount).await?;
        self.record_cctp_burn(id, burn_receipt).await
    }

    /// Resumes a transfer stalled at `BridgingSubmitting`: scans the chain for an
    /// already-submitted burn (adopting it to avoid a double-burn) and otherwise
    /// issues the burn, then records it. Returns the burn receipt so the caller
    /// can continue the bridge.
    async fn resume_bridging_submitting(
        &self,
        id: &UsdcRebalanceId,
        amount: U256,
        from_block: u64,
    ) -> Result<BurnReceipt, UsdcTransferError> {
        let burn_receipt = match self
            .cctp_bridge
            .find_recent_burn(
                BridgeDirection::BaseToEthereum,
                amount,
                self.market_maker_wallet,
                from_block,
            )
            .await
        {
            Ok(Some(existing_tx)) => {
                info!(target: "rebalance", %existing_tx, "Adopting already-submitted CCTP burn on resume");
                BurnReceipt {
                    tx: existing_tx,
                    amount,
                }
            }
            Ok(None) => self.burn_on_base(amount).await?,
            Err(error) => {
                warn!(target: "rebalance", "CCTP burn scan on Base failed: {error}");
                return Err(UsdcTransferError::Cctp(Box::new(error)));
            }
        };

        self.record_cctp_burn(id, burn_receipt).await
    }

    /// Burns USDC on Base. On failure the aggregate stays at `BridgingSubmitting`
    /// (no terminal event is emitted) so an apalis retry re-enters the scan path
    /// rather than re-burning, and exhausted retries latch the in-progress guard
    /// for operator reconciliation.
    async fn burn_on_base(&self, amount: U256) -> Result<BurnReceipt, UsdcTransferError> {
        self.cctp_bridge
            .burn(
                BridgeDirection::BaseToEthereum,
                amount,
                self.market_maker_wallet,
            )
            .await
            .map_err(|error| {
                warn!(target: "rebalance", "CCTP burn on Base failed: {error}");
                UsdcTransferError::Cctp(Box::new(error))
            })
    }

    /// Records the submitted CCTP burn transaction, advancing to `Bridging`.
    async fn record_cctp_burn(
        &self,
        id: &UsdcRebalanceId,
        burn_receipt: BurnReceipt,
    ) -> Result<BurnReceipt, UsdcTransferError> {
        self.cqrs
            .send(
                id,
                UsdcRebalanceCommand::InitiateBridging {
                    burn_tx: burn_receipt.tx,
                },
            )
            .await?;

        info!(target: "rebalance", burn_tx = %burn_receipt.tx, "CCTP burn on Base executed");
        Ok(burn_receipt)
    }

    /// Polls Circle for the attestation of a Base burn. On failure records
    /// `FailBridging` (valid from both `Bridging` and `Attested`) and returns the
    /// error. Does NOT emit `ReceiveAttestation` -- the caller records it only on
    /// the `Bridging` path, where it is valid; the `Attested` resume path must
    /// not re-emit it.
    #[instrument(target = "rebalance", skip(self, burn_receipt), fields(%id, burn_tx = %burn_receipt.tx), level = tracing::Level::DEBUG)]
    async fn poll_circle_attestation(
        &self,
        id: &UsdcRebalanceId,
        burn_receipt: &BurnReceipt,
    ) -> Result<AttestationResponse, UsdcTransferError> {
        match self
            .cctp_bridge
            .poll_attestation(BridgeDirection::BaseToEthereum, burn_receipt.tx)
            .await
        {
            Ok(response) => Ok(response),
            Err(error) => {
                warn!(target: "rebalance", "Attestation polling failed: {error}");
                self.cqrs
                    .send(
                        id,
                        UsdcRebalanceCommand::FailBridging {
                            reason: format!("Attestation polling failed: {error}"),
                        },
                    )
                    .await?;
                Err(UsdcTransferError::Cctp(Box::new(error)))
            }
        }
    }

    #[instrument(target = "rebalance", skip(self, attestation_response), fields(%id), level = tracing::Level::DEBUG)]
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
                warn!(target: "rebalance", "CCTP mint on Ethereum failed: {error}");
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

        info!(target: "rebalance",
            mint_tx = %mint_receipt.tx,
            amount = %mint_receipt.amount,
            fee = %mint_receipt.fee,
            "CCTP mint on Ethereum executed"
        );
        Ok(mint_receipt)
    }

    #[instrument(target = "rebalance", skip(self), fields(%id, %mint_tx), level = tracing::Level::DEBUG)]
    async fn poll_and_confirm_alpaca_deposit(
        &self,
        id: &UsdcRebalanceId,
        mint_tx: TxHash,
    ) -> Result<(), UsdcTransferError> {
        // Record the deposit initiation with the mint tx. This is a pure
        // intent record, never a fund-moving call: the CCTP mint already
        // deposited the USDC directly to the Alpaca-controlled address, so the
        // "deposit" IS the on-chain mint tx. That invariant is what makes
        // resume from `Bridged` safe -- re-sending `InitiateDeposit` here just
        // re-records the same mint tx, it does not move funds again. The only
        // Alpaca interaction (below) is a read-only poll to detect the deposit.
        self.cqrs
            .send(
                id,
                UsdcRebalanceCommand::InitiateDeposit {
                    deposit: TransferRef::OnchainTx(mint_tx),
                },
            )
            .await?;

        self.poll_alpaca_deposit_and_confirm(id, mint_tx).await
    }

    /// Polls Alpaca for the deposit identified by `mint_tx`, then sends
    /// `ConfirmDeposit`. Assumes the aggregate is already in
    /// `DepositInitiated` (caller has sent `InitiateDeposit`, or we are
    /// resuming from that state).
    #[instrument(target = "rebalance", skip(self), fields(%id, %mint_tx), level = tracing::Level::DEBUG)]
    async fn poll_alpaca_deposit_and_confirm(
        &self,
        id: &UsdcRebalanceId,
        mint_tx: TxHash,
    ) -> Result<(), UsdcTransferError> {
        info!(target: "rebalance", %mint_tx, "Polling Alpaca for deposit detection");

        let transfer = match self.alpaca_wallet.poll_deposit_by_tx_hash(&mint_tx).await {
            Ok(transfer) => transfer,
            Err(error) => {
                warn!(target: "rebalance", "Alpaca deposit polling failed: {error}");
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

        info!(target: "rebalance", "Alpaca deposit confirmed");
        Ok(())
    }
}

/// Converts a USDC decimal amount to U256 with 6 decimals.
///
/// Delegates to [`Usdc::to_u256_6_decimals`].
fn usdc_to_u256(usdc: Usdc) -> Result<U256, UsdcTransferError> {
    Ok(usdc.to_u256_6_decimals()?)
}

/// Converts a U256 amount (with 6 decimals) to USDC decimal.
pub(crate) fn u256_to_usdc(amount: U256) -> Result<Usdc, UsdcTransferError> {
    Ok(Usdc::new(Float::from_fixed_decimal(amount, 6)?))
}

#[cfg(test)]
mod tests {
    use alloy::node_bindings::{Anvil, AnvilInstance};
    use alloy::primitives::{B256, address, b256, fixed_bytes};
    use alloy::providers::ProviderBuilder;
    use alloy::providers::ext::AnvilApi as _;
    use alloy::signers::local::PrivateKeySigner;
    use httpmock::prelude::*;
    use reqwest::StatusCode;
    use serde_json::json;
    use sqlx::SqlitePool;
    use std::str::FromStr;
    use std::sync::Arc;
    use uuid::{Uuid, uuid};

    use st0x_execution::alpaca_broker_api::CryptoOrderFailureReason;
    use st0x_execution::{
        AlpacaAccountId, AlpacaBrokerApiCtx, AlpacaBrokerApiError, AlpacaBrokerApiMode, Executor,
        TimeInForce,
    };

    use st0x_bridge::Bridge;
    use st0x_bridge::cctp::{
        CctpAttestationMock, CctpBridge, CctpCtx, TestMintBurnToken, deploy_cctp_on_chain,
        link_chains, mint_usdc, set_max_burn_amount,
    };
    use st0x_event_sorcery::{AggregateError, LifecycleError, StoreBuilder, test_store};
    use st0x_evm::Wallet;
    use st0x_evm::local::RawPrivateKeyWallet;

    use super::*;
    use crate::alpaca_wallet::{AlpacaTransferId, AlpacaWalletClient, AlpacaWalletError};
    use crate::onchain::raindex::RaindexService;
    use crate::usdc_rebalance::{RebalanceDirection, TransferRef, UsdcRebalanceError};
    use crate::vault_registry::VaultRegistry;
    use st0x_finance::UsdcConversionError;

    fn usdc(value: &str) -> Usdc {
        Usdc::from_str(value).unwrap()
    }

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
                mint_scan_from_block: 100,
            },
        )
        .await
        .unwrap();

        cqrs.send(
            id,
            UsdcRebalanceCommand::ConfirmBridging {
                mint_tx,
                amount_received: usdc("99.99"),
                fee_collected: usdc("0.01"),
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

    /// Advances an aggregate through the full Alpaca->Base lifecycle up
    /// to (but not past) `DepositInitiated`, persisting the supplied
    /// `deposit_tx` as the on-chain reference for the vault deposit.
    async fn advance_to_deposit_initiated_alpaca_to_base(
        cqrs: &Store<UsdcRebalance>,
        id: &UsdcRebalanceId,
        amount: Usdc,
        deposit_tx: TxHash,
    ) {
        let burn_tx =
            fixed_bytes!("0xaaaa000000000000000000000000000000000000000000000000000000000001");
        let mint_tx =
            fixed_bytes!("0xaaaa111111111111111111111111111111111111111111111111111111111111");

        cqrs.send(
            id,
            UsdcRebalanceCommand::InitiateConversion {
                direction: RebalanceDirection::AlpacaToBase,
                amount,
                order_id: Uuid::new_v4(),
            },
        )
        .await
        .unwrap();
        cqrs.send(
            id,
            UsdcRebalanceCommand::ConfirmConversion {
                filled_amount: amount,
            },
        )
        .await
        .unwrap();
        cqrs.send(
            id,
            UsdcRebalanceCommand::Initiate {
                direction: RebalanceDirection::AlpacaToBase,
                amount,
                withdrawal: TransferRef::AlpacaId(AlpacaTransferId::from(Uuid::new_v4())),
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
                cctp_nonce: 12345,
                mint_scan_from_block: 100,
            },
        )
        .await
        .unwrap();
        cqrs.send(
            id,
            UsdcRebalanceCommand::ConfirmBridging {
                mint_tx,
                amount_received: amount,
                fee_collected: usdc("0"),
            },
        )
        .await
        .unwrap();
        cqrs.send(
            id,
            UsdcRebalanceCommand::InitiateDeposit {
                deposit: TransferRef::OnchainTx(deposit_tx),
            },
        )
        .await
        .unwrap();
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
            counter_trade_slippage_bps: st0x_execution::DEFAULT_ALPACA_COUNTER_TRADE_SLIPPAGE_BPS,
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
            #[cfg(feature = "test-support")]
            circle_api_base: st0x_bridge::cctp::CIRCLE_API_BASE.to_string(),
            #[cfg(feature = "test-support")]
            token_messenger: st0x_bridge::cctp::TOKEN_MESSENGER_V2,
            #[cfg(feature = "test-support")]
            message_transmitter: st0x_bridge::cctp::MESSAGE_TRANSMITTER_V2,
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

    /// Like [`create_test_onchain_services`] but points the CCTP bridge's Circle
    /// attestation polling at `circle_api_base` (a local mock), so attestation
    /// polling resolves immediately instead of retrying the real Circle endpoint
    /// for ~5 minutes. Test-support only.
    #[cfg(feature = "test-support")]
    async fn create_test_onchain_services_with_circle_api<Chain: Wallet + Clone>(
        wallet: Chain,
        circle_api_base: String,
    ) -> (CctpBridge<Chain, Chain>, RaindexService<Chain>) {
        let cctp_bridge = CctpBridge::try_from_ctx(CctpCtx {
            usdc_ethereum: USDC_ADDRESS,
            usdc_base: USDC_ADDRESS,
            ethereum_wallet: wallet.clone(),
            base_wallet: wallet.clone(),
            circle_api_base,
            token_messenger: st0x_bridge::cctp::TOKEN_MESSENGER_V2,
            message_transmitter: st0x_bridge::cctp::MESSAGE_TRANSMITTER_V2,
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
        let amount = usdc("1000.50");
        assert_eq!(usdc_to_u256(amount).unwrap(), U256::from(1_000_500_000u64));
    }

    #[test]
    fn test_usdc_to_u256_negative_amount() {
        let amount = usdc("-100");
        let error = usdc_to_u256(amount).unwrap_err();
        assert!(
            matches!(
                error,
                UsdcTransferError::UsdcConversion(UsdcConversionError::NegativeValue(_))
            ),
            "Expected UsdcConversion(NegativeValue) error, got: {error:?}"
        );
    }

    #[test]
    fn test_usdc_to_u256_zero_amount() {
        let amount = usdc("0");
        assert_eq!(usdc_to_u256(amount).unwrap(), U256::ZERO);
    }

    #[test]
    fn test_usdc_to_u256_rejects_excess_precision() {
        // 7 decimal places exceeds USDC's 6 decimal precision
        let amount = usdc("100.1234567");
        let error = usdc_to_u256(amount).unwrap_err();
        assert!(
            matches!(error, UsdcTransferError::UsdcConversion(_)),
            "Expected UsdcConversion error for lossy conversion, got: {error:?}"
        );
    }

    #[test]
    fn test_usdc_to_u256_fractional_precision() {
        // Test with precise fractional amounts (6 decimals for USDC)
        let amount = usdc("1000.123456");
        assert_eq!(usdc_to_u256(amount).unwrap(), U256::from(1_000_123_456u64));
    }

    #[test]
    fn test_usdc_to_u256_minimum_amount() {
        // Test near-minimum amounts (smallest USDC unit is 0.000001)
        let amount = usdc("0.000001");
        assert_eq!(usdc_to_u256(amount).unwrap(), U256::from(1u64));
    }

    #[test]
    fn test_usdc_to_u256_large_amount_no_overflow() {
        // Test large amounts that should work without overflow
        // $1 trillion in USDC
        let amount = usdc("1000000000000");
        assert_eq!(
            usdc_to_u256(amount).unwrap(),
            U256::from(1_000_000_000_000_000_000u64)
        );
    }

    #[test]
    fn test_usdc_to_u256_rejects_beyond_6_decimals() {
        // USDC has 6 decimals, anything beyond should be rejected as lossy
        let amount = usdc("100.1234567890");
        let error = usdc_to_u256(amount).unwrap_err();
        assert!(
            matches!(error, UsdcTransferError::UsdcConversion(_)),
            "Expected UsdcConversion error for lossy conversion, got: {error:?}"
        );
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
        let amount = usdc("1000");

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
        let amount = usdc("500");

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
        let amount = usdc("100");

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
        let amount = usdc("-500");

        let error = manager
            .execute_base_to_alpaca(&id, amount)
            .await
            .unwrap_err();
        assert!(
            matches!(
                error,
                UsdcTransferError::UsdcConversion(UsdcConversionError::NegativeValue(_))
            ),
            "Expected UsdcConversion(NegativeValue) error, got: {error:?}"
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
        let amount = usdc("1000");

        let error = manager
            .execute_base_to_alpaca(&id, amount)
            .await
            .unwrap_err();

        assert!(
            matches!(error, UsdcTransferError::Cctp(_)),
            "Expected Cctp error when CCTP burn contract call fails, got: {error:?}"
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
        let amount = usdc("1000");

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
        let amount = usdc("500");

        // execute_usdc_to_usd_conversion requires aggregate to be in DepositConfirmed state
        // (after a BaseToAlpaca deposit completes). With a fresh aggregate, it should fail.
        assert!(
            matches!(
                manager.execute_usdc_to_usd_conversion(&id, amount).await,
                Err(UsdcTransferError::Aggregate(error))
                    if matches!(
                        error.as_ref(),
                        AggregateError::UserError(
                            LifecycleError::Apply(UsdcRebalanceError::DepositNotConfirmed)
                        )
                    )
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
        let amount = usdc("1000");

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
        let amount = usdc("1000");

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
        let amount = usdc("1000");

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
                mint_scan_from_block: 100,
            },
        )
        .await
        .unwrap();

        cqrs.send(
            &id,
            UsdcRebalanceCommand::ConfirmBridging {
                mint_tx,
                amount_received: usdc("99.99"),
                fee_collected: usdc("0.01"),
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
        let amount_received = usdc("99.99");
        let _place_mock = create_conversion_order_pending_mock(&server, "99.99");

        let _get_mock = create_get_order_mock(
            &server,
            "61e7b016-9c91-4a97-b912-615c9d365c9d",
            "rejected",
            "99.99",
        );

        assert!(
            matches!(
                manager
                    .execute_usdc_to_usd_conversion(&id, amount_received)
                    .await,
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
        let amount = usdc("1000");

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
                .json_body_includes(r#"{"symbol":"USDCUSD"}"#);
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
        let amount = usdc("1000");

        manager
            .execute_alpaca_to_base(&id, amount)
            .await
            .unwrap_err();

        // Conversion MUST be called before withdrawal
        assert!(
            conversion_mock.calls() >= 1,
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
                .json_body_includes(r#"{"symbol":"USDCUSD"}"#);
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "id": "61e7b016-9c91-4a97-b912-615c9d365c9d",
                    "symbol": "USDCUSD",
                    "qty": "99.99",
                    "status": "filled",
                    "side": "sell",
                    "filled_avg_price": "0.9999",
                    "filled_qty": "99.99",
                    "created_at": "2024-01-15T10:30:00Z"
                }));
        });

        let _get_order_mock = create_get_order_mock(
            &server,
            "61e7b016-9c91-4a97-b912-615c9d365c9d",
            "filled",
            "99.99",
        );

        let id = UsdcRebalanceId(Uuid::new_v4());
        let amount = usdc("1000");
        let amount_received = usdc("99.99");

        advance_to_deposit_confirmed_base_to_alpaca(&cqrs, &id, amount).await;

        manager
            .execute_usdc_to_usd_conversion(&id, amount_received)
            .await
            .unwrap();

        assert!(
            conversion_mock.calls() >= 1,
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
        let amount = usdc("1000");

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
        let amount = usdc("1000");

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
                Err(UsdcTransferError::Aggregate(error))
                    if matches!(
                        error.as_ref(),
                        AggregateError::UserError(
                            LifecycleError::Apply(UsdcRebalanceError::AlreadyInitiated)
                        )
                    )
            ),
            "Expected AlreadyInitiated error"
        );

        // Verify no order was placed - CQRS failure should prevent side effects
        assert_eq!(
            order_mock.calls(),
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
        let amount = usdc("1000");

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
        let amount = usdc("1000");

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

        withdrawal_result.unwrap();
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
        let requested_amount = usdc("1000");

        let filled_amount = manager
            .execute_usd_to_usdc_conversion(&id, requested_amount)
            .await
            .unwrap();

        // Should return the actual filled amount, not the requested amount
        assert_eq!(
            filled_amount,
            usdc("999.5"),
            "Should return actual filled amount, not requested amount"
        );
    }

    /// Builds a `CrossVenueCashTransfer` whose `cqrs` store is reachable to
    /// the caller. Used by resume-routing tests that pre-stage aggregate
    /// state before invoking `resume_base_to_alpaca`.
    async fn make_resume_test_manager(
        server: &MockServer,
    ) -> (
        CrossVenueCashTransfer<
            RawPrivateKeyWallet<impl alloy::providers::Provider + Clone + use<>>,
        >,
        Arc<Store<UsdcRebalance>>,
        alloy::node_bindings::AnvilInstance,
    ) {
        let (anvil, endpoint, private_key) = setup_anvil();
        let alpaca_broker = Arc::new(create_test_broker_service(server).await);
        let alpaca_wallet = Arc::new(create_test_wallet_service(server));
        let wallet = create_test_wallet(&endpoint, &private_key);
        let (cctp_bridge, vault_service) = create_test_onchain_services(wallet).await;
        let cqrs = create_test_store_instance().await;
        let market_maker_wallet = address!("0x1111111111111111111111111111111111111111");

        let manager = CrossVenueCashTransfer::new(
            alpaca_broker,
            alpaca_wallet,
            Arc::new(cctp_bridge),
            Arc::new(vault_service),
            cqrs.clone(),
            market_maker_wallet,
            TEST_VAULT_ID,
        );

        (manager, cqrs, anvil)
    }

    /// Like [`make_resume_test_manager`] but with the CCTP bridge's Circle
    /// attestation polling pointed at a local mock so attestation resolves fast.
    #[cfg(feature = "test-support")]
    async fn make_resume_test_manager_with_circle_api(
        server: &MockServer,
        circle_api_base: String,
    ) -> (
        CrossVenueCashTransfer<
            RawPrivateKeyWallet<impl alloy::providers::Provider + Clone + use<>>,
        >,
        Arc<Store<UsdcRebalance>>,
        alloy::node_bindings::AnvilInstance,
    ) {
        let (anvil, endpoint, private_key) = setup_anvil();
        let alpaca_broker = Arc::new(create_test_broker_service(server).await);
        let alpaca_wallet = Arc::new(create_test_wallet_service(server));
        let wallet = create_test_wallet(&endpoint, &private_key);
        let (cctp_bridge, vault_service) =
            create_test_onchain_services_with_circle_api(wallet, circle_api_base).await;
        let cqrs = create_test_store_instance().await;
        let market_maker_wallet = address!("0x1111111111111111111111111111111111111111");

        let manager = CrossVenueCashTransfer::new(
            alpaca_broker,
            alpaca_wallet,
            Arc::new(cctp_bridge),
            Arc::new(vault_service),
            cqrs.clone(),
            market_maker_wallet,
            TEST_VAULT_ID,
        );

        (manager, cqrs, anvil)
    }

    /// Drives the aggregate through `Initiate` -> `ConfirmWithdrawal` ->
    /// `InitiateBridging` -> `ReceiveAttestation` so it lands at `Attested`
    /// (stops before `ConfirmBridging`).
    async fn advance_to_attested_base_to_alpaca(
        cqrs: &Store<UsdcRebalance>,
        id: &UsdcRebalanceId,
        amount: Usdc,
    ) -> TxHash {
        let burn_tx =
            fixed_bytes!("0xaaaa000000000000000000000000000000000000000000000000000000000001");

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
                cctp_nonce: 99_999,
                // Scan from genesis, not the sibling helpers' `100`: the resume test
                // built on this helper actually scans from this block for an
                // already-submitted mint, and the fresh test chain (advanced only via
                // event-store commands) sits below 100. 0 makes that scan a valid
                // range that genuinely finds no mint, so resume proceeds to mint.
                mint_scan_from_block: 0,
            },
        )
        .await
        .unwrap();
        burn_tx
    }

    /// Drives the aggregate through `Initiate` -> `ConfirmWithdrawal` ->
    /// `InitiateBridging` -> `ReceiveAttestation` -> `ConfirmBridging` so
    /// the next callable step is the Alpaca deposit initiation.
    async fn advance_to_bridged_base_to_alpaca(
        cqrs: &Store<UsdcRebalance>,
        id: &UsdcRebalanceId,
        amount: Usdc,
        amount_received: Usdc,
    ) -> (TxHash, TxHash) {
        let burn_tx =
            fixed_bytes!("0xaaaa000000000000000000000000000000000000000000000000000000000001");
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
                cctp_nonce: 99_999,
                mint_scan_from_block: 100,
            },
        )
        .await
        .unwrap();
        cqrs.send(
            id,
            UsdcRebalanceCommand::ConfirmBridging {
                mint_tx,
                amount_received,
                fee_collected: usdc("0.01"),
            },
        )
        .await
        .unwrap();
        (burn_tx, mint_tx)
    }

    /// Drives a BaseToAlpaca transfer all the way to `Converting`, recording
    /// `correlation_id` as the conversion order's correlation key. This is the
    /// post-deposit state a crash can strand once the Alpaca conversion order is
    /// placed but `ConfirmConversion` has not yet been emitted.
    async fn advance_to_converting_base_to_alpaca(
        cqrs: &Store<UsdcRebalance>,
        id: &UsdcRebalanceId,
        amount: Usdc,
        amount_received: Usdc,
        correlation_id: Uuid,
    ) {
        advance_to_bridged_base_to_alpaca(cqrs, id, amount, amount_received).await;
        cqrs.send(
            id,
            UsdcRebalanceCommand::InitiateDeposit {
                deposit: TransferRef::OnchainTx(fixed_bytes!(
                    "0xbbbb111111111111111111111111111111111111111111111111111111111111"
                )),
            },
        )
        .await
        .unwrap();
        cqrs.send(id, UsdcRebalanceCommand::ConfirmDeposit)
            .await
            .unwrap();
        cqrs.send(
            id,
            UsdcRebalanceCommand::InitiatePostDepositConversion {
                order_id: correlation_id,
                amount: amount_received,
            },
        )
        .await
        .unwrap();
    }

    /// Mocks the Alpaca `orders:by_client_order_id` lookup the `Converting`
    /// resume uses, returning a crypto order with the given status/fill so each
    /// resume-from-`Converting` test can pick the outcome it exercises.
    fn mock_conversion_lookup<'server>(
        server: &'server MockServer,
        correlation_id: Uuid,
        status: &str,
        filled_qty: &str,
    ) -> httpmock::Mock<'server> {
        server.mock(|when, then| {
            when.method(GET)
                .path("/v1/trading/accounts/904837e3-3b76-47ec-b432-046db621571b/orders:by_client_order_id")
                .query_param("client_order_id", correlation_id.to_string());
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "id": "904837e3-3b76-47ec-b432-046db621571b",
                    "symbol": "USDCUSD",
                    "qty": "99.99",
                    "status": status,
                    "filled_avg_price": "1.0001",
                    "filled_qty": filled_qty,
                    "created_at": "2025-01-06T12:00:00Z"
                }));
        })
    }

    #[cfg(feature = "test-support")]
    #[tokio::test]
    async fn resume_base_to_alpaca_from_attested_does_not_re_emit_receive_attestation() {
        let server = MockServer::start();

        // Return a `complete` attestation for any /v2/messages request so
        // poll_circle_attestation resolves immediately (no 5-minute real-API
        // retry). The message must be >= 44 bytes with the upper 24 bytes of the
        // 32-byte nonce (offset 12..36) zero so AttestationResponse::new's u64
        // nonce check passes -- a mostly-zero 100-byte message with one low byte.
        let mut message = vec![0u8; 100];
        message[43] = 1;
        let message_hex = format!("0x{}", alloy::hex::encode(&message));
        let attestation_hex = format!("0x{}", "ab".repeat(65));
        let _attestation_mock = server.mock(|when, then| {
            when.method(GET).path_includes("/v2/messages/");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "messages": [{
                        "status": "complete",
                        "message": message_hex,
                        "attestation": attestation_hex,
                    }]
                }));
        });

        let (manager, cqrs, _anvil) =
            make_resume_test_manager_with_circle_api(&server, server.base_url()).await;
        let id = UsdcRebalanceId(Uuid::new_v4());
        let amount = usdc("100");
        advance_to_attested_base_to_alpaca(&cqrs, &id, amount).await;

        let staged = cqrs.load(&id).await.unwrap().expect("aggregate exists");
        assert!(
            matches!(staged, UsdcRebalance::Attested { .. }),
            "staging must land at Attested, got {staged:?}",
        );

        let error = manager
            .resume_base_to_alpaca(&id, amount)
            .await
            .unwrap_err();

        // The OLD (buggy) behavior re-sent ReceiveAttestation from Attested, which
        // the aggregate rejects -> UsdcTransferError::Aggregate(InvalidCommand).
        // The fix routes Attested through continue_from_attested (poll -> mint, no
        // ReceiveAttestation), so resume gets PAST the Attested gate and instead
        // fails at the mint on the undeployed contract -> Cctp.
        assert!(
            !matches!(&error, UsdcTransferError::Aggregate(_)),
            "resume from Attested must NOT re-emit ReceiveAttestation (the aggregate \
             rejects it from Attested); got: {error:?}",
        );
        assert!(
            matches!(error, UsdcTransferError::Cctp(_)),
            "resume from Attested should proceed to mint and fail with a Cctp contract \
             error; got: {error:?}",
        );
    }

    #[tokio::test]
    async fn resume_base_to_alpaca_from_terminal_failure_returns_previously_failed_error() {
        let server = MockServer::start();
        let (manager, cqrs, _anvil) = make_resume_test_manager(&server).await;

        let id = UsdcRebalanceId(Uuid::new_v4());
        let amount = usdc("100");

        let burn_tx =
            fixed_bytes!("0xcccc000000000000000000000000000000000000000000000000000000000002");
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
            UsdcRebalanceCommand::FailBridging {
                reason: "test-induced failure".into(),
            },
        )
        .await
        .unwrap();

        let error = manager
            .resume_base_to_alpaca(&id, amount)
            .await
            .unwrap_err();
        assert!(
            matches!(
                error,
                UsdcTransferError::PreviouslyFailedAggregate { id: ref e_id }
                    if *e_id == id
            ),
            "Expected PreviouslyFailedAggregate, got: {error:?}",
        );
    }

    #[tokio::test]
    async fn resume_base_to_alpaca_from_conversion_complete_is_noop() {
        let server = MockServer::start();
        let (manager, cqrs, _anvil) = make_resume_test_manager(&server).await;

        let id = UsdcRebalanceId(Uuid::new_v4());
        let amount = usdc("100");
        let amount_received = usdc("99.99");
        advance_to_bridged_base_to_alpaca(&cqrs, &id, amount, amount_received).await;
        cqrs.send(
            &id,
            UsdcRebalanceCommand::InitiateDeposit {
                deposit: TransferRef::OnchainTx(fixed_bytes!(
                    "0xbbbb111111111111111111111111111111111111111111111111111111111111"
                )),
            },
        )
        .await
        .unwrap();
        cqrs.send(&id, UsdcRebalanceCommand::ConfirmDeposit)
            .await
            .unwrap();
        cqrs.send(
            &id,
            UsdcRebalanceCommand::InitiatePostDepositConversion {
                order_id: Uuid::new_v4(),
                amount: amount_received,
            },
        )
        .await
        .unwrap();
        cqrs.send(
            &id,
            UsdcRebalanceCommand::ConfirmConversion {
                filled_amount: amount_received,
            },
        )
        .await
        .unwrap();

        // No mocks for Alpaca or CCTP services — a no-op resume must NOT call them.
        manager.resume_base_to_alpaca(&id, amount).await.unwrap();
    }

    #[tokio::test]
    async fn resume_base_to_alpaca_from_converting_confirms_from_filled_order() {
        let server = MockServer::start();
        let (manager, cqrs, _anvil) = make_resume_test_manager(&server).await;

        let id = UsdcRebalanceId(Uuid::new_v4());
        let amount = usdc("100");
        let amount_received = usdc("99.99");
        // Correlation id persisted before the conversion order was placed; it is
        // the Alpaca client_order_id the resume looks the order up by.
        let correlation_id = uuid!("33333333-3333-4333-8333-333333333333");

        advance_to_converting_base_to_alpaca(&cqrs, &id, amount, amount_received, correlation_id)
            .await;

        // The crash happened after the conversion order was placed at Alpaca but
        // before `ConfirmConversion` was emitted. On resume the order is found
        // filled by its client_order_id and the conversion is confirmed without
        // placing a second order (no POST /orders mock is configured, so a
        // re-placement would 501 and fail the test loud).
        let lookup_mock = mock_conversion_lookup(&server, correlation_id, "filled", "99.99");

        manager.resume_base_to_alpaca(&id, amount).await.unwrap();

        lookup_mock.assert();
        let state = cqrs.load(&id).await.unwrap();
        assert!(
            matches!(
                &state,
                Some(UsdcRebalance::ConversionComplete { filled_amount, .. })
                    if *filled_amount == usdc("99.99")
            ),
            "expected ConversionComplete with filled_amount 99.99, got {state:?}"
        );
    }

    #[tokio::test]
    async fn resume_base_to_alpaca_from_converting_still_settling_retries() {
        let server = MockServer::start();
        let (manager, cqrs, _anvil) = make_resume_test_manager(&server).await;

        let id = UsdcRebalanceId(Uuid::new_v4());
        let amount = usdc("100");
        let amount_received = usdc("99.99");
        let correlation_id = uuid!("44444444-4444-4444-8444-444444444444");

        advance_to_converting_base_to_alpaca(&cqrs, &id, amount, amount_received, correlation_id)
            .await;

        // Order exists but is not yet terminal: resume must surface a retryable
        // error and leave the aggregate in `Converting` for apalis to re-check.
        let lookup_mock = mock_conversion_lookup(&server, correlation_id, "new", "0");

        let error = manager
            .resume_base_to_alpaca(&id, amount)
            .await
            .unwrap_err();

        lookup_mock.assert();
        assert!(
            matches!(error, UsdcTransferError::ConversionStillSettling { id: ref erred } if *erred == id),
            "expected ConversionStillSettling for {id}, got {error:?}"
        );
        let state = cqrs.load(&id).await.unwrap();
        assert!(
            matches!(state, Some(UsdcRebalance::Converting { .. })),
            "aggregate must remain Converting on a pending order, got {state:?}"
        );
    }

    #[tokio::test]
    async fn resume_base_to_alpaca_from_converting_fails_on_terminally_failed_order() {
        let server = MockServer::start();
        let (manager, cqrs, _anvil) = make_resume_test_manager(&server).await;

        let id = UsdcRebalanceId(Uuid::new_v4());
        let amount = usdc("100");
        let amount_received = usdc("99.99");
        let correlation_id = uuid!("55555555-5555-4555-8555-555555555555");

        advance_to_converting_base_to_alpaca(&cqrs, &id, amount, amount_received, correlation_id)
            .await;

        // A rejected order is terminal: resume fails the conversion in the
        // aggregate and returns the indeterminate error for operator follow-up.
        let lookup_mock = mock_conversion_lookup(&server, correlation_id, "rejected", "0");

        let error = manager
            .resume_base_to_alpaca(&id, amount)
            .await
            .unwrap_err();

        lookup_mock.assert();
        assert!(
            matches!(error, UsdcTransferError::ResumeIndeterminateConversion { id: ref erred } if *erred == id),
            "expected ResumeIndeterminateConversion for {id}, got {error:?}"
        );
        let state = cqrs.load(&id).await.unwrap();
        assert!(
            matches!(state, Some(UsdcRebalance::ConversionFailed { .. })),
            "aggregate must be ConversionFailed after a rejected order, got {state:?}"
        );
    }

    #[tokio::test]
    async fn resume_base_to_alpaca_from_converting_fails_when_order_never_reached_alpaca() {
        let server = MockServer::start();
        let (manager, cqrs, _anvil) = make_resume_test_manager(&server).await;

        let id = UsdcRebalanceId(Uuid::new_v4());
        let amount = usdc("100");
        let amount_received = usdc("99.99");
        let correlation_id = uuid!("66666666-6666-4666-8666-666666666666");

        advance_to_converting_base_to_alpaca(&cqrs, &id, amount, amount_received, correlation_id)
            .await;

        // 404 means the crash happened before the order ever reached Alpaca:
        // resume fails the conversion rather than blindly re-placing it.
        let lookup_mock = server.mock(|when, then| {
            when.method(GET)
                .path("/v1/trading/accounts/904837e3-3b76-47ec-b432-046db621571b/orders:by_client_order_id")
                .query_param("client_order_id", correlation_id.to_string());
            then.status(404)
                .header("content-type", "application/json")
                .json_body(json!({ "message": "order not found" }));
        });

        let error = manager
            .resume_base_to_alpaca(&id, amount)
            .await
            .unwrap_err();

        lookup_mock.assert();
        assert!(
            matches!(error, UsdcTransferError::ResumeIndeterminateConversion { id: ref erred } if *erred == id),
            "expected ResumeIndeterminateConversion for {id}, got {error:?}"
        );
        let state = cqrs.load(&id).await.unwrap();
        assert!(
            matches!(state, Some(UsdcRebalance::ConversionFailed { .. })),
            "aggregate must be ConversionFailed when the order never reached Alpaca, got {state:?}"
        );
    }

    #[tokio::test]
    async fn resume_base_to_alpaca_from_bridged_skips_burn_and_mint() {
        let server = MockServer::start();
        let (manager, cqrs, _anvil) = make_resume_test_manager(&server).await;

        let id = UsdcRebalanceId(Uuid::new_v4());
        let amount = usdc("100");
        let amount_received = usdc("99.99");
        let (_burn_tx, mint_tx) =
            advance_to_bridged_base_to_alpaca(&cqrs, &id, amount, amount_received).await;

        // Mock Alpaca deposit poll for the mint tx and the subsequent
        // USDC->USD conversion. No CCTP mocks are configured — a resume from
        // Bridged that tried to re-burn or re-mint would attempt to hit the
        // (un-deployed) CCTP contracts via anvil and fail the test loud.
        let _transfers_mock = server.mock(|when, then| {
            when.method(GET)
                .path("/v1/accounts/904837e3-3b76-47ec-b432-046db621571b/wallets/transfers");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([{
                    "id": "61e7b016-9c91-4a97-b912-615c9d365c9d",
                    "direction": "INCOMING",
                    "amount": "99.99",
                    "usd_value": "99.99",
                    "chain": "ethereum",
                    "asset": "USDC",
                    "from_address": "0x0000000000000000000000000000000000000001",
                    "to_address": "0x1111111111111111111111111111111111111111",
                    "status": "COMPLETE",
                    "tx_hash": format!("{mint_tx:#x}"),
                    "created_at": "2024-01-01T00:00:00Z",
                    "network_fee": "0.5",
                    "fees": "0"
                }]));
        });
        let _conversion_mock = create_conversion_order_mock(&server, "99.99");
        let _get_order_mock = create_get_order_mock(
            &server,
            "61e7b016-9c91-4a97-b912-615c9d365c9d",
            "filled",
            "99.99",
        );

        manager.resume_base_to_alpaca(&id, amount).await.unwrap();

        let final_state = cqrs.load(&id).await.unwrap().expect("aggregate exists");
        assert!(
            matches!(final_state, UsdcRebalance::ConversionComplete { .. }),
            "Expected aggregate to reach ConversionComplete after resume from Bridged, \
             got: {final_state:?}"
        );
    }

    /// Resume from `DepositInitiated` polls the Alpaca deposit and runs the
    /// USDC->USD conversion without re-touching CCTP -- the on-chain mint already
    /// landed, so the deposit is a read-only poll, not a fund-moving call.
    #[tokio::test]
    async fn resume_base_to_alpaca_from_deposit_initiated_polls_and_converts() {
        let server = MockServer::start();
        let (manager, cqrs, _anvil) = make_resume_test_manager(&server).await;

        let id = UsdcRebalanceId(Uuid::new_v4());
        let amount = usdc("100");
        let amount_received = usdc("99.99");
        let (_burn_tx, mint_tx) =
            advance_to_bridged_base_to_alpaca(&cqrs, &id, amount, amount_received).await;
        cqrs.send(
            &id,
            UsdcRebalanceCommand::InitiateDeposit {
                deposit: TransferRef::OnchainTx(mint_tx),
            },
        )
        .await
        .unwrap();

        // Mock the Alpaca deposit poll + USDC->USD conversion. No CCTP mocks --
        // a resume from DepositInitiated that re-burned/re-minted would hit the
        // un-deployed CCTP contracts via anvil and fail the test loud.
        let _transfers_mock = server.mock(|when, then| {
            when.method(GET)
                .path("/v1/accounts/904837e3-3b76-47ec-b432-046db621571b/wallets/transfers");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([{
                    "id": "61e7b016-9c91-4a97-b912-615c9d365c9d",
                    "direction": "INCOMING",
                    "amount": "99.99",
                    "usd_value": "99.99",
                    "chain": "ethereum",
                    "asset": "USDC",
                    "from_address": "0x0000000000000000000000000000000000000001",
                    "to_address": "0x1111111111111111111111111111111111111111",
                    "status": "COMPLETE",
                    "tx_hash": format!("{mint_tx:#x}"),
                    "created_at": "2024-01-01T00:00:00Z",
                    "network_fee": "0.5",
                    "fees": "0"
                }]));
        });
        let _conversion_mock = create_conversion_order_mock(&server, "99.99");
        let _get_order_mock = create_get_order_mock(
            &server,
            "61e7b016-9c91-4a97-b912-615c9d365c9d",
            "filled",
            "99.99",
        );

        manager.resume_base_to_alpaca(&id, amount).await.unwrap();

        let final_state = cqrs.load(&id).await.unwrap().expect("aggregate exists");
        assert!(
            matches!(final_state, UsdcRebalance::ConversionComplete { .. }),
            "Expected ConversionComplete after resume from DepositInitiated, got: {final_state:?}"
        );
    }

    /// `resume_base_to_alpaca` must reject an aggregate carrying the AlpacaToBase
    /// direction -- the guard fires before any side-effecting call, so a transfer
    /// for the other direction can never be driven down the Base->Alpaca path.
    #[tokio::test]
    async fn resume_base_to_alpaca_rejects_alpaca_to_base_direction() {
        let server = MockServer::start();
        let (manager, cqrs, _anvil) = make_resume_test_manager(&server).await;

        let id = UsdcRebalanceId(Uuid::new_v4());
        let amount = usdc("100");
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
        cqrs.send(
            &id,
            UsdcRebalanceCommand::ConfirmConversion {
                filled_amount: amount,
            },
        )
        .await
        .unwrap();

        // No Alpaca/CCTP mocks: the direction guard must reject before any
        // side-effecting call.
        let error = manager
            .resume_base_to_alpaca(&id, amount)
            .await
            .unwrap_err();
        assert!(
            matches!(
                error,
                UsdcTransferError::ResumeDirectionMismatch {
                    direction: RebalanceDirection::AlpacaToBase,
                    ..
                }
            ),
            "Expected ResumeDirectionMismatch for AlpacaToBase resume, got: {error:?}"
        );
    }

    struct DualChainCctp {
        _base_anvil: AnvilInstance,
        _ethereum_anvil: AnvilInstance,
        base_endpoint: String,
        ethereum_endpoint: String,
        token_messenger: Address,
        message_transmitter: Address,
        bot_key: B256,
        bot_address: Address,
        attester_key: B256,
    }

    /// Deploys CCTP V2 on two fresh Anvil chains (Base + Ethereum), points the
    /// canonical USDC address at a mint/burn token on both, links them, and
    /// funds the bot wallet with USDC to burn. Deterministic deploys give
    /// identical contract addresses on both chains, so one token messenger /
    /// message transmitter pair drives the bridge.
    async fn deploy_dual_chain_cctp() -> DualChainCctp {
        let base_anvil = Anvil::new().spawn();
        let ethereum_anvil = Anvil::new().chain_id(1u64).spawn();
        let base_endpoint = base_anvil.endpoint();
        let ethereum_endpoint = ethereum_anvil.endpoint();

        // Account 0 is the bot wallet (burns/mints); account 1 is the CCTP
        // deployer, kept distinct to avoid nonce collisions on Base's instant
        // mining.
        let bot_key = B256::from_slice(&base_anvil.keys()[0].to_bytes());
        let deployer_key = B256::from_slice(&base_anvil.keys()[1].to_bytes());
        let bot_address = PrivateKeySigner::from_bytes(&bot_key).unwrap().address();

        let attester_key = B256::random();
        let attester_address = PrivateKeySigner::from_bytes(&attester_key)
            .unwrap()
            .address();

        // Fund the deployer on Base (Ethereum Anvil pre-funds its own accounts).
        let base_provider = ProviderBuilder::new()
            .connect(&base_endpoint)
            .await
            .unwrap();
        let ethereum_provider = ProviderBuilder::new()
            .connect(&ethereum_endpoint)
            .await
            .unwrap();
        let deployer_address = PrivateKeySigner::from_bytes(&deployer_key)
            .unwrap()
            .address();
        base_provider
            .anvil_set_balance(deployer_address, U256::from(10).pow(U256::from(20)))
            .await
            .unwrap();

        let mut ethereum_cctp =
            deploy_cctp_on_chain(&ethereum_endpoint, &deployer_key, 0, attester_address)
                .await
                .unwrap();
        let mut base_cctp =
            deploy_cctp_on_chain(&base_endpoint, &deployer_key, 6, attester_address)
                .await
                .unwrap();

        // The bridge uses USDC at the canonical address; place the mint/burn
        // token bytecode there on both chains so CCTP can mint/burn it.
        ethereum_provider
            .anvil_set_code(USDC_ADDRESS, TestMintBurnToken::DEPLOYED_BYTECODE.clone())
            .await
            .unwrap();
        base_provider
            .anvil_set_code(USDC_ADDRESS, TestMintBurnToken::DEPLOYED_BYTECODE.clone())
            .await
            .unwrap();
        set_max_burn_amount(
            &ethereum_endpoint,
            &deployer_key,
            ethereum_cctp.token_minter,
            USDC_ADDRESS,
            U256::from(1_000_000_000_000u64),
        )
        .await
        .unwrap();
        set_max_burn_amount(
            &base_endpoint,
            &deployer_key,
            base_cctp.token_minter,
            USDC_ADDRESS,
            U256::from(1_000_000_000_000u64),
        )
        .await
        .unwrap();
        ethereum_cctp.usdc = USDC_ADDRESS;
        base_cctp.usdc = USDC_ADDRESS;

        link_chains(
            &ethereum_endpoint,
            &base_endpoint,
            &deployer_key,
            &ethereum_cctp,
            &base_cctp,
        )
        .await
        .unwrap();

        // Fund the bot's Base wallet so it can burn.
        mint_usdc(
            &base_endpoint,
            &deployer_key,
            USDC_ADDRESS,
            bot_address,
            U256::from(1_000_000_000_000u64),
        )
        .await
        .unwrap();

        DualChainCctp {
            token_messenger: base_cctp.token_messenger,
            message_transmitter: base_cctp.message_transmitter,
            base_endpoint,
            ethereum_endpoint,
            bot_key,
            bot_address,
            attester_key,
            _base_anvil: base_anvil,
            _ethereum_anvil: ethereum_anvil,
        }
    }

    /// Resume from `Attested` after the CCTP mint already landed on-chain must
    /// ADOPT that mint (scan + `ConfirmBridging`) rather than re-calling
    /// `receiveMessage`, which reverts on the already-used nonce and would latch
    /// a terminal `BridgingFailed` for a transfer whose USDC was in fact minted.
    ///
    /// Deterministically reproduces the post-mint crash window (mint on-chain,
    /// aggregate still `Attested`, `ConfirmBridging` not yet persisted) by
    /// producing a real mint over dual-chain CCTP, then driving the aggregate to
    /// `Attested` and resuming. Reaching `ConversionComplete` proves adoption: a
    /// re-mint would revert on the used nonce and never get past the bridge.
    #[tokio::test]
    async fn resume_base_to_alpaca_from_attested_adopts_existing_mint() {
        let chains = deploy_dual_chain_cctp().await;

        let attestation = CctpAttestationMock::start().await;
        let _watcher = attestation
            .start_watcher(
                ProviderBuilder::new()
                    .connect(&chains.ethereum_endpoint)
                    .await
                    .unwrap(),
                ProviderBuilder::new()
                    .connect(&chains.base_endpoint)
                    .await
                    .unwrap(),
                chains.attester_key,
            )
            .await
            .unwrap();

        let cctp_bridge = Arc::new(
            CctpBridge::try_from_ctx(CctpCtx {
                usdc_ethereum: USDC_ADDRESS,
                usdc_base: USDC_ADDRESS,
                ethereum_wallet: create_test_wallet(&chains.ethereum_endpoint, &chains.bot_key),
                base_wallet: create_test_wallet(&chains.base_endpoint, &chains.bot_key),
                circle_api_base: attestation.base_url(),
                token_messenger: chains.token_messenger,
                message_transmitter: chains.message_transmitter,
            })
            .unwrap(),
        );

        // Produce a real on-chain mint to the market maker wallet.
        let market_maker_wallet = address!("0x1111111111111111111111111111111111111111");
        let amount = usdc("100");
        let amount_u256 = usdc_to_u256(amount).unwrap();
        let burn_receipt = cctp_bridge
            .burn(
                BridgeDirection::BaseToEthereum,
                amount_u256,
                market_maker_wallet,
            )
            .await
            .unwrap();
        let attestation_response = cctp_bridge
            .poll_attestation(BridgeDirection::BaseToEthereum, burn_receipt.tx)
            .await
            .unwrap();
        // Capture the scan bound where production does -- after attestation,
        // immediately before minting -- so the adopt window matches the real
        // flow and a mint that landed earlier would fall outside it.
        let mint_scan_from_block = cctp_bridge
            .destination_block(BridgeDirection::BaseToEthereum)
            .await
            .unwrap();
        let mint_receipt = cctp_bridge
            .mint(BridgeDirection::BaseToEthereum, &attestation_response)
            .await
            .unwrap();

        // Build the manager on the same bridge with Alpaca mocked.
        let server = MockServer::start();
        let alpaca_broker = Arc::new(create_test_broker_service(&server).await);
        let alpaca_wallet = Arc::new(create_test_wallet_service(&server));
        let pool = crate::test_utils::setup_test_db().await;
        let (_vault_registry_store, vault_registry_projection) =
            StoreBuilder::<VaultRegistry>::new(pool)
                .build(())
                .await
                .unwrap();
        let vault_service = RaindexService::new(
            create_test_wallet(&chains.base_endpoint, &chains.bot_key),
            ORDERBOOK_ADDRESS,
            vault_registry_projection,
            chains.bot_address,
        );
        let cqrs = create_test_store_instance().await;
        let manager = CrossVenueCashTransfer::new(
            alpaca_broker,
            alpaca_wallet,
            Arc::clone(&cctp_bridge),
            Arc::new(vault_service),
            cqrs.clone(),
            market_maker_wallet,
            TEST_VAULT_ID,
        );

        // Drive the aggregate to `Attested`, recording the real burn tx and the
        // captured scan bound. cctp_nonce is irrelevant to the adopt path.
        let id = UsdcRebalanceId(Uuid::new_v4());
        cqrs.send(
            &id,
            UsdcRebalanceCommand::Initiate {
                direction: RebalanceDirection::BaseToAlpaca,
                amount,
                withdrawal: TransferRef::OnchainTx(burn_receipt.tx),
            },
        )
        .await
        .unwrap();
        cqrs.send(&id, UsdcRebalanceCommand::ConfirmWithdrawal)
            .await
            .unwrap();
        cqrs.send(
            &id,
            UsdcRebalanceCommand::InitiateBridging {
                burn_tx: burn_receipt.tx,
            },
        )
        .await
        .unwrap();
        cqrs.send(
            &id,
            UsdcRebalanceCommand::ReceiveAttestation {
                attestation: attestation_response.as_bytes().to_vec(),
                cctp_nonce: attestation_response.nonce(),
                mint_scan_from_block,
            },
        )
        .await
        .unwrap();

        // Downstream Alpaca legs after adoption: deposit detection (by the mint
        // tx) then the USDC->USD conversion.
        let _transfers_mock = server.mock(|when, then| {
            when.method(GET)
                .path("/v1/accounts/904837e3-3b76-47ec-b432-046db621571b/wallets/transfers");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([{
                    "id": "61e7b016-9c91-4a97-b912-615c9d365c9d",
                    "direction": "INCOMING",
                    "amount": "100",
                    "usd_value": "100",
                    "chain": "ethereum",
                    "asset": "USDC",
                    "from_address": "0x0000000000000000000000000000000000000001",
                    "to_address": "0x1111111111111111111111111111111111111111",
                    "status": "COMPLETE",
                    "tx_hash": format!("{:#x}", mint_receipt.tx),
                    "created_at": "2024-01-01T00:00:00Z",
                    "network_fee": "0",
                    "fees": "0"
                }]));
        });
        let _conversion_mock = create_conversion_order_mock(&server, "100");
        let _get_order_mock = create_get_order_mock(
            &server,
            "61e7b016-9c91-4a97-b912-615c9d365c9d",
            "filled",
            "100",
        );

        // A re-mint here would revert on the already-used nonce and latch
        // `BridgingFailed`; reaching `ConversionComplete` proves the mint was
        // adopted from the chain scan instead.
        manager.resume_base_to_alpaca(&id, amount).await.unwrap();

        let final_state = cqrs.load(&id).await.unwrap().expect("aggregate exists");
        assert!(
            matches!(final_state, UsdcRebalance::ConversionComplete { .. }),
            "Expected ConversionComplete after adopting the existing mint on resume, \
             got: {final_state:?}"
        );
    }

    #[tokio::test]
    async fn resume_base_to_alpaca_from_bridged_makes_no_fund_moving_deposit_call() {
        let server = MockServer::start();
        let (manager, cqrs, _anvil) = make_resume_test_manager(&server).await;

        let id = UsdcRebalanceId(Uuid::new_v4());
        let amount = usdc("100");
        let amount_received = usdc("99.99");
        let (_burn_tx, mint_tx) =
            advance_to_bridged_base_to_alpaca(&cqrs, &id, amount, amount_received).await;

        // The CCTP mint already credited the Alpaca-monitored address, so resume
        // only OBSERVES the deposit via this GET poll, never re-issues a transfer.
        let deposit_poll_mock = server.mock(|when, then| {
            when.method(GET)
                .path("/v1/accounts/904837e3-3b76-47ec-b432-046db621571b/wallets/transfers");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([{
                    "id": "61e7b016-9c91-4a97-b912-615c9d365c9d",
                    "direction": "INCOMING",
                    "amount": "99.99",
                    "usd_value": "99.99",
                    "chain": "ethereum",
                    "asset": "USDC",
                    "from_address": "0x0000000000000000000000000000000000000001",
                    "to_address": "0x1111111111111111111111111111111111111111",
                    "status": "COMPLETE",
                    "tx_hash": format!("{mint_tx:#x}"),
                    "created_at": "2024-01-01T00:00:00Z",
                    "network_fee": "0.5",
                    "fees": "0"
                }]));
        });
        let _conversion_mock = create_conversion_order_mock(&server, "99.99");
        let _get_order_mock = create_get_order_mock(
            &server,
            "61e7b016-9c91-4a97-b912-615c9d365c9d",
            "filled",
            "99.99",
        );

        // A fund-moving Alpaca transfer is a POST to /wallets/transfers, and the
        // withdrawal path GETs /wallets/whitelists. Resume from Bridged must do
        // NEITHER -- the deposit was effected by the mint, not an Alpaca API call.
        let transfers_post_mock = server.mock(|when, then| {
            when.method(POST)
                .path("/v1/accounts/904837e3-3b76-47ec-b432-046db621571b/wallets/transfers");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({}));
        });
        let whitelist_mock = server.mock(|when, then| {
            when.method(GET)
                .path("/v1/accounts/904837e3-3b76-47ec-b432-046db621571b/wallets/whitelists");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([]));
        });

        manager.resume_base_to_alpaca(&id, amount).await.unwrap();

        deposit_poll_mock.assert();
        assert_eq!(
            transfers_post_mock.calls(),
            0,
            "resume from Bridged must NOT POST a fund-moving Alpaca transfer; the CCTP \
             mint already deposited directly to the Alpaca address",
        );
        assert_eq!(
            whitelist_mock.calls(),
            0,
            "resume from Bridged must NOT touch the withdrawal whitelist path",
        );

        let final_state = cqrs.load(&id).await.unwrap().expect("aggregate exists");
        assert!(
            matches!(final_state, UsdcRebalance::ConversionComplete { .. }),
            "Expected ConversionComplete after resume from Bridged, got: {final_state:?}",
        );
    }

    /// Hypothesis: resuming `resume_alpaca_to_base` from `DepositInitiated`
    /// must re-verify the persisted on-chain deposit tx (status +
    /// configured confirmation depth) before transitioning to
    /// `DepositConfirmed`. A reorg, mempool drop, or partially-mined tx
    /// observed at the time of `InitiateDeposit` can later be invalid
    /// when the process restarts; advancing solely from persisted state
    /// would mark the rebalance complete while the deposit isn't on
    /// chain. The handler must surface the unverified tx as an error so
    /// the worker retries instead of silently confirming.
    #[tokio::test]
    async fn resume_alpaca_to_base_from_deposit_initiated_reverifies_deposit_tx() {
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
            cqrs.clone(),
            market_maker_wallet,
            TEST_VAULT_ID,
        );

        let id = UsdcRebalanceId(Uuid::new_v4());
        let amount = usdc("100");
        // A persisted deposit tx that never landed on chain (reorged out,
        // dropped, or never mined): the live anvil node has no record of it, so
        // `confirm_tx` cannot confirm it. Resume must surface that as an error
        // rather than blindly transitioning the aggregate to complete. If the
        // handler skipped re-verification, confirm would not run and the resume
        // would return `Ok` -- so the error result guards that regression.
        let deposit_tx =
            fixed_bytes!("0xdddd000000000000000000000000000000000000000000000000000000000001");

        advance_to_deposit_initiated_alpaca_to_base(&cqrs, &id, amount, deposit_tx).await;

        let result = manager.resume_alpaca_to_base(&id, amount).await;

        assert!(
            result.is_err(),
            "Resume must surface the unverified deposit as an error, got Ok: {result:?}"
        );

        let final_state = cqrs.load(&id).await.unwrap().expect("aggregate exists");
        assert!(
            matches!(final_state, UsdcRebalance::DepositInitiated { .. }),
            "Aggregate must remain in `DepositInitiated` when the deposit tx \
             cannot be confirmed; got: {final_state:?}"
        );
    }
}

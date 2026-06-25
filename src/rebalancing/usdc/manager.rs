//! [`CrossVenueCashTransfer`] orchestrates USDC cross-venue transfers.
//!
//! Coordinates between `AlpacaBrokerApi`, `AlpacaWalletService`,
//! `CctpBridge`, `RaindexService`, and the `UsdcRebalance` aggregate to
//! execute USDC transfers between Alpaca and Base.

use alloy::primitives::{Address, B256, TxHash, U256};
use chrono::{DateTime, Utc};
use rain_math_float::Float;
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, info, instrument, warn};
use uuid::Uuid;

use st0x_bridge::cctp::{AttestationResponse, CctpBridge, CctpError};
use st0x_bridge::{Attestation, Bridge, BridgeDirection, BurnReceipt, BurnTxStatus, MintReceipt};
use st0x_event_sorcery::Store;
use st0x_evm::{USDC_BASE, Wallet};
use st0x_execution::alpaca_broker_api::CryptoOrderResponse;
use st0x_execution::{
    AlpacaTransferId, AlpacaWalletError, AlpacaWalletService, ClientOrderId, ConversionDirection,
    CryptoOrderOutcome, Network, Positive, TokenSymbol, Transfer, TransferStatus,
};
use st0x_finance::Usdc;
use st0x_raindex::{Raindex, RaindexError, RaindexService, RaindexVaultId};

use super::UsdcTransferError;
use crate::telemetry::broker::InstrumentedAlpacaBroker;
use crate::usdc_rebalance::{
    ConversionAmounts, RebalanceDirection, TransferRef, UsdcRebalance, UsdcRebalanceCommand,
    UsdcRebalanceId,
};

/// Attempts to commit `RecordPendingBurn` in the detached submit-and-record
/// task before failing terminally. The burn is already broadcast (irreversible)
/// by the time we record, so a transient SQLite lock must not lose the hash --
/// retrying a few times clears the common contention case.
const BURN_RECORD_ATTEMPTS: u32 = 3;

/// Base delay between `RecordPendingBurn` attempts; scaled linearly by attempt
/// number (100ms, 200ms). A SQLite write-lock clears fast, so a short linear
/// backoff is enough.
const BURN_RECORD_RETRY_BACKOFF: Duration = Duration::from_millis(100);

/// Upper bound on the whole `submit_burn` call -- the allowance check, fee query,
/// AND the `depositForBurn` broadcast. These normally return in seconds; the
/// bound exists so a black-holed RPC connection cannot hang the burn
/// indefinitely. On elapse the submission is treated as INCONCLUSIVE (the
/// broadcast may already have reached the network) and the transfer fails closed
/// (`BurnSubmitInconclusive`) rather than reburning. Bounding the whole call (not
/// just the broadcast) means a slow pre-broadcast step can also trip the
/// fail-closed even though no burn went out -- a deliberately conservative
/// trade-off: an unnecessary operator reconciliation is far cheaper than a
/// double burn, and at 120 s a non-hung allowance/fee query never reaches it.
///
/// MUST stay well below the job's per-attempt `transfer_attempt_timeout` (1h in
/// prod) so this fail-closed result propagates out of the detached task while the
/// outer per-attempt-timeout await is still active -- otherwise the outer timeout
/// could cancel the await and redrive before the fail-closed surfaced.
const BURN_BROADCAST_TIMEOUT: Duration = Duration::from_secs(120);

/// Tuning parameters for the USDC settlement flow.
///
/// Bundled together so constructors that require these values stay within
/// the 8-argument clippy threshold.
#[derive(Debug)]
pub(crate) struct UsdcSettlementParams {
    pub(crate) attestation_retry_deadline: Duration,
    pub(crate) required_confirmations: u64,
    /// Circle attestation/fee API base URL (test-only override; production
    /// builds use the [`st0x_bridge::cctp::CIRCLE_API_BASE`] constant).
    #[cfg(feature = "test-support")]
    pub(crate) circle_api_base: String,
    /// `TokenMessengerV2` contract address (test-only override).
    #[cfg(feature = "test-support")]
    pub(crate) token_messenger: Address,
    /// `MessageTransmitterV2` contract address (test-only override).
    #[cfg(feature = "test-support")]
    pub(crate) message_transmitter: Address,
}

/// Ethereum-specific helpers used by [`CrossVenueCashTransfer`] that fall
/// outside the generic [`Bridge`] interface (chain-specific queries, USDC
/// transfers, and scan utilities). Defined here so the struct can be generic
/// over `B` while still calling these operations through the trait in the
/// shared impl block. [`CctpBridge`] implements this by delegating to its
/// inherent methods. Tests implement it with `unimplemented!()` stubs for
/// code paths that do not exercise these operations.
#[async_trait::async_trait]
pub(crate) trait UsdcBridgeHelper: Send + Sync + 'static {
    /// Returns the number of confirmations for `tx_hash` on Ethereum, or
    /// `None` if the transaction has not yet been mined.
    async fn ethereum_tx_confirmations(&self, tx_hash: TxHash) -> Result<Option<u64>, CctpError>;

    /// Returns the block in which `tx_hash` was mined on Ethereum.
    async fn ethereum_tx_block(&self, tx_hash: TxHash) -> Result<u64, CctpError>;

    /// Returns the USDC balance of `holder` on Ethereum.
    async fn ethereum_usdc_balance(&self, holder: Address) -> Result<U256, CctpError>;

    /// Sends `amount` USDC (6-decimal) from the bot wallet to `to` on
    /// Ethereum, waits for confirmation, and returns the transaction hash.
    async fn send_usdc_on_ethereum(&self, to: Address, amount: U256) -> Result<TxHash, CctpError>;

    /// Scans Ethereum for a USDC `Transfer(from, to, value == amount)` event
    /// at or after `from_block`, returning the most recent matching tx hash.
    async fn find_recent_usdc_transfer(
        &self,
        from: Address,
        to: Address,
        amount: U256,
        from_block: u64,
    ) -> Result<Option<TxHash>, CctpError>;
}

#[async_trait::async_trait]
impl<EthWallet: Wallet, BaseWallet: Wallet> UsdcBridgeHelper for CctpBridge<EthWallet, BaseWallet> {
    async fn ethereum_tx_confirmations(&self, tx_hash: TxHash) -> Result<Option<u64>, CctpError> {
        self.ethereum_tx_confirmations(tx_hash).await
    }

    async fn ethereum_tx_block(&self, tx_hash: TxHash) -> Result<u64, CctpError> {
        self.ethereum_tx_block(tx_hash).await
    }

    async fn ethereum_usdc_balance(&self, holder: Address) -> Result<U256, CctpError> {
        self.ethereum_usdc_balance(holder).await
    }

    async fn send_usdc_on_ethereum(&self, to: Address, amount: U256) -> Result<TxHash, CctpError> {
        self.send_usdc_on_ethereum(to, amount).await
    }

    async fn find_recent_usdc_transfer(
        &self,
        from: Address,
        to: Address,
        amount: U256,
        from_block: u64,
    ) -> Result<Option<TxHash>, CctpError> {
        self.find_recent_usdc_transfer(from, to, amount, from_block)
            .await
    }
}

/// Classifies a failed vault `withdraw_usdc`. An atomic
/// [`RaindexError::InsufficientVaultLiquidity`] revert means the vault could not
/// cover the request; it withdrew nothing (atomic revert), so retrying only
/// reverts again until the vault is refunded. It is surfaced as a distinct,
/// contextful error the job latches for operator reconciliation (no auto-retry)
/// rather than the opaque `Vault` wrap, which the job redrives. Any other error
/// keeps the opaque wrap and the normal redrive path.
fn classify_vault_withdrawal_error(error: RaindexError) -> UsdcTransferError {
    match error {
        RaindexError::InsufficientVaultLiquidity {
            token,
            requested,
            received,
        } => {
            warn!(target: "rebalance", %token, %requested, %received, "Vault under-funded on withdraw; latching for operator reconciliation (no auto-retry)");
            UsdcTransferError::InsufficientVaultLiquidity {
                token,
                requested,
                received,
            }
        }
        other => {
            warn!(target: "rebalance", "Vault withdrawal failed: {other}");
            UsdcTransferError::Vault(other)
        }
    }
}

/// Orchestrates USDC rebalancing between Alpaca (Ethereum) and Rain (Base).
///
/// # Type Parameters
///
/// * `Chain` - Wallet type used for both Ethereum and Base chains
/// * `B` - Bridge implementation; defaults to [`CctpBridge<Chain, Chain>`]
pub(crate) struct CrossVenueCashTransfer<Chain: Wallet, B = CctpBridge<Chain, Chain>> {
    alpaca_broker: InstrumentedAlpacaBroker,
    alpaca_wallet: Arc<AlpacaWalletService>,
    cctp_bridge: Arc<B>,
    raindex: Arc<RaindexService<Chain>>,
    cqrs: Arc<Store<UsdcRebalance>>,
    market_maker_wallet: Address,
    vault_id: RaindexVaultId,
    attestation_retry_deadline: Duration,
    required_confirmations: u64,
}

enum AttestationPollOutcome {
    Received(AttestationResponse),
    TimedOut,
}

impl<
    Chain: Wallet,
    B: Bridge<Error = CctpError, Attestation = AttestationResponse> + UsdcBridgeHelper,
> CrossVenueCashTransfer<Chain, B>
{
    pub(crate) fn new(
        alpaca_broker: InstrumentedAlpacaBroker,
        alpaca_wallet: Arc<AlpacaWalletService>,
        cctp_bridge: Arc<B>,
        raindex: Arc<RaindexService<Chain>>,
        cqrs: Arc<Store<UsdcRebalance>>,
        market_maker_wallet: Address,
        vault_id: RaindexVaultId,
        settlement: &UsdcSettlementParams,
    ) -> Self {
        Self {
            alpaca_broker,
            alpaca_wallet,
            cctp_bridge,
            raindex,
            cqrs,
            market_maker_wallet,
            vault_id,
            attestation_retry_deadline: settlement.attestation_retry_deadline,
            required_confirmations: settlement.required_confirmations,
        }
    }

    fn attestation_retry_deadline_at(
        &self,
        id: &UsdcRebalanceId,
    ) -> Result<DateTime<Utc>, UsdcTransferError> {
        let duration =
            chrono::Duration::from_std(self.attestation_retry_deadline).map_err(|_| {
                UsdcTransferError::AttestationRetryDeadlineOverflow {
                    id: id.clone(),
                    retry_deadline: self.attestation_retry_deadline,
                }
            })?;

        Utc::now().checked_add_signed(duration).ok_or_else(|| {
            UsdcTransferError::AttestationRetryDeadlineOverflow {
                id: id.clone(),
                retry_deadline: self.attestation_retry_deadline,
            }
        })
    }

    async fn record_attestation_timeout(
        &self,
        id: &UsdcRebalanceId,
    ) -> Result<(), UsdcTransferError> {
        let retry_deadline_at = self.attestation_retry_deadline_at(id)?;

        self.cqrs
            .send(
                id,
                UsdcRebalanceCommand::TimeoutAttestation { retry_deadline_at },
            )
            .await?;

        warn!(
            target: "rebalance",
            %id,
            %retry_deadline_at,
            "Circle attestation poll timed out; leaving USDC rebalance retryable until the deadline"
        );

        Ok(())
    }

    /// Polls Circle for the attestation, classifying the result.
    ///
    /// A `CctpError::AttestationTimeout` is a recoverable, retryable condition
    /// (the attestation simply has not arrived yet) and maps to
    /// [`AttestationPollOutcome::TimedOut`]. Any other `CctpError` is a hard
    /// failure (malformed response, placeholder nonce, transport error): the
    /// USDC is already burned, so the aggregate is moved to `BridgingFailed`
    /// via `FailBridging` -- surfacing a durable terminal state for operator
    /// reconciliation rather than wedging the rebalance in a non-terminal state
    /// while the worker exhausts retries and opens its recovering circuit.
    async fn poll_cctp_attestation(
        &self,
        id: &UsdcRebalanceId,
        direction: BridgeDirection,
        burn_tx: TxHash,
    ) -> Result<AttestationPollOutcome, UsdcTransferError> {
        match self.cctp_bridge.poll_attestation(direction, burn_tx).await {
            Ok(response) => Ok(AttestationPollOutcome::Received(response)),
            Err(CctpError::AttestationTimeout { attempts, source }) => {
                warn!(
                    target: "rebalance",
                    attempts,
                    ?source,
                    "Circle attestation poll timed out"
                );
                Ok(AttestationPollOutcome::TimedOut)
            }
            Err(error) => {
                warn!(target: "rebalance", %id, ?error, "Attestation polling failed");
                self.cqrs
                    .send(
                        id,
                        UsdcRebalanceCommand::FailBridging {
                            reason: format!("attestation polling failed: {error}"),
                        },
                    )
                    .await?;
                Err(UsdcTransferError::Cctp(Box::new(error)))
            }
        }
    }

    /// Polls Circle once, failing the bridge if the retry deadline has already
    /// elapsed.
    ///
    /// The deadline is a soft bound: it is checked here, between redrive
    /// attempts, not inside the poll. A poll that begins before the deadline
    /// runs to completion (up to its own internal timeout), so the effective
    /// cutoff is the first redrive at or after `retry_deadline_at`, which can
    /// overshoot by one poll window plus the redrive delay. Negligible at the
    /// production 24h default.
    async fn poll_cctp_attestation_until_deadline(
        &self,
        id: &UsdcRebalanceId,
        direction: BridgeDirection,
        burn_tx: TxHash,
        retry_deadline_at: DateTime<Utc>,
    ) -> Result<AttestationPollOutcome, UsdcTransferError> {
        if Utc::now() >= retry_deadline_at {
            warn!(
                target: "rebalance",
                %id,
                %retry_deadline_at,
                "Circle attestation retry deadline elapsed"
            );
            self.cqrs
                .send(
                    id,
                    UsdcRebalanceCommand::FailBridging {
                        reason: "attestation retry deadline elapsed".to_string(),
                    },
                )
                .await?;
            return Err(UsdcTransferError::AttestationRetryDeadlineElapsed { id: id.clone() });
        }

        self.poll_cctp_attestation(id, direction, burn_tx).await
    }

    async fn record_attestation(
        &self,
        id: &UsdcRebalanceId,
        direction: BridgeDirection,
        response: &AttestationResponse,
    ) -> Result<(), UsdcTransferError> {
        // Capture the destination head before minting so a crash before
        // `ConfirmBridging` resumes by scanning for the already-submitted mint
        // instead of re-minting, which reverts on the already-used CCTP nonce.
        // A lookup failure here is transient (a destination RPC/read hiccup):
        // the aggregate is still in `Bridging`/`AwaitingAttestation` with no
        // attestation recorded yet, and both directions have resume entry points
        // for those states that re-poll the attestation idempotently. So
        // propagate the error and let the job retry rather than latching a
        // terminal `FailBridging` and stranding already-burned USDC behind manual
        // reconciliation.
        let mint_scan_from_block = match self.cctp_bridge.destination_block(direction).await {
            Ok(block) => block,
            Err(error) => {
                warn!(
                    target: "rebalance",
                    %id,
                    ?error,
                    "Destination head lookup failed; leaving aggregate in \
                     Bridging/AwaitingAttestation for retry"
                );
                return Err(UsdcTransferError::Cctp(Box::new(error)));
            }
        };

        self.cqrs
            .send(
                id,
                UsdcRebalanceCommand::ReceiveAttestation {
                    attestation: response.as_bytes().to_vec(),
                    cctp_nonce: response.nonce(),
                    message: response.message_bytes().to_vec(),
                    mint_scan_from_block,
                },
            )
            .await?;

        info!(target: "rebalance", "Circle attestation received");
        Ok(())
    }

    /// Obtains the [`AttestationResponse`] for an `Attested` resume.
    ///
    /// When the full CCTP message envelope was persisted with the attestation
    /// (the common case since envelope persistence landed), the response is
    /// reconstructed from stored data and no Circle call is made -- making the
    /// `Attested` resume deterministic and offline-capable. Transfers whose
    /// `BridgeAttestationReceived` predates the envelope field carry no message,
    /// so they fall back to re-polling Circle (idempotent for a completed
    /// attestation).
    async fn attested_attestation_response(
        &self,
        id: &UsdcRebalanceId,
        mint_direction: BridgeDirection,
        burn_tx: TxHash,
        attestation: Vec<u8>,
        cctp_nonce: B256,
        message: Option<Vec<u8>>,
    ) -> Result<AttestationResponse, UsdcTransferError> {
        let Some(message) = message else {
            warn!(
                target: "rebalance",
                %id,
                "Attested transfer predates envelope persistence; re-polling Circle for the attestation"
            );
            return match self
                .poll_cctp_attestation(id, mint_direction, burn_tx)
                .await?
            {
                AttestationPollOutcome::Received(response) => Ok(response),
                AttestationPollOutcome::TimedOut => {
                    Err(UsdcTransferError::AttestationTimedOut { id: id.clone() })
                }
            };
        };

        info!(
            target: "rebalance",
            %id,
            "Reconstructing attestation from the persisted message envelope; no Circle re-poll needed"
        );

        // A reconstruction failure means the persisted envelope is unusable
        // (corrupt bytes, placeholder nonce). The USDC is already burned, so move
        // the aggregate to `BridgingFailed` for operator reconciliation rather
        // than wedging it in `Attested` while the worker exhausts retries --
        // mirroring the hard-error arm of `poll_cctp_attestation`.
        let response = match self
            .cctp_bridge
            .reconstruct_attestation(message, attestation)
        {
            Ok(response) => response,
            Err(error) => {
                warn!(target: "rebalance", %id, ?error, "Reconstructing attestation from the persisted envelope failed");
                self.cqrs
                    .send(
                        id,
                        UsdcRebalanceCommand::FailBridging {
                            reason: format!("attestation reconstruction failed: {error}"),
                        },
                    )
                    .await?;
                return Err(UsdcTransferError::Cctp(Box::new(error)));
            }
        };

        // The nonce is persisted twice: standalone (`cctp_nonce`) and embedded in
        // the envelope. Both originate from the same attestation, so a mismatch
        // means the persisted record is internally inconsistent (storage
        // corruption or manual repair). Refuse to mint against an unverifiable
        // nonce; the burn is durable, so surface a terminal failure for operator
        // reconciliation.
        let reconstructed = response.nonce();
        if reconstructed != cctp_nonce {
            warn!(target: "rebalance", %id, %reconstructed, recorded = %cctp_nonce, "Reconstructed attestation nonce does not match the recorded cctp_nonce");
            self.cqrs
                .send(
                    id,
                    UsdcRebalanceCommand::FailBridging {
                        reason: format!(
                            "attestation nonce mismatch: reconstructed {reconstructed}, recorded {cctp_nonce}"
                        ),
                    },
                )
                .await?;
            return Err(UsdcTransferError::AttestationNonceMismatch {
                id: id.clone(),
                recorded: cctp_nonce,
                reconstructed,
            });
        }

        Ok(response)
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
        let correlation_id = ClientOrderId::from_uuid(Uuid::new_v4());

        info!(target: "rebalance", %amount, %correlation_id, "Starting USD to USDC conversion");

        // Record intent BEFORE placing order so we can track failures
        self.cqrs
            .send(
                id,
                UsdcRebalanceCommand::InitiateConversion {
                    direction: RebalanceDirection::AlpacaToBase,
                    amount,
                    order_id: correlation_id.clone(),
                },
            )
            .await?;

        let alpaca_amount = amount.inner();

        let order = match self
            .alpaca_broker
            .convert_usdc_usd(
                alpaca_amount,
                ConversionDirection::UsdToUsdc,
                &correlation_id,
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

        let conversion = self
            .record_conversion_or_fail(id, &correlation_id, &order, ConversionDirection::UsdToUsdc)
            .await?;
        let received_amount = conversion.received_amount;

        self.cqrs
            .send(id, UsdcRebalanceCommand::ConfirmConversion { conversion })
            .await?;

        info!(target: "rebalance",
            order_id = %order.id,
            requested = ?amount,
            source_amount = ?conversion.source_amount,
            received_amount = ?conversion.received_amount,
            "USD to USDC conversion completed"
        );
        Ok(received_amount)
    }

    /// Converts USDC to USD buying power.
    ///
    /// Used at the end of BaseToAlpaca flow, after deposit is confirmed.
    /// Places a sell order on USDC/USD and polls until filled.
    ///
    /// Returns the actual USD proceeds credited at Alpaca.
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
        let correlation_id = ClientOrderId::from_uuid(Uuid::new_v4());

        info!(target: "rebalance", %amount, %correlation_id, "Starting USDC to USD conversion");

        // Record intent BEFORE placing order so we can track failures
        self.cqrs
            .send(
                id,
                UsdcRebalanceCommand::InitiatePostDepositConversion {
                    order_id: correlation_id.clone(),
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
                &correlation_id,
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

        let conversion = self
            .record_conversion_or_fail(id, &correlation_id, &order, ConversionDirection::UsdcToUsd)
            .await?;
        let proceeds = conversion.received_amount;

        self.cqrs
            .send(id, UsdcRebalanceCommand::ConfirmConversion { conversion })
            .await?;

        info!(target: "rebalance",
            order_id = %order.id,
            requested = ?amount,
            source_amount = ?conversion.source_amount,
            received_amount = ?conversion.received_amount,
            "USDC to USD conversion completed"
        );
        Ok(proceeds)
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
    // A single state-dispatch match with one trivial arm per aggregate state.
    // Splitting it would scatter the resume routing across pointless helpers; per
    // the project guidance, suppress the length lint instead.
    #[allow(clippy::too_many_lines)]
    #[instrument(target = "rebalance", skip(self), fields(%id, %amount), level = tracing::Level::DEBUG)]
    pub(crate) async fn resume_alpaca_to_base(
        &self,
        id: &UsdcRebalanceId,
        amount: Usdc,
    ) -> Result<(), UsdcTransferError> {
        use RebalanceDirection::*;
        use UsdcRebalance::*;

        let state = self.cqrs.load(id).await?;

        info!(
            target: "rebalance",
            ?state,
            "Resuming Alpaca->Base transfer from aggregate state",
        );

        match state {
            None => self.execute_alpaca_to_base(id, amount).await,

            Some(Converting {
                direction: AlpacaToBase,
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

            Some(ConversionComplete {
                direction: AlpacaToBase,
                conversion,
                ..
            }) => {
                self.continue_alpaca_to_base_from_conversion_complete(
                    id,
                    conversion.received_amount,
                )
                .await
            }

            Some(Withdrawing {
                direction: AlpacaToBase,
                amount,
                withdrawal_ref,
                initiated_at,
                ..
            }) => {
                let TransferRef::AlpacaId(transfer_id) = withdrawal_ref else {
                    return Err(UsdcTransferError::WithdrawalRefMustBeAlpacaId { id: id.clone() });
                };

                let withdrawal_tx = self
                    .poll_and_confirm_withdrawal(id, &transfer_id, initiated_at)
                    .await?;
                // Thread the confirmed withdrawal_tx so the burn-scan lower bound
                // is derived from the withdrawal tx block (not the raw chain head).
                self.continue_alpaca_to_base_from_withdrawal_complete(id, amount, withdrawal_tx)
                    .await
            }

            Some(WithdrawalComplete {
                direction: AlpacaToBase,
                amount,
                withdrawal_tx,
                ..
            }) => {
                // Thread the persisted withdrawal_tx so the durable re-check fires
                // on apalis redrive (primary gate does not re-run from this arm).
                self.continue_alpaca_to_base_from_withdrawal_complete(id, amount, withdrawal_tx)
                    .await
            }

            Some(Bridging {
                direction: AlpacaToBase,
                amount,
                burn_tx_hash,
                ..
            }) => {
                // The `Bridging` state carries only the nominal `amount`; it does
                // not store the actual burn amount (only `BridgingSubmitting` does
                // via its `burn_amount` field). At this point the burn has already
                // been committed and the nominal serves as the best available proxy.
                // `continue_alpaca_to_base_from_bridging` only uses `burn_amount`
                // for `BurnReceipt`; the actual minted amount comes from the
                // on-chain mint receipt, so supplying the nominal here is harmless.
                self.continue_alpaca_to_base_from_bridging(id, usdc_to_u256(amount)?, burn_tx_hash)
                    .await
            }

            Some(UsdcRebalance::AwaitingAttestation {
                direction: RebalanceDirection::AlpacaToBase,
                amount,
                burn_tx_hash,
                retry_deadline_at,
                ..
            }) => {
                self.continue_alpaca_to_base_from_awaiting_attestation(
                    id,
                    amount,
                    burn_tx_hash,
                    retry_deadline_at,
                )
                .await
            }

            // `Attested` must NOT route through the `Bridging` path: that re-emits
            // `ReceiveAttestation`, which the aggregate rejects from `Attested`,
            // stranding the already-burned USDC. Reconstruct the mint instead --
            // adopt an already-submitted one or re-poll the attestation without
            // re-recording it -- then go straight to mint + vault deposit.
            Some(Attested {
                direction: AlpacaToBase,
                burn_tx_hash,
                cctp_nonce,
                attestation,
                message,
                mint_scan_from_block,
                ..
            }) => {
                self.continue_alpaca_to_base_from_attested(
                    id,
                    burn_tx_hash,
                    attestation,
                    cctp_nonce,
                    message,
                    mint_scan_from_block,
                )
                .await
            }

            Some(Bridged {
                direction: AlpacaToBase,
                amount_received,
                ..
            }) => {
                self.continue_alpaca_to_base_from_bridged(id, amount_received)
                    .await
            }

            Some(DepositInitiated {
                direction: AlpacaToBase,
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
                match self.raindex.confirm_tx(deposit_tx).await {
                    Ok(()) => self.confirm_deposit(id).await,
                    // A dropped tx (gone from the mempool, will never mine) is a
                    // terminal failure -- distinct from a still-pending tx that
                    // confirm_tx merely couldn't confirm yet. Without this, a
                    // dropped deposit retries forever until the breaker trips.
                    // Record FailDeposit so an operator can reconcile, then
                    // surface the error.
                    Err(error) if error.is_transaction_dropped() => {
                        warn!(
                            target: "rebalance",
                            %deposit_tx,
                            "Recorded deposit tx was dropped from the mempool; recording \
                             terminal FailDeposit for operator reconciliation",
                        );
                        self.cqrs
                            .send(
                                id,
                                UsdcRebalanceCommand::FailDeposit {
                                    reason: format!(
                                        "Deposit tx {deposit_tx} dropped from the mempool \
                                         and will not mine"
                                    ),
                                },
                            )
                            .await?;
                        Err(UsdcTransferError::Vault(error))
                    }
                    Err(error) => Err(UsdcTransferError::Vault(error)),
                }
            }

            // Terminal-success no-ops: an AlpacaToBase deposit already
            // confirmed, or an operator-reconciled transfer resolved
            // out-of-band -- both have nothing left to drive.
            Some(
                DepositConfirmed {
                    direction: AlpacaToBase,
                    ..
                }
                | Reconciled { .. },
            ) => Ok(()),

            Some(
                Converting {
                    direction: BaseToAlpaca,
                    ..
                }
                | ConversionComplete {
                    direction: BaseToAlpaca,
                    ..
                }
                | Withdrawing {
                    direction: BaseToAlpaca,
                    ..
                }
                | WithdrawalComplete {
                    direction: BaseToAlpaca,
                    ..
                }
                | Bridging {
                    direction: BaseToAlpaca,
                    ..
                }
                | AwaitingAttestation {
                    direction: BaseToAlpaca,
                    ..
                }
                | Attested {
                    direction: BaseToAlpaca,
                    ..
                }
                | Bridged {
                    direction: BaseToAlpaca,
                    ..
                }
                | DepositInitiated {
                    direction: BaseToAlpaca,
                    ..
                }
                | DepositConfirmed {
                    direction: BaseToAlpaca,
                    ..
                },
            ) => Err(UsdcTransferError::ResumeDirectionMismatch {
                id: id.clone(),
                direction: BaseToAlpaca,
            }),

            // `BridgingSubmitting` for AlpacaToBase: a crash occurred after
            // `BeginBridging` (durable intent) but before `InitiateBridging`
            // (burn recorded). Scan Ethereum for an already-submitted burn to
            // avoid a double-burn, then continue the bridge.
            Some(BridgingSubmitting {
                direction: AlpacaToBase,
                amount,
                from_block,
                burn_amount,
                pending_burn_tx,
                ..
            }) => {
                let nominal_u256 = usdc_to_u256(amount)?;
                let scan_amount = if let Some(burn_amount) = burn_amount {
                    usdc_to_u256(burn_amount)?
                } else {
                    warn!(
                        target: "rebalance",
                        %id,
                        nominal = %amount,
                        "BridgingSubmitting has no persisted burn_amount; \
                         falling back to nominal scan amount for legacy compatibility"
                    );
                    nominal_u256
                };
                let burn_receipt = self
                    .resume_bridging_submitting_ethereum(
                        id,
                        scan_amount,
                        from_block,
                        pending_burn_tx,
                    )
                    .await?;
                // Pass burn_receipt.amount (what was actually burned, per the scan)
                // so BurnReceipt records the real amount, not the nominal.
                self.continue_alpaca_to_base_from_bridging(id, burn_receipt.amount, burn_receipt.tx)
                    .await
            }

            // `WithdrawalSubmitting` is produced only by the BaseToAlpaca flow
            // (on-chain vault withdrawal). `BridgingSubmitting { direction:
            // BaseToAlpaca }` is produced only by the BaseToAlpaca burn-on-Base
            // path. Reaching either here means the aggregate belongs to the
            // other direction.
            Some(
                WithdrawalSubmitting { direction, .. }
                | BridgingSubmitting {
                    direction: direction @ BaseToAlpaca,
                    ..
                },
            ) => Err(UsdcTransferError::ResumeDirectionMismatch {
                id: id.clone(),
                direction,
            }),

            Some(
                WithdrawalFailed { .. }
                | BridgingFailed { .. }
                | DepositFailed { .. }
                | ConversionFailed { .. },
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

        // Approximate initiated_at: the aggregate's initiated_at was just set
        // moments ago by the Initiate command handler. Reading it back from the
        // aggregate is not worth an extra DB round-trip; on any re-poll redrive,
        // resume_alpaca_to_base reads the exact aggregate value anyway. The
        // 4-hour operator-alert deadline absorbs any sub-second discrepancy here.
        let withdrawal_tx = self
            .poll_and_confirm_withdrawal(id, &transfer.id, Utc::now())
            .await?;
        // Thread the confirmed withdrawal_tx so the burn-scan lower bound
        // is derived from the withdrawal tx block (not the raw chain head).
        self.continue_alpaca_to_base_from_withdrawal_complete(id, filled_amount, withdrawal_tx)
            .await
    }

    /// Drives an Alpaca->Base transfer from `WithdrawalComplete` through to
    /// terminal: burn -> attestation -> mint -> vault deposit.
    async fn continue_alpaca_to_base_from_withdrawal_complete(
        &self,
        id: &UsdcRebalanceId,
        amount: Usdc,
        withdrawal_tx: Option<TxHash>,
    ) -> Result<(), UsdcTransferError> {
        // DURABLE confirmation re-check: fires on the redrive path
        // (`WithdrawalComplete` -> resume) when the primary gate in
        // `poll_and_confirm_withdrawal` does not re-run. An RPC failure here
        // is transient (the aggregate is already in the durable
        // `WithdrawalComplete` state), so we return `SettlementCheckTransient`
        // so the job delayed-redrives instead of consuming the apalis retry
        // budget. None means Alpaca returned no tx hash; fall through to the
        // balance gate.
        if let Some(tx) = withdrawal_tx {
            match self
                .cctp_bridge
                .ethereum_tx_confirmations(tx)
                .await
                .map_err(|error| UsdcTransferError::SettlementCheckTransient {
                    id: id.clone(),
                    source: Box::new(error),
                })? {
                None => {
                    warn!(
                        target: "rebalance",
                        %id,
                        %tx,
                        "Withdrawal tx not yet mined on redrive; retrying"
                    );
                    return Err(UsdcTransferError::WithdrawalTxUnderconfirmed {
                        id: id.clone(),
                        tx,
                        required: self.required_confirmations,
                        actual: 0,
                    });
                }
                Some(confirmations) if confirmations < self.required_confirmations => {
                    warn!(
                        target: "rebalance",
                        %id,
                        %tx,
                        confirmations,
                        required = self.required_confirmations,
                        "Withdrawal tx under-confirmed on redrive; retrying"
                    );
                    return Err(UsdcTransferError::WithdrawalTxUnderconfirmed {
                        id: id.clone(),
                        tx,
                        required: self.required_confirmations,
                        actual: confirmations,
                    });
                }
                Some(_) => {}
            }
        }

        // Read the actual USDC balance in the market-maker wallet.
        // When the withdrawal tx is confirmed (primary gate above passed), this
        // balance IS the received amount — use it directly as burn_amount.
        // Using the balance (rather than re-reading the tx receipt) means the
        // received amount reflects any Alpaca withdrawal fee, which lowers the
        // actual landing amount below the nominal. When no tx hash was returned
        // by Alpaca (withdrawal_tx: None), the balance is also the only
        // settlement signal.
        // Note: when withdrawal_tx is Some, ethereum_tx_block is still called
        // below for the burn-scan lower bound — that is an independent RPC call
        // unrelated to the amount derivation.
        //
        // The wallet MUST hold only USDC attributable to this rebalance: the
        // invariant is that the market-maker wallet is flushed to zero by each
        // CCTP burn, so the balance after a withdrawal == the received amount.
        //
        // Three cases:
        //   actual == 0            -> delayed-redrive (settlement not yet on-chain)
        //   0 < actual <= nominal  -> burn actual (valid; fee-reduced received amount)
        //   actual > nominal       -> explicit reconciliation error (wallet-empty
        //                            invariant broken; ambient/residual USDC present;
        //                            cannot distinguish withdrawal from residual)
        let nominal_u256 = usdc_to_u256(amount)?;
        let actual_balance = self.read_ethereum_usdc_balance(id).await?;

        let burn_amount = if actual_balance == U256::ZERO {
            // Zero balance after a confirmed withdrawal is unexpected: either the
            // funds haven't settled (rare timing) or the wallet invariant is broken.
            // Delayed-redrive so the job retries once settlement completes.
            warn!(
                target: "rebalance",
                %id,
                nominal = %amount,
                "Market-maker wallet USDC balance is zero after confirmed withdrawal; retrying"
            );
            return Err(UsdcTransferError::WalletUsdcInsufficient {
                id: id.clone(),
                nominal: amount,
            });
        } else if actual_balance > nominal_u256 {
            // Wallet holds MORE than the nominal: the wallet-empty invariant is
            // broken. Ambient or residual USDC from a prior rebalance is present
            // and we cannot distinguish those funds from this withdrawal's funds.
            // Burning any amount here would risk burning funds belonging to a
            // prior rebalance. Emit FailBridging for operator reconciliation.
            let balance = u256_to_usdc(actual_balance)?;
            error!(
                target: "rebalance",
                %id,
                %balance,
                nominal = %amount,
                actual_raw = %actual_balance,
                "Market-maker wallet holds more USDC than nominal; \
                 wallet-empty invariant broken — ambient/residual USDC detected; \
                 failing for operator reconciliation"
            );
            self.cqrs
                .send(
                    id,
                    UsdcRebalanceCommand::FailBridging {
                        reason: format!(
                            "wallet holds {balance} USDC > nominal {amount}; \
                             wallet-empty invariant broken; operator reconciliation required"
                        ),
                    },
                )
                .await?;
            return Err(UsdcTransferError::WalletUsdcAmbientBalance {
                id: id.clone(),
                balance,
                nominal: amount,
            });
        } else {
            // Wallet holds between 0 (exclusive) and nominal (inclusive).
            // This is the valid range under the wallet-empty invariant:
            // actual == nominal is the exact-receipt case (no fee), and
            // actual < nominal is the fee-deducted received amount (prod scenario).
            // Burn what arrived.

            // Log the fee delta when Alpaca deducted a withdrawal fee so operators
            // have an audit trail for P&L reconciliation.
            match u256_to_usdc(actual_balance) {
                Ok(received) if received < amount => match amount - received {
                    Ok(delta) => info!(
                        target: "rebalance",
                        %id,
                        nominal = %amount,
                        %received,
                        %delta,
                        "Alpaca withdrawal fee deducted; bridging received amount, not nominal"
                    ),
                    Err(error) => warn!(
                        target: "rebalance",
                        %id,
                        %error,
                        nominal = %amount,
                        %received,
                        "Alpaca fee-delta subtraction failed; delta unknown"
                    ),
                },
                Ok(_) => {}
                Err(error) => {
                    // Defensive: `actual_balance <= nominal_u256` and
                    // `nominal_u256` was produced by `usdc_to_u256(amount)?`
                    // (lossless), so this conversion should always succeed for
                    // valid USDC amounts. If it somehow fails (precision overflow
                    // in a future type change), the burn still proceeds with the
                    // raw U256 -- only the fee-delta log is skipped.
                    warn!(
                        target: "rebalance",
                        %id,
                        %error,
                        actual_balance_raw = %actual_balance,
                        "Fee-delta log skipped: actual balance U256->Usdc conversion failed"
                    );
                }
            }
            actual_balance
        };

        // Derive the burn-scan lower bound from this rebalance's confirmed
        // withdrawal tx block when available: the burn lands in a strictly later
        // block than the deposit (find_recent_burn uses block > from_block), so
        // the withdrawal block is a valid exclusive lower bound that excludes
        // any identical burn from a prior rebalance. When no tx hash was
        // returned by Alpaca, fall back to the raw chain head (residual
        // risk: a stale RPC head could include a prior burn; operators must
        // reconcile if two rebalances produce the same scan fingerprint).
        let burn_from_block = if let Some(tx) = withdrawal_tx {
            Some(
                self.cctp_bridge
                    .ethereum_tx_block(tx)
                    .await
                    .map_err(|error| UsdcTransferError::SettlementCheckTransient {
                        id: id.clone(),
                        source: Box::new(error),
                    })?,
            )
        } else {
            None
        };

        let burn_receipt = self
            .execute_cctp_burn_on_ethereum(id, burn_amount, burn_from_block)
            .await?;

        // Pass the actual (capped) burn amount through so BurnReceipt records
        // what was truly burned, not the nominal requested amount.
        self.continue_alpaca_to_base_from_bridging(id, burn_receipt.amount, burn_receipt.tx)
            .await
    }

    /// Drives an Alpaca->Base transfer from `Bridging`/`Attested` through to
    /// terminal. Re-polls the Circle attestation (idempotent for completed
    /// attestations) so we obtain a fresh [`AttestationResponse`] suitable
    /// for [`Bridge::mint`].
    ///
    /// `burn_amount` is the actual amount burned on Ethereum (may be less than
    /// the nominal when Alpaca deducted a withdrawal fee). It is stored in
    /// `BurnReceipt` for financial-accounting accuracy.
    async fn continue_alpaca_to_base_from_bridging(
        &self,
        id: &UsdcRebalanceId,
        // U256: SDK burn boundary; all call sites already hold U256 (on-chain data
        // or usdc_to_u256 output) so no conversion is needed here.
        burn_amount: U256,
        burn_tx_hash: TxHash,
    ) -> Result<(), UsdcTransferError> {
        let burn_receipt = BurnReceipt {
            tx: burn_tx_hash,
            amount: burn_amount,
        };

        let attestation_response = match self
            .poll_cctp_attestation(id, BridgeDirection::EthereumToBase, burn_receipt.tx)
            .await?
        {
            AttestationPollOutcome::Received(response) => {
                self.record_attestation(id, BridgeDirection::EthereumToBase, &response)
                    .await?;
                response
            }
            AttestationPollOutcome::TimedOut => {
                self.record_attestation_timeout(id).await?;
                return Err(UsdcTransferError::AttestationTimedOut { id: id.clone() });
            }
        };

        let mint_receipt = self.execute_cctp_mint(id, attestation_response).await?;

        self.continue_alpaca_to_base_from_bridged(id, u256_to_usdc(mint_receipt.amount)?)
            .await
    }

    /// Drives an Alpaca->Base transfer after a previous attestation timeout.
    /// Success records the delayed attestation and continues; another timeout
    /// stays retryable without appending another timeout event.
    async fn continue_alpaca_to_base_from_awaiting_attestation(
        &self,
        id: &UsdcRebalanceId,
        amount: Usdc,
        burn_tx_hash: TxHash,
        retry_deadline_at: DateTime<Utc>,
    ) -> Result<(), UsdcTransferError> {
        let burn_receipt = BurnReceipt {
            tx: burn_tx_hash,
            amount: usdc_to_u256(amount)?,
        };

        let attestation_response = match self
            .poll_cctp_attestation_until_deadline(
                id,
                BridgeDirection::EthereumToBase,
                burn_receipt.tx,
                retry_deadline_at,
            )
            .await?
        {
            AttestationPollOutcome::Received(response) => {
                self.record_attestation(id, BridgeDirection::EthereumToBase, &response)
                    .await?;
                response
            }
            AttestationPollOutcome::TimedOut => {
                return Err(UsdcTransferError::AttestationTimedOut { id: id.clone() });
            }
        };

        let mint_receipt = self.execute_cctp_mint(id, attestation_response).await?;

        self.continue_alpaca_to_base_from_bridged(id, u256_to_usdc(mint_receipt.amount)?)
            .await
    }

    /// Drives an Alpaca->Base transfer from `Attested` through to terminal
    /// WITHOUT re-emitting `ReceiveAttestation` (the aggregate already recorded
    /// it and rejects the command from `Attested`, which would strand the burned
    /// USDC).
    ///
    /// The mint may already have been submitted before a crash, so this first
    /// scans the Base chain (bounded by `mint_scan_from_block`) for an
    /// already-submitted mint to the market-maker wallet and adopts it via
    /// `ConfirmBridging` -- re-minting would revert on the already-used CCTP
    /// nonce. Only if no mint is found does it reconstruct the attestation from
    /// the persisted message envelope (or re-poll Circle for transfers predating
    /// envelope persistence) and mint afresh.
    async fn continue_alpaca_to_base_from_attested(
        &self,
        id: &UsdcRebalanceId,
        burn_tx_hash: TxHash,
        attestation: Vec<u8>,
        cctp_nonce: B256,
        message: Option<Vec<u8>>,
        mint_scan_from_block: Option<u64>,
    ) -> Result<(), UsdcTransferError> {
        // Events persisted before crash-safe resume carry no scan bound. Scanning
        // from genesis could adopt an unrelated mint to the same wallet, so refuse
        // to auto-resume and surface for manual reconciliation instead.
        let Some(mint_scan_from_block) = mint_scan_from_block else {
            warn!(target: "rebalance", %id, "Cannot resume Attested transfer: no mint scan bound captured");
            return Err(UsdcTransferError::ResumeWithoutMintScanBound { id: id.clone() });
        };

        if let Some(mint_receipt) = self
            .cctp_bridge
            .find_recent_mint(
                BridgeDirection::EthereumToBase,
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
                .continue_alpaca_to_base_from_bridged(id, amount_received)
                .await;
        }

        // No mint landed yet: reconstruct the attestation from persisted data
        // (or re-poll Circle for transfers predating envelope persistence)
        // WITHOUT re-emitting `ReceiveAttestation`, then mint on Base.
        // `execute_cctp_mint` emits `ConfirmBridging`, advancing the aggregate to
        // `Bridged`.
        let attestation_response = self
            .attested_attestation_response(
                id,
                BridgeDirection::EthereumToBase,
                burn_tx_hash,
                attestation,
                cctp_nonce,
                message,
            )
            .await?;
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

        // Convert USD to USDC - use the actual received amount for subsequent steps
        let usdc_amount = self.execute_usd_to_usdc_conversion(id, amount).await?;

        let transfer = self.initiate_alpaca_withdrawal(id, usdc_amount).await?;

        // Approximate initiated_at: same reasoning as
        // continue_alpaca_to_base_from_conversion_complete -- the aggregate's value
        // was set moments ago; resume_alpaca_to_base uses the exact stored value on
        // any re-poll redrive.
        let withdrawal_tx = self
            .poll_and_confirm_withdrawal(id, &transfer.id, Utc::now())
            .await?;

        // Thread the confirmed withdrawal_tx so the burn-scan lower bound is
        // derived from the withdrawal tx block (not the raw chain head).
        self.continue_alpaca_to_base_from_withdrawal_complete(id, usdc_amount, withdrawal_tx)
            .await?;

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
        initiated_at: DateTime<Utc>,
    ) -> Result<Option<alloy::primitives::TxHash>, UsdcTransferError> {
        let transfer = match self
            .alpaca_wallet
            .poll_transfer_until_complete(transfer_id)
            .await
        {
            Ok(transfer) => transfer,
            Err(error) => {
                // CONSERVATIVE FAIL-CLOSED: every `AlpacaWalletError` returned by
                // `poll_transfer_until_complete` (including ApiError{4xx/5xx},
                // TransferTimeout, Reqwest, ParseError, TransferNotFound, and
                // InvalidStatusTransition) is treated as INDETERMINATE -- the
                // withdrawal may have already succeeded on Alpaca's side even if
                // the poll did not confirm it. This is intentional: Alpaca's
                // documented determinate terminal failure is delivered as
                // `Ok(transfer)` with `status == TransferStatus::Failed` and no
                // tx hash, which is handled below. Error responses from the polling
                // endpoint do NOT constitute a determinate "funds never left" signal
                // in the Alpaca Broker API, so we never emit `FailWithdrawal` on an
                // error path.
                //
                // TransferNotFound specifically (the by-id endpoint returns 404)
                // is also classified as INDETERMINATE here. This is intentionally
                // fail-closed: an absent transfer ID does not confirm the
                // withdrawal never left. Guard is held, re-poll continues. Recovery
                // for a permanently-absent UUID is operational (see docs/cli-ops.md
                // "Withdrawal poll inconclusive"), not automated.
                //
                // Alpaca Broker API: GET
                // /v1/accounts/{account_id}/wallets/transfers/{transfer_id}
                // (docs.alpaca.markets/us/reference/getcryptofundingtransfer-1).
                // The transfer lifecycle terminates in COMPLETE or FAILED status
                // delivered as a Transfer payload. HTTP 4xx/5xx responses are
                // transient (auth, network, Alpaca-side load) and do not indicate
                // the transfer's ultimate fate -- the same transfer ID must be
                // re-polled to determine the outcome.
                //
                // Consequence of the conservative assumption: if a poll error is
                // encountered the aggregate stays in Withdrawing (guard held,
                // AlpacaTransferId recorded). The job redrive re-polls the same
                // transfer ID. After 4 hours of failed polls the operator is paged.
                // Worst case: a stuck operator page + manual intervention. No funds
                // are lost and no re-withdrawal can occur, which is the correct
                // safe failure direction for a money-movement operation.
                warn!(
                    target: "rebalance",
                    %id, %transfer_id,
                    "Alpaca withdrawal polling inconclusive; keeping Withdrawing \
                     state for delayed redrive (will re-poll same transfer ID): {error}"
                );
                return Err(UsdcTransferError::WithdrawalPollInconclusive {
                    id: id.clone(),
                    initiated_at,
                    source: error,
                });
            }
        };

        if let (TransferStatus::Failed, Some(tx_hash)) = (transfer.status, transfer.tx) {
            warn!(
                target: "rebalance",
                %id, %transfer_id, %tx_hash,
                "Alpaca withdrawal reported Failed with an on-chain tx hash; treating as \
                 inconclusive and keeping Withdrawing state for delayed redrive"
            );
            return Err(UsdcTransferError::WithdrawalPollInconclusive {
                id: id.clone(),
                initiated_at,
                source: AlpacaWalletError::FailedTransferHasTx {
                    transfer_id: *transfer_id,
                    tx_hash,
                },
            });
        }

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

        // Advance the aggregate to WithdrawalComplete NOW, before the on-chain
        // confirmation-depth check below. This is intentional: if the confirmation
        // wait returns early (tx not yet mined or under-confirmed), the aggregate is
        // already in WithdrawalComplete, so on apalis redrive the resume path enters
        // continue_alpaca_to_base_from_withdrawal_complete and uses the staleness-safe
        // fallback balance gate -- it never re-polls Alpaca. Without this ordering, a
        // transient Alpaca API error on a redrive would hit the poll_transfer error arm
        // and send FailWithdrawal against a withdrawal that already succeeded.
        let withdrawal_tx = transfer.tx;

        self.cqrs
            .send(
                id,
                UsdcRebalanceCommand::ConfirmWithdrawal { withdrawal_tx },
            )
            .await?;

        info!(target: "rebalance", "Alpaca withdrawal confirmed");

        // PRIMARY settlement gate: wait for the configured required_confirmations
        // on the on-chain tx that delivered the withdrawn USDC to the market-maker
        // wallet. Alpaca reports "Complete" before the tx is visible network-wide on
        // load-balanced RPC nodes, so a balance-read immediately after the status
        // change can hit a lagging node and return stale data. If the tx is not yet
        // sufficiently confirmed, return WithdrawalTxUnderconfirmed (retryable) --
        // the aggregate is already in WithdrawalComplete and withdrawal_tx is
        // persisted, so on apalis redrive the resume path enters
        // continue_alpaca_to_base_from_withdrawal_complete and re-runs this same
        // confirmation check durably before any burn. If the tx hash is absent
        // (Alpaca did not return one), fall through directly to the balance gate.
        if let Some(tx) = withdrawal_tx {
            match self
                .cctp_bridge
                .ethereum_tx_confirmations(tx)
                .await
                .map_err(|error| UsdcTransferError::SettlementCheckTransient {
                    id: id.clone(),
                    source: Box::new(error),
                })? {
                None => {
                    // Tx not yet mined; aggregate is already WithdrawalComplete so
                    // apalis redrive enters the balance-gate path, not Alpaca re-poll.
                    warn!(
                        target: "rebalance",
                        %id,
                        %tx,
                        "Alpaca withdrawal tx not yet mined; retrying"
                    );
                    return Err(UsdcTransferError::WithdrawalTxUnderconfirmed {
                        id: id.clone(),
                        tx,
                        required: self.required_confirmations,
                        actual: 0,
                    });
                }
                Some(confirmations) if confirmations < self.required_confirmations => {
                    // Under-confirmed; aggregate is already WithdrawalComplete so
                    // apalis redrive enters the balance-gate path, not Alpaca re-poll.
                    warn!(
                        target: "rebalance",
                        %id,
                        %tx,
                        confirmations,
                        required = self.required_confirmations,
                        "Alpaca withdrawal tx under-confirmed; retrying"
                    );
                    return Err(UsdcTransferError::WithdrawalTxUnderconfirmed {
                        id: id.clone(),
                        tx,
                        required: self.required_confirmations,
                        actual: confirmations,
                    });
                }
                Some(confirmations) => {
                    info!(
                        target: "rebalance",
                        %id,
                        %tx,
                        confirmations,
                        "Alpaca withdrawal tx confirmed on-chain"
                    );
                }
            }
        } else {
            warn!(
                target: "rebalance",
                %id,
                "Alpaca withdrawal transfer has no tx hash; skipping confirmation check \
                 (fallback balance gate will verify USDC is present before burn)"
            );
        }

        Ok(withdrawal_tx)
    }

    /// Reads the market-maker Ethereum wallet USDC balance.
    ///
    /// Returns the raw balance (in atomic units, 6 decimals) for the caller to
    /// use as the CCTP burn amount. The wallet is used exclusively as a CCTP
    /// burn source between rebalances, so its balance after a confirmed
    /// withdrawal equals the amount actually received (nominal minus any fees).
    ///
    /// RPC failure is transient: the aggregate is in `WithdrawalComplete` (a
    /// durable state), so `SettlementCheckTransient` is returned so the job
    /// delayed-redrives instead of consuming the apalis retry budget.
    async fn read_ethereum_usdc_balance(
        &self,
        id: &UsdcRebalanceId,
    ) -> Result<U256, UsdcTransferError> {
        self.cctp_bridge
            .ethereum_usdc_balance(self.market_maker_wallet)
            .await
            .map_err(|error| UsdcTransferError::SettlementCheckTransient {
                id: id.clone(),
                source: Box::new(error),
            })
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
        // Submit the deposit and persist its tx hash as `InitiateDeposit` BEFORE
        // confirming. `deposit_usdc` would submit AND confirm atomically, only
        // recording the hash afterwards -- a crash during the (potentially long)
        // confirmation wait would leave the aggregate in `Bridged`, and resume
        // would re-enter here and submit a SECOND deposit for the same funds.
        // Persisting the hash first lands a crash in `DepositInitiated`, whose
        // resume arm re-verifies the recorded tx via `confirm_tx` instead of
        // re-depositing.
        let deposit_tx = match self
            .raindex
            .submit_deposit_usdc(self.vault_id, amount)
            .await
        {
            Ok(tx) => tx,
            Err(error) => {
                // The submission failed before any deposit tx was broadcast
                // (`submit_pending` only returns a hash once the tx is accepted),
                // so nothing moved. Leave the aggregate in `Bridged` and let
                // apalis retry the deposit from there -- re-attempting is safe and
                // cannot double-deposit. Do NOT emit `FailDeposit`: it is invalid
                // from `Bridged` (only valid once a deposit has been initiated),
                // and a transient submission error is not a terminal failure.
                warn!(target: "rebalance", "Vault deposit submission failed: {error}");
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

        // Confirm the recorded deposit. A failure here leaves the aggregate in
        // `DepositInitiated` (the hash is persisted) rather than emitting
        // `FailDeposit`: the deposit may still confirm, and the `DepositInitiated`
        // resume arm re-checks it. Propagate so apalis retries from there.
        self.raindex.confirm_tx(deposit_tx).await?;

        info!(target: "rebalance", %deposit_tx, "Vault deposit submitted, recorded, and confirmed");
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
    /// 5. Execute CCTP mint on Ethereum (credits the bot wallet)
    ///    -> `ConfirmBridging` command
    /// 6. Send the minted USDC from the bot wallet directly to Alpaca's deposit
    ///    address (fresh path; the resume-from-`Bridged` path scans-or-adopts for
    ///    idempotency, mirroring the burn) -> `InitiateDeposit` records the send tx
    /// 7. Poll Alpaca by the send tx until deposit credited -> `ConfirmDeposit`
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
    /// - `Attested` reconstructs the `AttestationResponse` from the persisted
    ///   message envelope and mints with no Circle call. Transfers whose
    ///   attestation predates envelope persistence carry no message and fall
    ///   back to re-polling Circle (idempotent for a completed attestation).
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
                Self::require_base_to_alpaca(id, direction)?;
                let amount_u256 = usdc_to_u256(amount)?;
                self.resume_withdrawal_submitting(id, amount, amount_u256, from_block)
                    .await?;
                self.continue_from_withdrawal_complete(id, amount).await
            }

            Some(UsdcRebalance::Withdrawing {
                direction, amount, ..
            }) => {
                Self::require_base_to_alpaca(id, direction)?;
                self.cqrs
                    .send(
                        id,
                        UsdcRebalanceCommand::ConfirmWithdrawal {
                            withdrawal_tx: None,
                        },
                    )
                    .await?;
                self.continue_from_withdrawal_complete(id, amount).await
            }

            Some(UsdcRebalance::WithdrawalComplete {
                direction, amount, ..
            }) => {
                Self::require_base_to_alpaca(id, direction)?;
                self.continue_from_withdrawal_complete(id, amount).await
            }

            Some(UsdcRebalance::BridgingSubmitting {
                direction,
                amount,
                from_block,
                pending_burn_tx,
                ..
            }) => {
                Self::require_base_to_alpaca(id, direction)?;
                let amount_u256 = usdc_to_u256(amount)?;
                let burn_receipt = self
                    .resume_bridging_submitting(id, amount_u256, from_block, pending_burn_tx)
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
                Self::require_base_to_alpaca(id, direction)?;
                self.continue_from_bridging(id, amount, burn_tx_hash).await
            }

            Some(UsdcRebalance::AwaitingAttestation {
                direction,
                amount,
                burn_tx_hash,
                retry_deadline_at,
                ..
            }) => {
                Self::require_base_to_alpaca(id, direction)?;
                self.continue_from_awaiting_attestation(id, amount, burn_tx_hash, retry_deadline_at)
                    .await
            }

            Some(UsdcRebalance::Attested {
                direction,
                burn_tx_hash,
                cctp_nonce,
                attestation,
                message,
                mint_scan_from_block,
                ..
            }) => {
                Self::require_base_to_alpaca(id, direction)?;
                self.continue_from_attested(
                    id,
                    direction,
                    burn_tx_hash,
                    attestation,
                    cctp_nonce,
                    message,
                    mint_scan_from_block,
                )
                .await
            }

            Some(UsdcRebalance::Bridged {
                direction,
                amount_received,
                mint_tx_hash,
                ..
            }) => {
                Self::require_base_to_alpaca(id, direction)?;
                self.continue_from_bridged_resume(id, amount_received, mint_tx_hash)
                    .await
            }

            Some(UsdcRebalance::DepositInitiated {
                direction,
                amount,
                deposit_ref,
                ..
            }) => {
                Self::require_base_to_alpaca(id, direction)?;
                // The recorded `deposit_ref` is the USDC SEND tx (minted USDC
                // forwarded to Alpaca's deposit address), not the mint tx. The
                // send already moved funds before `InitiateDeposit` was recorded,
                // so this resume re-polls Alpaca by it -- no further send occurs.
                let TransferRef::OnchainTx(send_tx) = deposit_ref else {
                    return Err(UsdcTransferError::DepositRefMustBeOnchain { id: id.clone() });
                };
                self.poll_alpaca_deposit_and_confirm(id, send_tx).await?;
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
                Self::require_base_to_alpaca(id, direction)?;
                self.resume_converting(id, order_id).await
            }

            Some(UsdcRebalance::ConversionComplete { direction, .. }) => {
                Self::require_base_to_alpaca(id, direction)?;
                Ok(())
            }

            // A post-burn BridgingFailed is recoverable: the CCTP burn consumed
            // the source USDC and the mint may have landed (e.g. a transient
            // receipt error failed the bridge while receiveMessage succeeded).
            // Re-check on-chain, un-fail to Bridged, and finish the deposit.
            Some(UsdcRebalance::BridgingFailed {
                direction,
                burn_tx_hash,
                ..
            }) => {
                Self::require_base_to_alpaca(id, direction)?;
                self.recover_from_bridging_failed(id, burn_tx_hash).await
            }

            Some(
                UsdcRebalance::WithdrawalFailed { .. }
                | UsdcRebalance::DepositFailed { .. }
                | UsdcRebalance::ConversionFailed { .. },
            ) => Err(UsdcTransferError::PreviouslyFailedAggregate { id: id.clone() }),

            // Operator-reconciled is a clearing terminal: the transfer was
            // resolved out-of-band, so a resume has nothing to drive.
            Some(UsdcRebalance::Reconciled { .. }) => Ok(()),
        }
    }

    /// Recover a post-burn `BridgingFailed` whose CCTP mint may have landed:
    /// re-poll the attestation, idempotently mint (a consumed nonce returns the
    /// existing receipt rather than re-minting), un-fail to `Bridged` via
    /// `RecoverBridging`, then drive the deposit leg to terminal so the
    /// in-progress guard clears through the normal path. A pre-burn failure (no
    /// burn tx) has no mint to adopt and is left for manual reconciliation.
    ///
    /// BaseToAlpaca only (mint lands on Ethereum); an `AlpacaToBase`
    /// `BridgingFailed` is still surfaced for manual reconciliation.
    async fn recover_from_bridging_failed(
        &self,
        id: &UsdcRebalanceId,
        burn_tx_hash: Option<TxHash>,
    ) -> Result<(), UsdcTransferError> {
        let Some(burn_tx_hash) = burn_tx_hash else {
            warn!(
                target: "rebalance",
                %id,
                "Pre-burn BridgingFailed has no mint to recover; leaving for \
                 manual reconciliation"
            );
            return Err(UsdcTransferError::PreviouslyFailedAggregate { id: id.clone() });
        };

        info!(
            target: "rebalance",
            %id,
            %burn_tx_hash,
            "Recovering post-burn BridgingFailed: re-checking the CCTP mint on-chain"
        );

        // Poll the bridge directly (not `poll_cctp_attestation`, which fails the
        // bridge on error) because the aggregate is already terminally failed --
        // a transient poll/mint error must leave it recoverable for the next
        // redrive, not re-fail it.
        let attestation = match self
            .cctp_bridge
            .poll_attestation(BridgeDirection::BaseToEthereum, burn_tx_hash)
            .await
        {
            Ok(attestation) => attestation,
            // Circle has not signed yet. Surface the dedicated timeout error so
            // the job reschedules on the attestation redrive cadence rather than
            // the generic backoff, mirroring `poll_cctp_attestation` -- without
            // failing the (already-terminal) aggregate.
            Err(CctpError::AttestationTimeout { attempts, source }) => {
                warn!(
                    target: "rebalance",
                    %id,
                    attempts,
                    ?source,
                    "Attestation not yet available during bridge recovery; rescheduling redrive"
                );
                return Err(UsdcTransferError::AttestationTimedOut { id: id.clone() });
            }
            Err(error) => return Err(UsdcTransferError::Cctp(Box::new(error))),
        };

        // Idempotent: if the nonce was already consumed, `mint` returns the
        // existing receipt via `recover_already_minted` instead of re-minting.
        let mint_receipt = self
            .cctp_bridge
            .mint(BridgeDirection::BaseToEthereum, &attestation)
            .await
            .map_err(|error| UsdcTransferError::Cctp(Box::new(error)))?;

        let amount_received = u256_to_usdc(mint_receipt.amount)?;

        self.cqrs
            .send(
                id,
                UsdcRebalanceCommand::RecoverBridging {
                    mint_tx: mint_receipt.tx,
                    amount_received,
                    fee_collected: u256_to_usdc(mint_receipt.fee)?,
                },
            )
            .await?;

        self.continue_from_bridged_resume(id, amount_received, mint_receipt.tx)
            .await
    }

    fn require_base_to_alpaca(
        id: &UsdcRebalanceId,
        direction: RebalanceDirection,
    ) -> Result<(), UsdcTransferError> {
        if matches!(direction, RebalanceDirection::BaseToAlpaca) {
            Ok(())
        } else {
            Err(UsdcTransferError::ResumeDirectionMismatch {
                id: id.clone(),
                direction,
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
        correlation_id: ClientOrderId,
    ) -> Result<(), UsdcTransferError> {
        let Some(order) = self
            .alpaca_broker
            .find_conversion_order(&correlation_id)
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

        // Mirror the normal placement flow: a still-settling conversion is awaited
        // to a terminal state rather than failing the job. Failing here would burn
        // the finite apalis retry budget -- the order can settle slower than the
        // per-attempt window -- and let the timeout sweep clear the in-progress
        // guard while the conversion is still healthy, the partial-completion clobber
        // this resume path exists to prevent.
        let order = match order.classify() {
            CryptoOrderOutcome::Pending => {
                warn!(target: "rebalance", order_id = %order.id, "Resumed conversion still settling; awaiting terminal state");
                self.alpaca_broker
                    .poll_conversion_to_terminal(order.id)
                    .await?
            }
            _ => order,
        };

        match order.classify() {
            CryptoOrderOutcome::Filled => {
                let conversion = conversion_amounts_from_order(
                    &order,
                    &correlation_id,
                    ConversionDirection::UsdcToUsd,
                )?;

                self.cqrs
                    .send(id, UsdcRebalanceCommand::ConfirmConversion { conversion })
                    .await?;
                info!(target: "rebalance", order_id = %order.id, received_amount = %conversion.received_amount, "Resumed conversion confirmed from already-filled order");
                Ok(())
            }
            CryptoOrderOutcome::Pending => {
                // `poll_conversion_to_terminal` returns only on a terminal state, so a
                // Pending classification here is unreachable; fail indeterminate for
                // operator follow-up rather than silently looping.
                warn!(target: "rebalance", order_id = %order.id, "Resumed conversion still pending after awaiting terminal state");
                Err(UsdcTransferError::ResumeIndeterminateConversion { id: id.clone() })
            }
            CryptoOrderOutcome::Failed(reason) => {
                // Alpaca terminal statuses can carry a nonzero partial fill: real USDC
                // converted before the order terminated. Surface it in the failure
                // reason and logs so an operator reconciles the converted amount rather
                // than recording a clean full failure that hides it. Conservatively
                // treat an un-checkable fill as needing reconciliation.
                let partial_fill = order
                    .filled_quantity
                    .filter(|filled| filled.is_zero().map(|zero| !zero).unwrap_or(true));

                warn!(target: "rebalance", order_id = %order.id, ?reason, ?partial_fill, "Resumed conversion order failed terminally");
                let partial_suffix = partial_fill
                    .map(|filled| {
                        format!(" (partial fill {} needs reconciliation)", Usdc::new(filled))
                    })
                    .unwrap_or_default();
                self.cqrs
                    .send(
                        id,
                        UsdcRebalanceCommand::FailConversion {
                            reason: format!("conversion order failed: {reason:?}{partial_suffix}"),
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
        let attestation_response = match self
            .poll_cctp_attestation(id, BridgeDirection::BaseToEthereum, burn_receipt.tx)
            .await?
        {
            AttestationPollOutcome::Received(response) => {
                self.record_attestation(id, BridgeDirection::BaseToEthereum, &response)
                    .await?;
                response
            }
            AttestationPollOutcome::TimedOut => {
                self.record_attestation_timeout(id).await?;
                return Err(UsdcTransferError::AttestationTimedOut { id: id.clone() });
            }
        };

        info!(target: "rebalance", "Circle attestation received for Base burn");
        self.mint_and_continue(id, attestation_response).await
    }

    /// Drives the transfer from `AwaitingAttestation`: re-poll Circle until the
    /// retry deadline, recording `ReceiveAttestation` only when the attestation
    /// actually arrives.
    async fn continue_from_awaiting_attestation(
        &self,
        id: &UsdcRebalanceId,
        amount: Usdc,
        burn_tx_hash: TxHash,
        retry_deadline_at: DateTime<Utc>,
    ) -> Result<(), UsdcTransferError> {
        let burn_receipt = BurnReceipt {
            tx: burn_tx_hash,
            amount: usdc_to_u256(amount)?,
        };

        let attestation_response = match self
            .poll_cctp_attestation_until_deadline(
                id,
                BridgeDirection::BaseToEthereum,
                burn_receipt.tx,
                retry_deadline_at,
            )
            .await?
        {
            AttestationPollOutcome::Received(response) => {
                self.record_attestation(id, BridgeDirection::BaseToEthereum, &response)
                    .await?;
                response
            }
            AttestationPollOutcome::TimedOut => {
                return Err(UsdcTransferError::AttestationTimedOut { id: id.clone() });
            }
        };

        self.mint_and_continue(id, attestation_response).await
    }

    /// Drives the transfer from `Attested` through to terminal.
    ///
    /// The mint may already have been submitted before a crash, so this first
    /// scans the destination chain (bounded by `mint_scan_from_block`) for an
    /// already-submitted mint to the market maker wallet and adopts it via
    /// `ConfirmBridging` -- re-minting would revert on the already-used CCTP
    /// nonce and fail a transfer whose USDC was in fact minted. When no mint is
    /// found, it reconstructs the [`AttestationResponse`] from the persisted
    /// message envelope and mints with no Circle call, WITHOUT re-emitting
    /// `ReceiveAttestation` -- which the aggregate rejects from `Attested`.
    /// Transfers whose attestation predates envelope persistence carry no
    /// message and fall back to re-polling Circle.
    ///
    /// A re-poll timeout in the fallback path retries until success (no deadline)
    /// by design: reaching `Attested` means Circle already issued an attestation
    /// once, so a re-poll timeout is transient and bounding it would strand
    /// already-burned USDC. The `attestation_retry_deadline` bounds only the
    /// `AwaitingAttestation` wait. A hard (non-timeout) poll error still fails
    /// the bridge -- see [`Self::poll_cctp_attestation`].
    async fn continue_from_attested(
        &self,
        id: &UsdcRebalanceId,
        direction: RebalanceDirection,
        burn_tx_hash: TxHash,
        attestation: Vec<u8>,
        cctp_nonce: B256,
        message: Option<Vec<u8>>,
        mint_scan_from_block: Option<u64>,
    ) -> Result<(), UsdcTransferError> {
        // Events persisted before crash-safe resume carry no scan bound. Scanning
        // from genesis could adopt an unrelated mint to the same wallet, so refuse
        // to auto-resume and surface for manual reconciliation instead.
        let Some(mint_scan_from_block) = mint_scan_from_block else {
            warn!(target: "rebalance", %id, "Cannot resume Attested transfer: no mint scan bound captured");
            return Err(UsdcTransferError::ResumeWithoutMintScanBound { id: id.clone() });
        };

        // The mint lands on the destination chain of `direction`; scanning the
        // wrong chain would miss the already-submitted mint and fall through to a
        // double-mint that reverts on the used CCTP nonce.
        let mint_direction = match direction {
            RebalanceDirection::BaseToAlpaca => BridgeDirection::BaseToEthereum,
            RebalanceDirection::AlpacaToBase => BridgeDirection::EthereumToBase,
        };

        if let Some(mint_receipt) = self
            .cctp_bridge
            .find_recent_mint(
                mint_direction,
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
                .continue_from_bridged_resume(id, amount_received, mint_receipt.tx)
                .await;
        }

        let attestation_response = self
            .attested_attestation_response(
                id,
                mint_direction,
                burn_tx_hash,
                attestation,
                cctp_nonce,
                message,
            )
            .await?;
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
        self.continue_from_bridged_fresh(id, u256_to_usdc(mint_receipt.amount)?)
            .await
    }

    /// Drives a FRESH transfer from `Bridged` to terminal: send the minted USDC
    /// directly to Alpaca's deposit address, record the send, poll Alpaca for the
    /// credit, then convert USDC->USD.
    ///
    /// # Why an explicit send (load-bearing)
    ///
    /// The CCTP mint credits the bot's OWN wallet
    /// ([`execute_cctp_burn_on_base`](Self::execute_cctp_burn_on_base) sets the
    /// burn's `mintRecipient` to `self.market_maker_wallet`), NOT an Alpaca
    /// deposit address. Alpaca funds only when USDC is transferred to the
    /// per-account deposit address from `get_wallet_address`. So this leg fetches
    /// that address and SENDS the minted USDC there -- a real fund-moving call --
    /// then polls Alpaca by the SEND tx (not the mint tx). The recorded
    /// `deposit_ref` is therefore the SEND tx.
    ///
    /// # Fresh vs resume (load-bearing)
    ///
    /// This is the fresh entry, reached immediately after this execution minted on
    /// Ethereum (no prior deposit send exists), so it sends DIRECTLY with no
    /// pre-send chain scan -- mirroring the burn, where the fresh
    /// [`execute_cctp_burn_on_base`](Self::execute_cctp_burn_on_base) burns
    /// directly and only the resume path scans. Scanning here would needlessly
    /// wait for finality on the very first send (and, in tests where the chain head
    /// sits at the mint block, never conclude).
    ///
    /// Crash-safety is preserved by the resume path, not this one: a crash after
    /// the direct send but before `InitiateDeposit` lands the aggregate back in
    /// `Bridged`, and the `Bridged` resume arm re-enters via
    /// [`continue_from_bridged_resume`](Self::continue_from_bridged_resume), whose
    /// pre-send scan ADOPTS the existing send instead of re-sending -- exactly the
    /// burn invariant.
    async fn continue_from_bridged_fresh(
        &self,
        id: &UsdcRebalanceId,
        amount_received: Usdc,
    ) -> Result<(), UsdcTransferError> {
        let send_tx = self.send_alpaca_deposit(id, amount_received).await?;
        self.finish_deposit(id, amount_received, send_tx).await
    }

    /// Resumes a transfer stalled at `Bridged` (a crash after the mint, possibly
    /// after a deposit send): scans the chain for an already-submitted send and
    /// adopts it, otherwise sends, then drives the deposit leg to terminal.
    ///
    /// The pre-send scan
    /// ([`find_recent_usdc_transfer`](st0x_bridge::cctp::CctpBridge::find_recent_usdc_transfer))
    /// is what makes resume safe: bounded by the mint tx's own block (the send
    /// lands at or after the mint), it ADOPTS an already-submitted matching
    /// transfer instead of re-sending. A scan failure (inconclusive or RPC error)
    /// returns an error WITHOUT sending -- never a blind re-send -- mirroring
    /// [`resume_bridging_submitting`](Self::resume_bridging_submitting) and
    /// `find_recent_burn`. The `DepositInitiated` resume arm re-polls by the
    /// recorded send tx, so once the send is recorded no further send occurs.
    async fn continue_from_bridged_resume(
        &self,
        id: &UsdcRebalanceId,
        amount_received: Usdc,
        mint_tx: TxHash,
    ) -> Result<(), UsdcTransferError> {
        let send_tx = self
            .send_or_adopt_alpaca_deposit(id, amount_received, mint_tx)
            .await?;
        self.finish_deposit(id, amount_received, send_tx).await
    }

    /// Records the deposit send, polls Alpaca for the credit, then converts
    /// USDC->USD. Shared tail of the fresh and resume `Bridged` paths -- the only
    /// difference between them is how `send_tx` is obtained (direct send vs
    /// scan-or-adopt).
    async fn finish_deposit(
        &self,
        id: &UsdcRebalanceId,
        amount_received: Usdc,
        send_tx: TxHash,
    ) -> Result<(), UsdcTransferError> {
        self.cqrs
            .send(
                id,
                UsdcRebalanceCommand::InitiateDeposit {
                    deposit: TransferRef::OnchainTx(send_tx),
                },
            )
            .await?;

        self.poll_alpaca_deposit_and_confirm(id, send_tx).await?;
        self.execute_usdc_to_usd_conversion(id, amount_received)
            .await?;
        Ok(())
    }

    /// Fetches Alpaca's per-account USDC (Ethereum) deposit address.
    async fn fetch_alpaca_deposit_address(
        &self,
        id: &UsdcRebalanceId,
    ) -> Result<Address, UsdcTransferError> {
        let usdc = TokenSymbol::new("USDC");
        let ethereum = Network::new("ethereum");

        match self
            .alpaca_wallet
            .get_wallet_address(&usdc, &ethereum)
            .await
        {
            Ok(address) => Ok(address),
            Err(error) => {
                warn!(target: "rebalance", %id, "Fetching Alpaca deposit address failed: {error}");
                Err(UsdcTransferError::AlpacaWallet(error))
            }
        }
    }

    /// Sends the minted USDC DIRECTLY to Alpaca's deposit address (no pre-send
    /// scan). Fresh path only: there is no prior send to adopt because the mint
    /// just completed in this execution. Crash-safety is provided by the resume
    /// path's [`send_or_adopt_alpaca_deposit`](Self::send_or_adopt_alpaca_deposit),
    /// mirroring how the fresh burn path skips the scan that the resume burn path
    /// performs.
    #[instrument(target = "rebalance", skip(self), fields(%id, %amount_received), level = tracing::Level::DEBUG)]
    async fn send_alpaca_deposit(
        &self,
        id: &UsdcRebalanceId,
        amount_received: Usdc,
    ) -> Result<TxHash, UsdcTransferError> {
        let deposit_address = self.fetch_alpaca_deposit_address(id).await?;
        let amount_u256 = usdc_to_u256(amount_received)?;

        let send_tx = self
            .cctp_bridge
            .send_usdc_on_ethereum(deposit_address, amount_u256)
            .await
            .map_err(|error| UsdcTransferError::Cctp(Box::new(error)))?;

        info!(target: "rebalance", %id, %send_tx, %deposit_address, %amount_received, "Sent minted USDC to Alpaca deposit address");
        Ok(send_tx)
    }

    /// Sends the minted USDC to Alpaca's deposit address, or adopts an
    /// already-submitted send found on chain (crash-safe resume).
    ///
    /// Fetches the per-account USDC deposit address, bounds the scan by the mint
    /// tx's block, and either adopts a matching existing transfer or submits the
    /// ERC20 transfer. A scan failure returns an error without sending, so a
    /// transient RPC/finality hiccup never triggers a blind double-send. Resume
    /// path only -- the fresh path sends directly via
    /// [`send_alpaca_deposit`](Self::send_alpaca_deposit).
    #[instrument(target = "rebalance", skip(self), fields(%id, %amount_received, %mint_tx), level = tracing::Level::DEBUG)]
    async fn send_or_adopt_alpaca_deposit(
        &self,
        id: &UsdcRebalanceId,
        amount_received: Usdc,
        mint_tx: TxHash,
    ) -> Result<TxHash, UsdcTransferError> {
        let deposit_address = self.fetch_alpaca_deposit_address(id).await?;
        let amount_u256 = usdc_to_u256(amount_received)?;

        // Bound the idempotency scan by the mint's block: the deposit send lands
        // at or after the mint, so no earlier transfer can be this deposit's.
        let from_block = self
            .cctp_bridge
            .ethereum_tx_block(mint_tx)
            .await
            .map_err(|error| UsdcTransferError::Cctp(Box::new(error)))?;

        // Fail safe on a scan error: do NOT send. A re-send off an inconclusive or
        // failed scan could forward the minted USDC twice.
        if let Some(existing_tx) = self
            .cctp_bridge
            .find_recent_usdc_transfer(
                self.market_maker_wallet,
                deposit_address,
                amount_u256,
                from_block,
            )
            .await
            .map_err(|error| UsdcTransferError::Cctp(Box::new(error)))?
        {
            info!(target: "rebalance", %id, %existing_tx, %deposit_address, "Adopting already-submitted USDC deposit transfer to Alpaca on resume");
            return Ok(existing_tx);
        }

        let send_tx = self
            .cctp_bridge
            .send_usdc_on_ethereum(deposit_address, amount_u256)
            .await
            .map_err(|error| UsdcTransferError::Cctp(Box::new(error)))?;

        info!(target: "rebalance", %id, %send_tx, %deposit_address, %amount_received, "Sent minted USDC to Alpaca deposit address");
        Ok(send_tx)
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
            Err(error) => return Err(classify_vault_withdrawal_error(error)),
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
            Err(error) => return Err(classify_vault_withdrawal_error(error)),
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
            .send(
                id,
                UsdcRebalanceCommand::ConfirmWithdrawal {
                    withdrawal_tx: None,
                },
            )
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
            .send(
                id,
                UsdcRebalanceCommand::BeginBridging {
                    from_block,
                    burn_amount: Some(u256_to_usdc(amount)?),
                },
            )
            .await?;

        let burn_receipt = self
            .burn_recording_pending(
                id,
                BridgeDirection::BaseToEthereum,
                amount,
                self.market_maker_wallet,
            )
            .await?;
        self.record_cctp_burn(id, burn_receipt).await
    }

    /// Resumes a transfer stalled at `BridgingSubmitting`: scans the chain for an
    /// already-submitted burn (adopting it to avoid a double-burn) and otherwise
    /// issues the burn, then records it. Returns the burn receipt so the caller
    /// can continue the bridge.
    ///
    /// `pending_burn_tx` is the burn hash durably recorded by `RecordPendingBurn`
    /// before the receipt was awaited: when present, its on-chain status is
    /// checked directly (`burn_status`) instead of a mempool-blind log scan,
    /// closing the double-burn window on the timeout/crash redrive path.
    ///
    /// An empty scan (no recorded hash and no burn found on-chain) FAILS CLOSED
    /// (`BurnSubmitInconclusive`) rather than reburning, because reaching this
    /// resume path means a burn submission was already initiated (`BeginBridging`
    /// emitted) and a broadcast may be in flight. A reburn is issued ONLY when the
    /// recorded burn mined REVERTED (`pending_burn_tx.is_some()` -- no funds moved),
    /// the one case where rescanning and reburning is safe. The genuine first burn
    /// is issued by `execute_cctp_burn_on_base`, never here.
    async fn resume_bridging_submitting(
        &self,
        id: &UsdcRebalanceId,
        amount: U256,
        from_block: u64,
        pending_burn_tx: Option<TxHash>,
    ) -> Result<BurnReceipt, UsdcTransferError> {
        if let Some(adopted) = self
            .check_pending_burn(id, BridgeDirection::BaseToEthereum, amount, pending_burn_tx)
            .await?
        {
            return self.record_cctp_burn(id, adopted).await;
        }

        // `check_pending_burn` returned `Ok(None)`: scan for an already-mined burn to
        // adopt. The scan-EMPTY action depends on why we fell through -- a recorded
        // burn that mined REVERTED (`Some`, no funds moved) reburns safely; no
        // recorded hash (`None`) fails closed (a broadcast may be in flight).
        let reburn_on_empty = pending_burn_tx.is_some();

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
            Ok(None) if reburn_on_empty => {
                self.burn_recording_pending(
                    id,
                    BridgeDirection::BaseToEthereum,
                    amount,
                    self.market_maker_wallet,
                )
                .await?
            }
            Ok(None) => {
                error!(
                    target: "rebalance",
                    %id,
                    "BridgingSubmitting resume: no recorded burn tx and none found on-chain; \
                     a broadcast may be in flight, failing closed for operator reconciliation \
                     (no auto-reburn)"
                );
                return Err(UsdcTransferError::BurnSubmitInconclusive { id: id.clone() });
            }
            // ScanInconclusive: the chain head hasn't advanced far enough past
            // `from_block` to trust an empty scan result. The aggregate is at
            // `BridgingSubmitting` (a durable state), so return
            // `SettlementCheckTransient` so the job delayed-redrives instead of
            // consuming the apalis retry budget.
            Err(error @ CctpError::ScanInconclusive { .. }) => {
                warn!(
                    target: "rebalance",
                    %id,
                    "CCTP burn scan on Base inconclusive; will retry after delay"
                );
                return Err(UsdcTransferError::SettlementCheckTransient {
                    id: id.clone(),
                    source: Box::new(error),
                });
            }
            Err(error) => {
                warn!(target: "rebalance", "CCTP burn scan on Base failed: {error}");
                return Err(UsdcTransferError::Cctp(Box::new(error)));
            }
        };

        self.record_cctp_burn(id, burn_receipt).await
    }

    /// Two-phase CCTP burn that records the broadcast tx hash BEFORE awaiting its
    /// receipt. `submit_burn` broadcasts and returns the hash; `RecordPendingBurn`
    /// durably persists it; `confirm_burn` then awaits the receipt. A crash/timeout
    /// between submit and confirm leaves `pending_burn_tx` set, so the redrive
    /// checks that exact tx (`burn_status`) instead of a mempool-blind log scan --
    /// closing the double-burn window on the timeout path.
    ///
    /// The `submit_burn -> RecordPendingBurn` critical section runs on a detached
    /// task (`tokio::spawn`) so the hedging job's per-attempt `tokio::time::timeout`
    /// can never cancel the resume future between broadcasting the burn and durably
    /// recording its hash: the spawned task completes regardless, leaving
    /// `pending_burn_tx` set for the redrive to adopt. `confirm_burn` runs after and
    /// stays cancellable (and bounded) by that job timeout.
    ///
    /// The one-shot allowance retry lives here at the manager level (not inside the
    /// bridge) so each broadcast's hash is recorded: `submit_burn` re-runs the
    /// allowance check and fee query on every call, preserving the atomic burn's
    /// retry-on-revert semantics while making each attempt's hash durable. Bounded
    /// to exactly TWO attempts (unrolled below so the bound is obvious): a burn
    /// that mines REVERTED on confirm moved no funds, so re-submitting once is
    /// safe; a second revert (or any non-revert confirm error) is terminal. On
    /// failure the aggregate stays at `BridgingSubmitting` (no terminal event), so
    /// an apalis retry re-enters the resume path.
    ///
    /// This two-attempt retry covers only CONFIRM-time reverts (a recorded hash
    /// that mined `MinedReverted`). A SUBMISSION-time revert is different: because
    /// each `submit_and_record_burn` clears `pending_burn_tx` before broadcasting,
    /// a revert at submission leaves `pending_burn_tx: None`, so the `BurnRevert`
    /// redrive's resume FAILS CLOSED (`BurnSubmitInconclusive`) rather than
    /// auto-reburning off a stale hash.
    async fn burn_recording_pending(
        &self,
        id: &UsdcRebalanceId,
        direction: BridgeDirection,
        amount: U256,
        recipient: Address,
    ) -> Result<BurnReceipt, UsdcTransferError> {
        // First attempt.
        let burn_tx = self
            .submit_and_record_burn(id, direction, amount, recipient)
            .await?;
        match self
            .cctp_bridge
            .confirm_burn(direction, burn_tx, amount)
            .await
        {
            Ok(burn_receipt) => return Ok(burn_receipt),
            Err(error) if error.is_revert() => {
                warn!(target: "rebalance", %burn_tx, "CCTP burn reverted on confirm; re-submitting once");
            }
            Err(error) => {
                warn!(target: "rebalance", "CCTP burn confirm failed: {error}");
                return Err(UsdcTransferError::Cctp(Box::new(error)));
            }
        }

        // Final attempt after the first burn reverted: re-submit (which re-records
        // the new hash) and re-confirm. Its outcome is returned unconditionally.
        let burn_tx = self
            .submit_and_record_burn(id, direction, amount, recipient)
            .await?;
        match self
            .cctp_bridge
            .confirm_burn(direction, burn_tx, amount)
            .await
        {
            Ok(burn_receipt) => Ok(burn_receipt),
            Err(error) if error.is_revert() => {
                warn!(target: "rebalance", "CCTP burn reverted on confirm after retry: {error}");
                Err(UsdcTransferError::BurnRevert(Box::new(error)))
            }
            Err(error) => {
                warn!(target: "rebalance", "CCTP burn confirm failed: {error}");
                Err(UsdcTransferError::Cctp(Box::new(error)))
            }
        }
    }

    /// Broadcasts the burn and durably records its hash via `RecordPendingBurn`,
    /// on a detached task so a cancelling job timeout cannot drop the future
    /// between the (irreversible) broadcast and the record. The task captures only
    /// owned values plus the shared `Arc`s, so it outlives a cancelled caller.
    /// Returns the broadcast tx hash once the hash is committed. The record is
    /// retried a bounded number of times; if every attempt fails the task returns
    /// the TERMINAL `BurnRecordFailed` (never a silent hash loss), and a JoinError
    /// (panic) surfaces as `BurnRecordTaskFailed`.
    ///
    /// The broadcast itself is bounded by [`BURN_BROADCAST_TIMEOUT`]. A submission
    /// that times out or fails with a NON-revert (transport) error after the
    /// request was sent is INCONCLUSIVE -- the broadcast may have reached the
    /// network -- and surfaces the TERMINAL `BurnSubmitInconclusive` so the
    /// transfer fails closed rather than letting a retry reburn a possibly-live
    /// burn. Only a clean revert (no funds moved) stays on the `BurnRevert`
    /// bounded-redrive path. The error-level `burn_tx` log emitted after
    /// exhausting record retries is the operator's recovery signal -- it
    /// persists in the log sink even if the calling `JoinHandle` is dropped.
    async fn submit_and_record_burn(
        &self,
        id: &UsdcRebalanceId,
        direction: BridgeDirection,
        amount: U256,
        recipient: Address,
    ) -> Result<TxHash, UsdcTransferError> {
        let cctp_bridge = Arc::clone(&self.cctp_bridge);
        let cqrs = Arc::clone(&self.cqrs);
        let task_id = id.clone();

        tokio::spawn(async move {
            // Clear any stale recorded burn hash BEFORE broadcasting, so that if recording THIS
            // burn's hash fails, the resume path sees `pending_burn_tx: None` and fails closed
            // instead of reburning off the stale hash (double-burn). A no-op (no event) on the
            // first burn where pending_burn_tx is already None.
            cqrs.send(&task_id, UsdcRebalanceCommand::ClearPendingBurn)
                .await
                .inspect_err(|error| {
                    error!(
                        target: "rebalance",
                        id = %task_id,
                        ?error,
                        "Failed to clear pending burn before broadcast; not broadcasting"
                    );
                })
                .map_err(UsdcTransferError::from)?;

            let burn_tx = match tokio::time::timeout(
                BURN_BROADCAST_TIMEOUT,
                cctp_bridge.submit_burn(direction, amount, recipient),
            )
            .await
            {
                Ok(Ok(burn_tx)) => burn_tx,
                // A clean revert moved no funds, so the bounded revert-redrive can
                // safely reburn. NOTE: the pre-broadcast `ClearPendingBurn` above
                // already reset `pending_burn_tx` to None, so a SUBMISSION-time
                // revert leaves the aggregate at
                // `BridgingSubmitting { pending_burn_tx: None }`. The redrive's
                // resume therefore FAILS CLOSED (`BurnSubmitInconclusive`) rather
                // than auto-reburning; only a CONFIRM-time revert (a recorded hash
                // that mined `MinedReverted`) takes the scan+reburn path.
                Ok(Err(error)) if error.is_revert() => {
                    warn!(target: "rebalance", id = %task_id, "CCTP burn reverted on submission: {error}");
                    return Err(UsdcTransferError::BurnRevert(Box::new(error)));
                }
                // A non-revert (transport/RPC) error during submission is
                // INCONCLUSIVE: it may have fired BEFORE the broadcast (e.g. the
                // allowance read or fee query, or connection-refused -- no burn) or
                // AFTER the request reached the network (the burn may be live). We
                // cannot distinguish, so fail closed conservatively rather than let a
                // retry reburn a possibly-live burn.
                Ok(Err(error)) => {
                    error!(
                        target: "rebalance",
                        id = %task_id,
                        ?error,
                        "CCTP burn submission failed with a non-revert error; broadcast may \
                         have landed, failing closed"
                    );
                    return Err(UsdcTransferError::BurnSubmitInconclusive { id: task_id });
                }
                // A timed-out broadcast may likewise have reached the network.
                Err(_) => {
                    error!(
                        target: "rebalance",
                        id = %task_id,
                        timeout = ?BURN_BROADCAST_TIMEOUT,
                        "CCTP burn submission timed out; broadcast may have landed, failing closed"
                    );
                    return Err(UsdcTransferError::BurnSubmitInconclusive { id: task_id });
                }
            };

            // The burn is now broadcast (irreversible). Record its hash durably,
            // retrying transient commit failures (e.g. a SQLite write-lock). If
            // every attempt fails, return a TERMINAL error rather than proceeding
            // to confirm as if the burn were safely recorded: a lost hash would let
            // a later redrive treat the burn as un-broadcast and reburn off a
            // mempool-blind scan.
            for attempt in 1..=BURN_RECORD_ATTEMPTS {
                match cqrs
                    .send(
                        &task_id,
                        UsdcRebalanceCommand::RecordPendingBurn { burn_tx },
                    )
                    .await
                {
                    Ok(()) => return Ok(burn_tx),
                    Err(error) => {
                        error!(
                            target: "rebalance",
                            id = %task_id,
                            %burn_tx,
                            attempt,
                            max = BURN_RECORD_ATTEMPTS,
                            ?error,
                            "Failed to commit RecordPendingBurn for broadcast burn"
                        );
                        if attempt < BURN_RECORD_ATTEMPTS {
                            tokio::time::sleep(BURN_RECORD_RETRY_BACKOFF * attempt).await;
                        }
                    }
                }
            }

            error!(
                target: "rebalance",
                id = %task_id,
                %burn_tx,
                "Exhausted RecordPendingBurn retries for broadcast burn; failing terminally \
                 to avoid silently losing the burn tx hash"
            );
            Err(UsdcTransferError::BurnRecordFailed {
                id: task_id,
                burn_tx,
            })
        })
        .await
        .map_err(|join_error| {
            error!(
                target: "rebalance",
                %id,
                %join_error,
                "Burn submit-and-record task failed to join (panicked)"
            );
            UsdcTransferError::BurnRecordTaskFailed { id: id.clone() }
        })?
    }

    /// Checks the durably-recorded pending burn tx before the mempool-blind log
    /// scan, so a burn broadcast on a crashed/timed-out attempt is adopted rather
    /// than re-burned. Called only from the `BridgingSubmitting` RESUME path (after
    /// `BeginBridging`); the genuine first burn is issued by `execute_cctp_burn_on_*`.
    ///
    /// Returns:
    /// - `Ok(Some(receipt))` to adopt a MINED-success burn (validated via
    ///   `confirm_burn`, so the MessageSent check matches the normal path);
    /// - `Ok(None)` to fall through to the chain scan (which adopts an already-mined
    ///   burn if found). This covers two cases the CALLER distinguishes by
    ///   `pending_burn_tx`: a recorded tx that mined REVERTED (`Some`; an empty scan
    ///   then reburns safely -- a reverted burn moved no funds), and NO recorded hash
    ///   (`None`; an empty scan then FAILS CLOSED -- a broadcast may be in flight, so
    ///   the resume path must NEVER auto-reburn). See `resume_bridging_submitting`;
    /// - `Err(SettlementCheckTransient)` when the tx is still PENDING (delayed
    ///   redrive -- the caller must NOT reburn); and
    /// - `Err(BurnTxDropped)` when the tx is classified DROPPED -- TERMINAL. Once a
    ///   burn tx hash is durably recorded, an ambiguous "dropped" classification
    ///   (which a load-balanced RPC can produce for a still-pending burn) NEVER
    ///   auto-issues a second burn; it pages the operator for manual on-chain
    ///   verification instead.
    async fn check_pending_burn(
        &self,
        id: &UsdcRebalanceId,
        direction: BridgeDirection,
        amount: U256,
        pending_burn_tx: Option<TxHash>,
    ) -> Result<Option<BurnReceipt>, UsdcTransferError> {
        let Some(burn_tx) = pending_burn_tx else {
            // No recorded hash: fall through to the scan, which ADOPTS an already-mined
            // burn if one is found (crash-safe recovery). The caller decides the
            // scan-EMPTY action by `pending_burn_tx.is_none()`: fail closed (a broadcast
            // may be in flight), never auto-reburn. See `resume_bridging_submitting`.
            return Ok(None);
        };

        let status = self
            .cctp_bridge
            .burn_status(direction, burn_tx)
            .await
            .map_err(|error| UsdcTransferError::SettlementCheckTransient {
                id: id.clone(),
                source: Box::new(error),
            })?;

        match status {
            BurnTxStatus::MinedSuccess => {
                info!(
                    target: "rebalance",
                    %id,
                    %burn_tx,
                    "Adopting durably-recorded CCTP burn on resume (mined successfully)"
                );
                // Adopt via confirm_burn (not a bare BurnReceipt) so the adopt path
                // runs the same MessageSent validation as the normal burn path. The
                // receipt is already mined-success, so a non-revert confirm error
                // (e.g. MessageSentEventNotFound) is the actionable failure -> Cctp.
                let burn_receipt = self
                    .cctp_bridge
                    .confirm_burn(direction, burn_tx, amount)
                    .await
                    .map_err(|error| {
                        warn!(target: "rebalance", %id, %burn_tx, "Adopt-path confirm of recorded burn failed: {error}");
                        UsdcTransferError::Cctp(Box::new(error))
                    })?;
                Ok(Some(burn_receipt))
            }
            BurnTxStatus::Pending => {
                warn!(
                    target: "rebalance",
                    %id,
                    %burn_tx,
                    "Recorded CCTP burn not yet mined; will retry after delay (no reburn)"
                );
                Err(UsdcTransferError::SettlementCheckTransient {
                    id: id.clone(),
                    source: Box::new(CctpError::BurnTxPending { burn_tx }),
                })
            }
            BurnTxStatus::MinedReverted => {
                warn!(
                    target: "rebalance",
                    %id,
                    %burn_tx,
                    "Recorded CCTP burn reverted (no on-chain effect); rescanning and reburning"
                );
                Ok(None)
            }
            BurnTxStatus::Dropped => {
                // A burn tx hash is durably recorded, so we must NEVER auto-issue a
                // second burn for an ambiguous "dropped" classification: a
                // load-balanced RPC can falsely report a still-pending burn as
                // dropped, and a reburn there would double-burn. Page the operator
                // for manual on-chain verification instead.
                error!(
                    target: "rebalance",
                    %id,
                    %burn_tx,
                    "Recorded CCTP burn not mined and no longer in mempool (dropped); \
                     NOT reburning -- operator must verify on-chain"
                );
                Err(UsdcTransferError::BurnTxDropped {
                    id: id.clone(),
                    burn_tx,
                })
            }
        }
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

        info!(target: "rebalance", burn_tx = %burn_receipt.tx, "CCTP burn executed");
        Ok(burn_receipt)
    }

    /// Emits `BeginBridging` (durable intent), burns on Ethereum, then records
    /// the burn. A crash after `BeginBridging` but before `InitiateBridging`
    /// lands the aggregate at `BridgingSubmitting`, from which
    /// `resume_bridging_submitting_ethereum` scans for the burn instead of
    /// re-burning.
    ///
    /// `burn_from_block`: when `Some`, uses the caller-supplied block as the
    /// scan lower bound (derived from this rebalance's confirmed withdrawal tx
    /// block, which strictly precedes any burn for this rebalance). When
    /// `None`, falls back to `source_block(EthereumToBase)` (residual risk:
    /// a stale RPC head could include a prior identical burn; reconcile
    /// manually if two rebalances share the same scan fingerprint).
    async fn execute_cctp_burn_on_ethereum(
        &self,
        id: &UsdcRebalanceId,
        amount: U256,
        burn_from_block: Option<u64>,
    ) -> Result<BurnReceipt, UsdcTransferError> {
        let from_block = match burn_from_block {
            Some(block) => block,
            None => self
                .cctp_bridge
                .source_block(BridgeDirection::EthereumToBase)
                .await
                .map_err(|error| UsdcTransferError::SettlementCheckTransient {
                    id: id.clone(),
                    source: Box::new(error),
                })?,
        };
        self.cqrs
            .send(
                id,
                UsdcRebalanceCommand::BeginBridging {
                    from_block,
                    burn_amount: Some(u256_to_usdc(amount)?),
                },
            )
            .await?;

        let burn_receipt = self
            .burn_recording_pending(
                id,
                BridgeDirection::EthereumToBase,
                amount,
                self.market_maker_wallet,
            )
            .await?;
        self.record_cctp_burn(id, burn_receipt).await
    }

    /// Resumes a transfer stalled at `BridgingSubmitting` (AlpacaToBase direction):
    /// scans Ethereum for an already-submitted burn (adopting it to avoid a
    /// double-burn) and otherwise issues the burn, then records it.
    ///
    /// `pending_burn_tx` is the burn hash durably recorded by `RecordPendingBurn`
    /// before the receipt was awaited: when present, its on-chain status is
    /// checked directly (`burn_status`) instead of a mempool-blind log scan,
    /// closing the double-burn window on the timeout/crash redrive path.
    ///
    /// An empty scan (no recorded hash and no burn found on-chain) FAILS CLOSED
    /// (`BurnSubmitInconclusive`) rather than reburning; a reburn is issued ONLY
    /// when the recorded burn mined REVERTED (`pending_burn_tx.is_some()`). The
    /// genuine first burn is issued by `execute_cctp_burn_on_ethereum`, never here.
    async fn resume_bridging_submitting_ethereum(
        &self,
        id: &UsdcRebalanceId,
        amount: U256,
        from_block: u64,
        pending_burn_tx: Option<TxHash>,
    ) -> Result<BurnReceipt, UsdcTransferError> {
        if let Some(adopted) = self
            .check_pending_burn(id, BridgeDirection::EthereumToBase, amount, pending_burn_tx)
            .await?
        {
            return self.record_cctp_burn(id, adopted).await;
        }

        // Scan-EMPTY action: reburn only for a recorded-burn revert (`Some`, no funds
        // moved); fail closed when no hash was recorded (`None`, a broadcast may be
        // in flight). See `resume_bridging_submitting` for the full rationale.
        let reburn_on_empty = pending_burn_tx.is_some();

        let burn_receipt = match self
            .cctp_bridge
            .find_recent_burn(
                BridgeDirection::EthereumToBase,
                amount,
                self.market_maker_wallet,
                from_block,
            )
            .await
        {
            Ok(Some(existing_tx)) => {
                info!(
                    target: "rebalance",
                    %existing_tx,
                    "Adopting already-submitted CCTP burn on Ethereum resume"
                );
                BurnReceipt {
                    tx: existing_tx,
                    amount,
                }
            }
            Ok(None) if reburn_on_empty => {
                self.burn_recording_pending(
                    id,
                    BridgeDirection::EthereumToBase,
                    amount,
                    self.market_maker_wallet,
                )
                .await?
            }
            Ok(None) => {
                error!(
                    target: "rebalance",
                    %id,
                    "BridgingSubmitting resume (Ethereum): no recorded burn tx and none found \
                     on-chain; a broadcast may be in flight, failing closed for operator \
                     reconciliation (no auto-reburn)"
                );
                return Err(UsdcTransferError::BurnSubmitInconclusive { id: id.clone() });
            }
            // ScanInconclusive: the chain head hasn't advanced far enough past
            // `from_block` to trust an empty scan result. The aggregate is at
            // `BridgingSubmitting` (a durable state), so return
            // `SettlementCheckTransient` so the job delayed-redrives instead of
            // consuming the apalis retry budget.
            Err(error @ CctpError::ScanInconclusive { .. }) => {
                warn!(
                    target: "rebalance",
                    %id,
                    "CCTP burn scan on Ethereum inconclusive; will retry after delay"
                );
                return Err(UsdcTransferError::SettlementCheckTransient {
                    id: id.clone(),
                    source: Box::new(error),
                });
            }
            Err(error) => {
                warn!(target: "rebalance", "CCTP burn scan on Ethereum failed: {error}");
                return Err(UsdcTransferError::Cctp(Box::new(error)));
            }
        };

        self.record_cctp_burn(id, burn_receipt).await
    }

    #[instrument(target = "rebalance", skip(self, attestation_response), fields(%id), level = tracing::Level::DEBUG)]
    async fn execute_cctp_mint_on_ethereum(
        &self,
        id: &UsdcRebalanceId,
        attestation_response: AttestationResponse,
    ) -> Result<MintReceipt, UsdcTransferError> {
        // This `Err` arm records a durable, terminal `FailBridging`. What it
        // implies about the CCTP nonce depends on which `mint()` error landed:
        //
        // - A `receiveMessage` SUBMISSION error (e.g. a dropped/timed-out
        //   receipt on a load-balanced RPC) is routed through
        //   `recover_already_minted`, which probes `usedNonces()` and returns
        //   the real receipt if the mint landed. Reaching this arm that way
        //   means recovery found no receipt: the nonce read UNUSED, or the probe
        //   was inconclusive (the `usedNonces()` read or log scan failed, or the
        //   receipt could not be reconstructed). Both re-surface the submit
        //   error, so "definitely unused" and "unknown" are indistinguishable.
        // - Other `mint()` errors skip recovery entirely: notably a SUCCESSFUL
        //   `receiveMessage` whose receipt lacks the `MintAndWithdraw` event
        //   returns `MintAndWithdrawEventNotFound` with no `usedNonces()` probe,
        //   and there the nonce may already be consumed.
        //
        // Do not re-probe synchronously here -- a re-check cannot observe a mint
        // that confirms after this decision, and recovery is deferred, not lost:
        // a post-burn `BridgingFailed` (carries `burn_tx_hash`) is later un-failed
        // by `recover_from_bridging_failed` on a resume/redrive, which re-polls the
        // attestation, idempotently adopts the landed mint, and drives the deposit
        // leg to terminal. That resume is operator-triggered today (CLI/apalis
        // redrive); a pre-burn failure has no mint to adopt and stays terminal.
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

    /// Polls Alpaca for the deposit identified by the USDC `send_tx` (the
    /// transfer of minted USDC to Alpaca's deposit address), then sends
    /// `ConfirmDeposit`. Assumes the aggregate is already in `DepositInitiated`
    /// (caller has sent `InitiateDeposit`, or we are resuming from that state).
    #[instrument(target = "rebalance", skip(self), fields(%id, %send_tx), level = tracing::Level::DEBUG)]
    async fn poll_alpaca_deposit_and_confirm(
        &self,
        id: &UsdcRebalanceId,
        send_tx: TxHash,
    ) -> Result<(), UsdcTransferError> {
        info!(target: "rebalance", %send_tx, "Polling Alpaca for deposit detection");

        let transfer = match self.alpaca_wallet.poll_deposit_by_tx_hash(&send_tx).await {
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

    async fn record_conversion_or_fail(
        &self,
        id: &UsdcRebalanceId,
        correlation_id: &ClientOrderId,
        order: &CryptoOrderResponse,
        direction: ConversionDirection,
    ) -> Result<ConversionAmounts, UsdcTransferError> {
        match conversion_amounts_from_order(order, correlation_id, direction) {
            Ok(conversion) => Ok(conversion),
            Err(error) => {
                warn!(
                    order_id = %order.id,
                    ?direction,
                    %error,
                    "Failed to derive settled conversion amounts"
                );
                self.cqrs
                    .send(
                        id,
                        UsdcRebalanceCommand::FailConversion {
                            reason: error.to_string(),
                        },
                    )
                    .await?;
                Err(error)
            }
        }
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

fn conversion_amounts_from_order(
    order: &CryptoOrderResponse,
    correlation_id: &ClientOrderId,
    direction: ConversionDirection,
) -> Result<ConversionAmounts, UsdcTransferError> {
    let filled_quantity =
        order
            .filled_quantity
            .ok_or_else(|| UsdcTransferError::MissingFilledQuantity {
                order_id: correlation_id.clone(),
            })?;
    let filled_average_price =
        order
            .filled_average_price
            .ok_or_else(|| UsdcTransferError::MissingFilledAveragePrice {
                order_id: correlation_id.clone(),
            })?;

    let filled_quantity = Usdc::new(filled_quantity);
    let cash_proceeds = std::ops::Mul::mul(filled_quantity, filled_average_price)?;

    Ok(match direction {
        ConversionDirection::UsdToUsdc => ConversionAmounts::new(cash_proceeds, filled_quantity),
        ConversionDirection::UsdcToUsd => ConversionAmounts::new(filled_quantity, cash_proceeds),
    })
}

#[cfg(test)]
mod tests {
    use alloy::node_bindings::Anvil;
    use alloy::primitives::{B256, address, b256, fixed_bytes};
    use alloy::providers::ext::AnvilApi as _;
    use alloy::providers::{Provider, ProviderBuilder};
    use alloy::signers::local::PrivateKeySigner;
    use alloy::sol_types::SolEvent;
    use httpmock::prelude::*;
    use reqwest::StatusCode;
    use serde_json::json;
    use sqlx::SqlitePool;
    use std::str::FromStr;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use uuid::{Uuid, uuid};

    use st0x_execution::alpaca_broker_api::CryptoOrderFailureReason;
    use st0x_execution::{
        AlpacaAccountId, AlpacaBrokerApi, AlpacaBrokerApiCtx, AlpacaBrokerApiError,
        AlpacaBrokerApiMode, Executor, TimeInForce,
    };

    use st0x_bridge::Bridge;
    use st0x_bridge::cctp::{
        CctpAttestationMock, CctpBridge, CctpCtx, TestMintBurnToken, deploy_cctp_on_chain,
        link_chains, mint_usdc, set_max_burn_amount,
    };
    use st0x_event_sorcery::{AggregateError, LifecycleError, test_store};
    use st0x_evm::local::RawPrivateKeyWallet;
    use st0x_evm::{Evm, EvmError, IERC20, NoOpErrorRegistry, Wallet};
    use st0x_execution::{AlpacaTransferId, AlpacaWalletClient, AlpacaWalletError, PollingConfig};
    use st0x_raindex::{RaindexContracts, RaindexService};

    use super::*;
    use crate::telemetry::TelemetrySender;
    use crate::test_utils::{TestAnvilInstance, spawn_anvil, spawn_anvil_pair};
    use crate::usdc_rebalance::{
        RebalanceDirection, TransferRef, UsdcRebalanceError, UsdcRebalanceEvent,
    };
    use st0x_finance::UsdcConversionError;

    /// A minimal bridge double for tests that exercise `burn_recording_pending`.
    ///
    /// `submit_burn` returns a different hash on each call (first: all-1 bytes,
    /// second: all-2 bytes). `confirm_burn` returns an `EvmError::Reverted` on
    /// the first call and a successful `BurnReceipt` on the second call.
    /// All other Bridge and UsdcBridgeHelper methods `unimplemented!()`.
    struct MockBridge {
        submit_call_count: AtomicUsize,
        confirm_call_count: AtomicUsize,
    }

    impl MockBridge {
        fn new() -> Self {
            Self {
                submit_call_count: AtomicUsize::new(0),
                confirm_call_count: AtomicUsize::new(0),
            }
        }
    }

    #[async_trait::async_trait]
    impl st0x_bridge::Bridge for MockBridge {
        type Error = CctpError;
        type Attestation = AttestationResponse;

        async fn burn(
            &self,
            _direction: BridgeDirection,
            _amount: U256,
            _recipient: Address,
        ) -> Result<BurnReceipt, CctpError> {
            unimplemented!("MockBridge: burn not used in this test")
        }

        async fn submit_burn(
            &self,
            _direction: BridgeDirection,
            _amount: U256,
            _recipient: Address,
        ) -> Result<TxHash, CctpError> {
            let count = self.submit_call_count.fetch_add(1, Ordering::SeqCst);
            // Distinct, deterministic hash per call: first burn -> [1; 32],
            // second (retry) burn -> [2; 32]. The retry path issues exactly two
            // burns, so any further call is a test-logic error.
            let hash_byte: u8 = match count {
                0 => 1,
                1 => 2,
                other => panic!(
                    "MockBridge::submit_burn called {} times; expected exactly 2",
                    other + 1
                ),
            };
            Ok(TxHash::from([hash_byte; 32]))
        }

        async fn confirm_burn(
            &self,
            _direction: BridgeDirection,
            tx_hash: TxHash,
            amount: U256,
        ) -> Result<BurnReceipt, CctpError> {
            let count = self.confirm_call_count.fetch_add(1, Ordering::SeqCst);
            if count == 0 {
                Err(CctpError::Evm(EvmError::Reverted { tx_hash }))
            } else {
                Ok(BurnReceipt {
                    tx: tx_hash,
                    amount,
                })
            }
        }

        async fn burn_status(
            &self,
            _direction: BridgeDirection,
            _tx_hash: TxHash,
        ) -> Result<st0x_bridge::BurnTxStatus, CctpError> {
            unimplemented!("MockBridge: burn_status not used in this test")
        }

        async fn poll_attestation(
            &self,
            _direction: BridgeDirection,
            _burn_tx: TxHash,
        ) -> Result<AttestationResponse, CctpError> {
            unimplemented!("MockBridge: poll_attestation not used in this test")
        }

        async fn mint(
            &self,
            _direction: BridgeDirection,
            _attestation: &AttestationResponse,
        ) -> Result<st0x_bridge::MintReceipt, CctpError> {
            unimplemented!("MockBridge: mint not used in this test")
        }

        fn reconstruct_attestation(
            &self,
            _message: Vec<u8>,
            _attestation: Vec<u8>,
        ) -> Result<AttestationResponse, CctpError> {
            unimplemented!("MockBridge: reconstruct_attestation not used in this test")
        }

        async fn find_recent_burn(
            &self,
            _direction: BridgeDirection,
            _amount: U256,
            _recipient: Address,
            _from_block: u64,
        ) -> Result<Option<TxHash>, CctpError> {
            unimplemented!("MockBridge: find_recent_burn not used in this test")
        }

        async fn find_recent_mint(
            &self,
            _direction: BridgeDirection,
            _recipient: Address,
            _from_block: u64,
        ) -> Result<Option<st0x_bridge::MintReceipt>, CctpError> {
            unimplemented!("MockBridge: find_recent_mint not used in this test")
        }

        async fn destination_block(&self, _direction: BridgeDirection) -> Result<u64, CctpError> {
            unimplemented!("MockBridge: destination_block not used in this test")
        }

        async fn source_block(&self, _direction: BridgeDirection) -> Result<u64, CctpError> {
            unimplemented!("MockBridge: source_block not used in this test")
        }
    }

    #[async_trait::async_trait]
    impl UsdcBridgeHelper for MockBridge {
        async fn ethereum_tx_confirmations(
            &self,
            _tx_hash: TxHash,
        ) -> Result<Option<u64>, CctpError> {
            unimplemented!("MockBridge: ethereum_tx_confirmations not used in this test")
        }

        async fn ethereum_tx_block(&self, _tx_hash: TxHash) -> Result<u64, CctpError> {
            unimplemented!("MockBridge: ethereum_tx_block not used in this test")
        }

        async fn ethereum_usdc_balance(&self, _holder: Address) -> Result<U256, CctpError> {
            unimplemented!("MockBridge: ethereum_usdc_balance not used in this test")
        }

        async fn send_usdc_on_ethereum(
            &self,
            _to: Address,
            _amount: U256,
        ) -> Result<TxHash, CctpError> {
            unimplemented!("MockBridge: send_usdc_on_ethereum not used in this test")
        }

        async fn find_recent_usdc_transfer(
            &self,
            _from: Address,
            _to: Address,
            _amount: U256,
            _from_block: u64,
        ) -> Result<Option<TxHash>, CctpError> {
            unimplemented!("MockBridge: find_recent_usdc_transfer not used in this test")
        }
    }

    fn usdc(value: &str) -> Usdc {
        Usdc::from_str(value).unwrap()
    }

    /// A conversion with equal source and received amounts (no slippage),
    /// for tests that only care about the post-conversion effective amount.
    fn par_conversion(amount: Usdc) -> ConversionAmounts {
        ConversionAmounts::new(amount, amount)
    }

    const USDC_ADDRESS: Address = address!("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48");
    const ORDERBOOK_ADDRESS: Address = address!("0x1234567890123456789012345678901234567890");
    const TEST_VAULT_ID: RaindexVaultId = RaindexVaultId(b256!(
        "0x0000000000000000000000000000000000000000000000000000000000000001"
    ));
    const TEST_ATTESTATION_RETRY_DEADLINE: Duration = Duration::from_secs(24 * 60 * 60);

    fn test_settlement_params() -> UsdcSettlementParams {
        UsdcSettlementParams {
            attestation_retry_deadline: TEST_ATTESTATION_RETRY_DEADLINE,
            required_confirmations: 3,
            #[cfg(feature = "test-support")]
            circle_api_base: st0x_bridge::cctp::CIRCLE_API_BASE.to_string(),
            #[cfg(feature = "test-support")]
            token_messenger: st0x_bridge::cctp::TOKEN_MESSENGER_V2,
            #[cfg(feature = "test-support")]
            message_transmitter: st0x_bridge::cctp::MESSAGE_TRANSMITTER_V2,
        }
    }

    async fn create_test_store_instance() -> Arc<Store<UsdcRebalance>> {
        let pool = SqlitePool::connect(":memory:").await.unwrap();
        sqlx::migrate!().run(&pool).await.unwrap();

        Arc::new(test_store(pool))
    }

    /// Advances aggregate through: Initiate -> ConfirmWithdrawal ->
    /// InitiateBridging -> ReceiveAttestation -> ConfirmBridging ->
    /// InitiateDeposit -> ConfirmDeposit
    async fn advance_to_deposit_confirmed_base_to_alpaca(
        cqrs: &Store<UsdcRebalance>,
        id: &UsdcRebalanceId,
        amount: Usdc,
        bridged_amount_received: Usdc,
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

        cqrs.send(
            id,
            UsdcRebalanceCommand::ConfirmWithdrawal {
                withdrawal_tx: None,
            },
        )
        .await
        .unwrap();

        cqrs.send(id, UsdcRebalanceCommand::InitiateBridging { burn_tx })
            .await
            .unwrap();

        cqrs.send(
            id,
            UsdcRebalanceCommand::ReceiveAttestation {
                attestation: vec![0x01],
                cctp_nonce: B256::left_padding_from(&99999u64.to_be_bytes()),
                message: valid_cctp_message(),
                mint_scan_from_block: 100,
            },
        )
        .await
        .unwrap();

        cqrs.send(
            id,
            UsdcRebalanceCommand::ConfirmBridging {
                mint_tx,
                amount_received: bridged_amount_received,
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

    fn setup_anvil() -> (TestAnvilInstance, String, B256) {
        let anvil = spawn_anvil(Anvil::new());
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
        use UsdcRebalanceCommand::*;

        let burn_tx =
            fixed_bytes!("0xaaaa000000000000000000000000000000000000000000000000000000000001");
        let mint_tx =
            fixed_bytes!("0xaaaa111111111111111111111111111111111111111111111111111111111111");

        cqrs.send(
            id,
            InitiateConversion {
                direction: RebalanceDirection::AlpacaToBase,
                amount,
                order_id: ClientOrderId::from_uuid(Uuid::new_v4()),
            },
        )
        .await
        .unwrap();
        cqrs.send(
            id,
            ConfirmConversion {
                conversion: par_conversion(amount),
            },
        )
        .await
        .unwrap();
        cqrs.send(
            id,
            Initiate {
                direction: RebalanceDirection::AlpacaToBase,
                amount,
                withdrawal: TransferRef::AlpacaId(AlpacaTransferId::from(Uuid::new_v4())),
            },
        )
        .await
        .unwrap();
        cqrs.send(
            id,
            ConfirmWithdrawal {
                withdrawal_tx: None,
            },
        )
        .await
        .unwrap();
        cqrs.send(id, InitiateBridging { burn_tx }).await.unwrap();
        cqrs.send(
            id,
            ReceiveAttestation {
                attestation: vec![0x01],
                cctp_nonce: B256::left_padding_from(&12345u64.to_be_bytes()),
                message: valid_cctp_message(),
                mint_scan_from_block: 100,
            },
        )
        .await
        .unwrap();
        cqrs.send(
            id,
            ConfirmBridging {
                mint_tx,
                amount_received: amount,
                fee_collected: usdc("0"),
            },
        )
        .await
        .unwrap();
        cqrs.send(
            id,
            InitiateDeposit {
                deposit: TransferRef::OnchainTx(deposit_tx),
            },
        )
        .await
        .unwrap();
    }

    /// Advances an Alpaca->Base aggregate to the terminal `DepositConfirmed`
    /// state by confirming the persisted vault deposit.
    async fn advance_to_deposit_confirmed_alpaca_to_base(
        cqrs: &Store<UsdcRebalance>,
        id: &UsdcRebalanceId,
        amount: Usdc,
        deposit_tx: TxHash,
    ) {
        advance_to_deposit_initiated_alpaca_to_base(cqrs, id, amount, deposit_tx).await;
        cqrs.send(id, UsdcRebalanceCommand::ConfirmDeposit)
            .await
            .unwrap();
    }

    /// Drives an Alpaca->Base aggregate through `InitiateConversion` ->
    /// `ConfirmConversion` -> `Initiate` -> `ConfirmWithdrawal` ->
    /// `InitiateBridging` -> `ReceiveAttestation` so it lands at `Attested`,
    /// where the next callable step is the mint.
    async fn advance_to_attested_alpaca_to_base(
        cqrs: &Store<UsdcRebalance>,
        id: &UsdcRebalanceId,
        amount: Usdc,
    ) {
        use UsdcRebalanceCommand::*;

        let burn_tx =
            fixed_bytes!("0xaaaa000000000000000000000000000000000000000000000000000000000001");

        cqrs.send(
            id,
            InitiateConversion {
                direction: RebalanceDirection::AlpacaToBase,
                amount,
                order_id: ClientOrderId::from_uuid(Uuid::new_v4()),
            },
        )
        .await
        .unwrap();
        cqrs.send(
            id,
            ConfirmConversion {
                conversion: par_conversion(amount),
            },
        )
        .await
        .unwrap();
        cqrs.send(
            id,
            Initiate {
                direction: RebalanceDirection::AlpacaToBase,
                amount,
                withdrawal: TransferRef::AlpacaId(AlpacaTransferId::from(Uuid::new_v4())),
            },
        )
        .await
        .unwrap();
        cqrs.send(
            id,
            ConfirmWithdrawal {
                withdrawal_tx: None,
            },
        )
        .await
        .unwrap();
        cqrs.send(id, InitiateBridging { burn_tx }).await.unwrap();
        cqrs.send(
            id,
            ReceiveAttestation {
                attestation: vec![0x01],
                // Must equal the nonce embedded in `valid_cctp_message()` (byte 43 = 1):
                // an `Attested` resume cross-checks the re-derived envelope nonce against
                // this recorded value and fails on a mismatch.
                cctp_nonce: B256::left_padding_from(&1u64.to_be_bytes()),
                message: valid_cctp_message(),
                // Scan from genesis: the fresh test chain sits below 100, so a 0
                // bound makes the find_recent_mint scan a valid range that
                // genuinely finds no mint, letting resume proceed to the mint.
                mint_scan_from_block: 0,
            },
        )
        .await
        .unwrap();
    }

    /// Drives an Alpaca->Base aggregate to `Bridged` (mint confirmed), where the
    /// next callable step is the crash-safe vault deposit.
    async fn advance_to_bridged_alpaca_to_base(
        cqrs: &Store<UsdcRebalance>,
        id: &UsdcRebalanceId,
        amount: Usdc,
    ) {
        advance_to_attested_alpaca_to_base(cqrs, id, amount).await;

        let mint_tx =
            fixed_bytes!("0xaaaa111111111111111111111111111111111111111111111111111111111111");
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

    fn create_test_onchain_services<Chain: Wallet + Clone>(
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

        let owner = wallet.address();

        let vault_service = RaindexService::new(
            wallet,
            RaindexContracts {
                inventory: ORDERBOOK_ADDRESS,
                orderbook: ORDERBOOK_ADDRESS,
            },
            owner,
        );

        (cctp_bridge, vault_service)
    }

    /// Like [`create_test_onchain_services`] but points the CCTP bridge's Circle
    /// attestation polling at `circle_api_base` (a local mock), so attestation
    /// polling resolves immediately instead of retrying the real Circle endpoint
    /// for ~5 minutes. Test-support only.
    #[cfg(feature = "test-support")]
    fn create_test_onchain_services_with_circle_api<Chain: Wallet + Clone>(
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

        let owner = wallet.address();
        let vault_service = RaindexService::new(
            wallet,
            RaindexContracts {
                inventory: ORDERBOOK_ADDRESS,
                orderbook: ORDERBOOK_ADDRESS,
            },
            owner,
        );

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

    /// Creates a mock with explicit fill quantity and average price.
    fn create_get_order_mock_with_fill<'a>(
        server: &'a MockServer,
        order_id: &str,
        requested_qty: &str,
        filled_qty: &str,
        filled_avg_price: &str,
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
                    "filled_avg_price": filled_avg_price,
                    "filled_qty": filled_qty,
                    "created_at": "2024-01-15T10:30:00Z"
                }));
        })
    }

    fn create_get_order_mock_missing_average_price<'a>(
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
                    "filled_avg_price": null,
                    "filled_qty": filled_qty,
                    "created_at": "2024-01-15T10:30:00Z"
                }));
        })
    }

    fn create_get_order_mock_missing_quantity<'a>(
        server: &'a MockServer,
        order_id: &str,
        requested_qty: &str,
        filled_avg_price: &str,
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
                    "filled_avg_price": filled_avg_price,
                    "filled_qty": null,
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

        let alpaca_broker = InstrumentedAlpacaBroker::new(
            create_test_broker_service(&server).await,
            TelemetrySender::disabled(),
        );
        let alpaca_wallet = Arc::new(create_test_wallet_service(&server));
        let wallet = create_test_wallet(&endpoint, &private_key);
        let (cctp_bridge, vault_service) = create_test_onchain_services(wallet);
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
            &test_settlement_params(),
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

        let alpaca_broker = InstrumentedAlpacaBroker::new(
            create_test_broker_service(&server).await,
            TelemetrySender::disabled(),
        );
        let alpaca_wallet = Arc::new(create_test_wallet_service(&server));
        let wallet = create_test_wallet(&endpoint, &private_key);
        let (cctp_bridge, vault_service) = create_test_onchain_services(wallet);
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
            &test_settlement_params(),
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

        let alpaca_broker = InstrumentedAlpacaBroker::new(
            create_test_broker_service(&server).await,
            TelemetrySender::disabled(),
        );
        let alpaca_wallet = Arc::new(create_test_wallet_service(&server));
        let wallet = create_test_wallet(&endpoint, &private_key);
        let (cctp_bridge, vault_service) = create_test_onchain_services(wallet);
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
            &test_settlement_params(),
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

        let alpaca_broker = InstrumentedAlpacaBroker::new(
            create_test_broker_service(&server).await,
            TelemetrySender::disabled(),
        );
        let alpaca_wallet = Arc::new(create_test_wallet_service(&server));
        let wallet = create_test_wallet(&endpoint, &private_key);
        let (cctp_bridge, vault_service) = create_test_onchain_services(wallet);
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
            &test_settlement_params(),
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

    /// A non-revert CCTP burn-submission failure on the forward path (here: the
    /// burn RPCs fail because no CCTP contracts are deployed) must FAIL CLOSED as
    /// `BurnSubmitInconclusive` -- the broadcast may have reached the network, so a
    /// later retry must not reburn. (A clean revert, by contrast, would surface
    /// `BurnRevert` and take the bounded revert-redrive path.)
    #[tokio::test]
    async fn test_execute_base_to_alpaca_cctp_burn_submit_failure_fails_closed() {
        let server = MockServer::start();
        let (_anvil, endpoint, private_key) = setup_anvil();

        let alpaca_broker = InstrumentedAlpacaBroker::new(
            create_test_broker_service(&server).await,
            TelemetrySender::disabled(),
        );
        let alpaca_wallet = Arc::new(create_test_wallet_service(&server));
        let wallet = create_test_wallet(&endpoint, &private_key);
        let (cctp_bridge, vault_service) = create_test_onchain_services(wallet);
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
            &test_settlement_params(),
        );

        let id = UsdcRebalanceId(Uuid::new_v4());
        let amount = usdc("1000");

        let error = manager
            .execute_base_to_alpaca(&id, amount)
            .await
            .unwrap_err();

        assert!(
            matches!(error, UsdcTransferError::BurnSubmitInconclusive { .. }),
            "a non-revert CCTP burn submission failure must fail closed as \
             BurnSubmitInconclusive (the broadcast may have landed), got: {error:?}"
        );
    }

    #[tokio::test]
    async fn test_execute_usd_to_usdc_conversion_places_buy_order() {
        let server = MockServer::start();
        let (_anvil, endpoint, private_key) = setup_anvil();

        let _account_mock = create_broker_account_mock(&server);
        let alpaca_broker = InstrumentedAlpacaBroker::new(
            create_test_broker_service(&server).await,
            TelemetrySender::disabled(),
        );
        let alpaca_wallet = Arc::new(create_test_wallet_service(&server));
        let wallet = create_test_wallet(&endpoint, &private_key);
        let (cctp_bridge, vault_service) = create_test_onchain_services(wallet);
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
            &test_settlement_params(),
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
        let alpaca_broker = InstrumentedAlpacaBroker::new(
            create_test_broker_service(&server).await,
            TelemetrySender::disabled(),
        );
        let alpaca_wallet = Arc::new(create_test_wallet_service(&server));
        let wallet = create_test_wallet(&endpoint, &private_key);
        let (cctp_bridge, vault_service) = create_test_onchain_services(wallet);
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
            &test_settlement_params(),
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
        let alpaca_broker = InstrumentedAlpacaBroker::new(
            create_test_broker_service(&server).await,
            TelemetrySender::disabled(),
        );
        let alpaca_wallet = Arc::new(create_test_wallet_service(&server));
        let wallet = create_test_wallet(&endpoint, &private_key);
        let (cctp_bridge, vault_service) = create_test_onchain_services(wallet);
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
            &test_settlement_params(),
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
        let alpaca_broker = InstrumentedAlpacaBroker::new(
            create_test_broker_service(&server).await,
            TelemetrySender::disabled(),
        );
        let alpaca_wallet = Arc::new(create_test_wallet_service(&server));
        let wallet = create_test_wallet(&endpoint, &private_key);
        let (cctp_bridge, vault_service) = create_test_onchain_services(wallet);
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
            &test_settlement_params(),
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
        let alpaca_broker = InstrumentedAlpacaBroker::new(
            create_test_broker_service(&server).await,
            TelemetrySender::disabled(),
        );
        let alpaca_wallet = Arc::new(create_test_wallet_service(&server));
        let wallet = create_test_wallet(&endpoint, &private_key);
        let (cctp_bridge, vault_service) = create_test_onchain_services(wallet);
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

        cqrs.send(
            &id,
            UsdcRebalanceCommand::ConfirmWithdrawal {
                withdrawal_tx: None,
            },
        )
        .await
        .unwrap();

        cqrs.send(&id, UsdcRebalanceCommand::InitiateBridging { burn_tx })
            .await
            .unwrap();

        cqrs.send(
            &id,
            UsdcRebalanceCommand::ReceiveAttestation {
                attestation: vec![0x01],
                cctp_nonce: B256::left_padding_from(&12345u64.to_be_bytes()),
                message: valid_cctp_message(),
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
            &test_settlement_params(),
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
        let alpaca_broker = InstrumentedAlpacaBroker::new(
            create_test_broker_service(&server).await,
            TelemetrySender::disabled(),
        );
        let alpaca_wallet = Arc::new(create_test_wallet_service(&server));
        let wallet = create_test_wallet(&endpoint, &private_key);
        let (cctp_bridge, vault_service) = create_test_onchain_services(wallet);
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
            &test_settlement_params(),
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
                    order_id: ClientOrderId::from_uuid(Uuid::new_v4()),
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

        let alpaca_broker = InstrumentedAlpacaBroker::new(
            create_test_broker_service(&server).await,
            TelemetrySender::disabled(),
        );
        let alpaca_wallet = Arc::new(create_test_wallet_service(&server));
        let wallet = create_test_wallet(&endpoint, &private_key);
        let (cctp_bridge, vault_service) = create_test_onchain_services(wallet);
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
            &test_settlement_params(),
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

        let alpaca_broker = InstrumentedAlpacaBroker::new(
            create_test_broker_service(&server).await,
            TelemetrySender::disabled(),
        );
        let alpaca_wallet = Arc::new(create_test_wallet_service(&server));
        let wallet = create_test_wallet(&endpoint, &private_key);
        let (cctp_bridge, vault_service) = create_test_onchain_services(wallet);
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
            &test_settlement_params(),
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

        advance_to_deposit_confirmed_base_to_alpaca(&cqrs, &id, amount, amount_received).await;

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
        let alpaca_broker = InstrumentedAlpacaBroker::new(
            create_test_broker_service(&server).await,
            TelemetrySender::disabled(),
        );
        let alpaca_wallet = Arc::new(create_test_wallet_service(&server));
        let wallet = create_test_wallet(&endpoint, &private_key);
        let (cctp_bridge, vault_service) = create_test_onchain_services(wallet);
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
            &test_settlement_params(),
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
        let alpaca_broker = InstrumentedAlpacaBroker::new(
            create_test_broker_service(&server).await,
            TelemetrySender::disabled(),
        );
        let alpaca_wallet = Arc::new(create_test_wallet_service(&server));
        let wallet = create_test_wallet(&endpoint, &private_key);
        let (cctp_bridge, vault_service) = create_test_onchain_services(wallet);
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
                order_id: ClientOrderId::from_uuid(Uuid::new_v4()),
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
            &test_settlement_params(),
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
        let alpaca_broker = InstrumentedAlpacaBroker::new(
            create_test_broker_service(&server).await,
            TelemetrySender::disabled(),
        );
        let alpaca_wallet = Arc::new(create_test_wallet_service(&server));
        let wallet = create_test_wallet(&endpoint, &private_key);
        let (cctp_bridge, vault_service) = create_test_onchain_services(wallet);
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
            &test_settlement_params(),
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
                    order_id: ClientOrderId::from_uuid(Uuid::new_v4()),
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
        let alpaca_broker = InstrumentedAlpacaBroker::new(
            create_test_broker_service(&server).await,
            TelemetrySender::disabled(),
        );
        let alpaca_wallet = Arc::new(create_test_wallet_service(&server));
        let wallet = create_test_wallet(&endpoint, &private_key);
        let (cctp_bridge, vault_service) = create_test_onchain_services(wallet);
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
            &test_settlement_params(),
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
    async fn usd_to_usdc_conversion_returns_actual_received_amount() {
        let server = MockServer::start();
        let (_anvil, endpoint, private_key) = setup_anvil();

        let _account_mock = create_broker_account_mock(&server);
        let alpaca_broker = InstrumentedAlpacaBroker::new(
            create_test_broker_service(&server).await,
            TelemetrySender::disabled(),
        );
        let alpaca_wallet = Arc::new(create_test_wallet_service(&server));
        let wallet = create_test_wallet(&endpoint, &private_key);
        let (cctp_bridge, vault_service) = create_test_onchain_services(wallet);
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
            &test_settlement_params(),
        );

        // Request 1000, but only 999.5 fills due to slippage
        let _order_mock = create_conversion_order_pending_mock(&server, "1000");
        let _get_mock = create_get_order_mock_with_fill(
            &server,
            "61e7b016-9c91-4a97-b912-615c9d365c9d",
            "1000",
            "999.5",
            "1.0001",
        );

        let id = UsdcRebalanceId(Uuid::new_v4());
        let requested_amount = usdc("1000");

        let received_amount = manager
            .execute_usd_to_usdc_conversion(&id, requested_amount)
            .await
            .unwrap();

        // Should return the actual received amount, not the requested amount.
        assert_eq!(
            received_amount,
            usdc("999.5"),
            "Should return actual received amount, not requested amount"
        );
    }

    #[tokio::test]
    async fn usd_to_usdc_conversion_missing_average_price_fails_aggregate() {
        let server = MockServer::start();
        let (_anvil, endpoint, private_key) = setup_anvil();

        let _account_mock = create_broker_account_mock(&server);
        let alpaca_broker = InstrumentedAlpacaBroker::new(
            create_test_broker_service(&server).await,
            TelemetrySender::disabled(),
        );
        let alpaca_wallet = Arc::new(create_test_wallet_service(&server));
        let wallet = create_test_wallet(&endpoint, &private_key);
        let (cctp_bridge, vault_service) = create_test_onchain_services(wallet);
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
            &test_settlement_params(),
        );

        let _order_mock = create_conversion_order_pending_mock(&server, "1000");
        let _get_mock = create_get_order_mock_missing_average_price(
            &server,
            "61e7b016-9c91-4a97-b912-615c9d365c9d",
            "1000",
            "1000",
        );

        let id = UsdcRebalanceId(Uuid::new_v4());
        let amount = usdc("1000");

        assert!(
            matches!(
                manager.execute_usd_to_usdc_conversion(&id, amount).await,
                Err(UsdcTransferError::MissingFilledAveragePrice { .. })
            ),
            "Missing fill price should fail the conversion"
        );

        let retry_result = cqrs
            .send(
                &id,
                UsdcRebalanceCommand::FailConversion {
                    reason: "should already be failed".to_string(),
                },
            )
            .await;

        assert!(
            matches!(
                retry_result,
                Err(AggregateError::UserError(LifecycleError::Apply(
                    UsdcRebalanceError::ConversionAlreadyCompleted
                )))
            ),
            "Aggregate should already be in ConversionFailed, got: {retry_result:?}"
        );
    }

    #[tokio::test]
    async fn usd_to_usdc_conversion_missing_quantity_fails_aggregate() {
        let server = MockServer::start();
        let (_anvil, endpoint, private_key) = setup_anvil();

        let _account_mock = create_broker_account_mock(&server);
        let alpaca_broker = InstrumentedAlpacaBroker::new(
            create_test_broker_service(&server).await,
            TelemetrySender::disabled(),
        );
        let alpaca_wallet = Arc::new(create_test_wallet_service(&server));
        let wallet = create_test_wallet(&endpoint, &private_key);
        let (cctp_bridge, vault_service) = create_test_onchain_services(wallet);
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
            &test_settlement_params(),
        );

        let _order_mock = create_conversion_order_pending_mock(&server, "1000");
        let _get_mock = create_get_order_mock_missing_quantity(
            &server,
            "61e7b016-9c91-4a97-b912-615c9d365c9d",
            "1000",
            "1.0001",
        );

        let id = UsdcRebalanceId(Uuid::new_v4());
        let amount = usdc("1000");

        assert!(
            matches!(
                manager.execute_usd_to_usdc_conversion(&id, amount).await,
                Err(UsdcTransferError::MissingFilledQuantity { .. })
            ),
            "Missing fill quantity should fail the conversion"
        );

        let retry_result = cqrs
            .send(
                &id,
                UsdcRebalanceCommand::FailConversion {
                    reason: "should already be failed".to_string(),
                },
            )
            .await;

        assert!(
            matches!(
                retry_result,
                Err(AggregateError::UserError(LifecycleError::Apply(
                    UsdcRebalanceError::ConversionAlreadyCompleted
                )))
            ),
            "Aggregate should already be in ConversionFailed, got: {retry_result:?}"
        );
    }

    #[tokio::test]
    async fn usdc_to_usd_conversion_returns_actual_proceeds() {
        let server = MockServer::start();
        let (_anvil, endpoint, private_key) = setup_anvil();

        let _account_mock = create_broker_account_mock(&server);
        let alpaca_broker = InstrumentedAlpacaBroker::new(
            create_test_broker_service(&server).await,
            TelemetrySender::disabled(),
        );
        let alpaca_wallet = Arc::new(create_test_wallet_service(&server));
        let wallet = create_test_wallet(&endpoint, &private_key);
        let (cctp_bridge, vault_service) = create_test_onchain_services(wallet);
        let cqrs = create_test_store_instance().await;

        let market_maker_wallet = address!("0x1111111111111111111111111111111111111111");
        let id = UsdcRebalanceId(Uuid::new_v4());
        let rebalance_amount = usdc("1000");
        let deposited_amount = usdc("1000");

        advance_to_deposit_confirmed_base_to_alpaca(&cqrs, &id, rebalance_amount, deposited_amount)
            .await;

        let manager = CrossVenueCashTransfer::new(
            alpaca_broker,
            alpaca_wallet,
            Arc::new(cctp_bridge),
            Arc::new(vault_service),
            Arc::clone(&cqrs),
            market_maker_wallet,
            TEST_VAULT_ID,
            &test_settlement_params(),
        );

        let _order_mock = create_conversion_order_pending_mock(&server, "1000");
        let _get_mock = create_get_order_mock_with_fill(
            &server,
            "61e7b016-9c91-4a97-b912-615c9d365c9d",
            "1000",
            "1000",
            "0.9983",
        );

        let proceeds = manager
            .execute_usdc_to_usd_conversion(&id, deposited_amount)
            .await
            .unwrap();

        assert_eq!(
            proceeds,
            usdc("998.3"),
            "Should return actual USD proceeds, not the USDC sold amount"
        );
    }

    #[tokio::test]
    async fn usdc_to_usd_conversion_missing_average_price_fails_aggregate() {
        let server = MockServer::start();
        let (_anvil, endpoint, private_key) = setup_anvil();

        let _account_mock = create_broker_account_mock(&server);
        let alpaca_broker = InstrumentedAlpacaBroker::new(
            create_test_broker_service(&server).await,
            TelemetrySender::disabled(),
        );
        let alpaca_wallet = Arc::new(create_test_wallet_service(&server));
        let wallet = create_test_wallet(&endpoint, &private_key);
        let (cctp_bridge, vault_service) = create_test_onchain_services(wallet);
        let cqrs = create_test_store_instance().await;

        let market_maker_wallet = address!("0x1111111111111111111111111111111111111111");
        let id = UsdcRebalanceId(Uuid::new_v4());
        let rebalance_amount = usdc("1000");
        let deposited_amount = usdc("1000");

        advance_to_deposit_confirmed_base_to_alpaca(&cqrs, &id, rebalance_amount, deposited_amount)
            .await;

        let manager = CrossVenueCashTransfer::new(
            alpaca_broker,
            alpaca_wallet,
            Arc::new(cctp_bridge),
            Arc::new(vault_service),
            Arc::clone(&cqrs),
            market_maker_wallet,
            TEST_VAULT_ID,
            &test_settlement_params(),
        );

        let _order_mock = create_conversion_order_pending_mock(&server, "1000");
        let _get_mock = create_get_order_mock_missing_average_price(
            &server,
            "61e7b016-9c91-4a97-b912-615c9d365c9d",
            "1000",
            "1000",
        );

        assert!(
            matches!(
                manager
                    .execute_usdc_to_usd_conversion(&id, deposited_amount)
                    .await,
                Err(UsdcTransferError::MissingFilledAveragePrice { .. })
            ),
            "Missing fill price should fail the post-deposit conversion"
        );

        let retry_result = cqrs
            .send(
                &id,
                UsdcRebalanceCommand::FailConversion {
                    reason: "should already be failed".to_string(),
                },
            )
            .await;

        assert!(
            matches!(
                retry_result,
                Err(AggregateError::UserError(LifecycleError::Apply(
                    UsdcRebalanceError::ConversionAlreadyCompleted
                )))
            ),
            "Aggregate should already be in ConversionFailed, got: {retry_result:?}"
        );
    }

    #[tokio::test]
    async fn usdc_to_usd_conversion_missing_quantity_fails_aggregate() {
        let server = MockServer::start();
        let (_anvil, endpoint, private_key) = setup_anvil();

        let _account_mock = create_broker_account_mock(&server);
        let alpaca_broker = InstrumentedAlpacaBroker::new(
            create_test_broker_service(&server).await,
            TelemetrySender::disabled(),
        );
        let alpaca_wallet = Arc::new(create_test_wallet_service(&server));
        let wallet = create_test_wallet(&endpoint, &private_key);
        let (cctp_bridge, vault_service) = create_test_onchain_services(wallet);
        let cqrs = create_test_store_instance().await;

        let market_maker_wallet = address!("0x1111111111111111111111111111111111111111");
        let id = UsdcRebalanceId(Uuid::new_v4());
        let rebalance_amount = usdc("1000");
        let deposited_amount = usdc("1000");

        advance_to_deposit_confirmed_base_to_alpaca(&cqrs, &id, rebalance_amount, deposited_amount)
            .await;

        let manager = CrossVenueCashTransfer::new(
            alpaca_broker,
            alpaca_wallet,
            Arc::new(cctp_bridge),
            Arc::new(vault_service),
            Arc::clone(&cqrs),
            market_maker_wallet,
            TEST_VAULT_ID,
            &test_settlement_params(),
        );

        let _order_mock = create_conversion_order_pending_mock(&server, "1000");
        let _get_mock = create_get_order_mock_missing_quantity(
            &server,
            "61e7b016-9c91-4a97-b912-615c9d365c9d",
            "1000",
            "0.9983",
        );

        assert!(
            matches!(
                manager
                    .execute_usdc_to_usd_conversion(&id, deposited_amount)
                    .await,
                Err(UsdcTransferError::MissingFilledQuantity { .. })
            ),
            "Missing fill quantity should fail the post-deposit conversion"
        );

        let retry_result = cqrs
            .send(
                &id,
                UsdcRebalanceCommand::FailConversion {
                    reason: "should already be failed".to_string(),
                },
            )
            .await;

        assert!(
            matches!(
                retry_result,
                Err(AggregateError::UserError(LifecycleError::Apply(
                    UsdcRebalanceError::ConversionAlreadyCompleted
                )))
            ),
            "Aggregate should already be in ConversionFailed, got: {retry_result:?}"
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
        TestAnvilInstance,
    ) {
        let (anvil, endpoint, private_key) = setup_anvil();
        let alpaca_broker = InstrumentedAlpacaBroker::new(
            create_test_broker_service(server).await,
            TelemetrySender::disabled(),
        );
        let alpaca_wallet = Arc::new(create_test_wallet_service(server));
        let wallet = create_test_wallet(&endpoint, &private_key);
        let (cctp_bridge, vault_service) = create_test_onchain_services(wallet);
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
            &test_settlement_params(),
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
        TestAnvilInstance,
    ) {
        let (anvil, endpoint, private_key) = setup_anvil();
        let alpaca_broker = InstrumentedAlpacaBroker::new(
            create_test_broker_service(server).await,
            TelemetrySender::disabled(),
        );
        let alpaca_wallet = Arc::new(create_test_wallet_service(server));
        let wallet = create_test_wallet(&endpoint, &private_key);
        let (cctp_bridge, vault_service) =
            create_test_onchain_services_with_circle_api(wallet, circle_api_base);
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
            &test_settlement_params(),
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
        cqrs.send(
            id,
            UsdcRebalanceCommand::ConfirmWithdrawal {
                withdrawal_tx: None,
            },
        )
        .await
        .unwrap();
        cqrs.send(id, UsdcRebalanceCommand::InitiateBridging { burn_tx })
            .await
            .unwrap();
        cqrs.send(
            id,
            UsdcRebalanceCommand::ReceiveAttestation {
                attestation: vec![0x01],
                // Must equal the nonce embedded in `valid_cctp_message()` (byte 43 = 1):
                // an `Attested` resume cross-checks the re-derived envelope nonce against
                // this recorded value and fails on a mismatch.
                cctp_nonce: B256::left_padding_from(&1u64.to_be_bytes()),
                message: valid_cctp_message(),
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

    async fn advance_to_awaiting_attestation_base_to_alpaca(
        cqrs: &Store<UsdcRebalance>,
        id: &UsdcRebalanceId,
        amount: Usdc,
        retry_deadline_at: DateTime<Utc>,
    ) -> TxHash {
        let burn_tx =
            fixed_bytes!("0xaaaa000000000000000000000000000000000000000000000000000000000002");

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
        cqrs.send(
            id,
            UsdcRebalanceCommand::ConfirmWithdrawal {
                withdrawal_tx: None,
            },
        )
        .await
        .unwrap();
        cqrs.send(id, UsdcRebalanceCommand::InitiateBridging { burn_tx })
            .await
            .unwrap();
        cqrs.send(
            id,
            UsdcRebalanceCommand::TimeoutAttestation { retry_deadline_at },
        )
        .await
        .unwrap();

        burn_tx
    }

    async fn advance_to_awaiting_attestation_alpaca_to_base(
        cqrs: &Store<UsdcRebalance>,
        id: &UsdcRebalanceId,
        amount: Usdc,
        retry_deadline_at: DateTime<Utc>,
    ) -> TxHash {
        let burn_tx =
            fixed_bytes!("0xbbbb000000000000000000000000000000000000000000000000000000000002");

        cqrs.send(
            id,
            UsdcRebalanceCommand::InitiateConversion {
                direction: RebalanceDirection::AlpacaToBase,
                amount,
                order_id: ClientOrderId::from_uuid(Uuid::new_v4()),
            },
        )
        .await
        .unwrap();
        cqrs.send(
            id,
            UsdcRebalanceCommand::ConfirmConversion {
                conversion: par_conversion(amount),
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
        cqrs.send(
            id,
            UsdcRebalanceCommand::ConfirmWithdrawal {
                withdrawal_tx: None,
            },
        )
        .await
        .unwrap();
        cqrs.send(id, UsdcRebalanceCommand::InitiateBridging { burn_tx })
            .await
            .unwrap();
        cqrs.send(
            id,
            UsdcRebalanceCommand::TimeoutAttestation { retry_deadline_at },
        )
        .await
        .unwrap();

        burn_tx
    }

    #[tokio::test]
    async fn resume_base_to_alpaca_from_awaiting_attestation_after_deadline_fails_bridging() {
        let server = MockServer::start();
        let (manager, cqrs, _anvil) = make_resume_test_manager(&server).await;
        let id = UsdcRebalanceId(Uuid::new_v4());
        let amount = usdc("100");
        let retry_deadline_at = Utc::now() - chrono::Duration::seconds(1);
        let burn_tx =
            advance_to_awaiting_attestation_base_to_alpaca(&cqrs, &id, amount, retry_deadline_at)
                .await;

        let error = manager
            .resume_base_to_alpaca(&id, amount)
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            UsdcTransferError::AttestationRetryDeadlineElapsed { .. }
        ));
        let state = cqrs.load(&id).await.unwrap().expect("aggregate exists");
        assert!(
            matches!(
                state,
                UsdcRebalance::BridgingFailed {
                    burn_tx_hash: Some(state_burn_tx),
                    ..
                } if state_burn_tx == burn_tx
            ),
            "Expected BridgingFailed with burn tx after deadline, got {state:?}"
        );
    }

    #[tokio::test]
    async fn resume_alpaca_to_base_from_awaiting_attestation_after_deadline_fails_bridging() {
        let server = MockServer::start();
        let (manager, cqrs, _anvil) = make_resume_test_manager(&server).await;
        let id = UsdcRebalanceId(Uuid::new_v4());
        let amount = usdc("100");
        let retry_deadline_at = Utc::now() - chrono::Duration::seconds(1);
        let burn_tx =
            advance_to_awaiting_attestation_alpaca_to_base(&cqrs, &id, amount, retry_deadline_at)
                .await;

        let error = manager
            .resume_alpaca_to_base(&id, amount)
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            UsdcTransferError::AttestationRetryDeadlineElapsed { .. }
        ));
        let state = cqrs.load(&id).await.unwrap().expect("aggregate exists");
        assert!(
            matches!(
                state,
                UsdcRebalance::BridgingFailed {
                    burn_tx_hash: Some(state_burn_tx),
                    ..
                } if state_burn_tx == burn_tx
            ),
            "Expected BridgingFailed with burn tx after deadline, got {state:?}"
        );
    }

    /// A CCTP message envelope valid for nonce extraction: >= MIN_MESSAGE_LENGTH
    /// (44 bytes) with a non-zero nonce in bytes 12..44 (byte 43 set), so
    /// `AttestationResponse::from_parts` reconstructs it without rejecting a
    /// placeholder nonce. Used as the persisted `message` in `ReceiveAttestation`
    /// fixtures so an `Attested` resume reconstructs rather than re-polling.
    fn valid_cctp_message() -> Vec<u8> {
        // A full CCTP envelope (>= MESSAGE_BODY_INDEX bytes): `from_parts` rejects
        // anything shorter as a truncated/corrupt envelope. Byte 43 (last byte of
        // the nonce at bytes 12..44) is set to 1 so the extracted nonce is 1.
        let mut message = vec![0u8; 200];
        message[43] = 1;
        message
    }

    /// Mocks Circle's `/v2/messages/` lookup to return a `complete` attestation
    /// immediately, so a re-poll from `AwaitingAttestation` resolves without the
    /// 5-minute real-API retry. The message is a full-length CCTP envelope (>=
    /// MESSAGE_BODY_INDEX) so it passes the shared `from_parts` validation the
    /// poll path now enforces, with byte 43 = 1 giving an extracted nonce of 1.
    fn mock_complete_attestation(server: &MockServer) -> httpmock::Mock<'_> {
        let mut message = vec![0u8; 200];
        message[43] = 1;
        let message_hex = format!("0x{}", alloy::hex::encode(&message));
        let attestation_hex = format!("0x{}", "ab".repeat(65));
        server.mock(|when, then| {
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
        })
    }

    /// Happy path of the feature: resuming from `AwaitingAttestation` while the
    /// retry deadline is still in the future and the attestation has now
    /// arrived must record `ReceiveAttestation` (advancing past
    /// `AwaitingAttestation`) and proceed to mint -- NOT time out again and NOT
    /// fail on the deadline. The mint then fails in the local test harness, but
    /// the recorded `cctp_nonce` surviving into the failed state is the proof
    /// the attestation was received first: a timeout would leave the nonce
    /// `None` (and the aggregate still in `AwaitingAttestation`).
    #[cfg(feature = "test-support")]
    #[tokio::test]
    async fn resume_base_to_alpaca_from_awaiting_attestation_before_deadline_records_attestation() {
        let server = MockServer::start();
        let _attestation_mock = mock_complete_attestation(&server);
        let (manager, cqrs, _anvil) =
            make_resume_test_manager_with_circle_api(&server, server.base_url()).await;
        let id = UsdcRebalanceId(Uuid::new_v4());
        let amount = usdc("100");
        let retry_deadline_at = Utc::now() + chrono::Duration::hours(1);
        let burn_tx =
            advance_to_awaiting_attestation_base_to_alpaca(&cqrs, &id, amount, retry_deadline_at)
                .await;

        let error = manager
            .resume_base_to_alpaca(&id, amount)
            .await
            .unwrap_err();

        assert!(
            matches!(error, UsdcTransferError::Cctp(_)),
            "attestation arriving before the deadline must record it and proceed to \
             mint, not time out or fail on the deadline, got: {error:?}"
        );
        let state = cqrs.load(&id).await.unwrap().expect("aggregate exists");
        assert!(
            matches!(
                &state,
                UsdcRebalance::BridgingFailed {
                    burn_tx_hash: Some(state_burn_tx),
                    cctp_nonce: Some(_),
                    ..
                } if *state_burn_tx == burn_tx
            ),
            "the recorded cctp_nonce must survive into the failed state, proving the \
             attestation was received (a timeout would leave it None), got {state:?}"
        );
    }

    /// Symmetric to the Base->Alpaca happy-path resume above, for the
    /// Alpaca->Base direction.
    #[cfg(feature = "test-support")]
    #[tokio::test]
    async fn resume_alpaca_to_base_from_awaiting_attestation_before_deadline_records_attestation() {
        let server = MockServer::start();
        let _attestation_mock = mock_complete_attestation(&server);
        let (manager, cqrs, _anvil) =
            make_resume_test_manager_with_circle_api(&server, server.base_url()).await;
        let id = UsdcRebalanceId(Uuid::new_v4());
        let amount = usdc("100");
        let retry_deadline_at = Utc::now() + chrono::Duration::hours(1);
        let burn_tx =
            advance_to_awaiting_attestation_alpaca_to_base(&cqrs, &id, amount, retry_deadline_at)
                .await;

        let error = manager
            .resume_alpaca_to_base(&id, amount)
            .await
            .unwrap_err();

        assert!(
            matches!(error, UsdcTransferError::Cctp(_)),
            "attestation arriving before the deadline must record it and proceed to \
             mint, not time out or fail on the deadline, got: {error:?}"
        );
        let state = cqrs.load(&id).await.unwrap().expect("aggregate exists");
        assert!(
            matches!(
                &state,
                UsdcRebalance::BridgingFailed {
                    burn_tx_hash: Some(state_burn_tx),
                    cctp_nonce: Some(_),
                    ..
                } if *state_burn_tx == burn_tx
            ),
            "the recorded cctp_nonce must survive into the failed state, proving the \
             attestation was received (a timeout would leave it None), got {state:?}"
        );
    }

    /// Mocks Circle's lookup to return a `status == "complete"` response that is
    /// missing the required `message` field -- a definitively-malformed response
    /// the bridge surfaces as `CctpError::MalformedAttestation` (RAI-837), not a
    /// retryable timeout.
    fn mock_malformed_complete_attestation(server: &MockServer) -> httpmock::Mock<'_> {
        let attestation_hex = format!("0x{}", "ab".repeat(65));
        server.mock(|when, then| {
            when.method(GET).path_includes("/v2/messages/");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "messages": [{
                        "status": "complete",
                        "attestation": attestation_hex,
                    }]
                }));
        })
    }

    /// RAI-837: a malformed `complete` attestation is a hard error, not a
    /// retryable timeout. Resuming from `AwaitingAttestation` against such a
    /// response must move the aggregate straight to `BridgingFailed` (immediate
    /// operator reconciliation) rather than leave it retryable in
    /// `AwaitingAttestation` until the 24h deadline.
    #[cfg(feature = "test-support")]
    #[tokio::test]
    async fn resume_from_awaiting_attestation_with_malformed_complete_fails_bridging() {
        let server = MockServer::start();
        let _attestation_mock = mock_malformed_complete_attestation(&server);
        let (manager, cqrs, _anvil) =
            make_resume_test_manager_with_circle_api(&server, server.base_url()).await;
        let id = UsdcRebalanceId(Uuid::new_v4());
        let amount = usdc("100");
        let retry_deadline_at = Utc::now() + chrono::Duration::hours(1);
        let burn_tx =
            advance_to_awaiting_attestation_base_to_alpaca(&cqrs, &id, amount, retry_deadline_at)
                .await;

        let error = manager
            .resume_base_to_alpaca(&id, amount)
            .await
            .unwrap_err();

        let UsdcTransferError::Cctp(inner) = &error else {
            panic!("expected UsdcTransferError::Cctp, got: {error:?}");
        };
        assert!(
            matches!(**inner, CctpError::MalformedAttestation { .. }),
            "a malformed complete attestation must surface as a terminal \
             MalformedAttestation, not a retryable AttestationTimeout, got: {error:?}"
        );
        let state = cqrs.load(&id).await.unwrap().expect("aggregate exists");
        assert!(
            matches!(
                &state,
                UsdcRebalance::BridgingFailed {
                    burn_tx_hash: Some(state_burn_tx),
                    ..
                } if *state_burn_tx == burn_tx
            ),
            "a malformed complete attestation must fail the bridge immediately, not leave it \
             retryable in AwaitingAttestation, got {state:?}"
        );
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
        cqrs.send(
            id,
            UsdcRebalanceCommand::ConfirmWithdrawal {
                withdrawal_tx: None,
            },
        )
        .await
        .unwrap();
        cqrs.send(id, UsdcRebalanceCommand::InitiateBridging { burn_tx })
            .await
            .unwrap();
        cqrs.send(
            id,
            UsdcRebalanceCommand::ReceiveAttestation {
                attestation: vec![0x01],
                cctp_nonce: B256::left_padding_from(&99_999u64.to_be_bytes()),
                message: valid_cctp_message(),
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
        correlation_id: &ClientOrderId,
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
                order_id: correlation_id.clone(),
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
        correlation_id: &ClientOrderId,
        status: &str,
        filled_quantity: &str,
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
                    "filled_avg_price": "1",
                    "filled_qty": filled_quantity,
                    "created_at": "2025-01-06T12:00:00Z"
                }));
        })
    }

    /// Mocks the by-id crypto-order GET that `poll_conversion_to_terminal` polls
    /// (the order id matches the one `mock_conversion_lookup` returns), so a
    /// resume that awaits a settling order can pick the terminal outcome it sees.
    fn mock_get_crypto_order<'server>(
        server: &'server MockServer,
        status: &str,
        filled_quantity: &str,
    ) -> httpmock::Mock<'server> {
        server.mock(|when, then| {
            when.method(GET).path(
                "/v1/trading/accounts/904837e3-3b76-47ec-b432-046db621571b/orders/904837e3-3b76-47ec-b432-046db621571b",
            );
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "id": "904837e3-3b76-47ec-b432-046db621571b",
                    "symbol": "USDCUSD",
                    "qty": "99.99",
                    "status": status,
                    "filled_avg_price": "1",
                    "filled_qty": filled_quantity,
                    "created_at": "2025-01-06T12:00:00Z"
                }));
        })
    }

    #[cfg(feature = "test-support")]
    #[tokio::test]
    async fn resume_base_to_alpaca_from_attested_reconstructs_without_repolling_circle() {
        let server = MockServer::start();

        // A `complete` attestation is mocked, but the resume must NOT call it:
        // the persisted message envelope lets it reconstruct the attestation
        // offline. The mock exists only to prove (via 0 hits) that no re-poll
        // happens.
        let attestation_mock = mock_complete_attestation(&server);

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
        // The fix routes Attested through continue_from_attested (reconstruct ->
        // mint, no ReceiveAttestation), so resume gets PAST the Attested gate and
        // instead fails at the mint on the undeployed contract -> Cctp.
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
        assert_eq!(
            attestation_mock.calls(),
            0,
            "resume from Attested must reconstruct the attestation from the persisted \
             message envelope, not re-poll Circle",
        );
    }

    fn valid_message_nonce() -> B256 {
        // `valid_cctp_message()` sets byte 43 = 1, so the nonce extracted from it
        // is 1. The staging helpers record this as `cctp_nonce`.
        B256::left_padding_from(&1u64.to_be_bytes())
    }

    #[cfg(feature = "test-support")]
    #[tokio::test]
    async fn attested_attestation_response_without_message_repolls_circle() {
        let server = MockServer::start();
        let attestation_mock = mock_complete_attestation(&server);

        let (manager, cqrs, _anvil) =
            make_resume_test_manager_with_circle_api(&server, server.base_url()).await;
        let id = UsdcRebalanceId(Uuid::new_v4());
        let amount = usdc("100");
        let burn_tx = advance_to_attested_base_to_alpaca(&cqrs, &id, amount).await;

        // A transfer whose `BridgeAttestationReceived` predates envelope
        // persistence carries `message: None`, so the response must be re-fetched
        // from Circle (the backward-compatible fallback).
        let response = manager
            .attested_attestation_response(
                &id,
                BridgeDirection::BaseToEthereum,
                burn_tx,
                vec![0x01],
                valid_message_nonce(),
                None,
            )
            .await
            .unwrap();

        assert_eq!(response.nonce(), valid_message_nonce());
        assert_eq!(
            attestation_mock.calls(),
            1,
            "a legacy None-envelope resume must re-poll Circle exactly once",
        );
    }

    #[cfg(feature = "test-support")]
    #[tokio::test]
    async fn attested_attestation_response_with_corrupt_envelope_fails_bridging() {
        let server = MockServer::start();
        let attestation_mock = mock_complete_attestation(&server);

        let (manager, cqrs, _anvil) =
            make_resume_test_manager_with_circle_api(&server, server.base_url()).await;
        let id = UsdcRebalanceId(Uuid::new_v4());
        let amount = usdc("100");
        let burn_tx = advance_to_attested_base_to_alpaca(&cqrs, &id, amount).await;

        // A persisted envelope too short to extract a nonce is unusable. The USDC
        // is already burned, so the aggregate must move to `BridgingFailed` for
        // operator reconciliation rather than wedge in `Attested`.
        let error = manager
            .attested_attestation_response(
                &id,
                BridgeDirection::BaseToEthereum,
                burn_tx,
                vec![0x01],
                valid_message_nonce(),
                Some(vec![0u8; 10]),
            )
            .await
            .unwrap_err();

        assert!(
            matches!(error, UsdcTransferError::Cctp(_)),
            "a corrupt envelope must surface a Cctp error, got: {error:?}",
        );

        let state = cqrs.load(&id).await.unwrap().expect("aggregate exists");
        assert!(
            matches!(state, UsdcRebalance::BridgingFailed { .. }),
            "a reconstruction failure must fail the bridge, got: {state:?}",
        );
        assert_eq!(
            attestation_mock.calls(),
            0,
            "a reconstruction failure must not re-poll Circle",
        );
    }

    #[cfg(feature = "test-support")]
    #[tokio::test]
    async fn attested_attestation_response_with_mismatched_nonce_fails_bridging() {
        let server = MockServer::start();
        let attestation_mock = mock_complete_attestation(&server);

        let (manager, cqrs, _anvil) =
            make_resume_test_manager_with_circle_api(&server, server.base_url()).await;
        let id = UsdcRebalanceId(Uuid::new_v4());
        let amount = usdc("100");
        let burn_tx = advance_to_attested_base_to_alpaca(&cqrs, &id, amount).await;

        // `valid_cctp_message()` embeds nonce 1; recording a different cctp_nonce
        // makes the persisted record internally inconsistent. Minting against an
        // unverifiable nonce is refused -> `BridgingFailed`.
        let recorded = B256::left_padding_from(&999u64.to_be_bytes());
        let error = manager
            .attested_attestation_response(
                &id,
                BridgeDirection::BaseToEthereum,
                burn_tx,
                vec![0x01],
                recorded,
                Some(valid_cctp_message()),
            )
            .await
            .unwrap_err();

        assert!(
            matches!(
                error,
                UsdcTransferError::AttestationNonceMismatch { recorded: error_recorded, reconstructed, .. }
                    if error_recorded == recorded && reconstructed == valid_message_nonce()
            ),
            "a nonce mismatch must surface AttestationNonceMismatch, got: {error:?}",
        );

        let state = cqrs.load(&id).await.unwrap().expect("aggregate exists");
        assert!(
            matches!(state, UsdcRebalance::BridgingFailed { .. }),
            "a nonce mismatch must fail the bridge, got: {state:?}",
        );
        assert_eq!(
            attestation_mock.calls(),
            0,
            "a nonce mismatch must not re-poll Circle",
        );
    }

    #[tokio::test]
    async fn resume_base_to_alpaca_from_pre_burn_failure_returns_previously_failed_error() {
        let server = MockServer::start();
        let (manager, cqrs, _anvil) = make_resume_test_manager(&server).await;

        let id = UsdcRebalanceId(Uuid::new_v4());
        let amount = usdc("100");

        let withdraw_tx =
            fixed_bytes!("0xcccc000000000000000000000000000000000000000000000000000000000002");
        cqrs.send(
            &id,
            UsdcRebalanceCommand::Initiate {
                direction: RebalanceDirection::BaseToAlpaca,
                amount,
                withdrawal: TransferRef::OnchainTx(withdraw_tx),
            },
        )
        .await
        .unwrap();
        cqrs.send(
            &id,
            UsdcRebalanceCommand::ConfirmWithdrawal {
                withdrawal_tx: None,
            },
        )
        .await
        .unwrap();
        // Fail BEFORE the burn (no `InitiateBridging`), so the resulting
        // `BridgingFailed` carries `burn_tx_hash: None`. A pre-burn failure has
        // no mint to adopt, so resume must still reject it as terminal -- it is
        // NOT recoverable. (A post-burn `BridgingFailed` IS now recovered, see
        // `resume_base_to_alpaca_from_bridging_failed_recovers_to_terminal`.)
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
                order_id: ClientOrderId::from_uuid(Uuid::new_v4()),
                amount: amount_received,
            },
        )
        .await
        .unwrap();
        cqrs.send(
            &id,
            UsdcRebalanceCommand::ConfirmConversion {
                conversion: par_conversion(amount_received),
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
        let correlation_id =
            ClientOrderId::from_uuid(uuid!("33333333-3333-4333-8333-333333333333"));

        advance_to_converting_base_to_alpaca(&cqrs, &id, amount, amount_received, &correlation_id)
            .await;

        // The crash happened after the conversion order was placed at Alpaca but
        // before `ConfirmConversion` was emitted. On resume the order is found
        // filled by its client_order_id and the conversion is confirmed without
        // placing a second order (no POST /orders mock is configured, so a
        // re-placement would 501 and fail the test loud).
        let lookup_mock = mock_conversion_lookup(&server, &correlation_id, "filled", "99.99");

        manager.resume_base_to_alpaca(&id, amount).await.unwrap();

        lookup_mock.assert();
        let state = cqrs.load(&id).await.unwrap();
        assert!(
            matches!(
                &state,
                Some(UsdcRebalance::ConversionComplete { conversion, .. })
                    if conversion.received_amount == usdc("99.99")
            ),
            "expected ConversionComplete with received_amount 99.99, got {state:?}"
        );
    }

    #[tokio::test]
    async fn resume_base_to_alpaca_from_converting_errors_when_filled_order_lacks_filled_qty() {
        let server = MockServer::start();
        let (manager, cqrs, _anvil) = make_resume_test_manager(&server).await;

        let id = UsdcRebalanceId(Uuid::new_v4());
        let amount = usdc("100");
        let amount_received = usdc("99.99");
        let correlation_id =
            ClientOrderId::from_uuid(uuid!("66666666-6666-4666-8666-666666666666"));

        advance_to_converting_base_to_alpaca(&cqrs, &id, amount, amount_received, &correlation_id)
            .await;

        // A `filled` order whose `filled_qty` is absent must surface
        // MissingFilledQuantity rather than confirming an unknown amount: the
        // converted USDC is genuinely unknown and needs operator reconciliation,
        // not a silently-fabricated confirmation.
        let lookup_mock = server.mock(|when, then| {
            when.method(GET)
                .path("/v1/trading/accounts/904837e3-3b76-47ec-b432-046db621571b/orders:by_client_order_id")
                .query_param("client_order_id", correlation_id.to_string());
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "id": "904837e3-3b76-47ec-b432-046db621571b",
                    "symbol": "USDCUSD",
                    "qty": "99.99",
                    "status": "filled",
                    "filled_avg_price": "1.0001",
                    "created_at": "2025-01-06T12:00:00Z"
                }));
        });

        let error = manager
            .resume_base_to_alpaca(&id, amount)
            .await
            .unwrap_err();

        lookup_mock.assert();
        assert!(
            matches!(error, UsdcTransferError::MissingFilledQuantity { ref order_id } if *order_id == correlation_id),
            "expected MissingFilledQuantity for {correlation_id}, got {error:?}"
        );
        let state = cqrs.load(&id).await.unwrap();
        assert!(
            matches!(state, Some(UsdcRebalance::Converting { .. })),
            "aggregate must remain Converting when the fill amount is unknown, got {state:?}"
        );
    }

    #[tokio::test]
    async fn resume_base_to_alpaca_from_converting_awaits_settling_order_then_confirms() {
        let server = MockServer::start();
        let (manager, cqrs, _anvil) = make_resume_test_manager(&server).await;

        let id = UsdcRebalanceId(Uuid::new_v4());
        let amount = usdc("100");
        let amount_received = usdc("99.99");
        let correlation_id =
            ClientOrderId::from_uuid(uuid!("44444444-4444-4444-8444-444444444444"));

        advance_to_converting_base_to_alpaca(&cqrs, &id, amount, amount_received, &correlation_id)
            .await;

        // The order is still settling when first looked up. Resume must await it to
        // a terminal state (mirroring the normal placement flow) rather than failing
        // the job; the poll then observes the fill and the conversion is confirmed.
        let lookup_mock = mock_conversion_lookup(&server, &correlation_id, "new", "0");
        let poll_mock = mock_get_crypto_order(&server, "filled", "99.99");

        manager.resume_base_to_alpaca(&id, amount).await.unwrap();

        lookup_mock.assert();
        poll_mock.assert();
        let state = cqrs.load(&id).await.unwrap();
        assert!(
            matches!(
                &state,
                Some(UsdcRebalance::ConversionComplete { conversion, .. })
                    if conversion.received_amount == usdc("99.99")
            ),
            "expected ConversionComplete with received_amount 99.99 after awaiting settle, got {state:?}"
        );
    }

    #[tokio::test]
    async fn resume_base_to_alpaca_from_converting_awaits_settling_order_then_fails() {
        let server = MockServer::start();
        let (manager, cqrs, _anvil) = make_resume_test_manager(&server).await;

        let id = UsdcRebalanceId(Uuid::new_v4());
        let amount = usdc("100");
        let amount_received = usdc("99.99");
        let correlation_id =
            ClientOrderId::from_uuid(uuid!("44444444-4444-4444-8444-444444444444"));

        advance_to_converting_base_to_alpaca(&cqrs, &id, amount, amount_received, &correlation_id)
            .await;

        // Still settling at lookup, then the poll observes a terminal failure: the
        // conversion fails for operator follow-up instead of confirming.
        let lookup_mock = mock_conversion_lookup(&server, &correlation_id, "new", "0");
        let poll_mock = mock_get_crypto_order(&server, "canceled", "0");

        let error = manager
            .resume_base_to_alpaca(&id, amount)
            .await
            .unwrap_err();

        lookup_mock.assert();
        poll_mock.assert();
        assert!(
            matches!(error, UsdcTransferError::ResumeIndeterminateConversion { id: ref erred } if *erred == id),
            "expected ResumeIndeterminateConversion for {id}, got {error:?}"
        );
        let state = cqrs.load(&id).await.unwrap();
        assert!(
            matches!(state, Some(UsdcRebalance::ConversionFailed { .. })),
            "aggregate must be ConversionFailed after the awaited order terminates failed, got {state:?}"
        );
    }

    #[tokio::test]
    async fn resume_base_to_alpaca_from_converting_fails_on_terminally_failed_order() {
        let server = MockServer::start();
        let (manager, cqrs, _anvil) = make_resume_test_manager(&server).await;

        let id = UsdcRebalanceId(Uuid::new_v4());
        let amount = usdc("100");
        let amount_received = usdc("99.99");
        let correlation_id =
            ClientOrderId::from_uuid(uuid!("55555555-5555-4555-8555-555555555555"));

        advance_to_converting_base_to_alpaca(&cqrs, &id, amount, amount_received, &correlation_id)
            .await;

        // A rejected order is terminal: resume fails the conversion in the
        // aggregate and returns the indeterminate error for operator follow-up.
        let lookup_mock = mock_conversion_lookup(&server, &correlation_id, "rejected", "0");

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
    async fn resume_base_to_alpaca_from_converting_surfaces_partial_fill_in_failure_reason() {
        let server = MockServer::start();
        let (manager, cqrs, _anvil) = make_resume_test_manager(&server).await;

        let id = UsdcRebalanceId(Uuid::new_v4());
        let amount = usdc("100");
        let amount_received = usdc("99.99");
        let correlation_id =
            ClientOrderId::from_uuid(uuid!("77777777-7777-4777-8777-777777777777"));

        advance_to_converting_base_to_alpaca(&cqrs, &id, amount, amount_received, &correlation_id)
            .await;

        // A canceled order that partially filled: real USDC converted before the
        // order terminated. The conversion still fails for operator follow-up, but
        // the recorded reason must surface the partial fill so the converted USDC is
        // not silently lost.
        let lookup_mock = mock_conversion_lookup(&server, &correlation_id, "canceled", "42.5");

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
        let Some(UsdcRebalance::ConversionFailed { reason, .. }) = state else {
            panic!(
                "aggregate must be ConversionFailed after a partial-then-canceled order, got {state:?}"
            );
        };
        assert!(
            reason.contains("reconciliation"),
            "failure reason must surface the partial fill for reconciliation, got: {reason}"
        );
    }

    #[tokio::test]
    async fn resume_base_to_alpaca_from_converting_fails_when_order_never_reached_alpaca() {
        let server = MockServer::start();
        let (manager, cqrs, _anvil) = make_resume_test_manager(&server).await;

        let id = UsdcRebalanceId(Uuid::new_v4());
        let amount = usdc("100");
        let amount_received = usdc("99.99");
        let correlation_id =
            ClientOrderId::from_uuid(uuid!("66666666-6666-4666-8666-666666666666"));

        advance_to_converting_base_to_alpaca(&cqrs, &id, amount, amount_received, &correlation_id)
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

    /// Resume from `Bridged` must NOT re-burn or re-mint: the bridge step is
    /// already done. With no CCTP contracts deployed, a re-burn/re-mint attempt
    /// would hit un-deployed contracts and fail loud. Driving to
    /// `ConversionComplete` via the deposit-send (adopting an already-submitted
    /// transfer) proves the resume touches only the deposit + conversion legs.
    #[tokio::test]
    async fn resume_base_to_alpaca_from_bridged_skips_burn_and_mint() {
        let chain = deploy_ethereum_usdc_chain().await;
        let server = MockServer::start();
        let _address_mock = mock_alpaca_deposit_address(&server);

        let alpaca_wallet = Arc::new(create_test_wallet_service(&server));
        let cqrs = create_test_store_instance().await;
        let manager = build_deposit_manager(&chain, &server, alpaca_wallet, cqrs.clone()).await;

        let id = UsdcRebalanceId(Uuid::new_v4());
        let amount = usdc("100");
        let amount_received = usdc("99.99");
        let amount_u256 = usdc_to_u256(amount_received).unwrap();
        stage_bridged_with_mint_tx(&cqrs, &id, amount, amount_received, chain.mint_tx).await;

        // Pre-submit the deposit transfer so resume adopts it (no second send),
        // then drives the deposit + conversion to terminal. No CCTP mocks: a
        // re-burn/re-mint would hit the un-deployed CCTP contracts and fail loud.
        let bridge_wallet = create_test_wallet(&chain.endpoint, &chain.bot_key);
        let existing_send = bridge_wallet
            .submit::<NoOpErrorRegistry, _>(
                USDC_ADDRESS,
                IERC20::transferCall {
                    to: ALPACA_DEPOSIT_ADDRESS,
                    amount: amount_u256,
                },
                "pre-existing deposit send",
            )
            .await
            .unwrap()
            .transaction_hash;

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
                    "from_address": format!("{:#x}", chain.bot_address),
                    "to_address": format!("{ALPACA_DEPOSIT_ADDRESS:#x}"),
                    "status": "COMPLETE",
                    "tx_hash": format!("{existing_send:#x}"),
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
                order_id: ClientOrderId::from_uuid(Uuid::new_v4()),
            },
        )
        .await
        .unwrap();
        cqrs.send(
            &id,
            UsdcRebalanceCommand::ConfirmConversion {
                conversion: par_conversion(amount),
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
        _base_anvil: TestAnvilInstance,
        _ethereum_anvil: TestAnvilInstance,
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
        let (base_anvil, ethereum_anvil) =
            spawn_anvil_pair(Anvil::new(), Anvil::new().chain_id(1u64));
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

        // Mint to the bot wallet (the Ethereum signer): in production
        // `market_maker_wallet` IS the bot wallet, and the BaseToAlpaca deposit
        // leg sends the minted USDC from it to Alpaca's deposit address.
        let market_maker_wallet = chains.bot_address;
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
        let alpaca_broker = InstrumentedAlpacaBroker::new(
            create_test_broker_service(&server).await,
            TelemetrySender::disabled(),
        );
        let alpaca_wallet = Arc::new(create_test_wallet_service(&server));
        let vault_service = RaindexService::new(
            create_test_wallet(&chains.base_endpoint, &chains.bot_key),
            RaindexContracts {
                inventory: ORDERBOOK_ADDRESS,
                orderbook: ORDERBOOK_ADDRESS,
            },
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
            &test_settlement_params(),
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
        cqrs.send(
            &id,
            UsdcRebalanceCommand::ConfirmWithdrawal {
                withdrawal_tx: None,
            },
        )
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
                message: attestation_response.message_bytes().to_vec(),
                mint_scan_from_block,
            },
        )
        .await
        .unwrap();

        // After mint adoption, the deposit leg fetches Alpaca's address and sends
        // the minted USDC there. Pre-submit that transfer so the deposit scan
        // adopts it (the crash-before-record window) and the transfers poll can
        // match its known tx hash.
        let _address_mock = mock_alpaca_deposit_address(&server);
        let deposit_send = cctp_bridge
            .send_usdc_on_ethereum(ALPACA_DEPOSIT_ADDRESS, mint_receipt.amount)
            .await
            .unwrap();

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
                    "from_address": format!("{:#x}", chains.bot_address),
                    "to_address": format!("{ALPACA_DEPOSIT_ADDRESS:#x}"),
                    "status": "COMPLETE",
                    "tx_hash": format!("{deposit_send:#x}"),
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

    /// The un-fail core: a post-burn `BridgingFailed` whose mint actually landed
    /// is un-failed and driven to terminal on resume. We mint for real, then fail
    /// the aggregate post-burn, then resume. `recover_from_bridging_failed`
    /// re-polls, mints idempotently (the consumed nonce returns the existing
    /// receipt rather than reverting), emits `RecoverBridging`, and finishes the
    /// deposit leg. Reaching `ConversionComplete` proves the un-fail + adoption +
    /// terminal drive all happened -- a re-mint would revert on the used nonce and
    /// re-latch `BridgingFailed`.
    #[tokio::test]
    async fn resume_base_to_alpaca_from_bridging_failed_recovers_to_terminal() {
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

        // Produce a real on-chain mint so the nonce is already consumed before
        // recovery runs -- recovery must adopt it, not re-mint. Mint to the bot
        // wallet (the Ethereum signer): in production `market_maker_wallet` IS the
        // bot wallet, and the deposit leg sends the minted USDC from it to Alpaca.
        let market_maker_wallet = chains.bot_address;
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
        let mint_receipt = cctp_bridge
            .mint(BridgeDirection::BaseToEthereum, &attestation_response)
            .await
            .unwrap();

        let server = MockServer::start();
        let alpaca_broker = InstrumentedAlpacaBroker::new(
            create_test_broker_service(&server).await,
            TelemetrySender::disabled(),
        );
        let alpaca_wallet = Arc::new(create_test_wallet_service(&server));
        let vault_service = RaindexService::new(
            create_test_wallet(&chains.base_endpoint, &chains.bot_key),
            RaindexContracts {
                inventory: ORDERBOOK_ADDRESS,
                orderbook: ORDERBOOK_ADDRESS,
            },
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
            &test_settlement_params(),
        );

        // Drive to a POST-burn BridgingFailed: InitiateBridging records the burn
        // tx, then FailBridging preserves it (a transient receipt error failing
        // the bridge while the mint had in fact landed -- the incident this
        // recovery path exists to repair).
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
        cqrs.send(
            &id,
            UsdcRebalanceCommand::ConfirmWithdrawal {
                withdrawal_tx: None,
            },
        )
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
            UsdcRebalanceCommand::FailBridging {
                reason: "transient receipt error while the mint actually landed".into(),
            },
        )
        .await
        .unwrap();

        // Sanity: the aggregate is terminally failed before recovery.
        let failed_state = cqrs.load(&id).await.unwrap().expect("aggregate exists");
        assert!(
            matches!(failed_state, UsdcRebalance::BridgingFailed { .. }),
            "expected BridgingFailed before recovery, got: {failed_state:?}"
        );

        // After the un-fail, the deposit leg fetches Alpaca's address and sends
        // the minted USDC there. Pre-submit that transfer so the deposit scan
        // adopts it and the transfers poll can match its known tx hash.
        let _address_mock = mock_alpaca_deposit_address(&server);
        let deposit_send = cctp_bridge
            .send_usdc_on_ethereum(ALPACA_DEPOSIT_ADDRESS, mint_receipt.amount)
            .await
            .unwrap();

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
                    "from_address": format!("{:#x}", chains.bot_address),
                    "to_address": format!("{ALPACA_DEPOSIT_ADDRESS:#x}"),
                    "status": "COMPLETE",
                    "tx_hash": format!("{deposit_send:#x}"),
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

        // Resume the terminally-failed transfer: it must recover, not stay failed.
        manager.resume_base_to_alpaca(&id, amount).await.unwrap();

        let final_state = cqrs.load(&id).await.unwrap().expect("aggregate exists");
        assert!(
            matches!(final_state, UsdcRebalance::ConversionComplete { .. }),
            "Expected ConversionComplete after recovering a post-burn BridgingFailed, \
             got: {final_state:?}"
        );
    }

    /// A persisted envelope must actually mint, not merely reconstruct. Rebuild
    /// the `AttestationResponse` from the stored message + attestation bytes
    /// (exactly as an `Attested` resume does via `from_parts`) and mint it
    /// on-chain over dual-chain CCTP with no Circle re-poll. A successful mint
    /// receipt for the burned amount proves the stored envelope alone is
    /// sufficient to call `receiveMessage` -- guarding against persisting an
    /// envelope shape that round-trips but cannot mint.
    #[tokio::test]
    async fn persisted_envelope_reconstructs_into_a_mintable_response() {
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

        let cctp_bridge = CctpBridge::try_from_ctx(CctpCtx {
            usdc_ethereum: USDC_ADDRESS,
            usdc_base: USDC_ADDRESS,
            ethereum_wallet: create_test_wallet(&chains.ethereum_endpoint, &chains.bot_key),
            base_wallet: create_test_wallet(&chains.base_endpoint, &chains.bot_key),
            circle_api_base: attestation.base_url(),
            token_messenger: chains.token_messenger,
            message_transmitter: chains.message_transmitter,
        })
        .unwrap();

        let market_maker_wallet = address!("0x1111111111111111111111111111111111111111");
        let amount_u256 = usdc_to_u256(usdc("100")).unwrap();
        let burn_receipt = cctp_bridge
            .burn(
                BridgeDirection::BaseToEthereum,
                amount_u256,
                market_maker_wallet,
            )
            .await
            .unwrap();
        let response = cctp_bridge
            .poll_attestation(BridgeDirection::BaseToEthereum, burn_receipt.tx)
            .await
            .unwrap();

        // Persist then reconstruct exactly as the `Attested` resume path does:
        // store only the envelope + attestation bytes, then rebuild with no
        // Circle call.
        let reconstructed = cctp_bridge
            .reconstruct_attestation(
                response.message_bytes().to_vec(),
                response.as_bytes().to_vec(),
            )
            .unwrap();

        let mint_receipt = cctp_bridge
            .mint(BridgeDirection::BaseToEthereum, &reconstructed)
            .await
            .unwrap();

        assert_eq!(
            mint_receipt.amount, amount_u256,
            "the reconstructed envelope must mint the full burned amount",
        );
    }

    /// Resume from `Bridged` MUST move funds: the CCTP mint credits the bot's own
    /// wallet, not Alpaca, so the deposit leg fetches Alpaca's deposit address and
    /// SENDS the minted USDC there. This is the root-cause fix -- the old poll-only
    /// leg never sent, so the deposit dead-ended. Asserts the address lookup ran
    /// and the on-chain USDC moved bot -> Alpaca address by the deposit amount.
    #[tokio::test]
    async fn resume_base_to_alpaca_from_bridged_sends_fund_moving_deposit() {
        let chain = deploy_ethereum_usdc_chain().await;
        let server = MockServer::start();
        let address_mock = mock_alpaca_deposit_address(&server);

        let alpaca_wallet = Arc::new(create_short_poll_wallet_service(&server));
        let cqrs = create_test_store_instance().await;
        let manager = build_deposit_manager(&chain, &server, alpaca_wallet, cqrs.clone()).await;

        let id = UsdcRebalanceId(Uuid::new_v4());
        let amount = usdc("100");
        let amount_received = usdc("99.99");
        stage_bridged_with_mint_tx(&cqrs, &id, amount, amount_received, chain.mint_tx).await;

        let alpaca_before = usdc_balance_of(&chain, ALPACA_DEPOSIT_ADDRESS).await;
        assert_eq!(alpaca_before, U256::ZERO, "Alpaca address starts unfunded");

        // No deposit transfer is mocked, so the leg sends, then fails at the poll
        // (short timeout). The send is what we assert on.
        let error = manager
            .resume_base_to_alpaca(&id, amount)
            .await
            .unwrap_err();
        assert!(
            matches!(error, UsdcTransferError::AlpacaWallet(_)),
            "with no deposit detected the leg fails at the Alpaca poll, got: {error:?}",
        );

        address_mock.assert();
        let amount_u256 = usdc_to_u256(amount_received).unwrap();
        assert_eq!(
            usdc_balance_of(&chain, ALPACA_DEPOSIT_ADDRESS).await,
            amount_u256,
            "resume from Bridged MUST send the minted USDC to Alpaca's deposit address",
        );

        // The recorded deposit_ref is the SEND tx (not the mint tx), and that tx
        // transfers USDC to the Alpaca deposit address.
        let state = cqrs.load(&id).await.unwrap().expect("aggregate exists");
        let recorded = deposit_ref_tx(&state);
        assert_ne!(
            recorded, chain.mint_tx,
            "the recorded deposit_ref must be the SEND tx, not the mint tx",
        );
        assert_eq!(
            usdc_transfer_recipient(&chain, recorded).await,
            ALPACA_DEPOSIT_ADDRESS,
            "the recorded send tx must transfer USDC to the Alpaca deposit address",
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

        let alpaca_broker = InstrumentedAlpacaBroker::new(
            create_test_broker_service(&server).await,
            TelemetrySender::disabled(),
        );
        let alpaca_wallet = Arc::new(create_test_wallet_service(&server));
        let wallet = create_test_wallet(&endpoint, &private_key);
        let (cctp_bridge, vault_service) = create_test_onchain_services(wallet);
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
            &test_settlement_params(),
        );

        let id = UsdcRebalanceId(Uuid::new_v4());
        let amount = usdc("100");
        // A persisted deposit tx that never landed on chain (reorged out,
        // dropped, or never mined): the live anvil node has no record of it, so
        // `confirm_tx` reports it dropped. Rather than retrying confirm forever
        // (a transient-vs-dropped confusion), resume must re-verify the tx and,
        // finding it gone, record a terminal `FailDeposit` for operator
        // reconciliation -- never blindly transitioning the aggregate to complete.
        let deposit_tx =
            fixed_bytes!("0xdddd000000000000000000000000000000000000000000000000000000000001");

        advance_to_deposit_initiated_alpaca_to_base(&cqrs, &id, amount, deposit_tx).await;

        let error = manager
            .resume_alpaca_to_base(&id, amount)
            .await
            .unwrap_err();

        assert!(
            matches!(error, UsdcTransferError::Vault(_)),
            "Resume must surface the unconfirmable deposit as a Vault (confirm_tx) error, \
             got: {error:?}"
        );

        let final_state = cqrs.load(&id).await.unwrap().expect("aggregate exists");
        assert!(
            matches!(final_state, UsdcRebalance::DepositFailed { .. }),
            "A dropped deposit tx (will never mine) must transition to `DepositFailed` for \
             operator reconciliation rather than retrying forever; got: {final_state:?}"
        );
    }

    #[tokio::test]
    async fn resume_alpaca_to_base_from_deposit_confirmed_is_noop() {
        let server = MockServer::start();
        let (_anvil, endpoint, private_key) = setup_anvil();

        let alpaca_broker = InstrumentedAlpacaBroker::new(
            create_test_broker_service(&server).await,
            TelemetrySender::disabled(),
        );
        let alpaca_wallet = Arc::new(create_test_wallet_service(&server));
        let wallet = create_test_wallet(&endpoint, &private_key);
        let (cctp_bridge, vault_service) = create_test_onchain_services(wallet);
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
            &test_settlement_params(),
        );

        let id = UsdcRebalanceId(Uuid::new_v4());
        let amount = usdc("100");
        let deposit_tx =
            fixed_bytes!("0xdddd000000000000000000000000000000000000000000000000000000000002");

        advance_to_deposit_confirmed_alpaca_to_base(&cqrs, &id, amount, deposit_tx).await;

        // Resuming a transfer that already reached the terminal DepositConfirmed
        // state must be a pure no-op: re-running any side effect (re-deposit,
        // re-mint) would double-spend. It returns Ok and leaves the aggregate
        // exactly where it was.
        manager.resume_alpaca_to_base(&id, amount).await.unwrap();

        let final_state = cqrs.load(&id).await.unwrap().expect("aggregate exists");
        assert!(
            matches!(final_state, UsdcRebalance::DepositConfirmed { .. }),
            "resume from terminal DepositConfirmed must remain DepositConfirmed (no-op), \
             got: {final_state:?}"
        );
    }

    #[tokio::test]
    async fn resume_alpaca_to_base_from_attested_reconstructs_without_repolling_circle() {
        let server = MockServer::start();

        // A `complete` attestation is mocked, but the resume must NOT call it:
        // the persisted message envelope lets it reconstruct the attestation
        // offline. The mock exists only to prove (via 0 hits) that no re-poll
        // happens. (Same shape as the Base->Alpaca sibling test.)
        let attestation_mock = mock_complete_attestation(&server);

        let (manager, cqrs, _anvil) =
            make_resume_test_manager_with_circle_api(&server, server.base_url()).await;
        let id = UsdcRebalanceId(Uuid::new_v4());
        let amount = usdc("100");
        advance_to_attested_alpaca_to_base(&cqrs, &id, amount).await;

        let staged = cqrs.load(&id).await.unwrap().expect("aggregate exists");
        assert!(
            matches!(staged, UsdcRebalance::Attested { .. }),
            "staging must land at Attested, got {staged:?}",
        );

        let error = manager
            .resume_alpaca_to_base(&id, amount)
            .await
            .unwrap_err();

        // The OLD (buggy) behavior routed Attested through
        // continue_alpaca_to_base_from_bridging -> poll_attestation, re-sending
        // ReceiveAttestation, which the aggregate rejects from Attested ->
        // UsdcTransferError::Aggregate(InvalidCommand). The fix routes Attested
        // through continue_alpaca_to_base_from_attested (no ReceiveAttestation),
        // so resume gets PAST the Attested gate and fails later at the CCTP
        // find/mint on the undeployed contract -> Cctp.
        assert!(
            !matches!(&error, UsdcTransferError::Aggregate(_)),
            "resume from Attested must NOT re-emit ReceiveAttestation (the aggregate \
             rejects it from Attested); got: {error:?}",
        );
        assert!(
            matches!(error, UsdcTransferError::Cctp(_)),
            "resume from Attested should proceed past the gate to the CCTP mint and \
             fail with a Cctp error; got: {error:?}",
        );
        assert_eq!(
            attestation_mock.calls(),
            0,
            "resume from Attested must reconstruct the attestation from the persisted \
             message envelope, not re-poll Circle",
        );
    }

    #[tokio::test]
    async fn resume_alpaca_to_base_from_bridged_deposit_submission_failure_stays_bridged() {
        let server = MockServer::start();
        // No vault is funded on this bare anvil, so the deposit4 submission
        // fails -- exercising `deposit_to_vault`'s submission-failure path.
        let (manager, cqrs, _anvil) = make_resume_test_manager(&server).await;
        let id = UsdcRebalanceId(Uuid::new_v4());
        let amount = usdc("100");
        advance_to_bridged_alpaca_to_base(&cqrs, &id, amount).await;

        let staged = cqrs.load(&id).await.unwrap().expect("aggregate exists");
        assert!(
            matches!(staged, UsdcRebalance::Bridged { .. }),
            "staging must land at Bridged, got {staged:?}",
        );

        let error = manager
            .resume_alpaca_to_base(&id, amount)
            .await
            .unwrap_err();

        // A deposit submission failure surfaces the real Vault error. The old
        // code instead emitted `FailDeposit` from `Bridged` (invalid -> the
        // aggregate rejects it with `DepositNotInitiated`), masking the cause;
        // the fix propagates the Vault error directly.
        assert!(
            matches!(error, UsdcTransferError::Vault(_)),
            "a failed deposit submission must surface as a Vault error, got: {error:?}",
        );

        // Crucially, nothing was deposited, so the aggregate stays in `Bridged`
        // for a safe retry -- it must NOT advance to `DepositInitiated` (no tx to
        // confirm) or strand in a terminal failure for a transient error.
        let final_state = cqrs.load(&id).await.unwrap().expect("aggregate exists");
        assert!(
            matches!(final_state, UsdcRebalance::Bridged { .. }),
            "a failed deposit submission must leave the aggregate in `Bridged` for safe \
             retry (no half-recorded deposit), got: {final_state:?}",
        );
    }

    /// A destination-head lookup failure inside `record_attestation` is a
    /// transient RPC error: the attestation has not been recorded yet, so the
    /// aggregate is still in `Bridging` and resumable. It must NOT latch a
    /// terminal `BridgingFailed` -- doing so would strand already-burned USDC
    /// behind manual reconciliation for a momentary destination RPC hiccup.
    #[cfg(feature = "test-support")]
    #[tokio::test]
    async fn resume_alpaca_to_base_destination_block_failure_stays_bridging() {
        let server = MockServer::start();
        let _attestation_mock = mock_complete_attestation(&server);
        let (manager, cqrs, anvil) =
            make_resume_test_manager_with_circle_api(&server, server.base_url()).await;
        let id = UsdcRebalanceId(Uuid::new_v4());
        let amount = usdc("100");

        // Drive to `Bridging` (burn recorded, attestation not yet received) so
        // the resume re-polls the attestation and then performs the destination
        // head lookup.
        let burn_tx =
            fixed_bytes!("0xaaaa000000000000000000000000000000000000000000000000000000000001");
        cqrs.send(
            &id,
            UsdcRebalanceCommand::InitiateConversion {
                direction: RebalanceDirection::AlpacaToBase,
                amount,
                order_id: ClientOrderId::from_uuid(Uuid::new_v4()),
            },
        )
        .await
        .unwrap();
        cqrs.send(
            &id,
            UsdcRebalanceCommand::ConfirmConversion {
                conversion: par_conversion(amount),
            },
        )
        .await
        .unwrap();
        cqrs.send(
            &id,
            UsdcRebalanceCommand::Initiate {
                direction: RebalanceDirection::AlpacaToBase,
                amount,
                withdrawal: TransferRef::AlpacaId(AlpacaTransferId::from(Uuid::new_v4())),
            },
        )
        .await
        .unwrap();
        cqrs.send(
            &id,
            UsdcRebalanceCommand::ConfirmWithdrawal {
                withdrawal_tx: None,
            },
        )
        .await
        .unwrap();
        cqrs.send(&id, UsdcRebalanceCommand::InitiateBridging { burn_tx })
            .await
            .unwrap();

        // Kill the chain so the post-attestation `destination_block` lookup
        // fails with a transient RPC error. Attestation polling hits the Circle
        // HTTP mock, not the chain, so it still resolves.
        drop(anvil);

        let error = manager
            .resume_alpaca_to_base(&id, amount)
            .await
            .unwrap_err();

        assert!(
            matches!(error, UsdcTransferError::Cctp(_)),
            "a destination head lookup failure must surface as a transient Cctp error, \
             got: {error:?}",
        );

        let state = cqrs.load(&id).await.unwrap().expect("aggregate exists");
        assert!(
            matches!(state, UsdcRebalance::Bridging { .. }),
            "a transient destination head lookup failure must leave the aggregate in \
             `Bridging` for retry, not latch a terminal `BridgingFailed`, got: {state:?}",
        );
    }

    /// On-chain harness for the BaseToAlpaca deposit-send leg: a single Ethereum
    /// anvil with `TestMintBurnToken` at the canonical USDC address and the bot
    /// wallet funded, plus a real funding tx whose hash/block stand in for the
    /// CCTP mint that the deposit scan is bounded by.
    struct EthereumUsdcChain {
        endpoint: String,
        bot_key: B256,
        bot_address: Address,
        mint_tx: TxHash,
        _anvil: TestAnvilInstance,
    }

    /// Deploys USDC on a fresh Ethereum anvil, funds the bot wallet, and records
    /// a real on-chain tx (the USDC mint to the bot) usable as the deposit scan's
    /// `mint_tx` bound. The bot is both the CCTP mint recipient and the deposit
    /// sender, mirroring production where `market_maker_wallet` is the bot wallet.
    async fn deploy_ethereum_usdc_chain() -> EthereumUsdcChain {
        let anvil = spawn_anvil(Anvil::new().chain_id(1u64));
        let endpoint = anvil.endpoint();
        let bot_key = B256::from_slice(&anvil.keys()[0].to_bytes());
        let bot_address = PrivateKeySigner::from_bytes(&bot_key).unwrap().address();

        let provider = ProviderBuilder::new().connect(&endpoint).await.unwrap();
        provider
            .anvil_set_code(USDC_ADDRESS, TestMintBurnToken::DEPLOYED_BYTECODE.clone())
            .await
            .unwrap();

        // A real USDC mint to the bot: funds the send and gives the resume scan a
        // genuine mint tx whose block bounds the transfer scan.
        let signer = PrivateKeySigner::from_bytes(&bot_key).unwrap();
        let bot_provider = ProviderBuilder::new()
            .wallet(alloy::network::EthereumWallet::from(signer))
            .connect(&endpoint)
            .await
            .unwrap();
        let token = TestMintBurnToken::new(USDC_ADDRESS, &bot_provider);
        let mint_receipt = token
            .mint(bot_address, U256::from(1_000_000_000_000u64))
            .send()
            .await
            .unwrap()
            .get_receipt()
            .await
            .unwrap();

        // Advance the head a few blocks past the mint so the finality-gated
        // deposit scan can conclude a true absence (Ok(None)) rather than a
        // retryable ScanInconclusive, letting a fresh resume proceed to the send.
        provider.anvil_mine(Some(5), None).await.unwrap();

        EthereumUsdcChain {
            endpoint,
            bot_key,
            bot_address,
            mint_tx: mint_receipt.transaction_hash,
            _anvil: anvil,
        }
    }

    /// Builds a manager whose CCTP bridge and vault both run against
    /// `chain` (Ethereum), with `market_maker_wallet` set to the bot wallet so
    /// the minted USDC is held by the address that signs the deposit send.
    /// `wallet_polling` overrides the Alpaca deposit poll cadence (a short timeout
    /// lets a no-match poll fail fast instead of hanging 30 minutes).
    async fn build_deposit_manager(
        chain: &EthereumUsdcChain,
        server: &MockServer,
        alpaca_wallet: Arc<AlpacaWalletService>,
        cqrs: Arc<Store<UsdcRebalance>>,
    ) -> CrossVenueCashTransfer<RawPrivateKeyWallet<impl alloy::providers::Provider + Clone + use<>>>
    {
        let alpaca_broker = InstrumentedAlpacaBroker::new(
            create_test_broker_service(server).await,
            TelemetrySender::disabled(),
        );
        let bridge_wallet = create_test_wallet(&chain.endpoint, &chain.bot_key);
        let (cctp_bridge, vault_service) = create_test_onchain_services(bridge_wallet);

        CrossVenueCashTransfer::new(
            alpaca_broker,
            alpaca_wallet,
            Arc::new(cctp_bridge),
            Arc::new(vault_service),
            cqrs,
            chain.bot_address,
            TEST_VAULT_ID,
            &test_settlement_params(),
        )
    }

    /// Builds an Alpaca wallet service against `server` with a short deposit poll
    /// timeout so a no-match poll resolves quickly in tests.
    fn create_short_poll_wallet_service(server: &MockServer) -> AlpacaWalletService {
        let client = AlpacaWalletClient::new(
            server.base_url(),
            TEST_ACCOUNT_ID,
            "test_key".to_string(),
            "test_secret".to_string(),
        );
        let polling = PollingConfig {
            interval: Duration::from_millis(10),
            timeout: Duration::from_millis(200),
            max_retries: 1,
            min_retry_delay: Duration::from_millis(5),
            max_retry_delay: Duration::from_millis(20),
        };

        AlpacaWalletService::new_with_client(client, Some(polling))
    }

    const ALPACA_DEPOSIT_ADDRESS: Address = address!("0x000000000000000000000000000000000000A1BC");

    /// Mocks the Alpaca `GET /wallets?asset=USDC&network=ethereum` deposit-address
    /// lookup the deposit-send leg calls before transferring.
    fn mock_alpaca_deposit_address(server: &MockServer) -> httpmock::Mock<'_> {
        server.mock(|when, then| {
            when.method(GET)
                .path("/v1/accounts/904837e3-3b76-47ec-b432-046db621571b/wallets")
                .query_param("asset", "USDC")
                .query_param("network", "ethereum");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "asset_id": "5d0de74f-827b-41a7-9f74-9c07c08fe55f",
                    "address": format!("{ALPACA_DEPOSIT_ADDRESS:#x}"),
                    "created_at": "2025-08-07T08:52:40.656166Z"
                }));
        })
    }

    /// Reads `holder`'s USDC balance on `chain` for asserting the deposit send
    /// actually credited the Alpaca address (and debited the bot).
    async fn usdc_balance_of(chain: &EthereumUsdcChain, holder: Address) -> U256 {
        let wallet = create_test_wallet(&chain.endpoint, &chain.bot_key);
        wallet
            .call::<NoOpErrorRegistry, _>(USDC_ADDRESS, IERC20::balanceOfCall { account: holder })
            .await
            .unwrap()
    }

    /// Reads the on-chain `deposit_ref` (the deposit send tx) recorded by
    /// `InitiateDeposit` and preserved into the failed state.
    fn deposit_ref_tx(state: &UsdcRebalance) -> TxHash {
        match state {
            UsdcRebalance::DepositInitiated {
                deposit_ref: TransferRef::OnchainTx(tx),
                ..
            }
            | UsdcRebalance::DepositFailed {
                deposit_ref: Some(TransferRef::OnchainTx(tx)),
                ..
            } => *tx,
            other => {
                panic!("expected a deposit state with an on-chain deposit_ref, got: {other:?}")
            }
        }
    }

    /// Idempotent resume: when a matching USDC transfer already exists on chain
    /// since the mint block, the leg ADOPTS it and does NOT send a second
    /// transfer (the bot's nonce is unchanged), then polls + confirms + converts
    /// to terminal.
    #[tokio::test]
    async fn resume_base_to_alpaca_from_bridged_adopts_existing_send() {
        let chain = deploy_ethereum_usdc_chain().await;
        let server = MockServer::start();
        let address_mock = mock_alpaca_deposit_address(&server);

        let alpaca_wallet = Arc::new(create_test_wallet_service(&server));
        let cqrs = create_test_store_instance().await;
        let manager = build_deposit_manager(&chain, &server, alpaca_wallet, cqrs.clone()).await;

        let id = UsdcRebalanceId(Uuid::new_v4());
        let amount = usdc("100");
        let amount_received = usdc("99.99");
        let amount_u256 = usdc_to_u256(amount_received).unwrap();
        stage_bridged_with_mint_tx(&cqrs, &id, amount, amount_received, chain.mint_tx).await;

        // Pre-submit the deposit transfer on chain (the crash-before-record
        // window): resume must adopt THIS tx instead of sending a second time.
        let bridge_wallet = create_test_wallet(&chain.endpoint, &chain.bot_key);
        let existing_send = bridge_wallet
            .submit::<NoOpErrorRegistry, _>(
                USDC_ADDRESS,
                IERC20::transferCall {
                    to: ALPACA_DEPOSIT_ADDRESS,
                    amount: amount_u256,
                },
                "pre-existing deposit send",
            )
            .await
            .unwrap()
            .transaction_hash;
        let nonce_after_send = bridge_wallet
            .provider()
            .get_transaction_count(chain.bot_address)
            .await
            .unwrap();

        // Deposit detection (by the adopted send tx) + USDC->USD conversion.
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
                    "from_address": format!("{:#x}", chain.bot_address),
                    "to_address": format!("{ALPACA_DEPOSIT_ADDRESS:#x}"),
                    "status": "COMPLETE",
                    "tx_hash": format!("{existing_send:#x}"),
                    "created_at": "2024-01-01T00:00:00Z",
                    "network_fee": "0",
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

        address_mock.assert();
        assert_eq!(
            bridge_wallet
                .provider()
                .get_transaction_count(chain.bot_address)
                .await
                .unwrap(),
            nonce_after_send,
            "adopting the existing send must NOT submit a second transfer (nonce unchanged)",
        );

        let state = cqrs.load(&id).await.unwrap().expect("aggregate exists");
        assert!(
            matches!(state, UsdcRebalance::ConversionComplete { .. }),
            "adopting the existing send must drive the deposit + conversion to terminal, got: {state:?}",
        );
    }

    /// Scan failure -> error, no send: when the pre-send chain scan cannot be run
    /// (the chain is unreachable), the leg returns an error and submits NO
    /// transfer, so a transient RPC fault never double-spends.
    #[tokio::test]
    async fn resume_base_to_alpaca_from_bridged_scan_failure_does_not_send() {
        let chain = deploy_ethereum_usdc_chain().await;
        let server = MockServer::start();
        let _address_mock = mock_alpaca_deposit_address(&server);

        let alpaca_wallet = Arc::new(create_test_wallet_service(&server));
        let cqrs = create_test_store_instance().await;
        let manager = build_deposit_manager(&chain, &server, alpaca_wallet, cqrs.clone()).await;

        let id = UsdcRebalanceId(Uuid::new_v4());
        let amount = usdc("100");
        let amount_received = usdc("99.99");
        stage_bridged_with_mint_tx(&cqrs, &id, amount, amount_received, chain.mint_tx).await;

        let bot_before = usdc_balance_of(&chain, chain.bot_address).await;
        assert_eq!(
            bot_before,
            U256::from(1_000_000_000_000u64),
            "the bot holds the full minted balance before the resume attempt",
        );

        // Kill the chain so the mint-block lookup / transfer scan fails. Dropping
        // the whole chain drops its anvil instance. The leg must surface a Cctp
        // error from the failed scan -- which happens strictly BEFORE the send --
        // so no transfer is ever submitted.
        drop(chain);

        let error = manager
            .resume_base_to_alpaca(&id, amount)
            .await
            .unwrap_err();
        assert!(
            matches!(error, UsdcTransferError::Cctp(_)),
            "a scan/chain failure before the send must surface a Cctp error, got: {error:?}",
        );
    }

    /// The FRESH `Bridged` entry (immediately after this execution minted) MUST
    /// send the deposit DIRECTLY with no pre-send chain scan -- mirroring the
    /// fresh burn path. The scan is finality-gated (head must be
    /// `SCAN_FINALITY_MARGIN` past the mint block) and would otherwise wedge with
    /// a retryable `ScanInconclusive` whenever the chain head sits at the mint
    /// block, exactly the e2e-anvil condition. This test keeps the head AT the
    /// mint block and asserts the fresh leg still sends and records the send tx.
    #[tokio::test]
    async fn continue_from_bridged_fresh_sends_directly_without_scan() {
        let chain = deploy_ethereum_usdc_chain_head_at_mint().await;
        let server = MockServer::start();
        let address_mock = mock_alpaca_deposit_address(&server);

        let alpaca_wallet = Arc::new(create_short_poll_wallet_service(&server));
        let cqrs = create_test_store_instance().await;
        let manager = build_deposit_manager(&chain, &server, alpaca_wallet, cqrs.clone()).await;

        let id = UsdcRebalanceId(Uuid::new_v4());
        let amount = usdc("100");
        let amount_received = usdc("99.99");
        stage_bridged_with_mint_tx(&cqrs, &id, amount, amount_received, chain.mint_tx).await;

        let alpaca_before = usdc_balance_of(&chain, ALPACA_DEPOSIT_ADDRESS).await;
        assert_eq!(alpaca_before, U256::ZERO, "Alpaca address starts unfunded");

        // No deposit transfer is mocked, so the fresh leg sends, then fails at the
        // poll (short timeout). The DIRECT send (no scan, no finality wait) is the
        // behavior under test.
        let error = manager
            .continue_from_bridged_fresh(&id, amount_received)
            .await
            .unwrap_err();
        assert!(
            matches!(error, UsdcTransferError::AlpacaWallet(_)),
            "with no deposit detected the leg fails at the Alpaca poll, got: {error:?}",
        );

        address_mock.assert();
        let amount_u256 = usdc_to_u256(amount_received).unwrap();
        assert_eq!(
            usdc_balance_of(&chain, ALPACA_DEPOSIT_ADDRESS).await,
            amount_u256,
            "the fresh leg MUST send the minted USDC to Alpaca's deposit address even with the \
             head at the mint block (no finality-gated scan)",
        );

        let state = cqrs.load(&id).await.unwrap().expect("aggregate exists");
        let recorded = deposit_ref_tx(&state);
        assert_eq!(
            usdc_transfer_recipient(&chain, recorded).await,
            ALPACA_DEPOSIT_ADDRESS,
            "the recorded send tx must transfer USDC to the Alpaca deposit address",
        );
    }

    /// Like [`deploy_ethereum_usdc_chain`] but leaves the chain head AT the mint
    /// block (no extra blocks mined). The finality-gated deposit scan cannot
    /// conclude here, so this isolates the fresh path's no-scan direct send.
    async fn deploy_ethereum_usdc_chain_head_at_mint() -> EthereumUsdcChain {
        let anvil = spawn_anvil(Anvil::new().chain_id(1u64));
        let endpoint = anvil.endpoint();
        let bot_key = B256::from_slice(&anvil.keys()[0].to_bytes());
        let bot_address = PrivateKeySigner::from_bytes(&bot_key).unwrap().address();

        let provider = ProviderBuilder::new().connect(&endpoint).await.unwrap();
        provider
            .anvil_set_code(USDC_ADDRESS, TestMintBurnToken::DEPLOYED_BYTECODE.clone())
            .await
            .unwrap();

        let signer = PrivateKeySigner::from_bytes(&bot_key).unwrap();
        let bot_provider = ProviderBuilder::new()
            .wallet(alloy::network::EthereumWallet::from(signer))
            .connect(&endpoint)
            .await
            .unwrap();
        let token = TestMintBurnToken::new(USDC_ADDRESS, &bot_provider);
        let mint_receipt = token
            .mint(bot_address, U256::from(1_000_000_000_000u64))
            .send()
            .await
            .unwrap()
            .get_receipt()
            .await
            .unwrap();

        EthereumUsdcChain {
            endpoint,
            bot_key,
            bot_address,
            mint_tx: mint_receipt.transaction_hash,
            _anvil: anvil,
        }
    }

    /// Reads the `to` of the USDC `Transfer` event emitted by `tx` on `chain`.
    async fn usdc_transfer_recipient(chain: &EthereumUsdcChain, tx: TxHash) -> Address {
        let provider = ProviderBuilder::new()
            .connect(&chain.endpoint)
            .await
            .unwrap();
        let receipt = provider
            .get_transaction_receipt(tx)
            .await
            .unwrap()
            .expect("send tx receipt exists");
        receipt
            .inner
            .logs()
            .iter()
            .find_map(|log| {
                IERC20::Transfer::decode_log(log.as_ref())
                    .ok()
                    .map(|event| event.to)
            })
            .expect("a USDC Transfer event is present in the send tx")
    }

    /// Drives the aggregate to `Bridged` recording `mint_tx` as the mint tx hash,
    /// so resume from `Bridged` re-enters `continue_from_bridged_resume` with a
    /// real on-chain mint tx to bound the deposit scan.
    async fn stage_bridged_with_mint_tx(
        cqrs: &Store<UsdcRebalance>,
        id: &UsdcRebalanceId,
        amount: Usdc,
        amount_received: Usdc,
        mint_tx: TxHash,
    ) {
        let burn_tx =
            fixed_bytes!("0xaaaa000000000000000000000000000000000000000000000000000000000099");
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
        cqrs.send(
            id,
            UsdcRebalanceCommand::ConfirmWithdrawal {
                withdrawal_tx: None,
            },
        )
        .await
        .unwrap();
        cqrs.send(id, UsdcRebalanceCommand::InitiateBridging { burn_tx })
            .await
            .unwrap();
        cqrs.send(
            id,
            UsdcRebalanceCommand::ReceiveAttestation {
                attestation: vec![0x01],
                cctp_nonce: B256::left_padding_from(&99_999u64.to_be_bytes()),
                message: valid_cctp_message(),
                mint_scan_from_block: 0,
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
    }

    // -------------------------------------------------------------------------
    // Settlement gate tests (on-chain USDC confirmation before CCTP burn)
    // -------------------------------------------------------------------------

    /// Sets up a single-chain Ethereum anvil with USDC deployed at USDC_ADDRESS
    /// and optionally mints `usdc_amount` to `recipient`.
    async fn deploy_ethereum_usdc_chain_with_balance(
        usdc_amount: U256,
        recipient: Address,
    ) -> EthereumUsdcChain {
        let anvil = spawn_anvil(Anvil::new().chain_id(1u64));
        let endpoint = anvil.endpoint();
        let bot_key = B256::from_slice(&anvil.keys()[0].to_bytes());
        let bot_address = PrivateKeySigner::from_bytes(&bot_key).unwrap().address();

        let provider = ProviderBuilder::new().connect(&endpoint).await.unwrap();
        provider
            .anvil_set_code(USDC_ADDRESS, TestMintBurnToken::DEPLOYED_BYTECODE.clone())
            .await
            .unwrap();

        let signer = PrivateKeySigner::from_bytes(&bot_key).unwrap();
        let bot_provider = ProviderBuilder::new()
            .wallet(alloy::network::EthereumWallet::from(signer))
            .connect(&endpoint)
            .await
            .unwrap();

        let token = TestMintBurnToken::new(USDC_ADDRESS, &bot_provider);
        let mint_receipt = token
            .mint(recipient, usdc_amount)
            .send()
            .await
            .unwrap()
            .get_receipt()
            .await
            .unwrap();

        EthereumUsdcChain {
            endpoint,
            bot_key,
            bot_address,
            mint_tx: mint_receipt.transaction_hash,
            _anvil: anvil,
        }
    }

    /// Builds a manager whose CCTP Ethereum endpoint points at `chain` and whose
    /// `market_maker_wallet` is the given address. The Base endpoint uses the same
    /// anvil (tests do not reach Base).
    async fn build_manager_with_ethereum_chain(
        chain: &EthereumUsdcChain,
        server: &MockServer,
        market_maker_wallet: Address,
    ) -> (
        CrossVenueCashTransfer<
            RawPrivateKeyWallet<impl alloy::providers::Provider + Clone + use<>>,
        >,
        Arc<Store<UsdcRebalance>>,
    ) {
        let alpaca_broker = InstrumentedAlpacaBroker::new(
            create_test_broker_service(server).await,
            TelemetrySender::disabled(),
        );
        let alpaca_wallet = Arc::new(create_test_wallet_service(server));
        let wallet = create_test_wallet(&chain.endpoint, &chain.bot_key);
        let (cctp_bridge, vault_service) = create_test_onchain_services(wallet);
        let cqrs = create_test_store_instance().await;

        let manager = CrossVenueCashTransfer::new(
            alpaca_broker,
            alpaca_wallet,
            Arc::new(cctp_bridge),
            Arc::new(vault_service),
            cqrs.clone(),
            market_maker_wallet,
            TEST_VAULT_ID,
            &test_settlement_params(),
        );

        (manager, cqrs)
    }

    /// Like [`build_manager_with_ethereum_chain`] but applies the fast burn-drop
    /// policy so an absent recorded burn tx classifies as `Dropped` immediately
    /// (no production 30 s grace), letting the dropped-burn resume test run fast.
    #[cfg(feature = "test-support")]
    async fn build_manager_with_ethereum_chain_fast_burn_drop(
        chain: &EthereumUsdcChain,
        server: &MockServer,
        market_maker_wallet: Address,
    ) -> (
        CrossVenueCashTransfer<
            RawPrivateKeyWallet<impl alloy::providers::Provider + Clone + use<>>,
        >,
        Arc<Store<UsdcRebalance>>,
    ) {
        let alpaca_broker = InstrumentedAlpacaBroker::new(
            create_test_broker_service(server).await,
            TelemetrySender::disabled(),
        );
        let alpaca_wallet = Arc::new(create_test_wallet_service(server));
        let wallet = create_test_wallet(&chain.endpoint, &chain.bot_key);
        let (cctp_bridge, vault_service) = create_test_onchain_services(wallet);
        let cctp_bridge = cctp_bridge.with_fast_burn_drop_policy();
        let cqrs = create_test_store_instance().await;

        let manager = CrossVenueCashTransfer::new(
            alpaca_broker,
            alpaca_wallet,
            Arc::new(cctp_bridge),
            Arc::new(vault_service),
            cqrs.clone(),
            market_maker_wallet,
            TEST_VAULT_ID,
            &test_settlement_params(),
        );

        (manager, cqrs)
    }

    /// Stages the aggregate at `WithdrawalComplete` (AlpacaToBase direction).
    async fn advance_to_withdrawal_complete_alpaca_to_base(
        cqrs: &Store<UsdcRebalance>,
        id: &UsdcRebalanceId,
        amount: Usdc,
    ) {
        use UsdcRebalanceCommand::*;

        cqrs.send(
            id,
            InitiateConversion {
                direction: RebalanceDirection::AlpacaToBase,
                amount,
                order_id: ClientOrderId::from_uuid(Uuid::new_v4()),
            },
        )
        .await
        .unwrap();
        cqrs.send(
            id,
            ConfirmConversion {
                conversion: par_conversion(amount),
            },
        )
        .await
        .unwrap();
        cqrs.send(
            id,
            Initiate {
                direction: RebalanceDirection::AlpacaToBase,
                amount,
                withdrawal: TransferRef::AlpacaId(AlpacaTransferId::from(Uuid::new_v4())),
            },
        )
        .await
        .unwrap();
        cqrs.send(
            id,
            ConfirmWithdrawal {
                withdrawal_tx: None,
            },
        )
        .await
        .unwrap();
    }

    /// Hypothesis: when Alpaca deducts a withdrawal fee and the wallet receives
    /// LESS than the nominal amount, `continue_alpaca_to_base_from_withdrawal_complete`
    /// uses the ACTUAL wallet balance (not nominal) as the CCTP burn amount.
    /// The aggregate must advance past WithdrawalComplete (BeginBridging emitted).
    ///
    /// This test verifies AC#1, AC#2, and AC#3: a short delivery no longer
    /// wedges forever. The burn proceeds with the received amount.
    #[tokio::test]
    async fn withdrawal_fee_shortfall_burns_received_amount_not_nominal() {
        let market_maker_wallet = address!("0x2222222222222222222222222222222222222222");

        // Alpaca retained a 2 USDC fee: wallet received 998 USDC, not 1000.
        let received_amount = U256::from(998_000_000u64); // 998 USDC in atomic units
        let chain =
            deploy_ethereum_usdc_chain_with_balance(received_amount, market_maker_wallet).await;

        let server = MockServer::start();
        let (manager, cqrs) =
            build_manager_with_ethereum_chain(&chain, &server, market_maker_wallet).await;

        // Deploy a REVERT contract at TOKEN_MESSENGER_V2 so the burn fails
        // predictably. The bytecode is PUSH1 0 PUSH1 0 REVERT (0x60 0x00 0x60
        // 0x00 0xFD), which pushes two zero arguments for the REVERT opcode so
        // the stack is not underflowed. This produces a proper EVM revert
        // (error code 3 / "execution reverted"), recognized by is_revert().
        // A bare 0xFD with empty stack would cause StackUnderflow (code -32603),
        // which is_revert() does not classify as a revert-class error.
        // BeginBridging fires before the burn attempt; the REVERT leaves the
        // aggregate at BridgingSubmitting (not BridgingFailed).
        let revert_bytecode = alloy::primitives::Bytes::from(vec![0x60u8, 0x00, 0x60, 0x00, 0xFD]);
        let provider = ProviderBuilder::new()
            .connect(&chain.endpoint)
            .await
            .unwrap();
        provider
            .anvil_set_code(st0x_bridge::cctp::TOKEN_MESSENGER_V2, revert_bytecode)
            .await
            .unwrap();

        let id = UsdcRebalanceId(Uuid::new_v4());
        // Nominal was 1000 USDC but only 998 landed.
        let nominal = usdc("1000");

        advance_to_withdrawal_complete_alpaca_to_base(&cqrs, &id, nominal).await;

        // The burn attempt fails (Cctp error from the fee query or the REVERT
        // contract). Either way the error must not be a balance-gate wedge.
        let error = manager
            .continue_alpaca_to_base_from_withdrawal_complete(&id, nominal, None)
            .await
            .unwrap_err();

        assert!(
            matches!(error, UsdcTransferError::BurnRevert(_)),
            "Expected BurnRevert error from REVERT-bytecode burn (0xFD), got: {error:?}"
        );

        // Aggregate at BridgingSubmitting confirms BeginBridging was emitted
        // before the burn was attempted; the balance gate did not wedge here.
        let state = cqrs.load(&id).await.unwrap().expect("aggregate exists");
        assert!(
            matches!(
                state,
                UsdcRebalance::BridgingSubmitting {
                    direction: RebalanceDirection::AlpacaToBase,
                    ..
                }
            ),
            "Aggregate must be at BridgingSubmitting (burn attempted); \
             WithdrawalComplete would indicate the balance gate incorrectly wedged; \
             got: {state:?}"
        );

        // The persisted burn_amount must be the RECEIVED (capped) amount, not
        // nominal. This verifies crash-resume uses the correct scan amount.
        let UsdcRebalance::BridgingSubmitting { burn_amount, .. } = state else {
            panic!("Expected BridgingSubmitting state; got: {state:?}");
        };
        let expected_received = u256_to_usdc(received_amount).unwrap();
        assert_eq!(
            burn_amount,
            Some(expected_received),
            "BridgingSubmitting.burn_amount must be the received (capped) amount \
             (998 USDC), not nominal (1000 USDC)"
        );
    }

    /// Hypothesis: zero wallet balance after confirmed withdrawal returns
    /// WalletUsdcInsufficient (delayed-redrive), NOT BridgingFailed. The
    /// aggregate stays in WithdrawalComplete for the next attempt.
    #[tokio::test]
    async fn withdrawal_zero_balance_redrives_not_fails() {
        let market_maker_wallet = address!("0x2222222222222222222222222222222222222222");

        // Wallet holds zero USDC -- withdrawal funds have not settled yet.
        let chain = deploy_ethereum_usdc_chain_with_balance(U256::ZERO, market_maker_wallet).await;

        let server = MockServer::start();
        let (manager, cqrs) =
            build_manager_with_ethereum_chain(&chain, &server, market_maker_wallet).await;

        let id = UsdcRebalanceId(Uuid::new_v4());
        let amount = usdc("1");

        advance_to_withdrawal_complete_alpaca_to_base(&cqrs, &id, amount).await;

        let error = manager
            .continue_alpaca_to_base_from_withdrawal_complete(&id, amount, None)
            .await
            .unwrap_err();

        let UsdcTransferError::WalletUsdcInsufficient { nominal, .. } = &error else {
            panic!("Expected WalletUsdcInsufficient; got: {error:?}");
        };
        assert_eq!(
            *nominal,
            usdc("1"),
            "WalletUsdcInsufficient nominal must match the requested amount"
        );

        // No FailBridging; aggregate stays in WithdrawalComplete for next redrive.
        let state = cqrs.load(&id).await.unwrap().expect("aggregate exists");
        assert!(
            matches!(
                state,
                UsdcRebalance::WithdrawalComplete {
                    direction: RebalanceDirection::AlpacaToBase,
                    ..
                }
            ),
            "Aggregate must stay in WithdrawalComplete (retryable), not BridgingFailed; \
             got: {state:?}"
        );
    }

    /// Hypothesis: when `withdrawal_tx` is `None`, the tx-confirmation gate is
    /// entirely skipped (no RPC call to `ethereum_tx_block`). The burn-scan
    /// `from_block` falls back to `source_block(EthereumToBase)` (chain head).
    ///
    /// This test is distinct from `withdrawal_fee_shortfall_burns_received_amount_not_nominal`:
    /// that test exercises the fee-shortfall cap logic; this one focuses on the
    /// `None`-tx code path, verifying that a missing tx hash does not cause an
    /// RPC confirmation call (which would fail and wedge the aggregate).
    ///
    /// Proof: the MockServer has no RPC endpoint registered for
    /// `eth_getTransactionByHash`. If the confirmation gate were entered, the
    /// mock would return 404 and the call would propagate a `SettlementCheckTransient`
    /// error — not the `Cctp` error we assert below.
    #[tokio::test]
    async fn withdrawal_no_tx_hash_skips_confirmation_gate() {
        let market_maker_wallet = address!("0x2222222222222222222222222222222222222222");

        // 998 USDC in wallet (fee deducted scenario, no tx hash from Alpaca).
        let received_amount = U256::from(998_000_000u64);
        let chain =
            deploy_ethereum_usdc_chain_with_balance(received_amount, market_maker_wallet).await;

        let server = MockServer::start();
        let (manager, cqrs) =
            build_manager_with_ethereum_chain(&chain, &server, market_maker_wallet).await;

        // REVERT contract: confirms burn was attempted (not wedged by gates).
        // The bytecode PUSH1 0 PUSH1 0 REVERT (0x60 0x00 0x60 0x00 0xFD)
        // produces a proper EVM revert (code 3 / "execution reverted") so
        // is_revert() classifies it as a BurnRevert. A bare 0xFD with empty
        // stack causes StackUnderflow (code -32603), which is_revert() rejects.
        let revert_bytecode = alloy::primitives::Bytes::from(vec![0x60u8, 0x00, 0x60, 0x00, 0xFD]);
        let provider = ProviderBuilder::new()
            .connect(&chain.endpoint)
            .await
            .unwrap();
        provider
            .anvil_set_code(st0x_bridge::cctp::TOKEN_MESSENGER_V2, revert_bytecode)
            .await
            .unwrap();

        let id = UsdcRebalanceId(Uuid::new_v4());
        let nominal = usdc("1000");

        advance_to_withdrawal_complete_alpaca_to_base(&cqrs, &id, nominal).await;

        // withdrawal_tx = None: no confirmation gate is entered at all.
        // If the confirmation gate ran, the mock server would return 404 (no
        // tx-confirmation endpoint registered), producing SettlementCheckTransient.
        // A BurnRevert error proves the gate was skipped and the burn was attempted.
        let error = manager
            .continue_alpaca_to_base_from_withdrawal_complete(&id, nominal, None)
            .await
            .unwrap_err();

        assert!(
            matches!(error, UsdcTransferError::BurnRevert(_)),
            "Expected BurnRevert error from REVERT-bytecode burn (0xFD, confirmation gate \
             skipped); SettlementCheckTransient would indicate the gate incorrectly fired; \
             got: {error:?}"
        );

        let state = cqrs.load(&id).await.unwrap().expect("aggregate exists");
        assert!(
            matches!(
                state,
                UsdcRebalance::BridgingSubmitting {
                    direction: RebalanceDirection::AlpacaToBase,
                    ..
                }
            ),
            "Aggregate must be at BridgingSubmitting (burn proceeded); got: {state:?}"
        );
    }

    /// Hypothesis: when the market-maker wallet holds MORE USDC than the nominal,
    /// `continue_alpaca_to_base_from_withdrawal_complete` returns
    /// `WalletUsdcAmbientBalance` and NO burn is attempted. The aggregate moves
    /// to `BridgingFailed` (FailBridging emitted) for operator reconciliation.
    ///
    /// This guards the wallet-empty-between-rebalances invariant: if ambient or
    /// residual USDC from a prior rebalance is present, we cannot distinguish
    /// the withdrawal's funds from the residual, so we must not burn at all.
    #[tokio::test]
    async fn wallet_balance_exceeding_nominal_returns_ambient_balance_error() {
        let market_maker_wallet = address!("0x2222222222222222222222222222222222222222");
        let nominal = usdc("1000");
        let nominal_u256 = usdc_to_u256(nominal).unwrap();

        // Wallet holds nominal + 50 USDC: the wallet-empty invariant is broken.
        let ambient_balance = nominal_u256 + U256::from(50_000_000u64);
        let chain =
            deploy_ethereum_usdc_chain_with_balance(ambient_balance, market_maker_wallet).await;

        let server = MockServer::start();
        let (manager, cqrs) =
            build_manager_with_ethereum_chain(&chain, &server, market_maker_wallet).await;

        let id = UsdcRebalanceId(Uuid::new_v4());

        advance_to_withdrawal_complete_alpaca_to_base(&cqrs, &id, nominal).await;

        let error = manager
            .continue_alpaca_to_base_from_withdrawal_complete(&id, nominal, None)
            .await
            .unwrap_err();

        // Must error with WalletUsdcAmbientBalance, NOT attempt the burn.
        let UsdcTransferError::WalletUsdcAmbientBalance {
            id: err_id,
            balance,
            nominal: err_nominal,
        } = error
        else {
            panic!(
                "Expected WalletUsdcAmbientBalance (ambient USDC in wallet, \
                 no burn attempted); got: {error:?}"
            );
        };
        assert_eq!(err_id, id);
        assert_eq!(err_nominal, nominal);
        assert!(
            balance > nominal,
            "balance ({balance}) must exceed nominal ({nominal})"
        );

        // Aggregate must have moved to BridgingFailed (FailBridging emitted):
        // no burn occurred, but the state is terminal for operator reconciliation.
        let state = cqrs.load(&id).await.unwrap().expect("aggregate exists");
        assert!(
            matches!(
                state,
                UsdcRebalance::BridgingFailed {
                    burn_tx_hash: None,
                    ..
                }
            ),
            "Aggregate must be at BridgingFailed with no burn tx \
             (ambient USDC detected before any burn); got: {state:?}"
        );
    }

    /// Hypothesis: when `withdrawal_tx` is `Some` (confirmation gate passes) but
    /// the wallet holds zero USDC, `continue_alpaca_to_base_from_withdrawal_complete`
    /// returns `WalletUsdcInsufficient` (delayed-redrive) and the aggregate stays
    /// at `WithdrawalComplete`. This is the timing-edge-case scenario: the
    /// withdrawal tx is confirmed but the funds have not yet settled on the
    /// destination RPC node.
    #[tokio::test]
    async fn withdrawal_zero_balance_with_confirmed_tx_redrives_not_fails() {
        let market_maker_wallet = address!("0x2222222222222222222222222222222222222222");

        // Wallet holds zero USDC -- funds not yet visible on this RPC node.
        let chain = deploy_ethereum_usdc_chain_with_balance(U256::ZERO, market_maker_wallet).await;

        // Mine extra blocks so the withdrawal tx has >= required_confirmations (3).
        let provider = ProviderBuilder::new()
            .connect(&chain.endpoint)
            .await
            .unwrap();
        provider.anvil_mine(Some(5), None).await.unwrap();

        let server = MockServer::start();
        let (manager, cqrs) =
            build_manager_with_ethereum_chain(&chain, &server, market_maker_wallet).await;

        let id = UsdcRebalanceId(Uuid::new_v4());
        let amount = usdc("1");

        advance_to_withdrawal_complete_alpaca_to_base(&cqrs, &id, amount).await;

        // withdrawal_tx = Some(confirmed_tx): the confirmation gate passes, but
        // the balance read returns zero (RPC node lag).
        let confirmed_tx = chain.mint_tx;
        let error = manager
            .continue_alpaca_to_base_from_withdrawal_complete(&id, amount, Some(confirmed_tx))
            .await
            .unwrap_err();

        let UsdcTransferError::WalletUsdcInsufficient { nominal, .. } = &error else {
            panic!(
                "Expected WalletUsdcInsufficient (zero balance after confirmed tx); \
                 got: {error:?}"
            );
        };
        assert_eq!(
            *nominal, amount,
            "WalletUsdcInsufficient nominal must match the requested amount"
        );

        // Aggregate stays in WithdrawalComplete -- retryable, not BridgingFailed.
        let state = cqrs.load(&id).await.unwrap().expect("aggregate exists");
        assert!(
            matches!(
                state,
                UsdcRebalance::WithdrawalComplete {
                    direction: RebalanceDirection::AlpacaToBase,
                    ..
                }
            ),
            "Aggregate must stay in WithdrawalComplete (retryable); got: {state:?}"
        );
    }

    /// Hypothesis: a pre-commit burn failure (burn tx REVERTS on-chain) does NOT
    /// emit FailBridging; the crash-safe `execute_cctp_burn_on_ethereum` emits
    /// `BeginBridging` BEFORE the burn call, landing the aggregate at
    /// `BridgingSubmitting`. The burn revert leaves it there (no terminal event),
    /// so apalis retries enter `resume_bridging_submitting_ethereum` which scans
    /// for the burn before re-attempting -- avoiding any double-burn.
    ///
    /// A REVERT-only contract at the TokenMessenger address ensures any tx
    /// that reaches the contract level also reverts. Calling an address with
    /// no deployed bytecode would succeed (no-code = success with no events)
    /// and trigger the post-commit MessageSentEventNotFound path instead.
    #[tokio::test]
    async fn pre_commit_burn_failure_does_not_produce_bridging_failed() {
        let market_maker_wallet = address!("0x2222222222222222222222222222222222222222");

        // Sufficient USDC balance to pass the balance gate.
        let required = U256::from(1_000_000u64);
        let chain = deploy_ethereum_usdc_chain_with_balance(required, market_maker_wallet).await;

        let server = MockServer::start();
        let (manager, cqrs) =
            build_manager_with_ethereum_chain(&chain, &server, market_maker_wallet).await;

        // Deploy a REVERT contract at TOKEN_MESSENGER_V2. The bytecode is
        // PUSH1 0 PUSH1 0 REVERT (0x60 0x00 0x60 0x00 0xFD): two zero
        // arguments for REVERT so the stack is not underflowed. This produces
        // a proper EVM revert (code 3 / "execution reverted") recognized by
        // is_revert(). A bare 0xFD with empty stack causes StackUnderflow
        // (code -32603) which is_revert() does not classify as a revert.
        let revert_bytecode = alloy::primitives::Bytes::from(vec![0x60u8, 0x00, 0x60, 0x00, 0xFD]);
        let provider = ProviderBuilder::new()
            .connect(&chain.endpoint)
            .await
            .unwrap();
        provider
            .anvil_set_code(st0x_bridge::cctp::TOKEN_MESSENGER_V2, revert_bytecode)
            .await
            .unwrap();

        let id = UsdcRebalanceId(Uuid::new_v4());
        let amount = usdc("1");

        advance_to_withdrawal_complete_alpaca_to_base(&cqrs, &id, amount).await;

        // execute_cctp_burn_on_ethereum is called indirectly through
        // continue_alpaca_to_base_from_withdrawal_complete. BeginBridging is
        // emitted before the burn attempt; the REVERT leaves the aggregate at
        // BridgingSubmitting with no FailBridging.
        let error = manager
            .continue_alpaca_to_base_from_withdrawal_complete(&id, amount, None)
            .await
            .unwrap_err();

        assert!(
            matches!(error, UsdcTransferError::BurnRevert(_)),
            "Expected BurnRevert error from REVERT-bytecode burn (0xFD pre-commit), got: {error:?}"
        );

        // Aggregate must be in BridgingSubmitting (BeginBridging was emitted before
        // the burn). No FailBridging -- the burn was never committed; retries are safe.
        let state = cqrs.load(&id).await.unwrap().expect("aggregate exists");
        assert!(
            matches!(
                state,
                UsdcRebalance::BridgingSubmitting {
                    direction: RebalanceDirection::AlpacaToBase,
                    ..
                }
            ),
            "Aggregate must be at BridgingSubmitting after pre-commit burn failure \
             (BeginBridging persisted, burn failed before InitiateBridging); got: {state:?}"
        );
    }

    /// Helper: creates an HTTP mock that serves a single COMPLETE transfer with
    /// the given `tx_hash` for the transfer polling endpoint.
    fn mock_complete_withdrawal_with_tx(
        server: &MockServer,
        transfer_uuid: Uuid,
        tx_hash: Option<TxHash>,
    ) -> httpmock::Mock<'_> {
        let tx_hash_value = tx_hash.map_or(serde_json::Value::Null, |tx| {
            serde_json::Value::String(format!("{tx:#x}"))
        });

        server.mock(|when, then| {
            when.method(GET)
                .path(format!(
                    "/v1/accounts/904837e3-3b76-47ec-b432-046db621571b/wallets/transfers/{transfer_uuid}"
                ));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "id": transfer_uuid.to_string(),
                    "direction": "OUTGOING",
                    "amount": "100",
                    "usd_value": "100",
                    "chain": "ethereum",
                    "asset": "USDC",
                    "from_address": "0x0000000000000000000000000000000000000001",
                    "to_address": "0x2222222222222222222222222222222222222222",
                    "status": "COMPLETE",
                    "tx_hash": tx_hash_value,
                    "created_at": "2024-01-01T00:00:00Z",
                    "network_fee": "0",
                    "fees": "0"
                }));
        })
    }

    /// Hypothesis: poll_and_confirm_withdrawal returns WithdrawalTxUnderconfirmed
    /// (retryable) when the withdrawal tx exists but has fewer than the required
    /// number of confirmations (3). The aggregate MUST advance to WithdrawalComplete
    /// before the early return so that apalis redrives enter the balance-gate path
    /// instead of re-polling Alpaca.
    #[tokio::test]
    async fn withdrawal_tx_with_fewer_than_required_confirmations_returns_retryable_error() {
        // Deploy USDC with balance; the endpoint gives us a provider for block checks.
        let market_maker_wallet = address!("0x2222222222222222222222222222222222222222");
        let chain = deploy_ethereum_usdc_chain_with_balance(U256::ZERO, market_maker_wallet).await;

        // The mint_tx in `chain` was mined in block N. The anvil head is at N
        // (no extra blocks mined), so confirmations = 1 (the inclusion block
        // counts) < REQUIRED (3).
        let tx_with_one_confirmation = chain.mint_tx;

        let server = MockServer::start();
        let (manager, cqrs) =
            build_manager_with_ethereum_chain(&chain, &server, market_maker_wallet).await;

        let transfer_uuid = Uuid::new_v4();
        let _transfer_mock = mock_complete_withdrawal_with_tx(
            &server,
            transfer_uuid,
            Some(tx_with_one_confirmation),
        );

        let id = UsdcRebalanceId(Uuid::new_v4());
        let amount = usdc("1");

        // Stage aggregate at Withdrawing.
        let withdrawal_id = AlpacaTransferId::from(transfer_uuid);
        cqrs.send(
            &id,
            UsdcRebalanceCommand::InitiateConversion {
                direction: RebalanceDirection::AlpacaToBase,
                amount,
                order_id: ClientOrderId::from_uuid(Uuid::new_v4()),
            },
        )
        .await
        .unwrap();
        cqrs.send(
            &id,
            UsdcRebalanceCommand::ConfirmConversion {
                conversion: par_conversion(amount),
            },
        )
        .await
        .unwrap();
        cqrs.send(
            &id,
            UsdcRebalanceCommand::Initiate {
                direction: RebalanceDirection::AlpacaToBase,
                amount,
                withdrawal: TransferRef::AlpacaId(withdrawal_id),
            },
        )
        .await
        .unwrap();

        let error = manager
            .poll_and_confirm_withdrawal(&id, &withdrawal_id, Utc::now())
            .await
            .unwrap_err();

        assert!(
            matches!(
                error,
                UsdcTransferError::WithdrawalTxUnderconfirmed {
                    actual: 1,
                    required: 3,
                    ..
                }
            ),
            "Expected WithdrawalTxUnderconfirmed with 1 actual confirmation, got: {error:?}"
        );

        // Aggregate MUST have advanced to WithdrawalComplete before the early return
        // so that apalis redrives enter the balance-gate path instead of re-polling Alpaca.
        let state = cqrs.load(&id).await.unwrap().expect("aggregate exists");
        assert!(
            matches!(
                state,
                UsdcRebalance::WithdrawalComplete {
                    direction: RebalanceDirection::AlpacaToBase,
                    ..
                }
            ),
            "Aggregate must advance to WithdrawalComplete when tx is under-confirmed; got: {state:?}"
        );
    }

    /// Hypothesis: poll_and_confirm_withdrawal returns WithdrawalTxUnderconfirmed
    /// when the withdrawal tx hash is not yet mined (not in the chain). The
    /// unmined path reports actual=0 and a distinct log; the aggregate MUST advance
    /// to WithdrawalComplete before the early return.
    #[tokio::test]
    async fn withdrawal_tx_not_yet_mined_returns_retryable_error() {
        let market_maker_wallet = address!("0x2222222222222222222222222222222222222222");
        let chain = deploy_ethereum_usdc_chain_with_balance(U256::ZERO, market_maker_wallet).await;

        // A random tx hash that was never submitted to the chain.
        let unmined_tx = TxHash::ZERO;

        let server = MockServer::start();
        let (manager, cqrs) =
            build_manager_with_ethereum_chain(&chain, &server, market_maker_wallet).await;

        let transfer_uuid = Uuid::new_v4();
        let _transfer_mock =
            mock_complete_withdrawal_with_tx(&server, transfer_uuid, Some(unmined_tx));

        let id = UsdcRebalanceId(Uuid::new_v4());
        let amount = usdc("1");
        let withdrawal_id = AlpacaTransferId::from(transfer_uuid);

        // Stage at Withdrawing.
        cqrs.send(
            &id,
            UsdcRebalanceCommand::InitiateConversion {
                direction: RebalanceDirection::AlpacaToBase,
                amount,
                order_id: ClientOrderId::from_uuid(Uuid::new_v4()),
            },
        )
        .await
        .unwrap();
        cqrs.send(
            &id,
            UsdcRebalanceCommand::ConfirmConversion {
                conversion: par_conversion(amount),
            },
        )
        .await
        .unwrap();
        cqrs.send(
            &id,
            UsdcRebalanceCommand::Initiate {
                direction: RebalanceDirection::AlpacaToBase,
                amount,
                withdrawal: TransferRef::AlpacaId(withdrawal_id),
            },
        )
        .await
        .unwrap();

        let error = manager
            .poll_and_confirm_withdrawal(&id, &withdrawal_id, Utc::now())
            .await
            .unwrap_err();

        // The not-yet-mined path reports actual=0 (tx block is unknown);
        // the log message distinguishes it from a mined-but-0-confirmed tx.
        assert!(
            matches!(
                error,
                UsdcTransferError::WithdrawalTxUnderconfirmed {
                    actual: 0,
                    required: 3,
                    ..
                }
            ),
            "Expected WithdrawalTxUnderconfirmed for unmined tx, got: {error:?}"
        );

        // Aggregate MUST have advanced to WithdrawalComplete before the early return
        // so that apalis redrives enter the balance-gate path instead of re-polling Alpaca.
        let state = cqrs.load(&id).await.unwrap().expect("aggregate exists");
        assert!(
            matches!(
                state,
                UsdcRebalance::WithdrawalComplete {
                    direction: RebalanceDirection::AlpacaToBase,
                    ..
                }
            ),
            "Aggregate must advance to WithdrawalComplete for unmined tx; got: {state:?}"
        );
    }

    /// Hypothesis: poll_and_confirm_withdrawal returns WithdrawalTxUnderconfirmed
    /// when the withdrawal tx has exactly 3 - 1
    /// (the off-by-one boundary: one short is still retryable).
    #[tokio::test]
    async fn withdrawal_tx_at_n_minus_1_confirmations_returns_retryable_error() {
        let market_maker_wallet = address!("0x2222222222222222222222222222222222222222");
        let chain = deploy_ethereum_usdc_chain_with_balance(U256::ZERO, market_maker_wallet).await;

        // Mine REQUIRED - 2 extra blocks so tx confirmations = REQUIRED - 1
        // (the inclusion block counts as confirmation 1, plus the extras).
        let provider = ProviderBuilder::new()
            .connect(&chain.endpoint)
            .await
            .unwrap();
        provider.anvil_mine(Some(3 - 2), None).await.unwrap();

        let tx_hash = chain.mint_tx;
        let server = MockServer::start();
        let (manager, cqrs) =
            build_manager_with_ethereum_chain(&chain, &server, market_maker_wallet).await;

        let transfer_uuid = Uuid::new_v4();
        let _transfer_mock =
            mock_complete_withdrawal_with_tx(&server, transfer_uuid, Some(tx_hash));

        let id = UsdcRebalanceId(Uuid::new_v4());
        let amount = usdc("1");
        let withdrawal_id = AlpacaTransferId::from(transfer_uuid);

        // Stage at Withdrawing.
        cqrs.send(
            &id,
            UsdcRebalanceCommand::InitiateConversion {
                direction: RebalanceDirection::AlpacaToBase,
                amount,
                order_id: ClientOrderId::from_uuid(Uuid::new_v4()),
            },
        )
        .await
        .unwrap();
        cqrs.send(
            &id,
            UsdcRebalanceCommand::ConfirmConversion {
                conversion: par_conversion(amount),
            },
        )
        .await
        .unwrap();
        cqrs.send(
            &id,
            UsdcRebalanceCommand::Initiate {
                direction: RebalanceDirection::AlpacaToBase,
                amount,
                withdrawal: TransferRef::AlpacaId(withdrawal_id),
            },
        )
        .await
        .unwrap();

        let error = manager
            .poll_and_confirm_withdrawal(&id, &withdrawal_id, Utc::now())
            .await
            .unwrap_err();

        assert!(
            matches!(
                error,
                UsdcTransferError::WithdrawalTxUnderconfirmed {
                    actual,
                    required: 3,
                    ..
                } if actual == 3 - 1
            ),
            "Expected WithdrawalTxUnderconfirmed with actual=REQUIRED-1, got: {error:?}"
        );

        // Aggregate MUST have advanced to WithdrawalComplete before the early return
        // so that apalis redrives enter the balance-gate path instead of re-polling Alpaca.
        let state = cqrs.load(&id).await.unwrap().expect("aggregate exists");
        assert!(
            matches!(
                state,
                UsdcRebalance::WithdrawalComplete {
                    direction: RebalanceDirection::AlpacaToBase,
                    ..
                }
            ),
            "Aggregate must advance to WithdrawalComplete at N-1 confirmations; got: {state:?}"
        );
    }

    /// Hypothesis: poll_and_confirm_withdrawal sends ConfirmWithdrawal and
    /// advances the aggregate to WithdrawalComplete when the withdrawal tx has
    /// at least 3.
    #[tokio::test]
    async fn withdrawal_tx_with_required_confirmations_sends_confirm_withdrawal() {
        let market_maker_wallet = address!("0x2222222222222222222222222222222222222222");
        let chain = deploy_ethereum_usdc_chain_with_balance(U256::ZERO, market_maker_wallet).await;

        // Mine REQUIRED - 1 extra blocks so the tx reaches exactly REQUIRED
        // confirmations (the inclusion block counts as confirmation 1).
        let provider = ProviderBuilder::new()
            .connect(&chain.endpoint)
            .await
            .unwrap();
        provider.anvil_mine(Some(3 - 1), None).await.unwrap();

        let tx_hash = chain.mint_tx;
        let server = MockServer::start();
        let (manager, cqrs) =
            build_manager_with_ethereum_chain(&chain, &server, market_maker_wallet).await;

        let transfer_uuid = Uuid::new_v4();
        let _transfer_mock =
            mock_complete_withdrawal_with_tx(&server, transfer_uuid, Some(tx_hash));

        let id = UsdcRebalanceId(Uuid::new_v4());
        let amount = usdc("1");
        let withdrawal_id = AlpacaTransferId::from(transfer_uuid);

        // Stage at Withdrawing.
        cqrs.send(
            &id,
            UsdcRebalanceCommand::InitiateConversion {
                direction: RebalanceDirection::AlpacaToBase,
                amount,
                order_id: ClientOrderId::from_uuid(Uuid::new_v4()),
            },
        )
        .await
        .unwrap();
        cqrs.send(
            &id,
            UsdcRebalanceCommand::ConfirmConversion {
                conversion: par_conversion(amount),
            },
        )
        .await
        .unwrap();
        cqrs.send(
            &id,
            UsdcRebalanceCommand::Initiate {
                direction: RebalanceDirection::AlpacaToBase,
                amount,
                withdrawal: TransferRef::AlpacaId(withdrawal_id),
            },
        )
        .await
        .unwrap();

        manager
            .poll_and_confirm_withdrawal(&id, &withdrawal_id, Utc::now())
            .await
            .unwrap();

        // Aggregate must be in WithdrawalComplete.
        let state = cqrs.load(&id).await.unwrap().expect("aggregate exists");
        assert!(
            matches!(
                state,
                UsdcRebalance::WithdrawalComplete {
                    direction: RebalanceDirection::AlpacaToBase,
                    ..
                }
            ),
            "Aggregate must advance to WithdrawalComplete when tx is confirmed; got: {state:?}"
        );
    }

    /// Hypothesis: when Alpaca returns no tx_hash on the withdrawal transfer,
    /// poll_and_confirm_withdrawal logs a warning and falls through -- it sends
    /// ConfirmWithdrawal and advances the aggregate to WithdrawalComplete.
    /// (The fallback balance gate covers the burn step.)
    #[tokio::test]
    async fn withdrawal_tx_absent_falls_through_to_balance_gate() {
        let market_maker_wallet = address!("0x2222222222222222222222222222222222222222");
        let chain = deploy_ethereum_usdc_chain_with_balance(U256::ZERO, market_maker_wallet).await;

        let server = MockServer::start();
        let (manager, cqrs) =
            build_manager_with_ethereum_chain(&chain, &server, market_maker_wallet).await;

        let transfer_uuid = Uuid::new_v4();
        // tx_hash is None -- Alpaca did not return one.
        let _transfer_mock = mock_complete_withdrawal_with_tx(&server, transfer_uuid, None);

        let id = UsdcRebalanceId(Uuid::new_v4());
        let amount = usdc("1");
        let withdrawal_id = AlpacaTransferId::from(transfer_uuid);

        // Stage at Withdrawing.
        cqrs.send(
            &id,
            UsdcRebalanceCommand::InitiateConversion {
                direction: RebalanceDirection::AlpacaToBase,
                amount,
                order_id: ClientOrderId::from_uuid(Uuid::new_v4()),
            },
        )
        .await
        .unwrap();
        cqrs.send(
            &id,
            UsdcRebalanceCommand::ConfirmConversion {
                conversion: par_conversion(amount),
            },
        )
        .await
        .unwrap();
        cqrs.send(
            &id,
            UsdcRebalanceCommand::Initiate {
                direction: RebalanceDirection::AlpacaToBase,
                amount,
                withdrawal: TransferRef::AlpacaId(withdrawal_id),
            },
        )
        .await
        .unwrap();

        // Falls through to ConfirmWithdrawal even without a tx hash.
        manager
            .poll_and_confirm_withdrawal(&id, &withdrawal_id, Utc::now())
            .await
            .unwrap();

        let state = cqrs.load(&id).await.unwrap().expect("aggregate exists");
        assert!(
            matches!(
                state,
                UsdcRebalance::WithdrawalComplete {
                    direction: RebalanceDirection::AlpacaToBase,
                    ..
                }
            ),
            "WithdrawalComplete expected when tx_hash is absent (fall-through); got: {state:?}"
        );
    }

    /// Hypothesis (combined path): when Alpaca returns no tx_hash AND the wallet
    /// balance is insufficient (USDC not yet settled), poll_and_confirm_withdrawal
    /// still advances the aggregate to WithdrawalComplete (guard latched), and
    /// the subsequent balance gate returns WalletUsdcInsufficient (retryable) --
    /// no burn is attempted.
    #[tokio::test]
    async fn withdrawal_tx_absent_and_insufficient_balance_returns_wallet_usdc_insufficient() {
        let market_maker_wallet = address!("0x2222222222222222222222222222222222222222");

        // Deploy USDC chain with ZERO balance -- withdrawal not yet settled.
        let chain = deploy_ethereum_usdc_chain_with_balance(U256::ZERO, market_maker_wallet).await;

        let server = MockServer::start();
        let (manager, cqrs) =
            build_manager_with_ethereum_chain(&chain, &server, market_maker_wallet).await;

        let transfer_uuid = Uuid::new_v4();
        // tx_hash is None -- Alpaca did not return one.
        let _transfer_mock = mock_complete_withdrawal_with_tx(&server, transfer_uuid, None);

        let id = UsdcRebalanceId(Uuid::new_v4());
        let amount = usdc("1");
        let withdrawal_id = AlpacaTransferId::from(transfer_uuid);

        // Stage at Withdrawing.
        cqrs.send(
            &id,
            UsdcRebalanceCommand::InitiateConversion {
                direction: RebalanceDirection::AlpacaToBase,
                amount,
                order_id: ClientOrderId::from_uuid(Uuid::new_v4()),
            },
        )
        .await
        .unwrap();
        cqrs.send(
            &id,
            UsdcRebalanceCommand::ConfirmConversion {
                conversion: par_conversion(amount),
            },
        )
        .await
        .unwrap();
        cqrs.send(
            &id,
            UsdcRebalanceCommand::Initiate {
                direction: RebalanceDirection::AlpacaToBase,
                amount,
                withdrawal: TransferRef::AlpacaId(withdrawal_id),
            },
        )
        .await
        .unwrap();

        // poll_and_confirm_withdrawal succeeds (no tx hash to check) and advances
        // the aggregate to WithdrawalComplete.
        manager
            .poll_and_confirm_withdrawal(&id, &withdrawal_id, Utc::now())
            .await
            .unwrap();

        let state = cqrs.load(&id).await.unwrap().expect("aggregate exists");
        assert!(
            matches!(
                state,
                UsdcRebalance::WithdrawalComplete {
                    direction: RebalanceDirection::AlpacaToBase,
                    ..
                }
            ),
            "Aggregate must be WithdrawalComplete after absent-tx poll; got: {state:?}"
        );

        // continue_alpaca_to_base_from_withdrawal_complete reads the wallet balance
        // and returns WalletUsdcInsufficient (retryable) because the wallet holds
        // zero USDC. No FailBridging is emitted; the aggregate stays in
        // WithdrawalComplete for the next delayed-redrive attempt.
        let error = manager
            .continue_alpaca_to_base_from_withdrawal_complete(&id, amount, None)
            .await
            .unwrap_err();

        assert!(
            matches!(error, UsdcTransferError::WalletUsdcInsufficient { .. }),
            "Expected WalletUsdcInsufficient when balance is zero; got: {error:?}"
        );

        // Aggregate must still be in WithdrawalComplete -- the balance gate is
        // staleness-safe (no FailBridging emitted on zero balance).
        let state = cqrs.load(&id).await.unwrap().expect("aggregate exists");
        assert!(
            matches!(
                state,
                UsdcRebalance::WithdrawalComplete {
                    direction: RebalanceDirection::AlpacaToBase,
                    ..
                }
            ),
            "Aggregate must remain WithdrawalComplete after zero balance; got: {state:?}"
        );
    }

    /// Hypothesis: on ANY poll error (ApiError, TransferTimeout, reqwest::Error),
    /// `poll_and_confirm_withdrawal` returns `WithdrawalPollInconclusive` WITHOUT
    /// emitting `FailWithdrawal`. The aggregate stays in `Withdrawing` with the
    /// guard held so no fresh re-withdrawal can be started on the next tick.
    #[tokio::test]
    async fn poll_and_confirm_withdrawal_on_poll_error_returns_inconclusive_without_failing() {
        let market_maker_wallet = address!("0x2222222222222222222222222222222222222222");
        let chain = deploy_ethereum_usdc_chain_with_balance(U256::ZERO, market_maker_wallet).await;

        let server = MockServer::start();
        let (manager, cqrs) =
            build_manager_with_ethereum_chain(&chain, &server, market_maker_wallet).await;
        let withdrawal_uuid = Uuid::new_v4();
        let withdrawal_id = AlpacaTransferId::from(withdrawal_uuid);

        // Mock the transfers endpoint to return a 400 Bad Request, triggering
        // AlpacaWalletError::ApiError -- an indeterminate poll error.
        let _error_mock = server.mock(|when, then| {
            when.method(httpmock::Method::GET)
                .path(format!(
                    "/v1/accounts/904837e3-3b76-47ec-b432-046db621571b/wallets/transfers/{withdrawal_uuid}"
                ));
            then.status(400)
                .header("content-type", "application/json")
                .json_body(serde_json::json!({"message": "bad request"}));
        });

        let id = UsdcRebalanceId(Uuid::new_v4());
        let amount = usdc("1");

        // Stage aggregate at Withdrawing{AlpacaToBase}.
        cqrs.send(
            &id,
            UsdcRebalanceCommand::InitiateConversion {
                direction: RebalanceDirection::AlpacaToBase,
                amount,
                order_id: ClientOrderId::from_uuid(Uuid::new_v4()),
            },
        )
        .await
        .unwrap();
        cqrs.send(
            &id,
            UsdcRebalanceCommand::ConfirmConversion {
                conversion: par_conversion(amount),
            },
        )
        .await
        .unwrap();
        cqrs.send(
            &id,
            UsdcRebalanceCommand::Initiate {
                direction: RebalanceDirection::AlpacaToBase,
                amount,
                withdrawal: TransferRef::AlpacaId(withdrawal_id),
            },
        )
        .await
        .unwrap();

        let error = manager
            .poll_and_confirm_withdrawal(&id, &withdrawal_id, Utc::now())
            .await
            .unwrap_err();

        // The error must be WithdrawalPollInconclusive, NOT AlpacaWallet or
        // WithdrawalFailed. The indeterminate path does not emit FailWithdrawal.
        assert!(
            matches!(error, UsdcTransferError::WithdrawalPollInconclusive { .. }),
            "Expected WithdrawalPollInconclusive on poll error, got: {error:?}"
        );

        // Aggregate must still be Withdrawing (guard held, no FailWithdrawal emitted).
        let state = cqrs.load(&id).await.unwrap().expect("aggregate exists");
        assert!(
            matches!(
                state,
                UsdcRebalance::Withdrawing {
                    direction: RebalanceDirection::AlpacaToBase,
                    ..
                }
            ),
            "Aggregate must stay in Withdrawing on inconclusive poll error; got: {state:?}"
        );
        assert!(
            state.holds_rebalance_guard(),
            "Guard must stay held when poll is inconclusive (Withdrawing state)"
        );
    }

    /// Hypothesis: a DETERMINATE poll failure (Alpaca returns
    /// TransferStatus::Failed with no tx hash) still sends FailWithdrawal and moves
    /// the aggregate to WithdrawalFailed (guard released).
    #[tokio::test]
    async fn poll_and_confirm_withdrawal_on_alpaca_transfer_failed_sends_fail_withdrawal() {
        let market_maker_wallet = address!("0x2222222222222222222222222222222222222222");
        let chain = deploy_ethereum_usdc_chain_with_balance(U256::ZERO, market_maker_wallet).await;

        let server = MockServer::start();
        let (manager, cqrs) =
            build_manager_with_ethereum_chain(&chain, &server, market_maker_wallet).await;

        let transfer_uuid = Uuid::new_v4();
        // Mock Alpaca to return a FAILED terminal status with no on-chain tx.
        let _failed_mock = server.mock(|when, then| {
            when.method(httpmock::Method::GET)
                .path(format!(
                    "/v1/accounts/904837e3-3b76-47ec-b432-046db621571b/wallets/transfers/{transfer_uuid}"
                ));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(serde_json::json!({
                    "id": transfer_uuid.to_string(),
                    "direction": "OUTGOING",
                    "amount": "100",
                    "usd_value": "100",
                    "chain": "ethereum",
                    "asset": "USDC",
                    "from_address": "0x0000000000000000000000000000000000000001",
                    "to_address": "0x2222222222222222222222222222222222222222",
                    "status": "FAILED",
                    "tx_hash": null,
                    "created_at": "2024-01-01T00:00:00Z",
                    "network_fee": "0",
                    "fees": "0"
                }));
        });

        let id = UsdcRebalanceId(Uuid::new_v4());
        let amount = usdc("1");
        let withdrawal_id = AlpacaTransferId::from(transfer_uuid);

        // Stage aggregate at Withdrawing{AlpacaToBase}.
        cqrs.send(
            &id,
            UsdcRebalanceCommand::InitiateConversion {
                direction: RebalanceDirection::AlpacaToBase,
                amount,
                order_id: ClientOrderId::from_uuid(Uuid::new_v4()),
            },
        )
        .await
        .unwrap();
        cqrs.send(
            &id,
            UsdcRebalanceCommand::ConfirmConversion {
                conversion: par_conversion(amount),
            },
        )
        .await
        .unwrap();
        cqrs.send(
            &id,
            UsdcRebalanceCommand::Initiate {
                direction: RebalanceDirection::AlpacaToBase,
                amount,
                withdrawal: TransferRef::AlpacaId(withdrawal_id),
            },
        )
        .await
        .unwrap();

        let error = manager
            .poll_and_confirm_withdrawal(&id, &withdrawal_id, Utc::now())
            .await
            .unwrap_err();

        // Determinate Alpaca FAILED -> WithdrawalFailed error returned.
        assert!(
            matches!(error, UsdcTransferError::WithdrawalFailed { .. }),
            "Expected WithdrawalFailed on determinate Alpaca Failed status, got: {error:?}"
        );

        // Aggregate must be in WithdrawalFailed (guard released).
        let state = cqrs.load(&id).await.unwrap().expect("aggregate exists");
        assert!(
            matches!(
                state,
                UsdcRebalance::WithdrawalFailed {
                    direction: RebalanceDirection::AlpacaToBase,
                    ..
                }
            ),
            "Aggregate must be in WithdrawalFailed after determinate Alpaca Failed; got: {state:?}"
        );
        assert!(
            !state.holds_rebalance_guard(),
            "Guard must be released after WithdrawalFailed"
        );
    }

    /// Hypothesis: a FAILED Alpaca withdrawal that includes an on-chain tx hash is
    /// not a determinate "funds never left" signal. The aggregate stays in
    /// Withdrawing with the guard held so the same transfer can be re-polled.
    #[tokio::test]
    async fn poll_and_confirm_withdrawal_on_failed_transfer_with_tx_returns_inconclusive() {
        let market_maker_wallet = address!("0x2222222222222222222222222222222222222222");
        let chain = deploy_ethereum_usdc_chain_with_balance(U256::ZERO, market_maker_wallet).await;

        let server = MockServer::start();
        let (manager, cqrs) =
            build_manager_with_ethereum_chain(&chain, &server, market_maker_wallet).await;

        let transfer_uuid = Uuid::new_v4();
        let tx_hash = b256!("0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef");
        let _failed_mock = server.mock(|when, then| {
            when.method(httpmock::Method::GET)
                .path(format!(
                    "/v1/accounts/904837e3-3b76-47ec-b432-046db621571b/wallets/transfers/{transfer_uuid}"
                ));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(serde_json::json!({
                    "id": transfer_uuid.to_string(),
                    "direction": "OUTGOING",
                    "amount": "100",
                    "usd_value": "100",
                    "chain": "ethereum",
                    "asset": "USDC",
                    "from_address": "0x0000000000000000000000000000000000000001",
                    "to_address": "0x2222222222222222222222222222222222222222",
                    "status": "FAILED",
                    "tx_hash": format!("{tx_hash:#x}"),
                    "created_at": "2024-01-01T00:00:00Z",
                    "network_fee": "0",
                    "fees": "0"
                }));
        });

        let id = UsdcRebalanceId(Uuid::new_v4());
        let amount = usdc("1");
        let withdrawal_id = AlpacaTransferId::from(transfer_uuid);

        cqrs.send(
            &id,
            UsdcRebalanceCommand::InitiateConversion {
                direction: RebalanceDirection::AlpacaToBase,
                amount,
                order_id: ClientOrderId::from_uuid(Uuid::new_v4()),
            },
        )
        .await
        .unwrap();
        cqrs.send(
            &id,
            UsdcRebalanceCommand::ConfirmConversion {
                conversion: par_conversion(amount),
            },
        )
        .await
        .unwrap();
        cqrs.send(
            &id,
            UsdcRebalanceCommand::Initiate {
                direction: RebalanceDirection::AlpacaToBase,
                amount,
                withdrawal: TransferRef::AlpacaId(withdrawal_id),
            },
        )
        .await
        .unwrap();

        let error = manager
            .poll_and_confirm_withdrawal(&id, &withdrawal_id, Utc::now())
            .await
            .unwrap_err();

        assert!(
            matches!(
                error,
                UsdcTransferError::WithdrawalPollInconclusive {
                    source: AlpacaWalletError::FailedTransferHasTx { tx_hash: observed, .. },
                    ..
                } if observed == tx_hash
            ),
            "FAILED with a tx hash must be treated as inconclusive, got: {error:?}"
        );

        let state = cqrs.load(&id).await.unwrap().expect("aggregate exists");
        assert!(
            matches!(
                state,
                UsdcRebalance::Withdrawing {
                    direction: RebalanceDirection::AlpacaToBase,
                    ..
                }
            ),
            "Aggregate must stay Withdrawing when FAILED carries a tx hash; got: {state:?}"
        );
        assert!(
            state.holds_rebalance_guard(),
            "Guard must stay held when FAILED carries a tx hash"
        );
    }

    /// Hypothesis: Alpaca returning 404 for the by-id transfer lookup
    /// (`TransferNotFound`) does NOT emit `FailWithdrawal`. The aggregate stays
    /// in `Withdrawing` with the guard held.
    ///
    /// This pins the contract that only `Ok(transfer)` with
    /// `TransferStatus::Failed` and no tx hash is a determinate terminal that
    /// yields `FailWithdrawal`. A missing UUID is intentionally fail-closed: the
    /// transfer may still be in progress and the UUID absence does not confirm
    /// funds never left. Recovery is operational (see docs/cli-ops.md).
    #[tokio::test]
    async fn poll_and_confirm_withdrawal_on_transfer_not_found_returns_inconclusive() {
        let market_maker_wallet = address!("0x2222222222222222222222222222222222222222");
        let chain = deploy_ethereum_usdc_chain_with_balance(U256::ZERO, market_maker_wallet).await;

        let server = MockServer::start();
        let (manager, cqrs) =
            build_manager_with_ethereum_chain(&chain, &server, market_maker_wallet).await;

        let transfer_uuid = Uuid::new_v4();
        let _not_found_mock = server.mock(|when, then| {
            when.method(httpmock::Method::GET)
                .path(format!(
                    "/v1/accounts/904837e3-3b76-47ec-b432-046db621571b/wallets/transfers/{transfer_uuid}"
                ));
            then.status(404)
                .header("content-type", "application/json")
                .json_body(serde_json::json!({"message": "transfer not found"}));
        });

        let id = UsdcRebalanceId(Uuid::new_v4());
        let amount = usdc("1");
        let withdrawal_id = AlpacaTransferId::from(transfer_uuid);

        // Stage aggregate at Withdrawing{AlpacaToBase}.
        cqrs.send(
            &id,
            UsdcRebalanceCommand::InitiateConversion {
                direction: RebalanceDirection::AlpacaToBase,
                amount,
                order_id: ClientOrderId::from_uuid(Uuid::new_v4()),
            },
        )
        .await
        .unwrap();
        cqrs.send(
            &id,
            UsdcRebalanceCommand::ConfirmConversion {
                conversion: par_conversion(amount),
            },
        )
        .await
        .unwrap();
        cqrs.send(
            &id,
            UsdcRebalanceCommand::Initiate {
                direction: RebalanceDirection::AlpacaToBase,
                amount,
                withdrawal: TransferRef::AlpacaId(withdrawal_id),
            },
        )
        .await
        .unwrap();

        let error = manager
            .poll_and_confirm_withdrawal(&id, &withdrawal_id, Utc::now())
            .await
            .unwrap_err();

        // TransferNotFound -> WithdrawalPollInconclusive, NOT WithdrawalFailed.
        // The guard stays held.
        assert!(
            matches!(error, UsdcTransferError::WithdrawalPollInconclusive { .. }),
            "TransferNotFound must map to WithdrawalPollInconclusive (not FailWithdrawal); got: {error:?}"
        );

        let state = cqrs.load(&id).await.unwrap().expect("aggregate exists");
        assert!(
            matches!(
                state,
                UsdcRebalance::Withdrawing {
                    direction: RebalanceDirection::AlpacaToBase,
                    ..
                }
            ),
            "Aggregate must stay Withdrawing on TransferNotFound (guard held); got: {state:?}"
        );
        assert!(
            state.holds_rebalance_guard(),
            "Guard must stay held on TransferNotFound -- UUID absence is not a determinate failure signal"
        );
    }

    /// Hypothesis: after a prior inconclusive poll (aggregate stays Withdrawing),
    /// a resumed call that finds Alpaca now reports Complete advances the aggregate
    /// to WithdrawalComplete. Re-polling the same AlpacaTransferId is idempotent
    /// and the withdraw is adopted.
    #[tokio::test]
    async fn poll_and_confirm_withdrawal_after_prior_inconclusive_adopts_complete() {
        let market_maker_wallet = address!("0x2222222222222222222222222222222222222222");
        let chain = deploy_ethereum_usdc_chain_with_balance(U256::ZERO, market_maker_wallet).await;

        let server = MockServer::start();
        let (manager, cqrs) =
            build_manager_with_ethereum_chain(&chain, &server, market_maker_wallet).await;

        let transfer_uuid = Uuid::new_v4();
        let withdrawal_id = AlpacaTransferId::from(transfer_uuid);

        let id = UsdcRebalanceId(Uuid::new_v4());
        let amount = usdc("1");

        // Stage aggregate at Withdrawing{AlpacaToBase}.
        cqrs.send(
            &id,
            UsdcRebalanceCommand::InitiateConversion {
                direction: RebalanceDirection::AlpacaToBase,
                amount,
                order_id: ClientOrderId::from_uuid(Uuid::new_v4()),
            },
        )
        .await
        .unwrap();
        cqrs.send(
            &id,
            UsdcRebalanceCommand::ConfirmConversion {
                conversion: par_conversion(amount),
            },
        )
        .await
        .unwrap();
        cqrs.send(
            &id,
            UsdcRebalanceCommand::Initiate {
                direction: RebalanceDirection::AlpacaToBase,
                amount,
                withdrawal: TransferRef::AlpacaId(withdrawal_id),
            },
        )
        .await
        .unwrap();

        // First call: poll returns a 400 error -> inconclusive, aggregate stays Withdrawing.
        let mut error_mock = server.mock(|when, then| {
            when.method(httpmock::Method::GET)
                .path(format!(
                    "/v1/accounts/904837e3-3b76-47ec-b432-046db621571b/wallets/transfers/{transfer_uuid}"
                ));
            then.status(400)
                .header("content-type", "application/json")
                .json_body(serde_json::json!({"message": "bad request"}));
        });

        let error = manager
            .poll_and_confirm_withdrawal(&id, &withdrawal_id, Utc::now())
            .await
            .unwrap_err();

        assert!(
            matches!(error, UsdcTransferError::WithdrawalPollInconclusive { .. }),
            "First call must return WithdrawalPollInconclusive, got: {error:?}"
        );
        error_mock.assert();
        // Explicitly delete the 400 mock before registering the 200 mock so the
        // second call is not matched by the still-registered 400 handler.
        error_mock.delete();

        // Second call (simulating the delayed-redrive re-poll): Alpaca now reports COMPLETE.
        // No tx hash so the aggregate goes to WithdrawalComplete without a confirmation check.
        let complete_mock = server.mock(|when, then| {
            when.method(httpmock::Method::GET)
                .path(format!(
                    "/v1/accounts/904837e3-3b76-47ec-b432-046db621571b/wallets/transfers/{transfer_uuid}"
                ));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(serde_json::json!({
                    "id": transfer_uuid.to_string(),
                    "direction": "OUTGOING",
                    "amount": "100",
                    "usd_value": "100",
                    "chain": "ethereum",
                    "asset": "USDC",
                    "from_address": "0x0000000000000000000000000000000000000001",
                    "to_address": "0x2222222222222222222222222222222222222222",
                    "status": "COMPLETE",
                    "tx_hash": null,
                    "created_at": "2024-01-01T00:00:00Z",
                    "network_fee": "0",
                    "fees": "0"
                }));
        });

        manager
            .poll_and_confirm_withdrawal(&id, &withdrawal_id, Utc::now())
            .await
            .unwrap();

        complete_mock.assert();

        // Aggregate must have advanced to WithdrawalComplete.
        let state = cqrs.load(&id).await.unwrap().expect("aggregate exists");
        assert!(
            matches!(
                state,
                UsdcRebalance::WithdrawalComplete {
                    direction: RebalanceDirection::AlpacaToBase,
                    ..
                }
            ),
            "Aggregate must advance to WithdrawalComplete after second (Complete) poll; got: {state:?}"
        );
    }

    /// Regression: `resume_alpaca_to_base` from `Withdrawing{AlpacaToBase}` must
    /// thread the aggregate's stored `initiated_at` into `WithdrawalPollInconclusive`,
    /// NOT a fresh `Utc::now()`. The durable 4-hour operator-alert deadline is derived
    /// from this field and must survive restarts without resetting. A regression that
    /// changed manager.rs line 719 from `initiated_at` (destructured from the aggregate)
    /// to `Utc::now()` would silently pass all other tests while breaking the guarantee.
    #[tokio::test]
    async fn resume_alpaca_to_base_from_withdrawing_threads_aggregate_initiated_at() {
        let market_maker_wallet = address!("0x2222222222222222222222222222222222222222");
        let chain = deploy_ethereum_usdc_chain_with_balance(U256::ZERO, market_maker_wallet).await;

        let server = MockServer::start();
        let (manager, cqrs) =
            build_manager_with_ethereum_chain(&chain, &server, market_maker_wallet).await;
        let withdrawal_uuid = Uuid::new_v4();
        let withdrawal_id = AlpacaTransferId::from(withdrawal_uuid);

        // Mock Alpaca's transfers endpoint to return a 400 -- triggers
        // WithdrawalPollInconclusive via the fail-closed indeterminate error path.
        let _error_mock = server.mock(|when, then| {
            when.method(httpmock::Method::GET)
                .path(format!(
                    "/v1/accounts/904837e3-3b76-47ec-b432-046db621571b/wallets/transfers/{withdrawal_uuid}"
                ));
            then.status(400)
                .header("content-type", "application/json")
                .json_body(serde_json::json!({"message": "bad request"}));
        });

        let id = UsdcRebalanceId(Uuid::new_v4());
        let amount = usdc("1");

        // Stage at Withdrawing{AlpacaToBase} via the real CQRS store.
        cqrs.send(
            &id,
            UsdcRebalanceCommand::InitiateConversion {
                direction: RebalanceDirection::AlpacaToBase,
                amount,
                order_id: ClientOrderId::from_uuid(Uuid::new_v4()),
            },
        )
        .await
        .unwrap();
        cqrs.send(
            &id,
            UsdcRebalanceCommand::ConfirmConversion {
                conversion: par_conversion(amount),
            },
        )
        .await
        .unwrap();
        cqrs.send(
            &id,
            UsdcRebalanceCommand::Initiate {
                direction: RebalanceDirection::AlpacaToBase,
                amount,
                withdrawal: TransferRef::AlpacaId(withdrawal_id),
            },
        )
        .await
        .unwrap();

        // Load the aggregate to read the stored initiated_at (withdrawal initiation time).
        let state = cqrs.load(&id).await.unwrap().expect("aggregate exists");
        let UsdcRebalance::Withdrawing {
            initiated_at: aggregate_initiated_at,
            ..
        } = state
        else {
            panic!("Expected Withdrawing state; got: {state:?}");
        };

        // Call resume_alpaca_to_base (the full path, not just poll_and_confirm_withdrawal).
        // This is the path that destructures initiated_at from the aggregate.
        let error = manager
            .resume_alpaca_to_base(&id, amount)
            .await
            .unwrap_err();

        let UsdcTransferError::WithdrawalPollInconclusive {
            initiated_at: error_initiated_at,
            ..
        } = error
        else {
            panic!("Expected WithdrawalPollInconclusive; got: {error:?}");
        };

        // The error's initiated_at must equal the aggregate's stored value.
        // If this path were changed to Utc::now(), the timestamps would differ
        // and this assertion would catch the regression.
        assert_eq!(
            error_initiated_at, aggregate_initiated_at,
            "resume_alpaca_to_base must thread the aggregate's stored initiated_at \
             into WithdrawalPollInconclusive, not a fresh Utc::now()"
        );
    }

    /// Hypothesis: `ConfirmWithdrawal` from a non-Withdrawing state (e.g.
    /// WithdrawalComplete) is rejected by the aggregate cleanly via a SendError.
    /// The guard stays held; no double-adoption or panic occurs.
    #[tokio::test]
    async fn confirm_withdrawal_rejected_from_non_withdrawing_state() {
        let market_maker_wallet = address!("0x2222222222222222222222222222222222222222");
        let chain = deploy_ethereum_usdc_chain_with_balance(U256::ZERO, market_maker_wallet).await;

        let server = MockServer::start();
        let (manager, cqrs) =
            build_manager_with_ethereum_chain(&chain, &server, market_maker_wallet).await;

        let id = UsdcRebalanceId(Uuid::new_v4());
        let amount = usdc("1");

        // Seed aggregate to WithdrawalComplete.
        advance_to_withdrawal_complete_alpaca_to_base(&cqrs, &id, amount).await;

        let transfer_uuid = Uuid::new_v4();
        let withdrawal_id = AlpacaTransferId::from(transfer_uuid);

        // Mock Alpaca to return COMPLETE (poll call will succeed, but ConfirmWithdrawal
        // command will be rejected by the aggregate).
        let _complete_mock = server.mock(|when, then| {
            when.method(httpmock::Method::GET)
                .path(format!(
                    "/v1/accounts/904837e3-3b76-47ec-b432-046db621571b/wallets/transfers/{transfer_uuid}"
                ));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(serde_json::json!({
                    "id": transfer_uuid.to_string(),
                    "direction": "OUTGOING",
                    "amount": "100",
                    "usd_value": "100",
                    "chain": "ethereum",
                    "asset": "USDC",
                    "from_address": "0x0000000000000000000000000000000000000001",
                    "to_address": "0x2222222222222222222222222222222222222222",
                    "status": "COMPLETE",
                    "tx_hash": null,
                    "created_at": "2024-01-01T00:00:00Z",
                    "network_fee": "0",
                    "fees": "0"
                }));
        });

        let result = manager
            .poll_and_confirm_withdrawal(&id, &withdrawal_id, Utc::now())
            .await;

        // ConfirmWithdrawal from WithdrawalComplete is rejected by the aggregate.
        // The error propagates as UsdcTransferError::Aggregate (no panic, no guard corruption).
        assert!(
            matches!(result, Err(UsdcTransferError::Aggregate(_))),
            "Expected Aggregate error when ConfirmWithdrawal is rejected from WithdrawalComplete, \
             got: {result:?}"
        );

        // Aggregate must still be in WithdrawalComplete (no corruption, guard held).
        let state = cqrs.load(&id).await.unwrap().expect("aggregate exists");
        assert!(
            matches!(
                state,
                UsdcRebalance::WithdrawalComplete {
                    direction: RebalanceDirection::AlpacaToBase,
                    ..
                }
            ),
            "Aggregate must remain WithdrawalComplete after rejected ConfirmWithdrawal; got: {state:?}"
        );
        assert!(
            state.holds_rebalance_guard(),
            "Guard must remain held after rejected ConfirmWithdrawal"
        );
    }

    /// Hypothesis: when the burn confirms a receipt but the receipt contains no
    /// MessageSent event (CctpError::MessageSentEventNotFound),
    /// `execute_cctp_burn_on_ethereum` leaves the aggregate at `BridgingSubmitting`
    /// (BeginBridging was persisted before the burn). On apalis redrive
    /// `resume_bridging_submitting_ethereum` scans Ethereum for the
    /// confirmed-but-event-missing burn tx (via `find_recent_burn`) and adopts it.
    #[tokio::test]
    async fn post_commit_burn_message_sent_not_found_lands_at_bridging_submitting() {
        let market_maker_wallet = address!("0x2222222222222222222222222222222222222222");

        // Mint enough USDC to pass the balance gate (1 USDC = 1_000_000 units).
        let required = U256::from(1_000_000u64);
        let chain = deploy_ethereum_usdc_chain_with_balance(required, market_maker_wallet).await;

        let server = MockServer::start();
        let (manager, cqrs) =
            build_manager_with_ethereum_chain(&chain, &server, market_maker_wallet).await;

        // Place a STOP-only contract at TOKEN_MESSENGER_V2: the call succeeds
        // (receipt.status = 1) with empty return data and no logs, triggering
        // CctpError::MessageSentEventNotFound after the receipt is confirmed.
        let stop_bytecode = alloy::primitives::Bytes::from(vec![0x00u8]);
        let provider = ProviderBuilder::new()
            .connect(&chain.endpoint)
            .await
            .unwrap();
        provider
            .anvil_set_code(st0x_bridge::cctp::TOKEN_MESSENGER_V2, stop_bytecode)
            .await
            .unwrap();

        let id = UsdcRebalanceId(Uuid::new_v4());
        let amount = usdc("1");

        advance_to_withdrawal_complete_alpaca_to_base(&cqrs, &id, amount).await;

        let error = manager
            .continue_alpaca_to_base_from_withdrawal_complete(&id, amount, None)
            .await
            .unwrap_err();

        assert!(
            matches!(error, UsdcTransferError::Cctp(_)),
            "Expected Cctp error from post-commit MessageSentEventNotFound; got: {error:?}"
        );

        // Aggregate must be in BridgingSubmitting: BeginBridging was persisted
        // before the burn, so on redrive the scan path handles the
        // confirmed-but-event-missing burn. The in-progress guard is latched
        // and the transfer is recoverable via the crash-safe scan.
        let state = cqrs.load(&id).await.unwrap().expect("aggregate exists");
        assert!(
            matches!(
                state,
                UsdcRebalance::BridgingSubmitting {
                    direction: RebalanceDirection::AlpacaToBase,
                    ..
                }
            ),
            "Aggregate must be at BridgingSubmitting after post-commit burn \
             MessageSentEventNotFound (crash-safe scan handles recovery); got: {state:?}"
        );
    }

    /// Hypothesis: resuming resume_alpaca_to_base from WithdrawalComplete with
    /// insufficient USDC balance returns a retryable WalletUsdcInsufficient error,
    /// the aggregate stays in WithdrawalComplete, and the guard stays latched.
    #[tokio::test]
    async fn resume_from_withdrawal_complete_with_insufficient_balance_returns_retryable_error() {
        let market_maker_wallet = address!("0x2222222222222222222222222222222222222222");

        // Zero USDC on Ethereum -- withdrawal has not settled.
        let chain = deploy_ethereum_usdc_chain_with_balance(U256::ZERO, market_maker_wallet).await;

        let server = MockServer::start();
        let (manager, cqrs) =
            build_manager_with_ethereum_chain(&chain, &server, market_maker_wallet).await;

        let id = UsdcRebalanceId(Uuid::new_v4());
        let amount = usdc("1");

        advance_to_withdrawal_complete_alpaca_to_base(&cqrs, &id, amount).await;

        let error = manager
            .resume_alpaca_to_base(&id, amount)
            .await
            .unwrap_err();

        assert!(
            matches!(error, UsdcTransferError::WalletUsdcInsufficient { .. }),
            "Expected WalletUsdcInsufficient on resume from WithdrawalComplete with \
             no settled balance; got: {error:?}"
        );

        // Guard: aggregate stays in WithdrawalComplete.
        let state = cqrs.load(&id).await.unwrap().expect("aggregate exists");
        assert!(
            matches!(
                state,
                UsdcRebalance::WithdrawalComplete {
                    direction: RebalanceDirection::AlpacaToBase,
                    ..
                }
            ),
            "Aggregate must remain in WithdrawalComplete; got: {state:?}"
        );
    }

    // -------------------------------------------------------------------------
    // Crash-safe AlpacaToBase burn via BridgingSubmitting
    // -------------------------------------------------------------------------

    /// Advances the aggregate to `BridgingSubmitting` (AlpacaToBase) with the
    /// given `from_block`. Pre-stages the BridgingSubmitting state for tests.
    async fn advance_to_bridging_submitting_alpaca_to_base(
        cqrs: &Store<UsdcRebalance>,
        id: &UsdcRebalanceId,
        amount: Usdc,
        from_block: u64,
    ) {
        advance_to_withdrawal_complete_alpaca_to_base(cqrs, id, amount).await;
        cqrs.send(
            id,
            UsdcRebalanceCommand::BeginBridging {
                from_block,
                burn_amount: None,
            },
        )
        .await
        .unwrap();
    }

    /// Advances the aggregate to `BridgingSubmitting` (AlpacaToBase) with an
    /// explicit `burn_amount`. Used to test the crash-resume path where the
    /// persisted burn amount is the fee-reduced received amount (not nominal).
    async fn advance_to_bridging_submitting_alpaca_to_base_with_burn_amount(
        cqrs: &Store<UsdcRebalance>,
        id: &UsdcRebalanceId,
        amount: Usdc,
        from_block: u64,
        burn_amount: Usdc,
    ) {
        advance_to_withdrawal_complete_alpaca_to_base(cqrs, id, amount).await;
        cqrs.send(
            id,
            UsdcRebalanceCommand::BeginBridging {
                from_block,
                burn_amount: Some(burn_amount),
            },
        )
        .await
        .unwrap();
    }

    /// Advances the aggregate to `BridgingSubmitting { direction: BaseToAlpaca }`.
    /// Used by the resume direction-mismatch test.
    async fn advance_to_bridging_submitting_base_to_alpaca(
        cqrs: &Store<UsdcRebalance>,
        id: &UsdcRebalanceId,
        amount: Usdc,
        from_block: u64,
    ) {
        let burn_tx =
            fixed_bytes!("0xbbbb000000000000000000000000000000000000000000000000000000000001");
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
        cqrs.send(
            id,
            UsdcRebalanceCommand::ConfirmWithdrawal {
                withdrawal_tx: None,
            },
        )
        .await
        .unwrap();
        cqrs.send(
            id,
            UsdcRebalanceCommand::BeginBridging {
                from_block,
                burn_amount: None,
            },
        )
        .await
        .unwrap();
    }

    /// `resume_alpaca_to_base` from `BridgingSubmitting { direction: BaseToAlpaca }`
    /// must return `ResumeDirectionMismatch` (not enter the Ethereum scan path).
    #[tokio::test]
    async fn resume_alpaca_to_base_from_bridging_submitting_base_to_alpaca_returns_direction_mismatch()
     {
        let server = MockServer::start();
        let (manager, cqrs, _anvil) = make_resume_test_manager(&server).await;

        let id = UsdcRebalanceId(Uuid::new_v4());
        let amount = usdc("1");

        advance_to_bridging_submitting_base_to_alpaca(&cqrs, &id, amount, 0).await;

        // Verify the pre-condition: aggregate is at BridgingSubmitting{BaseToAlpaca}.
        let state = cqrs.load(&id).await.unwrap().expect("aggregate exists");
        assert!(
            matches!(
                state,
                UsdcRebalance::BridgingSubmitting {
                    direction: RebalanceDirection::BaseToAlpaca,
                    ..
                }
            ),
            "Pre-condition: aggregate must be at BridgingSubmitting{{BaseToAlpaca}}; got: {state:?}"
        );

        // ResumeDirectionMismatch fires before any chain access, so no live chain is
        // needed -- the mismatch is detected purely from the aggregate state.
        let error = manager
            .resume_alpaca_to_base(&id, amount)
            .await
            .unwrap_err();

        assert!(
            matches!(
                error,
                UsdcTransferError::ResumeDirectionMismatch {
                    direction: RebalanceDirection::BaseToAlpaca,
                    ..
                }
            ),
            "BridgingSubmitting{{BaseToAlpaca}} must yield ResumeDirectionMismatch, \
             got: {error:?}"
        );
    }

    /// `resume_bridging_submitting` (Base->Alpaca, burning via BaseToEthereum)
    /// returns `SettlementCheckTransient` -- not `Cctp` -- when the burn scan is
    /// inconclusive (chain head not yet `SCAN_FINALITY_MARGIN` blocks past
    /// `from_block`). Mirrors the Ethereum counterpart so an inconclusive scan
    /// delayed-redrives instead of tripping the terminal circuit. The aggregate
    /// stays at `BridgingSubmitting`.
    #[tokio::test]
    async fn resume_bridging_submitting_base_scan_inconclusive_returns_settlement_transient() {
        let server = MockServer::start();
        let (manager, cqrs, anvil) = make_resume_test_manager(&server).await;
        let id = UsdcRebalanceId(Uuid::new_v4());
        let amount = usdc("1");
        let amount_u256 = usdc_to_u256(amount).unwrap();
        let provider = ProviderBuilder::new().connect_http(anvil.endpoint().parse().unwrap());
        let from_block = provider.get_block_number().await.unwrap();
        advance_to_bridging_submitting_base_to_alpaca(&cqrs, &id, amount, from_block).await;
        let error = manager
            .resume_bridging_submitting(&id, amount_u256, from_block, None)
            .await
            .unwrap_err();
        assert!(
            matches!(error, UsdcTransferError::SettlementCheckTransient { .. }),
            "an inconclusive Base burn scan must yield SettlementCheckTransient (delayed \
             redrive), not Cctp (terminal); got: {error:?}"
        );
        let state = cqrs.load(&id).await.unwrap().expect("aggregate exists");
        assert!(
            matches!(
                state,
                UsdcRebalance::BridgingSubmitting {
                    direction: RebalanceDirection::BaseToAlpaca,
                    ..
                }
            ),
            "Aggregate must stay at BridgingSubmitting after an inconclusive scan; got: {state:?}"
        );
    }

    /// `execute_cctp_burn_on_ethereum` emits `BeginBridging`
    /// BEFORE the burn and leaves the aggregate at `BridgingSubmitting` when the
    /// burn fails (STOP-bytecode path: tx mines but MessageSent event is absent).
    ///
    /// Covers two invariants in one test:
    ///   - BeginBridging is persisted before the burn is attempted (durable
    ///     ordering that makes the crash-safe scan possible).
    ///   - The aggregate ends at BridgingSubmitting (not WithdrawalComplete),
    ///     so a subsequent apalis redrive enters resume_bridging_submitting_ethereum
    ///     (scan path) rather than re-burning.
    #[tokio::test]
    async fn execute_cctp_burn_on_ethereum_records_begin_bridging_before_burn_and_leaves_bridging_submitting()
     {
        let market_maker_wallet = address!("0x2222222222222222222222222222222222222222");

        let required = U256::from(1_000_000u64);
        let chain = deploy_ethereum_usdc_chain_with_balance(required, market_maker_wallet).await;

        let server = MockServer::start();
        let (manager, cqrs) =
            build_manager_with_ethereum_chain(&chain, &server, market_maker_wallet).await;

        // STOP-only contract: burn tx mines and gets a receipt (status = 1) but
        // no MessageSent event -- simulates the post-commit error window.
        let stop_bytecode = alloy::primitives::Bytes::from(vec![0x00u8]);
        let provider = ProviderBuilder::new()
            .connect(&chain.endpoint)
            .await
            .unwrap();
        provider
            .anvil_set_code(st0x_bridge::cctp::TOKEN_MESSENGER_V2, stop_bytecode)
            .await
            .unwrap();

        let id = UsdcRebalanceId(Uuid::new_v4());
        let amount = usdc("1");
        let amount_u256 = usdc_to_u256(amount).unwrap();

        advance_to_withdrawal_complete_alpaca_to_base(&cqrs, &id, amount).await;

        let error = manager
            .execute_cctp_burn_on_ethereum(&id, amount_u256, None)
            .await
            .unwrap_err();

        assert!(
            matches!(error, UsdcTransferError::Cctp(_)),
            "Expected Cctp error (post-commit burn), got: {error:?}"
        );

        // Key invariant: aggregate is at BridgingSubmitting (BeginBridging persisted
        // before the burn), so redrive enters the scan path not the re-burn path.
        let state = cqrs.load(&id).await.unwrap().expect("aggregate exists");
        assert!(
            matches!(
                state,
                UsdcRebalance::BridgingSubmitting {
                    direction: RebalanceDirection::AlpacaToBase,
                    ..
                }
            ),
            "Aggregate must be at BridgingSubmitting so redrive scans instead of re-burning; \
             got: {state:?}"
        );
    }

    /// `resume_bridging_submitting_ethereum` with a scan error (chain
    /// unreachable) returns `UsdcTransferError::Cctp` and does NOT burn.
    /// The aggregate stays at `BridgingSubmitting`.
    #[tokio::test]
    async fn resume_bridging_submitting_ethereum_scan_error_returns_cctp_no_burn() {
        let market_maker_wallet = address!("0x2222222222222222222222222222222222222222");

        let chain =
            deploy_ethereum_usdc_chain_with_balance(U256::from(1_000_000u64), market_maker_wallet)
                .await;

        let server = MockServer::start();
        let (manager, cqrs) =
            build_manager_with_ethereum_chain(&chain, &server, market_maker_wallet).await;

        let id = UsdcRebalanceId(Uuid::new_v4());
        let amount = usdc("1");
        let amount_u256 = usdc_to_u256(amount).unwrap();

        advance_to_bridging_submitting_alpaca_to_base(&cqrs, &id, amount, 0).await;

        // Drop the chain so the scan RPC call fails.
        drop(chain);

        let error = manager
            .resume_bridging_submitting_ethereum(&id, amount_u256, 0, None)
            .await
            .unwrap_err();

        assert!(
            matches!(error, UsdcTransferError::Cctp(_)),
            "Scan error must surface as Cctp (retryable), got: {error:?}"
        );

        // Aggregate must remain at BridgingSubmitting -- no burn, no InitiateBridging.
        let state = cqrs.load(&id).await.unwrap().expect("aggregate exists");
        assert!(
            matches!(
                state,
                UsdcRebalance::BridgingSubmitting {
                    direction: RebalanceDirection::AlpacaToBase,
                    ..
                }
            ),
            "Aggregate must stay at BridgingSubmitting after scan error; got: {state:?}"
        );
    }

    /// `resume_alpaca_to_base` routes a `BridgingSubmitting { direction:
    /// AlpacaToBase }` aggregate to `resume_bridging_submitting_ethereum`
    /// rather than `ResumeDirectionMismatch`.
    ///
    /// Drops the chain immediately after staging so any burn/scan attempt
    /// fails with a Cctp error. The test asserts we get Cctp (entered the
    /// resume path) rather than ResumeDirectionMismatch (wrong path).
    #[tokio::test]
    async fn resume_alpaca_to_base_from_bridging_submitting_routes_to_ethereum_scan() {
        let market_maker_wallet = address!("0x2222222222222222222222222222222222222222");

        let chain =
            deploy_ethereum_usdc_chain_with_balance(U256::from(1_000_000u64), market_maker_wallet)
                .await;

        let server = MockServer::start();
        let (manager, cqrs) =
            build_manager_with_ethereum_chain(&chain, &server, market_maker_wallet).await;

        let id = UsdcRebalanceId(Uuid::new_v4());
        let amount = usdc("1");

        advance_to_bridging_submitting_alpaca_to_base(&cqrs, &id, amount, 0).await;

        // Drop the chain so the scan fails with Cctp (proves entry into the scan
        // path, not ResumeDirectionMismatch which would not touch the chain).
        drop(chain);

        let error = manager
            .resume_alpaca_to_base(&id, amount)
            .await
            .unwrap_err();

        assert!(
            matches!(error, UsdcTransferError::Cctp(_)),
            "BridgingSubmitting AlpacaToBase must route to scan path (Cctp error on \
             chain drop), not ResumeDirectionMismatch; got: {error:?}"
        );
    }

    /// `continue_alpaca_to_base_from_withdrawal_complete` with an
    /// under-confirmed `withdrawal_tx` returns `WithdrawalTxUnderconfirmed`
    /// (retryable) and does NOT attempt any burn. The aggregate stays at
    /// `WithdrawalComplete`.
    #[tokio::test]
    async fn continue_from_withdrawal_complete_under_confirmed_tx_returns_retryable() {
        let market_maker_wallet = address!("0x2222222222222222222222222222222222222222");

        // Zero USDC -- if the burn were attempted the balance gate would fail,
        // but the confirmation check must fire FIRST, before the balance gate.
        let chain = deploy_ethereum_usdc_chain_with_balance(U256::ZERO, market_maker_wallet).await;

        // The mint_tx was mined in the most-recent block (0 extra blocks = 1
        // confirmation, the inclusion block, at the chain head).
        let under_confirmed_tx = chain.mint_tx;

        let server = MockServer::start();
        let (manager, cqrs) =
            build_manager_with_ethereum_chain(&chain, &server, market_maker_wallet).await;

        let id = UsdcRebalanceId(Uuid::new_v4());
        let amount = usdc("1");

        advance_to_withdrawal_complete_alpaca_to_base(&cqrs, &id, amount).await;

        let error = manager
            .continue_alpaca_to_base_from_withdrawal_complete(&id, amount, Some(under_confirmed_tx))
            .await
            .unwrap_err();

        assert!(
            matches!(
                error,
                UsdcTransferError::WithdrawalTxUnderconfirmed {
                    actual: 1,
                    required: 3,
                    ..
                }
            ),
            "Expected WithdrawalTxUnderconfirmed (1 confirmation), got: {error:?}"
        );

        // Aggregate must remain in WithdrawalComplete -- no burn was attempted.
        let state = cqrs.load(&id).await.unwrap().expect("aggregate exists");
        assert!(
            matches!(
                state,
                UsdcRebalance::WithdrawalComplete {
                    direction: RebalanceDirection::AlpacaToBase,
                    ..
                }
            ),
            "Aggregate must stay at WithdrawalComplete after under-confirmed tx; \
             got: {state:?}"
        );
    }

    /// `continue_alpaca_to_base_from_withdrawal_complete` with
    /// `withdrawal_tx: None` skips the confirmation gate and proceeds to the
    /// balance gate (returns WalletUsdcInsufficient if balance is zero).
    #[tokio::test]
    async fn continue_from_withdrawal_complete_none_tx_skips_confirmation_gate() {
        let market_maker_wallet = address!("0x2222222222222222222222222222222222222222");

        // Zero USDC ensures the balance gate fires rather than the burn,
        // confirming that we got PAST the confirmation gate (which would have
        // fired first if withdrawal_tx were Some).
        let chain = deploy_ethereum_usdc_chain_with_balance(U256::ZERO, market_maker_wallet).await;

        let server = MockServer::start();
        let (manager, cqrs) =
            build_manager_with_ethereum_chain(&chain, &server, market_maker_wallet).await;

        let id = UsdcRebalanceId(Uuid::new_v4());
        let amount = usdc("1");

        advance_to_withdrawal_complete_alpaca_to_base(&cqrs, &id, amount).await;

        let error = manager
            .continue_alpaca_to_base_from_withdrawal_complete(&id, amount, None)
            .await
            .unwrap_err();

        // WalletUsdcInsufficient proves we passed the (skipped) confirmation gate.
        assert!(
            matches!(error, UsdcTransferError::WalletUsdcInsufficient { .. }),
            "Expected WalletUsdcInsufficient (balance gate fired after skipped \
             confirmation gate), got: {error:?}"
        );
    }

    /// `resume_alpaca_to_base` from `WithdrawalComplete { withdrawal_tx:
    /// Some(tx), .. }` with an under-confirmed tx returns
    /// `WithdrawalTxUnderconfirmed` without attempting any burn. The durable
    /// re-check fires even on the redrive path (aggregate already at
    /// `WithdrawalComplete`).
    #[tokio::test]
    async fn resume_from_withdrawal_complete_under_confirmed_tx_returns_retryable() {
        let market_maker_wallet = address!("0x2222222222222222222222222222222222222222");

        // Zero USDC: if the burn were reached the balance gate would fire, but
        // confirmation check must fire first.
        let chain = deploy_ethereum_usdc_chain_with_balance(U256::ZERO, market_maker_wallet).await;

        // mint_tx has 0 extra blocks mined = 1 confirmation (inclusion block) at head.
        let under_confirmed_tx = chain.mint_tx;

        let server = MockServer::start();
        let (manager, cqrs) =
            build_manager_with_ethereum_chain(&chain, &server, market_maker_wallet).await;

        let id = UsdcRebalanceId(Uuid::new_v4());
        let amount = usdc("1");

        // Stage with withdrawal_tx set to the under-confirmed tx, mirroring what
        // poll_and_confirm_withdrawal would have persisted on the first attempt.
        use UsdcRebalanceCommand::*;
        cqrs.send(
            &id,
            InitiateConversion {
                direction: RebalanceDirection::AlpacaToBase,
                amount,
                order_id: ClientOrderId::from_uuid(Uuid::new_v4()),
            },
        )
        .await
        .unwrap();
        cqrs.send(
            &id,
            ConfirmConversion {
                conversion: par_conversion(amount),
            },
        )
        .await
        .unwrap();
        cqrs.send(
            &id,
            Initiate {
                direction: RebalanceDirection::AlpacaToBase,
                amount,
                withdrawal: TransferRef::AlpacaId(AlpacaTransferId::from(Uuid::new_v4())),
            },
        )
        .await
        .unwrap();
        cqrs.send(
            &id,
            ConfirmWithdrawal {
                withdrawal_tx: Some(under_confirmed_tx),
            },
        )
        .await
        .unwrap();

        // Redrive: the primary gate does NOT re-run (aggregate is at
        // WithdrawalComplete, not Withdrawing), but the durable re-check in
        // continue_alpaca_to_base_from_withdrawal_complete fires.
        let error = manager
            .resume_alpaca_to_base(&id, amount)
            .await
            .unwrap_err();

        assert!(
            matches!(
                error,
                UsdcTransferError::WithdrawalTxUnderconfirmed {
                    actual: 1,
                    required: 3,
                    ..
                }
            ),
            "Expected WithdrawalTxUnderconfirmed on redrive from WithdrawalComplete \
             with under-confirmed tx, got: {error:?}"
        );

        // Aggregate must stay at WithdrawalComplete (no burn initiated).
        let state = cqrs.load(&id).await.unwrap().expect("aggregate exists");
        assert!(
            matches!(
                state,
                UsdcRebalance::WithdrawalComplete {
                    direction: RebalanceDirection::AlpacaToBase,
                    ..
                }
            ),
            "Aggregate must stay at WithdrawalComplete on under-confirmed redrive; \
             got: {state:?}"
        );
    }

    /// When `withdrawal_tx` is `Some`, `continue_alpaca_to_base_from_withdrawal_complete`
    /// derives `from_block` from that tx's block (not the raw chain head) and threads
    /// it into `BeginBridging`. The aggregate at `BridgingSubmitting` carries that
    /// `from_block`, so `resume_bridging_submitting_ethereum` scans from the
    /// withdrawal-tx block rather than a potentially stale head.
    #[tokio::test]
    async fn withdrawal_complete_with_tx_uses_tx_block_as_burn_scan_lower_bound() {
        let market_maker_wallet = address!("0x2222222222222222222222222222222222222222");

        // Mint USDC so the balance gate passes.
        let required = U256::from(1_000_000u64);
        let chain = deploy_ethereum_usdc_chain_with_balance(required, market_maker_wallet).await;

        // The mint_tx is our stand-in for the Alpaca withdrawal tx. Mine enough
        // extra blocks so `ethereum_tx_confirmations` returns >= required_confirmations
        // (the test_settlement_params uses 3 required).
        let provider = ProviderBuilder::new()
            .connect(&chain.endpoint)
            .await
            .unwrap();
        provider.anvil_mine(Some(5), None).await.unwrap();

        // Record the chain head AFTER mining extra blocks. The burn-scan lower bound
        // must equal the withdrawal-tx block (which is BEFORE the current head), not
        // the current head.
        let chain_head_after_mining: u64 = provider.get_block_number().await.unwrap();

        // Look up the block the withdrawal-tx was mined in.
        let withdrawal_tx = chain.mint_tx;

        let server = MockServer::start();
        let (manager, cqrs) =
            build_manager_with_ethereum_chain(&chain, &server, market_maker_wallet).await;

        // STOP-only contract: the burn tx mines (status = 1) but no MessageSent
        // event, triggering a Cctp error. This lets us observe the from_block in
        // BridgingSubmitting without completing the burn successfully.
        let stop_bytecode = alloy::primitives::Bytes::from(vec![0x00u8]);
        provider
            .anvil_set_code(st0x_bridge::cctp::TOKEN_MESSENGER_V2, stop_bytecode)
            .await
            .unwrap();

        let id = UsdcRebalanceId(Uuid::new_v4());
        let amount = usdc("1");

        advance_to_withdrawal_complete_alpaca_to_base(&cqrs, &id, amount).await;

        let error = manager
            .continue_alpaca_to_base_from_withdrawal_complete(&id, amount, Some(withdrawal_tx))
            .await
            .unwrap_err();

        assert!(
            matches!(error, UsdcTransferError::Cctp(_)),
            "Expected Cctp error from STOP-bytecode burn; got: {error:?}"
        );

        // The aggregate must be at BridgingSubmitting with from_block derived from
        // the withdrawal tx, not from the raw chain head.
        let state = cqrs.load(&id).await.unwrap().expect("aggregate exists");
        let UsdcRebalance::BridgingSubmitting { from_block, .. } = state else {
            panic!("Expected BridgingSubmitting, got: {state:?}");
        };

        // from_block must equal the withdrawal tx block exactly, proving the scan
        // bound is derived from the tx rather than the raw chain head.
        let expected_from_block = manager
            .cctp_bridge
            .ethereum_tx_block(withdrawal_tx)
            .await
            .unwrap();

        assert_eq!(
            from_block, expected_from_block,
            "from_block must equal the withdrawal tx block, not a stale chain head"
        );
        assert!(
            from_block < chain_head_after_mining,
            "the fixture must keep the withdrawal tx block before the current head"
        );
    }

    /// `resume_bridging_submitting_ethereum` adopts an already-submitted burn
    /// (scan returns Some) without re-burning, then advances to `Bridging`. The
    /// nonce-increment check proves no second burn was submitted.
    ///
    /// Full dual-chain CCTP is needed to call `find_recent_burn` for real.
    #[cfg(feature = "test-support")]
    #[tokio::test]
    async fn resume_bridging_submitting_ethereum_adopts_existing_burn() {
        let chains = deploy_dual_chain_cctp().await;
        let amount = usdc("100");
        let amount_u256 = usdc_to_u256(amount).unwrap();

        let cctp_bridge = Arc::new(
            CctpBridge::try_from_ctx(CctpCtx {
                usdc_ethereum: USDC_ADDRESS,
                usdc_base: USDC_ADDRESS,
                ethereum_wallet: create_test_wallet(&chains.ethereum_endpoint, &chains.bot_key),
                base_wallet: create_test_wallet(&chains.base_endpoint, &chains.bot_key),
                circle_api_base: st0x_bridge::cctp::CIRCLE_API_BASE.to_string(),
                token_messenger: chains.token_messenger,
                message_transmitter: chains.message_transmitter,
            })
            .unwrap(),
        );

        // Burn on Ethereum BEFORE calling resume, so the scan can find it.
        // Fund the Ethereum bot wallet with USDC first (deploy_dual_chain_cctp
        // funds Base, not Ethereum).
        let deployer_key = B256::from_slice(chains.bot_key.as_slice());
        mint_usdc(
            &chains.ethereum_endpoint,
            &deployer_key,
            USDC_ADDRESS,
            chains.bot_address,
            amount_u256 * U256::from(10u64),
        )
        .await
        .unwrap();

        let from_block = cctp_bridge
            .source_block(BridgeDirection::EthereumToBase)
            .await
            .unwrap();

        let existing_burn = cctp_bridge
            .burn(
                BridgeDirection::EthereumToBase,
                amount_u256,
                chains.bot_address,
            )
            .await
            .unwrap();

        // Build the manager.
        let server = MockServer::start();
        let alpaca_broker = InstrumentedAlpacaBroker::new(
            create_test_broker_service(&server).await,
            TelemetrySender::disabled(),
        );
        let alpaca_wallet = Arc::new(create_test_wallet_service(&server));
        let vault_service = RaindexService::new(
            create_test_wallet(&chains.base_endpoint, &chains.bot_key),
            RaindexContracts {
                inventory: ORDERBOOK_ADDRESS,
                orderbook: ORDERBOOK_ADDRESS,
            },
            chains.bot_address,
        );
        let cqrs = create_test_store_instance().await;
        let manager = CrossVenueCashTransfer::new(
            alpaca_broker,
            alpaca_wallet,
            Arc::clone(&cctp_bridge),
            Arc::new(vault_service),
            cqrs.clone(),
            chains.bot_address,
            TEST_VAULT_ID,
            &test_settlement_params(),
        );

        let id = UsdcRebalanceId(Uuid::new_v4());
        advance_to_bridging_submitting_alpaca_to_base(&cqrs, &id, amount, from_block).await;

        // Capture the nonce before calling resume -- adopt must not add a tx.
        let ethereum_provider = ProviderBuilder::new()
            .connect(&chains.ethereum_endpoint)
            .await
            .unwrap();
        let nonce_before = ethereum_provider
            .get_transaction_count(chains.bot_address)
            .await
            .unwrap();

        let burn_receipt = manager
            .resume_bridging_submitting_ethereum(&id, amount_u256, from_block, None)
            .await
            .unwrap();

        // Adopted burn: same tx, no new transaction.
        assert_eq!(
            burn_receipt.tx, existing_burn.tx,
            "Adopted burn must return the existing burn tx hash"
        );
        assert_eq!(
            ethereum_provider
                .get_transaction_count(chains.bot_address)
                .await
                .unwrap(),
            nonce_before,
            "Adopting the existing burn must NOT submit a second burn (nonce unchanged)"
        );

        // Aggregate must be at Bridging (InitiateBridging emitted by record_cctp_burn).
        let state = cqrs.load(&id).await.unwrap().expect("aggregate exists");
        assert!(
            matches!(
                state,
                UsdcRebalance::Bridging {
                    direction: RebalanceDirection::AlpacaToBase,
                    burn_tx_hash,
                    ..
                } if burn_tx_hash == existing_burn.tx
            ),
            "Aggregate must reach Bridging with the adopted burn tx; got: {state:?}"
        );
    }

    /// `burn_recording_pending` persists `RecordPendingBurn` BEFORE awaiting the
    /// burn receipt (and thus before `InitiateBridging`). Uses a STOP-only
    /// TokenMessenger so the burn broadcasts and is recorded, then `confirm_burn`
    /// fails with `MessageSentEventNotFound`: the aggregate must be left at
    /// `BridgingSubmitting` with `pending_burn_tx` set, proving the hash was
    /// durably recorded before the (failed) confirm -- never advancing to
    /// `Bridging`.
    #[tokio::test]
    async fn burn_recording_pending_records_pending_burn_before_confirm() {
        let market_maker_wallet = address!("0x2222222222222222222222222222222222222222");

        let required = U256::from(1_000_000u64);
        let chain = deploy_ethereum_usdc_chain_with_balance(required, market_maker_wallet).await;

        let server = MockServer::start();
        let (manager, cqrs) =
            build_manager_with_ethereum_chain(&chain, &server, market_maker_wallet).await;

        // STOP-only TokenMessenger: the burn tx mines (status = 1) with no
        // MessageSent event, so submit_burn broadcasts and RecordPendingBurn fires,
        // then confirm_burn returns MessageSentEventNotFound.
        let stop_bytecode = alloy::primitives::Bytes::from(vec![0x00u8]);
        let provider = ProviderBuilder::new()
            .connect(&chain.endpoint)
            .await
            .unwrap();
        provider
            .anvil_set_code(st0x_bridge::cctp::TOKEN_MESSENGER_V2, stop_bytecode)
            .await
            .unwrap();

        let id = UsdcRebalanceId(Uuid::new_v4());
        let amount = usdc("1");
        let amount_u256 = usdc_to_u256(amount).unwrap();

        advance_to_withdrawal_complete_alpaca_to_base(&cqrs, &id, amount).await;

        let error = manager
            .execute_cctp_burn_on_ethereum(&id, amount_u256, None)
            .await
            .unwrap_err();

        assert!(
            matches!(error, UsdcTransferError::Cctp(_)),
            "STOP-contract confirm must fail with Cctp (MessageSentEventNotFound); got: {error:?}"
        );

        // The aggregate must be at BridgingSubmitting with pending_burn_tx set:
        // RecordPendingBurn was committed before the confirm, and InitiateBridging
        // (which would advance to Bridging) never ran.
        let state = cqrs.load(&id).await.unwrap().expect("aggregate exists");
        let UsdcRebalance::BridgingSubmitting {
            direction,
            pending_burn_tx,
            ..
        } = state
        else {
            panic!("expected BridgingSubmitting, got: {state:?}");
        };
        assert_eq!(direction, RebalanceDirection::AlpacaToBase);
        let Some(recorded_tx) = pending_burn_tx else {
            panic!(
                "RecordPendingBurn must be persisted before the (failed) confirm, \
                 so pending_burn_tx is Some"
            );
        };
        assert_ne!(
            recorded_tx,
            TxHash::ZERO,
            "the recorded pending burn tx must be the real broadcast hash"
        );
    }

    /// `burn_recording_pending` retries the burn when the first `confirm_burn`
    /// returns a revert: the second `submit_burn` broadcasts a new hash and the
    /// second `confirm_burn` succeeds. Verified via `MockBridge` so the test
    /// runs without an Anvil instance or a real CCTP contract.
    ///
    /// Asserts:
    /// 1. Returns `Ok` whose `receipt.tx` equals the SECOND submitted hash.
    /// 2. The aggregate stays at `BridgingSubmitting` with `pending_burn_tx`
    ///    set to the second hash.
    /// 3. Exactly 2 `PendingBurnRecorded` events exist in the event store
    ///    and the last one carries the second hash.
    #[tokio::test]
    async fn burn_recording_pending_retries_and_succeeds_on_second_attempt() {
        let pool = SqlitePool::connect(":memory:").await.unwrap();
        sqlx::migrate!().run(&pool).await.unwrap();
        let cqrs = Arc::new(test_store(pool.clone()));

        let id = UsdcRebalanceId(Uuid::new_v4());
        let amount = usdc("1");
        let amount_u256 = usdc_to_u256(amount).unwrap();
        let recipient = address!("0x2222222222222222222222222222222222222222");

        advance_to_bridging_submitting_alpaca_to_base(&cqrs, &id, amount, 0).await;

        let server = MockServer::start();
        let alpaca_broker = InstrumentedAlpacaBroker::new(
            create_test_broker_service(&server).await,
            TelemetrySender::disabled(),
        );
        let alpaca_wallet = Arc::new(create_test_wallet_service(&server));

        // A real wallet is required for the Chain type parameter even though
        // burn_recording_pending never calls into RaindexService.
        let (_anvil, endpoint, private_key) = setup_anvil();
        let wallet = create_test_wallet(&endpoint, &private_key);
        let vault_service = RaindexService::new(
            wallet,
            RaindexContracts {
                inventory: ORDERBOOK_ADDRESS,
                orderbook: ORDERBOOK_ADDRESS,
            },
            recipient,
        );

        let mock_bridge = Arc::new(MockBridge::new());

        let manager = CrossVenueCashTransfer::new(
            alpaca_broker,
            alpaca_wallet,
            Arc::clone(&mock_bridge),
            Arc::new(vault_service),
            cqrs.clone(),
            recipient,
            TEST_VAULT_ID,
            &test_settlement_params(),
        );

        let receipt = manager
            .burn_recording_pending(&id, BridgeDirection::EthereumToBase, amount_u256, recipient)
            .await
            .unwrap();

        // Assertion 1: the returned receipt carries the SECOND submitted hash.
        let expected_second_hash = TxHash::from([2u8; 32]);
        assert_eq!(
            receipt.tx, expected_second_hash,
            "burn_recording_pending must return the second-attempt hash after a revert"
        );
        assert_eq!(
            receipt.amount, amount_u256,
            "receipt amount must match input amount"
        );

        // Assertion 2: the aggregate is at BridgingSubmitting with the second hash.
        let state = cqrs.load(&id).await.unwrap().unwrap();
        let UsdcRebalance::BridgingSubmitting {
            pending_burn_tx, ..
        } = state
        else {
            panic!("expected BridgingSubmitting after burn_recording_pending, got: {state:?}");
        };
        assert_eq!(
            pending_burn_tx,
            Some(expected_second_hash),
            "pending_burn_tx must be the second hash after a successful retry"
        );

        // Assertion 3: exactly 2 PendingBurnRecorded events; the last carries [2u8; 32].
        let id_str = id.to_string();
        let pending_burn_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM events WHERE aggregate_id = ? \
             AND event_type = 'UsdcRebalanceEvent::PendingBurnRecorded'",
        )
        .bind(&id_str)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            pending_burn_count, 2,
            "expected exactly 2 PendingBurnRecorded events, got: {pending_burn_count}"
        );

        let last_payload: String = sqlx::query_scalar(
            "SELECT payload FROM events WHERE aggregate_id = ? \
             AND event_type = 'UsdcRebalanceEvent::PendingBurnRecorded' \
             ORDER BY sequence DESC LIMIT 1",
        )
        .bind(&id_str)
        .fetch_one(&pool)
        .await
        .unwrap();
        let last_event: UsdcRebalanceEvent = serde_json::from_str(&last_payload).unwrap();
        let UsdcRebalanceEvent::PendingBurnRecorded {
            burn_tx: last_burn_tx,
            ..
        } = last_event
        else {
            panic!("expected PendingBurnRecorded variant in last event payload");
        };
        assert_eq!(
            last_burn_tx, expected_second_hash,
            "last PendingBurnRecorded must carry the second-attempt hash"
        );
    }

    /// CORRECTION C: when `pending_burn_tx` is set and `burn_status` reports the
    /// recorded burn MINED, resume adopts it via `confirm_burn` (which runs the
    /// MessageSent validation, not a bare receipt) and advances to `Bridging`
    /// WITHOUT a log scan or a second burn. The scan lower bound is set ABOVE the
    /// burn block, so the scan alone could not have found it -- proving the
    /// receipt-based adopt path, not the scan, did the adoption.
    #[cfg(feature = "test-support")]
    #[tokio::test]
    async fn resume_bridging_submitting_ethereum_adopts_pending_burn_when_mined() {
        let chains = deploy_dual_chain_cctp().await;
        let amount = usdc("100");
        let amount_u256 = usdc_to_u256(amount).unwrap();

        let cctp_bridge = Arc::new(
            CctpBridge::try_from_ctx(CctpCtx {
                usdc_ethereum: USDC_ADDRESS,
                usdc_base: USDC_ADDRESS,
                ethereum_wallet: create_test_wallet(&chains.ethereum_endpoint, &chains.bot_key),
                base_wallet: create_test_wallet(&chains.base_endpoint, &chains.bot_key),
                circle_api_base: st0x_bridge::cctp::CIRCLE_API_BASE.to_string(),
                token_messenger: chains.token_messenger,
                message_transmitter: chains.message_transmitter,
            })
            .unwrap(),
        );

        let deployer_key = B256::from_slice(chains.bot_key.as_slice());
        mint_usdc(
            &chains.ethereum_endpoint,
            &deployer_key,
            USDC_ADDRESS,
            chains.bot_address,
            amount_u256 * U256::from(10u64),
        )
        .await
        .unwrap();

        let existing_burn = cctp_bridge
            .burn(
                BridgeDirection::EthereumToBase,
                amount_u256,
                chains.bot_address,
            )
            .await
            .unwrap();

        let server = MockServer::start();
        let alpaca_broker = InstrumentedAlpacaBroker::new(
            create_test_broker_service(&server).await,
            TelemetrySender::disabled(),
        );
        let alpaca_wallet = Arc::new(create_test_wallet_service(&server));
        let vault_service = RaindexService::new(
            create_test_wallet(&chains.base_endpoint, &chains.bot_key),
            RaindexContracts {
                inventory: ORDERBOOK_ADDRESS,
                orderbook: ORDERBOOK_ADDRESS,
            },
            chains.bot_address,
        );
        let cqrs = create_test_store_instance().await;
        let manager = CrossVenueCashTransfer::new(
            alpaca_broker,
            alpaca_wallet,
            Arc::clone(&cctp_bridge),
            Arc::new(vault_service),
            cqrs.clone(),
            chains.bot_address,
            TEST_VAULT_ID,
            &test_settlement_params(),
        );

        let ethereum_provider = ProviderBuilder::new()
            .connect(&chains.ethereum_endpoint)
            .await
            .unwrap();
        // Scan lower bound ABOVE the burn block: a log scan from here cannot find
        // the burn (it is at an earlier block), so adoption can only come from the
        // pending-tx receipt check.
        let from_block = ethereum_provider.get_block_number().await.unwrap() + 1_000;

        let id = UsdcRebalanceId(Uuid::new_v4());
        advance_to_bridging_submitting_alpaca_to_base(&cqrs, &id, amount, from_block).await;

        let nonce_before = ethereum_provider
            .get_transaction_count(chains.bot_address)
            .await
            .unwrap();

        let burn_receipt = manager
            .resume_bridging_submitting_ethereum(
                &id,
                amount_u256,
                from_block,
                Some(existing_burn.tx),
            )
            .await
            .unwrap();

        assert_eq!(
            burn_receipt.tx, existing_burn.tx,
            "adopt path must return the recorded pending burn tx"
        );
        assert_eq!(
            ethereum_provider
                .get_transaction_count(chains.bot_address)
                .await
                .unwrap(),
            nonce_before,
            "adopting the pending burn must NOT submit a second burn (nonce unchanged)"
        );

        let state = cqrs.load(&id).await.unwrap().expect("aggregate exists");
        assert!(
            matches!(
                state,
                UsdcRebalance::Bridging {
                    direction: RebalanceDirection::AlpacaToBase,
                    burn_tx_hash,
                    ..
                } if burn_tx_hash == existing_burn.tx
            ),
            "aggregate must reach Bridging with the adopted pending burn; got: {state:?}"
        );
    }

    /// The adopt path in `check_pending_burn` calls `confirm_burn` (not a bare
    /// `BurnReceipt` construction) so that a recorded burn whose receipt mines
    /// successfully but emits NO `MessageSent` event is REJECTED -- never silently
    /// adopted. Uses a STOP-only contract at TOKEN_MESSENGER_V2 so the tx mines
    /// with status=1 and no logs. Staging its hash as `pending_burn_tx` then
    /// driving through `resume_bridging_submitting_ethereum` proves the adopt path
    /// runs the MessageSent check and surfaces
    /// `UsdcTransferError::Cctp(MessageSentEventNotFound)` rather than advancing
    /// the aggregate to `Bridging` with a hash whose message was never emitted.
    #[tokio::test]
    async fn resume_bridging_submitting_ethereum_adopt_path_rejects_missing_message_sent() {
        let market_maker_wallet = address!("0x2222222222222222222222222222222222222222");

        let chain =
            deploy_ethereum_usdc_chain_with_balance(U256::from(1_000_000u64), market_maker_wallet)
                .await;

        let provider = ProviderBuilder::new()
            .connect(&chain.endpoint)
            .await
            .unwrap();

        // STOP-only contract at TOKEN_MESSENGER_V2: any tx to this address mines
        // with status=1 (success) and emits no logs. This mimics a burn tx whose
        // EVM call succeeded but whose MessageSent event was somehow absent --
        // the post-commit error class that `confirm_burn` catches.
        let stop_bytecode = alloy::primitives::Bytes::from(vec![0x00u8]);
        provider
            .anvil_set_code(st0x_bridge::cctp::TOKEN_MESSENGER_V2, stop_bytecode)
            .await
            .unwrap();

        // Submit a raw tx to TOKEN_MESSENGER_V2 (STOP bytecode) so we have a
        // mined-success hash with no logs without going through `submit_burn` (which
        // would require a live Circle fee endpoint). This is the hash that would
        // have been durably recorded by `RecordPendingBurn` in production.
        let bot_signer = PrivateKeySigner::from_bytes(&chain.bot_key).unwrap();
        let bot_provider = ProviderBuilder::new()
            .wallet(alloy::network::EthereumWallet::from(bot_signer))
            .connect(&chain.endpoint)
            .await
            .unwrap();
        let stop_receipt = bot_provider
            .send_transaction(alloy::rpc::types::TransactionRequest {
                to: Some(st0x_bridge::cctp::TOKEN_MESSENGER_V2.into()),
                ..Default::default()
            })
            .await
            .unwrap()
            .get_receipt()
            .await
            .unwrap();
        assert!(
            stop_receipt.status(),
            "STOP-bytecode tx must mine with status=1 (success)"
        );
        let stop_tx = stop_receipt.transaction_hash;

        let server = MockServer::start();
        let (manager, cqrs) =
            build_manager_with_ethereum_chain(&chain, &server, market_maker_wallet).await;

        let id = UsdcRebalanceId(Uuid::new_v4());
        let amount = usdc("1");
        let amount_u256 = usdc_to_u256(amount).unwrap();
        let from_block = provider.get_block_number().await.unwrap();
        advance_to_bridging_submitting_alpaca_to_base(&cqrs, &id, amount, from_block).await;

        // Stage `stop_tx` as the durably-recorded pending burn hash, exactly as
        // `submit_and_record_burn` would in production.
        cqrs.send(
            &id,
            UsdcRebalanceCommand::RecordPendingBurn { burn_tx: stop_tx },
        )
        .await
        .unwrap();

        let error = manager
            .resume_bridging_submitting_ethereum(&id, amount_u256, from_block, Some(stop_tx))
            .await
            .unwrap_err();

        // The adopt path must call `confirm_burn` and reject the no-MessageSent receipt.
        let UsdcTransferError::Cctp(cctp_err) = &error else {
            panic!("expected UsdcTransferError::Cctp (MessageSentEventNotFound); got: {error:?}");
        };
        assert!(
            matches!(**cctp_err, CctpError::MessageSentEventNotFound { .. }),
            "inner CCTP error must be MessageSentEventNotFound; got: {cctp_err:?}"
        );

        // Aggregate must remain at BridgingSubmitting -- no terminal failure emitted.
        let state = cqrs.load(&id).await.unwrap().expect("aggregate exists");
        assert!(
            matches!(
                state,
                UsdcRebalance::BridgingSubmitting {
                    direction: RebalanceDirection::AlpacaToBase,
                    ..
                }
            ),
            "aggregate must stay at BridgingSubmitting when the adopt-path MessageSent \
             check fails; got: {state:?}"
        );
    }

    /// CORRECTION A: when `pending_burn_tx` is set but `burn_status` reports the
    /// recorded burn still PENDING (no receipt, but the tx is visible in the
    /// mempool), resume returns `SettlementCheckTransient` (delayed redrive) and
    /// does NOT reburn -- the aggregate stays at `BridgingSubmitting`. The pending
    /// tx is created on a `--no-mining`-equivalent (auto-mine disabled) chain so it
    /// sits in the mempool unmined, exactly the case a chain-head-margin heuristic
    /// alone would misclassify as dropped.
    #[tokio::test]
    async fn resume_bridging_submitting_ethereum_pending_burn_returns_transient_no_reburn() {
        let market_maker_wallet = address!("0x2222222222222222222222222222222222222222");
        let chain =
            deploy_ethereum_usdc_chain_with_balance(U256::from(1_000_000u64), market_maker_wallet)
                .await;

        let server = MockServer::start();
        let (manager, cqrs) =
            build_manager_with_ethereum_chain(&chain, &server, market_maker_wallet).await;

        // Disable auto-mining and broadcast a tx so it sits unmined in the mempool:
        // get_transaction_receipt is None while get_transaction_by_hash is Some.
        let bot_signer = PrivateKeySigner::from_bytes(&chain.bot_key).unwrap();
        let bot_provider = ProviderBuilder::new()
            .wallet(alloy::network::EthereumWallet::from(bot_signer))
            .connect(&chain.endpoint)
            .await
            .unwrap();
        bot_provider.anvil_set_auto_mine(false).await.unwrap();
        let pending = bot_provider
            .send_transaction(alloy::rpc::types::TransactionRequest {
                to: Some(market_maker_wallet.into()),
                value: Some(U256::from(1u64)),
                gas: Some(21_000),
                ..Default::default()
            })
            .await
            .unwrap();
        let pending_tx = *pending.tx_hash();
        let nonce_before = bot_provider
            .get_transaction_count(chain.bot_address)
            .await
            .unwrap();

        let id = UsdcRebalanceId(Uuid::new_v4());
        let amount = usdc("1");
        let amount_u256 = usdc_to_u256(amount).unwrap();
        let from_block = bot_provider.get_block_number().await.unwrap();
        advance_to_bridging_submitting_alpaca_to_base(&cqrs, &id, amount, from_block).await;

        let error = manager
            .resume_bridging_submitting_ethereum(&id, amount_u256, from_block, Some(pending_tx))
            .await
            .unwrap_err();

        assert!(
            matches!(error, UsdcTransferError::SettlementCheckTransient { .. }),
            "a mempool-visible (still-pending) recorded burn must yield \
             SettlementCheckTransient (delayed redrive), not reburn; got: {error:?}"
        );
        assert_eq!(
            bot_provider
                .get_transaction_count(chain.bot_address)
                .await
                .unwrap(),
            nonce_before,
            "a still-pending recorded burn must NOT submit a burn (nonce unchanged)"
        );

        let state = cqrs.load(&id).await.unwrap().expect("aggregate exists");
        assert!(
            matches!(
                state,
                UsdcRebalance::BridgingSubmitting {
                    direction: RebalanceDirection::AlpacaToBase,
                    ..
                }
            ),
            "aggregate must stay at BridgingSubmitting on a pending recorded burn; got: {state:?}"
        );
    }

    /// When `pending_burn_tx` is set but `burn_status` reports the recorded burn
    /// DROPPED (no receipt and absent from the mempool past the grace + miss
    /// threshold), resume returns the TERMINAL `BurnTxDropped` and does NOT reburn:
    /// once a burn tx hash is durably recorded, an ambiguous drop pages the operator
    /// rather than risking a double-burn off a load-balanced RPC misclassification.
    /// A fabricated, never-broadcast tx hash drives the drop; the fast burn-drop
    /// policy concludes `Dropped` without waiting out the production grace window.
    #[tokio::test]
    async fn resume_bridging_submitting_ethereum_dropped_burn_pages_operator_no_reburn() {
        let market_maker_wallet = address!("0x2222222222222222222222222222222222222222");
        let chain =
            deploy_ethereum_usdc_chain_with_balance(U256::from(1_000_000u64), market_maker_wallet)
                .await;

        let server = MockServer::start();
        let (manager, cqrs) =
            build_manager_with_ethereum_chain_fast_burn_drop(&chain, &server, market_maker_wallet)
                .await;

        let provider = ProviderBuilder::new()
            .connect(&chain.endpoint)
            .await
            .unwrap();
        let nonce_before = provider
            .get_transaction_count(chain.bot_address)
            .await
            .unwrap();

        // A hash that was never broadcast: no receipt and absent from the mempool,
        // so the fast drop policy classifies it Dropped on the first poll.
        let dropped_tx =
            b256!("0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef");

        let id = UsdcRebalanceId(Uuid::new_v4());
        let amount = usdc("1");
        let amount_u256 = usdc_to_u256(amount).unwrap();
        let from_block = provider.get_block_number().await.unwrap();
        advance_to_bridging_submitting_alpaca_to_base(&cqrs, &id, amount, from_block).await;

        let error = manager
            .resume_bridging_submitting_ethereum(&id, amount_u256, from_block, Some(dropped_tx))
            .await
            .unwrap_err();

        assert!(
            matches!(
                error,
                UsdcTransferError::BurnTxDropped { burn_tx, .. } if burn_tx == dropped_tx
            ),
            "a dropped recorded burn must page the operator with terminal BurnTxDropped, \
             not reburn; got: {error:?}"
        );
        assert_eq!(
            provider
                .get_transaction_count(chain.bot_address)
                .await
                .unwrap(),
            nonce_before,
            "a dropped recorded burn must NOT submit a second burn (nonce unchanged)"
        );

        let state = cqrs.load(&id).await.unwrap().expect("aggregate exists");
        assert!(
            matches!(
                state,
                UsdcRebalance::BridgingSubmitting {
                    direction: RebalanceDirection::AlpacaToBase,
                    ..
                }
            ),
            "aggregate must stay at BridgingSubmitting on a dropped recorded burn \
             (operator reconciles); got: {state:?}"
        );
    }

    /// When `pending_burn_tx` is set but `burn_status` reports the recorded burn
    /// MINED-REVERTED, resume falls through to the scan-or-reburn path and issues a
    /// NEW burn (a reverted burn moved no funds, so reburning is SAFE) -- never a
    /// transient or terminal error. The recorded tx is a REVERT-opcode tx (mines
    /// with status 0); the scan finds no prior burn, so a fresh burn is submitted
    /// and the aggregate advances to `Bridging` carrying the NEW burn tx.
    #[cfg(feature = "test-support")]
    #[tokio::test]
    async fn resume_bridging_submitting_ethereum_reverted_burn_reburns() {
        let chains = deploy_dual_chain_cctp().await;
        let amount = usdc("100");
        let amount_u256 = usdc_to_u256(amount).unwrap();

        let cctp_bridge = Arc::new(
            CctpBridge::try_from_ctx(CctpCtx {
                usdc_ethereum: USDC_ADDRESS,
                usdc_base: USDC_ADDRESS,
                ethereum_wallet: create_test_wallet(&chains.ethereum_endpoint, &chains.bot_key),
                base_wallet: create_test_wallet(&chains.base_endpoint, &chains.bot_key),
                circle_api_base: st0x_bridge::cctp::CIRCLE_API_BASE.to_string(),
                token_messenger: chains.token_messenger,
                message_transmitter: chains.message_transmitter,
            })
            .unwrap(),
        );

        let deployer_key = B256::from_slice(chains.bot_key.as_slice());
        mint_usdc(
            &chains.ethereum_endpoint,
            &deployer_key,
            USDC_ADDRESS,
            chains.bot_address,
            amount_u256 * U256::from(10u64),
        )
        .await
        .unwrap();

        let server = MockServer::start();
        let alpaca_broker = InstrumentedAlpacaBroker::new(
            create_test_broker_service(&server).await,
            TelemetrySender::disabled(),
        );
        let alpaca_wallet = Arc::new(create_test_wallet_service(&server));
        let vault_service = RaindexService::new(
            create_test_wallet(&chains.base_endpoint, &chains.bot_key),
            RaindexContracts {
                inventory: ORDERBOOK_ADDRESS,
                orderbook: ORDERBOOK_ADDRESS,
            },
            chains.bot_address,
        );
        let cqrs = create_test_store_instance().await;
        let manager = CrossVenueCashTransfer::new(
            alpaca_broker,
            alpaca_wallet,
            Arc::clone(&cctp_bridge),
            Arc::new(vault_service),
            cqrs.clone(),
            chains.bot_address,
            TEST_VAULT_ID,
            &test_settlement_params(),
        );

        // A REVERT-opcode tx (PUSH1 0, PUSH1 0, REVERT): explicit gas skips
        // estimation so it broadcasts and mines with status 0. Its hash stands in
        // for a recorded burn whose receipt reverted.
        let bot_signer = PrivateKeySigner::from_bytes(&chains.bot_key).unwrap();
        let bot_provider = ProviderBuilder::new()
            .wallet(alloy::network::EthereumWallet::from(bot_signer))
            .connect(&chains.ethereum_endpoint)
            .await
            .unwrap();
        let revert_addr = address!("0x00000000000000000000000000000000000000bb");
        bot_provider
            .anvil_set_code(
                revert_addr,
                alloy::primitives::Bytes::from(vec![0x60, 0x00, 0x60, 0x00, 0xfd]),
            )
            .await
            .unwrap();
        let reverted_receipt = bot_provider
            .send_transaction(alloy::rpc::types::TransactionRequest {
                to: Some(revert_addr.into()),
                gas: Some(100_000),
                ..Default::default()
            })
            .await
            .unwrap()
            .get_receipt()
            .await
            .unwrap();
        assert!(
            !reverted_receipt.status(),
            "the recorded burn tx must mine reverted"
        );
        let reverted_tx = reverted_receipt.transaction_hash;

        // Scan lower bound below the head, then advance well past it so the burn
        // scan is conclusive-empty (no prior burn) and the reburn proceeds.
        let from_block = bot_provider.get_block_number().await.unwrap();
        bot_provider.anvil_mine(Some(5), None).await.unwrap();

        let id = UsdcRebalanceId(Uuid::new_v4());
        advance_to_bridging_submitting_alpaca_to_base(&cqrs, &id, amount, from_block).await;

        let burn_receipt = manager
            .resume_bridging_submitting_ethereum(&id, amount_u256, from_block, Some(reverted_tx))
            .await
            .unwrap();

        assert_ne!(
            burn_receipt.tx, reverted_tx,
            "a reverted recorded burn must trigger a NEW burn, not adopt the reverted tx"
        );

        let state = cqrs.load(&id).await.unwrap().expect("aggregate exists");
        assert!(
            matches!(
                state,
                UsdcRebalance::Bridging {
                    direction: RebalanceDirection::AlpacaToBase,
                    burn_tx_hash,
                    ..
                } if burn_tx_hash == burn_receipt.tx && burn_tx_hash != reverted_tx
            ),
            "aggregate must reach Bridging carrying the NEW burn tx; got: {state:?}"
        );
    }

    /// CORRECTION B regression: a per-attempt timeout that cancels the resume
    /// future during the receipt/confirm wait -- AFTER the burn was broadcast and
    /// `RecordPendingBurn` committed -- must NOT lose the tx hash. The
    /// `submit_burn -> RecordPendingBurn` critical section runs on a detached task,
    /// so it completes despite the cancellation; the redrive then ADOPTS the
    /// recorded tx (no second burn). Mining is disabled so the confirm wait hangs
    /// (forcing the timeout) and the burn stays pending until the redrive mines it.
    #[cfg(feature = "test-support")]
    #[tokio::test]
    async fn timeout_during_confirm_records_burn_then_redrive_adopts_no_double_burn() {
        let chains = deploy_dual_chain_cctp().await;
        let amount = usdc("100");
        let amount_u256 = usdc_to_u256(amount).unwrap();

        let cctp_bridge = Arc::new(
            CctpBridge::try_from_ctx(CctpCtx {
                usdc_ethereum: USDC_ADDRESS,
                usdc_base: USDC_ADDRESS,
                ethereum_wallet: create_test_wallet(&chains.ethereum_endpoint, &chains.bot_key),
                base_wallet: create_test_wallet(&chains.base_endpoint, &chains.bot_key),
                circle_api_base: st0x_bridge::cctp::CIRCLE_API_BASE.to_string(),
                token_messenger: chains.token_messenger,
                message_transmitter: chains.message_transmitter,
            })
            .unwrap(),
        );

        let deployer_key = B256::from_slice(chains.bot_key.as_slice());
        mint_usdc(
            &chains.ethereum_endpoint,
            &deployer_key,
            USDC_ADDRESS,
            chains.bot_address,
            amount_u256 * U256::from(10u64),
        )
        .await
        .unwrap();

        // A first real burn sets the standing allowance to MAX, so the timed-out
        // burn below need not mine an approve (which would hang with mining off).
        cctp_bridge
            .burn(
                BridgeDirection::EthereumToBase,
                amount_u256,
                chains.bot_address,
            )
            .await
            .unwrap();

        let server = MockServer::start();
        let alpaca_broker = InstrumentedAlpacaBroker::new(
            create_test_broker_service(&server).await,
            TelemetrySender::disabled(),
        );
        let alpaca_wallet = Arc::new(create_test_wallet_service(&server));
        let vault_service = RaindexService::new(
            create_test_wallet(&chains.base_endpoint, &chains.bot_key),
            RaindexContracts {
                inventory: ORDERBOOK_ADDRESS,
                orderbook: ORDERBOOK_ADDRESS,
            },
            chains.bot_address,
        );
        let cqrs = create_test_store_instance().await;
        let manager = CrossVenueCashTransfer::new(
            alpaca_broker,
            alpaca_wallet,
            Arc::clone(&cctp_bridge),
            Arc::new(vault_service),
            cqrs.clone(),
            chains.bot_address,
            TEST_VAULT_ID,
            &test_settlement_params(),
        );

        let ethereum_provider = ProviderBuilder::new()
            .connect(&chains.ethereum_endpoint)
            .await
            .unwrap();
        let from_block = ethereum_provider.get_block_number().await.unwrap();

        let id = UsdcRebalanceId(Uuid::new_v4());
        advance_to_withdrawal_complete_alpaca_to_base(&cqrs, &id, amount).await;

        // Disable mining so confirm_burn (await_receipt) hangs, forcing the timeout
        // to cancel the resume future AFTER the burn broadcast and RecordPendingBurn.
        ethereum_provider.anvil_set_auto_mine(false).await.unwrap();

        let timed_out = tokio::time::timeout(
            Duration::from_secs(2),
            manager.execute_cctp_burn_on_ethereum(&id, amount_u256, Some(from_block)),
        )
        .await;
        assert!(
            timed_out.is_err(),
            "the burn confirm must not return while mining is disabled (the per-attempt \
             timeout must fire); got: {timed_out:?}"
        );

        // The detached submit-and-record task commits RecordPendingBurn despite the
        // cancellation: poll until pending_burn_tx is durably set. A deadline-bounded
        // loop (not a fixed iteration count) waits on the actual condition, so it is
        // not timing-flaky -- it returns as soon as the event commits and fails only
        // if the commit never lands within the generous timeout.
        let recorded = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if let Some(UsdcRebalance::BridgingSubmitting {
                    pending_burn_tx: Some(tx),
                    ..
                }) = cqrs.load(&id).await.unwrap()
                {
                    break tx;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .expect(
            "the detached submit-and-record task must commit RecordPendingBurn even though the \
             confirm wait was cancelled by the timeout",
        );

        // Mine the still-pending burn, then redrive: burn_status now reports
        // MinedSuccess and the redrive adopts the recorded tx, no second burn.
        ethereum_provider.anvil_set_auto_mine(true).await.unwrap();
        ethereum_provider.anvil_mine(Some(1), None).await.unwrap();

        let nonce_before_redrive = ethereum_provider
            .get_transaction_count(chains.bot_address)
            .await
            .unwrap();

        let burn_receipt = manager
            .resume_bridging_submitting_ethereum(&id, amount_u256, from_block, Some(recorded))
            .await
            .unwrap();

        assert_eq!(
            burn_receipt.tx, recorded,
            "the redrive must adopt the durably-recorded burn tx, not reburn"
        );
        assert_eq!(
            ethereum_provider
                .get_transaction_count(chains.bot_address)
                .await
                .unwrap(),
            nonce_before_redrive,
            "adopting the recorded burn must NOT submit a second burn (nonce unchanged)"
        );

        let state = cqrs.load(&id).await.unwrap().expect("aggregate exists");
        assert!(
            matches!(
                state,
                UsdcRebalance::Bridging {
                    direction: RebalanceDirection::AlpacaToBase,
                    burn_tx_hash,
                    ..
                } if burn_tx_hash == recorded
            ),
            "aggregate must reach Bridging with the adopted recorded burn; got: {state:?}"
        );
    }

    /// Base-direction (hedging) counterpart of
    /// `resume_bridging_submitting_ethereum_pending_burn_returns_transient_no_reburn`:
    /// `resume_bridging_submitting` (Base->Alpaca, burning BaseToEthereum) must also
    /// return `SettlementCheckTransient` (no reburn) when the recorded burn is still
    /// pending in the mempool, proving `check_pending_burn` is symmetric across
    /// directions. The aggregate stays at `BridgingSubmitting`.
    #[tokio::test]
    async fn resume_bridging_submitting_base_pending_burn_returns_transient_no_reburn() {
        let server = MockServer::start();
        let (manager, cqrs, anvil) = make_resume_test_manager(&server).await;

        // Broadcast a tx into the mempool unmined (auto-mine off): no receipt, but
        // get_transaction_by_hash returns it -> burn_status classifies Pending.
        let bot_key = B256::from_slice(&anvil.keys()[0].to_bytes());
        let bot_signer = PrivateKeySigner::from_bytes(&bot_key).unwrap();
        let bot_address = bot_signer.address();
        let bot_provider = ProviderBuilder::new()
            .wallet(alloy::network::EthereumWallet::from(bot_signer))
            .connect(&anvil.endpoint())
            .await
            .unwrap();
        bot_provider.anvil_set_auto_mine(false).await.unwrap();
        let pending = bot_provider
            .send_transaction(alloy::rpc::types::TransactionRequest {
                to: Some(address!("0x00000000000000000000000000000000000000dd").into()),
                value: Some(U256::from(1u64)),
                gas: Some(21_000),
                ..Default::default()
            })
            .await
            .unwrap();
        let pending_tx = *pending.tx_hash();
        let nonce_before = bot_provider
            .get_transaction_count(bot_address)
            .await
            .unwrap();

        let id = UsdcRebalanceId(Uuid::new_v4());
        let amount = usdc("1");
        let amount_u256 = usdc_to_u256(amount).unwrap();
        let from_block = bot_provider.get_block_number().await.unwrap();
        advance_to_bridging_submitting_base_to_alpaca(&cqrs, &id, amount, from_block).await;

        let error = manager
            .resume_bridging_submitting(&id, amount_u256, from_block, Some(pending_tx))
            .await
            .unwrap_err();

        assert!(
            matches!(error, UsdcTransferError::SettlementCheckTransient { .. }),
            "a mempool-visible recorded burn must yield SettlementCheckTransient on the \
             Base direction too; got: {error:?}"
        );
        assert_eq!(
            bot_provider
                .get_transaction_count(bot_address)
                .await
                .unwrap(),
            nonce_before,
            "a still-pending recorded burn must NOT submit a burn (nonce unchanged)"
        );

        let state = cqrs.load(&id).await.unwrap().expect("aggregate exists");
        assert!(
            matches!(
                state,
                UsdcRebalance::BridgingSubmitting {
                    direction: RebalanceDirection::BaseToAlpaca,
                    ..
                }
            ),
            "aggregate must stay at BridgingSubmitting on a pending recorded burn; got: {state:?}"
        );
    }

    /// Base-direction (hedging) counterpart of
    /// `resume_bridging_submitting_ethereum_adopts_pending_burn_when_mined`: when
    /// `pending_burn_tx` is set and `burn_status` reports the recorded burn MINED,
    /// `resume_bridging_submitting` (Base->Alpaca, burning via BaseToEthereum) adopts
    /// it via `confirm_burn` and advances to `Bridging` WITHOUT a second burn. The
    /// scan lower bound sits ABOVE the burn block, so the scan alone could not have
    /// found it -- proving the receipt-based adopt path, not the scan, did the work.
    #[cfg(feature = "test-support")]
    #[tokio::test]
    async fn resume_bridging_submitting_base_adopts_pending_burn_when_mined() {
        let chains = deploy_dual_chain_cctp().await;
        let amount = usdc("100");
        let amount_u256 = usdc_to_u256(amount).unwrap();

        let cctp_bridge = Arc::new(
            CctpBridge::try_from_ctx(CctpCtx {
                usdc_ethereum: USDC_ADDRESS,
                usdc_base: USDC_ADDRESS,
                ethereum_wallet: create_test_wallet(&chains.ethereum_endpoint, &chains.bot_key),
                base_wallet: create_test_wallet(&chains.base_endpoint, &chains.bot_key),
                circle_api_base: st0x_bridge::cctp::CIRCLE_API_BASE.to_string(),
                token_messenger: chains.token_messenger,
                message_transmitter: chains.message_transmitter,
            })
            .unwrap(),
        );

        // `deploy_dual_chain_cctp` pre-funds the Base bot wallet with USDC, so the
        // burn proceeds without a separate mint.
        let existing_burn = cctp_bridge
            .burn(
                BridgeDirection::BaseToEthereum,
                amount_u256,
                chains.bot_address,
            )
            .await
            .unwrap();

        let server = MockServer::start();
        let alpaca_broker = InstrumentedAlpacaBroker::new(
            create_test_broker_service(&server).await,
            TelemetrySender::disabled(),
        );
        let alpaca_wallet = Arc::new(create_test_wallet_service(&server));
        let vault_service = RaindexService::new(
            create_test_wallet(&chains.base_endpoint, &chains.bot_key),
            RaindexContracts {
                inventory: ORDERBOOK_ADDRESS,
                orderbook: ORDERBOOK_ADDRESS,
            },
            chains.bot_address,
        );
        let cqrs = create_test_store_instance().await;
        let manager = CrossVenueCashTransfer::new(
            alpaca_broker,
            alpaca_wallet,
            Arc::clone(&cctp_bridge),
            Arc::new(vault_service),
            cqrs.clone(),
            chains.bot_address,
            TEST_VAULT_ID,
            &test_settlement_params(),
        );

        let base_provider = ProviderBuilder::new()
            .connect(&chains.base_endpoint)
            .await
            .unwrap();
        // Scan lower bound ABOVE the burn block: a log scan from here cannot find
        // the burn (it is at an earlier block), so adoption can only come from the
        // pending-tx receipt check.
        let from_block = base_provider.get_block_number().await.unwrap() + 1_000;

        let id = UsdcRebalanceId(Uuid::new_v4());
        advance_to_bridging_submitting_base_to_alpaca(&cqrs, &id, amount, from_block).await;

        let nonce_before = base_provider
            .get_transaction_count(chains.bot_address)
            .await
            .unwrap();

        let burn_receipt = manager
            .resume_bridging_submitting(&id, amount_u256, from_block, Some(existing_burn.tx))
            .await
            .unwrap();

        assert_eq!(
            burn_receipt.tx, existing_burn.tx,
            "adopt path must return the recorded pending burn tx"
        );
        assert_eq!(
            base_provider
                .get_transaction_count(chains.bot_address)
                .await
                .unwrap(),
            nonce_before,
            "adopting the pending burn must NOT submit a second burn (nonce unchanged)"
        );

        let state = cqrs.load(&id).await.unwrap().expect("aggregate exists");
        assert!(
            matches!(
                state,
                UsdcRebalance::Bridging {
                    direction: RebalanceDirection::BaseToAlpaca,
                    burn_tx_hash,
                    ..
                } if burn_tx_hash == existing_burn.tx
            ),
            "aggregate must reach Bridging with the adopted pending burn; got: {state:?}"
        );
    }

    /// Base-direction (hedging) counterpart of
    /// `resume_bridging_submitting_ethereum_dropped_burn_pages_operator_no_reburn`:
    /// when `pending_burn_tx` is set but `burn_status` reports the recorded burn
    /// DROPPED, `resume_bridging_submitting` returns the TERMINAL `BurnTxDropped`
    /// and does NOT reburn -- once a burn tx hash is durably recorded, an ambiguous
    /// drop pages the operator rather than risking a double-burn off a load-balanced
    /// RPC misclassification. A fabricated, never-broadcast tx hash drives the drop;
    /// the fast burn-drop policy concludes `Dropped` without waiting out the
    /// production grace window.
    #[tokio::test]
    async fn resume_bridging_submitting_base_dropped_burn_pages_operator_no_reburn() {
        let (_anvil, endpoint, private_key) = setup_anvil();

        let server = MockServer::start();
        let alpaca_broker = InstrumentedAlpacaBroker::new(
            create_test_broker_service(&server).await,
            TelemetrySender::disabled(),
        );
        let alpaca_wallet = Arc::new(create_test_wallet_service(&server));
        let wallet = create_test_wallet(&endpoint, &private_key);
        let (cctp_bridge, vault_service) = create_test_onchain_services(wallet);
        let cctp_bridge = cctp_bridge.with_fast_burn_drop_policy();
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
            &test_settlement_params(),
        );

        // The bridge's Base wallet (Anvil account 0) is the address a reburn would
        // send from, so its nonce is what must stay unchanged.
        let bridge_wallet = PrivateKeySigner::from_bytes(&private_key)
            .unwrap()
            .address();
        let provider = ProviderBuilder::new().connect(&endpoint).await.unwrap();
        let nonce_before = provider.get_transaction_count(bridge_wallet).await.unwrap();

        // A hash that was never broadcast: no receipt and absent from the mempool,
        // so the fast drop policy classifies it Dropped on the first poll.
        let dropped_tx =
            b256!("0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef");

        let id = UsdcRebalanceId(Uuid::new_v4());
        let amount = usdc("1");
        let amount_u256 = usdc_to_u256(amount).unwrap();
        let from_block = provider.get_block_number().await.unwrap();
        advance_to_bridging_submitting_base_to_alpaca(&cqrs, &id, amount, from_block).await;

        let error = manager
            .resume_bridging_submitting(&id, amount_u256, from_block, Some(dropped_tx))
            .await
            .unwrap_err();

        assert!(
            matches!(
                error,
                UsdcTransferError::BurnTxDropped { burn_tx, .. } if burn_tx == dropped_tx
            ),
            "a dropped recorded burn must page the operator with terminal BurnTxDropped, \
             not reburn; got: {error:?}"
        );
        assert_eq!(
            provider.get_transaction_count(bridge_wallet).await.unwrap(),
            nonce_before,
            "a dropped recorded burn must NOT submit a second burn (nonce unchanged)"
        );

        let state = cqrs.load(&id).await.unwrap().expect("aggregate exists");
        assert!(
            matches!(
                state,
                UsdcRebalance::BridgingSubmitting {
                    direction: RebalanceDirection::BaseToAlpaca,
                    ..
                }
            ),
            "aggregate must stay at BridgingSubmitting on a dropped recorded burn \
             (operator reconciles); got: {state:?}"
        );
    }

    /// Base-direction (hedging) counterpart of
    /// `resume_bridging_submitting_ethereum_reverted_burn_reburns`: when
    /// `pending_burn_tx` is set but `burn_status` reports the recorded burn
    /// MINED-REVERTED, `resume_bridging_submitting` (Base->Alpaca, burning via
    /// BaseToEthereum) falls through to scan-or-reburn and issues a NEW burn (a
    /// reverted burn moved no funds, so reburning is SAFE), advancing to `Bridging`
    /// carrying the NEW tx. The recorded tx is a REVERT-opcode tx (mines status 0);
    /// the scan finds no prior burn, so a fresh burn is submitted.
    #[cfg(feature = "test-support")]
    #[tokio::test]
    async fn resume_bridging_submitting_base_reverted_burn_reburns() {
        let chains = deploy_dual_chain_cctp().await;
        let amount = usdc("100");
        let amount_u256 = usdc_to_u256(amount).unwrap();

        let cctp_bridge = Arc::new(
            CctpBridge::try_from_ctx(CctpCtx {
                usdc_ethereum: USDC_ADDRESS,
                usdc_base: USDC_ADDRESS,
                ethereum_wallet: create_test_wallet(&chains.ethereum_endpoint, &chains.bot_key),
                base_wallet: create_test_wallet(&chains.base_endpoint, &chains.bot_key),
                circle_api_base: st0x_bridge::cctp::CIRCLE_API_BASE.to_string(),
                token_messenger: chains.token_messenger,
                message_transmitter: chains.message_transmitter,
            })
            .unwrap(),
        );

        let server = MockServer::start();
        let alpaca_broker = InstrumentedAlpacaBroker::new(
            create_test_broker_service(&server).await,
            TelemetrySender::disabled(),
        );
        let alpaca_wallet = Arc::new(create_test_wallet_service(&server));
        let vault_service = RaindexService::new(
            create_test_wallet(&chains.base_endpoint, &chains.bot_key),
            RaindexContracts {
                inventory: ORDERBOOK_ADDRESS,
                orderbook: ORDERBOOK_ADDRESS,
            },
            chains.bot_address,
        );
        let cqrs = create_test_store_instance().await;
        let manager = CrossVenueCashTransfer::new(
            alpaca_broker,
            alpaca_wallet,
            Arc::clone(&cctp_bridge),
            Arc::new(vault_service),
            cqrs.clone(),
            chains.bot_address,
            TEST_VAULT_ID,
            &test_settlement_params(),
        );

        // A REVERT-opcode tx (PUSH1 0, PUSH1 0, REVERT): explicit gas skips
        // estimation so it broadcasts and mines with status 0. Its hash stands in
        // for a recorded burn whose receipt reverted. Burned on the BASE chain
        // because `BaseToEthereum` reads burn status from Base.
        let bot_signer = PrivateKeySigner::from_bytes(&chains.bot_key).unwrap();
        let bot_provider = ProviderBuilder::new()
            .wallet(alloy::network::EthereumWallet::from(bot_signer))
            .connect(&chains.base_endpoint)
            .await
            .unwrap();
        let revert_addr = address!("0x00000000000000000000000000000000000000bb");
        bot_provider
            .anvil_set_code(
                revert_addr,
                alloy::primitives::Bytes::from(vec![0x60, 0x00, 0x60, 0x00, 0xfd]),
            )
            .await
            .unwrap();
        let reverted_receipt = bot_provider
            .send_transaction(alloy::rpc::types::TransactionRequest {
                to: Some(revert_addr.into()),
                gas: Some(100_000),
                ..Default::default()
            })
            .await
            .unwrap()
            .get_receipt()
            .await
            .unwrap();
        assert!(
            !reverted_receipt.status(),
            "the recorded burn tx must mine reverted"
        );
        let reverted_tx = reverted_receipt.transaction_hash;

        // Scan lower bound below the head, then advance well past it so the burn
        // scan is conclusive-empty (no prior burn) and the reburn proceeds.
        let from_block = bot_provider.get_block_number().await.unwrap();
        bot_provider.anvil_mine(Some(5), None).await.unwrap();

        let id = UsdcRebalanceId(Uuid::new_v4());
        advance_to_bridging_submitting_base_to_alpaca(&cqrs, &id, amount, from_block).await;

        let burn_receipt = manager
            .resume_bridging_submitting(&id, amount_u256, from_block, Some(reverted_tx))
            .await
            .unwrap();

        assert_ne!(
            burn_receipt.tx, reverted_tx,
            "a reverted recorded burn must trigger a NEW burn, not adopt the reverted tx"
        );

        let state = cqrs.load(&id).await.unwrap().expect("aggregate exists");
        assert!(
            matches!(
                state,
                UsdcRebalance::Bridging {
                    direction: RebalanceDirection::BaseToAlpaca,
                    burn_tx_hash,
                    ..
                } if burn_tx_hash == burn_receipt.tx && burn_tx_hash != reverted_tx
            ),
            "aggregate must reach Bridging carrying the NEW burn tx; got: {state:?}"
        );
    }

    /// Fail-closed: a NON-revert (transport/RPC) submit error must classify as the
    /// TERMINAL `BurnSubmitInconclusive`, never the bounded-redrive `BurnRevert` nor
    /// `Cctp` -- the broadcast may have reached the network, so a retry could
    /// double-burn. The CCTP bridge's Base wallet points at a dead endpoint, so
    /// every submission RPC (the pre-broadcast allowance read / fee query included)
    /// fails with a non-revert transport error; that conservative fail-closed is
    /// intended.
    #[tokio::test]
    async fn submit_and_record_burn_non_revert_submit_error_fails_closed_inconclusive() {
        // Nothing listens on port 1: every RPC fails fast with connection-refused
        // (a non-revert transport error), never a revert.
        let dead_endpoint = "http://127.0.0.1:1";
        let bot_key = b256!("0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80");
        let wallet = create_test_wallet(dead_endpoint, &bot_key);
        let (cctp_bridge, vault_service) = create_test_onchain_services(wallet);

        let server = MockServer::start();
        let alpaca_broker = InstrumentedAlpacaBroker::new(
            create_test_broker_service(&server).await,
            TelemetrySender::disabled(),
        );
        let alpaca_wallet = Arc::new(create_test_wallet_service(&server));
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
            &test_settlement_params(),
        );

        let id = UsdcRebalanceId(Uuid::new_v4());
        let amount = usdc("100");
        let amount_u256 = usdc_to_u256(amount).unwrap();

        // Stage to BridgingSubmitting so the pre-broadcast ClearPendingBurn (a
        // no-op, since no burn is recorded yet) succeeds and the burn submission is
        // actually attempted -- production only burns from BridgingSubmitting.
        advance_to_bridging_submitting_base_to_alpaca(&cqrs, &id, amount, 0).await;

        let error = manager
            .burn_recording_pending(
                &id,
                BridgeDirection::BaseToEthereum,
                amount_u256,
                market_maker_wallet,
            )
            .await
            .unwrap_err();

        assert!(
            matches!(error, UsdcTransferError::BurnSubmitInconclusive { .. }),
            "a non-revert (transport) submit error must fail closed as \
             BurnSubmitInconclusive, never BurnRevert (clean reburn) or Cctp; got: {error:?}"
        );
    }

    /// `check_pending_burn` maps a transient RPC failure from `burn_status` (the
    /// recorded burn's on-chain status cannot be read) to `SettlementCheckTransient`
    /// so the job delayed-redrives rather than surfacing a terminal error. The CCTP
    /// bridge's wallet points at a dead endpoint, so the `burn_status` receipt read
    /// fails with a transport error.
    #[tokio::test]
    async fn check_pending_burn_rpc_error_maps_to_settlement_transient() {
        let dead_endpoint = "http://127.0.0.1:1";
        let bot_key = b256!("0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80");
        let wallet = create_test_wallet(dead_endpoint, &bot_key);
        let (cctp_bridge, vault_service) = create_test_onchain_services(wallet);

        let server = MockServer::start();
        let alpaca_broker = InstrumentedAlpacaBroker::new(
            create_test_broker_service(&server).await,
            TelemetrySender::disabled(),
        );
        let alpaca_wallet = Arc::new(create_test_wallet_service(&server));
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
            &test_settlement_params(),
        );

        let id = UsdcRebalanceId(Uuid::new_v4());
        let amount_u256 = usdc_to_u256(usdc("100")).unwrap();
        let pending_tx =
            b256!("0x00000000000000000000000000000000000000000000000000000000feedface");

        let error = manager
            .check_pending_burn(
                &id,
                BridgeDirection::BaseToEthereum,
                amount_u256,
                Some(pending_tx),
            )
            .await
            .unwrap_err();

        assert!(
            matches!(error, UsdcTransferError::SettlementCheckTransient { .. }),
            "a transient burn_status RPC error must map to SettlementCheckTransient \
             (delayed redrive), not a terminal error; got: {error:?}"
        );
    }

    /// Fail-closed: a burn that broadcasts (irreversible) but whose
    /// `RecordPendingBurn` commit never lands must surface the TERMINAL
    /// `BurnRecordFailed`, never silently lose the hash. The CCTP bridge and Base
    /// wallet are real and funded, so `submit_burn` actually broadcasts. The
    /// aggregate is staged via a writable pool, then driven through a READ-ONLY
    /// pool: the pre-broadcast `ClearPendingBurn` is a no-op (no recorded burn yet)
    /// that only reads, so it succeeds and the burn broadcasts, but every
    /// `RecordPendingBurn` write fails READONLY and the bounded retries exhaust.
    /// (A fully-closed pool would instead fail the pre-broadcast `ClearPendingBurn`
    /// load, so it cannot isolate the post-broadcast record-failure path.)
    #[cfg(feature = "test-support")]
    #[tokio::test]
    async fn burn_recording_pending_record_failure_after_broadcast_fails_closed() {
        let chains = deploy_dual_chain_cctp().await;
        let amount = usdc("100");
        let amount_u256 = usdc_to_u256(amount).unwrap();

        let cctp_bridge = Arc::new(
            CctpBridge::try_from_ctx(CctpCtx {
                usdc_ethereum: USDC_ADDRESS,
                usdc_base: USDC_ADDRESS,
                ethereum_wallet: create_test_wallet(&chains.ethereum_endpoint, &chains.bot_key),
                base_wallet: create_test_wallet(&chains.base_endpoint, &chains.bot_key),
                circle_api_base: st0x_bridge::cctp::CIRCLE_API_BASE.to_string(),
                token_messenger: chains.token_messenger,
                message_transmitter: chains.message_transmitter,
            })
            .unwrap(),
        );

        let server = MockServer::start();
        let alpaca_broker = InstrumentedAlpacaBroker::new(
            create_test_broker_service(&server).await,
            TelemetrySender::disabled(),
        );
        let alpaca_wallet = Arc::new(create_test_wallet_service(&server));
        let vault_service = RaindexService::new(
            create_test_wallet(&chains.base_endpoint, &chains.bot_key),
            RaindexContracts {
                inventory: ORDERBOOK_ADDRESS,
                orderbook: ORDERBOOK_ADDRESS,
            },
            chains.bot_address,
        );

        // Stage the aggregate on a file-backed WRITABLE pool, then close it so the
        // committed events live in the DB file. A rollback journal (Delete) keeps
        // all data in the main file, so a later read-only connection sees it
        // without WAL sidecar files.
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.sqlite");

        let writable = SqlitePool::connect_with(
            sqlx::sqlite::SqliteConnectOptions::new()
                .filename(&db_path)
                .create_if_missing(true)
                .journal_mode(sqlx::sqlite::SqliteJournalMode::Delete),
        )
        .await
        .unwrap();
        sqlx::migrate!().run(&writable).await.unwrap();
        let writable_cqrs = test_store::<UsdcRebalance>(writable.clone());

        let base_provider = ProviderBuilder::new()
            .connect(&chains.base_endpoint)
            .await
            .unwrap();
        let from_block = base_provider.get_block_number().await.unwrap();

        let id = UsdcRebalanceId(Uuid::new_v4());
        advance_to_bridging_submitting_base_to_alpaca(&writable_cqrs, &id, amount, from_block)
            .await;
        writable.close().await;

        // Reopen the same DB READ-ONLY: the pre-broadcast `ClearPendingBurn` is a
        // no-op (no recorded burn yet) needing only a read, so it succeeds and the
        // burn broadcasts; every `RecordPendingBurn` write then fails READONLY,
        // exhausting the bounded retries.
        let read_only = SqlitePool::connect_with(
            sqlx::sqlite::SqliteConnectOptions::new()
                .filename(&db_path)
                .read_only(true),
        )
        .await
        .unwrap();
        let cqrs = Arc::new(test_store(read_only));
        let manager = CrossVenueCashTransfer::new(
            alpaca_broker,
            alpaca_wallet,
            Arc::clone(&cctp_bridge),
            Arc::new(vault_service),
            cqrs.clone(),
            chains.bot_address,
            TEST_VAULT_ID,
            &test_settlement_params(),
        );

        let error = manager
            .burn_recording_pending(
                &id,
                BridgeDirection::BaseToEthereum,
                amount_u256,
                chains.bot_address,
            )
            .await
            .unwrap_err();

        let UsdcTransferError::BurnRecordFailed {
            id: error_id,
            burn_tx,
        } = error
        else {
            panic!(
                "a burn that broadcasts but whose pending-burn record never commits must fail closed as BurnRecordFailed; got: {error:?}"
            );
        };
        assert_eq!(
            error_id, id,
            "BurnRecordFailed id must match the transfer id"
        );
        assert_ne!(
            burn_tx,
            TxHash::ZERO,
            "BurnRecordFailed must carry the broadcast burn tx hash, not zero"
        );
    }

    /// `resume_alpaca_to_base` from `BridgingSubmitting { burn_amount: Some(received) }`
    /// uses the persisted received amount as the burn-scan key and ADOPTS the
    /// existing burn without issuing a second `depositForBurn`.
    ///
    /// This is the crash-resume path when Alpaca deducted a withdrawal fee: the
    /// persisted `burn_amount` is the fee-reduced received amount (e.g. 98 USDC
    /// from a 100 USDC nominal). The scan must target that exact amount to find
    /// the in-flight burn and avoid a double-burn.
    ///
    /// The test stages an Ethereum burn for the received amount, then stages
    /// `BridgingSubmitting { burn_amount: Some(received) }` and calls
    /// `resume_bridging_submitting_ethereum` with the same received amount (the
    /// value `resume_alpaca_to_base` derives from the persisted aggregate field).
    /// A nonce check proves no second burn was submitted.
    #[cfg(feature = "test-support")]
    #[tokio::test]
    async fn resume_alpaca_to_base_with_persisted_burn_amount_scans_received_amount_not_nominal() {
        let chains = deploy_dual_chain_cctp().await;
        let nominal = usdc("100");
        // Simulate a 2 USDC withdrawal fee: received = 98 USDC.
        let received = usdc("98");
        let received_u256 = usdc_to_u256(received).unwrap();
        // If the scan incorrectly used nominal, it would look for 100 USDC and
        // miss the 98-USDC burn, then issue a second burn.
        let nominal_u256 = usdc_to_u256(nominal).unwrap();

        let cctp_bridge = Arc::new(
            CctpBridge::try_from_ctx(CctpCtx {
                usdc_ethereum: USDC_ADDRESS,
                usdc_base: USDC_ADDRESS,
                ethereum_wallet: create_test_wallet(&chains.ethereum_endpoint, &chains.bot_key),
                base_wallet: create_test_wallet(&chains.base_endpoint, &chains.bot_key),
                circle_api_base: st0x_bridge::cctp::CIRCLE_API_BASE.to_string(),
                token_messenger: chains.token_messenger,
                message_transmitter: chains.message_transmitter,
            })
            .unwrap(),
        );

        // Fund the Ethereum bot wallet with enough USDC to burn the received amount.
        let deployer_key = B256::from_slice(chains.bot_key.as_slice());
        mint_usdc(
            &chains.ethereum_endpoint,
            &deployer_key,
            USDC_ADDRESS,
            chains.bot_address,
            nominal_u256 * U256::from(10u64),
        )
        .await
        .unwrap();

        // Capture the scan lower bound BEFORE the burn.
        let from_block = cctp_bridge
            .source_block(BridgeDirection::EthereumToBase)
            .await
            .unwrap();

        // Submit the burn for the RECEIVED amount (98 USDC, not the 100 nominal).
        let existing_burn = cctp_bridge
            .burn(
                BridgeDirection::EthereumToBase,
                received_u256,
                chains.bot_address,
            )
            .await
            .unwrap();

        assert_eq!(
            existing_burn.amount, received_u256,
            "Pre-condition: burn was submitted for the received amount"
        );

        // Build the manager.
        let server = MockServer::start();
        let alpaca_broker = InstrumentedAlpacaBroker::new(
            create_test_broker_service(&server).await,
            TelemetrySender::disabled(),
        );
        let alpaca_wallet = Arc::new(create_test_wallet_service(&server));
        let vault_service = RaindexService::new(
            create_test_wallet(&chains.base_endpoint, &chains.bot_key),
            RaindexContracts {
                inventory: ORDERBOOK_ADDRESS,
                orderbook: ORDERBOOK_ADDRESS,
            },
            chains.bot_address,
        );
        let cqrs = create_test_store_instance().await;
        let manager = CrossVenueCashTransfer::new(
            alpaca_broker,
            alpaca_wallet,
            Arc::clone(&cctp_bridge),
            Arc::new(vault_service),
            cqrs.clone(),
            chains.bot_address,
            TEST_VAULT_ID,
            &test_settlement_params(),
        );

        let id = UsdcRebalanceId(Uuid::new_v4());

        // Stage `BridgingSubmitting` with burn_amount: Some(received) -- the
        // crash state after a fee-deducted withdrawal burned the received amount
        // but the process crashed before `InitiateBridging` was persisted.
        advance_to_bridging_submitting_alpaca_to_base_with_burn_amount(
            &cqrs, &id, nominal, from_block, received,
        )
        .await;

        // Verify the pre-condition: aggregate is at BridgingSubmitting with
        // burn_amount: Some(received).
        let staged = cqrs.load(&id).await.unwrap().expect("aggregate exists");
        let UsdcRebalance::BridgingSubmitting { burn_amount, .. } = staged else {
            panic!("Expected BridgingSubmitting, got: {staged:?}");
        };
        assert_eq!(
            burn_amount,
            Some(received),
            "Pre-condition: BridgingSubmitting must carry burn_amount=Some(received)"
        );

        // Capture the nonce before calling resume -- adopt must not submit a tx.
        let ethereum_provider = ProviderBuilder::new()
            .connect(&chains.ethereum_endpoint)
            .await
            .unwrap();
        let nonce_before = ethereum_provider
            .get_transaction_count(chains.bot_address)
            .await
            .unwrap();

        // Call resume_bridging_submitting_ethereum with the received amount --
        // exactly what resume_alpaca_to_base extracts from the persisted
        // burn_amount field and passes to the scan. The scan must find the
        // existing 98-USDC burn and adopt it; searching for nominal (100 USDC)
        // would miss it and issue a second burn.
        let burn_receipt = manager
            .resume_bridging_submitting_ethereum(&id, received_u256, from_block, None)
            .await
            .unwrap();

        // Adopted: the returned receipt points at the existing burn tx.
        assert_eq!(
            burn_receipt.tx, existing_burn.tx,
            "Resume with received amount must adopt the existing burn tx"
        );
        assert_eq!(
            burn_receipt.amount, received_u256,
            "Adopted burn receipt must carry the received (not nominal) amount"
        );

        let nonce_after = ethereum_provider
            .get_transaction_count(chains.bot_address)
            .await
            .unwrap();
        assert_eq!(
            nonce_before, nonce_after,
            "Adopting the existing burn must NOT submit a second burn (nonce unchanged)"
        );

        // Aggregate must be at Bridging (the adopted burn was recorded via
        // record_cctp_burn -> InitiateBridging).
        let final_state = cqrs.load(&id).await.unwrap().expect("aggregate exists");
        assert!(
            matches!(
                final_state,
                UsdcRebalance::Bridging {
                    direction: RebalanceDirection::AlpacaToBase,
                    burn_tx_hash,
                    ..
                } if burn_tx_hash == existing_burn.tx
            ),
            "Aggregate must reach Bridging with the adopted burn tx; got: {final_state:?}"
        );
    }

    /// When `pending_burn_tx` is None and `find_recent_burn` finds no prior burn,
    /// `resume_bridging_submitting_ethereum` FAILS CLOSED (`BurnSubmitInconclusive`)
    /// rather than reburning: reaching the resume path means a burn submission was
    /// already initiated (`BeginBridging`), so a broadcast may be in flight and an
    /// auto-reburn could double-burn. The genuine first burn is the forward
    /// `execute_cctp_burn_on_ethereum` path, never the resume path.
    #[cfg(feature = "test-support")]
    #[tokio::test]
    async fn resume_bridging_submitting_ethereum_fails_closed_when_no_recorded_burn() {
        let chains = deploy_dual_chain_cctp().await;
        let amount = usdc("100");
        let amount_u256 = usdc_to_u256(amount).unwrap();

        let cctp_bridge = Arc::new(
            CctpBridge::try_from_ctx(CctpCtx {
                usdc_ethereum: USDC_ADDRESS,
                usdc_base: USDC_ADDRESS,
                ethereum_wallet: create_test_wallet(&chains.ethereum_endpoint, &chains.bot_key),
                base_wallet: create_test_wallet(&chains.base_endpoint, &chains.bot_key),
                circle_api_base: st0x_bridge::cctp::CIRCLE_API_BASE.to_string(),
                token_messenger: chains.token_messenger,
                message_transmitter: chains.message_transmitter,
            })
            .unwrap(),
        );

        // Fund the Ethereum bot wallet.
        let deployer_key = B256::from_slice(chains.bot_key.as_slice());
        mint_usdc(
            &chains.ethereum_endpoint,
            &deployer_key,
            USDC_ADDRESS,
            chains.bot_address,
            amount_u256 * U256::from(10u64),
        )
        .await
        .unwrap();

        let server = MockServer::start();
        let alpaca_broker = InstrumentedAlpacaBroker::new(
            create_test_broker_service(&server).await,
            TelemetrySender::disabled(),
        );
        let alpaca_wallet = Arc::new(create_test_wallet_service(&server));
        let vault_service = RaindexService::new(
            create_test_wallet(&chains.base_endpoint, &chains.bot_key),
            RaindexContracts {
                inventory: ORDERBOOK_ADDRESS,
                orderbook: ORDERBOOK_ADDRESS,
            },
            chains.bot_address,
        );
        let cqrs = create_test_store_instance().await;
        let manager = CrossVenueCashTransfer::new(
            alpaca_broker,
            alpaca_wallet,
            Arc::clone(&cctp_bridge),
            Arc::new(vault_service),
            cqrs.clone(),
            chains.bot_address,
            TEST_VAULT_ID,
            &test_settlement_params(),
        );

        let id = UsdcRebalanceId(Uuid::new_v4());

        // Capture the current block BEFORE any burn so the scan window opens here.
        // No burn has been submitted yet, so find_recent_burn returns None.
        let ethereum_provider = ProviderBuilder::new()
            .connect(&chains.ethereum_endpoint)
            .await
            .unwrap();
        let from_block = ethereum_provider.get_block_number().await.unwrap();

        // Mine 2+ extra blocks so find_recent_burn's finality margin (SCAN_FINALITY_MARGIN=2)
        // is satisfied; otherwise the scan returns ScanInconclusive instead of None.
        ethereum_provider.anvil_mine(Some(3), None).await.unwrap();

        advance_to_bridging_submitting_alpaca_to_base(&cqrs, &id, usdc("100"), from_block).await;

        let nonce_before = ethereum_provider
            .get_transaction_count(chains.bot_address)
            .await
            .unwrap();

        let error = manager
            .resume_bridging_submitting_ethereum(&id, amount_u256, from_block, None)
            .await
            .unwrap_err();

        assert!(
            matches!(error, UsdcTransferError::BurnSubmitInconclusive { .. }),
            "resume with no recorded burn and none found on-chain must FAIL CLOSED, \
             never reburn; got: {error:?}"
        );

        // No burn was issued -- the bot wallet nonce is unchanged.
        let nonce_after = ethereum_provider
            .get_transaction_count(chains.bot_address)
            .await
            .unwrap();
        assert_eq!(
            nonce_after, nonce_before,
            "a fail-closed resume must NOT issue a burn (nonce unchanged); \
             nonce_before={nonce_before} nonce_after={nonce_after}"
        );

        // Aggregate stays at BridgingSubmitting for operator reconciliation.
        let state = cqrs.load(&id).await.unwrap().expect("aggregate exists");
        assert!(
            matches!(
                state,
                UsdcRebalance::BridgingSubmitting {
                    direction: RebalanceDirection::AlpacaToBase,
                    ..
                }
            ),
            "aggregate must stay at BridgingSubmitting on a fail-closed resume; got: {state:?}"
        );
    }

    /// Base-direction (hedging) counterpart of
    /// `resume_bridging_submitting_ethereum_fails_closed_when_no_recorded_burn`:
    /// when `pending_burn_tx` is None and `find_recent_burn` finds no prior burn,
    /// `resume_bridging_submitting` FAILS CLOSED (`BurnSubmitInconclusive`) rather
    /// than reburning. Reaching the resume path means a burn submission was already
    /// initiated (`BeginBridging`), so a broadcast may be in flight and an
    /// auto-reburn could double-burn.
    #[tokio::test]
    async fn resume_bridging_submitting_base_fails_closed_when_no_recorded_burn() {
        let (_anvil, endpoint, private_key) = setup_anvil();

        let server = MockServer::start();
        let alpaca_broker = InstrumentedAlpacaBroker::new(
            create_test_broker_service(&server).await,
            TelemetrySender::disabled(),
        );
        let alpaca_wallet = Arc::new(create_test_wallet_service(&server));
        let wallet = create_test_wallet(&endpoint, &private_key);
        let (cctp_bridge, vault_service) = create_test_onchain_services(wallet);
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
            &test_settlement_params(),
        );

        // The bridge's Base wallet (Anvil account 0) is the address a reburn would
        // send from, so its nonce is what must stay unchanged.
        let bridge_wallet = PrivateKeySigner::from_bytes(&private_key)
            .unwrap()
            .address();
        let provider = ProviderBuilder::new().connect(&endpoint).await.unwrap();
        let nonce_before = provider.get_transaction_count(bridge_wallet).await.unwrap();

        let id = UsdcRebalanceId(Uuid::new_v4());
        let amount = usdc("1");
        let amount_u256 = usdc_to_u256(amount).unwrap();

        // Capture the scan lower bound at the current head BEFORE any burn (none is
        // ever submitted here), then mine past the finality margin
        // (SCAN_FINALITY_MARGIN=2) so an empty scan resolves to Ok(None) rather than
        // ScanInconclusive -- driving the no-recorded-burn fail-closed branch.
        let from_block = provider.get_block_number().await.unwrap();
        provider.anvil_mine(Some(3), None).await.unwrap();

        advance_to_bridging_submitting_base_to_alpaca(&cqrs, &id, amount, from_block).await;

        let error = manager
            .resume_bridging_submitting(&id, amount_u256, from_block, None)
            .await
            .unwrap_err();

        assert!(
            matches!(error, UsdcTransferError::BurnSubmitInconclusive { .. }),
            "resume with no recorded burn and none found on-chain must FAIL CLOSED, \
             never reburn; got: {error:?}"
        );

        assert_eq!(
            provider.get_transaction_count(bridge_wallet).await.unwrap(),
            nonce_before,
            "a fail-closed resume must NOT issue a burn (nonce unchanged)"
        );

        let state = cqrs.load(&id).await.unwrap().expect("aggregate exists");
        assert!(
            matches!(
                state,
                UsdcRebalance::BridgingSubmitting {
                    direction: RebalanceDirection::BaseToAlpaca,
                    ..
                }
            ),
            "aggregate must stay at BridgingSubmitting on a fail-closed resume; got: {state:?}"
        );
    }

    /// `resume_bridging_submitting_ethereum` adopts an existing burn when given the
    /// nominal U256 as the scan key, without issuing a second burn transaction.
    ///
    /// This exercises the lower-level function directly. The full dispatch path
    /// (where `resume_alpaca_to_base` computes the scan key from `burn_amount` via
    /// `unwrap_or(nominal_u256)`) is covered separately by
    /// `resume_alpaca_to_base_with_persisted_burn_amount_scans_received_amount_not_nominal`.
    #[cfg(feature = "test-support")]
    #[tokio::test]
    async fn resume_bridging_submitting_ethereum_adopts_existing_burn_for_nominal_scan_key() {
        let chains = deploy_dual_chain_cctp().await;
        let nominal = usdc("100");
        let nominal_u256 = usdc_to_u256(nominal).unwrap();

        let cctp_bridge = Arc::new(
            CctpBridge::try_from_ctx(CctpCtx {
                usdc_ethereum: USDC_ADDRESS,
                usdc_base: USDC_ADDRESS,
                ethereum_wallet: create_test_wallet(&chains.ethereum_endpoint, &chains.bot_key),
                base_wallet: create_test_wallet(&chains.base_endpoint, &chains.bot_key),
                circle_api_base: st0x_bridge::cctp::CIRCLE_API_BASE.to_string(),
                token_messenger: chains.token_messenger,
                message_transmitter: chains.message_transmitter,
            })
            .unwrap(),
        );

        // Fund the Ethereum bot wallet with enough USDC to burn.
        let deployer_key = B256::from_slice(chains.bot_key.as_slice());
        mint_usdc(
            &chains.ethereum_endpoint,
            &deployer_key,
            USDC_ADDRESS,
            chains.bot_address,
            nominal_u256 * U256::from(10u64),
        )
        .await
        .unwrap();

        // Capture the scan lower bound BEFORE the burn.
        let from_block = cctp_bridge
            .source_block(BridgeDirection::EthereumToBase)
            .await
            .unwrap();

        // Submit the burn for the NOMINAL amount (no fee deduction in pre-migration path).
        let existing_burn = cctp_bridge
            .burn(
                BridgeDirection::EthereumToBase,
                nominal_u256,
                chains.bot_address,
            )
            .await
            .unwrap();

        // Build the manager.
        let server = MockServer::start();
        let alpaca_broker = InstrumentedAlpacaBroker::new(
            create_test_broker_service(&server).await,
            TelemetrySender::disabled(),
        );
        let alpaca_wallet = Arc::new(create_test_wallet_service(&server));
        let vault_service = RaindexService::new(
            create_test_wallet(&chains.base_endpoint, &chains.bot_key),
            RaindexContracts {
                inventory: ORDERBOOK_ADDRESS,
                orderbook: ORDERBOOK_ADDRESS,
            },
            chains.bot_address,
        );
        let cqrs = create_test_store_instance().await;
        let manager = CrossVenueCashTransfer::new(
            alpaca_broker,
            alpaca_wallet,
            Arc::clone(&cctp_bridge),
            Arc::new(vault_service),
            cqrs.clone(),
            chains.bot_address,
            TEST_VAULT_ID,
            &test_settlement_params(),
        );

        let id = UsdcRebalanceId(Uuid::new_v4());

        // Stage pre-migration `BridgingSubmitting { burn_amount: None }`.
        advance_to_bridging_submitting_alpaca_to_base(&cqrs, &id, nominal, from_block).await;

        // Pre-condition: aggregate has burn_amount: None.
        let staged = cqrs.load(&id).await.unwrap().expect("aggregate exists");
        let UsdcRebalance::BridgingSubmitting { burn_amount, .. } = staged else {
            panic!("Expected BridgingSubmitting, got: {staged:?}");
        };
        assert_eq!(
            burn_amount, None,
            "Pre-condition: BridgingSubmitting must carry burn_amount=None (pre-migration)"
        );

        // Capture nonce before resume -- adopt must not submit a tx.
        let ethereum_provider = ProviderBuilder::new()
            .connect(&chains.ethereum_endpoint)
            .await
            .unwrap();
        let nonce_before = ethereum_provider
            .get_transaction_count(chains.bot_address)
            .await
            .unwrap();

        // Call resume_bridging_submitting_ethereum with the value that
        // resume_alpaca_to_base derives: `burn_amount.map(usdc_to_u256).transpose()?
        //   .unwrap_or(nominal_u256)` == nominal_u256 when burn_amount=None.
        // The scan finds the existing nominal burn and adopts it without re-burning.
        let burn_receipt = manager
            .resume_bridging_submitting_ethereum(&id, nominal_u256, from_block, None)
            .await
            .unwrap();

        // Adopted: receipt points at the existing burn tx.
        assert_eq!(
            burn_receipt.tx, existing_burn.tx,
            "Resume with nominal_u256 (unwrap_or fallback) must adopt the existing burn tx"
        );
        assert_eq!(
            burn_receipt.amount, nominal_u256,
            "Adopted burn receipt must carry the nominal amount"
        );

        // No second burn submitted.
        let nonce_after = ethereum_provider
            .get_transaction_count(chains.bot_address)
            .await
            .unwrap();
        assert_eq!(
            nonce_before, nonce_after,
            "Pre-migration resume (burn_amount=None -> nominal scan) must adopt the \
             existing burn without submitting a second transaction (nonce unchanged)"
        );

        // Aggregate must reach Bridging (InitiateBridging emitted by record_cctp_burn).
        let final_state = cqrs.load(&id).await.unwrap().expect("aggregate exists");
        assert!(
            matches!(
                final_state,
                UsdcRebalance::Bridging {
                    direction: RebalanceDirection::AlpacaToBase,
                    burn_tx_hash,
                    ..
                } if burn_tx_hash == existing_burn.tx
            ),
            "Aggregate must reach Bridging with the adopted nominal burn tx; \
             got: {final_state:?}"
        );
    }

    /// Builds a manager whose CctpBridge points at an unreachable RPC endpoint,
    /// so any `ethereum_tx_confirmations` or `ethereum_tx_block` call fails with
    /// a transport error. Used to exercise the `SettlementCheckTransient` path.
    async fn build_manager_with_dead_rpc(
        server: &MockServer,
    ) -> (
        CrossVenueCashTransfer<
            RawPrivateKeyWallet<impl alloy::providers::Provider + Clone + use<>>,
        >,
        Arc<Store<UsdcRebalance>>,
    ) {
        let dead_endpoint = "http://127.0.0.1:1/";
        // A throwaway key -- no real txs will be sent against the dead endpoint.
        let dead_key = B256::from_slice(&[0x01u8; 32]);
        let wallet = create_test_wallet(dead_endpoint, &dead_key);

        let alpaca_broker = InstrumentedAlpacaBroker::new(
            create_test_broker_service(server).await,
            TelemetrySender::disabled(),
        );
        let alpaca_wallet = Arc::new(create_test_wallet_service(server));
        let (cctp_bridge, vault_service) = create_test_onchain_services(wallet.clone());
        let cqrs = create_test_store_instance().await;

        let manager = CrossVenueCashTransfer::new(
            alpaca_broker,
            alpaca_wallet,
            Arc::new(cctp_bridge),
            Arc::new(vault_service),
            cqrs.clone(),
            wallet.address(),
            TEST_VAULT_ID,
            &test_settlement_params(),
        );

        (manager, cqrs)
    }

    #[test]
    fn under_funded_vault_withdraw_surfaces_distinct_terminal_error() {
        // An atomic InsufficientVaultLiquidity revert withdrew nothing, so it
        // must surface the distinct terminal variant (which the job latches for
        // operator reconciliation, no auto-retry) rather than the opaque Vault
        // wrap the job redrives.
        let error = classify_vault_withdrawal_error(RaindexError::InsufficientVaultLiquidity {
            token: USDC_BASE,
            requested: U256::from(1_000_000u64),
            received: U256::from(400_000u64),
        });

        let UsdcTransferError::InsufficientVaultLiquidity {
            token,
            requested,
            received,
        } = error
        else {
            panic!("under-funded revert must surface the distinct terminal error, got: {error:?}");
        };
        assert_eq!(token, USDC_BASE);
        assert_eq!(requested, U256::from(1_000_000u64));
        assert_eq!(received, U256::from(400_000u64));
    }

    #[test]
    fn non_under_funded_vault_withdraw_wraps_opaquely_for_redrive() {
        // Any other RaindexError keeps the opaque Vault wrap so the caller's
        // normal redrive path still runs.
        let error = classify_vault_withdrawal_error(RaindexError::ZeroAmount);

        assert!(
            matches!(error, UsdcTransferError::Vault(RaindexError::ZeroAmount)),
            "non-under-funded errors must wrap opaquely as Vault, got: {error:?}"
        );
    }

    /// Hypothesis: in `continue_alpaca_to_base_from_withdrawal_complete` (durable
    /// re-check path), an `ethereum_tx_confirmations` RPC failure returns
    /// `SettlementCheckTransient` -- not `Cctp` -- and the aggregate stays at
    /// `WithdrawalComplete`. The job delayed-redrives instead of consuming the
    /// apalis retry budget.
    #[tokio::test]
    async fn durable_recheck_rpc_failure_yields_settlement_check_transient() {
        let server = MockServer::start();
        let (manager, cqrs) = build_manager_with_dead_rpc(&server).await;

        let id = UsdcRebalanceId(Uuid::new_v4());
        let amount = usdc("1");

        advance_to_withdrawal_complete_alpaca_to_base(&cqrs, &id, amount).await;

        // A non-zero tx hash forces the confirmation re-check; the dead RPC will fail.
        let fake_tx = TxHash::from_slice(&[0xabu8; 32]);

        let error = manager
            .continue_alpaca_to_base_from_withdrawal_complete(&id, amount, Some(fake_tx))
            .await
            .unwrap_err();

        assert!(
            matches!(error, UsdcTransferError::SettlementCheckTransient { .. }),
            "RPC failure in durable confirmation re-check must yield \
             SettlementCheckTransient, not Cctp; got: {error:?}"
        );

        let state = cqrs.load(&id).await.unwrap().expect("aggregate exists");
        assert!(
            matches!(
                state,
                UsdcRebalance::WithdrawalComplete {
                    direction: RebalanceDirection::AlpacaToBase,
                    ..
                }
            ),
            "Aggregate must stay at WithdrawalComplete after transient RPC failure; \
             got: {state:?}"
        );
    }

    /// Hypothesis: in `poll_and_confirm_withdrawal` (primary settlement gate),
    /// an `ethereum_tx_confirmations` RPC failure after `ConfirmWithdrawal` has
    /// been persisted returns `SettlementCheckTransient` -- not `Cctp` -- and
    /// the aggregate stays at `WithdrawalComplete`. The job delayed-redrives
    /// instead of consuming the apalis retry budget.
    #[tokio::test]
    async fn primary_gate_rpc_failure_after_confirm_withdrawal_yields_settlement_check_transient() {
        let server = MockServer::start();
        let (manager, cqrs) = build_manager_with_dead_rpc(&server).await;

        let id = UsdcRebalanceId(Uuid::new_v4());
        let amount = usdc("1");

        // A fake tx hash for Alpaca to return; the dead RPC will fail when
        // `poll_and_confirm_withdrawal` tries to check its confirmation depth.
        let fake_tx = TxHash::from_slice(&[0xabu8; 32]);

        let transfer_uuid = Uuid::new_v4();
        let _transfer_mock =
            mock_complete_withdrawal_with_tx(&server, transfer_uuid, Some(fake_tx));

        let withdrawal_id = AlpacaTransferId::from(transfer_uuid);

        // Stage aggregate at Withdrawing.
        cqrs.send(
            &id,
            UsdcRebalanceCommand::InitiateConversion {
                direction: RebalanceDirection::AlpacaToBase,
                amount,
                order_id: ClientOrderId::from_uuid(Uuid::new_v4()),
            },
        )
        .await
        .unwrap();
        cqrs.send(
            &id,
            UsdcRebalanceCommand::ConfirmConversion {
                conversion: par_conversion(amount),
            },
        )
        .await
        .unwrap();
        cqrs.send(
            &id,
            UsdcRebalanceCommand::Initiate {
                direction: RebalanceDirection::AlpacaToBase,
                amount,
                withdrawal: TransferRef::AlpacaId(withdrawal_id),
            },
        )
        .await
        .unwrap();

        let error = manager
            .poll_and_confirm_withdrawal(&id, &withdrawal_id, Utc::now())
            .await
            .unwrap_err();

        assert!(
            matches!(error, UsdcTransferError::SettlementCheckTransient { .. }),
            "RPC failure in primary settlement gate must yield SettlementCheckTransient, \
             not Cctp; got: {error:?}"
        );

        let state = cqrs.load(&id).await.unwrap().expect("aggregate exists");
        assert!(
            matches!(
                state,
                UsdcRebalance::WithdrawalComplete {
                    direction: RebalanceDirection::AlpacaToBase,
                    ..
                }
            ),
            "Aggregate must advance to WithdrawalComplete before the RPC failure so \
             apalis redrives enter the durable re-check path; got: {state:?}"
        );
    }
}

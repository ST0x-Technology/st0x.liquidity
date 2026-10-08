//! USDC cross-venue transfers between Alpaca and Base.
//!
//! [`CrossVenueCashTransfer`] drives USDC transfers in both directions through
//! direct `execute_alpaca_to_base` / `execute_base_to_alpaca` entry points,
//! with matching `resume_*` paths for apalis-driven crash recovery. Each
//! transfer handles USD/USDC conversion, withdrawal, CCTP bridging, and deposit.

mod corridors;
mod driver_pause;
mod job;
mod manager;
mod relay;

pub(crate) use corridors::{CorridorTransfer, UsdcCorridorTransfers};
pub(crate) use driver_pause::{
    DriverNotQuiesced, UsdcDriverGate, UsdcDriverPause, UsdcDriverPauseGuard, usdc_driver_pause,
};
pub(crate) use job::{
    ResumeAlpacaToBase, ResumeBaseToAlpaca, TransferUsdcToHedging, TransferUsdcToHedgingCtx,
    TransferUsdcToHedgingJobQueue, TransferUsdcToMarketMaking, TransferUsdcToMarketMakingCtx,
    TransferUsdcToMarketMakingJobQueue, UnderfundedAlertLatch, UnrecordedGuardRelease,
};
#[cfg(test)]
pub(crate) use manager::RecoveredCctpMint;
pub(crate) use manager::{
    CctpMintRecoveryError, RecheckUsdcDeposit, RecoverCctpMint, RecoveredMintAmounts,
    RestorePreparedDepositSends, RestoredDepositSends, UsdcBridgeHelper, UsdcRecheckError,
    u256_to_usdc,
};
pub use manager::{
    CrossVenueCashTransfer, DepositSendNotSuperseded, EthereumChainMissing,
    MarketMakingUsdcEndpoints, UsdcSettlementParams, deposit_send_required_confirmations,
    verify_deposit_send_superseded,
};
pub(crate) use relay::RelayHop;

use std::collections::BTreeSet;
use std::time::Duration;

use alloy::primitives::{Address, TxHash, U256};
use chrono::{DateTime, Utc};
use itertools::Itertools;
use rain_math_float::FloatError;
use thiserror::Error;
use tracing::error;

use st0x_bridge::cctp::CctpError;
use st0x_bridge::corridor::{HopKind, UsdcCorridor};
use st0x_bridge::relay::{
    BasisPointsOutOfRange, QuoteAcceptanceError, RelayBridgeError, RelayError,
};
use st0x_event_sorcery::SendError;
use st0x_execution::{
    AlpacaBrokerApiError, AlpacaWalletError, ClientOrderId, InvalidSharesError, NotPositive,
};
use st0x_finance::{Usd, Usdc, UsdcConversionError};
use st0x_raindex::RaindexError;

use crate::bot_gas::redrive::BotGasFailureClassifier;
use crate::inventory::InventoryViewError;
use crate::native_gas::GasReadinessFailure;
use crate::usdc_rebalance::{RebalanceDirection, UsdcRebalance, UsdcRebalanceId};

#[derive(Debug, Error)]
pub enum UsdcTransferError {
    #[error(transparent)]
    GasReadiness(#[from] GasReadinessFailure),
    #[error("Alpaca wallet error: {0}")]
    AlpacaWallet(#[from] AlpacaWalletError),
    #[error("Alpaca broker API error: {0}")]
    AlpacaBrokerApi(#[from] AlpacaBrokerApiError),
    #[error("CCTP bridge error: {0}")]
    Cctp(#[from] Box<CctpError>),
    /// Constructed only at the bot-gas enqueue site
    /// (`CrossVenueCashTransfer::enqueue_bot_gas_cost`), deliberately NOT via
    /// `#[from]`: the job layer treats this variant as best-effort bookkeeping
    /// (`Ok(())` plus an unbounded, budget-free redrive with no operator
    /// notifier), so a blanket conversion would let any unrelated queue-push
    /// failure in this module classify the same way and loop silently instead
    /// of paging. Mirrors `MintError::BotGasEnqueue` in
    /// `crate::rebalancing::equity`.
    #[error("Failed to enqueue bot-gas receipt cost recording: {0}")]
    BotGasEnqueue(crate::conductor::job::QueuePushError),
    /// Emitted by the two-phase burn path (`burn_recording_pending`) when the
    /// burn fails with a revert-class error: EVM execution reverts and pre-flight
    /// rejections that produce no on-chain state change. Safe to redrive via the
    /// scan-or-reburn path in `resume_bridging_submitting` /
    /// `resume_bridging_submitting_ethereum`.
    ///
    /// IMPORTANT: The double-burn safety guarantee does NOT come from this
    /// classification. It comes from `resume_bridging_submitting` /
    /// `resume_bridging_submitting_ethereum` scanning for an existing burn (via
    /// `find_recent_burn`) before attempting a new one, with the scan lower bound
    /// (`from_block`) durably recorded in the `BeginBridging` / `BridgingSubmitting`
    /// event before the burn call. This variant identifies errors where the EVM
    /// produced no lasting state change (post-mining reverts, pre-flight rejections).
    /// The safety guarantee is the scan, not this variant.
    #[error("CCTP burn revert (no on-chain state change): {0}")]
    BurnRevert(Box<CctpError>),
    #[error("Vault error: {0}")]
    Vault(#[from] RaindexError),
    /// A fail-closed lookup for a durably recorded `WithdrawalSubmitting`
    /// transfer could not determine whether the withdrawal mined. Transient
    /// transport failures and inconclusive scans delayed-redrive without
    /// consuming the Apalis retry budget; formal RPC rejections and other
    /// deterministic failures remain [`Self::Vault`].
    ///
    /// `initiated_at` is the durable `WithdrawalSubmitting.initiated_at`
    /// timestamp, threaded here so the job handler computes a durable deadline:
    /// before the deadline the redrive is silent; at or after it the operator is
    /// paged (guard held, redrive continues at a slower cadence) so an
    /// indefinitely-inconclusive scan cannot redrive forever unnoticed.
    #[error(
        "USDC rebalance {id}: vault withdrawal scan inconclusive or failed \
         transiently; will retry after delay"
    )]
    WithdrawalScanTransient {
        id: UsdcRebalanceId,
        initiated_at: DateTime<Utc>,
        #[source]
        source: Box<RaindexError>,
    },
    /// The shared inventory reverted a `withdraw4` because the vault could not
    /// cover the requested amount (a concurrent clear drained it), and a
    /// withdraw for this transfer may have been broadcast. Distinct from the
    /// opaque `Vault` wrap so it is not redriven blindly: the job latches the
    /// aggregate at `WithdrawalSubmitting` (no auto-retry). A resume only
    /// adopts a withdrawal mined after `from_block`; if the operator finds none
    /// on chain, they fail the transfer with `fail-usdc-transfer`.
    #[error(
        "USDC rebalance {id}: inventory vault under-funded on withdraw of {token}: requested \
         {requested}, vault could cover only {received}; a withdraw may have been broadcast \
         after block {from_block}, latched for operator reconciliation"
    )]
    InsufficientVaultLiquidity {
        id: UsdcRebalanceId,
        from_block: u64,
        token: Address,
        requested: U256,
        received: U256,
    },
    /// The shared inventory could not cover the withdraw, and the evm layer
    /// proved the withdraw was rejected before broadcast. `RejectWithdrawal`
    /// is committed: the transfer is failed, nothing left the vault, and the
    /// guard clears so rebalancing can plan again.
    #[error(
        "USDC rebalance {id}: inventory vault under-funded on withdraw of {token}: requested \
         {requested}, vault could cover only {received}; rejected before broadcast, transfer \
         failed"
    )]
    WithdrawalRejectedUnderfunded {
        id: UsdcRebalanceId,
        token: Address,
        requested: U256,
        received: U256,
    },
    #[error("Aggregate error: {0}")]
    Aggregate(Box<SendError<UsdcRebalance>>),
    #[error("Withdrawal failed with terminal status: {status}")]
    WithdrawalFailed { status: String },
    #[error("Deposit failed with terminal status: {status}")]
    DepositFailed { status: String },
    #[error("USDC conversion error: {0}")]
    UsdcConversion(#[from] UsdcConversionError),
    #[error("Float operation error: {0}")]
    Float(#[from] FloatError),
    #[error("Invalid shares: {0}")]
    InvalidShares(#[from] InvalidSharesError),
    #[error(transparent)]
    NotPositive(#[from] NotPositive<Usdc>),
    /// The USD side of the same guard, produced when the AlpacaToBase buy is
    /// sized at zero or below; refused before any aggregate event exists.
    #[error(transparent)]
    NotPositiveUsd(#[from] NotPositive<Usd>),
    #[error("inventory capacity calculation failed: {0}")]
    InventoryView(#[from] InventoryViewError),
    #[error("Alpaca did not report withdrawable cash while resizing USDC rebalance {id}")]
    WithdrawableCashUnavailable { id: UsdcRebalanceId },
    #[error(
        "USDC rebalance {id} resized USD notional {available} is below the Alpaca-to-Base minimum {minimum}"
    )]
    ResizedConversionBelowMinimum {
        id: UsdcRebalanceId,
        available: Usd,
        minimum: Usd,
    },
    #[error(
        "Conversion order {order_id} filled but \
         filled_quantity is missing"
    )]
    MissingFilledQuantity { order_id: ClientOrderId },
    #[error(
        "Conversion order {order_id} filled but \
         filled_average_price is missing"
    )]
    MissingFilledAveragePrice { order_id: ClientOrderId },
    #[error(
        "USDC rebalance {id} conversion could not be completed on resume \
         (order not found at Alpaca or terminally failed); failed for \
         operator reconciliation"
    )]
    ResumeIndeterminateConversion { id: UsdcRebalanceId },
    /// A USDC conversion order placement (`convert_usdc_usd`) failed with a
    /// classified broker rate-limit (429). Conversion placement fails fast
    /// unconditionally on ANY error -- `FailConversion` has already been sent
    /// (recorded in the event's `reason`) by the time this is returned --
    /// but a 429 specifically must not surface as `AlpacaBrokerApi` (which
    /// would keep the underlying error as a downcastable `#[source]`): the
    /// job's backpressure classifier (`find_backpressure`, which walks the
    /// `.source()` chain) would then reschedule an already-terminalized
    /// aggregate instead of treating this as the terminal failure it is.
    /// Retrying a conversion placement risks submitting the order twice
    /// against real money, so this variant deliberately carries no
    /// `#[source]`. Every other placement failure (a non-429 broker error, a
    /// terminally rejected/canceled/expired order, ...) keeps surfacing the
    /// original `AlpacaBrokerApi` variant instead, since `find_backpressure`
    /// already classifies those as non-backpressure and preserving the
    /// specific failure reason is useful for operators and callers.
    #[error(
        "USDC rebalance {id} conversion order placement failed; recorded as \
         ConversionFailed for operator reconciliation (not retriable)"
    )]
    ConversionPlacementFailed { id: UsdcRebalanceId },
    #[error(
        "USDC rebalance {id} attestation polling timed out; retrying until \
         attestation retry deadline"
    )]
    AttestationTimedOut { id: UsdcRebalanceId },
    #[error(
        "USDC rebalance {id} attestation retry deadline elapsed; failed for \
         operator reconciliation"
    )]
    AttestationRetryDeadlineElapsed { id: UsdcRebalanceId },
    /// Alpaca withdrawal polling returned an indeterminate result: the poll timed
    /// out or returned a transport/API error without observing a terminal status
    /// (`Complete` or `Failed`). The aggregate stays in `Withdrawing` (guard
    /// held, Alpaca transfer ID recorded). This is NOT a terminal failure: a
    /// delayed redrive re-polls the same Alpaca transfer ID (idempotent). Only
    /// `TransferStatus::Failed` with no tx hash (Alpaca's determinate terminal
    /// for a failed withdrawal that did not broadcast on-chain) produces
    /// `FailWithdrawal`; `Failed` with a tx hash stays indeterminate.
    ///
    /// Mirrors `SettlementCheckTransient` / `WithdrawalTxUnderconfirmed` in
    /// redrive semantics: unbounded, returns `Ok` from the job.
    ///
    /// `initiated_at` is the `Withdrawing.initiated_at` timestamp from the
    /// aggregate, threaded here so the job handler can compute a durable
    /// deadline: before the deadline only a warn log fires; at or after the
    /// deadline the operator is paged (while the guard stays held and
    /// re-polling continues).
    #[error(
        "USDC rebalance {id}: Alpaca withdrawal polling inconclusive \
         (timeout or transient error); transfer may still be in progress \
         (Alpaca transfer ID preserved in Withdrawing state)"
    )]
    WithdrawalPollInconclusive {
        id: UsdcRebalanceId,
        initiated_at: DateTime<Utc>,
        #[source]
        source: AlpacaWalletError,
    },
    #[error(
        "USDC rebalance {id} attestation retry deadline duration {retry_deadline:?} \
         cannot be represented as an absolute timestamp"
    )]
    AttestationRetryDeadlineOverflow {
        id: UsdcRebalanceId,
        retry_deadline: Duration,
    },
    #[error(
        "USDC rebalance {id} recorded cctp_nonce {recorded} does not match nonce \
         {reconstructed} of the persisted message envelope or the Circle re-poll; failed \
         for operator reconciliation rather than minting against an unverifiable nonce"
    )]
    AttestationNonceMismatch {
        id: UsdcRebalanceId,
        recorded: alloy::primitives::B256,
        reconstructed: alloy::primitives::B256,
    },
    #[error("USDC rebalance {id} cannot resume: aggregate is in terminal failure state")]
    PreviouslyFailedAggregate { id: UsdcRebalanceId },
    /// A state the service's hop never reaches (a swap state on CCTP, a
    /// burn state on Relay): the corridor check passed, so the store holds a
    /// state its corridor cannot reach. Refused before any call.
    #[error("USDC rebalance {id} is in {state}, which the {hop} hop never reaches")]
    StateOffHop {
        id: UsdcRebalanceId,
        state: &'static str,
        hop: HopKind,
    },
    #[error(transparent)]
    BasisPoints(#[from] BasisPointsOutOfRange),
    #[error("Relay API error: {0}")]
    RelayApi(#[from] Box<RelayError>),
    #[error("Relay bridge error: {0}")]
    RelayBridge(#[from] Box<RelayBridgeError>),
    /// Relay's quote is outside the corridor's bounds.
    #[error("USDC rebalance {id}: Relay quote refused: {source}")]
    SwapQuoteOutOfBounds {
        id: UsdcRebalanceId,
        #[source]
        source: QuoteAcceptanceError,
    },
    /// Another send took a nonce between the swap's approve and deposit: the
    /// approve went out alone and the next attempt signs the deposit again.
    #[error("USDC rebalance {id}: another send split the swap's approve and deposit; retrying")]
    SwapPairSplit { id: UsdcRebalanceId },
    /// The task that signs and persists the swap pair panicked. A pair it
    /// persisted is sent by the retry; nonces it reserved and did not persist
    /// stall later sends from the chain wallet until a restart.
    #[error("USDC rebalance {id}: the Relay pair prepare task panicked")]
    SwapPrepareTaskPanicked { id: UsdcRebalanceId },
    /// The work before and around the swap pair's persist outlasted the
    /// corridor's `quote_max_age`, so the chain wallet's prepare lock is
    /// released. A pair signed after it is discarded when its signing returns.
    #[error("USDC rebalance {id}: the Relay pair prepare outlasted {timeout:?}; retrying")]
    SwapPrepareTimedOut {
        id: UsdcRebalanceId,
        timeout: std::time::Duration,
    },
    /// The recorded quote is past its deadline or older than the corridor's
    /// `quote_max_age`: nothing was signed and the transfer holds its guard
    /// at `SwapQuoted`.
    #[error(
        "USDC rebalance {id}: Relay quote from {quoted_at} (deadline {deadline}) expired; \
         nothing was signed"
    )]
    SwapQuoteExpired {
        id: UsdcRebalanceId,
        quoted_at: DateTime<Utc>,
        deadline: DateTime<Utc>,
    },
    /// Relay has not paid the deposit yet, or its payment is not on chain
    /// with its confirmations yet: the job reads the status again later.
    #[error("USDC rebalance {id}: the Relay deposit of {deposited_at} is not paid yet")]
    RelayFillPending {
        id: UsdcRebalanceId,
        deposited_at: DateTime<Utc>,
    },
    /// The persisted swap deposit mined and reverted, moving nothing; the
    /// next attempt re-quotes, or redeposits once the corridor's budget of
    /// reverted deposits is spent.
    #[error("USDC rebalance {id}: Relay deposit {deposit_tx} reverted")]
    SwapDepositReverted {
        id: UsdcRebalanceId,
        deposit_tx: TxHash,
    },
    /// The fill or refund Relay names does not prove on chain: the transfer
    /// holds its guard and the status is read again later.
    #[error("USDC rebalance {id}: the Relay payment does not prove on chain: {source}")]
    SwapPaymentUnverified {
        id: UsdcRebalanceId,
        #[source]
        source: Box<RelayBridgeError>,
    },
    #[error(
        "USDC transfer corridor mismatch: transfer {id} runs on the {recorded} corridor, \
         this service serves {}; left untouched for the operator",
        .served.iter().join(", ")
    )]
    CorridorMismatch {
        id: UsdcRebalanceId,
        recorded: UsdcCorridor,
        served: BTreeSet<UsdcCorridor>,
        /// Whether the transfer still holds the rebalance guard, so a build
        /// that serves `recorded` must still resume it.
        holds_guard: bool,
    },
    #[error(
        "USDC transfer corridor mismatch: transfer {id} asks for the {requested} corridor, \
         this service serves {}; nothing was recorded",
        .served.iter().join(", ")
    )]
    CorridorNotServed {
        id: UsdcRebalanceId,
        requested: UsdcCorridor,
        served: BTreeSet<UsdcCorridor>,
    },
    #[error(
        "USDC rebalance {id} DepositInitiated has non-onchain deposit ref; \
         BaseToAlpaca always records the mint tx as OnchainTx"
    )]
    DepositRefMustBeOnchain { id: UsdcRebalanceId },
    #[error(
        "USDC rebalance {id} cannot resume via Base->Alpaca entrypoint: \
         aggregate direction is {direction:?}"
    )]
    ResumeDirectionMismatch {
        id: UsdcRebalanceId,
        direction: RebalanceDirection,
    },
    #[error(
        "USDC rebalance {id} adopted a withdrawal that realized {withdrawn} but \
         {requested} was requested; failed for operator reconciliation rather \
         than burning more than was withdrawn"
    )]
    AdoptedWithdrawalAmountMismatch {
        id: UsdcRebalanceId,
        withdrawn: alloy::primitives::U256,
        requested: alloy::primitives::U256,
    },
    /// The USD->USDC conversion settled below Alpaca's withdrawal minimum,
    /// which the trigger can only enforce on the amount it *requests*: a
    /// stalled conversion whose remainder was cancelled delivers whatever
    /// filled, and any non-zero fill under the floor produces a withdrawal
    /// Alpaca rejects. Named here rather than left to that rejection so the
    /// recorded cause is the short conversion, and so the amount now sitting
    /// in the Alpaca crypto wallet is in the failure itself.
    #[error(
        "USDC rebalance {id} converted only {converted}, below Alpaca's {minimum} \
         withdrawal minimum; no withdrawal was attempted and the converted USDC is \
         held in the Alpaca crypto wallet pending operator reconciliation"
    )]
    ConversionBelowWithdrawalMinimum {
        id: UsdcRebalanceId,
        converted: Usdc,
        minimum: Usdc,
    },
    /// The conversion order's outcome could not be established: the deadline
    /// cancel was never confirmed settled, or the broker stopped recognising
    /// the order. The order may still be live and still fill, so the
    /// rebalance is deliberately NOT terminalized -- it stays latched at
    /// `Converting` with the in-flight guard held, the same fail-closed
    /// treatment an inconclusive burn submission gets. Terminalizing instead
    /// would release the guard and re-arm a second conversion for the same
    /// imbalance while real money can still move.
    #[error(
        "USDC rebalance {id} conversion outcome could not be established, so the \
         rebalance is latched for operator reconciliation: {source}"
    )]
    ConversionOutcomeUnresolved {
        id: UsdcRebalanceId,
        /// Boxed to keep it off every `Result<_, UsdcTransferError>` in this
        /// module: the broker error is by far the largest variant payload, and
        /// inlining it widens the error type for callers that can never return
        /// this variant.
        #[source]
        source: Box<AlpacaBrokerApiError>,
    },
    /// The post-deposit USDC->USD conversion sold less USDC than was
    /// deposited, because a stalled order's remainder was cancelled. The
    /// unconverted USDC stays in the Alpaca crypto wallet, which the offchain
    /// cash inventory does not read, so the rebalance is failed for
    /// reconciliation rather than confirmed as a success that moved only part
    /// of the cash.
    #[error(
        "USDC rebalance {id} post-deposit conversion sold only {converted}; {unconverted} \
         is stranded as USDC in the Alpaca crypto wallet and needs operator reconciliation"
    )]
    PostDepositConversionShortFill {
        id: UsdcRebalanceId,
        converted: Usdc,
        unconverted: Usdc,
    },
    #[error(
        "USDC rebalance {id} Withdrawing has non-Alpaca withdrawal ref; \
         AlpacaToBase always records the Alpaca transfer ID"
    )]
    WithdrawalRefMustBeAlpacaId {
        id: crate::usdc_rebalance::UsdcRebalanceId,
    },
    /// An AlpacaToBase withdrawal has no tx hash to credit the delivered USDC
    /// from: a legacy aggregate, or Alpaca never reported the hash before the
    /// settlement deadline. The manager moves the aggregate to `BridgingFailed`.
    #[error(
        "USDC rebalance {id}: no recorded withdrawal tx hash; cannot credit \
         Ethereum USDC to the Alpaca withdrawal"
    )]
    WithdrawalTxMissing { id: UsdcRebalanceId },
    /// The withdrawal tx paid the market-maker wallet nothing, or more than
    /// the nominal withdrawal, so it is not this withdrawal's delivery. The
    /// aggregate is moved to `BridgingFailed` for operator reconciliation; no
    /// burn is attempted.
    #[error(
        "USDC rebalance {id}: withdrawal tx {tx} credited {credited} base units to \
         the market-maker wallet against nominal {nominal}; failed for operator \
         reconciliation"
    )]
    WithdrawalCreditMismatch {
        id: UsdcRebalanceId,
        tx: TxHash,
        credited: U256,
        nominal: Usdc,
    },
    /// Another `UsdcRebalance` already recorded this withdrawal tx, so it
    /// cannot be this withdrawal's delivery. The aggregate is moved to
    /// `BridgingFailed` for operator reconciliation without recording the tx.
    #[error(
        "USDC rebalance {id}: withdrawal tx {tx} is already recorded by USDC rebalance \
         {recorded_by}; failed for operator reconciliation"
    )]
    WithdrawalTxAlreadyRecorded {
        id: UsdcRebalanceId,
        tx: TxHash,
        /// The other aggregate's id as persisted in the event store.
        recorded_by: String,
    },
    /// The event store could not be read to check that no other transfer
    /// recorded the withdrawal tx. The aggregate stays `Withdrawing`, so a
    /// retry re-polls the same Alpaca transfer.
    #[error(
        "USDC rebalance {id}: could not check whether another transfer recorded its withdrawal tx"
    )]
    WithdrawalTxLookupFailed {
        id: UsdcRebalanceId,
        #[source]
        source: sqlx::Error,
    },
    /// The withdrawal tx receipt was read, but its USDC credit cannot be
    /// computed (an undecodable Transfer log, or a sum that overflows). A reread
    /// cannot change the receipt, so the aggregate is moved to `BridgingFailed`
    /// for operator reconciliation; no burn is attempted.
    #[error(
        "USDC rebalance {id}: USDC credit of withdrawal tx {tx} cannot be computed; \
         failed for operator reconciliation"
    )]
    WithdrawalCreditUnreadable {
        id: UsdcRebalanceId,
        tx: TxHash,
        #[source]
        source: Box<CctpError>,
    },
    /// The retryable settlement wait outlived the configured settlement
    /// retry deadline (anchored on the durable `WithdrawalComplete`
    /// `confirmed_at`). `FailBridging` has already been sent by the time
    /// this is returned: the aggregate is a pre-burn `BridgingFailed`,
    /// reconcile-eligible because the withdrawal completed and the funds
    /// are off Alpaca. The job pages the operator and must NOT redrive.
    #[error(
        "USDC rebalance {id}: withdrawal settlement retry deadline elapsed; \
         bridge marked failed for operator reconciliation \
         (`transfer reconcile --kind usdc`)"
    )]
    SettlementRetryDeadlineElapsed { id: UsdcRebalanceId },
    #[error(
        "USDC rebalance {id}: withdrawal tx {tx} has only {actual} confirmations, \
         need {required}; waiting for on-chain settlement"
    )]
    WithdrawalTxUnderconfirmed {
        id: UsdcRebalanceId,
        tx: TxHash,
        required: u64,
        actual: u64,
    },
    /// No `[chains.ethereum]` entry, so the withdrawal tx has no depth to
    /// reach: refused before any burn.
    #[error(transparent)]
    EthereumChainMissing(#[from] EthereumChainMissing),
    /// An RPC call in the settlement phase (confirmation re-check, balance read,
    /// or burn scan) failed transiently. The aggregate is in
    /// `WithdrawalComplete` or `BridgingSubmitting` -- a durable, resumable
    /// state -- so this is safe to delayed-redrive exactly like
    /// `WithdrawalTxUnderconfirmed`.
    #[error(
        "USDC rebalance {id}: settlement-phase RPC check failed transiently; \
         will retry after delay"
    )]
    SettlementCheckTransient {
        id: UsdcRebalanceId,
        #[source]
        source: Box<CctpError>,
    },
    /// A post-burn CCTP mint whose recovery could not be resolved: the recovery
    /// window expired without ever getting a conclusive `usedNonces()` read, the
    /// nonce read consumed but its receipt could not be reconstructed, or the
    /// pre-mint lookup of the nonce's mint (`find_attested_mint`, run on
    /// `Attested` resume before minting is attempted) failed. The aggregate
    /// stays in whichever durable pre-mint state it was already in
    /// (`Bridging`, `AwaitingAttestation`, `Attested`, or a post-burn
    /// `BridgingFailed`), so this is safe to delayed-redrive: declaring a
    /// terminal failure here would strand the rebalancing guard on state that
    /// was never actually observed, or on funds that may have already moved.
    ///
    /// Mirrors `WithdrawalPollInconclusive` in redrive semantics: unbounded,
    /// returns `Ok` from the job. `initiated_at` is threaded from the
    /// aggregate's current state (all four reachable states persist it) so the
    /// job handler can compute a durable deadline: before the deadline only a
    /// warn log fires; at or after the deadline the operator is paged on every
    /// redrive while the guard stays held and redriving continues.
    #[error(
        "USDC rebalance {id}: CCTP mint recovery inconclusive (nonce state \
         unknown or receipt unreconstructible); mint may already have landed"
    )]
    MintRecoveryInconclusive {
        id: UsdcRebalanceId,
        initiated_at: DateTime<Utc>,
        #[source]
        source: Box<CctpError>,
    },
    /// The detached submit-and-record burn task (spawned so a cancelling job
    /// timeout cannot drop the broadcast->record critical section) panicked and
    /// failed to join. The burn may or may not have broadcast. TERMINAL and
    /// fail-closed at the job layer: the transfer latches at `BridgingSubmitting`
    /// for operator reconciliation rather than auto-redriving, because a redrive
    /// with no recorded tx would fall to the mempool-blind scan and could reburn a
    /// still-pending burn.
    #[error("USDC rebalance {id}: burn submit-and-record task panicked")]
    BurnRecordTaskFailed { id: UsdcRebalanceId },
    /// The burn was broadcast but `RecordPendingBurn` failed to commit after
    /// retries, so the burn tx hash was NOT durably recorded. TERMINAL and
    /// fail-closed at the job layer: rather than proceeding to confirm (or letting
    /// an Apalis retry treat the burn as un-broadcast and reburn off a
    /// mempool-blind scan), the transfer latches at `BridgingSubmitting` for
    /// operator reconciliation. The broadcast burn hash is in the logs; manual
    /// on-chain verification is required before any further action.
    #[error(
        "USDC rebalance {id}: burn broadcast but RecordPendingBurn could not be \
         committed; failed for operator reconciliation (burn tx {burn_tx})"
    )]
    BurnRecordFailed {
        id: UsdcRebalanceId,
        burn_tx: TxHash,
    },
    /// The CCTP burn submission did not return a usable tx hash: it either timed
    /// out (the broadcast may still have reached the network) or failed with a
    /// non-revert (transport/RPC) error after the request was sent. The burn may
    /// or may not be on-chain, so its fate is INCONCLUSIVE. TERMINAL and
    /// fail-closed at the job layer: the transfer latches at `BridgingSubmitting`
    /// for operator reconciliation rather than auto-reburning, since a reburn
    /// could double-burn if the original submission did land. A clean revert (no
    /// funds moved) is NOT this error -- that stays on the bounded revert-redrive
    /// path.
    #[error(
        "USDC rebalance {id}: CCTP burn submission inconclusive (timed out or \
         non-revert transport error after broadcast); failed for operator \
         reconciliation (verify on-chain before any reburn)"
    )]
    BurnSubmitInconclusive { id: UsdcRebalanceId },
    /// A durably-recorded burn tx was classified `Dropped` (no receipt and absent
    /// from the mempool past the grace window): the recorded burn is not mined and
    /// is no longer in the mempool. TERMINAL: once a burn tx hash is durably
    /// recorded the system NEVER auto-issues a second burn for an ambiguous
    /// "dropped" classification (a load-balanced RPC could misreport a still-pending
    /// burn as dropped, causing a double-burn). The operator must manually verify
    /// on-chain whether the burn landed before any reburn.
    #[error(
        "USDC rebalance {id}: recorded burn tx {burn_tx} not mined and no longer in \
         the mempool; manual on-chain verification required before any reburn"
    )]
    BurnTxDropped {
        id: UsdcRebalanceId,
        burn_tx: TxHash,
    },
    /// A Base->Alpaca deposit send cannot be resolved automatically, and a
    /// resend could move the minted USDC twice. `FailDeposit` is already
    /// committed; the job pages and does not retry.
    #[error(
        "USDC rebalance {id}: {cause}; deposit marked failed for operator \
         reconciliation ({})",
        .cause.operator_step()
    )]
    DepositSendUnresolved {
        id: UsdcRebalanceId,
        cause: UnresolvedDepositSend,
    },
    /// The signed Base->Alpaca deposit send is persisted but not confirmed
    /// yet: its broadcast was not accepted, its receipt is not known, or it
    /// was dropped. The aggregate stays `Bridged` and the job redrives, which
    /// broadcasts the same signed bytes again; it can never send twice.
    /// `prepared_at` anchors the durable operator alert deadline.
    #[error("USDC rebalance {id}: signed deposit send {tx} is not confirmed yet: {cause}")]
    DepositSendReconciliationPending {
        id: UsdcRebalanceId,
        tx: TxHash,
        prepared_at: DateTime<Utc>,
        cause: DepositSendPending,
    },
    /// The task that signs and persists the deposit send panicked. The signed
    /// send is either persisted, and the retry broadcasts it, or it is not,
    /// and the retry signs one; nothing was broadcast.
    #[error("USDC rebalance {id}: the deposit send prepare task panicked; nothing was broadcast")]
    DepositSendTaskPanicked { id: UsdcRebalanceId },
    /// The event store could not be read to check whether another transfer
    /// claims a same-amount send found on resume. Nothing was sent; the retry
    /// scans again.
    #[error(
        "USDC rebalance {id}: could not check whether another transfer claims deposit send {tx}"
    )]
    DepositSendLookup {
        id: UsdcRebalanceId,
        tx: TxHash,
        #[source]
        source: sqlx::Error,
    },
}

/// Why a signed Base->Alpaca deposit send is not confirmed yet.
#[derive(Debug, Error)]
pub enum DepositSendPending {
    /// The RPC did not accept the broadcast. The node may not see the tx
    /// yet, or another tx took its nonce.
    #[error("its broadcast was not accepted: {0}")]
    Broadcast(#[source] Box<CctpError>),
    /// Its receipt could not be read to the required confirmations.
    #[error("its confirmation is not known: {0}")]
    Confirmation(#[source] Box<CctpError>),
    /// Absent from the mempool, never mined.
    #[error("it was dropped from the mempool")]
    Dropped,
}

/// Why a Base->Alpaca deposit send cannot be resolved automatically.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum UnresolvedDepositSend {
    /// No send was recorded, yet a send of the same amount from the wallet
    /// to the deposit address landed after the mint. It can be another
    /// transfer's send through the shared wallet, so it is never adopted.
    #[error(
        "no deposit send was recorded, but send {tx} of the same amount to the \
         Alpaca deposit address landed after the mint and may belong to another transfer"
    )]
    UnrecordedSend { tx: TxHash },
    #[error("the signed deposit send {tx} was mined reverted")]
    SignedSendReverted { tx: TxHash },
}

impl UnresolvedDepositSend {
    /// The operator command that settles a deposit failed for this cause.
    pub(crate) const fn operator_step(self) -> &'static str {
        match self {
            // No send is recorded on the transfer: the operator must find
            // this transfer's own send on chain, if there is one.
            Self::UnrecordedSend { .. } => {
                "`transfer recheck --kind usdc --deposit-tx <hash>` with this transfer's own \
                 send if Alpaca credited it, else `transfer reconcile --kind usdc`"
            }
            Self::SignedSendReverted { .. } => {
                "the send moved no USDC; settle the minted USDC with `transfer reconcile --kind usdc`"
            }
        }
    }
}

impl UsdcTransferError {
    pub(crate) fn gas_readiness_retry_interval(&self) -> Option<Duration> {
        match self {
            Self::GasReadiness(failure) => failure.retry_interval(),
            Self::AlpacaWallet(_)
            | Self::AlpacaBrokerApi(_)
            | Self::Cctp(_)
            | Self::BurnRevert(_)
            | Self::Vault(_)
            | Self::InsufficientVaultLiquidity { .. }
            | Self::WithdrawalRejectedUnderfunded { .. }
            | Self::Aggregate(_)
            | Self::WithdrawalFailed { .. }
            | Self::DepositFailed { .. }
            | Self::UsdcConversion(_)
            | Self::ConversionPlacementFailed { .. }
            | Self::Float(_)
            | Self::InvalidShares(_)
            | Self::NotPositive(_)
            | Self::NotPositiveUsd(_)
            | Self::InventoryView(_)
            | Self::WithdrawableCashUnavailable { .. }
            | Self::ResizedConversionBelowMinimum { .. }
            | Self::MissingFilledQuantity { .. }
            | Self::MissingFilledAveragePrice { .. }
            | Self::ResumeIndeterminateConversion { .. }
            | Self::ConversionBelowWithdrawalMinimum { .. }
            | Self::BotGasEnqueue(_)
            | Self::ConversionOutcomeUnresolved { .. }
            | Self::PostDepositConversionShortFill { .. }
            | Self::AttestationTimedOut { .. }
            | Self::AttestationRetryDeadlineElapsed { .. }
            | Self::SettlementRetryDeadlineElapsed { .. }
            | Self::WithdrawalPollInconclusive { .. }
            | Self::AttestationRetryDeadlineOverflow { .. }
            | Self::AttestationNonceMismatch { .. }
            | Self::PreviouslyFailedAggregate { .. }
            | Self::CorridorMismatch { .. }
            | Self::CorridorNotServed { .. }
            | Self::DepositRefMustBeOnchain { .. }
            | Self::ResumeDirectionMismatch { .. }
            | Self::AdoptedWithdrawalAmountMismatch { .. }
            | Self::WithdrawalRefMustBeAlpacaId { .. }
            | Self::WithdrawalTxMissing { .. }
            | Self::WithdrawalCreditMismatch { .. }
            | Self::WithdrawalCreditUnreadable { .. }
            | Self::WithdrawalTxAlreadyRecorded { .. }
            | Self::WithdrawalTxLookupFailed { .. }
            | Self::WithdrawalTxUnderconfirmed { .. }
            | Self::WithdrawalScanTransient { .. }
            | Self::SettlementCheckTransient { .. }
            | Self::MintRecoveryInconclusive { .. }
            | Self::BurnRecordTaskFailed { .. }
            | Self::BurnRecordFailed { .. }
            | Self::BurnSubmitInconclusive { .. }
            | Self::BurnTxDropped { .. }
            | Self::DepositSendUnresolved { .. }
            | Self::DepositSendReconciliationPending { .. }
            | Self::DepositSendTaskPanicked { .. }
            | Self::DepositSendLookup { .. }
            | Self::StateOffHop { .. }
            | Self::BasisPoints(_)
            | Self::RelayApi(_)
            | Self::RelayBridge(_)
            | Self::SwapQuoteOutOfBounds { .. }
            | Self::SwapPairSplit { .. }
            | Self::SwapPrepareTaskPanicked { .. }
            | Self::SwapPrepareTimedOut { .. }
            | Self::SwapQuoteExpired { .. }
            | Self::RelayFillPending { .. }
            | Self::SwapDepositReverted { .. }
            | Self::SwapPaymentUnverified { .. }
            | Self::EthereumChainMissing(_) => None,
        }
    }
}

impl BotGasFailureClassifier for UsdcTransferError {
    fn is_bot_gas_enqueue_failure(&self) -> bool {
        match self {
            Self::BotGasEnqueue(_) => true,
            Self::GasReadiness(_)
            | Self::AlpacaWallet(_)
            | Self::AlpacaBrokerApi(_)
            | Self::Cctp(_)
            | Self::BurnRevert(_)
            | Self::Vault(_)
            | Self::InsufficientVaultLiquidity { .. }
            | Self::WithdrawalRejectedUnderfunded { .. }
            | Self::Aggregate(_)
            | Self::WithdrawalFailed { .. }
            | Self::DepositFailed { .. }
            | Self::UsdcConversion(_)
            | Self::ConversionPlacementFailed { .. }
            | Self::Float(_)
            | Self::InvalidShares(_)
            | Self::NotPositive(_)
            | Self::NotPositiveUsd(_)
            | Self::InventoryView(_)
            | Self::WithdrawableCashUnavailable { .. }
            | Self::ResizedConversionBelowMinimum { .. }
            | Self::MissingFilledQuantity { .. }
            | Self::MissingFilledAveragePrice { .. }
            | Self::ResumeIndeterminateConversion { .. }
            | Self::ConversionBelowWithdrawalMinimum { .. }
            | Self::ConversionOutcomeUnresolved { .. }
            | Self::PostDepositConversionShortFill { .. }
            | Self::AttestationTimedOut { .. }
            | Self::AttestationRetryDeadlineElapsed { .. }
            | Self::WithdrawalPollInconclusive { .. }
            | Self::AttestationRetryDeadlineOverflow { .. }
            | Self::AttestationNonceMismatch { .. }
            | Self::PreviouslyFailedAggregate { .. }
            | Self::CorridorMismatch { .. }
            | Self::CorridorNotServed { .. }
            | Self::DepositRefMustBeOnchain { .. }
            | Self::ResumeDirectionMismatch { .. }
            | Self::AdoptedWithdrawalAmountMismatch { .. }
            | Self::WithdrawalRefMustBeAlpacaId { .. }
            | Self::WithdrawalTxMissing { .. }
            | Self::WithdrawalCreditMismatch { .. }
            | Self::WithdrawalCreditUnreadable { .. }
            | Self::WithdrawalTxAlreadyRecorded { .. }
            | Self::WithdrawalTxLookupFailed { .. }
            | Self::SettlementRetryDeadlineElapsed { .. }
            | Self::WithdrawalTxUnderconfirmed { .. }
            | Self::WithdrawalScanTransient { .. }
            | Self::SettlementCheckTransient { .. }
            | Self::MintRecoveryInconclusive { .. }
            | Self::BurnRecordTaskFailed { .. }
            | Self::BurnRecordFailed { .. }
            | Self::BurnSubmitInconclusive { .. }
            | Self::BurnTxDropped { .. }
            | Self::DepositSendUnresolved { .. }
            | Self::DepositSendReconciliationPending { .. }
            | Self::DepositSendTaskPanicked { .. }
            | Self::DepositSendLookup { .. }
            | Self::StateOffHop { .. }
            | Self::BasisPoints(_)
            | Self::RelayApi(_)
            | Self::RelayBridge(_)
            | Self::SwapQuoteOutOfBounds { .. }
            | Self::SwapPairSplit { .. }
            | Self::SwapPrepareTaskPanicked { .. }
            | Self::SwapPrepareTimedOut { .. }
            | Self::SwapQuoteExpired { .. }
            | Self::RelayFillPending { .. }
            | Self::SwapDepositReverted { .. }
            | Self::SwapPaymentUnverified { .. }
            | Self::EthereumChainMissing(_) => false,
        }
    }
}

impl From<SendError<UsdcRebalance>> for UsdcTransferError {
    fn from(error: SendError<UsdcRebalance>) -> Self {
        Self::Aggregate(Box::new(error))
    }
}

/// Refuses, before any call, a transfer none of the `served` corridors
/// carries: one recorded on another corridor, or a fresh one asking for
/// another. The transfer is left untouched. A recorded one that holds the
/// guard re-queues its job for a build that serves it and the rebalancing
/// service pages once (one that holds none ends its job); a fresh one
/// retries and dead-letters, which pages once.
fn refuse_unserved_corridor(
    id: &UsdcRebalanceId,
    requested: UsdcCorridor,
    served: &BTreeSet<UsdcCorridor>,
    state: Option<&UsdcRebalance>,
) -> Result<(), UsdcTransferError> {
    let corridor = state.map_or(requested, UsdcRebalance::corridor);
    if served.contains(&corridor) {
        return Ok(());
    }

    Err(unserved_corridor(id, requested, served, state))
}

/// The refusal of a transfer on a corridor none of `served` carries: a
/// mismatch for a recorded one, not served for a fresh one. Logged here,
/// where the refusal is decided.
fn unserved_corridor(
    id: &UsdcRebalanceId,
    requested: UsdcCorridor,
    served: &BTreeSet<UsdcCorridor>,
    state: Option<&UsdcRebalance>,
) -> UsdcTransferError {
    let error = state.map_or_else(
        || UsdcTransferError::CorridorNotServed {
            id: id.clone(),
            requested,
            served: served.clone(),
        },
        |state| UsdcTransferError::CorridorMismatch {
            id: id.clone(),
            recorded: state.corridor(),
            served: served.clone(),
            holds_guard: state.holds_rebalance_guard(),
        },
    );

    error!(target: "rebalance", %id, "{error}");
    error
}

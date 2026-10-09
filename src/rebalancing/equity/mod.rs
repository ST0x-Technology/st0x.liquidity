//! Cross-venue equity transfer.
//!
//! [`CrossVenueEquityTransfer`] drives equity transfers in both directions
//! through `resume_equity_to_market_making` / `resume_equity_to_hedging`
//! entry points, each backed by an apalis job:
//!
//! - **Hedging -> Market-Making** (mint): requests tokenized equity from
//!   Alpaca and deposits it into a Raindex vault.
//! - **Market-Making -> Hedging** (redemption): withdraws tokenized equity
//!   from a Raindex vault and sends it to Alpaca for redemption.

mod authorization_job;
mod job;
mod resume_job;

pub(crate) use authorization_job::{
    DeliverMintAuthorization, DeliverMintAuthorizationCtx, DeliverMintAuthorizationJobQueue,
};
#[cfg(test)]
pub(crate) use job::{
    ResumeEquityToHedging, ResumeEquityToMarketMaking, TransferEquityToMarketMakingJobError,
};
pub(crate) use job::{
    TransferEquityToHedging, TransferEquityToHedgingCtx, TransferEquityToHedgingJobQueue,
    TransferEquityToMarketMaking, TransferEquityToMarketMakingCtx,
    TransferEquityToMarketMakingJobQueue,
};
pub(crate) use resume_job::{
    ResumeTokenizationAggregate, ResumeTokenizationCtx, ResumeTokenizationJobQueue,
    ResumeTokenizationTarget,
};

use alloy::eips::eip2718::EIP7702_TX_TYPE_ID;
use alloy::hex::FromHexError;
use alloy::primitives::{Address, B256, TxHash, U256};
use alloy::rpc::types::TransactionReceipt;
use alloy::sol_types::SolCall as _;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use rain_math_float::Float;
use sqlx::SqlitePool;
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Weak};
use std::time::Duration;
use thiserror::Error;
use tracing::{debug, error, info, instrument, warn};

use st0x_config::{ChainEquities, ChainRegistry};
use st0x_event_sorcery::{AggregateError, LifecycleError, SendError, Store};
use st0x_evm::{Chain, EvmError, MinedTx, PreparedTransaction};
use st0x_execution::{FractionalShares, SharesConversionError, Symbol};
use st0x_issuance_dto::VaultModeTag;
use st0x_raindex::{Raindex, RaindexError, RaindexVaultId};
use st0x_tokenization::{
    AlpacaTokenizationError, IssuerRequestId, MintVerificationError, TokenizationRequest,
    TokenizationRequestId, TokenizationRequestIdError, TokenizationRequestStatus, Tokenizer,
    TokenizerError,
};
use st0x_wrapper::{
    UnderlyingPerWrapped, UnwrapConfirmation, UnwrappedToken, WrapConfirmation, Wrapper,
    WrapperError,
};

use super::RebalancingService;
use super::trigger::RecoveryClaim;
use crate::alerts::Notifier;
use crate::bindings::IRaindexInventory::withdraw4Call;
use crate::bot_gas::redrive::BotGasFailureClassifier;
use crate::bot_gas::{
    BotGasEnqueueFailure, BotGasOperationCategory, BotGasReceiptCostEnqueuer,
    RecordBotGasReceiptCost,
};
use crate::conductor::job::QueuePushError;
use crate::equity_redemption::{
    DetectionFailure, EquityRedemption, EquityRedemptionCommand, EquityRedemptionError,
    RedemptionAggregateId, UnwrappedProvenance, reattest_legacy_underlying,
};
use crate::mint_authorization::{ConfiguredMintAuthorizer, VaultModeCheckError, VaultModeReader};
use crate::native_gas::{ConfiguredGasReadiness, GasReadinessFailure, TransferGasRoute};
use crate::tokenized_equity_mint::{
    TOKENIZED_EQUITY_DECIMALS, TokenizedEquityMint, TokenizedEquityMintCommand,
};
use crate::vault_lookup::{VaultLookup, VaultLookupError};
/// Delay before retrying an accepted or potentially accepted withdrawal whose
/// receipt/log state is not yet conclusive. Replacement rows are durable and
/// intentionally uncapped: transaction finality must not consume the queue's
/// finite retry budget.
const WITHDRAWAL_RECONCILIATION_REDRIVE_DELAY: std::time::Duration =
    std::time::Duration::from_secs(30);

/// Duration after which repeated withdrawal-reconciliation redrives page the
/// operator via the notifier. Mirrors the USDC sibling's
/// `WITHDRAWAL_POLL_ALERT_DEADLINE`: the deadline is durable, anchored on the
/// persisted `VaultWithdrawSubmitting`/`VaultWithdrawSubmitted` timestamp, so
/// the countdown survives restarts. A prepared vault withdrawal signed at a fee
/// the market then outran will not confirm at that fee and is never fee bumped,
/// so before the deadline the redrive is silent; the deadline keeps that
/// otherwise silent stall from becoming a multiday outage while later sends
/// from this wallet queue behind its nonce. It can still mine when fees drop,
/// so it is never treated as dead.
const WITHDRAWAL_RECONCILIATION_ALERT_DEADLINE: Duration = Duration::from_secs(4 * 60 * 60);

/// Redrive delay used AFTER the withdrawal-reconciliation alert deadline has
/// elapsed. Mirrors `WITHDRAWAL_POLL_POST_DEADLINE_REDRIVE_DELAY`: slows the
/// cadence from 30 s to prevent alert fatigue while the guard stays held and the
/// idempotent resume keeps running (or an operator reconciles the withdrawal).
const WITHDRAWAL_RECONCILIATION_POST_DEADLINE_REDRIVE_DELAY: Duration =
    Duration::from_secs(30 * 60);

/// Time after which the newest candidate of a pending send to the issuer is redriven at
/// [`ISSUER_SEND_SLOW_REDRIVE_DELAY`] instead of
/// [`WITHDRAWAL_RECONCILIATION_REDRIVE_DELAY`]. The operator is paged once at
/// `transfer_timeout` by the rebalancing trigger, so this only slows the
/// cadence; every redrive still considers a fee replacement.
const ISSUER_SEND_SLOW_REDRIVE_AFTER: Duration = Duration::from_secs(4 * 60 * 60);

/// Redrive delay of a pending send to the issuer once its newest candidate has been
/// unmined for [`ISSUER_SEND_SLOW_REDRIVE_AFTER`].
const ISSUER_SEND_SLOW_REDRIVE_DELAY: Duration = Duration::from_secs(30 * 60);

/// The next drive of a redemption that may hold a persisted signed issuer
/// send.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct IssuerSendRedrive {
    /// The newest signed copy, or `None` when the redemption's state could not
    /// be read.
    pub(crate) tx_hash: Option<TxHash>,
    pub(crate) delay: Duration,
}

/// The redrive of a redemption that holds a persisted signed send to the issuer, with
/// the delay before the next drive anchored on when its newest copy was
/// signed. `None` when the redemption holds no signed send.
///
/// A signed send reserves its nonce until it resolves, so every failure to
/// drive it is redriven instead of spending a job's finite retry budget. Once
/// that budget is spent nothing drives the send until a restart, and every
/// later send from the wallet queues behind its nonce. A state that cannot be
/// read may hold such a send, so it is redriven too rather than falling through
/// to the budget.
pub(crate) async fn issuer_send_redrive(
    redemption_store: &Store<EquityRedemption>,
    aggregate_id: &RedemptionAggregateId,
) -> Option<IssuerSendRedrive> {
    let aggregate = match redemption_store.load(aggregate_id).await {
        Ok(aggregate) => aggregate?,
        Err(error) => {
            warn!(target: "rebalance", %aggregate_id, ?error, "Could not read the redemption to check for a signed send to the issuer; redriving it");
            return Some(IssuerSendRedrive {
                tx_hash: None,
                delay: WITHDRAWAL_RECONCILIATION_REDRIVE_DELAY,
            });
        }
    };
    let EquityRedemption::SendPending {
        prepared_send: Some(_),
        last_send_signed_at,
        ..
    } = &aggregate
    else {
        return None;
    };
    let tx_hash = aggregate.reconcilable_signed_tx()?.tx_hash();
    let unmined_for = last_send_signed_at
        .and_then(|signed_at| Utc::now().signed_duration_since(signed_at).to_std().ok())
        .unwrap_or_default();
    let delay = if unmined_for >= ISSUER_SEND_SLOW_REDRIVE_AFTER {
        ISSUER_SEND_SLOW_REDRIVE_DELAY
    } else {
        WITHDRAWAL_RECONCILIATION_REDRIVE_DELAY
    };
    Some(IssuerSendRedrive {
        tx_hash: Some(tx_hash),
        delay,
    })
}

/// Pages that a legacy redemption's recorded underlying is not the token its
/// vault attests, so its send to the issuer is never signed. Retrying cannot change
/// that, so the job stops instead of spending its retry budget.
pub(crate) async fn page_underlying_mismatch(
    notifier: &Arc<dyn Notifier>,
    aggregate_id: &RedemptionAggregateId,
    error: &RedemptionError,
) {
    let RedemptionError::ReattestLegacyUnderlying(
        EquityRedemptionError::LegacyUnderlyingMismatch {
            symbol,
            recorded,
            attested,
        },
    ) = error
    else {
        return;
    };
    let message = format!(
        "Equity redemption {aggregate_id} ({symbol}): its legacy record names the underlying \
         {recorded}, but the vault attests {attested}, so the bot will not sign its send to the \
         issuer, and retrying cannot change that. The unwrapped tokens stay in the bot wallet, and \
         the redemption keeps its guard and inflight. Check onchain which token the wallet holds, \
         then end the redemption with `stox transfer fail --kind redemption --id {aggregate_id} \
         --reason <reason>` and handle those tokens by hand."
    );
    if let Err(alert_error) = notifier.notify(&message).await {
        warn!(target: "rebalance", %aggregate_id, %alert_error, "Failed to deliver the underlying mismatch alert");
    }
}

/// The durable timestamp a stuck withdrawal's reconciliation deadline is
/// anchored on, or `None` when the aggregate is not in a withdrawal-submitting
/// state -- in which case the redrive stays silent, since only a submitted
/// withdrawal can be stuck awaiting confirmation.
fn withdrawal_reconciliation_anchor(aggregate: &EquityRedemption) -> Option<DateTime<Utc>> {
    match aggregate {
        EquityRedemption::VaultWithdrawSubmitting { submitting_at, .. } => Some(*submitting_at),
        EquityRedemption::VaultWithdrawSubmitted { submitted_at, .. } => Some(*submitted_at),
        _ => None,
    }
}

/// Chooses the next withdrawal-reconciliation redrive delay, paging the operator
/// once the durable deadline anchored on the aggregate's persisted submit
/// timestamp has elapsed.
///
/// Before the deadline the redrive is silent at
/// [`WITHDRAWAL_RECONCILIATION_REDRIVE_DELAY`]; at or after it the operator is
/// paged on every redrive and the cadence slows to
/// [`WITHDRAWAL_RECONCILIATION_POST_DEADLINE_REDRIVE_DELAY`] to avoid alert
/// fatigue. A prepared vault withdrawal is never fee-bumped
/// (`submit::broadcast_prepared` only ever rebroadcasts identical bytes), so a
/// withdrawal the market out-fees stalls indefinitely; this bounds that
/// otherwise-silent stall into a paged, deadline-driven incident. Alerting can
/// never fail the redrive -- a delivery error is logged and swallowed, matching
/// the USDC sibling.
pub(crate) async fn withdrawal_reconciliation_redrive_delay(
    redemption_store: &Store<EquityRedemption>,
    aggregate_id: &RedemptionAggregateId,
    notifier: &Arc<dyn Notifier>,
) -> Duration {
    let elapsed = redemption_store
        .load(aggregate_id)
        .await
        .ok()
        .flatten()
        .as_ref()
        .and_then(withdrawal_reconciliation_anchor)
        .and_then(|anchor| Utc::now().signed_duration_since(anchor).to_std().ok());

    match elapsed {
        Some(elapsed) if elapsed >= WITHDRAWAL_RECONCILIATION_ALERT_DEADLINE => {
            let message = format!(
                "Equity redemption {aggregate_id}: Raindex vault withdrawal has stayed \
                 unconfirmed for {elapsed:?} (>{WITHDRAWAL_RECONCILIATION_ALERT_DEADLINE:?}). A \
                 prepared withdrawal signed at a fee the market then outran will not confirm \
                 at that fee and is never fee bumped, so later sends from this wallet queue \
                 behind its nonce; it can still mine when fees drop, so do not settle the \
                 equity by hand yet. Automatic redrive continues at a slower cadence (guard \
                 held). Check the withdrawal on chain. (1) If it mined successfully, do not \
                 reconcile: the redrive confirms it (if no job remains, run \
                 `stox transfer resume --kind equity` or restart the bot). (2) If it mined and \
                 reverted, it used its nonce and moved nothing: once it has the chain's \
                 required confirmations, settle the equity by hand and run \
                 `stox transfer reconcile --kind redemption --id {aggregate_id} --reason \
                 <reason>` with no --superseding-tx. (3) If it has no receipt but another tx \
                 from the bot wallet already mined at its nonce and did the withdrawal (for \
                 example a wallet speed up of the same withdraw4), do not settle by hand: \
                 adopt that tx with `st0x-liquidity-client --env <env> debug adopt-withdrawal \
                 {aggregate_id} --replacement-tx <tx> --reason <reason>`, and the redrive \
                 confirms it and continues the redemption. (4) If it has no receipt and \
                 nothing mined at its nonce (pending or dropped), cancel it: send a 0-value \
                 self-transfer with no calldata (not EIP-7702) from the bot wallet at its \
                 nonce, with fees above the withdrawal's, and wait for the chain's required \
                 confirmations. Then settle the equity by hand and run \
                 `stox transfer reconcile --kind redemption --id {aggregate_id} --reason \
                 <reason> --superseding-tx <cancel tx>`. A reverted and a cancelled withdrawal \
                 both moved nothing, so the settlement is the same. Reconcile refuses until \
                 the chain proves the withdrawal can never land, and releases its reservation \
                 and the wallet's hold on its nonce."
            );
            if let Err(alert_error) = notifier.notify(&message).await {
                warn!(
                    target: "rebalance",
                    %aggregate_id,
                    %alert_error,
                    "Failed to deliver withdrawal-reconciliation deadline alert"
                );
            }
            WITHDRAWAL_RECONCILIATION_POST_DEADLINE_REDRIVE_DELAY
        }
        _ => WITHDRAWAL_RECONCILIATION_REDRIVE_DELAY,
    }
}

/// Why a redemption's signed vault withdrawal is not proven unable to land,
/// so the redemption must not be reconciled.
#[derive(Debug, Error)]
pub enum WithdrawalNotSuperseded {
    /// The persisted bytes are not a signed transaction, so its signer cannot
    /// be read.
    #[error("vault withdrawal {tx} is not a readable signed transaction; its signer is unknown")]
    UnreadableWithdrawal { tx: TxHash },
    /// Nonces are per sender, so only a tx from the withdrawal's signer can
    /// take its nonce, and the check reads the configured wallet's txs only.
    #[error(
        "vault withdrawal {tx} was signed by {signer}, not the bot wallet {bot_wallet} (was \
         the key rotated?): only a tx from {signer} at its nonce supersedes it"
    )]
    WithdrawalSignedByAnotherWallet {
        tx: TxHash,
        signer: Address,
        bot_wallet: Address,
    },
    #[error(
        "vault withdrawal {tx} is mined and succeeded: the withdrawal went through, so do not \
         reconcile or settle it by hand. The redemption continues once the bot confirms it; \
         if no job drives it, run `stox transfer resume --kind equity` or restart the bot"
    )]
    WithdrawalWentThrough { tx: TxHash },
    #[error(
        "vault withdrawal {tx} reverted, which used its nonce and moved nothing, but has \
         {confirmations} of the {required} required confirmations; retry once it has them"
    )]
    WithdrawalRevertUnconfirmed {
        tx: TxHash,
        confirmations: u64,
        required: u64,
    },
    #[error(
        "vault withdrawal {tx}, signed at nonce {nonce}, has no canonical receipt on this node. \
         That does not prove it cannot land: it may be pending below the market fee and mine \
         when fees drop, or a lagging node may not show it yet. Cancel it with a higher fee \
         0-value self-transfer with no calldata (not EIP-7702) from the bot wallet at nonce \
         {nonce}, then name that tx with \
         --superseding-tx (API: supersedingTx) once it has the required confirmations"
    )]
    NoSupersedingTx { tx: TxHash, nonce: u64 },
    #[error(
        "superseding tx {tx} is the vault withdrawal itself, which is not mined: name the tx \
         that took its nonce"
    )]
    SupersedingTxIsTheWithdrawal { tx: TxHash },
    #[error(
        "superseding tx {superseding} is not mined (unknown hash, or still pending); retry once \
         it is mined"
    )]
    SupersedingTxNotMined { superseding: TxHash },
    #[error("superseding tx {superseding} was sent by {from}, not the bot wallet {bot_wallet}")]
    SupersedingTxFromAnotherSender {
        superseding: TxHash,
        from: Address,
        bot_wallet: Address,
    },
    #[error(
        "superseding tx {superseding} is at nonce {superseding_nonce}, not the vault \
         withdrawal's nonce {nonce}"
    )]
    SupersedingTxAtAnotherNonce {
        superseding: TxHash,
        superseding_nonce: u64,
        nonce: u64,
    },
    #[error(
        "superseding tx {superseding} has {confirmations} of the {required} required \
         confirmations; retry once it has them"
    )]
    SupersedingTxUnconfirmed {
        superseding: TxHash,
        confirmations: u64,
        required: u64,
    },
    /// Only a plain cancel provably moved nothing: any other successful tx (a
    /// fee bumped copy of the withdrawal, a call through another contract, a
    /// contract creation, or one whose receipt holds a log, as code run at the
    /// wallet via EIP-7702 would leave if it moved anything) may have
    /// withdrawn the vault.
    #[error(
        "superseding tx {superseding} succeeded but is not a plain cancel (a 0-value transfer \
         with no calldata from the bot wallet {bot_wallet} to itself, not EIP-7702, that \
         emitted no logs), so it may have withdrawn the vault: do not reconcile or settle \
         by hand. If it did the withdrawal (for example a wallet speed up of the same \
         withdraw4), adopt it so the redemption finishes: `st0x-liquidity-client --env <env> \
         debug adopt-withdrawal <id> --replacement-tx {superseding} --reason <reason>`. Otherwise \
         check what it did onchain"
    )]
    SupersedingTxNotAPlainCancel {
        superseding: TxHash,
        bot_wallet: Address,
    },
    #[error("no [chains.{chain}] required_confirmations: it gates the vault withdrawal check")]
    NoConfirmationDepth { chain: Chain },
    #[error(transparent)]
    ChainServicesMissing(#[from] ChainServicesMissing),
    /// Reading the chain failed: transient, retry later.
    #[error("could not read tx {tx} on chain; retry")]
    Read {
        tx: TxHash,
        #[source]
        source: Box<RaindexError>,
    },
}

/// Which signed transaction of a redemption a reconcile must prove can never
/// land.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SignedRedemptionTx {
    /// The vault withdrawal that started the redemption.
    VaultWithdrawal,
    /// The send to the issuer of the unwrapped tokens, and each fee replacement of it.
    IssuerSend,
}

/// A reconcile refused by the chain check, worded for the signed tx it read.
///
/// A send to the issuer that went through must be left to recovery to record
/// `TokensSent`, not treated as a withdrawal.
#[derive(Debug)]
pub struct SignedTxNotSuperseded {
    pub kind: SignedRedemptionTx,
    pub refusal: WithdrawalNotSuperseded,
}

impl std::fmt::Display for SignedTxNotSuperseded {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use WithdrawalNotSuperseded::*;

        if self.kind == SignedRedemptionTx::VaultWithdrawal {
            return write!(formatter, "{}", self.refusal);
        }
        match &self.refusal {
            UnreadableWithdrawal { tx } => write!(
                formatter,
                "send {tx} to the issuer is not a readable signed transaction; its signer is unknown"
            ),
            WithdrawalSignedByAnotherWallet {
                tx,
                signer,
                bot_wallet,
            } => write!(
                formatter,
                "send {tx} to the issuer was signed by {signer}, not the bot wallet {bot_wallet} \
                 (was the key rotated?): only a tx from {signer} at its nonce supersedes it"
            ),
            WithdrawalWentThrough { tx } => write!(
                formatter,
                "send {tx} to the issuer is mined and succeeded: the tokens reached the issuer's \
                 redemption wallet, so do not reconcile or settle by hand. Recovery records it \
                 as TokensSent and then follows the Alpaca redemption; if no job drives it, run \
                 `stox transfer resume --kind equity` or restart the bot"
            ),
            WithdrawalRevertUnconfirmed {
                tx,
                confirmations,
                required,
            } => write!(
                formatter,
                "send {tx} to the issuer reverted, which used its nonce and moved nothing, but has \
                 {confirmations} of the {required} required confirmations; retry once it has them"
            ),
            NoSupersedingTx { tx, nonce } => write!(
                formatter,
                "send {tx} to the issuer, signed at nonce {nonce}, has no canonical receipt on \
                 this node. That does not prove it cannot land: it may be pending below the market \
                 fee and mine when fees drop, or a lagging node may not show it yet. Cancel it \
                 with a higher fee 0-value self-transfer with no calldata (not EIP-7702) from the \
                 bot wallet at nonce {nonce}, then name that tx with --superseding-tx (API: \
                 supersedingTx) once it has the required confirmations"
            ),
            SupersedingTxIsTheWithdrawal { tx } => write!(
                formatter,
                "superseding tx {tx} is the send to the issuer itself, which is not mined: name \
                 the tx that took its nonce"
            ),
            SupersedingTxAtAnotherNonce {
                superseding,
                superseding_nonce,
                nonce,
            } => write!(
                formatter,
                "superseding tx {superseding} is at nonce {superseding_nonce}, not the issuer \
                 send's nonce {nonce}"
            ),
            SupersedingTxNotAPlainCancel {
                superseding,
                bot_wallet,
            } => write!(
                formatter,
                "superseding tx {superseding} succeeded but is not a plain cancel (a 0-value \
                 transfer with no calldata from the bot wallet {bot_wallet} to itself, not \
                 EIP-7702, that emitted no logs), so it may have sent the tokens: do not \
                 reconcile or settle by hand. If it is one of the redemption's own signed copies \
                 of the send, recovery records it as TokensSent; otherwise check what it did \
                 onchain"
            ),
            NoConfirmationDepth { chain } => write!(
                formatter,
                "no [chains.{chain}] required_confirmations: it gates the check of the send to the issuer"
            ),
            SupersedingTxNotMined { .. }
            | SupersedingTxFromAnotherSender { .. }
            | SupersedingTxUnconfirmed { .. }
            | ChainServicesMissing(_)
            | Read { .. } => write!(formatter, "{}", self.refusal),
        }
    }
}

impl std::error::Error for SignedTxNotSuperseded {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        std::error::Error::source(&self.refusal)
    }
}

/// Proves that none of `signed` can ever land.
///
/// `signed` is a redemption's signed vault withdrawal, or every signed copy of
/// its send to the issuer, which share one nonce (see `verify_withdrawal_superseded`
/// for the proof each must pass). A copy that went through outranks any other
/// refusal, so the operator is told the send landed rather than to cancel an
/// older copy. With no `superseding_tx`, a copy that reverted with the required
/// confirmations used the shared nonce and moved nothing, so it is the proof
/// for the other copies.
pub async fn verify_signed_redemption_txs_superseded(
    raindex: &dyn Raindex,
    kind: SignedRedemptionTx,
    signed: &[&PreparedTransaction],
    superseding_tx: Option<TxHash>,
    bot_wallet: Address,
    required_confirmations: u64,
) -> Result<(), SignedTxNotSuperseded> {
    let mut first_refusal = None;
    let mut reverted_copy = None;
    for prepared in signed {
        match verify_withdrawal_superseded(
            raindex,
            prepared,
            superseding_tx,
            bot_wallet,
            required_confirmations,
        )
        .await
        {
            Ok(()) => {
                if superseding_tx.is_none() {
                    reverted_copy.get_or_insert_with(|| prepared.tx_hash());
                }
            }
            Err(refusal @ WithdrawalNotSuperseded::WithdrawalWentThrough { .. }) => {
                return Err(SignedTxNotSuperseded { kind, refusal });
            }
            // A copy that reverted short of its confirmations is the next step
            // to report: an older copy's missing receipt would ask for a cancel
            // at a nonce already used.
            Err(refusal @ WithdrawalNotSuperseded::WithdrawalRevertUnconfirmed { .. }) => {
                first_refusal = Some(refusal);
            }
            Err(refusal) => {
                if !matches!(
                    first_refusal,
                    Some(WithdrawalNotSuperseded::WithdrawalRevertUnconfirmed { .. })
                ) {
                    first_refusal.get_or_insert(refusal);
                }
            }
        }
    }
    let Some(refusal) = first_refusal else {
        return Ok(());
    };
    let Some(reverted) = reverted_copy else {
        return Err(SignedTxNotSuperseded { kind, refusal });
    };

    for prepared in signed {
        if prepared.tx_hash() == reverted {
            continue;
        }
        verify_withdrawal_superseded(
            raindex,
            prepared,
            Some(reverted),
            bot_wallet,
            required_confirmations,
        )
        .await
        .map_err(|refusal| SignedTxNotSuperseded { kind, refusal })?;
    }
    Ok(())
}

/// Proves that `prepared`, the signed vault withdrawal of a redemption, can
/// never land, reading its chain through `raindex`.
///
/// `prepared` must be signed by `bot_wallet`, since nonces are per sender. If
/// the withdrawal itself is mined, a success means it went through and is
/// refused, while a revert used its nonce and moved nothing, so with
/// `required_confirmations` it is proof on its own. Otherwise the
/// operator-named `superseding_tx` must be a different tx from `bot_wallet` at
/// the withdrawal's nonce with `required_confirmations` that moved nothing:
/// either it reverted, or it is a plain cancel, a 0-value transfer with no
/// calldata from `bot_wallet` to itself that is not EIP-7702 and emitted no
/// logs. A vault withdrawal always logs (the inventory's withdraw event and the
/// token transfer), and a reverted frame drops its logs, so a successful tx
/// with none moved nothing, even if code ran at the wallet through an EIP-7702
/// delegation. Any other successful tx may have withdrawn the vault, so it is
/// refused. Unlike the USDC
/// deposit send, no other transfer's tx can hold the nonce, because the bot
/// keeps it reserved from signing until the withdrawal confirms or is
/// definitively dropped. Only a tx the node shows
/// mined in the canonical chain counts, so a node that lags refuses rather
/// than proves.
pub async fn verify_withdrawal_superseded(
    raindex: &dyn Raindex,
    prepared: &PreparedTransaction,
    superseding_tx: Option<TxHash>,
    bot_wallet: Address,
    required_confirmations: u64,
) -> Result<(), WithdrawalNotSuperseded> {
    let tx = prepared.tx_hash();
    let nonce = prepared.nonce();

    let signer = prepared
        .signer()
        .ok_or(WithdrawalNotSuperseded::UnreadableWithdrawal { tx })?;
    if signer != bot_wallet {
        return Err(WithdrawalNotSuperseded::WithdrawalSignedByAnotherWallet {
            tx,
            signer,
            bot_wallet,
        });
    }

    if let Some(withdrawal) = read_mined_tx(raindex, tx).await? {
        if withdrawal.succeeded {
            return Err(WithdrawalNotSuperseded::WithdrawalWentThrough { tx });
        }
        if withdrawal.confirmations < required_confirmations {
            return Err(WithdrawalNotSuperseded::WithdrawalRevertUnconfirmed {
                tx,
                confirmations: withdrawal.confirmations,
                required: required_confirmations,
            });
        }
        return Ok(());
    }

    let Some(superseding) = superseding_tx else {
        return Err(WithdrawalNotSuperseded::NoSupersedingTx { tx, nonce });
    };
    if superseding == tx {
        return Err(WithdrawalNotSuperseded::SupersedingTxIsTheWithdrawal { tx });
    }

    let Some(MinedTx {
        from,
        to,
        nonce: superseding_nonce,
        value,
        input,
        tx_type,
        emitted_logs,
        succeeded,
        confirmations,
    }) = read_mined_tx(raindex, superseding).await?
    else {
        return Err(WithdrawalNotSuperseded::SupersedingTxNotMined { superseding });
    };

    if from != bot_wallet {
        return Err(WithdrawalNotSuperseded::SupersedingTxFromAnotherSender {
            superseding,
            from,
            bot_wallet,
        });
    }
    if superseding_nonce != nonce {
        return Err(WithdrawalNotSuperseded::SupersedingTxAtAnotherNonce {
            superseding,
            superseding_nonce,
            nonce,
        });
    }
    if confirmations < required_confirmations {
        return Err(WithdrawalNotSuperseded::SupersedingTxUnconfirmed {
            superseding,
            confirmations,
            required: required_confirmations,
        });
    }

    if !succeeded {
        return Ok(());
    }

    let plain_cancel = to == Some(bot_wallet)
        && value.is_zero()
        && input.is_empty()
        && tx_type != EIP7702_TX_TYPE_ID
        && !emitted_logs;
    if !plain_cancel {
        return Err(WithdrawalNotSuperseded::SupersedingTxNotAPlainCancel {
            superseding,
            bot_wallet,
        });
    }

    Ok(())
}

/// Refuses to reconcile a redemption holding only its withdrawal's hash once
/// that hash mined and succeeded.
///
/// Such a redemption holds a withdrawal from before the signed bytes were kept
/// (an adopted replacement is refused by `Reconcile` itself, with no read). A
/// success means the equity left the vault and
/// the redrive confirms it, as for a signed withdrawal that went through. With
/// no signed bytes there is no nonce to prove unused, so anything else
/// reconciles on the operator's word.
pub async fn verify_hash_only_withdrawal_not_through(
    raindex: &dyn Raindex,
    tx: TxHash,
) -> Result<(), WithdrawalNotSuperseded> {
    match read_mined_tx(raindex, tx).await? {
        Some(MinedTx {
            succeeded: true, ..
        }) => Err(WithdrawalNotSuperseded::WithdrawalWentThrough { tx }),
        Some(_) | None => Ok(()),
    }
}

/// How long the newest signed candidate of a send to the issuer may stay unmined
/// before a fee replacement is considered. A replacement is signed only when
/// the network's fee estimate has also risen above the candidate's fee.
pub(crate) const REDEMPTION_SEND_REPLACEMENT_AFTER: std::time::Duration =
    std::time::Duration::from_secs(3 * 60);

/// Sign once and store before broadcast. A send already persisted is never
/// signed again with a new nonce: it is only fee-replaced at its own nonce
/// (see [`replace_stale_redemption_send`]). A legacy `SendPending` (which may
/// already have moved tokens under an unrecorded hash) is refused.
async fn prepare_and_persist_redemption_send(
    services: &EquityTransferServices,
    store: &Store<EquityRedemption>,
    aggregate_id: &RedemptionAggregateId,
    replacement_cutoff: Option<Duration>,
) -> Result<(), RedemptionError> {
    let entity =
        store
            .load(aggregate_id)
            .await?
            .ok_or_else(|| RedemptionError::EntityNotFound {
                aggregate_id: aggregate_id.clone(),
            })?;
    if let EquityRedemption::SendPending { prepared_send, .. } = &entity {
        if prepared_send.is_none() {
            return Err(RedemptionError::LegacyIssuerSendPending {
                aggregate_id: aggregate_id.clone(),
            });
        }
        replace_stale_redemption_send(services, store, aggregate_id, &entity, replacement_cutoff)
            .await;
        return Ok(());
    }
    let EquityRedemption::TokensUnwrapped {
        symbol,
        chain,
        underlying_token,
        unwrapped_amount,
        unwrap_block,
        ..
    } = entity
    else {
        return Err(RedemptionError::UnexpectedEntity { entity });
    };
    let chain_services = services.for_chain(chain)?;
    // With no redemption wallet the send can never be signed, so nothing can
    // land: fail the redemption now instead of holding its guard, inflight and
    // reservation until `transfer_timeout`.
    let Some(redemption_wallet) = chain_services.tokenizer.redemption_wallet() else {
        warn!(target: "rebalance", %aggregate_id, %chain, "No issuer redemption wallet is configured; failing the redemption before signing its send");
        store
            .send(
                aggregate_id,
                EquityRedemptionCommand::FailTransfer {
                    reason: format!(
                        "No issuer redemption wallet is configured on {chain}; the send was \
                         never signed"
                    ),
                },
            )
            .await?;
        let entity =
            store
                .load(aggregate_id)
                .await?
                .ok_or_else(|| RedemptionError::EntityNotFound {
                    aggregate_id: aggregate_id.clone(),
                })?;
        return Err(RedemptionError::SendFailed { entity });
    };
    let token = match underlying_token {
        UnwrappedProvenance::Attested { attested } => attested,
        UnwrappedProvenance::Legacy(recorded) => {
            reattest_legacy_underlying(chain_services, &symbol, recorded)
                .await
                .map_err(RedemptionError::ReattestLegacyUnderlying)?
        }
    };
    // A load-balanced backend that has not indexed the unwrap block yet sees
    // a zero balance, so the send would revert. Waiting on the tokenizer's
    // own provider keeps a caught-up wrapper provider from masking it.
    if let Some(block) = unwrap_block {
        chain_services
            .tokenizer
            .wait_for_block(block)
            .await
            .map_err(TokenizerError::from)?;
    }
    let prepared = chain_services
        .tokenizer
        .prepare_redemption_send(token, unwrapped_amount)
        .await?;

    if let Err(error) = store
        .send(
            aggregate_id,
            EquityRedemptionCommand::PrepareSend {
                prepared: prepared.clone(),
                redemption_wallet,
            },
        )
        .await
    {
        error!(target: "rebalance", %aggregate_id, ?error, "Failed to persist the signed redemption send; not broadcasting");
        release_unpersisted_redemption_send(
            store,
            chain_services.tokenizer.as_ref(),
            aggregate_id,
            prepared.tx_hash(),
            &error,
        )
        .await;
        return Err(error.into());
    }

    Ok(())
}

/// Persists a fee replacement of a pending send to the issuer whose newest candidate
/// has stayed unmined for [`REDEMPTION_SEND_REPLACEMENT_AFTER`], when the
/// network's fee estimate has risen above that candidate's fee. Without it, a
/// send priced out by a fee spike never mines and its held nonce blocks every
/// later send from the wallet.
///
/// The replacement carries the same transfer at the same nonce, so at most one
/// candidate lands, and it is persisted before any broadcast. Nothing is
/// replaced while a candidate has a receipt or a receipt lookup fails, nor
/// once `replacement_cutoff` (the transfer timeout) has passed since the send
/// was first signed: the operator is paged then and may cancel the send at its
/// nonce, and a later replacement could outbid that cancel. Every failure here
/// is logged and left to the next redrive: confirming the existing candidates
/// must not wait on it.
async fn replace_stale_redemption_send(
    services: &EquityTransferServices,
    store: &Store<EquityRedemption>,
    aggregate_id: &RedemptionAggregateId,
    entity: &EquityRedemption,
    replacement_cutoff: Option<Duration>,
) {
    let EquityRedemption::SendPending {
        chain,
        last_send_signed_at: Some(signed_at),
        first_send_signed_at,
        ..
    } = entity
    else {
        return;
    };
    if let (Some(cutoff), Some(first_signed_at)) = (replacement_cutoff, first_send_signed_at)
        && Utc::now()
            .signed_duration_since(*first_signed_at)
            .to_std()
            .is_ok_and(|pending_for| pending_for >= cutoff)
    {
        debug!(target: "rebalance", %aggregate_id, ?cutoff, "Redemption send is past its transfer timeout; the operator was paged, so it is no longer fee-replaced");
        return;
    }
    let unmined_for = Utc::now().signed_duration_since(*signed_at);
    if unmined_for.to_std().unwrap_or_default() < REDEMPTION_SEND_REPLACEMENT_AFTER {
        return;
    }
    let candidates = entity.issuer_send_candidates();
    let Some(newest) = candidates.last() else {
        return;
    };
    let tokenizer = match services.for_chain(*chain) {
        Ok(chain_services) => chain_services.tokenizer.as_ref(),
        Err(error) => {
            warn!(target: "rebalance", %aggregate_id, %error, "Cannot consider a fee replacement of the redemption send");
            return;
        }
    };

    for candidate in &candidates {
        let tx_hash = candidate.tx_hash();
        match tokenizer.redemption_send_mined(tx_hash).await {
            Ok(false) => {}
            Ok(true) => return,
            Err(error) => {
                warn!(target: "rebalance", %aggregate_id, %tx_hash, %error, "Receipt lookup failed; not fee-replacing the redemption send");
                return;
            }
        }
    }

    let wallet = tokenizer.signing_wallet();
    if !newest.signed_by(wallet) {
        warn!(target: "rebalance", %aggregate_id, tx_hash = %newest.tx_hash(), signer = ?newest.signer(), %wallet, "Redemption send was signed by another wallet; not fee-replacing it");
        return;
    }
    tokenizer.restore_redemption_send(newest).await;
    let replacement = match tokenizer.prepare_redemption_send_replacement(newest).await {
        Ok(Some(replacement)) => replacement,
        Ok(None) => {
            info!(target: "rebalance", %aggregate_id, tx_hash = %newest.tx_hash(), ?unmined_for, "Redemption send is unmined but priced at the market; not fee-replacing it");
            return;
        }
        Err(error) => {
            warn!(target: "rebalance", %aggregate_id, tx_hash = %newest.tx_hash(), %error, "Could not sign a fee replacement of the redemption send");
            return;
        }
    };

    let replacement_hash = replacement.tx_hash();
    match store
        .send(
            aggregate_id,
            EquityRedemptionCommand::ReplaceSend { replacement },
        )
        .await
    {
        Ok(()) => warn!(
            target: "rebalance",
            %aggregate_id,
            replaced = %newest.tx_hash(),
            replacement = %replacement_hash,
            ?unmined_for,
            "Persisted a fee replacement of the redemption send; broadcasting it next"
        ),
        // Nothing reached the wallet: the replacement is recorded at the nonce
        // only when it is broadcast, so the unpersisted bytes are just dropped.
        Err(error) => warn!(
            target: "rebalance",
            %aggregate_id,
            replacement = %replacement_hash,
            ?error,
            "Failed to persist a fee replacement of the redemption send; not broadcasting it"
        ),
    }
}

async fn read_mined_tx(
    raindex: &dyn Raindex,
    tx: TxHash,
) -> Result<Option<MinedTx>, WithdrawalNotSuperseded> {
    raindex
        .mined_tx(tx)
        .await
        .map_err(|source| WithdrawalNotSuperseded::Read {
            tx,
            source: Box::new(source),
        })
}

/// The depth a tx on `chain` needs before it proves a signed vault withdrawal
/// can never land. Shared by the bot and the CLI so both read the same chain.
pub fn withdrawal_required_confirmations(
    chains: &ChainRegistry,
    chain: Chain,
) -> Result<u64, WithdrawalNotSuperseded> {
    chains
        .required_confirmations(chain)
        .ok_or(WithdrawalNotSuperseded::NoConfirmationDepth { chain })
}

/// Why a tx the operator named is not adoptable as a redemption's vault
/// withdrawal in place of its signed one.
#[derive(Debug, Error)]
pub enum ReplacementNotAdoptable {
    /// The persisted bytes are not a signed `withdraw4`, so its signer, the
    /// contract it calls and the vault it withdraws cannot be read.
    #[error(
        "vault withdrawal {tx} is not a readable signed withdraw4; its signer, target and vault \
         are unknown"
    )]
    UnreadableWithdrawal { tx: TxHash },
    /// Nonces are per sender, so only a tx from the withdrawal's signer can
    /// take its nonce, and the check reads the configured wallet's txs only.
    #[error(
        "vault withdrawal {tx} was signed by {signer}, not the bot wallet {bot_wallet} (was the \
         key rotated?): only a tx from {signer} at its nonce can replace it"
    )]
    WithdrawalSignedByAnotherWallet {
        tx: TxHash,
        signer: Address,
        bot_wallet: Address,
    },
    #[error(
        "replacement {tx} is the vault withdrawal itself: if it mined, the redrive confirms it \
         with nothing to adopt"
    )]
    ReplacementIsTheWithdrawal { tx: TxHash },
    #[error(
        "replacement {replacement} is not mined (unknown hash, or still pending); retry once it \
         is mined"
    )]
    ReplacementNotMined { replacement: TxHash },
    #[error("replacement {replacement} was sent by {from}, not the bot wallet {bot_wallet}")]
    ReplacementFromAnotherSender {
        replacement: TxHash,
        from: Address,
        bot_wallet: Address,
    },
    #[error(
        "replacement {replacement} is at nonce {replacement_nonce}, not the vault withdrawal's \
         nonce {nonce}"
    )]
    ReplacementAtAnotherNonce {
        replacement: TxHash,
        replacement_nonce: u64,
        nonce: u64,
    },
    #[error(
        "replacement {replacement} has {confirmations} of the {required} required \
         confirmations; retry once it has them"
    )]
    ReplacementUnconfirmed {
        replacement: TxHash,
        confirmations: u64,
        required: u64,
    },
    /// A reverted tx used the nonce and moved nothing, so it withdrew nothing
    /// to adopt; it proves the withdrawal can never land instead.
    #[error(
        "replacement {replacement} reverted, so it withdrew nothing: reconcile the redemption \
         with --superseding-tx {replacement} instead"
    )]
    ReplacementReverted { replacement: TxHash },
    #[error(
        "replacement {replacement} calls {called:?}, not {target}, the contract the vault \
         withdrawal calls, so it did not do the withdrawal"
    )]
    ReplacementCallsAnotherContract {
        replacement: TxHash,
        called: Option<Address>,
        target: Address,
    },
    #[error("replacement {replacement} is not a withdraw4 call, so it did not do the withdrawal")]
    ReplacementNotAWithdrawal { replacement: TxHash },
    /// `withdraw4` pays its caller, so the recipient is already the bot
    /// wallet; the amount may differ, since a partial withdrawal is recorded
    /// from the receipt.
    #[error(
        "replacement {replacement} withdraws token {token} from vault {vault_id}, not token \
         {expected_token} from vault {expected_vault_id} as the vault withdrawal does"
    )]
    ReplacementWithdrawsAnotherVault {
        replacement: TxHash,
        token: Address,
        vault_id: B256,
        expected_token: Address,
        expected_vault_id: B256,
    },
    /// `ConfirmWithdraw` records the vault transfer from the receipt and
    /// refuses one with none, and an adopted redemption cannot be reconciled,
    /// so a replacement that moved nothing would strand it.
    #[error(
        "replacement {replacement} paid the bot wallet {bot_wallet} none of token {token} \
         (for example a withdraw4 of zero), so it did not do the withdrawal"
    )]
    ReplacementWithdrewNothing {
        replacement: TxHash,
        token: Address,
        bot_wallet: Address,
    },
    /// The receipt's transfer logs could not be summed (a `Transfer` that does
    /// not decode, or a sum that overflows), so what it paid is unknown.
    #[error("could not read what replacement {replacement} paid the bot wallet: {source}")]
    ReplacementReceiptUnreadable {
        replacement: TxHash,
        #[source]
        source: Box<EquityRedemptionError>,
    },
    /// The trigger and the inventory reservation booked the signed
    /// withdrawal's amount, so adopting a larger one would move shares nothing
    /// reserved. A smaller one is adopted: the receipt records what moved.
    #[error(
        "replacement {replacement} withdraws {}, more than the {} the vault withdrawal booked, \
         so it is not adopted",
        st0x_float_serde::format_float_with_fallback(target),
        st0x_float_serde::format_float_with_fallback(expected)
    )]
    ReplacementWithdrawsMore {
        replacement: TxHash,
        target: Float,
        expected: Float,
    },
    /// A tx at a reused nonce can be another redemption's withdrawal, whose
    /// vault transfer that redemption already records.
    #[error(
        "replacement {replacement} is already the vault withdrawal of redemption {redemption}, \
         so it did not do this redemption's withdrawal"
    )]
    ReplacementIsAnotherRedemptionsWithdrawal {
        replacement: TxHash,
        redemption: RedemptionAggregateId,
    },
    #[error("no [chains.{chain}] required_confirmations: it gates the replacement check")]
    NoConfirmationDepth { chain: Chain },
    #[error(transparent)]
    ChainServicesMissing(#[from] ChainServicesMissing),
    /// Reading the chain failed: transient, retry later.
    #[error("could not read tx {tx} on chain; retry")]
    Read {
        tx: TxHash,
        #[source]
        source: Box<RaindexError>,
    },
}

/// Checks, reading its chain through `raindex`, that `replacement` did what
/// `prepared`, a redemption's signed vault withdrawal, would have done, so the
/// redemption can adopt it as its withdrawal.
///
/// `prepared` must be signed by `bot_wallet`, since nonces are per sender.
/// `replacement` must be a different tx from `bot_wallet` at the withdrawal's
/// nonce with `required_confirmations`, and a successful `withdraw4` to the
/// contract the withdrawal calls, from the same token and vault. `withdraw4`
/// pays its caller, so the recipient is the bot wallet; the amount may be
/// smaller but not larger, and its receipt must show a transfer of the token
/// to the bot wallet.
/// Taking the nonce means the signed withdrawal can never land, and that call
/// is what moved the equity, so the redemption continues from it:
/// `ConfirmWithdraw` records what its receipt actually transferred, and
/// refuses a receipt that transferred the withdrawal's token nowhere it
/// expects. Only a tx the node shows mined in the canonical chain counts, so
/// a node that lags refuses rather than adopts.
pub async fn verify_withdrawal_replacement(
    raindex: &dyn Raindex,
    prepared: &PreparedTransaction,
    replacement: TxHash,
    bot_wallet: Address,
    required_confirmations: u64,
) -> Result<(), ReplacementNotAdoptable> {
    let tx = prepared.tx_hash();
    let nonce = prepared.nonce();

    let signer = prepared
        .signer()
        .ok_or(ReplacementNotAdoptable::UnreadableWithdrawal { tx })?;
    if signer != bot_wallet {
        return Err(ReplacementNotAdoptable::WithdrawalSignedByAnotherWallet {
            tx,
            signer,
            bot_wallet,
        });
    }
    let target = prepared
        .to()
        .ok_or(ReplacementNotAdoptable::UnreadableWithdrawal { tx })?;
    let withdrawal = prepared
        .input()
        .and_then(|input| withdraw4Call::abi_decode(&input).ok())
        .ok_or(ReplacementNotAdoptable::UnreadableWithdrawal { tx })?;

    if replacement == tx {
        return Err(ReplacementNotAdoptable::ReplacementIsTheWithdrawal { tx });
    }

    let Some(MinedTx {
        from,
        to,
        nonce: replacement_nonce,
        input,
        succeeded,
        confirmations,
        ..
    }) = raindex
        .mined_tx(replacement)
        .await
        .map_err(|source| ReplacementNotAdoptable::Read {
            tx: replacement,
            source: Box::new(source),
        })?
    else {
        return Err(ReplacementNotAdoptable::ReplacementNotMined { replacement });
    };

    if from != bot_wallet {
        return Err(ReplacementNotAdoptable::ReplacementFromAnotherSender {
            replacement,
            from,
            bot_wallet,
        });
    }
    if replacement_nonce != nonce {
        return Err(ReplacementNotAdoptable::ReplacementAtAnotherNonce {
            replacement,
            replacement_nonce,
            nonce,
        });
    }
    if confirmations < required_confirmations {
        return Err(ReplacementNotAdoptable::ReplacementUnconfirmed {
            replacement,
            confirmations,
            required: required_confirmations,
        });
    }
    if !succeeded {
        return Err(ReplacementNotAdoptable::ReplacementReverted { replacement });
    }
    if to != Some(target) {
        return Err(ReplacementNotAdoptable::ReplacementCallsAnotherContract {
            replacement,
            called: to,
            target,
        });
    }

    let Ok(call) = withdraw4Call::abi_decode(&input) else {
        return Err(ReplacementNotAdoptable::ReplacementNotAWithdrawal { replacement });
    };
    if (call.token, call.vaultId) != (withdrawal.token, withdrawal.vaultId) {
        return Err(ReplacementNotAdoptable::ReplacementWithdrawsAnotherVault {
            replacement,
            token: call.token,
            vault_id: call.vaultId,
            expected_token: withdrawal.token,
            expected_vault_id: withdrawal.vaultId,
        });
    }
    let (target, expected) = (
        Float::from_raw(call.targetAmount),
        Float::from_raw(withdrawal.targetAmount),
    );
    // A float the contract accepted always compares; fail closed if not.
    if target.gt(expected).unwrap_or(true) {
        return Err(ReplacementNotAdoptable::ReplacementWithdrawsMore {
            replacement,
            target,
            expected,
        });
    }

    let receipt = raindex
        .tx_receipt(replacement)
        .await
        .map_err(|source| ReplacementNotAdoptable::Read {
            tx: replacement,
            source: Box::new(source),
        })?
        .ok_or(ReplacementNotAdoptable::ReplacementNotMined { replacement })?;
    match crate::equity_redemption::actual_withdrawn_amount_from_receipt(
        &receipt, call.token, bot_wallet,
    ) {
        Ok(_) => {}
        Err(EquityRedemptionError::RaindexWithdrawTransferNotFound { .. }) => {
            return Err(ReplacementNotAdoptable::ReplacementWithdrewNothing {
                replacement,
                token: call.token,
                bot_wallet,
            });
        }
        Err(source) => {
            return Err(ReplacementNotAdoptable::ReplacementReceiptUnreadable {
                replacement,
                source: Box::new(source),
            });
        }
    }

    Ok(())
}

/// After a failed `PrepareSend`, releases the signed send's nonce only when the
/// send provably never reached storage: the aggregate rejected the command, a
/// concurrent write made it conflict, or a reload shows a state that cannot
/// hold this hash. A load error, or a later
/// state the send may have reached, keeps the reservation: a nonce gap until
/// restart is recoverable, a rewound nonce under a live send is not.
async fn release_unpersisted_redemption_send(
    store: &Store<EquityRedemption>,
    tokenizer: &dyn Tokenizer,
    aggregate_id: &RedemptionAggregateId,
    tx_hash: TxHash,
    error: &SendError<EquityRedemption>,
) {
    // A conflict also proves the event was not written: optimistic concurrency
    // refused the whole commit.
    let rejected = matches!(
        error,
        AggregateError::UserError(LifecycleError::Apply(_)) | AggregateError::AggregateConflict
    );
    let unpersisted = rejected
        || match store.load(aggregate_id).await {
            Ok(None | Some(EquityRedemption::TokensUnwrapped { .. })) => true,
            Ok(Some(EquityRedemption::SendPending { prepared_send, .. })) => prepared_send
                .as_ref()
                .is_none_or(|stored| stored.prepared.tx_hash() != tx_hash),
            Ok(Some(_)) | Err(_) => false,
        };

    if unpersisted {
        tokenizer.discard_redemption_send(tx_hash).await;
    } else {
        warn!(target: "rebalance", %aggregate_id, %tx_hash, "Signed redemption send may have been persisted; keeping its nonce reserved");
    }
}

/// Data extracted from the TokensReceived aggregate state for
/// onchain verification and subsequent wrapping.
struct TokensReceivedData {
    shares_minted: U256,
    tx_hash: TxHash,
    symbol: Symbol,
    chain: Chain,
    wallet: Address,
}

/// Result of re-checking a stuck transfer against the tokenization provider.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RecheckOutcome {
    /// The provider had settled the request; the aggregate was un-failed and
    /// the workflow resumed (or, for redemption, completed).
    Recovered,
    /// The aggregate was active (not failed); the normal workflow was resumed.
    Resumed,
    /// The aggregate was already in its terminal success state.
    AlreadyCompleted,
    /// The provider request is not yet completed; nothing changed.
    LeftUnchanged,
    /// The redemption tx has not been detected by the provider yet.
    NotDetectedYet,
    /// Another transfer for the same symbol is currently in progress, so
    /// recovery was refused: rebuilding tracking would overwrite the live
    /// transfer's in-flight balance. Retry once the symbol is free.
    Conflict,
    /// The failure happened past the recoverable stage (e.g. a mint that
    /// already received tokens, or a redemption that never sent them).
    /// Provider-completion recovery does not apply.
    NotRecoverable,
}

#[derive(Debug, Error)]
pub(crate) enum RecheckError {
    #[error(transparent)]
    ChainServicesMissing(#[from] ChainServicesMissing),
    #[error(transparent)]
    Mint(#[from] MintError),
    #[error(transparent)]
    Redemption(#[from] RedemptionError),
    #[error(transparent)]
    Tokenizer(#[from] TokenizerError),
    #[error(transparent)]
    Rebalancing(#[from] super::trigger::RebalancingServiceError),
    #[error(transparent)]
    Database(#[from] sqlx::Error),
    #[error("completed provider request {0} is missing its onchain tx hash")]
    MissingTxHash(TokenizationRequestId),
    #[error("mint {0} has no accepted provider request to re-check")]
    NoAcceptedRequest(IssuerRequestId),
    #[error("mint {id} has an unparseable wallet address in its event history")]
    MalformedWallet {
        id: IssuerRequestId,
        #[source]
        source: FromHexError,
    },
    #[error("mint {id} has an empty tokenization request id in its event history")]
    MalformedTokenizationRequestId {
        id: IssuerRequestId,
        #[source]
        source: TokenizationRequestIdError,
    },
}

/// Context for re-checking a failed mint: the wallet and provider request
/// from the aggregate's event history, plus whether the mint progressed past
/// acceptance (in which case provider-completion recovery does not apply).
struct MintRecheckContext {
    wallet: Address,
    tokenization_request_id: TokenizationRequestId,
    received_tokens: bool,
}

async fn load_mint_recheck_context(
    pool: &SqlitePool,
    id: &IssuerRequestId,
) -> Result<MintRecheckContext, RecheckError> {
    let IssuerRequestId(raw_id) = id;

    let row: Option<(String, String, bool)> = sqlx::query_as(
        "SELECT \
             json_extract(requested.payload, '$.MintRequested.wallet'), \
             json_extract(accepted.payload, '$.MintAccepted.tokenization_request_id'), \
             EXISTS( \
                 SELECT 1 FROM events received \
                 WHERE received.aggregate_type = 'TokenizedEquityMint' \
                   AND received.aggregate_id = requested.aggregate_id \
                   AND received.event_type IN ( \
                       'TokenizedEquityMintEvent::TokensReceived', \
                       'TokenizedEquityMintEvent::ProviderCompletionRecovered' \
                   ) \
             ) \
         FROM events requested \
         INNER JOIN events accepted \
             ON accepted.aggregate_type = requested.aggregate_type \
            AND accepted.aggregate_id = requested.aggregate_id \
            AND accepted.event_type = 'TokenizedEquityMintEvent::MintAccepted' \
         WHERE requested.aggregate_type = 'TokenizedEquityMint' \
           AND requested.aggregate_id = ?1 \
           AND requested.event_type = 'TokenizedEquityMintEvent::MintRequested' \
         ORDER BY accepted.sequence DESC \
         LIMIT 1",
    )
    .bind(raw_id.to_string())
    .fetch_optional(pool)
    .await?;

    let Some((raw_wallet, raw_tokenization_request_id, received_tokens)) = row else {
        return Err(RecheckError::NoAcceptedRequest(id.clone()));
    };

    Ok(MintRecheckContext {
        wallet: raw_wallet
            .parse()
            .map_err(|source| RecheckError::MalformedWallet {
                id: id.clone(),
                source,
            })?,
        tokenization_request_id: TokenizationRequestId::try_new(&raw_tokenization_request_id)
            .map_err(|source| RecheckError::MalformedTokenizationRequestId {
                id: id.clone(),
                source,
            })?,
        received_tokens,
    })
}

/// Everything one chain contributes to an equity transfer.
///
/// The wallet that signs there, the contracts that chain's addresses name, the
/// issuer that mints on it and the asset table that lists what may move. Every
/// address in here means something only on its own chain, so an entry is never
/// a fallback for another chain's transfer.
#[derive(Clone)]
pub struct ChainEquityServices {
    /// The market-making wallet on this chain: mints land here and wraps are
    /// signed from it.
    pub wallet: Address,
    pub raindex: Arc<dyn Raindex>,
    pub vault_lookup: Arc<dyn VaultLookup>,
    pub tokenizer: Arc<dyn Tokenizer>,
    pub wrapper: Arc<dyn Wrapper>,
    /// Signs MintAuthV1 recipient authorizations for orchestrator-mode
    /// mints (RAI-1243). `Disabled` while the config has no
    /// `[orchestrator]` entry for this chain; `SignMintAuthorization` then
    /// fails loudly instead of guessing an orchestrator address.
    pub mint_authorizer: ConfiguredMintAuthorizer,
    /// Native-gas admission for a fresh transfer on this chain. `Unwired`
    /// is fail-closed in production, so a chain the conductor built no
    /// readiness for cannot start a transfer.
    pub gas_readiness: ConfiguredGasReadiness,
    /// What this chain lists under `[chains.<name>.trading.assets.equities]`.
    pub equities: ChainEquities,
}

/// Services shared by both equity transfer aggregates, keyed by the chain
/// the transfer runs on.
///
/// Both `TokenizedEquityMint` (hedging -> market-making) and
/// `EquityRedemption` (market-making -> hedging) resolve their entry from
/// the chain their record names, so a transfer never reaches another
/// chain's vault, issuer or wallet.
#[derive(Clone)]
pub struct EquityTransferServices {
    pub chains: BTreeMap<Chain, ChainEquityServices>,
    /// Enqueues bot-gas cost recording after a confirmed transfer tx (ADR
    /// 0017). Chain-independent: the job carries the chain it was raised on
    /// and the worker picks the provider from it.
    pub bot_gas_enqueuer: BotGasReceiptCostEnqueuer,
}

/// A transfer named a chain the services map does not carry. Never a
/// fallback to the primary: the transfer would sign with the wrong wallet
/// and settle against addresses that mean nothing on its chain.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize, Error)]
#[error(
    "no equity transfer services are wired for {chain}; the transfer cannot reach that chain's \
     wallet, vault or issuer"
)]
pub struct ChainServicesMissing {
    pub chain: Chain,
}

impl EquityTransferServices {
    /// Missing chains and listings cannot start new equity operations.
    pub fn rebalancing_mode(&self, chain: Chain, symbol: &Symbol) -> st0x_config::RebalancingMode {
        let Some(services) = self.chains.get(&chain) else {
            return st0x_config::RebalancingMode::Disabled;
        };
        services
            .equities
            .symbols
            .get(symbol)
            .map_or(st0x_config::RebalancingMode::Disabled, |asset| {
                asset.rebalancing
            })
    }

    /// The services for `chain`, or the chain by name.
    pub fn for_chain(&self, chain: Chain) -> Result<&ChainEquityServices, ChainServicesMissing> {
        self.chains
            .get(&chain)
            .ok_or(ChainServicesMissing { chain })
            .inspect_err(|error| {
                error!(target: "rebalance", %error, "Equity transfer named an unwired chain");
            })
    }

    /// Constructs a services instance whose methods all panic, on every
    /// chain.
    ///
    /// Safe for sending commands that never invoke services (e.g., the
    /// `FailWrapping`, `FailAcceptance`, `FailRaindexDeposit`, `FailTransfer`,
    /// and `Reconcile` commands). Used by the CLI `transfer fail` and
    /// `transfer reconcile` subcommands where no real broker/RPC connection
    /// exists. Every chain is present so a command on any record reaches the
    /// panicking stubs rather than a missing-chain error that would hide
    /// which service the command actually wanted.
    pub fn panicking() -> Self {
        let chains = Chain::ALL
            .into_iter()
            .map(|chain| {
                (
                    chain,
                    ChainEquityServices {
                        wallet: Address::ZERO,
                        raindex: Arc::new(PanickingRaindex),
                        vault_lookup: Arc::new(PanickingVaultLookup),
                        tokenizer: Arc::new(PanickingTokenizer),
                        wrapper: Arc::new(PanickingWrapper),
                        mint_authorizer: ConfiguredMintAuthorizer::Disabled,
                        gas_readiness: ConfiguredGasReadiness::Unwired,
                        equities: ChainEquities::default(),
                    },
                )
            })
            .collect();

        Self {
            chains,
            bot_gas_enqueuer: BotGasReceiptCostEnqueuer::Disabled,
        }
    }

    /// Test-only services whose `ConfirmWithdraw` resolves a withdrawal of
    /// `amount` for `token` on Base, letting a redemption be driven to
    /// `WithdrawnFromRaindex` (the earliest force-failable origin) and on to a
    /// terminal `Failed` entirely through the aggregate command path, never a
    /// direct `events` insert (docs/cqrs.md forbids those, including in tests).
    #[cfg(test)]
    pub(crate) fn confirming_withdrawal(token: Address, amount: U256) -> Self {
        Self {
            chains: BTreeMap::from([(
                Chain::Base,
                ChainEquityServices {
                    wallet: Address::ZERO,
                    raindex: Arc::new(
                        crate::onchain::mock::MockRaindex::new()
                            .with_withdraw_transfer(token, amount),
                    ),
                    vault_lookup: Arc::new(
                        crate::vault_lookup::MockVaultLookup::new()
                            .with_default_vault(RaindexVaultId(alloy::primitives::B256::ZERO)),
                    ),
                    tokenizer: Arc::new(st0x_tokenization::mock::MockTokenizer::new()),
                    wrapper: Arc::new(st0x_wrapper::MockWrapper::new()),
                    mint_authorizer: ConfiguredMintAuthorizer::Disabled,
                    gas_readiness: ConfiguredGasReadiness::Unwired,
                    equities: ChainEquities::default(),
                },
            )]),
            bot_gas_enqueuer: BotGasReceiptCostEnqueuer::Disabled,
        }
    }
}

/// Panicking Raindex stub for CLI-only use. All methods panic.
struct PanickingRaindex;

#[async_trait]
impl Raindex for PanickingRaindex {
    async fn withdraw(
        &self,
        _: Address,
        _: RaindexVaultId,
        _: U256,
        _: u8,
    ) -> Result<TxHash, RaindexError> {
        unimplemented!("PanickingRaindex: not available in CLI context")
    }

    async fn submit_deposit(
        &self,
        _: Address,
        _: RaindexVaultId,
        _: U256,
        _: u8,
    ) -> Result<TxHash, RaindexError> {
        unimplemented!("PanickingRaindex: not available in CLI context")
    }

    async fn prepare_withdraw(
        &self,
        _: Address,
        _: RaindexVaultId,
        _: U256,
        _: u8,
    ) -> Result<PreparedTransaction, RaindexError> {
        unimplemented!("PanickingRaindex: not available in CLI context")
    }

    async fn broadcast_prepared_withdraw(
        &self,
        _: &PreparedTransaction,
    ) -> Result<TxHash, RaindexError> {
        unimplemented!("PanickingRaindex: not available in CLI context")
    }

    async fn discard_prepared_withdraw(&self, _: TxHash) {
        unimplemented!("PanickingRaindex: not available in CLI context")
    }

    async fn release_superseded_withdraw(&self, _: TxHash) {
        unimplemented!("PanickingRaindex: not available in CLI context")
    }

    async fn restore_submitted_withdrawal(
        &self,
        _: TxHash,
        _: Option<&PreparedTransaction>,
    ) -> Result<(), RaindexError> {
        unimplemented!("PanickingRaindex: not available in CLI context")
    }

    async fn submit_withdraw(
        &self,
        _: Address,
        _: RaindexVaultId,
        _: U256,
        _: u8,
    ) -> Result<TxHash, RaindexError> {
        unimplemented!("PanickingRaindex: not available in CLI context")
    }

    async fn current_block(&self) -> Result<u64, RaindexError> {
        unimplemented!("PanickingRaindex: not available in CLI context")
    }

    async fn find_recent_withdrawal(
        &self,
        _: Address,
        _: RaindexVaultId,
        _: u64,
    ) -> Result<(TxHash, U256), RaindexError> {
        unimplemented!("PanickingRaindex: not available in CLI context")
    }

    async fn tx_mined(&self, _: TxHash) -> Result<bool, RaindexError> {
        unimplemented!("PanickingRaindex: not available in CLI context")
    }

    async fn mined_tx(&self, _: TxHash) -> Result<Option<MinedTx>, RaindexError> {
        unimplemented!("PanickingRaindex: not available in CLI context")
    }

    async fn tx_receipt(&self, _: TxHash) -> Result<Option<TransactionReceipt>, RaindexError> {
        unimplemented!("PanickingRaindex: not available in CLI context")
    }

    async fn confirm_tx_receipt(&self, _: TxHash) -> Result<TransactionReceipt, RaindexError> {
        unimplemented!("PanickingRaindex: not available in CLI context")
    }
}

/// Panicking VaultLookup stub for CLI-only use. All methods panic.
struct PanickingVaultLookup;

#[async_trait]
impl VaultLookup for PanickingVaultLookup {
    async fn vault_id_for_token(&self, _: Address) -> Result<RaindexVaultId, VaultLookupError> {
        unimplemented!("PanickingVaultLookup: not available in CLI context")
    }

    async fn vault_token_for_symbol(&self, _: &Symbol) -> Result<Address, VaultLookupError> {
        unimplemented!("PanickingVaultLookup: not available in CLI context")
    }
}

/// Panicking Tokenizer stub for CLI-only use. All methods panic.
struct PanickingTokenizer;

#[async_trait]
impl Tokenizer for PanickingTokenizer {
    async fn request_mint(
        &self,
        _: Symbol,
        _: FractionalShares,
        _: Address,
        _: IssuerRequestId,
    ) -> Result<TokenizationRequest, TokenizerError> {
        unimplemented!("PanickingTokenizer: not available in CLI context")
    }

    async fn find_mint_by_issuer_request_id(
        &self,
        _: &IssuerRequestId,
    ) -> Result<Option<TokenizationRequest>, TokenizerError> {
        unimplemented!("PanickingTokenizer: not available in CLI context")
    }

    async fn poll_mint_until_complete(
        &self,
        _: &TokenizationRequestId,
    ) -> Result<TokenizationRequest, TokenizerError> {
        unimplemented!("PanickingTokenizer: not available in CLI context")
    }

    async fn get_request(
        &self,
        _: &TokenizationRequestId,
    ) -> Result<TokenizationRequest, TokenizerError> {
        unimplemented!("PanickingTokenizer: not available in CLI context")
    }

    fn redemption_wallet(&self) -> Option<Address> {
        unimplemented!("PanickingTokenizer: not available in CLI context")
    }

    async fn wait_for_block(&self, _: u64) -> Result<(), EvmError> {
        unimplemented!("PanickingTokenizer: not available in CLI context")
    }

    async fn send_for_redemption(
        &self,
        _: UnwrappedToken,
        _: U256,
    ) -> Result<TxHash, TokenizerError> {
        unimplemented!("PanickingTokenizer: not available in CLI context")
    }

    async fn prepare_redemption_send(
        &self,
        _token: UnwrappedToken,
        _amount: U256,
    ) -> Result<st0x_evm::PreparedTransaction, TokenizerError> {
        unimplemented!("PanickingTokenizer: not available in CLI context")
    }
    async fn broadcast_redemption_send(
        &self,
        _: &st0x_evm::PreparedTransaction,
    ) -> Result<TxHash, TokenizerError> {
        unimplemented!("PanickingTokenizer: not available in CLI context")
    }
    fn signing_wallet(&self) -> Address {
        unimplemented!("PanickingTokenizer: not available in CLI context")
    }
    async fn prepare_redemption_send_replacement(
        &self,
        _: &st0x_evm::PreparedTransaction,
    ) -> Result<Option<st0x_evm::PreparedTransaction>, TokenizerError> {
        unimplemented!("PanickingTokenizer: not available in CLI context")
    }
    async fn confirm_redemption_send(
        &self,
        _: TxHash,
    ) -> Result<st0x_tokenization::RedemptionSendReceipt, TokenizerError> {
        unimplemented!("PanickingTokenizer: not available in CLI context")
    }
    async fn restore_redemption_send(&self, _: &st0x_evm::PreparedTransaction) {
        unimplemented!("PanickingTokenizer: not available in CLI context")
    }
    async fn discard_redemption_send(&self, _: TxHash) {
        unimplemented!("PanickingTokenizer: not available in CLI context")
    }
    async fn redemption_send_mined(&self, _: TxHash) -> Result<bool, TokenizerError> {
        unimplemented!("PanickingTokenizer: not available in CLI context")
    }
    async fn release_superseded_redemption_send(&self, _: TxHash) {
        unimplemented!("PanickingTokenizer: not available in CLI context")
    }

    async fn poll_for_redemption(&self, _: &TxHash) -> Result<TokenizationRequest, TokenizerError> {
        unimplemented!("PanickingTokenizer: not available in CLI context")
    }

    async fn find_redemption_by_tx(
        &self,
        _: &TxHash,
    ) -> Result<Option<TokenizationRequest>, TokenizerError> {
        unimplemented!("PanickingTokenizer: not available in CLI context")
    }

    async fn poll_redemption_until_complete(
        &self,
        _: &TokenizationRequestId,
    ) -> Result<TokenizationRequest, TokenizerError> {
        unimplemented!("PanickingTokenizer: not available in CLI context")
    }

    async fn verify_mint_tx(
        &self,
        _: TxHash,
        _: Address,
        _: Address,
        _: U256,
    ) -> Result<(), MintVerificationError> {
        unimplemented!("PanickingTokenizer: not available in CLI context")
    }

    async fn list_pending_requests(&self) -> Result<Vec<TokenizationRequest>, TokenizerError> {
        unimplemented!("PanickingTokenizer: not available in CLI context")
    }
}

/// Panicking Wrapper stub for CLI-only use. All methods panic.
struct PanickingWrapper;

#[async_trait]
impl Wrapper for PanickingWrapper {
    async fn get_ratio_for_symbol(&self, _: &Symbol) -> Result<UnderlyingPerWrapped, WrapperError> {
        unimplemented!("PanickingWrapper: not available in CLI context")
    }

    fn lookup_underlying(&self, _: &Symbol) -> Result<Address, WrapperError> {
        unimplemented!("PanickingWrapper: not available in CLI context")
    }

    fn lookup_derivative(&self, _: &Symbol) -> Result<Address, WrapperError> {
        unimplemented!("PanickingWrapper: not available in CLI context")
    }

    async fn attest_underlying(&self, _: &Symbol) -> Result<UnwrappedToken, WrapperError> {
        unimplemented!("PanickingWrapper: not available in CLI context")
    }

    async fn to_wrapped(
        &self,
        _: Address,
        _: U256,
        _: Address,
    ) -> Result<(TxHash, U256), WrapperError> {
        unimplemented!("PanickingWrapper: not available in CLI context")
    }

    async fn to_underlying(
        &self,
        _: Address,
        _: U256,
        _: Address,
        _: Address,
    ) -> Result<(TxHash, U256), WrapperError> {
        unimplemented!("PanickingWrapper: not available in CLI context")
    }

    async fn donate(&self, _: Address, _: U256) -> Result<TxHash, WrapperError> {
        unimplemented!("PanickingWrapper: not available in CLI context")
    }

    async fn submit_wrap(&self, _: Address, _: U256, _: Address) -> Result<TxHash, WrapperError> {
        unimplemented!("PanickingWrapper: not available in CLI context")
    }

    async fn confirm_wrap(&self, _: Address, _: TxHash) -> Result<WrapConfirmation, WrapperError> {
        unimplemented!("PanickingWrapper: not available in CLI context")
    }

    async fn submit_unwrap(
        &self,
        _: Address,
        _: U256,
        _: Address,
        _: Address,
    ) -> Result<TxHash, WrapperError> {
        unimplemented!("PanickingWrapper: not available in CLI context")
    }

    async fn confirm_unwrap(
        &self,
        _: Address,
        _: TxHash,
    ) -> Result<UnwrapConfirmation, WrapperError> {
        unimplemented!("PanickingWrapper: not available in CLI context")
    }

    async fn wait_for_block(&self, _: u64) -> Result<(), WrapperError> {
        unimplemented!("PanickingWrapper: not available in CLI context")
    }

    fn owner(&self) -> Address {
        unimplemented!("PanickingWrapper: not available in CLI context")
    }
}

#[derive(Debug, Error)]
pub enum MintError {
    #[error("Equity rebalancing is {mode} for {symbol} on {chain}; new operations require enabled")]
    RebalancingNotEnabled {
        chain: Chain,
        symbol: Symbol,
        mode: st0x_config::RebalancingMode,
    },
    #[error(transparent)]
    ChainServicesMissing(#[from] ChainServicesMissing),
    #[error(transparent)]
    GasReadiness(#[from] GasReadinessFailure),
    #[error("Aggregate error: {0}")]
    Aggregate(Box<SendError<TokenizedEquityMint>>),
    #[error("Wrapper error: {0}")]
    Wrapper(#[from] WrapperError),
    #[error("Raindex error: {0}")]
    Raindex(#[from] RaindexError),
    #[error("Vault lookup error: {0}")]
    VaultLookup(#[from] VaultLookupError),
    #[error("Onchain mint verification failed: {0}")]
    Verification(#[from] MintVerificationError),
    #[error(
        "Entity not found after command: expected {expected_state} \
         for {issuer_request_id}"
    )]
    EntityNotFound {
        issuer_request_id: IssuerRequestId,
        expected_state: &'static str,
    },
    #[error(
        "Unexpected mint state for {issuer_request_id}: expected \
         {expected_state}, got {entity:?}"
    )]
    UnexpectedState {
        issuer_request_id: IssuerRequestId,
        expected_state: &'static str,
        entity: Box<TokenizedEquityMint>,
    },
    /// Enqueueing the bot-gas receipt cost recording job failed after the
    /// confirming step (wrap or vault deposit) already succeeded onchain --
    /// a local SQLite write, safe to retry since the aggregate has not
    /// advanced past the confirmed state yet. Carries a [`BotGasEnqueueFailure`]
    /// (not a bare `QueuePushError`) so the tx hash the failed enqueue was
    /// for survives into any wrapper (e.g. `WrappedEquityRecoveryError`) that
    /// needs to re-surface it -- see [`crate::bot_gas::redrive`].
    #[error("Failed to enqueue bot-gas receipt cost recording: {0}")]
    BotGasEnqueue(BotGasEnqueueFailure),
    /// The asset's `vault_mode` could not be determined from issuance. The
    /// saga must not guess: assuming vault-direct would skip a required
    /// authorization and stall the mint at issuance's on-chain step. The
    /// mint stays `MintAccepted`; the resume path retries the check.
    #[error("Vault-mode check failed: {0}")]
    VaultModeCheck(#[from] VaultModeCheckError),
    /// `vault_mode` reads orchestrator but the record's chain lists no
    /// `[chains.<name>.trading.assets.equities.<symbol>] tokenized_equity`
    /// to bind the authorization to. Never resolved through another
    /// chain's table: the same symbol is a different contract there.
    #[error("No tokenized-equity address configured for {symbol} on {chain}")]
    UnknownTokenizedEquity { chain: Chain, symbol: Symbol },
    /// Enqueueing the authorization delivery job failed -- a local SQLite
    /// write. The signed authorization is already persisted on the
    /// aggregate, so the resume path re-enqueues without re-signing.
    #[error("Failed to enqueue mint-authorization delivery: {0}")]
    AuthorizationEnqueue(#[from] QueuePushError),
}

/// Selector for `ERC20InsufficientBalance(address,uint256,uint256)`.
const ERC20_INSUFFICIENT_BALANCE_SELECTOR: &str = "0xe450d38c";

impl MintError {
    /// Returns `true` if the underlying error is an RPC-level
    /// `ERC20InsufficientBalance` revert, indicating the wallet has
    /// zero tokens because they were already deposited in a previous
    /// session.
    ///
    /// Matches both `Raindex` and `Wrapper` variants: currently only
    /// `Raindex` is reachable from `try_deposit_or_recover`, but the
    /// broader match keeps this predicate correct for any `MintError`
    /// regardless of call site.
    fn is_insufficient_balance_revert(&self) -> bool {
        let (Self::Raindex(RaindexError::Evm(evm_error))
        | Self::Wrapper(WrapperError::Evm(evm_error))) = self
        else {
            return false;
        };

        let EvmError::Transport(rpc_error) = evm_error.underlying() else {
            return false;
        };

        rpc_error.as_error_resp().is_some_and(|payload| {
            payload.data.as_ref().is_some_and(|data| {
                data.get()
                    .trim_matches('"')
                    .starts_with(ERC20_INSUFFICIENT_BALANCE_SELECTOR)
            })
        })
    }
}

impl From<SendError<TokenizedEquityMint>> for MintError {
    fn from(error: SendError<TokenizedEquityMint>) -> Self {
        Self::Aggregate(Box::new(error))
    }
}

impl BotGasFailureClassifier for MintError {
    fn is_bot_gas_enqueue_failure(&self) -> bool {
        match self {
            Self::BotGasEnqueue(_) => true,
            Self::RebalancingNotEnabled { .. }
            | Self::ChainServicesMissing(_)
            | Self::GasReadiness(_)
            | Self::Aggregate(_)
            | Self::Wrapper(_)
            | Self::Raindex(_)
            | Self::VaultLookup(_)
            | Self::Verification(_)
            | Self::EntityNotFound { .. }
            | Self::UnexpectedState { .. }
            | Self::VaultModeCheck(_)
            | Self::UnknownTokenizedEquity { .. }
            | Self::AuthorizationEnqueue(_) => false,
        }
    }
}

/// Whether a failed `resume_mint` or `resume_redemption` can succeed on a
/// later attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) enum ResumeFailureKind {
    /// RPC, confirmation, gas, node sync or local persistence: the same call
    /// can succeed once the condition clears.
    Retryable,
    /// A missing or terminal mint, a broken invariant, a revert or a config
    /// gap: every retry fails the same way.
    Permanent,
}

/// A `resume_mint` failure as the wallet-recovery aggregates record it.
///
/// `MintError` can't be carried as a typed `#[source]` here:
/// `EventSourced::Error` must be `Clone + Serialize + DeserializeOwned`,
/// which the RPC, sqlx and aggregate errors it wraps are not (see
/// [`BotGasEnqueueFailure`]). `kind` gives callers a matchable
/// classification; `message` is the rendered `MintError`, for diagnostics.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, Error)]
#[error("{message}")]
pub(crate) struct MintResumeFailure {
    pub(crate) kind: ResumeFailureKind,
    pub(crate) message: String,
}

impl MintResumeFailure {
    pub(crate) fn from_mint_error(error: &MintError) -> Self {
        Self {
            kind: error.resume_failure_kind(),
            message: error.to_string(),
        }
    }
}

impl MintError {
    fn resume_failure_kind(&self) -> ResumeFailureKind {
        use ResumeFailureKind::{Permanent, Retryable};

        match self {
            Self::Aggregate(error) => match error.as_ref() {
                AggregateError::UserError(_) => Permanent,
                _ => Retryable,
            },
            Self::Wrapper(error) => match error {
                WrapperError::Evm(evm) if evm.is_revert() => Permanent,
                WrapperError::SymbolNotConfigured(_)
                | WrapperError::MissingDepositEvent
                | WrapperError::MissingWithdrawEvent
                | WrapperError::MissingUnderlyingTransfer { .. }
                | WrapperError::RedeemExceedsMax { .. }
                | WrapperError::VaultAssetMismatch { .. } => Permanent,
                WrapperError::Evm(_)
                | WrapperError::Contract(_)
                | WrapperError::Ratio(_)
                | WrapperError::MissingBlockNumber { .. } => Retryable,
            },
            Self::Raindex(RaindexError::Evm(evm)) if evm.is_revert() => Permanent,
            Self::Verification(error) => verification_resume_failure_kind(error),
            Self::Raindex(RaindexError::ZeroAmount)
            | Self::EntityNotFound { .. }
            | Self::UnexpectedState { .. }
            | Self::UnknownTokenizedEquity { .. } => Permanent,
            Self::RebalancingNotEnabled { .. }
            | Self::ChainServicesMissing(_)
            | Self::GasReadiness(_)
            | Self::Raindex(_)
            | Self::VaultLookup(_)
            | Self::BotGasEnqueue(_)
            | Self::VaultModeCheck(_)
            | Self::AuthorizationEnqueue(_) => Retryable,
        }
    }
}

/// Distinguishes mint failures before vs after tokens were received from
/// Alpaca. Post-receipt failures must NOT clear the in-progress guard
/// because real tokens exist in the wallet and startup recovery will
/// resume them.
#[derive(Debug, Error)]
pub enum MintTransferError {
    /// Failure before Alpaca delivered tokens. Safe to clear guard and
    /// retry from scratch.
    #[error(transparent)]
    PreReceipt(MintError),

    /// Failure after tokens were received (verify/wrap/deposit stage).
    /// Tokens exist in the wallet; guard must stay set for recovery.
    #[error(transparent)]
    PostReceipt(MintError),
}

impl MintTransferError {
    pub(crate) fn gas_readiness_retry_interval(&self) -> Option<std::time::Duration> {
        match self {
            Self::PreReceipt(MintError::GasReadiness(failure)) => failure.retry_interval(),
            Self::PreReceipt(_) | Self::PostReceipt(_) => None,
        }
    }
}

impl BotGasFailureClassifier for MintTransferError {
    fn is_bot_gas_enqueue_failure(&self) -> bool {
        match self {
            Self::PreReceipt(inner) | Self::PostReceipt(inner) => {
                inner.is_bot_gas_enqueue_failure()
            }
        }
    }
}

fn mint_reached_post_receipt(entity: &TokenizedEquityMint) -> bool {
    matches!(
        entity,
        TokenizedEquityMint::TokensReceived { .. }
            | TokenizedEquityMint::WrapSubmitted { .. }
            | TokenizedEquityMint::TokensWrapped { .. }
            | TokenizedEquityMint::VaultDepositSubmitted { .. }
            | TokenizedEquityMint::DepositedIntoRaindex { .. }
    )
}

fn classify_mint_resume_error(
    reached: Result<TokenizedEquityMint, MintError>,
    error: MintError,
) -> MintTransferError {
    match reached {
        Ok(entity) if mint_reached_post_receipt(&entity) => MintTransferError::PostReceipt(error),
        Ok(_) | Err(_) => MintTransferError::PreReceipt(error),
    }
}

#[derive(Debug, Error)]
pub enum RedemptionError {
    #[error("Equity rebalancing is {mode} for {symbol} on {chain}; new operations require enabled")]
    RebalancingNotEnabled {
        chain: Chain,
        symbol: Symbol,
        mode: st0x_config::RebalancingMode,
    },
    #[error(
        "Redemption {aggregate_id} has a legacy send without a persisted transaction; verify its prior issuer transfer before resuming"
    )]
    LegacyIssuerSendPending { aggregate_id: RedemptionAggregateId },
    /// The legacy record's underlying could not be re-attested before its
    /// send to the issuer was signed. A mismatch is permanent (see
    /// [`is_permanent_underlying_mismatch`](Self::is_permanent_underlying_mismatch));
    /// a failed read is not.
    #[error(
        "Could not re-attest the legacy redemption's underlying before signing its send to the issuer"
    )]
    ReattestLegacyUnderlying(#[source] EquityRedemptionError),
    #[error("The prepare task of the send to the issuer failed to join")]
    PrepareTaskJoin(#[from] tokio::task::JoinError),

    #[error(transparent)]
    ChainServicesMissing(#[from] ChainServicesMissing),
    #[error(transparent)]
    GasReadiness(#[from] GasReadinessFailure),
    #[error(transparent)]
    Send(#[from] SendError<EquityRedemption>),
    #[error(transparent)]
    Raindex(#[from] RaindexError),
    #[error(transparent)]
    VaultLookup(#[from] VaultLookupError),
    #[error(transparent)]
    Alpaca(#[from] AlpacaTokenizationError),
    #[error(transparent)]
    Tokenizer(#[from] TokenizerError),
    #[error(transparent)]
    SharesConversion(#[from] SharesConversionError),
    #[error("Failed to enqueue redemption-send bot-gas receipt cost recording: {0}")]
    BotGasEnqueue(BotGasEnqueueFailure),
    #[error("Entity not found after command: {aggregate_id}")]
    EntityNotFound { aggregate_id: RedemptionAggregateId },
    #[error("Token send to Alpaca failed: {entity:?}")]
    SendFailed { entity: EquityRedemption },
    #[error("Unexpected entity: {entity:?}")]
    UnexpectedEntity { entity: EquityRedemption },
    #[error("Unexpected tokenization status: still pending after polling")]
    UnexpectedPendingStatus,
    #[error("Redemption was rejected by Alpaca")]
    Rejected,
    #[error(
        "redemption {aggregate_id} is in legacy VaultWithdrawPending without a \
         trustworthy chain-scan lower bound; operator reconciliation is required"
    )]
    LegacyVaultWithdrawPending { aggregate_id: RedemptionAggregateId },
    #[error(
        "prepared vault withdrawal hash mismatch: expected {expected}, broadcast returned {actual}"
    )]
    PreparedWithdrawalHashMismatch { expected: TxHash, actual: TxHash },
}

/// A `resume_redemption` failure as the wallet-recovery aggregates record it.
/// Carries a rendered message for the same reason as [`MintResumeFailure`].
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, Error)]
#[error("{message}")]
pub(crate) struct RedemptionResumeFailure {
    pub(crate) kind: ResumeFailureKind,
    pub(crate) message: String,
}

impl RedemptionResumeFailure {
    pub(crate) fn from_redemption_error(error: &RedemptionError) -> Self {
        Self {
            kind: error.resume_failure_kind(),
            message: error.to_string(),
        }
    }
}

fn alpaca_resume_failure_kind(error: &AlpacaTokenizationError) -> ResumeFailureKind {
    use ResumeFailureKind::{Permanent, Retryable};

    match error {
        AlpacaTokenizationError::ApiError { status, .. }
            if status.is_server_error() || *status == reqwest::StatusCode::TOO_MANY_REQUESTS =>
        {
            Retryable
        }
        AlpacaTokenizationError::Reqwest(_)
        | AlpacaTokenizationError::Auth(_)
        | AlpacaTokenizationError::PollTimeout { .. } => Retryable,
        AlpacaTokenizationError::Gateway(hop) if hop.retryable || hop.retryable_with_same_key => {
            Retryable
        }
        AlpacaTokenizationError::ApiError { .. }
        | AlpacaTokenizationError::PrivateKeyJwtUnsupported
        | AlpacaTokenizationError::JsonParse(_)
        | AlpacaTokenizationError::Utf8(_)
        | AlpacaTokenizationError::InsufficientPosition { .. }
        | AlpacaTokenizationError::UnsupportedAccount
        | AlpacaTokenizationError::InvalidParameters { .. }
        | AlpacaTokenizationError::RequestNotFound { .. }
        | AlpacaTokenizationError::DuplicateMintIssuerRequestId { .. }
        | AlpacaTokenizationError::InvalidBaseUrl(_)
        | AlpacaTokenizationError::WrongNetwork { .. }
        | AlpacaTokenizationError::NetworkMissing { .. }
        | AlpacaTokenizationError::Gateway(_)
        | AlpacaTokenizationError::NotSent(_) => Permanent,
    }
}

/// A provider error or a receipt a lagging node has not indexed yet can clear
/// on a later read; a revert or a wrong transfer cannot.
fn verification_resume_failure_kind(error: &MintVerificationError) -> ResumeFailureKind {
    match error {
        MintVerificationError::Provider(_) | MintVerificationError::ReceiptNotFound { .. } => {
            ResumeFailureKind::Retryable
        }
        MintVerificationError::TransactionReverted { .. }
        | MintVerificationError::NoMatchingTransfer { .. }
        | MintVerificationError::InsufficientTransferAmount { .. }
        | MintVerificationError::TransferOverflow { .. } => ResumeFailureKind::Permanent,
    }
}

impl RedemptionError {
    fn resume_failure_kind(&self) -> ResumeFailureKind {
        use ResumeFailureKind::{Permanent, Retryable};

        match self {
            Self::ReattestLegacyUnderlying(_) if self.is_permanent_underlying_mismatch() => {
                Permanent
            }
            Self::Raindex(RaindexError::Evm(evm)) | Self::Tokenizer(TokenizerError::Evm(evm))
                if evm.is_revert() =>
            {
                Permanent
            }
            Self::Alpaca(error) | Self::Tokenizer(TokenizerError::Alpaca(error)) => {
                alpaca_resume_failure_kind(error)
            }
            Self::Tokenizer(TokenizerError::MintVerification(error)) => {
                verification_resume_failure_kind(error)
            }
            // The onchain step already landed; only recording its gas cost
            // failed, and a later enqueue can succeed.
            Self::Send(AggregateError::UserError(LifecycleError::Apply(
                EquityRedemptionError::BotGasEnqueueFailed(_),
            ))) => Retryable,
            Self::Send(AggregateError::UserError(_))
            | Self::Raindex(RaindexError::ZeroAmount)
            | Self::Tokenizer(TokenizerError::MissingRedemptionWallet)
            | Self::SharesConversion(_)
            | Self::EntityNotFound { .. }
            | Self::SendFailed { .. }
            | Self::UnexpectedEntity { .. }
            | Self::Rejected
            | Self::LegacyVaultWithdrawPending { .. }
            | Self::PreparedWithdrawalHashMismatch { .. } => Permanent,
            Self::Send(_)
            | Self::RebalancingNotEnabled { .. }
            | Self::LegacyIssuerSendPending { .. }
            | Self::ReattestLegacyUnderlying(_)
            | Self::PrepareTaskJoin(_)
            | Self::ChainServicesMissing(_)
            | Self::GasReadiness(_)
            | Self::Raindex(_)
            | Self::VaultLookup(_)
            | Self::Tokenizer(TokenizerError::Evm(_))
            | Self::BotGasEnqueue(_)
            | Self::UnexpectedPendingStatus => Retryable,
        }
    }

    pub(crate) fn gas_readiness_retry_interval(&self) -> Option<std::time::Duration> {
        match self {
            Self::GasReadiness(failure) => failure.retry_interval(),
            _ => None,
        }
    }

    /// The vault attests another underlying than the legacy record names, so
    /// the bot refuses to send the recorded token. Retrying cannot change it.
    pub(crate) fn is_permanent_underlying_mismatch(&self) -> bool {
        matches!(
            self,
            Self::ReattestLegacyUnderlying(EquityRedemptionError::LegacyUnderlyingMismatch { .. })
        )
    }

    /// The redemption is still resolving onchain and its own jobs drive it:
    /// a pending withdrawal or send to the issuer, a send signed by a rotated key, or a
    /// legacy send an operator must verify. Not a failure of the caller.
    pub(crate) fn is_still_in_progress(&self) -> bool {
        self.is_reconciliation_pending()
            || matches!(self, Self::LegacyIssuerSendPending { .. })
            || matches!(
                self,
                Self::Send(AggregateError::UserError(LifecycleError::Apply(
                    EquityRedemptionError::RedemptionSendSignedByAnotherWallet { .. },
                )))
            )
    }

    pub(crate) fn is_reconciliation_pending(&self) -> bool {
        matches!(self, Self::Raindex(error) if error.is_reconciliation_pending())
            || matches!(
                self,
                Self::Send(AggregateError::UserError(LifecycleError::Apply(
                    EquityRedemptionError::RaindexWithdrawReconciliationPending { .. },
                )))
            )
            || matches!(
                self,
                Self::Send(AggregateError::UserError(LifecycleError::Apply(
                    EquityRedemptionError::RedemptionSendUnresolved { .. },
                )))
            )
    }
}

impl BotGasFailureClassifier for RedemptionError {
    fn is_bot_gas_enqueue_failure(&self) -> bool {
        match self {
            Self::Send(AggregateError::UserError(LifecycleError::Apply(
                EquityRedemptionError::BotGasEnqueueFailed(_),
            )))
            | Self::BotGasEnqueue(_) => true,
            Self::RebalancingNotEnabled { .. }
            | Self::LegacyIssuerSendPending { .. }
            | Self::ReattestLegacyUnderlying(_)
            | Self::PrepareTaskJoin(_)
            | Self::ChainServicesMissing(_)
            | Self::GasReadiness(_)
            | Self::Send(_)
            | Self::Raindex(_)
            | Self::VaultLookup(_)
            | Self::Alpaca(_)
            | Self::Tokenizer(_)
            | Self::SharesConversion(_)
            | Self::EntityNotFound { .. }
            | Self::SendFailed { .. }
            | Self::UnexpectedEntity { .. }
            | Self::UnexpectedPendingStatus
            | Self::Rejected
            | Self::LegacyVaultWithdrawPending { .. }
            | Self::PreparedWithdrawalHashMismatch { .. } => false,
        }
    }
}

/// Result of wrapping received mint tokens into ERC-4626 shares.
///
/// Returned by [`CrossVenueEquityTransfer::wrap_received_mint`] to give
/// each element a clear domain name and avoid positional ambiguity in the
/// `(Address, U256, u64)` tuple it replaces.
struct WrappedMintResult {
    /// ERC-4626 derivative token address (the vault).
    token: Address,
    /// Number of ERC-4626 shares minted by the wrap.
    shares: U256,
    /// Block number in which the wrap transaction was confirmed.
    block: u64,
}

/// Orchestrates equity transfers between Raindex and Alpaca.
///
/// Holds CQRS stores for both directions and domain service traits for
/// vault operations and tokenization. External code drives transfers only
/// through [`Self::resume_equity_to_market_making`] and
/// [`Self::resume_equity_to_hedging`].
type RedemptionPrepareLocks =
    std::sync::Mutex<HashMap<RedemptionAggregateId, Weak<tokio::sync::Mutex<()>>>>;

pub struct CrossVenueEquityTransfer {
    /// The per-chain entries every step resolves through -- the same map the
    /// two aggregates' command handlers read. The chain comes from the
    /// aggregate the step is driving, so a resume never borrows another
    /// chain's wallet, registry, RPC or issuer.
    services: EquityTransferServices,
    mint_store: Arc<Store<TokenizedEquityMint>>,
    redemption_store: Arc<Store<EquityRedemption>>,
    redemption_send_prepare: RedemptionPrepareLocks,
    /// Mint-authorization capability for orchestrator-mode assets
    /// (RAI-1243). Defaults to [`ConfiguredMintAuthorization::VaultDirectOnly`]
    /// (an explicit assertion, mirroring `BotGasReceiptCostEnqueuer`'s
    /// explicit-absence shape); production opts into
    /// [`ConfiguredMintAuthorization::Wired`] via
    /// [`Self::with_mint_authorization`].
    mint_authorization: ConfiguredMintAuthorization,
    rebalancing: Option<Weak<RebalancingService>>,
}

/// Whether this transfer can produce and deliver MintAuthV1 recipient
/// authorizations. Absence is a stated capability, not an inferred one: a
/// construction site choosing `VaultDirectOnly` asserts that every asset it
/// mints is vault-direct, rather than silently defaulting there.
pub(crate) enum ConfiguredMintAuthorization {
    /// Full orchestrator-mode wiring -- the server path, which always has
    /// the issuance client and job queue to build it.
    Wired(Box<MintAuthorizationWiring>),
    /// No wiring: the caller (CLI transfer subcommands, recovery jobs,
    /// tests) asserts the assets it mints are vault-direct. Without the
    /// wiring `vault_mode` cannot be read at all, so an orchestrator-mode
    /// mint through such a path proceeds unauthorized and stalls at
    /// polling -- surfaced by a warning per mint.
    VaultDirectOnly,
}

/// Everything the mint saga needs to produce and deliver a MintAuthV1
/// recipient authorization for orchestrator-mode assets (RAI-1243). The
/// `token` the MintAuth binds is not here: it comes from the record chain's
/// own [`ChainEquityServices::equities`] table.
pub(crate) struct MintAuthorizationWiring {
    /// Reads each asset's `vault_mode` from issuance -- the single source
    /// of truth for which assets need an authorization; the bot keeps no
    /// asset-mode list of its own.
    pub(crate) vault_mode_reader: Arc<dyn VaultModeReader>,
    /// Delivery job queue; enqueued idempotently per issuer request id.
    pub(crate) delivery_queue: DeliverMintAuthorizationJobQueue,
}

impl CrossVenueEquityTransfer {
    pub fn new(
        services: EquityTransferServices,
        mint_store: Arc<Store<TokenizedEquityMint>>,
        redemption_store: Arc<Store<EquityRedemption>>,
    ) -> Self {
        Self {
            services,
            mint_store,
            redemption_store,
            redemption_send_prepare: std::sync::Mutex::new(HashMap::new()),
            mint_authorization: ConfiguredMintAuthorization::VaultDirectOnly,
            rebalancing: None,
        }
    }

    /// Links fresh-transfer cleanup to the service's deferred startup restores.
    pub(crate) fn with_rebalancing_service(mut self, service: &Arc<RebalancingService>) -> Self {
        self.rebalancing = Some(Arc::downgrade(service));
        self
    }

    async fn cancel_deferred_restore(
        &self,
        reservation_id: crate::position::EquityTransferReservationId,
    ) {
        if let Some(service) = self.rebalancing.as_ref().and_then(Weak::upgrade) {
            service
                .cancel_pending_equity_transfer_reservation_restore(reservation_id)
                .await;
        }
    }

    /// The redemption event store, for callers that must read a redemption's
    /// durable state directly -- e.g. the reconciliation-redrive deadline
    /// ([`withdrawal_reconciliation_redrive_delay`]) driven by the generic
    /// resume job, whose ctx holds only this transfer.
    pub(crate) fn redemption_store(&self) -> &Arc<Store<EquityRedemption>> {
        &self.redemption_store
    }

    /// Proves through `chain`'s own raindex and bot wallet that none of
    /// `signed`, the signed vault withdrawal or every signed copy of the issuer
    /// send of a redemption on that chain, can ever land; see
    /// [`verify_signed_redemption_txs_superseded`].
    pub(crate) async fn verify_signed_txs_superseded(
        &self,
        chain: Chain,
        kind: SignedRedemptionTx,
        signed: &[&PreparedTransaction],
        superseding_tx: Option<TxHash>,
        required_confirmations: u64,
    ) -> Result<(), SignedTxNotSuperseded> {
        let services = self
            .services
            .for_chain(chain)
            .map_err(|error| SignedTxNotSuperseded {
                kind,
                refusal: error.into(),
            })?;

        verify_signed_redemption_txs_superseded(
            services.raindex.as_ref(),
            kind,
            signed,
            superseding_tx,
            services.wallet,
            required_confirmations,
        )
        .await
    }

    /// Checks through `chain`'s own raindex that `tx`, the only withdrawal a
    /// redemption on that chain holds, did not go through; see
    /// [`verify_hash_only_withdrawal_not_through`].
    pub(crate) async fn verify_hash_only_withdrawal_not_through(
        &self,
        chain: Chain,
        tx: TxHash,
    ) -> Result<(), WithdrawalNotSuperseded> {
        let services = self.services.for_chain(chain)?;

        verify_hash_only_withdrawal_not_through(services.raindex.as_ref(), tx).await
    }

    /// Checks through `chain`'s own raindex and bot wallet that `replacement`
    /// can be adopted in place of `prepared`, the signed vault withdrawal of a
    /// redemption on that chain; see [`verify_withdrawal_replacement`].
    pub(crate) async fn verify_withdrawal_replacement(
        &self,
        chain: Chain,
        prepared: &PreparedTransaction,
        replacement: TxHash,
        required_confirmations: u64,
    ) -> Result<(), ReplacementNotAdoptable> {
        let services = self.services.for_chain(chain)?;

        verify_withdrawal_replacement(
            services.raindex.as_ref(),
            prepared,
            replacement,
            services.wallet,
            required_confirmations,
        )
        .await
    }

    /// Opts this transfer into orchestrator-mode mint authorization.
    /// Called at the production wiring site only.
    pub(crate) fn with_mint_authorization(mut self, wiring: MintAuthorizationWiring) -> Self {
        self.mint_authorization = ConfiguredMintAuthorization::Wired(Box::new(wiring));
        self
    }

    /// Ensures an orchestrator-mode mint has its recipient authorization
    /// signed and its delivery enqueued before polling begins; a no-op for
    /// vault-direct assets. Idempotent end to end: signing no-ops once an
    /// authorization exists (the nonce is the mint's on-chain idempotency
    /// key and must never be re-minted), and the delivery enqueue is keyed
    /// on the issuer request id.
    async fn ensure_mint_authorization(
        &self,
        issuer_request_id: &IssuerRequestId,
        chain: Chain,
        symbol: &Symbol,
    ) -> Result<(), MintError> {
        let wiring = match &self.mint_authorization {
            ConfiguredMintAuthorization::Wired(wiring) => wiring,
            // The construction site asserted vault-direct-only; without the
            // wiring `vault_mode` cannot be read, so the assertion cannot
            // be verified -- surface it per mint.
            ConfiguredMintAuthorization::VaultDirectOnly => {
                warn!(
                    target: "rebalance",
                    %issuer_request_id,
                    %symbol,
                    "Transfer constructed VaultDirectOnly; minting without a \
                     vault-mode check (an orchestrator-mode asset would \
                     stall at polling -- such mints must run through the \
                     server, which always wires authorization)"
                );
                return Ok(());
            }
        };

        // Fails closed: an indeterminate mode must not be guessed as
        // vault-direct, or a required authorization would be skipped and
        // the mint would stall at issuance's on-chain step.
        match wiring.vault_mode_reader.vault_mode(symbol).await? {
            VaultModeTag::VaultDirect => Ok(()),
            VaultModeTag::Orchestrator => {
                // The MintAuth names the tokenized equity as deployed on the
                // record's chain; the primary's table would bind the wrong
                // contract wherever the addresses differ.
                let token = self
                    .services
                    .for_chain(chain)?
                    .equities
                    .symbols
                    .get(symbol)
                    .map(|equity| equity.tokenized_equity)
                    .ok_or_else(|| MintError::UnknownTokenizedEquity {
                        chain,
                        symbol: symbol.clone(),
                    })?;

                self.mint_store
                    .send(
                        issuer_request_id,
                        TokenizedEquityMintCommand::SignMintAuthorization { token },
                    )
                    .await?;

                // Bounds delivery to ONE live chain per mint: redrive
                // successors carry no idempotency key, so without this
                // check a resume during a delayed-redrive window would
                // start a second chain. Fails open on a guard read error
                // (warn + push): a duplicate chain delivers byte-identical
                // payloads and is benign, while a skipped push would stall
                // the delivery.
                let delivery_live = authorization_job::has_live_delivery_job(
                    &wiring.delivery_queue,
                    issuer_request_id,
                )
                .await
                .unwrap_or_else(|error| {
                    warn!(
                        target: "rebalance",
                        %issuer_request_id,
                        ?error,
                        "Live-delivery guard read failed; enqueueing anyway \
                         (duplicate chains are benign)"
                    );
                    false
                });
                if delivery_live {
                    info!(
                        target: "rebalance",
                        %issuer_request_id,
                        %symbol,
                        "Mint authorization delivery already live; not \
                         enqueueing another"
                    );
                    return Ok(());
                }

                let mut delivery_queue = wiring.delivery_queue.clone();
                delivery_queue
                    .push_idempotent(
                        &issuer_request_id.to_string(),
                        DeliverMintAuthorization {
                            issuer_request_id: issuer_request_id.clone(),
                            redrive_attempts: 0,
                        },
                    )
                    .await?;

                info!(
                    target: "rebalance",
                    %issuer_request_id,
                    %symbol,
                    "Mint authorization signed and delivery enqueued"
                );
                Ok(())
            }
        }
    }

    /// Enqueues bot-gas cost recording for a confirmed mint-side tx (vault
    /// deposit or wrap) on the chain the mint runs on. Converts a push
    /// failure into `MintError::BotGasEnqueue` carrying a
    /// `BotGasEnqueueFailure` -- built here (not via `#[from]`) so the tx
    /// hash the failed enqueue was for is captured while it's still in
    /// scope; `QueuePushError` alone can't carry it back out through
    /// `MintError`.
    async fn enqueue_bot_gas_cost(
        &self,
        chain: Chain,
        tx_hash: TxHash,
        category: BotGasOperationCategory,
        symbol: Symbol,
    ) -> Result<(), MintError> {
        self.services
            .bot_gas_enqueuer
            .enqueue(RecordBotGasReceiptCost::for_transfer_tx(
                chain, tx_hash, category, symbol,
            ))
            .await
            .map_err(|error| {
                MintError::BotGasEnqueue(BotGasEnqueueFailure::from_queue_push_error(
                    tx_hash, &error,
                ))
            })
    }

    /// Loads the aggregate after Poll and extracts fields from the
    /// TokensReceived state needed for verification and wrapping.
    async fn load_tokens_received(
        &self,
        issuer_request_id: &IssuerRequestId,
    ) -> Result<TokensReceivedData, MintError> {
        let entity = self
            .mint_store
            .load(issuer_request_id)
            .await?
            .ok_or_else(|| MintError::EntityNotFound {
                issuer_request_id: issuer_request_id.clone(),
                expected_state: "TokensReceived",
            })?;

        match entity {
            TokenizedEquityMint::TokensReceived {
                shares_minted,
                tx_hash,
                symbol,
                chain,
                wallet,
                ..
            } => Ok(TokensReceivedData {
                shares_minted,
                tx_hash,
                symbol,
                chain,
                wallet,
            }),
            other => Err(MintError::UnexpectedState {
                issuer_request_id: issuer_request_id.clone(),
                expected_state: "TokensReceived",
                entity: Box::new(other),
            }),
        }
    }

    async fn load_mint_entity(
        &self,
        issuer_request_id: &IssuerRequestId,
    ) -> Result<TokenizedEquityMint, MintError> {
        self.mint_store
            .load(issuer_request_id)
            .await?
            .ok_or_else(|| MintError::EntityNotFound {
                issuer_request_id: issuer_request_id.clone(),
                expected_state: "active mint state",
            })
    }

    async fn finalize_received_mint(
        &self,
        issuer_request_id: &IssuerRequestId,
        tokens_received: TokensReceivedData,
    ) -> Result<(), MintError> {
        self.verify_received_mint(&tokens_received).await?;

        let WrappedMintResult {
            token,
            shares,
            block,
        } = self
            .wrap_received_mint(issuer_request_id, &tokens_received)
            .await?;

        // Wait for the RPC node to catch up to the block where the wrap tx
        // confirmed before depositing. Without this, a load-balanced backend
        // that hasn't indexed the wrap block yet sees the wrapped-token balance
        // as zero and the vault deposit reverts with ERC20InsufficientBalance.
        self.services
            .for_chain(tokens_received.chain)?
            .wrapper
            .wait_for_block(block)
            .await?;

        self.deposit_wrapped_mint(
            issuer_request_id,
            &tokens_received.symbol,
            tokens_received.chain,
            token,
            shares,
        )
        .await
    }

    async fn deposit_wrapped_mint(
        &self,
        issuer_request_id: &IssuerRequestId,
        symbol: &Symbol,
        chain: Chain,
        wrapped_token: Address,
        wrapped_shares: U256,
    ) -> Result<(), MintError> {
        let chain_services = self.services.for_chain(chain)?;
        let vault_id = chain_services
            .vault_lookup
            .vault_id_for_token(wrapped_token)
            .await?;

        let vault_deposit_tx_hash = chain_services
            .raindex
            .submit_deposit(
                wrapped_token,
                vault_id,
                wrapped_shares,
                TOKENIZED_EQUITY_DECIMALS,
            )
            .await?;

        self.mint_store
            .send(
                issuer_request_id,
                TokenizedEquityMintCommand::SubmitVaultDeposit {
                    vault_deposit_tx_hash,
                },
            )
            .await?;

        chain_services
            .raindex
            .confirm_tx(vault_deposit_tx_hash)
            .await?;
        self.enqueue_bot_gas_cost(
            chain,
            vault_deposit_tx_hash,
            BotGasOperationCategory::VaultDeposit,
            symbol.clone(),
        )
        .await?;

        self.mint_store
            .send(
                issuer_request_id,
                TokenizedEquityMintCommand::DepositToVault {
                    vault_deposit_tx_hash,
                },
            )
            .await?;

        info!(target: "rebalance", %symbol, %vault_deposit_tx_hash, "Mint workflow completed");
        Ok(())
    }

    /// Attempts vault deposit; if it reverts with `ERC20InsufficientBalance`,
    /// the deposit already landed in a previous session -- advance the
    /// aggregate to terminal state. Used by both the `TokensWrapped` and
    /// `WrapSubmitted` resume paths.
    ///
    /// # Invariant: tokens only leave the wallet via vault deposit
    ///
    /// This recovery assumes that the *only* way wrapped tokens leave the
    /// wallet is through a successful Raindex vault deposit. Under this
    /// invariant, a zero balance at retry time proves the deposit landed
    /// in a prior session that crashed before persisting the CQRS event.
    ///
    /// If this invariant is violated (e.g. manual token transfer, bug in
    /// another code path), the recovery would incorrectly mark the mint
    /// complete while tokens are lost. The invariant holds today because
    /// the wrap -> deposit lifecycle is the sole consumer of these tokens
    /// and runs atomically within a single task.
    async fn try_deposit_or_recover(
        &self,
        issuer_request_id: &IssuerRequestId,
        symbol: &Symbol,
        chain: Chain,
        wrapped_token: Address,
        wrapped_shares: U256,
    ) -> Result<(), MintError> {
        match self
            .deposit_wrapped_mint(
                issuer_request_id,
                symbol,
                chain,
                wrapped_token,
                wrapped_shares,
            )
            .await
        {
            Ok(()) => Ok(()),
            Err(error) if error.is_insufficient_balance_revert() => {
                warn!(
                    target: "rebalance",
                    %issuer_request_id,
                    %symbol,
                    ?error,
                    "Vault deposit reverted on resume -- deposit likely \
                    already landed in a previous session; closing operation"
                );

                // The real deposit tx hash is unknown (see the TxHash::ZERO
                // comment below), so its bot-gas cost can never be enqueued
                // here -- there is no tx hash to enqueue a
                // RecordBotGasReceiptCost job against. This is a real,
                // narrow financial-data gap (a confirmed bot-signed tx whose
                // gas cost will never be recorded), so it is logged loudly
                // rather than silently skipped -- see AGENTS.md's "CRITICAL:
                // Financial Data Integrity" and SPEC.md's bot-gas "Known
                // gaps".
                warn!(
                    target: "rebalance",
                    %issuer_request_id,
                    %symbol,
                    "Bot-gas receipt cost: the real vault-deposit tx hash for this \
                     crash-recovered mint is unknown (TxHash::ZERO sentinel); its gas \
                     cost cannot be recorded and this cost fact is permanently lost"
                );

                // TxHash::ZERO signals that the real deposit TX hash is
                // unknown -- the deposit succeeded in a previous session
                // that crashed before persisting the CQRS event.
                self.mint_store
                    .send(
                        issuer_request_id,
                        TokenizedEquityMintCommand::DepositToVault {
                            vault_deposit_tx_hash: TxHash::ZERO,
                        },
                    )
                    .await?;

                Ok(())
            }
            Err(error) => Err(error),
        }
    }

    async fn verify_received_mint(
        &self,
        tokens_received: &TokensReceivedData,
    ) -> Result<(), MintError> {
        info!(target: "rebalance",
            shares_minted = %tokens_received.shares_minted,
            tx_hash = %tokens_received.tx_hash,
            "Tokens received, verifying onchain"
        );

        let chain_services = self.services.for_chain(tokens_received.chain)?;
        let unwrapped_token = chain_services
            .wrapper
            .lookup_underlying(&tokens_received.symbol)?;
        chain_services
            .tokenizer
            .verify_mint_tx(
                tokens_received.tx_hash,
                unwrapped_token,
                tokens_received.wallet,
                tokens_received.shares_minted,
            )
            .await
            .inspect_err(|error| {
                warn!(target: "rebalance", %error, "Onchain mint verification failed");
            })?;

        Ok(())
    }

    async fn wrap_received_mint(
        &self,
        issuer_request_id: &IssuerRequestId,
        tokens_received: &TokensReceivedData,
    ) -> Result<WrappedMintResult, MintError> {
        info!(target: "rebalance", "Onchain verification passed, wrapping into ERC-4626 shares");

        let chain_services = self.services.for_chain(tokens_received.chain)?;
        let token = chain_services
            .wrapper
            .lookup_derivative(&tokens_received.symbol)?;

        let wrap_tx_hash = chain_services
            .wrapper
            .submit_wrap(token, tokens_received.shares_minted, tokens_received.wallet)
            .await?;

        self.mint_store
            .send(
                issuer_request_id,
                TokenizedEquityMintCommand::SubmitWrap { wrap_tx_hash },
            )
            .await?;

        let WrapConfirmation { shares, block } = chain_services
            .wrapper
            .confirm_wrap(token, wrap_tx_hash)
            .await?;
        self.enqueue_bot_gas_cost(
            tokens_received.chain,
            wrap_tx_hash,
            BotGasOperationCategory::Wrap,
            tokens_received.symbol.clone(),
        )
        .await?;

        self.mint_store
            .send(
                issuer_request_id,
                TokenizedEquityMintCommand::WrapTokens {
                    wrap_tx_hash,
                    wrapped_shares: shares,
                    wrap_block: block,
                },
            )
            .await?;

        info!(target: "rebalance", %wrap_tx_hash, %shares, "Tokens wrapped, depositing to Raindex vault");
        Ok(WrappedMintResult {
            token,
            shares,
            block,
        })
    }

    #[allow(clippy::cognitive_complexity)]
    pub(crate) async fn resume_mint(
        &self,
        issuer_request_id: &IssuerRequestId,
    ) -> Result<(), MintError> {
        loop {
            match self.load_mint_entity(issuer_request_id).await? {
                TokenizedEquityMint::MintAccepted { symbol, chain, .. } => {
                    info!(%issuer_request_id, "Resuming accepted mint");
                    // Idempotent: signing no-ops once an authorization
                    // exists and the delivery enqueue is keyed on the
                    // issuer request id, so a resumed orchestrator-mode
                    // mint re-delivers its PERSISTED authorization rather
                    // than minting a fresh nonce.
                    self.ensure_mint_authorization(issuer_request_id, chain, &symbol)
                        .await?;
                    self.mint_store
                        .send(issuer_request_id, TokenizedEquityMintCommand::Poll)
                        .await?;
                }
                TokenizedEquityMint::TokensReceived { .. } => {
                    info!(%issuer_request_id, "Resuming received mint");
                    let tokens_received = self.load_tokens_received(issuer_request_id).await?;
                    return self
                        .finalize_received_mint(issuer_request_id, tokens_received)
                        .await;
                }
                TokenizedEquityMint::WrapSubmitted {
                    wrap_tx_hash,
                    symbol,
                    chain,
                    ..
                } => {
                    info!(%issuer_request_id, %wrap_tx_hash, "Resuming submitted wrap");
                    let chain_services = self.services.for_chain(chain)?;
                    let wrapped_token = chain_services.wrapper.lookup_derivative(&symbol)?;
                    let WrapConfirmation {
                        shares: wrapped_shares,
                        block: wrap_block,
                    } = chain_services
                        .wrapper
                        .confirm_wrap(wrapped_token, wrap_tx_hash)
                        .await?;
                    self.enqueue_bot_gas_cost(
                        chain,
                        wrap_tx_hash,
                        BotGasOperationCategory::Wrap,
                        symbol.clone(),
                    )
                    .await?;

                    self.mint_store
                        .send(
                            issuer_request_id,
                            TokenizedEquityMintCommand::WrapTokens {
                                wrap_tx_hash,
                                wrapped_shares,
                                wrap_block,
                            },
                        )
                        .await?;

                    chain_services.wrapper.wait_for_block(wrap_block).await?;

                    info!(target: "rebalance", %wrap_tx_hash, %wrapped_shares, "Wrap confirmed on resume, depositing to Raindex vault");
                    return self
                        .try_deposit_or_recover(
                            issuer_request_id,
                            &symbol,
                            chain,
                            wrapped_token,
                            wrapped_shares,
                        )
                        .await;
                }
                TokenizedEquityMint::TokensWrapped {
                    symbol,
                    chain,
                    wrapped_shares,
                    wrap_block,
                    ..
                } => {
                    info!(%issuer_request_id, "Resuming wrapped mint");
                    let chain_services = self.services.for_chain(chain)?;
                    let wrapped_token = chain_services.wrapper.lookup_derivative(&symbol)?;

                    // Skip the wait for legacy aggregates persisted before wrap_block was added.
                    if let Some(block) = wrap_block {
                        chain_services.wrapper.wait_for_block(block).await?;
                    }

                    return self
                        .try_deposit_or_recover(
                            issuer_request_id,
                            &symbol,
                            chain,
                            wrapped_token,
                            wrapped_shares,
                        )
                        .await;
                }
                TokenizedEquityMint::VaultDepositSubmitted {
                    vault_deposit_tx_hash,
                    symbol,
                    chain,
                    ..
                } => {
                    info!(%issuer_request_id, %vault_deposit_tx_hash, "Resuming submitted vault deposit");
                    self.services
                        .for_chain(chain)?
                        .raindex
                        .confirm_tx(vault_deposit_tx_hash)
                        .await?;
                    self.enqueue_bot_gas_cost(
                        chain,
                        vault_deposit_tx_hash,
                        BotGasOperationCategory::VaultDeposit,
                        symbol.clone(),
                    )
                    .await?;

                    self.mint_store
                        .send(
                            issuer_request_id,
                            TokenizedEquityMintCommand::DepositToVault {
                                vault_deposit_tx_hash,
                            },
                        )
                        .await?;

                    info!(target: "rebalance", %symbol, %vault_deposit_tx_hash, "Vault deposit confirmed on resume");
                    return Ok(());
                }
                TokenizedEquityMint::DepositedIntoRaindex { .. }
                | TokenizedEquityMint::Failed { .. }
                | TokenizedEquityMint::Reconciled { .. } => return Ok(()),
                TokenizedEquityMint::MintRequested { .. } => {
                    info!(%issuer_request_id, "Reconciling requested mint");
                    self.mint_store
                        .send(
                            issuer_request_id,
                            TokenizedEquityMintCommand::ReconcileMintRequest {
                                issuer_request_id: issuer_request_id.clone(),
                            },
                        )
                        .await?;
                }
            }
        }
    }

    /// Signs the exact vault withdrawal, persists that identity, broadcasts its
    /// bytes, records the hash, then confirms it.
    async fn withdraw_from_raindex(
        &self,
        aggregate_id: &RedemptionAggregateId,
        symbol: &Symbol,
        chain: Chain,
        quantity: FractionalShares,
        token: Address,
        amount: U256,
    ) -> Result<(), RedemptionError> {
        let chain_services = self.services.for_chain(chain)?;
        let vault_id = chain_services
            .vault_lookup
            .vault_id_for_token(token)
            .await?;
        let prepared = chain_services
            .raindex
            .prepare_withdraw(token, vault_id, amount, TOKENIZED_EQUITY_DECIMALS)
            .await?;

        let persist_result = self
            .redemption_store
            .send(
                aggregate_id,
                EquityRedemptionCommand::Redeem {
                    symbol: symbol.clone(),
                    chain,
                    quantity: quantity.inner(),
                    token,
                    vault_id,
                    amount,
                    from_block: 0,
                    prepared: prepared.clone(),
                },
            )
            .await;
        if let Err(error) = persist_result {
            // A reactor error may be returned after the event committed. Reload
            // before touching nonce state: only a definitely absent aggregate
            // proves these signed bytes were not persisted and will never be
            // broadcast by recovery.
            if matches!(self.redemption_store.load(aggregate_id).await, Ok(None)) {
                chain_services
                    .raindex
                    .discard_prepared_withdraw(prepared.tx_hash())
                    .await;
            }
            return Err(error.into());
        }

        self.broadcast_and_record_vault_withdrawal(aggregate_id, chain, &prepared)
            .await?;

        self.redemption_store
            .send(aggregate_id, EquityRedemptionCommand::ConfirmWithdraw)
            .await?;

        Ok(())
    }

    /// Release the wallet nonce reservation a reconciled redemption's prepared
    /// vault withdrawal still holds. The reconcile itself is pure bookkeeping
    /// and never reaches the wallet, so the running bot releases the reservation
    /// here the first time a resume observes the durable `Reconciled`. Another
    /// transaction has already mined at the withdrawal's nonce, so allocation is
    /// not rewound. The underlying release is idempotent, so a redriven or
    /// duplicate observation is a harmless no-op.
    pub(crate) async fn discard_reconciled_withdrawal(
        &self,
        chain: Chain,
        tx_hash: TxHash,
    ) -> Result<(), RedemptionError> {
        self.services
            .for_chain(chain)?
            .raindex
            .release_superseded_withdraw(tx_hash)
            .await;
        Ok(())
    }

    pub(crate) async fn discard_reconciled_issuer_send(
        &self,
        chain: Chain,
        tx_hash: TxHash,
    ) -> Result<(), RedemptionError> {
        self.services
            .for_chain(chain)?
            .tokenizer
            .release_superseded_redemption_send(tx_hash)
            .await;
        Ok(())
    }

    async fn broadcast_and_record_vault_withdrawal(
        &self,
        aggregate_id: &RedemptionAggregateId,
        chain: Chain,
        prepared: &PreparedTransaction,
    ) -> Result<(), RedemptionError> {
        let tx_hash = self.broadcast_vault_withdrawal(chain, prepared).await?;
        self.record_vault_withdrawal_submission(aggregate_id, tx_hash)
            .await
    }

    async fn broadcast_vault_withdrawal(
        &self,
        chain: Chain,
        prepared: &PreparedTransaction,
    ) -> Result<TxHash, RedemptionError> {
        let expected = prepared.tx_hash();
        info!(target: "rebalance", %expected, "Broadcasting prepared Raindex vault withdrawal");
        let actual = self
            .services
            .for_chain(chain)?
            .raindex
            .broadcast_prepared_withdraw(prepared)
            .await?;
        if actual != expected {
            return Err(RedemptionError::PreparedWithdrawalHashMismatch { expected, actual });
        }
        Ok(actual)
    }

    async fn record_vault_withdrawal_submission(
        &self,
        aggregate_id: &RedemptionAggregateId,
        tx_hash: TxHash,
    ) -> Result<(), RedemptionError> {
        self.redemption_store
            .send(
                aggregate_id,
                EquityRedemptionCommand::RecordWithdrawSubmission { tx_hash },
            )
            .await?;
        Ok(())
    }

    /// Signs the send to the issuer once and persists it (`PrepareSend`) before any
    /// broadcast, on a detached task so a cancelled caller (a dropped API
    /// request or job future) cannot stop between the two: a signed send that
    /// was never persisted keeps its nonce reserved and stalls every later
    /// send from the wallet. A failed write goes through
    /// [`release_unpersisted_redemption_send`].
    async fn prepare_redemption_send(
        &self,
        aggregate_id: &RedemptionAggregateId,
    ) -> Result<(), RedemptionError> {
        let prepare_lock = {
            let mut locks = self
                .redemption_send_prepare
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            locks.retain(|_, lock| lock.strong_count() > 0);
            let lock = locks
                .get(aggregate_id)
                .and_then(Weak::upgrade)
                .unwrap_or_else(|| Arc::new(tokio::sync::Mutex::new(())));
            locks.insert(aggregate_id.clone(), Arc::downgrade(&lock));
            drop(locks);
            lock
        };
        let services = self.services.clone();
        let store = Arc::clone(&self.redemption_store);
        let task_id = aggregate_id.clone();
        let replacement_cutoff = self
            .rebalancing
            .as_ref()
            .and_then(Weak::upgrade)
            .map(|service| service.transfer_timeout());
        // `PrepareSend` can outlive a cancelled caller, so the task continues
        // the caller's projection slot.
        let projection_slot =
            crate::conductor::projection_pause::projection_slot_for_detached_work().await;

        tokio::spawn(async move {
            let _projection_slot = projection_slot;
            let _prepare_guard = prepare_lock.lock().await;
            prepare_and_persist_redemption_send(&services, &store, &task_id, replacement_cutoff)
                .await
        })
        .await
        .inspect_err(|join_error| {
            error!(target: "rebalance", %aggregate_id, %join_error, "Redemption send prepare-and-persist task failed to join (panicked)");
        })?
    }

    /// Unwraps ERC-4626 tokens and sends to Alpaca, returning the
    /// redemption tx hash.
    async fn unwrap_and_send(
        &self,
        aggregate_id: &RedemptionAggregateId,
    ) -> Result<TxHash, RedemptionError> {
        self.redemption_store
            .send(aggregate_id, EquityRedemptionCommand::UnwrapTokens)
            .await?;

        self.redemption_store
            .send(aggregate_id, EquityRedemptionCommand::SubmitUnwrap)
            .await?;

        self.redemption_store
            .send(aggregate_id, EquityRedemptionCommand::ConfirmUnwrap)
            .await?;

        info!(target: "rebalance", %aggregate_id, "Tokens unwrapped, sending to Alpaca");

        self.prepare_redemption_send(aggregate_id).await?;
        self.redemption_store
            .send(aggregate_id, EquityRedemptionCommand::SendTokens)
            .await?;

        let entity = self.redemption_store.load(aggregate_id).await?.ok_or(
            RedemptionError::EntityNotFound {
                aggregate_id: aggregate_id.clone(),
            },
        )?;

        match entity {
            EquityRedemption::TokensSent {
                chain,
                symbol,
                redemption_tx,
                ..
            } => {
                self.enqueue_redemption_send_gas_cost(chain, redemption_tx, symbol)
                    .await?;
                Ok(redemption_tx)
            }
            entity @ EquityRedemption::Failed { .. } => Err(RedemptionError::SendFailed { entity }),
            entity => Err(RedemptionError::UnexpectedEntity { entity }),
        }
    }

    /// Enqueues the gas fact only after `TokensSent` has persisted the
    /// non-idempotent transfer's hash. A failed queue write can therefore be
    /// retried from aggregate state without sending tokens again.
    async fn enqueue_redemption_send_gas_cost(
        &self,
        chain: Chain,
        redemption_tx: TxHash,
        symbol: Symbol,
    ) -> Result<(), RedemptionError> {
        self.services
            .bot_gas_enqueuer
            .enqueue(RecordBotGasReceiptCost::for_transfer_tx(
                chain,
                redemption_tx,
                BotGasOperationCategory::WalletTransfer,
                symbol,
            ))
            .await
            .map_err(|error| {
                RedemptionError::BotGasEnqueue(BotGasEnqueueFailure::from_queue_push_error(
                    redemption_tx,
                    &error,
                ))
            })
    }

    /// Polls for redemption detection and records it.
    async fn poll_detection(
        &self,
        aggregate_id: &RedemptionAggregateId,
        chain: Chain,
        tx_hash: &TxHash,
    ) -> Result<TokenizationRequestId, RedemptionError> {
        let detected = match self
            .services
            .for_chain(chain)?
            .tokenizer
            .poll_for_redemption(tx_hash)
            .await
        {
            Ok(req) => req,
            Err(error) => {
                warn!(target: "rebalance", %error, %tx_hash, "Polling for redemption detection failed");
                let failure = match &error {
                    TokenizerError::Alpaca(AlpacaTokenizationError::PollTimeout { .. }) => {
                        DetectionFailure::Timeout
                    }
                    TokenizerError::Alpaca(other) => DetectionFailure::ApiError {
                        status_code: other.status_code().map(|status| status.as_u16()),
                    },
                    TokenizerError::Evm(_) | TokenizerError::MissingRedemptionWallet => {
                        warn!(target: "rebalance", %tx_hash, "Unexpected onchain error during redemption detection");
                        DetectionFailure::ApiError { status_code: None }
                    }
                    TokenizerError::MintVerification(verification_error) => {
                        warn!(target: "rebalance",
                            %verification_error,
                            %tx_hash,
                            "Unexpected MintVerification error during redemption detection"
                        );
                        DetectionFailure::ApiError { status_code: None }
                    }
                };

                self.redemption_store
                    .send(
                        aggregate_id,
                        EquityRedemptionCommand::FailDetection { failure },
                    )
                    .await?;

                return Err(error.into());
            }
        };

        self.redemption_store
            .send(
                aggregate_id,
                EquityRedemptionCommand::Detect {
                    tokenization_request_id: detected.id.clone(),
                },
            )
            .await?;

        Ok(detected.id)
    }

    /// Polls for completion and finalizes the redemption.
    async fn poll_completion(
        &self,
        aggregate_id: &RedemptionAggregateId,
        chain: Chain,
        request_id: &TokenizationRequestId,
    ) -> Result<(), RedemptionError> {
        let completed = match self
            .services
            .for_chain(chain)?
            .tokenizer
            .poll_redemption_until_complete(request_id)
            .await
        {
            Ok(req) => req,
            Err(error) => {
                warn!(target: "rebalance", %error, %request_id, "Polling for completion failed");
                self.redemption_store
                    .send(
                        aggregate_id,
                        EquityRedemptionCommand::RejectRedemption {
                            reason: error.to_string(),
                        },
                    )
                    .await?;
                return Err(error.into());
            }
        };

        match completed.status {
            TokenizationRequestStatus::Completed => {
                self.redemption_store
                    .send(aggregate_id, EquityRedemptionCommand::Complete)
                    .await?;
                Ok(())
            }
            TokenizationRequestStatus::Rejected => {
                self.redemption_store
                    .send(
                        aggregate_id,
                        EquityRedemptionCommand::RejectRedemption {
                            reason: "Alpaca rejected the redemption request".to_string(),
                        },
                    )
                    .await?;
                Err(RedemptionError::Rejected)
            }
            TokenizationRequestStatus::Pending => {
                warn!(target: "rebalance", %request_id, "poll_redemption_until_complete returned Pending status");
                Err(RedemptionError::UnexpectedPendingStatus)
            }
        }
    }

    async fn load_redemption_entity(
        &self,
        aggregate_id: &RedemptionAggregateId,
    ) -> Result<EquityRedemption, RedemptionError> {
        self.redemption_store
            .load(aggregate_id)
            .await?
            .ok_or_else(|| RedemptionError::EntityNotFound {
                aggregate_id: aggregate_id.clone(),
            })
    }

    #[allow(clippy::cognitive_complexity)]
    pub(crate) async fn resume_redemption(
        &self,
        aggregate_id: &RedemptionAggregateId,
    ) -> Result<(), RedemptionError> {
        loop {
            match self.load_redemption_entity(aggregate_id).await? {
                EquityRedemption::VaultWithdrawPending { .. } => {
                    return Err(RedemptionError::LegacyVaultWithdrawPending {
                        aggregate_id: aggregate_id.clone(),
                    });
                }
                EquityRedemption::VaultWithdrawSubmitting {
                    chain, prepared, ..
                } => {
                    info!(
                        %aggregate_id,
                        tx_hash = %prepared.tx_hash(),
                        "Broadcasting persisted vault withdrawal"
                    );
                    self.broadcast_and_record_vault_withdrawal(aggregate_id, chain, &prepared)
                        .await?;
                    self.redemption_store
                        .send(aggregate_id, EquityRedemptionCommand::ConfirmWithdraw)
                        .await?;
                }
                EquityRedemption::VaultWithdrawSubmitted {
                    chain,
                    tx_hash,
                    prepared,
                    ..
                } => {
                    self.services
                        .for_chain(chain)?
                        .raindex
                        .restore_submitted_withdrawal(tx_hash, prepared.as_ref())
                        .await?;
                    if let Some(prepared) = prepared.as_ref() {
                        info!(
                            %aggregate_id,
                            %tx_hash,
                            "Rebroadcasting persisted submitted vault withdrawal"
                        );
                        self.broadcast_vault_withdrawal(chain, prepared).await?;
                    }
                    info!(%aggregate_id, "Resuming submitted vault withdrawal");
                    self.redemption_store
                        .send(aggregate_id, EquityRedemptionCommand::ConfirmWithdraw)
                        .await?;
                }
                EquityRedemption::WithdrawnFromRaindex { .. } => {
                    self.resume_withdrawn_redemption(aggregate_id).await?;
                }
                EquityRedemption::UnwrapPending { .. } => {
                    info!(%aggregate_id, "Resuming pending unwrap");
                    self.redemption_store
                        .send(aggregate_id, EquityRedemptionCommand::SubmitUnwrap)
                        .await?;
                }
                EquityRedemption::UnwrapSubmitted { .. } => {
                    info!(%aggregate_id, "Resuming submitted unwrap");
                    self.redemption_store
                        .send(aggregate_id, EquityRedemptionCommand::ConfirmUnwrap)
                        .await?;
                }
                EquityRedemption::TokensUnwrapped { .. } => {
                    self.resume_unwrapped_redemption(aggregate_id).await?;
                }
                EquityRedemption::SendPending { .. } => {
                    info!(%aggregate_id, "Resuming pending send");
                    self.prepare_redemption_send(aggregate_id).await?;
                    self.redemption_store
                        .send(aggregate_id, EquityRedemptionCommand::SendTokens)
                        .await?;
                }
                EquityRedemption::TokensSent {
                    chain,
                    symbol,
                    redemption_tx,
                    ..
                } => {
                    self.resume_sent_redemption(aggregate_id, chain, symbol, &redemption_tx)
                        .await?;
                }
                EquityRedemption::Pending {
                    tokenization_request_id,
                    chain,
                    ..
                } => {
                    return self
                        .resume_pending_redemption(aggregate_id, chain, &tokenization_request_id)
                        .await;
                }
                EquityRedemption::Completed { .. }
                | EquityRedemption::Failed { .. }
                | EquityRedemption::Reconciled { .. } => {
                    return Ok(());
                }
            }
        }
    }

    async fn resume_withdrawn_redemption(
        &self,
        aggregate_id: &RedemptionAggregateId,
    ) -> Result<(), RedemptionError> {
        info!(%aggregate_id, "Resuming withdrawn redemption");
        self.redemption_store
            .send(aggregate_id, EquityRedemptionCommand::UnwrapTokens)
            .await?;
        Ok(())
    }

    async fn resume_unwrapped_redemption(
        &self,
        aggregate_id: &RedemptionAggregateId,
    ) -> Result<(), RedemptionError> {
        info!(%aggregate_id, "Resuming unwrapped redemption");
        self.prepare_redemption_send(aggregate_id).await?;
        Ok(())
    }

    async fn resume_sent_redemption(
        &self,
        aggregate_id: &RedemptionAggregateId,
        chain: Chain,
        symbol: Symbol,
        redemption_tx: &TxHash,
    ) -> Result<(), RedemptionError> {
        info!(%aggregate_id, "Resuming sent redemption");
        self.enqueue_redemption_send_gas_cost(chain, *redemption_tx, symbol)
            .await?;

        match self
            .poll_detection(aggregate_id, chain, redemption_tx)
            .await
        {
            Ok(_) => Ok(()),
            Err(error) => {
                self.ignore_redemption_error_if_terminal(aggregate_id, error)
                    .await
            }
        }
    }

    async fn resume_pending_redemption(
        &self,
        aggregate_id: &RedemptionAggregateId,
        chain: Chain,
        tokenization_request_id: &TokenizationRequestId,
    ) -> Result<(), RedemptionError> {
        info!(%aggregate_id, "Resuming detected redemption");
        match self
            .poll_completion(aggregate_id, chain, tokenization_request_id)
            .await
        {
            Ok(()) => Ok(()),
            Err(error) => {
                self.ignore_redemption_error_if_terminal(aggregate_id, error)
                    .await
            }
        }
    }

    async fn ignore_redemption_error_if_terminal(
        &self,
        aggregate_id: &RedemptionAggregateId,
        error: RedemptionError,
    ) -> Result<(), RedemptionError> {
        if self.redemption_is_terminal(aggregate_id).await? {
            Ok(())
        } else {
            Err(error)
        }
    }

    async fn redemption_is_terminal(
        &self,
        aggregate_id: &RedemptionAggregateId,
    ) -> Result<bool, RedemptionError> {
        Ok(matches!(
            self.load_redemption_entity(aggregate_id).await?,
            EquityRedemption::Completed { .. }
                | EquityRedemption::Failed { .. }
                | EquityRedemption::Reconciled { .. }
        ))
    }

    /// Re-checks a stuck mint against the tokenization provider and, if the
    /// provider has settled it, un-fails the aggregate and resumes the
    /// wrap/deposit workflow. Active (non-failed) mints simply resume.
    ///
    /// Dispatches `RecoverProviderCompletion` through the reactor-wired mint
    /// store after rebuilding tracking, so the live inventory view is
    /// corrected. Must run inside the bot process for the reactor to fire.
    pub(crate) async fn recover_mint(
        &self,
        issuer_request_id: &IssuerRequestId,
        pool: &SqlitePool,
        rebalancing: &RebalancingService,
    ) -> Result<RecheckOutcome, RecheckError> {
        let entity = self.load_mint_entity(issuer_request_id).await?;

        let (symbol, quantity) = match &entity {
            // Both terminals are already settled: nothing to recheck.
            TokenizedEquityMint::DepositedIntoRaindex { .. }
            | TokenizedEquityMint::Reconciled { .. } => {
                return Ok(RecheckOutcome::AlreadyCompleted);
            }
            TokenizedEquityMint::Failed {
                symbol, quantity, ..
            } => (symbol.clone(), FractionalShares::new(*quantity)),
            _ => {
                self.resume_mint(issuer_request_id).await?;
                return Ok(RecheckOutcome::Resumed);
            }
        };

        let context = load_mint_recheck_context(pool, issuer_request_id).await?;

        // Provider-completion recovery only applies to mints that failed at
        // acceptance (tokens never received). A mint that already received
        // tokens and then failed while wrapping/depositing must not be reset
        // to TokensReceived and re-wrapped against tokens that already moved.
        if context.received_tokens {
            warn!(
                target: "rebalance",
                %issuer_request_id,
                "Mint failed after receiving tokens; provider-completion recovery does not apply"
            );
            return Ok(RecheckOutcome::NotRecoverable);
        }

        let request = match self
            .services
            .for_chain(entity.chain())?
            .tokenizer
            .get_request(&context.tokenization_request_id)
            .await
        {
            Ok(request) => request,
            // A missing provider request is "left unchanged", not a hard error:
            // the request may not exist or may not be visible yet. Other tokenizer
            // failures (transient, verification) still propagate.
            Err(TokenizerError::Alpaca(AlpacaTokenizationError::RequestNotFound { id })) => {
                warn!(
                    target: "rebalance",
                    %issuer_request_id,
                    %id,
                    "Provider request not found; leaving mint unchanged"
                );
                return Ok(RecheckOutcome::LeftUnchanged);
            }
            Err(error) => return Err(error.into()),
        };

        if request.status != TokenizationRequestStatus::Completed {
            return Ok(RecheckOutcome::LeftUnchanged);
        }

        let tx_hash = request
            .tx_hash
            .ok_or_else(|| RecheckError::MissingTxHash(request.id.clone()))?;

        // Rebuild tracking (and restore the canonical in-flight inventory shape,
        // clearing any timeout tombstone) before dispatching so the reactor
        // applies the ProviderCompletionRecovered inventory effect when it
        // processes the event below.
        //
        // The rollback below covers a *persistence* failure (store.send returns
        // Err -> the event was never committed and the reactor never ran). It
        // does NOT cover a reactor-application failure: cqrs-es dispatches the
        // reactor after persisting, and the bridge logs reactor errors without
        // propagating them, so send still returns Ok. A reactor failure here
        // would leave the event persisted but inventory un-completed until the
        // next inventory poll/restart reconciles it.
        // Compare-and-claim: refuse recovery while a *different* mint for this
        // symbol is live (rebuilding would overwrite its in-flight balance, since
        // `set_inflight` replaces rather than adds). A slot still owned by this
        // same mint -- e.g. the live process never observed its failure, as with
        // a reactor-less failure injection -- is not a conflict; that is exactly
        // the stale state recovery reconciles. The check is atomic with the
        // in-flight restore so a concurrent mint cannot claim the slot in between.
        let (rollback, recovery_guard) = match rebalancing
            .rebuild_mint_tracking_for_recovery(issuer_request_id, &entity, request.id.clone())
            .await?
        {
            RecoveryClaim::Conflict => return Ok(RecheckOutcome::Conflict),
            RecoveryClaim::Claimed(rollback) => (rollback, None),
            RecoveryClaim::Guarded { rollback, guard } => (rollback, Some(guard)),
        };

        if let Err(error) = self
            .mint_store
            .send(
                issuer_request_id,
                TokenizedEquityMintCommand::RecoverProviderCompletion {
                    issuer_request_id: issuer_request_id.clone(),
                    wallet: context.wallet,
                    tokenization_request_id: request.id,
                    tx_hash,
                    fees: request.fees,
                },
            )
            .await
        {
            // The recovery event was not persisted, so the reactor never ran to
            // complete the in-flight that the rebuild restored. Undo the rebuild
            // so the live inventory does not show a phantom in-flight transfer or
            // a symbol locked in-progress until the next restart.
            if let Err(rollback_error) = rebalancing
                .rollback_mint_tracking_for_recovery(
                    issuer_request_id,
                    &symbol,
                    entity.chain(),
                    quantity,
                    rollback,
                )
                .await
            {
                error!(
                    target: "rebalance",
                    %issuer_request_id,
                    ?rollback_error,
                    "Failed to roll back mint recovery state after dispatch failure"
                );
            }

            return Err(MintError::from(error).into());
        }

        // The recovery event is committed and the reactor has corrected
        // inventory. A resume failure now (e.g. a transient RPC error during
        // wrapping) must not be fatal: the aggregate is recovered and startup
        // recovery will finish the workflow. Clear the in-progress guard so the
        // symbol is not locked out of rebalancing until the next restart.
        if let Err(error) = self.resume_mint(issuer_request_id).await {
            warn!(
                target: "rebalance",
                %issuer_request_id,
                ?error,
                "Mint recovered but resume failed; cleared in-progress guard, workflow resumes on next startup"
            );
            rebalancing
                .abandon_mint_recovery_guard(issuer_request_id, &symbol)
                .await;
        }

        if let Some(guard) = recovery_guard {
            guard.release();
        }

        Ok(RecheckOutcome::Recovered)
    }

    /// Re-checks a stuck redemption against the tokenization provider and, if
    /// the provider has settled it, un-fails the aggregate to `Completed`.
    /// Active (non-failed) redemptions simply resume.
    ///
    /// Dispatches `RecoverProviderCompletion` through the reactor-wired
    /// redemption store after rebuilding tracking, so the in-flight transfer
    /// is completed in the live inventory view.
    pub(crate) async fn recover_redemption(
        &self,
        aggregate_id: &RedemptionAggregateId,
        rebalancing: &RebalancingService,
    ) -> Result<RecheckOutcome, RecheckError> {
        let entity = self.load_redemption_entity(aggregate_id).await?;

        let (symbol, tokenization_request_id, redemption_tx) = match &entity {
            // Both terminals are already settled: nothing to recheck.
            EquityRedemption::Completed { .. } | EquityRedemption::Reconciled { .. } => {
                return Ok(RecheckOutcome::AlreadyCompleted);
            }
            EquityRedemption::Failed {
                symbol,
                redemption_tx: Some(redemption_tx),
                tokenization_request_id,
                ..
            } => (
                symbol.clone(),
                tokenization_request_id.clone(),
                *redemption_tx,
            ),
            // A redemption that failed before sending tokens has nothing for
            // the provider to have settled.
            EquityRedemption::Failed { .. } => return Ok(RecheckOutcome::NotRecoverable),
            _ => {
                self.resume_redemption(aggregate_id).await?;
                return Ok(RecheckOutcome::Resumed);
            }
        };

        let chain_services = self.services.for_chain(entity.chain())?;
        let request = match &tokenization_request_id {
            // A missing provider request leaves the redemption unchanged rather
            // than failing the operator command; transient/other errors propagate.
            Some(request_id) => match chain_services.tokenizer.get_request(request_id).await {
                Ok(request) => request,
                Err(TokenizerError::Alpaca(AlpacaTokenizationError::RequestNotFound { id })) => {
                    warn!(
                        target: "rebalance",
                        %aggregate_id,
                        %id,
                        "Provider request not found; leaving redemption unchanged"
                    );
                    return Ok(RecheckOutcome::LeftUnchanged);
                }
                Err(error) => return Err(error.into()),
            },
            None => match chain_services
                .tokenizer
                .find_redemption_by_tx(&redemption_tx)
                .await?
            {
                Some(request) => request,
                None => return Ok(RecheckOutcome::NotDetectedYet),
            },
        };

        if request.status != TokenizationRequestStatus::Completed {
            return Ok(RecheckOutcome::LeftUnchanged);
        }

        // `load` can observe the committed failure before that command's inline
        // terminal reactor has cleaned up its live tracking. A no-event command
        // on the same aggregate waits behind that command and all its reactors,
        // so the rebuild below cannot be erased by delayed terminal cleanup.
        self.redemption_store
            .send(
                aggregate_id,
                EquityRedemptionCommand::SynchronizeProviderCompletionRecovery,
            )
            .await
            .map_err(RedemptionError::from)?;

        // Compare-and-claim: refuse recovery while a *different* redemption for
        // this symbol is live (see recover_mint for the rationale). A slot still
        // owned by this same redemption is not a conflict. The check is atomic
        // with the in-flight restore.
        let (rollback, recovery_guard) = match rebalancing
            .rebuild_redemption_tracking_for_recovery(aggregate_id, &entity)
            .await?
        {
            RecoveryClaim::Conflict => return Ok(RecheckOutcome::Conflict),
            RecoveryClaim::Claimed(rollback) => (rollback, None),
            RecoveryClaim::Guarded { rollback, guard } => (rollback, Some(guard)),
        };

        if let Err(error) = self
            .redemption_store
            .send(
                aggregate_id,
                EquityRedemptionCommand::RecoverProviderCompletion {
                    tokenization_request_id: request.id,
                },
            )
            .await
        {
            // The recovery event was not persisted, so the reactor never ran to
            // complete the in-flight that the rebuild restored. Undo the rebuild
            // so the live inventory does not show a phantom in-flight transfer or
            // a symbol locked in-progress until the next restart.
            if let Err(rollback_error) = rebalancing
                .rollback_redemption_tracking_for_recovery(aggregate_id, &symbol, rollback)
                .await
            {
                error!(
                    target: "rebalance",
                    %aggregate_id,
                    ?rollback_error,
                    "Failed to roll back redemption recovery state after dispatch failure"
                );
            }

            return Err(RedemptionError::from(error).into());
        }

        if let Some(guard) = recovery_guard {
            guard.release();
        }

        Ok(RecheckOutcome::Recovered)
    }
}

impl CrossVenueEquityTransfer {
    /// Hedging -> Market-Making: drives the mint lifecycle (tokenize equity
    /// on Alpaca, wrap, deposit into the Raindex vault) for the given
    /// `issuer_request_id`, whether fresh or interrupted.
    ///
    /// The id is chosen by the caller at enqueue time so apalis retries (and
    /// bot restarts that re-pick the job row) re-enter the same aggregate: an
    /// absent aggregate starts a new mint, an in-flight one resumes from its
    /// persisted state, and a terminal one is a no-op.
    #[instrument(
        target = "rebalance",
        skip_all,
        fields(%issuer_request_id, %symbol, %chain, %quantity)
    )]
    pub async fn resume_equity_to_market_making(
        &self,
        issuer_request_id: &IssuerRequestId,
        symbol: &Symbol,
        chain: Chain,
        quantity: FractionalShares,
    ) -> Result<(), MintTransferError> {
        let existing = self
            .mint_store
            .load(issuer_request_id)
            .await
            .map_err(|error| MintTransferError::PreReceipt(error.into()))?;

        match existing {
            None => {
                self.start_mint(issuer_request_id, symbol, chain, quantity)
                    .await
            }
            Some(
                TokenizedEquityMint::MintRequested { .. }
                | TokenizedEquityMint::MintAccepted { .. },
            ) => {
                if let Err(error) = self.resume_mint(issuer_request_id).await {
                    let reached = self.load_mint_entity(issuer_request_id).await;
                    return Err(classify_mint_resume_error(reached, error));
                }

                Ok(())
            }
            Some(_) => self
                .resume_mint(issuer_request_id)
                .await
                .map_err(MintTransferError::PostReceipt),
        }
    }

    async fn start_mint(
        &self,
        issuer_request_id: &IssuerRequestId,
        symbol: &Symbol,
        chain: Chain,
        quantity: FractionalShares,
    ) -> Result<(), MintTransferError> {
        let chain_services = self
            .services
            .for_chain(chain)
            .map_err(MintError::from)
            .map_err(MintTransferError::PreReceipt)?;

        let mode = self.services.rebalancing_mode(chain, symbol);
        if !mode.starts_operations() {
            self.cancel_deferred_restore(crate::position::EquityTransferReservationId::from_uuid(
                issuer_request_id.0,
            ))
            .await;
            return Err(MintTransferError::PreReceipt(
                MintError::RebalancingNotEnabled {
                    chain,
                    symbol: symbol.clone(),
                    mode,
                },
            ));
        }

        chain_services
            .gas_readiness
            .ensure_ready(TransferGasRoute::Equity)
            .await
            .map_err(MintError::from)
            .map_err(MintTransferError::PreReceipt)?;

        let wallet = chain_services.wallet;
        debug!(target: "rebalance", %issuer_request_id, %chain, %wallet, "Requesting mint");

        // Pre-receipt: no tokens exist yet, safe to retry on failure.
        self.mint_store
            .send(
                issuer_request_id,
                TokenizedEquityMintCommand::RequestMint {
                    issuer_request_id: issuer_request_id.clone(),
                    symbol: symbol.clone(),
                    chain,
                    quantity: quantity.inner(),
                    wallet,
                },
            )
            .await
            .map_err(|error| MintTransferError::PreReceipt(error.into()))?;

        self.mint_store
            .send(
                issuer_request_id,
                TokenizedEquityMintCommand::SubmitMintRequest {
                    issuer_request_id: issuer_request_id.clone(),
                },
            )
            .await
            .map_err(|error| MintTransferError::PreReceipt(error.into()))?;

        let submitted = self
            .load_mint_entity(issuer_request_id)
            .await
            .map_err(MintTransferError::PreReceipt)?;
        match submitted {
            TokenizedEquityMint::Failed { .. } => {
                warn!(
                    target: "rebalance",
                    %issuer_request_id,
                    %symbol,
                    "Mint rejected at submission; abandoning the transfer"
                );
                return Ok(());
            }
            TokenizedEquityMint::MintAccepted { .. } => {}
            entity => {
                return Err(MintTransferError::PreReceipt(MintError::UnexpectedState {
                    issuer_request_id: issuer_request_id.clone(),
                    expected_state: "MintAccepted or Failed",
                    entity: Box::new(entity),
                }));
            }
        }

        // Orchestrator-mode assets need the recipient authorization signed
        // and on its way to issuance BEFORE polling: issuance will not mint
        // (and the poll cannot complete) until the authorization arrives.
        // Still pre-receipt -- a failure here leaves the mint resumable.
        self.ensure_mint_authorization(issuer_request_id, chain, symbol)
            .await
            .map_err(MintTransferError::PreReceipt)?;

        info!(target: "rebalance", "Mint request accepted, polling for completion");

        self.mint_store
            .send(issuer_request_id, TokenizedEquityMintCommand::Poll)
            .await
            .map_err(|error| MintTransferError::PreReceipt(error.into()))?;

        // Post-receipt: tokens exist in wallet from this point on.
        let tokens_received = self
            .load_tokens_received(issuer_request_id)
            .await
            .map_err(MintTransferError::PostReceipt)?;

        self.finalize_received_mint(issuer_request_id, tokens_received)
            .await
            .map_err(MintTransferError::PostReceipt)
    }
}

impl CrossVenueEquityTransfer {
    /// Market-Making -> Hedging: drives the redemption lifecycle (withdraw
    /// tokenized equity from the Raindex vault, unwrap, send to Alpaca for
    /// redemption) for the given `aggregate_id`, whether fresh or
    /// interrupted.
    ///
    /// The id is chosen by the caller at enqueue time so apalis retries (and
    /// bot restarts that re-pick the job row) re-enter the same aggregate: an
    /// absent aggregate starts a new redemption, an in-flight one resumes
    /// from its persisted state, and a terminal one is a no-op.
    #[instrument(
        target = "rebalance",
        skip_all,
        fields(%aggregate_id, %symbol, %chain, %quantity)
    )]
    pub async fn resume_equity_to_hedging(
        &self,
        aggregate_id: &RedemptionAggregateId,
        symbol: &Symbol,
        chain: Chain,
        quantity: FractionalShares,
    ) -> Result<(), RedemptionError> {
        let existing = self.redemption_store.load(aggregate_id).await?;

        match existing {
            None => {
                self.start_redemption(aggregate_id, symbol, chain, quantity)
                    .await
            }
            Some(_) => self.resume_redemption(aggregate_id).await,
        }
    }

    async fn start_redemption(
        &self,
        aggregate_id: &RedemptionAggregateId,
        symbol: &Symbol,
        chain: Chain,
        quantity: FractionalShares,
    ) -> Result<(), RedemptionError> {
        let chain_services = self.services.for_chain(chain)?;
        let mode = self.services.rebalancing_mode(chain, symbol);
        if !mode.starts_operations() {
            self.cancel_deferred_restore(crate::position::EquityTransferReservationId::from_uuid(
                aggregate_id.0,
            ))
            .await;
            return Err(RedemptionError::RebalancingNotEnabled {
                chain,
                symbol: symbol.clone(),
                mode,
            });
        }

        chain_services
            .gas_readiness
            .ensure_ready(TransferGasRoute::Equity)
            .await?;

        let token = chain_services
            .vault_lookup
            .vault_token_for_symbol(symbol)
            .await?;
        let amount = quantity.to_u256_18_decimals()?;

        info!(target: "rebalance", %token, %amount, %aggregate_id, "Starting equity transfer to hedging venue");

        self.withdraw_from_raindex(aggregate_id, symbol, chain, quantity, token, amount)
            .await?;

        info!(target: "rebalance", "Withdrawn from Raindex, unwrapping and sending to Alpaca");

        let redemption_tx = self.unwrap_and_send(aggregate_id).await?;

        info!(target: "rebalance", %redemption_tx, "Tokens sent, polling for detection");
        let request_id = self
            .poll_detection(aggregate_id, chain, &redemption_tx)
            .await?;

        info!(target: "rebalance", %request_id, "Redemption detected, awaiting completion");
        self.poll_completion(aggregate_id, chain, &request_id)
            .await?;

        info!(target: "rebalance", "Equity transfer to hedging venue completed successfully");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::inventory::PollFreshness;
    use alloy::primitives::{Address, B256, Bytes, address};
    use async_trait::async_trait;
    use chrono::Utc;
    use futures_util::poll;
    use rain_math_float::Float;
    use sqlx::SqlitePool;
    use std::collections::{BTreeMap, HashMap};
    use std::pin::pin;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;
    use tokio::sync::{Notify, broadcast};

    use st0x_config::{
        AllocationCtx, ChainAssets, ChainEquities, ChainEquityAsset, OperationMode,
        RebalancingMode, UsdcCorridors,
    };
    use st0x_dto::Statement;
    use st0x_event_sorcery::{
        AggregateError, EntityList, LifecycleError, Never, Reactor, StoreBuilder, deps, test_store,
    };
    use st0x_evm::Chain;
    use st0x_execution::{FractionalShares, Symbol};
    use st0x_float_macro::float;
    use st0x_tokenization::mock::{
        MockCompletionOutcome, MockDetectionOutcome, MockMintRequestOutcome, MockTokenizer,
        MockVerificationOutcome,
    };
    use st0x_tokenization::tokenization_request_id;
    use st0x_tokenization::{ClientRequestId, TokenizationRequestType, issuer_request_id};
    use st0x_wrapper::MockWrapper;

    use super::*;
    use crate::bot_gas::{RecordBotGasReceiptCostJobQueue, pending_bot_gas_jobs};
    use crate::equity_redemption::{
        EquityRedemptionError, EquityRedemptionEvent, redemption_aggregate_id,
    };
    use crate::inventory::{BroadcastingInventory, Inventory, InventoryView, Venue};
    use crate::mint_authorization::{
        MintAuthorizationError, MintAuthorizer, MockMintAuthorizer, SignedMintAuthorization,
        StubVaultModeReader,
    };
    use crate::native_gas::GasReadiness;
    use crate::onchain::mock::{ConfirmTxBehavior, DepositBehavior, DepositCall, MockRaindex};
    use crate::rebalancing::{
        ChainRebalancingConfig, RebalancingSchedulers, RebalancingServiceConfig,
    };
    use crate::tokenized_equity_mint::TokenizedEquityMintEvent;
    use crate::usdc_rebalance::UsdcRebalance;
    use crate::vault_lookup::MockVaultLookup;
    use crate::vault_registry::{VaultRegistry, VaultRegistryId};

    fn mock_vault_lookup() -> MockVaultLookup {
        MockVaultLookup::new()
            .with_symbol_token(Symbol::new("TEST").unwrap(), Address::ZERO)
            .with_vault(Address::ZERO, RaindexVaultId(B256::ZERO))
            .with_default_vault(RaindexVaultId(B256::ZERO))
    }

    struct BlockingRedemptionTerminalReactor {
        entered: Notify,
        release: Notify,
    }

    deps!(BlockingRedemptionTerminalReactor, [EquityRedemption]);

    #[async_trait]
    impl Reactor for BlockingRedemptionTerminalReactor {
        type Error = Never;

        async fn react(
            &self,
            event: <Self::Dependencies as EntityList>::Event,
        ) -> Result<(), Self::Error> {
            let (_id, event) = event.into_inner();
            if matches!(event, EquityRedemptionEvent::RedemptionRejected { .. }) {
                self.entered.notify_one();
                self.release.notified().await;
            }

            Ok(())
        }
    }

    /// One chain's entry, distinguished by the tokenizer the caller passes so
    /// a test can tell which chain's services a command reached.
    fn chain_services(tokenizer: Arc<dyn Tokenizer>) -> ChainEquityServices {
        ChainEquityServices {
            wallet: Address::ZERO,
            raindex: Arc::new(MockRaindex::new()),
            vault_lookup: Arc::new(mock_vault_lookup()),
            tokenizer,
            wrapper: Arc::new(MockWrapper::new()),
            mint_authorizer: ConfiguredMintAuthorizer::Disabled,
            gas_readiness: ConfiguredGasReadiness::Unwired,
            equities: enabled_transfer_listings(),
        }
    }

    /// A chain the map does not carry is named rather than served by another
    /// chain's wallet, vault and issuer.
    #[test]
    fn for_chain_names_a_chain_with_no_services() {
        let services = EquityTransferServices {
            chains: BTreeMap::from([(Chain::Base, chain_services(Arc::new(MockTokenizer::new())))]),
            bot_gas_enqueuer: BotGasReceiptCostEnqueuer::Disabled,
        };

        assert_eq!(
            services.for_chain(Chain::Base).unwrap().wallet,
            Address::ZERO
        );

        let Err(ChainServicesMissing { chain }) = services.for_chain(Chain::Ethereum) else {
            panic!("an unwired chain must be refused, not served by another chain's services");
        };
        assert_eq!(chain, Chain::Ethereum);
    }

    /// One chain's mocks, kept by the test so it can tell which chain's
    /// services a resume drove.
    struct ChainMocks {
        raindex: Arc<MockRaindex>,
        vault_lookup: Arc<MockVaultLookup>,
        tokenizer: Arc<MockTokenizer>,
        wrapper: Arc<MockWrapper>,
    }

    impl ChainMocks {
        fn new() -> Self {
            Self {
                raindex: Arc::new(MockRaindex::new()),
                vault_lookup: Arc::new(mock_vault_lookup()),
                tokenizer: Arc::new(
                    MockTokenizer::new()
                        .with_detection_outcome(MockDetectionOutcome::Detected)
                        .with_completion_outcome(MockCompletionOutcome::Completed),
                ),
                wrapper: Arc::new(MockWrapper::new()),
            }
        }

        fn entry(&self) -> ChainEquityServices {
            ChainEquityServices {
                wallet: Address::ZERO,
                raindex: self.raindex.clone(),
                vault_lookup: self.vault_lookup.clone(),
                tokenizer: self.tokenizer.clone(),
                wrapper: self.wrapper.clone(),
                mint_authorizer: ConfiguredMintAuthorizer::Disabled,
                gas_readiness: ConfiguredGasReadiness::Unwired,
                equities: enabled_transfer_listings(),
            }
        }

        /// Nothing on this chain was touched: no vault read, no wrap wait, no
        /// deposit, no confirmation and no issuer call.
        fn assert_untouched(&self, chain: Chain) {
            assert_eq!(self.vault_lookup.lookups(), 0, "{chain} vault registry");
            assert_eq!(
                self.wrapper.wait_for_block_calls(),
                Vec::<u64>::new(),
                "{chain} wrapper"
            );
            assert_eq!(self.raindex.last_deposit_call(), None, "{chain} deposit");
            assert_eq!(self.raindex.last_confirmed_tx(), None, "{chain} confirm");
            assert_eq!(self.tokenizer.call_count(), 0, "{chain} issuer");
        }
    }

    /// A transfer whose Base and Ethereum entries are distinct mocks.
    async fn two_chain_transfer() -> (CrossVenueEquityTransfer, ChainMocks, ChainMocks) {
        let base = ChainMocks::new();
        let ethereum = ChainMocks::new();
        let services = EquityTransferServices {
            chains: BTreeMap::from([
                (Chain::Base, base.entry()),
                (Chain::Ethereum, ethereum.entry()),
            ]),
            bot_gas_enqueuer: BotGasReceiptCostEnqueuer::Disabled,
        };

        let pool = SqlitePool::connect(":memory:").await.unwrap();
        sqlx::migrate!().run(&pool).await.unwrap();
        let mint_store = Arc::new(test_store(pool.clone(), services.clone()));
        let redemption_store = Arc::new(test_store(pool, services.clone()));

        let transfer = CrossVenueEquityTransfer::new(services, mint_store, redemption_store);

        (transfer, base, ethereum)
    }

    /// Requests and submits an Ethereum mint, leaving it accepted by the
    /// issuer so a test can advance it to any later step.
    async fn request_ethereum_mint(transfer: &CrossVenueEquityTransfer, id: &IssuerRequestId) {
        transfer
            .mint_store
            .send(
                id,
                TokenizedEquityMintCommand::RequestMint {
                    issuer_request_id: id.clone(),
                    symbol: Symbol::new("AAPL").unwrap(),
                    chain: Chain::Ethereum,
                    quantity: float!(10),
                    wallet: Address::ZERO,
                },
            )
            .await
            .unwrap();
        transfer
            .mint_store
            .send(
                id,
                TokenizedEquityMintCommand::SubmitMintRequest {
                    issuer_request_id: id.clone(),
                },
            )
            .await
            .unwrap();
    }

    /// A mint resumed from `TokensReceived` wraps and deposits on the chain
    /// its record names, so Base's wrapper, registry and orderbook stay idle.
    #[tokio::test]
    async fn a_resumed_ethereum_mint_wraps_and_deposits_on_ethereum() {
        let (transfer, base, ethereum) = two_chain_transfer().await;
        let id = issuer_request_id("ISS-ETHEREUM-TOKENS-RECEIVED");
        request_ethereum_mint(&transfer, &id).await;
        transfer
            .mint_store
            .send(&id, TokenizedEquityMintCommand::Poll)
            .await
            .unwrap();

        transfer.resume_mint(&id).await.unwrap();

        assert_eq!(
            ethereum.raindex.last_deposit_call(),
            Some(DepositCall {
                token: Address::ZERO,
                vault_id: RaindexVaultId(B256::ZERO),
                amount: U256::from(10_000_000_000_000_000_000_u128),
                decimals: TOKENIZED_EQUITY_DECIMALS,
            }),
            "the wrapped shares must be deposited through Ethereum's orderbook"
        );
        assert_eq!(
            ethereum.vault_lookup.lookups(),
            1,
            "the destination vault must come from Ethereum's registry"
        );
        base.assert_untouched(Chain::Base);
    }

    /// A mint resumed from `WrapSubmitted` confirms the wrap on the chain that
    /// submitted it.
    #[tokio::test]
    async fn a_resumed_ethereum_wrap_confirms_on_ethereum() {
        let (transfer, base, ethereum) = two_chain_transfer().await;
        let id = issuer_request_id("ISS-ETHEREUM-WRAP-SUBMITTED");
        request_ethereum_mint(&transfer, &id).await;
        let wrap_tx_hash = TxHash::with_last_byte(7);
        ethereum
            .wrapper
            .seed_submitted_amount(wrap_tx_hash, U256::from(1_u64));
        for command in [
            TokenizedEquityMintCommand::Poll,
            TokenizedEquityMintCommand::SubmitWrap { wrap_tx_hash },
        ] {
            transfer.mint_store.send(&id, command).await.unwrap();
        }

        transfer.resume_mint(&id).await.unwrap();

        assert_eq!(
            ethereum.wrapper.wait_for_block_calls(),
            vec![0],
            "the wrap must be confirmed against Ethereum's wrapper"
        );
        assert_eq!(
            ethereum.raindex.last_deposit_call(),
            Some(DepositCall {
                token: Address::ZERO,
                vault_id: RaindexVaultId(B256::ZERO),
                amount: U256::from(1_u64),
                decimals: TOKENIZED_EQUITY_DECIMALS,
            }),
            "the deposit must follow on Ethereum's orderbook"
        );
        base.assert_untouched(Chain::Base);
    }

    /// A mint resumed from `VaultDepositSubmitted` confirms the deposit tx on
    /// the chain it was broadcast to.
    #[tokio::test]
    async fn a_resumed_ethereum_vault_deposit_confirms_on_ethereum() {
        let (transfer, base, ethereum) = two_chain_transfer().await;
        let id = issuer_request_id("ISS-ETHEREUM-DEPOSIT-SUBMITTED");
        request_ethereum_mint(&transfer, &id).await;
        let vault_deposit_tx_hash = TxHash::with_last_byte(9);
        for command in [
            TokenizedEquityMintCommand::Poll,
            TokenizedEquityMintCommand::SubmitWrap {
                wrap_tx_hash: TxHash::with_last_byte(8),
            },
            TokenizedEquityMintCommand::WrapTokens {
                wrap_tx_hash: TxHash::with_last_byte(8),
                wrapped_shares: U256::from(1_u64),
                wrap_block: 1,
            },
            TokenizedEquityMintCommand::SubmitVaultDeposit {
                vault_deposit_tx_hash,
            },
        ] {
            transfer.mint_store.send(&id, command).await.unwrap();
        }

        transfer.resume_mint(&id).await.unwrap();

        assert_eq!(
            ethereum.raindex.last_confirmed_tx(),
            Some(vault_deposit_tx_hash),
            "the deposit must be confirmed on Ethereum's RPC"
        );
        base.assert_untouched(Chain::Base);
    }

    /// A fresh redemption resolves its vault token through the chain it was
    /// asked for, never the primary's registry.
    #[tokio::test]
    async fn a_fresh_ethereum_redemption_reads_ethereums_registry() {
        let (transfer, base, ethereum) = two_chain_transfer().await;
        let id = redemption_aggregate_id("ETHEREUM-FRESH-REDEMPTION");

        transfer
            .resume_equity_to_hedging(
                &id,
                &Symbol::new("TEST").unwrap(),
                Chain::Ethereum,
                FractionalShares::new(float!(50)),
            )
            .await
            .unwrap();

        assert_eq!(
            ethereum.vault_lookup.lookups(),
            2,
            "the redeemed token must come from Ethereum's registry"
        );
        assert_eq!(
            ethereum.tokenizer.call_count(),
            8,
            "the redemption must be sent through Ethereum's issuer"
        );
        base.assert_untouched(Chain::Base);
    }

    /// A redemption resumed from `TokensSent` polls the issuer of the chain
    /// the tokens left from.
    #[tokio::test]
    async fn a_resumed_ethereum_redemption_polls_ethereums_issuer() {
        let (transfer, base, ethereum) = two_chain_transfer().await;
        let id = redemption_aggregate_id("ETHEREUM-REDEMPTION-RESUME");
        let symbol = Symbol::new("TEST").unwrap();
        let quantity = FractionalShares::new(float!(50));
        let token = ethereum
            .vault_lookup
            .vault_token_for_symbol(&symbol)
            .await
            .unwrap();

        transfer
            .withdraw_from_raindex(
                &id,
                &symbol,
                Chain::Ethereum,
                quantity,
                token,
                quantity.to_u256_18_decimals().unwrap(),
            )
            .await
            .unwrap();
        transfer.unwrap_and_send(&id).await.unwrap();
        let issuer_calls_before_resume = ethereum.tokenizer.call_count();

        transfer.resume_redemption(&id).await.unwrap();

        assert_eq!(
            ethereum.tokenizer.call_count(),
            issuer_calls_before_resume + 2,
            "the detection and completion polls must reach Ethereum's issuer"
        );
        base.assert_untouched(Chain::Base);
    }

    /// A redemption resumed from `Pending` waits on the completion of the
    /// issuer that already detected it, not the primary's.
    #[tokio::test]
    async fn a_resumed_ethereum_pending_redemption_polls_ethereums_issuer() {
        let (transfer, base, ethereum) = two_chain_transfer().await;
        let id = redemption_aggregate_id("ETHEREUM-PENDING-RESUME");
        let symbol = Symbol::new("TEST").unwrap();
        let quantity = FractionalShares::new(float!(50));
        let token = ethereum
            .vault_lookup
            .vault_token_for_symbol(&symbol)
            .await
            .unwrap();

        transfer
            .withdraw_from_raindex(
                &id,
                &symbol,
                Chain::Ethereum,
                quantity,
                token,
                quantity.to_u256_18_decimals().unwrap(),
            )
            .await
            .unwrap();
        transfer.unwrap_and_send(&id).await.unwrap();
        transfer
            .redemption_store
            .send(
                &id,
                EquityRedemptionCommand::Detect {
                    tokenization_request_id: tokenization_request_id("ETHEREUM-PENDING"),
                },
            )
            .await
            .unwrap();
        let issuer_calls_before_resume = ethereum.tokenizer.call_count();

        transfer.resume_redemption(&id).await.unwrap();

        assert_eq!(
            ethereum.tokenizer.call_count(),
            issuer_calls_before_resume + 1,
            "the completion poll must reach Ethereum's issuer"
        );
        base.assert_untouched(Chain::Base);
    }

    /// A resume follows the chain the record names: both the genesis request
    /// and the later poll reach that chain's issuer, never the primary's.
    #[tokio::test]
    async fn a_resumed_ethereum_mint_uses_the_ethereum_services() {
        let base_tokenizer = Arc::new(MockTokenizer::new());
        let ethereum_tokenizer = Arc::new(MockTokenizer::new());
        let services = EquityTransferServices {
            chains: BTreeMap::from([
                (Chain::Base, chain_services(base_tokenizer.clone())),
                (Chain::Ethereum, chain_services(ethereum_tokenizer.clone())),
            ]),
            bot_gas_enqueuer: BotGasReceiptCostEnqueuer::Disabled,
        };

        let pool = SqlitePool::connect(":memory:").await.unwrap();
        sqlx::migrate!().run(&pool).await.unwrap();
        let mint_store = Arc::new(test_store(pool.clone(), services.clone()));
        let redemption_store = Arc::new(test_store(pool, services.clone()));
        let transfer =
            CrossVenueEquityTransfer::new(services.clone(), mint_store, redemption_store);

        let id = issuer_request_id("ISS-ETHEREUM-RESUME");
        transfer
            .mint_store
            .send(
                &id,
                TokenizedEquityMintCommand::RequestMint {
                    issuer_request_id: id.clone(),
                    symbol: Symbol::new("AAPL").unwrap(),
                    chain: Chain::Ethereum,
                    quantity: float!(10),
                    wallet: Address::ZERO,
                },
            )
            .await
            .unwrap();

        transfer.resume_mint(&id).await.unwrap();

        assert_eq!(
            (
                base_tokenizer.call_count(),
                ethereum_tokenizer.mint_lookup_call_count(),
                ethereum_tokenizer.mint_request_call_count(),
                ethereum_tokenizer.call_count(),
            ),
            (0, 1, 1, 4),
            "the reconcile lookup, the mint request, the resumed poll and the \
             onchain verification must all reach Ethereum's issuer"
        );
    }

    fn enabled_transfer_listings() -> ChainEquities {
        let mut equities = equities_listing("AAPL", Address::ZERO);
        equities
            .symbols
            .extend(equities_listing("TEST", Address::ZERO).symbols);
        equities
    }

    fn mock_services() -> EquityTransferServices {
        EquityTransferServices {
            chains: BTreeMap::from([(
                Chain::Base,
                ChainEquityServices {
                    wallet: Address::ZERO,
                    raindex: Arc::new(MockRaindex::new()),
                    vault_lookup: Arc::new(mock_vault_lookup()),
                    tokenizer: Arc::new(MockTokenizer::new()),
                    wrapper: Arc::new(MockWrapper::new()),
                    mint_authorizer: ConfiguredMintAuthorizer::Disabled,
                    gas_readiness: ConfiguredGasReadiness::Unwired,
                    equities: enabled_transfer_listings(),
                },
            )]),
            bot_gas_enqueuer: BotGasReceiptCostEnqueuer::Disabled,
        }
    }

    async fn submit_requested_mint(transfer: &CrossVenueEquityTransfer, id: &IssuerRequestId) {
        transfer
            .mint_store
            .send(
                id,
                TokenizedEquityMintCommand::SubmitMintRequest {
                    issuer_request_id: id.clone(),
                },
            )
            .await
            .unwrap();
    }

    async fn seed_requested_mint(
        transfer: &CrossVenueEquityTransfer,
        id: &IssuerRequestId,
        symbol: Symbol,
        quantity: FractionalShares,
    ) {
        transfer
            .mint_store
            .send(
                id,
                TokenizedEquityMintCommand::RequestMint {
                    issuer_request_id: id.clone(),
                    symbol,
                    quantity: quantity.inner(),
                    chain: Chain::Base,
                    wallet: transfer.services.for_chain(Chain::Base).unwrap().wallet,
                },
            )
            .await
            .unwrap();
    }

    async fn insert_mint_event(
        pool: &SqlitePool,
        id: &IssuerRequestId,
        sequence: i64,
        event_type: &str,
        payload: &str,
    ) {
        let IssuerRequestId(raw) = id;
        sqlx::query(
            "INSERT INTO events \
             (aggregate_type, aggregate_id, sequence, event_type, event_version, payload, metadata) \
             VALUES ('TokenizedEquityMint', ?, ?, ?, '1.0', ?, '{}')",
        )
        .bind(raw.to_string())
        .bind(sequence)
        .bind(event_type)
        .bind(payload)
        .execute(pool)
        .await
        .unwrap();
    }

    fn mint_accepted_payload(id: &IssuerRequestId) -> String {
        format!(
            r#"{{"MintAccepted":{{"issuer_request_id":"{id}","tokenization_request_id":"tok-1","accepted_at":"2026-01-01T00:00:01Z"}}}}"#
        )
    }

    fn provider_completion_recovered_payload(id: &IssuerRequestId) -> String {
        format!(
            r#"{{"ProviderCompletionRecovered":{{"issuer_request_id":"{id}","wallet":"0x0000000000000000000000000000000000000001","tokenization_request_id":"tok-1","tx_hash":"0x1111111111111111111111111111111111111111111111111111111111111111","shares_minted":"10000000000000000000","fees":null,"recovered_at":"2026-01-01T00:02:00Z"}}}}"#
        )
    }

    #[tokio::test]
    async fn recheck_context_treats_recovered_mint_as_having_received_tokens() {
        // A mint recovered once via ProviderCompletionRecovered evolves into the
        // TokensReceived state without writing a literal TokensReceived event. If
        // it then re-fails at wrapping, a second recheck must NOT recover it again
        // (which would re-wrap already-moved tokens), so received_tokens must be
        // true even though no TokensReceived event exists.
        let pool = crate::test_utils::setup_test_db().await;
        let id = issuer_request_id("mint-rerecover");

        insert_mint_event(
            &pool,
            &id,
            0,
            "TokenizedEquityMintEvent::MintRequested",
            r#"{"MintRequested":{"symbol":"AAPL","quantity":"10","wallet":"0x0000000000000000000000000000000000000001","requested_at":"2026-01-01T00:00:00Z"}}"#,
        )
        .await;
        insert_mint_event(
            &pool,
            &id,
            1,
            "TokenizedEquityMintEvent::MintAccepted",
            &mint_accepted_payload(&id),
        )
        .await;
        insert_mint_event(
            &pool,
            &id,
            2,
            "TokenizedEquityMintEvent::ProviderCompletionRecovered",
            &provider_completion_recovered_payload(&id),
        )
        .await;

        let context = load_mint_recheck_context(&pool, &id).await.unwrap();

        assert!(
            context.received_tokens,
            "a mint already recovered via ProviderCompletionRecovered must count as having received tokens"
        );
    }

    /// Reproduces the production recovery path end to end: a mint that failed
    /// at acceptance (injected reactor-less, exactly as the simulate-failures
    /// harness does) is recovered by dispatching `RecoverProviderCompletion`
    /// through the reactor-wired store. The recovered quantity must leave the
    /// Hedging in-flight balance (the dashboard's "Inflight" column) and land in
    /// MarketMaking available -- not stay stuck in-flight forever.
    #[tokio::test]
    async fn recover_mint_clears_hedging_inflight() {
        let (pool, apalis_pool) = crate::test_utils::setup_test_pools().await;
        let symbol = Symbol::new("AAPL").unwrap();
        let id = issuer_request_id("mint-recover-inflight");

        // Reactor-less failure injection: the live reactor never observes these
        // events, so its inventory holds the full Hedging balance with nothing
        // in-flight -- the stale state recovery must reconcile.
        insert_mint_event(
            &pool,
            &id,
            1,
            "TokenizedEquityMintEvent::MintRequested",
            r#"{"MintRequested":{"symbol":"AAPL","quantity":"10","wallet":"0x0000000000000000000000000000000000000001","requested_at":"2026-01-01T00:00:00Z"}}"#,
        )
        .await;
        insert_mint_event(
            &pool,
            &id,
            2,
            "TokenizedEquityMintEvent::MintAccepted",
            &mint_accepted_payload(&id),
        )
        .await;
        insert_mint_event(
            &pool,
            &id,
            3,
            "TokenizedEquityMintEvent::MintAcceptanceFailed",
            r#"{"MintAcceptanceFailed":{"reason":"simulate: timeout","failed_at":"2026-01-01T00:00:02Z"}}"#,
        )
        .await;

        let (event_sender, _event_receiver) = broadcast::channel::<Statement>(16);
        let inventory = Arc::new(BroadcastingInventory::new(
            InventoryView::default().with_equity(
                symbol.clone(),
                FractionalShares::ZERO,
                FractionalShares::new(float!(100)),
            ),
            event_sender,
        ));

        let service = Arc::new(RebalancingService::new(
            RebalancingServiceConfig {
                poll_freshness: PollFreshness::always_fresh(),
                inventory_staleness_bound: Duration::from_secs(300),
                cash_reserved: None,
                hedge_floor: st0x_execution::HedgeFloor::default(),
                allocation: AllocationCtx::base_test(),
                usdc: UsdcCorridors::base_cctp_disabled(),
                transfer_timeout: Duration::from_secs(1800),
                recovery_hold_alert_after: Duration::from_secs(60 * 60),
                chains: BTreeMap::from([(
                    Chain::Base,
                    ChainRebalancingConfig::for_test(ChainAssets::default()),
                )]),
            },
            Arc::new(test_store::<VaultRegistry>(pool.clone(), ())),
            BTreeMap::from([(
                Chain::Base,
                VaultRegistryId {
                    chain: st0x_evm::Chain::Base,
                    orderbook: address!("0x0000000000000000000000000000000000000001"),
                    owner: address!("0x0000000000000000000000000000000000000002"),
                },
            )]),
            inventory.clone(),
            BTreeMap::from([(
                Chain::Base,
                Arc::new(MockWrapper::new()) as Arc<dyn Wrapper>,
            )]),
            RebalancingSchedulers::new(&apalis_pool),
            Arc::new(crate::alerts::LogNotifier),
        ));

        // Reactor-wired stores -- the production wiring that dispatches committed
        // events to the reactor's `on_mint`.
        let (mint_store, _mint_projection) = StoreBuilder::<TokenizedEquityMint>::new(pool.clone())
            .with(service.clone())
            .build(mock_services())
            .await
            .unwrap();
        let (redemption_store, _redemption_projection) =
            StoreBuilder::<EquityRedemption>::new(pool.clone())
                .with(service.clone())
                .build(mock_services())
                .await
                .unwrap();
        service
            .set_stores(
                mint_store.clone(),
                redemption_store.clone(),
                Arc::new(test_store::<UsdcRebalance>(pool.clone(), ())),
            )
            .await;

        // The provider reports the request settled: get_request must find a
        // Completed request (with a tx_hash) under the aggregate's
        // tokenization_request_id ("tok-1").
        let mut completed_request = TokenizationRequest::mock_completed();
        completed_request.id = tokenization_request_id("tok-1");
        let tokenizer =
            Arc::new(MockTokenizer::new().with_pending_requests(vec![completed_request]));

        let transfer = CrossVenueEquityTransfer::new(
            mock_services_with(
                &(tokenizer as Arc<dyn Tokenizer>),
                &(Arc::new(MockRaindex::new()) as Arc<dyn Raindex>),
                &(Arc::new(MockWrapper::new()) as Arc<dyn Wrapper>),
            ),
            mint_store,
            redemption_store,
        );

        let outcome = transfer.recover_mint(&id, &pool, &service).await.unwrap();
        assert!(
            matches!(outcome, RecheckOutcome::Recovered),
            "expected Recovered, got {outcome:?}"
        );

        let (hedging_inflight, market_making_available) = {
            let view = inventory.read().await;

            (
                view.equity_inflight(&symbol, Venue::Hedging),
                view.equity_available(&symbol, Venue::MarketMaking),
            )
        };
        assert_eq!(
            hedging_inflight,
            Some(FractionalShares::ZERO),
            "recovered mint left its quantity stuck in Hedging in-flight"
        );
        assert_eq!(
            market_making_available,
            Some(FractionalShares::new(float!(10))),
            "recovered quantity should land in MarketMaking available"
        );
    }

    /// Recovery must not double-count an in-flight that is ALREADY established.
    ///
    /// The realistic stuck state -- what the simulate-failures harness and the
    /// CLI `transfer fail` ops tool both produce -- is: `MintAccepted` ran
    /// `start` so the quantity sits in Hedging in-flight, but the failure was
    /// recorded out-of-process so the reactor never ran `cancel`. Recovery then
    /// runs against an in-flight already at the mint quantity; re-establishing
    /// it with a `Start` double-counts (qty -> 2*qty), and the completion
    /// removes only one copy, leaving the quantity stuck in-flight. Recovery
    /// must reconcile it to zero idempotently.
    #[tokio::test]
    async fn recover_mint_does_not_double_count_existing_inflight() {
        let (pool, apalis_pool) = crate::test_utils::setup_test_pools().await;
        let symbol = Symbol::new("AAPL").unwrap();
        let id = issuer_request_id("mint-double-count");

        insert_mint_event(
            &pool,
            &id,
            1,
            "TokenizedEquityMintEvent::MintRequested",
            r#"{"MintRequested":{"symbol":"AAPL","quantity":"10","wallet":"0x0000000000000000000000000000000000000001","requested_at":"2026-01-01T00:00:00Z"}}"#,
        )
        .await;
        insert_mint_event(
            &pool,
            &id,
            2,
            "TokenizedEquityMintEvent::MintAccepted",
            &mint_accepted_payload(&id),
        )
        .await;
        insert_mint_event(
            &pool,
            &id,
            3,
            "TokenizedEquityMintEvent::MintAcceptanceFailed",
            r#"{"MintAcceptanceFailed":{"reason":"simulate: timeout","failed_at":"2026-01-01T00:00:02Z"}}"#,
        )
        .await;

        let (event_sender, _event_receiver) = broadcast::channel::<Statement>(16);

        // MintAccepted's `start` already moved the quantity into Hedging
        // in-flight (available 100 -> 90, in-flight 10); the out-of-process
        // failure never ran `cancel`, so the in-flight is still established.
        let seeded = InventoryView::default()
            .with_equity(
                symbol.clone(),
                FractionalShares::ZERO,
                FractionalShares::new(float!(90)),
            )
            .update_equity(
                &symbol,
                Inventory::set_inflight(Venue::Hedging, FractionalShares::new(float!(10))),
                Utc::now(),
            )
            .unwrap();
        let inventory = Arc::new(BroadcastingInventory::new(seeded, event_sender));

        let service = Arc::new(RebalancingService::new(
            RebalancingServiceConfig {
                poll_freshness: PollFreshness::always_fresh(),
                inventory_staleness_bound: Duration::from_secs(300),
                cash_reserved: None,
                hedge_floor: st0x_execution::HedgeFloor::default(),
                allocation: AllocationCtx::base_test(),
                usdc: UsdcCorridors::base_cctp_disabled(),
                transfer_timeout: Duration::from_secs(1800),
                recovery_hold_alert_after: Duration::from_secs(60 * 60),
                chains: BTreeMap::from([(
                    Chain::Base,
                    ChainRebalancingConfig::for_test(ChainAssets::default()),
                )]),
            },
            Arc::new(test_store::<VaultRegistry>(pool.clone(), ())),
            BTreeMap::from([(
                Chain::Base,
                VaultRegistryId {
                    chain: st0x_evm::Chain::Base,
                    orderbook: address!("0x0000000000000000000000000000000000000001"),
                    owner: address!("0x0000000000000000000000000000000000000002"),
                },
            )]),
            inventory.clone(),
            BTreeMap::from([(
                Chain::Base,
                Arc::new(MockWrapper::new()) as Arc<dyn Wrapper>,
            )]),
            RebalancingSchedulers::new(&apalis_pool),
            Arc::new(crate::alerts::LogNotifier),
        ));

        let (mint_store, _mint_projection) = StoreBuilder::<TokenizedEquityMint>::new(pool.clone())
            .with(service.clone())
            .build(mock_services())
            .await
            .unwrap();
        let (redemption_store, _redemption_projection) =
            StoreBuilder::<EquityRedemption>::new(pool.clone())
                .with(service.clone())
                .build(mock_services())
                .await
                .unwrap();
        service
            .set_stores(
                mint_store.clone(),
                redemption_store.clone(),
                Arc::new(test_store::<UsdcRebalance>(pool.clone(), ())),
            )
            .await;

        let mut completed_request = TokenizationRequest::mock_completed();
        completed_request.id = tokenization_request_id("tok-1");
        let tokenizer =
            Arc::new(MockTokenizer::new().with_pending_requests(vec![completed_request]));

        let transfer = CrossVenueEquityTransfer::new(
            mock_services_with(
                &(tokenizer as Arc<dyn Tokenizer>),
                &(Arc::new(MockRaindex::new()) as Arc<dyn Raindex>),
                &(Arc::new(MockWrapper::new()) as Arc<dyn Wrapper>),
            ),
            mint_store,
            redemption_store,
        );

        let outcome = transfer.recover_mint(&id, &pool, &service).await.unwrap();
        assert!(
            matches!(outcome, RecheckOutcome::Recovered),
            "expected Recovered, got {outcome:?}"
        );

        let (hedging_inflight, market_making_available) = {
            let view = inventory.read().await;

            (
                view.equity_inflight(&symbol, Venue::Hedging),
                view.equity_available(&symbol, Venue::MarketMaking),
            )
        };
        assert_eq!(
            hedging_inflight,
            Some(FractionalShares::ZERO),
            "recovery double-counted the already-established in-flight, leaving it stuck"
        );
        assert_eq!(
            market_making_available,
            Some(FractionalShares::new(float!(10))),
            "recovered quantity should land in MarketMaking available"
        );
    }

    /// A reconciled mint is terminal: `resume_mint` must be a clean no-op for
    /// an apalis retry, leaving the aggregate in `Reconciled`.
    #[tokio::test]
    async fn resume_mint_is_noop_on_reconciled_mint() {
        let transfer = create_equity_transfer(
            Arc::new(MockTokenizer::new()),
            Arc::new(MockRaindex::new()),
            Arc::new(MockWrapper::new()),
        )
        .await;

        let id = issuer_request_id("ISS-RECONCILED");
        transfer
            .mint_store
            .send(
                &id,
                TokenizedEquityMintCommand::RequestMint {
                    chain: Chain::Base,
                    issuer_request_id: id.clone(),
                    symbol: Symbol::new("AAPL").unwrap(),
                    quantity: float!(10),
                    wallet: Address::ZERO,
                },
            )
            .await
            .unwrap();
        transfer
            .mint_store
            .send(
                &id,
                TokenizedEquityMintCommand::FailAcceptance {
                    reason: "stranded mid-flight".to_string(),
                },
            )
            .await
            .unwrap();
        transfer
            .mint_store
            .send(
                &id,
                TokenizedEquityMintCommand::Reconcile {
                    reason: "wrapped manually via wrap-equity".to_string(),
                },
            )
            .await
            .unwrap();

        transfer
            .resume_mint(&id)
            .await
            .expect("a reconciled mint must be a clean no-op for the job retry");

        let entity = transfer.mint_store.load(&id).await.unwrap().unwrap();
        assert!(
            matches!(entity, TokenizedEquityMint::Reconciled { .. }),
            "resume must leave a reconciled mint terminal, got: {entity:?}"
        );
    }

    #[tokio::test]
    async fn resume_requested_mint_replays_when_provider_has_no_match() {
        let tokenizer = Arc::new(MockTokenizer::new());
        let transfer = create_equity_transfer(
            tokenizer.clone(),
            Arc::new(MockRaindex::new()),
            Arc::new(MockWrapper::new()),
        )
        .await;
        let id = issuer_request_id("ISS-REQUESTED-NO-MATCH");
        seed_requested_mint(
            &transfer,
            &id,
            Symbol::new("AAPL").unwrap(),
            FractionalShares::new(float!(10)),
        )
        .await;

        transfer.resume_mint(&id).await.unwrap();

        assert_eq!(tokenizer.mint_lookup_call_count(), 1);
        assert_eq!(tokenizer.mint_request_call_count(), 1);
    }

    #[tokio::test]
    async fn resume_requested_mint_adopts_provider_match_without_resubmitting() {
        let id = issuer_request_id("ISS-REQUESTED-MATCH");
        let mut existing_request = TokenizationRequest::mock(TokenizationRequestStatus::Pending);
        existing_request.r#type = Some(TokenizationRequestType::Mint);
        existing_request.underlying_symbol = Symbol::new("AAPL").unwrap();
        existing_request.quantity = FractionalShares::new(float!(10));
        existing_request.client_request_id = Some(ClientRequestId::from(&id));
        let tokenizer =
            Arc::new(MockTokenizer::new().with_pending_requests(vec![existing_request]));
        let transfer = create_equity_transfer(
            tokenizer.clone(),
            Arc::new(MockRaindex::new()),
            Arc::new(MockWrapper::new()),
        )
        .await;
        seed_requested_mint(
            &transfer,
            &id,
            Symbol::new("AAPL").unwrap(),
            FractionalShares::new(float!(10)),
        )
        .await;

        transfer.resume_mint(&id).await.unwrap();

        assert_eq!(tokenizer.mint_lookup_call_count(), 1);
        assert_eq!(tokenizer.mint_request_call_count(), 0);
    }

    /// Services listing AAPL on Base as paused, driven by `tokenizer`, as
    /// after a restart into `paused` with mints already persisted.
    fn paused_aapl_services(tokenizer: &Arc<MockTokenizer>) -> EquityTransferServices {
        let mut services = mock_services();
        let mut equities = equities_listing("AAPL", Address::ZERO);
        equities
            .symbols
            .get_mut(&Symbol::new("AAPL").unwrap())
            .unwrap()
            .rebalancing = RebalancingMode::Paused;
        let base = services.chains.get_mut(&Chain::Base).unwrap();
        base.equities = equities;
        base.tokenizer = tokenizer.clone();
        services
    }

    #[tokio::test]
    async fn requested_mint_on_paused_listing_stays_requested_without_replaying_the_request() {
        let tokenizer = Arc::new(MockTokenizer::new());
        let transfer = create_equity_transfer_with_services(paused_aapl_services(&tokenizer)).await;
        let id = issuer_request_id("ISS-REQUESTED-PAUSED");
        let symbol = Symbol::new("AAPL").unwrap();
        let quantity = FractionalShares::new(float!(10));
        seed_requested_mint(&transfer, &id, symbol.clone(), quantity).await;

        let error = transfer
            .resume_equity_to_market_making(&id, &symbol, Chain::Base, quantity)
            .await
            .unwrap_err();

        assert!(
            matches!(error, MintTransferError::PreReceipt(_)),
            "an inconclusive lookup must surface as a pre-receipt retry, got: {error:?}"
        );
        assert_eq!(tokenizer.mint_lookup_call_count(), 1);
        assert_eq!(tokenizer.mint_request_call_count(), 0);
        let entity = transfer.mint_store.load(&id).await.unwrap().unwrap();
        assert!(
            matches!(entity, TokenizedEquityMint::MintRequested { .. }),
            "an inconclusive lookup on a paused listing must keep the mint requested, got: {entity:?}"
        );
    }

    #[tokio::test]
    async fn requested_mint_on_paused_listing_adopts_a_provider_match() {
        let id = issuer_request_id("ISS-REQUESTED-PAUSED-MATCH");
        let mut existing_request = TokenizationRequest::mock(TokenizationRequestStatus::Pending);
        existing_request.r#type = Some(TokenizationRequestType::Mint);
        existing_request.underlying_symbol = Symbol::new("AAPL").unwrap();
        existing_request.quantity = FractionalShares::new(float!(10));
        existing_request.client_request_id = Some(ClientRequestId::from(&id));
        let tokenizer =
            Arc::new(MockTokenizer::new().with_pending_requests(vec![existing_request]));
        let transfer = create_equity_transfer_with_services(paused_aapl_services(&tokenizer)).await;
        seed_requested_mint(
            &transfer,
            &id,
            Symbol::new("AAPL").unwrap(),
            FractionalShares::new(float!(10)),
        )
        .await;

        transfer.resume_mint(&id).await.unwrap();

        assert_eq!(tokenizer.mint_request_call_count(), 0);
        let entity = transfer.mint_store.load(&id).await.unwrap().unwrap();
        assert!(
            matches!(entity, TokenizedEquityMint::DepositedIntoRaindex { .. }),
            "a mint the provider already holds must keep resuming while paused, got: {entity:?}"
        );
    }

    #[tokio::test]
    async fn accepted_mint_on_paused_listing_keeps_resuming() {
        let tokenizer = Arc::new(MockTokenizer::new());
        let transfer = create_equity_transfer_with_services(paused_aapl_services(&tokenizer)).await;
        let id = issuer_request_id("ISS-ACCEPTED-PAUSED");
        let symbol = Symbol::new("AAPL").unwrap();
        let quantity = FractionalShares::new(float!(10));
        seed_requested_mint(&transfer, &id, symbol.clone(), quantity).await;
        submit_requested_mint(&transfer, &id).await;

        transfer
            .resume_equity_to_market_making(&id, &symbol, Chain::Base, quantity)
            .await
            .unwrap();

        assert_eq!(tokenizer.mint_request_call_count(), 1);
        let entity = transfer.mint_store.load(&id).await.unwrap().unwrap();
        assert!(
            matches!(entity, TokenizedEquityMint::DepositedIntoRaindex { .. }),
            "an accepted mint must keep resuming while paused, got: {entity:?}"
        );
    }

    /// A reconciled redemption is terminal: `resume_redemption` must be a clean
    /// no-op for an apalis retry, leaving the aggregate in `Reconciled`.
    #[tokio::test]
    async fn resume_redemption_is_noop_on_reconciled_redemption() {
        let transfer = create_equity_transfer(
            Arc::new(MockTokenizer::new()),
            Arc::new(MockRaindex::new()),
            Arc::new(MockWrapper::new()),
        )
        .await;

        let id = redemption_aggregate_id("redeem-reconciled");
        let symbol = Symbol::new("TEST").unwrap();
        let token = transfer
            .services
            .for_chain(Chain::Base)
            .unwrap()
            .vault_lookup
            .vault_token_for_symbol(&symbol)
            .await
            .unwrap();
        let amount = FractionalShares::new(float!(50))
            .to_u256_18_decimals()
            .unwrap();

        transfer
            .withdraw_from_raindex(
                &id,
                &symbol,
                Chain::Base,
                FractionalShares::new(float!(50)),
                token,
                amount,
            )
            .await
            .unwrap();
        transfer.unwrap_and_send(&id).await.unwrap();
        transfer
            .redemption_store
            .send(
                &id,
                EquityRedemptionCommand::FailDetection {
                    failure: DetectionFailure::Timeout,
                },
            )
            .await
            .unwrap();
        transfer
            .redemption_store
            .send(
                &id,
                EquityRedemptionCommand::Reconcile {
                    reason: "deposited manually via vault-deposit".to_string(),
                    proven_withdrawal: None,
                },
            )
            .await
            .unwrap();

        transfer
            .resume_redemption(&id)
            .await
            .expect("a reconciled redemption must be a clean no-op for the job retry");

        let entity = transfer.redemption_store.load(&id).await.unwrap().unwrap();
        assert!(
            matches!(entity, EquityRedemption::Reconciled { .. }),
            "resume must leave a reconciled redemption terminal, got: {entity:?}"
        );
    }

    /// A reconciled mint is already settled, so the operator recheck path
    /// reports `AlreadyCompleted` without attempting provider recovery.
    #[tokio::test]
    async fn recover_mint_reports_already_completed_on_reconciled_mint() {
        let (transfer, service, pool) = transfer_with_rebalancing_service().await;

        let id = issuer_request_id("mint-recover-reconciled");
        transfer
            .mint_store
            .send(
                &id,
                TokenizedEquityMintCommand::RequestMint {
                    chain: Chain::Base,
                    issuer_request_id: id.clone(),
                    symbol: Symbol::new("AAPL").unwrap(),
                    quantity: float!(10),
                    wallet: Address::ZERO,
                },
            )
            .await
            .unwrap();
        transfer
            .mint_store
            .send(
                &id,
                TokenizedEquityMintCommand::FailAcceptance {
                    reason: "stranded mid-flight".to_string(),
                },
            )
            .await
            .unwrap();
        transfer
            .mint_store
            .send(
                &id,
                TokenizedEquityMintCommand::Reconcile {
                    reason: "wrapped manually via wrap-equity".to_string(),
                },
            )
            .await
            .unwrap();

        let outcome = transfer.recover_mint(&id, &pool, &service).await.unwrap();
        assert!(
            matches!(outcome, RecheckOutcome::AlreadyCompleted),
            "a reconciled mint must recheck as AlreadyCompleted, got {outcome:?}"
        );
    }

    /// A reconciled redemption is already settled, so the operator recheck path
    /// reports `AlreadyCompleted` without attempting provider recovery.
    #[tokio::test]
    async fn recover_redemption_reports_already_completed_on_reconciled_redemption() {
        let (transfer, service, _pool) = transfer_with_rebalancing_service().await;

        let id = redemption_aggregate_id("redeem-recover-reconciled");
        let symbol = Symbol::new("TEST").unwrap();
        let token = transfer
            .services
            .for_chain(Chain::Base)
            .unwrap()
            .vault_lookup
            .vault_token_for_symbol(&symbol)
            .await
            .unwrap();
        let amount = FractionalShares::new(float!(50))
            .to_u256_18_decimals()
            .unwrap();

        transfer
            .withdraw_from_raindex(
                &id,
                &symbol,
                Chain::Base,
                FractionalShares::new(float!(50)),
                token,
                amount,
            )
            .await
            .unwrap();
        transfer.unwrap_and_send(&id).await.unwrap();
        transfer
            .redemption_store
            .send(
                &id,
                EquityRedemptionCommand::FailDetection {
                    failure: DetectionFailure::Timeout,
                },
            )
            .await
            .unwrap();
        transfer
            .redemption_store
            .send(
                &id,
                EquityRedemptionCommand::Reconcile {
                    reason: "deposited manually via vault-deposit".to_string(),
                    proven_withdrawal: None,
                },
            )
            .await
            .unwrap();

        let outcome = transfer.recover_redemption(&id, &service).await.unwrap();
        assert!(
            matches!(outcome, RecheckOutcome::AlreadyCompleted),
            "a reconciled redemption must recheck as AlreadyCompleted, got {outcome:?}"
        );
    }

    #[tokio::test]
    async fn provider_completion_recovery_waits_for_committed_failure_reactor() {
        let (pool, apalis_pool) = crate::test_utils::setup_test_pools().await;
        let symbol = Symbol::new("TEST").unwrap();
        let id = redemption_aggregate_id("recover-after-terminal-reactor");
        let request_id = tokenization_request_id("TOK-recover-after-terminal-reactor");
        let mut completed_request = TokenizationRequest::mock_completed();
        completed_request.id = request_id.clone();
        let tokenizer =
            Arc::new(MockTokenizer::new().with_pending_requests(vec![completed_request]));
        let services = mock_services_with(
            &(tokenizer.clone() as Arc<dyn Tokenizer>),
            &(Arc::new(MockRaindex::new()) as Arc<dyn Raindex>),
            &(Arc::new(MockWrapper::new()) as Arc<dyn Wrapper>),
        );
        let (event_sender, _event_receiver) = broadcast::channel::<Statement>(16);
        let inventory = Arc::new(BroadcastingInventory::new(
            InventoryView::default().with_equity(
                symbol.clone(),
                FractionalShares::new(float!(100)),
                FractionalShares::ZERO,
            ),
            event_sender,
        ));
        let service = Arc::new(RebalancingService::new(
            RebalancingServiceConfig {
                poll_freshness: PollFreshness::always_fresh(),
                inventory_staleness_bound: Duration::from_secs(300),
                cash_reserved: None,
                hedge_floor: st0x_execution::HedgeFloor::default(),
                allocation: AllocationCtx::base_test(),
                usdc: UsdcCorridors::base_cctp_disabled(),
                transfer_timeout: Duration::from_secs(1800),
                recovery_hold_alert_after: Duration::from_secs(60 * 60),
                chains: BTreeMap::from([(
                    Chain::Base,
                    ChainRebalancingConfig::for_test(ChainAssets::default()),
                )]),
            },
            Arc::new(test_store::<VaultRegistry>(pool.clone(), ())),
            BTreeMap::from([(
                Chain::Base,
                VaultRegistryId {
                    chain: st0x_evm::Chain::Base,
                    orderbook: address!("0x0000000000000000000000000000000000000001"),
                    owner: address!("0x0000000000000000000000000000000000000002"),
                },
            )]),
            inventory.clone(),
            BTreeMap::from([(
                Chain::Base,
                Arc::new(MockWrapper::new()) as Arc<dyn Wrapper>,
            )]),
            RebalancingSchedulers::new(&apalis_pool),
            Arc::new(crate::alerts::LogNotifier),
        ));
        let blocker = Arc::new(BlockingRedemptionTerminalReactor {
            entered: Notify::new(),
            release: Notify::new(),
        });
        let (mint_store, _mint_projection) = StoreBuilder::<TokenizedEquityMint>::new(pool.clone())
            .with(service.clone())
            .build(services.clone())
            .await
            .unwrap();
        let (redemption_store, _redemption_projection) =
            StoreBuilder::<EquityRedemption>::new(pool.clone())
                .with(blocker.clone())
                .with(service.clone())
                .build(services.clone())
                .await
                .unwrap();
        service
            .set_stores(
                mint_store.clone(),
                redemption_store.clone(),
                Arc::new(test_store::<UsdcRebalance>(pool, ())),
            )
            .await;
        let transfer =
            CrossVenueEquityTransfer::new(services, mint_store, redemption_store.clone());

        advance_redemption_to_tokens_sent(&transfer, &id, &symbol).await;
        redemption_store
            .send(
                &id,
                EquityRedemptionCommand::Detect {
                    tokenization_request_id: request_id.clone(),
                },
            )
            .await
            .unwrap();

        let rejecting_store = redemption_store.clone();
        let rejecting_id = id.clone();
        let failure = tokio::spawn(async move {
            rejecting_store
                .send(
                    &rejecting_id,
                    EquityRedemptionCommand::RejectRedemption {
                        reason: "provider rejected redemption".to_string(),
                    },
                )
                .await
        });
        tokio::time::timeout(Duration::from_secs(5), blocker.entered.notified())
            .await
            .expect("rejection must reach the blocking reactor");

        assert!(matches!(
            redemption_store.load(&id).await.unwrap().unwrap(),
            EquityRedemption::Failed { .. }
        ));
        let provider_calls_before_recovery = tokenizer.call_count();
        let mut recovery = pin!(transfer.recover_redemption(&id, &service));
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                assert!(poll!(&mut recovery).is_pending());
                if tokenizer.call_count() > provider_calls_before_recovery {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("recovery must reach the completed provider request");

        blocker.release.notify_one();
        let (failure_result, recovery_result) =
            tokio::time::timeout(Duration::from_secs(5), async {
                tokio::join!(failure, recovery)
            })
            .await
            .expect("failure cleanup and provider recovery must finish without deadlock");
        failure_result.unwrap().unwrap();
        assert_eq!(recovery_result.unwrap(), RecheckOutcome::Recovered);

        let inventory = inventory.read().await;
        assert_eq!(
            inventory.equity_available(&symbol, Venue::MarketMaking),
            Some(FractionalShares::new(float!(50)))
        );
        assert_eq!(
            inventory.equity_inflight(&symbol, Venue::MarketMaking),
            Some(FractionalShares::ZERO)
        );
        assert_eq!(
            inventory.equity_available(&symbol, Venue::Hedging),
            Some(FractionalShares::new(float!(50)))
        );
        assert_eq!(inventory.active_redemption(&symbol), None);
        drop(inventory);
        assert!(
            !service
                .equity_in_progress
                .read()
                .unwrap()
                .contains_key(&symbol)
        );
    }

    /// Builds a transfer wired to a real `RebalancingService` sharing the same
    /// command stores, so a seeded aggregate is visible to the `recover_*`
    /// recheck entry points.
    pub(super) async fn transfer_with_rebalancing_service() -> (
        CrossVenueEquityTransfer,
        Arc<RebalancingService>,
        SqlitePool,
    ) {
        let (pool, apalis_pool) = crate::test_utils::setup_test_pools().await;
        let (event_sender, _event_receiver) = broadcast::channel::<Statement>(16);
        let inventory = Arc::new(BroadcastingInventory::new(
            InventoryView::default(),
            event_sender,
        ));

        let service = Arc::new(RebalancingService::new(
            RebalancingServiceConfig {
                poll_freshness: PollFreshness::always_fresh(),
                inventory_staleness_bound: Duration::from_secs(300),
                cash_reserved: None,
                hedge_floor: st0x_execution::HedgeFloor::default(),
                allocation: AllocationCtx::base_test(),
                usdc: UsdcCorridors::base_cctp_disabled(),
                transfer_timeout: Duration::from_secs(1800),
                recovery_hold_alert_after: Duration::from_secs(60 * 60),
                chains: BTreeMap::from([(
                    Chain::Base,
                    ChainRebalancingConfig::for_test(ChainAssets::default()),
                )]),
            },
            Arc::new(test_store::<VaultRegistry>(pool.clone(), ())),
            BTreeMap::from([(
                Chain::Base,
                VaultRegistryId {
                    chain: st0x_evm::Chain::Base,
                    orderbook: address!("0x0000000000000000000000000000000000000001"),
                    owner: address!("0x0000000000000000000000000000000000000002"),
                },
            )]),
            inventory,
            BTreeMap::from([(
                Chain::Base,
                Arc::new(MockWrapper::new()) as Arc<dyn Wrapper>,
            )]),
            RebalancingSchedulers::new(&apalis_pool),
            Arc::new(crate::alerts::LogNotifier),
        ));

        let services = mock_services();
        let (mint_store, _mint_projection) = StoreBuilder::<TokenizedEquityMint>::new(pool.clone())
            .with(service.clone())
            .build(services.clone())
            .await
            .unwrap();
        let (redemption_store, _redemption_projection) =
            StoreBuilder::<EquityRedemption>::new(pool.clone())
                .with(service.clone())
                .build(services.clone())
                .await
                .unwrap();
        service
            .set_stores(
                mint_store.clone(),
                redemption_store.clone(),
                Arc::new(test_store::<UsdcRebalance>(pool.clone(), ())),
            )
            .await;

        let transfer = CrossVenueEquityTransfer::new(services, mint_store, redemption_store)
            .with_rebalancing_service(&service);

        (transfer, service, pool)
    }

    async fn create_equity_transfer(
        tokenizer: Arc<dyn Tokenizer>,
        raindex: Arc<dyn Raindex>,
        wrapper: Arc<dyn Wrapper>,
    ) -> CrossVenueEquityTransfer {
        let (transfer, _pool) = create_equity_transfer_with_pool(tokenizer, raindex, wrapper).await;
        transfer
    }

    /// [`mock_services`] with every entry's gas admission wired, so a test can
    /// exercise the refusal a low native balance produces.
    fn mock_services_with_gas_readiness(readiness: &Arc<GasReadiness>) -> EquityTransferServices {
        let mut services = mock_services();
        for chain_services in services.chains.values_mut() {
            chain_services.gas_readiness = ConfiguredGasReadiness::Wired(readiness.clone());
        }

        services
    }

    /// [`mock_services`] with the caller's mocks in every entry, so a test
    /// drives its own tokenizer, orderbook and wrapper on whichever chain its
    /// record names.
    fn mock_services_with(
        tokenizer: &Arc<dyn Tokenizer>,
        raindex: &Arc<dyn Raindex>,
        wrapper: &Arc<dyn Wrapper>,
    ) -> EquityTransferServices {
        let mut services = mock_services();
        for chain_services in services.chains.values_mut() {
            chain_services.tokenizer = tokenizer.clone();
            chain_services.raindex = raindex.clone();
            chain_services.wrapper = wrapper.clone();
        }

        services
    }

    /// Like [`create_equity_transfer`] but on caller-supplied services, for
    /// tests that need a wired gas check or a second chain's entry.
    async fn create_equity_transfer_with_services(
        services: EquityTransferServices,
    ) -> CrossVenueEquityTransfer {
        let pool = SqlitePool::connect(":memory:").await.unwrap();
        sqlx::migrate!().run(&pool).await.unwrap();

        let mint_store = Arc::new(test_store(pool.clone(), services.clone()));
        let redemption_store = Arc::new(test_store(pool, services.clone()));

        CrossVenueEquityTransfer::new(services, mint_store, redemption_store)
    }

    /// Like [`create_equity_transfer`] but also returns the backing pool, so a
    /// test can seed legacy events directly (e.g. a `TokensWrapped` event
    /// persisted before the `wrap_block` field existed).
    async fn create_equity_transfer_with_pool(
        tokenizer: Arc<dyn Tokenizer>,
        raindex: Arc<dyn Raindex>,
        wrapper: Arc<dyn Wrapper>,
    ) -> (CrossVenueEquityTransfer, SqlitePool) {
        let pool = SqlitePool::connect(":memory:").await.unwrap();
        sqlx::migrate!().run(&pool).await.unwrap();
        let services = mock_services_with(&tokenizer, &raindex, &wrapper);

        let mint_store = Arc::new(test_store(pool.clone(), services.clone()));
        let redemption_store = Arc::new(test_store(pool.clone(), services.clone()));

        let transfer = CrossVenueEquityTransfer::new(services, mint_store, redemption_store);

        (transfer, pool)
    }

    fn withdrawal_amount() -> U256 {
        U256::from(50_000_000_000_000_000_000_u128)
    }

    async fn seed_withdrawal_intent(
        transfer: &CrossVenueEquityTransfer,
        id: &RedemptionAggregateId,
        from_block: u64,
    ) {
        transfer
            .redemption_store
            .send(
                id,
                EquityRedemptionCommand::Redeem {
                    symbol: Symbol::new("TEST").unwrap(),
                    chain: Chain::Base,
                    quantity: float!(50),
                    token: Address::ZERO,
                    vault_id: RaindexVaultId(B256::ZERO),
                    amount: withdrawal_amount(),
                    from_block,
                    prepared: crate::equity_redemption::prepared_withdrawal_for_test(),
                },
            )
            .await
            .unwrap();
    }

    fn restarted_transfer(
        transfer: &CrossVenueEquityTransfer,
        pool: SqlitePool,
    ) -> CrossVenueEquityTransfer {
        let services = transfer.services.clone();
        let mint_store = Arc::new(test_store(pool.clone(), services.clone()));
        let redemption_store = Arc::new(test_store(pool, services.clone()));
        CrossVenueEquityTransfer::new(services, mint_store, redemption_store)
    }

    #[tokio::test]
    async fn resume_broadcasts_prepared_withdrawal_persisted_before_first_attempt() {
        let raindex =
            Arc::new(MockRaindex::new().with_confirm_behavior(ConfirmTxBehavior::Retryable));
        let (transfer, pool) = create_equity_transfer_with_pool(
            Arc::new(MockTokenizer::new()),
            raindex.clone(),
            Arc::new(MockWrapper::new()),
        )
        .await;
        let id = redemption_aggregate_id("withdraw-prepared-before-broadcast");
        seed_withdrawal_intent(&transfer, &id, 101).await;

        assert_eq!(raindex.withdraw_submissions(), 0);
        let restarted = restarted_transfer(&transfer, pool);
        let error = restarted.resume_redemption(&id).await.unwrap_err();

        assert!(
            error.is_reconciliation_pending(),
            "receipt reconciliation should remain retryable, got: {error:?}"
        );
        assert_eq!(
            raindex.withdraw_submissions(),
            1,
            "restart must broadcast the exact prepared transaction"
        );
        assert!(matches!(
            restarted.redemption_store.load(&id).await.unwrap(),
            Some(EquityRedemption::VaultWithdrawSubmitted {
                tx_hash: TxHash::ZERO,
                ..
            })
        ));
        assert_eq!(
            raindex.restored_prepared_withdrawals(),
            0,
            "the submitting-state path records ownership while broadcasting \
             and must not redundantly restore before its first confirmation"
        );
    }

    /// A redemption whose signed withdrawal was replaced by a mined copy (a
    /// wallet "speed up") finishes once the copy is adopted. Resume restores
    /// the adopted hash with no signed bytes, which records it at the
    /// withdrawal's nonce so confirming it releases that nonce; it never
    /// rebroadcasts the dead withdrawal, and it runs the redemption to the end.
    #[tokio::test]
    async fn an_adopted_withdrawal_replacement_is_confirmed_and_the_redemption_completes() {
        let raindex =
            Arc::new(MockRaindex::new().with_withdraw_transfer(Address::ZERO, withdrawal_amount()));
        let tokenizer: Arc<dyn Tokenizer> = Arc::new(
            MockTokenizer::new()
                .with_detection_outcome(MockDetectionOutcome::Detected)
                .with_completion_outcome(MockCompletionOutcome::Completed),
        );
        let (transfer, _pool) = create_equity_transfer_with_pool(
            tokenizer,
            raindex.clone(),
            Arc::new(MockWrapper::new()),
        )
        .await;
        let id = redemption_aggregate_id("withdraw-replaced-by-a-mined-copy");
        seed_withdrawal_intent(&transfer, &id, 0).await;
        let speed_up = TxHash::repeat_byte(0x5E);
        transfer
            .redemption_store
            .send(
                &id,
                EquityRedemptionCommand::AdoptWithdrawalReplacement {
                    replacement_tx: speed_up,
                    replaced_withdrawal: crate::equity_redemption::prepared_withdrawal_for_test()
                        .tx_hash(),
                    reason: "wallet sped up the withdrawal".to_string(),
                },
            )
            .await
            .unwrap();

        transfer.resume_redemption(&id).await.unwrap();

        let entity = transfer.redemption_store.load(&id).await.unwrap().unwrap();
        assert!(
            matches!(entity, EquityRedemption::Completed { .. }),
            "an adopted replacement must let the redemption complete, got {entity:?}"
        );
        assert_eq!(
            raindex.restore_submitted_withdrawal_calls(),
            vec![(speed_up, false)],
            "resume must restore the adopted hash, not the signed withdrawal, so \
             confirming it releases the withdrawal's nonce"
        );
        assert_eq!(
            raindex.withdraw_submissions(),
            0,
            "the replaced withdrawal must never be rebroadcast"
        );
    }

    #[tokio::test]
    async fn dropped_submitted_withdrawal_rebroadcasts_persisted_bytes_on_every_resume() {
        let raindex = Arc::new(MockRaindex::new().with_confirm_behavior(ConfirmTxBehavior::Fail));
        let (transfer, pool) = create_equity_transfer_with_pool(
            Arc::new(MockTokenizer::new()),
            raindex.clone(),
            Arc::new(MockWrapper::new()),
        )
        .await;
        let id = redemption_aggregate_id("withdraw-submitted-dropped");
        seed_withdrawal_intent(&transfer, &id, 102).await;
        let prepared_hash = match transfer.redemption_store.load(&id).await.unwrap() {
            Some(EquityRedemption::VaultWithdrawSubmitting { prepared, .. }) => prepared.tx_hash(),
            state => panic!("expected durable prepared withdrawal, got {state:?}"),
        };
        transfer
            .record_vault_withdrawal_submission(&id, prepared_hash)
            .await
            .unwrap();

        let restarted = restarted_transfer(&transfer, pool);
        for expected_submissions in 1..=2 {
            let error = restarted.resume_redemption(&id).await.unwrap_err();
            assert!(
                error.is_reconciliation_pending(),
                "a dropped durable withdrawal must remain retryable, got {error:?}"
            );
            assert_eq!(
                raindex.withdraw_submissions(),
                expected_submissions,
                "each resume must rebroadcast the exact persisted transaction \
                 before checking its receipt again"
            );
            assert!(matches!(
                restarted.redemption_store.load(&id).await.unwrap(),
                Some(EquityRedemption::VaultWithdrawSubmitted {
                    tx_hash,
                    prepared: Some(prepared),
                    ..
                }) if tx_hash == prepared_hash && prepared.tx_hash() == prepared_hash
            ));
        }
        assert_eq!(
            raindex.restored_prepared_withdrawals(),
            2,
            "each resume must restore durable nonce ownership before rebroadcast"
        );
    }

    #[tokio::test]
    async fn lost_broadcast_response_rebroadcasts_persisted_transaction_after_restart() {
        let raindex = Arc::new(MockRaindex::new().accepting_withdraw_then_losing_response());
        let (transfer, pool) = create_equity_transfer_with_pool(
            Arc::new(MockTokenizer::new()),
            raindex.clone(),
            Arc::new(MockWrapper::new()),
        )
        .await;
        let id = redemption_aggregate_id("withdraw-response-lost");

        let first_error = transfer
            .withdraw_from_raindex(
                &id,
                &Symbol::new("TEST").unwrap(),
                Chain::Base,
                FractionalShares::new(float!(50)),
                Address::ZERO,
                withdrawal_amount(),
            )
            .await
            .unwrap_err();
        assert!(matches!(first_error, RedemptionError::Raindex(_)));
        let prepared_hash = match transfer.redemption_store.load(&id).await.unwrap() {
            Some(EquityRedemption::VaultWithdrawSubmitting { prepared, .. }) => prepared.tx_hash(),
            state => panic!("prepared transaction must remain durable, got {state:?}"),
        };

        let restarted = restarted_transfer(&transfer, pool);
        let second_error = restarted.resume_redemption(&id).await.unwrap_err();

        assert!(matches!(second_error, RedemptionError::Raindex(_)));
        assert_eq!(raindex.withdraw_submissions(), 2);
        assert!(matches!(
            restarted.redemption_store.load(&id).await.unwrap(),
            Some(EquityRedemption::VaultWithdrawSubmitting { prepared, .. })
                if prepared.tx_hash() == prepared_hash
        ));
    }

    #[tokio::test]
    async fn legacy_pending_withdrawal_requires_operator_reconciliation() {
        let raindex = Arc::new(MockRaindex::new());
        let (transfer, pool) = create_equity_transfer_with_pool(
            Arc::new(MockTokenizer::new()),
            raindex.clone(),
            Arc::new(MockWrapper::new()),
        )
        .await;
        let id = redemption_aggregate_id("legacy-withdraw-pending");
        let payload = r#"{"VaultWithdrawPending":{"symbol":"TEST","chain":"base","quantity":"50","token":"0x0000000000000000000000000000000000000000","wrapped_amount":"50000000000000000000","pending_at":"2026-01-01T00:00:00Z"}}"#;
        sqlx::query(
            "INSERT INTO events \
             (aggregate_type, aggregate_id, sequence, event_type, event_version, payload, metadata) \
             VALUES ('EquityRedemption', ?1, 1, \
             'EquityRedemptionEvent::VaultWithdrawPending', '1', ?2, '{}')",
        )
        .bind(id.to_string())
        .bind(payload)
        .execute(&pool)
        .await
        .unwrap();
        let snapshot_payload = format!(r#"{{"Live":{payload}}}"#);
        sqlx::query(
            "INSERT INTO snapshots \
             (aggregate_type, aggregate_id, last_sequence, payload, timestamp, snapshot_version) \
             VALUES ('EquityRedemption', ?1, 1, ?2, '2026-01-01T00:00:00Z', 7)",
        )
        .bind(id.to_string())
        .bind(snapshot_payload)
        .execute(&pool)
        .await
        .unwrap();

        let restarted = restarted_transfer(&transfer, pool);

        let error = restarted.resume_redemption(&id).await.unwrap_err();

        assert!(
            matches!(
                error,
                RedemptionError::LegacyVaultWithdrawPending {
                    ref aggregate_id
                } if aggregate_id == &id
            ),
            "legacy pending must produce the typed conservative refusal, got: {error:?}"
        );
        assert_eq!(raindex.withdraw_submissions(), 0);
    }

    async fn advance_redemption_to_tokens_sent(
        transfer: &CrossVenueEquityTransfer,
        id: &RedemptionAggregateId,
        symbol: &Symbol,
    ) -> TxHash {
        let token = transfer
            .services
            .for_chain(Chain::Base)
            .unwrap()
            .vault_lookup
            .vault_token_for_symbol(symbol)
            .await
            .unwrap();
        let quantity = FractionalShares::new(float!(50));
        let amount = quantity.to_u256_18_decimals().unwrap();

        transfer
            .withdraw_from_raindex(id, symbol, Chain::Base, quantity, token, amount)
            .await
            .unwrap();
        transfer.unwrap_and_send(id).await.unwrap()
    }

    #[tokio::test]
    async fn mint_transfer_sends_mint_and_deposit_commands() {
        let transfer = create_equity_transfer(
            Arc::new(MockTokenizer::new()),
            Arc::new(MockRaindex::new()),
            Arc::new(MockWrapper::new()),
        )
        .await;

        transfer
            .resume_equity_to_market_making(
                &issuer_request_id("ISS-TEST"),
                &Symbol::new("AAPL").unwrap(),
                Chain::Base,
                FractionalShares::new(float!(100.0)),
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn definitive_mint_rejection_completes_without_later_steps() {
        let tokenizer = Arc::new(
            MockTokenizer::new().with_mint_request_outcome(MockMintRequestOutcome::DefinitiveError),
        );
        let transfer = create_equity_transfer(
            tokenizer.clone(),
            Arc::new(MockRaindex::new()),
            Arc::new(MockWrapper::new()),
        )
        .await;
        let id = issuer_request_id("ISS-DEFINITIVE-REJECTION");

        transfer
            .resume_equity_to_market_making(
                &id,
                &Symbol::new("AAPL").unwrap(),
                Chain::Base,
                FractionalShares::new(float!(100.0)),
            )
            .await
            .unwrap();

        assert_eq!(tokenizer.mint_request_call_count(), 1);
        let entity = transfer.mint_store.load(&id).await.unwrap().unwrap();
        assert!(matches!(entity, TokenizedEquityMint::Failed { .. }));
    }

    #[tokio::test]
    async fn fresh_mint_refuses_low_base_gas_before_creating_aggregate() {
        let transfer = create_equity_transfer_with_services(mock_services_with_gas_readiness(
            &crate::native_gas::GasReadiness::for_test(
                U256::ZERO,
                U256::from(1_u64),
                U256::MAX,
                U256::from(1_u64),
            ),
        ))
        .await;
        let id = issuer_request_id("ISS-LOW-GAS");

        let error = transfer
            .resume_equity_to_market_making(
                &id,
                &Symbol::new("AAPL").unwrap(),
                Chain::Base,
                FractionalShares::new(float!(100.0)),
            )
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            MintTransferError::PreReceipt(MintError::GasReadiness(_))
        ));
        assert!(
            transfer.mint_store.load(&id).await.unwrap().is_none(),
            "gas refusal must happen before RequestMint creates aggregate state"
        );
    }

    /// Acceptance criterion: a full mint (RequestMint through
    /// DepositToVault) enqueues exactly one `Wrap` and one `VaultDeposit`
    /// bot-gas job, each carrying the symbol and Base chain.
    #[tokio::test]
    async fn mint_transfer_enqueues_wrap_and_vault_deposit_bot_gas_jobs() {
        let (pool, apalis_pool) = crate::test_utils::setup_test_pools().await;
        let services = mock_services();
        let mint_store = Arc::new(test_store(pool.clone(), services.clone()));
        let redemption_store = Arc::new(test_store(pool, services.clone()));
        let queue = RecordBotGasReceiptCostJobQueue::new(&apalis_pool);

        let transfer = CrossVenueEquityTransfer::new(
            EquityTransferServices {
                bot_gas_enqueuer: BotGasReceiptCostEnqueuer::Enabled(queue),
                ..services.clone()
            },
            mint_store,
            redemption_store,
        );

        let symbol = Symbol::new("AAPL").unwrap();
        transfer
            .resume_equity_to_market_making(
                &issuer_request_id("ISS-BOT-GAS"),
                &symbol,
                Chain::Base,
                FractionalShares::new(float!(100.0)),
            )
            .await
            .unwrap();

        let jobs = pending_bot_gas_jobs(&apalis_pool).await;
        assert_eq!(
            jobs.len(),
            2,
            "expected exactly one Wrap and one VaultDeposit job"
        );
        let wrap_jobs: Vec<_> = jobs
            .iter()
            .filter(|job| job.category == BotGasOperationCategory::Wrap)
            .collect();
        let deposit_jobs: Vec<_> = jobs
            .iter()
            .filter(|job| job.category == BotGasOperationCategory::VaultDeposit)
            .collect();
        assert_eq!(wrap_jobs.len(), 1);
        assert_eq!(deposit_jobs.len(), 1);
        for job in [wrap_jobs[0], deposit_jobs[0]] {
            assert_eq!(job.chain, Chain::Base);
            assert_eq!(job.symbol, Some(symbol.clone()));
        }
    }

    /// Acceptance criterion: an enqueue failure after a confirmed
    /// wrap propagates as a hard error rather than being swallowed. A fresh
    /// mint reaches `wrap_received_mint`'s enqueue call (after `confirm_wrap`)
    /// strictly before `deposit_wrapped_mint`'s, so this test exercises the
    /// WRAP-side enqueue only; see `deposit_enqueue_failure_propagates` below
    /// for the deposit side.
    #[tokio::test]
    async fn wrap_enqueue_failure_propagates() {
        let (pool, apalis_pool) = crate::test_utils::setup_test_pools().await;
        let services = mock_services();
        let mint_store = Arc::new(test_store(pool.clone(), services.clone()));
        let redemption_store = Arc::new(test_store(pool, services.clone()));
        let queue = RecordBotGasReceiptCostJobQueue::new(&apalis_pool);
        apalis_pool.close().await;

        let transfer = CrossVenueEquityTransfer::new(
            EquityTransferServices {
                bot_gas_enqueuer: BotGasReceiptCostEnqueuer::Enabled(queue),
                ..services.clone()
            },
            mint_store,
            redemption_store,
        );

        let error = transfer
            .resume_equity_to_market_making(
                &issuer_request_id("ISS-BOT-GAS-FAIL"),
                &Symbol::new("AAPL").unwrap(),
                Chain::Base,
                FractionalShares::new(float!(100.0)),
            )
            .await
            .unwrap_err();

        assert!(
            matches!(error, MintTransferError::PostReceipt(inner) if matches!(inner, MintError::BotGasEnqueue(_)))
        );
    }

    /// Isolates the DEPOSIT-side enqueue call (`deposit_wrapped_mint`'s,
    /// reached again on resume from `VaultDepositSubmitted`): seeds the
    /// aggregate straight to `VaultDepositSubmitted` via direct commands (so
    /// the wrap-side enqueue, which runs earlier in a fresh mint, never
    /// fires), then resumes with a closed apalis pool. The failure must leave
    /// the aggregate un-advanced -- still `VaultDepositSubmitted`, not
    /// `DepositedIntoRaindex` -- so a retry re-attempts both `confirm_tx` and
    /// the enqueue.
    #[tokio::test]
    async fn deposit_enqueue_failure_propagates() {
        let (pool, apalis_pool) = crate::test_utils::setup_test_pools().await;
        let services = mock_services();
        let mint_store = Arc::new(test_store(pool.clone(), services.clone()));
        let redemption_store = Arc::new(test_store(pool, services.clone()));
        let queue = RecordBotGasReceiptCostJobQueue::new(&apalis_pool);

        let transfer = CrossVenueEquityTransfer::new(
            EquityTransferServices {
                bot_gas_enqueuer: BotGasReceiptCostEnqueuer::Enabled(queue),
                ..services.clone()
            },
            mint_store,
            redemption_store,
        );

        let id = issuer_request_id("ISS-BOT-GAS-DEPOSIT-FAIL");
        transfer
            .mint_store
            .send(
                &id,
                TokenizedEquityMintCommand::RequestMint {
                    chain: Chain::Base,
                    issuer_request_id: id.clone(),
                    symbol: Symbol::new("AAPL").unwrap(),
                    quantity: float!(10),
                    wallet: Address::ZERO,
                },
            )
            .await
            .unwrap();
        submit_requested_mint(&transfer, &id).await;
        transfer
            .mint_store
            .send(&id, TokenizedEquityMintCommand::Poll)
            .await
            .unwrap();
        let wrap_tx = TxHash::random();
        transfer
            .mint_store
            .send(
                &id,
                TokenizedEquityMintCommand::SubmitWrap {
                    wrap_tx_hash: wrap_tx,
                },
            )
            .await
            .unwrap();
        transfer
            .mint_store
            .send(
                &id,
                TokenizedEquityMintCommand::WrapTokens {
                    wrap_tx_hash: wrap_tx,
                    wrapped_shares: U256::from(10_000_000_000_000_000_000u128),
                    wrap_block: 1,
                },
            )
            .await
            .unwrap();
        let vault_deposit_tx_hash = TxHash::random();
        transfer
            .mint_store
            .send(
                &id,
                TokenizedEquityMintCommand::SubmitVaultDeposit {
                    vault_deposit_tx_hash,
                },
            )
            .await
            .unwrap();

        let entity = transfer.mint_store.load(&id).await.unwrap().unwrap();
        assert!(
            matches!(entity, TokenizedEquityMint::VaultDepositSubmitted { .. }),
            "expected VaultDepositSubmitted, got: {entity:?}"
        );

        apalis_pool.close().await;

        let error = transfer.resume_mint(&id).await.unwrap_err();

        assert!(
            matches!(error, MintError::BotGasEnqueue(_)),
            "expected BotGasEnqueue, got: {error:?}"
        );

        let entity = transfer.mint_store.load(&id).await.unwrap().unwrap();
        assert!(
            matches!(entity, TokenizedEquityMint::VaultDepositSubmitted { .. }),
            "aggregate must remain in VaultDepositSubmitted (un-advanced) so the retry \
             re-attempts confirm_tx and the enqueue; got: {entity:?}"
        );
    }

    #[tokio::test]
    async fn resume_mint_from_accepted_completes_workflow() {
        let transfer = create_equity_transfer(
            Arc::new(MockTokenizer::new()),
            Arc::new(MockRaindex::new()),
            Arc::new(MockWrapper::new()),
        )
        .await;

        let id = issuer_request_id("ISS-RESUME");
        transfer
            .mint_store
            .send(
                &id,
                TokenizedEquityMintCommand::RequestMint {
                    chain: Chain::Base,
                    issuer_request_id: id.clone(),
                    symbol: Symbol::new("AAPL").unwrap(),
                    quantity: float!(10),
                    wallet: Address::ZERO,
                },
            )
            .await
            .unwrap();
        submit_requested_mint(&transfer, &id).await;

        transfer.resume_mint(&id).await.unwrap();

        let entity = transfer.mint_store.load(&id).await.unwrap().unwrap();
        assert!(
            matches!(entity, TokenizedEquityMint::DepositedIntoRaindex { .. }),
            "Expected deposited mint after resume, got: {entity:?}"
        );
    }

    #[tokio::test]
    async fn fresh_queued_jobs_for_a_disabled_listing_start_no_aggregate() {
        let symbol = Symbol::new("AAPL").unwrap();
        let mut services = mock_services();
        let mut equities = equities_listing("AAPL", Address::ZERO);
        equities.symbols.get_mut(&symbol).unwrap().rebalancing = RebalancingMode::Disabled;
        services.chains.get_mut(&Chain::Base).unwrap().equities = equities;
        let transfer = create_equity_transfer_with_services(services).await;
        let mint_id = issuer_request_id("queued-disabled-mint");
        let redemption_id = redemption_aggregate_id("queued-disabled-redemption");
        let quantity = FractionalShares::new(float!(10));
        assert!(matches!(
            transfer
                .resume_equity_to_market_making(&mint_id, &symbol, Chain::Base, quantity)
                .await,
            Err(MintTransferError::PreReceipt(
                MintError::RebalancingNotEnabled { .. }
            ))
        ));
        assert!(transfer.mint_store.load(&mint_id).await.unwrap().is_none());
        assert!(matches!(
            transfer
                .resume_equity_to_hedging(&redemption_id, &symbol, Chain::Base, quantity)
                .await,
            Err(RedemptionError::RebalancingNotEnabled { .. })
        ));
        assert!(
            transfer
                .redemption_store
                .load(&redemption_id)
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn queued_transfers_do_not_start_when_the_listing_is_paused() {
        let symbol = Symbol::new("AAPL").unwrap();
        let mut services = mock_services();
        let mut equities = equities_listing("AAPL", Address::ZERO);
        equities.symbols.get_mut(&symbol).unwrap().rebalancing = RebalancingMode::Paused;
        services.chains.get_mut(&Chain::Base).unwrap().equities = equities;
        let transfer = create_equity_transfer_with_services(services).await;
        let mint_id = issuer_request_id("queued-paused-mint");
        let redemption_id = redemption_aggregate_id("queued-paused-redemption");
        let quantity = FractionalShares::new(float!(10));

        assert!(matches!(
            transfer
                .resume_equity_to_market_making(&mint_id, &symbol, Chain::Base, quantity)
                .await,
            Err(MintTransferError::PreReceipt(
                MintError::RebalancingNotEnabled {
                    chain: Chain::Base,
                    ..
                }
            ))
        ));
        assert!(transfer.mint_store.load(&mint_id).await.unwrap().is_none());
        assert!(matches!(
            transfer
                .resume_equity_to_hedging(&redemption_id, &symbol, Chain::Base, quantity)
                .await,
            Err(RedemptionError::RebalancingNotEnabled {
                chain: Chain::Base,
                ..
            })
        ));
        assert!(
            transfer
                .redemption_store
                .load(&redemption_id)
                .await
                .unwrap()
                .is_none()
        );
    }

    /// A chain's equity table listing one symbol at `tokenized_equity`.
    fn equities_listing(symbol: &str, tokenized_equity: Address) -> ChainEquities {
        ChainEquities {
            operational_limit: None,
            symbols: HashMap::from([(
                Symbol::new(symbol).unwrap(),
                ChainEquityAsset {
                    tokenized_equity,
                    tokenized_equity_derivative: Address::ZERO,
                    vault_ids: Vec::new(),
                    trading: OperationMode::Enabled,
                    rebalancing: RebalancingMode::Enabled,
                    wrapped_equity_recovery: OperationMode::Disabled,
                    operational_limit: None,
                    target_share: None,
                },
            )]),
        }
    }

    /// Builds a transfer with full mint-authorization wiring: a stubbed
    /// vault-mode reader, an Enabled mock authorizer on Base listing AAPL,
    /// and a real (in-memory) delivery queue. Returns the event pool and
    /// the apalis pool for assertions.
    async fn create_authorization_wired_transfer(
        mode: VaultModeTag,
    ) -> (
        CrossVenueEquityTransfer,
        SqlitePool,
        apalis_sqlite::SqlitePool,
    ) {
        let mut services = mock_services();
        for chain_services in services.chains.values_mut() {
            chain_services.mint_authorizer =
                ConfiguredMintAuthorizer::Enabled(Arc::new(MockMintAuthorizer));
            chain_services.equities = equities_listing("AAPL", Address::repeat_byte(0x11));
        }

        authorization_wired_transfer(services, mode).await
    }

    /// Wires mint authorization onto `services`: a stubbed vault-mode
    /// reader and a real (in-memory) delivery queue.
    async fn authorization_wired_transfer(
        services: EquityTransferServices,
        mode: VaultModeTag,
    ) -> (
        CrossVenueEquityTransfer,
        SqlitePool,
        apalis_sqlite::SqlitePool,
    ) {
        let (pool, apalis_pool) = crate::test_utils::setup_test_pools().await;
        let mint_store = Arc::new(test_store(pool.clone(), services.clone()));
        let redemption_store = Arc::new(test_store(pool.clone(), services.clone()));

        let transfer =
            CrossVenueEquityTransfer::new(services.clone(), mint_store, redemption_store)
                .with_mint_authorization(MintAuthorizationWiring {
                    vault_mode_reader: Arc::new(StubVaultModeReader(mode)),
                    delivery_queue: DeliverMintAuthorizationJobQueue::new(&apalis_pool),
                });

        (transfer, pool, apalis_pool)
    }

    /// Test authorizer that keeps every token it was asked to bind, so a
    /// test can tell which chain's tokenized equity an authorization names.
    #[derive(Default)]
    struct RecordingMintAuthorizer {
        bound_tokens: Mutex<Vec<Address>>,
    }

    #[async_trait]
    impl MintAuthorizer for RecordingMintAuthorizer {
        async fn sign_mint_authorization(
            &self,
            token: Address,
            _quantity: Float,
            nonce: B256,
        ) -> Result<SignedMintAuthorization, MintAuthorizationError> {
            self.bound_tokens.lock().unwrap().push(token);
            Ok(SignedMintAuthorization {
                nonce,
                signature: Bytes::from(vec![0x42; 65]),
            })
        }
    }

    /// An orchestrator-mode mint recorded on Ethereum binds Ethereum's
    /// tokenized-equity address, never Base's: the same symbol is a
    /// different contract on each chain, and a MintAuth over the wrong one
    /// authorizes nothing the issuer will mint.
    #[tokio::test]
    async fn an_ethereum_mint_authorization_binds_ethereums_token_address() {
        let base_token = Address::repeat_byte(0xba);
        let ethereum_token = Address::repeat_byte(0xe7);
        let ethereum_authorizer = Arc::new(RecordingMintAuthorizer::default());
        let mut services = mock_services();
        services.chains.get_mut(&Chain::Base).unwrap().equities =
            equities_listing("AAPL", base_token);
        let mut ethereum = chain_services(Arc::new(MockTokenizer::new()));
        ethereum.mint_authorizer = ConfiguredMintAuthorizer::Enabled(ethereum_authorizer.clone());
        ethereum.equities = equities_listing("AAPL", ethereum_token);
        services.chains.insert(Chain::Ethereum, ethereum);
        let (transfer, _pool, _apalis_pool) =
            authorization_wired_transfer(services, VaultModeTag::Orchestrator).await;

        let id = issuer_request_id("ISS-ETHEREUM-AUTH");
        let symbol = Symbol::new("AAPL").unwrap();
        transfer
            .mint_store
            .send(
                &id,
                TokenizedEquityMintCommand::RequestMint {
                    chain: Chain::Ethereum,
                    issuer_request_id: id.clone(),
                    symbol: symbol.clone(),
                    quantity: float!(10),
                    wallet: Address::ZERO,
                },
            )
            .await
            .unwrap();
        submit_requested_mint(&transfer, &id).await;

        transfer.resume_mint(&id).await.unwrap();

        let bound_tokens = ethereum_authorizer.bound_tokens.lock().unwrap().clone();
        assert_eq!(
            bound_tokens,
            vec![ethereum_token],
            "the authorization must bind Ethereum's tokenized equity, not Base's {base_token}"
        );
    }

    async fn count_events(pool: &SqlitePool, event_type: &str) -> i64 {
        sqlx::query_scalar("SELECT COUNT(*) FROM events WHERE event_type = ?")
            .bind(event_type)
            .fetch_one(pool)
            .await
            .unwrap()
    }

    async fn count_delivery_jobs(apalis_pool: &apalis_sqlite::SqlitePool) -> i64 {
        sqlx_apalis::query_scalar::<_, i64>("SELECT COUNT(*) FROM Jobs WHERE job_type = ?")
            .bind(std::any::type_name::<DeliverMintAuthorization>())
            .fetch_one(apalis_pool)
            .await
            .unwrap()
    }

    /// An orchestrator-mode mint resumed from `MintAccepted` signs its
    /// authorization, enqueues exactly one delivery, and still completes
    /// the workflow -- and a SECOND resume never signs a fresh nonce or
    /// duplicates the delivery job (the resume idempotency invariant).
    #[tokio::test]
    async fn resume_of_orchestrator_mint_signs_once_and_enqueues_delivery() {
        let (transfer, pool, apalis_pool) =
            create_authorization_wired_transfer(VaultModeTag::Orchestrator).await;

        let id = issuer_request_id("ISS-ORCH-RESUME");
        transfer
            .mint_store
            .send(
                &id,
                TokenizedEquityMintCommand::RequestMint {
                    chain: Chain::Base,
                    issuer_request_id: id.clone(),
                    symbol: Symbol::new("AAPL").unwrap(),
                    quantity: float!(10),
                    wallet: Address::ZERO,
                },
            )
            .await
            .unwrap();
        submit_requested_mint(&transfer, &id).await;

        transfer.resume_mint(&id).await.unwrap();

        let entity = transfer.mint_store.load(&id).await.unwrap().unwrap();
        assert!(
            matches!(entity, TokenizedEquityMint::DepositedIntoRaindex { .. }),
            "Expected deposited mint after resume, got: {entity:?}"
        );
        assert_eq!(
            count_events(&pool, "TokenizedEquityMintEvent::MintAuthorizationSigned").await,
            1
        );
        assert_eq!(count_delivery_jobs(&apalis_pool).await, 1);

        // Second resume: the mint is DepositedIntoRaindex, so resume_mint
        // short-circuits on the terminal state before ever reaching
        // authorization -- the unchanged counts pin that short-circuit.
        // (Signing idempotency itself is pinned by
        // `re_signing_keeps_the_original_nonce` in tokenized_equity_mint.)
        transfer.resume_mint(&id).await.unwrap();
        assert_eq!(
            count_events(&pool, "TokenizedEquityMintEvent::MintAuthorizationSigned").await,
            1
        );
        assert_eq!(count_delivery_jobs(&apalis_pool).await, 1);
    }

    /// A restart during a delayed-redrive window leaves an UNKEYED pending
    /// successor row (`push_with_delay` records no idempotency key), which
    /// `push_idempotent` alone cannot see. The live-delivery guard must
    /// bound the mint to one chain: re-ensuring the authorization enqueues
    /// nothing while that successor is live.
    #[tokio::test]
    async fn ensure_does_not_start_a_second_delivery_chain_over_a_redrive_successor() {
        let (transfer, _pool, apalis_pool) =
            create_authorization_wired_transfer(VaultModeTag::Orchestrator).await;

        let id = issuer_request_id("ISS-ORCH-REDRIVE");
        let symbol = Symbol::new("AAPL").unwrap();
        transfer
            .mint_store
            .send(
                &id,
                TokenizedEquityMintCommand::RequestMint {
                    chain: Chain::Base,
                    issuer_request_id: id.clone(),
                    symbol: symbol.clone(),
                    quantity: float!(10),
                    wallet: Address::ZERO,
                },
            )
            .await
            .unwrap();
        submit_requested_mint(&transfer, &id).await;
        transfer
            .mint_store
            .send(
                &id,
                TokenizedEquityMintCommand::SignMintAuthorization {
                    token: Address::repeat_byte(0x11),
                },
            )
            .await
            .unwrap();

        // The state a crash mid-redrive leaves behind: one delayed,
        // UNKEYED successor row.
        let mut queue = DeliverMintAuthorizationJobQueue::new(&apalis_pool);
        queue
            .push_with_delay(
                DeliverMintAuthorization {
                    issuer_request_id: id.clone(),
                    redrive_attempts: 3,
                },
                std::time::Duration::from_secs(30),
            )
            .await
            .unwrap();
        assert_eq!(count_delivery_jobs(&apalis_pool).await, 1);

        transfer
            .ensure_mint_authorization(&id, Chain::Base, &symbol)
            .await
            .unwrap();

        assert_eq!(
            count_delivery_jobs(&apalis_pool).await,
            1,
            "the live-delivery guard must not start a second chain over a \
             pending redrive successor"
        );
    }

    /// A vault-direct asset takes the pre-authorization path untouched: no
    /// signing event, no delivery job.
    #[tokio::test]
    async fn resume_of_vault_direct_mint_signs_nothing() {
        let (transfer, pool, apalis_pool) =
            create_authorization_wired_transfer(VaultModeTag::VaultDirect).await;

        let id = issuer_request_id("ISS-DIRECT-RESUME");
        transfer
            .mint_store
            .send(
                &id,
                TokenizedEquityMintCommand::RequestMint {
                    chain: Chain::Base,
                    issuer_request_id: id.clone(),
                    symbol: Symbol::new("AAPL").unwrap(),
                    quantity: float!(10),
                    wallet: Address::ZERO,
                },
            )
            .await
            .unwrap();
        submit_requested_mint(&transfer, &id).await;

        transfer.resume_mint(&id).await.unwrap();

        let entity = transfer.mint_store.load(&id).await.unwrap().unwrap();
        assert!(
            matches!(entity, TokenizedEquityMint::DepositedIntoRaindex { .. }),
            "Expected deposited mint after resume, got: {entity:?}"
        );
        assert_eq!(
            count_events(&pool, "TokenizedEquityMintEvent::MintAuthorizationSigned").await,
            0
        );
        assert_eq!(count_delivery_jobs(&apalis_pool).await, 0);
    }

    /// An orchestrator-mode asset its chain does not list under
    /// `[chains.<name>.trading.assets.equities]` cannot bind the EIP-712
    /// MintAuth to a token: the mint must fail naming the chain and symbol,
    /// never proceed unauthorized.
    #[tokio::test]
    async fn orchestrator_asset_without_token_address_fails_before_signing() {
        let (transfer, pool, apalis_pool) =
            create_authorization_wired_transfer(VaultModeTag::Orchestrator).await;

        // TSLA is absent from the helper's AAPL-only Base equity table.
        let tsla = Symbol::new("TSLA").unwrap();
        let error = transfer
            .ensure_mint_authorization(&issuer_request_id("ISS-NO-TOKEN"), Chain::Base, &tsla)
            .await
            .unwrap_err();

        assert!(
            matches!(
                &error,
                MintError::UnknownTokenizedEquity { chain: Chain::Base, symbol } if *symbol == tsla
            ),
            "expected UnknownTokenizedEquity naming Base and TSLA, got {error:?}"
        );
        assert_eq!(
            count_events(&pool, "TokenizedEquityMintEvent::MintAuthorizationSigned").await,
            0,
            "the failed lookup must precede any signing"
        );
        assert_eq!(count_delivery_jobs(&apalis_pool).await, 0);
    }

    /// A transfer built without `with_mint_authorization` carries the
    /// explicit `VaultDirectOnly` assertion: `ensure_mint_authorization`
    /// cannot read `vault_mode` at all, so it must sign nothing and
    /// enqueue nothing -- pinned so the assertion stays a stated
    /// capability rather than drifting into silent behavior.
    #[tokio::test]
    async fn vault_direct_only_transfer_signs_nothing_and_enqueues_nothing() {
        let (pool, apalis_pool) = crate::test_utils::setup_test_pools().await;
        let mut services = mock_services();
        for chain_services in services.chains.values_mut() {
            chain_services.mint_authorizer =
                ConfiguredMintAuthorizer::Enabled(Arc::new(MockMintAuthorizer));
        }
        let mint_store = Arc::new(test_store(pool.clone(), services.clone()));
        let redemption_store = Arc::new(test_store(pool.clone(), services.clone()));
        let transfer =
            CrossVenueEquityTransfer::new(services.clone(), mint_store, redemption_store);

        let id = issuer_request_id("ISS-VAULT-DIRECT-ONLY");
        let symbol = Symbol::new("AAPL").unwrap();
        transfer
            .mint_store
            .send(
                &id,
                TokenizedEquityMintCommand::RequestMint {
                    chain: Chain::Base,
                    issuer_request_id: id.clone(),
                    symbol: symbol.clone(),
                    quantity: float!(10),
                    wallet: Address::ZERO,
                },
            )
            .await
            .unwrap();
        submit_requested_mint(&transfer, &id).await;

        transfer
            .ensure_mint_authorization(&id, Chain::Base, &symbol)
            .await
            .unwrap();

        assert_eq!(
            count_events(&pool, "TokenizedEquityMintEvent::MintAuthorizationSigned").await,
            0,
            "a VaultDirectOnly transfer must never sign an authorization"
        );
        assert_eq!(count_delivery_jobs(&apalis_pool).await, 0);
    }

    /// Re-running the job entry point with an id whose aggregate already
    /// exists must resume from the persisted state (the crash-recovery path)
    /// rather than re-requesting the mint from Alpaca.
    #[tokio::test]
    async fn resume_equity_to_market_making_resumes_existing_aggregate_despite_low_gas() {
        let mut transfer = create_equity_transfer_with_services(mock_services_with_gas_readiness(
            &crate::native_gas::GasReadiness::for_test(
                U256::ZERO,
                U256::from(1_u64),
                U256::MAX,
                U256::from(1_u64),
            ),
        ))
        .await;

        let id = issuer_request_id("ISS-CRASH-RESUME");
        let symbol = Symbol::new("AAPL").unwrap();

        let mut equities = equities_listing("AAPL", Address::ZERO);
        equities.symbols.get_mut(&symbol).unwrap().rebalancing = RebalancingMode::Paused;
        transfer
            .services
            .chains
            .get_mut(&Chain::Base)
            .unwrap()
            .equities = equities;

        // First attempt crashes after the mint request was accepted: the
        // aggregate is persisted in MintAccepted.
        transfer
            .mint_store
            .send(
                &id,
                TokenizedEquityMintCommand::RequestMint {
                    chain: Chain::Base,
                    issuer_request_id: id.clone(),
                    symbol: symbol.clone(),
                    quantity: float!(10),
                    wallet: Address::ZERO,
                },
            )
            .await
            .unwrap();
        submit_requested_mint(&transfer, &id).await;

        // The re-enqueued job runs the same entry point with the same id; a
        // fresh RequestMint here would fail with AlreadyInProgress, so
        // completing proves the resume path was taken.
        transfer
            .resume_equity_to_market_making(
                &id,
                &symbol,
                Chain::Base,
                FractionalShares::new(float!(10)),
            )
            .await
            .unwrap();

        let entity = transfer.mint_store.load(&id).await.unwrap().unwrap();
        assert!(
            matches!(entity, TokenizedEquityMint::DepositedIntoRaindex { .. }),
            "Expected deposited mint after job re-run, got: {entity:?}"
        );
    }

    /// A terminal aggregate is a no-op for the job entry point: apalis
    /// retries after a partial failure must not error once the aggregate
    /// has already completed.
    #[tokio::test]
    async fn resume_equity_to_market_making_is_noop_on_completed_mint() {
        let transfer = create_equity_transfer(
            Arc::new(MockTokenizer::new()),
            Arc::new(MockRaindex::new()),
            Arc::new(MockWrapper::new()),
        )
        .await;

        let id = issuer_request_id("ISS-DONE");
        let symbol = Symbol::new("AAPL").unwrap();

        transfer
            .resume_equity_to_market_making(
                &id,
                &symbol,
                Chain::Base,
                FractionalShares::new(float!(10)),
            )
            .await
            .unwrap();

        let entity = transfer.mint_store.load(&id).await.unwrap().unwrap();
        assert!(
            matches!(entity, TokenizedEquityMint::DepositedIntoRaindex { .. }),
            "Expected deposited mint, got: {entity:?}"
        );

        transfer
            .resume_equity_to_market_making(
                &id,
                &symbol,
                Chain::Base,
                FractionalShares::new(float!(10)),
            )
            .await
            .expect("a completed mint must be a clean no-op for the job retry");
    }

    #[tokio::test]
    async fn resume_redemption_from_tokens_sent_completes_workflow() {
        let tokenizer: Arc<dyn Tokenizer> = Arc::new(
            MockTokenizer::new()
                .with_detection_outcome(MockDetectionOutcome::Detected)
                .with_completion_outcome(MockCompletionOutcome::Completed),
        );
        let transfer = create_equity_transfer(
            tokenizer,
            Arc::new(MockRaindex::new()),
            Arc::new(MockWrapper::new()),
        )
        .await;

        let id = redemption_aggregate_id("redemption-resume");
        let symbol = Symbol::new("TEST").unwrap();
        let token = transfer
            .services
            .for_chain(Chain::Base)
            .unwrap()
            .vault_lookup
            .vault_token_for_symbol(&symbol)
            .await
            .unwrap();
        let amount = FractionalShares::new(float!(50))
            .to_u256_18_decimals()
            .unwrap();

        transfer
            .withdraw_from_raindex(
                &id,
                &symbol,
                Chain::Base,
                FractionalShares::new(float!(50)),
                token,
                amount,
            )
            .await
            .unwrap();
        transfer.unwrap_and_send(&id).await.unwrap();

        transfer.resume_redemption(&id).await.unwrap();

        let entity = transfer.redemption_store.load(&id).await.unwrap().unwrap();
        assert!(
            matches!(entity, EquityRedemption::Completed { .. }),
            "Expected completed redemption after resume, got: {entity:?}"
        );
    }

    #[tokio::test]
    async fn resume_redemption_from_tokens_sent_reenqueues_wallet_transfer_gas() {
        let tokenizer: Arc<dyn Tokenizer> = Arc::new(
            MockTokenizer::new()
                .with_detection_outcome(MockDetectionOutcome::Detected)
                .with_completion_outcome(MockCompletionOutcome::Completed),
        );
        let mut transfer = create_equity_transfer(
            tokenizer,
            Arc::new(MockRaindex::new()),
            Arc::new(MockWrapper::new()),
        )
        .await;
        let id = redemption_aggregate_id("redemption-gas-resume");
        let symbol = Symbol::new("TEST").unwrap();
        let redemption_tx = advance_redemption_to_tokens_sent(&transfer, &id, &symbol).await;
        let (_pool, apalis_pool) = crate::test_utils::setup_test_pools().await;
        let queue = RecordBotGasReceiptCostJobQueue::new(&apalis_pool);
        transfer.services.bot_gas_enqueuer = BotGasReceiptCostEnqueuer::Enabled(queue);

        transfer.resume_redemption(&id).await.unwrap();

        let jobs = pending_bot_gas_jobs(&apalis_pool).await;
        assert_eq!(jobs.len(), 1, "expected exactly one recovered gas job");
        assert_eq!(jobs[0].category, BotGasOperationCategory::WalletTransfer);
        assert_eq!(jobs[0].chain, Chain::Base);
        assert_eq!(jobs[0].tx_hash, redemption_tx);
        assert_eq!(jobs[0].symbol, Some(symbol));
    }

    #[tokio::test]
    async fn resume_redemption_enqueue_failure_does_not_resend_tokens() {
        let tokenizer = Arc::new(
            MockTokenizer::new()
                .with_detection_outcome(MockDetectionOutcome::Detected)
                .with_completion_outcome(MockCompletionOutcome::Completed),
        );
        let mut transfer = create_equity_transfer(
            tokenizer.clone(),
            Arc::new(MockRaindex::new()),
            Arc::new(MockWrapper::new()),
        )
        .await;
        let id = redemption_aggregate_id("redemption-gas-enqueue-failure");
        let symbol = Symbol::new("TEST").unwrap();
        advance_redemption_to_tokens_sent(&transfer, &id, &symbol).await;
        let calls_before_resume = tokenizer.call_count();
        let (_pool, apalis_pool) = crate::test_utils::setup_test_pools().await;
        let queue = RecordBotGasReceiptCostJobQueue::new(&apalis_pool);
        apalis_pool.close().await;
        transfer.services.bot_gas_enqueuer = BotGasReceiptCostEnqueuer::Enabled(queue);

        let error = transfer.resume_redemption(&id).await.unwrap_err();

        assert!(error.is_bot_gas_enqueue_failure());
        assert_eq!(
            tokenizer.call_count(),
            calls_before_resume,
            "retrying accounting must not call the tokenizer or resend tokens"
        );
        let entity = transfer.redemption_store.load(&id).await.unwrap().unwrap();
        assert!(
            matches!(entity, EquityRedemption::TokensSent { .. }),
            "failed accounting enqueue must leave the persisted send resumable, got: {entity:?}"
        );
    }

    /// A fresh id runs the full redemption flow through the job entry point.
    #[tokio::test]
    async fn resume_equity_to_hedging_starts_fresh_redemption() {
        let tokenizer: Arc<dyn Tokenizer> = Arc::new(
            MockTokenizer::new()
                .with_detection_outcome(MockDetectionOutcome::Detected)
                .with_completion_outcome(MockCompletionOutcome::Completed),
        );
        let transfer = create_equity_transfer(
            tokenizer,
            Arc::new(MockRaindex::new()),
            Arc::new(MockWrapper::new()),
        )
        .await;

        let id = redemption_aggregate_id("redeem-fresh");
        transfer
            .resume_equity_to_hedging(
                &id,
                &Symbol::new("TEST").unwrap(),
                Chain::Base,
                FractionalShares::new(float!(50)),
            )
            .await
            .unwrap();

        let entity = transfer.redemption_store.load(&id).await.unwrap().unwrap();
        assert!(
            matches!(entity, EquityRedemption::Completed { .. }),
            "Expected completed redemption, got: {entity:?}"
        );
    }

    #[tokio::test]
    async fn fresh_redemption_refuses_low_base_gas_before_creating_aggregate() {
        let transfer = create_equity_transfer_with_services(mock_services_with_gas_readiness(
            &crate::native_gas::GasReadiness::for_test(
                U256::ZERO,
                U256::from(1_u64),
                U256::MAX,
                U256::from(1_u64),
            ),
        ))
        .await;
        let id = redemption_aggregate_id("redemption-low-gas");

        let error = transfer
            .resume_equity_to_hedging(
                &id,
                &Symbol::new("AAPL").unwrap(),
                Chain::Base,
                FractionalShares::new(float!(50)),
            )
            .await
            .unwrap_err();

        assert!(matches!(error, RedemptionError::GasReadiness(_)));
        assert!(
            transfer.redemption_store.load(&id).await.unwrap().is_none(),
            "gas refusal must happen before the Raindex withdrawal creates aggregate state"
        );
    }

    /// Acceptance criterion: a full redemption (withdraw through the
    /// redemption-wallet send) enqueues exactly one `VaultWithdraw`, one
    /// `Unwrap`, and one `WalletTransfer` bot-gas job, each carrying the
    /// symbol and Base chain.
    #[tokio::test]
    async fn redemption_transfer_enqueues_vault_withdraw_and_unwrap_bot_gas_jobs() {
        let (pool, apalis_pool) = crate::test_utils::setup_test_pools().await;
        let queue = RecordBotGasReceiptCostJobQueue::new(&apalis_pool);
        let tokenizer: Arc<dyn Tokenizer> = Arc::new(
            MockTokenizer::new()
                .with_detection_outcome(MockDetectionOutcome::Detected)
                .with_completion_outcome(MockCompletionOutcome::Completed),
        );
        let services = EquityTransferServices {
            chains: BTreeMap::from([(
                Chain::Base,
                ChainEquityServices {
                    wallet: Address::ZERO,
                    raindex: Arc::new(MockRaindex::new()),
                    vault_lookup: Arc::new(mock_vault_lookup()),
                    tokenizer: tokenizer.clone(),
                    wrapper: Arc::new(MockWrapper::new()),
                    mint_authorizer: ConfiguredMintAuthorizer::Disabled,
                    gas_readiness: ConfiguredGasReadiness::Unwired,
                    equities: equities_listing("TEST", Address::ZERO),
                },
            )]),
            bot_gas_enqueuer: BotGasReceiptCostEnqueuer::Enabled(queue),
        };
        let mint_store = Arc::new(test_store(pool.clone(), services.clone()));
        let redemption_store = Arc::new(test_store(pool, services.clone()));
        let transfer =
            CrossVenueEquityTransfer::new(services.clone(), mint_store, redemption_store);

        let symbol = Symbol::new("TEST").unwrap();
        let id = redemption_aggregate_id("redeem-bot-gas");
        transfer
            .resume_equity_to_hedging(&id, &symbol, Chain::Base, FractionalShares::new(float!(50)))
            .await
            .unwrap();

        let jobs = pending_bot_gas_jobs(&apalis_pool).await;
        assert_eq!(
            jobs.len(),
            3,
            "expected exactly one VaultWithdraw, one Unwrap, and one WalletTransfer job"
        );
        let withdraw_jobs: Vec<_> = jobs
            .iter()
            .filter(|job| job.category == BotGasOperationCategory::VaultWithdraw)
            .collect();
        let unwrap_jobs: Vec<_> = jobs
            .iter()
            .filter(|job| job.category == BotGasOperationCategory::Unwrap)
            .collect();
        let wallet_transfer_jobs: Vec<_> = jobs
            .iter()
            .filter(|job| job.category == BotGasOperationCategory::WalletTransfer)
            .collect();
        assert_eq!(withdraw_jobs.len(), 1);
        assert_eq!(unwrap_jobs.len(), 1);
        assert_eq!(wallet_transfer_jobs.len(), 1);
        for job in [withdraw_jobs[0], unwrap_jobs[0], wallet_transfer_jobs[0]] {
            assert_eq!(job.chain, Chain::Base);
            assert_eq!(job.symbol, Some(symbol.clone()));
        }
    }

    /// Acceptance criterion: an enqueue failure after a confirmed
    /// vault withdraw propagates as a hard error rather than being swallowed.
    #[tokio::test]
    async fn withdraw_enqueue_failure_propagates() {
        let (pool, apalis_pool) = crate::test_utils::setup_test_pools().await;
        let queue = RecordBotGasReceiptCostJobQueue::new(&apalis_pool);
        apalis_pool.close().await;
        let symbol = Symbol::new("TEST").unwrap();
        let token = mock_vault_lookup()
            .vault_token_for_symbol(&symbol)
            .await
            .unwrap();
        let amount = FractionalShares::new(float!(50))
            .to_u256_18_decimals()
            .unwrap();
        let services = EquityTransferServices {
            chains: BTreeMap::from([(
                Chain::Base,
                ChainEquityServices {
                    wallet: Address::ZERO,
                    raindex: Arc::new(MockRaindex::new().with_withdraw_transfer(token, amount)),
                    vault_lookup: Arc::new(mock_vault_lookup()),
                    tokenizer: Arc::new(MockTokenizer::new()),
                    wrapper: Arc::new(MockWrapper::new()),
                    mint_authorizer: ConfiguredMintAuthorizer::Disabled,
                    gas_readiness: ConfiguredGasReadiness::Unwired,
                    equities: equities_listing("AAPL", Address::ZERO),
                },
            )]),
            bot_gas_enqueuer: BotGasReceiptCostEnqueuer::Enabled(queue),
        };
        let redemption_store = Arc::new(test_store::<EquityRedemption>(pool, services.clone()));

        let id = redemption_aggregate_id("redeem-bot-gas-fail");
        redemption_store
            .send(
                &id,
                EquityRedemptionCommand::Redeem {
                    chain: Chain::Base,
                    symbol: symbol.clone(),
                    quantity: float!(50),
                    token,
                    vault_id: RaindexVaultId(B256::ZERO),
                    amount,
                    from_block: 0,
                    prepared: crate::equity_redemption::prepared_withdrawal_for_test(),
                },
            )
            .await
            .unwrap();
        redemption_store
            .send(
                &id,
                EquityRedemptionCommand::RecordWithdrawSubmission {
                    tx_hash: alloy::primitives::TxHash::ZERO,
                },
            )
            .await
            .unwrap();

        let error = redemption_store
            .send(&id, EquityRedemptionCommand::ConfirmWithdraw)
            .await
            .unwrap_err();

        assert!(
            matches!(
                error,
                AggregateError::UserError(LifecycleError::Apply(
                    EquityRedemptionError::BotGasEnqueueFailed(_)
                ))
            ),
            "expected BotGasEnqueueFailed, got: {error:?}"
        );
    }

    /// A redemption whose withdrawal landed but whose gas-cost enqueue failed
    /// is retried by recovery, not recorded as a permanent recovery failure.
    #[test]
    fn a_bot_gas_enqueue_failure_after_a_landed_step_is_retryable() {
        let error = RedemptionError::Send(AggregateError::UserError(LifecycleError::Apply(
            EquityRedemptionError::BotGasEnqueueFailed(crate::bot_gas::BotGasEnqueueFailure {
                tx_hash: alloy::primitives::TxHash::ZERO,
                kind: crate::bot_gas::QueuePushFailureKind::Push,
                message: "pool closed".to_string(),
            }),
        )));

        assert_eq!(error.resume_failure_kind(), ResumeFailureKind::Retryable);
    }

    /// Re-running the job entry point with an id whose aggregate already
    /// exists must resume from the persisted state (the crash-recovery path)
    /// rather than re-running the vault withdrawal from scratch.
    #[tokio::test]
    async fn resume_equity_to_hedging_resumes_existing_aggregate_despite_low_gas() {
        let tokenizer: Arc<dyn Tokenizer> = Arc::new(
            MockTokenizer::new()
                .with_detection_outcome(MockDetectionOutcome::Detected)
                .with_completion_outcome(MockCompletionOutcome::Completed),
        );
        let mut services = mock_services_with(
            &tokenizer,
            &(Arc::new(MockRaindex::new()) as Arc<dyn Raindex>),
            &(Arc::new(MockWrapper::new()) as Arc<dyn Wrapper>),
        );
        let readiness = crate::native_gas::GasReadiness::for_test(
            U256::ZERO,
            U256::from(1_u64),
            U256::MAX,
            U256::from(1_u64),
        );
        for chain_services in services.chains.values_mut() {
            chain_services.gas_readiness = ConfiguredGasReadiness::Wired(readiness.clone());
        }
        let mut equities = equities_listing("TEST", Address::ZERO);
        equities
            .symbols
            .get_mut(&Symbol::new("TEST").unwrap())
            .unwrap()
            .rebalancing = RebalancingMode::Paused;
        services.chains.get_mut(&Chain::Base).unwrap().equities = equities;
        let transfer = create_equity_transfer_with_services(services).await;

        let id = redemption_aggregate_id("redeem-crash-resume");
        let symbol = Symbol::new("TEST").unwrap();
        let quantity = FractionalShares::new(float!(50));
        let token = transfer
            .services
            .for_chain(Chain::Base)
            .unwrap()
            .vault_lookup
            .vault_token_for_symbol(&symbol)
            .await
            .unwrap();
        let amount = quantity.to_u256_18_decimals().unwrap();

        // First attempt crashes after the withdrawal: the aggregate is
        // persisted mid-flight.
        transfer
            .withdraw_from_raindex(&id, &symbol, Chain::Base, quantity, token, amount)
            .await
            .unwrap();

        // The re-enqueued job runs the same entry point with the same id; a
        // fresh Redeem command here would fail on the already-initialized
        // aggregate, so completing proves the resume path was taken.
        transfer
            .resume_equity_to_hedging(&id, &symbol, Chain::Base, quantity)
            .await
            .unwrap();

        let entity = transfer.redemption_store.load(&id).await.unwrap().unwrap();
        assert!(
            matches!(entity, EquityRedemption::Completed { .. }),
            "Expected completed redemption after job re-run, got: {entity:?}"
        );
    }

    /// A terminal aggregate is a no-op for the job entry point: apalis
    /// retries after a partial failure must not error once the aggregate has
    /// already completed.
    #[tokio::test]
    async fn resume_equity_to_hedging_is_noop_on_completed_redemption() {
        let tokenizer: Arc<dyn Tokenizer> = Arc::new(
            MockTokenizer::new()
                .with_detection_outcome(MockDetectionOutcome::Detected)
                .with_completion_outcome(MockCompletionOutcome::Completed),
        );
        let transfer = create_equity_transfer(
            tokenizer,
            Arc::new(MockRaindex::new()),
            Arc::new(MockWrapper::new()),
        )
        .await;

        let id = redemption_aggregate_id("redeem-done");
        let symbol = Symbol::new("TEST").unwrap();
        let quantity = FractionalShares::new(float!(50));

        transfer
            .resume_equity_to_hedging(&id, &symbol, Chain::Base, quantity)
            .await
            .unwrap();

        transfer
            .resume_equity_to_hedging(&id, &symbol, Chain::Base, quantity)
            .await
            .expect("a completed redemption must be a clean no-op for the job retry");
    }

    #[tokio::test]
    async fn redemption_transfer_full_workflow_succeeds() {
        let tokenizer: Arc<dyn Tokenizer> = Arc::new(
            MockTokenizer::new()
                .with_detection_outcome(MockDetectionOutcome::Detected)
                .with_completion_outcome(MockCompletionOutcome::Completed),
        );
        let raindex: Arc<dyn Raindex> = Arc::new(MockRaindex::new());

        let transfer =
            create_equity_transfer(tokenizer, raindex, Arc::new(MockWrapper::new())).await;

        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            transfer.resume_equity_to_hedging(
                &redemption_aggregate_id("redeem-workflow"),
                &Symbol::new("TEST").unwrap(),
                Chain::Base,
                FractionalShares::new(float!(50)),
            ),
        )
        .await
        .expect("redemption transfer timed out")
        .unwrap();
    }

    #[tokio::test]
    async fn redemption_transfer_fails_on_detection_timeout() {
        let tokenizer: Arc<dyn Tokenizer> =
            Arc::new(MockTokenizer::new().with_detection_outcome(MockDetectionOutcome::Timeout));
        let raindex: Arc<dyn Raindex> = Arc::new(MockRaindex::new());

        let transfer =
            create_equity_transfer(tokenizer, raindex, Arc::new(MockWrapper::new())).await;

        let error = transfer
            .resume_equity_to_hedging(
                &redemption_aggregate_id("redeem-detection-timeout"),
                &Symbol::new("TEST").unwrap(),
                Chain::Base,
                FractionalShares::new(float!(50)),
            )
            .await
            .unwrap_err();

        assert!(
            matches!(error, RedemptionError::Tokenizer(_)),
            "Expected Tokenizer error, got: {error:?}"
        );
    }

    #[tokio::test]
    async fn redemption_transfer_fails_on_detection_api_error() {
        let tokenizer: Arc<dyn Tokenizer> =
            Arc::new(MockTokenizer::new().with_detection_outcome(MockDetectionOutcome::ApiError));
        let raindex: Arc<dyn Raindex> = Arc::new(MockRaindex::new());

        let transfer =
            create_equity_transfer(tokenizer, raindex, Arc::new(MockWrapper::new())).await;

        let error = transfer
            .resume_equity_to_hedging(
                &redemption_aggregate_id("redeem-detection-api-error"),
                &Symbol::new("TEST").unwrap(),
                Chain::Base,
                FractionalShares::new(float!(50)),
            )
            .await
            .unwrap_err();

        assert!(
            matches!(error, RedemptionError::Tokenizer(_)),
            "Expected Tokenizer error, got: {error:?}"
        );
    }

    #[tokio::test]
    async fn redemption_transfer_fails_on_completion_rejection() {
        let tokenizer: Arc<dyn Tokenizer> = Arc::new(
            MockTokenizer::new()
                .with_detection_outcome(MockDetectionOutcome::Detected)
                .with_completion_outcome(MockCompletionOutcome::Rejected),
        );
        let raindex: Arc<dyn Raindex> = Arc::new(MockRaindex::new());

        let transfer =
            create_equity_transfer(tokenizer, raindex, Arc::new(MockWrapper::new())).await;

        let error = transfer
            .resume_equity_to_hedging(
                &redemption_aggregate_id("redeem-completion-rejected"),
                &Symbol::new("TEST").unwrap(),
                Chain::Base,
                FractionalShares::new(float!(50)),
            )
            .await
            .unwrap_err();

        assert!(
            matches!(error, RedemptionError::Rejected),
            "Expected Rejected error, got: {error:?}"
        );
    }

    #[tokio::test]
    async fn redemption_transfer_fails_on_pending_status() {
        let tokenizer: Arc<dyn Tokenizer> = Arc::new(
            MockTokenizer::new()
                .with_detection_outcome(MockDetectionOutcome::Detected)
                .with_completion_outcome(MockCompletionOutcome::Pending),
        );
        let raindex: Arc<dyn Raindex> = Arc::new(MockRaindex::new());

        let transfer =
            create_equity_transfer(tokenizer, raindex, Arc::new(MockWrapper::new())).await;

        let error = transfer
            .resume_equity_to_hedging(
                &redemption_aggregate_id("redeem-pending-status"),
                &Symbol::new("TEST").unwrap(),
                Chain::Base,
                FractionalShares::new(float!(50)),
            )
            .await
            .unwrap_err();

        assert!(
            matches!(error, RedemptionError::UnexpectedPendingStatus),
            "Expected UnexpectedPendingStatus error, got: {error:?}"
        );
    }

    #[tokio::test]
    async fn mint_transfer_fails_when_wrapper_fails() {
        let transfer = create_equity_transfer(
            Arc::new(MockTokenizer::new()),
            Arc::new(MockRaindex::new()),
            Arc::new(MockWrapper::failing()),
        )
        .await;

        let error = transfer
            .resume_equity_to_market_making(
                &issuer_request_id("ISS-TEST"),
                &Symbol::new("AAPL").unwrap(),
                Chain::Base,
                FractionalShares::new(float!(100.0)),
            )
            .await
            .unwrap_err();

        assert!(
            matches!(error, MintTransferError::PostReceipt(MintError::Wrapper(_))),
            "Expected PostReceipt(Wrapper) error, got: {error:?}"
        );
    }

    #[tokio::test]
    async fn mint_transfer_fails_when_raindex_deposit_fails() {
        let transfer = create_equity_transfer(
            Arc::new(MockTokenizer::new()),
            Arc::new(MockRaindex::new().with_deposit_behavior(DepositBehavior::FailGeneric)),
            Arc::new(MockWrapper::new()),
        )
        .await;

        let error = transfer
            .resume_equity_to_market_making(
                &issuer_request_id("ISS-TEST"),
                &Symbol::new("AAPL").unwrap(),
                Chain::Base,
                FractionalShares::new(float!(100.0)),
            )
            .await
            .unwrap_err();

        assert!(
            matches!(error, MintTransferError::PostReceipt(MintError::Raindex(_))),
            "Expected PostReceipt(Raindex) error, got: {error:?}"
        );
    }

    #[tokio::test]
    async fn mint_transfer_fails_when_underlying_lookup_fails() {
        let transfer = create_equity_transfer(
            Arc::new(MockTokenizer::new()),
            Arc::new(MockRaindex::new()),
            Arc::new(MockWrapper::failing_lookup()),
        )
        .await;

        let error = transfer
            .resume_equity_to_market_making(
                &issuer_request_id("ISS-TEST"),
                &Symbol::new("AAPL").unwrap(),
                Chain::Base,
                FractionalShares::new(float!(100)),
            )
            .await
            .unwrap_err();

        assert!(
            matches!(
                error,
                MintTransferError::PostReceipt(MintError::Wrapper(
                    WrapperError::SymbolNotConfigured(_)
                ))
            ),
            "Expected PostReceipt(Wrapper(SymbolNotConfigured)) error, got: {error:?}"
        );
    }

    #[tokio::test]
    async fn mint_transfer_fails_when_derivative_lookup_fails() {
        let transfer = create_equity_transfer(
            Arc::new(MockTokenizer::new()),
            Arc::new(MockRaindex::new()),
            Arc::new(MockWrapper::failing_derivative_lookup()),
        )
        .await;

        let error = transfer
            .resume_equity_to_market_making(
                &issuer_request_id("ISS-TEST"),
                &Symbol::new("AAPL").unwrap(),
                Chain::Base,
                FractionalShares::new(float!(100)),
            )
            .await
            .unwrap_err();

        assert!(
            matches!(
                error,
                MintTransferError::PostReceipt(MintError::Wrapper(
                    WrapperError::SymbolNotConfigured(_)
                ))
            ),
            "Expected PostReceipt(Wrapper(SymbolNotConfigured)) error, got: {error:?}"
        );
    }

    /// Verifies that mint deposits the derivative token (from
    /// `lookup_derivative`) to the Raindex vault, not the
    /// base tokenized share (from `lookup_underlying`).
    #[tokio::test]
    async fn mint_deposits_derivative_token_not_base_tokenized_share() {
        let base_share = Address::random();
        let derivative = Address::random();

        let raindex = Arc::new(MockRaindex::new());
        let wrapper = Arc::new(
            MockWrapper::new()
                .with_tokenized_shares(base_share)
                .with_wrapped_token(derivative),
        );

        let transfer = create_equity_transfer(
            Arc::new(MockTokenizer::new()),
            Arc::clone(&raindex) as Arc<dyn Raindex>,
            wrapper,
        )
        .await;

        transfer
            .resume_equity_to_market_making(
                &issuer_request_id("ISS-TEST"),
                &Symbol::new("AAPL").unwrap(),
                Chain::Base,
                FractionalShares::new(float!(100)),
            )
            .await
            .unwrap();

        let deposited = raindex.last_deposited_token().expect("deposit was called");
        assert_eq!(
            deposited, derivative,
            "Deposit should use the derivative token, not the base tokenized share"
        );
        assert_ne!(
            deposited, base_share,
            "Deposit must not use the base tokenized share"
        );
    }

    #[tokio::test]
    async fn mint_transfer_fails_when_receipt_not_found() {
        let tokenizer = MockTokenizer::new()
            .with_verification_outcome(MockVerificationOutcome::ReceiptNotFound);

        let transfer = create_equity_transfer(
            Arc::new(tokenizer),
            Arc::new(MockRaindex::new()),
            Arc::new(MockWrapper::new()),
        )
        .await;

        let error = transfer
            .resume_equity_to_market_making(
                &issuer_request_id("ISS-TEST"),
                &Symbol::new("AAPL").unwrap(),
                Chain::Base,
                FractionalShares::new(float!(100.0)),
            )
            .await
            .unwrap_err();

        assert!(
            matches!(
                error,
                MintTransferError::PostReceipt(MintError::Verification(
                    MintVerificationError::ReceiptNotFound { .. }
                ))
            ),
            "Expected PostReceipt(Verification(ReceiptNotFound)) error, got: {error:?}"
        );
    }

    #[tokio::test]
    async fn mint_transfer_fails_when_transaction_reverted() {
        let tokenizer = MockTokenizer::new()
            .with_verification_outcome(MockVerificationOutcome::TransactionReverted);

        let transfer = create_equity_transfer(
            Arc::new(tokenizer),
            Arc::new(MockRaindex::new()),
            Arc::new(MockWrapper::new()),
        )
        .await;

        let error = transfer
            .resume_equity_to_market_making(
                &issuer_request_id("ISS-TEST"),
                &Symbol::new("AAPL").unwrap(),
                Chain::Base,
                FractionalShares::new(float!(100.0)),
            )
            .await
            .unwrap_err();

        assert!(
            matches!(
                error,
                MintTransferError::PostReceipt(MintError::Verification(
                    MintVerificationError::TransactionReverted { .. }
                ))
            ),
            "Expected PostReceipt(Verification(TransactionReverted)) error, got: {error:?}"
        );
    }

    #[tokio::test]
    async fn mint_transfer_fails_when_no_matching_transfer() {
        let tokenizer = MockTokenizer::new()
            .with_verification_outcome(MockVerificationOutcome::NoMatchingTransfer);

        let transfer = create_equity_transfer(
            Arc::new(tokenizer),
            Arc::new(MockRaindex::new()),
            Arc::new(MockWrapper::new()),
        )
        .await;

        let error = transfer
            .resume_equity_to_market_making(
                &issuer_request_id("ISS-TEST"),
                &Symbol::new("AAPL").unwrap(),
                Chain::Base,
                FractionalShares::new(float!(100.0)),
            )
            .await
            .unwrap_err();

        assert!(
            matches!(
                error,
                MintTransferError::PostReceipt(MintError::Verification(
                    MintVerificationError::NoMatchingTransfer { .. }
                ))
            ),
            "Expected PostReceipt(Verification(NoMatchingTransfer)) error, got: {error:?}"
        );
    }

    #[tokio::test]
    async fn mint_transfer_fails_when_insufficient_transfer_amount() {
        let tokenizer = MockTokenizer::new()
            .with_verification_outcome(MockVerificationOutcome::InsufficientTransferAmount);

        let transfer = create_equity_transfer(
            Arc::new(tokenizer),
            Arc::new(MockRaindex::new()),
            Arc::new(MockWrapper::new()),
        )
        .await;

        let error = transfer
            .resume_equity_to_market_making(
                &issuer_request_id("ISS-TEST"),
                &Symbol::new("AAPL").unwrap(),
                Chain::Base,
                FractionalShares::new(float!(100.0)),
            )
            .await
            .unwrap_err();

        assert!(
            matches!(
                error,
                MintTransferError::PostReceipt(MintError::Verification(
                    MintVerificationError::InsufficientTransferAmount { .. }
                ))
            ),
            "Expected Verification(InsufficientTransferAmount) error, got: {error:?}"
        );
    }

    #[tokio::test]
    async fn resume_mint_recovers_when_deposit_reverts() {
        let transfer = create_equity_transfer(
            Arc::new(MockTokenizer::new()),
            Arc::new(
                MockRaindex::new().with_deposit_behavior(DepositBehavior::FailExecutionReverted),
            ),
            Arc::new(MockWrapper::new()),
        )
        .await;

        let id = issuer_request_id("ISS-REVERT-RECOVERY");

        // Advance the aggregate to TokensWrapped state manually:
        // RequestMint -> MintAccepted -> TokensReceived -> WrapSubmitted
        // -> TokensWrapped
        transfer
            .mint_store
            .send(
                &id,
                TokenizedEquityMintCommand::RequestMint {
                    chain: Chain::Base,
                    issuer_request_id: id.clone(),
                    symbol: Symbol::new("AAPL").unwrap(),
                    quantity: float!(10),
                    wallet: Address::ZERO,
                },
            )
            .await
            .unwrap();
        submit_requested_mint(&transfer, &id).await;

        // Poll advances to TokensReceived
        transfer
            .mint_store
            .send(&id, TokenizedEquityMintCommand::Poll)
            .await
            .unwrap();

        let wrap_tx = TxHash::random();
        transfer
            .mint_store
            .send(
                &id,
                TokenizedEquityMintCommand::SubmitWrap {
                    wrap_tx_hash: wrap_tx,
                },
            )
            .await
            .unwrap();

        let wrapped_shares = U256::from(10_000_000_000_000_000_000u128);
        transfer
            .mint_store
            .send(
                &id,
                TokenizedEquityMintCommand::WrapTokens {
                    wrap_tx_hash: wrap_tx,
                    wrapped_shares,
                    wrap_block: 1,
                },
            )
            .await
            .unwrap();

        // Verify we're in TokensWrapped state
        let entity = transfer.mint_store.load(&id).await.unwrap().unwrap();
        assert!(
            matches!(entity, TokenizedEquityMint::TokensWrapped { .. }),
            "Expected TokensWrapped, got: {entity:?}"
        );

        // Resume should detect zero balance and advance to terminal
        transfer.resume_mint(&id).await.unwrap();

        let entity = transfer.mint_store.load(&id).await.unwrap().unwrap();
        let TokenizedEquityMint::DepositedIntoRaindex {
            vault_deposit_tx_hash,
            ..
        } = entity
        else {
            panic!("Expected DepositedIntoRaindex after revert recovery, got: {entity:?}");
        };
        assert_eq!(
            vault_deposit_tx_hash,
            TxHash::ZERO,
            "Recovered deposit must use TxHash::ZERO sentinel"
        );
    }

    #[tokio::test]
    async fn resume_mint_from_wrap_submitted_recovers_when_deposit_reverts() {
        let mock_wrapper = MockWrapper::new();
        let wrap_tx = TxHash::random();

        // Pre-seed the mock so confirm_wrap recognises the tx hash that the
        // aggregate will store in WrapSubmitted state.
        mock_wrapper.seed_submitted_amount(wrap_tx, U256::from(10_000_000_000_000_000_000u128));

        let transfer = create_equity_transfer(
            Arc::new(MockTokenizer::new()),
            Arc::new(
                MockRaindex::new().with_deposit_behavior(DepositBehavior::FailExecutionReverted),
            ),
            Arc::new(mock_wrapper),
        )
        .await;

        let id = issuer_request_id("ISS-WRAP-SUBMITTED-RECOVERY");

        // Advance aggregate to WrapSubmitted state:
        // RequestMint -> MintAccepted -> TokensReceived -> WrapSubmitted
        transfer
            .mint_store
            .send(
                &id,
                TokenizedEquityMintCommand::RequestMint {
                    chain: Chain::Base,
                    issuer_request_id: id.clone(),
                    symbol: Symbol::new("AAPL").unwrap(),
                    quantity: float!(10),
                    wallet: Address::ZERO,
                },
            )
            .await
            .unwrap();
        submit_requested_mint(&transfer, &id).await;

        transfer
            .mint_store
            .send(&id, TokenizedEquityMintCommand::Poll)
            .await
            .unwrap();

        transfer
            .mint_store
            .send(
                &id,
                TokenizedEquityMintCommand::SubmitWrap {
                    wrap_tx_hash: wrap_tx,
                },
            )
            .await
            .unwrap();

        let entity = transfer.mint_store.load(&id).await.unwrap().unwrap();
        assert!(
            matches!(entity, TokenizedEquityMint::WrapSubmitted { .. }),
            "Expected WrapSubmitted, got: {entity:?}"
        );

        // Resume from WrapSubmitted: confirms wrap, then deposit reverts,
        // recovery should advance to terminal state.
        transfer.resume_mint(&id).await.unwrap();

        let entity = transfer.mint_store.load(&id).await.unwrap().unwrap();
        let TokenizedEquityMint::DepositedIntoRaindex {
            vault_deposit_tx_hash,
            ..
        } = entity
        else {
            panic!(
                "Expected DepositedIntoRaindex after WrapSubmitted revert recovery, got: {entity:?}"
            );
        };
        assert_eq!(
            vault_deposit_tx_hash,
            TxHash::ZERO,
            "Recovered deposit must use TxHash::ZERO sentinel"
        );
    }

    #[tokio::test]
    async fn resume_mint_from_tokens_wrapped_calls_wait_for_block_with_wrap_block() {
        // Keep a typed reference so we can inspect both wait_for_block and
        // deposit call records after resume. Ordering is verified by confirming
        // both happened: wait_for_block must be called (the guard) and the
        // deposit must also succeed (proving the guard did not abort the flow).
        // Strict sequential ordering (wait_for_block strictly before deposit)
        // cannot be asserted with the current separate-mock seam — the
        // existing test `mint_transfer_fails_when_wait_for_block_fails` covers
        // the abort path, proving the guard is on the critical path.
        let mock_wrapper: Arc<MockWrapper> = Arc::new(MockWrapper::new());
        let mock_raindex: Arc<MockRaindex> = Arc::new(MockRaindex::new());
        let wrap_tx = TxHash::random();
        let wrap_block = 9999u64;

        let transfer = create_equity_transfer(
            Arc::new(MockTokenizer::new()),
            Arc::clone(&mock_raindex) as Arc<dyn Raindex>,
            Arc::clone(&mock_wrapper) as Arc<dyn Wrapper>,
        )
        .await;

        let id = issuer_request_id("ISS-TOKENS-WRAPPED-WAIT");

        // Advance aggregate to TokensWrapped state manually.
        transfer
            .mint_store
            .send(
                &id,
                TokenizedEquityMintCommand::RequestMint {
                    chain: Chain::Base,
                    issuer_request_id: id.clone(),
                    symbol: Symbol::new("AAPL").unwrap(),
                    quantity: float!(10),
                    wallet: Address::ZERO,
                },
            )
            .await
            .unwrap();
        submit_requested_mint(&transfer, &id).await;

        transfer
            .mint_store
            .send(&id, TokenizedEquityMintCommand::Poll)
            .await
            .unwrap();

        transfer
            .mint_store
            .send(
                &id,
                TokenizedEquityMintCommand::SubmitWrap {
                    wrap_tx_hash: wrap_tx,
                },
            )
            .await
            .unwrap();

        let wrapped_shares = U256::from(10_000_000_000_000_000_000u128);
        transfer
            .mint_store
            .send(
                &id,
                TokenizedEquityMintCommand::WrapTokens {
                    wrap_tx_hash: wrap_tx,
                    wrapped_shares,
                    wrap_block,
                },
            )
            .await
            .unwrap();

        let entity = transfer.mint_store.load(&id).await.unwrap().unwrap();
        assert!(
            matches!(entity, TokenizedEquityMint::TokensWrapped { .. }),
            "Expected TokensWrapped state before resume, got: {entity:?}"
        );

        transfer.resume_mint(&id).await.unwrap();

        let calls = mock_wrapper.wait_for_block_calls();

        assert_eq!(
            calls,
            vec![wrap_block],
            "wait_for_block must be called exactly once with wrap_block={wrap_block} on TokensWrapped resume"
        );

        // Confirm the deposit also ran — proving wait_for_block did not abort
        // the flow and the guard is on the critical path to deposit.
        // (strict sequential ordering is covered by the abort test
        // `resume_mint_from_tokens_wrapped_fails_when_wait_for_block_fails`)
        assert_eq!(
            mock_raindex.last_deposited_token(),
            Some(Address::ZERO),
            "submit_deposit must have been called with the derivative token after wait_for_block on TokensWrapped resume"
        );
    }

    /// Verifies that `resume_mint` from `TokensWrapped` state with `wrap_block:
    /// None` skips `wait_for_block` entirely (backward-compat path for aggregates
    /// persisted before the field was added).
    ///
    /// If the `if let Some(block) = wrap_block` guard is accidentally removed,
    /// this test catches it: `wait_for_block` would be called with a sentinel
    /// value (0 or similar) and the assertion below would fail.
    #[tokio::test]
    async fn resume_mint_from_tokens_wrapped_skips_wait_for_block_when_wrap_block_is_none() {
        let mock_wrapper: Arc<MockWrapper> = Arc::new(MockWrapper::new());
        let wrap_tx = TxHash::random();

        let (transfer, pool) = create_equity_transfer_with_pool(
            Arc::new(MockTokenizer::new()),
            Arc::new(MockRaindex::new()),
            Arc::clone(&mock_wrapper) as Arc<dyn Wrapper>,
        )
        .await;

        let id = issuer_request_id("ISS-TOKENS-WRAPPED-NO-BLOCK");

        transfer
            .mint_store
            .send(
                &id,
                TokenizedEquityMintCommand::RequestMint {
                    chain: Chain::Base,
                    issuer_request_id: id.clone(),
                    symbol: Symbol::new("AAPL").unwrap(),
                    quantity: float!(10),
                    wallet: Address::ZERO,
                },
            )
            .await
            .unwrap();
        submit_requested_mint(&transfer, &id).await;

        transfer
            .mint_store
            .send(&id, TokenizedEquityMintCommand::Poll)
            .await
            .unwrap();

        transfer
            .mint_store
            .send(
                &id,
                TokenizedEquityMintCommand::SubmitWrap {
                    wrap_tx_hash: wrap_tx,
                },
            )
            .await
            .unwrap();

        // Simulate a legacy aggregate: a `TokensWrapped` event persisted before
        // the `wrap_block` field existed. The live `WrapTokens` command now
        // requires `wrap_block`, so the only way to reach a `wrap_block: None`
        // state is to replay an old event -- seeded here directly with the
        // field omitted, exercising the `#[serde(default)]` backward-compat path.
        let wrapped_shares = U256::from(10_000_000_000_000_000_000u128);
        let legacy_payload = {
            let mut value = serde_json::to_value(TokenizedEquityMintEvent::TokensWrapped {
                wrap_tx_hash: wrap_tx,
                wrapped_shares,
                wrapped_at: Utc::now(),
                wrap_block: None,
            })
            .unwrap();
            value["TokensWrapped"]
                .as_object_mut()
                .unwrap()
                .remove("wrap_block");
            value.to_string()
        };

        let IssuerRequestId(raw_id) = &id;
        let next_sequence: i64 = sqlx::query_scalar(
            "SELECT COALESCE(MAX(sequence), -1) + 1 FROM events WHERE aggregate_id = ?",
        )
        .bind(raw_id.to_string())
        .fetch_one(&pool)
        .await
        .unwrap();

        insert_mint_event(
            &pool,
            &id,
            next_sequence,
            "TokenizedEquityMintEvent::TokensWrapped",
            &legacy_payload,
        )
        .await;

        let entity = transfer.mint_store.load(&id).await.unwrap().unwrap();
        assert!(
            matches!(entity, TokenizedEquityMint::TokensWrapped { .. }),
            "Expected TokensWrapped state before resume, got: {entity:?}"
        );

        transfer.resume_mint(&id).await.unwrap();

        assert_eq!(
            mock_wrapper.wait_for_block_calls(),
            Vec::<u64>::new(),
            "wait_for_block must NOT be called when wrap_block is None (legacy aggregate)"
        );
    }

    /// Verifies that `resume_mint` from `TokensWrapped` state propagates a
    /// `wait_for_block` failure as `MintError::Wrapper(WrapperError::Evm(..))`,
    /// and that the deposit does NOT run when `wait_for_block` fails.
    ///
    /// This proves that `wait_for_block` is on the critical path before the
    /// deposit in the `TokensWrapped` resume branch. A refactor that swaps the
    /// order (deposit first, wait_for_block second) would make this test fail
    /// while the happy-path test still passes.
    #[tokio::test]
    async fn resume_mint_from_tokens_wrapped_fails_when_wait_for_block_fails() {
        let mock_raindex: Arc<MockRaindex> = Arc::new(MockRaindex::new());
        let wrap_tx = TxHash::random();

        let transfer = create_equity_transfer(
            Arc::new(MockTokenizer::new()),
            Arc::clone(&mock_raindex) as Arc<dyn Raindex>,
            Arc::new(MockWrapper::failing_wait_for_block()),
        )
        .await;

        let id = issuer_request_id("ISS-TOKENS-WRAPPED-WAIT-FAIL");

        transfer
            .mint_store
            .send(
                &id,
                TokenizedEquityMintCommand::RequestMint {
                    chain: Chain::Base,
                    issuer_request_id: id.clone(),
                    symbol: Symbol::new("AAPL").unwrap(),
                    quantity: float!(10),
                    wallet: Address::ZERO,
                },
            )
            .await
            .unwrap();
        submit_requested_mint(&transfer, &id).await;

        transfer
            .mint_store
            .send(&id, TokenizedEquityMintCommand::Poll)
            .await
            .unwrap();

        transfer
            .mint_store
            .send(
                &id,
                TokenizedEquityMintCommand::SubmitWrap {
                    wrap_tx_hash: wrap_tx,
                },
            )
            .await
            .unwrap();

        let wrapped_shares = U256::from(10_000_000_000_000_000_000u128);
        transfer
            .mint_store
            .send(
                &id,
                TokenizedEquityMintCommand::WrapTokens {
                    wrap_tx_hash: wrap_tx,
                    wrapped_shares,
                    wrap_block: 9999u64,
                },
            )
            .await
            .unwrap();

        let entity = transfer.mint_store.load(&id).await.unwrap().unwrap();
        assert!(
            matches!(entity, TokenizedEquityMint::TokensWrapped { .. }),
            "expected TokensWrapped state before resume, got: {entity:?}"
        );

        let error = transfer
            .resume_mint(&id)
            .await
            .expect_err("wait_for_block failure must propagate from TokensWrapped resume");

        assert!(
            matches!(
                error,
                MintError::Wrapper(WrapperError::Evm(EvmError::NodeBehindRequiredBlock { .. }))
            ),
            "expected MintError::Wrapper wrapping NodeBehindRequiredBlock, got: {error:?}"
        );

        assert!(
            mock_raindex.last_deposited_token().is_none(),
            "deposit must NOT run when wait_for_block fails in TokensWrapped resume"
        );
    }

    /// Verifies that `resume_mint` from `WrapSubmitted` state propagates a
    /// `wait_for_block` failure as `MintError::Wrapper(WrapperError::Evm(..))`.
    ///
    /// The `WrapSubmitted` branch calls `wait_for_block` unconditionally (block
    /// comes from a freshly confirmed tx). A missing `?` or wrong error mapping
    /// would let the flow silently proceed to deposit against a stale node --
    /// the exact regression this PR was written to prevent.
    #[tokio::test]
    async fn resume_mint_from_wrap_submitted_fails_when_wait_for_block_fails() {
        let mock_wrapper = MockWrapper::failing_wait_for_block();
        let wrap_tx = TxHash::random();

        // Pre-seed so confirm_wrap recognises the tx hash stored in WrapSubmitted.
        mock_wrapper.seed_submitted_amount(wrap_tx, U256::from(10_000_000_000_000_000_000u128));

        let transfer = create_equity_transfer(
            Arc::new(MockTokenizer::new()),
            Arc::new(MockRaindex::new()),
            Arc::new(mock_wrapper),
        )
        .await;

        let id = issuer_request_id("ISS-WRAP-SUBMITTED-WAIT-FAIL");

        // Advance to WrapSubmitted: RequestMint -> Poll -> SubmitWrap
        transfer
            .mint_store
            .send(
                &id,
                TokenizedEquityMintCommand::RequestMint {
                    chain: Chain::Base,
                    issuer_request_id: id.clone(),
                    symbol: Symbol::new("AAPL").unwrap(),
                    quantity: float!(10),
                    wallet: Address::ZERO,
                },
            )
            .await
            .unwrap();
        submit_requested_mint(&transfer, &id).await;

        transfer
            .mint_store
            .send(&id, TokenizedEquityMintCommand::Poll)
            .await
            .unwrap();

        transfer
            .mint_store
            .send(
                &id,
                TokenizedEquityMintCommand::SubmitWrap {
                    wrap_tx_hash: wrap_tx,
                },
            )
            .await
            .unwrap();

        let entity = transfer.mint_store.load(&id).await.unwrap().unwrap();
        assert!(
            matches!(entity, TokenizedEquityMint::WrapSubmitted { .. }),
            "expected WrapSubmitted state before resume, got: {entity:?}"
        );

        let error = transfer
            .resume_mint(&id)
            .await
            .expect_err("wait_for_block failure must propagate from WrapSubmitted resume");

        assert!(
            matches!(
                error,
                MintError::Wrapper(WrapperError::Evm(EvmError::NodeBehindRequiredBlock { .. }))
            ),
            "expected MintError::Wrapper wrapping NodeBehindRequiredBlock, got: {error:?}"
        );
    }

    /// Verifies that the mint happy-path propagates a `wait_for_block` failure
    /// as `MintTransferError::PostReceipt(MintError::Wrapper(..))`.
    ///
    /// `finalize_received_mint` calls `wait_for_block(wrap_block)` unconditionally
    /// after wrapping. A missing `?` or wrong error mapping would let the flow
    /// silently deposit against a stale node.
    #[tokio::test]
    async fn mint_transfer_fails_when_wait_for_block_fails() {
        let transfer = create_equity_transfer(
            Arc::new(MockTokenizer::new()),
            Arc::new(MockRaindex::new()),
            Arc::new(MockWrapper::failing_wait_for_block()),
        )
        .await;

        let error = transfer
            .resume_equity_to_market_making(
                &issuer_request_id("ISS-WAIT-BLOCK-FAIL"),
                &Symbol::new("AAPL").unwrap(),
                Chain::Base,
                FractionalShares::new(float!(10.0)),
            )
            .await
            .expect_err("wait_for_block failure must propagate in the mint happy path");

        assert!(
            matches!(
                error,
                MintTransferError::PostReceipt(MintError::Wrapper(WrapperError::Evm(
                    EvmError::NodeBehindRequiredBlock { .. }
                )))
            ),
            "expected PostReceipt(Wrapper(NodeBehindRequiredBlock)), got: {error:?}"
        );
    }

    #[test]
    fn insufficient_balance_revert_is_recognized_when_rejected_before_broadcast() {
        let revert = || {
            EvmError::Transport(alloy::transports::RpcError::ErrorResp(
                alloy::rpc::json_rpc::ErrorPayload {
                    code: 3,
                    message: "execution reverted".into(),
                    data: Some(
                        serde_json::value::to_raw_value(&format!(
                            "{ERC20_INSUFFICIENT_BALANCE_SELECTOR}00"
                        ))
                        .unwrap(),
                    ),
                },
            ))
        };

        assert!(MintError::Raindex(RaindexError::Evm(revert())).is_insufficient_balance_revert());
        assert!(
            MintError::Raindex(RaindexError::Evm(EvmError::RejectedBeforeBroadcast {
                source: Box::new(revert()),
            }))
            .is_insufficient_balance_revert(),
            "a deposit whose gas estimate reverted is rejected before broadcast, and the \
             recovery must still recognize the insufficient-balance revert"
        );
    }

    /// Seeds a redemption at `TokensUnwrapped` from history, so a test picks
    /// the unwrapped token's provenance and the unwrap block.
    async fn seed_unwrapped_redemption(
        pool: &SqlitePool,
        id: &RedemptionAggregateId,
        underlying_token: UnwrappedProvenance,
        unwrap_block: Option<u64>,
    ) {
        let events = [
            EquityRedemptionEvent::WithdrawnFromRaindex {
                symbol: Symbol::new("AAPL").unwrap(),
                quantity: float!(1),
                token: Address::repeat_byte(0x11),
                wrapped_amount: U256::from(1_000_000_000_000_000_000_u128),
                actual_wrapped_amount: None,
                raindex_withdraw_tx: TxHash::repeat_byte(0x12),
                raindex_withdraw_block: None,
                withdrawn_at: Utc::now(),
            },
            EquityRedemptionEvent::TokensUnwrapped {
                quantity: Some(float!(1)),
                underlying_token,
                unwrap_tx_hash: TxHash::repeat_byte(0x13),
                unwrapped_amount: U256::from(1_000_000_000_000_000_000_u128),
                unwrap_block,
                unwrapped_at: Utc::now(),
            },
        ];
        for (sequence, event) in (1_i64..).zip(events) {
            sqlx::query(
                "INSERT INTO events \
                 (aggregate_type, aggregate_id, sequence, event_type, event_version, payload, \
                  metadata) \
                 VALUES ('EquityRedemption', ?1, ?2, ?3, '1', ?4, '{}')",
            )
            .bind(id.to_string())
            .bind(sequence)
            .bind(st0x_event_sorcery::DomainEvent::event_type(&event))
            .bind(serde_json::to_string(&event).unwrap())
            .execute(pool)
            .await
            .unwrap();
        }
    }

    async fn transfer_at_tokens_unwrapped(
        tokenizer: Arc<MockTokenizer>,
        wrapper: Arc<dyn Wrapper>,
        label: &str,
        underlying_token: UnwrappedProvenance,
        unwrap_block: Option<u64>,
    ) -> (CrossVenueEquityTransfer, SqlitePool, RedemptionAggregateId) {
        let (transfer, pool) =
            create_equity_transfer_with_pool(tokenizer, Arc::new(MockRaindex::new()), wrapper)
                .await;
        let id = redemption_aggregate_id(label);
        seed_unwrapped_redemption(&pool, &id, underlying_token, unwrap_block).await;
        assert!(matches!(
            transfer.redemption_store.load(&id).await.unwrap(),
            Some(EquityRedemption::TokensUnwrapped { .. })
        ));
        (transfer, pool, id)
    }

    fn attested_underlying() -> UnwrappedProvenance {
        UnwrappedProvenance::Attested {
            attested: UnwrappedToken::unchecked(Address::repeat_byte(0x21)),
        }
    }

    /// A redemption interrupted before the vault attestation existed carries
    /// an address copied from config. Preparing its send must re-attest
    /// through the wrapper and refuse when the vault's asset() is not that
    /// address, even after the config has been corrected.
    #[tokio::test]
    async fn prepare_redemption_send_refuses_a_legacy_underlying_the_vault_does_not_attest() {
        let configured = Address::random();
        let recorded = Address::random();
        let tokenizer = Arc::new(MockTokenizer::new());
        let (transfer, _pool, id) = transfer_at_tokens_unwrapped(
            tokenizer.clone(),
            Arc::new(
                MockWrapper::new()
                    .with_tokenized_shares(configured)
                    .attesting_unwrapped_token(configured),
            ),
            "legacy-underlying-mismatch",
            UnwrappedProvenance::Legacy(recorded),
            None,
        )
        .await;

        let error = transfer.prepare_redemption_send(&id).await.unwrap_err();

        assert!(
            matches!(
                &error,
                RedemptionError::ReattestLegacyUnderlying(
                    EquityRedemptionError::LegacyUnderlyingMismatch {
                        recorded: mismatched,
                        attested,
                        ..
                    }
                ) if *mismatched == recorded && *attested == configured
            ),
            "got: {error:?}"
        );
        assert!(error.is_permanent_underlying_mismatch());
        assert!(
            tokenizer.send_for_redemption_calls().is_empty(),
            "nothing may be signed"
        );
        assert!(matches!(
            transfer.redemption_store.load(&id).await.unwrap(),
            Some(EquityRedemption::TokensUnwrapped { .. })
        ));
    }

    /// With no issuer redemption wallet configured the send can never be
    /// signed, so the redemption fails at once instead of holding its guard,
    /// inflight and reservation until `transfer_timeout`.
    #[tokio::test]
    async fn a_redemption_with_no_redemption_wallet_fails_without_signing() {
        let tokenizer = Arc::new(MockTokenizer::new().with_no_redemption_wallet());
        let (transfer, _pool, id) = transfer_at_tokens_unwrapped(
            tokenizer.clone(),
            Arc::new(MockWrapper::new()),
            "no-redemption-wallet",
            attested_underlying(),
            None,
        )
        .await;

        let error = transfer.resume_redemption(&id).await.unwrap_err();

        assert!(
            matches!(
                &error,
                RedemptionError::SendFailed {
                    entity: EquityRedemption::Failed { .. }
                }
            ),
            "got: {error:?}"
        );
        assert!(matches!(
            transfer.redemption_store.load(&id).await.unwrap(),
            Some(EquityRedemption::Failed { reason: Some(reason), .. })
                if reason.contains("never signed")
        ));
        assert!(
            tokenizer.send_for_redemption_calls().is_empty(),
            "nothing may be signed"
        );
        transfer
            .resume_redemption(&id)
            .await
            .expect("a resume of the failed redemption is a clean no-op");
    }

    /// A signed send whose `PrepareSend` write fails never reached storage, so
    /// its nonce is released for the next send instead of stalling the wallet.
    #[tokio::test]
    async fn a_store_failure_while_persisting_a_signed_send_releases_its_nonce() {
        let signed = TxHash::repeat_byte(0x5E);
        let tokenizer = Arc::new(MockTokenizer::new().with_redemption_tx(signed));
        let (transfer, pool, id) = transfer_at_tokens_unwrapped(
            tokenizer.clone(),
            Arc::new(MockWrapper::new()),
            "prepare-store-failure",
            attested_underlying(),
            None,
        )
        .await;
        sqlx::query(
            "CREATE TRIGGER reject_send_prepared BEFORE INSERT ON events \
             WHEN NEW.event_type = 'EquityRedemptionEvent::SendPrepared' \
             BEGIN SELECT RAISE(ABORT, 'injected store failure'); END",
        )
        .execute(&pool)
        .await
        .unwrap();

        let error = transfer.resume_redemption(&id).await.unwrap_err();

        assert!(matches!(error, RedemptionError::Send(_)), "got: {error:?}");
        assert_eq!(tokenizer.send_for_redemption_calls().len(), 1);
        assert_eq!(tokenizer.redemption_send_discards(), vec![signed]);
        assert!(tokenizer.redemption_send_broadcasts().is_empty());
        assert!(matches!(
            transfer.redemption_store.load(&id).await.unwrap(),
            Some(EquityRedemption::TokensUnwrapped { .. })
        ));
    }

    /// Persists `original` as the redemption's signed send, signed
    /// `signed_ago` before now.
    async fn sign_send_at(
        transfer: &CrossVenueEquityTransfer,
        id: &RedemptionAggregateId,
        original: TxHash,
        signed_ago: chrono::Duration,
    ) {
        transfer
            .redemption_store
            .send(
                id,
                EquityRedemptionCommand::PrepareSendAt {
                    prepared: PreparedTransaction::for_test(original, 7),
                    redemption_wallet: Address::repeat_byte(0xAB),
                    pending_at: Utc::now() - signed_ago,
                },
            )
            .await
            .unwrap();
    }

    /// A send that has stayed unmined past the bound while the market fee rose
    /// above its own is re-signed at its nonce and persisted before its
    /// broadcast; the transfer then confirms through the replacement's hash.
    #[tokio::test]
    async fn a_stale_send_confirms_through_its_fee_replacement() {
        let original = TxHash::repeat_byte(0x51);
        let replacement = TxHash::repeat_byte(0x52);
        let tokenizer = Arc::new(
            MockTokenizer::new()
                .with_redemption_send_replacement(replacement)
                .with_mined_redemption_sends(vec![replacement]),
        );
        let (transfer, _pool, id) = transfer_at_tokens_unwrapped(
            tokenizer.clone(),
            Arc::new(MockWrapper::new()),
            "fee-replaced-send",
            attested_underlying(),
            None,
        )
        .await;
        sign_send_at(&transfer, &id, original, chrono::Duration::minutes(10)).await;

        transfer.prepare_redemption_send(&id).await.unwrap();
        let replaced = transfer.redemption_store.load(&id).await.unwrap().unwrap();
        assert_eq!(
            replaced
                .issuer_send_candidates()
                .iter()
                .map(|candidate| candidate.tx_hash())
                .collect::<Vec<_>>(),
            vec![original, replacement],
            "the replacement is persisted before any broadcast"
        );
        assert!(tokenizer.redemption_send_broadcasts().is_empty());

        transfer
            .redemption_store
            .send(&id, EquityRedemptionCommand::SendTokens)
            .await
            .unwrap();

        assert!(matches!(
            transfer.redemption_store.load(&id).await.unwrap(),
            Some(EquityRedemption::TokensSent { redemption_tx, .. }) if redemption_tx == replacement
        ));
        assert_eq!(tokenizer.redemption_send_replacements(), vec![original]);
        assert_eq!(tokenizer.redemption_send_broadcasts(), vec![replacement]);
        assert_eq!(tokenizer.redemption_send_confirms(), vec![replacement]);
    }

    /// A replaced send can still mine in place of its replacement: it is
    /// confirmed, and the replacement is never broadcast.
    #[tokio::test]
    async fn a_replaced_send_that_mines_is_confirmed_in_place_of_its_replacement() {
        let original = TxHash::repeat_byte(0x53);
        let replacement = TxHash::repeat_byte(0x54);
        let tokenizer = Arc::new(
            MockTokenizer::new()
                .with_redemption_send_replacement(replacement)
                .with_mined_redemption_sends(vec![original]),
        );
        let (transfer, _pool, id) = transfer_at_tokens_unwrapped(
            tokenizer.clone(),
            Arc::new(MockWrapper::new()),
            "replaced-send-mines",
            attested_underlying(),
            None,
        )
        .await;
        sign_send_at(&transfer, &id, original, chrono::Duration::minutes(10)).await;
        transfer
            .redemption_store
            .send(
                &id,
                EquityRedemptionCommand::ReplaceSend {
                    replacement: PreparedTransaction::for_test(replacement, 7),
                },
            )
            .await
            .unwrap();

        transfer
            .redemption_store
            .send(&id, EquityRedemptionCommand::SendTokens)
            .await
            .unwrap();

        assert!(matches!(
            transfer.redemption_store.load(&id).await.unwrap(),
            Some(EquityRedemption::TokensSent { redemption_tx, .. }) if redemption_tx == original
        ));
        assert!(tokenizer.redemption_send_broadcasts().is_empty());
        assert_eq!(tokenizer.redemption_send_confirms(), vec![original]);
    }

    /// An unresolved send to the issuer redrives on its own clock, anchored on when
    /// its newest copy was signed: every 30s at first, then slower once it has
    /// stayed unmined for hours.
    #[tokio::test]
    async fn an_issuer_send_redrive_slows_once_its_newest_copy_is_hours_old() {
        let tokenizer = Arc::new(MockTokenizer::new());
        for (label, signed_ago, expected) in [
            (
                "fresh-send-redrive",
                chrono::Duration::minutes(1),
                WITHDRAWAL_RECONCILIATION_REDRIVE_DELAY,
            ),
            (
                "old-send-redrive",
                chrono::Duration::hours(5),
                ISSUER_SEND_SLOW_REDRIVE_DELAY,
            ),
        ] {
            let (transfer, _pool, id) = transfer_at_tokens_unwrapped(
                tokenizer.clone(),
                Arc::new(MockWrapper::new()),
                label,
                attested_underlying(),
                None,
            )
            .await;
            assert_eq!(
                issuer_send_redrive(&transfer.redemption_store, &id).await,
                None
            );
            let send = TxHash::repeat_byte(0x57);
            sign_send_at(&transfer, &id, send, signed_ago).await;

            assert_eq!(
                issuer_send_redrive(&transfer.redemption_store, &id).await,
                Some(IssuerSendRedrive {
                    tx_hash: Some(send),
                    delay: expected,
                }),
                "{label}"
            );
        }
    }

    /// Once the transfer timeout has passed since the send was first signed,
    /// the operator has been paged and may cancel at its nonce, so the send is
    /// no longer fee-replaced: a replacement could outbid that cancel.
    #[tokio::test]
    async fn a_send_past_its_transfer_timeout_is_not_re_signed() {
        let original = TxHash::repeat_byte(0x58);
        let tokenizer = Arc::new(
            MockTokenizer::new()
                .with_redemption_send_replacement(TxHash::repeat_byte(0x59))
                .with_redemption_receipt_missing(),
        );
        let (transfer, _pool, id) = transfer_at_tokens_unwrapped(
            tokenizer.clone(),
            Arc::new(MockWrapper::new()),
            "send-past-timeout",
            attested_underlying(),
            None,
        )
        .await;
        sign_send_at(&transfer, &id, original, chrono::Duration::minutes(10)).await;

        prepare_and_persist_redemption_send(
            &transfer.services,
            &transfer.redemption_store,
            &id,
            Some(Duration::from_secs(5 * 60)),
        )
        .await
        .unwrap();
        assert!(tokenizer.redemption_send_replacements().is_empty());

        prepare_and_persist_redemption_send(
            &transfer.services,
            &transfer.redemption_store,
            &id,
            Some(Duration::from_secs(30 * 60)),
        )
        .await
        .unwrap();
        assert_eq!(
            tokenizer.redemption_send_replacements(),
            vec![original],
            "within the timeout the same send is re-signed"
        );
    }

    /// No replacement is signed before the bound, nor once a wallet reports
    /// the send priced at the market.
    #[tokio::test]
    async fn a_send_is_not_re_signed_before_the_bound_or_at_the_market_fee() {
        let original = TxHash::repeat_byte(0x55);
        let fresh_tokenizer = Arc::new(
            MockTokenizer::new()
                .with_redemption_send_replacement(TxHash::repeat_byte(0x56))
                .with_redemption_receipt_missing(),
        );
        let (fresh, _pool, fresh_id) = transfer_at_tokens_unwrapped(
            fresh_tokenizer.clone(),
            Arc::new(MockWrapper::new()),
            "fresh-send",
            attested_underlying(),
            None,
        )
        .await;
        sign_send_at(&fresh, &fresh_id, original, chrono::Duration::seconds(30)).await;
        fresh.prepare_redemption_send(&fresh_id).await.unwrap();
        assert!(fresh_tokenizer.redemption_send_replacements().is_empty());

        let market_tokenizer = Arc::new(MockTokenizer::new().with_redemption_receipt_missing());
        let (market, _pool, market_id) = transfer_at_tokens_unwrapped(
            market_tokenizer.clone(),
            Arc::new(MockWrapper::new()),
            "market-priced-send",
            attested_underlying(),
            None,
        )
        .await;
        sign_send_at(&market, &market_id, original, chrono::Duration::minutes(10)).await;
        market.prepare_redemption_send(&market_id).await.unwrap();
        assert_eq!(
            market_tokenizer.redemption_send_replacements(),
            vec![original]
        );
        assert_eq!(
            market
                .redemption_store
                .load(&market_id)
                .await
                .unwrap()
                .unwrap()
                .issuer_send_candidates()
                .len(),
            1,
            "a wallet that signs no replacement leaves the send as it is"
        );
    }

    /// The same legacy record whose address the vault does attest is signed
    /// with the attested token.
    #[tokio::test]
    async fn prepare_redemption_send_signs_the_attested_token_of_a_legacy_record() {
        let configured = Address::random();
        let tokenizer = Arc::new(MockTokenizer::new());
        let (transfer, _pool, id) = transfer_at_tokens_unwrapped(
            tokenizer.clone(),
            Arc::new(
                MockWrapper::new()
                    .with_tokenized_shares(configured)
                    .attesting_unwrapped_token(configured),
            ),
            "legacy-underlying-attested",
            UnwrappedProvenance::Legacy(configured),
            None,
        )
        .await;

        transfer.prepare_redemption_send(&id).await.unwrap();

        assert_eq!(
            tokenizer
                .send_for_redemption_calls()
                .into_iter()
                .map(UnwrappedToken::address)
                .collect::<Vec<_>>(),
            vec![configured]
        );
    }

    /// A load-balanced backend that has not indexed the unwrap block sees a
    /// zero balance, so preparation waits for that block before signing.
    #[tokio::test]
    async fn prepare_redemption_send_waits_for_the_unwrap_block_before_signing() {
        let tokenizer = Arc::new(MockTokenizer::new());
        let (transfer, _pool, id) = transfer_at_tokens_unwrapped(
            tokenizer.clone(),
            Arc::new(MockWrapper::new()),
            "prepare-waits-for-block",
            attested_underlying(),
            Some(1234),
        )
        .await;

        transfer.prepare_redemption_send(&id).await.unwrap();

        assert_eq!(tokenizer.wait_for_block_calls(), vec![1234]);
        assert_eq!(tokenizer.send_for_redemption_calls().len(), 1);
    }

    /// Records persisted before the unwrap block was recorded skip the wait.
    #[tokio::test]
    async fn prepare_redemption_send_skips_the_wait_without_an_unwrap_block() {
        let tokenizer = Arc::new(MockTokenizer::new());
        let (transfer, _pool, id) = transfer_at_tokens_unwrapped(
            tokenizer.clone(),
            Arc::new(MockWrapper::new()),
            "prepare-skips-wait",
            attested_underlying(),
            None,
        )
        .await;

        transfer.prepare_redemption_send(&id).await.unwrap();

        assert!(tokenizer.wait_for_block_calls().is_empty());
        assert_eq!(tokenizer.send_for_redemption_calls().len(), 1);
    }

    #[tokio::test]
    async fn prepare_redemption_send_signs_nothing_while_the_node_lags() {
        let tokenizer = Arc::new(MockTokenizer::new().failing_wait_for_block());
        let (transfer, _pool, id) = transfer_at_tokens_unwrapped(
            tokenizer.clone(),
            Arc::new(MockWrapper::new()),
            "prepare-node-lags",
            attested_underlying(),
            Some(42),
        )
        .await;

        let error = transfer.prepare_redemption_send(&id).await.unwrap_err();

        assert!(
            matches!(
                error,
                RedemptionError::Tokenizer(TokenizerError::Evm(
                    EvmError::NodeBehindRequiredBlock { .. }
                ))
            ),
            "got: {error:?}"
        );
        assert!(tokenizer.send_for_redemption_calls().is_empty());
    }

    #[tokio::test]
    async fn prepare_redemption_send_signs_once_and_persists_before_any_broadcast() {
        let tokenizer = Arc::new(MockTokenizer::new());
        let (transfer, _pool, id) = transfer_at_tokens_unwrapped(
            tokenizer.clone(),
            Arc::new(MockWrapper::new()),
            "prepare-send-once",
            attested_underlying(),
            None,
        )
        .await;

        transfer.prepare_redemption_send(&id).await.unwrap();
        let persisted = match transfer.redemption_store.load(&id).await.unwrap() {
            Some(EquityRedemption::SendPending {
                prepared_send: Some(send),
                ..
            }) => send.prepared.tx_hash(),
            state => panic!("expected a persisted signed send, got {state:?}"),
        };
        assert!(
            tokenizer.redemption_send_broadcasts().is_empty(),
            "preparation must not broadcast"
        );

        // A resume of the already prepared send must reuse it, not sign again.
        transfer.prepare_redemption_send(&id).await.unwrap();
        assert_eq!(tokenizer.send_for_redemption_calls().len(), 1);
        assert!(matches!(
            transfer.redemption_store.load(&id).await.unwrap(),
            Some(EquityRedemption::SendPending { prepared_send: Some(send), .. })
                if send.prepared.tx_hash() == persisted
        ));
        assert!(tokenizer.redemption_send_discards().is_empty());
    }

    #[tokio::test]
    async fn concurrent_redemption_send_prepares_sign_only_one_nonce() {
        let tokenizer = Arc::new(MockTokenizer::new());
        let (transfer, _pool, id) = transfer_at_tokens_unwrapped(
            tokenizer.clone(),
            Arc::new(MockWrapper::new()),
            "concurrent-send-prepare",
            attested_underlying(),
            None,
        )
        .await;
        let (first, second) = tokio::join!(
            transfer.prepare_redemption_send(&id),
            transfer.prepare_redemption_send(&id)
        );
        first.unwrap();
        second.unwrap();
        assert_eq!(tokenizer.send_for_redemption_calls().len(), 1);
        assert!(tokenizer.redemption_send_discards().is_empty());
        assert!(matches!(
            transfer.redemption_store.load(&id).await.unwrap(),
            Some(EquityRedemption::SendPending {
                prepared_send: Some(_),
                ..
            })
        ));
    }

    #[tokio::test]
    async fn a_legacy_send_pending_is_never_signed_or_resumed() {
        let tokenizer = Arc::new(MockTokenizer::new());
        let (transfer, pool, id) = transfer_at_tokens_unwrapped(
            tokenizer.clone(),
            Arc::new(MockWrapper::new()),
            "legacy-send-pending",
            attested_underlying(),
            None,
        )
        .await;
        let next_sequence: i64 =
            sqlx::query_scalar("SELECT MAX(sequence) + 1 FROM events WHERE aggregate_id = ?1")
                .bind(id.to_string())
                .fetch_one(&pool)
                .await
                .unwrap();
        sqlx::query(
            "INSERT INTO events \
             (aggregate_type, aggregate_id, sequence, event_type, event_version, payload, metadata) \
             VALUES ('EquityRedemption', ?1, ?2, 'EquityRedemptionEvent::SendPending', '1', \
             '{\"SendPending\":{\"pending_at\":\"2026-01-01T00:00:00Z\"}}', '{}')",
        )
        .bind(id.to_string())
        .bind(next_sequence)
        .execute(&pool)
        .await
        .unwrap();
        let restarted = restarted_transfer(&transfer, pool);

        assert!(matches!(
            restarted.prepare_redemption_send(&id).await,
            Err(RedemptionError::LegacyIssuerSendPending { .. })
        ));
        assert!(matches!(
            restarted.resume_redemption(&id).await,
            Err(RedemptionError::LegacyIssuerSendPending { .. })
        ));
        assert!(tokenizer.send_for_redemption_calls().is_empty());
        assert!(tokenizer.redemption_send_broadcasts().is_empty());
    }

    #[tokio::test]
    async fn a_cancelled_caller_still_persists_the_signed_send() {
        let tokenizer = Arc::new(MockTokenizer::new());
        let (transfer, _pool, id) = transfer_at_tokens_unwrapped(
            tokenizer.clone(),
            Arc::new(MockWrapper::new()),
            "prepare-send-cancelled",
            attested_underlying(),
            None,
        )
        .await;

        // A zero deadline drops the caller at its first suspension point, as a
        // disconnected API request would.
        tokio::time::timeout(Duration::ZERO, transfer.prepare_redemption_send(&id))
            .await
            .unwrap_err();

        tokio::time::timeout(Duration::from_secs(5), async {
            while !matches!(
                transfer.redemption_store.load(&id).await.unwrap(),
                Some(EquityRedemption::SendPending {
                    prepared_send: Some(_),
                    ..
                })
            ) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the signed send must reach storage after its caller is cancelled");
        assert_eq!(tokenizer.send_for_redemption_calls().len(), 1);
        assert!(tokenizer.redemption_send_discards().is_empty());
    }

    #[tokio::test]
    async fn a_prepare_send_rejected_by_a_concurrent_failure_releases_the_signed_nonce() {
        let tokenizer = Arc::new(MockTokenizer::new());
        let (transfer, _pool, id) = transfer_at_tokens_unwrapped(
            tokenizer.clone(),
            Arc::new(MockWrapper::new()),
            "prepare-send-concurrent-fail",
            attested_underlying(),
            None,
        )
        .await;
        // The operator or timeout sweep fails the transfer while it is signing.
        transfer
            .redemption_store
            .send(
                &id,
                EquityRedemptionCommand::FailTransfer {
                    reason: "operator".to_string(),
                },
            )
            .await
            .unwrap();
        let signed = PreparedTransaction::for_test(TxHash::repeat_byte(5), 9);
        let error = transfer
            .redemption_store
            .send(
                &id,
                EquityRedemptionCommand::PrepareSend {
                    prepared: signed.clone(),
                    redemption_wallet: Address::ZERO,
                },
            )
            .await
            .unwrap_err();

        release_unpersisted_redemption_send(
            &transfer.redemption_store,
            tokenizer.as_ref(),
            &id,
            signed.tx_hash(),
            &error,
        )
        .await;

        assert_eq!(tokenizer.redemption_send_discards(), vec![signed.tx_hash()]);
    }

    /// A conflict means a concurrent write (here the timeout sweep's failure)
    /// won and the `PrepareSend` commit was refused whole, so the signed send
    /// never reached storage even though the reload shows `Failed`.
    #[tokio::test]
    async fn a_prepare_send_conflict_releases_the_signed_nonce() {
        let tokenizer = Arc::new(MockTokenizer::new());
        let (transfer, _pool, id) = transfer_at_tokens_unwrapped(
            tokenizer.clone(),
            Arc::new(MockWrapper::new()),
            "prepare-send-conflict",
            attested_underlying(),
            None,
        )
        .await;
        transfer
            .redemption_store
            .send(
                &id,
                EquityRedemptionCommand::FailTransfer {
                    reason: "timeout sweep".to_string(),
                },
            )
            .await
            .unwrap();
        let signed = PreparedTransaction::for_test(TxHash::repeat_byte(7), 9);

        release_unpersisted_redemption_send(
            &transfer.redemption_store,
            tokenizer.as_ref(),
            &id,
            signed.tx_hash(),
            &AggregateError::AggregateConflict,
        )
        .await;

        assert_eq!(tokenizer.redemption_send_discards(), vec![signed.tx_hash()]);
    }

    /// A send to the issuer signed with `signer`'s key at nonce 41.
    fn send_signed_by(signer: &alloy::signers::local::PrivateKeySigner) -> PreparedTransaction {
        use alloy::consensus::{SignableTransaction as _, TxEip1559, TxEnvelope};
        use alloy::eips::eip2718::Encodable2718 as _;
        use alloy::signers::SignerSync as _;

        let unsigned = TxEip1559 {
            chain_id: 8453,
            nonce: 41,
            gas_limit: 80_000,
            max_fee_per_gas: 1_000_000_000,
            max_priority_fee_per_gas: 1_000_000,
            to: alloy::primitives::TxKind::Call(Address::repeat_byte(0x21)),
            value: U256::ZERO,
            access_list: alloy::eips::eip2930::AccessList::default(),
            input: alloy::primitives::Bytes::from_static(&[0xa9, 0x05, 0x9c, 0xbb]),
        };
        let signature = signer.sign_hash_sync(&unsigned.signature_hash()).unwrap();
        PreparedTransaction::from_raw(alloy::primitives::Bytes::from(
            TxEnvelope::from(unsigned.into_signed(signature)).encoded_2718(),
        ))
        .unwrap()
    }

    /// A send another key signed (a rotated wallet) never enters this wallet's
    /// nonce bookkeeping: it is not restored, rebroadcast or fee-replaced, only
    /// confirmed once a copy has a receipt.
    #[tokio::test]
    async fn a_send_signed_by_another_wallet_is_watched_but_never_restored() {
        let old_key = alloy::signers::local::PrivateKeySigner::random();
        let send = send_signed_by(&old_key);
        let tokenizer = Arc::new(
            MockTokenizer::new()
                .with_signing_wallet(Address::repeat_byte(0x4E))
                .with_redemption_send_replacement(TxHash::repeat_byte(0x4F))
                .with_mined_redemption_sends(vec![]),
        );
        let (transfer, _pool, id) = transfer_at_tokens_unwrapped(
            tokenizer.clone(),
            Arc::new(MockWrapper::new()),
            "send-from-rotated-key",
            attested_underlying(),
            None,
        )
        .await;
        transfer
            .redemption_store
            .send(
                &id,
                EquityRedemptionCommand::PrepareSendAt {
                    prepared: send.clone(),
                    redemption_wallet: Address::repeat_byte(0xAB),
                    pending_at: Utc::now() - chrono::Duration::minutes(10),
                },
            )
            .await
            .unwrap();

        transfer.prepare_redemption_send(&id).await.unwrap();
        let error = transfer
            .redemption_store
            .send(&id, EquityRedemptionCommand::SendTokens)
            .await
            .unwrap_err();

        assert!(
            matches!(
                &error,
                AggregateError::UserError(LifecycleError::Apply(
                    EquityRedemptionError::RedemptionSendSignedByAnotherWallet { tx_hash, signer, .. }
                )) if *tx_hash == send.tx_hash() && *signer == Some(old_key.address())
            ),
            "got: {error:?}"
        );
        assert!(RedemptionError::from(error).is_still_in_progress());
        assert!(tokenizer.redemption_send_restores().is_empty());
        assert!(tokenizer.redemption_send_broadcasts().is_empty());
        assert!(tokenizer.redemption_send_replacements().is_empty());

        let mined = Arc::new(
            MockTokenizer::new()
                .with_signing_wallet(Address::repeat_byte(0x4E))
                .with_mined_redemption_sends(vec![send.tx_hash()]),
        );
        let (watched, _pool, watched_id) = transfer_at_tokens_unwrapped(
            mined.clone(),
            Arc::new(MockWrapper::new()),
            "mined-send-from-rotated-key",
            attested_underlying(),
            None,
        )
        .await;
        watched
            .redemption_store
            .send(
                &watched_id,
                EquityRedemptionCommand::PrepareSend {
                    prepared: send.clone(),
                    redemption_wallet: Address::repeat_byte(0xAB),
                },
            )
            .await
            .unwrap();
        watched
            .redemption_store
            .send(&watched_id, EquityRedemptionCommand::SendTokens)
            .await
            .unwrap();
        assert!(matches!(
            watched.redemption_store.load(&watched_id).await.unwrap(),
            Some(EquityRedemption::TokensSent { redemption_tx, .. }) if redemption_tx == send.tx_hash()
        ));
        assert!(mined.redemption_send_restores().is_empty());
    }

    /// A redemption whose state cannot be read may hold a signed send, so it is
    /// redriven rather than left to the job's finite retry budget.
    #[tokio::test]
    async fn an_unreadable_redemption_is_redriven_as_a_possible_signed_send() {
        let (transfer, pool, id) = transfer_at_tokens_unwrapped(
            Arc::new(MockTokenizer::new()),
            Arc::new(MockWrapper::new()),
            "unreadable-redemption",
            attested_underlying(),
            None,
        )
        .await;
        sqlx::query("ALTER TABLE events RENAME TO events_unreadable")
            .execute(&pool)
            .await
            .unwrap();

        assert_eq!(
            issuer_send_redrive(&transfer.redemption_store, &id).await,
            Some(IssuerSendRedrive {
                tx_hash: None,
                delay: WITHDRAWAL_RECONCILIATION_REDRIVE_DELAY,
            })
        );
    }

    #[tokio::test]
    async fn an_ambiguous_prepare_send_failure_keeps_a_send_that_may_be_stored() {
        let tokenizer = Arc::new(MockTokenizer::new());
        let (transfer, _pool, id) = transfer_at_tokens_unwrapped(
            tokenizer.clone(),
            Arc::new(MockWrapper::new()),
            "prepare-send-ambiguous",
            attested_underlying(),
            None,
        )
        .await;
        let stored = PreparedTransaction::for_test(TxHash::repeat_byte(5), 9);
        let other = PreparedTransaction::for_test(TxHash::repeat_byte(6), 10);
        transfer
            .redemption_store
            .send(
                &id,
                EquityRedemptionCommand::PrepareSend {
                    prepared: stored.clone(),
                    redemption_wallet: Address::ZERO,
                },
            )
            .await
            .unwrap();
        // A reactor error after the commit: the store cannot say whether the
        // event was written.
        let ambiguous = || {
            AggregateError::UnexpectedError(Box::new(std::io::Error::other(
                "reactor failed after commit",
            )))
        };

        release_unpersisted_redemption_send(
            &transfer.redemption_store,
            tokenizer.as_ref(),
            &id,
            stored.tx_hash(),
            &ambiguous(),
        )
        .await;
        assert!(
            tokenizer.redemption_send_discards().is_empty(),
            "the stored send must keep its nonce"
        );

        release_unpersisted_redemption_send(
            &transfer.redemption_store,
            tokenizer.as_ref(),
            &id,
            other.tx_hash(),
            &ambiguous(),
        )
        .await;
        assert_eq!(
            tokenizer.redemption_send_discards(),
            vec![other.tx_hash()],
            "a send another attempt replaced never reached storage"
        );
    }
}

#[cfg(test)]
mod withdrawal_superseded_tests {
    use alloy::consensus::{SignableTransaction as _, TxEip1559, TxEnvelope};
    use alloy::eips::eip2718::{EIP1559_TX_TYPE_ID, EIP7702_TX_TYPE_ID, Encodable2718 as _};
    use alloy::eips::eip2930::AccessList;
    use alloy::primitives::{Address, Bytes, TxHash, TxKind, U256};
    use alloy::signers::SignerSync as _;
    use alloy::signers::local::PrivateKeySigner;

    use st0x_evm::{MinedTx, PreparedTransaction};
    use st0x_raindex::RaindexError;

    use super::{
        SignedRedemptionTx, SignedTxNotSuperseded, WithdrawalNotSuperseded,
        verify_signed_redemption_txs_superseded, verify_withdrawal_superseded,
    };
    use crate::onchain::mock::MockRaindex;

    /// The contract the signed withdrawal calls (the shared inventory).
    const INVENTORY: Address = Address::repeat_byte(0x1A);
    const NONCE: u64 = 12;
    const REQUIRED: u64 = 3;

    fn sign_withdrawal(signer: &PrivateKeySigner) -> PreparedTransaction {
        sign_at_fee(signer, 1_000_000_000)
    }

    /// The same call at the same nonce, signed with `max_fee_per_gas`: a fee
    /// replacement when the fee differs.
    fn sign_at_fee(signer: &PrivateKeySigner, max_fee_per_gas: u128) -> PreparedTransaction {
        let unsigned = TxEip1559 {
            chain_id: 8453,
            nonce: NONCE,
            gas_limit: 300_000,
            max_fee_per_gas,
            max_priority_fee_per_gas: 1_000_000,
            to: TxKind::Call(INVENTORY),
            value: U256::ZERO,
            access_list: AccessList::default(),
            input: Bytes::from_static(&[0xde, 0xad, 0xbe, 0xef]),
        };
        let signature = signer.sign_hash_sync(&unsigned.signature_hash()).unwrap();
        let envelope = TxEnvelope::from(unsigned.into_signed(signature));
        PreparedTransaction::from_raw(Bytes::from(envelope.encoded_2718())).unwrap()
    }

    /// A confirmed plain cancel from `bot` at the withdrawal's nonce: a 0-value
    /// self-transfer with no calldata that emitted no logs.
    fn plain_cancel(bot: Address) -> MinedTx {
        MinedTx {
            from: bot,
            to: Some(bot),
            nonce: NONCE,
            value: U256::ZERO,
            input: Bytes::new(),
            tx_type: EIP1559_TX_TYPE_ID,
            succeeded: true,
            emitted_logs: false,
            confirmations: REQUIRED,
        }
    }

    /// The signed withdrawal itself as mined: a call into the inventory, which
    /// logs when it succeeds.
    fn mined_withdrawal(bot: Address, succeeded: bool, confirmations: u64) -> MinedTx {
        MinedTx {
            to: Some(INVENTORY),
            input: Bytes::from_static(&[0xde, 0xad, 0xbe, 0xef]),
            succeeded,
            emitted_logs: succeeded,
            confirmations,
            ..plain_cancel(bot)
        }
    }

    struct Fixture {
        bot: Address,
        prepared: PreparedTransaction,
        cancel: TxHash,
    }

    fn fixture() -> Fixture {
        let signer = PrivateKeySigner::random();
        Fixture {
            bot: signer.address(),
            prepared: sign_withdrawal(&signer),
            cancel: TxHash::repeat_byte(0xCA),
        }
    }

    async fn verify(
        raindex: MockRaindex,
        fixture: &Fixture,
        superseding_tx: Option<TxHash>,
    ) -> Result<(), WithdrawalNotSuperseded> {
        verify_withdrawal_superseded(
            &raindex,
            &fixture.prepared,
            superseding_tx,
            fixture.bot,
            REQUIRED,
        )
        .await
    }

    /// Every signed copy of a send to the issuer shares one nonce, so a reconcile
    /// checks them all. A copy that went through outranks an older copy with no
    /// receipt, and the refusal names the send to the issuer and leaves it to recovery.
    #[tokio::test]
    async fn an_issuer_send_copy_that_went_through_refuses_the_reconcile_by_name() {
        let signer = PrivateKeySigner::random();
        let bot = signer.address();
        let original = sign_withdrawal(&signer);
        let replacement = sign_at_fee(&signer, 2_000_000_000);
        assert!(replacement.replaces(&original));
        let raindex = MockRaindex::new()
            .with_mined_tx(replacement.tx_hash(), mined_withdrawal(bot, true, REQUIRED));

        let error = verify_signed_redemption_txs_superseded(
            &raindex,
            SignedRedemptionTx::IssuerSend,
            &[&original, &replacement],
            None,
            bot,
            REQUIRED,
        )
        .await
        .unwrap_err();

        let SignedTxNotSuperseded {
            kind: SignedRedemptionTx::IssuerSend,
            refusal: WithdrawalNotSuperseded::WithdrawalWentThrough { tx },
        } = &error
        else {
            panic!("the mined copy went through: {error:?}");
        };
        assert_eq!(*tx, replacement.tx_hash());
        let message = error.to_string();
        assert!(
            message.starts_with(&format!("send {tx} to the issuer is mined and succeeded"))
                && message.contains("TokensSent")
                && !message.contains("withdrawal"),
            "the refusal must name the send to the issuer and its next step: {message}"
        );
    }

    /// The timeout page quotes the newest copy's fees, read from its bytes.
    #[test]
    fn a_signed_copy_reports_the_fees_it_was_signed_with() {
        let copy = sign_at_fee(&PrivateKeySigner::random(), 2_000_000_000);

        assert_eq!(copy.fees_per_gas(), Some((2_000_000_000, 1_000_000)));
        assert_eq!(
            PreparedTransaction::for_test(TxHash::repeat_byte(1), NONCE).fees_per_gas(),
            None
        );
    }

    /// A send whose newest copy reverted with the required confirmations used
    /// the shared nonce and moved nothing, so it proves the older copies dead
    /// too and the reconcile needs no `--superseding-tx`.
    #[tokio::test]
    async fn a_reverted_copy_proves_every_other_copy_of_the_send_dead() {
        let signer = PrivateKeySigner::random();
        let bot = signer.address();
        let original = sign_withdrawal(&signer);
        let replacement = sign_at_fee(&signer, 2_000_000_000);
        let raindex = MockRaindex::new().with_mined_tx(
            replacement.tx_hash(),
            mined_withdrawal(bot, false, REQUIRED),
        );

        verify_signed_redemption_txs_superseded(
            &raindex,
            SignedRedemptionTx::IssuerSend,
            &[&original, &replacement],
            None,
            bot,
            REQUIRED,
        )
        .await
        .expect("the reverted replacement took the shared nonce");

        let unconfirmed = MockRaindex::new().with_mined_tx(
            replacement.tx_hash(),
            mined_withdrawal(bot, false, REQUIRED - 1),
        );
        let error = verify_signed_redemption_txs_superseded(
            &unconfirmed,
            SignedRedemptionTx::IssuerSend,
            &[&original, &replacement],
            None,
            bot,
            REQUIRED,
        )
        .await
        .unwrap_err();
        assert!(
            matches!(
                error.refusal,
                WithdrawalNotSuperseded::WithdrawalRevertUnconfirmed { tx, .. }
                    if tx == replacement.tx_hash()
            ),
            "a revert short of its confirmations is reported, not a cancel: {error:?}"
        );
    }

    /// Every chain read here fails, so only a refusal that reads nothing
    /// returns `UnreadableWithdrawal`.
    #[tokio::test]
    async fn bytes_that_are_not_a_signed_envelope_are_unreadable_without_a_chain_read() {
        let fixture = Fixture {
            bot: Address::repeat_byte(0xB0),
            prepared: PreparedTransaction::for_test(TxHash::repeat_byte(0xAB), NONCE),
            cancel: TxHash::repeat_byte(0xCA),
        };
        let raindex = MockRaindex::new()
            .with_mined_tx_read_error(fixture.prepared.tx_hash())
            .with_mined_tx_read_error(fixture.cancel);

        let error = verify(raindex, &fixture, Some(fixture.cancel))
            .await
            .unwrap_err();

        let WithdrawalNotSuperseded::UnreadableWithdrawal { tx } = error else {
            panic!("bytes with no signer cannot be verified: {error:?}");
        };
        assert_eq!(tx, TxHash::repeat_byte(0xAB));
    }

    #[tokio::test]
    async fn a_mined_withdrawal_that_succeeded_went_through() {
        let fixture = fixture();
        let raindex = MockRaindex::new()
            .with_mined_tx(
                fixture.prepared.tx_hash(),
                mined_withdrawal(fixture.bot, true, REQUIRED),
            )
            .with_mined_tx(fixture.cancel, plain_cancel(fixture.bot));

        let error = verify(raindex, &fixture, Some(fixture.cancel))
            .await
            .unwrap_err();

        let WithdrawalNotSuperseded::WithdrawalWentThrough { tx } = error else {
            panic!("a successful withdrawal moved the vault: {error:?}");
        };
        assert_eq!(tx, fixture.prepared.tx_hash());
    }

    /// A reverted withdrawal used its nonce and moved nothing, so it proves
    /// itself with no replacement, but only once a reorg cannot undo it.
    #[tokio::test]
    async fn a_reverted_withdrawal_proves_itself_once_confirmed() {
        let fixture = fixture();
        let shallow = MockRaindex::new().with_mined_tx(
            fixture.prepared.tx_hash(),
            mined_withdrawal(fixture.bot, false, REQUIRED - 1),
        );
        let error = verify(shallow, &fixture, None).await.unwrap_err();
        let WithdrawalNotSuperseded::WithdrawalRevertUnconfirmed {
            tx,
            confirmations,
            required,
        } = error
        else {
            panic!("a revert below the depth can still be reorged out: {error:?}");
        };
        assert_eq!(tx, fixture.prepared.tx_hash());
        assert_eq!(confirmations, REQUIRED - 1);
        assert_eq!(required, REQUIRED);

        let deep = MockRaindex::new().with_mined_tx(
            fixture.prepared.tx_hash(),
            mined_withdrawal(fixture.bot, false, REQUIRED),
        );
        verify(deep, &fixture, None).await.unwrap();
    }

    #[tokio::test]
    async fn an_unmined_withdrawal_needs_the_tx_that_took_its_nonce() {
        let fixture = fixture();

        let error = verify(MockRaindex::new(), &fixture, None)
            .await
            .unwrap_err();

        let WithdrawalNotSuperseded::NoSupersedingTx { tx, nonce } = error else {
            panic!("a withdrawal with no receipt can still mine: {error:?}");
        };
        assert_eq!(tx, fixture.prepared.tx_hash());
        assert_eq!(nonce, NONCE);
    }

    #[tokio::test]
    async fn naming_the_withdrawal_as_its_own_superseding_tx_is_refused() {
        let fixture = fixture();

        let error = verify(
            MockRaindex::new(),
            &fixture,
            Some(fixture.prepared.tx_hash()),
        )
        .await
        .unwrap_err();

        let WithdrawalNotSuperseded::SupersedingTxIsTheWithdrawal { tx } = error else {
            panic!("an unmined withdrawal cannot take its own nonce: {error:?}");
        };
        assert_eq!(tx, fixture.prepared.tx_hash());
    }

    #[tokio::test]
    async fn an_unmined_superseding_tx_is_not_proof() {
        let fixture = fixture();

        let error = verify(MockRaindex::new(), &fixture, Some(fixture.cancel))
            .await
            .unwrap_err();

        let WithdrawalNotSuperseded::SupersedingTxNotMined { superseding } = error else {
            panic!("a pending cancel has not taken the nonce yet: {error:?}");
        };
        assert_eq!(superseding, fixture.cancel);
    }

    #[tokio::test]
    async fn a_failed_withdrawal_read_names_the_withdrawal() {
        let fixture = fixture();
        let raindex = MockRaindex::new()
            .with_mined_tx_read_error(fixture.prepared.tx_hash())
            .with_mined_tx(fixture.cancel, plain_cancel(fixture.bot));

        let error = verify(raindex, &fixture, Some(fixture.cancel))
            .await
            .unwrap_err();

        let WithdrawalNotSuperseded::Read { tx, source } = error else {
            panic!("an unread withdrawal proves nothing: {error:?}");
        };
        assert_eq!(tx, fixture.prepared.tx_hash());
        assert!(
            matches!(*source, RaindexError::RpcTransport(_)),
            "got: {source:?}"
        );
    }

    #[tokio::test]
    async fn a_failed_superseding_read_names_the_superseding_tx() {
        let fixture = fixture();
        let raindex = MockRaindex::new().with_mined_tx_read_error(fixture.cancel);

        let error = verify(raindex, &fixture, Some(fixture.cancel))
            .await
            .unwrap_err();

        let WithdrawalNotSuperseded::Read { tx, source } = error else {
            panic!("an unread superseding tx proves nothing: {error:?}");
        };
        assert_eq!(tx, fixture.cancel);
        assert!(
            matches!(*source, RaindexError::RpcTransport(_)),
            "got: {source:?}"
        );
    }

    #[tokio::test]
    async fn a_confirmed_cancel_from_the_bot_wallet_at_the_nonce_proves_it() {
        let fixture = fixture();
        let shallow = MockRaindex::new().with_mined_tx(
            fixture.cancel,
            MinedTx {
                confirmations: REQUIRED - 1,
                ..plain_cancel(fixture.bot)
            },
        );
        let error = verify(shallow, &fixture, Some(fixture.cancel))
            .await
            .unwrap_err();
        let WithdrawalNotSuperseded::SupersedingTxUnconfirmed {
            superseding,
            confirmations,
            required,
        } = error
        else {
            panic!("a cancel below the depth can still be reorged out: {error:?}");
        };
        assert_eq!(superseding, fixture.cancel);
        assert_eq!(confirmations, REQUIRED - 1);
        assert_eq!(required, REQUIRED);

        let deep = MockRaindex::new().with_mined_tx(fixture.cancel, plain_cancel(fixture.bot));
        verify(deep, &fixture, Some(fixture.cancel)).await.unwrap();
    }

    #[tokio::test]
    async fn a_tx_at_another_nonce_is_not_proof() {
        let fixture = fixture();
        let raindex = MockRaindex::new().with_mined_tx(
            fixture.cancel,
            MinedTx {
                nonce: NONCE + 1,
                ..plain_cancel(fixture.bot)
            },
        );

        let error = verify(raindex, &fixture, Some(fixture.cancel))
            .await
            .unwrap_err();

        let WithdrawalNotSuperseded::SupersedingTxAtAnotherNonce {
            superseding,
            superseding_nonce,
            nonce,
        } = error
        else {
            panic!("only a tx at the withdrawal's nonce stops it mining: {error:?}");
        };
        assert_eq!(superseding, fixture.cancel);
        assert_eq!(superseding_nonce, NONCE + 1);
        assert_eq!(nonce, NONCE);
    }

    #[tokio::test]
    async fn a_tx_from_another_sender_is_not_proof() {
        let fixture = fixture();
        let other = Address::repeat_byte(0x0E);
        let raindex = MockRaindex::new().with_mined_tx(fixture.cancel, plain_cancel(other));

        let error = verify(raindex, &fixture, Some(fixture.cancel))
            .await
            .unwrap_err();

        let WithdrawalNotSuperseded::SupersedingTxFromAnotherSender {
            superseding,
            from,
            bot_wallet,
        } = error
        else {
            panic!("nonces are per sender, so another sender's tx takes nothing: {error:?}");
        };
        assert_eq!(superseding, fixture.cancel);
        assert_eq!(from, other);
        assert_eq!(bot_wallet, fixture.bot);
    }

    /// Only a plain cancel provably moved nothing. Each shape below may have
    /// withdrawn the vault when it succeeded, and moved nothing when it
    /// reverted.
    #[tokio::test]
    async fn a_successful_superseding_tx_must_be_a_plain_cancel() {
        let fixture = fixture();
        let cancel = plain_cancel(fixture.bot);
        let shapes = [
            (
                "fee bumped copy of the withdrawal",
                mined_withdrawal(fixture.bot, true, REQUIRED),
            ),
            (
                "call through another contract",
                MinedTx {
                    to: Some(Address::repeat_byte(0x3C)),
                    input: Bytes::from_static(&[0xde, 0xad, 0xbe, 0xef]),
                    ..cancel.clone()
                },
            ),
            (
                "contract creation",
                MinedTx {
                    to: None,
                    input: Bytes::from_static(&[0x60, 0x00]),
                    ..cancel.clone()
                },
            ),
            (
                "self-transfer with value",
                MinedTx {
                    value: U256::from(1),
                    ..cancel.clone()
                },
            ),
            (
                "self-transfer with calldata",
                MinedTx {
                    input: Bytes::from_static(&[0x00]),
                    ..cancel.clone()
                },
            ),
            (
                "EIP-7702 self-transfer",
                MinedTx {
                    tx_type: EIP7702_TX_TYPE_ID,
                    ..cancel.clone()
                },
            ),
            (
                // What code run at the wallet through an EIP-7702 delegation
                // leaves when it moves anything.
                "self-transfer that emitted logs",
                MinedTx {
                    emitted_logs: true,
                    ..cancel
                },
            ),
        ];

        for (shape, mined) in shapes {
            let raindex = MockRaindex::new().with_mined_tx(fixture.cancel, mined.clone());
            let error = verify(raindex, &fixture, Some(fixture.cancel))
                .await
                .unwrap_err();
            let WithdrawalNotSuperseded::SupersedingTxNotAPlainCancel {
                superseding,
                bot_wallet,
            } = error
            else {
                panic!("a successful {shape} may have withdrawn the vault: {error:?}");
            };
            assert_eq!(superseding, fixture.cancel, "{shape}");
            assert_eq!(bot_wallet, fixture.bot, "{shape}");

            let reverted = MockRaindex::new().with_mined_tx(
                fixture.cancel,
                MinedTx {
                    succeeded: false,
                    emitted_logs: false,
                    ..mined
                },
            );
            verify(reverted, &fixture, Some(fixture.cancel))
                .await
                .unwrap_or_else(|error| panic!("a reverted {shape} moved nothing: {error:?}"));
        }
    }

    #[tokio::test]
    async fn a_withdrawal_signed_by_another_wallet_is_refused() {
        let fixture = fixture();
        let rotated_key = fixture.bot;
        let configured = Fixture {
            bot: Address::repeat_byte(0xB0),
            ..fixture
        };

        let error = verify(MockRaindex::new(), &configured, Some(configured.cancel))
            .await
            .unwrap_err();

        let WithdrawalNotSuperseded::WithdrawalSignedByAnotherWallet {
            tx,
            signer,
            bot_wallet,
        } = error
        else {
            panic!("the configured wallet cannot take a nonce of a rotated key: {error:?}");
        };
        assert_eq!(tx, configured.prepared.tx_hash());
        assert_eq!(signer, rotated_key);
        assert_eq!(bot_wallet, configured.bot);
    }
}

#[cfg(test)]
mod withdrawal_replacement_tests {
    use alloy::consensus::{SignableTransaction as _, TxEip1559, TxEnvelope};
    use alloy::eips::eip2718::{EIP1559_TX_TYPE_ID, Encodable2718 as _};
    use alloy::eips::eip2930::AccessList;
    use alloy::primitives::{Address, B256, Bytes, TxHash, TxKind, U256};
    use alloy::signers::SignerSync as _;
    use alloy::signers::local::PrivateKeySigner;
    use alloy::sol_types::SolCall as _;

    use st0x_evm::{MinedTx, PreparedTransaction};

    use super::{
        ReplacementNotAdoptable, WithdrawalNotSuperseded, verify_hash_only_withdrawal_not_through,
        verify_withdrawal_replacement, withdraw4Call,
    };
    use crate::onchain::mock::MockRaindex;

    const INVENTORY: Address = Address::repeat_byte(0x1A);
    const TOKEN: Address = Address::repeat_byte(0x70);
    const VAULT: B256 = B256::repeat_byte(0x7A);
    const NONCE: u64 = 12;
    const REQUIRED: u64 = 3;
    const REPLACEMENT: TxHash = TxHash::repeat_byte(0x5E);

    fn withdraw4(token: Address, vault_id: B256) -> Bytes {
        Bytes::from(
            withdraw4Call {
                token,
                vaultId: vault_id,
                targetAmount: B256::repeat_byte(0x01),
                tasks: Vec::new(),
            }
            .abi_encode(),
        )
    }

    fn sign(signer: &PrivateKeySigner, to: TxKind, input: Bytes) -> PreparedTransaction {
        let unsigned = TxEip1559 {
            chain_id: 8453,
            nonce: NONCE,
            gas_limit: 300_000,
            max_fee_per_gas: 1_000_000_000,
            max_priority_fee_per_gas: 1_000_000,
            to,
            value: U256::ZERO,
            access_list: AccessList::default(),
            input,
        };
        let signature = signer.sign_hash_sync(&unsigned.signature_hash()).unwrap();
        let envelope = TxEnvelope::from(unsigned.into_signed(signature));
        PreparedTransaction::from_raw(Bytes::from(envelope.encoded_2718())).unwrap()
    }

    fn speed_up(bot: Address, input: Bytes) -> MinedTx {
        MinedTx {
            from: bot,
            to: Some(INVENTORY),
            nonce: NONCE,
            value: U256::ZERO,
            input,
            tx_type: EIP1559_TX_TYPE_ID,
            succeeded: true,
            emitted_logs: true,
            confirmations: REQUIRED,
        }
    }

    /// The replacement read fails, so only a refusal that reads nothing
    /// returns the withdrawal side refusal.
    async fn refusal(prepared: &PreparedTransaction, bot: Address) -> ReplacementNotAdoptable {
        let raindex = MockRaindex::new().with_mined_tx_read_error(REPLACEMENT);
        verify_withdrawal_replacement(&raindex, prepared, REPLACEMENT, bot, REQUIRED)
            .await
            .unwrap_err()
    }

    #[tokio::test]
    async fn a_withdrawal_that_is_not_a_signed_withdraw4_is_unreadable_without_a_chain_read() {
        let signer = PrivateKeySigner::random();
        let bot = signer.address();
        let unsigned = PreparedTransaction::for_test(TxHash::repeat_byte(0xAB), NONCE);
        let creation = sign(&signer, TxKind::Create, withdraw4(TOKEN, VAULT));
        let not_withdraw4 = sign(
            &signer,
            TxKind::Call(INVENTORY),
            Bytes::from_static(&[0xde, 0xad, 0xbe, 0xef]),
        );

        for (case, prepared) in [
            ("unsigned bytes", unsigned),
            ("contract creation", creation),
            ("calldata that is not withdraw4", not_withdraw4),
        ] {
            let error = refusal(&prepared, bot).await;
            assert!(
                matches!(
                    error,
                    ReplacementNotAdoptable::UnreadableWithdrawal { tx } if tx == prepared.tx_hash()
                ),
                "{case}: {error:?}"
            );
        }
    }

    /// Nonces are per sender, so a withdrawal signed by a rotated out key
    /// cannot be replaced by the current wallet's tx at the same nonce.
    #[tokio::test]
    async fn a_withdrawal_signed_by_another_wallet_is_refused_without_a_chain_read() {
        let rotated_out = PrivateKeySigner::random();
        let bot = Address::repeat_byte(0xB0);
        let prepared = sign(
            &rotated_out,
            TxKind::Call(INVENTORY),
            withdraw4(TOKEN, VAULT),
        );

        let error = refusal(&prepared, bot).await;

        let ReplacementNotAdoptable::WithdrawalSignedByAnotherWallet {
            tx,
            signer,
            bot_wallet,
        } = error
        else {
            panic!("a rotated key's withdrawal must be refused: {error:?}");
        };
        assert_eq!(tx, prepared.tx_hash());
        assert_eq!(signer, rotated_out.address());
        assert_eq!(bot_wallet, bot);
    }

    /// Token and vault are each checked: a replacement differing in either one
    /// is refused, naming what it withdrew.
    #[tokio::test]
    async fn a_replacement_from_another_token_or_vault_is_refused() {
        let signer = PrivateKeySigner::random();
        let bot = signer.address();
        let prepared = sign(&signer, TxKind::Call(INVENTORY), withdraw4(TOKEN, VAULT));

        for (case, other_token, other_vault) in [
            ("another token", Address::repeat_byte(0x0E), VAULT),
            ("another vault", TOKEN, B256::repeat_byte(0x0E)),
        ] {
            let raindex = MockRaindex::new().with_mined_tx(
                REPLACEMENT,
                speed_up(bot, withdraw4(other_token, other_vault)),
            );
            let error =
                verify_withdrawal_replacement(&raindex, &prepared, REPLACEMENT, bot, REQUIRED)
                    .await
                    .unwrap_err();

            let ReplacementNotAdoptable::ReplacementWithdrawsAnotherVault {
                token,
                vault_id,
                expected_token,
                expected_vault_id,
                ..
            } = error
            else {
                panic!("{case} must be refused: {error:?}");
            };
            assert_eq!((token, vault_id), (other_token, other_vault), "{case}");
            assert_eq!(
                (expected_token, expected_vault_id),
                (TOKEN, VAULT),
                "{case}"
            );
        }
    }

    #[tokio::test]
    async fn a_hash_only_withdrawal_is_refused_only_once_it_went_through() {
        let bot = Address::repeat_byte(0xB0);
        let withdrawal = TxHash::repeat_byte(0x77);
        let succeeded = MockRaindex::new().with_mined_tx(withdrawal, speed_up(bot, Bytes::new()));
        let error = verify_hash_only_withdrawal_not_through(&succeeded, withdrawal)
            .await
            .unwrap_err();
        assert!(
            matches!(error, WithdrawalNotSuperseded::WithdrawalWentThrough { tx } if tx == withdrawal),
            "{error:?}"
        );

        let reverted = MockRaindex::new().with_mined_tx(
            withdrawal,
            MinedTx {
                succeeded: false,
                ..speed_up(bot, Bytes::new())
            },
        );
        verify_hash_only_withdrawal_not_through(&reverted, withdrawal)
            .await
            .unwrap();
        verify_hash_only_withdrawal_not_through(&MockRaindex::new(), withdrawal)
            .await
            .unwrap();

        let unreadable = MockRaindex::new().with_mined_tx_read_error(withdrawal);
        let error = verify_hash_only_withdrawal_not_through(&unreadable, withdrawal)
            .await
            .unwrap_err();
        assert!(
            matches!(error, WithdrawalNotSuperseded::Read { tx, .. } if tx == withdrawal),
            "a failed read is not proof either way: {error:?}"
        );
    }

    /// A receipt that pays the bot wallet none of the token means the
    /// replacement withdrew nothing; one whose transfer logs cannot be read
    /// says so, with the cause, instead of claiming nothing was paid.
    #[tokio::test]
    async fn a_replacement_receipt_is_refused_with_its_real_cause() {
        let signer = PrivateKeySigner::random();
        let bot = signer.address();
        let prepared = sign(&signer, TxKind::Call(INVENTORY), withdraw4(TOKEN, VAULT));
        let mined =
            MockRaindex::new().with_mined_tx(REPLACEMENT, speed_up(bot, withdraw4(TOKEN, VAULT)));

        let paid_elsewhere = mined.with_transfer_receipt(
            REPLACEMENT,
            TOKEN,
            Address::repeat_byte(0x0E),
            U256::from(7),
        );
        let error =
            verify_withdrawal_replacement(&paid_elsewhere, &prepared, REPLACEMENT, bot, REQUIRED)
                .await
                .unwrap_err();
        assert!(
            matches!(
                error,
                ReplacementNotAdoptable::ReplacementWithdrewNothing { .. }
            ),
            "{error:?}"
        );

        let undecodable = MockRaindex::new()
            .with_mined_tx(REPLACEMENT, speed_up(bot, withdraw4(TOKEN, VAULT)))
            .with_undecodable_transfer_receipt(REPLACEMENT, TOKEN);
        let error =
            verify_withdrawal_replacement(&undecodable, &prepared, REPLACEMENT, bot, REQUIRED)
                .await
                .unwrap_err();
        let ReplacementNotAdoptable::ReplacementReceiptUnreadable {
            replacement,
            source,
        } = error
        else {
            panic!("an unreadable receipt must name its cause: {error:?}");
        };
        assert_eq!(replacement, REPLACEMENT);
        assert!(
            matches!(
                *source,
                crate::equity_redemption::EquityRedemptionError::RaindexWithdrawTransferDecodeFailed { .. }
            ),
            "{source:?}"
        );
    }
}

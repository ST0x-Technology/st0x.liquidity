//! Shared transaction submission with nonce-race serialization and
//! replacement-fee escalation.
//!
//! Both wallet backends (`turnkey`, `local-signer`) duplicate the same
//! send path, so the recovery logic lives here and both call
//! [`send_with_recovery`]. It hardens submission against three failure
//! modes seen in production:
//!
//! 1. **Concurrent nonce race.** [`ResettableNonceManager`] can hand the
//!    same nonce to two concurrent sends during cache invalidation (see
//!    its doc comment). The whole assign-nonce -> sign -> broadcast
//!    sequence is therefore serialized behind a per-wallet mutex, so only
//!    one transaction is built at a given nonce at a time.
//! 2. **Stuck under-gassed pending transaction.** When a transaction is
//!    already pending in the mempool at the target nonce, a fresh send at
//!    the same nonce is rejected with "replacement transaction
//!    underpriced". We resubmit at that exact nonce with an escalating
//!    fee bump until the node accepts the replacement, or a bounded
//!    number of attempts is exhausted (then a hard error -- we never bump
//!    the fee without limit).
//! 3. **External nonce advance.** Another signer on the same wallet
//!    (e.g. a CLI, or a second service) consumes the nonce we were about
//!    to use, and the node rejects the send with "nonce too low". We
//!    re-target a bounded number of times using the highest of a
//!    monotonic lower bound learned from this recovery's own prior
//!    rejections, the node's reported next nonce, and a fresh
//!    `pending`-block read (see [`crate::nonce`] and
//!    `retry_after_nonce_too_low`'s doc comment for why no single source
//!    is trustworthy alone). If a re-targeted retry itself comes back
//!    "replacement transaction underpriced", the incumbent transaction at
//!    that nonce could be either an external co-signer's pending
//!    transaction or our own under-gassed one. The loop first asks
//!    [`crate::inflight_nonces::InFlightNonces`] whether this wallet's own
//!    bookkeeping recognizes the occupied nonce as one of its own unconfirmed
//!    sends: direct proof of that settles it immediately without any RPC
//!    read, surfacing the rejection rather than risk advancing past a
//!    transaction that could never mine if abandoned. Only when the tracker
//!    has no record for this nonce
//!    ([`crate::inflight_nonces::NonceOwnership::Unknown`] -- see that
//!    type's doc and the module doc below for exactly when this applies)
//!    does the loop fall back to comparing the rejected nonce with
//!    `latest_nonce()`, our own oldest unconfirmed nonce: strictly above
//!    it, the incumbent is treated as not ours, so the rejection is
//!    treated as continued external contention and consumes another
//!    attempt of the same retry budget. Exactly at it, the incumbent is
//!    plausibly our own stuck transaction, and advancing past it would
//!    open a permanent gap behind it that can never mine, so the rejection
//!    is surfaced immediately instead. Strictly below it, the rejected
//!    nonce has already been mined according to this follow-up read -- the
//!    contention resolved while we were retrying, or a different
//!    load-balanced node is simply ahead -- so the loop raises its floor
//!    to that value and continues within the retry budget rather than
//!    surfacing a rejection that no longer reflects reality.
//!
//! **Known limitations.** [`crate::inflight_nonces::InFlightNonces`] records
//! every successful submission from this module (base send, nonce-too-low
//! retry, and fee-bumped resubmit), and `Wallet::await_receipt` releases the
//! entry once the wait resolves definitively (mined, or proven dropped).
//! `resubmit_with_bumped_fee` and `retry_after_nonce_too_low` both consult it
//! before falling back to any inference: a nonce this process recorded is
//! *proven* this wallet's own, replacing what used to be only the
//! `latest_nonce()` inference below. But the tracker can only ever prove a
//! nonce is *ours* -- it cannot soundly prove a nonce belongs to a co-signer,
//! because an unrecorded nonce is equally explained by this wallet's own
//! pre-restart transaction that this process never touched (this tracker
//! starts empty on every restart). Proving the foreign case would require
//! persisting ownership evidence across restarts, which this module does not
//! do; it is tracked as a follow-up. Until then, every collision with an
//! unrecorded nonce -- whether it turns out to be a co-signer's transaction
//! or this wallet's own untracked one -- falls back to the same
//! `latest_nonce()` heuristic this module has always used:
//!
//! - RESOLVED: when the tracker *does* recognize the occupied nonce as this
//!   wallet's own, that is now direct proof instead of an inference from a
//!   single point-in-time `latest_nonce()` read through the same
//!   load-balanced RPC as every other nonce source -- a read that a lagging
//!   node could previously under-report, misclassifying the wallet's own
//!   stuck transaction as external contention. That comparison no longer
//!   runs at all once the tracker has direct proof.
//! - STILL OPEN: whenever the tracker has no record for the occupied nonce,
//!   this module has no proof either way -- it cannot tell "a co-signer's
//!   transaction" from "our own pre-restart transaction this process never
//!   recorded" -- and falls back to the same `latest_nonce()` heuristic
//!   above, with the same lagging-node risk it always had. No proactive
//!   refusal of a foreign nonce happens anywhere in this module: closing
//!   this gap requires durable ownership persistence, not more bookkeeping
//!   in this in-memory tracker.
//! - STILL OPEN, unrelated to ownership: selecting `max(hint,
//!   pending_nonce)` (mode 3), and raising the floor past a nonce an
//!   underpriced rejection proved occupied, can leave the cached nonce
//!   above the account's actual next free nonce. Once that happens, the
//!   cache is corrected only by a later RPC rejection that invalidates it;
//!   until then, further sends from this wallet are accepted into the
//!   node's queued (non-executable) pool behind the nonce gap rather than
//!   mining. A targeted self-heal for this -- clearing the cache from
//!   within `Wallet::await_receipt` when a send stalls -- was considered
//!   and deliberately not shipped here. Every signal available to drive it
//!   (whether a wait ended in a dropped/timed-out classification, and a
//!   node-local pending-block count) is a heuristic that can misfire on a
//!   lagging or load-balanced RPC read, and a false positive would clear
//!   the cache while a legitimate transaction is still live, causing the
//!   wallet to replace it. [`crate::inflight_nonces::InFlightNonces`]
//!   tracks nonce *ownership*, not cache *correctness*, so it does not
//!   close this gap by itself.
//!
//! The fee bump is **best-effort**: it escalates over the *current*
//! network estimate, not the incumbent stuck transaction's fee (which we
//! cannot read reliably on load-balanced RPC providers). This reliably
//! clears the dominant failure mode -- a concurrent send that collided on
//! the nonce moments earlier at a near-identical fee. If the stuck
//! transaction was instead priced well above the current estimate (e.g.
//! submitted during a gas spike that has since subsided), the bounded
//! escalation may not reach its fee; the loop then surfaces
//! [`EvmError::ReplacementUnderpriced`] so the caller fails loud rather
//! than overpaying without limit. Reading the incumbent's fee to
//! guarantee replacement is a possible future enhancement.
//!
//! Nonce-too-low recovery (mode 3) holds the per-wallet send lock for its
//! whole bounded worst case -- the sum of the retry backoffs plus the
//! per-attempt RPC round-trips, about 3s of sleep with the current
//! constants -- rather than releasing it between attempts. Other sends
//! from the same wallet queue behind it for that window. This is a
//! deliberate trade-off: releasing the lock mid-recovery would reopen the
//! concurrent-nonce-assignment race the lock exists to close.
//!
//! [`crate::inflight_nonces::InFlightNonces`] bounds its own memory growth
//! by age, not by wait outcome: see its module doc for why a timed-out wait
//! cannot be treated as license to release an entry.

use alloy::eips::eip1559::Eip1559Estimation;
use alloy::primitives::{Address, Bytes, TxHash};
use alloy::providers::Provider;
use alloy::providers::fillers::NonceManager;
use alloy::rpc::types::{TransactionReceipt, TransactionRequest};
use async_trait::async_trait;
use futures::lock::Mutex;
use std::cmp::Ordering;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{error, info, warn};

use crate::inflight_nonces::{InFlightNonces, NonceOwnership};
use crate::nonce::ResettableNonceManager;
use crate::{EvmError, NextNonceHint};

/// Number of escalating-fee resubmit attempts after the first
/// "replacement transaction underpriced" rejection. Each attempt bumps
/// the fee further; exhausting them is a hard error.
const MAX_REPLACEMENT_RESUBMITS: u32 = 4;

/// Number of re-sends after a "nonce too low" rejection. One immediate
/// retry is too few: it fails identically whenever the competing
/// transaction is still only in the mempool.
const MAX_NONCE_TOO_LOW_RETRIES: u32 = 3;

/// Backoff before a "nonce too low" re-send that cannot be re-targeted
/// from the node's reported nonce. Attempt `n` waits
/// `NONCE_RETRY_BACKOFF * n`, giving time either for the load-balanced RPC
/// to route the next read to a node that has seen the competing
/// transaction, or for that transaction to mine (see
/// `TxSubmitter::pending_nonce`'s doc comment for why a node's own pool is
/// not something later reads can otherwise count on converging into).
const NONCE_RETRY_BACKOFF: Duration = Duration::from_millis(500);

/// Fee bump per resubmit attempt, in percent added on top of the current
/// network estimate. Attempt `n` uses `FEE_BUMP_PCT_PER_ATTEMPT * n`
/// (15%, 30%, 45%, 60%). The first bump already clears the node's required
/// replacement margin (geth's `txpool.pricebump` default is 10%) against a
/// transaction priced near the current estimate. Escalating widens the
/// margin for a stuck transaction priced somewhat above the estimate, but
/// is bounded -- see the module-level note on best-effort replacement.
const FEE_BUMP_PCT_PER_ATTEMPT: u64 = 15;

/// Scale a fee value up by `pct` percent with checked arithmetic, rounding
/// up.
///
/// Like alloy's `Eip1559Estimation::scale_by_pct` but (a) propagates a hard
/// [`EvmError::ReplacementFeeOverflow`] instead of risking a silent `u128`
/// wrap -- fee fields are financial values that go straight into a signed
/// transaction -- and (b) rounds up (`div_ceil`) so the bump never
/// truncates below the intended margin. Overflow is only reachable on an
/// absurd RPC fee estimate (~`u128::MAX / 160`), never on real gwei-range
/// fees.
fn bump_fee(value: u128, pct: u64) -> Result<u128, EvmError> {
    value
        .checked_mul(u128::from(100 + pct))
        .map(|scaled| scaled.div_ceil(100))
        .ok_or(EvmError::ReplacementFeeOverflow)
}

/// The provider operations [`send_with_recovery`] depends on, narrowed to
/// a small surface so the recovery state machine can be unit-tested with
/// a scripted mock instead of a live RPC and signing fillers.
#[async_trait]
pub(crate) trait TxSubmitter: Send + Sync {
    /// Fill, sign, and broadcast `tx`, returning its hash. Pre-set fields
    /// (nonce, fees) are respected; absent ones are filled by the
    /// provider's fillers.
    async fn submit(&self, tx: TransactionRequest) -> Result<TxHash, EvmError>;

    /// Assigns the next nonce for `address` via `nonce_manager`, mirroring
    /// what the production filler chain does automatically -- surfaced here
    /// so the caller can pin it onto the request explicitly and record it,
    /// instead of leaving it invisible inside the filler.
    async fn assign_nonce(
        &self,
        nonce_manager: &ResettableNonceManager,
        address: Address,
    ) -> Result<u64, EvmError>;

    /// The lowest unconfirmed nonce (the `latest`-block transaction
    /// count) -- the nonce a stuck pending transaction occupies.
    async fn latest_nonce(&self, address: Address) -> Result<u64, EvmError>;

    /// The next nonce accounting for the queried node's own transaction
    /// pool (the `pending`-block transaction count) -- includes
    /// transactions broadcast but not yet mined. In go-ethereum's
    /// `internal/ethapi/api.go`, `GetTransactionCount` short-circuits the
    /// `pending` block tag straight into `Backend.GetPoolNonce`, which reads
    /// the transaction pool directly without any block or state resolution,
    /// so the read genuinely reflects unmined transactions that node knows
    /// about.
    ///
    /// On Base (an OP Stack chain) that pool is node-local and effectively
    /// private -- there is no gossiped public mempool. `eth_sendRawTransaction`
    /// is forwarded upstream to the sequencer, and op-geth's
    /// `EthAPIBackend.SendTx` retains a copy only in the receiving node's own
    /// local pool "for local RPC usage"; it is never broadcast to peers. A
    /// load-balanced RPC provider can therefore route this read to a node
    /// that never received a competing signer's transaction at all, and
    /// unlike an L1 gossip network, more retries do not reliably converge
    /// past that -- the read can stay blind to it until the transaction
    /// mines. Against a co-signer on the same wallet this read may
    /// contribute nothing; the node-reported hint (see
    /// `EvmError::next_nonce_hint`) and the recovery loop's own monotonic
    /// floor are what actually carry recovery in that case.
    async fn pending_nonce(&self, address: Address) -> Result<u64, EvmError>;

    /// Current EIP-1559 fee estimate from the network.
    async fn estimate_fees(&self) -> Result<Eip1559Estimation, EvmError>;
}

#[async_trait]
impl<P: Provider> TxSubmitter for P {
    async fn submit(&self, tx: TransactionRequest) -> Result<TxHash, EvmError> {
        let pending = self.send_transaction(tx).await?;
        Ok(*pending.tx_hash())
    }

    async fn assign_nonce(
        &self,
        nonce_manager: &ResettableNonceManager,
        address: Address,
    ) -> Result<u64, EvmError> {
        Ok(nonce_manager.get_next_nonce(self, address).await?)
    }

    async fn latest_nonce(&self, address: Address) -> Result<u64, EvmError> {
        Ok(self.get_transaction_count(address).await?)
    }

    async fn pending_nonce(&self, address: Address) -> Result<u64, EvmError> {
        Ok(self.get_transaction_count(address).pending().await?)
    }

    async fn estimate_fees(&self) -> Result<Eip1559Estimation, EvmError> {
        Ok(self.estimate_eip1559_fees().await?)
    }
}

/// Submit `calldata` to `contract`, recovering from nonce races and a
/// stuck pending transaction at the target nonce.
///
/// Returns the submitted transaction hash without waiting for a receipt;
/// callers await confirmation separately. `submitter` is the wallet's
/// signing provider, `nonce_manager` its [`ResettableNonceManager`],
/// `in_flight` its [`InFlightNonces`] record of its own unconfirmed sends,
/// and `send_lock` serializes all sends from this wallet.
///
/// The nonce is assigned explicitly via [`TxSubmitter::assign_nonce`] and
/// pinned onto the request before it is submitted, rather than left for the
/// signing provider's own filler to assign invisibly: this is what makes the
/// nonce a successful send used knowable to the caller, both for recording
/// it in `in_flight` and for handing it directly to
/// [`resubmit_with_bumped_fee`] instead of that function re-deriving it.
/// Pinning it this way relies on alloy's `NonceFiller::status` returning
/// `FillerControlFlow::Finished` once a request already carries a nonce
/// (alloy-provider 1.6.3, `src/fillers/nonce.rs`): the production filler
/// chain (`local::SignerProvider` / `turnkey::SignerProvider`) still installs
/// this same `nonce_manager` as a `NonceFiller`, and that short-circuit is
/// what stops it from drawing a second, overwriting nonce from the cache
/// once one is already pinned.
pub(crate) async fn send_with_recovery<Submitter>(
    submitter: &Submitter,
    nonce_manager: &ResettableNonceManager,
    in_flight: &InFlightNonces,
    send_lock: &Mutex<()>,
    address: Address,
    contract: Address,
    calldata: Bytes,
    note: &str,
) -> Result<TxHash, EvmError>
where
    Submitter: TxSubmitter,
{
    // Hold the lock across the whole send so two concurrent callers
    // cannot build transactions at the same nonce.
    let _guard = send_lock.lock().await;

    let nonce = submitter.assign_nonce(nonce_manager, address).await?;

    let tx = TransactionRequest::default()
        .to(contract)
        .input(calldata.clone().into())
        .nonce(nonce);

    let error = match submitter.submit(tx).await {
        Ok(tx_hash) => {
            info!(target: "wallet", %tx_hash, note, nonce, "Transaction submitted");
            in_flight.record(address, nonce, tx_hash);
            return Ok(tx_hash);
        }
        Err(error) => error,
    };

    if error.is_nonce_too_low() {
        return retry_after_nonce_too_low(
            submitter,
            nonce_manager,
            in_flight,
            address,
            contract,
            calldata,
            note,
            error,
        )
        .await;
    }

    if error.is_replacement_underpriced() {
        warn!(target: "wallet", %contract, note, %error, "Transaction underpriced \
            -- a pending transaction occupies the nonce; resubmitting with bumped fee");
        return resubmit_with_bumped_fee(
            submitter,
            nonce_manager,
            in_flight,
            address,
            contract,
            calldata,
            note,
            nonce,
        )
        .await;
    }

    warn!(target: "wallet", %contract, note, %error, "Transaction rejected by RPC \
        -- invalidating nonce cache to prevent nonce gap");
    nonce_manager.invalidate();
    Err(error)
}

/// This recovery loop's own monotonic lower bound, accumulated from its own
/// prior rejected retries and, before the loop starts, from the triggering
/// rejection's own signals (see `retry_after_nonce_too_low`'s doc comment).
/// A distinct type from [`NextNonceHint`] -- the node-reported hint -- so
/// the two cannot be silently transposed at a `NonceSource::resolve` call
/// site: both are otherwise plain `u64` values flowing through the same
/// loop, and swapping them would seed a retry from a stale, already-
/// superseded floor where an authoritative node hint was expected, or vice
/// versa.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct NonceFloor(u64);

/// Raise a monotonic nonce floor to at least `candidate_floor`. Never
/// lowers an existing floor: a later rejection can only push the floor
/// forward, since an earlier attempt in the same recovery may have already
/// proven a higher nonce unusable.
fn raise_nonce_floor(current_floor: Option<NonceFloor>, candidate_floor: u64) -> NonceFloor {
    let Some(NonceFloor(floor)) = current_floor else {
        return NonceFloor(candidate_floor);
    };

    NonceFloor(floor.max(candidate_floor))
}

/// This wallet's own oldest unconfirmed nonce, read via `TxSubmitter::latest_nonce`
/// right after an underpriced retry during nonce-too-low recovery. A distinct
/// type from the plain `u64` nonce the retry was submitted at, so the three-way
/// comparison between the two in `retry_after_nonce_too_low` cannot silently
/// take some other `u64` in scope (`sourced_nonce`, a floor, ...) in its place:
/// this value only ever arrives through this one RPC read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct WalletOldestUnconfirmedNonce(u64);

/// Outcome of [`resolve_underpriced_via_latest_nonce`]'s cold-start
/// comparison, so its caller does not have to re-derive whether to stop
/// this recovery or continue it from a bare `Result`/`Ordering` value.
enum LatestNonceFallback {
    /// The rejected nonce is plausibly this wallet's own stuck transaction
    /// (or the cross-check needed to tell otherwise itself failed). The
    /// caller must invalidate the nonce cache and surface the rejection
    /// that triggered this fallback.
    Stop,
    /// The rejected nonce is not this wallet's own stuck transaction;
    /// continue within budget with this recovery's floor raised to at
    /// least the given value.
    RaiseFloorTo(u64),
}

/// Cold-start fallback for an underpriced rejection during nonce-too-low
/// recovery, used only when [`crate::inflight_nonces::InFlightNonces`] has
/// no opinion yet for this address ([`NonceOwnership::Unknown`]) -- see
/// `retry_after_nonce_too_low`'s doc comment for the full three-way
/// comparison this performs against `latest_nonce()`, this wallet's own
/// oldest unconfirmed nonce, and the module doc's "Known limitation" for why
/// this comparison cannot prove ownership either way.
async fn resolve_underpriced_via_latest_nonce<Submitter>(
    submitter: &Submitter,
    address: Address,
    contract: Address,
    note: &str,
    attempt: u32,
    next_nonce: u64,
    retry_error: &EvmError,
) -> LatestNonceFallback
where
    Submitter: TxSubmitter,
{
    let latest_nonce_value = match submitter.latest_nonce(address).await {
        Ok(latest_nonce_value) => WalletOldestUnconfirmedNonce(latest_nonce_value),
        Err(latest_nonce_error) => {
            warn!(
                target: "wallet", %contract, note, attempt, %retry_error,
                %latest_nonce_error,
                "Nonce-too-low retry rejected as underpriced, and confirming \
                 whether the occupied nonce is our own stuck transaction \
                 failed too -- surfacing the retry rejection rather than \
                 risk advancing past our own transaction"
            );
            return LatestNonceFallback::Stop;
        }
    };
    // Extracted here, right at the comparison: keeping it wrapped up to
    // this point means no other `u64` in scope can be passed to
    // `submitter.latest_nonce`'s result slot by mistake.
    let WalletOldestUnconfirmedNonce(latest_nonce_value) = latest_nonce_value;

    match next_nonce.cmp(&latest_nonce_value) {
        Ordering::Equal => {
            warn!(
                target: "wallet", %contract, note, attempt, next_nonce,
                latest_nonce_value, %retry_error,
                "Nonce-too-low retry rejected as underpriced at the wallet's \
                 own oldest unconfirmed nonce -- treated as plausibly our \
                 own stuck transaction rather than external contention \
                 (ownership cannot be proven from this read alone, see the \
                 module doc's Known limitation); stopping here rather than \
                 opening a permanent gap past it"
            );
            LatestNonceFallback::Stop
        }
        Ordering::Less => {
            warn!(
                target: "wallet", %contract, note, attempt, next_nonce,
                latest_nonce_value, %retry_error,
                "Nonce-too-low retry rejected as underpriced below our own \
                 oldest unconfirmed nonce -- the rejected nonce has already \
                 been mined per this follow-up read, so the contention \
                 resolved while this recovery was retrying; raising the \
                 floor and retrying within budget instead of surfacing a \
                 rejection the chain has moved past"
            );
            LatestNonceFallback::RaiseFloorTo(latest_nonce_value)
        }
        Ordering::Greater => {
            warn!(
                target: "wallet", %contract, note, attempt, next_nonce,
                latest_nonce_value, %retry_error,
                "Nonce-too-low retry rejected as underpriced above our own \
                 oldest unconfirmed nonce -- the nonce is occupied but its \
                 owner is unknown, so it is not fee-bumped; treating as \
                 continued external nonce contention and retrying within \
                 budget"
            );
            LatestNonceFallback::RaiseFloorTo(next_nonce.saturating_add(1))
        }
    }
}

/// The mutually exclusive combinations of nonce sources available on a
/// single `retry_after_nonce_too_low` attempt, named so the source-selection
/// arms in that loop read as a closed set rather than an ad-hoc tuple match.
/// See `retry_after_nonce_too_low`'s doc comment for why each source can
/// independently regress and why none is trusted alone.
enum NonceSource {
    /// Both the node-reported hint and a fresh pending-block read succeeded.
    HintAndPending { hint: u64, pending_nonce: u64 },
    /// Only the node-reported hint is available; the pending-block read
    /// failed.
    HintOnly { hint: u64, pending_error: EvmError },
    /// Only a fresh pending-block read is available; the rejection that
    /// triggered this recovery carried no parseable next-nonce hint.
    PendingOnly { pending_nonce: u64 },
    /// Neither source is available on this attempt, but an earlier rejected
    /// retry in this same recovery already proved a floor safe to fall
    /// back on.
    FloorFallback { floor: u64, pending_error: EvmError },
    /// Neither source is available, and no floor has been established
    /// either -- this attempt must be skipped without submitting a retry.
    NoSource { pending_error: EvmError },
}

impl NonceSource {
    /// Resolves the sources available on one attempt into the single
    /// matching variant. `nonce_floor` is only consulted when both `hint`
    /// and `pending` are unavailable, mirroring the original tuple match.
    /// `hint` and `nonce_floor` are deliberately distinct types
    /// ([`NextNonceHint`] and [`NonceFloor`]) even though both wrap a plain
    /// `u64`: swapping the two arguments at this call site would seed a
    /// retry from the wrong source without a compile error if they shared
    /// a type.
    fn resolve(
        hint: Option<NextNonceHint>,
        pending: Result<u64, EvmError>,
        nonce_floor: Option<NonceFloor>,
    ) -> Self {
        match (hint, pending) {
            (Some(NextNonceHint(hint)), Ok(pending_nonce)) => Self::HintAndPending {
                hint,
                pending_nonce,
            },
            (Some(NextNonceHint(hint)), Err(pending_error)) => Self::HintOnly {
                hint,
                pending_error,
            },
            (None, Ok(pending_nonce)) => Self::PendingOnly { pending_nonce },
            (None, Err(pending_error)) => match nonce_floor {
                Some(NonceFloor(floor)) => Self::FloorFallback {
                    floor,
                    pending_error,
                },
                None => Self::NoSource { pending_error },
            },
        }
    }
}

/// Re-send after a "nonce too low" rejection, re-targeting the nonce on
/// every attempt until the node accepts the transaction or the attempt
/// budget is exhausted.
///
/// `error` is the rejection that triggered the recovery. Each attempt
/// computes a target nonce from three sources: a monotonic lower bound
/// this loop has learned from its own prior rejections (see below), the
/// node's reported next nonce (`error.next_nonce_hint()` -- geth's
/// head-state nonce, a floor but not aware of unmined transactions, see its
/// doc comment), and a fresh `pending_nonce` read (aware of the queried
/// node's own transaction pool, but that pool is node-local and, on Base,
/// effectively private -- see `TxSubmitter::pending_nonce`'s doc comment for
/// why a load-balanced RPC can route this read to a node that never saw a
/// competing signer's transaction at all).
/// The target is the highest of whichever sources are available; when
/// neither the hint nor the pending read is available at all, the loop
/// falls back to the monotonic floor if one has already been established
/// by an earlier rejection in this same recovery (the only trustworthy
/// source left once both external reads are unavailable). Only when no
/// floor exists either is the cache invalidated and this attempt skipped
/// without submitting a retry: a cold-cache resend would re-fetch
/// `latest`, the exact stale value that caused the incident, so the loop
/// instead moves on to the next attempt, which re-reads `pending_nonce`
/// and may succeed. A skipped attempt still consumes one unit of the
/// retry budget without sending anything.
///
/// The monotonic lower bound exists because a hint or a pending read can
/// each independently regress: a retry rejected nonce-too-low without a
/// parseable hint falls back to a fresh `pending_nonce` read alone, and
/// that read can land on a node that has not yet seen the very rejection
/// that just proved a nonce too low. It is seeded before attempt 1 even
/// runs, from the higher of the triggering rejection's own hint and the
/// nonce cache's current value: the base send that triggered this recovery
/// filled its nonce through `ResettableNonceManager::get_next_nonce` before
/// being rejected, so the cache already holds one past the nonce the node
/// just rejected -- a proven lower bound even on a hint-less rejection
/// (an RPC proxy or non-geth client rephrasing it, an expected outcome per
/// `EvmError::next_nonce_hint`'s doc comment), where the hint alone would
/// leave attempt 1 with nothing stopping it from re-targeting at or below
/// the nonce just proven too low. Once a retry submitted at nonce `N` is
/// itself rejected -- either nonce-too-low (proving `N` too low) or
/// underpriced (proving `N` occupied by another pending transaction) --
/// `N` is proven unusable for the rest of this recovery; the loop
/// remembers `N + 1` as a floor and never seeds a later attempt below it,
/// regardless of what a subsequent hint or pending read reports.
///
/// A retry rejected as "replacement transaction underpriced" proves only
/// that the target nonce is occupied by a pending transaction, not who it
/// belongs to: the incumbent may be another signer sharing this wallet, or
/// it may equally be our own under-gassed pending transaction (see the
/// module doc). It is therefore NOT fee-bumped. The loop first asks
/// `in_flight.ownership(address, next_nonce)` for direct proof:
/// [`NonceOwnership::Ours`] means this wallet's own bookkeeping recognizes
/// the nonce as one of its own unconfirmed sends, so the rejection is
/// surfaced immediately rather than advancing past a transaction that could
/// never mine if abandoned (the same outcome the old `Ordering::Equal`
/// coincidence-based check produced, now resting on proof instead of
/// inference). [`NonceOwnership::Unknown`] -- this process has no record of
/// the nonce at all, whether because it belongs to a co-signer or because it
/// is this wallet's own pre-restart transaction the tracker never saw (see
/// [`crate::inflight_nonces`]'s module doc) -- falls back to the pre-existing
/// heuristic, verbatim: a `latest_nonce()` read of this wallet's own oldest
/// unconfirmed nonce, compared against the rejected nonce three ways (see the
/// module doc's "Known limitation" for why this comparison rests on a
/// single, uncross-checked read and cannot prove ownership either way):
/// strictly above it, the incumbent is concluded not ours, and the rejection
/// is treated as continued external contention; exactly at it, the rejected
/// nonce is the wallet's oldest unconfirmed nonce and is therefore treated as
/// plausibly our own, so the rejection is surfaced immediately instead of
/// advancing, to avoid opening a permanent gap behind a transaction that can
/// never mine; strictly below it, the rejected nonce has already been mined
/// per this follow-up read -- the contention resolved while this loop was
/// retrying -- so the floor is raised to that value and the loop continues
/// within the retry budget instead of surfacing a rejection the chain has
/// since moved past.
async fn retry_after_nonce_too_low<Submitter>(
    submitter: &Submitter,
    nonce_manager: &ResettableNonceManager,
    in_flight: &InFlightNonces,
    address: Address,
    contract: Address,
    calldata: Bytes,
    note: &str,
    mut error: EvmError,
) -> Result<TxHash, EvmError>
where
    Submitter: TxSubmitter,
{
    // Monotonic lower bound learned from this loop's own rejected
    // retries; see the doc comment above. Seeded from the higher of the
    // triggering rejection's own hint and the nonce cache's current value
    // (one past the nonce the base send just had rejected): either signal
    // alone is already proof that no send should go below it, so a stale
    // `pending_nonce` read on attempt 1 must not be allowed to undercut
    // whichever is higher.
    let cached_next_nonce = nonce_manager.peek_next_nonce(address).await;
    let mut nonce_floor: Option<NonceFloor> = match (error.next_nonce_hint(), cached_next_nonce) {
        (Some(NextNonceHint(hint)), Some(cached)) => Some(NonceFloor(hint.max(cached))),
        (Some(NextNonceHint(hint)), None) => Some(NonceFloor(hint)),
        (None, Some(cached)) => Some(NonceFloor(cached)),
        (None, None) => None,
    };

    for attempt in 1..=MAX_NONCE_TOO_LOW_RETRIES {
        let hint = error.next_nonce_hint();

        // A node-reported hint is authoritative enough to retry
        // immediately on the first attempt: it is the node's own floor
        // for the next accepted nonce, so retrying at it costs nothing
        // even if it is stale. A pending-only target carries no such
        // guarantee -- the pending read is node-local (see
        // `TxSubmitter::pending_nonce`), so a load-balanced RPC can route
        // it to a node that never saw the competing transaction at all --
        // so every hint-less case, and every attempt after the first,
        // backs off first, giving time either for a different
        // load-balanced route to land on a node that did see it, or for
        // the competing transaction to mine, before the nonce is
        // (re-)selected below.
        if attempt > 1 || hint.is_none() {
            sleep(NONCE_RETRY_BACKOFF * attempt).await;
        }

        let pending = submitter.pending_nonce(address).await;

        let sourced_nonce = match NonceSource::resolve(hint, pending, nonce_floor) {
            NonceSource::HintAndPending {
                hint,
                pending_nonce,
            } => {
                let target = hint.max(pending_nonce);
                warn!(
                    target: "wallet", %contract, note, attempt, hint, pending_nonce, target,
                    "Nonce too low -- seeding the cache with the higher of the node's \
                     reported next nonce and the pending-block count (external nonce \
                     change detected)"
                );
                target
            }
            NonceSource::HintOnly {
                hint,
                pending_error,
            } => {
                warn!(
                    target: "wallet", %contract, note, attempt, hint, %pending_error,
                    "Nonce too low -- pending-nonce fetch failed, seeding the cache \
                     with the node-reported next nonce alone"
                );
                hint
            }
            NonceSource::PendingOnly { pending_nonce } => {
                warn!(
                    target: "wallet", %contract, note, attempt, pending_nonce,
                    "Nonce too low without a reported next nonce -- seeding the cache \
                     with the pending-block transaction count"
                );
                pending_nonce
            }
            NonceSource::FloorFallback {
                floor,
                pending_error,
            } => {
                warn!(
                    target: "wallet", %contract, note, attempt, floor, %pending_error,
                    "Nonce too low without a reported next nonce, and the \
                     pending-nonce fetch failed too -- falling back to the floor \
                     already proven safe by an earlier rejected retry in this \
                     same recovery, since no external source can be trusted here"
                );
                floor
            }
            NonceSource::NoSource { pending_error } => {
                warn!(
                    target: "wallet", %contract, note, attempt, %pending_error,
                    "Nonce too low without a reported next nonce, and the \
                     pending-nonce fetch failed too -- invalidating the cache \
                     and consuming this attempt without submitting a retry, \
                     because a retry right now would cold-fetch the stale \
                     latest nonce that caused the incident"
                );
                nonce_manager.invalidate();
                continue;
            }
        };

        let next_nonce = match nonce_floor {
            Some(NonceFloor(floor)) if floor > sourced_nonce => {
                warn!(
                    target: "wallet", %contract, note, attempt, floor, sourced_nonce,
                    "Nonce too low -- raising the sourced target to the floor already \
                     proven safe by an earlier rejected retry in this same recovery"
                );
                floor
            }
            _ => sourced_nonce,
        };

        nonce_manager.set_next_nonce(address, next_nonce).await;

        let retry_tx = TransactionRequest::default()
            .to(contract)
            .input(calldata.clone().into());

        match submitter.submit(retry_tx).await {
            Ok(tx_hash) => {
                info!(target: "wallet", %tx_hash, note, attempt, "Transaction submitted");
                in_flight.record(address, next_nonce, tx_hash);
                return Ok(tx_hash);
            }
            Err(retry_error) if retry_error.is_nonce_too_low() => {
                nonce_floor = Some(raise_nonce_floor(nonce_floor, next_nonce.saturating_add(1)));
                error = retry_error;
            }
            Err(retry_error) if retry_error.is_replacement_underpriced() => {
                match in_flight.ownership(address, next_nonce) {
                    NonceOwnership::Ours => {
                        warn!(
                            target: "wallet", %contract, note, attempt, next_nonce, %retry_error,
                            "Nonce-too-low retry rejected as underpriced at a nonce this \
                             wallet's own bookkeeping proves is one of its own unconfirmed \
                             sends -- stopping here rather than opening a permanent gap past \
                             a transaction that could never mine if abandoned"
                        );
                        nonce_manager.invalidate();
                        return Err(retry_error);
                    }
                    NonceOwnership::Unknown => {
                        match resolve_underpriced_via_latest_nonce(
                            submitter,
                            address,
                            contract,
                            note,
                            attempt,
                            next_nonce,
                            &retry_error,
                        )
                        .await
                        {
                            LatestNonceFallback::Stop => {
                                nonce_manager.invalidate();
                                return Err(retry_error);
                            }
                            LatestNonceFallback::RaiseFloorTo(floor_candidate) => {
                                nonce_floor = Some(raise_nonce_floor(nonce_floor, floor_candidate));
                                error = retry_error;
                            }
                        }
                    }
                }
            }
            Err(retry_error) => {
                error!(
                    target: "wallet", %retry_error, note, attempt,
                    "Nonce-too-low retry failed for another reason -- invalidating nonce cache"
                );
                nonce_manager.invalidate();
                return Err(retry_error);
            }
        }
    }

    error!(
        target: "wallet", %contract, note, %error,
        attempts = MAX_NONCE_TOO_LOW_RETRIES,
        "Exhausted nonce-too-low recovery attempts under continued external \
         nonce contention -- invalidating nonce cache and surfacing the final \
         rejection"
    );
    nonce_manager.invalidate();

    Err(error)
}

/// Resubmit a transaction at `nonce` with an escalating fee until the node
/// accepts the replacement or the attempt budget is exhausted.
///
/// Only called right after a base send at `nonce` comes back "replacement
/// transaction underpriced" (see `send_with_recovery`), which now hands this
/// function the exact nonce that send was rejected at -- rather than this
/// function re-deriving it via `latest_nonce()`, which equals the rejected
/// nonce only when the wallet has no other unmined transaction sitting below
/// it; when it does, `latest` undercounts and `latest_nonce()` would pin the
/// wrong nonce.
///
/// This is a best-effort replacement, regardless of whether
/// `in_flight.ownership(address, nonce)` answers [`NonceOwnership::Ours`] or
/// [`NonceOwnership::Unknown`]: this tracker can only ever prove a nonce is
/// this wallet's own, never that it belongs to a co-signer (see
/// [`crate::inflight_nonces`]'s module doc), so there is no sound basis here
/// to refuse fee-bumping an unrecorded nonce on the theory that it must be a
/// co-signer's live transaction -- it could equally be this wallet's own
/// pre-restart send the tracker never saw. Both cases therefore run the same
/// escalation loop below, exactly as this function always has; closing this
/// gap for good requires durable ownership persistence across restarts,
/// tracked as a follow-up, not more bookkeeping here.
///
/// Each escalation attempt re-estimates the network fee, bumps it, and pins
/// the nonce explicitly so the filler targets that transaction for
/// replacement. Does not touch the nonce cache on success -- seeding
/// `nonce + 1` here could move the cache backwards over another send from
/// this wallet still in flight; `nonce_manager` is only used to
/// invalidate on a hard failure. On success, records `(address, nonce,
/// tx_hash)` in `in_flight`, same as every other successful submission in
/// this module.
///
/// A "nonce too low" rejection during escalation means the incumbent at
/// this nonce confirmed while this loop was still racing it with a higher
/// fee -- but not necessarily our own replacement. This is deliberately
/// NOT routed into the nonce-too-low recovery in
/// [`retry_after_nonce_too_low`]: that recovery resubmits `calldata` at a
/// newly retargeted nonce, and if the incumbent that just confirmed was
/// this same logical operation (this function's own earlier, now-mined
/// attempt at the original nonce), that resubmit would execute it a
/// second time. A replacement pinned at a single, fixed nonce can only
/// ever have one of {original, bumped} mine, so no such double-execution
/// risk exists in this function's own retry -- but retargeting to a
/// *different* nonce loses that guarantee. The rejection is therefore
/// surfaced as a hard error instead, so the caller can re-check on-chain
/// state before deciding whether the operation still needs to be sent at
/// all.
async fn resubmit_with_bumped_fee<Submitter>(
    submitter: &Submitter,
    nonce_manager: &ResettableNonceManager,
    in_flight: &InFlightNonces,
    address: Address,
    contract: Address,
    calldata: Bytes,
    note: &str,
    nonce: u64,
) -> Result<TxHash, EvmError>
where
    Submitter: TxSubmitter,
{
    for attempt in 1..=MAX_REPLACEMENT_RESUBMITS {
        let estimate = submitter
            .estimate_fees()
            .await
            .inspect_err(|_| nonce_manager.invalidate())?;
        let pct = FEE_BUMP_PCT_PER_ATTEMPT * u64::from(attempt);
        let max_fee_per_gas = bump_fee(estimate.max_fee_per_gas, pct)?;
        let max_priority_fee_per_gas = bump_fee(estimate.max_priority_fee_per_gas, pct)?;

        let tx = TransactionRequest::default()
            .to(contract)
            .input(calldata.clone().into())
            .nonce(nonce)
            .max_fee_per_gas(max_fee_per_gas)
            .max_priority_fee_per_gas(max_priority_fee_per_gas);

        info!(
            target: "wallet", %contract, note, attempt, nonce,
            max_fee_per_gas, max_priority_fee_per_gas,
            "Resubmitting stuck transaction with bumped fee"
        );

        let error = match submitter.submit(tx).await {
            Ok(tx_hash) => {
                info!(target: "wallet", %tx_hash, note, attempt, "Replacement transaction accepted");
                in_flight.record(address, nonce, tx_hash);
                return Ok(tx_hash);
            }
            Err(error) => error,
        };

        if error.is_replacement_underpriced() {
            warn!(target: "wallet", %contract, note, attempt, "Replacement still \
                underpriced; escalating fee");
            continue;
        }

        if error.is_nonce_too_low() {
            warn!(
                target: "wallet", %contract, note, attempt, %error,
                "Nonce too low while escalating a fee-bumped replacement -- the \
                 incumbent at this nonce confirmed while we were racing it with a \
                 higher fee; see this function's doc comment for why this is not \
                 routed into nonce-too-low recovery (that would risk resending this \
                 same calldata a second time if the incumbent was our own earlier \
                 attempt). Invalidating the cache and surfacing a terminal error \
                 rather than gambling on a resend"
            );
            nonce_manager.invalidate();
            return Err(error);
        }

        // Any other error is non-recoverable. Invalidate the cache and
        // surface it.
        nonce_manager.invalidate();
        return Err(error);
    }

    nonce_manager.invalidate();
    Err(EvmError::ReplacementUnderpriced {
        attempts: MAX_REPLACEMENT_RESUBMITS,
    })
}

/// Releases `tx_hash`'s [`InFlightNonces`] entry once a wait for its receipt
/// has resolved, if the outcome is decisive; called by both wallet backends'
/// `await_receipt` after `wait_for_receipt` completes, mirroring how
/// `send_with_recovery` above is this module's single shared entry point for
/// the send side.
///
/// - `Ok(_)` (mined) and a dropped-transaction error (proven gone from both
///   the receipt lookup and the mempool past the grace period --
///   [`EvmError::is_transaction_dropped`]) are both definitive: the nonce
///   this transaction occupied is no longer occupied by it, so its entry is
///   released.
/// - Every other outcome -- most importantly `Err(EvmError::ReceiptTimeout
///   { .. })` -- proves nothing: the transaction may still mine after the
///   wait gives up. Releasing here would let a later "replacement
///   transaction underpriced" rejection at this same nonce be misclassified
///   as external and bumped/skipped past, replacing the wallet's own still-
///   live transaction. The entry is therefore deliberately left in place
///   until a later confirm or drop proves otherwise.
///
/// Delegates the decisive/inconclusive classification to
/// [`EvmError::is_transaction_dropped`] rather than matching
/// `EvmError::TransactionDropped` here directly: that method already
/// exhaustively matches every `EvmError` variant, so a future variant that
/// should also be treated as decisive is a single, compiler-checked place to
/// update, instead of a second copy of the classification that could
/// silently drift from it.
pub(crate) async fn release_in_flight_after_wait(
    in_flight: &InFlightNonces,
    send_lock: &Mutex<()>,
    address: Address,
    tx_hash: TxHash,
    result: &Result<TransactionReceipt, EvmError>,
) {
    let is_decisive = match result {
        Ok(_) => true,
        Err(error) => error.is_transaction_dropped(),
    };

    if is_decisive {
        // Brief critical section: only updates this wallet's own ownership
        // bookkeeping, never the nonce cache itself. Safe to serialize
        // behind the send lock even though the wait above ran unlocked for
        // up to the confirmation timeout, because this acquisition is
        // instantaneous.
        let _guard = send_lock.lock().await;
        in_flight.release(address, tx_hash);
    }
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;
    use std::collections::{HashSet, VecDeque};
    use std::sync::Mutex as StdMutex;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use alloy::consensus::{Receipt, ReceiptEnvelope, ReceiptWithBloom};
    use alloy::node_bindings::Anvil;
    use alloy::primitives::{Bloom, Bytes, TxHash, U256, address};
    use alloy::providers::ProviderBuilder;
    use alloy::providers::ext::AnvilApi;
    use alloy::providers::fillers::NonceManager;
    use alloy::providers::mock::Asserter;
    use alloy::rpc::json_rpc::ErrorPayload;
    use alloy::transports::RpcError;
    use tokio::time::Instant;

    use super::*;

    const CONTRACT: Address = address!("00000000000000000000000000000000000000c1");
    const WALLET: Address = address!("00000000000000000000000000000000000000a9");
    const STUCK_NONCE: u64 = 7;

    fn rpc_error(message: impl Into<Cow<'static, str>>) -> EvmError {
        EvmError::Transport(RpcError::ErrorResp(ErrorPayload {
            code: -32000,
            message: message.into(),
            data: None,
        }))
    }

    fn underpriced() -> EvmError {
        rpc_error("replacement transaction underpriced")
    }

    fn nonce_too_low() -> EvmError {
        rpc_error("nonce too low")
    }

    /// The verbatim message format go-ethereum's
    /// `core/txpool/validation.go`, `ValidateTransactionWithState`,
    /// produces via
    /// `fmt.Errorf("%w: next nonce %v, tx nonce %v", core.ErrNonceTooLow, next, tx.Nonce())`
    /// (see `EvmError::next_nonce_hint`'s doc comment). op-geth inherits
    /// this unmodified, so this fixture pins the parser against the real
    /// upstream format rather than a restatement of the parser's own
    /// assumption.
    fn nonce_too_low_with_hint() -> EvmError {
        rpc_error("nonce too low: next nonce 13476, tx nonce 13475")
    }

    const HINTED_NONCE: u64 = 13476;

    /// Like [`nonce_too_low_with_hint`] but with a caller-supplied next
    /// nonce, so a test can script rejections that carry distinct hints
    /// and tell which one the recovery loop actually surfaced.
    fn nonce_too_low_with_hint_at(next_nonce: u64) -> EvmError {
        rpc_error(format!(
            "nonce too low: next nonce {next_nonce}, tx nonce {}",
            next_nonce - 1
        ))
    }

    /// Default `pending_nonce` reply -- distinct from both `STUCK_NONCE`
    /// and `HINTED_NONCE` so tests can tell which source a seeded nonce
    /// came from.
    const DEFAULT_PENDING_NONCE: u64 = 20;

    fn reverted() -> EvmError {
        rpc_error("execution reverted")
    }

    fn base_fees() -> Eip1559Estimation {
        Eip1559Estimation {
            max_fee_per_gas: 1_000_000_000,
            max_priority_fee_per_gas: 100_000_000,
        }
    }

    /// One of `MockSubmitter`'s RPC-shaped methods to make fail with a
    /// scripted transport error. Grouped into a set on `MockSubmitter`
    /// rather than one `bool` field per method, per clippy's
    /// `struct_excessive_bools`.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    enum ScriptedFailure {
        AssignNonce,
        LatestNonce,
        EstimateFees,
        PendingNonce,
    }

    /// Scripted [`TxSubmitter`]: returns queued `submit` results in order,
    /// records the transactions it received, and tracks peak concurrency
    /// so the serialization lock can be asserted.
    struct MockSubmitter {
        results: StdMutex<VecDeque<Result<TxHash, EvmError>>>,
        sent: StdMutex<Vec<TransactionRequest>>,
        active: AtomicUsize,
        max_active: AtomicUsize,
        delay: Duration,
        fees: Eip1559Estimation,
        latest_nonce_value: u64,
        latest_nonce_calls: AtomicUsize,
        failures: HashSet<ScriptedFailure>,
        // Values `pending_nonce` returns, popped in order on successive
        // calls. Once only one value is left, it is returned repeatedly
        // instead of being consumed, so a single-value queue (the common
        // case, set via `with_pending_nonce`) behaves like a constant.
        pending_nonce_queue: StdMutex<VecDeque<u64>>,
        nonce_manager: StdMutex<Option<ResettableNonceManager>>,
    }

    impl MockSubmitter {
        fn new(results: Vec<Result<TxHash, EvmError>>) -> Self {
            Self {
                results: StdMutex::new(results.into()),
                sent: StdMutex::new(Vec::new()),
                active: AtomicUsize::new(0),
                max_active: AtomicUsize::new(0),
                delay: Duration::ZERO,
                fees: base_fees(),
                latest_nonce_value: STUCK_NONCE,
                latest_nonce_calls: AtomicUsize::new(0),
                failures: HashSet::new(),
                pending_nonce_queue: StdMutex::new(VecDeque::from([DEFAULT_PENDING_NONCE])),
                nonce_manager: StdMutex::new(None),
            }
        }

        fn with_delay(mut self, delay: Duration) -> Self {
            self.delay = delay;
            self
        }

        fn with_fees(mut self, fees: Eip1559Estimation) -> Self {
            self.fees = fees;
            self
        }

        fn with_failing_latest_nonce(mut self) -> Self {
            self.failures.insert(ScriptedFailure::LatestNonce);
            self
        }

        /// Makes `assign_nonce` fail unconditionally, bypassing the
        /// cache/seed logic entirely -- simulates a transport error on the
        /// RPC read a cold nonce-cache fetch would otherwise make.
        fn with_failing_assign_nonce(mut self) -> Self {
            self.failures.insert(ScriptedFailure::AssignNonce);
            self
        }

        /// Overrides the value `latest_nonce` returns, so a test can pin
        /// the wallet's own oldest unconfirmed nonce independently of
        /// `STUCK_NONCE`, e.g. to exercise the underpriced-retry branch
        /// that compares a rejected nonce against it.
        fn with_latest_nonce(mut self, latest_nonce_value: u64) -> Self {
            self.latest_nonce_value = latest_nonce_value;
            self
        }

        fn with_failing_estimate_fees(mut self) -> Self {
            self.failures.insert(ScriptedFailure::EstimateFees);
            self
        }

        fn with_pending_nonce(mut self, pending_nonce: u64) -> Self {
            self.pending_nonce_queue = StdMutex::new(VecDeque::from([pending_nonce]));
            self
        }

        /// Scripts a sequence of `pending_nonce` values, one per call, so a
        /// test can prove the read happens fresh on every attempt rather
        /// than once for the whole recovery. Once the sequence is
        /// exhausted, the last value is returned repeatedly.
        fn with_pending_nonce_sequence(mut self, pending_nonces: Vec<u64>) -> Self {
            self.pending_nonce_queue = StdMutex::new(pending_nonces.into());
            self
        }

        fn with_failing_pending_nonce(mut self) -> Self {
            self.failures.insert(ScriptedFailure::PendingNonce);
            self
        }

        fn sent(&self) -> Vec<TransactionRequest> {
            self.sent.lock().expect("sent lock").clone()
        }

        /// Number of times `latest_nonce` was called, so a test can assert
        /// direct `InFlightNonces` proof skipped the RPC read entirely
        /// rather than merely asserting the right outcome was reached.
        fn latest_nonce_calls(&self) -> usize {
            self.latest_nonce_calls.load(Ordering::SeqCst)
        }

        /// Attaches the nonce manager under test so `submit` can simulate
        /// the production nonce filler (see [`Self::fill_nonce`]).
        fn attach_nonce_manager(&self, nonce_manager: ResettableNonceManager) {
            *self.nonce_manager.lock().expect("nonce_manager lock") = Some(nonce_manager);
        }

        /// Simulates the production nonce filler for a request with no
        /// nonce set: if the attached cache holds a seeded value, consumes
        /// it by delegating to `ResettableNonceManager::get_next_nonce` --
        /// the exact method the real `NonceManager` filler calls -- so this
        /// test double cannot drift from its cached-path increment rule.
        /// This lets tests assert on the exact nonce a retry was built at,
        /// not merely on what was seeded beforehand. Leaves the request
        /// untouched when no manager is attached or the cache is unseeded:
        /// no test here exercises the cold RPC-fetch path, so `get_next_nonce`
        /// is only ever called once a seeded value is confirmed present,
        /// and the provider handed to it carries no scripted responses --
        /// if the cold path were ever hit by mistake, the call panics
        /// loudly instead of silently returning a made-up nonce.
        async fn fill_nonce(&self, tx: TransactionRequest) -> TransactionRequest {
            if tx.nonce.is_some() {
                return tx;
            }

            let attached = self
                .nonce_manager
                .lock()
                .expect("nonce_manager lock")
                .clone();
            let Some(nonce_manager) = attached else {
                return tx;
            };

            if nonce_manager.peek_next_nonce(WALLET).await.is_none() {
                return tx;
            }

            let unused_provider = ProviderBuilder::new().connect_mocked_client(Asserter::new());
            let next_nonce = nonce_manager
                .get_next_nonce(&unused_provider, WALLET)
                .await
                .expect("cache was just confirmed seeded; the cold RPC path must not run");

            tx.nonce(next_nonce)
        }
    }

    #[async_trait]
    impl TxSubmitter for MockSubmitter {
        async fn submit(&self, tx: TransactionRequest) -> Result<TxHash, EvmError> {
            let active_now = self.active.fetch_add(1, Ordering::SeqCst) + 1;
            self.max_active.fetch_max(active_now, Ordering::SeqCst);

            if !self.delay.is_zero() {
                tokio::time::sleep(self.delay).await;
            }

            let tx = self.fill_nonce(tx).await;
            self.sent.lock().expect("sent lock").push(tx);
            let result = self
                .results
                .lock()
                .expect("results lock")
                .pop_front()
                .expect("unexpected submit call: no scripted result left");

            self.active.fetch_sub(1, Ordering::SeqCst);
            result
        }

        async fn assign_nonce(
            &self,
            nonce_manager: &ResettableNonceManager,
            address: Address,
        ) -> Result<u64, EvmError> {
            if self.failures.contains(&ScriptedFailure::AssignNonce) {
                return Err(rpc_error("connection reset"));
            }

            if nonce_manager.peek_next_nonce(address).await.is_none() {
                // No test in this module scripts a real cold RPC fetch for
                // the base send's own nonce assignment; seeding 0 here
                // stands in for "a fresh chain reports nonce 0", the same
                // deterministic starting point every un-seeded test in this
                // module already assumes for its address.
                nonce_manager.set_next_nonce(address, 0).await;
            }

            let unused_provider = ProviderBuilder::new().connect_mocked_client(Asserter::new());
            let next_nonce = nonce_manager
                .get_next_nonce(&unused_provider, address)
                .await
                .expect("cache was just confirmed seeded above; the cold RPC path must not run");

            Ok(next_nonce)
        }

        async fn latest_nonce(&self, _address: Address) -> Result<u64, EvmError> {
            self.latest_nonce_calls.fetch_add(1, Ordering::SeqCst);
            if self.failures.contains(&ScriptedFailure::LatestNonce) {
                return Err(rpc_error("connection reset"));
            }
            Ok(self.latest_nonce_value)
        }

        async fn pending_nonce(&self, _address: Address) -> Result<u64, EvmError> {
            if self.failures.contains(&ScriptedFailure::PendingNonce) {
                return Err(rpc_error("connection reset"));
            }

            let mut queue = self
                .pending_nonce_queue
                .lock()
                .expect("pending_nonce_queue lock");
            let next_value = if queue.len() > 1 {
                queue
                    .pop_front()
                    .expect("queue length just checked above 1")
            } else {
                *queue
                    .front()
                    .expect("pending_nonce_queue must never be emptied")
            };
            Ok(next_value)
        }

        async fn estimate_fees(&self) -> Result<Eip1559Estimation, EvmError> {
            if self.failures.contains(&ScriptedFailure::EstimateFees) {
                return Err(rpc_error("connection reset"));
            }
            Ok(self.fees)
        }
    }

    async fn run(mock: &MockSubmitter) -> Result<TxHash, EvmError> {
        run_with_manager(mock).await.0
    }

    /// Like [`run`] but also hands back the nonce manager, so the
    /// nonce-recovery paths can be asserted on what they left cached.
    async fn run_with_manager(
        mock: &MockSubmitter,
    ) -> (Result<TxHash, EvmError>, ResettableNonceManager) {
        let (result, nonce_manager, _in_flight) = run_with_manager_and_tracker(mock).await;
        (result, nonce_manager)
    }

    /// Like [`run_with_manager`] but also hands back the [`InFlightNonces`]
    /// tracker, so a test can assert on what a successful send recorded
    /// there.
    async fn run_with_manager_and_tracker(
        mock: &MockSubmitter,
    ) -> (
        Result<TxHash, EvmError>,
        ResettableNonceManager,
        InFlightNonces,
    ) {
        run_with_optionally_seeded_manager(mock, None).await
    }

    /// Like [`run_with_manager`] but pre-seeds the cache for `WALLET` with
    /// `seed_nonce` first, so a test can prove the recovery path actually
    /// overwrote or cleared the cache rather than merely finding it
    /// already empty.
    async fn run_with_seeded_manager(
        mock: &MockSubmitter,
        seed_nonce: u64,
    ) -> (Result<TxHash, EvmError>, ResettableNonceManager) {
        let (result, nonce_manager, _in_flight) =
            run_with_seeded_manager_and_tracker(mock, seed_nonce).await;
        (result, nonce_manager)
    }

    /// Like [`run_with_seeded_manager`] but also hands back the
    /// [`InFlightNonces`] tracker.
    async fn run_with_seeded_manager_and_tracker(
        mock: &MockSubmitter,
        seed_nonce: u64,
    ) -> (
        Result<TxHash, EvmError>,
        ResettableNonceManager,
        InFlightNonces,
    ) {
        run_with_optionally_seeded_manager(mock, Some((WALLET, seed_nonce))).await
    }

    /// Shared harness backing the functions above: builds a fresh nonce
    /// manager and [`InFlightNonces`] tracker, optionally seeding the cache
    /// for `seed.0` with `seed.1` first, attaches the manager to `mock`, and
    /// drives one `send_with_recovery` call through it. The seeded address
    /// need not be `WALLET`: a test proving `invalidate()` ran (which clears
    /// every address, not just `WALLET`'s) without wanting a floor
    /// established for `WALLET` itself seeds a different address instead.
    async fn run_with_optionally_seeded_manager(
        mock: &MockSubmitter,
        seed: Option<(Address, u64)>,
    ) -> (
        Result<TxHash, EvmError>,
        ResettableNonceManager,
        InFlightNonces,
    ) {
        let nonce_manager = ResettableNonceManager::default();
        if let Some((seed_address, seed_nonce)) = seed {
            nonce_manager.set_next_nonce(seed_address, seed_nonce).await;
        }
        mock.attach_nonce_manager(nonce_manager.clone());

        let in_flight = InFlightNonces::default();
        let send_lock = Mutex::new(());
        let result = send_with_recovery(
            mock,
            &nonce_manager,
            &in_flight,
            &send_lock,
            WALLET,
            CONTRACT,
            Bytes::from_static(b"calldata"),
            "test",
        )
        .await;

        (result, nonce_manager, in_flight)
    }

    #[test]
    fn next_nonce_hint_parses_the_node_reported_nonce() {
        assert_eq!(
            nonce_too_low_with_hint().next_nonce_hint(),
            Some(NextNonceHint(HINTED_NONCE))
        );
    }

    #[test]
    fn next_nonce_hint_is_absent_without_a_reported_nonce() {
        assert_eq!(nonce_too_low().next_nonce_hint(), None);
        assert_eq!(
            rpc_error("nonce too low: next nonce , tx nonce 13475").next_nonce_hint(),
            None,
            "a truncated value must not be mistaken for a nonce"
        );
    }

    #[test]
    fn next_nonce_hint_rejects_a_hex_formatted_value() {
        assert_eq!(
            rpc_error("nonce too low: next nonce 0x34ac, tx nonce 0x34ab").next_nonce_hint(),
            None,
            "a hex-formatted value is not the decimal format go-ethereum emits and must \
             not be misparsed as its leading digit"
        );
    }

    #[test]
    fn next_nonce_hint_rejects_a_value_with_a_leading_sign() {
        assert_eq!(
            rpc_error("nonce too low: next nonce +13476, tx nonce 13475").next_nonce_hint(),
            None,
            "a signed value is not the bare decimal format go-ethereum emits"
        );
    }

    #[test]
    fn next_nonce_hint_ignores_unrelated_errors() {
        assert_eq!(reverted().next_nonce_hint(), None);
        assert_eq!(underpriced().next_nonce_hint(), None);
    }

    #[tokio::test]
    async fn first_send_success_returns_hash_without_resubmit() {
        let hash = TxHash::repeat_byte(0x11);
        let mock = MockSubmitter::new(vec![Ok(hash)]);

        let result = run(&mock).await.unwrap();

        assert_eq!(result, hash);
        let sent = mock.sent();
        assert_eq!(sent.len(), 1, "only one submit expected");
        assert_eq!(
            sent[0].nonce,
            Some(0),
            "base send must pin the nonce it assigned via assign_nonce, not \
             leave it unset for a filler to assign invisibly"
        );
        assert_eq!(
            sent[0].max_fee_per_gas, None,
            "base send must not set a fee"
        );
    }

    #[tokio::test]
    async fn first_send_success_is_queryable_as_ours_at_the_assigned_nonce() {
        let hash = TxHash::repeat_byte(0x12);
        let mock = MockSubmitter::new(vec![Ok(hash)]);

        let (result, _nonce_manager, in_flight) = run_with_manager_and_tracker(&mock).await;

        assert_eq!(result.unwrap(), hash);
        let sent = mock.sent();
        let assigned_nonce = sent[0]
            .nonce
            .expect("base send must pin its assigned nonce");

        assert_eq!(
            in_flight.ownership(WALLET, assigned_nonce),
            NonceOwnership::Ours,
            "a successful base send must be recorded as this wallet's own"
        );
    }

    #[tokio::test]
    async fn a_rejected_base_send_records_no_in_flight_entry() {
        // The specific attempt that was rejected was, by definition, never
        // accepted onto the node -- recording it would make the ownership
        // check circular (the just-rejected attempt would look like proof
        // it owns the nonce that rejected it).
        let mock = MockSubmitter::new(vec![Err(reverted())]);

        let (result, _nonce_manager, in_flight) = run_with_manager_and_tracker(&mock).await;

        result.unwrap_err();
        let sent = mock.sent();
        let rejected_nonce = sent[0]
            .nonce
            .expect("base send must pin its assigned nonce");

        assert_eq!(
            in_flight.ownership(WALLET, rejected_nonce),
            NonceOwnership::Unknown,
            "a rejected base send must leave no entry behind for its nonce"
        );
    }

    #[tokio::test]
    async fn underpriced_then_success_resubmits_at_the_exact_rejected_nonce_with_bumped_fee() {
        // The cache is seeded above `STUCK_NONCE` (the value `latest_nonce()`
        // reports) specifically to prove the resubmit targets the nonce the
        // base send was actually assigned and rejected at, not a fresh
        // `latest_nonce()` read -- the direct regression test for a wallet
        // with other unconfirmed sends in flight, where `latest_nonce()`
        // undercounts (see the module doc's former "Known limitation").
        const ASSIGNED_NONCE: u64 = STUCK_NONCE + 1000;
        let hash = TxHash::repeat_byte(0x22);
        let mock = MockSubmitter::new(vec![Err(underpriced()), Ok(hash)]);

        let (result, _nonce_manager, in_flight) =
            run_with_seeded_manager_and_tracker(&mock, ASSIGNED_NONCE).await;

        assert_eq!(result.unwrap(), hash);
        let sent = mock.sent();
        assert_eq!(sent.len(), 2, "base send + one resubmit");

        assert_eq!(
            sent[0].nonce,
            Some(ASSIGNED_NONCE),
            "the base send must pin the nonce it assigned"
        );

        let resubmit = &sent[1];
        assert_eq!(
            resubmit.nonce,
            Some(ASSIGNED_NONCE),
            "resubmit must target the exact nonce the base send was rejected \
             at, not a fresh latest_nonce() read (STUCK_NONCE)"
        );
        // First bump is +15% over the network estimate.
        assert_eq!(resubmit.max_fee_per_gas, Some(1_000_000_000 * 115 / 100));
        assert_eq!(
            resubmit.max_priority_fee_per_gas,
            Some(100_000_000 * 115 / 100)
        );

        assert_eq!(
            in_flight.ownership(WALLET, ASSIGNED_NONCE),
            NonceOwnership::Ours,
            "a successful fee-bumped resubmit must be recorded at the pinned nonce"
        );
    }

    #[tokio::test]
    async fn resubmit_with_bumped_fee_success_leaves_nonce_cache_untouched() {
        // Regression test: a prior version seeded `nonce + 1` into the cache
        // on a successful replacement, which could move the cache backwards
        // over another send from this wallet still in flight. The seed was
        // deliberately removed; this pins that it stays removed.
        let hash = TxHash::repeat_byte(0x40);
        const SEED_NONCE: u64 = 50;
        let mock = MockSubmitter::new(vec![Err(underpriced()), Ok(hash)]);

        let (result, nonce_manager) = run_with_seeded_manager(&mock, SEED_NONCE).await;

        assert_eq!(result.unwrap(), hash);
        // The base send is the only step that touches the cache here: it
        // consumes the seeded value through the mock's nonce filler,
        // advancing the cache by exactly one, same as production's real
        // filler stack (see `local::LocalWallet`/`turnkey::TurnkeyWallet`
        // construction, both wired with `ResettableNonceManager`). The
        // resubmit pins the stuck nonce explicitly, so it never touches the
        // cache. If the removed `nonce + 1` seed were reintroduced, this
        // would read `Some(STUCK_NONCE + 1)` (8) instead.
        assert_eq!(
            nonce_manager.peek_next_nonce(WALLET).await,
            Some(SEED_NONCE + 1),
            "a successful fee-bumped replacement must not seed the cache on top \
             of what the base send's own nonce fill already advanced it to"
        );
    }

    #[tokio::test]
    async fn underpriced_escalates_fee_each_attempt_then_errors_after_budget() {
        let mock = MockSubmitter::new(vec![
            Err(underpriced()),
            Err(underpriced()),
            Err(underpriced()),
            Err(underpriced()),
            Err(underpriced()),
        ]);

        let error = run(&mock).await.unwrap_err();

        assert!(
            matches!(error, EvmError::ReplacementUnderpriced { attempts } if attempts == MAX_REPLACEMENT_RESUBMITS),
            "expected exhausted-budget error, got {error:?}"
        );

        let sent = mock.sent();
        assert_eq!(sent.len(), 5, "base send + 4 resubmit attempts");

        let assigned_nonce = sent[0]
            .nonce
            .expect("base send must pin its assigned nonce");

        // Resubmits escalate 15%, 30%, 45%, 60% over the estimate.
        let expected_bumps = [115, 130, 145, 160];
        for (index, pct) in expected_bumps.iter().enumerate() {
            let resubmit = &sent[index + 1];
            assert_eq!(
                resubmit.nonce,
                Some(assigned_nonce),
                "every resubmit must target the exact nonce the base send was \
                 assigned and rejected at"
            );
            assert_eq!(
                resubmit.max_fee_per_gas,
                Some(1_000_000_000 * pct / 100),
                "attempt {} fee bump",
                index + 1
            );
        }
    }

    #[tokio::test]
    async fn resubmit_with_bumped_fee_proceeds_when_nonce_is_provably_ours() {
        // The tracker already recognizes STUCK_NONCE as this wallet's own
        // unconfirmed send (e.g. the base send that triggered this resubmit
        // recorded it moments earlier), so the bounded escalation loop must
        // run exactly as it always has.
        let in_flight = InFlightNonces::default();
        let original_hash = TxHash::repeat_byte(0x9b);
        in_flight.record(WALLET, STUCK_NONCE, original_hash);
        let nonce_manager = ResettableNonceManager::default();
        let replacement_hash = TxHash::repeat_byte(0x9c);
        let mock = MockSubmitter::new(vec![Ok(replacement_hash)]);

        let result = resubmit_with_bumped_fee(
            &mock,
            &nonce_manager,
            &in_flight,
            WALLET,
            CONTRACT,
            Bytes::from_static(b"calldata"),
            "test",
            STUCK_NONCE,
        )
        .await;

        assert_eq!(result.unwrap(), replacement_hash);
        let sent = mock.sent();
        assert_eq!(
            sent.len(),
            1,
            "direct proof of ownership must proceed with the fee-bump loop, \
             same as today"
        );
        assert_eq!(sent[0].nonce, Some(STUCK_NONCE));
        assert_eq!(
            sent[0].max_fee_per_gas,
            Some(1_000_000_000 * 115 / 100),
            "the first attempt's usual 15% bump must still apply"
        );
    }

    #[tokio::test]
    async fn resubmit_with_bumped_fee_bumps_when_ownership_is_unknown_cold_start() {
        // No entry has ever been recorded for WALLET in this tracker --
        // the cold-start window right after a restart, before this
        // wallet's own first successful send. The fee-bump loop must still
        // proceed exactly as it always has, rather than refuse outright.
        let in_flight = InFlightNonces::default();
        assert_eq!(
            in_flight.ownership(WALLET, STUCK_NONCE),
            NonceOwnership::Unknown,
            "precondition: this tracker has never seen this address"
        );
        let nonce_manager = ResettableNonceManager::default();
        let hash = TxHash::repeat_byte(0x9d);
        let mock = MockSubmitter::new(vec![Ok(hash)]);

        let result = resubmit_with_bumped_fee(
            &mock,
            &nonce_manager,
            &in_flight,
            WALLET,
            CONTRACT,
            Bytes::from_static(b"calldata"),
            "test",
            STUCK_NONCE,
        )
        .await;

        assert_eq!(result.unwrap(), hash);
        assert_eq!(
            mock.sent().len(),
            1,
            "an Unknown ownership answer must still fall back to today's \
             best-effort fee-bump loop rather than refuse outright"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn nonce_too_low_with_hint_retries_at_the_reported_nonce() {
        let hash = TxHash::repeat_byte(0x33);
        // Default pending_nonce (DEFAULT_PENDING_NONCE) is below the hint,
        // so the hint must win.
        let mock = MockSubmitter::new(vec![Err(nonce_too_low_with_hint()), Ok(hash)]);

        let (result, nonce_manager) = run_with_manager(&mock).await;

        assert_eq!(result.unwrap(), hash);
        let sent = mock.sent();
        assert_eq!(sent.len(), 2, "base send + one nonce-too-low retry");
        assert_eq!(
            sent[1].nonce,
            Some(HINTED_NONCE),
            "the retry must be built at the nonce the node reported"
        );
        assert_eq!(
            nonce_manager.peek_next_nonce(WALLET).await,
            Some(HINTED_NONCE + 1),
            "a successful retry must leave the cache one past the nonce it was \
             built at, not invalidated or re-seeded -- a regression that dropped \
             the wallet back to a cold `latest` re-fetch on the next send would \
             re-create the very collision this recovery just resolved"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn nonce_too_low_retry_success_is_queryable_as_ours_at_the_retried_nonce() {
        let hash = TxHash::repeat_byte(0x91);
        let mock = MockSubmitter::new(vec![Err(nonce_too_low_with_hint()), Ok(hash)]);

        let (result, _nonce_manager, in_flight) = run_with_manager_and_tracker(&mock).await;

        assert_eq!(result.unwrap(), hash);
        assert_eq!(
            in_flight.ownership(WALLET, HINTED_NONCE),
            NonceOwnership::Ours,
            "a successful nonce-too-low retry must be recorded at the nonce it \
             was submitted at"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn nonce_too_low_with_hint_prefers_the_higher_pending_nonce() {
        let hash = TxHash::repeat_byte(0x35);
        let higher_pending_nonce = HINTED_NONCE + 500;
        let mock = MockSubmitter::new(vec![Err(nonce_too_low_with_hint()), Ok(hash)])
            .with_pending_nonce(higher_pending_nonce);

        let (result, _nonce_manager) = run_with_manager(&mock).await;

        assert_eq!(result.unwrap(), hash);
        assert_eq!(
            mock.sent()[1].nonce,
            Some(higher_pending_nonce),
            "the pending-block count is ahead of the node-reported nonce, so it must win"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn nonce_too_low_with_hint_falls_back_to_the_hint_when_pending_fetch_fails() {
        let hash = TxHash::repeat_byte(0x36);
        let mock = MockSubmitter::new(vec![Err(nonce_too_low_with_hint()), Ok(hash)])
            .with_failing_pending_nonce();

        let (result, _nonce_manager) = run_with_manager(&mock).await;

        assert_eq!(result.unwrap(), hash);
        assert_eq!(
            mock.sent()[1].nonce,
            Some(HINTED_NONCE),
            "a failed pending-nonce fetch must not block using the node-reported hint"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn nonce_too_low_without_hint_seeds_the_pending_nonce() {
        let hash = TxHash::repeat_byte(0x34);
        let mock = MockSubmitter::new(vec![Err(nonce_too_low()), Ok(hash)]);

        let (result, _nonce_manager) = run_with_manager(&mock).await;

        assert_eq!(result.unwrap(), hash);
        let sent = mock.sent();
        assert_eq!(sent.len(), 2, "base send + one nonce-too-low retry");
        assert_eq!(
            sent[1].nonce,
            Some(DEFAULT_PENDING_NONCE),
            "an unparsable rejection must still seed the cache from a pending-nonce read"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn nonce_too_low_without_hint_exhausts_budget_without_retry_when_pending_fetch_fails() {
        // Neither a node-reported hint nor a cached/pending nonce is
        // available on any attempt, so every retry must be skipped rather
        // than submitting a retry that would cold-fetch `latest` -- the
        // exact stale value that caused the incident. No results are
        // scripted at all: if the loop wrongly submitted a retry, it would
        // panic on an empty queue.
        //
        // Exercised by calling `retry_after_nonce_too_low` directly instead
        // of through `send_with_recovery`: the base send's own
        // `assign_nonce` call always seeds `WALLET`'s nonce cache first
        // (mirroring the real production filler, which populates the same
        // cache via the same `NonceManager::get_next_nonce` call), so by
        // the time `send_with_recovery` would reach this recovery loop, a
        // floor is always already established from that seed. This
        // specific "no floor established at all" case can therefore only be
        // exercised by entering the recovery loop directly against a cache
        // that has never been touched for `WALLET`. A different address
        // (`CONTRACT`) is pre-seeded with a sentinel instead, so the
        // assertion below only passes if `invalidate()` -- which clears
        // every address's cache, not just `WALLET`'s -- actually ran.
        const SEEDED_SENTINEL_NONCE: u64 = 999_999;
        let mock = MockSubmitter::new(vec![]).with_failing_pending_nonce();
        let nonce_manager = ResettableNonceManager::default();
        nonce_manager
            .set_next_nonce(CONTRACT, SEEDED_SENTINEL_NONCE)
            .await;
        let in_flight = InFlightNonces::default();

        let error = retry_after_nonce_too_low(
            &mock,
            &nonce_manager,
            &in_flight,
            WALLET,
            CONTRACT,
            Bytes::from_static(b"calldata"),
            "test",
            nonce_too_low(),
        )
        .await
        .unwrap_err();

        assert!(
            error.is_nonce_too_low(),
            "the original nonce-too-low rejection must be surfaced once the \
             budget is exhausted without any retry, got {error:?}"
        );
        assert_eq!(
            mock.sent().len(),
            0,
            "every attempt in this state must be consumed without submitting \
             anything -- a retry would cold-fetch the stale latest nonce that \
             caused the incident, so all MAX_NONCE_TOO_LOW_RETRIES attempts \
             burn budget with zero transactions sent"
        );
        assert_eq!(
            nonce_manager.peek_next_nonce(CONTRACT).await,
            None,
            "neither a hint nor a pending read is available, so the cache must be \
             cleared, not left holding the pre-seeded value"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn nonce_too_low_with_hint_retries_immediately_without_backoff() {
        let hash = TxHash::repeat_byte(0x39);
        let mock = MockSubmitter::new(vec![Err(nonce_too_low_with_hint()), Ok(hash)]);

        let started_at = Instant::now();
        let result = run(&mock).await;
        let elapsed = started_at.elapsed();

        result.unwrap();
        assert_eq!(
            elapsed,
            Duration::ZERO,
            "a node-reported hint is authoritative enough to retry with no backoff"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn nonce_too_low_without_hint_backs_off_even_with_a_pending_only_target() {
        let hash = TxHash::repeat_byte(0x3a);
        // No hint (unparsable rejection), but the pending-nonce read still
        // succeeds and computes a target -- this must still back off, since
        // a hint-less pending read carries no authority of its own (it is
        // node-local, see `TxSubmitter::pending_nonce`'s doc comment).
        let mock = MockSubmitter::new(vec![Err(nonce_too_low()), Ok(hash)]);

        let started_at = Instant::now();
        let result = run(&mock).await;
        let elapsed = started_at.elapsed();

        result.unwrap();
        assert_eq!(
            elapsed, NONCE_RETRY_BACKOFF,
            "a pending-only target without a node-reported hint must back off by \
             exactly the first attempt's backoff"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn nonce_too_low_backoff_scales_with_attempt_number_across_the_full_retry_budget() {
        // Every attempt here is hint-less, so every attempt backs off (see
        // `nonce_too_low_with_hint_retries_immediately_without_backoff` for
        // the hinted, no-backoff case, and
        // `nonce_too_low_without_hint_backs_off_even_with_a_pending_only_target`
        // for the attempt-1-only case). This exhausts the full retry budget
        // and pins the cumulative virtual elapsed time, so a regression that
        // flattened `NONCE_RETRY_BACKOFF * attempt` to a constant per-attempt
        // backoff on attempts 2/3 -- which every existing multi-attempt test
        // would miss, since none of them capture elapsed time -- would fail
        // this assertion.
        let mock = MockSubmitter::new(vec![
            Err(nonce_too_low()),
            Err(nonce_too_low()),
            Err(nonce_too_low()),
            Err(nonce_too_low()),
        ]);

        let started_at = Instant::now();
        let result = run(&mock).await;
        let elapsed = started_at.elapsed();

        let error = result.unwrap_err();
        assert!(
            error.is_nonce_too_low(),
            "expected the exhausted-budget nonce-too-low rejection, got {error:?}"
        );
        assert_eq!(
            mock.sent().len(),
            usize::try_from(MAX_NONCE_TOO_LOW_RETRIES).unwrap() + 1,
            "base send + the full retry budget"
        );
        assert_eq!(
            elapsed,
            NONCE_RETRY_BACKOFF * 1 + NONCE_RETRY_BACKOFF * 2 + NONCE_RETRY_BACKOFF * 3,
            "the cumulative backoff across all three attempts (500ms + 1000ms + \
             1500ms) must scale linearly with the attempt number, not repeat a \
             constant per-attempt backoff"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn nonce_too_low_retry_rejected_as_underpriced_retries_within_budget_without_fee_bump() {
        let hash = TxHash::repeat_byte(0x38);
        // The retry after the hinted nonce-too-low rejection comes back
        // underpriced, targeting HINTED_NONCE + 1 (the pending read is
        // higher than the hint here). `latest_nonce` (our own oldest
        // unconfirmed nonce) is pinned well below that, at STUCK_NONCE, so
        // the incumbent cannot be our own stuck transaction: the loop must
        // not fee-bump it, but treat it as continued external nonce
        // contention and consume another attempt of the same budget. The
        // rejection also proves the nonce it was submitted at is occupied,
        // raising the floor past it, so the next attempt lands one further
        // ahead even though the pending-nonce read alone stays unchanged.
        let mock = MockSubmitter::new(vec![
            Err(nonce_too_low_with_hint()),
            Err(underpriced()),
            Ok(hash),
        ])
        .with_pending_nonce(HINTED_NONCE + 1)
        .with_latest_nonce(STUCK_NONCE);

        let (result, _nonce_manager) = run_with_manager(&mock).await;

        assert_eq!(result.unwrap(), hash);
        let sent = mock.sent();
        assert_eq!(
            sent.len(),
            3,
            "base send + underpriced nonce retry + a further nonce retry, no fee-bump"
        );
        let final_retry = &sent[2];
        assert_eq!(
            final_retry.max_fee_per_gas, None,
            "an underpriced rejection during nonce recovery must not be fee-bumped -- \
             the nonce is occupied but the incumbent transaction's owner is unknown"
        );
        assert_eq!(
            final_retry.nonce,
            Some(HINTED_NONCE + 2),
            "the final retry must land past the nonce the underpriced rejection \
             just proved occupied (HINTED_NONCE + 1), not merely repeat the \
             unchanged pending-nonce read"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn nonce_too_low_retries_up_to_the_budget_then_surfaces_the_last_error() {
        // Each rejection carries a distinct next-nonce hint so the
        // assertion below can tell the last error from an earlier one --
        // a regression that surfaced (or retried against) a stale hint
        // would be caught here.
        let mock = MockSubmitter::new(vec![
            Err(nonce_too_low_with_hint_at(13476)),
            Err(nonce_too_low_with_hint_at(13477)),
            Err(nonce_too_low_with_hint_at(13478)),
            Err(nonce_too_low_with_hint_at(13479)),
        ]);

        let (result, nonce_manager) = run_with_manager(&mock).await;

        let error = result.unwrap_err();
        assert_eq!(
            error.next_nonce_hint(),
            Some(NextNonceHint(13479)),
            "the surfaced error must be the last rejection, not an earlier one"
        );
        assert_eq!(
            mock.sent().len(),
            usize::try_from(MAX_NONCE_TOO_LOW_RETRIES).unwrap() + 1,
            "base send + the full retry budget"
        );
        assert_eq!(
            nonce_manager.peek_next_nonce(WALLET).await,
            None,
            "an exhausted budget must leave the cache invalidated"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn nonce_too_low_retries_exhaust_budget_with_underpriced_as_final_error() {
        // Every retry after the first comes back "replacement transaction
        // underpriced" instead of "nonce too low" (continued external
        // contention, see `nonce_too_low_retry_rejected_as_underpriced_...`
        // above). The budget must still exhaust and surface that underpriced
        // rejection verbatim -- not a stale nonce-too-low error and not a
        // misleading claim that the retries were all nonce-too-low.
        let mock = MockSubmitter::new(vec![
            Err(nonce_too_low_with_hint()),
            Err(underpriced()),
            Err(underpriced()),
            Err(underpriced()),
        ]);

        let (result, nonce_manager) = run_with_manager(&mock).await;

        let error = result.unwrap_err();
        assert!(
            error.is_replacement_underpriced(),
            "the final surfaced error must be the last rejection (underpriced), \
             got {error:?}"
        );
        assert_eq!(
            mock.sent().len(),
            usize::try_from(MAX_NONCE_TOO_LOW_RETRIES).unwrap() + 1,
            "base send + the full retry budget"
        );
        assert_eq!(
            nonce_manager.peek_next_nonce(WALLET).await,
            None,
            "an exhausted budget must leave the cache invalidated even when the \
             final rejection was underpriced rather than nonce-too-low"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn nonce_too_low_retry_does_not_regress_below_a_floor_proven_by_this_recovery() {
        // Regression test for the exact incident this recovery exists to
        // tolerate: the first rejection reports "next nonce 13476"; the
        // retry built at 13476 is itself rejected nonce-too-low with no
        // parseable hint (a plausible phrasing variant); the pending-nonce
        // read that follows then lands on a stale node and reports 13475,
        // one below the nonce the loop just proved too low. Without a
        // monotonic floor the loop would retry at 13475 again -- exactly
        // the load-balanced-RPC staleness this recovery exists to
        // tolerate -- and burn the retry budget on a nonce already known
        // to fail.
        let hash = TxHash::repeat_byte(0x37);
        let mock = MockSubmitter::new(vec![
            Err(nonce_too_low_with_hint()),
            Err(nonce_too_low()),
            Ok(hash),
        ])
        .with_pending_nonce(HINTED_NONCE - 1);

        let (result, _nonce_manager) = run_with_manager(&mock).await;

        assert_eq!(result.unwrap(), hash);
        let sent = mock.sent();
        assert_eq!(sent.len(), 3, "base send + two nonce-too-low retries");
        assert_eq!(
            sent[1].nonce,
            Some(HINTED_NONCE),
            "the first retry uses the node-reported hint"
        );
        assert_eq!(
            sent[2].nonce,
            Some(HINTED_NONCE + 1),
            "the second retry must be built at least one above the nonce the \
             first retry just proved too low (HINTED_NONCE), never regressing \
             to the stale pending-nonce read of HINTED_NONCE - 1"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn nonce_floor_falls_back_to_the_established_floor_when_pending_read_fails() {
        // Regression test: the first retry has no parseable hint (a
        // plausible phrasing variant) and is itself rejected nonce-too-low,
        // proving the floor at HINTED_NONCE + 1. The pending-nonce read
        // then fails on every remaining attempt (a transient RPC blip).
        // Without consulting the floor here, the loop would invalidate and
        // skip every remaining attempt, submitting nothing further despite
        // already holding a nonce it has proven is safe to use.
        let hash = TxHash::repeat_byte(0x3d);
        let mock = MockSubmitter::new(vec![
            Err(nonce_too_low_with_hint()),
            Err(nonce_too_low()),
            Ok(hash),
        ])
        .with_failing_pending_nonce();

        let (result, _nonce_manager) = run_with_manager(&mock).await;

        assert_eq!(result.unwrap(), hash);
        let sent = mock.sent();
        assert_eq!(
            sent.len(),
            3,
            "base send + a first retry that establishes the floor + a second \
             retry that falls back to it, rather than being skipped"
        );
        assert_eq!(
            sent[1].nonce,
            Some(HINTED_NONCE),
            "the first retry uses the node-reported hint"
        );
        assert_eq!(
            sent[2].nonce,
            Some(HINTED_NONCE + 1),
            "the second retry must fall back to the floor proven safe by the \
             first retry's rejection, instead of being skipped entirely just \
             because neither a hint nor a pending read is available"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn nonce_too_low_retry_rejected_as_underpriced_does_not_regress_below_occupied_nonce() {
        // Regression test: an underpriced rejection proves the target nonce
        // is occupied by another pending transaction, so the next attempt
        // must not target that nonce or anything below it. A stale
        // pending-nonce read landing below the occupied nonce must not
        // regress the retry. `latest_nonce` is pinned well below the
        // occupied nonce (STUCK_NONCE), so this exercises the "safe,
        // strictly-above" branch: the incumbent cannot be our own stuck
        // transaction, and the floor may advance past it.
        let hash = TxHash::repeat_byte(0x3e);
        let mock = MockSubmitter::new(vec![
            Err(nonce_too_low_with_hint()),
            Err(underpriced()),
            Ok(hash),
        ])
        .with_pending_nonce(HINTED_NONCE - 1)
        .with_latest_nonce(STUCK_NONCE);

        let (result, _nonce_manager) = run_with_manager(&mock).await;

        assert_eq!(result.unwrap(), hash);
        let sent = mock.sent();
        assert_eq!(
            sent.len(),
            3,
            "base send + underpriced retry + a further retry that advances \
             past the occupied nonce"
        );
        assert_eq!(
            sent[1].nonce,
            Some(HINTED_NONCE),
            "the first retry uses the node-reported hint"
        );
        assert_eq!(
            sent[2].nonce,
            Some(HINTED_NONCE + 1),
            "the second retry must be built at least one above the nonce the \
             underpriced rejection just proved occupied (HINTED_NONCE), never \
             regressing to the stale pending-nonce read of HINTED_NONCE - 1"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn nonce_too_low_retry_rejected_as_underpriced_at_own_oldest_unconfirmed_nonce_stops() {
        // Regression test for the incident this recovery must not repeat:
        // an underpriced rejection alone does not prove the incumbent
        // belongs to an external co-signer. The retry lands at HINTED_NONCE
        // (pending stays at the default, below the hint) and comes back
        // underpriced. `latest_nonce` -- this wallet's own oldest
        // unconfirmed nonce -- is pinned to that exact value, the
        // signature of our own under-gassed pending transaction rather
        // than external contention. The loop must stop and surface the
        // rejection instead of raising the floor past it: doing so would
        // accept the next send into a gapped nonce behind a transaction
        // that can never mine, wedging the wallet until a restart.
        let mock = MockSubmitter::new(vec![Err(nonce_too_low_with_hint()), Err(underpriced())])
            .with_latest_nonce(HINTED_NONCE);

        let (result, nonce_manager) = run_with_manager(&mock).await;

        let error = result.unwrap_err();
        assert!(
            error.is_replacement_underpriced(),
            "expected the underpriced rejection surfaced verbatim, got {error:?}"
        );
        let sent = mock.sent();
        assert_eq!(
            sent.len(),
            2,
            "base send + one nonce-too-low retry -- the loop must stop rather \
             than submit a third send into a nonce gapped past our own stuck \
             transaction"
        );
        assert_eq!(
            sent[1].nonce,
            Some(HINTED_NONCE),
            "the retry targets the node-reported hint"
        );
        assert_eq!(
            nonce_manager.peek_next_nonce(WALLET).await,
            None,
            "stopping on our own stuck nonce must invalidate the cache, not \
             leave it seeded past a transaction that can never mine"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn nonce_too_low_retry_underpriced_below_latest_nonce_continues_within_budget() {
        // Regression test: an underpriced rejection at nonce N, followed by
        // a `latest_nonce()` read of N + 1, means the contention resolved
        // (or a different load-balanced node advanced) while this recovery
        // was retrying -- the rejected nonce is already mined, not stuck.
        // Treating this the same as the "our own stuck transaction" case
        // (N == latest_nonce_value) would wrongly abort recovery instead of
        // raising the floor and continuing to try within budget.
        let hash = TxHash::repeat_byte(0x3f);
        let mock = MockSubmitter::new(vec![
            Err(nonce_too_low_with_hint()),
            Err(underpriced()),
            Ok(hash),
        ])
        .with_latest_nonce(HINTED_NONCE + 1);

        let (result, _nonce_manager) = run_with_manager(&mock).await;

        assert_eq!(result.unwrap(), hash);
        let sent = mock.sent();
        assert_eq!(
            sent.len(),
            3,
            "base send + underpriced retry + a further retry that succeeds \
             once the floor is raised to the already-mined nonce"
        );
        assert_eq!(
            sent[1].nonce,
            Some(HINTED_NONCE),
            "the first retry targets the node-reported hint"
        );
        assert_eq!(
            sent[2].nonce,
            Some(HINTED_NONCE + 1),
            "the second retry must land at the latest-nonce read that proved \
             HINTED_NONCE already mined, not surface the underpriced \
             rejection as if it were our own stuck transaction"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn nonce_too_low_retry_rejected_as_underpriced_surfaces_when_latest_nonce_fetch_fails() {
        // If confirming whether the occupied nonce is our own fails, the
        // loop must take the safe branch and surface the retry's
        // underpriced rejection rather than gamble on advancing past what
        // might be our own stuck transaction.
        let mock = MockSubmitter::new(vec![Err(nonce_too_low_with_hint()), Err(underpriced())])
            .with_failing_latest_nonce();

        let (result, nonce_manager) = run_with_manager(&mock).await;

        let error = result.unwrap_err();
        assert!(
            error.is_replacement_underpriced(),
            "expected the underpriced rejection surfaced verbatim, got {error:?}"
        );
        assert_eq!(
            mock.sent().len(),
            2,
            "base send + one nonce-too-low retry -- no further send once the \
             latest-nonce check itself fails"
        );
        assert_eq!(
            nonce_manager.peek_next_nonce(WALLET).await,
            None,
            "a failed safety check must invalidate the cache, not leave it seeded"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn nonce_too_low_retry_underpriced_stops_when_nonce_is_provably_ours() {
        // The tracker already recognizes HINTED_NONCE as this wallet's own
        // unconfirmed send -- direct proof, not the inferred
        // `latest_nonce()` coincidence the Unknown fallback relies on.
        // `retry_after_nonce_too_low` is called directly (bypassing the base
        // send) so only the one retry submit needs a scripted result.
        let mock = MockSubmitter::new(vec![Err(underpriced())]);
        let nonce_manager = ResettableNonceManager::default();
        let in_flight = InFlightNonces::default();
        in_flight.record(WALLET, HINTED_NONCE, TxHash::repeat_byte(0x9e));

        let error = retry_after_nonce_too_low(
            &mock,
            &nonce_manager,
            &in_flight,
            WALLET,
            CONTRACT,
            Bytes::from_static(b"calldata"),
            "test",
            nonce_too_low_with_hint(),
        )
        .await
        .unwrap_err();

        assert!(
            error.is_replacement_underpriced(),
            "expected the underpriced rejection surfaced verbatim, got {error:?}"
        );
        assert_eq!(
            mock.sent().len(),
            1,
            "one retry attempt, rejected underpriced at a nonce this wallet's own \
             bookkeeping proves it owns -- must stop rather than continue"
        );
        assert_eq!(
            mock.latest_nonce_calls(),
            0,
            "direct ownership proof must skip the latest_nonce() RPC read entirely"
        );
        assert_eq!(
            nonce_manager.peek_next_nonce(WALLET).await,
            None,
            "stopping on our own proven nonce must invalidate the cache"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn nonce_floor_is_seeded_from_the_higher_of_the_triggering_hint_and_the_cached_nonce() {
        // Regression/defense test for the pre-loop floor seed. A prior
        // version of this test seeded the manager's cache with nothing and
        // asserted the retry landed on the triggering rejection's hint --
        // but `NonceSource::resolve`'s own `hint.max(pending_nonce)` already
        // guarantees that outcome on attempt 1 whether or not the pre-loop
        // seed exists, since attempt 1 recomputes the identical hint from
        // the same, still-unmutated `error`. That made the test vacuous: it
        // passed even with the pre-loop seed deleted.
        //
        // To actually exercise the pre-loop seed, the cache must hold a
        // value *higher* than the triggering hint: only then does
        // `max(hint, cached)` produce a floor attempt 1's own
        // `hint.max(pending_nonce)` could not have produced on its own,
        // proving the seed is load-bearing. `SEED_NONCE` is set well above
        // `HINTED_NONCE`, standing in for a wallet with other unconfirmed
        // sends already queued past the nonce this rejection reports; the
        // attempt-1 pending read is scripted low, so nothing but the
        // pre-loop seed can be responsible for the retry landing above the
        // hint.
        const SEED_NONCE: u64 = HINTED_NONCE + 100;
        let hash = TxHash::repeat_byte(0x3b);
        let mock = MockSubmitter::new(vec![Err(nonce_too_low_with_hint()), Ok(hash)])
            .with_pending_nonce(HINTED_NONCE - 50);

        let (result, _nonce_manager) = run_with_seeded_manager(&mock, SEED_NONCE).await;

        assert_eq!(result.unwrap(), hash);
        let sent = mock.sent();
        assert_eq!(
            sent[0].nonce,
            Some(SEED_NONCE),
            "the base send must consume the seeded cache value through the \
             production nonce filler, exactly as it would in the real wallet"
        );
        assert_eq!(
            sent[1].nonce,
            Some(SEED_NONCE + 1),
            "the retry must land at one past the cached nonce the base send \
             consumed, not merely at the triggering rejection's hint -- only \
             the pre-loop floor seed can produce this, since attempt 1's own \
             hint/pending sourcing tops out at HINTED_NONCE"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn nonce_too_low_retry_seeds_the_floor_from_the_base_sends_own_consumed_nonce() {
        // Regression test for the gap the pre-loop floor seed exists to
        // close: the base send that triggers this recovery consumes a
        // nonce from the cache before it is rejected, but the triggering
        // rejection itself carries no parseable hint (an RPC proxy or
        // non-geth client rephrasing it, an expected outcome per
        // `EvmError::next_nonce_hint`'s doc comment). Without folding the
        // cache's own value into the floor, attempt 1 would seed purely
        // from a fresh `pending_nonce` read, with nothing preventing it
        // landing at or below the nonce the base send just had rejected --
        // scripted here as `DEFAULT_PENDING_NONCE`, far below the seed.
        const SEED_NONCE: u64 = 50_000;
        let hash = TxHash::repeat_byte(0x3c);
        let mock = MockSubmitter::new(vec![Err(nonce_too_low()), Ok(hash)]);

        let (result, _nonce_manager) = run_with_seeded_manager(&mock, SEED_NONCE).await;

        assert_eq!(result.unwrap(), hash);
        let sent = mock.sent();
        assert_eq!(
            sent[0].nonce,
            Some(SEED_NONCE),
            "the base send must consume the seeded cache value through the \
             production nonce filler"
        );
        assert_eq!(
            sent[1].nonce,
            Some(SEED_NONCE + 1),
            "the hint-less retry must be built at least one past the nonce \
             the base send just had rejected, not at the far lower stale \
             pending-nonce read (DEFAULT_PENDING_NONCE)"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn nonce_too_low_re_reads_pending_nonce_fresh_on_every_attempt() {
        // Proves `pending_nonce` is read anew on each attempt rather than
        // cached for the whole recovery. The second pending value (5000) is
        // scripted far above the floor the first rejection establishes
        // (901), so a caching bug that reused the first value (900) would
        // be masked by the floor clamp to 901, not the fresh value 5000 --
        // the two are only equal if the read is stale, so this distinguishes
        // fresh-per-attempt reads from a hoisted-and-cached one.
        const FIRST_PENDING_NONCE: u64 = 900;
        const SECOND_PENDING_NONCE: u64 = 5_000;
        let hash = TxHash::repeat_byte(0x3f);
        let mock = MockSubmitter::new(vec![Err(nonce_too_low()), Err(nonce_too_low()), Ok(hash)])
            .with_pending_nonce_sequence(vec![FIRST_PENDING_NONCE, SECOND_PENDING_NONCE]);

        let (result, _nonce_manager) = run_with_manager(&mock).await;

        assert_eq!(result.unwrap(), hash);
        let sent = mock.sent();
        assert_eq!(sent.len(), 3, "base send + two nonce-too-low retries");
        assert_eq!(
            sent[1].nonce,
            Some(FIRST_PENDING_NONCE),
            "the first retry must use the attempt-1 pending-nonce read"
        );
        assert_eq!(
            sent[2].nonce,
            Some(SECOND_PENDING_NONCE),
            "the second retry must use a freshly re-read attempt-2 pending \
             value rather than the cached attempt-1 value"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn nonce_too_low_retry_failure_returns_error() {
        // Pre-seed a sentinel distinguishable from every other nonce used in
        // this test, so the cache assertion below only passes if
        // `invalidate()` actually cleared it.
        const SEEDED_SENTINEL_NONCE: u64 = 888_888;
        let mock = MockSubmitter::new(vec![Err(nonce_too_low()), Err(reverted())]);

        let (result, nonce_manager) = run_with_seeded_manager(&mock, SEEDED_SENTINEL_NONCE).await;

        let error = result.unwrap_err();
        assert!(
            matches!(&error, EvmError::Transport(rpc) if rpc.as_error_resp().is_some_and(|payload| payload.message.contains("execution reverted"))),
            "expected the retry's error, got {error:?}"
        );
        assert_eq!(mock.sent().len(), 2);
        assert_eq!(
            nonce_manager.peek_next_nonce(WALLET).await,
            None,
            "a hard failure on this branch must invalidate the cache, not leave \
             the pre-seeded sentinel value in place"
        );
    }

    #[tokio::test]
    async fn non_recoverable_error_returns_without_resubmit() {
        let mock = MockSubmitter::new(vec![Err(reverted())]);

        let error = run(&mock).await.unwrap_err();

        assert!(
            matches!(&error, EvmError::Transport(rpc) if rpc.as_error_resp().is_some_and(|payload| payload.message.contains("execution reverted"))),
            "expected the original revert surfaced verbatim, got {error:?}"
        );
        assert_eq!(mock.sent().len(), 1, "no resubmit for a hard revert");
    }

    #[tokio::test]
    async fn estimate_fees_failure_during_resubmit_surfaces_error() {
        // If the fee estimate fails mid-resubmit, the loop aborts and
        // surfaces the RPC error without sending a replacement.
        let mock = MockSubmitter::new(vec![Err(underpriced())]).with_failing_estimate_fees();

        let error = run(&mock).await.unwrap_err();

        assert!(
            matches!(&error, EvmError::Transport(rpc) if rpc.as_error_resp().is_some_and(|payload| payload.message.contains("connection reset"))),
            "expected the fee-estimate RPC error, got {error:?}"
        );
        assert_eq!(
            mock.sent().len(),
            1,
            "no replacement should be sent when the fee estimate fails"
        );
    }

    #[tokio::test]
    async fn assign_nonce_failure_surfaces_before_any_submit() {
        // `resubmit_with_bumped_fee` no longer fetches the nonce itself (it
        // now receives the exact nonce the base send was rejected at), so
        // the only remaining nonce-assignment failure mode is the base
        // send's own `assign_nonce` call, which must surface immediately --
        // there is nothing to resubmit or recover from, since no
        // transaction was ever built or sent.
        let mock = MockSubmitter::new(vec![]).with_failing_assign_nonce();

        let error = run(&mock).await.unwrap_err();

        assert!(
            matches!(&error, EvmError::Transport(rpc) if rpc.as_error_resp().is_some_and(|payload| payload.message.contains("connection reset"))),
            "expected the nonce-assignment RPC error, got {error:?}"
        );
        assert_eq!(
            mock.sent().len(),
            0,
            "no transaction should be submitted when nonce assignment itself fails"
        );
    }

    #[tokio::test]
    async fn fee_bump_overflow_surfaces_hard_error() {
        // An absurd RPC fee estimate must fail loud, never silently wrap.
        let mock = MockSubmitter::new(vec![Err(underpriced()), Ok(TxHash::repeat_byte(0x66))])
            .with_fees(Eip1559Estimation {
                max_fee_per_gas: u128::MAX,
                max_priority_fee_per_gas: u128::MAX,
            });

        let error = run(&mock).await.unwrap_err();

        assert!(
            matches!(error, EvmError::ReplacementFeeOverflow),
            "expected ReplacementFeeOverflow, got {error:?}"
        );
    }

    #[tokio::test]
    async fn nonce_too_low_during_resubmit_surfaces_immediately() {
        // The stuck transaction confirmed while we were escalating, so the
        // pinned nonce is now too low -- stop and surface it.
        let mock = MockSubmitter::new(vec![Err(underpriced()), Err(nonce_too_low())]);

        let error = run(&mock).await.unwrap_err();

        assert!(
            error.is_nonce_too_low(),
            "expected nonce-too-low, got {error:?}"
        );
        assert_eq!(mock.sent().len(), 2, "base send + one resubmit then stop");
    }

    /// `resubmit_with_bumped_fee` must not resend `calldata` at a
    /// retargeted nonce when the fee-bumped replacement itself comes back
    /// "nonce too low": see the function's doc comment for why that would
    /// risk executing the same operation a second time if the incumbent
    /// that just confirmed was this function's own earlier attempt.
    #[tokio::test]
    async fn nonce_too_low_during_resubmit_invalidates_cache_instead_of_retargeting() {
        const SEED_NONCE: u64 = 12_345;
        let mock = MockSubmitter::new(vec![Err(underpriced()), Err(nonce_too_low())]);

        let (result, nonce_manager) = run_with_seeded_manager(&mock, SEED_NONCE).await;

        let error = result.unwrap_err();
        assert!(
            error.is_nonce_too_low(),
            "expected the nonce-too-low rejection surfaced verbatim, got {error:?}"
        );
        assert_eq!(
            mock.sent().len(),
            2,
            "base send + one fee-bumped resubmit, no retargeted retry"
        );
        assert_eq!(
            nonce_manager.peek_next_nonce(WALLET).await,
            None,
            "a nonce-too-low rejection mid fee-escalation must invalidate the \
             cache, not seed a retargeted nonce for a resend that could \
             double-execute this same calldata"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn send_lock_serializes_concurrent_sends() {
        // Two concurrent sends share one lock; each submit holds for 20ms.
        // If serialized, peak concurrency is 1.
        let mock = MockSubmitter::new(vec![
            Ok(TxHash::repeat_byte(0x44)),
            Ok(TxHash::repeat_byte(0x55)),
        ])
        .with_delay(Duration::from_millis(20));

        let nonce_manager = ResettableNonceManager::default();
        let in_flight = InFlightNonces::default();
        let send_lock = Mutex::new(());

        let first = send_with_recovery(
            &mock,
            &nonce_manager,
            &in_flight,
            &send_lock,
            WALLET,
            CONTRACT,
            Bytes::from_static(b"a"),
            "a",
        );
        let second = send_with_recovery(
            &mock,
            &nonce_manager,
            &in_flight,
            &send_lock,
            WALLET,
            CONTRACT,
            Bytes::from_static(b"b"),
            "b",
        );

        let (first, second) = tokio::join!(first, second);
        first.unwrap();
        second.unwrap();

        assert_eq!(
            mock.max_active.load(Ordering::SeqCst),
            1,
            "sends must not overlap while the lock is held"
        );
    }

    /// Anvil is a single geth-style node, so it reports its own unmined
    /// transaction in the `pending`-block transaction count. This test pins
    /// alloy's `.pending()` block-tag encoding against a live node -- it
    /// proves the RPC call and its decoding are correct, nothing more. It
    /// cannot demonstrate cross-node or cross-signer visibility, since the
    /// transaction is submitted and queried through this same single node;
    /// see `TxSubmitter::pending_nonce`'s doc comment for why that pool is
    /// node-local, and effectively private on Base.
    #[tokio::test]
    async fn pending_nonce_reads_the_mempool_on_a_geth_style_node_like_anvil() {
        let anvil = Anvil::new().spawn();
        let address = anvil.addresses()[0];
        let provider = ProviderBuilder::new().connect_http(anvil.endpoint_url());

        provider.anvil_set_auto_mine(false).await.unwrap();

        let unmined_tx = TransactionRequest::default()
            .from(address)
            .to(address)
            .value(U256::ZERO);
        let _unmined_pending_tx = provider.send_transaction(unmined_tx).await.unwrap();

        let latest = provider.latest_nonce(address).await.unwrap();
        let pending = provider.pending_nonce(address).await.unwrap();

        assert_eq!(
            pending,
            latest + 1,
            "an unmined transaction sitting in the mempool must be reflected in the \
             pending count but not the latest (mined) one"
        );
    }

    /// A minimal, well-formed mined receipt for `tx_hash`. Only the shape
    /// matters for `release_in_flight_after_wait`'s tests below -- it never
    /// inspects field values, only whether `Result::Ok` was produced.
    fn mined_receipt(tx_hash: TxHash) -> TransactionReceipt {
        TransactionReceipt {
            inner: ReceiptEnvelope::Eip1559(ReceiptWithBloom {
                receipt: Receipt {
                    status: true.into(),
                    cumulative_gas_used: 0,
                    logs: vec![],
                },
                logs_bloom: Bloom::default(),
            }),
            transaction_hash: tx_hash,
            transaction_index: Some(0),
            block_hash: Some(TxHash::random()),
            block_number: Some(0),
            gas_used: 21_000,
            effective_gas_price: 1,
            blob_gas_used: None,
            blob_gas_price: None,
            from: Address::ZERO,
            to: Some(Address::ZERO),
            contract_address: None,
        }
    }

    #[tokio::test]
    async fn release_in_flight_after_wait_releases_on_confirm() {
        let in_flight = InFlightNonces::default();
        let send_lock = Mutex::new(());
        let tx_hash = TxHash::repeat_byte(0xa1);
        in_flight.record(WALLET, STUCK_NONCE, tx_hash);

        let result: Result<TransactionReceipt, EvmError> = Ok(mined_receipt(tx_hash));
        release_in_flight_after_wait(&in_flight, &send_lock, WALLET, tx_hash, &result).await;

        assert_eq!(
            in_flight.ownership(WALLET, STUCK_NONCE),
            NonceOwnership::Unknown,
            "a mined receipt is definitive: the entry must be released, \
             leaving this now-freed nonce unrecorded"
        );
    }

    #[tokio::test]
    async fn release_in_flight_after_wait_releases_on_dropped() {
        let in_flight = InFlightNonces::default();
        let send_lock = Mutex::new(());
        let tx_hash = TxHash::repeat_byte(0xa2);
        in_flight.record(WALLET, STUCK_NONCE, tx_hash);

        let result: Result<TransactionReceipt, EvmError> = Err(EvmError::TransactionDropped {
            tx_hash,
            elapsed_secs: 42,
        });
        release_in_flight_after_wait(&in_flight, &send_lock, WALLET, tx_hash, &result).await;

        assert_eq!(
            in_flight.ownership(WALLET, STUCK_NONCE),
            NonceOwnership::Unknown,
            "a transaction proven dropped from both the receipt lookup and the \
             mempool is definitive in the other direction: the entry must \
             still be released, leaving this now-freed nonce unrecorded"
        );
    }

    #[tokio::test]
    async fn release_in_flight_after_wait_does_not_release_on_timeout() {
        let in_flight = InFlightNonces::default();
        let send_lock = Mutex::new(());
        let tx_hash = TxHash::repeat_byte(0xa3);
        in_flight.record(WALLET, STUCK_NONCE, tx_hash);

        let result: Result<TransactionReceipt, EvmError> = Err(EvmError::ReceiptTimeout {
            tx_hash,
            timeout_secs: 120,
        });
        release_in_flight_after_wait(&in_flight, &send_lock, WALLET, tx_hash, &result).await;

        assert_eq!(
            in_flight.ownership(WALLET, STUCK_NONCE),
            NonceOwnership::Ours,
            "a timeout proves nothing -- the transaction may still mine after \
             the wait gives up, so the entry must survive so a later confirm \
             or drop can resolve it, and so a later underpriced rejection at \
             this nonce is not misclassified as external contention"
        );
    }

    #[tokio::test]
    async fn release_in_flight_after_wait_does_not_release_on_other_errors() {
        let in_flight = InFlightNonces::default();
        let send_lock = Mutex::new(());
        let tx_hash = TxHash::repeat_byte(0xa4);
        in_flight.record(WALLET, STUCK_NONCE, tx_hash);

        let result: Result<TransactionReceipt, EvmError> = Err(reverted());
        release_in_flight_after_wait(&in_flight, &send_lock, WALLET, tx_hash, &result).await;

        assert_eq!(
            in_flight.ownership(WALLET, STUCK_NONCE),
            NonceOwnership::Ours,
            "a non-decisive error (e.g. a transport failure after exhausting \
             the transport-error cap) proves nothing about the transaction \
             either, so the entry must not be released"
        );
    }
}

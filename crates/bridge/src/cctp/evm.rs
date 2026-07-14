//! Single-chain CCTP operations.

use alloy::primitives::{Address, B256, Bytes, FixedBytes, TxHash, U256};
use alloy::providers::Provider;
use alloy::rpc::types::{Filter, TransactionReceipt};
use alloy::sol;
use alloy::sol_types::{SolCall, SolEvent};
use std::future::Future;
use std::num::NonZeroU32;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::{Instant, MissedTickBehavior, interval};
use tracing::{debug, info, trace, warn};

#[cfg(test)]
use st0x_evm::Evm;
use st0x_evm::{
    Chain, EvmError, IntoErrorRegistry, MinedTx, NODE_SYNC_MAX_ATTEMPTS, NODE_SYNC_POLL_INTERVAL,
    PreparedTransaction, TransactionSubmission, Wallet, qualified_absence_head, wait_for_node_sync,
};

use super::{
    CctpError, CctpReceivedMessage, FAST_TRANSFER_THRESHOLD, MessageTransmitterV2, MintReceipt,
    MintScanFloorCheck, TokenMessengerV2, UsdcTransferStatus, parse_received_message,
};
use crate::BridgeDirection;

const CCTP_RECOVERY_LOG_BLOCK_CHUNK: u64 = 20_000;

/// Approve this amount to the CCTP TokenMessenger as the standing allowance.
///
/// `U256::MAX` is the Circle-standard CCTP integration pattern.
///
/// Note: FiatToken v2.2 (Circle's USDC implementation) unconditionally
/// decrements allowances in `_transferFrom` with no `type(uint256).max`
/// shortcut, so `U256::MAX` is decremented on every burn. This makes the
/// threshold top-up in [`ensure_standing_allowance`] a real code path, not
/// dead code. That said, the `TARGET / 2` threshold means correctness does not
/// depend solely on this assumption: even if a future USDC version added a
/// max-allowance shortcut the approve fires at most once per cold path anyway.
/// At realistic rebalancing sizes the allowance never drops below
/// [`STANDING_ALLOWANCE_THRESHOLD`], so the approve fires exactly once (at first
/// use or after a manual reset) and never on the hot path.
const STANDING_ALLOWANCE_TARGET: U256 = U256::MAX;

/// Top up the standing allowance when it falls below this threshold.
///
/// Equal to `STANDING_ALLOWANCE_TARGET / 2` = `U256::MAX / 2`. The standing
/// allowance model is safe because the SPEC guarantees a single USDC rebalance
/// in flight at a time (one burn at a time), so two concurrent burns cannot race
/// the same allowance. With that invariant, and at realistic rebalancing sizes,
/// this threshold is never reached in normal operation.
///
/// Expressed as `U256::from_limbs(...)` because `ruint`'s `Div` is not `const
/// fn`. A test pins `STANDING_ALLOWANCE_TARGET` against an independent
/// `from_limbs` literal and `STANDING_ALLOWANCE_THRESHOLD` against a runtime
/// `U256::MAX / 2` computation, so any divergence between the encoding and
/// mathematical intent is caught.
const STANDING_ALLOWANCE_THRESHOLD: U256 =
    U256::from_limbs([u64::MAX, u64::MAX, u64::MAX, u64::MAX >> 1]);

sol!(
    #![sol(all_derives = true, rpc)]
    #[derive(serde::Serialize, serde::Deserialize)]
    IERC20, env!("ST0X_IERC20_ABI")
);

/// Number of `eth_getLogs` scans that must agree a burn is absent before a
/// resume re-issues an irreversible burn. Defends against a single load-balanced
/// RPC node lagging and returning a false-empty result.
const SCAN_ATTEMPTS: u32 = 5;

/// Backoff between scan retries; different load-balanced nodes may answer each.
const SCAN_RETRY_BACKOFF: std::time::Duration = std::time::Duration::from_millis(150);

/// Blocks the chain head must be past `from_block` before an empty scan is
/// trusted as a true absence (the burn lands at/after `from_block`).
const SCAN_FINALITY_MARGIN: u64 = 2;

/// How far back [`CctpEndpoint::reconstruct_existing_mint`] scans from the
/// head. It runs only after [`CctpEndpoint::recover_already_minted`] saw
/// `usedNonces()` flip to consumed within its ~2 minute window, so the log is
/// within minutes of the head; this bounds the cost of a lagging node.
const RECONSTRUCTION_SCAN_LOOKBACK: Duration = Duration::from_secs(33 * 60 * 60 + 20 * 60);

/// How far back [`CctpEndpoint::find_existing_mint`] always looks from the
/// head, below a captured floor too: that floor is read after Circle attests,
/// so a relayer's mint can predate it. Above the 24 h attestation deadline; a
/// mint older than both is left to the operator.
const MINT_SCAN_LOOKBACK: Duration = Duration::from_secs(33 * 60 * 60 + 20 * 60);

/// How far below a captured floor [`CctpEndpoint::find_existing_mint`] scans,
/// for a relayer mint between Circle's attestation and the floor capture. The
/// bot polls attestations every 5 s, so this is many polls and job retries.
const CAPTURED_FLOOR_MARGIN: Duration = Duration::from_secs(10 * 60);

/// The mint scan bounds in one chain's blocks: the time spans above divided
/// by the chain's fastest block cadence, rounded up.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ScanWindow {
    pub(crate) floor_margin: u64,
    pub(crate) mint_lookback: u64,
    pub(crate) reconstruction_lookback: u64,
}

impl ScanWindow {
    pub(crate) const fn for_chain(chain: Chain) -> Self {
        let interval = whole_millis(chain.min_block_interval());

        Self {
            floor_margin: whole_millis(CAPTURED_FLOOR_MARGIN).div_ceil(interval),
            mint_lookback: whole_millis(MINT_SCAN_LOOKBACK).div_ceil(interval),
            reconstruction_lookback: whole_millis(RECONSTRUCTION_SCAN_LOOKBACK).div_ceil(interval),
        }
    }
}

/// Whole milliseconds in `duration`, saturating far above any span here.
const fn whole_millis(duration: Duration) -> u64 {
    duration
        .as_secs()
        .saturating_mul(1000)
        .saturating_add(duration.subsec_millis() as u64)
}

/// Delay between the `usedNonces()` probes that
/// [`CctpEvm::recover_already_minted`] runs after a failed `receiveMessage`.
///
/// This is the production default (see [`MintRecoveryConfig::defaults`]);
/// tests override the cadence via
/// [`CctpEndpoint::with_mint_recovery_config`] to pin behaviour against a
/// short, deterministic interval instead of racing or pausing real time
/// against this multi-minute production value.
const MINT_RECOVERY_PROBE_INTERVAL: Duration = Duration::from_secs(10);

/// Number of `usedNonces()` probes after a failed `receiveMessage`. The first
/// is immediate and the rest are spaced by [`MINT_RECOVERY_PROBE_INTERVAL`], so
/// together they span ~2 minutes (production default; see
/// [`MintRecoveryConfig`]).
///
/// How long [`CctpEvm::recover_already_minted`] keeps re-probing before it
/// concludes an attested message was not minted by anyone is sized for a
/// third-party relayer to deliver `receiveMessage` after our own submission
/// failed: the burn is irreversible and the attestation is valid, so a mint
/// can still land after our own submission fails. This bound is a **chosen**
/// trade-off, not a measured one: the sole production data point behind it is
/// a single incident where a third-party mint landed ~10 seconds after our
/// submission failed, and two minutes is a 12x pad over that one observation,
/// not a figure derived from Circle's CCTP V2 docs or a logged distribution of
/// attestation-to-mint deltas.
///
/// The trade-off is asymmetric and deliberately erred toward the safer side:
/// too short strands the rebalancing guard for hours (the original incident
/// this window fixes -- an operator must reconcile by hand), while too long
/// only delays a genuinely terminal failure by extra minutes on top of an
/// already-failed transfer. If a relayer is ever observed delivering later
/// than this window, widen it rather than assume the failure is terminal.
///
/// Non-zero by construction: a zero probe count would skip the probe loop
/// entirely and declare the "nonce never consumed" terminal outcome without a
/// single `usedNonces()` read -- precisely the false negative
/// [`CctpEvm::recover_already_minted`] exists to prevent. The `match` (rather
/// than an `unwrap`) keeps the check a compile-time evaluation with no runtime
/// panic path.
const MINT_RECOVERY_PROBES: NonZeroU32 = match NonZeroU32::new(13) {
    Some(probes) => probes,
    None => panic!("MINT_RECOVERY_PROBES must be non-zero"),
};

/// Tuning knobs for [`CctpEndpoint::recover_already_minted`]'s probe cadence.
/// Bundled into a struct, mirroring [`BurnDropConfig`], so tests can drive a
/// short, deterministic cadence against real awaited state instead of racing
/// or pausing the clock through production's multi-minute window (see
/// [`MINT_RECOVERY_PROBES`]).
#[derive(Debug, Clone, Copy)]
pub(super) struct MintRecoveryConfig {
    /// Delay between `usedNonces()` probes.
    pub(super) probe_interval: Duration,
    /// Number of probes; the first is immediate, the rest spaced by
    /// `probe_interval`. Non-zero so that a configured cadence can never skip
    /// the probe loop and reach the terminal "never minted" outcome without a
    /// single `usedNonces()` read (see [`MINT_RECOVERY_PROBES`]).
    pub(super) probes: NonZeroU32,
}

impl MintRecoveryConfig {
    /// Production cadence: [`MINT_RECOVERY_PROBES`] probes spaced
    /// [`MINT_RECOVERY_PROBE_INTERVAL`] apart.
    const fn defaults() -> Self {
        Self {
            probe_interval: MINT_RECOVERY_PROBE_INTERVAL,
            probes: MINT_RECOVERY_PROBES,
        }
    }

    /// Two probes 10 ms apart, for downstream tests (via
    /// [`with_fast_mint_recovery_policy`][fast_policy]).
    ///
    /// [fast_policy]: super::CctpBridge::with_fast_mint_recovery_policy
    #[cfg(any(test, feature = "test-support"))]
    pub(super) const fn fast() -> Self {
        Self {
            probe_interval: Duration::from_millis(10),
            probes: match NonZeroU32::new(2) {
                Some(probes) => probes,
                None => panic!("fast mint recovery probes must be non-zero"),
            },
        }
    }
}

/// Grace period before [`CctpEndpoint::burn_status`] may conclude a broadcast
/// burn tx was dropped from the mempool. Mirrors the wallet's `wait_for_receipt`
/// `DROPPED_TX_GRACE`: a freshly-broadcast tx is not visible on every node of a
/// load-balanced RPC for several seconds, and is not mined for ~1 block, so
/// concluding "dropped" earlier risks re-burning a tx that actually lands.
const BURN_DROP_GRACE: Duration = Duration::from_secs(30);

/// Consecutive `get_transaction_by_hash == None` observations (after the grace
/// period) required before [`CctpEndpoint::burn_status`] concludes the burn tx
/// was dropped. The count is strictly consecutive because any sighting -- a
/// mempool hit or a mined receipt -- returns from the poll loop (`Pending` or
/// `Mined*`) rather than continuing it, so the misses can never be interleaved
/// with a sighting. Mirrors the wallet's `wait_for_receipt`
/// `DROPPED_TX_CONSECUTIVE_MISSES`, debouncing the load-balanced-RPC race where a
/// single lagging node transiently reports a live tx as absent.
const BURN_DROP_CONSECUTIVE_MISSES: u32 = 3;

/// Poll interval while [`CctpEndpoint::burn_status`] waits out the drop grace
/// window. Matches the wallet's `wait_for_receipt` poll cadence.
const BURN_DROP_POLL_INTERVAL: Duration = Duration::from_secs(2);

/// Tuning knobs for [`CctpEndpoint::burn_status`]'s conservative drop policy.
/// Bundled into a struct so a fast variant can be injected in tests without a
/// long argument list, mirroring the wallet crate's `ReceiptWaitConfig`.
#[derive(Debug, Clone, Copy)]
pub(super) struct BurnDropConfig {
    /// Grace period before an absent tx may be suspected dropped.
    pub(super) grace: Duration,
    /// Consecutive mempool-absence observations required to conclude a drop.
    pub(super) consecutive_misses: u32,
    /// Interval between status polls while waiting out the grace window.
    pub(super) poll_interval: Duration,
}

impl BurnDropConfig {
    /// Defaults mirroring the wallet's `wait_for_receipt` drop policy.
    const fn defaults() -> Self {
        Self {
            grace: BURN_DROP_GRACE,
            consecutive_misses: BURN_DROP_CONSECUTIVE_MISSES,
            poll_interval: BURN_DROP_POLL_INTERVAL,
        }
    }

    /// Zero-grace, single-miss variant for tests. It still requires trusted
    /// nonce state and head progress; unknown or frozen evidence is Pending.
    #[cfg(any(test, feature = "test-support"))]
    pub(super) const fn fast() -> Self {
        Self {
            grace: Duration::ZERO,
            consecutive_misses: 1,
            poll_interval: Duration::from_millis(10),
        }
    }
}

/// Maps a sync result during the allowance retry to the error returned on
/// failure, preserving the original burn error as the actionable root cause.
///
/// On sync failure (`Err(sync_err)`), returns `Err(original_error)` — the
/// original burn revert is what the job retry queue and operator logs need to
/// diagnose, not the sync error. The sync error is already logged by the caller.
///
/// On sync success (`Ok(())`), returns `Ok(())` so the retry burn can proceed.
///
/// This pure function exists to make the error-selection logic unit-testable
/// independently of the async I/O path.
pub(super) fn apply_sync_result(
    original_error: CctpError,
    sync_result: Result<(), CctpError>,
) -> Result<(), CctpError> {
    sync_result.map_err(|_sync_err| original_error)
}

/// Single-chain CCTP endpoint with contract instances for cross-chain operations.
///
/// The wallet's provider is used for read-only view calls (e.g. allowance
/// checks). All write operations are submitted through the [`Wallet`] trait.
pub(crate) struct CctpEndpoint<W: Wallet> {
    /// USDC token address
    usdc_address: Address,
    /// TokenMessengerV2 contract address
    token_messenger_address: Address,
    /// MessageTransmitterV2 contract address
    message_transmitter_address: Address,
    /// Wallet for submitting write transactions
    wallet: W,
    /// Mint scan bounds in this chain's blocks.
    scan_window: ScanWindow,
    /// Poll interval between `eth_blockNumber` calls in [`wait_for_node_sync`].
    ///
    /// Production always uses [`NODE_SYNC_POLL_INTERVAL`]. Tests override it to
    /// `Duration::ZERO` to avoid sleeping through the 30-attempt budget.
    node_sync_poll_interval: Duration,
    /// Drop policy for [`burn_status`](Self::burn_status). Production uses
    /// [`BurnDropConfig::defaults`]; tests override it via
    /// [`with_burn_drop_config`](Self::with_burn_drop_config) so the drop grace
    /// resolves immediately instead of after the production 30 s window.
    burn_drop_config: BurnDropConfig,
    /// Retain evidence, not a verdict or nonce reservation, after a generic wallet drop.
    /// One slot is bounded; replacing it can only leave an older burn conservatively pending.
    suspected_drop_submission: Mutex<Option<(TxHash, TransactionSubmission)>>,
    /// Probe cadence for
    /// [`recover_already_minted`](Self::recover_already_minted). Production
    /// uses [`MintRecoveryConfig::defaults`]; tests override it via
    /// [`with_mint_recovery_config`](Self::with_mint_recovery_config) to drive
    /// a short cadence against real awaited state instead of racing or
    /// pausing the production multi-minute window.
    mint_recovery_config: MintRecoveryConfig,
}

impl<W: Wallet> CctpEndpoint<W> {
    /// Creates a new CCTP endpoint from a wallet and contract addresses.
    ///
    /// The wallet's provider is used for read-only view calls.
    /// The wallet itself handles signing and submission of write transactions.
    pub(crate) fn new(
        chain: Chain,
        usdc: Address,
        token_messenger: Address,
        message_transmitter: Address,
        wallet: W,
    ) -> Self {
        Self {
            usdc_address: usdc,
            token_messenger_address: token_messenger,
            message_transmitter_address: message_transmitter,
            wallet,
            scan_window: ScanWindow::for_chain(chain),
            node_sync_poll_interval: NODE_SYNC_POLL_INTERVAL,
            burn_drop_config: BurnDropConfig::defaults(),
            suspected_drop_submission: Mutex::new(None),
            mint_recovery_config: MintRecoveryConfig::defaults(),
        }
    }

    /// Ensures a standing `U256::MAX` allowance from the wallet to the
    /// TokenMessenger. This is a no-op (fast path) when the allowance is at or
    /// above [`STANDING_ALLOWANCE_THRESHOLD`]; on the slow path it approves
    /// `STANDING_ALLOWANCE_TARGET` and waits until at least one poll through the
    /// load-balanced endpoint returns a block at or above the approve block before
    /// returning, reducing (but not eliminating) the chance that a subsequent
    /// `depositForBurn` pre-flight hits a lagging node.
    ///
    /// The node-sync wait significantly reduces the dRPC load-balancing race
    /// window: a `depositForBurn` pre-flight `eth_call` can still hit a lagging
    /// node, but the defense-in-depth retry in
    /// [`deposit_for_burn_with_allowance_retry`] handles that case.
    pub(super) async fn ensure_standing_allowance<Registry: IntoErrorRegistry>(
        &self,
    ) -> Result<(), CctpError> {
        let allowance = self
            .wallet
            .call::<Registry, _>(
                self.usdc_address,
                IERC20::allowanceCall {
                    owner: self.wallet.address(),
                    spender: self.token_messenger_address,
                },
            )
            .await?;

        if allowance >= STANDING_ALLOWANCE_THRESHOLD {
            trace!(
                target: "bridge",
                ?allowance,
                "USDC allowance at or above standing threshold; skipping approve"
            );
            return Ok(());
        }

        info!(
            target: "bridge",
            ?allowance,
            standing_target = ?STANDING_ALLOWANCE_TARGET,
            "USDC allowance below standing threshold; approving U256::MAX to TokenMessenger"
        );

        let receipt = self
            .wallet
            .submit::<Registry, _>(
                self.usdc_address,
                IERC20::approveCall {
                    spender: self.token_messenger_address,
                    amount: STANDING_ALLOWANCE_TARGET,
                },
                "USDC standing allowance approve for CCTP",
            )
            .await?;

        let approve_block = receipt
            .block_number
            .ok_or(CctpError::TxReceiptMissingBlock {
                tx_hash: receipt.transaction_hash,
            })?;

        // Wait until at least one poll through the load-balanced endpoint returns
        // a block at or above the approve block. This reduces (but does not
        // eliminate) the chance that the subsequent depositForBurn pre-flight
        // eth_call hits a lagging node; the defense-in-depth retry handles
        // the residual race.
        wait_for_node_sync(
            self.wallet.provider(),
            approve_block,
            self.node_sync_poll_interval,
            NODE_SYNC_MAX_ATTEMPTS,
        )
        .await?;

        Ok(())
    }

    /// Submits a `depositForBurn` and retries once if it reverts.
    ///
    /// Test-only helper that exercises the endpoint-level retry path in isolation
    /// (without the fee re-query that `CctpBridge::retry_burn_if_revert` performs).
    /// Production callers use `CctpBridge::burn_internal`, which re-queries the
    /// Circle fast-transfer fee before the retry burn to avoid a stale fee bound.
    ///
    /// The retry is triggered by any revert-class failure (not by a post-revert
    /// allowance re-read). On a revert-class error nothing was minted, so one retry
    /// cannot double-burn. The one-shot bound prevents loops. If the second burn
    /// also reverts that error is final and propagates to the caller.
    ///
    /// Non-revert errors (transport timeouts, RPC connection failures) are NOT
    /// retried — `is_revert()` distinguishes them from EVM reverts.
    ///
    /// Note: `max_fee` is used as-is for the retry burn. On the cold
    /// `ensure_standing_allowance` path (~30 s sync wait), a Circle fee spike
    /// could make this stale. Production code re-queries the fee; this helper
    /// does not, which is acceptable for the unit-test scenarios it covers.
    ///
    /// If `ensure_standing_allowance` fails during the retry, the original burn
    /// revert is returned rather than the sync error — the burn revert is the
    /// actionable root cause that the job retry queue and operator logs need to see.
    #[cfg(test)]
    pub(super) async fn deposit_for_burn_with_allowance_retry<Registry: IntoErrorRegistry>(
        &self,
        amount: U256,
        recipient: Address,
        direction: BridgeDirection,
        max_fee: U256,
    ) -> Result<crate::BurnReceipt, CctpError> {
        let first_result = self
            .deposit_for_burn::<Registry>(amount, recipient, direction, max_fee)
            .await;

        let Err(original_error) = first_result else {
            return first_result;
        };

        // Only retry on revert-class errors. Transport or non-revert errors are
        // not allowance-related; propagate immediately.
        if !original_error.is_revert() {
            return Err(original_error);
        }

        warn!(
            target: "bridge",
            ?original_error,
            "depositForBurn reverted; re-running ensure_standing_allowance and retrying once"
        );

        // Re-run ensure_standing_allowance (cheap no-op on the hot path when
        // allowance is already MAX; approves and syncs on the cold path). If sync
        // fails, return the original burn revert — not the sync error — so the
        // caller and operator logs see the actionable root cause.
        let sync_result = self.ensure_standing_allowance::<Registry>().await;

        if let Err(sync_err) = &sync_result {
            warn!(
                target: "bridge",
                ?sync_err,
                ?original_error,
                "node-sync gate failed during allowance retry; returning original burn revert"
            );
        }

        apply_sync_result(original_error, sync_result)?;

        self.deposit_for_burn::<Registry>(amount, recipient, direction, max_fee)
            .await
    }

    pub(super) async fn deposit_for_burn<Registry: IntoErrorRegistry>(
        &self,
        amount: U256,
        recipient: Address,
        direction: BridgeDirection,
        max_fee: U256,
    ) -> Result<crate::BurnReceipt, CctpError> {
        info!(target: "bridge", %max_fee, %amount, "Depositing for burn with fast transfer");

        let receipt = self
            .wallet
            .submit::<Registry, _>(
                self.token_messenger_address,
                self.deposit_for_burn_call(amount, recipient, direction, max_fee),
                "depositForBurn",
            )
            .await?;

        if !receipt
            .inner
            .logs()
            .iter()
            .any(|log| MessageTransmitterV2::MessageSent::decode_log(log.as_ref()).is_ok())
        {
            return Err(CctpError::MessageSentEventNotFound {
                tx_hash: receipt.transaction_hash,
            });
        }

        Ok(crate::BurnReceipt {
            tx: receipt.transaction_hash,
            amount,
        })
    }

    /// Builds the `depositForBurn` call for a fast CCTP transfer, shared by the
    /// atomic [`deposit_for_burn`](Self::deposit_for_burn) and the two-phase
    /// [`submit_deposit_for_burn`](Self::submit_deposit_for_burn).
    fn deposit_for_burn_call(
        &self,
        amount: U256,
        recipient: Address,
        direction: BridgeDirection,
        max_fee: U256,
    ) -> TokenMessengerV2::depositForBurnCall {
        let recipient_bytes32 = FixedBytes::<32>::left_padding_from(recipient.as_slice());

        // bytes32(0) allows any address to call receiveMessage() on destination.
        // See: https://github.com/circlefin/evm-cctp-contracts/blob/master/src/TokenMessenger.sol
        let destination_caller = FixedBytes::<32>::ZERO;

        TokenMessengerV2::depositForBurnCall {
            amount,
            destinationDomain: direction.dest_domain(),
            mintRecipient: recipient_bytes32,
            burnToken: self.usdc_address,
            destinationCaller: destination_caller,
            maxFee: max_fee,
            minFinalityThreshold: FAST_TRANSFER_THRESHOLD,
        }
    }

    /// Broadcasts `depositForBurn` and returns its tx hash WITHOUT awaiting the
    /// receipt, so the caller can record the broadcast hash before confirming it
    /// (closing the double-burn window). Pair with
    /// [`confirm_burn`](Self::confirm_burn) to await and validate the receipt.
    pub(super) async fn submit_deposit_for_burn(
        &self,
        amount: U256,
        recipient: Address,
        direction: BridgeDirection,
        max_fee: U256,
    ) -> Result<TxHash, CctpError> {
        info!(target: "bridge", %max_fee, %amount, "Submitting depositForBurn (pending) for fast transfer");

        Ok(self
            .wallet
            .submit_pending(
                self.token_messenger_address,
                self.deposit_for_burn_call(amount, recipient, direction, max_fee),
                "depositForBurn",
            )
            .await?)
    }

    /// Awaits the receipt of a burn broadcast via
    /// [`submit_deposit_for_burn`](Self::submit_deposit_for_burn), decoding a
    /// revert, and validates the CCTP `MessageSent` event is present. `amount` is
    /// the burned input amount, carried through onto the returned [`BurnReceipt`]
    /// (mirroring [`deposit_for_burn`](Self::deposit_for_burn), whose receipt
    /// records the input amount, not the net-of-fee minted amount).
    pub(super) async fn confirm_burn<Registry: IntoErrorRegistry>(
        &self,
        tx_hash: TxHash,
        amount: U256,
    ) -> Result<crate::BurnReceipt, CctpError> {
        let receipt = self
            .wait_for_burn_receipt(tx_hash, self.wallet.confirm::<Registry>(tx_hash))
            .await?;

        if !receipt
            .inner
            .logs()
            .iter()
            .any(|log| MessageTransmitterV2::MessageSent::decode_log(log.as_ref()).is_ok())
        {
            return Err(CctpError::MessageSentEventNotFound { tx_hash });
        }

        Ok(crate::BurnReceipt {
            tx: tx_hash,
            amount,
        })
    }

    async fn clear_suspected_drop(&self, tx_hash: TxHash) {
        let mut retained = self.suspected_drop_submission.lock().await;
        if retained.as_ref().is_some_and(|(hash, _)| *hash == tx_hash) {
            *retained = None;
        }
    }

    async fn wait_for_burn_receipt(
        &self,
        tx_hash: TxHash,
        wait: impl Future<Output = Result<TransactionReceipt, EvmError>> + Send,
    ) -> Result<TransactionReceipt, EvmError> {
        let submission = self.wallet.transaction_submission(tx_hash);
        let result = wait.await;
        match &result {
            Ok(_) => self.clear_suspected_drop(tx_hash).await,
            Err(error) if error.is_transaction_dropped() => {
                match submission {
                    Some(submission) if submission.submitted_after_block.is_some() => {
                        *self.suspected_drop_submission.lock().await = Some((tx_hash, submission));
                    }
                    // Failed rebroadcast evidence must not resurrect an older boundary.
                    Some(_) => self.clear_suspected_drop(tx_hash).await,
                    None => {}
                }
            }
            Err(error) if error.is_revert() => self.clear_suspected_drop(tx_hash).await,
            Err(_) => {}
        }
        result
    }

    /// Resolves the on-chain status of a broadcast burn tx for crash-safe resume,
    /// using this endpoint's configured drop policy (`burn_drop_config`).
    ///
    /// `submitted_after_block` is the transfer's additional lower bound. It
    /// cannot replace complete wallet submission evidence: absence qualification
    /// uses the higher of that bound and the wallet's observed broadcast head.
    pub(super) async fn burn_status(
        &self,
        tx_hash: TxHash,
        submitted_after_block: u64,
    ) -> Result<crate::BurnTxStatus, CctpError> {
        self.burn_status_with_config(tx_hash, submitted_after_block, self.burn_drop_config)
            .await
    }

    /// Conservative drop detection: an independent grace + consecutive-miss poll
    /// loop modeled on the same drop policy as the wallet's `wait_for_receipt`
    /// (not a call into that function). Polls in a loop at `config.poll_interval`
    /// (first tick immediate) and resolves as follows:
    ///
    /// - a receipt arriving at any tick -> [`BurnTxStatus::MinedSuccess`] /
    ///   [`BurnTxStatus::MinedReverted`] (returns immediately)
    /// - no receipt, tx still visible via `get_transaction_by_hash` (mempool) ->
    ///   [`BurnTxStatus::Pending`] (returns immediately)
    /// - no receipt and tx absent from the mempool -> keeps polling with known
    ///   submission evidence; only once `config.grace` has elapsed AND
    ///   `config.consecutive_misses` consecutive post-grace absences are
    ///   qualified against an advancing canonical head beyond the submission
    ///   boundary and its unused sender nonce does it return suspected
    ///   [`BurnTxStatus::Dropped`]. Unknown identity, consumed nonce, missing
    ///   state or frozen/lagging heads remain [`BurnTxStatus::Pending`]. Head
    ///   progress starts after grace and has a further bounded observation
    ///   window: the larger of grace or enough polls for progress plus misses.
    ///   The verdict is not proof of global mempool absence and never permits
    ///   an automatic reburn.
    pub(super) async fn burn_status_with_config(
        &self,
        tx_hash: TxHash,
        submitted_after_block: u64,
        config: BurnDropConfig,
    ) -> Result<crate::BurnTxStatus, CctpError> {
        let provider = self.wallet.provider();
        let start = Instant::now();
        let observation_window = (0..SCAN_FINALITY_MARGIN)
            .fold(
                config
                    .poll_interval
                    .saturating_mul(config.consecutive_misses),
                |window, _| window.saturating_add(config.poll_interval),
            )
            .max(config.grace);
        let observation_limit = config.grace.saturating_add(observation_window);
        let mut consecutive_misses = 0u32;
        let mut reference_head = None;
        let mut poll = interval(config.poll_interval);
        poll.set_missed_tick_behavior(MissedTickBehavior::Delay);

        loop {
            // First tick completes immediately, so the receipt/mempool checks run
            // before any sleep; subsequent ticks pace the drop-grace polling.
            poll.tick().await;

            if let Some(receipt) = provider.get_transaction_receipt(tx_hash).await? {
                if receipt.status() {
                    // Success is re-validated downstream by `confirm_burn` (which waits
                    // the wallet's confirmation depth), so a premature success here is
                    // safe to return immediately.
                    return Ok(crate::BurnTxStatus::MinedSuccess);
                }
                // A reverted receipt drives the caller to reburn (a reverted burn moved
                // no funds), but a shallow reorg could re-include this exact tx as a
                // SUCCESS -- reburning before the revert is confirmation-deep would then
                // double-burn. Re-fetch through the wallet's confirmation-aware path
                // (mirroring the success path) before trusting the revert.
                let confirmed = match self
                    .wait_for_burn_receipt(tx_hash, self.wallet.await_receipt(tx_hash))
                    .await
                {
                    Ok(confirmed) => confirmed,
                    Err(error) if error.is_transaction_dropped() => {
                        return Ok(crate::BurnTxStatus::Dropped);
                    }
                    Err(error) => return Err(error.into()),
                };
                return Ok(if confirmed.status() {
                    crate::BurnTxStatus::MinedSuccess
                } else {
                    crate::BurnTxStatus::MinedReverted
                });
            }

            // No receipt. A tx still known to the node (mempool) may yet mine, so
            // it is Pending and must NOT be re-burned.
            if provider.get_transaction_by_hash(tx_hash).await?.is_some() {
                return Ok(crate::BurnTxStatus::Pending);
            }

            // Qualify independent receipt/mempool misses with sender state at
            // the exact canonical head hash. A separate latest nonce read can
            // hit a stale backend and falsely authenticate a hidden mined tx.
            let retained = *self.suspected_drop_submission.lock().await;
            let Some(mut submission) = self.wallet.transaction_submission(tx_hash).or_else(|| {
                retained
                    .filter(|(hash, _)| *hash == tx_hash)
                    .map(|(_, submission)| submission)
            }) else {
                warn!(%tx_hash, "No recorded sender nonce for burn; absence remains pending");
                return Ok(crate::BurnTxStatus::Pending);
            };
            let Some(tracked_floor) = submission.submitted_after_block else {
                warn!(%tx_hash, sender = %submission.sender, nonce = submission.nonce,
                    "No recorded submission boundary for burn; caller floor cannot qualify absence");
                return Ok(crate::BurnTxStatus::Pending);
            };
            submission.submitted_after_block = Some(tracked_floor.max(submitted_after_block));
            let head = match qualified_absence_head(provider, submission, SCAN_FINALITY_MARGIN)
                .await
            {
                Ok(Some(head)) => head,
                Ok(None) => {
                    consecutive_misses = 0;
                    reference_head = None;
                    if start.elapsed() >= observation_limit {
                        return Ok(crate::BurnTxStatus::Pending);
                    }
                    continue;
                }
                Err(error) => {
                    warn!(?error, %tx_hash, "Cannot qualify burn absence against canonical nonce state");
                    return Ok(crate::BurnTxStatus::Pending);
                }
            };
            if start.elapsed() < config.grace {
                continue;
            }

            // Advancement before grace is not freshness evidence for the
            // absence run being judged now. Give the post-grace reference a
            // bounded opportunity to advance, including zero-grace test seams.
            let reference = *reference_head.get_or_insert(head);
            if head.saturating_sub(reference) < SCAN_FINALITY_MARGIN {
                consecutive_misses = 0;
                if start.elapsed() >= observation_limit {
                    return Ok(crate::BurnTxStatus::Pending);
                }
                continue;
            }

            // Keep polling within this call rather than returning early: only
            // after grace AND consecutive qualified misses is a drop suspected.
            // This never authorizes automatically submitting another burn.
            consecutive_misses += 1;
            if consecutive_misses >= config.consecutive_misses {
                return Ok(crate::BurnTxStatus::Dropped);
            }
            if start.elapsed() >= observation_limit {
                return Ok(crate::BurnTxStatus::Pending);
            }
        }
    }

    /// Scans for a `DepositForBurn` event from this endpoint's wallet strictly
    /// after `from_block`, returning every candidate newest first.
    ///
    /// Crash-safe burn recovery: a transfer records the chain head before the
    /// burn, so on resume this detects an already-submitted burn instead of
    /// re-burning (which would burn USDC twice with at most one mint). Matching
    /// `(depositor, amount, destinationDomain, mintRecipient)` supplies a candidate,
    /// not ownership proof. A stale initial RPC head can include an earlier
    /// identical burn even with the exclusive lower bound. The caller must
    /// reject candidates already recorded by another transfer before adoption.
    ///
    /// Completes repeated full-range scans and the bounded finality gate even
    /// when candidates exist: callers may exclude every candidate as foreign.
    /// A lagging final head yields [`CctpError::ScanInconclusive`], not a partial
    /// range that could authorize a reburn after ownership filtering.
    pub(super) async fn find_recent_burns(
        &self,
        amount: U256,
        dest_domain: u32,
        recipient: Address,
        from_block: u64,
    ) -> Result<Vec<TxHash>, CctpError> {
        self.scan_burn(amount, dest_domain, recipient, from_block, None)
            .await
    }

    pub(super) async fn find_recorded_burn(
        &self,
        amount: U256,
        dest_domain: u32,
        recipient: Address,
        from_block: u64,
        burn_tx: TxHash,
    ) -> Result<crate::RecordedBurnScan, CctpError> {
        if !self
            .scan_burn(amount, dest_domain, recipient, from_block, Some(burn_tx))
            .await?
            .is_empty()
        {
            return Ok(crate::RecordedBurnScan::Found);
        }

        // A separate numeric head cannot establish the log backend's coverage.
        // Requalify the exact submission's canonical unused nonce after the scan.
        // This is a point-in-time suspected absence, not a future mining guarantee.
        Ok(match self.burn_status(burn_tx, from_block).await? {
            crate::BurnTxStatus::Dropped => crate::RecordedBurnScan::Absent,
            crate::BurnTxStatus::Pending
            | crate::BurnTxStatus::MinedSuccess
            | crate::BurnTxStatus::MinedReverted => crate::RecordedBurnScan::Inconclusive,
        })
    }

    async fn scan_burn(
        &self,
        amount: U256,
        dest_domain: u32,
        recipient: Address,
        from_block: u64,
        recorded_hash: Option<TxHash>,
    ) -> Result<Vec<TxHash>, CctpError> {
        let depositor = self.wallet.address();
        let mint_recipient = FixedBytes::<32>::left_padding_from(recipient.as_slice());
        let filter = Filter::new()
            .from_block(from_block)
            .address(self.token_messenger_address)
            .event_signature(TokenMessengerV2::DepositForBurn::SIGNATURE_HASH);

        let mut candidates: Vec<(u64, Option<u64>, TxHash)> = Vec::new();
        for attempt in 1..=SCAN_ATTEMPTS {
            let logs = self.wallet.provider().get_logs(&filter).await?;

            for log in logs.iter().rev() {
                if recorded_hash.is_some_and(|expected| log.transaction_hash != Some(expected)) {
                    continue;
                }
                let decoded = log.log_decode::<TokenMessengerV2::DepositForBurn>()?;
                let event = decoded.data();

                if event.depositor == depositor
                    && event.amount == amount
                    && event.destinationDomain == dest_domain
                    && event.mintRecipient == mint_recipient
                    && let Some(block) = log.block_number.filter(|block| *block > from_block)
                    && let Some(tx_hash) = log.transaction_hash
                {
                    debug!(target: "bridge", %tx_hash, from_block, "Found existing burn during resume");
                    if recorded_hash.is_some() {
                        return Ok(vec![tx_hash]);
                    }
                    if !candidates.iter().any(|(_, _, hash)| *hash == tx_hash) {
                        candidates.push((block, log.log_index, tx_hash));
                    }
                }
            }

            // Finish the bounded range check even with positive candidates:
            // ownership filtering may later exclude every one. This independent
            // head is not log-backend affinity proof; exact absence additionally
            // requires fresh canonical submission qualification in the caller.
            let head = self.wallet.provider().get_block_number().await?;
            let caught_up = head >= from_block.saturating_add(SCAN_FINALITY_MARGIN);

            if caught_up && attempt == SCAN_ATTEMPTS {
                candidates.sort_unstable_by(|left, right| {
                    right.0.cmp(&left.0).then_with(|| right.1.cmp(&left.1))
                });
                return Ok(candidates.into_iter().map(|(_, _, hash)| hash).collect());
            }

            if attempt < SCAN_ATTEMPTS {
                tokio::time::sleep(SCAN_RETRY_BACKOFF).await;
            }
        }

        Err(CctpError::ScanInconclusive { from_block })
    }

    /// Returns the current head of this endpoint's chain.
    pub(super) async fn current_block(&self) -> Result<u64, CctpError> {
        Ok(self.wallet.provider().get_block_number().await?)
    }

    /// Returns the block in which `tx_hash` was mined on this endpoint's chain.
    ///
    /// Used to derive the lower bound for [`find_recent_usdc_transfers`] from the
    /// known mint tx: the deposit send to Alpaca lands at or after the mint's
    /// block, so the mint block bounds the transfer scan exactly the way the
    /// captured head bounds [`find_recent_burns`]. Confirmation-aware: it polls via
    /// `await_receipt` rather than a single-shot lookup, so a load-balanced node
    /// that has not yet seen the mint does not yield a spurious "block missing".
    pub(super) async fn tx_block(&self, tx_hash: TxHash) -> Result<u64, CctpError> {
        let receipt = self.wallet.await_receipt(tx_hash).await?;

        receipt
            .block_number
            .ok_or(CctpError::TxReceiptMissingBlock { tx_hash })
    }

    /// Returns the number of confirmations `tx_hash` has on this endpoint's
    /// chain, or `None` if the transaction is not yet mined.
    ///
    /// Confirmations = (current head block) - (block the tx landed in) + 1.
    /// A tx in the current head has 1 confirmation (the inclusion block counts),
    /// matching the `required_confirmations` contract used across the codebase
    /// (alloy's `with_required_confirmations`, the e2e settlement helper). Used to
    /// gate operations on on-chain settlement without blocking -- the caller
    /// decides whether to retry if confirmations are insufficient.
    pub(super) async fn tx_confirmations(&self, tx_hash: TxHash) -> Result<Option<u64>, CctpError> {
        let Some(receipt) = self
            .wallet
            .provider()
            .get_transaction_receipt(tx_hash)
            .await?
        else {
            return Ok(None);
        };

        let Some(tx_block) = receipt.block_number else {
            return Ok(None);
        };

        let head = self.wallet.provider().get_block_number().await?;

        Ok(Some(head.saturating_sub(tx_block).saturating_add(1)))
    }

    /// Returns `tx_hash` as mined on this endpoint's chain; see
    /// [`st0x_evm::mined_tx`].
    pub(super) async fn mined_tx(&self, tx_hash: TxHash) -> Result<Option<MinedTx>, CctpError> {
        Ok(st0x_evm::mined_tx(self.wallet.provider(), tx_hash).await?)
    }

    /// Sums the USDC `Transfer` logs in `tx_hash`'s receipt that pay `recipient`:
    /// what that transaction credited to `recipient`, exact in base units.
    pub(super) async fn usdc_credited_in_tx(
        &self,
        tx_hash: TxHash,
        recipient: Address,
    ) -> Result<U256, CctpError> {
        let receipt = self.wallet.await_receipt(tx_hash).await?;

        usdc_credit_in_receipt(&receipt, self.usdc_address, None, recipient)
    }

    /// Like [`usdc_credited_in_tx`](Self::usdc_credited_in_tx), counting only
    /// the `Transfer` logs from `sender`. The hash comes from an operator, so
    /// one with no receipt yet is refused at once (`TxNotMined`) instead of
    /// waiting out the receipt wait's drop grace or timeout.
    pub(super) async fn usdc_sent_in_tx(
        &self,
        tx_hash: TxHash,
        sender: Address,
        recipient: Address,
    ) -> Result<U256, CctpError> {
        if self
            .wallet
            .provider()
            .get_transaction_receipt(tx_hash)
            .await?
            .is_none()
        {
            return Err(CctpError::TxNotMined { tx_hash });
        }

        let receipt = self.wallet.await_receipt(tx_hash).await?;

        usdc_credit_in_receipt(&receipt, self.usdc_address, Some(sender), recipient)
    }

    /// Signs a transfer of `amount` of this endpoint's USDC from the wallet to
    /// `to` without broadcasting it, reserving its nonce.
    ///
    /// This is the fund-moving leg of a BaseToAlpaca deposit: the CCTP mint
    /// credits the bot wallet, and this transfer forwards the minted USDC to
    /// Alpaca's deposit address. The caller persists the signed transfer
    /// before [`broadcast_usdc`](Self::broadcast_usdc) sends it, so every
    /// retry sends the same bytes and no second transfer can exist.
    pub(super) async fn prepare_usdc(
        &self,
        to: Address,
        amount: U256,
    ) -> Result<PreparedTransaction, EvmError> {
        self.wallet
            .prepare_pending(
                self.usdc_address,
                Bytes::from(IERC20::transferCall { to, amount }.abi_encode()),
                "USDC deposit to Alpaca",
            )
            .await
    }

    /// Broadcasts a transfer signed by [`prepare_usdc`](Self::prepare_usdc).
    /// Idempotent: a repeat sends the same bytes, and "already known" is
    /// success.
    pub(super) async fn broadcast_usdc(
        &self,
        prepared: &PreparedTransaction,
    ) -> Result<TxHash, EvmError> {
        self.wallet
            .broadcast_prepared(prepared, "USDC deposit to Alpaca")
            .await
    }

    pub(super) async fn discard_usdc(&self, prepared: &PreparedTransaction) {
        self.wallet.discard_prepared(prepared.tx_hash()).await;
    }

    pub(super) async fn restore_usdc(&self, prepared: &PreparedTransaction) {
        self.wallet.restore_prepared(prepared).await;
    }

    /// Awaits the receipt of a transfer broadcast by
    /// [`broadcast_usdc`](Self::broadcast_usdc) to the wallet's confirmation depth.
    /// A revert (decoded via `Registry`) and a drop are reported as statuses;
    /// any other error leaves the outcome unknown.
    pub(super) async fn confirm_usdc<Registry: IntoErrorRegistry>(
        &self,
        tx_hash: TxHash,
    ) -> Result<UsdcTransferStatus, CctpError> {
        match self.wallet.confirm::<Registry>(tx_hash).await {
            Ok(_) => Ok(UsdcTransferStatus::Confirmed),
            Err(error) if error.is_revert() => Ok(UsdcTransferStatus::Reverted),
            Err(error) if error.is_transaction_dropped() => Ok(UsdcTransferStatus::Dropped),
            Err(error) => Err(error.into()),
        }
    }

    /// Scans for USDC `Transfer(from, to, value == amount)` events at or after
    /// `from_block`, returning every matching transaction hash, newest first.
    ///
    /// Detects a legacy unrecorded deposit send: a BaseToAlpaca transfer that
    /// reached `Bridged` without a persisted signed send may already have sent
    /// the minted USDC, so resume refuses to send again when a match is not
    /// another transfer's send. The send lands at or after the mint, so the
    /// match is bounded to `from_block` (the mint's block) onward. Matching on
    /// `(from, to, value)` cannot tell this transfer's send from another
    /// transfer's same-amount send, so a match is never adopted.
    ///
    /// Returns an empty list ONLY when the queried node is confirmations-deep past
    /// `from_block` and repeated scans agree the transfer is absent; a node that
    /// may be lagging (the dRPC load-balancing hazard) yields a retryable
    /// [`CctpError::ScanInconclusive`], so the caller never re-sends off a single
    /// stale empty `eth_getLogs`.
    pub(super) async fn find_recent_usdc_transfers(
        &self,
        from: Address,
        to: Address,
        amount: U256,
        from_block: u64,
    ) -> Result<Vec<TxHash>, CctpError> {
        let from_topic = FixedBytes::<32>::left_padding_from(from.as_slice());
        let to_topic = FixedBytes::<32>::left_padding_from(to.as_slice());
        let filter = Filter::new()
            .from_block(from_block)
            .address(self.usdc_address)
            .event_signature(IERC20::Transfer::SIGNATURE_HASH)
            .topic1(from_topic)
            .topic2(to_topic);

        for attempt in 1..=SCAN_ATTEMPTS {
            let logs = self.wallet.provider().get_logs(&filter).await?;

            let mut matches = Vec::new();
            for log in logs.iter().rev() {
                let decoded = log.log_decode::<IERC20::Transfer>()?;
                let event = decoded.data();

                if event.value == amount
                    && log.block_number.is_some_and(|block| block >= from_block)
                    && let Some(tx_hash) = log.transaction_hash
                {
                    matches.push(tx_hash);
                }
            }

            if !matches.is_empty() {
                debug!(target: "bridge", ?matches, from_block, "Found existing USDC deposit transfers during resume");
                return Ok(matches);
            }

            // A single empty eth_getLogs from a load-balanced node is not
            // authoritative (dRPC lag). Only conclude a true absence once the head
            // is confirmations-deep past from_block AND repeated scans agree; else
            // retry, and if still inconclusive return a retryable error so the
            // caller never re-sends off a stale empty result.
            let head = self.wallet.provider().get_block_number().await?;
            let caught_up = head >= from_block.saturating_add(SCAN_FINALITY_MARGIN);

            if caught_up && attempt == SCAN_ATTEMPTS {
                return Ok(Vec::new());
            }

            if attempt < SCAN_ATTEMPTS {
                tokio::time::sleep(SCAN_RETRY_BACKOFF).await;
            }
        }

        Err(CctpError::ScanInconclusive { from_block })
    }

    /// Claims USDC on this chain by submitting the attestation.
    ///
    /// Parses the `MintAndWithdraw` event from the transaction receipt to extract
    /// the actual minted amount and fee collected. This is the source of truth
    /// for what the recipient actually received.
    pub(super) async fn claim<Registry: IntoErrorRegistry>(
        &self,
        direction: BridgeDirection,
        message: Bytes,
        attestation: Bytes,
    ) -> Result<MintReceipt, CctpError> {
        let receipt = match self
            .wallet
            .submit::<Registry, _>(
                self.message_transmitter_address,
                MessageTransmitterV2::receiveMessageCall {
                    message: message.clone(),
                    attestation,
                },
                "receiveMessage",
            )
            .await
        {
            Ok(receipt) => receipt,
            // receiveMessage reverts for many reasons. We cannot rely on the
            // revert reason string (it is decoder/provider dependent and may
            // not survive decoding), so we hand every revert to recovery, which
            // confirms structurally whether the nonce was already minted and
            // otherwise re-propagates this error unchanged.
            Err(error) => {
                return self
                    .recover_already_minted::<Registry>(direction, &message, error)
                    .await;
            }
        };

        parse_mint_receipt(&receipt).ok_or(CctpError::MintAndWithdrawEventNotFound)
    }

    /// Returns the receipt of an already-executed mint for the attested
    /// `message`, or `None` if its nonce has not been consumed on this chain.
    ///
    /// The zero-nonce and destination-domain checks run before the
    /// `usedNonces()` read: both are structurally deterministic for the fixed
    /// `message` bytes and independent of chain state, so a wrong-direction or
    /// unattested message fails immediately instead of reading the nonce first
    /// and reporting "not yet minted" for a message that could never mint
    /// here. Used proactively by crash-recovery resume, before minting.
    /// [`recover_already_minted`](Self::recover_already_minted) does not call
    /// this directly -- see its own doc for why.
    ///
    /// The log scan for a consumed nonce is floored at the lower of
    /// `scan_from_block` less [`CAPTURED_FLOOR_MARGIN`] and
    /// [`MINT_SCAN_LOOKBACK`] below the head, both in this chain's blocks.
    /// A log still missing after the lag retries is
    /// [`CctpError::MintNotFoundInScanWindow`], which carries whether the
    /// nonce was used below the floor so the caller can tell index lag from a
    /// mint below the floor. The scan never walks to genesis.
    pub(super) async fn find_existing_mint<Registry: IntoErrorRegistry>(
        &self,
        direction: BridgeDirection,
        message: &[u8],
        scan_from_block: Option<u64>,
    ) -> Result<Option<MintReceipt>, CctpError> {
        let received_message = validate_message_shape(message, direction)?;

        // Authoritative gate: usedNonces() is non-zero once the nonce has been
        // consumed (the mint executed and emitted MintAndWithdraw below).
        if !self
            .is_nonce_used::<Registry>(received_message.nonce)
            .await?
        {
            return Ok(None);
        }

        let lookback_floor = self
            .current_block()
            .await?
            .saturating_sub(self.scan_window.mint_lookback);

        // A relayer can mint before the floor was captured (burns name no
        // destination caller), so a captured floor is lowered by a margin and
        // never scans less than the bounded lookback. Matching is by nonce, so
        // a lower floor cannot adopt another mint.
        let from_block = scan_from_block.map_or(lookback_floor, |captured| {
            captured
                .saturating_sub(self.scan_window.floor_margin)
                .min(lookback_floor)
        });

        self.locate_mint_in_scan_window::<Registry>(&received_message, from_block)
            .await
            .map(Some)
    }

    /// Cheap authoritative gate: `true` once `nonce` has been consumed on this
    /// chain (the mint executed and emitted `MintAndWithdraw`).
    ///
    /// This is the only on-chain call
    /// [`recover_already_minted`](Self::recover_already_minted)'s probe loop
    /// makes on every probe -- the expensive log scan and receipt
    /// reconstruction ([`locate_mint_receipt`](Self::locate_mint_receipt)) run
    /// only once, after a probe confirms the nonce consumed, rather than on
    /// every probe: repeating an unbounded backward log scan across the whole
    /// multi-minute recovery window would multiply its cost by the probe
    /// count for no benefit, since this cheap view call already gives an
    /// authoritative answer.
    pub(super) async fn is_nonce_used<Registry: IntoErrorRegistry>(
        &self,
        nonce: B256,
    ) -> Result<bool, EvmError> {
        let nonce_used = self
            .wallet
            .call::<Registry, _>(
                self.message_transmitter_address,
                MessageTransmitterV2::usedNoncesCall(nonce),
            )
            .await?;

        Ok(!nonce_used.is_zero())
    }

    /// Whether `nonce` is consumed, read over the mint recovery probe window.
    ///
    /// One read can come from a load-balanced node behind the block holding
    /// the mint, so "unused" is returned only once every probe reads it unused.
    /// The first consumed read returns `true`; a failed read is returned as-is.
    pub(super) async fn is_nonce_used_across_probes<Registry: IntoErrorRegistry>(
        &self,
        nonce: B256,
    ) -> Result<bool, EvmError> {
        let mut poll = interval(self.mint_recovery_config.probe_interval);
        poll.set_missed_tick_behavior(MissedTickBehavior::Delay);

        for probe in 1..=self.mint_recovery_config.probes.get() {
            poll.tick().await;

            if self.is_nonce_used::<Registry>(nonce).await? {
                return Ok(true);
            }

            debug!(target: "bridge", %nonce, probe, "CCTP nonce reads unused");
        }

        Ok(false)
    }

    /// Locates and validates the mint receipt for a message whose nonce
    /// [`is_nonce_used`](Self::is_nonce_used) already confirmed consumed.
    /// Scans for the `MessageReceived` log matching the attested source
    /// domain and body, then validates the mint tx did not revert and emitted
    /// `MintAndWithdraw`.
    ///
    /// `min_block` floors the backward scan; passed straight through to
    /// [`find_received_message_tx`](Self::find_received_message_tx).
    async fn locate_mint_receipt(
        &self,
        received_message: &CctpReceivedMessage<'_>,
        min_block: u64,
    ) -> Result<MintReceipt, CctpError> {
        let (tx_hash, message_received_log_index) = self
            .find_received_message_tx(
                received_message.source_domain,
                received_message.nonce,
                received_message.message_body,
                min_block,
            )
            .await?;

        // The mint tx may have been submitted by another caller; await_receipt
        // polls and waits for confirmation depth (load-balanced RPCs may route
        // to a lagging node), rather than a bare single-shot
        // get_transaction_receipt.
        let receipt = self.wallet.await_receipt(tx_hash).await?;

        if !receipt.status() {
            return Err(CctpError::RecoveredMintReceiptReverted { tx_hash });
        }

        let mint_receipt = parse_mint_receipt_for_message(&receipt, message_received_log_index)
            .ok_or(CctpError::RecoveredMintAndWithdrawEventNotFound { tx_hash })?;

        info!(
            target: "bridge",
            nonce = %received_message.nonce,
            mint_tx = %mint_receipt.tx,
            amount = %mint_receipt.amount,
            fee_collected = %mint_receipt.fee_collected,
            "Recovered already-minted CCTP transfer"
        );

        Ok(mint_receipt)
    }

    /// Reconstructs the mint receipt once
    /// [`recover_already_minted`](Self::recover_already_minted)'s probe loop
    /// has confirmed the nonce consumed.
    ///
    /// Retries only [`CctpError::AlreadyMintedMessageNotFound`] -- the queried
    /// node's log index lagging behind the state its own `usedNonces()` view
    /// call already reflects -- up to `SCAN_ATTEMPTS` times spaced
    /// `SCAN_RETRY_BACKOFF` apart, the same dRPC-lag tolerance
    /// [`find_recent_burns`](Self::find_recent_burns) and
    /// [`find_recent_usdc_transfers`](Self::find_recent_usdc_transfers) already
    /// apply to their own `get_logs` scans. This runs at most once per
    /// `recover_already_minted` call (not once per probe). Each retry's scan
    /// is additionally floored at [`RECONSTRUCTION_SCAN_LOOKBACK`] behind the
    /// current head, so `SCAN_ATTEMPTS` retries are genuinely
    /// a handful of quick attempts instead of each one repeating a full
    /// backward walk to genesis. A failed head read is returned as-is: there is
    /// no floor to scan from, and the caller redrives.
    ///
    /// Any other failure means the nonce is authoritatively consumed but the
    /// receipt could not be validated (a reverted mint tx, a mismatched log, a
    /// missing event): this is returned as-is, and the caller reports it as
    /// [`CctpError::MintRecoveryInconclusive`] rather than a false "never
    /// minted" terminal failure, since the authoritative nonce read already
    /// proved the mint landed.
    async fn reconstruct_existing_mint<Registry: IntoErrorRegistry>(
        &self,
        received_message: &CctpReceivedMessage<'_>,
    ) -> Result<MintReceipt, CctpError> {
        let min_block = self
            .current_block()
            .await?
            .saturating_sub(self.scan_window.reconstruction_lookback);

        self.locate_mint_in_scan_window::<Registry>(received_message, min_block)
            .await
    }

    /// [`locate_mint_receipt_with_lag_retries`](Self::locate_mint_receipt_with_lag_retries),
    /// reporting a log still missing as [`CctpError::MintNotFoundInScanWindow`]
    /// with the [`check_mint_scan_floor`](Self::check_mint_scan_floor) result.
    async fn locate_mint_in_scan_window<Registry: IntoErrorRegistry>(
        &self,
        received_message: &CctpReceivedMessage<'_>,
        from_block: u64,
    ) -> Result<MintReceipt, CctpError> {
        let nonce = match self
            .locate_mint_receipt_with_lag_retries(received_message, from_block)
            .await
        {
            Err(CctpError::AlreadyMintedMessageNotFound { nonce }) => nonce,
            located => return located,
        };

        let floor_check = self
            .check_mint_scan_floor::<Registry>(nonce, from_block)
            .await?;

        warn!(
            target: "bridge",
            %nonce,
            from_block,
            ?floor_check,
            "CCTP nonce consumed but its mint is not in the scan window"
        );
        Err(CctpError::MintNotFoundInScanWindow {
            nonce,
            from_block,
            floor_check,
        })
    }

    /// Where the mint of the consumed `nonce` lies relative to a scan floored
    /// at `from_block`, from a `usedNonces()` read at the block below it. A
    /// failed read (e.g. a node without state that old) falls back to the
    /// floor block's timestamp rather than failing the lookup.
    async fn check_mint_scan_floor<Registry: IntoErrorRegistry>(
        &self,
        nonce: B256,
        from_block: u64,
    ) -> Result<MintScanFloorCheck, CctpError> {
        // A scan from genesis covers every block.
        let Some(below_floor) = from_block.checked_sub(1) else {
            return Ok(MintScanFloorCheck::MintInScanWindow);
        };

        match self
            .wallet
            .call_at::<Registry, _>(
                self.message_transmitter_address,
                MessageTransmitterV2::usedNoncesCall(nonce),
                below_floor,
            )
            .await
        {
            Ok(nonce_used) if nonce_used.is_zero() => Ok(MintScanFloorCheck::MintInScanWindow),
            Ok(_) => Ok(MintScanFloorCheck::MintBelowScanFloor),
            Err(historical_read_error) => {
                warn!(
                    target: "bridge",
                    %nonce,
                    below_floor,
                    ?historical_read_error,
                    "usedNonces() read below the mint scan floor failed; \
                     falling back to the floor block's timestamp"
                );

                let from_block_timestamp = self
                    .wallet
                    .provider()
                    .get_block_by_number(from_block.into())
                    .await?
                    .ok_or(CctpError::MintScanFloorBlockMissing { block: from_block })?
                    .header
                    .timestamp;

                Ok(MintScanFloorCheck::Unverified {
                    from_block_timestamp,
                })
            }
        }
    }

    /// [`locate_mint_receipt`](Self::locate_mint_receipt), retrying only
    /// [`CctpError::AlreadyMintedMessageNotFound`] (log-index lag behind the
    /// node's own `usedNonces()` answer) up to `SCAN_ATTEMPTS` times.
    async fn locate_mint_receipt_with_lag_retries(
        &self,
        received_message: &CctpReceivedMessage<'_>,
        min_block: u64,
    ) -> Result<MintReceipt, CctpError> {
        let mut attempt = 1;

        loop {
            match self.locate_mint_receipt(received_message, min_block).await {
                Ok(mint_receipt) => return Ok(mint_receipt),
                Err(CctpError::AlreadyMintedMessageNotFound { nonce })
                    if attempt < SCAN_ATTEMPTS =>
                {
                    warn!(
                        target: "bridge",
                        %nonce,
                        attempt,
                        max_attempts = SCAN_ATTEMPTS,
                        "MessageReceived log not yet visible for a nonce confirmed \
                         consumed; retrying (load-balanced RPC log-index lag)"
                    );
                    tokio::time::sleep(SCAN_RETRY_BACKOFF).await;
                    attempt += 1;
                }
                Err(other_error) => return Err(other_error),
            }
        }
    }

    /// Reactive recovery for a `receiveMessage` revert. Three outcomes:
    ///
    /// - the nonce was already minted: returns the existing receipt, via a
    ///   single [`reconstruct_existing_mint`](Self::reconstruct_existing_mint)
    ///   call once a probe confirms the nonce consumed;
    /// - the last probe's read was a clean `usedNonces()` read of unconsumed:
    ///   re-surfaces the original `submit_error` as a true terminal failure.
    ///   Earlier probes may have errored transiently along the way -- that
    ///   does not matter, since a consumed nonce is never un-consumed, so the
    ///   last clean read is authoritative and conclusively means the mint had
    ///   not landed as of that read -- a decided outcome, not a guess;
    /// - the window expires without ever getting a conclusive read (every
    ///   remaining probe itself errored transiently), OR the nonce is
    ///   confirmed consumed but its receipt could not be reconstructed:
    ///   returns [`CctpError::MintRecoveryInconclusive`] instead of masking an
    ///   unknown state, or a state we know landed, as a decided "never
    ///   minted" outcome. Declaring a terminal failure here would strand the
    ///   caller (the USDC rebalancing guard) on a false negative; the caller
    ///   should redrive instead.
    ///
    /// The zero-nonce and destination-domain fail-fast guards are shared with
    /// [`find_existing_mint`](Self::find_existing_mint); this probe loop,
    /// though, calls only [`is_nonce_used`](Self::is_nonce_used) on every
    /// probe -- the expensive log scan and receipt reconstruction run at most
    /// once, via [`reconstruct_existing_mint`](Self::reconstruct_existing_mint),
    /// after a probe confirms the nonce consumed, not on every probe.
    ///
    /// The probe is repeated over the configured window (production: ~2
    /// minutes, see [`MINT_RECOVERY_PROBES`] and [`MintRecoveryConfig`])
    /// rather than run once. Our own submission failing does not mean the
    /// mint will not happen: the burn is already irreversible and the
    /// attestation is valid,
    /// so **any** party -- a third-party relayer, or a later retry -- can
    /// deliver `receiveMessage` seconds afterwards. A single instant probe
    /// reports "not minted" for a message that is about to be minted, and the
    /// caller then declares a post-burn failure that strands the USDC
    /// rebalancing guard until an operator reconciles it by hand.
    ///
    /// Every probe error is retried, including one that is revert-shaped per
    /// [`EvmError::is_revert`]. An earlier version of this loop fast-failed
    /// on `is_revert()`, treating it as a deterministic on-chain outcome that
    /// would recur identically on every remaining probe. That assumption
    /// does not hold for the single call this loop makes,
    /// [`is_nonce_used`](Self::is_nonce_used): `usedNonces(bytes32)` is a
    /// plain public-mapping getter (see the vendored `MessageTransmitterV2`
    /// ABI -- a `view` function with no `require`/branch logic), so it
    /// cannot deterministically revert for a well-formed call. Any
    /// revert-shaped response observed here is therefore necessarily a
    /// provider/transport artifact (a misrouted or misbehaving RPC backend),
    /// not a decided on-chain fact -- exactly the class of failure this
    /// window exists to survive, so fast-failing on it risked reproducing
    /// the original incident on a transient blip.
    pub(super) async fn recover_already_minted<Registry: IntoErrorRegistry>(
        &self,
        direction: BridgeDirection,
        message: &[u8],
        submit_error: EvmError,
    ) -> Result<MintReceipt, CctpError> {
        // A failure here (an unparseable envelope, the reserved placeholder
        // nonce, or a destination-domain mismatch) is structurally
        // deterministic for the fixed `message` bytes: none of these depend
        // on chain state or RPC health, so all fail fast before any probe is
        // spent instead of burning the whole recovery window re-checking the
        // same bytes on every remaining probe. The caller's post-burn failure
        // surfaces why `receiveMessage` itself failed (`submit_error`), not
        // this internal check's own error, which is logged alongside it for
        // diagnosis.
        let received_message = match validate_message_shape(message, direction) {
            Ok(received_message) => received_message,
            Err(validation_error) => {
                warn!(
                    target: "bridge",
                    ?validation_error,
                    "CCTP mint recovery message failed structural validation; failing \
                     fast and surfacing the original submit error"
                );
                return Err(submit_error.into());
            }
        };

        let config = self.mint_recovery_config;

        // Tracks the last probe's outcome so the terminal handling below can
        // distinguish "every probe read the nonce as genuinely unconsumed"
        // from "the last probe errored, so the nonce state is unknown" --
        // collapsing both into one outcome misleads a caller (and an operator
        // reading a post-burn failure) into reconciling in the wrong
        // direction.
        let mut last_probe_error: Option<EvmError> = None;
        let mut unconsumed_reads = 0u32;

        let mut poll = interval(config.probe_interval);
        poll.set_missed_tick_behavior(MissedTickBehavior::Delay);

        for probe in 1..=config.probes.get() {
            // First tick completes immediately, so probe 1 runs before any
            // sleep; subsequent ticks pace the remaining probes.
            poll.tick().await;

            match self.is_nonce_used::<Registry>(received_message.nonce).await {
                Ok(true) => {
                    debug!(
                        target: "bridge",
                        probe,
                        "CCTP mint nonce reads consumed; reconstructing the mint receipt"
                    );
                    // The nonce is authoritatively confirmed consumed, so ANY
                    // reconstruction failure (a lagging log scan that outlasts
                    // SCAN_ATTEMPTS, a mismatched log, a reverted mint tx, a
                    // missing MintAndWithdraw event) is reported as
                    // MintRecoveryInconclusive, never as the raw underlying
                    // error: the mint is known to have landed, so declaring a
                    // terminal failure here would strand the caller on a false
                    // negative exactly like the case this function exists to
                    // prevent.
                    return self
                        .reconstruct_existing_mint::<Registry>(&received_message)
                        .await
                        .map_err(|reconstruction_error| CctpError::MintRecoveryInconclusive {
                            recovery_error: Box::new(reconstruction_error),
                        });
                }
                Ok(false) => {
                    last_probe_error = None;
                    unconsumed_reads += 1;
                    debug!(
                        target: "bridge",
                        probe,
                        "CCTP mint nonce still unconsumed after submit failure"
                    );
                }
                Err(evm_error) => {
                    // usedNonces() cannot deterministically revert (see this
                    // function's doc comment), so every probe error -- even
                    // one shaped like a revert -- is retried rather than
                    // treated as a decided terminal outcome.
                    warn!(
                        target: "bridge",
                        ?evm_error,
                        probe,
                        "CCTP mint recovery probe failed; retrying"
                    );
                    last_probe_error = Some(evm_error);
                }
            }
        }

        if let Some(last_probe_error) = last_probe_error {
            warn!(
                target: "bridge",
                ?last_probe_error,
                "CCTP mint recovery window expired with the nonce state UNKNOWN (the last \
                 probe errored rather than reading the nonce as unconsumed); the mint may \
                 still have landed. Returning a retryable error instead of declaring a \
                 false terminal failure"
            );
            return Err(CctpError::MintRecoveryInconclusive {
                recovery_error: Box::new(last_probe_error.into()),
            });
        }

        let probes = config.probes.get();
        let window = config
            .probe_interval
            .saturating_mul(probes.saturating_sub(1));
        warn!(
            target: "bridge",
            window_secs = window.as_secs(),
            unconsumed_reads,
            probes,
            "CCTP mint nonce read unconsumed on every probe that completed cleanly \
             ({unconsumed_reads} of {probes}); surfacing the original submit error"
        );

        Err(submit_error.into())
    }

    /// Locates the `receiveMessage` transaction that minted `nonce` and returns
    /// its hash alongside the matched `MessageReceived` log index (used to
    /// correlate the right `MintAndWithdraw` within a multicall transaction).
    ///
    /// `min_block` floors how far back the scan walks:
    /// [`find_existing_mint`](Self::find_existing_mint)'s floor, or
    /// [`RECONSTRUCTION_SCAN_LOOKBACK`] below the head for
    /// the reconstruction that just saw the nonce become consumed.
    async fn find_received_message_tx(
        &self,
        source_domain: u32,
        nonce: B256,
        message_body: &[u8],
        min_block: u64,
    ) -> Result<(TxHash, u64), CctpError> {
        let latest = self
            .wallet
            .provider()
            .get_block_number()
            .await
            .map_err(EvmError::from)?;
        // `min_block` was derived from a head read the caller took earlier; a
        // load-balanced endpoint can answer this second read from a node that
        // is behind that one. Clamping keeps `from_block <= to_block`, since an
        // inverted range is rejected outright by most providers and would be
        // charged against the caller's scan attempts as if the mint were
        // missing.
        let floor = min_block.min(latest);
        let mut to_block = latest;
        let mut saw_nonce = false;

        loop {
            let from_block = to_block
                .saturating_sub(CCTP_RECOVERY_LOG_BLOCK_CHUNK.saturating_sub(1))
                .max(floor);
            let filter = Filter::new()
                .address(self.message_transmitter_address)
                .from_block(from_block)
                .to_block(to_block)
                .event_signature(MessageTransmitterV2::MessageReceived::SIGNATURE_HASH)
                .topic2(nonce);
            let logs = self
                .wallet
                .provider()
                .get_logs(&filter)
                .await
                .map_err(EvmError::from)?;

            for log in logs {
                // The topic2(nonce) filter already restricts to our exact nonce,
                // so any returned log is a sighting -- record it before decoding
                // so a decode failure is not misreported as "nonce never seen".
                saw_nonce = true;

                let Ok(decoded) = log.log_decode::<MessageTransmitterV2::MessageReceived>() else {
                    warn!(
                        target: "bridge",
                        %nonce,
                        "MessageReceived log matched the nonce filter but failed to decode; skipping"
                    );
                    continue;
                };
                let event = decoded.data();

                if event.sourceDomain != source_domain || event.messageBody.as_ref() != message_body
                {
                    trace!(
                        target: "bridge",
                        %nonce,
                        log_source_domain = event.sourceDomain,
                        expected_source_domain = source_domain,
                        "MessageReceived log for nonce did not match attested source domain/body; skipping"
                    );
                    continue;
                }

                let tx_hash = decoded
                    .transaction_hash
                    .ok_or(CctpError::RecoveredMintLogMissingTxHash { nonce })?;
                let log_index = decoded
                    .log_index
                    .ok_or(CctpError::RecoveredMintLogMissingTxHash { nonce })?;

                return Ok((tx_hash, log_index));
            }

            if from_block <= floor {
                break;
            }

            to_block = from_block - 1;
        }

        if saw_nonce {
            Err(CctpError::RecoveredMintMessageMismatch { nonce })
        } else {
            Err(CctpError::AlreadyMintedMessageNotFound { nonce })
        }
    }

    /// Returns `holder`'s balance of this chain's USDC token.
    pub(super) async fn usdc_balance<Registry: IntoErrorRegistry>(
        &self,
        holder: Address,
    ) -> Result<U256, CctpError> {
        Ok(self
            .wallet
            .call::<Registry, _>(self.usdc_address, IERC20::balanceOfCall { account: holder })
            .await?)
    }

    #[cfg(test)]
    pub(super) fn usdc(&self) -> IERC20::IERC20Instance<&<W as Evm>::Provider> {
        IERC20::new(self.usdc_address, self.wallet.provider())
    }

    /// Pre-approve USDC spending via the wallet's signing path.
    ///
    /// Unlike `usdc().approve().send()` which uses the read-only provider,
    /// this submits through `Wallet::submit()` which has signing capability.
    #[cfg(test)]
    pub(super) async fn approve_usdc<Registry: IntoErrorRegistry>(
        &self,
        spender: Address,
        amount: U256,
    ) -> Result<(), CctpError> {
        self.wallet
            .submit::<Registry, _>(
                self.usdc_address,
                IERC20::approveCall { spender, amount },
                "test pre-approve USDC",
            )
            .await?;

        Ok(())
    }

    #[cfg(test)]
    pub(super) fn owner(&self) -> Address {
        self.wallet.address()
    }

    #[cfg(test)]
    pub(super) fn token_messenger_address(&self) -> Address {
        self.token_messenger_address
    }

    /// Sets a custom node-sync poll interval. Test-only: lets node-sync
    /// exhaustion paths run without sleeping at the production one-second
    /// cadence.
    #[cfg(test)]
    pub(super) fn with_node_sync_poll_interval(mut self, interval: Duration) -> Self {
        self.node_sync_poll_interval = interval;
        self
    }

    /// Overrides the [`burn_status`](Self::burn_status) drop policy. Test-only:
    /// lets resume tests shorten the grace without bypassing freshness
    /// instead of waiting out the production 30 s grace window.
    #[cfg(any(test, feature = "test-support"))]
    #[must_use]
    pub(super) fn with_burn_drop_config(mut self, config: BurnDropConfig) -> Self {
        self.burn_drop_config = config;
        self
    }

    /// Overrides the [`recover_already_minted`](Self::recover_already_minted)
    /// probe cadence. Test-only: lets recovery tests drive a short interval
    /// and probe count against real awaited state instead of racing or
    /// pausing the production multi-minute window.
    #[cfg(any(test, feature = "test-support"))]
    #[must_use]
    pub(super) fn with_mint_recovery_config(mut self, config: MintRecoveryConfig) -> Self {
        self.mint_recovery_config = config;
        self
    }
}

/// Parses `message` and validates it can structurally mint on `direction`'s
/// chain: rejects an unparseable envelope, the reserved placeholder nonce
/// (CCTP V2 assigns the real nonce only at attestation, so an unattested or
/// malformed message still carries the reserved zero nonce), and a
/// destination-domain mismatch (reconstructing a message attested for the
/// other direction can never succeed here). All three checks are
/// structurally deterministic for the fixed `message` bytes and independent
/// of chain state, so they fail fast before any `usedNonces()` read.
///
/// Shared by [`CctpEndpoint::find_existing_mint`] and
/// [`CctpEndpoint::recover_already_minted`], which apply different policies
/// to a validation failure (a direct error vs. logging it and re-surfacing
/// the original `receiveMessage` submit error) -- only the validation rule
/// itself is shared here.
fn validate_message_shape(
    message: &[u8],
    direction: BridgeDirection,
) -> Result<CctpReceivedMessage<'_>, CctpError> {
    let received_message = parse_received_message(message)?;

    if received_message.nonce == B256::ZERO {
        return Err(CctpError::PlaceholderNonce);
    }

    if received_message.destination_domain != direction.dest_domain() {
        return Err(CctpError::MessageDestinationDomainMismatch {
            expected: direction.dest_domain(),
            actual: received_message.destination_domain,
        });
    }

    Ok(received_message)
}

/// Sums the `usdc` `Transfer` logs in `receipt` that pay `recipient`, from
/// `sender` only when one is given. A log carrying the `Transfer` topic that
/// does not decode fails the read: skipping it would undercredit the
/// transfer silently.
fn usdc_credit_in_receipt(
    receipt: &TransactionReceipt,
    usdc: Address,
    sender: Option<Address>,
    recipient: Address,
) -> Result<U256, CctpError> {
    let tx_hash = receipt.transaction_hash;

    receipt
        .inner
        .logs()
        .iter()
        .filter(|log| {
            log.address() == usdc && log.topics().first() == Some(&IERC20::Transfer::SIGNATURE_HASH)
        })
        .map(|log| {
            IERC20::Transfer::decode_log(log.as_ref())
                .map_err(|source| CctpError::UsdcTransferLogDecode { tx_hash, source })
        })
        .try_fold(U256::ZERO, |credited, transfer| {
            let transfer = transfer?;
            if transfer.to != recipient || sender.is_some_and(|sender| transfer.from != sender) {
                return Ok(credited);
            }

            credited
                .checked_add(transfer.value)
                .ok_or(CctpError::UsdcCreditOverflow { tx_hash })
        })
}

fn parse_mint_receipt(receipt: &TransactionReceipt) -> Option<MintReceipt> {
    let mint_event = receipt
        .inner
        .logs()
        .iter()
        .find_map(|log| TokenMessengerV2::MintAndWithdraw::decode_log(log.as_ref()).ok())?;

    info!(
        target: "bridge",
        amount = %mint_event.amount,
        fee_collected = %mint_event.feeCollected,
        "Parsed MintAndWithdraw event"
    );

    Some(MintReceipt {
        tx: receipt.transaction_hash,
        amount: mint_event.amount,
        fee_collected: mint_event.feeCollected,
    })
}

/// Selects the `MintAndWithdraw` emitted by the same `receiveMessage` call that
/// produced the `MessageReceived` log at `message_received_log_index`.
///
/// CCTP V2 emits `MintAndWithdraw` (during `handleReceiveFinalizedMessage`)
/// before `MessageReceived` within a single `receiveMessage` call, so the mint
/// for our message is the one with the greatest log index strictly below the
/// matched `MessageReceived` log. This disambiguates relayer multicalls that
/// batch several `receiveMessage` calls -- and thus several `MintAndWithdraw`
/// events -- into one transaction, where taking the first event could attribute
/// another transfer's amount to ours.
fn parse_mint_receipt_for_message(
    receipt: &TransactionReceipt,
    message_received_log_index: u64,
) -> Option<MintReceipt> {
    let (_, mint_event) = receipt
        .inner
        .logs()
        .iter()
        .filter_map(|log| {
            let log_index = log.log_index?;

            if log_index >= message_received_log_index {
                return None;
            }

            let decoded = TokenMessengerV2::MintAndWithdraw::decode_log(log.as_ref()).ok()?;

            Some((log_index, decoded))
        })
        .max_by_key(|(log_index, _)| *log_index)?;

    info!(
        target: "bridge",
        amount = %mint_event.amount,
        fee_collected = %mint_event.feeCollected,
        "Parsed MintAndWithdraw event for recovered transfer"
    );

    Some(MintReceipt {
        tx: receipt.transaction_hash,
        amount: mint_event.amount,
        fee_collected: mint_event.feeCollected,
    })
}

#[cfg(test)]
mod tests {
    use alloy::consensus::{Receipt, ReceiptEnvelope, ReceiptWithBloom};
    use alloy::primitives::{Bloom, Log as PrimitiveLog, Signature};
    use alloy::providers::mock::Asserter;
    use alloy::providers::{ProviderBuilder, RootProvider};
    use alloy::rpc::types::Log;
    use async_trait::async_trait;
    use httpmock::{Mock, MockServer};
    use serde_json::json;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, Ordering};

    use st0x_evm::local::RawPrivateKeyWallet;

    use super::*;

    struct BurnProbeWallet {
        provider: RootProvider,
        hash: TxHash,
        submission: TransactionSubmission,
        confirmation: Mutex<Option<Result<TransactionReceipt, EvmError>>>,
        released: AtomicBool,
        receipt: Option<TransactionReceipt>,
    }

    #[async_trait]
    impl Evm for BurnProbeWallet {
        type Provider = RootProvider;

        fn provider(&self) -> &RootProvider {
            &self.provider
        }
    }

    #[async_trait]
    impl Wallet for BurnProbeWallet {
        fn address(&self) -> Address {
            self.submission.sender
        }

        fn transaction_submission(&self, hash: TxHash) -> Option<TransactionSubmission> {
            (hash == self.hash && !self.released.load(Ordering::SeqCst)).then_some(self.submission)
        }

        async fn sign_typed_data(&self, _: String, _: B256) -> Result<Signature, EvmError> {
            panic!("absence probing must not sign")
        }

        async fn prepare_pending(
            &self,
            _: Address,
            _: Bytes,
            _: &str,
        ) -> Result<PreparedTransaction, EvmError> {
            panic!("absence probing must not prepare")
        }

        async fn prepare_pending_with_gas_limit(
            &self,
            _: Address,
            _: Bytes,
            _: u64,
            _: &str,
        ) -> Result<PreparedTransaction, EvmError> {
            panic!("absence probing must not prepare")
        }

        async fn broadcast_prepared(
            &self,
            _: &PreparedTransaction,
            _: &str,
        ) -> Result<TxHash, EvmError> {
            panic!("absence probing must not broadcast")
        }

        async fn discard_prepared(&self, _: TxHash) {
            panic!("absence probing must not release nonce ownership")
        }

        async fn release_superseded(&self, _: TxHash) {
            panic!("absence probing must not release nonce ownership")
        }

        async fn restore_prepared(&self, _: &PreparedTransaction) {
            panic!("absence probing must not restore ownership")
        }

        async fn restore_transaction(&self, _: TxHash) -> Result<(), EvmError> {
            panic!("absence probing must not restore ownership")
        }

        async fn send_pending(&self, _: Address, _: Bytes, _: &str) -> Result<TxHash, EvmError> {
            panic!("absence probing must not send")
        }

        async fn await_receipt(&self, hash: TxHash) -> Result<TransactionReceipt, EvmError> {
            assert_eq!(hash, self.hash);
            let confirmation = self.confirmation.lock().unwrap().take();
            if let Some(confirmation) = confirmation {
                if confirmation
                    .as_ref()
                    .is_err_and(EvmError::is_transaction_dropped)
                {
                    self.released.store(true, Ordering::SeqCst);
                }
                return confirmation;
            }
            self.released.store(true, Ordering::SeqCst);
            self.receipt.as_ref().map_or_else(
                || {
                    Err(EvmError::TransactionDropped {
                        tx_hash: hash,
                        elapsed_secs: 30,
                    })
                },
                |receipt| Ok(receipt.clone()),
            )
        }

        async fn send(
            &self,
            _: Address,
            _: Bytes,
            _: &str,
        ) -> Result<TransactionReceipt, EvmError> {
            panic!("absence probing must not send")
        }
    }

    fn burn_probe_wallet(
        floor: Option<u64>,
        next_nonce: u64,
        heads: impl Iterator<Item = u64>,
    ) -> BurnProbeWallet {
        burn_probe_observations(floor, heads.map(|head| (head, next_nonce)))
    }

    fn burn_probe_observations(
        floor: Option<u64>,
        observations: impl Iterator<Item = (u64, u64)>,
    ) -> BurnProbeWallet {
        let asserter = Asserter::new();
        push_absent_burn_observations(&asserter, floor, observations);
        BurnProbeWallet {
            confirmation: Mutex::new(None),
            released: AtomicBool::new(false),
            receipt: None,
            provider: ProviderBuilder::new()
                .disable_recommended_fillers()
                .connect_mocked_client(asserter),
            hash: TxHash::random(),
            submission: TransactionSubmission {
                sender: Address::random(),
                nonce: 0,
                submitted_after_block: floor,
            },
        }
    }

    fn push_absent_burn_heads(
        asserter: &Asserter,
        floor: Option<u64>,
        next_nonce: u64,
        heads: impl Iterator<Item = u64>,
    ) {
        push_absent_burn_observations(asserter, floor, heads.map(|head| (head, next_nonce)));
    }

    fn push_absent_burn_observations(
        asserter: &Asserter,
        floor: Option<u64>,
        observations: impl Iterator<Item = (u64, u64)>,
    ) {
        for (head, next_nonce) in observations {
            asserter.push_success(&serde_json::Value::Null);
            asserter.push_success(&serde_json::Value::Null);
            let mut block: alloy::rpc::types::Block = alloy::rpc::types::Block::default();
            block.header.number = head;
            block.header.hash = alloy::primitives::BlockHash::random();
            asserter.push_success(&block);
            if head >= floor.unwrap_or(40) + SCAN_FINALITY_MARGIN {
                asserter.push_success(&next_nonce);
            }
        }
    }

    /// Known identity and advancing heads must not mask the recorded burn's
    /// additional submission floor with the wallet's older boundary.
    #[tokio::test]
    async fn burn_status_reports_pending_when_node_lags_submission_block() {
        let server = MockServer::start_async().await;
        let mut nonce_probes = Vec::new();
        let mut head_probes = Vec::new();
        // Match methods independently: caller-floor qualification skips nonce
        // reads, so a FIFO queue containing them would corrupt the next receipt.
        // IDs also make the observed heads advance without a stateful matcher.
        for id in 0u64..16 {
            for method in [
                "eth_getTransactionReceipt",
                "eth_getTransactionByHash",
                "eth_getBlockByNumber",
                "eth_getTransactionCount",
            ] {
                let result = match method {
                    "eth_getBlockByNumber" => {
                        let mut block: alloy::rpc::types::Block =
                            alloy::rpc::types::Block::default();
                        block.header.number = 42 + id;
                        block.header.hash = alloy::primitives::BlockHash::random();
                        serde_json::to_value(block).unwrap()
                    }
                    "eth_getTransactionCount" => json!("0x0"),
                    _ => serde_json::Value::Null,
                };
                let mock = server
                    .mock_async(|when, then| {
                        when.json_body_includes(json!({ "method": method, "id": id }).to_string());
                        then.status(200)
                            .json_body(json!({ "jsonrpc": "2.0", "id": id, "result": result }));
                    })
                    .await;
                match method {
                    "eth_getTransactionCount" => nonce_probes.push(mock),
                    "eth_getBlockByNumber" => head_probes.push(mock),
                    _ => {}
                }
            }
        }
        let mut wallet = burn_probe_wallet(Some(0), 0, std::iter::empty());
        wallet.provider = ProviderBuilder::new()
            .disable_recommended_fillers()
            .connect_http(server.url("/").parse().unwrap());
        let hash = wallet.hash;
        let endpoint = CctpEndpoint::new(
            Chain::Ethereum,
            Address::random(),
            Address::random(),
            Address::random(),
            wallet,
        );

        let status = endpoint
            .burn_status_with_config(
                hash,
                100,
                BurnDropConfig {
                    grace: Duration::ZERO,
                    consecutive_misses: 1,
                    poll_interval: Duration::from_millis(500),
                },
            )
            .await
            .unwrap();

        assert_eq!(
            status,
            crate::BurnTxStatus::Pending,
            "known identity and heads beyond wallet floor 0 but behind burn floor 100 \
             cannot qualify a drop"
        );
        assert!(
            head_probes.iter().map(Mock::calls).sum::<usize>() >= 2,
            "fixture must expose advancing heads, not an unknown-identity or frozen-head shortcut"
        );
        assert_eq!(
            nonce_probes.iter().map(Mock::calls).sum::<usize>(),
            0,
            "heads behind the caller floor must not qualify canonical nonce absence"
        );
    }

    async fn probe_burn(wallet: BurnProbeWallet, caller_floor: u64) -> crate::BurnTxStatus {
        let hash = wallet.hash;
        let evm = CctpEndpoint::new(
            Chain::Ethereum,
            Address::random(),
            Address::random(),
            Address::random(),
            wallet,
        );
        evm.burn_status_with_config(
            hash,
            caller_floor,
            BurnDropConfig {
                grace: Duration::from_millis(10),
                consecutive_misses: 3,
                poll_interval: Duration::from_millis(1),
            },
        )
        .await
        .unwrap()
    }

    async fn probe_reverted_burn(
        confirmation: impl FnOnce(TxHash) -> Result<TransactionReceipt, EvmError>,
    ) -> Result<crate::BurnTxStatus, CctpError> {
        let mut wallet = burn_probe_wallet(Some(40), 0, std::iter::empty());
        let hash = wallet.hash;
        let asserter = Asserter::new();
        let mut shallow_revert = receipt_with_logs(Vec::new());
        shallow_revert.transaction_hash = hash;
        match &mut shallow_revert.inner {
            ReceiptEnvelope::Eip1559(receipt) => receipt.receipt.status = false.into(),
            other => panic!("unexpected fixture receipt envelope {other:?}"),
        }
        asserter.push_success(&shallow_revert);
        wallet.provider = ProviderBuilder::new()
            .disable_recommended_fillers()
            .connect_mocked_client(asserter);
        wallet.confirmation = Mutex::new(Some(confirmation(hash)));
        let evm = CctpEndpoint::new(
            Chain::Ethereum,
            Address::random(),
            Address::random(),
            Address::random(),
            wallet,
        );
        evm.burn_status_with_config(hash, 40, BurnDropConfig::fast())
            .await
    }

    #[tokio::test]
    async fn burn_status_shallow_revert_then_confirmation_drop_reports_dropped() {
        let status = probe_reverted_burn(|hash| {
            Err(EvmError::TransactionDropped {
                tx_hash: hash,
                elapsed_secs: 30,
            })
        })
        .await;
        assert!(
            matches!(status, Ok(crate::BurnTxStatus::Dropped)),
            "a qualified confirmation drop must reach dropped-burn recovery: {status:?}"
        );
    }

    #[tokio::test]
    async fn burn_status_shallow_revert_preserves_confirmation_timeout() {
        let status = probe_reverted_burn(|hash| {
            Err(EvmError::ReceiptTimeout {
                tx_hash: hash,
                timeout_secs: 120,
            })
        })
        .await;
        assert!(
            matches!(
                status,
                Err(CctpError::Evm(EvmError::ReceiptTimeout {
                    timeout_secs: 120,
                    ..
                }))
            ),
            "a confirmation timeout must remain a typed retryable error: {status:?}"
        );
    }

    #[tokio::test]
    async fn burn_status_shallow_revert_uses_confirmed_success_after_reorg() {
        let status = probe_reverted_burn(|hash| {
            let mut confirmed = receipt_with_logs(Vec::new());
            confirmed.transaction_hash = hash;
            Ok(confirmed)
        })
        .await
        .unwrap();
        assert_eq!(status, crate::BurnTxStatus::MinedSuccess);
    }

    #[tokio::test(start_paused = true)]
    async fn burn_status_requires_advancing_canonical_unused_nonce_beyond_submission() {
        for (floor, nonce, advancing, expected) in [
            (40, 0, true, crate::BurnTxStatus::Dropped),
            (1000, 0, true, crate::BurnTxStatus::Pending),
            (40, 1, true, crate::BurnTxStatus::Pending),
            (40, 0, false, crate::BurnTxStatus::Pending),
        ] {
            let wallet = burn_probe_wallet(
                Some(floor),
                nonce,
                (0..200).map(|index| if advancing { 42 + index } else { 42 }),
            );
            let status = probe_burn(wallet, floor).await;
            assert_eq!(
                status, expected,
                "floor {floor}, nonce {nonce}, advancing {advancing}"
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn confirm_drop_retains_identity_for_fresh_burn_status_requalification() {
        for (next_nonce, advancing, expected) in [
            (0, true, crate::BurnTxStatus::Dropped),
            (1, true, crate::BurnTxStatus::Pending),
            (0, false, crate::BurnTxStatus::Pending),
        ] {
            let wallet = burn_probe_wallet(
                Some(40),
                next_nonce,
                (0..200).map(|index| if advancing { 42 + index } else { 42 }),
            );
            let hash = wallet.hash;
            let endpoint = CctpEndpoint::new(
                Chain::Ethereum,
                Address::random(),
                Address::random(),
                Address::random(),
                wallet,
            );
            let error = endpoint
                .confirm_burn::<st0x_evm::OpenChainErrorRegistry>(hash, U256::from(1))
                .await
                .unwrap_err();
            assert!(
                matches!(error, CctpError::Evm(EvmError::TransactionDropped { tx_hash, .. })
                if tx_hash == hash)
            );
            assert_eq!(
                endpoint.wallet.transaction_submission(hash),
                None,
                "generic wallet drops release their submission identity before returning"
            );
            assert_eq!(
                endpoint
                    .burn_status_with_config(hash, 40, BurnDropConfig::fast())
                    .await
                    .unwrap(),
                expected
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn confirm_drop_inconclusive_scan_requalifies_then_proves_exact_absence() {
        let mut wallet = burn_probe_wallet(Some(40), 0, std::iter::empty());
        let hash = wallet.hash;
        let asserter = Asserter::new();
        for _ in 0..SCAN_ATTEMPTS {
            asserter.push_success(&Vec::<Log>::new());
            asserter.push_success(&40u64);
        }
        push_absent_burn_heads(&asserter, Some(40), 0, [42, 43, 44].into_iter());
        for _ in 0..SCAN_ATTEMPTS {
            asserter.push_success(&Vec::<Log>::new());
            asserter.push_success(&44u64);
        }
        push_absent_burn_heads(&asserter, Some(40), 0, [45, 46, 47].into_iter());
        wallet.provider = ProviderBuilder::new()
            .disable_recommended_fillers()
            .connect_mocked_client(asserter);
        let endpoint = CctpEndpoint::new(
            Chain::Ethereum,
            Address::random(),
            Address::random(),
            Address::random(),
            wallet,
        )
        .with_burn_drop_config(BurnDropConfig::fast());
        let amount = U256::from(100);
        let recipient = Address::random();
        let error = endpoint
            .confirm_burn::<st0x_evm::OpenChainErrorRegistry>(hash, amount)
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            CctpError::Evm(EvmError::TransactionDropped { .. })
        ));
        assert_eq!(endpoint.wallet.transaction_submission(hash), None);
        assert!(matches!(
            endpoint
                .find_recorded_burn(
                    amount,
                    BridgeDirection::BaseToEthereum.dest_domain(),
                    recipient,
                    40,
                    hash
                )
                .await,
            Err(CctpError::ScanInconclusive { from_block: 40 })
        ));
        assert_eq!(
            endpoint
                .burn_status_with_config(hash, 40, BurnDropConfig::fast())
                .await
                .unwrap(),
            crate::BurnTxStatus::Dropped,
            "a failed scan after wallet release must not lose fresh requalification evidence"
        );
        assert_eq!(
            endpoint
                .find_recorded_burn(
                    amount,
                    BridgeDirection::BaseToEthereum.dest_domain(),
                    recipient,
                    40,
                    hash
                )
                .await
                .unwrap(),
            crate::RecordedBurnScan::Absent,
            "only the later qualified drop plus authoritative empty scan may page"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn recorded_burn_empty_scan_needs_fresh_canonical_absence_after_wallet_release() {
        for chain in [Chain::Ethereum, Chain::Base] {
            for (nonce, advancing, expected) in [
                (1, true, crate::RecordedBurnScan::Inconclusive),
                (0, false, crate::RecordedBurnScan::Inconclusive),
                (0, true, crate::RecordedBurnScan::Absent),
            ] {
                let mut wallet = burn_probe_wallet(Some(40), 0, std::iter::empty());
                let hash = wallet.hash;
                let asserter = Asserter::new();
                for _ in 0..SCAN_ATTEMPTS {
                    asserter.push_success(&Vec::<Log>::new());
                    asserter.push_success(&44u64);
                }
                push_absent_burn_heads(
                    &asserter,
                    Some(40),
                    nonce,
                    (0..200).map(|index| if advancing { 44 + index } else { 44 }),
                );
                wallet.provider = ProviderBuilder::new()
                    .disable_recommended_fillers()
                    .connect_mocked_client(asserter);
                let endpoint = CctpEndpoint::new(
                    chain,
                    Address::random(),
                    Address::random(),
                    Address::random(),
                    wallet,
                )
                .with_burn_drop_config(BurnDropConfig::fast());
                let error = endpoint
                    .confirm_burn::<st0x_evm::OpenChainErrorRegistry>(hash, U256::from(100))
                    .await
                    .unwrap_err();
                assert!(matches!(
                    error,
                    CctpError::Evm(EvmError::TransactionDropped { .. })
                ));
                assert_eq!(endpoint.wallet.transaction_submission(hash), None);
                assert_eq!(
                    endpoint
                        .find_recorded_burn(U256::from(100), 6, Address::random(), 40, hash)
                        .await
                        .unwrap(),
                    expected,
                    "stale empty logs and an independent numeric head cannot authenticate current absence"
                );
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn reverted_receipt_confirmation_drop_uses_the_same_suspected_drop_recovery() {
        let mut wallet = burn_probe_wallet(Some(40), 0, std::iter::empty());
        let hash = wallet.hash;
        let mut shallow_revert = receipt_with_logs(vec![]);
        shallow_revert.transaction_hash = hash;
        let ReceiptEnvelope::Eip1559(inner) = &mut shallow_revert.inner else {
            panic!("receipt fixture must be EIP-1559");
        };
        inner.receipt.status = false.into();
        let asserter = Asserter::new();
        asserter.push_success(&shallow_revert);
        push_absent_burn_heads(&asserter, Some(40), 0, (0..200).map(|index| 42 + index));
        wallet.provider = ProviderBuilder::new()
            .disable_recommended_fillers()
            .connect_mocked_client(asserter);
        let endpoint = CctpEndpoint::new(
            Chain::Ethereum,
            Address::random(),
            Address::random(),
            Address::random(),
            wallet,
        );
        assert_eq!(
            endpoint
                .burn_status_with_config(hash, 40, BurnDropConfig::fast())
                .await
                .unwrap(),
            crate::BurnTxStatus::Dropped,
            "a reorg during revert confirmation must use the manager's drop cross-check, never reburn"
        );
        assert_eq!(endpoint.wallet.transaction_submission(hash), None);
        assert_eq!(
            endpoint
                .burn_status_with_config(hash, 40, BurnDropConfig::fast())
                .await
                .unwrap(),
            crate::BurnTxStatus::Dropped,
            "later status checks must requalify using retained known identity"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn retained_drop_evidence_is_hash_scoped_and_never_masks_incomplete_live_identity() {
        for scenario in [
            "unrelated hash",
            "incomplete live identity",
            "released incomplete identity",
            "new endpoint",
            "missing boundary",
        ] {
            let mut wallet = burn_probe_wallet(Some(40), 0, (0..200).map(|index| 42 + index));
            if scenario == "missing boundary" {
                wallet.submission.submitted_after_block = None;
            }
            let hash = wallet.hash;
            let mut endpoint = CctpEndpoint::new(
                Chain::Ethereum,
                Address::random(),
                Address::random(),
                Address::random(),
                wallet,
            );
            let error = endpoint
                .confirm_burn::<st0x_evm::OpenChainErrorRegistry>(hash, U256::from(1))
                .await
                .unwrap_err();
            assert!(matches!(
                error,
                CctpError::Evm(EvmError::TransactionDropped { .. })
            ));
            let requested = match scenario {
                "unrelated hash" => TxHash::random(),
                "incomplete live identity" => {
                    endpoint.wallet.released.store(false, Ordering::SeqCst);
                    endpoint.wallet.submission.submitted_after_block = None;
                    hash
                }
                "released incomplete identity" => {
                    endpoint.wallet.released.store(false, Ordering::SeqCst);
                    endpoint.wallet.submission.submitted_after_block = None;
                    let error = endpoint
                        .confirm_burn::<st0x_evm::OpenChainErrorRegistry>(hash, U256::from(1))
                        .await
                        .unwrap_err();
                    assert!(matches!(
                        error,
                        CctpError::Evm(EvmError::TransactionDropped { .. })
                    ));
                    hash
                }
                "new endpoint" => {
                    endpoint = CctpEndpoint::new(
                        Chain::Ethereum,
                        Address::random(),
                        Address::random(),
                        Address::random(),
                        endpoint.wallet,
                    );
                    hash
                }
                "missing boundary" => hash,
                other => panic!("unexpected evidence scenario {other}"),
            };
            assert_eq!(
                endpoint
                    .burn_status_with_config(requested, 40, BurnDropConfig::fast())
                    .await
                    .unwrap(),
                crate::BurnTxStatus::Pending,
                "{scenario}"
            );
        }
    }

    #[tokio::test]
    async fn retained_drop_slot_is_bounded_and_matching_confirmation_clears_it() {
        for success in [true, false] {
            let wallet = burn_probe_wallet(Some(40), 0, std::iter::empty());
            let hash = wallet.hash;
            let submission = wallet.submission;
            let mut endpoint = CctpEndpoint::new(
                Chain::Ethereum,
                Address::random(),
                Address::random(),
                Address::random(),
                wallet,
            );
            *endpoint.suspected_drop_submission.lock().await = Some((TxHash::random(), submission));
            let error = endpoint
                .confirm_burn::<st0x_evm::OpenChainErrorRegistry>(hash, U256::from(1))
                .await
                .unwrap_err();
            assert!(matches!(
                error,
                CctpError::Evm(EvmError::TransactionDropped { .. })
            ));
            assert_eq!(
                *endpoint.suspected_drop_submission.lock().await,
                Some((hash, submission))
            );
            endpoint.clear_suspected_drop(TxHash::random()).await;
            assert_eq!(
                *endpoint.suspected_drop_submission.lock().await,
                Some((hash, submission))
            );

            let error = endpoint
                .wait_for_burn_receipt(hash, async {
                    Err(EvmError::ReceiptTimeout {
                        tx_hash: hash,
                        timeout_secs: 30,
                    })
                })
                .await
                .unwrap_err();
            assert!(matches!(error, EvmError::ReceiptTimeout { tx_hash, .. } if tx_hash == hash));
            assert_eq!(
                *endpoint.suspected_drop_submission.lock().await,
                Some((hash, submission)),
                "inconclusive confirmation errors must retain known evidence"
            );

            let mut receipt = receipt_with_logs(vec![]);
            receipt.transaction_hash = hash;
            let ReceiptEnvelope::Eip1559(inner) = &mut receipt.inner else {
                panic!("receipt fixture must be EIP-1559");
            };
            inner.receipt.status = success.into();
            endpoint.wallet.receipt = Some(receipt);
            let asserter = Asserter::new();
            asserter.push_success(&serde_json::Value::Null);
            endpoint.wallet.provider = ProviderBuilder::new()
                .disable_recommended_fillers()
                .connect_mocked_client(asserter);
            let error = endpoint
                .confirm_burn::<st0x_evm::OpenChainErrorRegistry>(hash, U256::from(1))
                .await
                .unwrap_err();
            if success {
                assert!(
                    matches!(error, CctpError::MessageSentEventNotFound { tx_hash } if tx_hash == hash)
                );
            } else {
                assert!(
                    matches!(error, CctpError::Evm(EvmError::Reverted { tx_hash }) if tx_hash == hash)
                );
            }
            assert_eq!(*endpoint.suspected_drop_submission.lock().await, None);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn burn_status_caller_floor_cannot_replace_unknown_submission_boundary() {
        let wallet = burn_probe_wallet(None, 0, (0..200).map(|index| 42 + index));
        assert_eq!(probe_burn(wallet, 40).await, crate::BurnTxStatus::Pending);
    }

    #[tokio::test(start_paused = true)]
    async fn burn_status_pre_grace_progress_cannot_qualify_frozen_post_grace_head() {
        let wallet = burn_probe_wallet(Some(40), 0, (0..200).map(|index| 42 + index.min(9)));
        let start = Instant::now();
        assert_eq!(probe_burn(wallet, 40).await, crate::BurnTxStatus::Pending);
        assert_eq!(start.elapsed(), Duration::from_millis(20));
    }

    #[tokio::test(start_paused = true)]
    async fn burn_status_allows_progress_after_grace_before_suspecting_drop() {
        let wallet = burn_probe_wallet(
            Some(40),
            0,
            (0u64..200).map(|index| 42 + index.saturating_sub(10)),
        );
        assert_eq!(probe_burn(wallet, 40).await, crate::BurnTxStatus::Dropped);
    }

    #[tokio::test(start_paused = true)]
    async fn burn_status_restored_prepared_without_boundary_remains_pending() {
        let probe = burn_probe_wallet(None, 0, (0..200).map(|index| 42 + index));
        let wallet = RawPrivateKeyWallet::new(&B256::random(), probe.provider, 1).unwrap();
        let hash = probe.hash;
        wallet
            .restore_prepared(&PreparedTransaction::for_test(hash, 0))
            .await;
        let evm = CctpEndpoint::new(
            Chain::Ethereum,
            Address::random(),
            Address::random(),
            Address::random(),
            wallet,
        );
        assert_eq!(
            evm.burn_status_with_config(
                hash,
                40,
                BurnDropConfig {
                    grace: Duration::from_millis(10),
                    consecutive_misses: 3,
                    poll_interval: Duration::from_millis(1),
                }
            )
            .await
            .unwrap(),
            crate::BurnTxStatus::Pending
        );
        assert!(
            evm.wallet.transaction_submission(hash).is_some(),
            "Pending must retain restored nonce ownership"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn burn_status_caller_floor_cannot_lower_known_submission_boundary() {
        let wallet = burn_probe_wallet(Some(1000), 0, (0..200).map(|index| 42 + index));
        assert_eq!(probe_burn(wallet, 40).await, crate::BurnTxStatus::Pending);
    }

    #[tokio::test(start_paused = true)]
    async fn burn_status_inconclusive_post_grace_poll_allows_later_fresh_progress() {
        for consumed_nonce in [false, true] {
            let wallet = burn_probe_observations(
                Some(40),
                (0..200).map(|index| {
                    if index == 10 {
                        if consumed_nonce { (60, 1) } else { (40, 0) }
                    } else {
                        (60 + index, 0)
                    }
                }),
            );
            let start = Instant::now();
            assert_eq!(
                probe_burn(wallet, 40).await,
                crate::BurnTxStatus::Dropped,
                "a single inconclusive post-grace poll cannot restart grace"
            );
            assert_eq!(start.elapsed(), Duration::from_millis(15));
        }
    }

    #[tokio::test(start_paused = true)]
    async fn burn_status_inconclusive_run_stays_pending_until_existing_bound() {
        for (head, nonce) in [(40, 0), (42, 1)] {
            let wallet = burn_probe_observations(Some(40), std::iter::repeat_n((head, nonce), 200));
            let start = Instant::now();
            assert_eq!(probe_burn(wallet, 40).await, crate::BurnTxStatus::Pending);
            assert_eq!(
                start.elapsed(),
                Duration::from_millis(20),
                "inconclusive canonical state must use the same bounded observation window"
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn burn_status_inconclusive_poll_resets_reference_and_consecutive_misses() {
        for consumed_nonce in [false, true] {
            let wallet = burn_probe_observations(
                Some(40),
                (0..200).map(|index| match index {
                    0..=13 => (60 + index, 0),
                    14 if consumed_nonce => (74, 1),
                    14 => (40, 0),
                    15 | 16 => (75, 0),
                    _ => (77 + index - 17, 0),
                }),
            );
            let start = Instant::now();
            assert_eq!(probe_burn(wallet, 40).await, crate::BurnTxStatus::Dropped);
            assert_eq!(
                start.elapsed(),
                Duration::from_millis(19),
                "inconclusive poll must reset both the old head reference and accumulated misses"
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn burn_status_zero_grace_still_allows_post_grace_progress() {
        let wallet = burn_probe_wallet(Some(40), 0, (0..200).map(|index| 42 + index));
        let hash = wallet.hash;
        let evm = CctpEndpoint::new(
            Chain::Ethereum,
            Address::random(),
            Address::random(),
            Address::random(),
            wallet,
        );
        assert_eq!(
            evm.burn_status_with_config(hash, 40, BurnDropConfig::fast())
                .await
                .unwrap(),
            crate::BurnTxStatus::Dropped
        );
    }

    /// Guards the values of the allowance constants against accidental change.
    ///
    /// `STANDING_ALLOWANCE_TARGET` is pinned against a concrete four-limb
    /// `U256::MAX` literal. `STANDING_ALLOWANCE_THRESHOLD` is pinned against
    /// `U256::MAX / U256::from(2u8)` computed at runtime -- an independent
    /// derivation that does not share the `from_limbs` expression used in the
    /// constant definition, so any divergence between the two formulas is caught.
    #[test]
    fn allowance_constants_have_expected_values() {
        // U256::MAX: all 256 bits set, represented as four 64-bit limbs of
        // 0xFFFF_FFFF_FFFF_FFFF (Rust uint uses little-endian limb order).
        assert_eq!(
            STANDING_ALLOWANCE_TARGET,
            U256::from_limbs([u64::MAX, u64::MAX, u64::MAX, u64::MAX]),
            "STANDING_ALLOWANCE_TARGET must be U256::MAX"
        );

        // Cross-check the constant against an independent runtime computation
        // (U256::MAX / 2) rather than duplicating the from_limbs expression.
        // If the constant's from_limbs encoding drifts from the mathematical
        // intention, this assertion will fail even if both sides use different
        // literal representations.
        assert_eq!(
            STANDING_ALLOWANCE_THRESHOLD,
            U256::MAX / U256::from(2u8),
            "STANDING_ALLOWANCE_THRESHOLD must be U256::MAX / 2"
        );
    }

    /// Pins the production recovery cadence to the two-minute window that
    /// `SPEC.md` and `recover_already_minted`'s doc both state.
    ///
    /// Every recovery test overrides the cadence to keep its runtime short, so
    /// without this assertion the production constants are never exercised and
    /// either could drift while the documented contract silently became wrong.
    /// The span is asserted as `probe_interval * (probes - 1)` because the
    /// first probe runs immediately, so only the remaining probes are spaced.
    #[test]
    fn default_mint_recovery_cadence_spans_the_documented_window() {
        let config = MintRecoveryConfig::defaults();

        assert_eq!(
            config
                .probe_interval
                .saturating_mul(config.probes.get().saturating_sub(1)),
            Duration::from_secs(120),
            "the production mint recovery window must stay at the documented two minutes"
        );
    }

    fn burn_log(sender: Address, recipient: Address, hash: TxHash, block: u64) -> Log {
        let event = TokenMessengerV2::DepositForBurn {
            burnToken: Address::random(),
            amount: U256::from(100),
            depositor: sender,
            mintRecipient: FixedBytes::<32>::left_padding_from(recipient.as_slice()),
            destinationDomain: 6,
            destinationTokenMessenger: B256::random(),
            destinationCaller: B256::ZERO,
            maxFee: U256::ZERO,
            minFinalityThreshold: 1000,
            hookData: Bytes::new(),
        };
        Log {
            inner: PrimitiveLog {
                address: Address::random(),
                data: event.encode_log_data(),
            },
            block_number: Some(block),
            transaction_hash: Some(hash),
            log_index: Some(0),
            ..Log::default()
        }
    }

    #[tokio::test(start_paused = true)]
    async fn generic_burn_candidates_require_complete_bounded_scan_even_when_nonempty() {
        for chain in [Chain::Ethereum, Chain::Base] {
            for head in [40u64, 45] {
                let mut wallet = burn_probe_wallet(Some(40), 0, std::iter::empty());
                let recipient = Address::random();
                let older = TxHash::random();
                let newer = TxHash::random();
                let old_log = burn_log(wallet.address(), recipient, older, 41);
                let new_log = burn_log(wallet.address(), recipient, newer, 43);
                let asserter = Asserter::new();
                for attempt in 1..=SCAN_ATTEMPTS {
                    let logs = if attempt == SCAN_ATTEMPTS {
                        vec![old_log.clone(), new_log.clone()]
                    } else {
                        vec![old_log.clone()]
                    };
                    asserter.push_success(&logs);
                    asserter.push_success(&head);
                }
                wallet.provider = ProviderBuilder::new()
                    .disable_recommended_fillers()
                    .connect_mocked_client(asserter);
                let endpoint = CctpEndpoint::new(
                    chain,
                    Address::random(),
                    Address::random(),
                    Address::random(),
                    wallet,
                );
                let result = endpoint
                    .find_recent_burns(U256::from(100), 6, recipient, 40)
                    .await;
                if head == 40 {
                    assert!(
                        matches!(result, Err(CctpError::ScanInconclusive { from_block: 40 })),
                        "a positive partial range cannot authorize ownership-excluded absence: {result:?}"
                    );
                } else {
                    assert_eq!(
                        result.unwrap(),
                        vec![newer, older],
                        "finish the complete range and order newer candidates before earlier responses"
                    );
                }
            }
        }
    }

    #[tokio::test]
    async fn recorded_burn_positive_log_does_not_need_unused_nonce_qualification() {
        let mut wallet = burn_probe_wallet(Some(40), 1, std::iter::empty());
        let hash = wallet.hash;
        let recipient = Address::random();
        let asserter = Asserter::new();
        asserter.push_success(&vec![burn_log(wallet.address(), recipient, hash, 41)]);
        wallet.provider = ProviderBuilder::new()
            .disable_recommended_fillers()
            .connect_mocked_client(asserter);
        let endpoint = CctpEndpoint::new(
            Chain::Ethereum,
            Address::random(),
            Address::random(),
            Address::random(),
            wallet,
        );
        assert_eq!(
            endpoint
                .find_recorded_burn(U256::from(100), 6, recipient, 40, hash)
                .await
                .unwrap(),
            crate::RecordedBurnScan::Found
        );
    }

    fn mint_log(log_index: u64, amount: u64) -> Log {
        let event = TokenMessengerV2::MintAndWithdraw {
            mintRecipient: Address::ZERO,
            amount: U256::from(amount),
            mintToken: Address::ZERO,
            feeCollected: U256::ZERO,
        };

        Log {
            inner: PrimitiveLog {
                address: Address::ZERO,
                data: event.encode_log_data(),
            },
            block_hash: None,
            block_number: None,
            block_timestamp: None,
            transaction_hash: Some(TxHash::ZERO),
            transaction_index: None,
            log_index: Some(log_index),
            removed: false,
        }
    }

    fn receipt_with_logs(logs: Vec<Log>) -> TransactionReceipt {
        TransactionReceipt {
            inner: ReceiptEnvelope::Eip1559(ReceiptWithBloom {
                receipt: Receipt {
                    status: true.into(),
                    cumulative_gas_used: 0,
                    logs,
                },
                logs_bloom: Bloom::default(),
            }),
            transaction_hash: TxHash::ZERO,
            transaction_index: Some(0),
            block_hash: None,
            block_number: Some(1),
            gas_used: 0,
            effective_gas_price: 0,
            blob_gas_used: None,
            blob_gas_price: None,
            from: Address::ZERO,
            to: Some(Address::ZERO),
            contract_address: None,
        }
    }

    const USDC: Address = Address::repeat_byte(0x11);
    const WALLET: Address = Address::repeat_byte(0x22);

    fn transfer_log(token: Address, to: Address, value: U256) -> Log {
        let event = IERC20::Transfer {
            from: Address::repeat_byte(0x33),
            to,
            value,
        };

        Log {
            inner: PrimitiveLog {
                address: token,
                data: event.encode_log_data(),
            },
            block_hash: None,
            block_number: None,
            block_timestamp: None,
            transaction_hash: Some(TxHash::ZERO),
            transaction_index: None,
            log_index: None,
            removed: false,
        }
    }

    #[test]
    fn usdc_credit_sums_every_usdc_transfer_to_the_wallet() {
        let receipt = receipt_with_logs(vec![
            transfer_log(USDC, WALLET, U256::from(600_000u64)),
            transfer_log(USDC, WALLET, U256::from(400_000u64)),
        ]);

        assert_eq!(
            usdc_credit_in_receipt(&receipt, USDC, None, WALLET).unwrap(),
            U256::from(1_000_000u64)
        );
    }

    #[test]
    fn usdc_credit_ignores_other_tokens_and_other_recipients() {
        let other_token = Address::repeat_byte(0x44);
        let elsewhere = Address::repeat_byte(0x55);
        let receipt = receipt_with_logs(vec![
            transfer_log(other_token, WALLET, U256::from(7_000_000u64)),
            transfer_log(USDC, elsewhere, U256::from(9_000_000u64)),
            transfer_log(USDC, WALLET, U256::from(1_000_000u64)),
            mint_log(3, 5_000_000),
        ]);

        assert_eq!(
            usdc_credit_in_receipt(&receipt, USDC, None, WALLET).unwrap(),
            U256::from(1_000_000u64)
        );
    }

    #[test]
    fn usdc_credit_overflow_is_an_error() {
        let receipt = receipt_with_logs(vec![
            transfer_log(USDC, WALLET, U256::MAX),
            transfer_log(USDC, WALLET, U256::from(1u64)),
        ]);

        let error = usdc_credit_in_receipt(&receipt, USDC, None, WALLET).unwrap_err();

        assert!(
            matches!(error, CctpError::UsdcCreditOverflow { tx_hash } if tx_hash == TxHash::ZERO),
            "got: {error:?}"
        );
    }

    #[test]
    fn usdc_credit_fails_on_an_undecodable_usdc_transfer_log() {
        // The Transfer topic without its indexed from/to topics.
        let malformed = Log {
            inner: PrimitiveLog::new_unchecked(
                USDC,
                vec![IERC20::Transfer::SIGNATURE_HASH],
                Bytes::new(),
            ),
            block_hash: None,
            block_number: None,
            block_timestamp: None,
            transaction_hash: Some(TxHash::ZERO),
            transaction_index: None,
            log_index: None,
            removed: false,
        };
        let receipt = receipt_with_logs(vec![
            transfer_log(USDC, WALLET, U256::from(1_000_000u64)),
            malformed,
        ]);

        let error = usdc_credit_in_receipt(&receipt, USDC, None, WALLET).unwrap_err();

        assert!(
            matches!(error, CctpError::UsdcTransferLogDecode { tx_hash, .. } if tx_hash == TxHash::ZERO),
            "got: {error:?}"
        );
    }

    #[test]
    fn parse_mint_receipt_for_message_selects_mint_immediately_below_message_received() {
        // Two batched receiveMessage calls in one tx emit two MintAndWithdraw
        // events; ours is the one immediately preceding our MessageReceived.
        let receipt = receipt_with_logs(vec![mint_log(0, 1_000), mint_log(1, 2_000)]);

        let mint = parse_mint_receipt_for_message(&receipt, 2)
            .expect("a MintAndWithdraw precedes log index 2");

        assert_eq!(
            mint.amount,
            U256::from(2_000u64),
            "must select the mint nearest below the MessageReceived log, not the first"
        );
    }

    #[test]
    fn parse_mint_receipt_for_message_ignores_mints_at_or_above_message_received() {
        let receipt = receipt_with_logs(vec![mint_log(0, 1_000), mint_log(1, 2_000)]);

        // MessageReceived at index 1 -> only the mint at index 0 qualifies.
        let mint = parse_mint_receipt_for_message(&receipt, 1)
            .expect("the MintAndWithdraw at index 0 qualifies");

        assert_eq!(mint.amount, U256::from(1_000u64));
    }

    #[test]
    fn parse_mint_receipt_for_message_returns_none_without_preceding_mint() {
        let receipt = receipt_with_logs(vec![mint_log(5, 1_000)]);

        assert!(
            parse_mint_receipt_for_message(&receipt, 0).is_none(),
            "no MintAndWithdraw below the MessageReceived log must yield None, not a later mint"
        );
    }

    // --- apply_sync_result unit tests ---

    /// Helper that constructs a representative non-revert `CctpError` for use as
    /// the `original_error` sentinel in `apply_sync_result` tests.
    fn sentinel_original_error() -> CctpError {
        CctpError::ScanInconclusive { from_block: 42 }
    }

    /// Helper that constructs a different `CctpError` to use as the sync error,
    /// distinct from the sentinel so tests can verify which error was returned.
    fn sentinel_sync_error() -> CctpError {
        CctpError::ScanInconclusive { from_block: 99 }
    }

    /// When `ensure_standing_allowance` fails during the retry, `apply_sync_result`
    /// returns the original burn error so operator logs see the actionable root cause.
    ///
    /// This covers the branch in `deposit_for_burn_with_allowance_retry` (and
    /// `retry_burn_if_revert`) where sync fails: the sync error is logged but the
    /// original error is what propagates.
    #[test]
    fn apply_sync_result_on_sync_failure_returns_original_error() {
        let original = sentinel_original_error();
        let sync_result: Result<(), CctpError> = Err(sentinel_sync_error());

        let result = apply_sync_result(original, sync_result);

        // Must return Err with the original error (from_block: 42), not the sync
        // error (from_block: 99).
        let CctpError::ScanInconclusive { from_block: 42 } = result.unwrap_err() else {
            panic!("expected original_error (from_block: 42) to be returned on sync failure");
        };
    }

    /// When `ensure_standing_allowance` succeeds during the retry, `apply_sync_result`
    /// returns `Ok(())` so the caller can proceed to issue the retry burn.
    #[test]
    fn apply_sync_result_on_sync_success_returns_ok() {
        let original = sentinel_original_error();
        let sync_result: Result<(), CctpError> = Ok(());

        apply_sync_result(original, sync_result).unwrap();
    }

    /// Base keeps the block counts it had before the windows became time
    /// spans; Ethereum's 12 s blocks give the same spans in fewer blocks.
    #[test]
    fn scan_windows_in_blocks_per_chain() {
        assert_eq!(
            ScanWindow::for_chain(Chain::Base),
            ScanWindow {
                floor_margin: 300,
                mint_lookback: 60_000,
                reconstruction_lookback: 60_000,
            }
        );
        assert_eq!(
            ScanWindow::for_chain(Chain::Ethereum),
            ScanWindow {
                floor_margin: 50,
                mint_lookback: 10_000,
                reconstruction_lookback: 10_000,
            }
        );
    }
}
